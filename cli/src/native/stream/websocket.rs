use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, watch, Mutex, Notify, RwLock};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::native::cdp::client::CdpClient;

use super::http::handle_http_request;
use super::{is_allowed_origin, timestamp_ms, IdleActivity, StreamFrame};

/// Highest per-client frame rate a client may request via the `config` message.
const MAX_CONFIGURABLE_FPS: u32 = 120;

/// Per-connection delivery settings, set by the client's `config` message.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
struct ClientConfig {
    /// Frames per second ceiling. 0 means uncapped.
    max_fps: u32,
    /// Keep at most one frame in flight, awaiting the client's ack.
    ///
    /// Invariant: latest-frame-wins holds only above the socket. Frames already
    /// handed to the transport are delivered in order, so only an ack proves the
    /// client kept up. Mirrors CDP's own `Page.screencastFrameAck`.
    ack_pacing: bool,
}

/// Parse a client `config` message into the settings it changes, leaving the
/// rest of `current` untouched. Returns `None` when the message carries no
/// recognized setting, so an unknown or malformed config never disturbs a
/// working connection.
fn apply_config(current: ClientConfig, parsed: &Value) -> Option<ClientConfig> {
    let mut next = current;
    let mut changed = false;

    // Clamped on u64 before narrowing so oversized values cap at the maximum
    // instead of wrapping.
    if let Some(fps) = parsed.get("maxFps").and_then(|v| v.as_u64()) {
        next.max_fps = fps.min(MAX_CONFIGURABLE_FPS as u64) as u32;
        changed = true;
    }
    if let Some(pacing) = parsed.get("pacing").and_then(|v| v.as_str()) {
        match pacing {
            "ack" => {
                next.ack_pacing = true;
                changed = true;
            }
            "push" => {
                next.ack_pacing = false;
                changed = true;
            }
            _ => {}
        }
    }

    if changed {
        Some(next)
    } else {
        None
    }
}

/// Settings declared on the WebSocket URL, applied before the first frame.
///
/// Invariant: only the URL can govern the opening frame, since the cached frame
/// is written before a `config` message can arrive. An unparsable value leaves
/// the default; a later `config` still wins.
fn config_from_upgrade(request: &str) -> ClientConfig {
    let mut cfg = ClientConfig::default();
    let Some(query) = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|target| target.split_once('?'))
        .map(|(_, query)| query)
    else {
        return cfg;
    };

    for (key, value) in query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.trim(), v.trim()))
    {
        match key {
            "maxFps" => {
                if let Ok(fps) = value.parse::<u64>() {
                    cfg.max_fps = fps.min(MAX_CONFIGURABLE_FPS as u64) as u32;
                }
            }
            "pacing" => match value {
                "ack" => cfg.ack_pacing = true,
                "push" => cfg.ack_pacing = false,
                _ => {}
            },
            _ => {}
        }
    }
    cfg
}

/// Read the acknowledged frame id from a client `ack` message. Acks are
/// cumulative: a client that skipped intermediate ids still unblocks the
/// writer by acknowledging the newest one it rendered.
fn parse_ack_seq(parsed: &Value) -> Option<u64> {
    parsed.get("seq").and_then(|v| v.as_u64())
}

/// Earliest instant the next frame may be sent. `None` (nothing delivered yet)
/// and `fps == 0` (uncapped) both mean "now": a cap bounds the interval between
/// deliveries and never delays the first one.
fn deadline_from(last_sent: Option<Instant>, fps: u32) -> Instant {
    match last_sent {
        Some(sent) if fps > 0 => sent + Duration::from_micros(1_000_000 / fps as u64),
        Some(sent) => sent,
        None => Instant::now(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn accept_loop(
    listener: TcpListener,
    frame_tx: broadcast::Sender<String>,
    frame_watch: watch::Receiver<Option<Arc<StreamFrame>>>,
    client_count: Arc<Mutex<usize>>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<Notify>,
    idle_activity: Arc<IdleActivity>,
    screencasting: Arc<Mutex<bool>>,
    cdp_session_id: Arc<RwLock<super::StreamBinding>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    last_tabs: Arc<RwLock<Vec<Value>>>,
    last_engine: Arc<RwLock<String>>,
    recording: Arc<Mutex<bool>>,
    mut shutdown_rx: watch::Receiver<bool>,
    session_name: String,
) {
    let session_name: Arc<str> = Arc::from(session_name);
    // Invariant: every per-connection task is owned here, never detached, so
    // awaiting this loop proves none survives. Teardown aborts rather than
    // drains, because a writer parked in `ws_tx.send()` against a client that
    // stopped reading would hold a graceful drain open forever.
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            // Reap finished connections so the set does not grow without bound.
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accept_result = listener.accept() => {
                let Ok((stream, addr)) = accept_result else {
                    break;
                };
                let frame_tx = frame_tx.clone();
                let frame_watch = frame_watch.clone();
                let client_count = client_count.clone();
                let client_slot = client_slot.clone();
                let client_notify = client_notify.clone();
                let idle_activity = idle_activity.clone();
                let screencasting = screencasting.clone();
                let cdp_session_id = cdp_session_id.clone();
                let vw = viewport_width.clone();
                let vh = viewport_height.clone();
                let lt = last_tabs.clone();
                let le = last_engine.clone();
                let rec = recording.clone();
                let shutdown_rx = shutdown_rx.clone();
                let sn = session_name.clone();

                connections.spawn(async move {
                    handle_connection(
                        stream,
                        addr,
                        frame_tx,
                        frame_watch,
                        client_count,
                        client_slot,
                        client_notify,
                        idle_activity,
                        screencasting,
                        cdp_session_id,
                        vw,
                        vh,
                        lt,
                        le,
                        rec,
                        shutdown_rx,
                        sn,
                    )
                    .await;
                });
            }
        }
    }
    connections.shutdown().await;
}

fn is_websocket_upgrade(request: &str) -> bool {
    request.lines().any(|line| {
        if let Some((name, value)) = line.split_once(':') {
            name.trim().eq_ignore_ascii_case("upgrade")
                && value.trim().eq_ignore_ascii_case("websocket")
        } else {
            false
        }
    })
}

/// Peek at the TCP stream to dispatch between WebSocket upgrade and plain HTTP.
#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    frame_tx: broadcast::Sender<String>,
    frame_watch: watch::Receiver<Option<Arc<StreamFrame>>>,
    client_count: Arc<Mutex<usize>>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<Notify>,
    idle_activity: Arc<IdleActivity>,
    screencasting: Arc<Mutex<bool>>,
    cdp_session_id: Arc<RwLock<super::StreamBinding>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    last_tabs: Arc<RwLock<Vec<Value>>>,
    last_engine: Arc<RwLock<String>>,
    recording: Arc<Mutex<bool>>,
    shutdown_rx: watch::Receiver<bool>,
    session_name: Arc<str>,
) {
    let mut buf = [0u8; 4096];
    let n = match stream.peek(&mut buf).await {
        Ok(n) => n,
        Err(_) => return,
    };
    let request = String::from_utf8_lossy(&buf[..n]);

    if is_websocket_upgrade(&request) {
        let frame_rx = frame_tx.subscribe();
        let initial_config = config_from_upgrade(&request);
        handle_ws_client(
            stream,
            addr,
            initial_config,
            frame_rx,
            frame_watch,
            client_count,
            client_slot,
            client_notify,
            idle_activity,
            screencasting,
            cdp_session_id,
            viewport_width,
            viewport_height,
            last_tabs,
            last_engine,
            recording,
            shutdown_rx,
        )
        .await;
    } else {
        handle_http_request(stream, &buf[..n], &last_tabs, &last_engine, &session_name).await;
    }
}

/// One WebSocket client, split in two halves: a reader task dispatching input
/// to CDP, and a writer loop delivering frames latest-first under an optional
/// per-client cap. Invariant: the halves never wait on each other, which is
/// what keeps input responsive while a frame is mid-write.
#[allow(clippy::result_large_err, clippy::too_many_arguments)]
async fn handle_ws_client(
    stream: TcpStream,
    _addr: SocketAddr,
    initial_config: ClientConfig,
    mut broadcast_rx: broadcast::Receiver<String>,
    mut frame_watch: watch::Receiver<Option<Arc<StreamFrame>>>,
    client_count: Arc<Mutex<usize>>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<Notify>,
    idle_activity: Arc<IdleActivity>,
    screencasting: Arc<Mutex<bool>>,
    cdp_session_id: Arc<RwLock<super::StreamBinding>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    last_tabs: Arc<RwLock<Vec<Value>>>,
    last_engine: Arc<RwLock<String>>,
    recording: Arc<Mutex<bool>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let callback =
        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
         resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
            let origin = req
                .headers()
                .get("origin")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            if !is_allowed_origin(origin.as_deref()) {
                let mut reject =
                    tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some(
                        "Origin not allowed".to_string(),
                    ));
                *reject.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
                return Err(reject);
            }
            Ok(resp)
        };

    let ws_stream = match tokio_tungstenite::accept_hdr_async(stream, callback).await {
        Ok(ws) => ws,
        Err(_) => return,
    };

    {
        let mut count = client_count.lock().await;
        *count += 1;
    }

    let (mut ws_tx, ws_rx) = ws_stream.split();

    // Watch channels, not atomics, so a mid-stream change wakes the writer's
    // select! below instead of leaving it asleep on a stale deadline.
    let (config_tx, mut config_rx) = watch::channel::<ClientConfig>(initial_config);
    let (ack_tx, mut ack_rx) = watch::channel::<u64>(0);
    // Spawned before the status, tabs and seed writes below: those are
    // unbounded sends, and a full receive queue would otherwise hold input
    // behind the whole handshake.
    let mut reader_task = AbortOnDrop(tokio::spawn(reader_loop(
        ws_rx,
        client_slot.clone(),
        cdp_session_id.clone(),
        config_tx,
        ack_tx,
        idle_activity.clone(),
    )));

    {
        let guard = client_slot.read().await;
        let connected = guard.is_some();
        // Seed identity before any cached pixels. Subsequent binding messages
        // invalidate in-flight frames on clients; the server still checks sends.
        let binding_message = cdp_session_id.read().await.message(guard.as_ref());
        let _ = ws_tx.send(Message::Text(binding_message.to_string())).await;
        let sc = *screencasting.lock().await;
        let vw = *viewport_width.lock().await;
        let vh = *viewport_height.lock().await;
        let eng = last_engine.read().await.clone();
        let rec = *recording.lock().await;
        let status = json!({
            "type": "status",
            "connected": connected,
            "screencasting": sc,
            "viewportWidth": vw,
            "viewportHeight": vh,
            "engine": eng,
            "recording": rec,
        });
        let _ = ws_tx.send(Message::Text(status.to_string())).await;

        let tabs = last_tabs.read().await;
        if !tabs.is_empty() {
            let tabs_msg = json!({
                "type": "tabs",
                "tabs": *tabs,
                "timestamp": timestamp_ms(),
            });
            let _ = ws_tx.send(Message::Text(tabs_msg.to_string())).await;
        }
    }

    // Invariant: only a successful send writes `last_sent`. `None` means
    // nothing was delivered, so the cap owes this connection no wait.
    let mut last_sent: Option<Instant> = None;
    // Id written but not yet acknowledged, ack pacing only. While set the
    // writer holds, and newer frames replace each other in the watch channel.
    let mut awaiting_ack: Option<u64> = None;

    // Seed with the newest frame, marked seen so the writer does not re-send
    // it. Charged against the cap, so a URL-declared cap governs the gap after.
    let initial_frame = frame_watch.borrow_and_update().clone();
    if let Some(frame) = initial_frame {
        if let Ok(prefix) = frame_binding_message(&frame, &client_slot, &cdp_session_id).await {
            if let Some(prefix) = prefix {
                let _ = ws_tx.send(Message::Text(prefix)).await;
            }
            if ws_tx.send(Message::Text(frame.json.clone())).await.is_ok() {
                last_sent = Some(Instant::now());
                if initial_config.ack_pacing {
                    awaiting_ack = frame.seq;
                }
            }
        }
    }

    client_notify.notify_one();

    let mut next_allowed = deadline_from(last_sent, initial_config.max_fps);
    let mut pending_frame = false;

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    let _ = ws_tx.send(Message::Close(None)).await;
                    break;
                }
            }
            _ = &mut reader_task.0 => {
                break;
            }
            msg = broadcast_rx.recv() => {
                match msg {
                    Ok(data) => {
                        // The latest frame may have overtaken queued controls.
                        // Do not roll the frontend back to an obsolete binding.
                        if let Ok(message) = serde_json::from_str::<Value>(&data) {
                            if message.get("type").and_then(Value::as_str) == Some("binding") {
                                let client = client_slot.read().await;
                                let binding = cdp_session_id.read().await;
                                if message.get("bindingEpoch").and_then(Value::as_str) != binding.epoch_for(client.as_ref()) {
                                    continue;
                                }
                            }
                        }
                        if ws_tx.send(Message::Text(data)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            changed = frame_watch.changed(), if !pending_frame => {
                if changed.is_err() {
                    break;
                }
                pending_frame = true;
            }
            changed = config_rx.changed() => {
                // The sender lives as long as the reader, so an error here
                // means the reader ended. Break and let cleanup run.
                if changed.is_err() {
                    break;
                }
                let cfg = *config_rx.borrow_and_update();
                // Loosening the cap pulls the deadline into the past, so a
                // pending frame goes out at once.
                next_allowed = deadline_from(last_sent, cfg.max_fps);
                // Leaving ack pacing releases a frame that is still waiting on
                // an acknowledgement the client will now never send.
                if !cfg.ack_pacing {
                    awaiting_ack = None;
                }
            }
            changed = ack_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let acked = *ack_rx.borrow_and_update();
                // Cumulative: an ack for a newer frame covers the one in
                // flight, so a client that renders out of order or skips ids
                // still unblocks the writer.
                if awaiting_ack.is_some_and(|seq| acked >= seq) {
                    awaiting_ack = None;
                }
            }
            _ = tokio::time::sleep_until(next_allowed), if pending_frame && awaiting_ack.is_none() => {
                // Invariant: read at send time, not arrival time. That is what
                // makes this latest-frame-wins; anything that arrived while the
                // writer waited is skipped rather than queued.
                let frame = frame_watch.borrow_and_update().clone();
                pending_frame = false;
                let cfg = *config_rx.borrow();
                if let Some(frame) = frame {
                    let Ok(prefix) = frame_binding_message(&frame, &client_slot, &cdp_session_id).await else { continue };
                    if let Some(prefix) = prefix {
                        if ws_tx.send(Message::Text(prefix)).await.is_err() { break; }
                    }
                    if cfg.ack_pacing {
                        awaiting_ack = frame.seq;
                        // Settle against the banked watermark: a client that
                        // acked ahead of the stream would never move it again.
                        if awaiting_ack.is_some_and(|seq| *ack_rx.borrow() >= seq) {
                            awaiting_ack = None;
                        }
                    }
                    if ws_tx.send(Message::Text(frame.json.clone())).await.is_err() {
                        break;
                    }
                    last_sent = Some(Instant::now());
                }
                next_allowed = deadline_from(last_sent, cfg.max_fps);
            }
        }
    }

    drop(reader_task);

    {
        let mut count = client_count.lock().await;
        *count = count.saturating_sub(1);
    }

    client_notify.notify_one();
}

/// Frame and broadcast channels are independently paced. Prefix a current
/// frame with its SAME binding so the first/only new frame cannot overtake its
/// invalidation message. Stale cached frames are skipped, never re-stamped.
async fn frame_binding_message(
    frame: &super::StreamFrame,
    client_slot: &RwLock<Option<Arc<CdpClient>>>,
    binding: &RwLock<super::StreamBinding>,
) -> Result<Option<String>, ()> {
    let Some(epoch) = frame.binding_epoch.as_deref() else {
        return Ok(None);
    };
    let client = client_slot.read().await;
    let current = binding.read().await;
    if current.epoch_for(client.as_ref()) != Some(epoch) {
        return Err(());
    }
    Ok(Some(current.message(client.as_ref()).to_string()))
}

/// Reads client messages and dispatches them without waiting on frame
/// delivery. Input reaches CDP sequentially: mouse move, press and release
/// must not be reordered.
async fn reader_loop(
    mut ws_rx: SplitStream<WebSocketStream<TcpStream>>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    cdp_session_id: Arc<RwLock<super::StreamBinding>>,
    config: watch::Sender<ClientConfig>,
    ack: watch::Sender<u64>,
    idle_activity: Arc<IdleActivity>,
) {
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let parsed: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let msg_type = parsed.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if msg_type == "config" {
                    // Bound to a local first: a `watch` Ref taken in the
                    // `if let` scrutinee lives for the whole body, and
                    // `send_replace` would deadlock against it.
                    let current = *config.borrow();
                    if let Some(next) = apply_config(current, &parsed) {
                        // send_replace, not send, so an unchanged value still
                        // wakes the writer to re-derive its deadline.
                        let _ = config.send_replace(next);
                    }
                    continue;
                }
                if msg_type == "ack" {
                    if let Some(seq) = parse_ack_seq(&parsed) {
                        // send_if_modified keeps the acknowledged id monotonic:
                        // a late ack for an older frame must not walk it back
                        // and re-block the writer.
                        ack.send_if_modified(|current| {
                            if seq > *current {
                                *current = seq;
                                true
                            } else {
                                false
                            }
                        });
                    }
                    continue;
                }
                // Validate text before activity tracking or acquiring ownership guards.
                if msg_type == "input_text" && input_text(&parsed).is_none() {
                    continue;
                }
                dispatch_bound_input(
                    msg_type,
                    &parsed,
                    &client_slot,
                    &cdp_session_id,
                    &idle_activity,
                )
                .await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
}

/// Validation and send are inside the SAME client/binding read guards that
/// exclude rebind writers. Never check a token and then reacquire a newer target.
async fn dispatch_bound_input(
    msg_type: &str,
    parsed: &Value,
    client_slot: &RwLock<Option<Arc<CdpClient>>>,
    binding: &RwLock<super::StreamBinding>,
    idle_activity: &IdleActivity,
) {
    if !is_user_input_message_type(msg_type)
        || (msg_type == "input_text" && input_text(parsed).is_none())
    {
        return;
    }
    let client_guard = client_slot.read().await;
    let Some(client) = client_guard.as_ref() else {
        return;
    };
    let binding_guard = binding.read().await;
    let Some(epoch) = binding_guard.epoch_for(Some(client)) else {
        return;
    };
    if parsed.get("bindingEpoch").and_then(Value::as_str) != Some(epoch)
        || ["sessionId", "targetId", "session_id", "target_id"]
            .iter()
            .any(|key| parsed.get(key).is_some())
    {
        return;
    }
    idle_activity.mark();
    dispatch_input(
        msg_type,
        parsed,
        client,
        binding_guard.session_id.as_deref(),
    )
    .await;
}

/// Aborts its task on drop.
///
/// Invariant: the reader is spawned inside the connection task, so aborting the
/// connection does not reach it. Without this guard the reader outlives
/// teardown and keeps dispatching input.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// `Input.dispatchKeyEvent` params for a client `input_keyboard` message.
///
/// Invariant: an omitted optional string is left out, never sent as `null`.
/// CDP rejects the whole command on a null string and the caller discards the
/// error, so one null silently drops the keystroke. `""` is not a substitute:
/// `text: ""` on a printable key inserts no character.
fn keyboard_params(parsed: &Value) -> Value {
    let mut params = serde_json::Map::new();
    params.insert(
        "type".into(),
        json!(parsed
            .get("eventType")
            .and_then(|v| v.as_str())
            .unwrap_or("keyDown")),
    );
    for field in ["key", "code", "text"] {
        if let Some(value) = parsed.get(field).and_then(|v| v.as_str()) {
            params.insert(field.into(), json!(value));
        }
    }
    params.insert(
        "windowsVirtualKeyCode".into(),
        json!(parsed
            .get("windowsVirtualKeyCode")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)),
    );
    params.insert(
        "modifiers".into(),
        json!(parsed
            .get("modifiers")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)),
    );
    Value::Object(params)
}

// Keep this UTF-8 byte limit aligned with dashboard/src/lib/viewport-input.ts.
const MAX_INPUT_TEXT_BYTES: usize = 64 * 1024;

/// Text-only, bounded stream message. No client-supplied session/target routing
/// or extra fields are accepted. Never log or broadcast its contents.
fn input_text(parsed: &Value) -> Option<&str> {
    let object = parsed.as_object()?;
    if object.len() != 3
        || object.get("type")?.as_str()? != "input_text"
        || object.get("bindingEpoch")?.as_str()?.len() != 36
    {
        return None;
    }
    let text = object.get("text")?.as_str()?;
    (!text.is_empty() && text.len() <= MAX_INPUT_TEXT_BYTES).then_some(text)
}

/// Dispatches one client input event to CDP.
///
/// Invariant: sends without awaiting Chrome's reply. The reply carries nothing
/// this path uses, and awaiting it serializes the whole reader behind one round
/// trip per event, so a mouse sweep delays the click behind it. Ordering still
/// holds: every send takes the same socket mutex and this task calls them in
/// order, so press never overtakes move.
async fn dispatch_input(
    msg_type: &str,
    parsed: &Value,
    client: &CdpClient,
    session_id: Option<&str>,
) {
    match msg_type {
        "input_mouse" => {
            let _ = client
                .send_command_no_wait(
                    "Input.dispatchMouseEvent",
                    Some(json!({
                        "type": parsed.get("eventType").and_then(|v| v.as_str()).unwrap_or("mouseMoved"),
                        "x": parsed.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        "y": parsed.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        "button": parsed.get("button").and_then(|v| v.as_str()).unwrap_or("none"),
                        "clickCount": parsed.get("clickCount").and_then(|v| v.as_i64()).unwrap_or(0),
                        "deltaX": parsed.get("deltaX").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        "deltaY": parsed.get("deltaY").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        "modifiers": parsed.get("modifiers").and_then(|v| v.as_i64()).unwrap_or(0),
                    })),
                    session_id,
                )
                .await;
        }
        "input_keyboard" => {
            let _ = client
                .send_command_no_wait(
                    "Input.dispatchKeyEvent",
                    Some(keyboard_params(parsed)),
                    session_id,
                )
                .await;
        }
        "input_text" => {
            if let (Some(text), Some(sid)) = (input_text(parsed), session_id) {
                let _ = client
                    .send_command_no_wait(
                        "Input.insertText",
                        Some(json!({ "text": text })),
                        Some(sid),
                    )
                    .await;
            }
        }
        "input_touch" => {
            let _ = client
                .send_command_no_wait(
                    "Input.dispatchTouchEvent",
                    Some(json!({
                        "type": parsed.get("eventType").and_then(|v| v.as_str()).unwrap_or("touchStart"),
                        "touchPoints": parsed.get("touchPoints").unwrap_or(&json!([])),
                        "modifiers": parsed.get("modifiers").and_then(|v| v.as_i64()).unwrap_or(0),
                    })),
                    session_id,
                )
                .await;
        }
        "status" => {}
        _ => {}
    }
}

fn is_user_input_message_type(msg_type: &str) -> bool {
    matches!(
        msg_type,
        "input_mouse" | "input_keyboard" | "input_touch" | "input_text"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    const TEST_EPOCH: &str = "11111111-1111-4111-8111-111111111111";

    #[test]
    fn dashboard_input_messages_count_as_user_activity() {
        for msg_type in ["input_mouse", "input_keyboard", "input_touch", "input_text"] {
            assert!(is_user_input_message_type(msg_type));
        }
        assert!(!is_user_input_message_type("status"));
        assert!(!is_user_input_message_type("frame"));
    }

    #[test]
    fn test_input_text_schema_and_utf8_byte_limit() {
        let valid = json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "line 1\nстрока 2 漢字"});
        assert_eq!(input_text(&valid), valid["text"].as_str());
        let at_limit = json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "a".repeat(MAX_INPUT_TEXT_BYTES)});
        assert!(input_text(&at_limit).is_some());
        for invalid in [
            json!(null),
            json!({"type": "input_text"}),
            json!({"type": "input_text", "text": "missing epoch"}),
            json!({"type": "input_text", "bindingEpoch": 7, "text": "wrong epoch type"}),
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": null}),
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": 123}),
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": ""}),
            json!({"type": "input_keyboard", "text": "not paste"}),
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "safe", "sessionId": "other"}),
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "a".repeat(MAX_INPUT_TEXT_BYTES + 1)}),
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "я".repeat(MAX_INPUT_TEXT_BYTES / 2 + 1)}),
        ] {
            assert!(input_text(&invalid).is_none());
        }
    }

    #[tokio::test]
    async fn test_input_text_dispatch_is_bounded_and_pins_session() {
        let (url, seen) = silent_cdp_server().await;
        let client = CdpClient::connect(&url).await.expect("mock cdp connect");
        let message =
            json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "synthetic\nтекст"});
        let dispatched = tokio::time::timeout(Duration::from_secs(5), async {
            dispatch_input("input_text", &message, &client, None).await;
            dispatch_input(
                "input_text",
                &json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": 3}),
                &client,
                Some("S-PINNED"),
            )
            .await;
            dispatch_input(
                "input_text",
                &json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "a".repeat(MAX_INPUT_TEXT_BYTES + 1)}),
                &client,
                Some("S-PINNED"),
            )
            .await;
            dispatch_input(
                "input_text",
                &json!({"type": "input_text", "bindingEpoch": TEST_EPOCH, "text": "no", "sessionId": "S-OTHER"}),
                &client,
                Some("S-PINNED"),
            )
            .await;
            dispatch_input("input_text", &message, &client, Some("S-PINNED")).await;
            dispatch_input("input_text", &message, &client, Some("")).await;
            // A trailing marker proves all earlier commands were drained.
            dispatch_input(
                "input_keyboard",
                &json!({"eventType": "keyUp", "key": "Control"}),
                &client,
                Some("S-PINNED"),
            )
            .await;
        })
        .await;
        assert!(dispatched.is_ok(), "text must not await a CDP reply");
        tokio::time::timeout(Duration::from_secs(5), async {
            while seen.lock().await.len() < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("mock should receive all dispatched input");
        let commands = seen.lock().await;
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0]["method"], "Input.insertText");
        assert_eq!(commands[0]["params"], json!({"text": "synthetic\nтекст"}));
        assert_eq!(commands[0]["sessionId"], "S-PINNED");
        assert_eq!(commands[1]["method"], "Input.insertText");
        assert!(commands[1].get("sessionId").is_none());
        assert_eq!(commands[2]["method"], "Input.dispatchKeyEvent");
        assert_eq!(client.pending_len().await, 0);
    }

    #[tokio::test]
    async fn test_stale_displayed_frame_paste_is_rejected_across_binding_changes() {
        let (url, seen) = silent_cdp_server().await;
        let client = Arc::new(CdpClient::connect(&url).await.unwrap());
        let activity = Arc::new(IdleActivity::new());
        let (server, slot) = super::super::StreamServer::start_without_client(
            0,
            "epoch-test".into(),
            false,
            activity.clone(),
        )
        .await
        .unwrap();
        *slot.write().await = Some(client.clone());
        server
            .bind_cdp_session_and_broadcast_tabs(Some("A".into()), &[])
            .await;
        // This token represents a frame still visible/in flight on the client.
        let displayed_a_epoch = server.cdp_session_id.read().await.epoch.clone();
        let displayed_frame = super::super::StreamFrame {
            binding_epoch: Some(displayed_a_epoch.clone()),
            seq: Some(1),
            json: "synthetic frame".into(),
        };
        let prefix = frame_binding_message(&displayed_frame, &slot, &server.cdp_session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&prefix).unwrap()["bindingEpoch"],
            displayed_a_epoch
        );
        let paste_a = json!({"type": "input_text", "bindingEpoch": displayed_a_epoch, "text": "synthetic stale paste"});
        server
            .bind_cdp_session_and_broadcast_tabs(Some("B".into()), &[])
            .await;
        let b_epoch = server.cdp_session_id.read().await.epoch.clone();
        assert_ne!(displayed_a_epoch, b_epoch);
        assert!(
            frame_binding_message(&displayed_frame, &slot, &server.cdp_session_id)
                .await
                .is_err(),
            "never refresh an old frame's epoch"
        );
        dispatch_bound_input(
            "input_text",
            &paste_a,
            &slot,
            &server.cdp_session_id,
            &activity,
        )
        .await;
        // Stale mouse/keyboard (including blur releases) cannot go to B either.
        for kind in ["input_mouse", "input_keyboard", "input_touch"] {
            dispatch_bound_input(
                kind,
                &json!({"bindingEpoch": displayed_a_epoch, "eventType": "keyUp", "key": "Control"}),
                &slot,
                &server.cdp_session_id,
                &activity,
            )
            .await;
        }
        let paste_b = json!({"type": "input_text", "bindingEpoch": b_epoch, "text": "synthetic B"});
        dispatch_bound_input(
            "input_text",
            &paste_b,
            &slot,
            &server.cdp_session_id,
            &activity,
        )
        .await;
        server
            .bind_cdp_session_and_broadcast_tabs(Some("A".into()), &[])
            .await;
        let fresh_a_epoch = server.cdp_session_id.read().await.epoch.clone();
        assert_ne!(
            displayed_a_epoch, fresh_a_epoch,
            "A->B->A must not reuse A's old token"
        );
        dispatch_bound_input(
            "input_text",
            &paste_a,
            &slot,
            &server.cdp_session_id,
            &activity,
        )
        .await;
        let fresh = json!({"type": "input_text", "bindingEpoch": fresh_a_epoch, "text": "synthetic fresh A"});
        dispatch_bound_input(
            "input_text",
            &fresh,
            &slot,
            &server.cdp_session_id,
            &activity,
        )
        .await;
        // A newly created server/binding never reuses an epoch, even for the
        // same CDP client/session, so reconnecting old pixels fail closed.
        let restarted = RwLock::new(super::super::StreamBinding::new(
            Some("A".into()),
            Some(&client),
        ));
        assert_ne!(restarted.read().await.epoch, fresh_a_epoch);
        dispatch_bound_input("input_text", &fresh, &slot, &restarted, &activity).await;
        // A marker after all attempted sends proves stale events sent nothing.
        dispatch_bound_input(
            "input_keyboard",
            &json!({"bindingEpoch": fresh_a_epoch, "eventType": "keyUp", "key": "Shift"}),
            &slot,
            &server.cdp_session_id,
            &activity,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while seen.lock().await.len() < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let commands = seen.lock().await;
        assert_eq!(commands.len(), 3, "no stale input may reach CDP");
        assert_eq!(commands[0]["method"], "Input.insertText");
        assert_eq!(commands[0]["sessionId"], "B");
        assert_eq!(commands[0]["params"]["text"], "synthetic B");
        assert_eq!(commands[1]["sessionId"], "A");
        assert_eq!(commands[1]["params"]["text"], "synthetic fresh A");
        assert_eq!(commands[2]["method"], "Input.dispatchKeyEvent");
        drop(commands);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_input_waiting_behind_rebind_rechecks_epoch_under_binding_guard() {
        let (url, seen) = silent_cdp_server().await;
        let client = Arc::new(CdpClient::connect(&url).await.unwrap());
        let slot = Arc::new(RwLock::new(Some(client.clone())));
        let binding = Arc::new(RwLock::new(super::super::StreamBinding::new(
            Some("A".into()),
            Some(&client),
        )));
        let old_epoch = binding.read().await.epoch.clone();
        let mut writer = binding.write().await;
        let waiting_slot = slot.clone();
        let waiting_binding = binding.clone();
        let activity = Arc::new(IdleActivity::new());
        let waiting_activity = activity.clone();
        let queued = tokio::spawn(async move {
            dispatch_bound_input("input_text", &json!({"type": "input_text", "bindingEpoch": old_epoch, "text": "must not reach B"}), &waiting_slot, &waiting_binding, &waiting_activity).await;
        });
        // The old event is queued behind a writer, not merely checked against
        // a client-side attribute. The send must validate the post-rebind state.
        tokio::time::timeout(Duration::from_secs(5), async {
            while slot.try_write().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("input must hold the client guard while waiting for binding");
        writer.rebind(Some("B".into()), Some(&client));
        let new_epoch = writer.epoch.clone();
        drop(writer);
        queued.await.unwrap();
        dispatch_bound_input(
            "input_text",
            &json!({"type": "input_text", "bindingEpoch": new_epoch, "text": "current B"}),
            &slot,
            &binding,
            &activity,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while seen.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let commands = seen.lock().await;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["sessionId"], "B");
        assert_eq!(commands[0]["params"]["text"], "current B");
    }

    #[tokio::test]
    async fn test_client_replacement_gap_rejects_old_epoch_even_with_same_session_id() {
        let (old_url, old_seen) = silent_cdp_server().await;
        let (new_url, new_seen) = silent_cdp_server().await;
        let old = Arc::new(CdpClient::connect(&old_url).await.unwrap());
        let new = Arc::new(CdpClient::connect(&new_url).await.unwrap());
        let slot = RwLock::new(Some(old.clone()));
        let binding = RwLock::new(super::super::StreamBinding::new(
            Some("same-session".into()),
            Some(&old),
        ));
        let epoch = binding.read().await.epoch.clone();
        let paste = json!({"type": "input_text", "bindingEpoch": epoch, "text": "old pixels"});
        let activity = IdleActivity::new();
        // Models update_stream_client's gap between changing client_slot and
        // rebinding the page session. An old token must authorize neither client.
        *slot.write().await = Some(new.clone());
        dispatch_bound_input("input_text", &paste, &slot, &binding, &activity).await;
        binding
            .write()
            .await
            .rebind(Some("same-session".into()), Some(&new));
        let fresh = binding.read().await.epoch.clone();
        assert_ne!(epoch, fresh);
        dispatch_bound_input("input_text", &paste, &slot, &binding, &activity).await;
        dispatch_bound_input(
            "input_text",
            &json!({"type": "input_text", "bindingEpoch": fresh, "text": "new pixels"}),
            &slot,
            &binding,
            &activity,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while new_seen.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(old_seen.lock().await.is_empty());
        let commands = new_seen.lock().await;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["params"]["text"], "new pixels");
    }

    fn fps_of(msg: serde_json::Value) -> Option<u32> {
        apply_config(ClientConfig::default(), &msg).map(|c| c.max_fps)
    }

    #[test]
    fn test_parse_config_max_fps_valid() {
        assert_eq!(fps_of(json!({"type": "config", "maxFps": 10})), Some(10));
        assert_eq!(fps_of(json!({"type": "config", "maxFps": 0})), Some(0));
        assert_eq!(fps_of(json!({"type": "config", "maxFps": 120})), Some(120));
    }

    #[test]
    fn test_parse_config_max_fps_clamps_without_wrapping() {
        assert_eq!(
            fps_of(json!({"type": "config", "maxFps": 500})),
            Some(MAX_CONFIGURABLE_FPS)
        );
        // u32::MAX + 2 would wrap to 1 if narrowed before clamping.
        assert_eq!(
            fps_of(json!({"type": "config", "maxFps": 4294967297u64})),
            Some(MAX_CONFIGURABLE_FPS)
        );
    }

    #[test]
    fn test_parse_config_max_fps_invalid() {
        assert_eq!(
            apply_config(ClientConfig::default(), &json!({"type": "config"})),
            None
        );
        assert_eq!(fps_of(json!({"type": "config", "maxFps": -5})), None);
        assert_eq!(fps_of(json!({"type": "config", "maxFps": "fast"})), None);
    }

    #[test]
    fn test_config_pacing_opt_in_and_out() {
        let acked = apply_config(
            ClientConfig::default(),
            &json!({"type": "config", "pacing": "ack"}),
        )
        .expect("pacing is a recognized setting");
        assert!(acked.ack_pacing);

        let back_to_push = apply_config(acked, &json!({"type": "config", "pacing": "push"}))
            .expect("push is a recognized setting");
        assert!(!back_to_push.ack_pacing);

        // An unknown pacing value leaves the connection as it was.
        assert_eq!(
            apply_config(acked, &json!({"type": "config", "pacing": "turbo"})),
            None
        );
    }

    #[test]
    fn test_config_settings_are_independent() {
        // Changing the cap must not drop the client out of ack pacing.
        let acked = apply_config(
            ClientConfig::default(),
            &json!({"type": "config", "pacing": "ack"}),
        )
        .unwrap();
        let recapped = apply_config(acked, &json!({"type": "config", "maxFps": 15})).unwrap();
        assert_eq!(recapped.max_fps, 15);
        assert!(recapped.ack_pacing);

        let repaced = apply_config(recapped, &json!({"type": "config", "pacing": "push"})).unwrap();
        assert_eq!(repaced.max_fps, 15);
        assert!(!repaced.ack_pacing);
    }

    fn upgrade(target: &str) -> String {
        format!(
            "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n\r\n",
            target
        )
    }

    #[test]
    fn test_config_from_upgrade_reads_url_settings() {
        let cfg = config_from_upgrade(&upgrade("/?pacing=ack&maxFps=10"));
        assert!(cfg.ack_pacing);
        assert_eq!(cfg.max_fps, 10);

        let cfg = config_from_upgrade(&upgrade("/?maxFps=5"));
        assert!(!cfg.ack_pacing);
        assert_eq!(cfg.max_fps, 5);
    }

    #[test]
    fn test_config_from_upgrade_defaults_when_absent_or_unparsable() {
        for target in [
            "/",
            "/?",
            "/?pacing=turbo",
            "/?maxFps=fast",
            "/?maxFps=-1",
            "/?other=1",
        ] {
            assert_eq!(
                config_from_upgrade(&upgrade(target)),
                ClientConfig::default(),
                "target {} should leave the defaults alone",
                target
            );
        }
    }

    #[test]
    fn test_config_from_upgrade_clamps_max_fps() {
        assert_eq!(
            config_from_upgrade(&upgrade("/?maxFps=100000")).max_fps,
            MAX_CONFIGURABLE_FPS
        );
        // Wider than u32: must clamp rather than wrap or fall back to the default.
        assert_eq!(
            config_from_upgrade(&upgrade("/?maxFps=4294967297")).max_fps,
            MAX_CONFIGURABLE_FPS
        );
    }

    #[test]
    fn test_parse_ack_seq() {
        assert_eq!(parse_ack_seq(&json!({"type": "ack", "seq": 42})), Some(42));
        assert_eq!(parse_ack_seq(&json!({"type": "ack"})), None);
        assert_eq!(parse_ack_seq(&json!({"type": "ack", "seq": -1})), None);
        assert_eq!(parse_ack_seq(&json!({"type": "ack", "seq": "42"})), None);
    }

    /// Guards the omit-never-null rule: a null string makes CDP reject the
    /// whole command, which silently drops the keystroke.
    /// A CDP endpoint that records every command and never replies, so a
    /// dispatcher that waits for a response hangs on the first event.
    async fn silent_cdp_server() -> (String, std::sync::Arc<Mutex<Vec<Value>>>) {
        use futures_util::StreamExt;
        let seen = std::sync::Arc::new(Mutex::new(Vec::<Value>::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/silent",
            listener.local_addr().unwrap().port()
        );
        let recorded = seen.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (_tx, mut rx) = ws.split();
            while let Some(Ok(Message::Text(text))) = rx.next().await {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    if v.get("method").and_then(Value::as_str).is_some() {
                        recorded.lock().await.push(v);
                    }
                }
            }
        });
        (url, seen)
    }

    /// Guards the no-await dispatch, and unlike the e2e it runs on every PR.
    ///
    /// Regression: awaiting Chrome's reply per event serialized the reader, so a
    /// click waited one round trip per queued move. Against a server that never
    /// replies, the awaiting version cannot get past the first event at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_input_dispatch_does_not_wait_for_a_cdp_reply() {
        let (url, seen) = silent_cdp_server().await;
        let client = CdpClient::connect(&url).await.expect("mock cdp connect");

        let events = [
            (
                "input_mouse",
                json!({ "eventType": "mouseMoved", "x": 1, "y": 1 }),
            ),
            (
                "input_mouse",
                json!({ "eventType": "mousePressed", "x": 1, "y": 1 }),
            ),
            (
                "input_keyboard",
                json!({ "eventType": "keyDown", "key": "a", "text": "a" }),
            ),
        ];
        let dispatched = tokio::time::timeout(Duration::from_secs(5), async {
            for (kind, payload) in &events {
                dispatch_input(kind, payload, &client, None).await;
            }
        })
        .await;
        assert!(
            dispatched.is_ok(),
            "dispatch blocked on a CDP reply that never comes"
        );

        // The commands still reach CDP, in the order they were dispatched.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if seen.lock().await.len() >= 3 || tokio::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            seen.lock()
                .await
                .iter()
                .map(|v| v["method"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "Input.dispatchMouseEvent",
                "Input.dispatchMouseEvent",
                "Input.dispatchKeyEvent"
            ],
            "input should reach CDP in dispatch order"
        );

        // Nothing was registered as awaiting a reply, so no orphan can accumulate.
        assert_eq!(client.pending_len().await, 0);
    }

    #[test]
    fn test_keyboard_params_omit_absent_strings_instead_of_sending_null() {
        // The dashboard's keyUp shape carries no text at all.
        let up = keyboard_params(&json!({
            "eventType": "keyUp", "key": "a", "code": "KeyA", "windowsVirtualKeyCode": 65
        }));
        assert_eq!(up["type"], "keyUp");
        assert_eq!(up["key"], "a");
        assert!(
            up.get("text").is_none(),
            "absent text must be omitted: {}",
            up
        );

        // The documented `char` shape carries only text.
        let ch = keyboard_params(&json!({ "eventType": "char", "text": "z" }));
        assert_eq!(ch["text"], "z");
        assert!(ch.get("key").is_none() && ch.get("code").is_none());

        // A non-string value is dropped rather than forwarded.
        let bad = keyboard_params(&json!({ "eventType": "keyDown", "key": 5, "code": null }));
        assert!(bad.get("key").is_none() && bad.get("code").is_none());

        // Numeric fields keep defaults so CDP always receives them.
        assert_eq!(bad["windowsVirtualKeyCode"], 0);
        assert_eq!(bad["modifiers"], 0);
        assert_eq!(keyboard_params(&json!({}))["type"], "keyDown");
    }

    /// A cap bounds the gap between deliveries and never postpones the first.
    #[test]
    fn test_deadline_charges_only_real_deliveries() {
        let now = Instant::now();
        assert!(deadline_from(None, 1) <= Instant::now());
        assert!(deadline_from(None, 0) <= Instant::now());
        assert_eq!(deadline_from(Some(now), 0), now);
        assert_eq!(
            deadline_from(Some(now), 2),
            now + Duration::from_millis(500)
        );
        assert_eq!(
            deadline_from(Some(now), 10),
            now + Duration::from_millis(100)
        );
    }
}
