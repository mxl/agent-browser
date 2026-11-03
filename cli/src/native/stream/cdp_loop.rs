use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::sync::{broadcast, watch, Mutex, RwLock};

use crate::native::cdp::client::CdpClient;
use crate::native::cdp::types::CdpEvent;
use crate::native::network;

use super::timestamp_ms;

/// Capture time of a screencast frame, in epoch milliseconds.
///
/// CDP sends `Network.TimeSinceEpoch`, a float in seconds; reading it as an
/// integer yields 0 for every frame. Milliseconds match this protocol's other
/// timestamps.
fn frame_timestamp_ms(meta: Option<&Value>) -> u64 {
    meta.and_then(|m| m.get("timestamp"))
        .and_then(|v| v.as_f64())
        .filter(|s| *s > 0.0)
        .map(|s| (s * 1000.0) as u64)
        .unwrap_or(0)
}

/// Preserve the actual screen DIP geometry, not the configured viewport or
/// JPEG pixel dimensions. Chrome already accounts for device scale (DPR).
fn frame_metadata(meta: Option<&Value>, fallback_width: u32, fallback_height: u32) -> Value {
    let number = |field: &str, fallback: f64| {
        meta.and_then(|m| m.get(field))
            .and_then(Value::as_f64)
            .unwrap_or(fallback)
    };
    json!({
        "offsetTop": number("offsetTop", 0.0),
        "pageScaleFactor": number("pageScaleFactor", 1.0),
        "deviceWidth": number("deviceWidth", fallback_width as f64),
        "deviceHeight": number("deviceHeight", fallback_height as f64),
        "scrollOffsetX": number("scrollOffsetX", 0.0),
        "scrollOffsetY": number("scrollOffsetY", 0.0),
        "timestamp": frame_timestamp_ms(meta),
    })
}

fn session_matches(active_session: Option<&str>, event_session: Option<&str>) -> bool {
    match active_session {
        Some("") => event_session.is_none_or(str::is_empty),
        Some(active) => event_session == Some(active),
        None => false,
    }
}

fn main_frame_id(frame_tree: &Value) -> Option<String> {
    frame_tree
        .get("frameTree")
        .and_then(|tree| tree.get("frame"))
        .and_then(|frame| frame.get("id"))
        .and_then(Value::as_str)
        .map(String::from)
}

async fn seed_main_frame_id(
    client: Arc<CdpClient>,
    session_id: Option<String>,
    delay: std::time::Duration,
    enabled: bool,
) -> Option<String> {
    if !enabled {
        std::future::pending::<()>().await;
    }
    tokio::time::sleep(delay).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.send_command_no_params("Page.getFrameTree", session_id.as_deref()),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .and_then(|tree| main_frame_id(&tree))
}

async fn publish_url(
    frame_tx: &broadcast::Sender<String>,
    last_tabs: &RwLock<Vec<Value>>,
    cdp_session_id: &RwLock<super::StreamBinding>,
    event_session_id: Option<&str>,
    url: &str,
) {
    let active_session = cdp_session_id.read().await;
    if !session_matches(active_session.session_id.as_deref(), event_session_id) {
        return;
    }
    {
        let mut tabs = last_tabs.write().await;
        for tab in tabs.iter_mut() {
            if tab.get("active").and_then(Value::as_bool).unwrap_or(false) {
                if let Some(tab) = tab.as_object_mut() {
                    tab.insert("url".to_string(), json!(url));
                }
            }
        }
    }
    let message = json!({
        "type": "url",
        "url": url,
        "timestamp": timestamp_ms(),
    });
    let _ = frame_tx.send(message.to_string());
}

// Keep the exact connection and page session that accepted the override.
// Capture epochs also rotate on resize, so they are not focus ownership IDs.
// A valid same-page viewport restart must not toggle focus or visibility.
struct FocusOwner {
    client: Arc<CdpClient>,
    session_id: String,
}

impl FocusOwner {
    fn matches(&self, client: &Arc<CdpClient>, session_id: Option<&str>) -> bool {
        Arc::ptr_eq(&self.client, client) && session_id == Some(self.session_id.as_str())
    }

    async fn set_enabled(&self, enabled: bool) -> bool {
        matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                self.client.send_command(
                    "Emulation.setFocusEmulationEnabled",
                    Some(json!({ "enabled": enabled })),
                    Some(&self.session_id),
                ),
            )
            .await,
            Ok(Ok(_))
        )
    }

    async fn release(self) {
        // CDP has no getter for another controller's prior override. Reset is
        // best effort, not restoration of foreign state or physical focus.
        if !self.set_enabled(false).await {
            eprintln!(
                "Warning: stream focus emulation reset failed; page focus may remain overridden."
            );
        }
    }
}

async fn release_focus(owner: &mut Option<FocusOwner>) {
    if let Some(old) = owner.take() {
        old.release().await;
    }
}

struct CarriedEvents {
    client: Arc<CdpClient>,
    receiver: broadcast::Receiver<CdpEvent>,
    queued: VecDeque<CdpEvent>,
}

/// Frames queued while stopping belong to the old screencast. Discard them
/// after the stop attempt while retaining navigation and runtime events that
/// must survive the restart.
fn preserve_queued_non_frame_events(
    receiver: &mut broadcast::Receiver<CdpEvent>,
    queued: &mut VecDeque<CdpEvent>,
) {
    loop {
        match receiver.try_recv() {
            Ok(event) if event.method != "Page.screencastFrame" => queued.push_back(event),
            Ok(_) => {}
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                break
            }
        }
    }
}

/// Subscribes to the active page's CDP events and broadcasts stream updates.
///
/// Frames use `frame_watch` so the latest value wins. Other messages stay on
/// the ordered `frame_tx` channel. URL updates follow only the active main
/// frame. Chrome also includes History API and fragment navigation.
#[allow(clippy::too_many_arguments)]
pub(super) async fn cdp_event_loop(
    frame_tx: broadcast::Sender<String>,
    frame_watch: watch::Sender<Option<Arc<super::StreamFrame>>>,
    screencast_config: Arc<super::ScreencastConfig>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<tokio::sync::Notify>,
    screencasting: Arc<Mutex<bool>>,
    client_count: Arc<Mutex<usize>>,
    cdp_session_id: Arc<RwLock<super::StreamBinding>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    last_tabs: Arc<RwLock<Vec<Value>>>,
    last_engine: Arc<RwLock<String>>,
    recording: Arc<Mutex<bool>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // CDP broadcasts are not replayed to new subscribers. Preserve the receiver
    // while restarting a screencast on the same client so navigation events
    // emitted during the restart remain queued for the rebound session.
    let mut carried_events: Option<CarriedEvents> = None;
    let mut focus_owner: Option<FocusOwner> = None;
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    let session_id = cdp_session_id.read().await.session_id.clone();
                    if *screencasting.lock().await {
                        let client = client_slot.read().await.clone();
                        if let Some(client) = client {
                            let _ = client
                                .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                .await;
                        }
                        let mut sc = screencasting.lock().await;
                        *sc = false;
                    }
                    release_focus(&mut focus_owner).await;
                    return;
                }
            }
            _ = client_notify.notified() => {}
        }

        let count = *client_count.lock().await;
        let guard = client_slot.read().await;

        if count > 0 {
            if let Some(ref client) = *guard {
                let (mut event_rx, mut queued_events) = match carried_events.take() {
                    Some(carried) if Arc::ptr_eq(&carried.client, client) => {
                        (carried.receiver, carried.queued)
                    }
                    _ => (client.subscribe(), VecDeque::new()),
                };
                let client_arc = Arc::clone(client);
                let binding = cdp_session_id.read().await;
                let session_id = binding.session_id.clone();
                let loop_epoch = binding.epoch_for(Some(client)).map(String::from);
                drop(binding);
                drop(guard);

                let vw = *viewport_width.lock().await;
                let vh = *viewport_height.lock().await;

                let eng = last_engine.read().await.clone();
                let is_chrome = eng == "chrome";
                let supports_screencast = is_chrome;
                let supports_same_document_navigation = is_chrome;

                if focus_owner.as_ref().is_some_and(|owner| {
                    !is_chrome
                        || loop_epoch.is_none()
                        || !owner.matches(&client_arc, session_id.as_deref())
                }) {
                    release_focus(&mut focus_owner).await;
                }
                if is_chrome && focus_owner.is_none() {
                    if let (Some(session_id), Some(epoch)) = (&session_id, &loop_epoch) {
                        let attempted = FocusOwner {
                            client: Arc::clone(&client_arc),
                            session_id: session_id.clone(),
                        };
                        // Hidden Chrome pages can stop producing frames. Emulating
                        // focus also makes the page visible, before capture starts,
                        // for the aggregate viewer lifetime on this connection.
                        if attempted.set_enabled(true).await {
                            focus_owner = Some(attempted);
                        } else {
                            eprintln!("Warning: stream focus emulation enable failed; continuing without confirmed focus.");
                            // A timeout can hide a late successful enable. Send one
                            // bounded reset to the attempted owner, never a new page.
                            attempted.release().await;
                        }
                        // Binding/viewer changes can arrive while the RPC waits.
                        // Do not start a stale cast or retain a stale override.
                        let current_client = client_slot.read().await.clone();
                        let binding = cdp_session_id.read().await;
                        let same_valid_page = current_client.as_ref().is_some_and(|client| {
                            Arc::ptr_eq(client, &client_arc)
                                && binding.session_id.as_ref() == Some(session_id)
                                && binding.epoch_for(Some(client)).is_some()
                        });
                        let capture_epoch_changed =
                            binding.epoch_for(Some(&client_arc)) != Some(epoch.as_str());
                        drop(binding);
                        let has_viewers = *client_count.lock().await > 0;
                        let shutting_down = *shutdown_rx.borrow();
                        if !same_valid_page || !has_viewers || shutting_down {
                            release_focus(&mut focus_owner).await;
                            if shutting_down {
                                return;
                            }
                            client_notify.notify_one();
                            continue;
                        }
                        if capture_epoch_changed {
                            // Resize during enable invalidates capture, not page
                            // focus. Restart with the new epoch without toggling.
                            carried_events = Some(CarriedEvents {
                                client: Arc::clone(&client_arc),
                                receiver: event_rx,
                                queued: queued_events,
                            });
                            client_notify.notify_one();
                            continue;
                        }
                    }
                }

                if supports_screencast {
                    let _ = client_arc
                        .send_command(
                            "Page.startScreencast",
                            Some(json!({
                                "format": "jpeg",
                                "quality": screencast_config.quality,
                                "maxWidth": screencast_config.max_width.unwrap_or(vw),
                                "maxHeight": screencast_config.max_height.unwrap_or(vh),
                                "everyNthFrame": 1,
                            })),
                            session_id.as_deref(),
                        )
                        .await;
                }

                {
                    let mut sc = screencasting.lock().await;
                    *sc = supports_screencast;
                }

                let rec = *recording.lock().await;
                let status = json!({
                    "type": "status",
                    "connected": true,
                    "screencasting": supports_screencast,
                    "viewportWidth": vw,
                    "viewportHeight": vh,
                    "engine": eng,
                    "recording": rec,
                });
                let _ = frame_tx.send(status.to_string());

                let frame_tree_seed = seed_main_frame_id(
                    Arc::clone(&client_arc),
                    session_id.clone(),
                    std::time::Duration::ZERO,
                    supports_same_document_navigation,
                )
                .fuse();
                tokio::pin!(frame_tree_seed);
                let mut seed_in_flight = supports_same_document_navigation;
                let mut active_main_frame_id = None;
                let mut pending_same_document = VecDeque::<(Option<String>, String, String)>::new();

                loop {
                    tokio::select! {
                        biased;

                        seeded_frame_id = &mut frame_tree_seed => {
                            seed_in_flight = false;
                            if active_main_frame_id.is_none() {
                                active_main_frame_id = seeded_frame_id;
                            }
                            if let Some(main_frame_id) = active_main_frame_id.as_deref() {
                                for (event_session_id, frame_id, url) in
                                    pending_same_document.drain(..)
                                {
                                    if frame_id == main_frame_id {
                                        publish_url(
                                            &frame_tx,
                                            &last_tabs,
                                            &cdp_session_id,
                                            event_session_id.as_deref(),
                                            &url,
                                        )
                                        .await;
                                    }
                                }
                            } else if !pending_same_document.is_empty() {
                                frame_tree_seed.set(
                                    seed_main_frame_id(
                                        Arc::clone(&client_arc),
                                        session_id.clone(),
                                        std::time::Duration::from_millis(250),
                                        true,
                                    )
                                    .fuse(),
                                );
                                seed_in_flight = true;
                            }
                        }
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                if supports_screencast {
                                    let _ = client_arc
                                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                        .await;
                                }
                                *screencasting.lock().await = false;
                                release_focus(&mut focus_owner).await;
                                return;
                            }
                        }
                        _ = client_notify.notified() => {
                            let count = *client_count.lock().await;
                            if count == 0 {
                                if supports_screencast {
                                    let _ = client_arc
                                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                        .await;
                                }
                                *screencasting.lock().await = false;
                                release_focus(&mut focus_owner).await;
                                break;
                            }
                            let client_changed = {
                                let guard = client_slot.read().await;
                                let same = guard
                                    .as_ref()
                                    .is_some_and(|c| Arc::ptr_eq(c, &client_arc));
                                !same
                            };
                            let binding = cdp_session_id.read().await;
                            let new_session_id = binding.session_id.clone();
                            let new_epoch = binding.epoch_for(Some(&client_arc));
                            let epoch_changed = new_epoch != loop_epoch.as_deref();
                            let binding_invalid = new_epoch.is_none();
                            drop(binding);
                            let session_changed = new_session_id != session_id;
                            let new_vw = *viewport_width.lock().await;
                            let new_vh = *viewport_height.lock().await;
                            let viewport_changed = new_vw != vw || new_vh != vh;
                            if client_changed || session_changed || epoch_changed || viewport_changed {
                                // The watch retains its latest value across CDP
                                // restarts. Invalidate it before stopping so a
                                // client cannot receive a frame from the old
                                // tab or viewport while the new cast starts.
                                frame_watch.send_replace(None);
                                if supports_screencast {
                                    let _ = client_arc
                                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                        .await;
                                }
                                *screencasting.lock().await = false;
                                if client_changed || session_changed || binding_invalid {
                                    release_focus(&mut focus_owner).await;
                                }
                                if !client_changed {
                                    preserve_queued_non_frame_events(
                                        &mut event_rx,
                                        &mut queued_events,
                                    );
                                    carried_events = Some(CarriedEvents {
                                        client: Arc::clone(&client_arc),
                                        receiver: event_rx,
                                        queued: queued_events,
                                    });
                                }
                                client_notify.notify_one();
                                break;
                            }
                        }
                        event = async {
                            match queued_events.pop_front() {
                                Some(event) => Ok(event),
                                None => event_rx.recv().await,
                            }
                        } => {
                            match event {
                                Ok(evt) => {
                                    if evt.method == "Page.frameNavigated" {
                                        if let Some(frame) = evt.params.get("frame") {
                                            let is_main = frame
                                                .get("parentId")
                                                .and_then(|v| v.as_str())
                                                .is_none_or(|s| s.is_empty());
                                            let is_active_session = session_matches(
                                                session_id.as_deref(),
                                                evt.session_id.as_deref(),
                                            );
                                            if is_main && is_active_session {
                                                if supports_screencast {
                                                    pending_same_document.clear();
                                                    active_main_frame_id = frame
                                                        .get("id")
                                                        .and_then(Value::as_str)
                                                        .map(String::from);
                                                }
                                                if let Some(url) = frame.get("url").and_then(|v| v.as_str()) {
                                                    publish_url(
                                                        &frame_tx,
                                                        &last_tabs,
                                                        &cdp_session_id,
                                                        evt.session_id.as_deref(),
                                                        url,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    } else if evt.method == "Page.navigatedWithinDocument" {
                                        let is_active_session = supports_same_document_navigation
                                            && session_matches(
                                                session_id.as_deref(),
                                                evt.session_id.as_deref(),
                                            );
                                        if is_active_session {
                                            if let (Some(frame_id), Some(url)) = (
                                                evt.params.get("frameId").and_then(Value::as_str),
                                                evt.params.get("url").and_then(Value::as_str),
                                            ) {
                                                if active_main_frame_id.is_none() {
                                                    if pending_same_document.len() == 64 {
                                                        pending_same_document.pop_front();
                                                    }
                                                    pending_same_document
                                                        .push_back((
                                                            evt.session_id.clone(),
                                                            frame_id.to_string(),
                                                            url.to_string(),
                                                        ));
                                                    if !seed_in_flight {
                                                        frame_tree_seed.set(
                                                            seed_main_frame_id(
                                                                Arc::clone(&client_arc),
                                                                session_id.clone(),
                                                                std::time::Duration::ZERO,
                                                                true,
                                                            )
                                                            .fuse(),
                                                        );
                                                        seed_in_flight = true;
                                                    }
                                                } else if Some(frame_id)
                                                    == active_main_frame_id.as_deref()
                                                {
                                                    publish_url(
                                                        &frame_tx,
                                                        &last_tabs,
                                                        &cdp_session_id,
                                                        evt.session_id.as_deref(),
                                                        url,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    } else if evt.method == "Page.screencastFrame" {
                                        let matches_loop_session = session_matches(
                                            session_id.as_deref(),
                                            evt.session_id.as_deref(),
                                        );
                                        let matches_bound_session = {
                                            let active_session = cdp_session_id.read().await;
                                            session_matches(
                                                active_session.session_id.as_deref(),
                                                evt.session_id.as_deref(),
                                            )
                                        };
                                        if !matches_loop_session || !matches_bound_session {
                                            continue;
                                        }
                                        if let Some(sid) = evt.params.get("sessionId").and_then(|v| v.as_i64()) {
                                            let _ = client_arc.send_command(
                                                "Page.screencastFrameAck",
                                                Some(json!({ "sessionId": sid })),
                                                evt.session_id.as_deref(),
                                            ).await;
                                        }

                                        if let Some(data) = evt.params.get("data").and_then(|v| v.as_str()) {
                                            let meta = evt.params.get("metadata");
                                            let seq = super::next_frame_seq();
                                            let mut msg = json!({
                                                "type": "frame",
                                                "seq": seq,
                                                "data": data,
                                                "metadata": frame_metadata(meta, vw, vh)
                                            });

                                            // The session or viewport can change while the
                                            // frame acknowledgement is in flight. Hold the
                                            // shared state guards through publication so the
                                            // corresponding setter cannot clear the cache and
                                            // then have this old frame repopulate it.
                                            let active_client = client_slot.read().await;
                                            let same_client = active_client
                                                .as_ref()
                                                .is_some_and(|client| Arc::ptr_eq(client, &client_arc));
                                            let active_session = cdp_session_id.read().await;
                                            let active_vw = viewport_width.lock().await;
                                            let active_vh = viewport_height.lock().await;
                                            let frame_is_current = same_client
                                                && loop_epoch.is_some()
                                                && active_session.epoch_for(Some(&client_arc)) == loop_epoch.as_deref()
                                                && session_matches(
                                                    active_session.session_id.as_deref(),
                                                    evt.session_id.as_deref(),
                                                )
                                                && *active_vw == vw
                                                && *active_vh == vh;
                                            if !frame_is_current {
                                                continue;
                                            }
                                            msg["bindingEpoch"] = json!(loop_epoch);
                                            frame_watch.send_replace(Some(Arc::new(
                                                super::StreamFrame {
                                                    binding_epoch: loop_epoch.clone(),
                                                    seq: Some(seq),
                                                    json: msg.to_string(),
                                                },
                                            )));
                                        }
                                    } else if evt.method == "Runtime.consoleAPICalled" {
                                        let level = evt.params.get("type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("log");
                                        let raw_args = evt.params.get("args")
                                            .and_then(|v| v.as_array())
                                            .cloned()
                                            .unwrap_or_default();
                                        let text = network::format_console_args(&raw_args);
                                        if !text.is_empty() {
                                            let mut msg = json!({
                                                "type": "console",
                                                "level": level,
                                                "text": text,
                                                "timestamp": timestamp_ms(),
                                            });
                                            if !raw_args.is_empty() {
                                                msg.as_object_mut().unwrap().insert(
                                                    "args".to_string(),
                                                    Value::Array(raw_args),
                                                );
                                            }
                                            let _ = frame_tx.send(msg.to_string());
                                        }
                                    } else if evt.method == "Runtime.exceptionThrown" {
                                        let text = evt.params.get("exceptionDetails")
                                            .and_then(|d| {
                                                d.get("exception")
                                                    .and_then(|e| e.get("description").and_then(|v| v.as_str()))
                                                    .or_else(|| d.get("text").and_then(|v| v.as_str()))
                                            })
                                            .unwrap_or("Unknown error");
                                        let line = evt.params.get("exceptionDetails")
                                            .and_then(|d| d.get("lineNumber").and_then(|v| v.as_i64()));
                                        let column = evt.params.get("exceptionDetails")
                                            .and_then(|d| d.get("columnNumber").and_then(|v| v.as_i64()));
                                        let msg = json!({
                                            "type": "page_error",
                                            "text": text,
                                            "line": line,
                                            "column": column,
                                            "timestamp": timestamp_ms(),
                                        });
                                        let _ = frame_tx.send(msg.to_string());
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => {
                                    release_focus(&mut focus_owner).await;
                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                carried_events = None;
                drop(guard);
                release_focus(&mut focus_owner).await;
            }
        } else {
            carried_events = None;
            let client = guard.clone();
            drop(guard);
            release_focus(&mut focus_owner).await;
            let was_screencasting = *screencasting.lock().await;
            if was_screencasting {
                if let Some(ref client) = client {
                    let session_id = cdp_session_id.read().await.session_id.clone();
                    let _ = client
                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                        .await;
                }
                let mut sc = screencasting.lock().await;
                *sc = false;
            }
        }
    }
}

pub async fn start_screencast(
    client: &CdpClient,
    session_id: &str,
    format: &str,
    quality: i32,
    max_width: i32,
    max_height: i32,
) -> Result<(), String> {
    client
        .send_command(
            "Page.startScreencast",
            Some(json!({
                "format": format,
                "quality": quality,
                "maxWidth": max_width,
                "maxHeight": max_height,
                "everyNthFrame": 1,
            })),
            Some(session_id),
        )
        .await?;
    Ok(())
}

pub async fn stop_screencast(client: &CdpClient, session_id: &str) -> Result<(), String> {
    client
        .send_command_no_params("Page.stopScreencast", Some(session_id))
        .await?;
    Ok(())
}

pub async fn ack_screencast_frame(
    client: &CdpClient,
    session_id: &str,
    screencast_session_id: i64,
) -> Result<(), String> {
    client
        .send_command(
            "Page.screencastFrameAck",
            Some(json!({ "sessionId": screencast_session_id })),
            Some(session_id),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    async fn mock_cdp_with_seed_delay(
        main_frame_id: &str,
        seed_delay: std::time::Duration,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        mock_cdp_with_rebind_events(main_frame_id, seed_delay, 0).await
    }

    async fn mock_cdp_with_rebind_events(
        main_frame_id: &str,
        seed_delay: std::time::Duration,
        events_on_next_stop: usize,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/mock",
            listener.local_addr().unwrap().port()
        );
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Value>();
        let methods = Arc::new(Mutex::new(Vec::new()));
        let recorded = methods.clone();
        let frame_id = main_frame_id.to_string();

        tokio::spawn(async move {
            let mut events_on_next_stop = events_on_next_stop;
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();

            loop {
                tokio::select! {
                    message = rx.next() => {
                        let Some(Ok(Message::Text(text))) = message else {
                            break;
                        };
                        let command: Value = serde_json::from_str(&text).unwrap();
                        let id = command["id"].as_u64().unwrap();
                        let method = command["method"].as_str().unwrap();
                        recorded.lock().await.push(method.to_string());
                        let result = if method == "Page.getFrameTree" {
                            let delay = tokio::time::sleep(seed_delay);
                            tokio::pin!(delay);
                            loop {
                                tokio::select! {
                                    _ = &mut delay => break,
                                    event = event_rx.recv() => {
                                        let Some(event) = event else {
                                            break;
                                        };
                                        tx.send(Message::Text(event.to_string())).await.unwrap();
                                    }
                                }
                            }
                            json!({ "frameTree": { "frame": { "id": frame_id } } })
                        } else if method == "Page.stopScreencast" && events_on_next_stop > 0 {
                            let event_count = std::mem::take(&mut events_on_next_stop);
                            for _ in 0..event_count {
                                let event = tokio::time::timeout(
                                    std::time::Duration::from_secs(5),
                                    event_rx.recv(),
                                )
                                .await
                                .expect("test should inject an event while screencast stops")
                                .expect("test event channel should stay open");
                                tx.send(Message::Text(event.to_string())).await.unwrap();
                            }
                            json!({})
                        } else {
                            json!({})
                        };
                        tx.send(Message::Text(json!({ "id": id, "result": result }).to_string()))
                            .await
                            .unwrap();
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        tx.send(Message::Text(event.to_string())).await.unwrap();
                    }
                }
            }
        });

        let client = Arc::new(CdpClient::connect(&url).await.unwrap());
        (client, event_tx, methods)
    }

    async fn mock_cdp(
        main_frame_id: &str,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        mock_cdp_with_seed_delay(main_frame_id, std::time::Duration::ZERO).await
    }

    async fn mock_cdp_with_first_seed_timeout(
        main_frame_id: &str,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/retry",
            listener.local_addr().unwrap().port()
        );
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Value>();
        let methods = Arc::new(Mutex::new(Vec::new()));
        let recorded = methods.clone();
        let frame_id = main_frame_id.to_string();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();
            let mut frame_tree_requests = 0;

            loop {
                tokio::select! {
                    message = rx.next() => {
                        let Some(Ok(Message::Text(text))) = message else {
                            break;
                        };
                        let command: Value = serde_json::from_str(&text).unwrap();
                        let id = command["id"].as_u64().unwrap();
                        let method = command["method"].as_str().unwrap();
                        recorded.lock().await.push(method.to_string());
                        if method == "Page.getFrameTree" {
                            frame_tree_requests += 1;
                            if frame_tree_requests == 1 {
                                continue;
                            }
                            tx.send(Message::Text(json!({
                                "id": id,
                                "result": { "frameTree": { "frame": { "id": frame_id } } }
                            }).to_string()))
                            .await
                            .unwrap();
                        } else {
                            tx.send(Message::Text(json!({ "id": id, "result": {} }).to_string()))
                                .await
                                .unwrap();
                        }
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        tx.send(Message::Text(event.to_string())).await.unwrap();
                    }
                }
            }
        });

        let client = Arc::new(CdpClient::connect(&url).await.unwrap());
        (client, event_tx, methods)
    }

    async fn silent_frame_tree_cdp() -> Arc<CdpClient> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/silent",
            listener.local_addr().unwrap().port()
        );
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();
            while let Some(Ok(message)) = rx.next().await {
                match message {
                    Message::Text(text) => {
                        let command: Value = serde_json::from_str(&text).unwrap();
                        let method = command["method"].as_str().unwrap();
                        if method != "Page.getFrameTree" {
                            let id = command["id"].as_u64().unwrap();
                            tx.send(Message::Text(json!({ "id": id, "result": {} }).to_string()))
                                .await
                                .unwrap();
                        }
                    }
                    Message::Ping(payload) => {
                        tx.send(Message::Pong(payload)).await.unwrap();
                    }
                    _ => {}
                }
            }
        });
        Arc::new(CdpClient::connect(&url).await.unwrap())
    }

    struct LoopHarness {
        client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
        client_count: Arc<Mutex<usize>>,
        events: mpsc::UnboundedSender<Value>,
        messages: broadcast::Receiver<String>,
        frames: watch::Receiver<Option<Arc<super::super::StreamFrame>>>,
        last_tabs: Arc<RwLock<Vec<Value>>>,
        cdp_session_id: Arc<RwLock<super::super::StreamBinding>>,
        viewport_width: Arc<Mutex<u32>>,
        client_notify: Arc<tokio::sync::Notify>,
        shutdown: watch::Sender<bool>,
        task: tokio::task::JoinHandle<()>,
        methods: Arc<Mutex<Vec<String>>>,
    }

    async fn start_loop_with_seed_delay(
        active_session: Option<&str>,
        main_frame_id: &str,
        seed_delay: std::time::Duration,
    ) -> LoopHarness {
        let (client, events, methods) = mock_cdp_with_seed_delay(main_frame_id, seed_delay).await;
        start_loop_with_client(active_session, client, events, methods).await
    }

    async fn start_loop_with_client(
        active_session: Option<&str>,
        client: Arc<CdpClient>,
        events: mpsc::UnboundedSender<Value>,
        methods: Arc<Mutex<Vec<String>>>,
    ) -> LoopHarness {
        start_loop_with_client_and_engine(active_session, client, events, methods, "chrome").await
    }

    async fn start_loop_with_client_and_engine(
        active_session: Option<&str>,
        client: Arc<CdpClient>,
        events: mpsc::UnboundedSender<Value>,
        methods: Arc<Mutex<Vec<String>>>,
        engine: &str,
    ) -> LoopHarness {
        let (frame_tx, messages) = broadcast::channel(64);
        let (frame_watch, frames) = watch::channel(None);
        let client_slot = Arc::new(RwLock::new(Some(client)));
        let client_notify = Arc::new(tokio::sync::Notify::new());
        let client_count = Arc::new(Mutex::new(1));
        let cdp_session_id = Arc::new(RwLock::new(super::super::StreamBinding::new(
            active_session.map(String::from),
            client_slot.read().await.as_ref(),
        )));
        let viewport_width = Arc::new(Mutex::new(1280));
        let last_tabs = Arc::new(RwLock::new(vec![
            json!({ "tabId": "t1", "url": "https://active.test/", "active": true }),
            json!({ "tabId": "t2", "url": "https://background.test/", "active": false }),
        ]));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(cdp_event_loop(
            frame_tx,
            frame_watch,
            Arc::new(super::super::ScreencastConfig::default()),
            client_slot.clone(),
            client_notify.clone(),
            Arc::new(Mutex::new(false)),
            client_count.clone(),
            cdp_session_id.clone(),
            viewport_width.clone(),
            Arc::new(Mutex::new(720)),
            last_tabs.clone(),
            Arc::new(RwLock::new(engine.to_string())),
            Arc::new(Mutex::new(false)),
            shutdown_rx,
        ));
        client_notify.notify_one();

        LoopHarness {
            client_slot,
            client_count,
            events,
            messages,
            frames,
            last_tabs,
            cdp_session_id,
            viewport_width,
            client_notify,
            shutdown,
            task,
            methods,
        }
    }

    async fn start_loop(active_session: Option<&str>, main_frame_id: &str) -> LoopHarness {
        start_loop_with_seed_delay(active_session, main_frame_id, std::time::Duration::ZERO).await
    }

    async fn next_message_of_type(messages: &mut broadcast::Receiver<String>, kind: &str) -> Value {
        loop {
            let text = tokio::time::timeout(std::time::Duration::from_secs(5), messages.recv())
                .await
                .expect("timed out waiting for stream message")
                .expect("stream channel closed");
            let message: Value = serde_json::from_str(&text).unwrap();
            if message["type"] == kind {
                return message;
            }
        }
    }

    async fn expect_no_url(messages: &mut broadcast::Receiver<String>) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = tokio::time::timeout(remaining, messages.recv()).await;
            let Ok(Ok(text)) = result else {
                return;
            };
            let message: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(message["type"], "url", "unexpected URL message: {message}");
        }
    }

    async fn stop_loop(harness: LoopHarness) {
        let _ = harness.shutdown.send(true);
        harness.task.await.unwrap();
    }

    async fn wait_for_method(methods: &Mutex<Vec<String>>, expected: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if methods.lock().await.iter().any(|method| method == expected) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for CDP method");
    }

    async fn publish_frame_and_wait(
        harness: &mut LoopHarness,
        session_id: &str,
        screencast_session_id: i64,
        data: &str,
    ) {
        harness
            .events
            .send(json!({
                "method": "Page.screencastFrame",
                "sessionId": session_id,
                "params": {
                    "sessionId": screencast_session_id,
                    "data": data,
                    "metadata": { "timestamp": 1.0 }
                }
            }))
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), harness.frames.changed())
            .await
            .expect("timed out waiting for test frame")
            .expect("frame watch should stay open");
        let frame = harness
            .frames
            .borrow()
            .clone()
            .expect("test frame should be cached");
        let payload: Value = serde_json::from_str(&frame.json).unwrap();
        assert_eq!(payload["data"], data);
        let binding = harness.cdp_session_id.read().await;
        assert_eq!(
            payload["bindingEpoch"].as_str(),
            Some(binding.epoch.as_str())
        );
        assert_eq!(frame.binding_epoch.as_deref(), Some(binding.epoch.as_str()));
    }

    // The mock page starts hidden and emits frames only while focus emulation
    // is enabled. Record complete commands to verify exact connection/session
    // ownership rather than inferring it from stream status.
    async fn focus_mock(
        reject_enable: bool,
        reject_reset: bool,
        enable_delay: std::time::Duration,
    ) -> (Arc<CdpClient>, Arc<Mutex<Vec<Value>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/focus", listener.local_addr().unwrap());
        let commands = Arc::new(Mutex::new(Vec::new()));
        let recorded = commands.clone();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut focused = false;
            while let Some(Ok(Message::Text(text))) = ws.next().await {
                let command: Value = serde_json::from_str(&text).unwrap();
                recorded.lock().await.push(command.clone());
                let method = command["method"].as_str().unwrap();
                let mut rejected = false;
                if method == "Emulation.setFocusEmulationEnabled" {
                    let enabled = command["params"]["enabled"].as_bool().unwrap();
                    if enabled {
                        tokio::time::sleep(enable_delay).await;
                    }
                    rejected = if enabled { reject_enable } else { reject_reset };
                    if !rejected {
                        focused = enabled;
                    }
                }
                let response = if rejected {
                    json!({"id": command["id"], "error": {"code": -32601, "message": "unsupported"}})
                } else {
                    json!({"id": command["id"], "result": {}})
                };
                if ws.send(Message::Text(response.to_string())).await.is_err() {
                    break;
                }
                if method == "Page.startScreencast" && focused {
                    let mut event = json!({"method": "Page.screencastFrame", "params": {
                        "sessionId": 1, "data": "fresh-visible-frame", "metadata": {"timestamp": 1.0}
                    }});
                    if let Some(session) = command.get("sessionId") {
                        event["sessionId"] = session.clone();
                    }
                    if ws.send(Message::Text(event.to_string())).await.is_err() {
                        break;
                    }
                }
            }
        });
        (Arc::new(CdpClient::connect(&url).await.unwrap()), commands)
    }

    async fn focus_harness(
        session: Option<&str>,
        engine: &str,
        reject_enable: bool,
        reject_reset: bool,
        delay: std::time::Duration,
    ) -> (LoopHarness, Arc<Mutex<Vec<Value>>>) {
        let (client, commands) = focus_mock(reject_enable, reject_reset, delay).await;
        let (events, _rx) = mpsc::unbounded_channel();
        let harness = start_loop_with_client_and_engine(
            session,
            client,
            events,
            Arc::new(Mutex::new(Vec::new())),
            engine,
        )
        .await;
        (harness, commands)
    }

    async fn wait_commands(commands: &Mutex<Vec<Value>>, method: &str, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if commands
                    .lock()
                    .await
                    .iter()
                    .filter(|c| c["method"] == method)
                    .count()
                    >= count
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("missing mock command");
    }

    fn focus_commands(commands: &[Value]) -> Vec<(Value, Value)> {
        commands
            .iter()
            .filter(|c| c["method"] == "Emulation.setFocusEmulationEnabled")
            .map(|c| (c["sessionId"].clone(), c["params"]["enabled"].clone()))
            .collect()
    }

    async fn resize_with_capture_epoch(harness: &LoopHarness, width: u32) {
        // Match set_viewport's relevant production transition and lock order:
        // dimensions change and the capture/input epoch rotates on the same page.
        let _client = harness.client_slot.read().await;
        let mut binding = harness.cdp_session_id.write().await;
        let mut vw = harness.viewport_width.lock().await;
        *vw = width;
        binding.epoch = uuid::Uuid::new_v4().to_string();
        drop(vw);
        harness.client_notify.notify_one();
    }

    #[tokio::test]
    async fn test_focus_hidden_page_frames_and_aggregate_viewer_lifetime() {
        let (mut h, commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            false,
            false,
            std::time::Duration::ZERO,
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), h.frames.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(h
            .frames
            .borrow()
            .as_ref()
            .unwrap()
            .json
            .contains("fresh-visible-frame"));
        {
            let log = commands.lock().await;
            assert_eq!(log[0]["method"], "Emulation.setFocusEmulationEnabled");
            assert_eq!(log[0]["sessionId"], "PAGE");
            assert_eq!(log[1]["method"], "Page.startScreencast");
            assert_eq!(log[1]["sessionId"], "PAGE");
        }
        *h.client_count.lock().await = 2;
        h.client_notify.notify_one();
        resize_with_capture_epoch(&h, 900).await;
        wait_commands(&commands, "Page.startScreencast", 2).await;
        *h.client_count.lock().await = 1;
        h.client_notify.notify_one();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert_eq!(focus_commands(&commands.lock().await).len(), 1);
        *h.client_count.lock().await = 0;
        h.client_notify.notify_one();
        wait_commands(&commands, "Emulation.setFocusEmulationEnabled", 2).await;
        stop_loop(h).await;
        assert_eq!(
            focus_commands(&commands.lock().await),
            vec![(json!("PAGE"), json!(true)), (json!("PAGE"), json!(false))]
        );
    }

    #[tokio::test]
    async fn test_focus_rebind_releases_exact_old_connection_and_session() {
        let (h, old_commands) = focus_harness(
            Some("OLD"),
            "chrome",
            false,
            false,
            std::time::Duration::ZERO,
        )
        .await;
        wait_commands(&old_commands, "Page.startScreencast", 1).await;
        let (new_client, new_commands) = focus_mock(false, false, std::time::Duration::ZERO).await;
        *h.client_slot.write().await = Some(new_client.clone());
        *h.cdp_session_id.write().await =
            super::super::StreamBinding::new(Some("NEW".into()), Some(&new_client));
        h.client_notify.notify_one();
        wait_commands(&new_commands, "Page.startScreencast", 1).await;
        assert_eq!(
            focus_commands(&old_commands.lock().await),
            vec![(json!("OLD"), json!(true)), (json!("OLD"), json!(false))]
        );
        stop_loop(h).await;
        assert_eq!(
            focus_commands(&new_commands.lock().await),
            vec![(json!("NEW"), json!(true)), (json!("NEW"), json!(false))]
        );
    }

    #[tokio::test]
    async fn test_focus_same_connection_epoch_rebind_and_root_session() {
        let (h, commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            false,
            false,
            std::time::Duration::ZERO,
        )
        .await;
        wait_commands(&commands, "Page.startScreencast", 1).await;
        let client = h.client_slot.read().await.clone().unwrap();
        *h.cdp_session_id.write().await =
            super::super::StreamBinding::new(Some("PAGE".into()), Some(&client));
        h.client_notify.notify_one();
        wait_commands(&commands, "Page.startScreencast", 2).await;
        assert_eq!(
            focus_commands(&commands.lock().await),
            vec![(json!("PAGE"), json!(true))]
        );
        *h.cdp_session_id.write().await =
            super::super::StreamBinding::new(Some(String::new()), Some(&client));
        h.client_notify.notify_one();
        wait_commands(&commands, "Page.startScreencast", 3).await;
        stop_loop(h).await;
        assert_eq!(
            focus_commands(&commands.lock().await),
            vec![
                (json!("PAGE"), json!(true)),
                (json!("PAGE"), json!(false)),
                (Value::Null, json!(true)),
                (Value::Null, json!(false)),
            ]
        );
    }

    #[tokio::test]
    async fn test_focus_skips_missing_or_invalid_binding_and_non_chrome() {
        for (session, engine) in [(None, "chrome"), (Some("PAGE"), "lightpanda")] {
            let (mut h, commands) =
                focus_harness(session, engine, false, false, std::time::Duration::ZERO).await;
            next_message_of_type(&mut h.messages, "status").await;
            stop_loop(h).await;
            assert!(focus_commands(&commands.lock().await).is_empty());
        }
    }

    #[tokio::test]
    async fn test_focus_no_viewers_then_first_viewer_unfreezes_hidden_page() {
        let (mut h, commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            false,
            false,
            std::time::Duration::ZERO,
        )
        .await;
        // This current-thread test changes the count before the spawned loop runs.
        *h.client_count.lock().await = 0;
        h.client_notify.notify_one();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(commands.lock().await.is_empty());
        assert!(h.frames.borrow().is_none());
        *h.client_count.lock().await = 1;
        h.client_notify.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), h.frames.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(h.frames.borrow().is_some());
        stop_loop(h).await;
        assert_eq!(focus_commands(&commands.lock().await).len(), 2);
    }

    #[tokio::test]
    async fn test_focus_invalid_client_binding_resets_old_and_never_enables_replacement() {
        let (mut h, old_commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            false,
            false,
            std::time::Duration::ZERO,
        )
        .await;
        let (replacement, commands) = focus_mock(false, false, std::time::Duration::ZERO).await;
        // Wait for the initial cast, then replace only the client. The binding
        // still names the old connection and cannot authorize focus on the new one.
        next_message_of_type(&mut h.messages, "status").await;
        *h.client_slot.write().await = Some(replacement);
        h.client_notify.notify_one();
        wait_commands(&commands, "Page.startScreencast", 1).await;
        stop_loop(h).await;
        assert!(focus_commands(&commands.lock().await).is_empty());
        assert_eq!(
            focus_commands(&old_commands.lock().await),
            vec![(json!("PAGE"), json!(true)), (json!("PAGE"), json!(false))]
        );
    }

    #[tokio::test]
    async fn test_focus_owned_reset_failure_is_bounded_and_does_not_block_shutdown() {
        let (mut h, commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            false,
            true,
            std::time::Duration::ZERO,
        )
        .await;
        next_message_of_type(&mut h.messages, "status").await;
        tokio::time::timeout(std::time::Duration::from_secs(2), stop_loop(h))
            .await
            .unwrap();
        assert_eq!(
            focus_commands(&commands.lock().await),
            vec![(json!("PAGE"), json!(true)), (json!("PAGE"), json!(false))]
        );
    }

    #[tokio::test]
    async fn test_focus_rejected_enable_and_reset_do_not_block_capture_or_claim_owner() {
        let (mut h, commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            true,
            true,
            std::time::Duration::ZERO,
        )
        .await;
        next_message_of_type(&mut h.messages, "status").await;
        assert!(h.frames.borrow().is_none());
        wait_commands(&commands, "Page.startScreencast", 1).await;
        stop_loop(h).await;
        // One enable attempt and one uncertainty cleanup, no owned reset on shutdown.
        assert_eq!(
            focus_commands(&commands.lock().await),
            vec![(json!("PAGE"), json!(true)), (json!("PAGE"), json!(false))]
        );
    }

    #[tokio::test]
    async fn test_focus_delayed_enable_retains_owner_across_realistic_resize_epoch() {
        let (mut h, commands) = focus_harness(
            Some("PAGE"),
            "chrome",
            false,
            false,
            std::time::Duration::from_millis(100),
        )
        .await;
        wait_commands(&commands, "Emulation.setFocusEmulationEnabled", 1).await;
        let old_epoch = h.cdp_session_id.read().await.epoch.clone();
        resize_with_capture_epoch(&h, 900).await;
        let new_epoch = h.cdp_session_id.read().await.epoch.clone();
        assert_ne!(old_epoch, new_epoch);
        next_message_of_type(&mut h.messages, "status").await;
        tokio::time::timeout(std::time::Duration::from_secs(5), h.frames.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            h.frames.borrow().as_ref().unwrap().binding_epoch.as_deref(),
            Some(new_epoch.as_str())
        );
        {
            let log = commands.lock().await;
            let casts: Vec<_> = log
                .iter()
                .filter(|c| c["method"] == "Page.startScreencast")
                .collect();
            assert_eq!(
                casts.len(),
                1,
                "must not start capture with the pre-resize epoch"
            );
            assert_eq!(casts[0]["params"]["maxWidth"], 900);
            assert_eq!(focus_commands(&log), vec![(json!("PAGE"), json!(true))]);
        }
        *h.client_count.lock().await = 0;
        h.client_notify.notify_one();
        wait_commands(&commands, "Emulation.setFocusEmulationEnabled", 2).await;
        stop_loop(h).await;
        assert_eq!(
            focus_commands(&commands.lock().await),
            vec![(json!("PAGE"), json!(true)), (json!("PAGE"), json!(false))]
        );
    }

    #[tokio::test]
    async fn test_focus_successful_delayed_enable_is_reset_on_pending_shutdown() {
        let (h, commands) = focus_harness(
            Some("OLD"),
            "chrome",
            false,
            false,
            std::time::Duration::from_millis(100),
        )
        .await;
        wait_commands(&commands, "Emulation.setFocusEmulationEnabled", 1).await;
        h.shutdown.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), h.task)
            .await
            .unwrap()
            .unwrap();
        let log = commands.lock().await;
        assert_eq!(
            focus_commands(&log),
            vec![(json!("OLD"), json!(true)), (json!("OLD"), json!(false))]
        );
        assert!(!log.iter().any(|c| c["method"] == "Page.startScreencast"));
    }

    #[tokio::test]
    async fn test_focus_delayed_enable_rebind_cleans_old_owner_before_new_capture() {
        let (h, commands) = focus_harness(
            Some("OLD"),
            "chrome",
            false,
            false,
            std::time::Duration::from_millis(1200),
        )
        .await;
        wait_commands(&commands, "Emulation.setFocusEmulationEnabled", 1).await;
        let client = h.client_slot.read().await.clone().unwrap();
        *h.cdp_session_id.write().await =
            super::super::StreamBinding::new(Some("NEW".into()), Some(&client));
        h.client_notify.notify_one();
        wait_commands(&commands, "Page.startScreencast", 1).await;
        stop_loop(h).await;
        let log = commands.lock().await;
        assert!(log
            .iter()
            .filter(|c| c["method"] == "Page.startScreencast")
            .all(|c| c["sessionId"] == "NEW"));
        assert_eq!(
            focus_commands(&log),
            vec![
                (json!("OLD"), json!(true)),
                (json!("OLD"), json!(false)),
                (json!("NEW"), json!(true)),
                (json!("NEW"), json!(false))
            ]
        );
    }

    #[test]
    fn test_frame_metadata_preserves_actual_dip_geometry() {
        let meta = json!({
            "deviceWidth": 973.5, "deviceHeight": 641.25,
            "offsetTop": 32.5, "pageScaleFactor": 2.0,
            "scrollOffsetX": 10.5, "scrollOffsetY": 420.25,
            "timestamp": 123.456,
        });
        let forwarded = frame_metadata(Some(&meta), 1280, 720);
        for field in [
            "deviceWidth",
            "deviceHeight",
            "offsetTop",
            "pageScaleFactor",
            "scrollOffsetX",
            "scrollOffsetY",
        ] {
            assert_eq!(forwarded[field], meta[field], "must preserve {field}");
        }
        assert_eq!(forwarded["timestamp"], 123456);
    }

    #[test]
    fn test_frame_metadata_falls_back_only_for_missing_values() {
        let forwarded = frame_metadata(Some(&json!({"deviceWidth": 800.0})), 1280, 720);
        assert_eq!(forwarded["deviceWidth"].as_f64(), Some(800.0));
        assert_eq!(forwarded["deviceHeight"].as_f64(), Some(720.0));
        assert_eq!(forwarded["offsetTop"].as_f64(), Some(0.0));
        assert_eq!(forwarded["pageScaleFactor"].as_f64(), Some(1.0));
    }

    /// Reading the float as an integer stamps every frame 0, so no client can
    /// measure frame age.
    #[test]
    fn test_frame_timestamp_converts_cdp_seconds_to_millis() {
        let meta = json!({ "timestamp": 1785038682.238_f64 });
        assert_eq!(frame_timestamp_ms(Some(&meta)), 1785038682238);
    }

    #[test]
    fn test_frame_timestamp_absent_or_zero_stays_zero() {
        assert_eq!(frame_timestamp_ms(None), 0);
        assert_eq!(frame_timestamp_ms(Some(&json!({}))), 0);
        assert_eq!(frame_timestamp_ms(Some(&json!({ "timestamp": 0 }))), 0);
        assert_eq!(frame_timestamp_ms(Some(&json!({ "timestamp": "nope" }))), 0);
    }

    #[tokio::test]
    async fn test_same_document_navigation_tracks_active_main_frame_and_ignores_child() {
        let mut harness = start_loop(Some("S-ACTIVE"), "F-MAIN").await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/spa",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        let history = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(history["url"], "https://active.test/spa");

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/spa#section",
                    "navigationType": "fragment"
                }
            }))
            .unwrap();
        let fragment = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(fragment["url"], "https://active.test/spa#section");

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-CHILD",
                    "url": "https://active.test/child",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-BACKGROUND",
                "params": {
                    "frameId": "F-BACKGROUND",
                    "url": "https://background.test/spa",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[0]["url"],
            "https://active.test/spa#section"
        );
        assert!(harness
            .methods
            .lock()
            .await
            .iter()
            .any(|method| method == "Page.getFrameTree"));

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_lightpanda_background_full_navigation_does_not_replace_active_url() {
        let (client, events, methods) = mock_cdp("F-MAIN").await;
        let mut harness = start_loop_with_client_and_engine(
            Some("S-ACTIVE"),
            client,
            events,
            methods,
            "lightpanda",
        )
        .await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frame": {
                        "id": "F-MAIN",
                        "url": "https://active.test/full"
                    }
                }
            }))
            .unwrap();
        let active = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(active["url"], "https://active.test/full");

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-BACKGROUND",
                "params": {
                    "frame": {
                        "id": "F-BACKGROUND",
                        "url": "https://background.test/changed"
                    }
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[0]["url"],
            "https://active.test/full"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_old_session_event_cannot_replace_new_active_tab_url() {
        let mut harness = start_loop(Some("S-OLD"), "F-OLD").await;
        next_message_of_type(&mut harness.messages, "status").await;

        {
            let mut session = harness.cdp_session_id.write().await;
            let mut tabs = harness.last_tabs.write().await;
            session.session_id = Some("S-NEW".to_string());
            session.epoch = uuid::Uuid::new_v4().to_string();
            *tabs = vec![
                json!({ "tabId": "t1", "url": "https://old.test/", "active": false }),
                json!({ "tabId": "t2", "url": "https://new.test/", "active": true }),
            ];
        }

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-OLD",
                "params": {
                    "frame": {
                        "id": "F-OLD",
                        "url": "https://old.test/late"
                    }
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[1]["url"],
            "https://new.test/"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_old_session_frame_is_ignored_before_rebind_notification() {
        let mut harness = start_loop(Some("S-OLD"), "F-OLD").await;
        next_message_of_type(&mut harness.messages, "status").await;

        {
            let mut binding = harness.cdp_session_id.write().await;
            binding.session_id = Some("S-NEW".to_string());
            binding.epoch = uuid::Uuid::new_v4().to_string();
        }
        harness
            .events
            .send(json!({
                "method": "Page.screencastFrame",
                "sessionId": "S-OLD",
                "params": {
                    "sessionId": 6,
                    "data": "stale-frame",
                    "metadata": { "timestamp": 1.0 }
                }
            }))
            .unwrap();
        harness
            .events
            .send(json!({
                "method": "Runtime.consoleAPICalled",
                "sessionId": "S-OLD",
                "params": {
                    "type": "log",
                    "args": [{ "type": "string", "value": "frame barrier" }]
                }
            }))
            .unwrap();

        next_message_of_type(&mut harness.messages, "console").await;
        assert!(
            harness.frames.borrow().is_none(),
            "the previous session must stop publishing frames as soon as the binding changes"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_navigation_survives_session_rebind() {
        let (client, events, methods) =
            mock_cdp_with_rebind_events("F-NEW", std::time::Duration::ZERO, 2).await;
        let mut harness = start_loop_with_client(Some("S-OLD"), client, events, methods).await;
        next_message_of_type(&mut harness.messages, "status").await;
        publish_frame_and_wait(&mut harness, "S-OLD", 6, "previous-tab-frame").await;

        {
            let mut binding = harness.cdp_session_id.write().await;
            binding.session_id = Some("S-NEW".to_string());
            binding.epoch = uuid::Uuid::new_v4().to_string();
        }
        harness.client_notify.notify_one();
        wait_for_method(&harness.methods, "Page.stopScreencast").await;
        harness
            .events
            .send(json!({
                "method": "Page.screencastFrame",
                "sessionId": "S-OLD",
                "params": {
                    "sessionId": 7,
                    "data": "stale-frame",
                    "metadata": { "timestamp": 1.0 }
                }
            }))
            .unwrap();
        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-NEW",
                "params": {
                    "frameId": "F-NEW",
                    "url": "https://new.test/immediate",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();

        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://new.test/immediate");
        assert!(
            harness.frames.borrow().is_none(),
            "a frame from the old session must not reach the rebound stream"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_viewport_rebind_preserves_navigation_without_replaying_stale_frame() {
        let (client, events, methods) =
            mock_cdp_with_rebind_events("F-MAIN", std::time::Duration::ZERO, 2).await;
        let mut harness = start_loop_with_client(Some("S-ACTIVE"), client, events, methods).await;
        next_message_of_type(&mut harness.messages, "status").await;
        publish_frame_and_wait(&mut harness, "S-ACTIVE", 7, "previous-size-frame").await;

        *harness.viewport_width.lock().await = 800;
        harness.client_notify.notify_one();
        wait_for_method(&harness.methods, "Page.stopScreencast").await;
        harness
            .events
            .send(json!({
                "method": "Page.screencastFrame",
                "sessionId": "S-ACTIVE",
                "params": {
                    "sessionId": 8,
                    "data": "old-size-frame",
                    "metadata": { "timestamp": 2.0 }
                }
            }))
            .unwrap();
        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/resized",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();

        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://active.test/resized");
        assert!(
            harness.frames.borrow().is_none(),
            "a frame captured at the old viewport must not be relabeled after restart"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_navigation_retries_after_seed_timeout() {
        let (client, events, methods) = mock_cdp_with_first_seed_timeout("F-MAIN").await;
        let mut harness = start_loop_with_client(Some("S-ACTIVE"), client, events, methods).await;
        next_message_of_type(&mut harness.messages, "status").await;
        wait_for_method(&harness.methods, "Page.getFrameTree").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/recovered",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();

        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://active.test/recovered");
        assert_eq!(
            harness
                .methods
                .lock()
                .await
                .iter()
                .filter(|method| method.as_str() == "Page.getFrameTree")
                .count(),
            2
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_navigation_waits_for_main_frame_seed() {
        let mut harness = start_loop_with_seed_delay(
            Some("S-ACTIVE"),
            "F-MAIN",
            std::time::Duration::from_millis(100),
        )
        .await;
        next_message_of_type(&mut harness.messages, "status").await;
        wait_for_method(&harness.methods, "Page.getFrameTree").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/immediate",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://active.test/immediate");

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_seed_buffer_discards_oldest_event_at_capacity() {
        let mut harness = start_loop_with_seed_delay(
            Some("S-ACTIVE"),
            "F-MAIN",
            std::time::Duration::from_millis(100),
        )
        .await;
        next_message_of_type(&mut harness.messages, "status").await;
        wait_for_method(&harness.methods, "Page.getFrameTree").await;

        for index in 0..65 {
            harness
                .events
                .send(json!({
                    "method": "Page.navigatedWithinDocument",
                    "sessionId": "S-ACTIVE",
                    "params": {
                        "frameId": "F-MAIN",
                        "url": format!("https://active.test/{index}"),
                        "navigationType": "historyApi"
                    }
                }))
                .unwrap();
        }

        let first = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(first["url"], "https://active.test/1");

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_background_full_navigation_does_not_replace_active_url() {
        let mut harness = start_loop(Some("S-ACTIVE"), "F-MAIN").await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frame": {
                        "id": "F-MAIN",
                        "url": "https://active.test/full"
                    }
                }
            }))
            .unwrap();
        let active = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(active["url"], "https://active.test/full");

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-BACKGROUND",
                "params": {
                    "frame": {
                        "id": "F-BACKGROUND",
                        "url": "https://background.test/changed"
                    }
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[0]["url"],
            "https://active.test/full"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_direct_page_navigation_matches_empty_active_session() {
        let mut harness = start_loop(Some(""), "F-DIRECT").await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "params": {
                    "frameId": "F-DIRECT",
                    "url": "https://provider.test/spa",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://provider.test/spa");

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_unanswered_frame_tree_seed_does_not_block_shutdown() {
        let client = silent_frame_tree_cdp().await;
        let (frame_tx, mut messages) = broadcast::channel(64);
        let (frame_watch, _) = watch::channel(None);
        let client_notify = Arc::new(tokio::sync::Notify::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(cdp_event_loop(
            frame_tx,
            frame_watch,
            Arc::new(super::super::ScreencastConfig::default()),
            Arc::new(RwLock::new(Some(client.clone()))),
            client_notify.clone(),
            Arc::new(Mutex::new(false)),
            Arc::new(Mutex::new(1)),
            Arc::new(RwLock::new(super::super::StreamBinding::new(
                Some("S-ACTIVE".to_string()),
                Some(&client),
            ))),
            Arc::new(Mutex::new(1280)),
            Arc::new(Mutex::new(720)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new("chrome".to_string())),
            Arc::new(Mutex::new(false)),
            shutdown_rx,
        ));
        client_notify.notify_one();

        next_message_of_type(&mut messages, "status").await;
        let started = tokio::time::Instant::now();
        let _ = shutdown.send(true);
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("frame tree seed should not hold shutdown for the CDP timeout")
            .unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "shutdown waited for the default CDP command timeout"
        );
        assert_eq!(client.pending_len().await, 0);
    }
}
