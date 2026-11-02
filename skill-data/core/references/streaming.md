# Live Streaming

Stream a session's viewport over WebSocket and drive it with remote input. This is what a remote preview or embedded dashboard connects to: the browser runs wherever the daemon runs (a sandbox, a container, a CI box), and the client renders frames and sends clicks back.

**Related**: [commands.md](commands.md) for full command reference, [SKILL.md](../SKILL.md) for quick start.

## Contents

- [Enabling the stream](#enabling-the-stream)
- [Connecting](#connecting)
- [Messages from the server](#messages-from-the-server)
- [Messages from the client](#messages-from-the-client)
- [Frame rate and staleness](#frame-rate-and-staleness)
- [Limitations](#limitations)

## Enabling the stream

Streaming is always available; the server binds an OS-assigned localhost port unless told otherwise.

```bash
agent-browser stream status --json     # Report enabled state, port, client count
agent-browser stream enable            # Create the server (--port to pin one)
agent-browser stream disable           # Tear it down
```

`AGENT_BROWSER_STREAM_PORT` pins the port for the whole daemon instead of passing `--port`.

Frame encoding is daemon-wide, read once at startup:

| Variable | Default | Notes |
|---|---|---|
| `AGENT_BROWSER_STREAM_QUALITY` | `80` | 0 to 100, clamped |
| `AGENT_BROWSER_STREAM_MAX_WIDTH` | the viewport | caps the frame, does not resize the page |
| `AGENT_BROWSER_STREAM_MAX_HEIGHT` | the viewport | same |

The live stream requests jpeg, since a `frame` message carries no format field. An explicit `screencast_start` reconfigures the same underlying screencast, so a client can still see the format change mid-stream; sniff the bytes rather than assuming. Measured on a busy page at 1280x720: quality 80 gives ~54 KB per frame, quality 20 gives ~25 KB, and quality 20 at 640x360 gives ~9 KB. An unusable value leaves the default.

Read the port from `stream status --json` rather than assuming one; the OS-assigned default changes per daemon.

## Connecting

Connect a WebSocket client to `ws://127.0.0.1:<port>`. Frame delivery starts automatically once a client attaches, so there is no subscribe message. Browser clients must load from `localhost`, `127.0.0.1`, `::1` or `file://`. Any other origin gets a 403 on the upgrade and needs a proxy.

## Messages from the server

Every message is JSON text with a `type` field.

- `binding`: `{"type":"binding","bindingEpoch":"<server-uuid>"}` announces the current Chrome input binding; a null epoch disables input. Clear drawn input state on changes or reconnect.
- `frame`: a viewport image plus its metadata. Delivered latest-first (see below).

```json
{
  "type": "frame",
  "bindingEpoch": "<server-uuid>",
  "seq": 41,
  "data": "<base64-encoded-jpeg>",
  "metadata": {
    "deviceWidth": 1280, "deviceHeight": 720, "pageScaleFactor": 1,
    "offsetTop": 0, "scrollOffsetX": 0, "scrollOffsetY": 0,
    "timestamp": 1785038682238
  }
}
```

`seq` is a monotonic frame id, echoed back under ack pacing and stable across browser relaunches. `metadata.timestamp` is the capture time in epoch milliseconds, so `Date.now() - timestamp` is the age of the frame being drawn. The other message types:

- `status`: connection state, screencasting flag, viewport size, engine, recording flag. Sent once on connect and again on change.
- `tabs`: the current tab list, sent on connect when tabs are known and on change.
- `url`: on Chrome, full-document, History API, and fragment navigation in the active tab's main frame. Child-frame and background-tab navigation is ignored.
- `console`: console events.

Status, tabs, url, and console travel on an ordered channel: they are delivered in order and are never replaced by a newer message the way frames are. They are not unconditionally durable. A client that falls far enough behind can lag out of that channel and lose messages it never saw, so treat console output as a live feed, not an audit log.

Frame geometry comes from CDP; configured viewport dimensions are only fallbacks for missing metadata. Keep each image with its metadata until it is drawn, and cancel older asynchronous decodes. For a bitmap of width `W`, use a canvas height of `deviceHeight * W / deviceWidth` and draw at `offsetTop * W / deviceWidth`. Map display coordinates into device-independent pixels and subtract `offsetTop` once from Y. Do not multiply by DPR or divide by `pageScaleFactor` for CDP input.

## Messages from the client

Echo `bindingEpoch` from the frame you finished drawing in every mouse, keyboard, and touch message. The server validates the epoch and sends input under the same client/binding guards, so a queued event cannot move to a newly bound target. Missing or stale epochs and client-supplied session/target routing are rejected. Do not stamp the latest binding onto old pixels. The epoch identifies a client and target binding, not a document; same-target navigation does not rotate it, and old-target held-key cleanup is not guaranteed.

These messages belong to the WebSocket protocol. They have no dedicated CLI command or MCP tool; existing CLI and MCP parser delegation is unchanged.

```json
{"type": "input_mouse", "bindingEpoch": "<drawn-frame-epoch>", "eventType": "mousePressed", "x": 40, "y": 40, "button": "left", "clickCount": 1}
{"type": "input_keyboard", "bindingEpoch": "<drawn-frame-epoch>", "eventType": "keyDown", "key": "a", "text": "a"}
{"type": "input_touch", "bindingEpoch": "<drawn-frame-epoch>", "eventType": "touchStart", "touchPoints": []}
{"type": "input_text", "bindingEpoch": "<drawn-frame-epoch>", "text": "line 1\nline 2"}
{"type": "config", "maxFps": 10}
{"type": "config", "pacing": "ack"}
{"type": "ack", "seq": 41}
```

Input dispatches to the browser on a task of its own, separate from frame delivery, so a click is not queued behind a frame write. Events are sent to the browser without waiting for its reply, so a click stays responsive behind a burst of mouse moves. Ordering is preserved: press never overtakes move. Mouse, keyboard, and touch input also reset the daemon idle timer, so an actively driven preview is not shut down by the idle timeout.

`input_text` accepts exactly three fields: `type: "input_text"`, the drawn frame's `bindingEpoch`, and a nonempty `text` string of at most 65,536 UTF-8 bytes. Invalid, oversized, or extra-field payloads are rejected without activity tracking. Accepted text resets the idle timer and dispatches CDP `Input.insertText` without waiting for a reply. It inserts text, not HTML or images, and does not update the remote clipboard.

The dashboard listens for user-initiated document paste only while the live canvas has focus. Ctrl/Meta+V and Shift+Insert stay native and are suppressed remotely, including their matching keyup. No clipboard API read is performed. Unicode codepoints retain their text; physical, numpad, and modifier keys use distinct key codes. Blur releases tracked keys using their originating binding; rebinding drops old key state rather than sending releases to another target.

`config` sets a per-client frame cap: 1 to 120, or `0` for uncapped (the default). It takes effect immediately, including when it loosens the cap. Each client's cap is its own; other connected clients are unaffected. A value above 120 is clamped to 120; a negative or non-numeric value is ignored, leaving the current cap in place. Neither rejects the connection.

Both settings can also be declared on the URL, which is the only way to have them cover the connection's opening frame: `ws://127.0.0.1:<port>/?pacing=ack&maxFps=10`. A `config` message sent after connecting still wins.

## Frame rate and staleness

The server holds only the newest frame per client and reads it at send time. A frame produced while an earlier one is still being written is skipped, not queued, so the application never builds a backlog.

Push pacing (the default) stops there, and the transport underneath is still ordered: frames already accepted by the socket are delivered in order, so a client that stalls drains whatever the kernel buffered before the writer blocked.

Ack pacing closes that gap. Send `{"type":"config","pacing":"ack"}` and the server keeps at most one frame in flight, waiting for `{"type":"ack","seq":N}` before sending the next. Every frame carries a monotonic `seq`; echo the one you finished rendering. Frames produced while an ack is outstanding replace each other and never reach the socket, so a client that stalls for ten seconds and resumes gets the current page, not ten seconds of history.

Under ack pacing one frame is in flight at a time, so the rate is one frame per transfer plus one acknowledgement round trip. Both the link's bandwidth and its latency bound it, and a link whose bandwidth-delay product exceeds a single frame goes underused. Ack pacing bounds one hop. With a proxy in the path, forward the renderer's acks; acks generated on receipt leave frames queued on the far side. Acks are cumulative, so acknowledging a newer id covers any older one. A client that opts in and then stops acking simply stops receiving frames; status, tabs, url, and console keep flowing.

The two settings compose: `pacing` bounds how much is in flight, `maxFps` bounds the rate. A constrained preview usually wants both.

## Limitations

- Localhost only. Exposing the stream beyond the machine is the embedder's job (tunnel, proxy, or port forward), and the origin allowlist applies to browser clients.
- Frames are images, not a video codec. Bandwidth scales with viewport size and page activity; cap the rate for constrained links.
- In push pacing the server cannot tell a slow renderer from a fast one beyond transport backpressure. Use ack pacing when that distinction matters.
