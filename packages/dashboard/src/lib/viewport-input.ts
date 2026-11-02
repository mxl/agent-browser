// Keep this UTF-8 byte limit aligned with stream/websocket.rs.
export const MAX_INPUT_TEXT_BYTES = 64 * 1024;

export interface FrameGeometry {
  deviceWidth: number;
  deviceHeight: number;
  offsetTop: number;
  pageScaleFactor: number;
}

export function validFrameGeometry(meta: FrameGeometry | null | undefined): meta is FrameGeometry {
  return !!meta && Number.isFinite(meta.deviceWidth) && meta.deviceWidth > 0 &&
    Number.isFinite(meta.deviceHeight) && meta.deviceHeight > 0 &&
    Number.isFinite(meta.offsetTop) && meta.offsetTop >= 0 &&
    meta.offsetTop < meta.deviceHeight &&
    Number.isFinite(meta.pageScaleFactor) && meta.pageScaleFactor > 0;
}

export function frameCanvasLayout(bitmapWidth: number, meta: FrameGeometry) {
  const bitmapScale = bitmapWidth / meta.deviceWidth;
  return {
    width: bitmapWidth,
    height: Math.round(meta.deviceHeight * bitmapScale),
    imageY: meta.offsetTop * bitmapScale,
  };
}

/** Map the displayed device screen into CDP Input's viewport DIP coordinates.
 * Render the image below offsetTop, just as DevTools' ScreencastView does.
 * Neither bitmap DPR nor the local window DPR belongs in this conversion.
 * Input uses visual viewport coordinates: pageScaleFactor is only needed when
 * converting these coordinates to DOM/layout coordinates, not for dispatch.
 */
export function viewportPoint(
  clientX: number,
  clientY: number,
  rect: { left: number; top: number; width: number; height: number },
  meta: FrameGeometry,
): { x: number; y: number } | null {
  if (!validFrameGeometry(meta) || rect.width <= 0 || rect.height <= 0) return null;
  const x = (clientX - rect.left) * meta.deviceWidth / rect.width;
  const y = (clientY - rect.top) * meta.deviceHeight / rect.height - meta.offsetTop;
  if (!Number.isFinite(x) || !Number.isFinite(y) || y < 0) return null;
  return { x: Math.round(x), y: Math.round(y) };
}

export function inputTextMessage(text: string) {
  // Reject, never truncate: a partial paste can silently corrupt user input.
  if (!text || text.length > MAX_INPUT_TEXT_BYTES ||
    new TextEncoder().encode(text).byteLength > MAX_INPUT_TEXT_BYTES) return null;
  return { type: "input_text", text };
}

type BoundInput = Record<string, unknown> & { bindingEpoch: string };
type SendInput = (message: BoundInput) => boolean;

/** In-memory human-input state. Epochs originate ONLY on drawn server frames.
 * Changing binding invalidates pixels and discards keys without releasing them
 * into the new target. Blur releases retain each key's originating epoch.
 */
export class CanvasInputState {
  bindingEpoch: string | null = null;
  drawn: { bindingEpoch: string; geometry: FrameGeometry } | null = null;
  readonly pasteKeys = new Set<string>();
  private readonly pressed = new Map<string, BoundInput>();

  bind(epoch: string | null) {
    if (epoch === this.bindingEpoch) return false;
    this.bindingEpoch = epoch;
    this.drawn = null;
    this.pressed.clear();
    this.pasteKeys.clear();
    return true;
  }

  canDraw(epoch: string) {
    return !!epoch && epoch === this.bindingEpoch;
  }

  didDraw(epoch: string, geometry: FrameGeometry) {
    if (!this.canDraw(epoch) || !validFrameGeometry(geometry)) return false;
    this.drawn = { bindingEpoch: epoch, geometry };
    return true;
  }

  boundMessage(message: Record<string, unknown>): BoundInput | null {
    if (!this.drawn || this.drawn.bindingEpoch !== this.bindingEpoch) return null;
    return { ...message, bindingEpoch: this.drawn.bindingEpoch };
  }

  sent(message: BoundInput) {
    if (message.type !== "input_keyboard") return;
    const id = String(message.code || message.key);
    if (message.eventType === "keyUp") this.pressed.delete(id);
    else if (message.eventType === "keyDown" || message.eventType === "rawKeyDown") this.pressed.set(id, message);
  }

  releaseKeys(send: SendInput) {
    this.pasteKeys.clear();
    const keys = [...this.pressed.entries()].reverse();
    for (const [id, down] of keys) {
      this.pressed.delete(id);
      if (down.bindingEpoch !== this.bindingEpoch) continue;
      let modifiers = 0;
      for (const held of this.pressed.values()) {
        if (held.key === "Alt") modifiers |= 1;
        if (held.key === "Control") modifiers |= 2;
        if (held.key === "Meta") modifiers |= 4;
        if (held.key === "Shift") modifiers |= 8;
      }
      // No text and no fresh binding token: a rebind racing blur is rejected
      // by sendInputAtom and again under the server binding read guards.
      const { text: _text, ...key } = down;
      send({ ...key, eventType: "keyUp", modifiers });
    }
  }
}

type Key = Pick<KeyboardEvent, "type" | "key" | "code" | "ctrlKey" | "metaKey" | "altKey" | "shiftKey">;

/** Leave native paste keydown uncancelled, and suppress its matching keyup even
 * if the modifier was released first. Never send a second remote paste or 'v'.
 */
export function forwardCanvasKey(e: Key, pasteKeys: Set<string>): boolean {
  const id = e.code || e.key;
  if (e.type === "keyup" && pasteKeys.delete(id)) return false;
  const paste = !e.altKey && ((e.ctrlKey || e.metaKey) &&
    (e.code === "KeyV" || e.key.toLowerCase() === "v") ||
    e.shiftKey && !e.ctrlKey && !e.metaKey && e.key === "Insert");
  if (e.type === "keydown" && paste) {
    pasteKeys.add(id);
    return false;
  }
  return true;
}

const KEY_INFO: Record<string, { text?: string; keyCode: number }> = {
  Enter: { text: "\r", keyCode: 13 },
  Tab: { text: "\t", keyCode: 9 },
  Backspace: { text: "\b", keyCode: 8 },
  Escape: { keyCode: 27 },
  ArrowLeft: { keyCode: 37 },
  ArrowUp: { keyCode: 38 },
  ArrowRight: { keyCode: 39 },
  ArrowDown: { keyCode: 40 },
  Delete: { keyCode: 46 },
  Home: { keyCode: 36 },
  End: { keyCode: 35 },
  PageUp: { keyCode: 33 },
  PageDown: { keyCode: 34 },
  Shift: { keyCode: 16 },
  Control: { keyCode: 17 },
  Alt: { keyCode: 18 },
  Meta: { keyCode: 91 },
  CapsLock: { keyCode: 20 },
};

// Moved unchanged from viewport.tsx in upstream v0.41.1 (PR #2263).
// All printable VK lookup, including physical-code adaptation, uses this mapping.
export function printableKeyCode(key: string): number {
  if (key.length !== 1) return 0;
  const code = key.charCodeAt(0);
  if (code >= 65 && code <= 90) return code;
  if (code >= 97 && code <= 122) return code - 32;
  if (code >= 48 && code <= 57) return code;
  switch (key) {
    case " ": return 32;
    case ";": case ":": return 186;
    case "=": case "+": return 187;
    case ",": case "<": return 188;
    case "-": case "_": return 189;
    case ".": case ">": return 190;
    case "/": case "?": return 191;
    case "`": case "~": return 192;
    case "[": case "{": return 219;
    case "\\": case "|": return 220;
    case "]": case "}": return 221;
    case "'": case "\"": return 222;
    default: return 0;
  }
}

// Adapt physical punctuation positions without duplicating upstream's OEM VKs.
const CODE_PRINTABLE_KEYS: Record<string, string> = {
  Minus: "-", Equal: "=", BracketLeft: "[", BracketRight: "]",
  Backslash: "\\", Semicolon: ";", Quote: "'", Comma: ",",
  Period: ".", Slash: "/", Backquote: "`",
};

const NUMPAD_KEY_CODES: Record<string, number> = {
  NumpadAdd: 107, NumpadSubtract: 109, NumpadMultiply: 106,
  NumpadDivide: 111, NumpadDecimal: 110,
};

function virtualKeyCode(e: Key): number {
  if (/^Key[A-Z]$/.test(e.code)) return printableKeyCode(e.code.slice(3));
  if (/^Digit[0-9]$/.test(e.code)) return printableKeyCode(e.code.slice(5));
  if (/^Numpad[0-9]$/.test(e.code)) return 96 + Number(e.code[6]);
  if (/^F([1-9]|1[0-2])$/.test(e.code)) return 111 + Number(e.code.slice(1));
  const physicalKey = CODE_PRINTABLE_KEYS[e.code];
  return NUMPAD_KEY_CODES[e.code] ?? KEY_INFO[e.key]?.keyCode ??
    printableKeyCode(physicalKey ?? e.key);
}

export function keyboardMessage(e: Key, eventType: "keyDown" | "keyUp") {
  // Modifier shortcuts must not also insert their printable character.
  const text = eventType === "keyDown" && !e.ctrlKey && !e.metaKey && !e.altKey
    ? (KEY_INFO[e.key]?.text ?? (Array.from(e.key).length === 1 ? e.key : undefined))
    : undefined;
  const modifiers = Number(e.altKey) | (Number(e.ctrlKey) << 1) |
    (Number(e.metaKey) << 2) | (Number(e.shiftKey) << 3);
  return {
    type: "input_keyboard",
    eventType: eventType === "keyDown" && text === undefined ? "rawKeyDown" : eventType,
    key: e.key,
    code: e.code,
    ...(text === undefined ? {} : { text }),
    windowsVirtualKeyCode: virtualKeyCode(e),
    modifiers,
  };
}
