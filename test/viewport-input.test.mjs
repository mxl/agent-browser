import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  MAX_INPUT_TEXT_BYTES,
  CanvasInputState,
  forwardCanvasKey,
  frameCanvasLayout,
  inputTextMessage,
  keyboardMessage,
  printableKeyCode,
  validFrameGeometry,
  viewportPoint,
} from '../packages/dashboard/src/lib/viewport-input.ts';

const meta = { deviceWidth: 1000, deviceHeight: 700, offsetTop: 0, pageScaleFactor: 1 };
const rect = { left: 10, top: 20, width: 500, height: 350 };
const key = (overrides = {}) => ({
  type: 'keydown', key: 'a', code: 'KeyA',
  ctrlKey: false, metaKey: false, altKey: false, shiftKey: false,
  ...overrides,
});

test('binding changes invalidate pixels and never stamp a new epoch onto an old canvas', () => {
  const state = new CanvasInputState();
  const paste = inputTextMessage('synthetic');
  state.bind('A');
  assert.equal(state.boundMessage(paste), null); // no drawn frame
  assert.equal(state.didDraw('A', meta), true);
  assert.deepEqual(state.boundMessage(paste), { ...paste, bindingEpoch: 'A' });
  state.bind('B');
  assert.equal(state.drawn, null);
  assert.equal(state.boundMessage(paste), null); // B binding alone is not pixels
  assert.equal(state.didDraw('A', meta), false); // delayed A bitmap decode
  assert.equal(state.boundMessage(paste), null);
  assert.equal(state.didDraw('B', meta), true);
  assert.equal(state.boundMessage(paste).bindingEpoch, 'B');
});

test('disconnect/reconnect and fresh A epochs cannot revive stale A frames', () => {
  const state = new CanvasInputState();
  state.bind('A-old');
  state.didDraw('A-old', meta);
  state.bind(null);
  assert.equal(state.boundMessage(inputTextMessage('synthetic')), null);
  assert.equal(state.didDraw('A-old', meta), false);
  state.bind('A-fresh');
  assert.equal(state.didDraw('A-old', meta), false);
  assert.equal(state.boundMessage(inputTextMessage('synthetic')), null);
  state.didDraw('A-fresh', meta);
  assert.equal(state.boundMessage(inputTextMessage('synthetic')).bindingEpoch, 'A-fresh');
});

test('blur releases remote keys with their original epoch and resets paste suppression', () => {
  const state = new CanvasInputState();
  state.bind('A');
  state.didDraw('A', meta);
  for (const event of [key({ key: 'Control', code: 'ControlLeft', ctrlKey: true }), key({ ctrlKey: true })]) {
    state.sent(state.boundMessage(keyboardMessage(event, 'keyDown')));
  }
  state.pasteKeys.add('KeyV');
  const released = [];
  state.releaseKeys((message) => { released.push(message); return true; });
  assert.deepEqual(released.map((m) => [m.key, m.eventType, m.modifiers, m.bindingEpoch]), [
    ['a', 'keyUp', 2, 'A'], ['Control', 'keyUp', 0, 'A'],
  ]);
  assert.ok(released.every((m) => !Object.hasOwn(m, 'text')));
  assert.equal(state.pasteKeys.size, 0);
  state.releaseKeys((message) => { released.push(message); return true; });
  assert.equal(released.length, 2); // canvas blur + window blur do not duplicate
});

test('rebind discards old key state and never releases modifiers to another target', () => {
  const state = new CanvasInputState();
  state.bind('A');
  state.didDraw('A', meta);
  state.sent(state.boundMessage(keyboardMessage(key({ key: 'Meta', code: 'MetaLeft', metaKey: true }), 'keyDown')));
  state.pasteKeys.add('KeyV');
  state.bind('B');
  state.didDraw('B', meta);
  const released = [];
  state.releaseKeys((message) => { released.push(message); return true; });
  assert.deepEqual(released, []);
  assert.equal(state.pasteKeys.size, 0);
});

test('maps actual frame dimensions, not a configured 1280x720 viewport', () => {
  assert.deepEqual(viewportPoint(260, 195, rect, meta), { x: 500, y: 350 });
  assert.deepEqual(viewportPoint(10, 20, rect, meta), { x: 0, y: 0 });
});

test('bitmap DPR and local DPR do not multiply input coordinates', () => {
  for (const bitmapWidth of [500, 1000, 2000]) {
    const layout = frameCanvasLayout(bitmapWidth, meta);
    assert.equal(layout.width, bitmapWidth);
    assert.equal(layout.height, bitmapWidth * 0.7);
    assert.deepEqual(viewportPoint(260, 195, rect, meta), { x: 500, y: 350 });
  }
});

test('renders top controls separately and subtracts offset exactly once for input', () => {
  const offsetMeta = { ...meta, offsetTop: 50, pageScaleFactor: 2 };
  assert.deepEqual(frameCanvasLayout(2000, offsetMeta), { width: 2000, height: 1400, imageY: 100 });
  assert.deepEqual(viewportPoint(260, 195, rect, offsetMeta), { x: 500, y: 300 });
  assert.equal(viewportPoint(10, 30, rect, offsetMeta), null);
  // Input uses visual viewport DIP, not DOM CSS: do not divide by pinch scale.
  assert.deepEqual(viewportPoint(260, 195, rect, { ...meta, pageScaleFactor: 2 }), { x: 500, y: 350 });
});

test('rejects missing/invalid frame geometry and zero-size display', () => {
  assert.equal(validFrameGeometry(undefined), false);
  for (const invalid of [
    { ...meta, deviceWidth: 0 }, { ...meta, deviceHeight: NaN },
    { ...meta, offsetTop: -1 }, { ...meta, offsetTop: 700 },
    { ...meta, pageScaleFactor: 0 },
  ]) {
    assert.equal(validFrameGeometry(invalid), false);
    assert.equal(viewportPoint(100, 100, rect, invalid), null);
  }
  assert.equal(viewportPoint(100, 100, { ...rect, width: 0 }, meta), null);
});

test('plain text message preserves Unicode/newlines and uses a bounded UTF-8 payload', () => {
  const text = 'synthetic\nстрока\t漢字';
  assert.deepEqual(inputTextMessage(text), { type: 'input_text', text });
  assert.equal(inputTextMessage(''), null);
  assert.notEqual(inputTextMessage('a'.repeat(MAX_INPUT_TEXT_BYTES)), null);
  assert.equal(inputTextMessage('a'.repeat(MAX_INPUT_TEXT_BYTES + 1)), null);
  assert.notEqual(inputTextMessage('я'.repeat(MAX_INPUT_TEXT_BYTES / 2)), null);
  assert.equal(inputTextMessage('я'.repeat(MAX_INPUT_TEXT_BYTES / 2 + 1)), null);
});

test('Ctrl/Meta+V are left native and neither down nor matching up reaches CDP', () => {
  for (const modifiers of [{ ctrlKey: true }, { metaKey: true }, { metaKey: true, shiftKey: true }]) {
    const suppressed = new Set();
    const down = key({ ...modifiers, key: 'v', code: 'KeyV' });
    assert.equal(forwardCanvasKey(down, suppressed), false);
    assert.equal(forwardCanvasKey(down, suppressed), false); // repeat
    // Even if the modifier is released before V, no stray V keyup is sent.
    assert.equal(forwardCanvasKey(key({ type: 'keyup', key: 'v', code: 'KeyV' }), suppressed), false);
    assert.equal(suppressed.size, 0);
    assert.equal(forwardCanvasKey(key({ key: 'v', code: 'KeyV' }), suppressed), true);
  }
});

test('native paste uses physical KeyV on non-Latin layouts and supports Shift+Insert', () => {
  assert.equal(forwardCanvasKey(key({ key: 'м', code: 'KeyV', ctrlKey: true }), new Set()), false);
  assert.equal(forwardCanvasKey(key({ key: 'Insert', code: 'Insert', shiftKey: true }), new Set()), false);
  assert.equal(forwardCanvasKey(key({ key: 'v', code: 'KeyV', ctrlKey: true, altKey: true }), new Set()), true);
});

test('ordinary printable keys and Shift preserve text with uppercase Windows VK codes', () => {
  const message = keyboardMessage(key(), 'keyDown');
  assert.equal(message.text, 'a');
  assert.equal(message.windowsVirtualKeyCode, 65);
  assert.equal(message.eventType, 'keyDown');
  const shifted = keyboardMessage(key({ key: 'A', shiftKey: true }), 'keyDown');
  assert.equal(shifted.text, 'A');
  assert.equal(shifted.modifiers, 8);
  assert.equal(keyboardMessage(key({ key: 'Enter', code: 'Enter' }), 'keyDown').text, '\r');
});

test('upstream printable mapping retains ASCII letters, digits, space and all OEM pairs', () => {
  for (let code = 65; code <= 90; code++) {
    assert.equal(printableKeyCode(String.fromCharCode(code)), code);
    assert.equal(printableKeyCode(String.fromCharCode(code + 32)), code);
  }
  for (const digit of '0123456789') assert.equal(printableKeyCode(digit), digit.charCodeAt(0));
  const cases = [
    [' ', 32], [';:', 186], ['=+', 187], [',<', 188], ['-_', 189],
    ['.>', 190], ['/?', 191], ['`~', 192], ['[{', 219], ['\\|', 220],
    [']}', 221], ["'\"", 222],
  ];
  for (const [characters, expected] of cases) {
    for (const character of characters) {
      assert.equal(printableKeyCode(character), expected);
      // Missing physical code uses upstream's key-based fallback unchanged.
      const message = keyboardMessage(key({ key: character, code: '' }), 'keyDown');
      assert.equal(message.windowsVirtualKeyCode, expected);
      assert.equal(message.text, character);
    }
  }
  for (const unsupported of ['', 'Enter', 'я', '漢', '\u{1f600}', '!']) {
    assert.equal(printableKeyCode(unsupported), 0);
  }
});

test('upstream codepoint-aware text retains BMP and astral Unicode without invented VKs', () => {
  for (const character of ['я', '漢', '\u{1f600}', '𝄞']) {
    const event = key({ key: character, code: '' });
    const down = keyboardMessage(event, 'keyDown');
    assert.equal(down.text, character);
    assert.equal(down.eventType, 'keyDown');
    assert.equal(down.windowsVirtualKeyCode, 0);
    assert.equal(Object.hasOwn(keyboardMessage(event, 'keyUp'), 'text'), false);
    const shortcut = keyboardMessage({ ...event, ctrlKey: true }, 'keyDown');
    assert.equal(shortcut.eventType, 'rawKeyDown');
    assert.equal(Object.hasOwn(shortcut, 'text'), false);
  }
  for (const nonSingleCodepoint of ['Dead', 'Unidentified', 'e\u0301', '\u{1f469}\u200d\u{1f4bb}']) {
    assert.equal(Object.hasOwn(keyboardMessage(key({ key: nonSingleCodepoint, code: '' }), 'keyDown'), 'text'), false);
  }
});

test('physical keys adapt through the canonical printable mapping on shifted and non-Latin layouts', () => {
  const cases = [
    ['ф', 'KeyA', 65], ['м', 'KeyV', 86], ['!', 'Digit1', 49],
    ['№', 'Digit3', 51], ['ж', 'Semicolon', 186], ['ё', 'Backquote', 192],
    ['Х', 'BracketLeft', 219], ['|', 'Backslash', 220],
  ];
  for (const [character, code, expected] of cases) {
    const event = key({ key: character, code, shiftKey: true });
    const down = keyboardMessage(event, 'keyDown');
    assert.equal(down.text, character);
    assert.equal(down.windowsVirtualKeyCode, expected);
    assert.equal(keyboardMessage(event, 'keyUp').windowsVirtualKeyCode, expected);
    const shortcut = keyboardMessage({ ...event, metaKey: true }, 'keyDown');
    assert.equal(shortcut.windowsVirtualKeyCode, expected);
    assert.equal(shortcut.eventType, 'rawKeyDown');
    assert.equal(Object.hasOwn(shortcut, 'text'), false);
  }
});

test('numpad, function and modifier VKs remain distinct from printable mapping', () => {
  const cases = [
    ['1', 'Numpad1', 97], ['+', 'NumpadAdd', 107], ['-', 'NumpadSubtract', 109],
    ['*', 'NumpadMultiply', 106], ['/', 'NumpadDivide', 111], ['.', 'NumpadDecimal', 110],
    ['F1', 'F1', 112], ['F12', 'F12', 123], ['Shift', 'ShiftLeft', 16],
    ['Control', 'ControlRight', 17], ['Alt', 'AltLeft', 18], ['Meta', 'MetaRight', 91],
  ];
  for (const [character, code, expected] of cases) {
    assert.equal(keyboardMessage(key({ key: character, code }), 'keyDown').windowsVirtualKeyCode, expected);
  }
});

test('modifier shortcuts have rawKeyDown/no text and balanced keyUp', () => {
  for (const modifiers of [{ ctrlKey: true }, { metaKey: true }, { altKey: true }]) {
    const event = key({ ...modifiers, key: 'c', code: 'KeyC' });
    assert.equal(forwardCanvasKey(event, new Set()), true);
    const down = keyboardMessage(event, 'keyDown');
    const up = keyboardMessage({ ...event, type: 'keyup' }, 'keyUp');
    assert.equal(down.eventType, 'rawKeyDown');
    assert.equal(down.windowsVirtualKeyCode, 67);
    assert.equal(Object.hasOwn(down, 'text'), false);
    assert.equal(up.eventType, 'keyUp');
    assert.equal(up.windowsVirtualKeyCode, 67);
    assert.equal(Object.hasOwn(up, 'text'), false);
  }
  assert.equal(keyboardMessage(key({ key: 'Control', code: 'ControlLeft', ctrlKey: true }), 'keyDown').windowsVirtualKeyCode, 17);
});
