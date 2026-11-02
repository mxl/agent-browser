import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CanvasInputState, frameCanvasLayout, validFrameGeometry, viewportPoint } from '../packages/dashboard/src/lib/viewport-input.ts';

const meta = { deviceWidth: 1000, deviceHeight: 700, offsetTop: 0, pageScaleFactor: 1 };
const rect = { left: 10, top: 20, width: 500, height: 350 };
test('binding changes invalidate pixels and never stamp a new epoch onto an old canvas', () => {
  const state = new CanvasInputState();
  const paste = { type: 'input_mouse', x: 1, y: 2 };
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
  assert.equal(state.boundMessage({ type: 'input_mouse', x: 1, y: 2 }), null);
  assert.equal(state.didDraw('A-old', meta), false);
  state.bind('A-fresh');
  assert.equal(state.didDraw('A-old', meta), false);
  assert.equal(state.boundMessage({ type: 'input_mouse', x: 1, y: 2 }), null);
  state.didDraw('A-fresh', meta);
  assert.equal(state.boundMessage({ type: 'input_mouse', x: 1, y: 2 }).bindingEpoch, 'A-fresh');
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

