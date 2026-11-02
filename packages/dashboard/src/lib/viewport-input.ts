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

