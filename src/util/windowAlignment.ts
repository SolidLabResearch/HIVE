export type EventTimeWindowDefinition = {
  rangeMs: number;
  stepMs: number;
  alignmentOriginMs: number;
};

export function alignWindowStart(
  timestampMs: number,
  stepMs: number,
  alignmentOriginMs: number,
): number {
  return (
    alignmentOriginMs +
    Math.floor((timestampMs - alignmentOriginMs) / stepMs) * stepMs
  );
}

export function alignWindowBoundsToOrigin(
  bounds: { start: number; end: number },
  definition: EventTimeWindowDefinition,
): { start: number; end: number } {
  const alignedStart = alignWindowStart(
    bounds.start,
    definition.stepMs,
    definition.alignmentOriginMs,
  );
  return {
    start: alignedStart,
    end: alignedStart + definition.rangeMs,
  };
}
