import type { PartialChunkResult } from "../../../util/chunkTypes";
import { alignWindowStart } from "../../../util/windowAlignment";
import type { CompletedChunkGroupState } from "./types";

export function getLogicalChunkGroupId(
  partial: Pick<PartialChunkResult, "window">,
): string {
  return buildLogicalChunkGroupIdFromBounds(
    partial.window.start,
    partial.window.end,
    Number(partial.window.alignmentOriginMs),
  );
}

export function buildLogicalChunkGroupIdFromBounds(
  start: number,
  end: number,
  alignmentOriginMs?: number | null,
): string {
  const normalizedAlignmentOriginMs = Number(alignmentOriginMs);
  if (Number.isFinite(normalizedAlignmentOriginMs)) {
    return `${normalizedAlignmentOriginMs}:${start}:${end}`;
  }
  return `${start}:${end}`;
}

export function insertCompletedChunkGroupOrdered(
  orderedGroups: CompletedChunkGroupState[],
  group: CompletedChunkGroupState,
): void {
  const lastGroup = orderedGroups[orderedGroups.length - 1];
  if (!lastGroup || lastGroup.start <= group.start) {
    orderedGroups.push(group);
    return;
  }

  let low = 0;
  let high = orderedGroups.length;
  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (orderedGroups[mid].start <= group.start) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  orderedGroups.splice(low, 0, group);
}

export function hasContiguousChunkGroups(
  groups: CompletedChunkGroupState[],
): boolean {
  for (let index = 1; index < groups.length; index += 1) {
    if (groups[index - 1].end !== groups[index].start) {
      return false;
    }
  }
  return true;
}

export function deriveExpectedChunkKeysForFinalWindow(args: {
  finalWindowStart: number;
  finalWindowEnd: number;
  chunkWindowWidthMs: number;
  alignmentOriginMs: number | null;
}): string[] {
  const {
    finalWindowStart,
    finalWindowEnd,
    chunkWindowWidthMs,
    alignmentOriginMs,
  } = args;
  const finalRangeMs = finalWindowEnd - finalWindowStart;
  if (
    !Number.isFinite(finalWindowStart) ||
    !Number.isFinite(finalWindowEnd) ||
    !Number.isFinite(chunkWindowWidthMs) ||
    chunkWindowWidthMs <= 0 ||
    finalRangeMs <= 0 ||
    finalRangeMs % chunkWindowWidthMs !== 0
  ) {
    return [];
  }

  const keys: string[] = [];
  for (
    let chunkStart = finalWindowStart;
    chunkStart < finalWindowEnd;
    chunkStart += chunkWindowWidthMs
  ) {
    keys.push(
      buildLogicalChunkGroupIdFromBounds(
        chunkStart,
        chunkStart + chunkWindowWidthMs,
        alignmentOriginMs,
      ),
    );
  }
  return keys;
}

export function deriveCandidateFinalWindowStartsForChunk(args: {
  chunkStart: number;
  chunkEnd: number;
  finalRangeMs: number;
  finalStepMs: number;
  chunkWindowWidthMs: number;
  alignmentOriginMs: number | null;
  minimumFinalWindowStartMs?: number | null;
}): number[] {
  const {
    chunkStart,
    chunkEnd,
    finalRangeMs,
    finalStepMs,
    chunkWindowWidthMs,
    alignmentOriginMs,
    minimumFinalWindowStartMs,
  } = args;
  if (
    !Number.isFinite(chunkStart) ||
    !Number.isFinite(chunkEnd) ||
    !Number.isFinite(finalRangeMs) ||
    !Number.isFinite(finalStepMs) ||
    !Number.isFinite(chunkWindowWidthMs) ||
    finalRangeMs <= 0 ||
    finalStepMs <= 0 ||
    chunkWindowWidthMs <= 0 ||
    chunkEnd - chunkStart !== chunkWindowWidthMs ||
    finalRangeMs % chunkWindowWidthMs !== 0
  ) {
    return [];
  }

  const minimumStart =
    minimumFinalWindowStartMs !== null &&
    minimumFinalWindowStartMs !== undefined &&
    Number.isFinite(minimumFinalWindowStartMs)
      ? minimumFinalWindowStartMs
      : Number.NEGATIVE_INFINITY;
  const chunkCount = finalRangeMs / chunkWindowWidthMs;
  const starts: number[] = [];

  for (let index = 0; index < chunkCount; index += 1) {
    const candidateStart = chunkStart - index * chunkWindowWidthMs;
    const candidateEnd = candidateStart + finalRangeMs;
    if (candidateStart > chunkStart || candidateEnd < chunkEnd) {
      continue;
    }
    if (candidateStart < minimumStart) {
      continue;
    }
    if (alignmentOriginMs !== null && Number.isFinite(alignmentOriginMs)) {
      if (
        alignWindowStart(
          candidateStart,
          finalStepMs,
          alignmentOriginMs,
        ) !== candidateStart
      ) {
        continue;
      }
    }
    starts.push(candidateStart);
  }

  return starts.sort((left, right) => left - right);
}
