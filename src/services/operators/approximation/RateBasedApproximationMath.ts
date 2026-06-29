export type ApproximationAggregation =
  | "SUM"
  | "AVG"
  | "COUNT"
  | "MIN"
  | "MAX";

export interface ApproximationWindowResult {
  start: number;
  end: number;
  value: number;
}

export interface ApproximationTargetWindow {
  start: number;
  end: number;
}

/**
 * Merges two window results with overlap subtraction for sliding windows.
 * @param win1 - First window result {start, end, value}.
 * @param win2 - Second window result {start, end, value}.
 * @param overlap - Overlap window result {start, end, value}.
 * @param target - Target window {start, end}.
 * @param agg - Aggregation function: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX'.
 * @returns The approximate aggregation for the target window.
 */
export function mergeSlidingWindowResults(
  win1: ApproximationWindowResult,
  win2: ApproximationWindowResult,
  overlap: ApproximationWindowResult,
  target: ApproximationTargetWindow,
  agg: ApproximationAggregation,
): number | string {
  const dur1 = win1.end - win1.start;
  const dur2 = win2.end - win2.start;
  const dur3 = target.end - target.start;
  const overlap_dur = Math.max(
    0,
    Math.min(win1.end, win2.end) - Math.max(win1.start, win2.start),
  );

  if (agg === "SUM" || agg === "AVG") {
    const combined_sum =
      win1.value * dur1 + win2.value * dur2 - overlap.value * overlap_dur;
    return agg === "AVG" ? combined_sum / dur3 : combined_sum;
  } else if (agg === "COUNT") {
    const combined = win1.value + win2.value - overlap.value;
    return combined;
  } else if (agg === "MIN") {
    return Math.min(win1.value, win2.value, overlap.value);
  } else if (agg === "MAX") {
    return Math.max(win1.value, win2.value, overlap.value);
  } else {
    return "Not Supported";
  }
}

export function mergeMultipleSlidingWindowResults(
  windows: ApproximationWindowResult[],
  target: ApproximationTargetWindow,
  agg: ApproximationAggregation,
  startIndex = 0,
): number | string {
  const debugEnabled =
    process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1";
  const debugLog = (...args: unknown[]) => {
    if (debugEnabled) {
      console.log(...args);
    }
  };

  debugLog(`DEBUG: mergeMultipleSlidingWindowResults called with:`);
  debugLog(`  - windows:`, debugEnabled ? JSON.stringify(windows) : "[redacted]");
  debugLog(`  - target:`, debugEnabled ? JSON.stringify(target) : "[redacted]");
  debugLog(`  - aggregation:`, agg);

  const activeStart = Math.max(0, startIndex);
  const overlapping: ApproximationWindowResult[] = [];
  for (let index = activeStart; index < windows.length; index += 1) {
    const window = windows[index];
    if (window.end > target.start && window.start < target.end) {
      overlapping.push(window);
    }
  }
  debugLog(
    `DEBUG: Found ${overlapping.length} overlapping windows:`,
    debugEnabled ? JSON.stringify(overlapping) : "[redacted]",
  );

  if (overlapping.length === 0) return 0;

  if (agg === "MIN") {
    const result = Math.min(...overlapping.map((w) => w.value));
    debugLog(`DEBUG: MIN result:`, result);
    return result;
  }
  if (agg === "MAX") {
    const result = Math.max(...overlapping.map((w) => w.value));
    debugLog(`DEBUG: MAX result:`, result);
    return result;
  }

  if (agg === "AVG") {
    let weightedSum = 0;
    let totalWeight = 0;

    debugLog(`DEBUG: Calculating weighted average:`);
    overlapping.forEach((w, idx) => {
      const overlapStart = Math.max(w.start, target.start);
      const overlapEnd = Math.min(w.end, target.end);
      const overlapDuration = overlapEnd - overlapStart;

      debugLog(
        `DEBUG: Window ${idx}: value=${w.value}, overlapDuration=${overlapDuration}`,
      );

      if (overlapDuration > 0) {
        const contribution = w.value * overlapDuration;
        weightedSum += contribution;
        totalWeight += overlapDuration;
        debugLog(
          `DEBUG: Added contribution=${contribution}, totalWeight=${totalWeight}, weightedSum=${weightedSum}`,
        );
      }
    });

    const result = totalWeight > 0 ? weightedSum / totalWeight : 0;
    debugLog(
      `DEBUG: Final AVG calculation: weightedSum=${weightedSum} / totalWeight=${totalWeight} = ${result}`,
    );
    return result;
  }

  const boundaries = new Set<number>();
  boundaries.add(target.start);
  boundaries.add(target.end);
  overlapping.forEach((w) => {
    if (w.start > target.start && w.start < target.end) boundaries.add(w.start);
    if (w.end > target.start && w.end < target.end) boundaries.add(w.end);
  });

  const sortedBoundaries = Array.from(boundaries).sort((a, b) => a - b);

  let totalSum = 0;

  for (let i = 0; i < sortedBoundaries.length - 1; i++) {
    const subStart = sortedBoundaries[i];
    const subEnd = sortedBoundaries[i + 1];
    const subDuration = subEnd - subStart;
    if (subDuration <= 0) continue;

    const coveringWindows = overlapping.filter(
      (w) => w.start <= subStart && w.end >= subEnd,
    );
    if (coveringWindows.length === 0) continue;

    if (agg === "SUM") {
      const subValueRateSum = coveringWindows.reduce((acc, w) => {
        const windowDuration = w.end - w.start;
        if (windowDuration <= 0) return acc;
        const rate = w.value / windowDuration;
        return acc + rate;
      }, 0);
      totalSum += subValueRateSum * subDuration;
    } else if (agg === "COUNT") {
      const subValueSum = coveringWindows.reduce((acc, w) => acc + w.value, 0);
      totalSum += subValueSum;
    }
  }

  return totalSum;
}
