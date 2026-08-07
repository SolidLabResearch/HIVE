import type { PartialChunkResult } from "../../../util/chunkTypes";
import type { AggregationFunction } from "../../../util/runtimeConfig";
import type {
  ChunkCoverageState,
  ChunkWindowDiagnostics,
  ComparableWindowDiagnostics,
  CompletedChunkGroupState,
  WindowRecompositionSummary,
} from "./types";

export function canRecomposeStructuredPartial(
  partial: PartialChunkResult,
  aggregationFunction: AggregationFunction,
): boolean {
  if (aggregationFunction === "COUNT") {
    return Number.isFinite(partial.count) || Number.isFinite(partial.value);
  }
  if (aggregationFunction === "SUM") {
    return Number.isFinite(partial.sum) || Number.isFinite(partial.value);
  }
  if (aggregationFunction === "AVG") {
    return (
      Number.isFinite(partial.count) &&
      (Number.isFinite(partial.sum) ||
        Number.isFinite(partial.avg) ||
        Number.isFinite(partial.value))
    );
  }
  if (aggregationFunction === "MIN" || aggregationFunction === "MAX") {
    return Number.isFinite(partial.value);
  }
  return false;
}

export function compactStructuredPartial(
  partial: PartialChunkResult,
  aggregationFunction: AggregationFunction,
): PartialChunkResult {
  if (!canRecomposeStructuredPartial(partial, aggregationFunction)) {
    return partial;
  }

  const { rdfPayload: _rdfPayload, ...compactPartial } = partial;
  return compactPartial;
}

export function summarizeChunkGroup(
  chunkGroupId: string,
  bySubquery: Map<string, PartialChunkResult>,
  aggregationFunction: AggregationFunction,
  coverageState: ChunkCoverageState,
): ChunkWindowDiagnostics {
  const partials = Array.from(bySubquery.values());
  const start = partials[0]?.window.start ?? 0;
  const end = partials[0]?.window.end ?? 0;
  const windowMetadata = partials[0]?.window;
  let countTotal = 0;
  let countAvailable = false;
  let sumTotal = 0;
  let sumAvailable = false;

  for (const partial of partials) {
    if (Number.isFinite(partial.count)) {
      countTotal += partial.count as number;
      countAvailable = true;
    }
    if (Number.isFinite(partial.sum)) {
      sumTotal += partial.sum as number;
      sumAvailable = true;
    } else if (Number.isFinite(partial.avg) && Number.isFinite(partial.count)) {
      sumTotal += (partial.avg as number) * (partial.count as number);
      sumAvailable = true;
    } else if (
      partial.aggregateFunction === "SUM" &&
      Number.isFinite(partial.value)
    ) {
      sumTotal += partial.value as number;
      sumAvailable = true;
    }
  }

  const avg =
    countAvailable && countTotal > 0 && sumAvailable
      ? sumTotal / countTotal
      : null;
  let value: number | null = null;

  if (aggregationFunction === "COUNT") {
    value = countAvailable ? countTotal : null;
  } else if (aggregationFunction === "SUM") {
    value = sumAvailable ? sumTotal : null;
  } else if (aggregationFunction === "AVG") {
    value = avg;
  } else if (aggregationFunction === "MIN") {
    const mins = partials
      .map((partial) => partial.value)
      .filter((entry): entry is number => Number.isFinite(entry));
    value = mins.length > 0 ? Math.min(...mins) : null;
  } else if (aggregationFunction === "MAX") {
    const maxs = partials
      .map((partial) => partial.value)
      .filter((entry): entry is number => Number.isFinite(entry));
    value = maxs.length > 0 ? Math.max(...maxs) : null;
  }

  const receivedChunkIdsBySubquery: Record<string, string[]> = {};
  const duplicateChunksIgnoredBySubquery: Record<string, string[]> = {};
  const missingSubqueryIds = coverageState.expectedSubqueryIds.filter(
    (subqueryId) => !bySubquery.has(subqueryId),
  );

  for (const subqueryId of coverageState.expectedSubqueryIds) {
    receivedChunkIdsBySubquery[subqueryId] = bySubquery.has(subqueryId)
      ? [bySubquery.get(subqueryId)!.chunkId]
      : [];
    duplicateChunksIgnoredBySubquery[subqueryId] = [
      ...(coverageState.duplicateChunksIgnoredBySubquery[subqueryId] ?? []),
    ];
  }

  const mins = partials
    .map((p) => (p.min !== undefined && p.min !== null ? p.min : p.value))
    .filter((v): v is number => Number.isFinite(v));
  const minVal = mins.length > 0 ? Math.min(...mins) : null;

  const maxs = partials
    .map((p) => (p.max !== undefined && p.max !== null ? p.max : p.value))
    .filter((v): v is number => Number.isFinite(v));
  const maxVal = maxs.length > 0 ? Math.max(...maxs) : null;

  return {
    chunkGroupId,
    start,
    end,
    count: countAvailable ? countTotal : null,
    sum: sumAvailable ? sumTotal : null,
    avg,
    min: minVal,
    max: maxVal,
    value,
    subqueries: partials.map((partial) => partial.subqueryId),
    producerIdentities: partials.map((partial) => ({
      canonicalProducerId: partial.canonicalProducerId ?? partial.subqueryId,
      runtimeProducerId: partial.runtimeProducerId ?? partial.subqueryId,
    })),
    receivedChunkIdsBySubquery,
    duplicateChunksIgnoredBySubquery,
    missingSubqueryIds,
    coverageComplete: missingSubqueryIds.length === 0,
    windowSemantics: windowMetadata?.windowSemantics,
    logicalTriggerTime: windowMetadata?.logicalTriggerTime ?? null,
    windowDataCloseTime: windowMetadata?.windowDataCloseTime ?? null,
    resultEmittedAt: windowMetadata?.resultEmittedAt ?? null,
    latencyFromLogicalTriggerMs: windowMetadata?.latencyFromLogicalTriggerMs ?? null,
    latencyFromWindowCloseMs: windowMetadata?.latencyFromWindowCloseMs ?? null,
    metadataSource: windowMetadata?.metadataSource ?? "reconstructed",
  };
}

export function summarizeWindowRecomposition(
  windowChunkGroups: CompletedChunkGroupState[],
  aggregationFunction: AggregationFunction,
): WindowRecompositionSummary | null {
  if (windowChunkGroups.length === 0) {
    return null;
  }

  const internalChunks = windowChunkGroups.map((group) => group.summary);
  const externalWindowStart = internalChunks[0].start;
  const externalWindowEnd = internalChunks[internalChunks.length - 1].end;
  let recomposedCount: number | null = null;
  let recomposedSum: number | null = null;
  let recomposedAvg: number | null = null;
  let recomposedMin: number | null = null;
  let recomposedMax: number | null = null;
  let resultValue: number | null = null;

  if (aggregationFunction === "COUNT") {
    const totalCount = internalChunks.reduce(
      (sum, chunk) => sum + (Number.isFinite(chunk.count) ? (chunk.count as number) : 0),
      0,
    );
    recomposedCount = totalCount;
    resultValue = totalCount;
  } else if (aggregationFunction === "SUM") {
    const totalSum = internalChunks.reduce((sum, chunk) => {
      if (Number.isFinite(chunk.sum)) {
        return sum + (chunk.sum as number);
      }
      if (Number.isFinite(chunk.value)) {
        return sum + (chunk.value as number);
      }
      return sum;
    }, 0);
    recomposedSum = totalSum;
    resultValue = totalSum;
  } else if (aggregationFunction === "AVG") {
    const totalCount = internalChunks.reduce(
      (sum, chunk) => sum + (Number.isFinite(chunk.count) ? (chunk.count as number) : 0),
      0,
    );
    const totalSum = internalChunks.reduce((sum, chunk) => {
      if (Number.isFinite(chunk.sum)) {
        return sum + (chunk.sum as number);
      }
      if (Number.isFinite(chunk.value)) {
        return sum + (chunk.value as number);
      }
      return sum;
    }, 0);
    recomposedCount = totalCount;
    recomposedSum = totalSum;
    recomposedAvg = totalCount > 0 ? totalSum / totalCount : null;
    resultValue = recomposedAvg;
  } else if (aggregationFunction === "MIN") {
    const mins = internalChunks
      .map((chunk) => {
        if (chunk.min !== undefined && chunk.min !== null && Number.isFinite(chunk.min)) {
          return chunk.min as number;
        }
        return chunk.value;
      })
      .filter((value): value is number => Number.isFinite(value ?? NaN));
    recomposedMin = mins.length > 0 ? Math.min(...mins) : null;
    resultValue = recomposedMin;
  } else if (aggregationFunction === "MAX") {
    const maxs = internalChunks
      .map((chunk) => {
        if (chunk.max !== undefined && chunk.max !== null && Number.isFinite(chunk.max)) {
          return chunk.max as number;
        }
        return chunk.value;
      })
      .filter((value): value is number => Number.isFinite(value ?? NaN));
    recomposedMax = maxs.length > 0 ? Math.max(...maxs) : null;
    resultValue = recomposedMax;
  }

  if (resultValue === null || !Number.isFinite(resultValue)) {
    return null;
  }

  return {
    externalWindowStart,
    externalWindowEnd,
    internalChunkGroupIds: internalChunks.map((chunk) => chunk.chunkGroupId),
    internalChunks,
    recomposedCount,
    recomposedSum,
    recomposedAvg,
    recomposedMin,
    recomposedMax,
    resultValue: resultValue as number,
  };
}

export function recomposeComparableWindow(
  windowChunkGroups: CompletedChunkGroupState[],
  aggregationFunction: AggregationFunction,
  externalWindowNumber: number,
): ComparableWindowDiagnostics | null {
  const summary = summarizeWindowRecomposition(
    windowChunkGroups,
    aggregationFunction,
  );

  if (!summary) {
    return null;
  }

  return {
    externalWindowNumber,
    externalWindowStart: summary.externalWindowStart,
    externalWindowEnd: summary.externalWindowEnd,
    internalChunkGroupIds: summary.internalChunkGroupIds,
    internalChunks: summary.internalChunks,
    recomposedCount: summary.recomposedCount,
    recomposedSum: summary.recomposedSum,
    recomposedAvg: summary.recomposedAvg,
    recomposedMin: summary.recomposedMin,
    recomposedMax: summary.recomposedMax,
    resultValue: summary.resultValue,
  };
}

export function recomputeExactResultFromPartials(
  partials: PartialChunkResult[],
  aggregationFunction: AggregationFunction,
): number | null {
  if (partials.length === 0) {
    return null;
  }

  if (aggregationFunction === "COUNT") {
    const totalCount = partials.reduce((sum, partial) => {
      if (Number.isFinite(partial.count)) {
        return sum + (partial.count as number);
      }
      if (Number.isFinite(partial.value)) {
        return sum + (partial.value as number);
      }
      return sum;
    }, 0);
    return totalCount;
  }

  if (aggregationFunction === "SUM") {
    const totalSum = partials.reduce((sum, partial) => {
      if (Number.isFinite(partial.sum)) {
        return sum + (partial.sum as number);
      }
      if (Number.isFinite(partial.value)) {
        return sum + (partial.value as number);
      }
      return sum;
    }, 0);
    return totalSum;
  }

  if (aggregationFunction === "AVG") {
    let totalCount = 0;
    let totalSum = 0;

    for (const partial of partials) {
      if (Number.isFinite(partial.count)) {
        totalCount += partial.count as number;
      }
      if (Number.isFinite(partial.sum)) {
        totalSum += partial.sum as number;
      } else if (Number.isFinite(partial.avg) && Number.isFinite(partial.count)) {
        totalSum += (partial.avg as number) * (partial.count as number);
      } else if (Number.isFinite(partial.value) && Number.isFinite(partial.count)) {
        totalSum += (partial.value as number) * (partial.count as number);
      }
    }

    return totalCount > 0 ? totalSum / totalCount : null;
  }

  if (aggregationFunction === "MIN") {
    const values = partials
      .map((partial) => partial.value)
      .filter((value): value is number => Number.isFinite(value));
    return values.length > 0 ? Math.min(...values) : null;
  }

  if (aggregationFunction === "MAX") {
    const values = partials
      .map((partial) => partial.value)
      .filter((value): value is number => Number.isFinite(value));
    return values.length > 0 ? Math.max(...values) : null;
  }

  return null;
}
