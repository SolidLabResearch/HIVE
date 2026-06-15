import type { PartialChunkResult } from "../../../util/chunkTypes";
import { buildCoverageRecordsFromGroups } from "./ChunkCoverage";
import type {
  ChunkCoverageState,
  ChunkEmissionProofEntry,
  ComparableWindowDiagnostics,
} from "./types";

export function buildChunkEmissionProofEntry(
  partials: PartialChunkResult[],
  comparableDiagnostics: ComparableWindowDiagnostics | undefined,
  emissionReason: string,
  emittedAt: number,
  coverageState?: ChunkCoverageState,
): ChunkEmissionProofEntry | null {
  if (comparableDiagnostics) {
    const windowChunkGroups = comparableDiagnostics.internalChunks.map((chunk) => ({
      chunkGroupId: chunk.chunkGroupId,
      start: chunk.start,
      end: chunk.end,
      summary: chunk,
    }));
    const coverage = buildCoverageRecordsFromGroups(
      windowChunkGroups,
      comparableDiagnostics.internalChunks[0]?.subqueries ?? [],
    );
    return {
      windowStart: comparableDiagnostics.externalWindowStart,
      windowEnd: comparableDiagnostics.externalWindowEnd,
      emittedAt,
      emissionReason,
      expectedSubqueryIds:
        comparableDiagnostics.internalChunks[0]?.subqueries ?? [],
      expectedSubqueryCount:
        comparableDiagnostics.internalChunks[0]?.subqueries.length ?? 0,
      requiredChunksBySubquery: coverage.requiredChunksBySubquery,
      receivedChunksUsedBySubquery: coverage.receivedChunksUsedBySubquery,
      missingChunksBySubquery: coverage.missingChunksBySubquery,
      duplicateChunksIgnoredBySubquery:
        coverage.duplicateChunksIgnoredBySubquery,
      coverageComplete: coverage.coverageComplete,
      allExpectedSubqueriesPresent: coverage.allExpectedSubqueriesPresent,
      emitted: true,
    };
  }

  if (partials.length === 0) {
    return null;
  }

  const expectedSubqueryIds =
    coverageState?.expectedSubqueryIds ?? partials.map((partial) => partial.subqueryId);
  const partialBySubquery = new Map(
    partials.map((partial) => [partial.subqueryId, partial]),
  );
  const requiredChunksBySubquery: Record<string, string[]> = {};
  const receivedChunksUsedBySubquery: Record<string, string[]> = {};
  const missingChunksBySubquery: Record<string, string[]> = {};
  const duplicateChunksIgnoredBySubquery: Record<string, string[]> = {};

  for (const subqueryId of expectedSubqueryIds) {
    const partial = partialBySubquery.get(subqueryId);
    const receivedChunkIds = partial ? [partial.chunkId] : [];
    requiredChunksBySubquery[subqueryId] = [...receivedChunkIds];
    receivedChunksUsedBySubquery[subqueryId] = [...receivedChunkIds];
    missingChunksBySubquery[subqueryId] = partial ? [] : [`missing:${subqueryId}`];
    duplicateChunksIgnoredBySubquery[subqueryId] = [
      ...(coverageState?.duplicateChunksIgnoredBySubquery[subqueryId] ?? []),
    ];
  }

  return {
    windowStart: partials[0].window.start,
    windowEnd: partials[0].window.end,
    emittedAt,
    emissionReason,
    expectedSubqueryIds,
    expectedSubqueryCount: expectedSubqueryIds.length,
    requiredChunksBySubquery,
    receivedChunksUsedBySubquery,
    missingChunksBySubquery,
    duplicateChunksIgnoredBySubquery,
    coverageComplete:
      coverageState !== undefined
        ? coverageState.expectedSubqueryIds.every(
            (subqueryId) => receivedChunksUsedBySubquery[subqueryId]?.length > 0,
          ) &&
          coverageState.expectedSubqueryIds.every(
            (subqueryId) => missingChunksBySubquery[subqueryId]?.length === 0,
          )
        : true,
    allExpectedSubqueriesPresent:
      coverageState !== undefined
        ? coverageState.expectedSubqueryIds.every(
            (subqueryId) => receivedChunksUsedBySubquery[subqueryId]?.length > 0,
          )
        : true,
    emitted: true,
  };
}
