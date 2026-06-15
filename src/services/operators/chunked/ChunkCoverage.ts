import type { PartialChunkResult } from "../../../util/chunkTypes";
import { getLogicalChunkGroupId } from "./chunkWindow";
import type { CompletedChunkGroupState } from "./types";

export function collectChunkByWindow(
  chunksByWindow: Map<string, Map<string, PartialChunkResult>>,
  partial: PartialChunkResult,
  expectedSubqueryIds: string[],
): { chunkGroupId: string; missingSubqueryIds: string[]; isComplete: boolean } {
  const chunkGroupId = getLogicalChunkGroupId(partial);
  if (!chunksByWindow.has(chunkGroupId)) {
    chunksByWindow.set(chunkGroupId, new Map<string, PartialChunkResult>());
  }
  const bySubquery = chunksByWindow.get(chunkGroupId)!;
  bySubquery.set(partial.subqueryId, partial);
  const missingSubqueryIds = expectedSubqueryIds.filter(
    (subqueryId) => !bySubquery.has(subqueryId),
  );
  return {
    chunkGroupId,
    missingSubqueryIds,
    isComplete: missingSubqueryIds.length === 0,
  };
}

export function buildCoverageRecordsFromGroups(
  windowChunkGroups: CompletedChunkGroupState[],
  expectedSubqueryIds: string[],
): {
  requiredChunksBySubquery: Record<string, string[]>;
  receivedChunksUsedBySubquery: Record<string, string[]>;
  missingChunksBySubquery: Record<string, string[]>;
  duplicateChunksIgnoredBySubquery: Record<string, string[]>;
  coverageComplete: boolean;
  allExpectedSubqueriesPresent: boolean;
} {
  const requiredChunksBySubquery: Record<string, string[]> = {};
  const receivedChunksUsedBySubquery: Record<string, string[]> = {};
  const missingChunksBySubquery: Record<string, string[]> = {};
  const duplicateChunksIgnoredBySubquery: Record<string, string[]> = {};

  for (const subqueryId of expectedSubqueryIds) {
    requiredChunksBySubquery[subqueryId] = [];
    receivedChunksUsedBySubquery[subqueryId] = [];
    missingChunksBySubquery[subqueryId] = [];
    duplicateChunksIgnoredBySubquery[subqueryId] = [];
  }

  for (const group of windowChunkGroups) {
    for (const subqueryId of expectedSubqueryIds) {
      const receivedChunkIds =
        group.summary.receivedChunkIdsBySubquery[subqueryId] ?? [];
      requiredChunksBySubquery[subqueryId].push(...receivedChunkIds);
      receivedChunksUsedBySubquery[subqueryId].push(...receivedChunkIds);
      const duplicates =
        group.summary.duplicateChunksIgnoredBySubquery[subqueryId] ?? [];
      duplicateChunksIgnoredBySubquery[subqueryId].push(...duplicates);
      if ((group.summary.missingSubqueryIds ?? []).includes(subqueryId)) {
        missingChunksBySubquery[subqueryId].push(group.chunkGroupId);
      }
    }
  }

  const allExpectedSubqueriesPresent = expectedSubqueryIds.every(
    (subqueryId) => receivedChunksUsedBySubquery[subqueryId].length > 0,
  );
  const coverageComplete =
    allExpectedSubqueriesPresent &&
    expectedSubqueryIds.every(
      (subqueryId) => missingChunksBySubquery[subqueryId].length === 0,
    );

  return {
    requiredChunksBySubquery,
    receivedChunksUsedBySubquery,
    missingChunksBySubquery,
    duplicateChunksIgnoredBySubquery,
    coverageComplete,
    allExpectedSubqueriesPresent,
  };
}
