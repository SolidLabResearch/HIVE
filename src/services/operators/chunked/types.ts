import type { AggregationFunction } from "../../../util/runtimeConfig";
import type { PartialChunkResult } from "../../../util/chunkTypes";
import type { CompatibleChunkReuseSpec } from "../../../util/chunkStateReuse";

export type SubqueryIdentity = {
  subqueryId: string;
  topic: string;
};

export type ChunkWindowDiagnostics = {
  chunkGroupId: string;
  start: number;
  end: number;
  count: number | null;
  sum: number | null;
  avg: number | null;
  value: number | null;
  min: number | null;
  max: number | null;
  subqueries: string[];
  receivedChunkIdsBySubquery: Record<string, string[]>;
  duplicateChunksIgnoredBySubquery: Record<string, string[]>;
  missingSubqueryIds: string[];
  coverageComplete: boolean;
  windowSemantics?: string;
  logicalTriggerTime?: number | null;
  windowDataCloseTime?: number | null;
  resultEmittedAt?: number | null;
  latencyFromLogicalTriggerMs?: number | null;
  latencyFromWindowCloseMs?: number | null;
  metadataSource?: "direct" | "reconstructed";
};

export type ComparableWindowDiagnostics = {
  externalWindowNumber: number;
  externalWindowStart: number;
  externalWindowEnd: number;
  internalChunkGroupIds: string[];
  internalChunks: ChunkWindowDiagnostics[];
  recomposedCount: number | null;
  recomposedSum: number | null;
  recomposedAvg: number | null;
  recomposedMin?: number | null;
  recomposedMax?: number | null;
  resultValue: number;
};

export type ChunkCoverageState = {
  chunkGroupId: string;
  expectedSubqueryIds: string[];
  receivedChunkIdsBySubquery: Record<string, string[]>;
  duplicateChunksIgnoredBySubquery: Record<string, string[]>;
};

export type ChunkEmissionProofEntry = {
  windowStart: number;
  windowEnd: number;
  emittedAt: number;
  emissionReason: string;
  expectedSubqueryIds: string[];
  expectedSubqueryCount: number;
  requiredChunksBySubquery: Record<string, string[]>;
  receivedChunksUsedBySubquery: Record<string, string[]>;
  missingChunksBySubquery: Record<string, string[]>;
  duplicateChunksIgnoredBySubquery: Record<string, string[]>;
  coverageComplete: boolean;
  allExpectedSubqueriesPresent: boolean;
  emitted: boolean;
};

export type WindowRecompositionSummary = {
  externalWindowStart: number;
  externalWindowEnd: number;
  internalChunkGroupIds: string[];
  internalChunks: ChunkWindowDiagnostics[];
  recomposedCount: number | null;
  recomposedSum: number | null;
  recomposedAvg: number | null;
  recomposedMin?: number | null;
  recomposedMax?: number | null;
  resultValue: number;
};

export type ParentPartialDiagnostics = {
  outputType: "parent_partial";
  comparable: false;
  benchmarkEventTimeAnchor: number | null;
  parentWindowNumber: number;
  parentWindowStart: number;
  parentWindowEndOrCoveredUntil: number;
  parentRangeMs: number;
  coveredDurationMs: number;
  chunksUsed: number;
  eventCount: number | null;
  sum: number | null;
  avg: number | null;
  min?: number | null;
  max?: number | null;
  resultValue: number;
  emittedAtMs: number;
  elapsedSinceRegistrationMs: number;
  delayPastPartialTriggerMs: number;
  internalChunkIds: string[];
  internalChunks: ChunkWindowDiagnostics[];
};

export type CompletedChunkGroupState = {
  chunkGroupId: string;
  start: number;
  end: number;
  summary: ChunkWindowDiagnostics;
};

export type DerivedOriginalChunkSummary = {
  chunkId: string;
  start: number;
  end: number;
  sum?: number;
  count?: number;
  min?: number;
  max?: number;
};

export type DerivedOriginalOutputConsumer = {
  emittedWindowCount: number;
  nextWindowStartIndex: number;
  orderedChunks: DerivedOriginalChunkSummary[];
  originalOutputTopic: string;
  originalQueryHash: string;
  projectionTerms: CompatibleChunkReuseSpec["projectionTerms"];
  reuseSpec: CompatibleChunkReuseSpec;
};

export type ChunkedDebugSummary = {
  chunkSizeMs: number;
  comparableOutputCadenceOnly: boolean;
  useImmediateTrigger: boolean;
  expectedSubqueryCount: number;
  expectedSubqueryIds: string[];
  subscribedTopics: string[];
  receivedChunkMessageCount: number;
  structuredChunkMessageCount: number;
  duplicateChunkCount: number;
  ignoredLegacyChunkCount: number;
  completedChunkGroupCount: number;
  comparableWindowEmissionCount: number;
  reconstructedSuperqueryResultCount: number;
  lastComparableWindowStart: number | null;
  lastComparableWindowEnd: number | null;
  missingChunkGroupCount: number;
  emittedIncompleteWindowCount: number;
  emissionProofWindowCount: number;
  coverageCompleteEmissionCount: number;
  coverageIncompleteBlockedCount: number;
  intervalTriggersWithoutEmission: number;
  intervalTriggersWithEmission: number;
};

export type ChunkProcessingState = {
  chunksByWindow: Map<string, Map<string, PartialChunkResult>>;
  chunkCoverageByWindow: Map<string, ChunkCoverageState>;
  completedChunkGroups: Map<string, CompletedChunkGroupState>;
  orderedCompletedChunkGroups: CompletedChunkGroupState[];
  finalWindowCoverageById: Map<string, FinalWindowCoverageState>;
  readyChunkGroupIds: string[];
  readyChunkGroupSet: Set<string>;
  nextComparableWindowStartIndex: number;
  nextComparableWindowStartMs: number | null;
  expectedSubqueryIds: string[];
  outputAggregationFunction: AggregationFunction;
  chunksPerComparableWindow: number;
  chunkGroupsPerOutputStep: number;
  chunkWindowWidthMs: number;
  alignmentOriginMs: number | null;
  comparableOutputCadenceOnly: boolean;
};

export type FinalWindowCoverageSnapshot = {
  finalWindowStart: number;
  finalWindowEnd: number;
  expectedChunkKeys: string[];
  receivedChunkKeys: string[];
  missingChunkKeys: string[];
  duplicateChunkKeys: string[];
  coverageComplete: boolean;
  completionReason?: string;
};

export type FinalWindowCoverageState = FinalWindowCoverageSnapshot & {
  finalWindowId: string;
  emitted: boolean;
};
