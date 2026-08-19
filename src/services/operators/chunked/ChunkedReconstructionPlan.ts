import {
  getBenchmarkEventTimeAnchor,
  getBenchmarkStartTime,
  getBenchmarkTargetWindowCount,
  getConfiguredWindowSemantics,
  getSessionId,
  getTimestampDomainMax,
  getTimestampDomainMin,
  useChunkedComparableOutputCadence,
  getChunkedUseImmediateTrigger,
  useCleanMqttSessionsForBenchmark,
} from "../../../util/runtimeConfig";
import type { ManagerOwnedProducerMapping } from "./types";

/** Immutable identity and semantics for one logical Chunked reconstruction. */
export type ChunkedReconstructionPlanConfig = Readonly<{
  planId: string;
  executionId: string;
  outputTopic: string;
  logRoot: string;
  sessionId: string;
  managerOwnedProducerMappings: ManagerOwnedProducerMapping[];
  skipLocalProducerSpawning: boolean;
  comparableOutputCadenceOnly: boolean;
  useImmediateTrigger: boolean;
  benchmarkEventTimeAnchor: number | null;
  benchmarkStartTime: number | null;
  benchmarkTargetWindowCount: number | null;
  timestampDomainMin: number | null;
  timestampDomainMax: number | null;
  windowSemantics: string;
  cleanMqttSessions: boolean;
  debugChunksEnabled: boolean;
  watermarkDebugEnabled: boolean;
}>;

/** Compatibility boundary for the legacy one-plan BeeWorker. */
export function createChunkedPlanConfigFromEnvironment(
  overrides: Partial<ChunkedReconstructionPlanConfig> = {},
): ChunkedReconstructionPlanConfig {
  const mappingsRaw = process.env.HIVE_PRODUCER_IDENTITY_MAPPINGS;
  const managerOwnedProducerMappings: ManagerOwnedProducerMapping[] = mappingsRaw ? JSON.parse(mappingsRaw) : [];
  const selectedMappings: ManagerOwnedProducerMapping[] = overrides.managerOwnedProducerMappings ?? managerOwnedProducerMappings;
  return Object.freeze({
    planId: overrides.planId ?? process.env.EXECUTION_ID ?? `chunked-${Date.now()}`,
    executionId: overrides.executionId ?? process.env.EXECUTION_ID ?? `chunked-${Date.now()}`,
    outputTopic: overrides.outputTopic ?? process.env.RESULT_TOPIC ?? "output",
    logRoot: overrides.logRoot ?? process.env.LOG_PATH ?? ".",
    sessionId: overrides.sessionId ?? getSessionId(),
    managerOwnedProducerMappings: Object.freeze(selectedMappings.map((mapping) => Object.freeze({ ...mapping }))) as unknown as ManagerOwnedProducerMapping[],
    skipLocalProducerSpawning: overrides.skipLocalProducerSpawning ?? process.env.HIVE_SKIP_CHUNK_PRODUCER_SPAWNING === "true",
    comparableOutputCadenceOnly: overrides.comparableOutputCadenceOnly ?? useChunkedComparableOutputCadence(),
    useImmediateTrigger: overrides.useImmediateTrigger ?? getChunkedUseImmediateTrigger(),
    benchmarkEventTimeAnchor: overrides.benchmarkEventTimeAnchor ?? getBenchmarkEventTimeAnchor(),
    benchmarkStartTime: overrides.benchmarkStartTime ?? getBenchmarkStartTime(),
    benchmarkTargetWindowCount: overrides.benchmarkTargetWindowCount ?? getBenchmarkTargetWindowCount(),
    timestampDomainMin: overrides.timestampDomainMin ?? getTimestampDomainMin(),
    timestampDomainMax: overrides.timestampDomainMax ?? getTimestampDomainMax(),
    windowSemantics: overrides.windowSemantics ?? getConfiguredWindowSemantics(),
    cleanMqttSessions: overrides.cleanMqttSessions ?? useCleanMqttSessionsForBenchmark(),
    debugChunksEnabled: overrides.debugChunksEnabled ?? process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1",
    watermarkDebugEnabled: overrides.watermarkDebugEnabled ?? process.env.HIVE_WATERMARK_DEBUG === "true",
  });
}

/** Mutable state deliberately scoped to one logical reconstruction plan. */
export class ChunkedPlanState {
  readonly latestWatermarkByProducer = new Map<string, number>();
  readonly acceptedContributions = new Set<string>();
  readonly firstObservedEventTimestampByTopic = new Map<string, number>();
  readonly lastObservedEventTimestampByTopic = new Map<string, number>();
  readonly chunkArrivalTimes = new Map<string, number>();
  readonly chunkWindowMap = new Map<string, { start: number; end: number }>();
  completed = false;
  failed = false;
}
