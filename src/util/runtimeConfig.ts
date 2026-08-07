export const ALLOWED_AGGREGATIONS = [
  "AVG",
  "SUM",
  "COUNT",
  "MIN",
  "MAX",
] as const;

export type AggregationFunction = (typeof ALLOWED_AGGREGATIONS)[number];

const DEFAULT_OUTPUT_WINDOW_RANGE = 120000;
const DEFAULT_OUTPUT_WINDOW_STEP = 60000;
const DEFAULT_SUB_WINDOW_RANGE = 60000;
const DEFAULT_SUB_WINDOW_STEP = 30000;
const DEFAULT_CHUNKED_USE_IMMEDIATE_TRIGGER = true;
const DEFAULT_CHUNKED_CADENCE_ONLY = false;
const DEFAULT_APPROXIMATION_COMPLETED_WINDOW_MODE = true;
const DEFAULT_APPROXIMATION_EARLY_TRIGGER_MODE = false;
const DEFAULT_K_SCALING_REUSE_MODE = "chunk-state";

export type KScalingReuseMode = "chunk-state" | "exact-final";
export type WindowMetadataSource = "direct" | "reconstructed";

export type BenchmarkWindowMetadata = {
  windowSemantics: string;
  logicalTriggerTime: number | null;
  windowStart: number | null;
  windowEnd: number | null;
  windowDataCloseTime: number | null;
  resultEmittedAt: number | null;
  latencyFromLogicalTriggerMs: number | null;
  latencyFromWindowCloseMs: number | null;
  metadataSource: WindowMetadataSource;
};

function parseNumericOrNull(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseBooleanFlag(
  value: string | undefined,
  fallback: boolean,
): boolean {
  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parsePositiveIntOrNull(value: string | undefined): number | null {
  if (value === undefined || value.trim() === "") {
    return null;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

export function getConfiguredAggregation(): AggregationFunction {
  const configuredAgg = (
    process.env.AGGREGATION_FUNCTION ||
    process.env.AGGREGATION_FUNC ||
    "AVG"
  ).toUpperCase();

  return ALLOWED_AGGREGATIONS.includes(
    configuredAgg as AggregationFunction,
  )
    ? (configuredAgg as AggregationFunction)
    : "AVG";
}

export function isApproximationDebugEnabled(): boolean {
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS,
    false,
  );
}

export function useCompactReusableResultPayload(): boolean {
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD,
    false,
  );
}

export function getOutputWindowRange(): number {
  return parsePositiveInt(
    process.env.OUTPUT_WINDOW_RANGE,
    DEFAULT_OUTPUT_WINDOW_RANGE,
  );
}

export function getOutputWindowStep(): number {
  return parsePositiveInt(
    process.env.OUTPUT_WINDOW_STEP,
    DEFAULT_OUTPUT_WINDOW_STEP,
  );
}

export function getSubWindowRange(): number {
  return parsePositiveInt(
    process.env.SUB_WINDOW_RANGE,
    DEFAULT_SUB_WINDOW_RANGE,
  );
}

export function getSubWindowStep(): number {
  return parsePositiveInt(
    process.env.SUB_WINDOW_STEP,
    DEFAULT_SUB_WINDOW_STEP,
  );
}

export function useChunkedComparableOutputCadence(): boolean {
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY,
    false,
  );
}

export function getChunkedCadenceOnly(): boolean {
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY,
    DEFAULT_CHUNKED_CADENCE_ONLY,
  );
}

export function getChunkedUseImmediateTrigger(): boolean {
  const comparableOutputOnly = useChunkedComparableOutputCadence();
  const cadenceOnly = getChunkedCadenceOnly();
  if (cadenceOnly) {
    return false;
  }
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER,
    comparableOutputOnly ? false : DEFAULT_CHUNKED_USE_IMMEDIATE_TRIGGER,
  );
}

export function getApproximationEarlyTriggerMode(): boolean {
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE,
    DEFAULT_APPROXIMATION_EARLY_TRIGGER_MODE,
  );
}

export function getApproximationCompletedWindowMode(): boolean {
  if (
    process.env.STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE !==
    undefined
  ) {
    return parseBooleanFlag(
      process.env.STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE,
      DEFAULT_APPROXIMATION_COMPLETED_WINDOW_MODE,
    );
  }

  if (getApproximationEarlyTriggerMode()) {
    return false;
  }

  return DEFAULT_APPROXIMATION_COMPLETED_WINDOW_MODE;
}

export function getResultTopic(defaultTopic: string): string {
  return process.env.RESULT_TOPIC || defaultTopic;
}

export function isExactFinalResultReuseEnabled(): boolean {
  return parseBooleanFlag(
    process.env.HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE,
    false,
  );
}

export function getKScalingReuseMode(): KScalingReuseMode {
  const raw = (process.env.K_SCALING_REUSE_MODE || DEFAULT_K_SCALING_REUSE_MODE)
    .trim()
    .toLowerCase();

  return raw === "exact-final" ? "exact-final" : "chunk-state";
}

export function getSessionId(): string {
  return process.env.SESSION_ID || Date.now().toString(36);
}

export function getBenchmarkEventTimeAnchor(): number | null {
  const raw = process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
  if (raw === undefined || raw.trim() === "") {
    return null;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getBenchmarkStartTime(): number | null {
  const raw = process.env.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME;
  if (raw === undefined || raw.trim() === "") {
    return null;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getBenchmarkTargetWindowCount(): number | null {
  return parsePositiveIntOrNull(
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS,
  );
}

export function getBenchmarkTopicPrefix(): string {
  return process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || "";
}

export function buildBenchmarkTopicName(baseTopic: string): string {
  const prefix = getBenchmarkTopicPrefix().trim().replace(/^\/+|\/+$/g, "");
  if (!prefix) {
    return baseTopic;
  }
  return `${prefix}/${baseTopic}`;
}

export function buildBenchmarkStreamIri(baseTopic: string): string {
  return `mqtt://localhost:1883/${buildBenchmarkTopicName(baseTopic)}`;
}

export function useCleanMqttSessionsForBenchmark(): boolean {
  return getBenchmarkTopicPrefix() !== "" || getBenchmarkEventTimeAnchor() !== null;
}

export function getTimestampDomainMin(): number | null {
  const raw = process.env.STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN;
  const parsed = Number.parseInt(raw || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getTimestampDomainMax(): number | null {
  const raw = process.env.STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX;
  const parsed = Number.parseInt(raw || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getConfiguredWindowSemantics(): string {
  return (process.env.RSP_WINDOW_SEMANTICS || "trailing").trim().toLowerCase() || "trailing";
}

export function buildBenchmarkWindowMetadata(
  metadata: Partial<BenchmarkWindowMetadata> & {
    windowSemantics?: string | null;
    logicalTriggerTime?: number | null;
    windowStart?: number | null;
    windowEnd?: number | null;
    windowDataCloseTime?: number | null;
    resultEmittedAt?: number | null;
    latencyFromLogicalTriggerMs?: number | null;
    latencyFromWindowCloseMs?: number | null;
    metadataSource?: WindowMetadataSource | null;
  } = {},
): BenchmarkWindowMetadata {
  const windowSemantics =
    metadata.windowSemantics?.trim?.() || getConfiguredWindowSemantics();
  const logicalTriggerTime = parseNumericOrNull(metadata.logicalTriggerTime);
  const windowStart = parseNumericOrNull(metadata.windowStart);
  const windowEnd = parseNumericOrNull(metadata.windowEnd);
  const windowDataCloseTime = parseNumericOrNull(
    metadata.windowDataCloseTime ?? windowEnd,
  );
  const resultEmittedAt = parseNumericOrNull(metadata.resultEmittedAt);
  const latencyFromLogicalTriggerMs = parseNumericOrNull(
    metadata.latencyFromLogicalTriggerMs ??
      (resultEmittedAt !== null && logicalTriggerTime !== null
        ? resultEmittedAt - logicalTriggerTime
        : null),
  );
  const latencyFromWindowCloseMs = parseNumericOrNull(
    metadata.latencyFromWindowCloseMs ??
      (resultEmittedAt !== null && windowDataCloseTime !== null
        ? resultEmittedAt - windowDataCloseTime
        : null),
  );

  return {
    windowSemantics,
    logicalTriggerTime,
    windowStart,
    windowEnd,
    windowDataCloseTime,
    resultEmittedAt,
    latencyFromLogicalTriggerMs,
    latencyFromWindowCloseMs,
    metadataSource: metadata.metadataSource ?? "reconstructed",
  };
}

export function buildSubQuerySelectClause(
  aggregation: AggregationFunction,
  topicSuffix: string,
): string {
  const aggAlias = `?agg${topicSuffix}`;
  const countAlias = `?count${topicSuffix}`;
  const sumAlias = `?sum${topicSuffix}`;
  const avgAlias = `?avg${topicSuffix}`;
  const minAlias = `?min${topicSuffix}`;
  const maxAlias = `?max${topicSuffix}`;

  const projections: string[] = [];
  projections.push(`(${aggregation}(?value) AS ${aggAlias})`);
  if (aggregation !== "COUNT") {
    projections.push(`(COUNT(?value) AS ${countAlias})`);
  }
  if (aggregation !== "SUM") {
    projections.push(`(SUM(?value) AS ${sumAlias})`);
  }
  if (aggregation !== "AVG") {
    projections.push(`(AVG(?value) AS ${avgAlias})`);
  }
  if (aggregation !== "MIN") {
    projections.push(`(MIN(?value) AS ${minAlias})`);
  }
  if (aggregation !== "MAX") {
    projections.push(`(MAX(?value) AS ${maxAlias})`);
  }
  return projections.join(" ");
}

export function buildOutputSelectClause(
  aggregation: AggregationFunction,
): string {
  const projections: string[] = [];
  projections.push(`(${aggregation}(?value) AS ?resultValue)`);
  if (aggregation !== "COUNT") {
    projections.push(`(COUNT(?value) AS ?eventCount)`);
  }
  if (aggregation !== "SUM") {
    projections.push(`(SUM(?value) AS ?sumValue)`);
  }
  if (aggregation !== "AVG") {
    projections.push(`(AVG(?value) AS ?avgValue)`);
  }
  projections.push(`(MIN(?ts) AS ?firstEventTimestamp)`);
  projections.push(`(MAX(?ts) AS ?lastEventTimestamp)`);
  return projections.join(" ");
}

export function buildBenchmarkResultPayload(
  approach: string,
  aggregation: AggregationFunction,
  sessionId: string,
  value: number,
  windowNumber: number,
  diagnostics: Record<string, unknown> = {},
  metadata: Record<string, unknown> = {},
) {
  return {
    approach,
    aggregationType: aggregation,
    sessionId,
    timestamp: Date.now(),
    value,
    windowNumber,
    ...diagnostics,
    ...metadata,
  };
}
