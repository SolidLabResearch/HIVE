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

export function getChunkedUseImmediateTrigger(): boolean {
  const comparableOutputOnly = useChunkedComparableOutputCadence();
  return parseBooleanFlag(
    process.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER,
    comparableOutputOnly ? false : DEFAULT_CHUNKED_USE_IMMEDIATE_TRIGGER,
  );
}

export function getResultTopic(defaultTopic: string): string {
  return process.env.RESULT_TOPIC || defaultTopic;
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

  if (aggregation === "COUNT") {
    return `(COUNT(?value) AS ${aggAlias}) (COUNT(?value) AS ${countAlias}) (SUM(?value) AS ${sumAlias}) (AVG(?value) AS ${avgAlias}) (MIN(?value) AS ${minAlias}) (MAX(?value) AS ${maxAlias})`;
  }

  return `(${aggregation}(?value) AS ${aggAlias}) (COUNT(?value) AS ${countAlias}) (SUM(?value) AS ${sumAlias}) (AVG(?value) AS ${avgAlias}) (MIN(?value) AS ${minAlias}) (MAX(?value) AS ${maxAlias})`;
}

export function buildOutputSelectClause(
  aggregation: AggregationFunction,
): string {
  return `(${aggregation}(?value) AS ?resultValue) (COUNT(?value) AS ?eventCount) (SUM(?value) AS ?sumValue) (AVG(?value) AS ?avgValue) (MIN(?ts) AS ?firstEventTimestamp) (MAX(?ts) AS ?lastEventTimestamp)`;
}

export function buildBenchmarkResultPayload(
  approach: string,
  aggregation: AggregationFunction,
  sessionId: string,
  value: number,
  windowNumber: number,
  diagnostics: Record<string, unknown> = {},
) {
  return {
    approach,
    aggregationType: aggregation,
    sessionId,
    timestamp: Date.now(),
    value,
    windowNumber,
    ...diagnostics,
  };
}
