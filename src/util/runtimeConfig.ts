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

export function getResultTopic(defaultTopic: string): string {
  return process.env.RESULT_TOPIC || defaultTopic;
}

export function getSessionId(): string {
  return process.env.SESSION_ID || Date.now().toString(36);
}

export function buildSubQuerySelectClause(
  aggregation: AggregationFunction,
  topicSuffix: string,
): string {
  const aggAlias = `?agg${topicSuffix}`;
  const countAlias = `?count${topicSuffix}`;

  if (aggregation === "COUNT") {
    return `(COUNT(?value) AS ${aggAlias})`;
  }

  return `(${aggregation}(?value) AS ${aggAlias}) (COUNT(?value) AS ${countAlias})`;
}

export function buildOutputSelectClause(
  aggregation: AggregationFunction,
): string {
  return `(${aggregation}(?value) AS ?resultValue)`;
}

export function buildBenchmarkResultPayload(
  approach: string,
  aggregation: AggregationFunction,
  sessionId: string,
  value: number,
  windowNumber: number,
) {
  return {
    approach,
    aggregationType: aggregation,
    sessionId,
    timestamp: Date.now(),
    value,
    windowNumber,
  };
}
