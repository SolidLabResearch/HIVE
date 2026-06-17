const DEFAULT_APPROACHES = ["fetching", "chunked"];
const DEFAULT_PATTERNS = ["low_variability"];
const DEFAULT_AGGREGATION_FUNCTION = "AVG";
const DEFAULT_REPLAY_DURATION_SECONDS = 900;
const DEFAULT_SUPERQUERY_STEP_SECONDS = 60;
const DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS = 60;
const DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS = 30;
const DEFAULT_GRANULARITY_SUPERQUERY_RANGE_SECONDS = 120;
const EXACT_FINAL_REUSE_DISABLED = false;
const DEFAULT_TIMESTAMP_DOMAIN_MIN = 1749592200000;
const DEFAULT_TIMESTAMP_DOMAIN_MAX = 1749592800000;
const DEFAULT_BENCHMARK_EVENT_TIME_ANCHOR = 1749592200000;

const EXPERIMENTS = {
  "superquery-range-scaling": {
    aliases: ["range", "superquery-range", "superquery-range-scaling"],
    scenarioFlag: "--ranges",
    scenarioLabelPrefix: "range",
    scenarioField: "superqueryRangeSeconds",
    defaultScenarios: [120, 180, 240, 300, 360, 420],
  },
  "chunk-granularity-sensitivity": {
    aliases: ["granularity", "chunk-granularity", "chunk-size", "chunk-granularity-sensitivity"],
    scenarioFlag: "--chunk-sizes",
    scenarioLabelPrefix: "chunk",
    scenarioField: "chunkSizeSeconds",
    defaultScenarios: [1, 5, 15, 30, 60],
  },
};

function normalizeExperimentName(raw) {
  const normalized = String(raw || "").trim().toLowerCase();
  for (const [experimentName, definition] of Object.entries(EXPERIMENTS)) {
    if (normalized === experimentName || definition.aliases.includes(normalized)) {
      return experimentName;
    }
  }
  throw new Error(
    `Unknown experiment "${raw}". Expected one of: ${Object.keys(EXPERIMENTS).join(", ")}`,
  );
}

function parseCsvList(value) {
  return String(value || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parsePositiveIntList(value) {
  return parseCsvList(value)
    .map((entry) => Number.parseInt(entry, 10))
    .filter((entry) => Number.isFinite(entry) && entry > 0);
}

function gcd(a, b) {
  const left = Math.abs(Number(a) || 0);
  const right = Math.abs(Number(b) || 0);
  if (left === 0) return right;
  if (right === 0) return left;
  return gcd(right, left % right);
}

function gcdList(values) {
  const filtered = values.filter((value) => Number.isFinite(value) && value > 0);
  if (filtered.length === 0) {
    return 1;
  }
  return filtered.reduce((acc, value) => gcd(acc, value), filtered[0]);
}

function toBooleanString(value) {
  return value ? "true" : "false";
}

function getScenarioLabel(experimentName, scenarioSeconds) {
  const definition = EXPERIMENTS[experimentName];
  if (!definition) {
    throw new Error(`Unknown experiment "${experimentName}"`);
  }
  return `${definition.scenarioLabelPrefix}-${scenarioSeconds}s`;
}

function computeExpectedChunkStatesPerResult({
  superqueryRangeSeconds,
  chunkSizeSeconds,
  streamCount = 2,
}) {
  if (
    !Number.isFinite(superqueryRangeSeconds) ||
    superqueryRangeSeconds <= 0 ||
    !Number.isFinite(chunkSizeSeconds) ||
    chunkSizeSeconds <= 0 ||
    !Number.isFinite(streamCount) ||
    streamCount <= 0
  ) {
    return null;
  }

  return Math.ceil(superqueryRangeSeconds / chunkSizeSeconds) * streamCount;
}

function buildScenarioConfig({
  experimentName,
  scenarioSeconds,
  aggregationFunction = DEFAULT_AGGREGATION_FUNCTION,
  pattern,
  iteration,
  replayDurationSeconds = DEFAULT_REPLAY_DURATION_SECONDS,
  superqueryStepSeconds = DEFAULT_SUPERQUERY_STEP_SECONDS,
  rangeScalingSubWindowRangeSeconds = DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS,
  rangeScalingSubWindowStepSeconds = DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS,
  granularitySuperqueryRangeSeconds = DEFAULT_GRANULARITY_SUPERQUERY_RANGE_SECONDS,
  exactFinalReuseEnabled = EXACT_FINAL_REUSE_DISABLED,
}) {
  const normalizedExperimentName = normalizeExperimentName(experimentName);
  const scenarioLabel = getScenarioLabel(normalizedExperimentName, scenarioSeconds);
  const normalizedAggregation = String(aggregationFunction || DEFAULT_AGGREGATION_FUNCTION)
    .trim()
    .toUpperCase();

  let outputWindowRangeSeconds;
  let outputWindowStepSeconds = superqueryStepSeconds;
  let subWindowRangeSeconds;
  let subWindowStepSeconds;

  if (normalizedExperimentName === "superquery-range-scaling") {
    outputWindowRangeSeconds = scenarioSeconds;
    subWindowRangeSeconds = rangeScalingSubWindowRangeSeconds;
    subWindowStepSeconds = rangeScalingSubWindowStepSeconds;
  } else if (normalizedExperimentName === "chunk-granularity-sensitivity") {
    outputWindowRangeSeconds = granularitySuperqueryRangeSeconds;
    subWindowRangeSeconds = scenarioSeconds;
    subWindowStepSeconds = scenarioSeconds;
  } else {
    throw new Error(`Unsupported experiment "${normalizedExperimentName}"`);
  }

  const chunkSizeSeconds = gcdList([
    outputWindowRangeSeconds,
    outputWindowStepSeconds,
    subWindowRangeSeconds,
    subWindowStepSeconds,
  ]);

  return {
    experimentName: normalizedExperimentName,
    scenarioSeconds,
    scenarioLabel,
    aggregationFunction: normalizedAggregation,
    metadata: {
      experiment_name: normalizedExperimentName,
      pattern,
      iteration,
      aggregation_function: normalizedAggregation,
      superquery_range_seconds: outputWindowRangeSeconds,
      superquery_step_seconds: outputWindowStepSeconds,
      chunk_size_seconds: chunkSizeSeconds,
      replay_duration_seconds: replayDurationSeconds,
      exact_final_reuse_enabled: exactFinalReuseEnabled,
      subquery_window_range_seconds: subWindowRangeSeconds,
      subquery_window_step_seconds: subWindowStepSeconds,
      expected_chunk_states_per_result: computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: outputWindowRangeSeconds,
        chunkSizeSeconds,
      }),
    },
    env: {
      AGGREGATION_FUNCTION: normalizedAggregation,
      AGGREGATION_FUNC: normalizedAggregation,
      OUTPUT_WINDOW_RANGE: String(outputWindowRangeSeconds * 1000),
      OUTPUT_WINDOW_STEP: String(outputWindowStepSeconds * 1000),
      SUB_WINDOW_RANGE: String(subWindowRangeSeconds * 1000),
      SUB_WINDOW_STEP: String(subWindowStepSeconds * 1000),
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "true",
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(
        replayDurationSeconds,
      ),
      HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE: toBooleanString(exactFinalReuseEnabled),
      K_SCALING_REUSE_MODE: "chunk-state",
      STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(DEFAULT_TIMESTAMP_DOMAIN_MIN),
      STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(DEFAULT_TIMESTAMP_DOMAIN_MAX),
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(
        DEFAULT_BENCHMARK_EVENT_TIME_ANCHOR,
      ),
      STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: String(
        DEFAULT_BENCHMARK_EVENT_TIME_ANCHOR,
      ),
    },
  };
}

module.exports = {
  DEFAULT_AGGREGATION_FUNCTION,
  DEFAULT_APPROACHES,
  DEFAULT_BENCHMARK_EVENT_TIME_ANCHOR,
  DEFAULT_GRANULARITY_SUPERQUERY_RANGE_SECONDS,
  DEFAULT_PATTERNS,
  DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS,
  DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS,
  DEFAULT_REPLAY_DURATION_SECONDS,
  DEFAULT_SUPERQUERY_STEP_SECONDS,
  DEFAULT_TIMESTAMP_DOMAIN_MAX,
  DEFAULT_TIMESTAMP_DOMAIN_MIN,
  EXACT_FINAL_REUSE_DISABLED,
  EXPERIMENTS,
  buildScenarioConfig,
  computeExpectedChunkStatesPerResult,
  gcdList,
  getScenarioLabel,
  normalizeExperimentName,
  parseCsvList,
  parsePositiveIntList,
};
