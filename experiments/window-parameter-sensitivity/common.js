const DEFAULT_APPROACHES = ["fetching", "chunked"];
const DEFAULT_PATTERNS = ["low_variability"];
const DEFAULT_AGGREGATION_FUNCTION = "AVG";
const DEFAULT_REPLAY_DURATION_SECONDS = 900;
const DEFAULT_SUPERQUERY_STEP_SECONDS = 60;
const DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS = 60;
const DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS = 30;
const DEFAULT_GRANULARITY_SUPERQUERY_RANGE_SECONDS = 120;
const DEFAULT_QUERY_TARGET_SCALING_SUPERQUERY_RANGE_SECONDS = 120;
const DEFAULT_QUERY_TARGET_SCALING_SUPERQUERY_STEP_SECONDS = 60;
const DEFAULT_QUERY_TARGET_SCALING_CHUNK_SIZE_SECONDS = 30;
const DEFAULT_QUERY_TARGET_SCALING_REAL_TARGET_COUNTS = [2];
const DEFAULT_QUERY_TARGET_SCALING_SYNTHETIC_TARGET_COUNTS = [2, 4, 6, 8];
const EXACT_FINAL_REUSE_DISABLED = false;
const DEFAULT_TIMESTAMP_DOMAIN_MIN = 1749592200000;
const DEFAULT_TIMESTAMP_DOMAIN_MAX = 1749592800000;
const DEFAULT_BENCHMARK_EVENT_TIME_ANCHOR = 1749592200000;
const DEFAULT_PAPER_TARGET_WINDOWS = 35;
const DEFAULT_PAPER_TRIMMED_WINDOW_START = 4;
const DEFAULT_PAPER_TRIMMED_WINDOW_END = 33;
const SYNTHETIC_TARGET_PREFIX = "syntheticTarget";

const REAL_QUERY_TARGET_DEFINITIONS = [
  {
    name: "wearableX",
    topicName: "wearableX",
    propertyName: "wearableX",
  },
  {
    name: "smartphoneX",
    topicName: "smartphoneX",
    propertyName: "smartphoneX",
  },
];

const EXPERIMENTS = {
  "superquery-range-scaling": {
    aliases: ["range", "superquery-range", "superquery-range-scaling"],
    scenarioFlag: "--ranges",
    scenarioLabelPrefix: "range",
    scenarioField: "superqueryRangeSeconds",
    defaultScenarios: [120, 180, 240, 300, 360, 420],
  },
  "chunk-granularity-sensitivity": {
    aliases: [
      "granularity",
      "chunk-granularity",
      "chunk-size",
      "chunk-granularity-sensitivity",
    ],
    scenarioFlag: "--chunk-sizes",
    scenarioLabelPrefix: "chunk",
    scenarioField: "chunkSizeSeconds",
    defaultScenarios: [1, 5, 15, 30, 60],
  },
  "query-target-scaling": {
    aliases: [
      "query-target-scaling",
      "different-target-same-window",
      "target-scaling",
      "targets",
    ],
    scenarioFlag: "--target-counts",
    scenarioLabelPrefix: "targets-k",
    scenarioField: "targetCount",
    defaultScenarios: [2],
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
  if (experimentName === "query-target-scaling") {
    return `${definition.scenarioLabelPrefix}${scenarioSeconds}`;
  }
  return `${definition.scenarioLabelPrefix}-${scenarioSeconds}s`;
}

function getQueryTargetScalingScenarioLabel(targetSource, targetCount) {
  return `${targetSource}-targets-k${targetCount}`;
}

function getRealQueryTargetDefinitions() {
  return REAL_QUERY_TARGET_DEFINITIONS.map((target) => ({ ...target }));
}

function getRealQueryTargetNames() {
  return getRealQueryTargetDefinitions().map((target) => target.name);
}

function buildSyntheticQueryTargetDefinitions(count) {
  const results = [];
  for (let index = 1; index <= count; index += 1) {
    const name = `${SYNTHETIC_TARGET_PREFIX}${index}`;
    results.push({
      name,
      topicName: name,
      propertyName: name,
    });
  }
  return results;
}

function findQueryTargetDefinition(name, availableTargets) {
  const normalized = String(name || "").trim().toLowerCase();
  return availableTargets.find(
    (target) => String(target.name || "").trim().toLowerCase() === normalized,
  );
}

function formatTargetSet(targetDefinitions) {
  return targetDefinitions.map((target) => target.name).join("|");
}

function formatTargetNames(targetDefinitions) {
  return targetDefinitions.map((target) => target.name).join(",");
}

function resolveQueryTargetDefinitions({
  requestedTargetNames = [],
  availableTargets = getRealQueryTargetDefinitions(),
} = {}) {
  const requested = requestedTargetNames.length > 0
    ? requestedTargetNames
    : availableTargets.map((target) => target.name);
  const resolved = [];
  const seen = new Set();

  for (const name of requested) {
    const definition = findQueryTargetDefinition(name, availableTargets);
    if (!definition) {
      throw new Error(
        `Unknown query target "${name}". Available real targets: ${availableTargets
          .map((target) => target.name)
          .join(", ")}`,
      );
    }

    if (seen.has(definition.name)) {
      continue;
    }
    seen.add(definition.name);
    resolved.push({ ...definition });
  }

  return resolved;
}

function normalizeTargetSource(value) {
  return String(value || "real").trim().toLowerCase() === "synthetic"
    ? "synthetic"
    : "real";
}

function resolveQueryTargetScalingScenarioDefinitions({
  targetSource = "real",
  targetCounts = [],
  requestedTargetNames = [],
  availableTargets = getRealQueryTargetDefinitions(),
} = {}) {
  const normalizedTargetSource = normalizeTargetSource(targetSource);
  const counts = targetCounts.length > 0
    ? targetCounts
    : normalizedTargetSource === "synthetic"
      ? [...DEFAULT_QUERY_TARGET_SCALING_SYNTHETIC_TARGET_COUNTS]
      : [...DEFAULT_QUERY_TARGET_SCALING_REAL_TARGET_COUNTS];
  const scenarios = [];

  if (normalizedTargetSource === "synthetic") {
    for (const targetCount of counts) {
      if (!Number.isFinite(targetCount) || targetCount <= 0) {
        continue;
      }
      const targetDefinitions = buildSyntheticQueryTargetDefinitions(targetCount);
      scenarios.push({
        targetSource: normalizedTargetSource,
        targetCount,
        targetDefinitions,
        scenarioLabel: getQueryTargetScalingScenarioLabel(
          normalizedTargetSource,
          targetCount,
        ),
        targetSet: formatTargetSet(targetDefinitions),
        targetNames: formatTargetNames(targetDefinitions),
        isSynthetic: true,
      });
    }
    return scenarios;
  }

  const resolvedTargets = resolveQueryTargetDefinitions({
    requestedTargetNames,
    availableTargets,
  });
  for (const targetCount of counts) {
    if (!Number.isFinite(targetCount) || targetCount <= 0) {
      continue;
    }
    if (targetCount > resolvedTargets.length) {
      throw new Error(
        `query-target-scaling requested K=${targetCount}, but only ${resolvedTargets.length} real targets are available (${resolvedTargets
          .map((target) => target.name)
          .join(", ")}). K=4 remains TODO until four real targets are configured.`,
      );
    }

    const targetDefinitions = resolvedTargets.slice(0, targetCount);
    scenarios.push({
      targetSource: normalizedTargetSource,
      targetCount,
      targetDefinitions,
      scenarioLabel: getQueryTargetScalingScenarioLabel(
        normalizedTargetSource,
        targetCount,
      ),
      targetSet: formatTargetSet(targetDefinitions),
      targetNames: formatTargetNames(targetDefinitions),
      isSynthetic: false,
    });
  }

  return scenarios;
}

function buildTargetMetadata(targetDefinitions, targetSource) {
  const normalizedTargetSource = normalizeTargetSource(targetSource);
  const targetCount = targetDefinitions.length;
  return {
    target_source: normalizedTargetSource,
    unique_target_count: targetCount,
    real_target_count: normalizedTargetSource === "real" ? targetCount : 0,
    synthetic_target_count: normalizedTargetSource === "synthetic" ? targetCount : 0,
    target_count: targetCount,
    target_set: formatTargetSet(targetDefinitions),
    target_names: formatTargetNames(targetDefinitions),
    is_synthetic_target_scaling: normalizedTargetSource === "synthetic",
  };
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
      target_window_count: DEFAULT_PAPER_TARGET_WINDOWS,
      trimmed_window_start: DEFAULT_PAPER_TRIMMED_WINDOW_START,
      trimmed_window_end: DEFAULT_PAPER_TRIMMED_WINDOW_END,
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
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(
        DEFAULT_PAPER_TARGET_WINDOWS,
      ),
      STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD: "1",
      STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
      STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
      STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "0",
      STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
      STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    },
  };
}

function buildQueryTargetScalingScenarioConfig({
  targetDefinitions,
  targetSource = "real",
  aggregationFunction = DEFAULT_AGGREGATION_FUNCTION,
  pattern,
  iteration,
  replayDurationSeconds = DEFAULT_REPLAY_DURATION_SECONDS,
  superqueryRangeSeconds = DEFAULT_QUERY_TARGET_SCALING_SUPERQUERY_RANGE_SECONDS,
  superqueryStepSeconds = DEFAULT_QUERY_TARGET_SCALING_SUPERQUERY_STEP_SECONDS,
  chunkSizeSeconds = DEFAULT_QUERY_TARGET_SCALING_CHUNK_SIZE_SECONDS,
  exactFinalReuseEnabled = EXACT_FINAL_REUSE_DISABLED,
}) {
  const normalizedAggregation = String(aggregationFunction || DEFAULT_AGGREGATION_FUNCTION)
    .trim()
    .toUpperCase();
  const normalizedTargets = Array.isArray(targetDefinitions)
    ? targetDefinitions.map((target) => ({ ...target }))
    : [];

  if (normalizedTargets.length <= 0) {
    throw new Error("query-target-scaling requires at least one target definition");
  }

  const targetMetadata = buildTargetMetadata(normalizedTargets, targetSource);

  return {
    experimentName: "query-target-scaling",
    scenarioSeconds: normalizedTargets.length,
    scenarioLabel: getQueryTargetScalingScenarioLabel(
      targetMetadata.target_source,
      normalizedTargets.length,
    ),
    aggregationFunction: normalizedAggregation,
    metadata: {
      experiment_name: "query-target-scaling",
      pattern,
      iteration,
      aggregation_function: normalizedAggregation,
      superquery_range_seconds: superqueryRangeSeconds,
      superquery_step_seconds: superqueryStepSeconds,
      chunk_size_seconds: chunkSizeSeconds,
      replay_duration_seconds: replayDurationSeconds,
      exact_final_reuse_enabled: exactFinalReuseEnabled,
      target_window_count: DEFAULT_PAPER_TARGET_WINDOWS,
      trimmed_window_start: DEFAULT_PAPER_TRIMMED_WINDOW_START,
      trimmed_window_end: DEFAULT_PAPER_TRIMMED_WINDOW_END,
      subquery_window_range_seconds: chunkSizeSeconds,
      subquery_window_step_seconds: chunkSizeSeconds,
      ...targetMetadata,
      expected_chunk_states_per_result: computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds,
        chunkSizeSeconds,
        streamCount: normalizedTargets.length,
      }),
    },
    env: {
      AGGREGATION_FUNCTION: normalizedAggregation,
      AGGREGATION_FUNC: normalizedAggregation,
      OUTPUT_WINDOW_RANGE: String(superqueryRangeSeconds * 1000),
      OUTPUT_WINDOW_STEP: String(superqueryStepSeconds * 1000),
      SUB_WINDOW_RANGE: String(chunkSizeSeconds * 1000),
      SUB_WINDOW_STEP: String(chunkSizeSeconds * 1000),
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
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(
        DEFAULT_PAPER_TARGET_WINDOWS,
      ),
      STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD: "1",
      STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
      STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
      STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "0",
      STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
      STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
      BENCHMARK_QUERY_TARGET_NAMES: formatTargetNames(normalizedTargets),
      BENCHMARK_QUERY_TARGET_SET: formatTargetSet(normalizedTargets),
      BENCHMARK_QUERY_TARGET_COUNT: String(normalizedTargets.length),
      BENCHMARK_TARGET_COUNT: String(normalizedTargets.length),
      BENCHMARK_TARGET_NAMES: formatTargetNames(normalizedTargets),
      BENCHMARK_TARGET_SOURCE: targetMetadata.target_source,
      BENCHMARK_QUERY_TARGETS_SYNTHETIC: targetMetadata.is_synthetic_target_scaling
        ? "true"
        : "false",
    },
  };
}

module.exports = {
  DEFAULT_AGGREGATION_FUNCTION,
  DEFAULT_APPROACHES,
  DEFAULT_BENCHMARK_EVENT_TIME_ANCHOR,
  DEFAULT_GRANULARITY_SUPERQUERY_RANGE_SECONDS,
  DEFAULT_PATTERNS,
  DEFAULT_QUERY_TARGET_SCALING_CHUNK_SIZE_SECONDS,
  DEFAULT_QUERY_TARGET_SCALING_REAL_TARGET_COUNTS,
  DEFAULT_QUERY_TARGET_SCALING_SUPERQUERY_RANGE_SECONDS,
  DEFAULT_QUERY_TARGET_SCALING_SUPERQUERY_STEP_SECONDS,
  DEFAULT_QUERY_TARGET_SCALING_SYNTHETIC_TARGET_COUNTS,
  DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS,
  DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS,
  DEFAULT_REPLAY_DURATION_SECONDS,
  DEFAULT_SUPERQUERY_STEP_SECONDS,
  DEFAULT_TIMESTAMP_DOMAIN_MAX,
  DEFAULT_TIMESTAMP_DOMAIN_MIN,
  EXACT_FINAL_REUSE_DISABLED,
  EXPERIMENTS,
  buildQueryTargetScalingScenarioConfig,
  buildScenarioConfig,
  buildSyntheticQueryTargetDefinitions,
  buildTargetMetadata,
  computeExpectedChunkStatesPerResult,
  formatTargetNames,
  formatTargetSet,
  gcdList,
  getQueryTargetScalingScenarioLabel,
  getRealQueryTargetDefinitions,
  getRealQueryTargetNames,
  getScenarioLabel,
  normalizeExperimentName,
  normalizeTargetSource,
  parseCsvList,
  parsePositiveIntList,
  resolveQueryTargetDefinitions,
  resolveQueryTargetScalingScenarioDefinitions,
};
