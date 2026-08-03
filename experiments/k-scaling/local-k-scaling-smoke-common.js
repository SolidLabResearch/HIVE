const fs = require("fs");
const path = require("path");
const {
  calculateRegistrationAnchoredLatencies,
  compareAggregateResultEquivalence,
  REGISTRATION_ANCHORED_LATENCY_SOURCE,
} = require("../../analysis/accuracy/accuracy-comparison-custom-patterns.js");

const OUTPUT_WINDOW_RANGE_MS = 120000;
const OUTPUT_WINDOW_STEP_MS = 60000;
const SUB_WINDOW_RANGE_MS = 60000;
const SUB_WINDOW_STEP_MS = 30000;
const TARGET_WINDOWS = 1;
const DEFAULT_K_VALUES = [1, 2, 4, 8, 32];
const DEFAULT_APPROACHES = ["fetching", "approximation", "chunked"];
const DEFAULT_ITERATIONS = 3;
const LATENCY_TOLERANCE_MS = 1;
const FLOAT_TOLERANCE = 1e-9;

const APPROACH_CONFIG = {
  fetching: {
    orchestrator: "dist/approaches/StreamingQueryFetchingKScalingOrchestrator.js",
    consumerLatencyFile: (index) => `fetching_latency_log_consumer_${index}.csv`,
    consumerDiagnosticsFile: (index) => `fetching_window_diagnostics_consumer_${index}.csv`,
  },
  approximation: {
    orchestrator: "dist/approaches/StreamingQueryApproximationKScalingOrchestrator.js",
    consumerLatencyFile: (index) => `approximation_latency_log_consumer_${index}.csv`,
    consumerDiagnosticsFile: () => null,
  },
  chunked: {
    orchestrator: "dist/approaches/StreamingQueryChunkedKScalingOrchestrator.js",
    consumerLatencyFile: (index) => `chunked_latency_log_consumer_${index}.csv`,
    consumerDiagnosticsFile: (index) => `chunked_window_diagnostics_consumer_${index}.csv`,
  },
};

function parseCsv(content) {
  const lines = String(content || "")
    .trim()
    .split(/\r?\n/)
    .filter(Boolean);
  if (lines.length < 2) {
    return [];
  }
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""]));
  });
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === "\"") {
      if (inQuotes && line[index + 1] === "\"") {
        current += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  values.push(current);
  return values;
}

function readCsvRows(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  return parseCsv(fs.readFileSync(filePath, "utf8"));
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toBoolean(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return null;
}

function median(values) {
  const finite = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (finite.length === 0) {
    return null;
  }
  const midpoint = Math.floor(finite.length / 2);
  return finite.length % 2 === 1
    ? finite[midpoint]
    : (finite[midpoint - 1] + finite[midpoint]) / 2;
}

function getApproachConfig(approach) {
  const config = APPROACH_CONFIG[approach];
  if (!config) {
    throw new Error(`Unsupported approach: ${approach}`);
  }
  return config;
}

function countConsumerLatencyFiles(logDir, approach, expectedK) {
  const config = getApproachConfig(approach);
  let existing = 0;
  const paths = [];
  for (let index = 1; index <= expectedK; index += 1) {
    const filePath = path.join(logDir, config.consumerLatencyFile(index));
    if (fs.existsSync(filePath)) {
      existing += 1;
      paths.push(filePath);
    }
  }
  return { existing, expected: expectedK, paths };
}

function listBenchmarkWindowSummaryPaths(logDir, approach, expectedK) {
  if (approach === "fetching" || approach === "chunked") {
    return [path.join(logDir, "benchmark_window_cap_summary.json")];
  }

  const paths = [];
  for (let index = 1; index <= expectedK; index += 1) {
    paths.push(
      path.join(logDir, `benchmark_window_cap_summary_consumer_${index}.json`),
    );
  }
  return paths;
}

function getExpectedBenchmarkSummaryCount(approach, expectedK) {
  return approach === "fetching" || approach === "chunked" ? 1 : expectedK;
}

function selectFirstCompleteRow(rows) {
  return rows.find((row) => (
    toBoolean(row.coverage_complete) === true
      && toBoolean(row.is_partial_window) === false
      && toBoolean(row.is_comparable_window) === true
  )) || null;
}

function extractRepresentativeWindow(logDir, approach, kValue, consumerIndex = 1) {
  const config = getApproachConfig(approach);
  const latencyPath = path.join(logDir, config.consumerLatencyFile(consumerIndex));
  const rows = readCsvRows(latencyPath);
  const row = selectFirstCompleteRow(rows);
  if (!row) {
    return {
      ok: false,
      latencyPath,
      reason: "missing first complete comparable latency row",
    };
  }

  const queryRegisteredAt = toNumber(row.query_registered_at);
  const resultEmittedAt = toNumber(row.result_emitted_at);
  const windowNumber = toNumber(row.window_number);
  const resultValue = toNumber(row.result_value);
  const outputWindowRangeMs =
    toNumber(row.window_duration_ms) ||
    OUTPUT_WINDOW_RANGE_MS;

  if (
    !Number.isFinite(queryRegisteredAt) ||
    !Number.isFinite(resultEmittedAt) ||
    !Number.isFinite(windowNumber) ||
    !Number.isFinite(resultValue)
  ) {
    return {
      ok: false,
      latencyPath,
      reason: "representative row missing numeric anchors",
    };
  }

  const latency = calculateRegistrationAnchoredLatencies({
    queryRegisteredAt,
    resultEmittedAt,
    windowNumber,
    outputWindowRangeMs: OUTPUT_WINDOW_RANGE_MS,
    outputWindowStepMs: OUTPUT_WINDOW_STEP_MS,
  });

  const expectedDifference = latency.queryToFirstResultMs - OUTPUT_WINDOW_RANGE_MS;
  const latencyConsistencyOk =
    Math.abs(expectedDifference - latency.postWindowCloseLatencyMs) <= LATENCY_TOLERANCE_MS;

  return {
    ok: latency.latencyMetricSource === REGISTRATION_ANCHORED_LATENCY_SOURCE && latencyConsistencyOk,
    approach,
    kValue,
    consumerIndex,
    latencyPath,
    windowNumber,
    windowStart: toNumber(row.window_start),
    windowEnd: toNumber(row.window_end),
    resultValue,
    queryRegisteredAt,
    resultEmittedAt,
    queryToFirstResultMs: latency.queryToFirstResultMs,
    postWindowCloseLatencyMs: latency.postWindowCloseLatencyMs,
    registrationAnchoredWindowCloseAt: latency.registrationAnchoredWindowCloseAt,
    latencyMetricSource: latency.latencyMetricSource,
    coverageComplete: true,
    row,
    reason: latencyConsistencyOk ? null : "registration anchored latency mismatch",
  };
}

function extractAllConsumerWindows(logDir, approach, kValue) {
  const consumers = [];
  for (let consumerIndex = 1; consumerIndex <= kValue; consumerIndex += 1) {
    consumers.push(
      extractRepresentativeWindow(logDir, approach, kValue, consumerIndex),
    );
  }

  return {
    ok: consumers.every((entry) => entry.ok),
    consumers,
  };
}

function readProcessTreeMetrics(logDir) {
  const csvPath = path.join(logDir, "process_tree_resource_usage.csv");
  const rows = readCsvRows(csvPath).map((row) => ({
    cpuSeconds: toNumber(row.tree_cpu_seconds),
    totalCpuPct: toNumber(row.total_cpu_pct),
    rssBytes: toNumber(row.tree_rss_bytes),
  }));
  const cpuSamples = rows.map((row) => row.totalCpuPct).filter(Number.isFinite);
  const peakRssMb = rows
    .map((row) => (Number.isFinite(row.rssBytes) ? row.rssBytes / (1024 * 1024) : null))
    .filter(Number.isFinite);
  const finalCpuSeconds = rows.length > 0 ? rows[rows.length - 1].cpuSeconds : null;

  return {
    csvPath,
    averageCpuPct: median(cpuSamples),
    peakRssMb: peakRssMb.length > 0 ? Math.max(...peakRssMb) : null,
    cpuSeconds: Number.isFinite(finalCpuSeconds) ? finalCpuSeconds : null,
    sampleCount: rows.length,
  };
}

function compareAgainstFetching(referenceValue, producedValue) {
  const comparison = compareAggregateResultEquivalence(producedValue, referenceValue);
  return {
    exactAgreement: comparison.exactAgreement,
    absoluteError: comparison.rawAbsoluteError,
    mae: comparison.rawAbsoluteError,
    maxAbsoluteError: comparison.rawAbsoluteError,
    comparisonTolerance: comparison.comparisonTolerance,
    comparisonMethod: comparison.comparisonMethod,
  };
}

function parseKScalingSelection(value, fallbackValues) {
  if (!value) {
    return [...fallbackValues];
  }
  return value
    .split(",")
    .map((entry) => Number.parseInt(entry.trim(), 10))
    .filter((entry) => Number.isFinite(entry) && entry > 0);
}

function parseApproachSelection(value, fallbackValues) {
  if (!value) {
    return [...fallbackValues];
  }
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function sanitizeTimestamp(date = new Date()) {
  return date.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

function buildCheckpointKey(approach, kValue, iteration) {
  return `${approach}-K${kValue}-iteration${iteration}`;
}

function buildCombinationMatrix({ approaches, kValues, iterations }) {
  const combinations = [];
  for (const approach of approaches) {
    for (const kValue of kValues) {
      for (let iteration = 1; iteration <= iterations; iteration += 1) {
        combinations.push({ approach, kValue, iteration });
      }
    }
  }
  return combinations;
}

module.exports = {
  APPROACH_CONFIG,
  DEFAULT_APPROACHES,
  DEFAULT_ITERATIONS,
  DEFAULT_K_VALUES,
  FLOAT_TOLERANCE,
  LATENCY_TOLERANCE_MS,
  OUTPUT_WINDOW_RANGE_MS,
  OUTPUT_WINDOW_STEP_MS,
  REGISTRATION_ANCHORED_LATENCY_SOURCE,
  SUB_WINDOW_RANGE_MS,
  SUB_WINDOW_STEP_MS,
  TARGET_WINDOWS,
  buildCheckpointKey,
  buildCombinationMatrix,
  compareAgainstFetching,
  countConsumerLatencyFiles,
  extractAllConsumerWindows,
  extractRepresentativeWindow,
  getExpectedBenchmarkSummaryCount,
  getApproachConfig,
  listBenchmarkWindowSummaryPaths,
  median,
  parseApproachSelection,
  parseCsv,
  parseKScalingSelection,
  readCsvRows,
  readJson,
  readProcessTreeMetrics,
  sanitizeTimestamp,
  selectFirstCompleteRow,
  toBoolean,
  toNumber,
};
