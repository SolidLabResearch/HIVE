#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const {
  cleanupStaleBenchmarkProcesses,
  delay,
  terminateChildProcessTree,
} = require("../../experiments/utils/processCleanup");
const { createBenchmarkReplayRunEnv } = require("../../experiments/utils/benchmarkReplayEnv");
const { finalizeMqttTrafficArtifacts } = require("../../dist/util/mqttTraffic");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const SCENARIO = "same_query_different_windows";
const DEFAULT_SCALES = [2, 4, 6, 8, 10];
const DEFAULT_APPROACHES = ["fetching", "naive_distributed", "approximation", "chunked"];
const DEFAULT_PATTERN = "low_variability";
const DEFAULT_ITERATIONS = 1;
const DEFAULT_FREQUENCY = 4;
const DEFAULT_REPLAY_DURATION_SECONDS = 210;
const SMOKE_REPLAY_DURATION_SECONDS = 150;
const RESULT_EPSILON = 1e-9;

const SCENARIO_CONFIG = {
  same_query_different_windows: {
    name: "same_query_different_windows",
    outputWindowRangeMs: 120000,
    outputWindowStepMs: 60000,
    reusableQueryWindows: [
      { id: "Q1", rangeMs: 30000, stepMs: 15000 },
      { id: "Q2", rangeMs: 45000, stepMs: 15000 },
      { id: "Q3", rangeMs: 60000, stepMs: 30000 },
      { id: "Q4", rangeMs: 75000, stepMs: 15000 },
      { id: "Q5", rangeMs: 90000, stepMs: 30000 },
      { id: "Q6", rangeMs: 105000, stepMs: 15000 },
      { id: "Q7", rangeMs: 120000, stepMs: 30000 },
      { id: "Q8", rangeMs: 135000, stepMs: 15000 },
      { id: "Q9", rangeMs: 150000, stepMs: 30000 },
      { id: "Q10", rangeMs: 180000, stepMs: 60000 },
    ],
  },
};

const APPROACHS = {
  fetching: {
    label: "fetching",
    orchestrator: "dist/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator.js",
    resultFile: "fetching_results.csv",
    latencyFile: "fetching_latency_log.csv",
    resourceFile: "fetching_client_side_resource_usage.csv",
    extraFiles: ["fetching_client_side_log.csv", "fetching_window_diagnostics.csv", "fetching_orchestrator.log", "replayer-log.csv"],
  },
  naive_distributed: {
    label: "naive_distributed",
    orchestrator: "dist/approaches/ScalabilitySameQueryDifferentWindowsNaiveDistributedOrchestrator.js",
    resultFile: "naive_distributed_results.csv",
    latencyFile: "naive_distributed_latency_log.csv",
    resourceFile: "naive_distributed_approach_resource_usage.csv",
    extraFiles: ["naive_distributed_approach_log.csv", "naive_distributed_orchestrator.log", "replayer-log.csv"],
  },
  approximation: {
    label: "approximation",
    orchestrator: "dist/approaches/ScalabilitySameQueryDifferentWindowsApproximationOrchestrator.js",
    resultFile: "approximation_results.csv",
    latencyFile: "approximation_latency_log.csv",
    resourceFile: "approximation_approach_resource_usage.csv",
    extraFiles: ["approximation_approach_log.csv", "approximation_orchestrator.log", "replayer-log.csv"],
  },
  chunked: {
    label: "chunked",
    orchestrator: "dist/approaches/ScalabilitySameQueryDifferentWindowsChunkedOrchestrator.js",
    resultFile: "chunked_results.csv",
    latencyFile: "chunked_latency_log.csv",
    resourceFile: "streaming_query_hive_resource_log.csv",
    extraFiles: [
      "streaming_query_chunk_aggregator_log.csv",
      "chunked_debug_summary.json",
      "chunked_parent_partial_latency_log.csv",
      "chunked_window_diagnostics.csv",
      "chunked_orchestrator.log",
      "replayer-log.csv",
    ],
  },
};

function printHelp() {
  console.log(`Usage: node scripts/benchmark/run-scalability-benchmarks.js [options]

Options:
  --scenario <name>           same_query_different_windows
  --scales <list>             Comma-separated: 2,4,6,8,10
  --approaches <list>         Comma-separated: fetching,naive_distributed,approximation,chunked
  --iterations <n>            Default: 1
  --pattern <name>            low_variability | step_pattern
  --duration <value>          Replay duration alias, accepts 150 / 150s / 150000ms
  --replay-duration <value>   Replay duration alias, accepts 150 / 150s / 150000ms
  --output-dir <path>         Optional benchmark output root
  --resume                    Reuse completed runs in the output dir, rerun incomplete ones
  --iteration <n>             Run only a single iteration number
  --smoke                     Reduced default matrix: scale 2 only, 150s replay
  --help                      Show this help
`);
}

function parseArgs(argv) {
  const args = {
    scenario: SCENARIO,
    scales: [...DEFAULT_SCALES],
    approaches: [...DEFAULT_APPROACHES],
    iterations: DEFAULT_ITERATIONS,
    pattern: DEFAULT_PATTERN,
    replayDurationSeconds: DEFAULT_REPLAY_DURATION_SECONDS,
    outputDir: null,
    resume: false,
    iteration: null,
    smoke: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--scenario":
        args.scenario = requireValue(arg, next);
        index += 1;
        break;
      case "--scales":
        args.scales = parseCsvIntegers(arg, next, DEFAULT_SCALES);
        index += 1;
        break;
      case "--approaches":
        args.approaches = parseCsvStrings(arg, next, DEFAULT_APPROACHES);
        index += 1;
        break;
      case "--iterations":
        args.iterations = parsePositiveInteger(arg, next);
        index += 1;
        break;
      case "--pattern":
        args.pattern = requireValue(arg, next);
        index += 1;
        break;
      case "--duration":
      case "--replay-duration":
        args.replayDurationSeconds = parseDurationSeconds(arg, next);
        index += 1;
        break;
      case "--output-dir":
        args.outputDir = requireValue(arg, next);
        index += 1;
        break;
      case "--resume":
        args.resume = true;
        break;
      case "--iteration":
        args.iteration = parsePositiveInteger(arg, next);
        index += 1;
        break;
      case "--smoke":
        args.smoke = true;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!SCENARIO_CONFIG[args.scenario]) {
    throw new Error(`Unsupported scenario: ${args.scenario}`);
  }
  if (!["low_variability", "step_pattern"].includes(args.pattern)) {
    throw new Error(`Unsupported pattern: ${args.pattern}`);
  }
  for (const approach of args.approaches) {
    if (!APPROACHS[approach]) {
      throw new Error(`Unsupported approach: ${approach}`);
    }
  }

  if (args.smoke) {
    if (!argv.includes("--scales")) {
      args.scales = [2];
    }
    if (!argv.includes("--iterations")) {
      args.iterations = 1;
    }
    if (!argv.includes("--duration") && !argv.includes("--replay-duration")) {
      args.replayDurationSeconds = SMOKE_REPLAY_DURATION_SECONDS;
    }
  }

  return args;
}

function requireValue(flag, value) {
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  return value;
}

function parsePositiveInteger(flag, value) {
  const parsed = Number.parseInt(requireValue(flag, value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`);
  }
  return parsed;
}

function parseCsvIntegers(flag, value, allowed) {
  return requireValue(flag, value)
    .split(",")
    .map((entry) => Number.parseInt(entry.trim(), 10))
    .filter((entry) => Number.isFinite(entry))
    .map((entry) => {
      if (!allowed.includes(entry)) {
        throw new Error(`${flag} contains unsupported value: ${entry}`);
      }
      return entry;
    });
}

function parseCsvStrings(flag, value, allowed) {
  return requireValue(flag, value)
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      if (!allowed.includes(entry)) {
        throw new Error(`${flag} contains unsupported value: ${entry}`);
      }
      return entry;
    });
}

function parseDurationSeconds(flag, rawValue) {
  const value = requireValue(flag, rawValue).trim().toLowerCase();
  if (value.endsWith("ms")) {
    const parsed = Number.parseFloat(value.slice(0, -2));
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error(`${flag} must be a positive duration`);
    }
    return Math.ceil(parsed / 1000);
  }
  if (value.endsWith("s")) {
    const parsed = Number.parseFloat(value.slice(0, -1));
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error(`${flag} must be a positive duration`);
    }
    return Math.ceil(parsed);
  }
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive duration`);
  }
  return Math.ceil(parsed);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readCsvRows(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) {
    return [];
  }
  const lines = content.split(/\r?\n/);
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).filter(Boolean).map((line) => {
    const values = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? "";
    });
    return row;
  });
}

function parseCsvLine(line) {
  return line.match(/("([^"]|"")*"|[^,]+)/g)?.map((entry) =>
    entry.startsWith("\"") && entry.endsWith("\"")
      ? entry.slice(1, -1).replace(/""/g, "\"")
      : entry,
  ) || [];
}

function copyFileIfExists(sourcePath, targetPath) {
  if (!fs.existsSync(sourcePath)) {
    return false;
  }
  ensureDir(path.dirname(targetPath));
  fs.copyFileSync(sourcePath, targetPath);
  return true;
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function quantile(values, percentile) {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(percentile * sorted.length) - 1));
  return sorted[index];
}

function mean(values) {
  if (values.length === 0) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function computeResourceMetrics(resourceCsvPath) {
  const rows = readCsvRows(resourceCsvPath);
  if (rows.length < 2) {
    return {
      meanCpuPercent: null,
      peakCpuPercent: null,
      meanMemoryMb: null,
      peakMemoryMb: null,
    };
  }

  const cpuPercents = [];
  const memoryValues = [];
  for (let index = 1; index < rows.length; index += 1) {
    const previous = rows[index - 1];
    const current = rows[index];
    const dt = Number(current.timestamp) - Number(previous.timestamp);
    const deltaCpuMs =
      (Number(current.cpu_user) - Number(previous.cpu_user)) +
      (Number(current.cpu_system) - Number(previous.cpu_system));
    if (dt > 0 && Number.isFinite(deltaCpuMs)) {
      cpuPercents.push((deltaCpuMs / dt) * 100);
    }
    const memory = Number(current.heapUsedMB);
    if (Number.isFinite(memory)) {
      memoryValues.push(memory);
    }
  }

  return {
    meanCpuPercent: mean(cpuPercents),
    peakCpuPercent: cpuPercents.length > 0 ? Math.max(...cpuPercents) : null,
    meanMemoryMb: mean(memoryValues),
    peakMemoryMb: memoryValues.length > 0 ? Math.max(...memoryValues) : null,
  };
}

function normalizeLatencyRows(latencyCsvPath) {
  const rows = readCsvRows(latencyCsvPath);
  return rows
    .map((row) => {
      const windowNumber = Number(row.window_number);
      const expectedWindowClose = Number(row.expected_window_close);
      const resultEmittedAt = Number(row.result_emitted_at);
      const resultValue = Number(row.result_value);
      if (
        !Number.isFinite(windowNumber) ||
        !Number.isFinite(expectedWindowClose) ||
        !Number.isFinite(resultEmittedAt) ||
        !Number.isFinite(resultValue)
      ) {
        return null;
      }
      return {
        windowNumber,
        expectedWindowClose,
        resultEmittedAt,
        windowEndLatencyMs: resultEmittedAt - expectedWindowClose,
        resultValue,
      };
    })
    .filter(Boolean);
}

function writeNormalizedResults(outputPath, rows) {
  const header = "window_number,expected_window_close,result_emitted_at,window_end_latency_ms,result_value\n";
  const body = rows
    .map((row) => [
      row.windowNumber,
      row.expectedWindowClose,
      row.resultEmittedAt,
      row.windowEndLatencyMs,
      row.resultValue,
    ].join(","))
    .join("\n");
  fs.writeFileSync(outputPath, body ? `${header}${body}\n` : header);
}

function computeLatencyMetrics(rows) {
  const values = rows.map((row) => row.windowEndLatencyMs);
  return {
    meanWindowEndLatencyMs: mean(values),
    p95WindowEndLatencyMs: quantile(values, 0.95),
  };
}

function computeAccuracyMetrics(baselineRows, candidateRows) {
  const baselineByWindow = new Map(baselineRows.map((row) => [row.windowNumber, row]));
  const matched = candidateRows
    .map((row) => {
      const baseline = baselineByWindow.get(row.windowNumber);
      if (!baseline) {
        return null;
      }
      const error = row.resultValue - baseline.resultValue;
      const absoluteError = Math.abs(error);
      const absolutePercentageError =
        baseline.resultValue === 0 ? null : (absoluteError / Math.abs(baseline.resultValue)) * 100;
      return {
        absoluteError,
        squaredError: error * error,
        absolutePercentageError,
        exact: absoluteError <= RESULT_EPSILON,
      };
    })
    .filter(Boolean);

  const absoluteErrors = matched.map((row) => row.absoluteError);
  const squaredErrors = matched.map((row) => row.squaredError);
  const percentageErrors = matched
    .map((row) => row.absolutePercentageError)
    .filter((value) => value !== null);

  return {
    matchedWindowCount: matched.length,
    baselineWindowCount: baselineRows.length,
    candidateWindowCount: candidateRows.length,
    mae: mean(absoluteErrors),
    rmse: squaredErrors.length > 0 ? Math.sqrt(mean(squaredErrors)) : null,
    mape: percentageErrors.length > 0 ? mean(percentageErrors) : null,
    exactRate: matched.length > 0
      ? matched.filter((row) => row.exact).length / matched.length
      : null,
  };
}

function getRawInputSubscribers(approach, scale) {
  switch (approach) {
    case "approximation":
      return scale;
    case "chunked":
      return scale + 1;
    case "naive_distributed":
      return scale + 1;
    case "fetching":
      return 1;
    default:
      return 1;
  }
}

function computeValidationSummary(approach, scale, mqttSummary, options = {}) {
  const {
    chunkedDebugSummary = null,
    reconstructedResultRowCount = 0,
    expectedRawInputSubscriberCount = getRawInputSubscribers(approach, scale),
  } = options;
  const validations = [];
  const warnings = [];

  validations.push({
    name: "steady_state_duration_seconds_positive",
    passed: Number(mqttSummary.steady_state_duration_seconds) > 0,
    actual: mqttSummary.steady_state_duration_seconds,
  });
  validations.push({
    name: "estimated_delivery_bytes_gte_published_application_bytes",
    passed: Number(mqttSummary.estimated_delivery_bytes) >= Number(mqttSummary.published_application_bytes),
    actual: {
      estimated_delivery_bytes: mqttSummary.estimated_delivery_bytes,
      published_application_bytes: mqttSummary.published_application_bytes,
    },
  });

  const expectedRawInputDeliveryBytes =
    Number(mqttSummary.raw_input_published_bytes || 0) *
    Number(mqttSummary.raw_input_subscriber_count || 0);
  validations.push({
    name: "raw_input_estimated_delivery_bytes_matches_subscriber_count",
    passed:
      Number(mqttSummary.raw_input_estimated_delivery_bytes || 0) === expectedRawInputDeliveryBytes,
    actual: mqttSummary.raw_input_estimated_delivery_bytes,
    expected: expectedRawInputDeliveryBytes,
  });
  validations.push({
    name: "raw_input_subscriber_count_matches_expected",
    passed: Number(mqttSummary.raw_input_subscriber_count || 0) === Number(expectedRawInputSubscriberCount),
    actual: mqttSummary.raw_input_subscriber_count,
    expected: expectedRawInputSubscriberCount,
  });

  if (approach === "chunked") {
    validations.push({
      name: "chunk_size_ms_positive",
      passed: Number(chunkedDebugSummary?.chunkSizeMs || 0) > 0,
      actual: chunkedDebugSummary?.chunkSizeMs ?? null,
    });
    validations.push({
      name: "chunk_result_count_positive",
      passed: Number(mqttSummary.chunk_result_count || 0) > 0,
      actual: mqttSummary.chunk_result_count,
    });
    validations.push({
      name: "chunk_result_estimated_delivery_bytes_positive",
      passed: Number(mqttSummary.chunk_result_estimated_delivery_bytes || 0) > 0,
      actual: mqttSummary.chunk_result_estimated_delivery_bytes,
    });
    validations.push({
      name: "reconstructed_superquery_result_rows_positive",
      passed: Number(reconstructedResultRowCount) > 0,
      actual: reconstructedResultRowCount,
    });
  }

  if (approach === "fetching") {
    validations.push({
      name: "chunk_result_count_zero",
      passed: Number(mqttSummary.chunk_result_count || 0) === 0,
      actual: mqttSummary.chunk_result_count,
      expected: 0,
    });
    validations.push({
      name: "chunk_result_estimated_delivery_bytes_zero",
      passed: Number(mqttSummary.chunk_result_estimated_delivery_bytes || 0) === 0,
      actual: mqttSummary.chunk_result_estimated_delivery_bytes,
      expected: 0,
    });
    const exactRate = Number(options.exactRate ?? 0);
    const mae = Number(options.mae ?? Number.POSITIVE_INFINITY);
    const mape = Number(options.mape ?? Number.POSITIVE_INFINITY);
    validations.push({
      name: "accuracy_exact_or_near_exact",
      passed: exactRate >= 0.99 || mae <= RESULT_EPSILON || mape <= 1e-6,
      actual: {
        exactRate: options.exactRate,
        mae: options.mae,
        mape: options.mape,
      },
    });
  }

  if (Number(mqttSummary.unknown_published_bytes || 0) > 0 || Number(mqttSummary.unknown_estimated_delivery_bytes || 0) > 0) {
    warnings.push({
      name: "unknown_bytes_detected",
      unknownPublishedBytes: Number(mqttSummary.unknown_published_bytes || 0),
      unknownEstimatedDeliveryBytes: Number(mqttSummary.unknown_estimated_delivery_bytes || 0),
    });
  }

  return {
    validations,
    warnings,
    allPassed: validations.every((validation) => validation.passed),
  };
}

function computeKbPerSecond(bytes, durationSeconds) {
  if (!(Number(durationSeconds) > 0)) {
    return 0;
  }
  return Number(bytes || 0) / Number(durationSeconds) / 1024;
}

function formatMetric(value) {
  return value === null || value === undefined || Number.isNaN(value) ? "" : String(value);
}

function mqttSummaryMatches(summary, mqttSummary) {
  if (!summary || !mqttSummary) {
    return false;
  }
  const keysToCheck = [
    "published_application_bytes",
    "estimated_delivery_bytes",
    "published_bandwidth_kb_s",
    "estimated_delivery_bandwidth_kb_s",
    "raw_input_published_bytes",
    "raw_input_estimated_delivery_bytes",
    "raw_input_estimated_delivery_bandwidth_kb_s",
    "raw_input_subscriber_count",
    "reuse_layer_estimated_delivery_bytes",
    "reuse_layer_bandwidth_kb_s",
    "chunk_result_count",
    "chunk_result_estimated_delivery_bytes",
    "chunk_bandwidth_kb_s",
    "unknown_published_bytes",
    "unknown_estimated_delivery_bytes",
    "steady_state_duration_seconds",
  ];
  return keysToCheck.every((key) => Number(summary[key] ?? NaN) === Number(mqttSummary[key] ?? NaN));
}

class ScalabilityRunner {
  constructor(config) {
    this.config = config;
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);
    this.scenarioRoot = config.outputDir
      ? path.resolve(REPO_ROOT, config.outputDir)
      : path.join(REPO_ROOT, "logs", "scalability", config.scenario);
    this.groundTruthRoot = path.join(this.scenarioRoot, "_ground_truth", config.pattern);
    if (config.outputDir && !config.resume && fs.existsSync(this.scenarioRoot)) {
      const existingEntries = fs.readdirSync(this.scenarioRoot);
      if (existingEntries.length > 0) {
        throw new Error(`Output directory already exists and is not empty: ${this.scenarioRoot}`);
      }
    }
    ensureDir(this.scenarioRoot);
    ensureDir(this.groundTruthRoot);
  }

  getReusableWindows(scale) {
    return SCENARIO_CONFIG[this.config.scenario].reusableQueryWindows.slice(0, scale);
  }

  getIterationDir(scale, approach, iteration) {
    return path.join(
      this.scenarioRoot,
      `scale_${scale}`,
      approach,
      `iteration${iteration}`,
    );
  }

  getGroundTruthIterationDir(iteration) {
    return path.join(this.groundTruthRoot, `iteration${iteration}`);
  }

  isValidCompletedRun(iterationDir) {
    const summaryPath = path.join(iterationDir, "summary.json");
    const mqttSummaryPath = path.join(iterationDir, "mqtt_traffic_summary.json");
    if (!fs.existsSync(summaryPath) || !fs.existsSync(mqttSummaryPath)) {
      return false;
    }
    try {
      const summary = readJsonIfExists(summaryPath);
      return Boolean(summary?.validation?.allPassed);
    } catch {
      return false;
    }
  }

  getDatasetPath() {
    return `custom_patterns/${this.config.pattern}`;
  }

  createEnv({ approach, scale, iteration, logDir }) {
    const topicPrefix = [
      "bench",
      "scalability",
      this.config.scenario,
      `scale${scale}`,
      approach,
      `iter${iteration}`,
    ].join("/");
    return this.replayEnv.withBenchmarkReplayEnv({
      ...process.env,
      DATA_PATH: this.getDatasetPath(),
      LOG_PATH: logDir,
      BENCHMARK_SCENARIO: this.config.scenario,
      BENCHMARK_SCALE: String(scale),
      BENCHMARK_APPROACH: approach,
      BENCHMARK_ITERATION: String(iteration),
      BENCHMARK_RAW_INPUT_SUBSCRIBERS: String(getRawInputSubscribers(approach, scale)),
      AGGREGATION_FUNCTION: "AVG",
      AGGREGATION_FUNC: "AVG",
      OUTPUT_WINDOW_RANGE: String(SCENARIO_CONFIG[this.config.scenario].outputWindowRangeMs),
      OUTPUT_WINDOW_STEP: String(SCENARIO_CONFIG[this.config.scenario].outputWindowStepMs),
      WEARABLE_FREQUENCY: String(DEFAULT_FREQUENCY),
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(this.config.replayDurationSeconds),
      STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "1",
      STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    });
  }

  cleanupScratchFiles() {
    [
      "fetching_client_side_log.csv",
      "fetching_latency_log.csv",
      "fetching_client_side_resource_usage.csv",
      "fetching_window_diagnostics.csv",
      "approximation_approach_log.csv",
      "approximation_latency_log.csv",
      "approximation_approach_resource_usage.csv",
      "streaming_query_chunk_aggregator_log.csv",
      "chunked_latency_log.csv",
      "chunked_parent_partial_latency_log.csv",
      "chunked_window_diagnostics.csv",
      "streaming_query_hive_resource_log.csv",
      "naive_distributed_approach_log.csv",
      "naive_distributed_latency_log.csv",
      "naive_distributed_approach_resource_usage.csv",
      "replayer-log.csv",
      "mqtt_traffic.ndjson",
      "mqtt_traffic.csv",
      "mqtt_traffic_summary.json",
    ].forEach((fileName) => {
      const filePath = path.join(REPO_ROOT, fileName);
      if (fs.existsSync(filePath)) {
        fs.rmSync(filePath, { force: true, recursive: true });
      }
    });
  }

  async runSingleProcessCase(approach, scale, iteration, logDir) {
    const config = APPROACHS[approach];
    const env = this.createEnv({ approach, scale, iteration, logDir });
    fs.rmSync(logDir, { recursive: true, force: true });
    ensureDir(logDir);
    this.cleanupScratchFiles();
    await cleanupStaleBenchmarkProcesses({ logger: () => {} });

    console.log(
      `[benchmark] scenario=${this.config.scenario} scale=${scale} approach=${approach} iteration=${iteration} output_dir=${logDir}`,
    );

    const orchestrator = spawn("node", [config.orchestrator], {
      cwd: REPO_ROOT,
      env,
      stdio: "pipe",
      detached: true,
    });
    const orchestratorLogPath = path.join(logDir, `${approach}_orchestrator.log`);
    const orchestratorLog = fs.createWriteStream(orchestratorLogPath);
    orchestrator.stdout.on("data", (chunk) => orchestratorLog.write(chunk));
    orchestrator.stderr.on("data", (chunk) => orchestratorLog.write(chunk));

    await delay(2000);

    const publisher = spawn("node", ["dist/streamer/src/publishSmartphoneOnly.js"], {
      cwd: REPO_ROOT,
      env,
      stdio: "pipe",
      detached: true,
    });
    const publisherLogPath = path.join(logDir, "publisher.log");
    const publisherLog = fs.createWriteStream(publisherLogPath);
    publisher.stdout.on("data", (chunk) => publisherLog.write(chunk));
    publisher.stderr.on("data", (chunk) => publisherLog.write(chunk));

    const publisherExitCode = await new Promise((resolve, reject) => {
      publisher.on("error", reject);
      publisher.on("close", resolve);
    });

    await delay(2000);
    await terminateChildProcessTree(publisher, { name: `${approach}-publisher`, logger: () => {} });
    await terminateChildProcessTree(orchestrator, { name: `${approach}-orchestrator`, logger: () => {} });
    await delay(750);

    const mqttSummary = finalizeMqttTrafficArtifacts({ logDir });
    return {
      success: publisherExitCode === 0,
      mqttSummary,
      logDir,
    };
  }

  moveApproachFiles(approach, targetDir) {
    const config = APPROACHS[approach];
    const files = [
      config.latencyFile,
      config.resourceFile,
      ...config.extraFiles,
    ];
    files.forEach((fileName) => {
      copyFileIfExists(path.join(REPO_ROOT, fileName), path.join(targetDir, fileName));
    });
    copyFileIfExists(path.join(REPO_ROOT, "mqtt_traffic.csv"), path.join(targetDir, "mqtt_traffic.csv"));
    copyFileIfExists(path.join(REPO_ROOT, "mqtt_traffic_summary.json"), path.join(targetDir, "mqtt_traffic_summary.json"));
  }

  copyGroundTruthArtifacts(sourceDir, targetDir) {
    copyFileIfExists(
      path.join(sourceDir, "ground_truth_results.csv"),
      path.join(targetDir, "ground_truth_results.csv"),
    );
    copyFileIfExists(
      path.join(sourceDir, "fetching_latency_log.csv"),
      path.join(targetDir, "ground_truth_latency_log.csv"),
    );
    copyFileIfExists(
      path.join(sourceDir, "fetching_client_side_resource_usage.csv"),
      path.join(targetDir, "ground_truth_resource_usage.csv"),
    );
    copyFileIfExists(
      path.join(sourceDir, "fetching_summary.json"),
      path.join(targetDir, "ground_truth_summary.json"),
    );
  }

  async ensureGroundTruth(iteration) {
    const groundTruthDir = this.getGroundTruthIterationDir(iteration);
    const existingResults = path.join(groundTruthDir, "ground_truth_results.csv");
    const existingSummary = path.join(groundTruthDir, "fetching_summary.json");
    if (fs.existsSync(existingResults) && fs.existsSync(existingSummary)) {
      return groundTruthDir;
    }

    ensureDir(groundTruthDir);
    const outcome = await this.runSingleProcessCase("fetching", 1, iteration, groundTruthDir);
    this.moveApproachFiles("fetching", groundTruthDir);
    const normalizedRows = normalizeLatencyRows(path.join(groundTruthDir, APPROACHS.fetching.latencyFile));
    writeNormalizedResults(path.join(groundTruthDir, "ground_truth_results.csv"), normalizedRows);
    copyFileIfExists(
      path.join(groundTruthDir, APPROACHS.fetching.resourceFile),
      path.join(groundTruthDir, "resource_usage.csv"),
    );
    writeJson(path.join(groundTruthDir, "fetching_summary.json"), {
      approach: "fetching",
      iteration,
      success: outcome.success,
      mqttTrafficSummary: outcome.mqttSummary,
      latency: computeLatencyMetrics(normalizedRows),
      resources: computeResourceMetrics(path.join(groundTruthDir, APPROACHS.fetching.resourceFile)),
    });
    return groundTruthDir;
  }

  async runApproach(scale, approach, iteration, groundTruthDir) {
    const iterationDir = this.getIterationDir(scale, approach, iteration);
    if (this.config.resume && this.isValidCompletedRun(iterationDir)) {
      console.log(
        `[benchmark] resume=skip scenario=${this.config.scenario} scale=${scale} approach=${approach} iteration=${iteration} output_dir=${iterationDir}`,
      );
      return readJsonIfExists(path.join(iterationDir, "summary.json"));
    }
    if (fs.existsSync(iterationDir)) {
      fs.rmSync(iterationDir, { recursive: true, force: true });
    }
    ensureDir(iterationDir);
    const outcome = await this.runSingleProcessCase(approach, scale, iteration, iterationDir);
    this.moveApproachFiles(approach, iterationDir);
    this.copyGroundTruthArtifacts(groundTruthDir, iterationDir);

    const latencyRows = normalizeLatencyRows(path.join(iterationDir, APPROACHS[approach].latencyFile));
    const resultFilePath = path.join(iterationDir, "results.csv");
    writeNormalizedResults(resultFilePath, latencyRows);
    copyFileIfExists(path.join(iterationDir, APPROACHS[approach].latencyFile), path.join(iterationDir, "latency_log.csv"));
    copyFileIfExists(path.join(iterationDir, APPROACHS[approach].resourceFile), path.join(iterationDir, "resource_usage.csv"));

    const baselineRows = normalizeLatencyRows(path.join(groundTruthDir, APPROACHS.fetching.latencyFile));
    const chunkedDebugSummary = approach === "chunked"
      ? readJsonIfExists(path.join(iterationDir, "chunked_debug_summary.json"))
      : null;
    const latencyMetrics = computeLatencyMetrics(latencyRows);
    const resourceMetrics = computeResourceMetrics(path.join(iterationDir, APPROACHS[approach].resourceFile));
    const accuracyMetrics = computeAccuracyMetrics(baselineRows, latencyRows);
    const mqttMetrics = {
      ...outcome.mqttSummary,
      raw_input_estimated_delivery_bandwidth_kb_s: computeKbPerSecond(
        outcome.mqttSummary.raw_input_estimated_delivery_bytes,
        outcome.mqttSummary.steady_state_duration_seconds,
      ),
    };
    const validationSummary = computeValidationSummary(approach, scale, outcome.mqttSummary, {
      chunkedDebugSummary,
      reconstructedResultRowCount: latencyRows.length,
      expectedRawInputSubscriberCount: getRawInputSubscribers(approach, scale),
      exactRate: accuracyMetrics.exactRate,
      mae: accuracyMetrics.mae,
      mape: accuracyMetrics.mape,
    });

    const summary = {
      scenario: this.config.scenario,
      scale,
      approach,
      iteration,
      pattern: this.config.pattern,
      replayDurationSeconds: this.config.replayDurationSeconds,
      reusableQueries: this.getReusableWindows(scale),
      metrics: {
        resources: resourceMetrics,
        latency: latencyMetrics,
        mqttTraffic: mqttMetrics,
        accuracy: accuracyMetrics,
        chunkedDebug: chunkedDebugSummary,
      },
      validation: validationSummary,
      artifacts: {
        summaryJson: "summary.json",
        mqttTrafficSummaryJson: "mqtt_traffic_summary.json",
        mqttTrafficCsv: "mqtt_traffic.csv",
        resourceUsageCsv: fs.existsSync(path.join(iterationDir, "resource_usage.csv")) ? "resource_usage.csv" : null,
        resultCsv: "results.csv",
        groundTruthResultsCsv: "ground_truth_results.csv",
      },
    };
    writeJson(path.join(iterationDir, "summary.json"), summary);
    console.log(
      `[benchmark] validation=${validationSummary.allPassed ? "pass" : "fail"} scenario=${this.config.scenario} scale=${scale} approach=${approach} iteration=${iteration}`,
    );
    return summary;
  }

  writeScenarioSummaryCsv(rows) {
    const outputPath = path.join(this.scenarioRoot, "scalability_summary.csv");
    const headers = [
      "scenario",
      "scale",
      "approach",
      "iteration",
      "mean_cpu_percent",
      "peak_cpu_percent",
      "mean_memory_mb",
      "peak_memory_mb",
      "mean_window_end_latency_ms",
      "p95_window_end_latency_ms",
      "published_application_bytes",
      "estimated_delivery_bytes",
      "published_bandwidth_kb_s",
      "estimated_delivery_bandwidth_kb_s",
      "raw_input_published_bytes",
      "raw_input_estimated_delivery_bytes",
      "raw_input_estimated_delivery_bandwidth_kb_s",
      "raw_input_subscriber_count",
      "reuse_layer_estimated_delivery_bytes",
      "reuse_layer_bandwidth_kb_s",
      "chunk_result_count",
      "chunk_result_estimated_delivery_bytes",
      "chunk_bandwidth_kb_s",
      "mae",
      "rmse",
      "mape",
      "exact_rate",
    ];
    const body = rows.map((row) => [
      row.scenario,
      row.scale,
      row.approach,
      row.iteration,
      formatMetric(row.metrics.resources.meanCpuPercent),
      formatMetric(row.metrics.resources.peakCpuPercent),
      formatMetric(row.metrics.resources.meanMemoryMb),
      formatMetric(row.metrics.resources.peakMemoryMb),
      formatMetric(row.metrics.latency.meanWindowEndLatencyMs),
      formatMetric(row.metrics.latency.p95WindowEndLatencyMs),
      formatMetric(row.metrics.mqttTraffic.published_application_bytes),
      formatMetric(row.metrics.mqttTraffic.estimated_delivery_bytes),
      formatMetric(row.metrics.mqttTraffic.published_bandwidth_kb_s),
      formatMetric(row.metrics.mqttTraffic.estimated_delivery_bandwidth_kb_s),
      formatMetric(row.metrics.mqttTraffic.raw_input_published_bytes),
      formatMetric(row.metrics.mqttTraffic.raw_input_estimated_delivery_bytes),
      formatMetric(row.metrics.mqttTraffic.raw_input_estimated_delivery_bandwidth_kb_s),
      formatMetric(row.metrics.mqttTraffic.raw_input_subscriber_count),
      formatMetric(row.metrics.mqttTraffic.reuse_layer_estimated_delivery_bytes),
      formatMetric(row.metrics.mqttTraffic.reuse_layer_bandwidth_kb_s),
      formatMetric(row.metrics.mqttTraffic.chunk_result_count),
      formatMetric(row.metrics.mqttTraffic.chunk_result_estimated_delivery_bytes),
      formatMetric(row.metrics.mqttTraffic.chunk_bandwidth_kb_s),
      formatMetric(row.metrics.accuracy.mae),
      formatMetric(row.metrics.accuracy.rmse),
      formatMetric(row.metrics.accuracy.mape),
      formatMetric(row.metrics.accuracy.exactRate),
    ].join(","));
    fs.writeFileSync(outputPath, `${headers.join(",")}\n${body.join("\n")}\n`);
  }

  async run() {
    const rows = [];
    const iterations = this.config.iteration ? [this.config.iteration] : Array.from({ length: this.config.iterations }, (_, idx) => idx + 1);
    for (const iteration of iterations) {
      const groundTruthDir = await this.ensureGroundTruth(iteration);
      for (const scale of this.config.scales) {
        for (const approach of this.config.approaches) {
          const summary = await this.runApproach(scale, approach, iteration, groundTruthDir);
          rows.push(summary);
          await delay(1000);
        }
      }
    }
    this.writeScenarioSummaryCsv(rows);
  }
}

async function main() {
  const config = parseArgs(process.argv.slice(2));
  const runner = new ScalabilityRunner(config);
  await runner.run();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
