#!/usr/bin/env node

const { spawn, execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse/sync");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const { finalizeMqttTrafficArtifacts } = require("../../dist/util/mqttTraffic");
const { startProcessTreeResourceLogging } = require("../../scripts/analysis-js/process-tree-resource-sampler");
const { compareResults } = require("../../analysis/accuracy/accuracy-comparison-custom-patterns.js");
const {
  attachComparableTiming,
  buildWindowMetadataFromRow,
  verifyOutputStepCadence,
} = require("../../scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js");
const { buildTrimmedIterationSelection } = require("../../scripts/benchmark/run-all-paper-benchmarks.js");

const OUTPUT_WINDOW_RANGE_MS = 120000;
const OUTPUT_WINDOW_STEP_MS = 60000;
const PAPER_TARGET_WINDOWS = 35;
const DEFAULT_REPLAY_FREQUENCY_HZ = 4;
const DEFAULT_DROP_WARMUP = 3;
const DEFAULT_DROP_COOLDOWN = 2;
const DEFAULT_ANALYSIS_WINDOW_START = 4;
const DEFAULT_ANALYSIS_WINDOW_END = 33;
const DEFAULT_TIMEOUT_BUFFER_MS = 2 * 60 * 1000;
const LOGS_DIR = path.join("experiments", "real-data-comparison", "logs");
const REAL_DATA_TOPIC_PREFIX_ROOT = "real-data-paper-ready";
const APPROACHES = [
  {
    name: "fetching",
    label: "Fetching Client Side",
    orchestrator: "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
    processTreeFile: "fetching_client_side_process_tree_resource_usage.csv",
    logFiles: {
      main: "fetching_client_side_log.csv",
      resource: "fetching_client_side_resource_usage.csv",
      latency: "fetching_latency_log.csv",
      replayer: "replayer-log.csv",
    },
  },
  {
    name: "approximation",
    label: "Approximation",
    orchestrator: "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
    processTreeFile: "approximation_approach_process_tree_resource_usage.csv",
    logFiles: {
      main: "approximation_approach_log.csv",
      resource: "approximation_approach_resource_usage.csv",
      latency: "approximation_latency_log.csv",
      replayer: "replayer-log.csv",
    },
  },
  {
    name: "chunked",
    label: "Chunked",
    orchestrator: "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
    processTreeFile: "streaming_query_hive_process_tree_resource_log.csv",
    logFiles: {
      main: "streaming_query_chunk_aggregator_log.csv",
      resource: "streaming_query_hive_resource_log.csv",
      latency: "chunked_latency_log.csv",
      replayer: "replayer-log.csv",
    },
  },
  {
    name: "naive_distributed",
    label: "Naive Distributed",
    orchestrator: "dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js",
    processTreeFile: "naive_distributed_approach_process_tree_resource_usage.csv",
    logFiles: {
      main: "naive_distributed_approach_log.csv",
      resource: "naive_distributed_approach_resource_usage.csv",
      latency: "naive_distributed_latency_log.csv",
      replayer: "replayer-log.csv",
    },
  },
];

function parseCliArgs(argv) {
  const args = {
    analyzeOnly: false,
    approachNames: null,
    iterations: 3,
    targetWindowCount: null,
  };

  const positional = [];
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "analyze-only") {
      args.analyzeOnly = true;
      continue;
    }
    if (arg === "--iterations" || arg === "-i") {
      const value = Number.parseInt(argv[index + 1] || "", 10);
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error(`${arg} requires a positive integer`);
      }
      args.iterations = value;
      index += 1;
      continue;
    }
    if (arg === "--approach" || arg === "--approaches") {
      const rawValue = String(argv[index + 1] || "").trim();
      if (!rawValue) {
        throw new Error(`${arg} requires a value`);
      }
      const selectedNames = rawValue
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      if (selectedNames.length === 0) {
        throw new Error(`${arg} requires a value`);
      }
      const invalidNames = selectedNames.filter((name) => !getApproachByName(name));
      if (invalidNames.length > 0) {
        throw new Error(`Unknown approach name(s): ${invalidNames.join(", ")}`);
      }
      args.approachNames = [...new Set(selectedNames)];
      index += 1;
      continue;
    }
    if (arg === "--target-windows") {
      const value = Number.parseInt(argv[index + 1] || "", 10);
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("--target-windows requires a positive integer");
      }
      args.targetWindowCount = value;
      index += 1;
      continue;
    }
    positional.push(arg);
  }

  if (positional.length > 0) {
    throw new Error(`Unknown argument(s): ${positional.join(", ")}`);
  }

  return args;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function prepareFreshIterationLogDir(dirPath) {
  fs.rmSync(dirPath, { recursive: true, force: true });
  fs.mkdirSync(dirPath, { recursive: true });
}

function readText(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, "utf8") : "";
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeFallbackRunSummary(logDir, benchmarkSummary, existingSummary = null) {
  const runSummaryPath = path.join(logDir, "run_summary.json");
  if (existingSummary?.completionStatus === "completed") {
    return existingSummary;
  }

  const fallbackSummary = {
    ...existingSummary,
    publisherExitReason: existingSummary?.publisherExitReason || (
      benchmarkSummary?.stopReason === "target_window_count_reached"
        ? "target_window_count_reached"
        : "bounded_target_stop"
    ),
    completionStatus: "completed",
    emittedFinalWindowCount:
      benchmarkSummary?.emittedFinalWindowCount ?? existingSummary?.emittedFinalWindowCount ?? null,
    targetWindowCount:
      benchmarkSummary?.targetWindowCount ?? existingSummary?.targetWindowCount ?? null,
    stopReason: benchmarkSummary?.stopReason || existingSummary?.stopReason || null,
    stoppedAfterTargetWindows:
      benchmarkSummary?.stoppedAfterTargetWindows ?? existingSummary?.stoppedAfterTargetWindows ?? null,
    finalWindowNumbers:
      benchmarkSummary?.finalWindowNumbers ?? existingSummary?.finalWindowNumbers ?? [],
  };

  writeJson(runSummaryPath, fallbackSummary);
  return fallbackSummary;
}

function writeCsv(filePath, rows, headers) {
  const header = `${headers.join(",")}\n`;
  const body = rows.map((row) => (
    headers.map((key) => {
      const value = row[key];
      if (value === undefined || value === null) {
        return "";
      }
      const asString = String(value);
      return /[",\n]/.test(asString)
        ? `"${asString.replace(/"/g, '""')}"`
        : asString;
    }).join(",")
  )).join("\n");
  fs.writeFileSync(filePath, body ? `${header}${body}\n` : header);
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toBooleanOrNull(value) {
  if (value === undefined || value === null || String(value).trim() === "") {
    return null;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "no", "off"].includes(normalized)) {
    return false;
  }
  return null;
}

function mean(values) {
  if (values.length === 0) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function parseCsv(filePath) {
  const content = readText(filePath).trim();
  if (!content) {
    return [];
  }
  return parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  });
}

function summarizeMetric(values) {
  const cleanValues = values.filter((value) => Number.isFinite(value));
  return {
    count: cleanValues.length,
    mean: mean(cleanValues),
    min: cleanValues.length > 0 ? Math.min(...cleanValues) : null,
    max: cleanValues.length > 0 ? Math.max(...cleanValues) : null,
  };
}

function parseMqttTrafficSummary(logDir) {
  const summaryPath = path.join(logDir, "mqtt_traffic_summary.json");
  if (!fs.existsSync(summaryPath)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  } catch (error) {
    console.error(`Error parsing MQTT traffic summary ${summaryPath}:`, error.message);
    return null;
  }
}

function parseMqttTrafficCsv(logDir) {
  return parseCsv(path.join(logDir, "mqtt_traffic.csv"));
}

function buildMqttSummary(logDir) {
  const csvRows = parseMqttTrafficCsv(logDir);
  return {
    summary: parseMqttTrafficSummary(logDir),
    csvRows,
    messageCounts: summarizeMqttMessageCounts(csvRows),
  };
}

function summarizeMqttMessageCounts(csvRows) {
  const normalizedRows = csvRows.map((row) => ({
    ...row,
    warmup: String(row.warmup || "").trim().toLowerCase() === "true",
  }));
  const steadyRows = normalizedRows.filter((row) => !row.warmup);
  const countedRows = steadyRows.length > 0 ? steadyRows : normalizedRows;
  const countsByType = {};

  for (const row of countedRows) {
    const type = String(row.messageType || "unknown").trim() || "unknown";
    countsByType[type] = (countsByType[type] || 0) + 1;
  }

  return {
    total: countedRows.length,
    byType: countsByType,
  };
}

function readBenchmarkWindowSummary(logDir) {
  const summaryPath = path.join(logDir, "benchmark_window_cap_summary.json");
  if (!fs.existsSync(summaryPath)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  } catch (error) {
    console.error(`Error parsing benchmark window summary ${summaryPath}:`, error.message);
    return null;
  }
}

function shouldStopPublisherAfterTargetReached(logDir) {
  const summary = readBenchmarkWindowSummary(logDir);
  return summary?.stoppedAfterTargetWindows === true &&
    summary?.stopReason === "target_window_count_reached";
}

function hasSuccessfulTargetWindowCapSummary(summary) {
  return summary?.stoppedAfterTargetWindows === true &&
    summary?.stopReason === "target_window_count_reached" &&
    Number.isFinite(summary?.targetWindowCount) &&
    Number.isFinite(summary?.emittedFinalWindowCount) &&
    summary.emittedFinalWindowCount >= summary.targetWindowCount;
}

function isBoundedTargetStopExitAcceptable(code, signal, targetReached) {
  if (!targetReached) {
    return code === 0;
  }

  return code === 0 || signal === "SIGTERM";
}

function shouldEnableFetchingStartupFirstEmitted(targetWindows) {
  return Number.isFinite(targetWindows) && targetWindows > 0 && targetWindows <= 5;
}

function getLastObservedAt(record, approachName) {
  if (approachName === "fetching" || approachName === "naive_distributed") {
    return toNumber(record.last_obs_received_at);
  }
  if (approachName === "approximation") {
    return toNumber(record.last_data_received_at);
  }
  return toNumber(record.last_chunk_received_at);
}

function normalizeLatencyRows(approachName, records, mqttSummary) {
  const rows = records.map((record) => {
    const windowNumber = toNumber(record.window_number);
    const queryRegisteredAt = toNumber(record.query_registered_at);
    const firstDataReceivedAt = toNumber(record.first_data_received_at);
    const expectedWindowClose = toNumber(record.expected_window_close);
    const resultEmittedAt = toNumber(record.result_emitted_at);
    const resultValue = toNumber(record.result_value);
    const lastObservedAt = getLastObservedAt(record, approachName);
    const metadata = buildWindowMetadataFromRow(record, {
      expectedWindowClose,
      resultEmittedAt,
    });
    const coverageComplete = toBooleanOrNull(
      record.coverage_complete ?? record.coverageComplete,
    );
    const isPartialWindow = toBooleanOrNull(
      record.is_partial_window ?? record.isPartialWindow,
    );
    const explicitComparable = toBooleanOrNull(
      record.is_comparable_window ?? record.isComparableWindow,
    );
    const windowDurationMs = toNumber(
      record.window_duration_ms ?? record.windowDurationMs,
    );
    const resolvedWindowDurationMs = Number.isFinite(windowDurationMs)
      ? windowDurationMs
      : (
        Number.isFinite(metadata.windowStart) && Number.isFinite(metadata.windowEnd)
          ? metadata.windowEnd - metadata.windowStart
          : null
      );
    const derivedComparable =
      coverageComplete === null
        ? null
        : (
          coverageComplete === true &&
          resolvedWindowDurationMs === OUTPUT_WINDOW_RANGE_MS
        );

    return {
      approach: approachName,
      windowNumber,
      warmup:
        record.warmup === undefined || record.warmup === null || String(record.warmup).trim() === ""
          ? null
          : String(record.warmup).trim().toLowerCase() === "true",
      queryRegisteredAt,
      firstDataReceivedAt,
      expectedWindowClose,
      lastObservedAt,
      resultEmittedAt,
      resultValue,
      windowStart: metadata.windowStart,
      windowEnd: metadata.windowEnd,
      windowDurationMs: resolvedWindowDurationMs,
      windowDataCloseTime: metadata.windowDataCloseTime,
      coverageComplete,
      isPartialWindow,
      isComparableWindow:
        explicitComparable !== null
          ? explicitComparable
          : derivedComparable,
      windowSemantics: metadata.windowSemantics,
      logicalTriggerTime: metadata.logicalTriggerTime,
      latencyFromLogicalTriggerMs: metadata.latencyFromLogicalTriggerMs,
      latencyFromWindowCloseMs: metadata.latencyFromWindowCloseMs,
      wallClockCloseToResultMs: toNumber(record.wall_clock_close_to_result_ms),
      latencyDomainStatus: String(record.latency_domain_status || "").trim(),
      metadataSource: metadata.metadataSource,
      registrationToResultMs:
        Number.isFinite(queryRegisteredAt) && Number.isFinite(resultEmittedAt)
          ? resultEmittedAt - queryRegisteredAt
          : null,
      dataStartToResultMs:
        Number.isFinite(firstDataReceivedAt) && Number.isFinite(resultEmittedAt)
          ? resultEmittedAt - firstDataReceivedAt
          : null,
      lastDataToResultMs:
        Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
          ? resultEmittedAt - lastObservedAt
          : null,
    };
  }).filter((row) => (
    Number.isFinite(row.windowNumber) &&
    Number.isFinite(row.resultEmittedAt) &&
    Number.isFinite(row.resultValue)
  ));

  const byWindow = new Map();
  for (const row of rows) {
    const windowKey =
      Number.isFinite(row.windowStart) && Number.isFinite(row.windowEnd)
        ? `${row.windowStart}:${row.windowEnd}`
        : `window-number:${row.windowNumber}`;
    const existing = byWindow.get(windowKey);
    if (!existing) {
      byWindow.set(windowKey, row);
      continue;
    }
    const existingComparable = existing.isComparableWindow === true;
    const nextComparable = row.isComparableWindow === true;
    if (nextComparable && !existingComparable) {
      byWindow.set(windowKey, row);
      continue;
    }
    if (nextComparable === existingComparable && row.resultEmittedAt > existing.resultEmittedAt) {
      byWindow.set(windowKey, row);
    }
  }

  return attachComparableTiming(
    [...byWindow.values()].sort((left, right) => left.windowNumber - right.windowNumber),
    mqttSummary,
  );
}

function selectComparableRows(rows) {
  const rowsWithExplicitComparableFlag = rows.filter((row) => row.isComparableWindow !== null);
  if (rowsWithExplicitComparableFlag.length === 0) {
    return rows;
  }
  return rows.filter((row) => row.isComparableWindow === true);
}

function summarizeProcessTreeMetrics(logDir, processTreeFile, emittedWindowCount) {
  const rows = parseCsv(path.join(logDir, processTreeFile)).map((row) => ({
    timestamp: toNumber(row.timestamp),
    treeRssBytes: toNumber(row.tree_rss_bytes),
    treeCpuSeconds: toNumber(row.tree_cpu_seconds),
  })).filter((row) => (
    Number.isFinite(row.timestamp) &&
    Number.isFinite(row.treeRssBytes) &&
    Number.isFinite(row.treeCpuSeconds)
  ));

  const rssMiB = rows
    .map((row) => row.treeRssBytes / (1024 * 1024))
    .filter((value) => Number.isFinite(value));
  const totalCpuSeconds = rows.length > 0 ? rows[rows.length - 1].treeCpuSeconds : null;

  return {
    sampleCount: rows.length,
    cpuSeconds: totalCpuSeconds,
    cpuSecondsPerEmittedWindow:
      Number.isFinite(totalCpuSeconds) && emittedWindowCount > 0
        ? totalCpuSeconds / emittedWindowCount
        : null,
    meanRssMiB: mean(rssMiB),
    peakRssMiB: rssMiB.length > 0 ? Math.max(...rssMiB) : null,
  };
}

function countMissingFromExpected(rows, startWindow, endWindow) {
  const seen = new Set(rows.map((row) => row.windowNumber));
  let missing = 0;
  for (let windowNumber = startWindow; windowNumber <= endWindow; windowNumber += 1) {
    if (!seen.has(windowNumber)) {
      missing += 1;
    }
  }
  return missing;
}

function selectWindows(rows, startWindow, endWindow) {
  return rows.filter((row) => row.windowNumber >= startWindow && row.windowNumber <= endWindow);
}

function buildPaperConfig(iterationCount, options = {}) {
  const configuredTargetWindows = Number.parseInt(options.targetWindowCount || "", 10);
  const targetWindows = configuredTargetWindows > 0
    ? configuredTargetWindows
    : (
      Number.parseInt(process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS || "", 10) ||
      PAPER_TARGET_WINDOWS
    );
  const startupCostMode = targetWindows === 1;
  const effectiveIterations = startupCostMode
    ? Math.max(iterationCount, 1)
    : Math.max(iterationCount, PAPER_TARGET_WINDOWS);
  const trimmed = startupCostMode
    ? {
      iterations: [1],
      startIteration: 1,
      endIteration: 1,
      label: "startup-cost-window-1",
    }
    : buildTrimmedIterationSelection({
      iterations: effectiveIterations,
      dropWarmup: DEFAULT_DROP_WARMUP,
      dropCooldown: DEFAULT_DROP_COOLDOWN,
    });

  return {
    targetWindows,
    startupCostMode,
    analysisWindowStart: trimmed.startIteration || DEFAULT_ANALYSIS_WINDOW_START,
    analysisWindowEnd: trimmed.endIteration || DEFAULT_ANALYSIS_WINDOW_END,
    trimmedIterations: trimmed.iterations,
    trimmedLabel: trimmed.label,
    defaultReplayFrequencyHz:
      Number.parseFloat(process.env.WEARABLE_FREQUENCY || "") || DEFAULT_REPLAY_FREQUENCY_HZ,
    outputWindowRangeMs:
      Number.parseInt(process.env.OUTPUT_WINDOW_RANGE || "", 10) || OUTPUT_WINDOW_RANGE_MS,
    outputWindowStepMs:
      Number.parseInt(process.env.OUTPUT_WINDOW_STEP || "", 10) || OUTPUT_WINDOW_STEP_MS,
  };
}

function getApproachByName(name) {
  return APPROACHES.find((approach) => approach.name === name) || null;
}

function computeRequiredEventTimeCoverageMs(paperConfig) {
  return (
    paperConfig.outputWindowRangeMs +
    ((paperConfig.targetWindows - 1) * paperConfig.outputWindowStepMs)
  );
}

function computeReplayDurationSeconds(paperConfig) {
  return Math.ceil(
    (
      computeRequiredEventTimeCoverageMs(paperConfig) +
      (3 * paperConfig.outputWindowStepMs)
    ) / 1000,
  );
}

function computeRunTimeoutMs(env, paperConfig) {
  const explicitTimeoutMs = Number.parseInt(env.STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS || "", 10);
  if (Number.isFinite(explicitTimeoutMs) && explicitTimeoutMs > 0) {
    return explicitTimeoutMs;
  }
  return (computeReplayDurationSeconds(paperConfig) * 1000) + DEFAULT_TIMEOUT_BUFFER_MS;
}

function resolveFiniteReplayDurationSeconds(env, paperConfig) {
  const inheritedDurationSeconds = Number.parseInt(
    env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS || "",
    10,
  );
  const requiredDurationSeconds = computeReplayDurationSeconds(paperConfig);

  if (!Number.isFinite(inheritedDurationSeconds) || inheritedDurationSeconds <= 0) {
    return requiredDurationSeconds;
  }

  return Math.max(inheritedDurationSeconds, requiredDurationSeconds);
}

class RealDataComparisonRunner {
  constructor(options = {}) {
    this.iterations = options.iterations || 3;
    this.selectedApproaches = options.approachNames
      ? APPROACHES.filter((approach) => options.approachNames.includes(approach.name))
      : APPROACHES;
    this.results = [];
    this.paperConfig = buildPaperConfig(this.iterations, {
      targetWindowCount: options.targetWindowCount,
    });
    ensureDir(LOGS_DIR);
  }

  cleanupStaleProcesses() {
    try {
      execSync('pkill -f "dist/approaches" 2>/dev/null || true', { stdio: "ignore" });
    } catch {}
    try {
      execSync('pkill -f "dist/services/BeeWorker" 2>/dev/null || true', { stdio: "ignore" });
    } catch {}
    try {
      execSync("lsof -ti:8080 | xargs kill -9 2>/dev/null || true", { stdio: "ignore" });
    } catch {}
    return new Promise((resolve) => setTimeout(resolve, 1500));
  }

  cleanupLegacyRootLogFiles(approach) {
    const rootLogFiles = new Set([
      approach.processTreeFile,
      ...Object.values(approach.logFiles),
    ]);

    for (const fileName of rootLogFiles) {
      const sourcePath = path.join(".", fileName);
      if (!fs.existsSync(sourcePath)) {
        continue;
      }
      try {
        fs.unlinkSync(sourcePath);
      } catch {}
    }
  }

  cleanupRealDataSummaryArtifacts() {
    if (!fs.existsSync(LOGS_DIR)) {
      return;
    }

    const summaryArtifactPattern = /^real_data_(comparison_results|paper_ready_.+_summary|startup_cost_summary)\.(json|csv)$/;
    for (const entry of fs.readdirSync(LOGS_DIR)) {
      if (!summaryArtifactPattern.test(entry)) {
        continue;
      }
      try {
        fs.unlinkSync(path.join(LOGS_DIR, entry));
      } catch {}
    }
  }

  buildRunEnv(approach, logDir, iteration) {
    const targetWindows = this.paperConfig.targetWindows;
    const replayDurationSeconds = resolveFiniteReplayDurationSeconds(process.env, this.paperConfig);
    const topicPrefix = `${REAL_DATA_TOPIC_PREFIX_ROOT}/${approach.name}/iteration${iteration}`;
    const sessionId = `real-data-paper-ready-${approach.name}-iteration${iteration}`;
    const replayEnv = createBenchmarkReplayRunEnv(process.env);

    return replayEnv.withBenchmarkReplayEnv({
      ...process.env,
      DATA_PATH: ".",
      LOG_PATH: logDir,
      SESSION_ID: process.env.SESSION_ID || sessionId,
      BENCHMARK_SCENARIO: "real-data-comparison",
      BENCHMARK_SCALE: "real-data",
      BENCHMARK_APPROACH: approach.name,
      BENCHMARK_ITERATION: String(iteration),
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX:
        process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || topicPrefix,
      WEARABLE_FREQUENCY:
        process.env.WEARABLE_FREQUENCY || String(DEFAULT_REPLAY_FREQUENCY_HZ),
      STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE:
        approach.name === "fetching" && shouldEnableFetchingStartupFirstEmitted(targetWindows)
          ? "1"
          : (process.env.STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE || "0"),
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(targetWindows),
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS:
        String(replayDurationSeconds),
      STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD:
        process.env.STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD || "1",
      STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE:
        process.env.STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE || "1",
      STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE:
        process.env.STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE || "0",
      STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY:
        process.env.STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY || "0",
      STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY:
        process.env.STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY || "0",
      STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER:
        approach.name === "chunked"
          ? "1"
          : (process.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER || "1"),
      STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS:
        process.env.STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS ||
        String(computeRunTimeoutMs(process.env, this.paperConfig)),
    });
  }

  async runSingleTest(approach, iteration) {
    await this.cleanupStaleProcesses();
    return new Promise((resolve, reject) => {
      const runLabel = `${approach.label} - Iteration ${iteration}`;
      console.log(`\n${"=".repeat(80)}`);
      console.log(`Running: ${runLabel}`);
      console.log("=".repeat(80));

      const logDir = path.join(LOGS_DIR, approach.name, `iteration${iteration}`);
      prepareFreshIterationLogDir(logDir);
      this.cleanupLegacyRootLogFiles(approach);

      const env = this.buildRunEnv(approach, logDir, iteration);
      const startTime = Date.now();

      try {
        const orchestrator = spawn("node", [approach.orchestrator], {
          stdio: "inherit",
          env,
        });
        const resourceSampler = startProcessTreeResourceLogging(
          path.join(logDir, approach.processTreeFile),
          orchestrator.pid,
          100,
        );

        let settled = false;
        let publisher = null;
        let timeout = null;
        let publisherFinished = false;
        let publisherExitCode = null;
        let publisherSignal = null;
        let publisherError = null;
        let orchestratorFinished = false;
        let orchestratorExitCode = null;
        let orchestratorSignal = null;
        let orchestratorError = null;
        let boundedTargetStopTriggered = false;
        let boundedTargetStopPoll = null;

        const stopBoundedRunAfterTargetReached = () => {
          if (boundedTargetStopTriggered || !shouldStopPublisherAfterTargetReached(logDir)) {
            return;
          }

          boundedTargetStopTriggered = true;
          console.log(
            `Target window cap reached for ${approach.name} iteration ${iteration}; stopping publisher/orchestrator for bounded run handoff.`,
          );

          try {
            publisher?.kill("SIGTERM");
          } catch {}
          try {
            orchestrator.kill("SIGTERM");
          } catch {}
        };

        const finalize = (result) => {
          if (settled) {
            return;
          }
          settled = true;

          if (timeout) {
            clearTimeout(timeout);
          }
          if (boundedTargetStopPoll) {
            clearInterval(boundedTargetStopPoll);
          }
          try {
            publisher?.kill();
          } catch {}
          try {
            orchestrator.kill();
          } catch {}
          try {
            resourceSampler.stop();
          } catch {}

          this.copyLogFiles(approach, logDir);
          const mqttTrafficSummary = finalizeMqttTrafficArtifacts({ logDir });
          const benchmarkSummary = readBenchmarkWindowSummary(logDir);
          const runSummaryPath = path.join(logDir, "run_summary.json");
          const existingRunSummary = fs.existsSync(runSummaryPath)
            ? JSON.parse(fs.readFileSync(runSummaryPath, "utf8"))
            : null;
          const runSummary = hasSuccessfulTargetWindowCapSummary(benchmarkSummary)
            ? writeFallbackRunSummary(logDir, benchmarkSummary, existingRunSummary)
            : existingRunSummary;

          resolve({
            ...result,
            approach: approach.name,
            iteration,
            logDir,
            mqttTrafficSummary,
            runSummary,
          });
        };

        const maybeFinalizeSuccessfulRun = () => {
          if (!publisherFinished || !orchestratorFinished) {
            return;
          }

          const duration = (Date.now() - startTime) / 1000;
          const boundedTargetReached = shouldStopPublisherAfterTargetReached(logDir);
          if (publisherError) {
            finalize({
              success: false,
              duration,
              error: publisherError,
            });
            return;
          }

          if (!isBoundedTargetStopExitAcceptable(
            publisherExitCode,
            publisherSignal,
            boundedTargetReached && boundedTargetStopTriggered,
          )) {
            finalize({
              success: false,
              duration,
              error: `publisher exited with code ${publisherExitCode}${publisherSignal ? ` signal ${publisherSignal}` : ""}`,
            });
            return;
          }

          if (orchestratorError) {
            finalize({
              success: false,
              duration,
              error: orchestratorError,
            });
            return;
          }

          if (!isBoundedTargetStopExitAcceptable(
            orchestratorExitCode,
            orchestratorSignal,
            boundedTargetReached && boundedTargetStopTriggered,
          )) {
            finalize({
              success: false,
              duration,
              error: `orchestrator exited with code ${orchestratorExitCode}${orchestratorSignal ? ` signal ${orchestratorSignal}` : ""}`,
            });
            return;
          }

          finalize({
            success: true,
            duration,
            error: null,
          });
        };

        orchestrator.on("error", (error) => {
          orchestratorFinished = true;
          orchestratorError = error.message;
          maybeFinalizeSuccessfulRun();
        });

        orchestrator.on("close", (code, signal) => {
          orchestratorFinished = true;
          orchestratorExitCode = code;
          orchestratorSignal = signal;
          if (code === 0 && publisher && !publisherFinished &&
            shouldStopPublisherAfterTargetReached(logDir)) {
            publisherFinished = true;
            publisherExitCode = 0;
            try {
              publisher.kill("SIGTERM");
            } catch {}
          }
          maybeFinalizeSuccessfulRun();
        });

        setTimeout(() => {
          publisher = spawn("node", ["dist/streamer/src/publish.js"], {
            stdio: "inherit",
            env,
          });
          boundedTargetStopPoll = setInterval(stopBoundedRunAfterTargetReached, 250);
          boundedTargetStopPoll.unref?.();

          timeout = setTimeout(() => {
            console.log("Timeout reached, stopping publisher/orchestrator.");
            try {
              orchestrator.kill();
            } catch {}
            try {
              publisher.kill();
            } catch {}
          }, computeRunTimeoutMs(env, this.paperConfig));

          publisher.on("close", (code, signal) => {
            publisherFinished = true;
            publisherExitCode = code;
            publisherSignal = signal;
            maybeFinalizeSuccessfulRun();
          });

          publisher.on("error", (error) => {
            publisherFinished = true;
            publisherError = error.message;
            maybeFinalizeSuccessfulRun();
          });
        }, 2000);
      } catch (error) {
        reject(error);
      }
    });
  }

  copyLogFiles(approach, logDir) {
    for (const logFile of Object.values(approach.logFiles)) {
      const sourcePath = path.join(".", logFile);
      const destinationPath = path.join(logDir, logFile);
      if (!fs.existsSync(sourcePath)) {
        continue;
      }
      fs.copyFileSync(sourcePath, destinationPath);
      fs.unlinkSync(sourcePath);
    }
  }

  async runAllTests() {
    console.log("Starting real-data 4-approach comparison");
    console.log(`Approaches: ${this.selectedApproaches.map((approach) => approach.label).join(", ")}`);
    console.log(`Iterations per approach: ${this.iterations}`);
    console.log(`Target windows per run: ${this.paperConfig.targetWindows}`);
    console.log(`Trimmed analysis windows: ${this.paperConfig.analysisWindowStart}..${this.paperConfig.analysisWindowEnd}`);
    console.log(`Default replay frequency: ${this.paperConfig.defaultReplayFrequencyHz} Hz\n`);

    const totalTests = this.selectedApproaches.length * this.iterations;
    let completedTests = 0;

    for (const approach of this.selectedApproaches) {
      for (let iteration = 1; iteration <= this.iterations; iteration += 1) {
        try {
          const result = await this.runSingleTest(approach, iteration);
          this.results.push(result);
        } catch (error) {
          this.results.push({
            approach: approach.name,
            iteration,
            success: false,
            error: error.message,
          });
        }
        completedTests += 1;
        console.log(`Progress: ${completedTests}/${totalTests}`);
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
    }
  }

  discoverExistingResults() {
    this.results = [];
    for (const approach of APPROACHES) {
      const approachDir = path.join(LOGS_DIR, approach.name);
      if (!fs.existsSync(approachDir)) {
        continue;
      }
      const iterations = fs.readdirSync(approachDir)
        .filter((entry) => /^iteration\d+$/.test(entry))
        .sort((left, right) => Number(left.replace("iteration", "")) - Number(right.replace("iteration", "")));
      for (const entry of iterations) {
        const iteration = Number.parseInt(entry.replace("iteration", ""), 10);
        const logDir = path.join(approachDir, entry);
        if (fs.existsSync(path.join(logDir, approach.logFiles.latency))) {
          this.results.push({
            approach: approach.name,
            iteration,
            success: true,
            logDir,
          });
        }
      }
    }
  }

  buildIterationRecord(runResult) {
    const approach = getApproachByName(runResult.approach);
    if (!approach || !runResult.logDir) {
      return null;
    }

    const latencyRows = parseCsv(path.join(runResult.logDir, approach.logFiles.latency));
    const mqttSummary = buildMqttSummary(runResult.logDir);
    const normalizedRows = normalizeLatencyRows(approach.name, latencyRows, mqttSummary);
    const comparableRows = selectComparableRows(normalizedRows);
    const trimmedRows = selectWindows(
      comparableRows,
      this.paperConfig.analysisWindowStart,
      this.paperConfig.analysisWindowEnd,
    );
    const resourceMetrics = summarizeProcessTreeMetrics(
      runResult.logDir,
      approach.processTreeFile,
      comparableRows.length,
    );
    const rawCadence = verifyOutputStepCadence(normalizedRows);
    const trimmedCadence = verifyOutputStepCadence(trimmedRows);

    return {
      approach: approach.name,
      label: approach.label,
      iteration: runResult.iteration,
      success: runResult.success !== false,
      runDurationSeconds: Number.isFinite(runResult.duration) ? runResult.duration : null,
      rawWindowCount: comparableRows.length,
      trimmedWindowCount: trimmedRows.length,
      expectedTrimmedWindowCount:
        this.paperConfig.analysisWindowEnd - this.paperConfig.analysisWindowStart + 1,
      missingTrimmedWindows: countMissingFromExpected(
        trimmedRows,
        this.paperConfig.analysisWindowStart,
        this.paperConfig.analysisWindowEnd,
      ),
      latency: {
        rawCloseToResultMs: summarizeMetric(normalizedRows.map((row) => row.anchorAlignedWindowCloseToResultMs)),
        trimmedCloseToResultMs: summarizeMetric(trimmedRows.map((row) => row.anchorAlignedWindowCloseToResultMs)),
        rawWindowCloseMs: summarizeMetric(normalizedRows.map((row) => row.latencyFromWindowCloseMs)),
        trimmedWindowCloseMs: summarizeMetric(trimmedRows.map((row) => row.latencyFromWindowCloseMs)),
      },
      resourceMetrics,
      cadence: {
        raw: rawCadence,
        trimmed: trimmedCadence,
      },
      mqttTrafficSummary: mqttSummary.summary,
      mqttMessageCountTotal: mqttSummary.messageCounts.total,
      mqttMessageCountsByType: mqttSummary.messageCounts.byType,
      normalizedRows: comparableRows,
      trimmedRows,
    };
  }

  buildPaperAnalysis(iterationRecordsByApproach) {
    const fetchingRecords = iterationRecordsByApproach.fetching || [];
    const baselineByIteration = new Map(
      fetchingRecords.map((record) => [record.iteration, record]),
    );
    const chunkedComparableOutputOnly = String(
      process.env.STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY || "0",
    ).trim() === "1";
    const chunkedCadenceOnly = String(
      process.env.STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY || "0",
    ).trim() === "1";
    const chunkedUseImmediateTrigger = String(
      process.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER || "1",
    ).trim() !== "0";
    const analysis = {
      methodology: {
        targetWindows: this.paperConfig.targetWindows,
        analyzedWindows: `${this.paperConfig.analysisWindowStart}..${this.paperConfig.analysisWindowEnd}`,
        outputWindowRangeMs: this.paperConfig.outputWindowRangeMs,
        outputWindowStepMs: this.paperConfig.outputWindowStepMs,
        firstOutputTriggerOffsetMs: this.paperConfig.outputWindowStepMs,
        replayFrequencyHz: this.paperConfig.defaultReplayFrequencyHz,
        approximationCompletedWindowMode: true,
        chunkedSemanticReadyMode:
          !chunkedComparableOutputOnly && !chunkedCadenceOnly && chunkedUseImmediateTrigger,
        compactReusableResultPayload: true,
        cpuMetric: "cpu_seconds",
        startupLatencyMetric: "first_result_registration_to_result_ms",
        steadyStateLatencyMetric: this.paperConfig.startupCostMode
          ? null
          : "result_emitted_at - mapped_output_trigger_wall_clock_ms",
        mode: this.paperConfig.startupCostMode ? "startup-cost" : "steady-state",
      },
      byApproach: {},
    };

    for (const approach of APPROACHES) {
      const records = iterationRecordsByApproach[approach.name] || [];
      const comparisons = [];

      for (const record of records) {
        const baseline = baselineByIteration.get(record.iteration);
        if (!baseline) {
          continue;
        }
        const comparison = compareResults(baseline.trimmedRows, record.trimmedRows);
        comparisons.push({
          iteration: record.iteration,
          matchedWindowCount: comparison.matchedWindowCount,
          fetchingOnlyWindows: comparison.baselineOnlyCount,
          approachOnlyWindows: comparison.approachOnlyCount,
          missingWindows: comparison.baselineOnlyCount,
          extraWindows: comparison.approachOnlyCount,
          mae: comparison.mae,
          mape: comparison.mape,
          rmse: comparison.rmse,
        });
      }

      analysis.byApproach[approach.name] = {
        label: approach.label,
        iterations: records.length,
        rawWindowCountMean: mean(records.map((record) => record.rawWindowCount)),
        trimmedWindowCountMean: mean(records.map((record) => record.trimmedWindowCount)),
        closeToResultLatencyMs: summarizeMetric(
          records.map((record) => record.latency.trimmedCloseToResultMs.mean),
        ),
        meanRssMiB: mean(records.map((record) => record.resourceMetrics.meanRssMiB)),
        peakRssMiB: mean(records.map((record) => record.resourceMetrics.peakRssMiB)),
        cpuSeconds: summarizeMetric(records.map((record) => record.resourceMetrics.cpuSeconds)),
        cpuSecondsPerWindow: summarizeMetric(
          records.map((record) => record.resourceMetrics.cpuSecondsPerEmittedWindow),
        ),
        mqttPublishedBytes: summarizeMetric(
          records.map((record) => toNumber(record.mqttTrafficSummary?.published_application_bytes)),
        ),
        mqttEstimatedDeliveryBytes: summarizeMetric(
          records.map((record) => toNumber(record.mqttTrafficSummary?.estimated_delivery_bytes)),
        ),
        mqttMessageCount: summarizeMetric(
          records.map((record) => record.mqttMessageCountTotal),
        ),
        outputTriggerCadence: {
          raw: {
            ok: records.every((record) => record.cadence.raw.ok),
            checkedPairs: records.map((record) => record.cadence.raw.checkedPairs),
            issues: records.flatMap((record) => record.cadence.raw.issues.map((issue) => ({
              iteration: record.iteration,
              ...issue,
            }))),
          },
          trimmed: {
            ok: records.every((record) => record.cadence.trimmed.ok),
            checkedPairs: records.map((record) => record.cadence.trimmed.checkedPairs),
            issues: records.flatMap((record) => record.cadence.trimmed.issues.map((issue) => ({
              iteration: record.iteration,
              ...issue,
            }))),
          },
        },
        completeness: {
          matchedWindowCount: summarizeMetric(comparisons.map((comparison) => comparison.matchedWindowCount)),
          fetchingOnlyWindows: summarizeMetric(comparisons.map((comparison) => comparison.fetchingOnlyWindows)),
          approachOnlyWindows: summarizeMetric(comparisons.map((comparison) => comparison.approachOnlyWindows)),
          missingWindows: summarizeMetric(comparisons.map((comparison) => comparison.missingWindows)),
          extraWindows: summarizeMetric(comparisons.map((comparison) => comparison.extraWindows)),
        },
        accuracy: {
          mae: summarizeMetric(comparisons.map((comparison) => comparison.mae)),
          mape: summarizeMetric(comparisons.map((comparison) => comparison.mape)),
          rmse: summarizeMetric(comparisons.map((comparison) => comparison.rmse)),
        },
        comparisons,
        records,
      };
    }

    return analysis;
  }

  analyzeResults() {
    console.log("\nAnalyzing results...\n");

    const legacy = {
      byApproach: {},
    };
    const iterationRecordsByApproach = {};

    for (const approach of APPROACHES) {
      const records = this.results
        .filter((result) => result.approach === approach.name && result.logDir)
        .map((result) => this.buildIterationRecord(result))
        .filter(Boolean);
      iterationRecordsByApproach[approach.name] = records;

      const latencies = records
        .flatMap((record) => record.normalizedRows.map((row) => row.anchorAlignedWindowCloseToResultMs))
        .filter((value) => Number.isFinite(value));
      const firstResultValues = records[0]?.normalizedRows.map((row) => row.resultValue) || [];

      legacy.byApproach[approach.name] = {
        label: approach.label,
        iterations: records.length,
        avgLatency: mean(latencies),
        minLatency: latencies.length > 0 ? Math.min(...latencies) : null,
        maxLatency: latencies.length > 0 ? Math.max(...latencies) : null,
        windowCloseCount: latencies.length,
        resultValues: firstResultValues,
        mqttTraffic: records[0]?.mqttTrafficSummary || null,
      };
    }

    const paper = this.buildPaperAnalysis(iterationRecordsByApproach);
    return {
      legacy,
      paper,
    };
  }

  generateLegacyReport(analysis) {
    const fetchingResults = analysis.byApproach.fetching?.resultValues || [];
    const csvRows = [
      "Approach,Iterations,Avg_Latency_ms,Min_Latency_ms,Max_Latency_ms,Window_Count,Accuracy_%,MAE,MAPE_%,published_application_bytes,estimated_delivery_bytes,published_bandwidth_kb_s,estimated_delivery_bandwidth_kb_s,raw_input_published_bytes,raw_input_estimated_delivery_bytes,raw_input_subscriber_count,reuse_layer_estimated_delivery_bytes,reuse_layer_bandwidth_kb_s,chunk_result_estimated_delivery_bytes,chunk_result_count,chunk_bandwidth_kb_s",
    ];

    for (const approach of APPROACHES) {
      const data = analysis.byApproach[approach.name];
      if (!data) {
        continue;
      }

      let accuracy = "";
      let mae = "";
      let mape = "";
      if (approach.name === "fetching") {
        accuracy = "100";
        mae = "0";
        mape = "0";
      } else if (data.resultValues && fetchingResults.length > 0) {
        const baselineRows = fetchingResults.map((value, index) => ({
          windowStart: index,
          windowEnd: index + 1,
          resultValue: value,
        }));
        const candidateRows = data.resultValues.map((value, index) => ({
          windowStart: index,
          windowEnd: index + 1,
          resultValue: value,
        }));
        const comparison = compareResults(baselineRows, candidateRows);
        accuracy = comparison.matchedWindowCount > 0
          ? `${(comparison.matchedWindowCount / Math.max(baselineRows.length, 1)) * 100}`
          : "";
        mae = comparison.mae ?? "";
        mape = comparison.mape ?? "";
      }

      const traffic = data.mqttTraffic || {};
      csvRows.push([
        approach.label,
        data.iterations,
        data.avgLatency ?? "",
        data.minLatency ?? "",
        data.maxLatency ?? "",
        data.windowCloseCount ?? "",
        accuracy,
        mae,
        mape,
        traffic.published_application_bytes ?? 0,
        traffic.estimated_delivery_bytes ?? 0,
        traffic.published_bandwidth_kb_s ?? 0,
        traffic.estimated_delivery_bandwidth_kb_s ?? 0,
        traffic.raw_input_published_bytes ?? 0,
        traffic.raw_input_estimated_delivery_bytes ?? 0,
        traffic.raw_input_subscriber_count ?? 0,
        traffic.reuse_layer_estimated_delivery_bytes ?? 0,
        traffic.reuse_layer_bandwidth_kb_s ?? 0,
        traffic.chunk_result_estimated_delivery_bytes ?? 0,
        traffic.chunk_result_count ?? 0,
        traffic.chunk_bandwidth_kb_s ?? 0,
      ].join(","));
    }

    fs.writeFileSync(
      path.join(LOGS_DIR, "real_data_comparison_results.csv"),
      `${csvRows.join("\n")}\n`,
    );
    writeJson(path.join(LOGS_DIR, "real_data_comparison_results.json"), {
      timestamp: new Date().toISOString(),
      analysis,
      rawResults: this.results,
    });
  }

  writePaperArtifacts(paperAnalysis) {
    const rawPath = path.join(LOGS_DIR, "real_data_paper_ready_raw_summary.json");
    const trimmedJsonPath = path.join(
      LOGS_DIR,
      `real_data_paper_ready_${this.paperConfig.trimmedLabel}_summary.json`,
    );
    const trimmedCsvPath = path.join(
      LOGS_DIR,
      `real_data_paper_ready_${this.paperConfig.trimmedLabel}_summary.csv`,
    );

    writeJson(rawPath, paperAnalysis);
    writeJson(trimmedJsonPath, {
      methodology: paperAnalysis.methodology,
      byApproach: Object.fromEntries(
        Object.entries(paperAnalysis.byApproach).map(([approach, data]) => [
          approach,
          {
            label: data.label,
            iterations: data.iterations,
            closeToResultLatencyMs: data.closeToResultLatencyMs,
            meanRssMiB: data.meanRssMiB,
            peakRssMiB: data.peakRssMiB,
            cpuSeconds: data.cpuSeconds,
            cpuSecondsPerWindow: data.cpuSecondsPerWindow,
            mqttPublishedBytes: data.mqttPublishedBytes,
            mqttEstimatedDeliveryBytes: data.mqttEstimatedDeliveryBytes,
            mqttMessageCount: data.mqttMessageCount,
            completeness: data.completeness,
            accuracy: data.accuracy,
          },
        ]),
      ),
    });

    const csvRows = APPROACHES.map((approach) => {
      const data = paperAnalysis.byApproach[approach.name];
      return {
        approach: data?.label || approach.label,
        iterations: data?.iterations ?? 0,
        analyzed_windows: paperAnalysis.methodology.analyzedWindows,
        latency_mean_ms: data?.closeToResultLatencyMs?.mean ?? "",
        latency_min_ms: data?.closeToResultLatencyMs?.min ?? "",
        latency_max_ms: data?.closeToResultLatencyMs?.max ?? "",
        mean_rss_mib: data?.meanRssMiB ?? "",
        peak_rss_mib: data?.peakRssMiB ?? "",
        cpu_seconds_mean: data?.cpuSeconds?.mean ?? "",
        cpu_seconds_per_window_mean: data?.cpuSecondsPerWindow?.mean ?? "",
        mqtt_published_bytes_mean: data?.mqttPublishedBytes?.mean ?? "",
        mqtt_estimated_delivery_bytes_mean: data?.mqttEstimatedDeliveryBytes?.mean ?? "",
        mqtt_message_count_mean: data?.mqttMessageCount?.mean ?? "",
        matched_windows_mean: data?.completeness?.matchedWindowCount?.mean ?? "",
        fetching_only_windows_mean: data?.completeness?.fetchingOnlyWindows?.mean ?? "",
        approach_only_windows_mean: data?.completeness?.approachOnlyWindows?.mean ?? "",
        mae_mean: data?.accuracy?.mae?.mean ?? "",
        mape_mean: data?.accuracy?.mape?.mean ?? "",
        rmse_mean: data?.accuracy?.rmse?.mean ?? "",
      };
    });

    writeCsv(trimmedCsvPath, csvRows, [
      "approach",
      "iterations",
      "analyzed_windows",
      "latency_mean_ms",
      "latency_min_ms",
      "latency_max_ms",
      "mean_rss_mib",
      "peak_rss_mib",
      "cpu_seconds_mean",
      "cpu_seconds_per_window_mean",
      "mqtt_published_bytes_mean",
      "mqtt_estimated_delivery_bytes_mean",
      "mqtt_message_count_mean",
      "matched_windows_mean",
      "fetching_only_windows_mean",
      "approach_only_windows_mean",
      "mae_mean",
      "mape_mean",
      "rmse_mean",
    ]);

    if (this.paperConfig.startupCostMode) {
      const startupSummaryPath = path.join(LOGS_DIR, "real_data_startup_cost_summary.json");
      writeJson(startupSummaryPath, {
        methodology: paperAnalysis.methodology,
        byApproach: Object.fromEntries(
          Object.entries(paperAnalysis.byApproach).map(([approach, data]) => [
            approach,
            {
              label: data.label,
              iterations: data.iterations,
              startupCostMs: data.closeToResultLatencyMs,
              resourceUsage: {
                meanRssMiB: data.meanRssMiB,
                peakRssMiB: data.peakRssMiB,
                cpuSeconds: data.cpuSeconds,
              },
              completeness: data.completeness,
              accuracy: data.accuracy,
            },
          ]),
        ),
      });
    }
  }

  generateReport(analysis) {
    this.generateLegacyReport(analysis.legacy);
    this.writePaperArtifacts(analysis.paper);
  }

  async run() {
    const startTime = Date.now();
    this.cleanupRealDataSummaryArtifacts();
    await this.runAllTests();
    const analysis = this.analyzeResults();
    this.generateReport(analysis);
    const durationMinutes = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`Total execution time: ${durationMinutes} minutes`);
  }
}

async function main() {
  const args = parseCliArgs(process.argv.slice(2));
  const runner = new RealDataComparisonRunner({
    iterations: args.iterations,
    approachNames: args.approachNames,
    targetWindowCount: args.targetWindowCount,
  });

  if (args.analyzeOnly) {
    runner.discoverExistingResults();
    const analysis = runner.analyzeResults();
    runner.generateReport(analysis);
    return;
  }

  await runner.run();
}

if (require.main === module) {
  main().catch((error) => {
    console.error("Comparison runner failed:", error);
    process.exit(1);
  });
} else {
  module.exports = {
    APPROACHES,
    RealDataComparisonRunner,
    buildPaperConfig,
    computeReplayDurationSeconds,
    computeRequiredEventTimeCoverageMs,
    computeRunTimeoutMs,
    getApproachByName,
    hasSuccessfulTargetWindowCapSummary,
    isBoundedTargetStopExitAcceptable,
    readBenchmarkWindowSummary,
    resolveFiniteReplayDurationSeconds,
    normalizeLatencyRows,
    parseCliArgs,
    prepareFreshIterationLogDir,
    shouldEnableFetchingStartupFirstEmitted,
    shouldStopPublisherAfterTargetReached,
    summarizeProcessTreeMetrics,
    writeFallbackRunSummary,
  };
}
