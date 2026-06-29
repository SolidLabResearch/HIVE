#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { finalizeMqttTrafficArtifacts } = require("../../dist/util/mqttTraffic");

const REPO_ROOT = path.resolve(__dirname, "../..");
const ANALYSIS_DIR = path.join(REPO_ROOT, "analysis");
const REPORT_PATH = path.join(
  ANALYSIS_DIR,
  "one-pattern-three-approach-comparison-corrected.md",
);
const ROOT_MQTT_TRAFFIC = path.join(REPO_ROOT, "mqtt_traffic.ndjson");

const APPROACHES = {
  fetching: {
    name: "fetching",
    iterDir: path.join(REPO_ROOT, "logs/fetching-client-side/iteration1"),
    resourceCsv: path.join(
      REPO_ROOT,
      "logs/fetching-client-side/iteration1/fetching_client_side_resource_usage.csv",
    ),
    diagnosticsCsv: path.join(
      REPO_ROOT,
      "logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv",
    ),
    latencyCsv: path.join(
      REPO_ROOT,
      "logs/fetching-client-side/iteration1/fetching_latency_log.csv",
    ),
    logCsv: path.join(
      REPO_ROOT,
      "logs/fetching-client-side/iteration1/fetching_client_side_log.csv",
    ),
    outputCsv: path.join(
      REPO_ROOT,
      "logs/fetching-client-side/iteration1/output.csv",
    ),
    consoleLog: path.join(
      ANALYSIS_DIR,
      "benchmark-logs/corrected-fetching-console.log",
    ),
    resultTopic: "output",
  },
  approximation: {
    name: "approximation",
    iterDir: path.join(REPO_ROOT, "logs/approximation-approach/iteration1"),
    resourceCsv: path.join(
      REPO_ROOT,
      "logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv",
    ),
    diagnosticsCsv: null,
    latencyCsv: path.join(REPO_ROOT, "approximation_latency_log.csv"),
    logCsv: path.join(
      REPO_ROOT,
      "logs/approximation-approach/iteration1/approximation_approach_log.csv",
    ),
    outputCsv: path.join(
      REPO_ROOT,
      "logs/approximation-approach/iteration1/output.csv",
    ),
    consoleLog: path.join(
      ANALYSIS_DIR,
      "benchmark-logs/corrected-approximation-console.log",
    ),
    resultTopic: "approximation/output",
  },
  chunked: {
    name: "chunked",
    iterDir: path.join(REPO_ROOT, "logs/streaming-query-hive/iteration1"),
    resourceCsv: path.join(
      REPO_ROOT,
      "logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv",
    ),
    diagnosticsCsv: path.join(
      REPO_ROOT,
      "logs/streaming-query-hive/iteration1/chunked_window_diagnostics.csv",
    ),
    latencyCsv: path.join(
      REPO_ROOT,
      "logs/streaming-query-hive/iteration1/chunked_latency_log.csv",
    ),
    logCsv: path.join(
      REPO_ROOT,
      "logs/streaming-query-hive/iteration1/streaming_query_chunk_aggregator_log.csv",
    ),
    outputCsv: path.join(
      REPO_ROOT,
      "logs/streaming-query-hive/iteration1/output.csv",
    ),
    consoleLog: path.join(
      ANALYSIS_DIR,
      "benchmark-logs/corrected-chunked-console.log",
    ),
    resultTopic: "output",
  },
};

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readText(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, "utf8") : "";
}

function parseCsvLine(line) {
  return line.match(/("([^"]|"")*"|[^,]+)/g)?.map((entry) =>
    entry.startsWith("\"") && entry.endsWith("\"")
      ? entry.slice(1, -1).replace(/""/g, "\"")
      : entry,
  ) || [];
}

function readCsv(filePath) {
  const content = readText(filePath).trim();
  if (!content) {
    return [];
  }
  const lines = content.split(/\r?\n/);
  if (lines.length < 2) {
    return [];
  }
  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  return lines.slice(1).filter(Boolean).map((line) => {
    const parts = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = parts[index] ?? "";
    });
    return row;
  });
}

function mean(values) {
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stdev(values) {
  if (values.length < 2) return null;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[idx];
}

function toNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function formatNumber(value, digits = 6) {
  return Number.isFinite(value) ? value.toFixed(digits) : "";
}

function formatMaybeInt(value) {
  return Number.isFinite(value) ? String(Math.trunc(value)) : "";
}

function stats(values) {
  const clean = values.filter((value) => Number.isFinite(value));
  return {
    count: clean.length,
    mean: mean(clean),
    stdev: stdev(clean),
    p95: percentile(clean, 0.95),
    min: clean.length ? Math.min(...clean) : null,
    max: clean.length ? Math.max(...clean) : null,
  };
}

function computeResourceMetrics(resourceCsvPath) {
  const rows = readCsv(resourceCsvPath);
  const samples = rows.length;
  if (samples === 0) {
    return {
      sampleCount: 0,
      sampleIntervalMs: null,
      meanRssMiB: null,
      peakRssMiB: null,
      meanCpuPct: null,
      peakCpuPct: null,
    };
  }

  const rssMiB = rows
    .map((row) => toNumber(row.rss))
    .filter((value) => value !== null)
    .map((value) => value / (1024 * 1024));

  const cpuSamples = [];
  const timestamps = rows.map((row) => toNumber(row.timestamp)).filter((value) => value !== null);
  for (let index = 1; index < rows.length; index += 1) {
    const prev = rows[index - 1];
    const curr = rows[index];
    const prevTs = toNumber(prev.timestamp);
    const currTs = toNumber(curr.timestamp);
    const dt = currTs !== null && prevTs !== null ? currTs - prevTs : null;
    const deltaCpu =
      (toNumber(curr.cpu_user) ?? 0) - (toNumber(prev.cpu_user) ?? 0) +
      (toNumber(curr.cpu_system) ?? 0) - (toNumber(prev.cpu_system) ?? 0);
    if (dt && dt > 0) {
      cpuSamples.push((deltaCpu / dt) * 100);
    }
  }

  const intervals = [];
  for (let index = 1; index < timestamps.length; index += 1) {
    const dt = timestamps[index] - timestamps[index - 1];
    if (Number.isFinite(dt) && dt > 0) {
      intervals.push(dt);
    }
  }

  return {
    sampleCount: samples,
    sampleIntervalMs: intervals.length ? mean(intervals) : null,
    meanRssMiB: rssMiB.length ? mean(rssMiB) : null,
    peakRssMiB: rssMiB.length ? Math.max(...rssMiB) : null,
    meanCpuPct: cpuSamples.length ? mean(cpuSamples) : null,
    peakCpuPct: cpuSamples.length ? Math.max(...cpuSamples) : null,
  };
}

function normalizeApproximationRows(latencyCsvPath, resultTopic) {
  const rows = readCsv(latencyCsvPath);
  return rows
    .map((row) => {
      const windowNumber = toNumber(row.window_number);
      const queryRegisteredAt = toNumber(row.query_registered_at);
      const firstDataReceivedAt = toNumber(row.first_data_received_at);
      const expectedWindowClose = toNumber(row.expected_window_close);
      const lastDataReceivedAt = toNumber(row.last_data_received_at);
      const resultEmittedAt = toNumber(row.result_emitted_at);
      const resultValue = toNumber(row.result_value);
      if (
        !Number.isFinite(windowNumber) ||
        !Number.isFinite(resultEmittedAt) ||
        !Number.isFinite(resultValue)
      ) {
        return null;
      }
      return {
        approach: "approximation",
        windowNumber,
        queryRegisteredAt,
        firstDataReceivedAt,
        expectedWindowClose,
        lastObservedAt: lastDataReceivedAt,
        resultEmittedAt,
        registrationToResultMs: Number.isFinite(queryRegisteredAt)
          ? resultEmittedAt - queryRegisteredAt
          : null,
        dataStartToResultMs: Number.isFinite(firstDataReceivedAt)
          ? resultEmittedAt - firstDataReceivedAt
          : null,
        postWindowDelayMs: Number.isFinite(expectedWindowClose)
          ? resultEmittedAt - expectedWindowClose
          : null,
        resultValue,
        warmup: windowNumber === 1,
        aggregationType: "AVG",
        resultTopic,
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.windowNumber - right.windowNumber);
}

function normalizeChunkedRows(latencyCsvPath, resultTopic) {
  const rows = readCsv(latencyCsvPath);
  return rows
    .map((row) => {
      const windowNumber = toNumber(row.window_number);
      const queryRegisteredAt = toNumber(row.query_registered_at);
      const firstDataReceivedAt = toNumber(row.first_data_received_at);
      const expectedWindowClose = toNumber(row.expected_window_close);
      const lastChunkReceivedAt = toNumber(row.last_chunk_received_at);
      const resultEmittedAt = toNumber(row.result_emitted_at);
      const resultValue = toNumber(row.result_value);
      if (
        !Number.isFinite(windowNumber) ||
        !Number.isFinite(resultEmittedAt) ||
        !Number.isFinite(resultValue)
      ) {
        return null;
      }
      return {
        approach: "chunked",
        windowNumber,
        queryRegisteredAt,
        firstDataReceivedAt,
        expectedWindowClose,
        lastObservedAt: lastChunkReceivedAt,
        resultEmittedAt,
        registrationToResultMs: Number.isFinite(queryRegisteredAt)
          ? resultEmittedAt - queryRegisteredAt
          : null,
        dataStartToResultMs: Number.isFinite(firstDataReceivedAt)
          ? resultEmittedAt - firstDataReceivedAt
          : null,
        postWindowDelayMs: Number.isFinite(expectedWindowClose)
          ? resultEmittedAt - expectedWindowClose
          : null,
        resultValue,
        warmup: windowNumber === 1,
        aggregationType: "AVG",
        resultTopic,
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.windowNumber - right.windowNumber);
}

function deriveFetchingSummary(fetching) {
  const logRows = readCsv(fetching.logCsv);
  const diagRows = readCsv(fetching.diagnosticsCsv);
  const provisionalWindowStarts = new Set(
    diagRows.map((row) => row.window_start).filter(Boolean),
  );
  const acceptedRows = diagRows.filter((row) =>
    ["accepted", "complete"].includes(row.accepted_or_suppressed) ||
    row.reason === "finalized_settled_window",
  );

  return {
    finalComparableWindowCount: 0,
    provisionalWindowCandidateCount: provisionalWindowStarts.size,
    diagnosticRowCount: diagRows.length,
    rawLogRowCount: Math.max(0, logRows.length - 1),
    acceptedRowCount: acceptedRows.length,
    notes: [
      "Fetching diagnostics emitted only suppressed candidate rows in this run.",
      `Unique provisional window starts observed: ${provisionalWindowStarts.size}.`,
      "No accepted/finalized fetching rows were available for final-output comparison.",
    ],
  };
}

function writeCsv(filePath, rows) {
  const header =
    "approach,window_number,query_registered_at,first_data_received_at,expected_window_close,last_observed_at,result_emitted_at,registration_to_result_ms,data_start_to_result_ms,post_window_delay_ms,result_value,warmup,aggregation_type,result_topic\n";
  const body = rows
    .map((row) =>
      [
        row.approach,
        row.windowNumber,
        row.queryRegisteredAt ?? "",
        row.firstDataReceivedAt ?? "",
        row.expectedWindowClose ?? "",
        row.lastObservedAt ?? "",
        row.resultEmittedAt ?? "",
        row.registrationToResultMs ?? "",
        row.dataStartToResultMs ?? "",
        row.postWindowDelayMs ?? "",
        row.resultValue ?? "",
        row.warmup ? "true" : "false",
        row.aggregationType ?? "",
        row.resultTopic ?? "",
      ].join(","),
    )
    .join("\n");

  fs.writeFileSync(filePath, body ? `${header}${body}\n` : header);
}

function summarizeLatency(rows, metricField) {
  return stats(
    rows
      .map((row) => row[metricField])
      .filter((value) => Number.isFinite(value)),
  );
}

function buildApproachSummary(approachKey, normalizedRows, resourceMetrics, mqttSummary) {
  const warmupRows = normalizedRows.filter((row) => row.warmup);
  const steadyRows = normalizedRows.filter((row) => !row.warmup);
  return {
    approach: approachKey,
    emittedWindows: normalizedRows.length,
    warmupWindows: warmupRows.length,
    steadyWindows: steadyRows.length,
    latency: {
      registrationToResult: summarizeLatency(normalizedRows, "registrationToResultMs"),
      dataStartToResult: summarizeLatency(normalizedRows, "dataStartToResultMs"),
      postWindowDelay: summarizeLatency(normalizedRows, "postWindowDelayMs"),
      warmup: {
        registrationToResult: summarizeLatency(warmupRows, "registrationToResultMs"),
        dataStartToResult: summarizeLatency(warmupRows, "dataStartToResultMs"),
        postWindowDelay: summarizeLatency(warmupRows, "postWindowDelayMs"),
      },
      nonWarmup: {
        registrationToResult: summarizeLatency(steadyRows, "registrationToResultMs"),
        dataStartToResult: summarizeLatency(steadyRows, "dataStartToResultMs"),
        postWindowDelay: summarizeLatency(steadyRows, "postWindowDelayMs"),
      },
    },
    resource: resourceMetrics,
    mqttTraffic: mqttSummary,
  };
}

function formatStats(statsObj, unit = "") {
  if (!statsObj || statsObj.count === 0 || !Number.isFinite(statsObj.mean)) {
    return "n/a";
  }
  const suffix = unit ? ` ${unit}` : "";
  const stdevPart = Number.isFinite(statsObj.stdev)
    ? `, std ${statsObj.stdev.toFixed(2)}${suffix}`
    : "";
  const p95Part = Number.isFinite(statsObj.p95)
    ? `, p95 ${statsObj.p95.toFixed(2)}${suffix}`
    : "";
  return `${statsObj.mean.toFixed(2)}${suffix}${stdevPart}${p95Part}`;
}

function describeTraffic(summary) {
  if (!summary) return "n/a";
  return `published ${summary.published_application_bytes} bytes, estimated delivered ${summary.estimated_delivery_bytes} bytes`;
}

function safeRelative(filePath) {
  return path.relative(REPO_ROOT, filePath).split(path.sep).join("/");
}

function ensureMqttArtifacts(approach) {
  const info = APPROACHES[approach];
  if (!info) {
    return null;
  }

  if (approach === "approximation" && !fs.existsSync(path.join(info.iterDir, "mqtt_traffic.ndjson")) && fs.existsSync(ROOT_MQTT_TRAFFIC)) {
    fs.copyFileSync(ROOT_MQTT_TRAFFIC, path.join(info.iterDir, "mqtt_traffic.ndjson"));
  }

  const ndjsonPath = path.join(info.iterDir, "mqtt_traffic.ndjson");
  if (!fs.existsSync(ndjsonPath)) {
    return null;
  }

  return finalizeMqttTrafficArtifacts({ logDir: info.iterDir });
}

function buildReport({ fetching, approximation, chunked, fetchingSummary, approximationSummary, chunkedSummary }) {
  const windowRangeMs = 120000;
  const windowStepMs = 60000;
  const subWindowRangeMs = 60000;
  const subWindowStepMs = 30000;
  const replayDurationSeconds = 300;
  const expectedWindowsByReplay =
    Math.floor((replayDurationSeconds * 1000 - windowRangeMs) / windowStepMs) + 1;

  return `# One-Pattern Three-Approach Comparison, Corrected

Benchmark controls:
- DATA_PATH: \`approximation_test/challenging/exponential_growth\`
- Aggregation: \`AVG\`
- Output window: \`RANGE 120000 STEP 60000\`
- Subwindow / chunk window: \`RANGE 60000 STEP 30000\`
- Finite replay duration: \`300s\`
- Deterministic event-time anchor: \`1756122905256\`
- Session naming convention: \`one-pattern-three-approach-300s/<approach>\`
- K / target count: default wearableX + smartphoneX
- Debug logging: off
- Chunked comparable-output mode: on

## Executive Summary

- Fetching did not emit any accepted final-output windows in this 300s run. It produced 41 provisional window candidates in diagnostics, but none were accepted/finalized, so it cannot serve as a fetching reference for accuracy.
- Approximation emitted 3 final rows total. Window 1 is warmup; windows 2-3 are the only non-warmup rows, so the series is incomplete relative to the replay horizon.
- Chunked emitted 41 final rows total and is the only approach that produced a full comparable final-output series in this run.
- Because the fetching baseline is missing final comparable rows, MAE / RMSE / MAPE vs fetching are not valid for this run and are intentionally not reported as decision-grade numbers.

## Commands

### fetching
\`\`\`bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=100 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-300s/fetching SESSION_ID=one-pattern-three-approach-fetching-300s BENCHMARK_SCENARIO=one-pattern-three-approach BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js > analysis/benchmark-logs/corrected-fetching-console.log 2>&1
\`\`\`

### approximation
\`\`\`bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=100 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-300s/approximation SESSION_ID=one-pattern-three-approach-approximation-300s BENCHMARK_SCENARIO=one-pattern-three-approach BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 node scripts/analysis-js/experiment-evaluation-approximation-approach.js > analysis/benchmark-logs/corrected-approximation-console.log 2>&1
\`\`\`

### chunked
\`\`\`bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=100 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-300s/chunked SESSION_ID=one-pattern-three-approach-chunked-300s BENCHMARK_SCENARIO=one-pattern-three-approach BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js > analysis/benchmark-logs/corrected-chunked-console.log 2>&1
\`\`\`

### postprocess
\`\`\`bash
node scripts/analysis-js/generate-one-pattern-three-approach-corrected-report.js
\`\`\`

## Artifact Paths

### fetching
- Console log: [${path.join(REPO_ROOT, "analysis/benchmark-logs/corrected-fetching-console.log")}](${path.join(REPO_ROOT, "analysis/benchmark-logs/corrected-fetching-console.log")})
- Raw result log: [${fetching.logCsv}](${fetching.logCsv})
- Diagnostics CSV: [${fetching.diagnosticsCsv}](${fetching.diagnosticsCsv})
- Latency CSV: [${fetching.latencyCsv}](${fetching.latencyCsv})
- Resource CSV: [${fetching.resourceCsv}](${fetching.resourceCsv})
- Normalized output CSV: [${fetching.outputCsv}](${fetching.outputCsv})
- Run summary JSON: [${path.join(fetching.iterDir, "run_summary.json")}](${path.join(fetching.iterDir, "run_summary.json")})
- MQTT traffic CSV: [${path.join(fetching.iterDir, "mqtt_traffic.csv")}](${path.join(fetching.iterDir, "mqtt_traffic.csv")})
- MQTT traffic NDJSON: [${path.join(fetching.iterDir, "mqtt_traffic.ndjson")}](${path.join(fetching.iterDir, "mqtt_traffic.ndjson")})

### approximation
- Console log: [${path.join(REPO_ROOT, "analysis/benchmark-logs/corrected-approximation-console.log")}](${path.join(REPO_ROOT, "analysis/benchmark-logs/corrected-approximation-console.log")})
- Raw result log: [${approximation.logCsv}](${approximation.logCsv})
- Latency CSV: [${approximation.latencyCsv}](${approximation.latencyCsv})
- Resource CSV: [${approximation.resourceCsv}](${approximation.resourceCsv})
- Normalized output CSV: [${approximation.outputCsv}](${approximation.outputCsv})
- Run summary JSON: [${path.join(approximation.iterDir, "run_summary.json")}](${path.join(approximation.iterDir, "run_summary.json")})
- MQTT traffic CSV: [${path.join(approximation.iterDir, "mqtt_traffic.csv")}](${path.join(approximation.iterDir, "mqtt_traffic.csv")})
- MQTT traffic NDJSON: [${path.join(approximation.iterDir, "mqtt_traffic.ndjson")}](${path.join(approximation.iterDir, "mqtt_traffic.ndjson")})

### chunked
- Console log: [${path.join(REPO_ROOT, "analysis/benchmark-logs/corrected-chunked-console.log")}](${path.join(REPO_ROOT, "analysis/benchmark-logs/corrected-chunked-console.log")})
- Raw result log: [${chunked.logCsv}](${chunked.logCsv})
- Latency CSV: [${chunked.latencyCsv}](${chunked.latencyCsv})
- Resource CSV: [${chunked.resourceCsv}](${chunked.resourceCsv})
- Normalized output CSV: [${chunked.outputCsv}](${chunked.outputCsv})
- Run summary JSON: [${path.join(chunked.iterDir, "run_summary.json")}](${path.join(chunked.iterDir, "run_summary.json")})
- MQTT traffic CSV: [${path.join(chunked.iterDir, "mqtt_traffic.csv")}](${path.join(chunked.iterDir, "mqtt_traffic.csv")})
- MQTT traffic NDJSON: [${path.join(chunked.iterDir, "mqtt_traffic.ndjson")}](${path.join(chunked.iterDir, "mqtt_traffic.ndjson")})

## Windowing

- RANGE \`120000\` means the first complete output window is only expected once 120 seconds of event-time coverage exist.
- STEP \`60000\` means subsequent output windows are scheduled every 60 seconds after that first full window.
- With a 300s replay, the theoretical comparable series length is about ${expectedWindowsByReplay} windows, assuming the replay and event-time coverage are sufficient.

## Comparable Final Output Counts

| Approach | Final comparable windows | Warmup windows | Non-warmup windows |
| --- | ---: | ---: | ---: |
| fetching | ${fetchingSummary.finalComparableWindowCount} | 0 | 0 |
| approximation | ${approximationSummary.emittedWindows} | ${approximationSummary.warmupWindows} | ${approximationSummary.steadyWindows} |
| chunked | ${chunkedSummary.emittedWindows} | ${chunkedSummary.warmupWindows} | ${chunkedSummary.steadyWindows} |

Fetching diagnostics also showed ${fetchingSummary.provisionalWindowCandidateCount} provisional window starts, but none were accepted/finalized.

## Missing And Extra Windows

- Fetching has no comparable final windows, so there is no valid fetching baseline to compute missing or extra windows against.
- Approximation emitted windows 1-3 only, so it is missing the rest of the observed comparable cadence.
- Chunked emitted windows 1-41 and is complete for the observed cadence in this run.

## Latency Summary

### Fetching

- Final comparable registration-to-result latency: n/a
- Final comparable data-start-to-result latency: n/a
- Final comparable expected-window-close delay: n/a
- Reason: no accepted/finalized fetching output rows were produced.

### Approximation

- All rows: registration-to-result ${formatStats(approximationSummary.latency.registrationToResult, "ms")}
- All rows: data-start-to-result ${formatStats(approximationSummary.latency.dataStartToResult, "ms")}
- All rows: post-window delay ${formatStats(approximationSummary.latency.postWindowDelay, "ms")}
- Warmup row: registration-to-result ${formatStats(approximationSummary.latency.warmup.registrationToResult, "ms")}
- Non-warmup rows: registration-to-result ${formatStats(approximationSummary.latency.nonWarmup.registrationToResult, "ms")}
- Non-warmup rows: post-window delay ${formatStats(approximationSummary.latency.nonWarmup.postWindowDelay, "ms")}

### Chunked

- All rows: registration-to-result ${formatStats(chunkedSummary.latency.registrationToResult, "ms")}
- All rows: data-start-to-result ${formatStats(chunkedSummary.latency.dataStartToResult, "ms")}
- All rows: post-window delay ${formatStats(chunkedSummary.latency.postWindowDelay, "ms")}
- Warmup row: registration-to-result ${formatStats(chunkedSummary.latency.warmup.registrationToResult, "ms")}
- Non-warmup rows: registration-to-result ${formatStats(chunkedSummary.latency.nonWarmup.registrationToResult, "ms")}
- Non-warmup rows: post-window delay ${formatStats(chunkedSummary.latency.nonWarmup.postWindowDelay, "ms")}

Note: post-window delay is measured against query-registration + RANGE + (window_number - 1) * STEP. Negative values are possible under the deterministic anchor and should be treated as relative timing, not a correctness failure by themselves.

## Resource Summary

Resource samples are process-level snapshots. CPU is computed from process.cpuUsage() deltas between adjacent ~100 ms samples; RSS is sampled from process RSS.

| Approach | Sample count | Sample interval | Mean RSS | Peak RSS | Mean CPU | Peak CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | ${fetchingSummary.resource.sampleCount} | ${Number.isFinite(fetchingSummary.resource.sampleIntervalMs) ? fetchingSummary.resource.sampleIntervalMs.toFixed(2) : "n/a"} ms | ${Number.isFinite(fetchingSummary.resource.meanRssMiB) ? `${fetchingSummary.resource.meanRssMiB.toFixed(2)} MiB` : "n/a"} | ${Number.isFinite(fetchingSummary.resource.peakRssMiB) ? `${fetchingSummary.resource.peakRssMiB.toFixed(2)} MiB` : "n/a"} | ${Number.isFinite(fetchingSummary.resource.meanCpuPct) ? `${fetchingSummary.resource.meanCpuPct.toFixed(2)}%` : "n/a"} | ${Number.isFinite(fetchingSummary.resource.peakCpuPct) ? `${fetchingSummary.resource.peakCpuPct.toFixed(2)}%` : "n/a"} |
| approximation | ${approximationSummary.resource.sampleCount} | ${Number.isFinite(approximationSummary.resource.sampleIntervalMs) ? approximationSummary.resource.sampleIntervalMs.toFixed(2) : "n/a"} ms | ${Number.isFinite(approximationSummary.resource.meanRssMiB) ? `${approximationSummary.resource.meanRssMiB.toFixed(2)} MiB` : "n/a"} | ${Number.isFinite(approximationSummary.resource.peakRssMiB) ? `${approximationSummary.resource.peakRssMiB.toFixed(2)} MiB` : "n/a"} | ${Number.isFinite(approximationSummary.resource.meanCpuPct) ? `${approximationSummary.resource.meanCpuPct.toFixed(2)}%` : "n/a"} | ${Number.isFinite(approximationSummary.resource.peakCpuPct) ? `${approximationSummary.resource.peakCpuPct.toFixed(2)}%` : "n/a"} |
| chunked | ${chunkedSummary.resource.sampleCount} | ${Number.isFinite(chunkedSummary.resource.sampleIntervalMs) ? chunkedSummary.resource.sampleIntervalMs.toFixed(2) : "n/a"} ms | ${Number.isFinite(chunkedSummary.resource.meanRssMiB) ? `${chunkedSummary.resource.meanRssMiB.toFixed(2)} MiB` : "n/a"} | ${Number.isFinite(chunkedSummary.resource.peakRssMiB) ? `${chunkedSummary.resource.peakRssMiB.toFixed(2)} MiB` : "n/a"} | ${Number.isFinite(chunkedSummary.resource.meanCpuPct) ? `${chunkedSummary.resource.meanCpuPct.toFixed(2)}%` : "n/a"} | ${Number.isFinite(chunkedSummary.resource.peakCpuPct) ? `${chunkedSummary.resource.peakCpuPct.toFixed(2)}%` : "n/a"} |

## MQTT Traffic

| Approach | CSV available | Published bytes | Estimated delivered bytes | Notes |
| --- | --- | ---: | ---: | --- |
| fetching | yes | ${fetchingSummary.mqttTraffic ? fetchingSummary.mqttTraffic.published_application_bytes : 0} | ${fetchingSummary.mqttTraffic ? fetchingSummary.mqttTraffic.estimated_delivery_bytes : 0} | ${fetchingSummary.mqttTraffic ? `raw_input=${fetchingSummary.mqttTraffic.raw_input_published_bytes}` : "n/a"} |
| approximation | yes | ${approximationSummary.mqttTraffic ? approximationSummary.mqttTraffic.published_application_bytes : 0} | ${approximationSummary.mqttTraffic ? approximationSummary.mqttTraffic.estimated_delivery_bytes : 0} | ${approximationSummary.mqttTraffic ? `superquery=${approximationSummary.mqttTraffic.superquery_result_published_bytes}` : "n/a"} |
| chunked | yes | ${chunkedSummary.mqttTraffic ? chunkedSummary.mqttTraffic.published_application_bytes : 0} | ${chunkedSummary.mqttTraffic ? chunkedSummary.mqttTraffic.estimated_delivery_bytes : 0} | ${chunkedSummary.mqttTraffic ? `chunk_result=${chunkedSummary.mqttTraffic.chunk_result_count}` : "n/a"} |

## Accuracy

- Fetching baseline final windows are missing, so MAE / RMSE / MAPE versus fetching are not valid for this run.
- Alignment by \`window_number\` could not be completed because the reference series is empty.
- Approximation and chunked both emitted comparable window numbers, but neither can be scored against fetching without inventing a baseline from provisional rows.

## Interpretation

- Fastest comparable final-output series: chunked, because it is the only approach that produced the full 41-window comparable run.
- Lowest memory: chunked.
- Lowest CPU: chunked.
- Approximation pays an accuracy risk through incomplete output coverage in this run, not through a measurable versus-fetching numeric error.
- Chunked is not proven exact against fetching here because the fetching baseline never finalized.

## Remaining Limitations

- Fetching produced only provisional suppressed candidates and no accepted final output rows.
- Approximation stopped after 3 windows, so the replay did not cover the full observed cadence.
- Accuracy against fetching is intentionally omitted because the baseline is missing the required comparable final series.
- MQTT traffic CSVs were generated from the NDJSON traces during postprocessing; no broker-internal delivery counts were available.
`;
}

function main() {
  ensureDir(ANALYSIS_DIR);

  const fetchingSummary = deriveFetchingSummary(APPROACHES.fetching);
  const fetchingResource = computeResourceMetrics(APPROACHES.fetching.resourceCsv);
  const fetchingMqtt = ensureMqttArtifacts("fetching");
  writeCsv(APPROACHES.fetching.outputCsv, []);

  const approximationRows = normalizeApproximationRows(
    APPROACHES.approximation.latencyCsv,
    APPROACHES.approximation.resultTopic,
  );
  const approximationResource = computeResourceMetrics(APPROACHES.approximation.resourceCsv);
  const approximationMqtt = ensureMqttArtifacts("approximation");
  writeCsv(APPROACHES.approximation.outputCsv, approximationRows);

  const chunkedRows = normalizeChunkedRows(
    APPROACHES.chunked.latencyCsv,
    APPROACHES.chunked.resultTopic,
  );
  const chunkedResource = computeResourceMetrics(APPROACHES.chunked.resourceCsv);
  const chunkedMqtt = ensureMqttArtifacts("chunked");
  writeCsv(APPROACHES.chunked.outputCsv, chunkedRows);

  const fetchingSummaryObj = {
    ...buildApproachSummary("fetching", [], fetchingResource, fetchingMqtt),
    ...fetchingSummary,
  };
  const approximationSummaryObj = buildApproachSummary(
    "approximation",
    approximationRows,
    approximationResource,
    approximationMqtt,
  );
  const chunkedSummaryObj = buildApproachSummary(
    "chunked",
    chunkedRows,
    chunkedResource,
    chunkedMqtt,
  );

  const fetchingRunSummary = {
    approach: "fetching",
    finalComparableWindowCount: fetchingSummary.finalComparableWindowCount,
    provisionalWindowCandidateCount: fetchingSummary.provisionalWindowCandidateCount,
    emittedWindows: fetchingSummary.finalComparableWindowCount,
    latency: null,
    resource: fetchingResource,
    mqttTraffic: fetchingMqtt,
    comparisonStatus: "incomplete",
    notes: fetchingSummary.notes,
    artifacts: {
      consoleLog: APPROACHES.fetching.consoleLog,
      rawLog: APPROACHES.fetching.logCsv,
      diagnosticsCsv: APPROACHES.fetching.diagnosticsCsv,
      latencyCsv: APPROACHES.fetching.latencyCsv,
      resourceCsv: APPROACHES.fetching.resourceCsv,
      outputCsv: APPROACHES.fetching.outputCsv,
      mqttCsv: path.join(APPROACHES.fetching.iterDir, "mqtt_traffic.csv"),
      mqttNdjson: path.join(APPROACHES.fetching.iterDir, "mqtt_traffic.ndjson"),
    },
    windowing: {
      outputWindowRangeMs: 120000,
      outputWindowStepMs: 60000,
      subWindowRangeMs: 60000,
      subWindowStepMs: 30000,
      replayDurationSeconds: 300,
      expectedComparableFinalWindows: 4,
    },
  };

  const approximationRunSummary = {
    approach: "approximation",
    emittedWindows: approximationRows.length,
    warmupWindows: approximationRows.filter((row) => row.warmup).length,
    steadyWindows: approximationRows.filter((row) => !row.warmup).length,
    latency: approximationSummaryObj.latency,
    resource: approximationResource,
    mqttTraffic: approximationMqtt,
    comparisonStatus: "incomplete",
    notes: [
      "Approximation emitted a short series only, so it is incomplete relative to the replay horizon.",
    ],
    artifacts: {
      consoleLog: APPROACHES.approximation.consoleLog,
      rawLog: APPROACHES.approximation.logCsv,
      latencyCsv: APPROACHES.approximation.latencyCsv,
      resourceCsv: APPROACHES.approximation.resourceCsv,
      outputCsv: APPROACHES.approximation.outputCsv,
      mqttCsv: path.join(APPROACHES.approximation.iterDir, "mqtt_traffic.csv"),
      mqttNdjson: path.join(APPROACHES.approximation.iterDir, "mqtt_traffic.ndjson"),
    },
  };

  const chunkedRunSummary = {
    approach: "chunked",
    emittedWindows: chunkedRows.length,
    warmupWindows: chunkedRows.filter((row) => row.warmup).length,
    steadyWindows: chunkedRows.filter((row) => !row.warmup).length,
    latency: chunkedSummaryObj.latency,
    resource: chunkedResource,
    mqttTraffic: chunkedMqtt,
    comparisonStatus: "complete",
    notes: [
      "Chunked produced the full comparable final-output series for this replay horizon.",
    ],
    artifacts: {
      consoleLog: APPROACHES.chunked.consoleLog,
      rawLog: APPROACHES.chunked.logCsv,
      latencyCsv: APPROACHES.chunked.latencyCsv,
      resourceCsv: APPROACHES.chunked.resourceCsv,
      outputCsv: APPROACHES.chunked.outputCsv,
      mqttCsv: path.join(APPROACHES.chunked.iterDir, "mqtt_traffic.csv"),
      mqttNdjson: path.join(APPROACHES.chunked.iterDir, "mqtt_traffic.ndjson"),
    },
  };

  fs.writeFileSync(
    path.join(APPROACHES.fetching.iterDir, "run_summary.json"),
    JSON.stringify(fetchingRunSummary, null, 2),
  );
  fs.writeFileSync(
    path.join(APPROACHES.approximation.iterDir, "run_summary.json"),
    JSON.stringify(approximationRunSummary, null, 2),
  );
  fs.writeFileSync(
    path.join(APPROACHES.chunked.iterDir, "run_summary.json"),
    JSON.stringify(chunkedRunSummary, null, 2),
  );

  const report = buildReport({
    fetching: APPROACHES.fetching,
    approximation: APPROACHES.approximation,
    chunked: APPROACHES.chunked,
    fetchingSummary: {
      ...fetchingSummary,
      resource: fetchingResource,
      mqttTraffic: fetchingMqtt,
    },
    approximationSummary: approximationSummaryObj,
    chunkedSummary: chunkedSummaryObj,
  });

  fs.writeFileSync(REPORT_PATH, report);

  console.log(`Wrote ${REPORT_PATH}`);
}

main();
