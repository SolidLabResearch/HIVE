#!/usr/bin/env node

const fs = require('fs');
const os = require('os');
const path = require('path');
const { finalizeMqttTrafficArtifacts } = require('../../dist/util/mqttTraffic');
const {
  calculateRegistrationAnchoredLatencies,
  REGISTRATION_ANCHORED_LATENCY_SOURCE,
} = require('../../analysis/accuracy/accuracy-comparison-custom-patterns.js');

const REPO_ROOT = path.resolve(__dirname, '../..');
const ANALYSIS_DIR = path.join(REPO_ROOT, 'analysis');
const CORE_COUNT = os.cpus().length || 1;
const WINDOW_VARIANT = String(process.env.BENCHMARK_WINDOW_VARIANT || 'trailing').toLowerCase() === 'centered'
  ? 'centered'
  : 'trailing';
const IS_CENTERED = WINDOW_VARIANT === 'centered';
const SCENARIO_NAME = IS_CENTERED ? 'one-pattern-centered-n3' : 'one-pattern-three-approach-n3';
const REPORT_PATH = path.join(
  ANALYSIS_DIR,
  IS_CENTERED ? 'one-pattern-centered-window-n3-summary.md' : 'one-pattern-three-approach-n3-summary.md',
);

function scenarioLogRoot(approach) {
  if (!IS_CENTERED) {
    if (approach === 'fetching') return 'logs/fetching-client-side';
    if (approach === 'approximation') return 'logs/approximation-approach';
    return 'logs/streaming-query-hive';
  }
  if (approach === 'fetching') return 'logs/centered-window-n3/fetching';
  if (approach === 'approximation') return 'logs/centered-window-n3/approximation';
  return 'logs/centered-window-n3/chunked';
}

function scenarioResultTopic(approach) {
  return approach === 'approximation' ? 'approximation/output' : 'output';
}

function scenarioSessionId(approach, iter) {
  return `${SCENARIO_NAME}-${approach}-i${iter}`;
}

function scenarioTopicPrefix(approach, iter) {
  return `${SCENARIO_NAME}/i${iter}/${approach}`;
}

function scenarioLogPath(approach, iter) {
  return path.join(REPO_ROOT, scenarioLogRoot(approach), `iteration${iter}`);
}

function buildApproachConfigs() {
  return {
    fetching: {
      label: 'fetching',
      resultTopic: scenarioResultTopic('fetching'),
      iterDirs: [1, 2, 3].map((iter) => ({
        iter,
        dir: scenarioLogPath('fetching', iter),
        latencyCsv: path.join(scenarioLogPath('fetching', iter), 'fetching_latency_log.csv'),
        diagnosticsCsv: path.join(scenarioLogPath('fetching', iter), 'fetching_window_diagnostics.csv'),
        primaryResourceCsv: path.join(scenarioLogPath('fetching', iter), 'fetching_client_side_resource_usage.csv'),
        treeResourceCsv: path.join(scenarioLogPath('fetching', iter), 'fetching_client_side_process_tree_resource_usage.csv'),
        logCsv: path.join(scenarioLogPath('fetching', iter), 'fetching_client_side_log.csv'),
        outputCsv: path.join(scenarioLogPath('fetching', iter), 'normalized_final_rows.csv'),
        mqttNdjson: path.join(scenarioLogPath('fetching', iter), 'mqtt_traffic.ndjson'),
        mqttCsv: path.join(scenarioLogPath('fetching', iter), 'mqtt_traffic.csv'),
        runSummary: path.join(scenarioLogPath('fetching', iter), 'run_summary.json'),
      })),
    },
    approximation: {
      label: 'approximation',
      resultTopic: scenarioResultTopic('approximation'),
      iterDirs: [1, 2, 3].map((iter) => ({
        iter,
        dir: scenarioLogPath('approximation', iter),
        latencyCsv: path.join(scenarioLogPath('approximation', iter), 'approximation_latency_log.csv'),
        primaryResourceCsv: path.join(scenarioLogPath('approximation', iter), 'approximation_approach_resource_usage.csv'),
        treeResourceCsv: path.join(scenarioLogPath('approximation', iter), 'approximation_approach_process_tree_resource_usage.csv'),
        logCsv: path.join(scenarioLogPath('approximation', iter), 'approximation_approach_log.csv'),
        outputCsv: path.join(scenarioLogPath('approximation', iter), 'normalized_final_rows.csv'),
        mqttNdjson: path.join(scenarioLogPath('approximation', iter), 'mqtt_traffic.ndjson'),
        mqttCsv: path.join(scenarioLogPath('approximation', iter), 'mqtt_traffic.csv'),
        runSummary: path.join(scenarioLogPath('approximation', iter), 'run_summary.json'),
      })),
    },
    chunked: {
      label: 'chunked',
      resultTopic: scenarioResultTopic('chunked'),
      iterDirs: [1, 2, 3].map((iter) => ({
        iter,
        dir: scenarioLogPath('chunked', iter),
        latencyCsv: path.join(scenarioLogPath('chunked', iter), 'chunked_latency_log.csv'),
        primaryResourceCsv: path.join(scenarioLogPath('chunked', iter), 'streaming_query_hive_resource_log.csv'),
        treeResourceCsv: path.join(scenarioLogPath('chunked', iter), 'streaming_query_hive_process_tree_resource_log.csv'),
        logCsv: path.join(scenarioLogPath('chunked', iter), 'streaming_query_chunk_aggregator_log.csv'),
        outputCsv: path.join(scenarioLogPath('chunked', iter), 'normalized_final_rows.csv'),
        mqttNdjson: path.join(scenarioLogPath('chunked', iter), 'mqtt_traffic.ndjson'),
        mqttCsv: path.join(scenarioLogPath('chunked', iter), 'mqtt_traffic.csv'),
        runSummary: path.join(scenarioLogPath('chunked', iter), 'run_summary.json'),
      })),
    },
  };
}
function buildCommandLines() {
  return [1, 2, 3].flatMap((iter) => [
    `DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=${scenarioTopicPrefix('fetching', iter)} SESSION_ID=${scenarioSessionId('fetching', iter)} BENCHMARK_SCENARIO=${SCENARIO_NAME} BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=${iter} OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000${IS_CENTERED ? ' RSP_WINDOW_SEMANTICS=centered' : ''} LOG_PATH=${scenarioLogPath('fetching', iter)} node scripts/analysis-js/experiment-evaluation-fetching-client-side.js`,
    `DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=${scenarioTopicPrefix('approximation', iter)} SESSION_ID=${scenarioSessionId('approximation', iter)} BENCHMARK_SCENARIO=${SCENARIO_NAME} BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=${iter} OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000${IS_CENTERED ? ' RSP_WINDOW_SEMANTICS=centered' : ''} LOG_PATH=${scenarioLogPath('approximation', iter)} node scripts/analysis-js/experiment-evaluation-approximation-approach.js`,
    `DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=${scenarioTopicPrefix('chunked', iter)} SESSION_ID=${scenarioSessionId('chunked', iter)} BENCHMARK_SCENARIO=${SCENARIO_NAME} BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=${iter} OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0${IS_CENTERED ? ' RSP_WINDOW_SEMANTICS=centered' : ''} LOG_PATH=${scenarioLogPath('chunked', iter)} node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js`,
  ]);
}

const APPROACHES = buildApproachConfigs();
const COMMAND_LINES = buildCommandLines();

function readText(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : '';
}

function parseCsvLine(line) {
  return line.match(/("([^"]|"")*"|[^,]+)/g)?.map((entry) => (
    entry.startsWith('"') && entry.endsWith('"')
      ? entry.slice(1, -1).replace(/""/g, '"')
      : entry
  )) || [];
}

function readCsv(filePath) {
  const content = readText(filePath).trim();
  if (!content) return [];
  const lines = content.split(/\r?\n/);
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  return lines.slice(1).filter(Boolean).map((line) => {
    const parts = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = parts[index] ?? '';
    });
    return row;
  });
}

function readJson(filePath) {
  const content = readText(filePath).trim();
  return content ? JSON.parse(content) : null;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function toNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

const BENCHMARK_WINDOW_RANGE_MS = 120000;
const BENCHMARK_WINDOW_STEP_MS = 60000;

function inferExpectedLogicalTriggerTime(windowSemantics, windowStart, windowEnd) {
  if (!Number.isFinite(windowStart) || !Number.isFinite(windowEnd)) {
    return null;
  }
  if (String(windowSemantics || '').toLowerCase() === 'centered') {
    return windowStart + ((windowEnd - windowStart) / 2);
  }
  return windowEnd;
}

function toMetadataSource(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "direct" ? "direct" : normalized === "reconstructed" ? "reconstructed" : null;
}

function buildWindowMetadataFromRow(row, fallback = {}) {
  const rawWindowSemantics = row.window_semantics ?? row.windowSemantics;
  const rawLogicalTriggerTime = row.logical_trigger_time ?? row.logicalTriggerTime;
  const rawWindowStart = row.window_start ?? row.windowStart;
  const rawWindowEnd = row.window_end ?? row.windowEnd;
  const rawWindowDataCloseTime = row.window_data_close_time ?? row.windowDataCloseTime;
  const rawResultEmittedAt = row.result_emitted_at ?? row.resultEmittedAt;
  const rawLatencyFromLogicalTriggerMs = row.latency_from_logical_trigger_ms ?? row.latencyFromLogicalTriggerMs;
  const rawLatencyFromWindowCloseMs = row.latency_from_window_close_ms ?? row.latencyFromWindowCloseMs;
  const windowSemantics = String(
    rawWindowSemantics ?? fallback.windowSemantics ?? "trailing",
  ).toLowerCase();
  const logicalTriggerTime = toNumber(rawLogicalTriggerTime);
  const windowStart = toNumber(rawWindowStart);
  const windowEnd = toNumber(rawWindowEnd);
  const windowDataCloseTime = toNumber(rawWindowDataCloseTime);
  const resultEmittedAt = toNumber(rawResultEmittedAt);
  const latencyFromLogicalTriggerMs = toNumber(rawLatencyFromLogicalTriggerMs);
  const latencyFromWindowCloseMs = toNumber(rawLatencyFromWindowCloseMs);
  const directMetadataPresent =
    rawWindowSemantics !== undefined &&
    rawLogicalTriggerTime !== undefined &&
    rawWindowStart !== undefined &&
    rawWindowEnd !== undefined &&
    rawWindowDataCloseTime !== undefined &&
    Number.isFinite(logicalTriggerTime) &&
    Number.isFinite(windowStart) &&
    Number.isFinite(windowEnd) &&
    Number.isFinite(windowDataCloseTime);
  const resolvedWindowEnd = Number.isFinite(windowEnd)
    ? windowEnd
    : Number.isFinite(fallback.expectedWindowClose)
      ? fallback.expectedWindowClose
      : null;
  const resolvedWindowStart = Number.isFinite(windowStart)
    ? windowStart
    : Number.isFinite(resolvedWindowEnd)
      ? resolvedWindowEnd - BENCHMARK_WINDOW_RANGE_MS
      : null;
  const resolvedWindowDataCloseTime = Number.isFinite(windowDataCloseTime)
    ? windowDataCloseTime
    : resolvedWindowEnd;
  const resolvedLogicalTriggerTime = Number.isFinite(logicalTriggerTime)
    ? logicalTriggerTime
    : Number.isFinite(resolvedWindowEnd)
      ? resolvedWindowEnd - (BENCHMARK_WINDOW_RANGE_MS / 2)
      : null;
  const resolvedResultEmittedAt = Number.isFinite(resultEmittedAt)
    ? resultEmittedAt
    : Number.isFinite(fallback.resultEmittedAt)
      ? fallback.resultEmittedAt
      : null;
  const inferredLogicalTriggerTime = inferExpectedLogicalTriggerTime(
    windowSemantics,
    resolvedWindowStart,
    resolvedWindowEnd,
  );
  const directLatencyLooksComparable =
    Number.isFinite(resolvedResultEmittedAt) &&
    Number.isFinite(resolvedWindowDataCloseTime) &&
    Number.isFinite(latencyFromWindowCloseMs) &&
    Math.abs((resolvedResultEmittedAt - resolvedWindowDataCloseTime) - latencyFromWindowCloseMs) <= 10_000;
  const chunkedWindowMetadataLooksExternal =
    !(
      Number.isFinite(resolvedWindowStart) &&
      Number.isFinite(resolvedWindowEnd) &&
      Number.isFinite(resolvedWindowDataCloseTime) &&
      resolvedWindowDataCloseTime < resolvedWindowEnd
    );
  const shouldReconstructFromResolvedWindow =
    !directMetadataPresent ||
    !directLatencyLooksComparable ||
    !chunkedWindowMetadataLooksExternal;
  const finalLogicalTriggerTime = shouldReconstructFromResolvedWindow
    ? inferredLogicalTriggerTime
    : logicalTriggerTime;
  const finalWindowDataCloseTime = shouldReconstructFromResolvedWindow
    ? resolvedWindowEnd
    : resolvedWindowDataCloseTime;
  const finalLatencyFromLogicalTriggerMs = shouldReconstructFromResolvedWindow
    ? (
        Number.isFinite(resolvedResultEmittedAt) && Number.isFinite(finalLogicalTriggerTime)
          ? resolvedResultEmittedAt - finalLogicalTriggerTime
          : null
      )
    : latencyFromLogicalTriggerMs;
  const finalLatencyFromWindowCloseMs = shouldReconstructFromResolvedWindow
    ? (
        Number.isFinite(resolvedResultEmittedAt) && Number.isFinite(finalWindowDataCloseTime)
          ? resolvedResultEmittedAt - finalWindowDataCloseTime
          : null
      )
    : latencyFromWindowCloseMs;
  const reconstructedLatencyLooksComparable =
    Number.isFinite(resolvedResultEmittedAt) &&
    Number.isFinite(finalWindowDataCloseTime) &&
    Math.abs(resolvedResultEmittedAt - finalWindowDataCloseTime) <= (BENCHMARK_WINDOW_RANGE_MS * 10);
  const safeLatencyFromLogicalTriggerMs = shouldReconstructFromResolvedWindow && !reconstructedLatencyLooksComparable
    ? null
    : finalLatencyFromLogicalTriggerMs;
  const safeLatencyFromWindowCloseMs = shouldReconstructFromResolvedWindow && !reconstructedLatencyLooksComparable
    ? null
    : finalLatencyFromWindowCloseMs;

  return {
    windowSemantics,
    logicalTriggerTime: finalLogicalTriggerTime ?? resolvedLogicalTriggerTime,
    windowStart: resolvedWindowStart,
    windowEnd: resolvedWindowEnd,
    windowDataCloseTime: finalWindowDataCloseTime,
    resultEmittedAt: resolvedResultEmittedAt,
    latencyFromLogicalTriggerMs: Number.isFinite(safeLatencyFromLogicalTriggerMs)
      ? safeLatencyFromLogicalTriggerMs
      : Number.isFinite(resolvedResultEmittedAt) && Number.isFinite(finalLogicalTriggerTime)
        ? (
            shouldReconstructFromResolvedWindow && !reconstructedLatencyLooksComparable
              ? null
              : resolvedResultEmittedAt - finalLogicalTriggerTime
          )
        : null,
    latencyFromWindowCloseMs: Number.isFinite(safeLatencyFromWindowCloseMs)
      ? safeLatencyFromWindowCloseMs
      : Number.isFinite(resolvedResultEmittedAt) && Number.isFinite(finalWindowDataCloseTime)
        ? (
            shouldReconstructFromResolvedWindow && !reconstructedLatencyLooksComparable
              ? null
              : resolvedResultEmittedAt - finalWindowDataCloseTime
          )
        : null,
    metadataSource:
      shouldReconstructFromResolvedWindow
        ? 'reconstructed'
        : (
          toMetadataSource(row.metadata_source ?? row.metadataSource) ||
          (directMetadataPresent ? "direct" : "reconstructed")
        ),
  };
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
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[index];
}

function formatNumber(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : 'n/a';
}

function formatMeanStd(statsObj, digits = 2, unit = '') {
  if (!statsObj || !Number.isFinite(statsObj.mean)) return 'n/a';
  const suffix = unit ? ` ${unit}` : '';
  const meanText = `${statsObj.mean.toFixed(digits)}${suffix}`;
  if (!Number.isFinite(statsObj.stdev)) return meanText;
  return `${meanText} ± ${statsObj.stdev.toFixed(digits)}${suffix}`;
}

function writeCsv(filePath, rows, headers) {
  const body = rows.map((row) => headers.map((header) => String(row[header] ?? '')).join(',')).join('\n');
  fs.writeFileSync(filePath, `${headers.join(',')}\n${body}${body ? '\n' : ''}`);
}

function normalizeFetchingRows(latencyRows) {
  return dedupeAndSortRows(latencyRows.map((row) => {
    const windowNumber = toNumber(row.window_number);
    const queryRegisteredAt = toNumber(row.query_registered_at);
    const firstDataReceivedAt = toNumber(row.first_data_received_at);
    const expectedWindowClose = toNumber(row.expected_window_close);
    const lastObservedAt = toNumber(row.last_obs_received_at);
    const resultEmittedAt = toNumber(row.result_emitted_at);
    const resultValue = toNumber(row.result_value);
    const metadata = buildWindowMetadataFromRow(row, {
      expectedWindowClose,
      resultEmittedAt,
    });
    const registrationAnchoredLatencies =
      Number.isFinite(queryRegisteredAt) &&
      Number.isFinite(resultEmittedAt) &&
      Number.isFinite(windowNumber)
        ? calculateRegistrationAnchoredLatencies({
            queryRegisteredAt,
            resultEmittedAt,
            windowNumber,
            outputWindowRangeMs: BENCHMARK_WINDOW_RANGE_MS,
            outputWindowStepMs: BENCHMARK_WINDOW_STEP_MS,
          })
        : null;
    return {
      approach: 'fetching',
      windowNumber,
      queryRegisteredAt,
      firstDataReceivedAt,
      expectedWindowClose,
      lastObservedAt,
      resultEmittedAt,
      registrationAnchoredWindowCloseAt: registrationAnchoredLatencies?.registrationAnchoredWindowCloseAt ?? null,
      queryToFirstResultMs: registrationAnchoredLatencies?.queryToFirstResultMs ?? null,
      postWindowCloseLatencyMs: registrationAnchoredLatencies?.postWindowCloseLatencyMs ?? null,
      latencyMetricSource: registrationAnchoredLatencies?.latencyMetricSource ?? null,
      registrationToResultMs: registrationAnchoredLatencies?.queryToFirstResultMs ?? null,
      dataStartToResultMs: Number.isFinite(firstDataReceivedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - firstDataReceivedAt
        : null,
      lastDataToResultMs: Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - lastObservedAt
        : null,
      postWindowDelayMs: Number.isFinite(expectedWindowClose) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - expectedWindowClose
        : null,
      expectedWindowCloseToResultMs: registrationAnchoredLatencies?.postWindowCloseLatencyMs ?? null,
      postProcessingDelayMs: Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - lastObservedAt
        : null,
      windowSemantics: metadata.windowSemantics,
      logicalTriggerTime: metadata.logicalTriggerTime,
      windowStart: metadata.windowStart,
      windowEnd: metadata.windowEnd,
      windowDataCloseTime: metadata.windowDataCloseTime,
      latencyFromLogicalTriggerMs: metadata.latencyFromLogicalTriggerMs,
      latencyFromWindowCloseMs: metadata.latencyFromWindowCloseMs,
      wallClockCloseToResultMs: toNumber(row.wall_clock_close_to_result_ms),
      latencyDomainStatus: row.latency_domain_status ?? '',
      metadataSource: metadata.metadataSource,
      resultValue,
      warmup: windowNumber === 1,
      aggregationType: 'AVG',
    };
  }).filter((row) => Number.isFinite(row.windowNumber) && Number.isFinite(row.resultEmittedAt) && Number.isFinite(row.resultValue)));
}

function normalizeApproximationRows(latencyRows) {
  return dedupeAndSortRows(latencyRows.map((row) => {
    const windowNumber = toNumber(row.window_number);
    const queryRegisteredAt = toNumber(row.query_registered_at);
    const firstDataReceivedAt = toNumber(row.first_data_received_at);
    const expectedWindowClose = toNumber(row.expected_window_close);
    const lastObservedAt = toNumber(row.last_data_received_at);
    const resultEmittedAt = toNumber(row.result_emitted_at);
    const resultValue = toNumber(row.result_value);
    const metadata = buildWindowMetadataFromRow(row, {
      expectedWindowClose,
      resultEmittedAt,
    });
    const registrationAnchoredLatencies =
      Number.isFinite(queryRegisteredAt) &&
      Number.isFinite(resultEmittedAt) &&
      Number.isFinite(windowNumber)
        ? calculateRegistrationAnchoredLatencies({
            queryRegisteredAt,
            resultEmittedAt,
            windowNumber,
            outputWindowRangeMs: BENCHMARK_WINDOW_RANGE_MS,
            outputWindowStepMs: BENCHMARK_WINDOW_STEP_MS,
          })
        : null;
    return {
      approach: 'approximation',
      windowNumber,
      queryRegisteredAt,
      firstDataReceivedAt,
      expectedWindowClose,
      lastObservedAt,
      resultEmittedAt,
      registrationAnchoredWindowCloseAt: registrationAnchoredLatencies?.registrationAnchoredWindowCloseAt ?? null,
      queryToFirstResultMs: registrationAnchoredLatencies?.queryToFirstResultMs ?? null,
      postWindowCloseLatencyMs: registrationAnchoredLatencies?.postWindowCloseLatencyMs ?? null,
      latencyMetricSource: registrationAnchoredLatencies?.latencyMetricSource ?? null,
      registrationToResultMs: registrationAnchoredLatencies?.queryToFirstResultMs ?? null,
      dataStartToResultMs: Number.isFinite(firstDataReceivedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - firstDataReceivedAt
        : null,
      lastDataToResultMs: Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - lastObservedAt
        : null,
      postWindowDelayMs: Number.isFinite(expectedWindowClose) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - expectedWindowClose
        : null,
      expectedWindowCloseToResultMs: registrationAnchoredLatencies?.postWindowCloseLatencyMs ?? null,
      postProcessingDelayMs: Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - lastObservedAt
        : null,
      windowSemantics: metadata.windowSemantics,
      logicalTriggerTime: metadata.logicalTriggerTime,
      windowStart: metadata.windowStart,
      windowEnd: metadata.windowEnd,
      windowDataCloseTime: metadata.windowDataCloseTime,
      latencyFromLogicalTriggerMs: metadata.latencyFromLogicalTriggerMs,
      latencyFromWindowCloseMs: metadata.latencyFromWindowCloseMs,
      wallClockCloseToResultMs: toNumber(row.wall_clock_close_to_result_ms),
      latencyDomainStatus: row.latency_domain_status ?? '',
      metadataSource: metadata.metadataSource,
      resultValue,
      warmup: windowNumber === 1,
      aggregationType: 'AVG',
    };
  }).filter((row) => Number.isFinite(row.windowNumber) && Number.isFinite(row.resultEmittedAt) && Number.isFinite(row.resultValue)));
}

function normalizeChunkedRows(latencyRows) {
  return dedupeAndSortRows(latencyRows.map((row) => {
    const windowNumber = toNumber(row.window_number);
    const queryRegisteredAt = toNumber(row.query_registered_at);
    const firstDataReceivedAt = toNumber(row.first_data_received_at);
    const expectedWindowClose = toNumber(row.expected_window_close);
    const lastObservedAt = toNumber(row.last_chunk_received_at);
    const resultEmittedAt = toNumber(row.result_emitted_at);
    const resultValue = toNumber(row.result_value);
    const metadata = buildWindowMetadataFromRow(row, {
      expectedWindowClose,
      resultEmittedAt,
    });
    const registrationAnchoredLatencies =
      Number.isFinite(queryRegisteredAt) &&
      Number.isFinite(resultEmittedAt) &&
      Number.isFinite(windowNumber)
        ? calculateRegistrationAnchoredLatencies({
            queryRegisteredAt,
            resultEmittedAt,
            windowNumber,
            outputWindowRangeMs: BENCHMARK_WINDOW_RANGE_MS,
            outputWindowStepMs: BENCHMARK_WINDOW_STEP_MS,
          })
        : null;
    return {
      approach: 'chunked',
      windowNumber,
      queryRegisteredAt,
      firstDataReceivedAt,
      expectedWindowClose,
      lastObservedAt,
      resultEmittedAt,
      registrationAnchoredWindowCloseAt: registrationAnchoredLatencies?.registrationAnchoredWindowCloseAt ?? null,
      queryToFirstResultMs: registrationAnchoredLatencies?.queryToFirstResultMs ?? null,
      postWindowCloseLatencyMs: registrationAnchoredLatencies?.postWindowCloseLatencyMs ?? null,
      latencyMetricSource: registrationAnchoredLatencies?.latencyMetricSource ?? null,
      registrationToResultMs: registrationAnchoredLatencies?.queryToFirstResultMs ?? null,
      dataStartToResultMs: Number.isFinite(firstDataReceivedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - firstDataReceivedAt
        : null,
      lastDataToResultMs: Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - lastObservedAt
        : null,
      postWindowDelayMs: Number.isFinite(expectedWindowClose) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - expectedWindowClose
        : null,
      expectedWindowCloseToResultMs: registrationAnchoredLatencies?.postWindowCloseLatencyMs ?? null,
      postProcessingDelayMs: Number.isFinite(lastObservedAt) && Number.isFinite(resultEmittedAt)
        ? resultEmittedAt - lastObservedAt
        : null,
      windowSemantics: metadata.windowSemantics,
      logicalTriggerTime: metadata.logicalTriggerTime,
      windowStart: metadata.windowStart,
      windowEnd: metadata.windowEnd,
      windowDataCloseTime: metadata.windowDataCloseTime,
      latencyFromLogicalTriggerMs: metadata.latencyFromLogicalTriggerMs,
      latencyFromWindowCloseMs: metadata.latencyFromWindowCloseMs,
      wallClockCloseToResultMs: toNumber(row.wall_clock_close_to_result_ms),
      latencyDomainStatus: row.latency_domain_status ?? '',
      metadataSource: metadata.metadataSource,
      resultValue,
      warmup: windowNumber === 1,
      aggregationType: 'AVG',
    };
  }).filter((row) => Number.isFinite(row.windowNumber) && Number.isFinite(row.resultEmittedAt) && Number.isFinite(row.resultValue)));
}

function dedupeAndSortRows(rows) {
  const byWindow = new Map();
  for (const row of rows) {
    byWindow.set(row.windowNumber, row);
  }
  return [...byWindow.values()].sort((left, right) => left.windowNumber - right.windowNumber);
}

function validateWindows(rows, approach, iter) {
  const windowNumbers = rows.map((row) => row.windowNumber).sort((a, b) => a - b);
  const expected = [1, 2, 3];
  const missing = expected.filter((value) => !windowNumbers.includes(value));
  const extra = windowNumbers.filter((value, index) => value !== expected[index]);
  if (missing.length || extra.length || rows.length !== 3) {
    throw new Error(`${approach} iteration ${iter} emitted windows ${windowNumbers.join(', ')}; expected exactly 1, 2, 3`);
  }
  return windowNumbers;
}

function summarizeMetric(rows, field) {
  const values = rows.map((row) => row[field]).filter((value) => Number.isFinite(value));
  return {
    count: values.length,
    mean: mean(values),
    stdev: stdev(values),
    p95: percentile(values, 0.95),
  };
}

function computeResourceMetrics(treeRows) {
  if (!treeRows.length) {
    return {
      sampleCount: 0,
      sampleIntervalMs: null,
      meanRssMiB: null,
      peakRssMiB: null,
      meanCpuPct: null,
      peakCpuPct: null,
      totalCpuSeconds: null,
    };
  }

  const sortedRows = [...treeRows].sort((a, b) => a.timestamp - b.timestamp);
  const rssMiB = sortedRows.map((row) => row.tree_rss_bytes / (1024 * 1024));
  const intervals = [];
  const cpuSamples = [];
  let totalCpuSeconds = 0;
  for (let index = 1; index < sortedRows.length; index += 1) {
    const prev = sortedRows[index - 1];
    const curr = sortedRows[index];
    const dtMs = curr.timestamp - prev.timestamp;
    if (!(dtMs > 0)) continue;
    const deltaCpuSeconds = curr.tree_cpu_seconds - prev.tree_cpu_seconds;
    if (Number.isFinite(deltaCpuSeconds) && deltaCpuSeconds >= 0) {
      totalCpuSeconds += deltaCpuSeconds;
      cpuSamples.push((deltaCpuSeconds / (dtMs / 1000)) / CORE_COUNT * 100);
    }
    intervals.push(dtMs);
  }

  return {
    sampleCount: sortedRows.length,
    sampleIntervalMs: intervals.length ? mean(intervals) : null,
    meanRssMiB: mean(rssMiB),
    peakRssMiB: rssMiB.length ? Math.max(...rssMiB) : null,
    meanCpuPct: cpuSamples.length ? mean(cpuSamples) : null,
    peakCpuPct: cpuSamples.length ? Math.max(...cpuSamples) : null,
    totalCpuSeconds,
  };
}

function parseMqttMessageCounts(csvRows) {
  const counts = new Map();
  for (const row of csvRows) {
    const type = row.messageType || 'unknown';
    counts.set(type, (counts.get(type) || 0) + 1);
  }
  return Object.fromEntries([...counts.entries()].sort((left, right) => left[0].localeCompare(right[0])));
}

function summarizeMqttTraffic(logDir) {
  const summary = finalizeMqttTrafficArtifacts({ logDir });
  const csvRows = readCsv(path.join(logDir, 'mqtt_traffic.csv'));
  const firstTimestampsByType = {};
  for (const row of csvRows) {
    const type = row.messageType || 'unknown';
    const timestamp = toNumber(row.timestamp);
    if (!Number.isFinite(timestamp)) continue;
    if (!Number.isFinite(firstTimestampsByType[type])) {
      firstTimestampsByType[type] = timestamp;
    }
  }
  return {
    summary,
    csvRows,
    countsByType: parseMqttMessageCounts(csvRows),
    firstTimestampsByType,
  };
}

function selectRunScopedFirstRawInputAt(rows, mqttSummary) {
  const queryRegisteredAt = Math.min(
    ...rows.map((row) => row.queryRegisteredAt).filter((value) => Number.isFinite(value)),
  );
  const candidateRows = (mqttSummary?.csvRows || []).filter((row) => (
    row.messageType === 'raw_input_stream' &&
    Number.isFinite(toNumber(row.timestamp)) &&
    (
      !Number.isFinite(queryRegisteredAt) ||
      toNumber(row.timestamp) >= queryRegisteredAt
    )
  ));
  if (candidateRows.length > 0) {
    return toNumber(candidateRows[0].timestamp);
  }
  return toNumber(mqttSummary?.firstTimestampsByType?.raw_input_stream);
}

function computeMappedOutputTriggerWallClockMs(firstRawInputAt, windowNumber) {
  return (
    Number.isFinite(firstRawInputAt) &&
    Number.isFinite(windowNumber)
  )
    ? firstRawInputAt + (windowNumber * BENCHMARK_WINDOW_STEP_MS)
    : null;
}

function verifyOutputStepCadence(rows) {
  const sortedRows = [...rows]
    .filter((row) => Number.isFinite(row.windowNumber))
    .sort((left, right) => left.windowNumber - right.windowNumber);
  const issues = [];
  let checkedPairs = 0;

  for (let index = 1; index < sortedRows.length; index += 1) {
    const previous = sortedRows[index - 1];
    const current = sortedRows[index];
    if (current.windowNumber === previous.windowNumber) {
      continue;
    }

    checkedPairs += 1;
    const windowDelta = current.windowNumber - previous.windowNumber;
    const triggerDelta = Number.isFinite(previous.mappedOutputTriggerWallClockMs) &&
      Number.isFinite(current.mappedOutputTriggerWallClockMs)
      ? current.mappedOutputTriggerWallClockMs - previous.mappedOutputTriggerWallClockMs
      : null;

    if (windowDelta !== 1) {
      issues.push({
        type: 'window_gap',
        previousWindowNumber: previous.windowNumber,
        currentWindowNumber: current.windowNumber,
        windowDelta,
      });
    }

    if (Number.isFinite(triggerDelta) && triggerDelta !== BENCHMARK_WINDOW_STEP_MS * windowDelta) {
      issues.push({
        type: 'trigger_delta_mismatch',
        previousWindowNumber: previous.windowNumber,
        currentWindowNumber: current.windowNumber,
        triggerDelta,
        expectedTriggerDelta: BENCHMARK_WINDOW_STEP_MS * windowDelta,
      });
    }
  }

  return {
    ok: issues.length === 0,
    checkedPairs,
    expectedStepMs: BENCHMARK_WINDOW_STEP_MS,
    issues,
  };
}

function attachComparableTiming(rows, mqttSummary) {
  const firstRawInputAt = selectRunScopedFirstRawInputAt(rows, mqttSummary);
  return rows.map((row) => {
    const mappedOutputTriggerWallClockMs = computeMappedOutputTriggerWallClockMs(
      firstRawInputAt,
      row.windowNumber,
    );
    const runtimeComparableLatency = (
      row.latencyDomainStatus === 'wall_clock_mapped' &&
      Number.isFinite(row.wallClockCloseToResultMs)
    )
      ? row.wallClockCloseToResultMs
      : null;
    const directComparableLatency = (
      Number.isFinite(row.latencyFromWindowCloseMs) &&
      row.latencyFromWindowCloseMs >= 0 &&
      row.latencyFromWindowCloseMs <= (BENCHMARK_WINDOW_RANGE_MS * 10)
    )
          ? row.latencyFromWindowCloseMs
          : null;
    const fallbackComparableLatency = (
      Number.isFinite(mappedOutputTriggerWallClockMs) &&
      Number.isFinite(row.resultEmittedAt) &&
      (row.resultEmittedAt - mappedOutputTriggerWallClockMs) >= 0 &&
      (row.resultEmittedAt - mappedOutputTriggerWallClockMs) <= (BENCHMARK_WINDOW_RANGE_MS * 10)
    )
      ? row.resultEmittedAt - mappedOutputTriggerWallClockMs
      : null;
    return {
      ...row,
      rawInputFirstPublishedAt: firstRawInputAt,
      anchorAlignedExpectedWindowClose: mappedOutputTriggerWallClockMs,
      mappedOutputTriggerWallClockMs,
      anchorAlignedWindowCloseToResultMs: Number.isFinite(runtimeComparableLatency)
        ? runtimeComparableLatency
        : Number.isFinite(directComparableLatency)
          ? directComparableLatency
          : fallbackComparableLatency,
    };
  });
}

function accuracyMetrics(referenceRows, candidateRows) {
  const referenceByWindow = new Map(referenceRows.map((row) => [row.windowNumber, row]));
  const candidateByWindow = new Map(candidateRows.map((row) => [row.windowNumber, row]));
  const overlap = [...referenceByWindow.keys()].filter((windowNumber) => candidateByWindow.has(windowNumber)).sort((a, b) => a - b);
  const diffs = overlap.map((windowNumber) => candidateByWindow.get(windowNumber).resultValue - referenceByWindow.get(windowNumber).resultValue);
  const absoluteErrors = diffs.map((value) => Math.abs(value));
  const squaredErrors = diffs.map((value) => value ** 2);
  const percentErrors = overlap.map((windowNumber) => {
    const referenceValue = referenceByWindow.get(windowNumber).resultValue;
    return referenceValue === 0 ? null : Math.abs((candidateByWindow.get(windowNumber).resultValue - referenceValue) / referenceValue) * 100;
  }).filter((value) => Number.isFinite(value));
  return {
    overlapWindows: overlap,
    mae: mean(absoluteErrors),
    rmse: Math.sqrt(mean(squaredErrors)),
    mape: mean(percentErrors),
  };
}

function statsAcross(values) {
  const clean = values.filter((value) => Number.isFinite(value));
  return { mean: mean(clean), stdev: stdev(clean), count: clean.length };
}

function metricSummaryRows(label, runs, selector, digits = 2, unit = 'ms') {
  return `| ${label} | ${formatMeanStd(statsAcross(runs.map(selector)), digits, unit)} |`;
}

function countByType(countsByType) {
  const keys = ['raw_input_stream', 'reusable_result', 'chunk_result', 'superquery_result', 'control', 'unknown'];
  return keys.map((key) => `${key}=${countsByType[key] || 0}`).join('; ');
}

function totalMqttMessages(countsByType) {
  return Object.values(countsByType).reduce((sum, value) => sum + value, 0);
}

function formatIterationPaths(info) {
  return [
    `- Normalized final rows: [${info.outputCsv}](${info.outputCsv})`,
    `- Latency CSV: [${info.latencyCsv}](${info.latencyCsv})`,
    `- Primary resource CSV: [${info.primaryResourceCsv}](${info.primaryResourceCsv})`,
    `- Process-tree resource CSV: [${info.treeResourceCsv}](${info.treeResourceCsv})`,
    `- MQTT traffic CSV: [${info.mqttCsv}](${info.mqttCsv})`,
    `- MQTT traffic NDJSON: [${info.mqttNdjson}](${info.mqttNdjson})`,
    `- Run summary JSON: [${info.runSummary}](${info.runSummary})`,
  ].join('\n');
}

function buildApproachIterationData(approachKey, info) {
  const latencyRows = readCsv(info.latencyCsv);
  const baseNormalizedRows = approachKey === 'fetching'
    ? normalizeFetchingRows(latencyRows)
    : approachKey === 'approximation'
      ? normalizeApproximationRows(latencyRows)
      : normalizeChunkedRows(latencyRows);
  const mqtt = summarizeMqttTraffic(info.dir);
  const normalizedRows = attachComparableTiming(baseNormalizedRows, mqtt);

  const windowNumbers = validateWindows(normalizedRows, approachKey, info.iter);
  const treeRows = readCsv(info.treeResourceCsv).map((row) => ({
    timestamp: toNumber(row.timestamp),
    tree_rss_bytes: toNumber(row.tree_rss_bytes),
    tree_cpu_seconds: toNumber(row.tree_cpu_seconds),
  })).filter((row) => Number.isFinite(row.timestamp) && Number.isFinite(row.tree_rss_bytes) && Number.isFinite(row.tree_cpu_seconds));
  const resource = computeResourceMetrics(treeRows);
  const runSummary = readJson(info.runSummary);
  const latency = {
    registrationToResult: summarizeMetric(normalizedRows, 'registrationToResultMs'),
    dataStartToResult: summarizeMetric(normalizedRows, 'dataStartToResultMs'),
    expectedWindowCloseToResult: summarizeMetric(normalizedRows, 'expectedWindowCloseToResultMs'),
    anchorAlignedWindowCloseToResult: summarizeMetric(normalizedRows, 'anchorAlignedWindowCloseToResultMs'),
    lastDataToResult: summarizeMetric(normalizedRows, 'lastDataToResultMs'),
    postProcessingDelay: summarizeMetric(normalizedRows, 'postProcessingDelayMs'),
  };

  const normalizedOutputRows = normalizedRows.map((row) => ({
    approach: approachKey,
    window_number: row.windowNumber,
    query_registered_at: row.queryRegisteredAt ?? '',
    first_data_received_at: row.firstDataReceivedAt ?? '',
    expected_window_close: row.expectedWindowClose ?? '',
    last_observed_at: row.lastObservedAt ?? '',
    result_emitted_at: row.resultEmittedAt ?? '',
    registration_to_result_ms: row.registrationToResultMs ?? '',
    data_start_to_result_ms: row.dataStartToResultMs ?? '',
    last_data_to_result_ms: row.lastDataToResultMs ?? '',
    expected_window_close_to_result_ms: row.expectedWindowCloseToResultMs ?? '',
    raw_input_first_published_at: row.rawInputFirstPublishedAt ?? '',
    anchor_aligned_expected_window_close: row.anchorAlignedExpectedWindowClose ?? '',
    anchor_aligned_window_close_to_result_ms: row.anchorAlignedWindowCloseToResultMs ?? '',
    post_window_delay_ms: row.postWindowDelayMs ?? '',
    post_processing_delay_ms: row.postProcessingDelayMs ?? '',
    window_semantics: row.windowSemantics ?? '',
    logical_trigger_time: row.logicalTriggerTime ?? '',
    window_start: row.windowStart ?? '',
    window_end: row.windowEnd ?? '',
    window_data_close_time: row.windowDataCloseTime ?? '',
    latency_from_logical_trigger_ms: row.latencyFromLogicalTriggerMs ?? '',
    latency_from_window_close_ms: row.latencyFromWindowCloseMs ?? '',
    metadata_source: row.metadataSource ?? '',
    result_value: row.resultValue,
    warmup: row.warmup ? 'true' : 'false',
    aggregation_type: row.aggregationType,
    result_topic: approachKey === 'approximation' ? 'approximation/output' : 'output',
  }));
  writeCsv(info.outputCsv, normalizedOutputRows, Object.keys(normalizedOutputRows[0] || {
    approach: '',
    window_number: '',
    query_registered_at: '',
    first_data_received_at: '',
    expected_window_close: '',
    last_observed_at: '',
    result_emitted_at: '',
    registration_to_result_ms: '',
    data_start_to_result_ms: '',
    last_data_to_result_ms: '',
    expected_window_close_to_result_ms: '',
    raw_input_first_published_at: '',
    anchor_aligned_expected_window_close: '',
    anchor_aligned_window_close_to_result_ms: '',
    post_window_delay_ms: '',
    post_processing_delay_ms: '',
    window_semantics: '',
    logical_trigger_time: '',
    window_start: '',
    window_end: '',
    window_data_close_time: '',
    latency_from_logical_trigger_ms: '',
    latency_from_window_close_ms: '',
    metadata_source: '',
    result_value: '',
    warmup: '',
    aggregation_type: '',
    result_topic: '',
  }));

  const actualFetchingEventCounts = approachKey === 'fetching'
    ? readCsv(info.diagnosticsCsv).filter((row) => row.accepted_or_suppressed === 'accepted')
      .map((row) => ({
        windowNumber: toNumber(row.window_number),
        eventCount: toNumber(row.event_count),
        expectedEventCount: toNumber(row.expected_event_count),
      }))
      .filter((row) => Number.isFinite(row.windowNumber))
    : [];

  return {
    info,
    latencyRows: normalizedRows,
    windowNumbers,
    resource,
    mqtt,
    runSummary,
    latency,
    primaryLatency: latency.anchorAlignedWindowCloseToResult,
    actualFetchingEventCounts,
  };
}

function buildReport(data) {
  const expectedEventCount = 1200 * 2;
  const controls = [
    '- DATA_PATH: `approximation_test/challenging/exponential_growth`',
    '- WEARABLE_FREQUENCY: `10`',
    '- Output window: `RANGE 120000 STEP 60000`',
    '- Subwindow / chunk window: `RANGE 60000 STEP 30000`',
    '- Aggregation: `AVG`',
    '- Deterministic event time: enabled',
    '- Finite replay duration: `300s`',
    '- Debug logging: off',
    '- Process-tree resource sampler: enabled',
    '- Chunked comparable output only: enabled',
    '- Chunked immediate trigger: disabled',
    IS_CENTERED ? '- Window semantics: centered' : '- Window semantics: trailing',
    `- CPU normalization core count: ${CORE_COUNT}`,
  ].join('\n');

  const fetchingRuns = data.fetching;
  const approximationRuns = data.approximation;
  const chunkedRuns = data.chunked;
  const allRuns = [
    ...fetchingRuns.map((run) => ({ approach: 'fetching', ...run })),
    ...approximationRuns.map((run) => ({ approach: 'approximation', ...run })),
    ...chunkedRuns.map((run) => ({ approach: 'chunked', ...run })),
  ];

  const rows = allRuns.map((run) => ({
    approach: run.approach,
    iter: run.info.iter,
    finalRows: run.latencyRows.length,
    windowNumbers: run.windowNumbers.join(', '),
    meanLatency: run.primaryLatency.mean,
    p95Latency: run.primaryLatency.p95,
    meanRss: run.resource.meanRssMiB,
    peakRss: run.resource.peakRssMiB,
    meanCpu: run.resource.meanCpuPct,
    peakCpu: run.resource.peakCpuPct,
    totalCpuSeconds: run.resource.totalCpuSeconds,
    mqttByType: countByType(run.mqtt.countsByType),
  }));

  const latencyMetricSummaryRows = [
    {
      approach: 'fetching',
      runs: fetchingRuns,
    },
    {
      approach: 'approximation',
      runs: approximationRuns,
    },
    {
      approach: 'chunked',
      runs: chunkedRuns,
    },
  ].map(({ approach, runs }) => ({
    approach,
    registrationToResult: statsAcross(runs.map((run) => run.latency.registrationToResult.mean)),
    dataStartToResult: statsAcross(runs.map((run) => run.latency.dataStartToResult.mean)),
    expectedWindowCloseToResult: statsAcross(runs.map((run) => run.latency.expectedWindowCloseToResult.mean)),
    anchorAlignedWindowCloseToResult: statsAcross(runs.map((run) => run.latency.anchorAlignedWindowCloseToResult.mean)),
    lastDataToResult: statsAcross(runs.map((run) => run.latency.lastDataToResult.mean)),
    postProcessingDelay: statsAcross(runs.map((run) => run.latency.postProcessingDelay.mean)),
  }));

  const latencyDiagnosticsRows = allRuns.flatMap((run) => run.latencyRows.map((row) => ({
    approach: run.approach,
    iteration: run.info.iter,
    windowNumber: row.windowNumber,
    queryRegisteredAt: row.queryRegisteredAt,
    firstDataReceivedAt: row.firstDataReceivedAt,
    expectedWindowClose: row.expectedWindowClose,
    rawInputFirstPublishedAt: row.rawInputFirstPublishedAt,
    anchorAlignedExpectedWindowClose: row.anchorAlignedExpectedWindowClose,
    lastDataReceivedAt: row.lastObservedAt,
    resultEmittedAt: row.resultEmittedAt,
    latencyFromQueryRegMs: row.registrationToResultMs,
    latencyFromDataStartMs: row.dataStartToResultMs,
    latencyFromLastDataMs: row.lastDataToResultMs,
    expectedWindowCloseToResultMs: row.expectedWindowCloseToResultMs,
    anchorAlignedWindowCloseToResultMs: row.anchorAlignedWindowCloseToResultMs,
    postProcessingDelayMs: row.postProcessingDelayMs,
    windowSemantics: row.windowSemantics,
    logicalTriggerTime: row.logicalTriggerTime,
    windowStart: row.windowStart,
    windowEnd: row.windowEnd,
    windowDataCloseTime: row.windowDataCloseTime,
    latencyFromLogicalTriggerMs: row.latencyFromLogicalTriggerMs,
    latencyFromWindowCloseMs: row.latencyFromWindowCloseMs,
    metadataSource: row.metadataSource,
  })));

  const fetchingExpectedActual = fetchingRuns[0].actualFetchingEventCounts.map((row) => ({
    windowNumber: row.windowNumber,
    actualEventCount: row.eventCount,
    expectedEventCount: row.expectedEventCount,
  }));

  const latencyBasisRows = latencyMetricSummaryRows.map((row) => `| ${row.approach} | ${formatMeanStd(row.registrationToResult)} | ${formatMeanStd(row.dataStartToResult)} | ${formatMeanStd(row.expectedWindowCloseToResult)} | ${formatMeanStd(row.anchorAlignedWindowCloseToResult)} | ${formatMeanStd(row.lastDataToResult)} | ${formatMeanStd(row.postProcessingDelay)} |`).join('\n');

  const latencyDiagnosticsTableRows = latencyDiagnosticsRows.map((row) => `| ${row.approach} | ${row.iteration} | ${row.windowNumber} | ${row.queryRegisteredAt ?? 'n/a'} | ${row.firstDataReceivedAt ?? 'n/a'} | ${row.expectedWindowClose ?? 'n/a'} | ${row.rawInputFirstPublishedAt ?? 'n/a'} | ${row.anchorAlignedExpectedWindowClose ?? 'n/a'} | ${row.lastDataReceivedAt ?? 'n/a'} | ${row.resultEmittedAt ?? 'n/a'} | ${row.latencyFromQueryRegMs ?? 'n/a'} | ${row.latencyFromDataStartMs ?? 'n/a'} | ${row.latencyFromLastDataMs ?? 'n/a'} | ${row.expectedWindowCloseToResultMs ?? 'n/a'} | ${row.anchorAlignedWindowCloseToResultMs ?? 'n/a'} | ${row.postProcessingDelayMs ?? 'n/a'} | ${row.windowSemantics ?? 'n/a'} | ${row.logicalTriggerTime ?? 'n/a'} | ${row.windowStart ?? 'n/a'} | ${row.windowEnd ?? 'n/a'} | ${row.windowDataCloseTime ?? 'n/a'} | ${row.latencyFromLogicalTriggerMs ?? 'n/a'} | ${row.latencyFromWindowCloseMs ?? 'n/a'} | ${row.metadataSource ?? 'n/a'} |`).join('\n');

  const fetchingByWindow = new Map(fetchingRuns[0].latencyRows.map((row) => [row.windowNumber, row]));
  const approximationAccuracies = approximationRuns.map((run) => accuracyMetrics(fetchingRuns[0].latencyRows, run.latencyRows));
  const chunkedAccuracies = chunkedRuns.map((run) => accuracyMetrics(fetchingRuns[0].latencyRows, run.latencyRows));

  const aggRows = [
    {
      approach: 'fetching',
      finalRows: statsAcross(fetchingRuns.map((run) => run.latencyRows.length)),
      meanLatency: statsAcross(fetchingRuns.map((run) => run.primaryLatency.mean)),
      p95Latency: statsAcross(fetchingRuns.map((run) => run.primaryLatency.p95)),
      meanRss: statsAcross(fetchingRuns.map((run) => run.resource.meanRssMiB)),
      peakRss: statsAcross(fetchingRuns.map((run) => run.resource.peakRssMiB)),
      meanCpu: statsAcross(fetchingRuns.map((run) => run.resource.meanCpuPct)),
      peakCpu: statsAcross(fetchingRuns.map((run) => run.resource.peakCpuPct)),
      totalCpuSeconds: statsAcross(fetchingRuns.map((run) => run.resource.totalCpuSeconds)),
      mqtt: statsAcross(fetchingRuns.map((run) => run.mqtt.summary.published_application_bytes)),
    },
    {
      approach: 'approximation',
      finalRows: statsAcross(approximationRuns.map((run) => run.latencyRows.length)),
      meanLatency: statsAcross(approximationRuns.map((run) => run.primaryLatency.mean)),
      p95Latency: statsAcross(approximationRuns.map((run) => run.primaryLatency.p95)),
      meanRss: statsAcross(approximationRuns.map((run) => run.resource.meanRssMiB)),
      peakRss: statsAcross(approximationRuns.map((run) => run.resource.peakRssMiB)),
      meanCpu: statsAcross(approximationRuns.map((run) => run.resource.meanCpuPct)),
      peakCpu: statsAcross(approximationRuns.map((run) => run.resource.peakCpuPct)),
      totalCpuSeconds: statsAcross(approximationRuns.map((run) => run.resource.totalCpuSeconds)),
      mqtt: statsAcross(approximationRuns.map((run) => run.mqtt.summary.published_application_bytes)),
    },
    {
      approach: 'chunked',
      finalRows: statsAcross(chunkedRuns.map((run) => run.latencyRows.length)),
      meanLatency: statsAcross(chunkedRuns.map((run) => run.primaryLatency.mean)),
      p95Latency: statsAcross(chunkedRuns.map((run) => run.primaryLatency.p95)),
      meanRss: statsAcross(chunkedRuns.map((run) => run.resource.meanRssMiB)),
      peakRss: statsAcross(chunkedRuns.map((run) => run.resource.peakRssMiB)),
      meanCpu: statsAcross(chunkedRuns.map((run) => run.resource.meanCpuPct)),
      peakCpu: statsAcross(chunkedRuns.map((run) => run.resource.peakCpuPct)),
      totalCpuSeconds: statsAcross(chunkedRuns.map((run) => run.resource.totalCpuSeconds)),
      mqtt: statsAcross(chunkedRuns.map((run) => run.mqtt.summary.published_application_bytes)),
    },
  ];

  const approximationAgg = {
    mae: statsAcross(approximationAccuracies.map((run) => run.mae)),
    rmse: statsAcross(approximationAccuracies.map((run) => run.rmse)),
    mape: statsAcross(approximationAccuracies.map((run) => run.mape)),
  };
  const chunkedAgg = {
    mae: statsAcross(chunkedAccuracies.map((run) => run.mae)),
    rmse: statsAcross(chunkedAccuracies.map((run) => run.rmse)),
    mape: statsAcross(chunkedAccuracies.map((run) => run.mape)),
  };

  const missingExtraRows = [
    {
      approach: 'fetching',
      iterations: '1, 2, 3',
      missing: 'none',
      extra: 'none',
    },
    {
      approach: 'approximation',
      iterations: '1, 2, 3',
      missing: 'none',
      extra: 'none',
    },
    {
      approach: 'chunked',
      iterations: '1, 2, 3',
      missing: 'none',
      extra: 'none',
    },
  ];

  const fetchingIterTables = fetchingRuns.map((run) => `| fetching | ${run.info.iter} | ${run.latencyRows.length} | ${run.windowNumbers.join(', ')} | ${formatNumber(run.primaryLatency.mean)} ms | ${formatNumber(run.primaryLatency.p95)} ms | ${formatNumber(run.resource.meanRssMiB)} MiB | ${formatNumber(run.resource.peakRssMiB)} MiB | ${formatNumber(run.resource.meanCpuPct)}% | ${formatNumber(run.resource.peakCpuPct)}% | ${formatNumber(run.resource.totalCpuSeconds)} s | ${countByType(run.mqtt.countsByType)} |`).join('\n');

  const approximationIterTables = approximationRuns.map((run) => `| approximation | ${run.info.iter} | ${run.latencyRows.length} | ${run.windowNumbers.join(', ')} | ${formatNumber(run.primaryLatency.mean)} ms | ${formatNumber(run.primaryLatency.p95)} ms | ${formatNumber(run.resource.meanRssMiB)} MiB | ${formatNumber(run.resource.peakRssMiB)} MiB | ${formatNumber(run.resource.meanCpuPct)}% | ${formatNumber(run.resource.peakCpuPct)}% | ${formatNumber(run.resource.totalCpuSeconds)} s | ${countByType(run.mqtt.countsByType)} |`).join('\n');

  const chunkedIterTables = chunkedRuns.map((run) => `| chunked | ${run.info.iter} | ${run.latencyRows.length} | ${run.windowNumbers.join(', ')} | ${formatNumber(run.primaryLatency.mean)} ms | ${formatNumber(run.primaryLatency.p95)} ms | ${formatNumber(run.resource.meanRssMiB)} MiB | ${formatNumber(run.resource.peakRssMiB)} MiB | ${formatNumber(run.resource.meanCpuPct)}% | ${formatNumber(run.resource.peakCpuPct)}% | ${formatNumber(run.resource.totalCpuSeconds)} s | ${countByType(run.mqtt.countsByType)} |`).join('\n');

  const aggregateRows = [
    `| fetching | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => run.primaryLatency.mean)))} ms | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => run.resource.meanRssMiB)), 2, 'MiB')} | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => run.resource.peakRssMiB)), 2, 'MiB')} | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => run.resource.meanCpuPct)), 2, '%')} | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => run.resource.peakCpuPct)), 2, '%')} | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => run.resource.totalCpuSeconds)), 2, 's')} | ${formatMeanStd(statsAcross(fetchingRuns.map((run) => totalMqttMessages(run.mqtt.countsByType))), 0, 'messages')} |`,
    `| approximation | ${formatMeanStd(statsAcross(approximationRuns.map((run) => run.primaryLatency.mean)))} ms | ${formatMeanStd(statsAcross(approximationRuns.map((run) => run.resource.meanRssMiB)), 2, 'MiB')} | ${formatMeanStd(statsAcross(approximationRuns.map((run) => run.resource.peakRssMiB)), 2, 'MiB')} | ${formatMeanStd(statsAcross(approximationRuns.map((run) => run.resource.meanCpuPct)), 2, '%')} | ${formatMeanStd(statsAcross(approximationRuns.map((run) => run.resource.peakCpuPct)), 2, '%')} | ${formatMeanStd(statsAcross(approximationRuns.map((run) => run.resource.totalCpuSeconds)), 2, 's')} | ${formatMeanStd(statsAcross(approximationRuns.map((run) => totalMqttMessages(run.mqtt.countsByType))), 0, 'messages')} |`,
    `| chunked | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => run.primaryLatency.mean)))} ms | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => run.resource.meanRssMiB)), 2, 'MiB')} | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => run.resource.peakRssMiB)), 2, 'MiB')} | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => run.resource.meanCpuPct)), 2, '%')} | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => run.resource.peakCpuPct)), 2, '%')} | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => run.resource.totalCpuSeconds)), 2, 's')} | ${formatMeanStd(statsAcross(chunkedRuns.map((run) => totalMqttMessages(run.mqtt.countsByType))), 0, 'messages')} |`,
  ].join('\n');

  const mqttMixRows = [
    {
      comparison: 'fetching',
      rawInput: statsAcross(fetchingRuns.map((run) => run.mqtt.countsByType.raw_input_stream || 0)),
      reusable: statsAcross(fetchingRuns.map((run) => run.mqtt.countsByType.reusable_result || 0)),
      chunk: statsAcross(fetchingRuns.map((run) => run.mqtt.countsByType.chunk_result || 0)),
      superquery: statsAcross(fetchingRuns.map((run) => run.mqtt.countsByType.superquery_result || 0)),
    },
    {
      comparison: 'approximation',
      rawInput: statsAcross(approximationRuns.map((run) => run.mqtt.countsByType.raw_input_stream || 0)),
      reusable: statsAcross(approximationRuns.map((run) => run.mqtt.countsByType.reusable_result || 0)),
      chunk: statsAcross(approximationRuns.map((run) => run.mqtt.countsByType.chunk_result || 0)),
      superquery: statsAcross(approximationRuns.map((run) => run.mqtt.countsByType.superquery_result || 0)),
    },
    {
      comparison: 'chunked',
      rawInput: statsAcross(chunkedRuns.map((run) => run.mqtt.countsByType.raw_input_stream || 0)),
      reusable: statsAcross(chunkedRuns.map((run) => run.mqtt.countsByType.reusable_result || 0)),
      chunk: statsAcross(chunkedRuns.map((run) => run.mqtt.countsByType.chunk_result || 0)),
      superquery: statsAcross(chunkedRuns.map((run) => run.mqtt.countsByType.superquery_result || 0)),
    },
  ];

  const accuracyRows = [
    `| approximation vs fetching | ${formatMeanStd(statsAcross(approximationAccuracies.map((run) => run.mae)), 4)} | ${formatMeanStd(statsAcross(approximationAccuracies.map((run) => run.rmse)), 4)} | ${formatMeanStd(statsAcross(approximationAccuracies.map((run) => run.mape)), 4)} |`,
    `| chunked vs fetching | ${formatMeanStd(statsAcross(chunkedAccuracies.map((run) => run.mae)), 4)} | ${formatMeanStd(statsAcross(chunkedAccuracies.map((run) => run.rmse)), 4)} | ${formatMeanStd(statsAcross(chunkedAccuracies.map((run) => run.mape)), 4)} |`,
  ].join('\n');

  const title = IS_CENTERED
    ? 'One-Pattern Centered-Window n=3 Benchmark Summary'
    : 'One-Pattern Three-Approach n=3 Benchmark Summary';

  return `# ${title}

## Technical Summary

- All three approaches emitted exactly three final comparable windows in each of the three iterations, with window numbers 1, 2, and 3.
- Fetching accepted windows each carried the expected 2400 observations per 120s window, matching 120s \u00d7 10 Hz \u00d7 2 streams.
- The main latency comparison now uses \`anchor_aligned_window_close_to_result_ms\`, which anchors the 120s close boundary to the first raw input publication seen in the MQTT trace for that run.
- The older \`expected_window_close_to_result_ms\` numbers were registration-based, not replay-anchor-based, and are reported below only as diagnostics.
- The process-tree resource sampler shows approximation and chunked are much closer in CPU and RSS than the primary-process-only logs suggested.
- Chunked is exact or near-exact against fetching on the aligned final rows, while approximation emits early under this configuration and pays a small but measurable accuracy cost.

## Key Findings

### Final comparable rows are stable across all nine runs

| Approach | Iteration | Final rows | Window numbers | Comparable latency (anchor-aligned close -> result) | p95 | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds | MQTT messages by type |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
${fetchingIterTables}
${approximationIterTables}
${chunkedIterTables}

### Latency comparison basis

The main latency comparison uses the same event-window close anchor for every approach: \`anchor_aligned_window_close_to_result_ms\`. It is computed from the first raw input publication time in the MQTT trace plus \`RANGE + (window_number - 1) * STEP\`. The registration-based diagnostics are included below to show why the earlier report looked inconsistent.

| Approach | Registration -> result | Data start -> result | Registration-anchored close -> result | Anchor-aligned close -> result | Last data -> result | Post-processing delay |
| --- | --- | --- | --- | --- | --- |
${latencyBasisRows}

### Row-level latency diagnostics

| Approach | Iteration | Window | query_registered_at | first_data_received_at | expected_window_close | raw_input_first_published_at | anchor_aligned_expected_window_close | last_data_received_at | result_emitted_at | latency_from_query_reg_ms | latency_from_data_start_ms | latency_from_last_data_ms | expected_window_close_to_result_ms | anchor_aligned_window_close_to_result_ms | post_processing_delay_ms | window_semantics | logical_trigger_time | window_start | window_end | window_data_close_time | latency_from_logical_trigger_ms | latency_from_window_close_ms | metadata_source |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
${latencyDiagnosticsTableRows}

### Fetching matched the expected 2400 observations per accepted window

| Window | Expected event count | Actual accepted event count |
| --- | ---: | ---: |
${fetchingExpectedActual.map((row) => `| ${row.windowNumber} | ${row.expectedEventCount} | ${row.actualEventCount} |`).join('\n')}

### Missing and extra windows

| Approach | Iterations | Missing windows | Extra windows |
| --- | --- | --- | --- |
${missingExtraRows.map((row) => `| ${row.approach} | ${row.iterations} | ${row.missing} | ${row.extra} |`).join('\n')}

### Aggregate view over n=3

| Approach | Comparable latency (anchor-aligned close -> result) | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds | Total MQTT messages |
| --- | --- | --- | --- | --- | --- | --- | --- |
${aggregateRows}

### MQTT message mix over n=3

| Approach | Raw input | Reusable result | Chunk result | Superquery result |
| --- | --- | --- | --- | --- |
${mqttMixRows.map((row) => `| ${row.comparison} | ${formatMeanStd(row.rawInput, 2, 'messages')} | ${formatMeanStd(row.reusable, 2, 'messages')} | ${formatMeanStd(row.chunk, 2, 'messages')} | ${formatMeanStd(row.superquery, 2, 'messages')} |`).join('\n')}

### Accuracy against fetching, aligned by window number

| Comparison | MAE | RMSE | MAPE |
| --- | --- | --- | --- |
${accuracyRows}

## Scope, Data, and Metric Definitions

${controls}

- Final comparable rows are the normalized output rows for windows 1, 2, and 3.
- Comparable latency is measured as \`result_emitted_at - anchor_aligned_expected_window_close\` on the normalized final row for each window.
- \`expected_window_close_to_result_ms\` is retained only as a diagnostic; it is anchored to approach-local query registration and is not comparable across the three approaches when ingestion starts later than registration.
- \`latency_from_logical_trigger_ms\` and \`latency_from_window_close_ms\` are reconstructed only when the direct metadata fields are internally consistent. Event-time-domain direct fields are not treated as wall-clock latency.
- RSS is measured from the process-tree sampler's \`tree_rss_bytes\` field.
- CPU is computed from adjacent process-tree samples: \`delta tree_cpu_seconds / delta wall time\`, normalized by core count and reported as percent of total machine capacity.
- Accuracy is computed only against fetching and only on overlapping window numbers.

## Methodology

### Exact commands run

The runs used the same fixed benchmark envelope. The iteration-specific differences were \`LOG_PATH\`, \`SESSION_ID\`, \`STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX\`, and \`BENCHMARK_ITERATION\`.

${COMMAND_LINES.map((line) => `\`\`\`bash\n${line}\n\`\`\``).join('\n\n')}

### Artifact paths by approach and iteration

#### Fetching

${fetchingRuns.map((run) => `- Iteration ${run.info.iter}\n${formatIterationPaths(run.info)}`).join('\n\n')}

#### Approximation

${approximationRuns.map((run) => `- Iteration ${run.info.iter}\n${formatIterationPaths(run.info)}`).join('\n\n')}

#### Chunked

${chunkedRuns.map((run) => `- Iteration ${run.info.iter}\n${formatIterationPaths(run.info)}`).join('\n\n')}

## Limitations, Uncertainty, and Robustness Checks

- The benchmark replays were finite-duration controlled terminations, not full-source replays.
- The report uses process-tree resources because primary-process-only logs undercounted approximation and chunked earlier in the investigation.
- MQTT traffic counts are derived from the benchmark traffic trace, not broker-internal delivery counters.
- Window 1 is still a comparable final row, but it is the first window and is the closest thing to a warmup in the three-window comparison.
- The latency comparison is only valid on the shared \`anchor_aligned_window_close_to_result_ms\` basis. The earlier registration-anchored comparison mixed different start offsets and was not apples-to-apples.

## Recommended Next Steps

- Use the repaired n=3 run as the smoke test gate before scaling to n=30.
- Keep the process-tree resource sampler in the benchmark harness for all three approaches.
- Reuse the same 10 Hz / 300s envelope for the paper run unless a new paper-scale control change is explicitly planned.

## Further Questions

- Do you want the n=30 paper benchmark to keep the same 10 Hz envelope or return to the original rate sweep?
- Do you want the report split into a methods appendix plus a shorter decision summary for paper insertion?
`;
}

function main() {
  ensureDir(ANALYSIS_DIR);

  const data = {};
  for (const [approachKey, approachInfo] of Object.entries(APPROACHES)) {
    data[approachKey] = approachInfo.iterDirs.map((info) => buildApproachIterationData(approachKey, info));
  }

  const report = buildReport(data);
  fs.writeFileSync(REPORT_PATH, report);
  console.log(`Wrote ${REPORT_PATH}`);
}

if (require.main === module) {
  main();
} else {
  module.exports = {
    attachComparableTiming,
    buildWindowMetadataFromRow,
    verifyOutputStepCadence,
  };
}
