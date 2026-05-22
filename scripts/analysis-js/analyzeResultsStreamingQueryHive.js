#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const csvParse = require('csv-parse/sync');
const { getReplayMetadata } = require('../../experiments/utils/benchmarkResultMetadata');

const LOGS_DIR = process.env.STREAMING_QUERY_HIVE_LOGS_DIR || 'logs/streaming-query-hive';
const OUT_CSV = 'streaming_query_hive_latency_summary.csv';
const ITERATIONS = Number.parseInt(process.env.STREAMING_QUERY_HIVE_ITERATIONS || '1', 10) || 1;
const NUM_CORES = Number.parseInt(process.env.STREAMING_QUERY_HIVE_NUM_CORES || '10', 10) || 10;
const ENV_DEBUG_CHUNKS = process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === '1';
const DEFAULT_DATA_PATH = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';

function printUsage(errorMessage = '') {
  if (errorMessage) {
    console.error(errorMessage);
    console.error('');
  }
  console.error('Usage: node scripts/analysis-js/analyzeResultsStreamingQueryHive.js \\');
  console.error('  --data-path <path> \\');
  console.error('  --iteration <number|string> \\');
  console.error('  --chunked-csv <path> \\');
  console.error('  --stdout-log <path> \\');
  console.error('  --metadata <path> \\');
  console.error('  --frequency <number> \\');
  console.error('  [--debug-chunks true|false]');
  console.error('');
  console.error('Environment fallbacks: DATA_PATH, WEARABLE_FREQUENCY,');
  console.error('  STREAMING_QUERY_HIVE_LOGS_DIR, STREAMING_QUERY_HIVE_ITERATIONS,');
  console.error('  STREAMING_QUERY_HIVE_NUM_CORES, STREAMING_QUERY_HIVE_DEBUG_CHUNKS,');
  console.error('  STREAMING_QUERY_HIVE_ITERATION, STREAMING_QUERY_HIVE_CHUNKED_CSV,');
  console.error('  STREAMING_QUERY_HIVE_STDOUT_LOG, STREAMING_QUERY_HIVE_METADATA.');
}

function parseBooleanFlag(value) {
  if (value === undefined || value === null || value === '') {
    return true;
  }

  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }
  return null;
}

function parseCliArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === '--help' || token === '-h') {
      args.help = true;
      continue;
    }

    if (!token.startsWith('--')) {
      continue;
    }

    const eqIndex = token.indexOf('=');
    const name = token.slice(2, eqIndex > -1 ? eqIndex : undefined);
    let value = eqIndex > -1 ? token.slice(eqIndex + 1) : undefined;

    if (value === undefined && i + 1 < argv.length && !argv[i + 1].startsWith('--')) {
      value = argv[i + 1];
      i += 1;
    }

    switch (name) {
      case 'data-path':
        args.dataPath = value;
        break;
      case 'iteration':
        args.iteration = value;
        break;
      case 'chunked-csv':
        args.chunkedCsv = value;
        break;
      case 'stdout-log':
        args.stdoutLog = value;
        break;
      case 'metadata':
        args.metadata = value;
        break;
      case 'frequency':
        args.frequency = value;
        break;
      case 'debug-chunks': {
        const parsed = parseBooleanFlag(value);
        if (parsed === null) {
          args.debugChunks = null;
        } else {
          args.debugChunks = parsed;
        }
        break;
      }
      default:
        break;
    }
  }

  return args;
}

function resolveDataDir(dataPath) {
  if (path.isAbsolute(dataPath)) {
    return dataPath;
  }

  if (fs.existsSync(dataPath)) {
    return dataPath;
  }

  const repoRelative = path.join('src/streamer/data', dataPath);
  return repoRelative;
}

function normalizeIterationLabel(iteration) {
  if (String(iteration).startsWith('iteration')) {
    return String(iteration);
  }

  return `iteration${iteration}`;
}

function resolvePathForIteration(explicitPath, fallbackName, iterationLabel) {
  if (explicitPath) {
    return explicitPath;
  }

  return path.join(LOGS_DIR, normalizeIterationLabel(iterationLabel), fallbackName);
}

function parseFrequencyValue(rawFrequency, dataPath) {
  const parsed = Number.parseFloat(rawFrequency);
  if (Number.isFinite(parsed)) {
    return parsed;
  }

  const fromPath = /freq_([0-9.]+)/.exec(dataPath || '');
  if (fromPath && Number.isFinite(Number.parseFloat(fromPath[1]))) {
    return Number.parseFloat(fromPath[1]);
  }

  return null;
}

function resolveAnalyzerConfig() {
  const cliArgs = parseCliArgs(process.argv.slice(2));
  if (cliArgs.help) {
    printUsage();
    process.exit(0);
  }

  const dataPath =
    cliArgs.dataPath !== undefined && cliArgs.dataPath !== null
      ? cliArgs.dataPath
      : process.env.DATA_PATH || DEFAULT_DATA_PATH;
  const frequencyRaw =
    cliArgs.frequency !== undefined && cliArgs.frequency !== null
      ? cliArgs.frequency
      : process.env.WEARABLE_FREQUENCY || '';
  const frequency = parseFrequencyValue(frequencyRaw, dataPath);
  const debugChunksRaw = cliArgs.debugChunks;
  const debugChunks =
    debugChunksRaw === undefined || debugChunksRaw === null
      ? ENV_DEBUG_CHUNKS
      : debugChunksRaw;

  const explicitIteration =
    cliArgs.iteration !== undefined && cliArgs.iteration !== null
      ? cliArgs.iteration
      : process.env.STREAMING_QUERY_HIVE_ITERATION ||
    null;

  const iterPathLabel = explicitIteration !== null ? normalizeIterationLabel(explicitIteration) : null;
  const mode = explicitIteration !== null ? 'single' : 'loop';

  return {
    dataPath,
    dataDir: resolveDataDir(dataPath),
    frequency,
    debugChunks,
    mode,
    explicitIteration,
    chunkedCsvPath: cliArgs.chunkedCsv || process.env.STREAMING_QUERY_HIVE_CHUNKED_CSV || null,
    stdoutLogPath: cliArgs.stdoutLog || process.env.STREAMING_QUERY_HIVE_STDOUT_LOG || null,
    metadataPath: cliArgs.metadata || process.env.STREAMING_QUERY_HIVE_METADATA || null,
    iterPathLabel,
    rawIteration: explicitIteration,
  };
}

const ANALYZER_CONFIG = resolveAnalyzerConfig();
const BASE_DATA_DIR = ANALYZER_CONFIG.dataDir;

function requireSingleIterationInputs(config) {
  const missing = [];

  if (!config.dataPath) {
    missing.push('--data-path');
  }
  if (!Number.isFinite(config.frequency)) {
    missing.push('--frequency');
  }
  if (!config.chunkedCsvPath) {
    missing.push('--chunked-csv');
  }
  if (!config.stdoutLogPath) {
    missing.push('--stdout-log');
  }
  if (!config.metadataPath) {
    missing.push('--metadata');
  }
  if (config.rawIteration === null || config.rawIteration === undefined) {
    missing.push('--iteration');
  }

  return missing;
}

function parseCsvLog(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }

  let content = fs.readFileSync(filePath, 'utf8');
  content = content
    .split('\n')
    .filter((line, idx) => line.trim() !== 'timestamp,message' || idx === 0)
    .join('\n')
    .replace(/\\"/g, '""');

  try {
    return csvParse.parse(content, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      skip_records_with_error: true,
    });
  } catch (error) {
    console.warn(`Error parsing CSV ${filePath}: ${error.message}`);
    return [];
  }
}

function parseNumeric(value) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseChunkGroupId(chunkGroupId) {
  const match = String(chunkGroupId).match(/^(.*):(\d+):(\d+)$/);
  if (!match) {
    return null;
  }

  const prefix = match[1];
  const firstColon = prefix.indexOf(':');
  if (firstColon < 0) {
    return null;
  }

  return {
    queryId: prefix.slice(0, firstColon),
    windowName: prefix.slice(firstColon + 1),
    windowStart: Number.parseInt(match[2], 10),
    windowEnd: Number.parseInt(match[3], 10),
  };
}

function parseChunkId(chunkId) {
  const match = String(chunkId).match(/^(.*):(\d+):(\d+):([^:]+)$/);
  if (!match) {
    return null;
  }

  const prefix = match[1];
  const firstColon = prefix.indexOf(':');
  if (firstColon < 0) {
    return null;
  }

  return {
    queryId: prefix.slice(0, firstColon),
    windowName: prefix.slice(firstColon + 1),
    windowStart: Number.parseInt(match[2], 10),
    windowEnd: Number.parseInt(match[3], 10),
    subqueryId: match[4],
  };
}

function collectNtFiles(rootDir) {
  if (!fs.existsSync(rootDir)) {
    return [];
  }

  const files = [];
  const stack = [rootDir];

  while (stack.length > 0) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.nt')) {
        files.push(fullPath);
      }
    }
  }

  return files;
}

function extractObservationsFromNt(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const obsBlocks = new Map();

  for (const line of content.split('\n')) {
    const subjMatch = line.match(/^<([^>]+)>/);
    if (!subjMatch) continue;

    const subj = subjMatch[1];
    if (!obsBlocks.has(subj)) {
      obsBlocks.set(subj, {});
    }
    const block = obsBlocks.get(subj);

    const tsMatch = line.match(/hasTimestamp[^"]*"([^"]+)"/);
    const valMatch = line.match(/hasValue[^"]*"([^"]+)"/);
    if (tsMatch) {
      block.ts = Date.parse(tsMatch[1]);
    }
    if (valMatch) {
      block.val = Number.parseFloat(valMatch[1]);
    }
  }

  return Array.from(obsBlocks.values())
    .filter((d) => Number.isFinite(d.ts) && Number.isFinite(d.val))
    .map((d) => ({ ts: d.ts, val: d.val }));
}

function loadBaselineObservations(baseDir) {
  const files = collectNtFiles(baseDir);
  const observations = [];

  for (const file of files) {
    observations.push(...extractObservationsFromNt(file));
  }

  observations.sort((a, b) => a.ts - b.ts);
  return observations;
}

function aggregate(values, aggregation) {
  if (values.length === 0) {
    return null;
  }

  switch (aggregation) {
    case 'AVG':
      return values.reduce((sum, value) => sum + value, 0) / values.length;
    case 'SUM':
      return values.reduce((sum, value) => sum + value, 0);
    case 'COUNT':
      return values.length;
    case 'MIN':
      return Math.min(...values);
    case 'MAX':
      return Math.max(...values);
    default:
      return null;
  }
}

function getAggregation() {
  return (
    process.env.AGGREGATION_FUNCTION ||
    process.env.AGGREGATION_FUNC ||
    'AVG'
  ).toUpperCase();
}

function buildBaselineResult(observations, windowStart, windowEnd, aggregation) {
  const values = observations
    .filter((entry) => entry.ts >= windowStart && entry.ts < windowEnd)
    .map((entry) => entry.val);

  return aggregate(values, aggregation);
}

function buildWindowState(records, stdoutLines) {
  const windows = new Map();
  const subqueryDetailsByChunkId = new Map();
  let ignoredLegacyChunkCount = 0;

  for (const line of stdoutLines) {
    const debugMatch = line.match(
      /\[DEBUG_CHUNKS\] subquery subqueryId=([^ ]+) windowName=(.*?) windowStart=(\d+) windowEnd=(\d+) chunkId=([^ ]+) value=([+-]?\d+(?:\.\d+)?(?:e[+-]?\d+)?) count=(\d+)/i,
    );
    if (debugMatch) {
      subqueryDetailsByChunkId.set(debugMatch[5], {
        subqueryId: debugMatch[1],
        windowName: debugMatch[2],
        windowStart: Number.parseInt(debugMatch[3], 10),
        windowEnd: Number.parseInt(debugMatch[4], 10),
        chunkId: debugMatch[5],
        value: Number.parseFloat(debugMatch[6]),
        count: Number.parseInt(debugMatch[7], 10),
      });
    }
  }

  for (const row of records) {
    const message = row.message || '';

    const ignoredMatch = message.match(
      /ignored legacy\/unstructured chunk on topic=.*; ignoredLegacyChunkCount=(\d+)/,
    );
    if (ignoredMatch) {
      ignoredLegacyChunkCount = Number.parseInt(ignoredMatch[1], 10);
    }

    const receivedMatch = message.match(
      /received chunkId=([^;]+); subqueryId=([^;]+); chunkGroupId=([^;]+); completeness=([^;]+); missing=(.+)$/,
    );
    if (receivedMatch) {
      const chunkId = receivedMatch[1].trim();
      const subqueryId = receivedMatch[2].trim();
      const chunkGroupId = receivedMatch[3].trim();
      const completeness = receivedMatch[4].trim();
      const missingRaw = receivedMatch[5].trim();
      const windowInfo = parseChunkGroupId(chunkGroupId);
      if (!windowInfo) {
        continue;
      }

      const windowKey = chunkGroupId;
      if (!windows.has(windowKey)) {
        windows.set(windowKey, {
          chunkGroupId,
          queryId: windowInfo.queryId,
          windowName: windowInfo.windowName,
          windowStart: windowInfo.windowStart,
          windowEnd: windowInfo.windowEnd,
          includedSubqueryIds: new Set(),
          receivedSubqueries: new Map(),
          chunkedResult: null,
          windowState: 'incomplete',
          recomposedLineSeen: false,
        });
      }

      const window = windows.get(windowKey);
      window.includedSubqueryIds.add(subqueryId);
      window.receivedSubqueries.set(subqueryId, {
        subqueryId,
        chunkId,
        completeness,
        missingSubqueryIds:
          missingRaw === 'none'
            ? []
            : missingRaw
                .split(',')
                .map((part) => part.trim())
                .filter(Boolean),
      });

      continue;
    }

    const includedMatch = message.match(
      /final emission chunkGroupId=([^;]+); includedSubqueries=([^\s]+)/,
    );
    if (includedMatch) {
      const chunkGroupId = includedMatch[1].trim();
      const included = includedMatch[2]
        .split(',')
        .map((part) => part.trim())
        .filter(Boolean);
      const windowInfo = parseChunkGroupId(chunkGroupId);
      if (!windowInfo) {
        continue;
      }

      const windowKey = chunkGroupId;
      if (!windows.has(windowKey)) {
        windows.set(windowKey, {
          chunkGroupId,
          queryId: windowInfo.queryId,
          windowName: windowInfo.windowName,
          windowStart: windowInfo.windowStart,
          windowEnd: windowInfo.windowEnd,
          includedSubqueryIds: new Set(),
          receivedSubqueries: new Map(),
          chunkedResult: null,
          windowState: 'incomplete',
          recomposedLineSeen: false,
        });
      }

      const window = windows.get(windowKey);
      for (const subqueryId of included) {
        window.includedSubqueryIds.add(subqueryId);
      }
      window.windowState = 'complete';
      continue;
    }

    const recomposedMatch = message.match(
      /final emission chunkGroupId=([^;]+); recomposedResult=([+-]?\d+(?:\.\d+)?(?:e[+-]?\d+)?)/i,
    );
    if (recomposedMatch) {
      const chunkGroupId = recomposedMatch[1].trim();
      const windowInfo = parseChunkGroupId(chunkGroupId);
      if (!windowInfo) {
        continue;
      }

      const windowKey = chunkGroupId;
      if (!windows.has(windowKey)) {
        windows.set(windowKey, {
          chunkGroupId,
          queryId: windowInfo.queryId,
          windowName: windowInfo.windowName,
          windowStart: windowInfo.windowStart,
          windowEnd: windowInfo.windowEnd,
          includedSubqueryIds: new Set(),
          receivedSubqueries: new Map(),
          chunkedResult: null,
          windowState: 'incomplete',
          recomposedLineSeen: false,
        });
      }

      const window = windows.get(windowKey);
      window.chunkedResult = Number.parseFloat(recomposedMatch[2]);
      window.recomposedLineSeen = true;
      window.windowState = 'complete';
      continue;
    }

    const resultMatch = message.match(/calculated result (.+)$/);
    if (resultMatch) {
      // Preserve for completeness; the recomposed result line is the comparison anchor.
      continue;
    }
  }

  const windowRecords = Array.from(windows.values()).sort(
    (a, b) => a.windowStart - b.windowStart,
  );

  for (const window of windowRecords) {
    const includedSubqueryIds = Array.from(window.includedSubqueryIds);
    window.subqueries = includedSubqueryIds.map((subqueryId) => {
      const received = window.receivedSubqueries.get(subqueryId) || {};
      const parsedChunk = parseChunkId(received.chunkId);
      const debugDetail =
        parsedChunk && subqueryDetailsByChunkId.has(received.chunkId)
          ? subqueryDetailsByChunkId.get(received.chunkId)
          : null;

      return {
        subqueryId,
        chunkId: received.chunkId || null,
        value: debugDetail ? debugDetail.value : null,
        count: debugDetail ? debugDetail.count : null,
        completeness: received.completeness || null,
        missingSubqueryIds: received.missingSubqueryIds || [],
      };
    });
  }

  return {
    windows: windowRecords,
    ignoredLegacyChunkCount,
  };
}

function loadReplayMetadata(metadataPath) {
  if (metadataPath && fs.existsSync(metadataPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(metadataPath, 'utf8'));
      if (parsed && typeof parsed === 'object') {
        if (parsed.replayMetadata && typeof parsed.replayMetadata === 'object') {
          return parsed.replayMetadata;
        }
        return parsed;
      }
    } catch (error) {
      console.warn(`Error parsing metadata ${metadataPath}: ${error.message}`);
    }
  }

  return getReplayMetadata(process.env);
}

function formatValue(value) {
  return Number.isFinite(value) ? value.toFixed(6) : 'N/A';
}

function emitMismatchReport({
  frequency,
  queryId,
  chunkGroupId,
  windowStart,
  windowEnd,
  baselineResult,
  chunkedResult,
  includedSubqueryIds,
  subqueries,
  replayMetadata,
  ignoredLegacyChunkCount,
  windowState,
}) {
  console.log('[DEBUG_CHUNKS] first mismatching logical window');
  console.log(
    JSON.stringify(
      {
        frequency,
        queryId,
        chunkGroupId,
        windowStart,
        windowEnd,
        baselineResult,
        chunkedResult,
        includedSubqueryIds,
        subqueries,
        replayMetadata,
        ignoredLegacyChunkCount,
        windowState,
      },
      null,
      2,
    ),
  );
}

function analyzeIteration(iteration) {
  const iterationDir = path.join(LOGS_DIR, normalizeIterationLabel(iteration));
  const logPath = resolvePathForIteration(
    ANALYZER_CONFIG.chunkedCsvPath,
    'streaming_query_chunk_aggregator_log.csv',
    iteration,
  );
  const resourcePath = path.join(iterationDir, 'streaming_query_hive_resource_log.csv');
  const stdoutPath = resolvePathForIteration(
    ANALYZER_CONFIG.stdoutLogPath,
    'streaming_query_hive_stdout.log',
    iteration,
  );

  let avgCpuPercent = '';
  let avgHeapUsedMB = '';

  if (fs.existsSync(resourcePath)) {
    const resourceContent = fs.readFileSync(resourcePath, 'utf8');
    const resourceRecords = csvParse.parse(resourceContent, {
      columns: true,
      skip_empty_lines: true,
    });
    const cpuPercents = [];
    const heapUsedMBs = [];
    for (let i = 1; i < resourceRecords.length; i++) {
      const prev = resourceRecords[i - 1];
      const curr = resourceRecords[i];
      const deltaCpuUser = Number(curr.cpu_user) - Number(prev.cpu_user);
      const deltaTime = Number(curr.timestamp) - Number(prev.timestamp);
      if (deltaTime > 0) {
        cpuPercents.push(((deltaCpuUser) / (deltaTime * NUM_CORES)) * 100);
      }
      heapUsedMBs.push(Number(curr.heapUsedMB));
    }
    if (cpuPercents.length > 0) {
      avgCpuPercent = (
        cpuPercents.reduce((a, b) => a + b, 0) / cpuPercents.length
      ).toFixed(2);
    }
    if (heapUsedMBs.length > 0) {
      avgHeapUsedMB = (
        heapUsedMBs.reduce((a, b) => a + b, 0) / heapUsedMBs.length
      ).toFixed(2);
    }
  }

  const records = parseCsvLog(logPath);
  if (records.length === 0) {
    console.warn(`Missing or empty log for iteration ${iteration}`);
    return {
      registeredTs: '',
      resultTs: '',
      resultValue: '',
      avgCpuPercent,
      avgHeapUsedMB,
      stdoutPath,
    };
  }

  let registeredTs = null;
  let resultTs = null;
  let resultValue = '';

  for (const row of records) {
    if (!registeredTs && row.message && row.message.includes('Registered Query')) {
      registeredTs = Number(row.timestamp);
    }
    if (registeredTs && !resultTs && row.message && row.message.includes('calculated result')) {
      resultTs = Number(row.timestamp);
      const valueMatch = row.message.match(/hasValue[^"]*"([+-]?\d*\.?\d+)"/);
      if (valueMatch) {
        resultValue = valueMatch[1];
      } else {
        const fallbackMatch = row.message.match(/"([+-]?\d*\.?\d+)"/);
        resultValue = fallbackMatch ? fallbackMatch[1] : '';
      }
      break;
    }
  }

  return {
    registeredTs: registeredTs || '',
    resultTs: resultTs || '',
    resultValue,
    avgCpuPercent,
    avgHeapUsedMB,
    stdoutPath,
  };
}

function emitDebugMismatchIfNeeded(iteration, stdoutPath) {
  if (!ANALYZER_CONFIG.debugChunks) {
    return;
  }

  const records = parseCsvLog(
    resolvePathForIteration(
      ANALYZER_CONFIG.chunkedCsvPath,
      'streaming_query_chunk_aggregator_log.csv',
      iteration,
    ),
  );
  const stdoutLines = fs.existsSync(stdoutPath)
    ? fs.readFileSync(stdoutPath, 'utf8').split('\n').filter(Boolean)
    : [];
  const aggregation = getAggregation();
  const baselineObservations = loadBaselineObservations(BASE_DATA_DIR);
  const replayMetadata = loadReplayMetadata(ANALYZER_CONFIG.metadataPath);
  const frequency = ANALYZER_CONFIG.frequency;

  const { windows, ignoredLegacyChunkCount } = buildWindowState(records, stdoutLines);
  let firstMismatch = null;

  for (const window of windows) {
    const baselineResult = buildBaselineResult(
      baselineObservations,
      window.windowStart,
      window.windowEnd,
      aggregation,
    );
    const chunkedResult = window.chunkedResult;

    if (
      baselineResult !== null &&
      chunkedResult !== null &&
      Math.abs(baselineResult - chunkedResult) > 1e-8
    ) {
      firstMismatch = {
        frequency: Number.isFinite(frequency) ? frequency : null,
        queryId: window.queryId,
        chunkGroupId: window.chunkGroupId,
        windowStart: window.windowStart,
        windowEnd: window.windowEnd,
        baselineResult,
        chunkedResult,
        includedSubqueryIds: Array.from(window.includedSubqueryIds),
        subqueries: window.subqueries || [],
        replayMetadata,
        ignoredLegacyChunkCount,
        windowState: window.windowState,
      };
      break;
    }
  }

  if (firstMismatch) {
    emitMismatchReport(firstMismatch);
  } else {
    console.log('chunked matches baseline for all compared windows');
  }
}

function runAnalyzerStartupDebug() {
  if (!ANALYZER_CONFIG.debugChunks) {
    return;
  }

  console.log('[DEBUG_CHUNKS] resolved analyzer inputs');
  console.log(
    JSON.stringify(
      {
        dataPath: ANALYZER_CONFIG.dataPath,
        dataDir: path.resolve(ANALYZER_CONFIG.dataDir),
        frequency: ANALYZER_CONFIG.frequency,
        iteration: ANALYZER_CONFIG.rawIteration || null,
        chunkedCsvPath: ANALYZER_CONFIG.chunkedCsvPath
          ? path.resolve(ANALYZER_CONFIG.chunkedCsvPath)
          : null,
        stdoutLogPath: ANALYZER_CONFIG.stdoutLogPath
          ? path.resolve(ANALYZER_CONFIG.stdoutLogPath)
          : null,
        metadataPath: ANALYZER_CONFIG.metadataPath
          ? path.resolve(ANALYZER_CONFIG.metadataPath)
          : null,
        logsDir: path.resolve(LOGS_DIR),
      },
      null,
      2,
    ),
  );
}

function getIterationsToAnalyze() {
  if (ANALYZER_CONFIG.mode === 'single') {
    return [ANALYZER_CONFIG.rawIteration];
  }

  return Array.from({ length: ITERATIONS }, (_, index) => index + 1);
}

const summaryRows = [
  [
    'iteration',
    'registered_query_ts',
    'first_result_ts',
    'latency_ms',
    'result_value',
    'avg_cpu_percent',
    'avg_heapUsedMB',
  ],
];

const missingInputs =
  ANALYZER_CONFIG.mode === 'single' ? requireSingleIterationInputs(ANALYZER_CONFIG) : [];

if (ANALYZER_CONFIG.mode === 'single' && missingInputs.length > 0) {
  printUsage(`Missing required analyzer inputs: ${missingInputs.join(', ')}`);
  process.exit(1);
}

runAnalyzerStartupDebug();

if (ANALYZER_CONFIG.mode === 'single') {
  const resolvedChunkedCsvPath = resolvePathForIteration(
    ANALYZER_CONFIG.chunkedCsvPath,
    'streaming_query_chunk_aggregator_log.csv',
    ANALYZER_CONFIG.rawIteration,
  );
  const resolvedStdoutLogPath = resolvePathForIteration(
    ANALYZER_CONFIG.stdoutLogPath,
    'streaming_query_hive_stdout.log',
    ANALYZER_CONFIG.rawIteration,
  );
  const resolvedMetadataPath = ANALYZER_CONFIG.metadataPath;
  const missingFiles = [];

  if (!fs.existsSync(BASE_DATA_DIR)) {
    missingFiles.push(`data directory ${BASE_DATA_DIR}`);
  }
  if (!fs.existsSync(resolvedChunkedCsvPath)) {
    missingFiles.push(`chunked csv ${resolvedChunkedCsvPath}`);
  }
  if (!fs.existsSync(resolvedStdoutLogPath)) {
    missingFiles.push(`stdout log ${resolvedStdoutLogPath}`);
  }
  if (!fs.existsSync(resolvedMetadataPath)) {
    missingFiles.push(`metadata ${resolvedMetadataPath}`);
  }

  if (missingFiles.length > 0) {
    printUsage(`Missing required analyzer inputs: ${missingFiles.join(', ')}`);
    process.exit(1);
  }
}

for (const iteration of getIterationsToAnalyze()) {
  const summary = analyzeIteration(iteration);
  summaryRows.push([
    iteration,
    summary.registeredTs,
    summary.resultTs,
    summary.registeredTs && summary.resultTs
      ? Number(summary.resultTs) - Number(summary.registeredTs)
      : '',
    summary.resultValue,
    summary.avgCpuPercent,
    summary.avgHeapUsedMB,
  ]);

  if (ANALYZER_CONFIG.debugChunks) {
    emitDebugMismatchIfNeeded(iteration, summary.stdoutPath);
  }
}

fs.writeFileSync(OUT_CSV, summaryRows.map((row) => row.join(',')).join('\n'));
console.log(`Latency and resource usage summary written to ${OUT_CSV}`);
