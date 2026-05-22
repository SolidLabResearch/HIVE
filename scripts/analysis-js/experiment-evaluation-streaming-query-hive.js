const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { getReplayMetadata } = require('../../experiments/utils/benchmarkResultMetadata');
const { createBenchmarkReplayRunEnv } = require('../../experiments/utils/benchmarkReplayEnv');

const RUNS = 1;
const LOGS_DIR = 'logs/streaming-query-hive';
const ANALYZER_SCRIPT = 'scripts/analysis-js/analyzeResultsStreamingQueryHive.js';
const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryChunkedApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const LOG_FILES = [
  'streaming_query_chunk_aggregator_log.csv',
  'streaming_query_hive_resource_log.csv',
  'replayer-log.csv',
  'streaming_query_hive_stdout.log',
  'streaming_query_hive_stderr.log',
];
const TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes

if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });

function quoteArg(value) {
  const stringValue = String(value);
  if (stringValue.length === 0) {
    return "''";
  }
  if (/^[A-Za-z0-9_./:-]+$/.test(stringValue)) {
    return stringValue;
  }
  return `'${stringValue.replace(/'/g, `'\\''`)}'`;
}

function killLingeringProcesses() {
  try {
    execSync('pkill -f StreamingQueryHiveApproachOrchestrator.js');
  } catch (e) { }
  try {
    execSync('pkill -f publish.js');
  } catch (e) { }
  // Optionally, kill all node processes (uncomment if needed)
  // try { execSync('pkill -f node'); } catch (e) {}
  // Optionally, kill MQTT broker (uncomment if needed)
  // try { execSync('pkill -f mosquitto'); } catch (e) { }
}

function parseFrequency(dataPath) {
  const fromEnv = Number.parseFloat(process.env.WEARABLE_FREQUENCY || '');
  if (Number.isFinite(fromEnv)) {
    return fromEnv;
  }

  const fromDataPath = /freq_([0-9.]+)/.exec(dataPath || '');
  if (fromDataPath) {
    const parsed = Number.parseFloat(fromDataPath[1]);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function runAnalyzer(iter, iterDir, deterministicEnv, dataPath) {
  const analyzerMetadataPath = path.join(iterDir, 'streaming_query_hive_metadata.json');
  fs.writeFileSync(
    analyzerMetadataPath,
    JSON.stringify(getReplayMetadata(deterministicEnv), null, 2),
  );

  const chunkedCsvPath = path.join(iterDir, 'streaming_query_chunk_aggregator_log.csv');
  const stdoutLogPath = path.join(iterDir, 'streaming_query_hive_stdout.log');
  const frequency = parseFrequency(dataPath);
  const debugChunks = process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === '1';

  if (!Number.isFinite(frequency)) {
    throw new Error(`Unable to determine analyzer frequency for data path: ${dataPath}`);
  }

  const args = [
    ANALYZER_SCRIPT,
    '--data-path',
    dataPath,
    '--iteration',
    String(iter),
    '--chunked-csv',
    chunkedCsvPath,
    '--stdout-log',
    stdoutLogPath,
    '--metadata',
    analyzerMetadataPath,
    '--frequency',
    String(frequency),
    '--debug-chunks',
    debugChunks ? 'true' : 'false',
  ];

  console.log(`Running analyzer: node ${args.map(quoteArg).join(' ')}`);
  const result = spawn('node', args, {
    stdio: 'inherit',
    env: deterministicEnv,
  });

  return new Promise((resolve, reject) => {
    result.on('exit', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Analyzer exited with code ${code}`));
      }
    });
    result.on('error', reject);
  });
}

async function runOnce(iter) {
  console.log(`--- Streaming Query Hive Run ${iter} ---`);

  killLingeringProcesses(); // Ensure no lingering processes before starting

  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const deterministicEnv = replayEnv.withBenchmarkReplayEnv(process.env);
  const dataPath = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
  const approach = spawn(APPROACH_CMD[0], APPROACH_CMD[1], { stdio: ['ignore', 'pipe', 'pipe'], env: deterministicEnv });
  let approachStdout = '';
  let approachStderr = '';
  approach.stdout.on('data', (data) => {
    const text = data.toString();
    approachStdout += text;
    process.stdout.write(text);
  });
  approach.stderr.on('data', (data) => {
    const text = data.toString();
    approachStderr += text;
    process.stderr.write(text);
  });

  await new Promise(res => setTimeout(res, 2000));
  const publisher = spawn(PUBLISH_CMD[0], PUBLISH_CMD[1], { stdio: ['ignore', 'pipe', 'pipe'], env: deterministicEnv });
  let publisherStdout = '';
  let publisherStderr = '';
  publisher.stdout.on('data', (data) => {
    const text = data.toString();
    publisherStdout += text;
    process.stdout.write(text);
  });
  publisher.stderr.on('data', (data) => {
    const text = data.toString();
    publisherStderr += text;
    process.stderr.write(text);
  });

  const timeout = setTimeout(() => {
    console.log('Timeout reached, killing processes...');
    approach.kill();
    publisher.kill();
    killLingeringProcesses(); // Extra cleanup on timeout
  }, TIMEOUT_MS);

  await new Promise((resolve) => { publisher.on('exit', () => resolve()); });
  await new Promise((resolve) => { approach.on('exit', () => resolve()); });

  clearTimeout(timeout);

  killLingeringProcesses(); // Ensure no lingering processes after run

  const iterDir = path.join(LOGS_DIR, `iteration${iter}`);
  if (!fs.existsSync(iterDir)) fs.mkdirSync(iterDir, { recursive: true });

  for (const file of LOG_FILES) {
    if (fs.existsSync(file)) {
      const newName = path.join(iterDir, file);
      fs.renameSync(file, newName);
    }
  }

  fs.writeFileSync(path.join(iterDir, 'streaming_query_hive_stdout.log'), `${approachStdout}${publisherStdout}`);
  fs.writeFileSync(path.join(iterDir, 'streaming_query_hive_stderr.log'), `${approachStderr}${publisherStderr}`);

  await runAnalyzer(iter, iterDir, deterministicEnv, dataPath);
}

(async () => {
  for (let i = 1; i <= RUNS; i++) {
    await runOnce(i);
    await new Promise(res => setTimeout(res, 2000));
  }
  console.log('All Streaming Query Hive runs complete.');
})();
