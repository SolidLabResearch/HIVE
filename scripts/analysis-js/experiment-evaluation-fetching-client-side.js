const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { createBenchmarkReplayRunEnv } = require('../../experiments/utils/benchmarkReplayEnv');
const { startProcessTreeResourceLogging } = require('./process-tree-resource-sampler');

const RUNS = 1;
const LOGS_DIR = 'logs/fetching-client-side';
const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const LOG_FILES = [
  'fetching_client_side_log.csv',
  'fetching_client_side_resource_usage.csv',
  'fetching_client_side_process_tree_resource_usage.csv',
  'replayer-log.csv'
];
const DEFAULT_TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes
const TIMEOUT_MS = Number.parseInt(
  process.env.STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS || String(DEFAULT_TIMEOUT_MS),
  10,
) || DEFAULT_TIMEOUT_MS;
const TARGET_WINDOW_COUNT = Number.parseInt(
  process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS || "",
  10,
);
const USE_TARGET_WINDOW_CAP = Number.isFinite(TARGET_WINDOW_COUNT) && TARGET_WINDOW_COUNT > 0;
const SKIP_LINGERING_CLEANUP = process.env.STREAMING_QUERY_HIVE_SKIP_LINGERING_PROCESS_CLEANUP === '1';

function resolveIterationDir(iter) {
  return process.env.LOG_PATH ? path.resolve(process.env.LOG_PATH) : path.join(LOGS_DIR, `iteration${iter}`);
}

if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });

function killLingeringProcesses() {
  if (SKIP_LINGERING_CLEANUP) {
    return;
  }
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

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }

  const content = fs.readFileSync(filePath, 'utf8').trim();
  return content ? JSON.parse(content) : null;
}

function finalizeRunSummary(iterDir) {
  const runSummaryPath = path.join(iterDir, 'run_summary.json');
  const capSummaryPath = path.join(iterDir, 'benchmark_window_cap_summary.json');
  const runSummary = readJsonIfExists(runSummaryPath) || {};
  const capSummary = readJsonIfExists(capSummaryPath) || {};
  const finalSummary = {
    ...runSummary,
    ...capSummary,
    targetWindowCount: Number.isFinite(TARGET_WINDOW_COUNT) ? TARGET_WINDOW_COUNT : runSummary.targetWindowCount ?? null,
    emittedFinalWindowCount: capSummary.emittedFinalWindowCount ?? runSummary.emittedFinalWindowCount ?? null,
    finalWindowNumbers: capSummary.finalWindowNumbers ?? runSummary.finalWindowNumbers ?? [],
    stoppedAfterTargetWindows: capSummary.stoppedAfterTargetWindows ?? runSummary.stoppedAfterTargetWindows ?? false,
    stopReason: capSummary.stopReason ?? runSummary.stopReason ?? runSummary.publisherExitReason ?? 'other',
  };
  fs.writeFileSync(runSummaryPath, `${JSON.stringify(finalSummary, null, 2)}\n`);
}

async function runOnce(iter) {
  console.log(`--- Streaming Query Hive Run ${iter} ---`);

  killLingeringProcesses(); // Ensure no lingering processes before starting

  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const runEnv = replayEnv.withBenchmarkReplayEnv(process.env);
  const approach = spawn(APPROACH_CMD[0], APPROACH_CMD[1], { stdio: 'inherit', env: runEnv });
  const iterDir = resolveIterationDir(iter);
  const processTreeLogPath = path.join(iterDir, 'fetching_client_side_process_tree_resource_usage.csv');
  const treeSampler = startProcessTreeResourceLogging(processTreeLogPath, approach.pid, 100);

  await new Promise(res => setTimeout(res, 2000));
  const publisher = spawn(PUBLISH_CMD[0], PUBLISH_CMD[1], { stdio: 'inherit', env: runEnv });

  const timeout = setTimeout(() => {
    console.log('Timeout reached, killing processes...');
    approach.kill();
    publisher.kill();
    killLingeringProcesses(); // Extra cleanup on timeout
  }, TIMEOUT_MS);

  if (USE_TARGET_WINDOW_CAP) {
    await new Promise((resolve) => { approach.on('exit', () => resolve()); });
    if (publisher.exitCode === null && publisher.signalCode === null) {
      publisher.kill();
    }
    await new Promise((resolve) => { publisher.on('exit', () => resolve()); });
  } else {
    await new Promise((resolve) => { publisher.on('exit', () => resolve()); });
    await new Promise((resolve) => { approach.on('exit', () => resolve()); });
  }

  clearTimeout(timeout);
  treeSampler.stop();

  killLingeringProcesses(); // Ensure no lingering processes after run

  if (!fs.existsSync(iterDir)) fs.mkdirSync(iterDir, { recursive: true });

  for (const file of LOG_FILES) {
    if (fs.existsSync(file)) {
      const newName = path.join(iterDir, file);
      fs.renameSync(file, newName);
    }
  }

  if (USE_TARGET_WINDOW_CAP) {
    finalizeRunSummary(iterDir);
  }
}

(async () => {
  for (let i = 1; i <= RUNS; i++) {
    await runOnce(i);
    await new Promise(res => setTimeout(res, 2000));
  }
  console.log('All Streaming Query Hive runs complete.');
})();
