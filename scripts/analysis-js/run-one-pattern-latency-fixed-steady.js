#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '../..');
const ANALYSIS_LOG_DIR = path.join(REPO_ROOT, 'analysis', 'benchmark-logs');
const SCENARIO_NAME = 'one-pattern-latency-fixed-15w-steady';
const DATA_PATH = 'approximation_test/challenging/exponential_growth';
const BASE_ENV = {
  DATA_PATH,
  WEARABLE_FREQUENCY: '10',
  STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: '1',
  STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: '1756122905256',
  STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: '1756122905256',
  OUTPUT_WINDOW_RANGE: '120000',
  OUTPUT_WINDOW_STEP: '60000',
  SUB_WINDOW_RANGE: '60000',
  SUB_WINDOW_STEP: '30000',
  AGGREGATION_FUNCTION: 'AVG',
  AGGREGATION_FUNC: 'AVG',
  STREAMING_QUERY_HIVE_DEBUG_CHUNKS: '0',
  STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: '1',
  STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: '15',
  STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS:
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS || '2000',
  STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS:
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS || '2700000',
  STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: '1',
  STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: '1',
  STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: '0',
  STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: '1',
  STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: '0',
  BENCHMARK_SCENARIO: SCENARIO_NAME,
  BENCHMARK_SCALE: 'challenging_exponential_growth',
  BENCHMARK_ITERATION: '1',
};

const RUN_PLAN = [
  {
    approach: 'fetching',
    runner: 'scripts/analysis-js/experiment-evaluation-fetching-client-side.js',
    logDir: 'logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1',
  },
  {
    approach: 'approximation',
    runner: 'scripts/analysis-js/experiment-evaluation-approximation-approach.js',
    logDir: 'logs/one-pattern-latency-fixed-15w-steady/approximation/iteration1',
  },
  {
    approach: 'chunked',
    runner: 'scripts/analysis-js/experiment-evaluation-streaming-query-hive.js',
    logDir: 'logs/one-pattern-latency-fixed-15w-steady/chunked/iteration1',
  },
];

function ensureCleanDir(dirPath) {
  fs.rmSync(dirPath, { recursive: true, force: true });
  fs.mkdirSync(dirPath, { recursive: true });
}

function runOne(plan) {
  const iterDir = path.join(REPO_ROOT, plan.logDir);
  ensureCleanDir(iterDir);

  const logPath = path.join(ANALYSIS_LOG_DIR, `steady-15w-${plan.approach}.log`);
  const out = fs.openSync(logPath, 'w');
  const env = {
    ...process.env,
    ...BASE_ENV,
    BENCHMARK_APPROACH: plan.approach,
    SESSION_ID: `${SCENARIO_NAME}-${plan.approach}-i1`,
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: `${SCENARIO_NAME}/i1/${plan.approach}`,
    LOG_PATH: iterDir,
  };

  process.stdout.write(`RUNNING ${plan.approach}\n`);
  const result = spawnSync('node', [plan.runner], {
    cwd: REPO_ROOT,
    env,
    stdio: ['ignore', out, out],
    encoding: 'utf8',
    maxBuffer: 10 * 1024 * 1024,
  });
  fs.closeSync(out);

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${plan.approach} exited with code ${result.status}`);
  }

  process.stdout.write(`DONE ${plan.approach}\n`);
}

function main() {
  fs.mkdirSync(ANALYSIS_LOG_DIR, { recursive: true });
  for (const plan of RUN_PLAN) {
    runOne(plan);
  }
}

main();
