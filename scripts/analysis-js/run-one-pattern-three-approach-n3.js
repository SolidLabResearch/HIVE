#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '../..');
const ANALYSIS_LOG_DIR = path.join(REPO_ROOT, 'analysis', 'benchmark-logs');
const WINDOW_VARIANT = String(process.env.BENCHMARK_WINDOW_VARIANT || 'trailing').toLowerCase() === 'centered'
  ? 'centered'
  : 'trailing';
const IS_CENTERED = WINDOW_VARIANT === 'centered';
const SCENARIO_NAME = IS_CENTERED ? 'one-pattern-centered-n3' : 'one-pattern-three-approach-n3';
const APPROACH_LOG_ROOTS = IS_CENTERED
  ? {
      fetching: 'logs/centered-window-n3/fetching',
      approximation: 'logs/centered-window-n3/approximation',
      chunked: 'logs/centered-window-n3/chunked',
    }
  : {
      fetching: 'logs/fetching-client-side',
      approximation: 'logs/approximation-approach',
      chunked: 'logs/streaming-query-hive',
    };
const BASE_ENV = {
  DATA_PATH: 'approximation_test/challenging/exponential_growth',
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
  STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: '3',
  STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: '300',
  STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS: '420000',
  STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: '1',
  STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: '0',
  ...(IS_CENTERED ? { RSP_WINDOW_SEMANTICS: 'centered' } : {}),
  BENCHMARK_SCENARIO: SCENARIO_NAME,
  BENCHMARK_SCALE: 'challenging_exponential_growth',
};

function buildRunPlan() {
  if (!IS_CENTERED) {
    return [
      {
        approach: 'fetching',
        iter: 3,
        runner: 'scripts/analysis-js/experiment-evaluation-fetching-client-side.js',
        logDir: 'logs/fetching-client-side/iteration3',
      },
      {
        approach: 'approximation',
        iter: 2,
        runner: 'scripts/analysis-js/experiment-evaluation-approximation-approach.js',
        logDir: 'logs/approximation-approach/iteration2',
      },
      {
        approach: 'approximation',
        iter: 3,
        runner: 'scripts/analysis-js/experiment-evaluation-approximation-approach.js',
        logDir: 'logs/approximation-approach/iteration3',
      },
      {
        approach: 'chunked',
        iter: 2,
        runner: 'scripts/analysis-js/experiment-evaluation-streaming-query-hive.js',
        logDir: 'logs/streaming-query-hive/iteration2',
      },
      {
        approach: 'chunked',
        iter: 3,
        runner: 'scripts/analysis-js/experiment-evaluation-streaming-query-hive.js',
        logDir: 'logs/streaming-query-hive/iteration3',
      },
    ];
  }

  return [1, 2, 3].flatMap((iter) => ([
    {
      approach: 'fetching',
      iter,
      runner: 'scripts/analysis-js/experiment-evaluation-fetching-client-side.js',
      logDir: `${APPROACH_LOG_ROOTS.fetching}/iteration${iter}`,
    },
    {
      approach: 'approximation',
      iter,
      runner: 'scripts/analysis-js/experiment-evaluation-approximation-approach.js',
      logDir: `${APPROACH_LOG_ROOTS.approximation}/iteration${iter}`,
    },
    {
      approach: 'chunked',
      iter,
      runner: 'scripts/analysis-js/experiment-evaluation-streaming-query-hive.js',
      logDir: `${APPROACH_LOG_ROOTS.chunked}/iteration${iter}`,
    },
  ]));
}

function buildEnv(approach, iter, logDir) {
  const sessionId = `${SCENARIO_NAME}-${approach}-i${iter}`;
  const topicPrefix = `${SCENARIO_NAME}/i${iter}/${approach}`;
  return {
    ...process.env,
    ...BASE_ENV,
    BENCHMARK_APPROACH: approach,
    BENCHMARK_ITERATION: String(iter),
    SESSION_ID: sessionId,
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    LOG_PATH: path.join(REPO_ROOT, logDir),
  };
}

function ensureCleanDir(dirPath) {
  fs.rmSync(dirPath, { recursive: true, force: true });
  fs.mkdirSync(dirPath, { recursive: true });
}

function runOne(plan) {
  const logPath = path.join(ANALYSIS_LOG_DIR, `${plan.approach}-i${plan.iter}.log`);
  const out = fs.openSync(logPath, 'w');
  const env = buildEnv(plan.approach, plan.iter, plan.logDir);
  ensureCleanDir(env.LOG_PATH);

  process.stdout.write(`RUNNING ${plan.approach} iter=${plan.iter}\n`);
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
    throw new Error(`${plan.approach} iter=${plan.iter} exited with code ${result.status}`);
  }
  process.stdout.write(`DONE ${plan.approach} iter=${plan.iter}\n`);
}

function main() {
  fs.mkdirSync(ANALYSIS_LOG_DIR, { recursive: true });
  for (const plan of buildRunPlan()) {
    runOne(plan);
  }
}

main();
