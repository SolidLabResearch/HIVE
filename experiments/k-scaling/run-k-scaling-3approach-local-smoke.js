#!/usr/bin/env node

const { spawn, execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const {
  cleanupStaleBenchmarkProcesses,
  delay,
  terminateChildProcessTree,
} = require("../utils/processCleanup");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const { finalizeMqttTrafficArtifacts } = require("../../dist/util/mqttTraffic");
const { startProcessTreeResourceLogging } = require("../../scripts/analysis-js/process-tree-resource-sampler");
const {
  APPROACH_CONFIG,
  DEFAULT_APPROACHES,
  DEFAULT_ITERATIONS,
  DEFAULT_K_VALUES,
  OUTPUT_WINDOW_RANGE_MS,
  OUTPUT_WINDOW_STEP_MS,
  SUB_WINDOW_RANGE_MS,
  SUB_WINDOW_STEP_MS,
  TARGET_WINDOWS,
  buildCheckpointKey,
  buildCombinationMatrix,
  countConsumerLatencyFiles,
  extractAllConsumerWindows,
  extractRepresentativeWindow,
  getExpectedBenchmarkSummaryCount,
  getApproachConfig,
  listBenchmarkWindowSummaryPaths,
  parseApproachSelection,
  parseKScalingSelection,
  readJson,
  readProcessTreeMetrics,
  sanitizeTimestamp,
} = require("./local-k-scaling-smoke-common");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const DEFAULT_TIMEOUT_MS = 6 * 60 * 1000;
const REQUIRED_REPLAY_DURATION_SECONDS = 185;
const CONTROL_PORT = 8080;

function parseArgs(argv) {
  const args = {
    kValues: DEFAULT_K_VALUES,
    approaches: DEFAULT_APPROACHES,
    iterations: DEFAULT_ITERATIONS,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    resumeRoot: null,
    stopOnInvalid: true,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--k-values":
        args.kValues = parseKScalingSelection(next, DEFAULT_K_VALUES);
        index += 1;
        break;
      case "--approaches":
        args.approaches = parseApproachSelection(next, DEFAULT_APPROACHES);
        index += 1;
        break;
      case "--iterations":
        args.iterations = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--timeout-ms":
        args.timeoutMs = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--resume-root":
        args.resumeRoot = path.resolve(REPO_ROOT, next || "");
        index += 1;
        break;
      case "--no-stop-on-invalid":
        args.stopOnInvalid = false;
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

  if (!Number.isFinite(args.iterations) || args.iterations <= 0) {
    throw new Error("--iterations must be a positive integer");
  }
  if (!Number.isFinite(args.timeoutMs) || args.timeoutMs <= 0) {
    throw new Error("--timeout-ms must be a positive integer");
  }
  for (const approach of args.approaches) {
    getApproachConfig(approach);
  }

  return args;
}

function printHelp() {
  console.log(`Usage: node experiments/k-scaling/run-k-scaling-3approach-local-smoke.js [options]

Options:
  --k-values <list>      Comma-separated K values (default: 1,2,4,8,32)
  --approaches <list>    Comma-separated approaches (default: fetching,approximation,chunked)
  --iterations <n>       Iterations per K/approach (default: 3)
  --timeout-ms <n>       Per-run timeout in milliseconds
  --resume-root <path>   Existing result root to resume
  --no-stop-on-invalid   Continue after a structurally invalid result
`);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function readBenchmarkWindowSummary(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
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
  return code === 0 || code === 143 || signal === "SIGTERM";
}

function runCommand(command, workdir = REPO_ROOT) {
  return execSync(command, {
    cwd: workdir,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trimEnd();
}

function collectRepoState() {
  return {
    branch: runCommand("git branch --show-current"),
    commit: runCommand("git rev-parse HEAD"),
    statusShort: runCommand("git status --short || true"),
    nodeVersion: runCommand("node -v"),
    npmVersion: runCommand("npm -v"),
    capturedAt: new Date().toISOString(),
  };
}

function buildResultRoot(resumeRoot) {
  if (resumeRoot) {
    return resumeRoot;
  }
  const timestamp = sanitizeTimestamp(new Date());
  return path.join(
    REPO_ROOT,
    "results",
    "paper-benchmarks",
    `k-scaling-3approach-3x-local-${timestamp}`,
  );
}

function buildRunRoot(resultRoot, approach, kValue, iteration) {
  return path.join(resultRoot, "raw", approach, `K${kValue}`, `iteration${iteration}`);
}

function collectBenchmarkWindowSummaries(runRoot, approach, kValue) {
  const paths = listBenchmarkWindowSummaryPaths(runRoot, approach, kValue);
  const entries = paths
    .map((filePath) => ({ filePath, summary: readBenchmarkWindowSummary(filePath) }))
    .filter((entry) => entry.summary);
  const successfulEntries = entries.filter((entry) =>
    hasSuccessfulTargetWindowCapSummary(entry.summary),
  );
  const expectedSuccessfulCount = getExpectedBenchmarkSummaryCount(approach, kValue);
  return {
    entries,
    successfulEntries,
    expectedSuccessfulCount,
    targetReached: successfulEntries.length >= expectedSuccessfulCount,
  };
}

function readCheckpoint(resultRoot, approach, kValue, iteration) {
  const checkpointPath = path.join(
    resultRoot,
    "checkpoints",
    `${buildCheckpointKey(approach, kValue, iteration)}.json`,
  );
  return fs.existsSync(checkpointPath) ? readJson(checkpointPath) : null;
}

function writeCheckpoint(resultRoot, approach, kValue, iteration, payload) {
  const checkpointDir = path.join(resultRoot, "checkpoints");
  ensureDir(checkpointDir);
  writeJson(
    path.join(checkpointDir, `${buildCheckpointKey(approach, kValue, iteration)}.json`),
    payload,
  );
}

function buildRunEnv({ approach, kValue, iteration, runRoot, timeoutMs }) {
  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const benchmarkStartTime = String(Date.now());
  const benchmarkEventTimeAnchor =
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR ||
    benchmarkStartTime;
  const benchmarkMinTimestamp = Number.parseInt(benchmarkEventTimeAnchor, 10);
  const benchmarkMaxTimestamp = benchmarkMinTimestamp + (REQUIRED_REPLAY_DURATION_SECONDS * 1000);
  const topicPrefix = [
    "paper-local-k-scaling",
    approach,
    `K${kValue}`,
    `iteration${iteration}`,
  ].join("/");
  const sessionId = `paper_local_k_scaling_${approach}_K${kValue}_iteration${iteration}`;
  return replayEnv.withBenchmarkReplayEnv({
    ...process.env,
    LOG_PATH: runRoot,
    DATA_PATH: "custom_patterns/low_variability",
    SESSION_ID: sessionId,
    RESULT_TOPIC: `paper-local-k-scaling/results/${approach}/K${kValue}/iteration${iteration}`,
    BENCHMARK_SCENARIO: "paper-local-k-scaling",
    BENCHMARK_SCALE: `K${kValue}`,
    BENCHMARK_APPROACH: approach,
    BENCHMARK_ITERATION: String(iteration),
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(TARGET_WINDOWS),
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(REQUIRED_REPLAY_DURATION_SECONDS),
    STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS: String(timeoutMs),
    STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: benchmarkStartTime,
    STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: benchmarkEventTimeAnchor,
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(benchmarkMinTimestamp),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(benchmarkMaxTimestamp),
    AGGREGATION_FUNCTION: "AVG",
    OUTPUT_WINDOW_RANGE: String(OUTPUT_WINDOW_RANGE_MS),
    OUTPUT_WINDOW_STEP: String(OUTPUT_WINDOW_STEP_MS),
    SUB_WINDOW_RANGE: String(SUB_WINDOW_RANGE_MS),
    SUB_WINDOW_STEP: String(SUB_WINDOW_STEP_MS),
    K_SCALING_K: String(kValue),
    K_SCALING_REUSE_MODE: "chunk-state",
    HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE: "false",
    STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "1",
    STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
    STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
    HIVE_PROFILE: "1",
  });
}

async function validateRunArtifacts({ runRoot, approach, kValue }) {
  const consumerFiles = countConsumerLatencyFiles(runRoot, approach, kValue);
  const allConsumerWindows = extractAllConsumerWindows(runRoot, approach, kValue);
  const representative = extractRepresentativeWindow(runRoot, approach, kValue, 1);
  const processTree = readProcessTreeMetrics(runRoot);
  const mqttSummaryPath = path.join(runRoot, "mqtt_traffic_summary.json");
  const resourceSummaryPath = path.join(runRoot, "resource_summary.json");
  const benchmarkSummaries = collectBenchmarkWindowSummaries(runRoot, approach, kValue);

  const failures = [];
  if (consumerFiles.existing !== kValue) {
    failures.push(`consumer latency file count mismatch (${consumerFiles.existing}/${kValue})`);
  }
  if (!allConsumerWindows.ok) {
    failures.push("missing complete comparable first-window result for one or more consumers");
  }
  if (benchmarkSummaries.successfulEntries.length < benchmarkSummaries.expectedSuccessfulCount) {
    failures.push(
      `benchmark window cap summary count mismatch (${benchmarkSummaries.successfulEntries.length}/${benchmarkSummaries.expectedSuccessfulCount})`,
    );
  }
  if (!representative.ok) {
    failures.push(representative.reason || "representative first result missing");
  }
  if (!Number.isFinite(processTree.averageCpuPct)) {
    failures.push("average CPU missing");
  }
  if (!Number.isFinite(processTree.peakRssMb)) {
    failures.push("peak RSS missing");
  }
  if (!fs.existsSync(mqttSummaryPath)) {
    failures.push("mqtt_traffic_summary.json missing");
  }
  if (!fs.existsSync(resourceSummaryPath)) {
    failures.push("resource_summary.json missing");
  }

  return {
    ok: failures.length === 0,
    failures,
    benchmarkSummaries,
    consumerFiles,
    allConsumerWindows,
    representative,
    processTree,
  };
}

async function runSingleExecution({ resultRoot, approach, kValue, iteration, timeoutMs }) {
  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  fs.rmSync(runRoot, { recursive: true, force: true });
  ensureDir(runRoot);
  await cleanupStaleBenchmarkProcesses({ logger: console.log, quiescenceMs: 1000 });

  const env = buildRunEnv({ approach, kValue, iteration, runRoot, timeoutMs });
  const orchestratorScript = getApproachConfig(approach).orchestrator;
  const orchestratorLogPath = path.join(runRoot, `${approach}_orchestrator.stdout.log`);
  const publisherLogPath = path.join(runRoot, "publisher.stdout.log");
  const startTime = new Date();
  const metadata = {
    approach,
    kValue,
    iteration,
    startTime: startTime.toISOString(),
    orchestratorScript,
    runRoot,
    timeoutMs,
    environment: {
      AGGREGATION_FUNCTION: env.AGGREGATION_FUNCTION,
      OUTPUT_WINDOW_RANGE: env.OUTPUT_WINDOW_RANGE,
      OUTPUT_WINDOW_STEP: env.OUTPUT_WINDOW_STEP,
      SUB_WINDOW_RANGE: env.SUB_WINDOW_RANGE,
      SUB_WINDOW_STEP: env.SUB_WINDOW_STEP,
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS,
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS,
      K_SCALING_K: env.K_SCALING_K,
      K_SCALING_REUSE_MODE: env.K_SCALING_REUSE_MODE,
      HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE: env.HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE,
    },
  };
  writeJson(path.join(runRoot, "run_metadata.json"), metadata);

  console.log(
    `[run-start] approach=${approach} K=${kValue} iteration=${iteration} start=${metadata.startTime} result_path=${runRoot}`,
  );

  let orchestrator = null;
  let publisher = null;
  let timeoutHandle = null;
  let publisherStartHandle = null;
  let targetReachedPollHandle = null;
  const processTree = { sampler: null, artifactsWritten: false };
  let finalized = false;
  let targetReachedTerminationStarted = false;

  const writeProcessTreeArtifacts = () => {
    if (!processTree.sampler || processTree.artifactsWritten) {
      return;
    }
    const processTreeStopSummary = processTree.sampler.stop();
    processTree.artifactsWritten = true;
    const processTreeMetrics = readProcessTreeMetrics(runRoot);
    writeJson(path.join(runRoot, "resource_summary.json"), {
      rootPid: processTreeStopSummary.rootPid ?? null,
      logPath: processTreeStopSummary.logPath ?? path.join(runRoot, "process_tree_resource_usage.csv"),
      sampleCount: processTreeMetrics.sampleCount,
      averageCpuPct: processTreeMetrics.averageCpuPct,
      peakRssMb: processTreeMetrics.peakRssMb,
      cpuSeconds: processTreeMetrics.cpuSeconds,
      trackerNegativeDeltaCount: processTreeStopSummary.trackerNegativeDeltaCount ?? 0,
      trackerResetLikeDeltaCount: processTreeStopSummary.trackerResetLikeDeltaCount ?? 0,
      perPid: processTreeStopSummary.perPid ?? [],
    });
  };

  const finish = async (result) => {
    if (finalized) return result;
    finalized = true;
    clearTimeout(timeoutHandle);
    clearTimeout(publisherStartHandle);
    clearInterval(targetReachedPollHandle);

    if (publisher) {
      await terminateChildProcessTree(publisher, { name: "publisher", logger: console.log });
    }
    if (orchestrator) {
      await terminateChildProcessTree(orchestrator, { name: "orchestrator", logger: console.log });
    }
    writeProcessTreeArtifacts();

    await delay(1000);
    const stalePortResult = runCommand(`lsof -ti:${CONTROL_PORT} || true`);
    result.portClean = stalePortResult.trim() === "";
    if (!result.portClean) {
      result.error = result.error || `port ${CONTROL_PORT} still occupied`;
      result.success = false;
    }

    writeJson(path.join(runRoot, "execution_result.json"), result);
    writeCheckpoint(resultRoot, approach, kValue, iteration, result);
    console.log(
      `[run-end] approach=${approach} K=${kValue} iteration=${iteration} end=${result.endTime} exit_status=${result.success ? "SUCCESS" : "FAILED"} result_path=${runRoot}`,
    );
    return result;
  };

  const orchestratorStdout = fs.openSync(orchestratorLogPath, "a");
  const publisherStdout = fs.openSync(publisherLogPath, "a");

  return new Promise((resolve) => {
    try {
      orchestrator = spawn("node", [orchestratorScript], {
        cwd: REPO_ROOT,
        env: { ...env, HIVE_PROCESS_ROLE: `${approach}_k_scaling_orchestrator` },
        stdio: ["ignore", orchestratorStdout, orchestratorStdout],
        detached: true,
      });
      processTree.sampler = startProcessTreeResourceLogging(
        path.join(runRoot, "process_tree_resource_usage.csv"),
        orchestrator.pid,
        100,
      );
    } catch (error) {
      resolve(finish({
        success: false,
        error: error.message,
        endTime: new Date().toISOString(),
        runRoot,
      }));
      return;
    }

    let orchestratorClosed = false;
    let orchestratorClose = { code: null, signal: null };
    let publisherClosed = false;
    let publisherClose = { code: null, signal: null };

    const maybeComplete = async (reason = "completed") => {
      if (!orchestratorClosed || !publisherClosed) {
        return;
      }
      finalizeMqttTrafficArtifacts({ logDir: runRoot });
      writeProcessTreeArtifacts();
      const benchmarkSummaries = collectBenchmarkWindowSummaries(runRoot, approach, kValue);
      const validation = await validateRunArtifacts({ runRoot, approach, kValue });
      const endTime = new Date().toISOString();
      const result = {
        success:
          isBoundedTargetStopExitAcceptable(
            orchestratorClose.code,
            orchestratorClose.signal,
            benchmarkSummaries.targetReached,
          ) &&
          isBoundedTargetStopExitAcceptable(
            publisherClose.code,
            publisherClose.signal,
            benchmarkSummaries.targetReached,
          ) &&
          validation.ok,
        reason,
        validation,
        benchmarkSummaries,
        orchestratorClose,
        publisherClose,
        endTime,
        runRoot,
        representativeWindow: validation.representative,
        allConsumerWindows: validation.allConsumerWindows,
        processTree: validation.processTree,
        consumerFiles: validation.consumerFiles,
      };
      resolve(finish(result));
    };

    orchestrator.on("close", (code, signal) => {
      orchestratorClosed = true;
      orchestratorClose = { code, signal };
      void maybeComplete(code === 0 ? "orchestrator_completed" : "orchestrator_failed");
    });
    orchestrator.on("error", (error) => {
      orchestratorClosed = true;
      orchestratorClose = { code: null, signal: `error:${error.message}` };
      void maybeComplete("orchestrator_error");
    });

    publisherStartHandle = setTimeout(() => {
      publisher = spawn("node", ["dist/streamer/src/publish.js"], {
        cwd: REPO_ROOT,
        env: { ...env, HIVE_PROCESS_ROLE: "benchmark_publisher" },
        stdio: ["ignore", publisherStdout, publisherStdout],
        detached: true,
      });

      publisher.on("close", (code, signal) => {
        publisherClosed = true;
        publisherClose = { code, signal };
        void maybeComplete(code === 0 ? "publisher_completed" : "publisher_failed");
      });
      publisher.on("error", (error) => {
        publisherClosed = true;
        publisherClose = { code: null, signal: `error:${error.message}` };
        void maybeComplete("publisher_error");
      });

      targetReachedPollHandle = setInterval(() => {
        if (targetReachedTerminationStarted || finalized) {
          return;
        }
        const benchmarkSummaries = collectBenchmarkWindowSummaries(runRoot, approach, kValue);
        if (!benchmarkSummaries.targetReached) {
          return;
        }
        targetReachedTerminationStarted = true;
        void Promise.all([
          publisher ? terminateChildProcessTree(publisher, { name: "publisher", logger: console.log }) : Promise.resolve(),
          orchestrator ? terminateChildProcessTree(orchestrator, { name: "orchestrator", logger: console.log }) : Promise.resolve(),
        ]).catch((error) => {
          console.error(`[target-stop] failed to terminate bounded run cleanly: ${error.message}`);
        });
      }, 500);
    }, 2500);

    timeoutHandle = setTimeout(() => {
      resolve(finish({
        success: false,
        error: `timeout after ${timeoutMs}ms`,
        reason: "timeout",
        endTime: new Date().toISOString(),
        runRoot,
      }));
    }, timeoutMs);
  }).finally(() => {
    try { fs.closeSync(orchestratorStdout); } catch {}
    try { fs.closeSync(publisherStdout); } catch {}
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const resultRoot = buildResultRoot(args.resumeRoot);
  ensureDir(resultRoot);
  ensureDir(path.join(resultRoot, "raw"));
  ensureDir(path.join(resultRoot, "checkpoints"));

  const repoState = collectRepoState();
  const matrix = buildCombinationMatrix({
    approaches: args.approaches,
    kValues: args.kValues,
    iterations: args.iterations,
  });
  const experimentConfig = {
    repoRoot: REPO_ROOT,
    resultRoot,
    repoState,
    kValues: args.kValues,
    approaches: args.approaches,
    iterations: args.iterations,
    targetWindows: TARGET_WINDOWS,
    outputWindowRangeMs: OUTPUT_WINDOW_RANGE_MS,
    outputWindowStepMs: OUTPUT_WINDOW_STEP_MS,
    subWindowRangeMs: SUB_WINDOW_RANGE_MS,
    subWindowStepMs: SUB_WINDOW_STEP_MS,
    aggregation: "AVG",
    inputStreams: ["wearableX", "smartphoneX"],
    deterministicReplay: true,
    replayDurationSeconds: REQUIRED_REPLAY_DURATION_SECONDS,
    totalConfigurations: args.kValues.length * args.approaches.length,
    totalExecutions: matrix.length,
    stopOnInvalid: args.stopOnInvalid,
  };
  writeJson(path.join(resultRoot, "experiment_config.json"), experimentConfig);
  writeJson(path.join(resultRoot, "repo_state.json"), repoState);

  console.log(`Expected matrix: ${experimentConfig.totalConfigurations} K/approach configurations`);
  console.log(`${experimentConfig.totalExecutions} total executions`);

  for (const combo of matrix) {
    const existing = readCheckpoint(resultRoot, combo.approach, combo.kValue, combo.iteration);
    if (existing?.success) {
      console.log(
        `[resume-skip] approach=${combo.approach} K=${combo.kValue} iteration=${combo.iteration} result_path=${existing.runRoot}`,
      );
      continue;
    }

    const result = await runSingleExecution({
      resultRoot,
      approach: combo.approach,
      kValue: combo.kValue,
      iteration: combo.iteration,
      timeoutMs: args.timeoutMs,
    });

    if (!result.success && args.stopOnInvalid && result.validation && !result.validation.ok) {
      throw new Error(
        `Stopping on structurally invalid result for ${combo.approach} K=${combo.kValue} iteration=${combo.iteration}: ${result.validation.failures.join("; ")}`,
      );
    }

    await delay(1500);
  }

  console.log(`Smoke execution complete. Result root: ${resultRoot}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
}
