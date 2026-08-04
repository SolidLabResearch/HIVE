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
  OUTPUT_WINDOW_RANGE_MS,
  OUTPUT_WINDOW_STEP_MS,
  SUB_WINDOW_RANGE_MS,
  SUB_WINDOW_STEP_MS,
  TARGET_WINDOWS,
  buildCombinationMatrix,
  buildScenarioKey,
  countConsumerLatencyFiles,
  createScenarioReplayAnchors,
  extractAllConsumerWindows,
  extractRepresentativeWindow,
  parseApproachSelection,
  parseKScalingSelection,
  readJson,
  readProcessTreeMetrics,
  sanitizeTimestamp,
} = require("../k-scaling/local-k-scaling-smoke-common");
const { validateUnifiedSuperqueryRun } = require("./superquery-scaling-validation");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const CONTROL_PORT = 8080;
const REQUIRED_REPLAY_DURATION_SECONDS = 185;
const DEFAULT_TIMEOUT_MS = 6 * 60 * 1000;
const STARTUP_READY_TIMEOUT_MS = 30 * 1000;
const TARGET_REACHED_SETTLE_TIMEOUT_MS = 15 * 1000;
const ARTIFACT_SETTLE_TIMEOUT_MS = 5 * 1000;
const POLL_INTERVAL_MS = 250;
const DEFAULT_K_VALUES = [1, 2, 4, 8, 32];
const DEFAULT_APPROACHES = ["fetching", "approximation", "chunked"];
const APPROACHES = {
  fetching: {
    orchestrator: "dist/approaches/StreamingQueryFetchingKScalingOrchestrator.js",
    validationApproach: "fetching",
  },
  approximation: {
    orchestrator: "dist/approaches/StreamingQueryApproximationExactFinalKScalingOrchestrator.js",
    validationApproach: "approximation",
  },
  chunked: {
    orchestrator: "dist/approaches/StreamingQueryChunkedExactFinalKScalingOrchestrator.js",
    validationApproach: "chunked",
  },
};

function runCommand(command, workdir = REPO_ROOT) {
  return execSync(command, {
    cwd: workdir,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trimEnd();
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function parseArgs(argv) {
  const args = {
    kValues: DEFAULT_K_VALUES,
    approaches: DEFAULT_APPROACHES,
    iterations: 1,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    plan: false,
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
      case "--plan":
        args.plan = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (!Number.isFinite(args.iterations) || args.iterations <= 0) {
    throw new Error("--iterations must be a positive integer");
  }
  for (const approach of args.approaches) {
    if (!APPROACHES[approach]) {
      throw new Error(`Unsupported approach: ${approach}`);
    }
  }
  return args;
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

function buildResultRoot() {
  return path.join(
    REPO_ROOT,
    "results",
    "paper-benchmarks",
    `experiment3-same-superqueries-1x-local-${sanitizeTimestamp(new Date())}`,
  );
}

function buildRunRoot(resultRoot, approach, kValue, iteration) {
  return path.join(resultRoot, "raw", approach, `K${kValue}`, `iteration${iteration}`);
}

function buildScenarioManifestPath(resultRoot, kValue, iteration) {
  return path.join(resultRoot, "scenarios", `K${kValue}`, `iteration${iteration}`, "scenario-manifest.json");
}

function buildScenarioManifest({ kValue, iteration, replayAnchor, repoState }) {
  return {
    experiment: "Experiment 3: Increasing Number of Same Superqueries",
    scenario_id: buildScenarioKey(kValue, iteration),
    K: kValue,
    iteration,
    replay_anchor: replayAnchor,
    fixture: "custom_patterns/low_variability",
    seed: null,
    window_range_ms: OUTPUT_WINDOW_RANGE_MS,
    window_step_ms: OUTPUT_WINDOW_STEP_MS,
    sub_window_range_ms: SUB_WINDOW_RANGE_MS,
    sub_window_step_ms: SUB_WINDOW_STEP_MS,
    aggregation: "AVG",
    target_streams: ["wearableX", "smartphoneX"],
    target_final_query_identity: "AVG wearableX+smartphoneX RANGE 120000 STEP 60000",
    approximation_configuration: {
      completed_window_mode: true,
      early_trigger_mode: false,
      policy: "rate-based-completed-window",
      rate: null,
    },
    publisher_configuration: {
      finite_replay: true,
      target_windows: TARGET_WINDOWS,
      required_replay_duration_seconds: REQUIRED_REPLAY_DURATION_SECONDS,
      control_port: CONTROL_PORT,
    },
    commit: repoState.commit,
    branch: repoState.branch,
    created_at: new Date().toISOString(),
  };
}

function buildRunEnv({ approach, kValue, iteration, runRoot, timeoutMs, replayAnchor }) {
  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const benchmarkMinTimestamp = Number.parseInt(replayAnchor, 10);
  const benchmarkMaxTimestamp = benchmarkMinTimestamp + (REQUIRED_REPLAY_DURATION_SECONDS * 1000);
  const topicPrefix = [
    "experiment3-same-superqueries",
    approach,
    `K${kValue}`,
    `iteration${iteration}`,
  ].join("/");
  return replayEnv.withBenchmarkReplayEnv({
    ...process.env,
    LOG_PATH: runRoot,
    DATA_PATH: "custom_patterns/low_variability",
    SESSION_ID: `e3_${approach}_K${kValue}_iteration${iteration}`,
    RESULT_TOPIC: `experiment3/results/${approach}/K${kValue}/iteration${iteration}`,
    BENCHMARK_SCENARIO: "experiment3-same-superqueries",
    BENCHMARK_SCENARIO_ID: buildScenarioKey(kValue, iteration),
    BENCHMARK_SCALE: `K${kValue}`,
    BENCHMARK_APPROACH: approach,
    BENCHMARK_ITERATION: String(iteration),
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(TARGET_WINDOWS),
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(REQUIRED_REPLAY_DURATION_SECONDS),
    STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS: String(timeoutMs),
    STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: replayAnchor,
    STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: replayAnchor,
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(benchmarkMinTimestamp),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(benchmarkMaxTimestamp),
    AGGREGATION_FUNCTION: "AVG",
    OUTPUT_WINDOW_RANGE: String(OUTPUT_WINDOW_RANGE_MS),
    OUTPUT_WINDOW_STEP: String(OUTPUT_WINDOW_STEP_MS),
    SUB_WINDOW_RANGE: String(SUB_WINDOW_RANGE_MS),
    SUB_WINDOW_STEP: String(SUB_WINDOW_STEP_MS),
    K_SCALING_K: String(kValue),
    K_SCALING_REUSE_MODE: approach === "chunked" ? "hierarchical-exact-final" : "chunk-state",
    HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE: "false",
    STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "1",
    STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
    STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
    HIVE_PROFILE: "1",
  });
}

function hasSuccessfulTargetWindowCapSummary(summary) {
  return summary?.stoppedAfterTargetWindows === true &&
    summary?.stopReason === "target_window_count_reached" &&
    Number.isFinite(summary?.targetWindowCount) &&
    Number.isFinite(summary?.emittedFinalWindowCount) &&
    summary.emittedFinalWindowCount >= summary.targetWindowCount;
}

function validateFetchingRun({ runRoot, kValue }) {
  const failures = [];
  const consumerFiles = countConsumerLatencyFiles(runRoot, "fetching", kValue);
  const allConsumerWindows = extractAllConsumerWindows(runRoot, "fetching", kValue);
  const representative = extractRepresentativeWindow(runRoot, "fetching", kValue, 1);
  const processTree = readProcessTreeMetrics(runRoot);
  const benchmarkSummary = readJson(path.join(runRoot, "benchmark_window_cap_summary.json"));

  if (consumerFiles.existing !== kValue) failures.push(`consumer latency file count mismatch (${consumerFiles.existing}/${kValue})`);
  if (!allConsumerWindows.ok) failures.push("missing complete comparable first-window result for one or more consumers");
  if (!hasSuccessfulTargetWindowCapSummary(benchmarkSummary)) failures.push("benchmark window cap summary did not reach target");
  if (!representative.ok) failures.push(representative.reason || "representative first result missing");
  if (!Number.isFinite(processTree.averageCpuPct)) failures.push("average CPU missing");
  if (!Number.isFinite(processTree.peakRssMb)) failures.push("peak RSS missing");

  return {
    ok: failures.length === 0,
    failures,
    consumerFiles,
    allConsumerWindows,
    representative,
    processTree,
    topology: {
      heavyweightExecutionCount: kValue,
      deliveryEventCount: kValue,
      subscriberCount: kValue,
      exactCount: kValue,
    },
  };
}

function validateRun({ runRoot, approach, kValue, expectedValue }) {
  const processTree = readProcessTreeMetrics(runRoot);
  if (approach === "fetching") {
    return validateFetchingRun({ runRoot, kValue });
  }
  const validation = validateUnifiedSuperqueryRun(runRoot, approach, kValue, {
    expectedValue,
    valueTolerance: approach === "chunked" ? 1e-9 : undefined,
  });
  return {
    ...validation,
    processTree,
    representative: validation.deliveryEvents?.[0] || null,
  };
}

async function waitForStructuralValidation({ runRoot, approach, kValue, expectedValue, timeoutMs }) {
  const startedAt = Date.now();
  let validation = validateRun({ runRoot, approach, kValue, expectedValue });
  let benchmarkSummary = readJson(path.join(runRoot, "benchmark_window_cap_summary.json"));
  while ((Date.now() - startedAt) < timeoutMs) {
    if (validation.ok && hasSuccessfulTargetWindowCapSummary(benchmarkSummary)) {
      return { ok: true, validation, benchmarkSummary };
    }
    await delay(POLL_INTERVAL_MS);
    validation = validateRun({ runRoot, approach, kValue, expectedValue });
    benchmarkSummary = readJson(path.join(runRoot, "benchmark_window_cap_summary.json"));
  }
  return { ok: false, validation, benchmarkSummary };
}

async function waitForFile(filePath, timeoutMs) {
  const startedAt = Date.now();
  while ((Date.now() - startedAt) < timeoutMs) {
    if (fs.existsSync(filePath)) return true;
    await delay(POLL_INTERVAL_MS);
  }
  return fs.existsSync(filePath);
}

function isBoundedTargetStopExitAcceptable(code, signal, targetReached) {
  if (!targetReached) return code === 0;
  return code === 0 || code === 143 || signal === "SIGTERM";
}

async function runSingleExecution({ resultRoot, approach, kValue, iteration, replayAnchor, timeoutMs, expectedValue }) {
  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  fs.rmSync(runRoot, { recursive: true, force: true });
  ensureDir(runRoot);
  await cleanupStaleBenchmarkProcesses({ logger: console.log, quiescenceMs: 1000 });

  const env = buildRunEnv({ approach, kValue, iteration, runRoot, timeoutMs, replayAnchor });
  const orchestratorScript = APPROACHES[approach].orchestrator;
  writeJson(path.join(runRoot, "run_metadata.json"), {
    approach,
    kValue,
    iteration,
    replayAnchor,
    orchestratorScript,
    runRoot,
    environment: {
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR,
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
      K_SCALING_K: env.K_SCALING_K,
    },
  });
  console.log(`[run-start] approach=${approach} K=${kValue} iteration=${iteration} replay_anchor=${replayAnchor} result_path=${runRoot}`);

  let orchestrator = null;
  let publisher = null;
  let timeoutHandle = null;
  let publisherStartHandle = null;
  let targetReachedPollHandle = null;
  let finalized = false;
  const processTree = { sampler: null, artifactsWritten: false };

  const writeProcessTreeArtifacts = () => {
    if (!processTree.sampler || processTree.artifactsWritten) return;
    const stopSummary = processTree.sampler.stop();
    processTree.artifactsWritten = true;
    const metrics = readProcessTreeMetrics(runRoot);
    writeJson(path.join(runRoot, "resource_summary.json"), {
      rootPid: stopSummary.rootPid ?? null,
      logPath: stopSummary.logPath ?? path.join(runRoot, "process_tree_resource_usage.csv"),
      sampleCount: metrics.sampleCount,
      averageCpuPct: metrics.averageCpuPct,
      peakRssMb: metrics.peakRssMb,
      peakProcessCount: metrics.peakProcessCount,
      cpuSeconds: metrics.cpuSeconds,
      perPid: stopSummary.perPid ?? [],
    });
  };

  const orchestratorStdout = fs.openSync(path.join(runRoot, `${approach}_orchestrator.stdout.log`), "a");
  const publisherStdout = fs.openSync(path.join(runRoot, "publisher.stdout.log"), "a");

  return new Promise((resolve) => {
    const finish = async (result) => {
      if (finalized) return result;
      finalized = true;
      clearTimeout(timeoutHandle);
      clearTimeout(publisherStartHandle);
      clearInterval(targetReachedPollHandle);
      if (publisher) await terminateChildProcessTree(publisher, { name: "publisher", logger: console.log });
      if (orchestrator) await terminateChildProcessTree(orchestrator, { name: "orchestrator", logger: console.log });
      writeProcessTreeArtifacts();
      finalizeMqttTrafficArtifacts({ logDir: runRoot });
      await delay(1000);
      const stalePortResult = runCommand(`lsof -ti:${CONTROL_PORT} || true`);
      result.portClean = stalePortResult.trim() === "";
      if (!result.portClean) {
        result.error = result.error || `port ${CONTROL_PORT} still occupied`;
        result.success = false;
      }
      writeJson(path.join(runRoot, "execution_result.json"), result);
      console.log(`[run-end] approach=${approach} K=${kValue} iteration=${iteration} exit_status=${result.success ? "SUCCESS" : "FAILED"} result_path=${runRoot}`);
      resolve(result);
      return result;
    };

    try {
      orchestrator = spawn("node", [orchestratorScript], {
        cwd: REPO_ROOT,
        env: { ...env, HIVE_PROCESS_ROLE: `${approach}_experiment3_orchestrator` },
        stdio: ["ignore", orchestratorStdout, orchestratorStdout],
        detached: true,
      });
      processTree.sampler = startProcessTreeResourceLogging(
        path.join(runRoot, "process_tree_resource_usage.csv"),
        orchestrator.pid,
        100,
      );
    } catch (error) {
      void finish({ success: false, error: error.message, endTime: new Date().toISOString(), runRoot });
      return;
    }

    let orchestratorClosed = false;
    let publisherClosed = false;
    let orchestratorClose = { code: null, signal: null };
    let publisherClose = { code: null, signal: null };
    let targetReachedAt = null;
    let targetReachedTerminationStarted = false;

    const maybeComplete = async (reason = "completed") => {
      if (!orchestratorClosed || !publisherClosed) return;
      writeProcessTreeArtifacts();
      const settled = await waitForStructuralValidation({
        runRoot,
        approach,
        kValue,
        expectedValue,
        timeoutMs: ARTIFACT_SETTLE_TIMEOUT_MS,
      });
      const targetReached = hasSuccessfulTargetWindowCapSummary(settled.benchmarkSummary);
      const result = {
        success:
          isBoundedTargetStopExitAcceptable(orchestratorClose.code, orchestratorClose.signal, targetReached) &&
          isBoundedTargetStopExitAcceptable(publisherClose.code, publisherClose.signal, targetReached) &&
          settled.validation.ok,
        reason,
        validation: settled.validation,
        orchestratorClose,
        publisherClose,
        endTime: new Date().toISOString(),
        runRoot,
        processTree: settled.validation.processTree,
      };
      await finish(result);
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

    const spawnPublisher = () => {
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
        if (finalized) return;
        const summary = readJson(path.join(runRoot, "benchmark_window_cap_summary.json"));
        if (!hasSuccessfulTargetWindowCapSummary(summary)) return;
        if (targetReachedAt === null) targetReachedAt = Date.now();
        if (targetReachedTerminationStarted) return;
        const validation = validateRun({ runRoot, approach, kValue, expectedValue });
        const canTerminate = validation.ok ||
          ((Date.now() - targetReachedAt) >= TARGET_REACHED_SETTLE_TIMEOUT_MS);
        if (!canTerminate) return;
        targetReachedTerminationStarted = true;
        void Promise.all([
          publisher ? terminateChildProcessTree(publisher, { name: "publisher", logger: console.log }) : Promise.resolve(),
          orchestrator ? terminateChildProcessTree(orchestrator, { name: "orchestrator", logger: console.log }) : Promise.resolve(),
        ]);
      }, POLL_INTERVAL_MS);
    };

    publisherStartHandle = setTimeout(() => {
      void (async () => {
        const ready = await waitForFile(path.join(runRoot, "startup_ready.json"), STARTUP_READY_TIMEOUT_MS);
        if (!ready) {
          await finish({
            success: false,
            error: `startup readiness barrier timed out after ${STARTUP_READY_TIMEOUT_MS}ms`,
            reason: "startup_ready_timeout",
            endTime: new Date().toISOString(),
            runRoot,
          });
          return;
        }
        if (!finalized) spawnPublisher();
      })();
    }, 0);

    timeoutHandle = setTimeout(() => {
      void finish({
        success: false,
        error: `timeout after ${timeoutMs}ms`,
        reason: "timeout",
        endTime: new Date().toISOString(),
        runRoot,
      });
    }, timeoutMs);
  }).finally(() => {
    try { fs.closeSync(orchestratorStdout); } catch {}
    try { fs.closeSync(publisherStdout); } catch {}
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const resultRoot = buildResultRoot();
  const repoState = collectRepoState();
  const replayAnchors = createScenarioReplayAnchors({
    kValues: args.kValues,
    iterations: args.iterations,
    baseAnchorMs: Date.now(),
  });
  const matrix = buildCombinationMatrix({
    approaches: args.approaches,
    kValues: args.kValues,
    iterations: args.iterations,
  });

  if (args.plan) {
    console.log(`Plan for ${resultRoot}`);
    for (const combo of matrix) {
      console.log(`MISSING K${combo.kValue} iteration${combo.iteration} ${combo.approach} :: attempt directory missing`);
    }
    return;
  }

  ensureDir(resultRoot);
  ensureDir(path.join(resultRoot, "raw"));
  ensureDir(path.join(resultRoot, "scenarios"));
  writeJson(path.join(resultRoot, "repo_state.json"), repoState);
  writeJson(path.join(resultRoot, "experiment_config.json"), {
    experiment: "Experiment 3: Increasing Number of Same Superqueries",
    repoRoot: REPO_ROOT,
    resultRoot,
    repoState,
    kValues: args.kValues,
    approaches: args.approaches,
    iterations: args.iterations,
    replayAnchors,
    targetWindows: TARGET_WINDOWS,
    outputWindowRangeMs: OUTPUT_WINDOW_RANGE_MS,
    outputWindowStepMs: OUTPUT_WINDOW_STEP_MS,
    subWindowRangeMs: SUB_WINDOW_RANGE_MS,
    subWindowStepMs: SUB_WINDOW_STEP_MS,
    aggregation: "AVG",
    inputStreams: ["wearableX", "smartphoneX"],
    deterministicReplay: true,
    totalExecutions: matrix.length,
    resourceSamplingIntervalMs: 100,
    resourceScope: "orchestrator-rooted process tree; publisher and MQTT broker excluded",
  });
  for (const kValue of args.kValues) {
    for (let iteration = 1; iteration <= args.iterations; iteration += 1) {
      const replayAnchor = replayAnchors[buildScenarioKey(kValue, iteration)];
      writeJson(
        buildScenarioManifestPath(resultRoot, kValue, iteration),
        buildScenarioManifest({ kValue, iteration, replayAnchor, repoState }),
      );
    }
  }

  console.log(`${matrix.length} total executions`);
  const fetchingReferences = new Map();
  for (const combo of matrix) {
    const replayAnchor = replayAnchors[buildScenarioKey(combo.kValue, combo.iteration)];
    const expectedValue = fetchingReferences.get(buildScenarioKey(combo.kValue, combo.iteration));
    const result = await runSingleExecution({
      resultRoot,
      approach: combo.approach,
      kValue: combo.kValue,
      iteration: combo.iteration,
      replayAnchor,
      timeoutMs: args.timeoutMs,
      expectedValue,
    });
    if (!result.success) {
      const details = result.validation?.failures?.length
        ? result.validation.failures.join("; ")
        : (result.error || result.reason || "execution failed");
      throw new Error(`Stopping on failed result for ${combo.approach} K=${combo.kValue}: ${details}`);
    }
    if (combo.approach === "fetching") {
      fetchingReferences.set(
        buildScenarioKey(combo.kValue, combo.iteration),
        result.validation.representative.resultValue,
      );
    }
    await delay(1500);
  }

  console.log(`Experiment 3 same-superqueries local smoke complete. Result root: ${resultRoot}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
}

module.exports = {
  buildRunEnv,
  validateRun,
};
