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
  DEFAULT_ITERATIONS,
  DEFAULT_K_VALUES,
  OUTPUT_WINDOW_RANGE_MS,
  OUTPUT_WINDOW_STEP_MS,
  SUB_WINDOW_RANGE_MS,
  SUB_WINDOW_STEP_MS,
  TARGET_WINDOWS,
  buildCheckpointKey,
  buildCombinationMatrix,
  buildScenarioKey,
  countConsumerLatencyFiles,
  createScenarioReplayAnchors,
  extractAllConsumerWindows,
  extractRepresentativeWindow,
  median,
  parseApproachSelection,
  parseKScalingSelection,
  readJson,
  readProcessTreeMetrics,
  sanitizeTimestamp,
} = require("./local-k-scaling-smoke-common");
const { validateExactFinalRun } = require("./exact-final-validation");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const DEFAULT_TIMEOUT_MS = 6 * 60 * 1000;
const REQUIRED_REPLAY_DURATION_SECONDS = 185;
const CONTROL_PORT = 8080;
const STARTUP_READY_TIMEOUT_MS = 30 * 1000;
const TARGET_REACHED_SETTLE_TIMEOUT_MS = 15 * 1000;
const ARTIFACT_SETTLE_TIMEOUT_MS = 5 * 1000;
const POLL_INTERVAL_MS = 250;
const ATTEMPT_STATES = {
  VALID: "VALID",
  INVALID: "INVALID",
  PARTIAL: "PARTIAL",
  MISSING: "MISSING",
};
const APPROACHES = {
  fetching: {
    orchestrator: "dist/approaches/StreamingQueryFetchingKScalingOrchestrator.js",
  },
  "exact-final": {
    orchestrator: "dist/approaches/StreamingQueryExactFinalKScalingOrchestrator.js",
  },
  approximation: {
    orchestrator: "dist/approaches/StreamingQueryApproximationKScalingOrchestrator.js",
  },
};
const DEFAULT_APPROACHES = ["fetching", "exact-final", "approximation"];

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
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function parseArgs(argv) {
  const args = {
    kValues: DEFAULT_K_VALUES,
    approaches: DEFAULT_APPROACHES,
    iterations: DEFAULT_ITERATIONS,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    resumeRoot: null,
    stopOnInvalid: true,
    plan: false,
    onlyApproaches: null,
    onlyKValues: null,
    onlyIterations: null,
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
      case "--only-approach":
        args.onlyApproaches = parseApproachSelection(next, []);
        index += 1;
        break;
      case "--only-k":
        args.onlyKValues = parseKScalingSelection(next, []);
        index += 1;
        break;
      case "--only-iteration":
        args.onlyIterations = String(next || "")
          .split(",")
          .map((value) => Number.parseInt(value.trim(), 10))
          .filter((value) => Number.isFinite(value) && value > 0);
        index += 1;
        break;
      case "--plan":
        args.plan = true;
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
    if (!APPROACHES[approach]) {
      throw new Error(`Unsupported approach: ${approach}`);
    }
  }
  if (args.onlyApproaches) {
    args.approaches = args.approaches.filter((approach) => args.onlyApproaches.includes(approach));
  }
  if (args.onlyKValues) {
    args.kValues = args.kValues.filter((kValue) => args.onlyKValues.includes(kValue));
  }
  if (args.onlyIterations) {
    args.onlyIterations = [...new Set(args.onlyIterations)].sort((left, right) => left - right);
  }
  if (args.approaches.length === 0) {
    throw new Error("No approaches selected after filters");
  }
  if (args.kValues.length === 0) {
    throw new Error("No K values selected after filters");
  }
  return args;
}

function printHelp() {
  console.log(`Usage: node experiments/k-scaling/run-equivalent-query-exact-final-local-smoke.js [options]

Options:
  --k-values <list>      Comma-separated K values (default: 1,2,4,8,32)
  --approaches <list>    Comma-separated approaches (default: fetching,exact-final,approximation)
  --iterations <n>       Iterations per K/approach (default: 3)
  --timeout-ms <n>       Per-run timeout in milliseconds
  --resume-root <path>   Existing result root to resume
  --only-approach <list> Restrict execution to one or more approaches
  --only-k <list>        Restrict execution to one or more K values
  --only-iteration <n>   Restrict execution to one or more iterations
  --plan                 Print the ordered execution plan without running
  --no-stop-on-invalid   Continue after a structurally invalid result
`);
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
  return path.join(
    REPO_ROOT,
    "results",
    "paper-benchmarks",
    `s1-equivalent-query-exact-final-3x-local-${sanitizeTimestamp(new Date())}`,
  );
}

function buildRunRoot(resultRoot, approach, kValue, iteration) {
  return path.join(resultRoot, "raw", approach, `K${kValue}`, `iteration${iteration}`);
}

function buildScenarioRoot(resultRoot, kValue, iteration) {
  return path.join(resultRoot, "scenarios", `K${kValue}`, `iteration${iteration}`);
}

function buildScenarioManifestPath(resultRoot, kValue, iteration) {
  return path.join(buildScenarioRoot(resultRoot, kValue, iteration), "scenario-manifest.json");
}

function buildScenarioManifest({
  kValue,
  iteration,
  replayAnchor,
}) {
  return {
    scenario_id: buildScenarioKey(kValue, iteration),
    K: kValue,
    iteration,
    replay_anchor: replayAnchor,
    fixture: "custom_patterns/low_variability",
    seed: null,
    window_range_ms: OUTPUT_WINDOW_RANGE_MS,
    window_step_ms: OUTPUT_WINDOW_STEP_MS,
    aggregation: "AVG",
    target_streams: ["wearableX", "smartphoneX"],
    created_at: new Date().toISOString(),
  };
}

function ensureScenarioManifest({
  resultRoot,
  kValue,
  iteration,
  replayAnchor,
  allowCreate,
}) {
  const manifestPath = buildScenarioManifestPath(resultRoot, kValue, iteration);
  if (fs.existsSync(manifestPath)) {
    return readJson(manifestPath);
  }
  if (!allowCreate) {
    return null;
  }
  ensureDir(path.dirname(manifestPath));
  const manifest = buildScenarioManifest({ kValue, iteration, replayAnchor });
  writeJson(manifestPath, manifest);
  return manifest;
}

function isCompleteRunMetadata(metadata, { approach, kValue, iteration, replayAnchor }) {
  if (!metadata || typeof metadata !== "object") {
    return false;
  }
  return metadata.approach === approach &&
    metadata.kValue === kValue &&
    metadata.iteration === iteration &&
    metadata.replayAnchor === replayAnchor &&
    metadata.environment &&
    metadata.environment.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR === replayAnchor &&
    metadata.environment.K_SCALING_K === String(kValue);
}

function archiveAttemptDirectory(runRoot, stateLabel) {
  if (!fs.existsSync(runRoot)) {
    return null;
  }
  const suffix = `${stateLabel.toLowerCase()}-${sanitizeTimestamp(new Date())}`;
  let archivedPath = `${runRoot}.${suffix}`;
  let counter = 1;
  while (fs.existsSync(archivedPath)) {
    archivedPath = `${runRoot}.${suffix}-${counter}`;
    counter += 1;
  }
  fs.renameSync(runRoot, archivedPath);
  return archivedPath;
}

function buildRunEnv({ approach, kValue, iteration, runRoot, timeoutMs, replayAnchor }) {
  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const benchmarkStartTime = replayAnchor;
  const benchmarkMinTimestamp = Number.parseInt(replayAnchor, 10);
  const benchmarkMaxTimestamp = benchmarkMinTimestamp + (REQUIRED_REPLAY_DURATION_SECONDS * 1000);
  const topicPrefix = [
    "s1-equivalent-query-exact-final",
    approach,
    `K${kValue}`,
    `iteration${iteration}`,
  ].join("/");
  return replayEnv.withBenchmarkReplayEnv({
    ...process.env,
    LOG_PATH: runRoot,
    DATA_PATH: "custom_patterns/low_variability",
    SESSION_ID: `s1_exact_final_${approach}_K${kValue}_iteration${iteration}`,
    RESULT_TOPIC: `s1-equivalent-query-exact-final/results/${approach}/K${kValue}/iteration${iteration}`,
    BENCHMARK_SCENARIO: "s1-equivalent-query-exact-final",
    BENCHMARK_SCALE: `K${kValue}`,
    BENCHMARK_APPROACH: approach,
    BENCHMARK_ITERATION: String(iteration),
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(TARGET_WINDOWS),
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(REQUIRED_REPLAY_DURATION_SECONDS),
    STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS: String(timeoutMs),
    STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: benchmarkStartTime,
    STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: replayAnchor,
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(benchmarkMinTimestamp),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(benchmarkMaxTimestamp),
    AGGREGATION_FUNCTION: "AVG",
    OUTPUT_WINDOW_RANGE: String(OUTPUT_WINDOW_RANGE_MS),
    OUTPUT_WINDOW_STEP: String(OUTPUT_WINDOW_STEP_MS),
    SUB_WINDOW_RANGE: String(SUB_WINDOW_RANGE_MS),
    SUB_WINDOW_STEP: String(SUB_WINDOW_STEP_MS),
    K_SCALING_K: String(kValue),
    K_SCALING_REUSE_MODE: approach === "exact-final" ? "exact-final" : "chunk-state",
    HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE: approach === "exact-final" ? "true" : "false",
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

function hasCompleteScenarioMetadata({
  resultRoot,
  approach,
  kValue,
  iteration,
}) {
  const manifest = readJson(buildScenarioManifestPath(resultRoot, kValue, iteration));
  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  const metadata = readJson(path.join(runRoot, "run_metadata.json"));
  if (!manifest) {
    return { ok: false, reason: "scenario manifest missing" };
  }
  if (!isCompleteRunMetadata(metadata, {
    approach,
    kValue,
    iteration,
    replayAnchor: manifest.replay_anchor,
  })) {
    return { ok: false, reason: "run metadata incomplete or anchor mismatch" };
  }
  return { ok: true, manifest, metadata };
}

function isBoundedTargetStopExitAcceptable(code, signal, targetReached) {
  if (!targetReached) {
    return code === 0;
  }
  return code === 0 || code === 143 || signal === "SIGTERM";
}

function validateStandardRun({ runRoot, approach, kValue }) {
  const failures = [];
  const consumerFiles = countConsumerLatencyFiles(runRoot, approach, kValue);
  const allConsumerWindows = extractAllConsumerWindows(runRoot, approach, kValue);
  const representative = extractRepresentativeWindow(runRoot, approach, kValue, 1);
  const processTree = readProcessTreeMetrics(runRoot);
  const benchmarkSummary = readJson(path.join(runRoot, "benchmark_window_cap_summary.json"));

  if (consumerFiles.existing !== kValue) {
    failures.push(`consumer latency file count mismatch (${consumerFiles.existing}/${kValue})`);
  }
  if (!allConsumerWindows.ok) {
    failures.push("missing complete comparable first-window result for one or more consumers");
  }
  if (!hasSuccessfulTargetWindowCapSummary(benchmarkSummary)) {
    failures.push("benchmark window cap summary did not reach target");
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

  return {
    ok: failures.length === 0,
    failures,
    consumerFiles,
    allConsumerWindows,
    representative,
    processTree,
    topology: {
      queryExecutionCount: kValue,
      deliveryEventCount: kValue,
    },
  };
}

function validateRun({ runRoot, approach, kValue }) {
  const processTree = readProcessTreeMetrics(runRoot);
  if (approach === "exact-final") {
    const validation = validateExactFinalRun(runRoot, kValue);
    return {
      ...validation,
      processTree,
      representative: validation.deliveries[0] || null,
    };
  }
  return validateStandardRun({ runRoot, approach, kValue });
}

function readCheckpoint(resultRoot, approach, kValue, iteration) {
  const checkpointPath = path.join(resultRoot, "checkpoints", `${buildCheckpointKey(approach, kValue, iteration)}.json`);
  return fs.existsSync(checkpointPath) ? readJson(checkpointPath) : null;
}

function writeCheckpoint(resultRoot, approach, kValue, iteration, payload) {
  ensureDir(path.join(resultRoot, "checkpoints"));
  writeJson(path.join(resultRoot, "checkpoints", `${buildCheckpointKey(approach, kValue, iteration)}.json`), payload);
}

function buildStartupReadyPath(runRoot) {
  return path.join(runRoot, "startup_ready.json");
}

async function waitForFile(filePath, timeoutMs) {
  const startedAt = Date.now();
  while ((Date.now() - startedAt) < timeoutMs) {
    if (fs.existsSync(filePath)) {
      return true;
    }
    await delay(POLL_INTERVAL_MS);
  }
  return fs.existsSync(filePath);
}

async function waitForStructuralValidation({
  runRoot,
  approach,
  kValue,
  timeoutMs,
  requireTargetSummary = false,
  validateRunImpl = validateRun,
  readSummaryImpl = (summaryPath) => readJson(summaryPath),
  delayImpl = delay,
  pollIntervalMs = POLL_INTERVAL_MS,
}) {
  const startedAt = Date.now();
  const summaryPath = path.join(runRoot, "benchmark_window_cap_summary.json");
  let validation = validateRunImpl({ runRoot, approach, kValue });
  let benchmarkSummary = readSummaryImpl(summaryPath);
  while ((Date.now() - startedAt) < timeoutMs) {
    const targetReached = hasSuccessfulTargetWindowCapSummary(benchmarkSummary);
    if (validation.ok && (!requireTargetSummary || targetReached)) {
      return { ok: true, validation, benchmarkSummary };
    }
    await delayImpl(pollIntervalMs);
    validation = validateRunImpl({ runRoot, approach, kValue });
    benchmarkSummary = readSummaryImpl(summaryPath);
  }
  return { ok: false, validation, benchmarkSummary };
}

function shouldRequireStartupBarrier(approach) {
  return approach === "fetching" || approach === "exact-final";
}

function shouldTerminateBoundedRun({
  validationOk,
  targetReachedAt,
  now,
  settleTimeoutMs = TARGET_REACHED_SETTLE_TIMEOUT_MS,
}) {
  if (validationOk) {
    return true;
  }
  if (!Number.isFinite(targetReachedAt) || !Number.isFinite(now)) {
    return false;
  }
  return (now - targetReachedAt) >= settleTimeoutMs;
}

function shouldStopAfterFailedResult(result, stopOnInvalid) {
  if (!stopOnInvalid || result?.success !== false) {
    return false;
  }
  return true;
}

async function resolveReusableExecution(resultRoot, approach, kValue, iteration) {
  const checkpoint = readCheckpoint(resultRoot, approach, kValue, iteration);
  if (checkpoint?.success) {
    return checkpoint;
  }

  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  if (!fs.existsSync(runRoot)) {
    return null;
  }

  const validation = validateRun({ runRoot, approach, kValue });
  const metadataState = hasCompleteScenarioMetadata({
    resultRoot,
    approach,
    kValue,
    iteration,
  });
  if (!validation.ok || !metadataState.ok) {
    return null;
  }

  const repairedCheckpoint = {
    success: true,
    reason: "resume_revalidated",
    validation,
    endTime: new Date().toISOString(),
    runRoot,
    processTree: validation.processTree,
    replayAnchor:
      readJson(path.join(runRoot, "run_metadata.json"))?.replayAnchor ?? null,
  };
  writeCheckpoint(resultRoot, approach, kValue, iteration, repairedCheckpoint);
  return repairedCheckpoint;
}

function classifyExistingExecution(resultRoot, approach, kValue, iteration) {
  const checkpoint = readCheckpoint(resultRoot, approach, kValue, iteration);
  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  const execResultPath = path.join(runRoot, "execution_result.json");
  const hasRunDir = fs.existsSync(runRoot);
  const execResult = readJson(execResultPath);
  const metadataState = hasCompleteScenarioMetadata({
    resultRoot,
    approach,
    kValue,
    iteration,
  });

  if (!hasRunDir && !checkpoint) {
    return { state: ATTEMPT_STATES.MISSING, checkpoint: null, reason: "attempt directory missing" };
  }

  if (execResult) {
    const validationOk = execResult.validation?.ok === true;
    const success = execResult.success === true;
    if (success && validationOk && metadataState.ok) {
      return { state: ATTEMPT_STATES.VALID, checkpoint: checkpoint || execResult, reason: null };
    }
    return {
      state: ATTEMPT_STATES.INVALID,
      checkpoint: checkpoint || execResult,
      reason: metadataState.ok ? "execution result reports failure or invalid validation" : metadataState.reason,
    };
  }

  if (checkpoint && checkpoint.success === true) {
    return {
      state: ATTEMPT_STATES.INVALID,
      checkpoint,
      reason: "successful checkpoint missing validated execution result",
    };
  }

  if (checkpoint) {
    return {
      state: ATTEMPT_STATES.INVALID,
      checkpoint,
      reason: "failed checkpoint present",
    };
  }

  return {
    state: ATTEMPT_STATES.PARTIAL,
    checkpoint: null,
    reason: "attempt directory exists without validated execution result",
  };
}

function collectScenarioAnchorState(resultRoot, kValue, iteration, approaches) {
  const manifest = readJson(buildScenarioManifestPath(resultRoot, kValue, iteration));
  const anchors = new Map();
  for (const approach of approaches) {
    const metadata = readJson(path.join(
      buildRunRoot(resultRoot, approach, kValue, iteration),
      "run_metadata.json",
    ));
    if (metadata?.replayAnchor) {
      anchors.set(approach, metadata.replayAnchor);
    }
  }
  const distinctAnchors = [...new Set(anchors.values())];
  const conflict =
    (manifest?.replay_anchor && distinctAnchors.some((anchor) => anchor !== manifest.replay_anchor)) ||
    distinctAnchors.length > 1;
  return {
    manifest,
    anchors,
    conflict,
    reason: conflict ? "scenario anchor conflict detected" : null,
  };
}

function buildExecutionPlan({
  resultRoot,
  approaches,
  kValues,
  iterations,
  replayAnchors,
  allowCreateManifests = false,
}) {
  const matrix = buildCombinationMatrix({ approaches, kValues, iterations });
  const categoryBuckets = {
    [ATTEMPT_STATES.INVALID]: [],
    [ATTEMPT_STATES.PARTIAL]: [],
    [ATTEMPT_STATES.MISSING]: [],
  };

  const invalidatedScenarios = new Map();
  for (const kValue of kValues) {
    for (let iteration = 1; iteration <= iterations; iteration += 1) {
      const manifest = ensureScenarioManifest({
        resultRoot,
        kValue,
        iteration,
        replayAnchor: replayAnchors[buildScenarioKey(kValue, iteration)],
        allowCreate: allowCreateManifests,
      });
      const scenarioState = collectScenarioAnchorState(resultRoot, kValue, iteration, approaches);
      if (!manifest && scenarioState.anchors.size > 0) {
        invalidatedScenarios.set(buildScenarioKey(kValue, iteration), "scenario manifest missing for existing attempts");
      } else if (scenarioState.conflict) {
        invalidatedScenarios.set(buildScenarioKey(kValue, iteration), scenarioState.reason);
      }
    }
  }

  for (const combo of matrix) {
    const scenarioKey = buildScenarioKey(combo.kValue, combo.iteration);
    if (invalidatedScenarios.has(scenarioKey)) {
      categoryBuckets[ATTEMPT_STATES.INVALID].push({
        ...combo,
        state: ATTEMPT_STATES.INVALID,
        reason: invalidatedScenarios.get(scenarioKey),
      });
      continue;
    }

    const classification = classifyExistingExecution(
      resultRoot,
      combo.approach,
      combo.kValue,
      combo.iteration,
    );
    if (classification.state === ATTEMPT_STATES.VALID) {
      continue;
    }
    categoryBuckets[classification.state].push({
      ...combo,
      state: classification.state,
      reason: classification.reason,
    });
  }

  return [
    ...categoryBuckets[ATTEMPT_STATES.INVALID],
    ...categoryBuckets[ATTEMPT_STATES.PARTIAL],
    ...categoryBuckets[ATTEMPT_STATES.MISSING],
  ];
}

async function runSingleExecution({ resultRoot, approach, kValue, iteration, timeoutMs, replayAnchor }) {
  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  const priorClassification = classifyExistingExecution(resultRoot, approach, kValue, iteration);
  let archivedAttemptPath = null;
  if (priorClassification.state === ATTEMPT_STATES.INVALID) {
    archivedAttemptPath = archiveAttemptDirectory(runRoot, "failed");
  } else if (priorClassification.state === ATTEMPT_STATES.PARTIAL) {
    archivedAttemptPath = archiveAttemptDirectory(runRoot, "partial");
  } else {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
  ensureDir(runRoot);
  await cleanupStaleBenchmarkProcesses({ logger: console.log, quiescenceMs: 1000 });

  const env = buildRunEnv({ approach, kValue, iteration, runRoot, timeoutMs, replayAnchor });
  const orchestratorScript = APPROACHES[approach].orchestrator;
  const metadata = {
    approach,
    kValue,
    iteration,
    startTime: new Date().toISOString(),
    orchestratorScript,
    runRoot,
    timeoutMs,
    replayAnchor,
    archivedAttemptPath,
    environment: {
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR,
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
      K_SCALING_K: env.K_SCALING_K,
      K_SCALING_REUSE_MODE: env.K_SCALING_REUSE_MODE,
      HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE: env.HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE,
    },
  };
  writeJson(path.join(runRoot, "run_metadata.json"), metadata);

  console.log(`[run-start] approach=${approach} K=${kValue} iteration=${iteration} replay_anchor=${replayAnchor} result_path=${runRoot}`);

  let orchestrator = null;
  let publisher = null;
  let timeoutHandle = null;
  let publisherStartHandle = null;
  let targetReachedPollHandle = null;
  let finalized = false;
  const processTree = { sampler: null, artifactsWritten: false };

  const writeProcessTreeArtifacts = () => {
    if (!processTree.sampler || processTree.artifactsWritten) {
      return;
    }
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
      trackerNegativeDeltaCount: stopSummary.trackerNegativeDeltaCount ?? 0,
      trackerResetLikeDeltaCount: stopSummary.trackerResetLikeDeltaCount ?? 0,
      perPid: stopSummary.perPid ?? [],
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
    finalizeMqttTrafficArtifacts({ logDir: runRoot });
    await delay(1000);
    const stalePortResult = runCommand(`lsof -ti:${CONTROL_PORT} || true`);
    result.portClean = stalePortResult.trim() === "";
    if (!result.portClean) {
      result.error = result.error || `port ${CONTROL_PORT} still occupied`;
      result.success = false;
    }
    writeJson(path.join(runRoot, "execution_result.json"), result);
    writeCheckpoint(resultRoot, approach, kValue, iteration, result);
    console.log(`[run-end] approach=${approach} K=${kValue} iteration=${iteration} exit_status=${result.success ? "SUCCESS" : "FAILED"} result_path=${runRoot}`);
    return result;
  };

  const orchestratorStdout = fs.openSync(path.join(runRoot, `${approach}_orchestrator.stdout.log`), "a");
  const publisherStdout = fs.openSync(path.join(runRoot, "publisher.stdout.log"), "a");

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
    let publisherClosed = false;
    let orchestratorClose = { code: null, signal: null };
    let publisherClose = { code: null, signal: null };
    let targetReachedAt = null;
    let targetReachedTerminationStarted = false;

    const maybeComplete = async (reason = "completed") => {
      if (!orchestratorClosed || !publisherClosed) {
        return;
      }
      writeProcessTreeArtifacts();
      const settledValidation = await waitForStructuralValidation({
        runRoot,
        approach,
        kValue,
        timeoutMs: ARTIFACT_SETTLE_TIMEOUT_MS,
      });
      const validation = settledValidation.validation;
      const benchmarkSummary = settledValidation.benchmarkSummary;
      const targetReached = hasSuccessfulTargetWindowCapSummary(benchmarkSummary);
      const result = {
        success:
          isBoundedTargetStopExitAcceptable(orchestratorClose.code, orchestratorClose.signal, targetReached) &&
          isBoundedTargetStopExitAcceptable(publisherClose.code, publisherClose.signal, targetReached) &&
          validation.ok,
        reason,
        validation,
        orchestratorClose,
        publisherClose,
        endTime: new Date().toISOString(),
        runRoot,
        processTree: validation.processTree,
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
        if (finalized) {
          return;
        }
        const summary = readJson(path.join(runRoot, "benchmark_window_cap_summary.json"));
        if (!hasSuccessfulTargetWindowCapSummary(summary)) {
          return;
        }
        if (targetReachedAt === null) {
          targetReachedAt = Date.now();
        }
        if (targetReachedTerminationStarted) {
          return;
        }
        const validation = validateRun({ runRoot, approach, kValue });
        if (!shouldTerminateBoundedRun({
          validationOk: validation.ok,
          targetReachedAt,
          now: Date.now(),
        })) {
          return;
        }
        targetReachedTerminationStarted = true;
        void Promise.all([
          publisher ? terminateChildProcessTree(publisher, { name: "publisher", logger: console.log }) : Promise.resolve(),
          orchestrator ? terminateChildProcessTree(orchestrator, { name: "orchestrator", logger: console.log }) : Promise.resolve(),
        ]).catch((error) => {
          console.error(`[target-stop] failed to terminate bounded run cleanly: ${error.message}`);
        });
      }, POLL_INTERVAL_MS);
    };

    publisherStartHandle = setTimeout(() => {
      void (async () => {
        if (shouldRequireStartupBarrier(approach)) {
          const ready = await waitForFile(buildStartupReadyPath(runRoot), STARTUP_READY_TIMEOUT_MS);
          if (!ready) {
            resolve(finish({
              success: false,
              error: `startup readiness barrier timed out after ${STARTUP_READY_TIMEOUT_MS}ms`,
              reason: "startup_ready_timeout",
              endTime: new Date().toISOString(),
              runRoot,
            }));
            return;
          }
        }
        if (!finalized) {
          spawnPublisher();
        }
      })();
    }, shouldRequireStartupBarrier(approach) ? 0 : 2500);

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

  const baseAnchorMs = Number.parseInt(
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR || "",
    10,
  );
  const replayAnchors = createScenarioReplayAnchors({
    kValues: args.kValues,
    iterations: args.iterations,
    baseAnchorMs: Number.isFinite(baseAnchorMs) ? baseAnchorMs : Date.now(),
  });

  const filteredIterations = args.onlyIterations || Array.from(
    { length: args.iterations },
    (_, index) => index + 1,
  );

  if (!args.plan) {
    ensureDir(resultRoot);
    ensureDir(path.join(resultRoot, "raw"));
    ensureDir(path.join(resultRoot, "checkpoints"));
    ensureDir(path.join(resultRoot, "scenarios"));
  }

  const repoState = collectRepoState();
  const matrix = buildCombinationMatrix({
    approaches: args.approaches,
    kValues: args.kValues,
    iterations: args.iterations,
  });
  const filteredMatrix = matrix.filter((combo) => filteredIterations.includes(combo.iteration));
  const plan = buildExecutionPlan({
    resultRoot,
    approaches: args.approaches,
    kValues: args.kValues,
    iterations: args.iterations,
    replayAnchors,
    allowCreateManifests: !args.plan,
  }).filter((combo) => filteredIterations.includes(combo.iteration));

  if (args.plan) {
    console.log(`Plan for ${resultRoot}`);
    for (const combo of plan) {
      console.log(
        `${combo.state} K${combo.kValue} iteration${combo.iteration} ${combo.approach}${combo.reason ? ` :: ${combo.reason}` : ""}`,
      );
    }
    return;
  }

  writeJson(path.join(resultRoot, "repo_state.json"), repoState);
  writeJson(path.join(resultRoot, "experiment_config.json"), {
    repoRoot: REPO_ROOT,
    resultRoot,
    repoState,
    kValues: args.kValues,
    approaches: args.approaches,
    iterations: args.iterations,
    selectedIterations: filteredIterations,
    replayAnchors,
    targetWindows: TARGET_WINDOWS,
    outputWindowRangeMs: OUTPUT_WINDOW_RANGE_MS,
    outputWindowStepMs: OUTPUT_WINDOW_STEP_MS,
    subWindowRangeMs: SUB_WINDOW_RANGE_MS,
    subWindowStepMs: SUB_WINDOW_STEP_MS,
    aggregation: "AVG",
    inputStreams: ["wearableX", "smartphoneX"],
    deterministicReplay: true,
    totalExecutions: filteredMatrix.length,
    plannedExecutions: plan.length,
    resourceSamplingIntervalMs: 100,
    resourceScope: "orchestrator-rooted process tree; publisher and MQTT broker excluded",
  });

  console.log(`${filteredMatrix.length} total executions`);
  for (const combo of plan) {
    const reusableExecution = await resolveReusableExecution(
      resultRoot,
      combo.approach,
      combo.kValue,
      combo.iteration,
    );
    if (reusableExecution?.success) {
      console.log(`[resume-skip] approach=${combo.approach} K=${combo.kValue} iteration=${combo.iteration} result_path=${reusableExecution.runRoot}`);
      continue;
    }
    const replayAnchor = replayAnchors[buildScenarioKey(combo.kValue, combo.iteration)];
    const result = await runSingleExecution({
      resultRoot,
      approach: combo.approach,
      kValue: combo.kValue,
      iteration: combo.iteration,
      timeoutMs: args.timeoutMs,
      replayAnchor,
    });
    if (shouldStopAfterFailedResult(result, args.stopOnInvalid)) {
      const failureDetails = result.validation?.failures?.length
        ? result.validation.failures.join("; ")
        : (result.error || result.reason || "execution failed");
      throw new Error(
        `Stopping on failed result for ${combo.approach} K=${combo.kValue} iteration=${combo.iteration}: ${failureDetails}`,
      );
    }
    await delay(1500);
  }

  console.log(`S1 exact-final local smoke execution complete. Result root: ${resultRoot}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
}

module.exports = {
  ATTEMPT_STATES,
  buildExecutionPlan,
  buildRunEnv,
  classifyExistingExecution,
  collectScenarioAnchorState,
  ensureScenarioManifest,
  shouldStopAfterFailedResult,
  shouldTerminateBoundedRun,
  validateRun,
  waitForStructuralValidation,
};
