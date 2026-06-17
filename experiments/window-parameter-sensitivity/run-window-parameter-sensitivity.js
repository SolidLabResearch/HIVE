#!/usr/bin/env node

const { execFileSync, execSync, spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const { getReplayMetadata } = require("../utils/benchmarkResultMetadata");
const { cleanupStaleBenchmarkProcesses } = require("../utils/processCleanup");
const {
  DEFAULT_AGGREGATION_FUNCTION,
  DEFAULT_APPROACHES,
  DEFAULT_PATTERNS,
  DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS,
  DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS,
  DEFAULT_REPLAY_DURATION_SECONDS,
  DEFAULT_SUPERQUERY_STEP_SECONDS,
  EXPERIMENTS,
  buildScenarioConfig,
  normalizeExperimentName,
  parseCsvList,
  parsePositiveIntList,
} = require("./common");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function openLogFds(stdoutPath, stderrPath) {
  fs.mkdirSync(path.dirname(stdoutPath), { recursive: true });
  fs.mkdirSync(path.dirname(stderrPath), { recursive: true });

  return {
    stdoutFd: fs.openSync(stdoutPath, "a"),
    stderrFd: fs.openSync(stderrPath, "a"),
  };
}

function closeLogFds(fds) {
  if (!fds) {
    return;
  }

  for (const fd of [fds.stdoutFd, fds.stderrFd]) {
    try {
      if (typeof fd === "number") {
        fs.closeSync(fd);
      }
    } catch {
      // ignore close errors
    }
  }
}

function terminateProcessTree(child, label, graceMs = 5000) {
  if (!child || !child.pid) {
    return Promise.resolve({
      sigtermSent: false,
      sigkillSent: false,
      sigkillRequired: false,
    });
  }

  return (async () => {
    console.log(`[cleanup] terminating process group label=${label} pid=${child.pid}`);

    let sigtermSent = false;
    let sigkillSent = false;
    let sigkillRequired = false;

    try {
      process.kill(-child.pid, "SIGTERM");
      sigtermSent = true;
    } catch {
      try {
        process.kill(child.pid, "SIGTERM");
        sigtermSent = true;
      } catch {
        // ignore
      }
    }

    await sleep(graceMs);

    let alive = false;
    try {
      process.kill(child.pid, 0);
      alive = true;
    } catch {
      // process already gone
    }

    if (alive) {
      try {
        process.kill(-child.pid, "SIGKILL");
        sigkillSent = true;
        sigkillRequired = true;
      } catch {
        try {
          process.kill(child.pid, "SIGKILL");
          sigkillSent = true;
          sigkillRequired = true;
        } catch {
          // ignore
        }
      }
    }

    return { sigtermSent, sigkillSent, sigkillRequired };
  })();
}

function getSelfExcludingPattern(pattern) {
  const parts = pattern.split("/");
  const lastPart = parts[parts.length - 1];
  if (lastPart.length > 0) {
    parts[parts.length - 1] = `[${lastPart[0]}]${lastPart.slice(1)}`;
  }
  return parts.join("/");
}

function checkStaleProcesses() {
  const patterns = [
    "StreamingQueryFetchingClientSideApproachOrchestrator",
    "StreamingQueryChunkedApproachOrchestrator",
    "dist/services/BeeWorker.js",
    "dist/streamer/src/publish",
  ];
  const foundProcesses = [];

  for (const pattern of patterns) {
    try {
      const selfExcluding = getSelfExcludingPattern(pattern);
      const output = execSync(`pgrep -f "${selfExcluding}"`, {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      });
      const pids = output
        .trim()
        .split("\n")
        .map((value) => Number.parseInt(value.trim(), 10))
        .filter((value) => Number.isFinite(value) && value !== process.pid);
      if (pids.length > 0) {
        foundProcesses.push({ pattern, pids });
      }
    } catch {
      // pgrep exit code 1 means no match
    }
  }

  return {
    foundStale: foundProcesses.length > 0,
    foundProcesses,
  };
}

function parsePsSnapshot(psOutput) {
  return psOutput
    .trim()
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.split(/\s+/, 5);
      if (parts.length < 4) {
        return null;
      }

      const pid = Number.parseInt(parts[0], 10);
      const ppid = Number.parseInt(parts[1], 10);
      const cpu = Number.parseFloat(parts[2]);
      const rssKb = Number.parseFloat(parts[3]);
      const command = parts[4] || "";

      if (!Number.isFinite(pid) || !Number.isFinite(ppid)) {
        return null;
      }

      return {
        pid,
        ppid,
        cpu: Number.isFinite(cpu) ? cpu : 0,
        rssKb: Number.isFinite(rssKb) ? rssKb : 0,
        command,
      };
    })
    .filter(Boolean);
}

function collectTreeStatsFromPs(rootPids) {
  const liveRootPids = rootPids.filter((pid) => {
    if (!Number.isFinite(pid)) {
      return false;
    }
    try {
      process.kill(pid, 0);
      return true;
    } catch {
      return false;
    }
  });

  if (liveRootPids.length === 0) {
    return null;
  }

  let snapshot;
  try {
    snapshot = parsePsSnapshot(
      execSync("ps -axo pid=,ppid=,%cpu=,rss=,command=", {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      }),
    );
  } catch {
    return null;
  }

  const childrenByParent = new Map();
  for (const proc of snapshot) {
    const children = childrenByParent.get(proc.ppid) || [];
    children.push(proc.pid);
    childrenByParent.set(proc.ppid, children);
  }

  const pidMap = new Map(snapshot.map((proc) => [proc.pid, proc]));
  const seen = new Set();
  const queue = [...liveRootPids];
  const tree = [];

  while (queue.length > 0) {
    const pid = queue.shift();
    if (seen.has(pid)) {
      continue;
    }
    seen.add(pid);

    const proc = pidMap.get(pid);
    if (!proc) {
      continue;
    }

    tree.push(proc);
    const children = childrenByParent.get(pid) || [];
    for (const childPid of children) {
      if (!seen.has(childPid)) {
        queue.push(childPid);
      }
    }
  }

  if (tree.length === 0) {
    return null;
  }

  const totalCpuPct = tree.reduce((sum, proc) => sum + proc.cpu, 0);
  const totalRssKb = tree.reduce((sum, proc) => sum + proc.rssKb, 0);

  return {
    rootPids: liveRootPids,
    tree,
    processCount: tree.length,
    totalCpuPct,
    meanCpuPct: totalCpuPct,
    peakCpuPct: totalCpuPct,
    totalRssMb: totalRssKb / 1024,
    meanRssMb: totalRssKb / 1024,
    peakRssMb: tree.reduce((max, proc) => Math.max(max, proc.rssKb / 1024), 0),
  };
}

class AttemptResourceSampler {
  constructor(logDir, options = {}) {
    this.logDir = logDir;
    this.sampleIntervalMs = options.sampleIntervalMs || 500;
    this.getRootPids = options.getRootPids || (() => []);
    this.startedAt = null;
    this.samples = [];
    this.timer = null;
    this.stopped = false;
    this.running = false;
    this.pidSamples = new Map();
  }

  start() {
    if (this.timer) {
      return;
    }

    this.startedAt = Date.now();
    this.timer = setInterval(() => {
      void this.sample();
    }, this.sampleIntervalMs);
    this.timer.unref?.();
    void this.sample();
  }

  async sample() {
    if (this.stopped || this.running) {
      return;
    }

    this.running = true;
    try {
      const stats = collectTreeStatsFromPs(this.getRootPids());
      if (!stats) {
        return;
      }

      const timestamp = Date.now();
      const elapsedMs = this.startedAt ? timestamp - this.startedAt : 0;
      this.samples.push({
        timestamp,
        elapsedMs,
        ...stats,
      });

      for (const proc of stats.tree || []) {
        const bucket = this.pidSamples.get(proc.pid) || [];
        bucket.push({
          timestamp,
          elapsedMs,
          pid: proc.pid,
          ppid: proc.ppid,
          cpuPct: proc.cpu,
          rssMb: proc.rssKb / 1024,
          command: proc.command,
        });
        this.pidSamples.set(proc.pid, bucket);
      }
    } finally {
      this.running = false;
    }
  }

  summarize() {
    const sampleCount = this.samples.length;
    const wallTimeMs = this.startedAt ? Date.now() - this.startedAt : 0;
    const meanCpuPct =
      sampleCount > 0
        ? this.samples.reduce((sum, sample) => sum + sample.totalCpuPct, 0) / sampleCount
        : 0;
    const peakCpuPct =
      sampleCount > 0 ? Math.max(...this.samples.map((sample) => sample.totalCpuPct)) : 0;
    const meanRssMb =
      sampleCount > 0
        ? this.samples.reduce((sum, sample) => sum + sample.totalRssMb, 0) / sampleCount
        : 0;
    const peakRssMb =
      sampleCount > 0 ? Math.max(...this.samples.map((sample) => sample.totalRssMb)) : 0;
    const peakProcessCount =
      sampleCount > 0 ? Math.max(...this.samples.map((sample) => sample.processCount)) : 0;
    const cpuSeconds = (meanCpuPct / 100) * (wallTimeMs / 1000);

    return {
      sampleIntervalMs: this.sampleIntervalMs,
      sampleCount,
      wallTimeMs,
      wallTimeSec: wallTimeMs / 1000,
      meanCpuPct,
      peakCpuPct,
      cpuSeconds,
      meanRssMb,
      peakRssMb,
      peakProcessCount,
    };
  }

  writeArtifacts(metadata = {}) {
    fs.mkdirSync(this.logDir, { recursive: true });

    const csvPath = path.join(this.logDir, "resource_usage.csv");
    const summaryPath = path.join(this.logDir, "resource_summary.json");
    const perPidPath = path.join(this.logDir, "resource_per_pid_summary.json");
    const lines = [
      "timestamp,elapsed_ms,root_pid_count,process_count,total_cpu_pct,total_rss_mb,peak_rss_mb",
      ...this.samples.map((sample) =>
        [
          sample.timestamp,
          sample.elapsedMs,
          sample.rootPids.length,
          sample.processCount,
          sample.totalCpuPct.toFixed(4),
          sample.totalRssMb.toFixed(3),
          sample.peakRssMb.toFixed(3),
        ].join(","),
      ),
    ];

    fs.writeFileSync(csvPath, `${lines.join("\n")}\n`);

    const perPidSummary = Array.from(this.pidSamples.entries())
      .map(([pid, samples]) => {
        const sampleCount = samples.length;
        const meanCpuPct =
          sampleCount > 0
            ? samples.reduce((sum, sample) => sum + sample.cpuPct, 0) / sampleCount
            : 0;
        const peakCpuPct =
          sampleCount > 0 ? Math.max(...samples.map((sample) => sample.cpuPct)) : 0;
        const meanRssMb =
          sampleCount > 0
            ? samples.reduce((sum, sample) => sum + sample.rssMb, 0) / sampleCount
            : 0;
        const peakRssMb =
          sampleCount > 0 ? Math.max(...samples.map((sample) => sample.rssMb)) : 0;
        const wallTimeSec =
          sampleCount > 1
            ? (samples[samples.length - 1].elapsedMs - samples[0].elapsedMs) / 1000
            : 0;
        return {
          pid,
          ppid: samples[0]?.ppid ?? null,
          command: samples[0]?.command || "",
          sampleCount,
          wallTimeSec,
          cpuSeconds: (meanCpuPct / 100) * wallTimeSec,
          meanCpuPct,
          peakCpuPct,
          meanRssMb,
          peakRssMb,
        };
      })
      .sort((left, right) => right.cpuSeconds - left.cpuSeconds);

    fs.writeFileSync(perPidPath, `${JSON.stringify(perPidSummary, null, 2)}\n`);
    fs.writeFileSync(
      summaryPath,
      `${JSON.stringify(
        {
          ...metadata,
          ...this.summarize(),
          csvPath: path.resolve(csvPath),
          summaryPath: path.resolve(summaryPath),
          perPidSummaryPath: path.resolve(perPidPath),
        },
        null,
        2,
      )}\n`,
    );

    return { csvPath, summaryPath, perPidPath };
  }

  async stop(metadata = {}) {
    if (this.stopped) {
      return this.writeArtifacts(metadata);
    }

    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    await this.sample();
    return this.writeArtifacts(metadata);
  }
}

function sumNumericFields(records, fieldNames) {
  const result = {};
  for (const fieldName of fieldNames) {
    result[fieldName] = records.reduce((sum, record) => {
      const value = Number(record?.[fieldName]);
      return sum + (Number.isFinite(value) ? value : 0);
    }, 0);
  }
  return result;
}

function buildProfileAggregate(logDir, metadata = {}) {
  if (!fs.existsSync(logDir)) {
    return null;
  }

  const profileFiles = fs
    .readdirSync(logDir)
    .filter(
      (fileName) =>
        /^hive_profile_summary\.[^.]+(?:_consumer_\d+)?\.json$/.test(fileName) &&
        fileName !== "hive_profile_summary.aggregate.json",
    )
    .sort();

  const processProfiles = profileFiles.map((fileName) => {
    const filePath = path.join(logDir, fileName);
    return {
      fileName,
      filePath: path.resolve(filePath),
      ...JSON.parse(fs.readFileSync(filePath, "utf8")),
    };
  });

  const summedCounterNames = [
    "shared_chunk_producers_created",
    "chunk_state_messages_published",
    "fallback_original_agent_rsps_started",
    "reconstructed_superquery_results",
    "rsp_engines_created",
    "mqtt_clients_created",
    "compatible_queries_detected",
    "original_agent_rsps_skipped",
    "original_agent_outputs_derived_from_chunks",
    "exact_final_result_reuse_hits",
    "final_result_topics_created",
    "final_result_topics_reused",
    "final_result_subscribers_registered",
    "chunk_reuse_paths_created",
    "chunk_reuse_paths_skipped_due_to_exact_hit",
    "reconstruction_paths_created",
    "reconstruction_paths_skipped",
    "fresh_executions_started",
    "canonical_query_hashes_seen",
    "chunk_consumers_registered",
    "chunk_groups_completed",
    "comparable_windows_emitted",
    "emitted_results",
    "rsp_query_processes_started",
    "duplicateChunkCount",
    "missingChunkGroups",
  ];

  const aggregate = {
    timestamp: new Date().toISOString(),
    logDir: path.resolve(logDir),
    processCount: processProfiles.length,
    metadata,
    processProfiles: processProfiles.map((profile) => ({
      fileName: profile.fileName,
      filePath: profile.filePath,
      pid: profile.processId,
      processRole: profile.processRole || null,
      processRoleGroup: profile.processRoleGroup || null,
      counters: profile.counters || {},
      timingsMs: profile.timingsMs || {},
    })),
    summedCounters: sumNumericFields(processProfiles, summedCounterNames),
  };

  const aggregatePath = path.join(logDir, "hive_profile_summary.aggregate.json");
  fs.writeFileSync(aggregatePath, `${JSON.stringify(aggregate, null, 2)}\n`);
  return {
    aggregatePath: path.resolve(aggregatePath),
    processProfiles: aggregate.processProfiles,
    summedCounters: aggregate.summedCounters,
  };
}

function getApproachScript(approach) {
  const mapping = {
    fetching: "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
    chunked: "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
  };

  const script = mapping[approach];
  if (!script) {
    throw new Error(`Unsupported approach "${approach}"`);
  }
  return path.resolve(process.cwd(), script);
}

function getRequiredResultFiles(approach) {
  if (approach === "fetching") {
    return ["fetching_latency_log.csv"];
  }
  if (approach === "chunked") {
    return [
      "chunked_latency_log.csv",
      "chunked_debug_summary.json",
      "chunked_emission_proof.json",
    ];
  }
  throw new Error(`Unsupported approach "${approach}"`);
}

function verifyExpectedOutputFiles(approach, logDir) {
  const requiredFiles = getRequiredResultFiles(approach);
  for (const fileName of requiredFiles) {
    const filePath = path.join(logDir, fileName);
    if (!fs.existsSync(filePath)) {
      return {
        valid: false,
        reason: `Required result file missing: ${fileName}`,
        emittedCount: 0,
      };
    }
  }

  const latencyFilePath = path.join(
    logDir,
    approach === "fetching" ? "fetching_latency_log.csv" : "chunked_latency_log.csv",
  );
  const content = fs.readFileSync(latencyFilePath, "utf8").trim();
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length <= 1) {
    return {
      valid: false,
      reason: `No emitted result windows found in ${path.basename(latencyFilePath)}`,
      emittedCount: 0,
    };
  }

  return {
    valid: true,
    requiredFiles,
    emittedCount: Math.max(0, lines.length - 1),
  };
}

function resolveScenarioList(options) {
  const experimentDefinition = EXPERIMENTS[options.experimentName];
  if (!experimentDefinition) {
    throw new Error(`Missing experiment definition for "${options.experimentName}"`);
  }

  if (options.experimentName === "superquery-range-scaling") {
    return options.ranges.length > 0 ? options.ranges : experimentDefinition.defaultScenarios;
  }
  return options.chunkSizes.length > 0
    ? options.chunkSizes
    : experimentDefinition.defaultScenarios;
}

function parseArgs(argv) {
  const parsed = {
    experimentName: null,
    iterations: 1,
    timeoutMs: null,
    replayDurationSeconds: DEFAULT_REPLAY_DURATION_SECONDS,
    approaches: [...DEFAULT_APPROACHES],
    patterns: [...DEFAULT_PATTERNS],
    ranges: [],
    chunkSizes: [],
    aggregationFunction: DEFAULT_AGGREGATION_FUNCTION,
    superqueryStepSeconds: DEFAULT_SUPERQUERY_STEP_SECONDS,
    rangeScalingSubWindowRangeSeconds:
      DEFAULT_RANGE_SCALING_SUB_WINDOW_RANGE_SECONDS,
    rangeScalingSubWindowStepSeconds:
      DEFAULT_RANGE_SCALING_SUB_WINDOW_STEP_SECONDS,
    extractAfterRun: true,
    skipBuild: false,
    logRoot: path.resolve(process.cwd(), "logs/window-parameter-sensitivity"),
    resultRoot: path.resolve(process.cwd(), "results/window-parameter-sensitivity"),
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    switch (arg) {
      case "--experiment":
        if (!next) throw new Error("--experiment requires a value");
        parsed.experimentName = normalizeExperimentName(next);
        index += 1;
        break;
      case "--iterations":
      case "-i":
        if (!next) throw new Error("--iterations requires a value");
        parsed.iterations = Number.parseInt(next, 10);
        index += 1;
        break;
      case "--timeout-ms":
      case "-t":
        if (!next) throw new Error("--timeout-ms requires a value");
        parsed.timeoutMs = Number.parseInt(next, 10);
        index += 1;
        break;
      case "--replay-duration-seconds":
        if (!next) throw new Error("--replay-duration-seconds requires a value");
        parsed.replayDurationSeconds = Number.parseInt(next, 10);
        index += 1;
        break;
      case "--patterns":
        if (!next) throw new Error("--patterns requires a value");
        parsed.patterns = parseCsvList(next);
        index += 1;
        break;
      case "--approaches":
        if (!next) throw new Error("--approaches requires a value");
        parsed.approaches = parseCsvList(next).map((value) => value.toLowerCase());
        index += 1;
        break;
      case "--ranges":
        if (!next) throw new Error("--ranges requires a value");
        parsed.ranges = parsePositiveIntList(next);
        index += 1;
        break;
      case "--chunk-sizes":
        if (!next) throw new Error("--chunk-sizes requires a value");
        parsed.chunkSizes = parsePositiveIntList(next);
        index += 1;
        break;
      case "--aggregation":
        if (!next) throw new Error("--aggregation requires a value");
        parsed.aggregationFunction = String(next).trim().toUpperCase();
        index += 1;
        break;
      case "--superquery-step-seconds":
        if (!next) throw new Error("--superquery-step-seconds requires a value");
        parsed.superqueryStepSeconds = Number.parseInt(next, 10);
        index += 1;
        break;
      case "--range-scaling-sub-window-range-seconds":
        if (!next) {
          throw new Error("--range-scaling-sub-window-range-seconds requires a value");
        }
        parsed.rangeScalingSubWindowRangeSeconds = Number.parseInt(next, 10);
        index += 1;
        break;
      case "--range-scaling-sub-window-step-seconds":
        if (!next) {
          throw new Error("--range-scaling-sub-window-step-seconds requires a value");
        }
        parsed.rangeScalingSubWindowStepSeconds = Number.parseInt(next, 10);
        index += 1;
        break;
      case "--log-root":
        if (!next) throw new Error("--log-root requires a value");
        parsed.logRoot = path.resolve(process.cwd(), next);
        index += 1;
        break;
      case "--result-root":
        if (!next) throw new Error("--result-root requires a value");
        parsed.resultRoot = path.resolve(process.cwd(), next);
        index += 1;
        break;
      case "--skip-build":
        parsed.skipBuild = true;
        break;
      case "--no-extract":
        parsed.extractAfterRun = false;
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

  if (!parsed.experimentName) {
    throw new Error("--experiment is required");
  }
  if (!Number.isFinite(parsed.iterations) || parsed.iterations <= 0) {
    throw new Error("--iterations must be a positive integer");
  }
  if (
    !Number.isFinite(parsed.replayDurationSeconds) ||
    parsed.replayDurationSeconds <= 0
  ) {
    throw new Error("--replay-duration-seconds must be a positive integer");
  }

  if (!parsed.timeoutMs) {
    parsed.timeoutMs = Math.max(
      300000,
      parsed.replayDurationSeconds * 1000 + 180000,
    );
  }

  return parsed;
}

function printHelp() {
  console.log(`Usage: node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js [options]

Required:
  --experiment <name>                         superquery-range-scaling | chunk-granularity-sensitivity

Common options:
  --iterations <n>                           Default: 1
  --replay-duration-seconds <n>              Default: 900
  --timeout-ms <n>                           Default: replay duration + 180000
  --patterns <list>                          Default: low_variability
  --approaches <list>                        Default: fetching,chunked
  --aggregation <name>                       Default: AVG
  --superquery-step-seconds <n>              Default: 60
  --log-root <path>                          Default: logs/window-parameter-sensitivity
  --result-root <path>                       Default: results/window-parameter-sensitivity
  --skip-build                               Skip npm run build
  --no-extract                               Do not run the extractor after the benchmark

Experiment 2 options:
  --ranges <list>                            Example: 120,180,240
  --range-scaling-sub-window-range-seconds   Default: 60
  --range-scaling-sub-window-step-seconds    Default: 30

Experiment 3 options:
  --chunk-sizes <list>                       Example: 1,5,15,30,60
`);
}

class WindowParameterSensitivityRunner {
  constructor(options) {
    this.options = options;
    this.baseLogDir = path.join(options.logRoot, options.experimentName);
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);
    this.activeAttemptCleanup = null;
    this.shutdownInProgress = false;
    this.installSignalHandlers();
  }

  installSignalHandlers() {
    const handleSignal = async (signal, exitCode) => {
      if (this.shutdownInProgress) {
        return;
      }
      this.shutdownInProgress = true;
      console.log(`Received ${signal}; cleaning up active benchmark processes.`);
      if (this.activeAttemptCleanup) {
        await this.activeAttemptCleanup();
      }
      await cleanupStaleBenchmarkProcesses({ logger: console.log });
      process.exit(exitCode);
    };

    process.on("SIGINT", () => void handleSignal("SIGINT", 130));
    process.on("SIGTERM", () => void handleSignal("SIGTERM", 143));
  }

  getIterationRootDir({ approach, patternName, scenarioLabel, iterationNum }) {
    return path.join(
      this.baseLogDir,
      approach,
      patternName,
      scenarioLabel,
      `iteration${iterationNum}`,
    );
  }

  async runSingleTest({ approach, patternName, scenarioSeconds, iterationNum }) {
    const scenarioConfig = buildScenarioConfig({
      experimentName: this.options.experimentName,
      scenarioSeconds,
      aggregationFunction: this.options.aggregationFunction,
      pattern: patternName,
      iteration: iterationNum,
      replayDurationSeconds: this.options.replayDurationSeconds,
      superqueryStepSeconds: this.options.superqueryStepSeconds,
      rangeScalingSubWindowRangeSeconds:
        this.options.rangeScalingSubWindowRangeSeconds,
      rangeScalingSubWindowStepSeconds:
        this.options.rangeScalingSubWindowStepSeconds,
      exactFinalReuseEnabled: false,
    });

    const minimumReplayDurationSeconds =
      scenarioConfig.metadata.superquery_range_seconds +
      scenarioConfig.metadata.superquery_step_seconds;
    if (
      scenarioConfig.metadata.replay_duration_seconds <
      minimumReplayDurationSeconds
    ) {
      throw new Error(
        `Replay duration ${scenarioConfig.metadata.replay_duration_seconds}s is too short for ${scenarioConfig.scenarioLabel}; expected at least ${minimumReplayDurationSeconds}s to emit a full comparable window`,
      );
    }

    const logDir = this.getIterationRootDir({
      approach,
      patternName,
      scenarioLabel: scenarioConfig.scenarioLabel,
      iterationNum,
    });
    fs.mkdirSync(logDir, { recursive: true });

    console.log("\n------------------------------------------------------------");
    console.log(
      `Running ${scenarioConfig.metadata.experiment_name}: approach=${approach} pattern=${patternName} scenario=${scenarioConfig.scenarioLabel} iteration=${iterationNum}`,
    );
    console.log(`Log Directory: ${logDir}`);
    console.log("------------------------------------------------------------");

    await cleanupStaleBenchmarkProcesses({ logger: () => {} });

    const topicPrefix = [
      "benchmark",
      "window-parameter-sensitivity",
      this.options.experimentName,
      approach,
      patternName,
      scenarioConfig.scenarioLabel,
      `iteration${iterationNum}`,
      Date.now().toString(36),
    ].join("/");
    const sessionToken = `${this.options.experimentName}_${approach}_${patternName}_${scenarioConfig.scenarioLabel}_iter${iterationNum}_${Date.now().toString(36)}`;

    const runMetadata = {
      ...scenarioConfig.metadata,
      approach,
      pattern: patternName,
      iteration: iterationNum,
      scenario_label: scenarioConfig.scenarioLabel,
      log_dir: path.resolve(logDir),
      exact_final_reuse_enabled: false,
    };

    const testEnv = this.replayEnv.withBenchmarkReplayEnv({
      ...process.env,
      ...scenarioConfig.env,
      SESSION_ID: sessionToken,
      LOG_PATH: logDir,
      LOG_DISABLE_FILE_OUTPUT: "0",
      HIVE_PROFILE: "1",
      HIVE_PROCESS_ROLE: `${approach}_orchestrator`,
      RESULT_TOPIC: `${topicPrefix}/results`,
      DATA_PATH: `custom_patterns/${patternName}`,
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
      BENCHMARK_APPROACH: approach,
      BENCHMARK_SCALE: scenarioConfig.scenarioLabel,
      BENCHMARK_ITERATION: String(iterationNum),
      BENCHMARK_EXPERIMENT_NAME: this.options.experimentName,
      BENCHMARK_PATTERN_NAME: patternName,
      BENCHMARK_SUPERQUERY_RANGE_SECONDS: String(
        scenarioConfig.metadata.superquery_range_seconds,
      ),
      BENCHMARK_SUPERQUERY_STEP_SECONDS: String(
        scenarioConfig.metadata.superquery_step_seconds,
      ),
      BENCHMARK_CHUNK_SIZE_SECONDS: String(
        scenarioConfig.metadata.chunk_size_seconds,
      ),
      BENCHMARK_REPLAY_DURATION_SECONDS: String(
        scenarioConfig.metadata.replay_duration_seconds,
      ),
    });

    const replayMetadata = getReplayMetadata(testEnv);
    const runMetadataPath = path.join(logDir, "run_metadata.json");

    let orchestratorProc = null;
    let publisherProc = null;
    let publisherTimer = null;
    let timeoutTimer = null;
    let finalized = false;
    let resolveFn = null;
    let rejectFn = null;
    let orchestratorExitCode = null;
    let orchestratorExitSignal = null;
    let publisherExitCode = null;
    let publisherExitSignal = null;
    let intentionalShutdown = false;
    let orchestratorLogFds = null;
    let publisherLogFds = null;
    let cleanupSigtermSent = false;
    let cleanupSigkillSent = false;
    let forcedCleanupRequired = false;
    let staleProcessesFoundAfterCleanup = false;

    const resourceMonitor = new AttemptResourceSampler(logDir, {
      sampleIntervalMs: 500,
      getRootPids: () => {
        const pids = [];
        if (orchestratorProc && Number.isFinite(orchestratorProc.pid)) {
          pids.push(orchestratorProc.pid);
        }
        if (publisherProc && Number.isFinite(publisherProc.pid)) {
          pids.push(publisherProc.pid);
        }
        return pids;
      },
    });

    const finalize = async (reason, err = null) => {
      if (finalized) {
        return;
      }
      finalized = true;
      intentionalShutdown = true;

      clearTimeout(publisherTimer);
      clearTimeout(timeoutTimer);

      let isSuccess = true;
      let failureReason = "";

      if (publisherExitCode !== null && publisherExitCode !== 0) {
        isSuccess = false;
        failureReason = `Publisher exited with non-zero code ${publisherExitCode}`;
      } else if (publisherExitSignal !== null) {
        isSuccess = false;
        failureReason = `Publisher exited with signal ${publisherExitSignal}`;
      } else if (
        orchestratorExitSignal === "SIGKILL" ||
        orchestratorExitSignal === "SIGABRT"
      ) {
        isSuccess = false;
        failureReason = `Orchestrator exited with signal ${orchestratorExitSignal}`;
      } else if (orchestratorExitCode !== null && orchestratorExitCode !== 0) {
        isSuccess = false;
        failureReason = `Orchestrator exited with non-zero code ${orchestratorExitCode}`;
      } else if (reason === "timeout") {
        isSuccess = false;
        failureReason = "Benchmark run timed out";
      } else if ((reason === "process_error" || reason === "publisher_error") && err) {
        isSuccess = false;
        failureReason = `Process error: ${err.message}`;
      } else if (reason === "orchestrator_unexpected_exit") {
        isSuccess = false;
        failureReason =
          "Orchestrator exited unexpectedly before publisher completion";
      }

      if (publisherProc) {
        const termination = await terminateProcessTree(publisherProc, "publisher", 2000);
        cleanupSigtermSent = cleanupSigtermSent || termination.sigtermSent;
        cleanupSigkillSent = cleanupSigkillSent || termination.sigkillSent;
        forcedCleanupRequired = forcedCleanupRequired || termination.sigkillRequired;
      }
      if (orchestratorProc) {
        const termination = await terminateProcessTree(
          orchestratorProc,
          "orchestrator",
          2000,
        );
        cleanupSigtermSent = cleanupSigtermSent || termination.sigtermSent;
        cleanupSigkillSent = cleanupSigkillSent || termination.sigkillSent;
        forcedCleanupRequired = forcedCleanupRequired || termination.sigkillRequired;
      }

      closeLogFds(orchestratorLogFds);
      closeLogFds(publisherLogFds);
      orchestratorLogFds = null;
      publisherLogFds = null;

      if (forcedCleanupRequired) {
        isSuccess = false;
        failureReason = failureReason
          ? `${failureReason}; forced SIGKILL cleanup was required`
          : "forced SIGKILL cleanup was required";
      }

      const staleBefore = checkStaleProcesses();
      if (staleBefore.foundStale) {
        staleProcessesFoundAfterCleanup = true;
        const pkillPatterns = [
          "StreamingQueryFetchingClientSideApproachOrchestrator",
          "StreamingQueryChunkedApproachOrchestrator",
          "dist/services/BeeWorker.js",
          "dist/streamer/src/publish",
        ];
        for (const pattern of pkillPatterns) {
          try {
            const selfExcluding = getSelfExcludingPattern(pattern);
            execSync(`pkill -f "${selfExcluding}" || true`, { stdio: "ignore" });
          } catch {
            // ignore pkill failures
          }
        }
        await sleep(1000);
        const staleAfter = checkStaleProcesses();
        if (staleAfter.foundStale) {
          isSuccess = false;
          const staleReason = `Stale processes remain after cleanup: ${JSON.stringify(
            staleAfter.foundProcesses,
          )}`;
          failureReason = failureReason ? `${failureReason}; ${staleReason}` : staleReason;
        }
      }

      let totalEmittedResults = 0;
      let requiredFiles = [];
      if (isSuccess) {
        const fileCheck = verifyExpectedOutputFiles(approach, logDir);
        if (!fileCheck.valid) {
          isSuccess = false;
          failureReason = fileCheck.reason;
        } else {
          totalEmittedResults = fileCheck.emittedCount;
          requiredFiles = fileCheck.requiredFiles;
        }
      }

      await sleep(500);
      const profileAggregate = buildProfileAggregate(logDir, runMetadata);
      const processCleanupOk = !staleProcessesFoundAfterCleanup && !forcedCleanupRequired;

      const resourceSummaryMetadata = {
        ...runMetadata,
        replayMetadata,
        terminationReason: reason,
        exitStatus: isSuccess ? "completed" : "error",
        error: isSuccess ? null : failureReason,
        orchestrator_exit_code: orchestratorExitCode,
        orchestrator_exit_signal: orchestratorExitSignal,
        publisher_exit_code: publisherExitCode,
        publisher_exit_signal: publisherExitSignal,
        cleanup_sigterm_sent: cleanupSigtermSent,
        cleanup_sigkill_sent: cleanupSigkillSent,
        cleanup_forced_sigkill_required: forcedCleanupRequired,
        stale_processes_found_after_cleanup: staleProcessesFoundAfterCleanup,
        process_cleanup_ok: processCleanupOk,
        run_status: isSuccess ? "SUCCESS" : "FAILED",
        failure_reason: failureReason || null,
        emitted_result_count: totalEmittedResults,
        required_result_files: requiredFiles,
        profile_aggregate_path: profileAggregate?.aggregatePath || null,
      };

      await resourceMonitor.stop(resourceSummaryMetadata);

      fs.writeFileSync(
        runMetadataPath,
        `${JSON.stringify(
          {
            ...runMetadata,
            replayMetadata,
            success: isSuccess,
            validity_reason: isSuccess ? "valid" : failureReason || "unknown_failure",
            emitted_result_count: totalEmittedResults,
            required_result_files: requiredFiles,
            run_status: isSuccess ? "SUCCESS" : "FAILED",
            process_cleanup_ok: processCleanupOk,
            cleanup_sigterm_sent: cleanupSigtermSent,
            cleanup_sigkill_sent: cleanupSigkillSent,
            cleanup_forced_sigkill_required: forcedCleanupRequired,
            stale_processes_found_after_cleanup: staleProcessesFoundAfterCleanup,
            orchestrator_exit_code: orchestratorExitCode,
            orchestrator_exit_signal: orchestratorExitSignal,
            publisher_exit_code: publisherExitCode,
            publisher_exit_signal: publisherExitSignal,
            profile_aggregate_path: profileAggregate?.aggregatePath || null,
          },
          null,
          2,
        )}\n`,
      );

      this.activeAttemptCleanup = null;

      if (isSuccess) {
        resolveFn?.();
      } else {
        rejectFn?.(new Error(failureReason || "Benchmark run failed"));
      }
    };

    this.activeAttemptCleanup = () => finalize("interrupted");

    return new Promise((resolve, reject) => {
      resolveFn = resolve;
      rejectFn = reject;

      const approachScript = getApproachScript(approach);
      if (!fs.existsSync(approachScript)) {
        reject(new Error(`Approach script not found: ${approachScript}`));
        return;
      }

      const orchestratorLogPath = path.join(logDir, `${approach}_orchestrator.log`);
      orchestratorLogFds = openLogFds(orchestratorLogPath, orchestratorLogPath);
      orchestratorProc = spawn("node", [approachScript], {
        env: testEnv,
        stdio: ["ignore", orchestratorLogFds.stdoutFd, orchestratorLogFds.stderrFd],
        detached: true,
      });

      resourceMonitor.start();

      orchestratorProc.on("error", (processError) => {
        void finalize("process_error", processError);
      });
      orchestratorProc.on("close", (code, signal) => {
        orchestratorExitCode = code;
        orchestratorExitSignal = signal;
        if (!intentionalShutdown && !finalized) {
          void finalize("orchestrator_unexpected_exit");
        }
      });

      publisherTimer = setTimeout(() => {
        if (finalized) {
          return;
        }

        const publisherLogPath = path.join(logDir, "publisher.log");
        publisherLogFds = openLogFds(publisherLogPath, publisherLogPath);
        publisherProc = spawn("node", ["dist/streamer/src/publish.js"], {
          env: {
            ...testEnv,
            HIVE_PROCESS_ROLE: "benchmark_publisher",
          },
          stdio: ["ignore", publisherLogFds.stdoutFd, publisherLogFds.stderrFd],
          detached: true,
        });

        publisherProc.on("error", (processError) => {
          void finalize("publisher_error", processError);
        });
        publisherProc.on("close", (code, signal) => {
          publisherExitCode = code;
          publisherExitSignal = signal;

          if (!finalized) {
            if (code !== 0) {
              void finalize("publisher_failed");
            } else {
              setTimeout(() => {
                void finalize("completed");
              }, 5000);
            }
          }
        });
      }, 5000);

      timeoutTimer = setTimeout(() => {
        if (!finalized) {
          void finalize("timeout");
        }
      }, this.options.timeoutMs);
    });
  }

  async run() {
    console.log(
      `Starting window-parameter benchmark: experiment=${this.options.experimentName}`,
    );
    console.log(`Log root: ${this.baseLogDir}`);

    if (!this.options.skipBuild) {
      console.log("Compiling TypeScript sources...");
      execFileSync("npm", ["run", "build"], { stdio: "inherit" });
    }

    const scenarios = resolveScenarioList(this.options);
    for (let iterationNum = 1; iterationNum <= this.options.iterations; iterationNum += 1) {
      console.log(
        `\n============================================================\nITERATION ${iterationNum} of ${this.options.iterations}\n============================================================`,
      );

      for (const approach of this.options.approaches) {
        for (const scenarioSeconds of scenarios) {
          for (const patternName of this.options.patterns) {
            try {
              await this.runSingleTest({
                approach,
                patternName,
                scenarioSeconds,
                iterationNum,
              });
              await sleep(2000);
            } catch (error) {
              console.error(
                `Failed run: experiment=${this.options.experimentName} approach=${approach} pattern=${patternName} scenario=${scenarioSeconds}s iteration=${iterationNum}`,
                error,
              );
            }
          }
        }
      }
    }
  }
}

function runExtractor(options) {
  const extractorPath = path.resolve(
    process.cwd(),
    "experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js",
  );
  const args = [
    extractorPath,
    "--experiment",
    options.experimentName,
    "--input-root",
    path.join(options.logRoot, options.experimentName),
    "--output-dir",
    path.join(options.resultRoot, options.experimentName),
  ];

  if (options.patterns.length > 0) {
    args.push("--patterns", options.patterns.join(","));
  }
  if (options.approaches.length > 0) {
    args.push("--approaches", options.approaches.join(","));
  }
  if (options.experimentName === "superquery-range-scaling" && options.ranges.length > 0) {
    args.push("--ranges", options.ranges.join(","));
  }
  if (
    options.experimentName === "chunk-granularity-sensitivity" &&
    options.chunkSizes.length > 0
  ) {
    args.push("--chunk-sizes", options.chunkSizes.join(","));
  }

  execFileSync("node", args, { stdio: "inherit" });
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  const runner = new WindowParameterSensitivityRunner(options);
  await runner.run();

  if (options.extractAfterRun) {
    runExtractor(options);
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error("Fatal benchmark runner error:", error);
    process.exit(1);
  });
}

module.exports = {
  AttemptResourceSampler,
  WindowParameterSensitivityRunner,
  buildProfileAggregate,
  checkStaleProcesses,
  collectTreeStatsFromPs,
  getApproachScript,
  getRequiredResultFiles,
  parseArgs,
  verifyExpectedOutputFiles,
};
