#!/usr/bin/env node

/**
 * K-Scaling Experiment Runner
 *
 * Runs K-scaling benchmarks for Fetching and Chunked approaches.
 * Holds query shape fixed (AVG, RANGE 120s STEP 60s, low_variability pattern).
 * Scales K = 1, 2, 4, 8, 16.
 *
 * Usage:
 *   node run-k-scaling-comparison.js
 *   node run-k-scaling-comparison.js --iterations 3 --timeout 300000
 */

const { execFileSync, spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const {
  cleanupStaleBenchmarkProcesses,
  delay,
  terminateChildProcessTree,
} = require("../utils/processCleanup");

const DEFAULT_K_VALUES = [1, 2, 4, 8];
const OPTIONAL_STRESS_K = 16;
const DEFAULT_APPROACHES = ["fetching", "chunked"];
const DEFAULT_PATTERNS = ["low_variability"];

// Parse command line arguments
const args = process.argv.slice(2);
let iterations = 3;
let timeout = 300000;

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--iterations" || args[i] === "-i") {
    iterations = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === "--timeout" || args[i] === "-t") {
    timeout = parseInt(args[i + 1], 10);
    i++;
  }
}

// Read options from environment or use defaults
const kValuesEnv = process.env.K_SCALING_VALUES;
const kValues = kValuesEnv
  ? kValuesEnv.split(",").map((k) => parseInt(k.trim(), 10))
  : [...DEFAULT_K_VALUES];

const approachesEnv = process.env.K_SCALING_SELECTED_APPROACHES;
const approaches = approachesEnv
  ? approachesEnv.split(",").map((a) => a.trim().toLowerCase())
  : DEFAULT_APPROACHES;

const patternsEnv = process.env.K_SCALING_SELECTED_PATTERNS;
const patterns = patternsEnv
  ? patternsEnv.split(",").map((p) => p.trim())
  : DEFAULT_PATTERNS;

const aggregationFunc = (process.env.AGGREGATION_FUNCTION || "AVG").toUpperCase();
const immediateTrigger = process.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER !== "false";

console.log("K-Scaling Experiment Configuration:");
console.log(`  Approaches: ${approaches.join(", ")}`);
console.log(`  K Values: ${kValues.join(", ")}`);
console.log(`  Patterns: ${patterns.join(", ")}`);
console.log(`  Iterations: ${iterations}`);
console.log(`  Timeout: ${timeout}ms`);
console.log(`  Aggregation: ${aggregationFunc}`);
console.log(`  Immediate Trigger: ${immediateTrigger}`);

function parsePsSnapshot(psOutput) {
  const rows = [];
  const lines = psOutput
    .trim()
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    const parts = line.split(/\s+/, 5);
    if (parts.length < 4) {
      continue;
    }

    const pid = parseInt(parts[0], 10);
    const ppid = parseInt(parts[1], 10);
    const cpu = parseFloat(parts[2]);
    const rssKb = parseFloat(parts[3]);
    const command = parts[4] || "";

    if (!Number.isFinite(pid) || !Number.isFinite(ppid)) {
      continue;
    }

    rows.push({
      pid,
      ppid,
      cpu: Number.isFinite(cpu) ? cpu : 0,
      rssKb: Number.isFinite(rssKb) ? rssKb : 0,
      command,
    });
  }

  return rows;
}

function collectTreeStatsFromPs(rootPids) {
  const liveRootPids = [...new Set((rootPids || []).filter((pid) => Number.isFinite(pid)))];
  if (liveRootPids.length === 0) {
    return null;
  }

  let snapshot;
  try {
    snapshot = parsePsSnapshot(
      execFileSync("ps", ["-A", "-o", "pid=,ppid=,pcpu=,rss=,comm="], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
        maxBuffer: 1024 * 1024 * 8,
      }),
    );
  } catch (err) {
    return null;
  }

  if (snapshot.length === 0) {
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

      this.samples.push({
        timestamp: Date.now(),
        elapsedMs: this.startedAt ? Date.now() - this.startedAt : 0,
        ...stats,
      });
      for (const proc of stats.tree || []) {
        const bucket = this.pidSamples.get(proc.pid) || [];
        bucket.push({
          timestamp: Date.now(),
          elapsedMs: this.startedAt ? Date.now() - this.startedAt : 0,
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
    const meanCpuPct = sampleCount > 0
      ? this.samples.reduce((sum, sample) => sum + sample.totalCpuPct, 0) / sampleCount
      : 0;
    const peakCpuPct = sampleCount > 0
      ? Math.max(...this.samples.map((sample) => sample.totalCpuPct))
      : 0;
    const meanRssMb = sampleCount > 0
      ? this.samples.reduce((sum, sample) => sum + sample.totalRssMb, 0) / sampleCount
      : 0;
    const peakRssMb = sampleCount > 0
      ? Math.max(...this.samples.map((sample) => sample.totalRssMb))
      : 0;
    const peakProcessCount = sampleCount > 0
      ? Math.max(...this.samples.map((sample) => sample.processCount))
      : 0;
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
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }

    const csvPath = path.join(this.logDir, "resource_usage.csv");
    const summaryPath = path.join(this.logDir, "resource_summary.json");
    const perPidPath = path.join(this.logDir, "resource_per_pid_summary.json");
    const lines = [
      "timestamp,elapsed_ms,root_pid_count,process_count,total_cpu_pct,mean_cpu_pct,peak_cpu_pct,total_rss_mb,mean_rss_mb,peak_rss_mb",
      ...this.samples.map((sample) =>
        [
          sample.timestamp,
          sample.elapsedMs,
          sample.rootPids.length,
          sample.processCount,
          sample.totalCpuPct.toFixed(4),
          sample.meanCpuPct.toFixed(4),
          sample.peakCpuPct.toFixed(4),
          sample.totalRssMb.toFixed(3),
          sample.meanRssMb.toFixed(3),
          sample.peakRssMb.toFixed(3),
        ].join(",")
      ),
    ];

    fs.writeFileSync(csvPath, `${lines.join("\n")}\n`);
    const perPidSummary = Array.from(this.pidSamples.entries())
      .map(([pid, samples]) => {
        const sampleCount = samples.length;
        const meanCpuPct = sampleCount > 0
          ? samples.reduce((sum, sample) => sum + sample.cpuPct, 0) / sampleCount
          : 0;
        const peakCpuPct = sampleCount > 0
          ? Math.max(...samples.map((sample) => sample.cpuPct))
          : 0;
        const meanRssMb = sampleCount > 0
          ? samples.reduce((sum, sample) => sum + sample.rssMb, 0) / sampleCount
          : 0;
        const peakRssMb = sampleCount > 0
          ? Math.max(...samples.map((sample) => sample.rssMb))
          : 0;
        const wallTimeSec = sampleCount > 1
          ? (samples[samples.length - 1].elapsedMs - samples[0].elapsedMs) / 1000
          : 0;
        const cpuSeconds = (meanCpuPct / 100) * wallTimeSec;
        return {
          pid,
          ppid: samples[0]?.ppid ?? null,
          command: samples[0]?.command || "",
          sampleCount,
          wallTimeSec,
          cpuSeconds,
          meanCpuPct,
          peakCpuPct,
          meanRssMb,
          peakRssMb,
        };
      })
      .sort((a, b) => b.cpuSeconds - a.cpuSeconds);
    fs.writeFileSync(perPidPath, JSON.stringify(perPidSummary, null, 2));
    fs.writeFileSync(
      summaryPath,
      JSON.stringify(
        {
          ...metadata,
          ...this.summarize(),
          csvPath: path.resolve(csvPath),
          summaryPath: path.resolve(summaryPath),
          perPidSummaryPath: path.resolve(perPidPath),
        },
        null,
        2,
      ),
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

class KScalingBenchmarkRunner {
  constructor() {
    this.baseLogDir = path.resolve(__dirname, "../../logs/k-scaling");
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);
    this.activeAttemptCleanup = null;
    this.shutdownInProgress = false;
    this.installSignalHandlers();
  }

  getIterationRootDir(approach, K, patternName, iterationNum) {
    return path.join(this.baseLogDir, approach, `K${K}`, patternName, `iteration${iterationNum}`);
  }

  getApproachScript(approach) {
    const scripts = {
      fetching: "dist/approaches/StreamingQueryFetchingKScalingOrchestrator.js",
      chunked: "dist/approaches/StreamingQueryChunkedKScalingOrchestrator.js",
    };
    return scripts[approach];
  }

  installSignalHandlers() {
    const handleSignal = (signal, exitCode) => {
      if (this.shutdownInProgress) return;
      this.shutdownInProgress = true;
      console.log(`\nReceived ${signal}; cleaning up benchmark processes...`);
      if (this.activeAttemptCleanup) {
        this.activeAttemptCleanup();
      }
      cleanupStaleBenchmarkProcesses({ logger: console.log });
      process.exit(exitCode);
    };

    process.on("SIGINT", () => handleSignal("SIGINT", 130));
    process.on("SIGTERM", () => handleSignal("SIGTERM", 143));
  }

  async runSingleTest(approach, K, patternName, iterationNum) {
    const logDir = this.getIterationRootDir(approach, K, patternName, iterationNum);
    fs.mkdirSync(logDir, { recursive: true });

    console.log(`\n------------------------------------------------------------`);
    console.log(`Running: Approach=${approach.toUpperCase()} K=${K} Pattern=${patternName} Iteration=${iterationNum}`);
    console.log(`Log Directory: ${logDir}`);
    console.log(`------------------------------------------------------------`);

    await cleanupStaleBenchmarkProcesses({ logger: () => {} });

    // Setup base environment variables
    const timestampDomainMin = 1749592200000;
    const timestampDomainMax = 1749592800000;
    const benchmarkEventTimeAnchor = 1749592200000;
    const sessionToken = `kscaling_${approach}_K${K}_${patternName}_iter${iterationNum}_${Date.now().toString(36)}`;

    const testEnv = this.replayEnv.withBenchmarkReplayEnv({
      ...process.env,
      SESSION_ID: sessionToken,
      AGGREGATION_FUNCTION: aggregationFunc,
      AGGREGATION_FUNC: aggregationFunc,
      STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: String(immediateTrigger),
      STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(timestampDomainMin),
      STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(timestampDomainMax),
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(benchmarkEventTimeAnchor),
      STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: String(benchmarkEventTimeAnchor),
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "true",
      K_SCALING_K: String(K),
      RESULT_TOPIC: `benchmark/results/${approach}/K${K}/iteration-${iterationNum}`,
      LOG_PATH: logDir,
      LOG_DISABLE_FILE_OUTPUT: "0",
      HIVE_PROFILE: "1",
    });

    let orchestratorProc = null;
    let publisherProc = null;
    let publisherTimer = null;
    let timeoutTimer = null;
    let finalized = false;

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
      if (finalized) return;
      finalized = true;

      console.log(`Finalizing test run (Reason: ${reason})...`);
      clearTimeout(publisherTimer);
      clearTimeout(timeoutTimer);

      const summary = resourceMonitor.summarize();
      await resourceMonitor.stop({
        approach,
        K,
        pattern: patternName,
        iteration: iterationNum,
        terminationReason: reason,
        exitStatus: err ? "error" : "completed",
        error: err ? err.message : null,
      });

      // Kill processes
      if (publisherProc) {
        console.log("Terminating publisher...");
        await terminateChildProcessTree(publisherProc.pid, { logger: () => {} });
      }
      if (orchestratorProc) {
        console.log("Terminating orchestrator...");
        await terminateChildProcessTree(orchestratorProc.pid, { logger: () => {} });
      }

      await cleanupStaleBenchmarkProcesses({ logger: () => {} });
      this.activeAttemptCleanup = null;
    };

    this.activeAttemptCleanup = () => {
      void finalize("interrupted");
    };

    return new Promise(async (resolve, reject) => {
      const approachScript = this.getApproachScript(approach);
      if (!fs.existsSync(approachScript)) {
        reject(new Error(`Approach script not found: ${approachScript}`));
        return;
      }

      // Start orchestrator process
      console.log(`Spawning orchestrator: node ${approachScript}`);
      orchestratorProc = spawn("node", [approachScript], {
        env: {
          ...testEnv,
          HIVE_PROCESS_ROLE: `${approach}_orchestrator`,
        },
        stdio: "pipe",
        detached: true,
      });

      resourceMonitor.start();

      // Write orchestrator logs
      const orchestratorLogPath = path.join(logDir, `${approach}_orchestrator.log`);
      const orchestratorLogStream = fs.createWriteStream(orchestratorLogPath);
      orchestratorProc.stdout.on("data", (chunk) => orchestratorLogStream.write(chunk));
      orchestratorProc.stderr.on("data", (chunk) => orchestratorLogStream.write(chunk));

      orchestratorProc.on("error", (err) => {
        console.error(`Orchestrator error: ${err.message}`);
        void finalize("process_error", err).then(resolve);
      });

      orchestratorProc.on("close", (code, signal) => {
        console.log(`Orchestrator exited: code=${code} signal=${signal}`);
        if (!finalized) {
          void finalize("orchestrator_exited").then(resolve);
        }
      });

      // Spawn publisher after 5s startup delay
      publisherTimer = setTimeout(() => {
        if (finalized) return;

        console.log("Spawning data publisher...");
        const dataPath = `custom_patterns/${patternName}`;
        publisherProc = spawn("node", ["dist/streamer/src/publish.js"], {
          env: {
            ...testEnv,
            DATA_PATH: dataPath,
            HIVE_PROCESS_ROLE: "benchmark_publisher",
          },
          stdio: "pipe",
          detached: true,
        });

        const publisherLogPath = path.join(logDir, "publisher.log");
        const publisherLogStream = fs.createWriteStream(publisherLogPath);
        publisherProc.stdout.on("data", (chunk) => publisherLogStream.write(chunk));
        publisherProc.stderr.on("data", (chunk) => publisherLogStream.write(chunk));

        publisherProc.on("error", (err) => {
          console.error(`Publisher error: ${err.message}`);
          void finalize("publisher_error", err).then(resolve);
        });

        publisherProc.on("close", (code) => {
          console.log(`Publisher completed: code=${code}`);
          // Give 3 seconds for trailing messages to recompose/propagate
          setTimeout(() => {
            void finalize("completed").then(resolve);
          }, 3000);
        });
      }, 5000);

      // Setup timeout safety
      timeoutTimer = setTimeout(() => {
        if (!finalized) {
          console.log(`Test timed out after ${timeout}ms.`);
          void finalize("timeout").then(resolve);
        }
      }, timeout);
    });
  }

  async run() {
    console.log("Starting K-scaling Benchmark Runner...");
    console.log(`Log Base Directory: ${this.baseLogDir}`);

    // Build the project first to ensure latest changes are included
    try {
      console.log("Compiling TypeScript sources...");
      execFileSync("npm", ["run", "build"], { stdio: "inherit" });
      console.log("TypeScript compilation successful.");
    } catch (err) {
      console.error("Failed to build TypeScript sources. Cannot run benchmark.");
      process.exit(1);
    }

    for (let iter = 1; iter <= iterations; iter++) {
      console.log(`\n============================================================`);
      console.log(`ITERATION ${iter} of ${iterations}`);
      console.log(`============================================================`);

      for (const approach of approaches) {
        for (const K of kValues) {
          // Smoke validation for K=16 if enabled
          if (K === OPTIONAL_STRESS_K && iter > 1) {
            // Only run K=16 once (during iteration 1) as a stress point
            console.log(`Skipping K=16 for iteration ${iter} (stress point runs in iteration 1 only)`);
            continue;
          }

          for (const patternName of patterns) {
            try {
              await this.runSingleTest(approach, K, patternName, iter);
              await delay(2000); // Wait between tests
            } catch (err) {
              console.error(`Failed run: Approach=${approach} K=${K} Pattern=${patternName} Iteration=${iter}`, err);
            }
          }
        }
      }
    }

    console.log("\nK-scaling Benchmark Runs Complete!");
  }
}

if (require.main === module) {
  const runner = new KScalingBenchmarkRunner();
  runner.run().catch((err) => {
    console.error("Fatal benchmark runner error:", err);
    process.exit(1);
  });
}
