#!/usr/bin/env node

/**
 * Custom Pattern Analysis - All Approaches
 *
 * Tests all three approaches (Fetching, Approximation, Chunked) across 5 custom patterns:
 * 1. Low Variability (μ=-23.0, σ=0.25)
 * 2. Step Pattern (v1=-23.0, v2=-15.0, t_step=60s)
 * 3. Spike Pattern (v_base=-23.0, v_spike=-5.0, Δt=1.25s)
 * 4. Low Freq. Oscillation (μ=-23.0, A=5.0, f=0.05Hz)
 * 5. High Freq. Oscillation (μ=-23.0, A=3.0, f=0.5Hz)
 *
 * Measures:
 * - Accuracy (MAPE, MAE, RMSE)
 * - First-event latency
 * - Resource usage (CPU, memory)
 *
 * Usage:
 *   node run-custom-patterns-comparison.js                    # Run all patterns (default 35 iterations)
 *   node run-custom-patterns-comparison.js --iterations 10    # Run with 10 iterations
 *   node run-custom-patterns-comparison.js --retries 1        # Retry failed cases once
 *   node run-custom-patterns-comparison.js -i 35              # Short flag
 *   node run-custom-patterns-comparison.js low_variability    # Run specific pattern
 */

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const { finalizeMqttTrafficArtifacts } = require("../../dist/util/mqttTraffic");
const {
  cleanupStaleBenchmarkProcesses,
  delay,
  terminateChildProcessTree,
} = require("../utils/processCleanup");

const SMOKE_PATTERN_TYPE = "low_variability";
const SMOKE_APPROACHES = ["fetching", "approximation"];
const ALL_PATTERN_TYPES = [
  "low_variability",
  "step_pattern",
  "spike_pattern",
  "low_freq_oscillation",
  "high_freq_oscillation",
];
const ALL_APPROACHES = [
  "fetching",
  "naive_distributed",
  "approximation",
  "chunked",
];

function extractFirstDatasetTimestampMs(filePath) {
  const content = fs.readFileSync(filePath, "utf8");
  const matches = content.matchAll(
    /<https:\/\/saref\.etsi\.org\/core\/hasTimestamp>\s+"([^"]+)"/g,
  );

  let firstTimestamp = null;
  for (const match of matches) {
    const epoch = Date.parse(match[1]);
    if (!Number.isFinite(epoch)) {
      continue;
    }
    if (firstTimestamp === null || epoch < firstTimestamp) {
      firstTimestamp = epoch;
    }
  }

  return firstTimestamp;
}

function extractDatasetTimestampBoundsMs(filePath) {
  const content = fs.readFileSync(filePath, "utf8");
  const matches = content.matchAll(
    /<https:\/\/saref\.etsi\.org\/core\/hasTimestamp>\s+"([^"]+)"/g,
  );

  let minTimestamp = null;
  let maxTimestamp = null;
  for (const match of matches) {
    const epoch = Date.parse(match[1]);
    if (!Number.isFinite(epoch)) {
      continue;
    }
    if (minTimestamp === null || epoch < minTimestamp) {
      minTimestamp = epoch;
    }
    if (maxTimestamp === null || epoch > maxTimestamp) {
      maxTimestamp = epoch;
    }
  }

  return {
    minTimestamp,
    maxTimestamp,
    durationMs:
      minTimestamp !== null && maxTimestamp !== null
        ? maxTimestamp - minTimestamp
        : null,
  };
}

function parseSelectionList(value, allowedValues) {
  if (!value) {
    return null;
  }

  const entries = value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);

  if (entries.length === 0) {
    return null;
  }

  const filtered = entries.filter((entry) => allowedValues.includes(entry));
  return [...new Set(filtered)];
}

function inferTerminationReason(result) {
  if (result?.terminationReason) {
    return result.terminationReason;
  }

  if (result?.extractionStatus && !["success", "skipped"].includes(result.extractionStatus)) {
    return "extraction_failed";
  }

  if (result?.reachedDurationLimit) {
    return "duration_limit_reached";
  }

  if (result?.benchmarkStatus === "interrupted") {
    return "manual_interrupt";
  }

  if (result?.benchmarkStatus === "completed") {
    return "process_exit";
  }

  if (result?.timedOut) {
    return "startup_timeout";
  }

  if (result?.benchmarkStatus === "failed" || result?.error) {
    return "process_error";
  }

  return "process_exit";
}

class CustomPatternComparisonRunner {
  constructor(iterations = 35, options = {}) {
    this.iterations = iterations;
    this.smokeMode = Boolean(options.smokeMode ?? process.env.PAPER_BENCHMARK_SMOKE === "1");
    this.retries = resolveRetryCount(options.retries, process.env.CUSTOM_PATTERN_RETRIES);
    this.allApproaches = ALL_APPROACHES;
    this.allPatterns = [
      {
        type: "low_variability",
        name: "Low Variability",
        params: "μ=-23.0, σ=0.25",
      },
      {
        type: "step_pattern",
        name: "Step Pattern",
        params: "v₁=-23.0, v₂=-15.0, t_step=60s",
      },
      {
        type: "spike_pattern",
        name: "Spike Pattern",
        params: "v_base=-23.0, v_spike=-5.0, Δt=1.25s",
      },
      {
        type: "low_freq_oscillation",
        name: "Low Freq. Oscillation",
        params: "μ=-23.0, A=5.0, f=0.05Hz",
      },
      {
        type: "high_freq_oscillation",
        name: "High Freq. Oscillation",
        params: "μ=-23.0, A=3.0, f=0.5Hz",
      },
    ];
    this.selectedPatternTypes = parseSelectionList(
      process.env.CUSTOM_PATTERN_SELECTED_PATTERNS,
      ALL_PATTERN_TYPES,
    );
    this.selectedApproaches = parseSelectionList(
      process.env.CUSTOM_PATTERN_SELECTED_APPROACHES,
      ALL_APPROACHES,
    );
    this.activeAttemptCleanup = null;
    this.shutdownInProgress = false;

    const baseApproaches = this.smokeMode ? SMOKE_APPROACHES : this.allApproaches;
    const smokePattern = this.allPatterns.find((pattern) => pattern.type === SMOKE_PATTERN_TYPE);
    const basePatterns = this.smokeMode
      ? (smokePattern ? [smokePattern] : [])
      : this.allPatterns;

    this.patterns = this.selectedPatternTypes
      ? basePatterns.filter((pattern) => this.selectedPatternTypes.includes(pattern.type))
      : basePatterns;
    this.approaches = this.selectedApproaches
      ? baseApproaches.filter((approach) => this.selectedApproaches.includes(approach))
      : baseApproaches;
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);

    this.baseLogDir = "./logs/custom-pattern-comparison";
    this.timeout = resolvePatternTestTimeoutMs(options.patternTestTimeoutMs, this.smokeMode);
    this.installSignalHandlers();
  }

  getIterationRootDir(approach, patternName, iterationNum) {
    return path.join(
      this.baseLogDir,
      approach,
      patternName,
      `iteration${iterationNum}`,
    );
  }

  getAttemptLogDir(approach, patternName, iterationNum, attemptNumber) {
    const iterationRootDir = this.getIterationRootDir(approach, patternName, iterationNum);
    if (this.retries <= 0) {
      return iterationRootDir;
    }
    return path.join(iterationRootDir, `attempt${attemptNumber}`);
  }

  getDataPath(pattern) {
    return `custom_patterns/${pattern.type}`;
  }

  getPatternName(pattern) {
    return pattern.type;
  }

  getApproachScript(approach) {
    const scripts = {
      fetching:
        "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
      approximation:
        "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
      chunked: "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
      naive_distributed: "dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js",
    };
    return scripts[approach];
  }

  cleanupStaleProcesses() {
    return cleanupStaleBenchmarkProcesses({ logger: (message) => console.log(message) });
  }

  installSignalHandlers() {
    if (CustomPatternComparisonRunner.signalHandlersInstalled) {
      return;
    }

    CustomPatternComparisonRunner.signalHandlersInstalled = true;

    const handleSignal = (signal, exitCode) => {
      void (async () => {
        if (this.shutdownInProgress) {
          return;
        }
        this.shutdownInProgress = true;
        console.log(`Received ${signal}; cleaning up benchmark processes...`);
        await this.cleanupActiveAttempt();
        await this.cleanupStaleProcesses();
        process.exit(exitCode);
      })();
    };

    process.on("SIGINT", () => handleSignal("SIGINT", 130));
    process.on("SIGTERM", () => handleSignal("SIGTERM", 143));
  }

  async cleanupActiveAttempt() {
    const cleanup = this.activeAttemptCleanup;
    if (!cleanup) {
      return;
    }

    this.activeAttemptCleanup = null;
    await cleanup();
  }

  cleanupAttemptArtifacts(logDir, iterationRootDir) {
    if (fs.existsSync(logDir)) {
      fs.rmSync(logDir, { recursive: true, force: true });
    }

    const rootArtifacts = [
      "streaming_query_chunk_aggregator_log.csv",
      "chunked_latency_log.csv",
      "chunked_parent_partial_latency_log.csv",
      "chunked_window_diagnostics.csv",
      "chunked_results.csv",
      "chunked_metadata.json",
      "fetching_client_side_log.csv",
      "fetching_latency_log.csv",
      "fetching_window_diagnostics.csv",
      "fetching_resource_usage.csv",
      "fetching_results.csv",
      "fetching_metadata.json",
      "approximation_approach_log.csv",
      "approximation_latency_log.csv",
      "approximation_approach_resource_usage.csv",
      "approximation_results.csv",
      "approximation_metadata.json",
      "naive_distributed_approach_log.csv",
      "naive_distributed_latency_log.csv",
      "naive_distributed_approach_resource_usage.csv",
      "naive_distributed_results.csv",
      "naive_distributed_metadata.json",
      "streaming_query_hive_resource_log.csv",
      "replayer-log.csv",
    ];

    for (const fileName of rootArtifacts) {
      const filePath = path.join(iterationRootDir, fileName);
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }
    }
  }

  cleanupApproximationScratchFiles() {
    const scratchFiles = [
      "approximation_approach_log.csv",
      "approximation_latency_log.csv",
      "approximation_approach_resource_usage.csv",
    ];

    for (const fileName of scratchFiles) {
      if (!fs.existsSync(fileName)) {
        continue;
      }

      fs.rmSync(fileName, { force: true });
      console.log(`[cleanup] approximation-scratch removed=${fileName}`);
    }
  }

  async runSingleTest(approach, pattern, iterationNum = 1, attemptNumber = 1) {
    const lifecycleTs = () => new Date().toISOString();
    const lifecycleLog = (event, details = {}) => {
      const parts = Object.entries(details)
        .filter(([, value]) => value !== undefined && value !== null && value !== "")
        .map(([key, value]) => `${key}=${value}`);
      const suffix = parts.length > 0 ? ` ${parts.join(" ")}` : "";
      console.log(`[lifecycle ${lifecycleTs()}] ${event}${suffix}`);
    };

    lifecycleLog("runSingleTest.entered", {
      approach,
      pattern: pattern?.type,
      iteration: iterationNum,
      attempt: attemptNumber,
    });
    await this.cleanupStaleProcesses();
    const patternName = this.getPatternName(pattern);
    const dataPath = this.getDataPath(pattern);

    console.log(`\n${"=".repeat(80)}`);
    console.log(
      `TESTING: ${approach.toUpperCase()} - ${pattern.name} - Iteration ${iterationNum}/${this.iterations}`,
    );
    console.log(`Attempt: ${attemptNumber}/${this.retries + 1}`);
    console.log(`Pattern: ${pattern.params}`);
    console.log(`Data: ${dataPath}`);
    console.log("=".repeat(80));

    const iterationRootDir = this.getIterationRootDir(approach, patternName, iterationNum);
    const logDir = this.getAttemptLogDir(approach, patternName, iterationNum, attemptNumber);
    const expectedResultPaths = {
      extractionSummary: path.join(logDir, `${approach}_results.csv`),
      attemptMetadata: path.join(logDir, "attempt_metadata.json"),
      publisherLog: path.join(logDir, "publisher.log"),
      orchestratorLog: path.join(logDir, `${approach}_orchestrator.log`),
    };
    lifecycleLog("runSingleTest.paths", {
      attemptDir: logDir,
      iterationRootDir,
      expectedResultPaths: JSON.stringify(expectedResultPaths),
      });
      this.cleanupAttemptArtifacts(logDir, iterationRootDir);
      if (approach === "approximation") {
        this.cleanupApproximationScratchFiles();
      }
      if (!fs.existsSync(iterationRootDir)) {
        fs.mkdirSync(iterationRootDir, { recursive: true });
      }
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }

    // Check if data exists
    const smartphoneDataPath = path.join(
      "src/streamer/data",
      dataPath,
      "smartphone.acceleration.x",
      "data.nt",
    );
    if (!fs.existsSync(smartphoneDataPath)) {
      console.log(`⚠️  Data not found: ${smartphoneDataPath}`);
      console.log(
        `\n💡 Generate data first: node scripts/generate-custom-patterns.js\n`,
      );
      return { success: false, error: "Data not found" };
    }

    const benchmarkEventTimeAnchor =
      extractFirstDatasetTimestampMs(smartphoneDataPath);
    if (!Number.isFinite(benchmarkEventTimeAnchor)) {
      return {
        success: false,
        error: `Unable to derive first dataset timestamp from ${smartphoneDataPath}`,
      };
    }

    const attemptId = `${approach}-${patternName}-iter${iterationNum}-attempt${attemptNumber}`;
    const topicPrefix = `bench/${attemptId}`;
    const timestampBounds = extractDatasetTimestampBoundsMs(smartphoneDataPath);
    const timestampDomainMin = benchmarkEventTimeAnchor;
    const timestampDomainMax =
      benchmarkEventTimeAnchor +
      this.timeout +
      120000 +
      Math.max(0, timestampBounds.durationMs || 0);

    const attemptMetadata = {
      attempt_id: attemptId,
      topic_prefix: topicPrefix,
      benchmark_event_time_anchor: benchmarkEventTimeAnchor,
      timestamp_domain_min: timestampDomainMin,
      timestamp_domain_max: timestampDomainMax,
      output_window_range: process.env.OUTPUT_WINDOW_RANGE || "120000",
      output_window_step: process.env.OUTPUT_WINDOW_STEP || "60000",
      sub_window_range: process.env.SUB_WINDOW_RANGE || "60000",
      sub_window_step: process.env.SUB_WINDOW_STEP || "30000",
      data_path: dataPath,
      approach,
      pattern: patternName,
      iteration: iterationNum,
      attempt: attemptNumber,
    };
    fs.writeFileSync(
      path.join(logDir, "attempt_metadata.json"),
      JSON.stringify(attemptMetadata, null, 2),
    );
    lifecycleLog("attempt.metadata.written", {
      attemptDir: logDir,
      attemptMetadataPath: path.join(logDir, "attempt_metadata.json"),
    });

    const env = this.replayEnv.withBenchmarkReplayEnv({
      ...process.env,
      DATA_PATH: dataPath,
      LOG_PATH: logDir,
      BENCHMARK_SCENARIO: "custom-pattern-comparison",
      BENCHMARK_SCALE: patternName,
      BENCHMARK_APPROACH: approach,
      BENCHMARK_ITERATION: String(iterationNum),
      STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
      STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: String(
        benchmarkEventTimeAnchor,
      ),
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(
        benchmarkEventTimeAnchor,
      ),
      STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(timestampDomainMin),
      STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(timestampDomainMax),
    });

    console.log(
      `Attempt start: id=${attemptId} topicPrefix=${topicPrefix} benchmarkAnchor=${benchmarkEventTimeAnchor} timestampRewriteMode=deterministic expectedTimestampDomain=[${timestampDomainMin},${timestampDomainMax}]`,
    );

    return new Promise((resolve) => {
      const startTime = Date.now();
      let reachedDurationLimit = false;
      let finalized = false;
      let cleanupPromise = null;
      let timeoutId = null;
      let publisherStartTimer = null;
      let approachProc = null;
      let publisherProc = null;
      let publisherActivitySeen = false;
      let childActivitySeen = false;
      let lastPublisherActivityAt = null;
      let lastChildActivityAt = null;
      let currentWaitState = "initializing";

      const cleanupAttempt = () => {
        lifecycleLog("cleanup.start", {
          attemptId,
          waitState: currentWaitState,
          lastPublisherActivityAt,
          lastChildActivityAt,
        });
        if (!cleanupPromise) {
          cleanupPromise = (async () => {
            if (timeoutId) {
              clearTimeout(timeoutId);
              timeoutId = null;
            }
            if (publisherStartTimer) {
              clearTimeout(publisherStartTimer);
              publisherStartTimer = null;
            }

            await terminateChildProcessTree(publisherProc, {
              name: `${attemptId} publisher`,
              logger: (message) => console.log(message),
            });
            await terminateChildProcessTree(approachProc, {
              name: `${attemptId} ${approach}`,
              logger: (message) => console.log(message),
            });
            await delay(500);
            lifecycleLog("cleanup.finish", {
              attemptId,
              waitState: currentWaitState,
              lastPublisherActivityAt,
              lastChildActivityAt,
            });
          })();
        }

        return cleanupPromise;
      };

      const finalizeAttempt = async (result) => {
        if (finalized) {
          return;
        }
        finalized = true;
        lifecycleLog("runSingleTest.finalize.entered", {
          attemptId,
          benchmarkStatus: result?.benchmarkStatus,
          extractionStatus: result?.extractionStatus,
          finalStatus: result?.finalStatus,
          waitState: currentWaitState,
        });
        this.activeAttemptCleanup = null;
        await cleanupAttempt();
        this.moveLogFiles(approach, logDir);
        const mqttTrafficSummary = finalizeMqttTrafficArtifacts({ logDir });
        lifecycleLog("runSingleTest.finalize.resolved", {
          attemptId,
          benchmarkStatus: result?.benchmarkStatus,
          extractionStatus: result?.extractionStatus,
          finalStatus: result?.finalStatus,
          waitState: currentWaitState,
        });
        resolve({
          ...result,
          mqttTrafficSummary,
        });
      };

      this.activeAttemptCleanup = cleanupAttempt;

      // Start approach
      console.log(`Starting ${approach} approach...`);
      lifecycleLog("child.spawn.requested", {
        attemptId,
        command: `node ${this.getApproachScript(approach)}`,
      });
      approachProc = spawn("node", [this.getApproachScript(approach)], {
        env,
        stdio: "pipe",
        detached: true,
      });
      lifecycleLog("child.spawned", {
        attemptId,
        pid: approachProc.pid,
        command: `node ${this.getApproachScript(approach)}`,
      });

      // Capture logs
      const approachLogPath = path.join(logDir, `${approach}_orchestrator.log`);
      const approachLogStream = fs.createWriteStream(approachLogPath);
      approachProc.stdout.on("data", (chunk) => {
        const ts = lifecycleTs();
        childActivitySeen = true;
        lastChildActivityAt = ts;
        if (!approachProc.__lifecycleStdoutSeen) {
          approachProc.__lifecycleStdoutSeen = true;
          lifecycleLog("child.stdout.firstLine", {
            attemptId,
            pid: approachProc.pid,
            timestamp: ts,
          });
        }
        approachLogStream.write(chunk);
      });
      approachProc.stderr.on("data", (chunk) => {
        const ts = lifecycleTs();
        childActivitySeen = true;
        lastChildActivityAt = ts;
        if (!approachProc.__lifecycleStderrSeen) {
          approachProc.__lifecycleStderrSeen = true;
          lifecycleLog("child.stderr.firstLine", {
            attemptId,
            pid: approachProc.pid,
            timestamp: ts,
          });
        }
        approachLogStream.write(chunk);
      });

      // Start publisher after delay
      approachProc.on("error", (err) => {
        console.log(`✗ ${approach} process error: ${err.message}`);
        void finalizeAttempt({
            approach,
            pattern: patternName,
            patternDisplayName: pattern.name,
            patternParams: pattern.params,
            iteration: iterationNum,
            attempt: attemptNumber,
            benchmarkStatus: "failed",
            terminationReason: "process_error",
            timedOut: false,
            reachedDurationLimit,
            exitCode: null,
            duration: Date.now() - startTime,
            durationMs: Date.now() - startTime,
            configuredTimeoutMs: this.timeout,
            logDir,
            iterationRootDir,
            benchmarkEventTimeAnchor,
            topicPrefix,
            error: err.message,
          });
      });
      approachProc.on("close", (code, signal) => {
        lifecycleLog("child.close", {
          attemptId,
          pid: approachProc.pid,
          code,
          signal,
          durationMs: Date.now() - startTime,
          lastChildActivityAt,
        });
      });

      publisherStartTimer = setTimeout(() => {
        if (finalized) {
          return;
        }
        console.log("Starting data publisher...");
        lifecycleLog("publisher.start.requested", {
          attemptId,
          waitState: currentWaitState,
        });
        currentWaitState = "waiting_for_publisher_close";
        publisherProc = spawn("node", ["dist/streamer/src/publish.js"], {
          env,
          stdio: "pipe",
          detached: true,
        });
        lifecycleLog("publisher.spawned", {
          attemptId,
          pid: publisherProc.pid,
          command: "node dist/streamer/src/publish.js",
        });

        const publisherLogPath = path.join(logDir, "publisher.log");
        const publisherLogStream = fs.createWriteStream(publisherLogPath);
        publisherProc.stdout.on("data", (chunk) => {
          const ts = lifecycleTs();
          publisherActivitySeen = true;
          lastPublisherActivityAt = ts;
          if (!publisherProc.__lifecycleStdoutSeen) {
            publisherProc.__lifecycleStdoutSeen = true;
            lifecycleLog("publisher.stdout.firstLine", {
              attemptId,
              pid: publisherProc.pid,
              timestamp: ts,
            });
          }
          publisherLogStream.write(chunk);
        });
        publisherProc.stderr.on("data", (chunk) => {
          const ts = lifecycleTs();
          publisherActivitySeen = true;
          lastPublisherActivityAt = ts;
          if (!publisherProc.__lifecycleStderrSeen) {
            publisherProc.__lifecycleStderrSeen = true;
            lifecycleLog("publisher.stderr.firstLine", {
              attemptId,
              pid: publisherProc.pid,
              timestamp: ts,
            });
          }
          publisherLogStream.write(chunk);
        });

        timeoutId = setTimeout(() => {
          lifecycleLog("timeout.watchdog.fired", {
            attemptId,
            timeoutMs: this.timeout,
            timestamp: lifecycleTs(),
            lastPublisherActivityAt,
            lastChildActivityAt,
            waitState: currentWaitState,
          });
          console.log("⏰ Benchmark duration limit reached");
          reachedDurationLimit = true;
          void cleanupAttempt();
        }, this.timeout);
        lifecycleLog("timeout.watchdog.armed", {
          attemptId,
          timeoutMs: this.timeout,
          armedAt: lifecycleTs(),
          waitState: currentWaitState,
        });

        publisherProc.on("error", (err) => {
          console.log(`✗ Publisher error: ${err.message}`);
          void finalizeAttempt({
            approach,
            pattern: patternName,
            patternDisplayName: pattern.name,
            patternParams: pattern.params,
            iteration: iterationNum,
            attempt: attemptNumber,
            benchmarkStatus: "failed",
            terminationReason: "process_error",
            timedOut: false,
            reachedDurationLimit,
            exitCode: null,
            duration: Date.now() - startTime,
            durationMs: Date.now() - startTime,
            configuredTimeoutMs: this.timeout,
            logDir,
            iterationRootDir,
            benchmarkEventTimeAnchor,
            topicPrefix,
            error: err.message,
          });
        });

        publisherProc.on("close", (code) => {
          void (async () => {
            lifecycleLog("publisher.close", {
              attemptId,
              pid: publisherProc.pid,
              code,
              signal: publisherProc.signalCode || null,
              durationMs: Date.now() - startTime,
              lastPublisherActivityAt,
            });
            currentWaitState = "waiting_for_extraction";
            const durationMs = Date.now() - startTime;
            const benchmarkStatus =
              reachedDurationLimit || code === 0 ? "completed" : "failed";
            const terminationReason = reachedDurationLimit
              ? "duration_limit_reached"
              : (code === 0 ? "process_exit" : "process_error");

            const result = {
              approach,
              pattern: patternName,
              patternDisplayName: pattern.name,
              patternParams: pattern.params,
              iteration: iterationNum,
              attempt: attemptNumber,
              benchmarkStatus,
              terminationReason,
              timedOut: false,
              reachedDurationLimit,
              exitCode: code,
              duration: durationMs,
              durationMs,
              configuredTimeoutMs: this.timeout,
              logDir,
              iterationRootDir,
              benchmarkEventTimeAnchor,
              topicPrefix,
            };

            await delay(2000);
            console.log(`✓ Test completed in ${(durationMs / 1000).toFixed(1)}s`);
            await finalizeAttempt(result);
          })();
        });
      }, 2000);
      lifecycleLog("runSingleTest.waiting", {
        attemptId,
        waitState: "waiting_for_publisher_spawn",
        attemptDir: logDir,
        expectedResultPaths: JSON.stringify(expectedResultPaths),
      });
    });
  }

  moveLogFiles(approach, logDir) {
    const logFileMap = {
      fetching: [
        "fetching_client_side_log.csv",
        "fetching_latency_log.csv",
        "fetching_window_diagnostics.csv",
        "fetching_resource_usage.csv",
        "replayer-log.csv",
      ],
      approximation: [
        "approximation_approach_log.csv",
        "approximation_latency_log.csv",
        "approximation_approach_resource_usage.csv",
        "replayer-log.csv",
      ],
      chunked: [
        "streaming_query_chunk_aggregator_log.csv",
        "chunked_latency_log.csv",
        "chunked_parent_partial_latency_log.csv",
        "chunked_window_diagnostics.csv",
        "streaming_query_hive_resource_log.csv",
        "replayer-log.csv",
      ],
      naive_distributed: [
        "naive_distributed_approach_log.csv",
        "naive_distributed_latency_log.csv",
        "naive_distributed_approach_resource_usage.csv",
        "replayer-log.csv",
      ],
    };

    const logFiles = logFileMap[approach] || [];

    logFiles.forEach((logFile) => {
      const srcPath = path.join(".", logFile);
      const destPath = path.join(logDir, logFile);

      if (fs.existsSync(srcPath)) {
        try {
          fs.copyFileSync(srcPath, destPath);
          fs.unlinkSync(srcPath);
          console.log(`  Moved ${logFile}`);
        } catch (err) {
          console.log(`  Failed to move ${logFile}: ${err.message}`);
        }
      }
    });
  }

  async extractResults(approach, pattern, iterationNum = 1, attemptNumber = 1, explicitLogDir = null) {
    const patternName = this.getPatternName(pattern);
    console.log(
      `\n📊 Extracting results for ${approach} - ${pattern.name} - Iteration ${iterationNum} - Attempt ${attemptNumber}...`,
    );
    console.log(
      `[lifecycle ${new Date().toISOString()}] extraction.start approach=${approach} pattern=${patternName} iteration=${iterationNum} attempt=${attemptNumber} logDir=${explicitLogDir || this.getAttemptLogDir(approach, patternName, iterationNum, attemptNumber)}`,
    );

    if (approach === "naive_distributed") {
      console.log("  Skipping extraction for naive_distributed; it is not part of the custom pattern accuracy comparison.");
      return { status: "skipped" };
    }

    return new Promise((resolve) => {
      const logDir = explicitLogDir || this.getAttemptLogDir(approach, patternName, iterationNum, attemptNumber);

      // Call extraction script with custom parameters
      const proc = spawn(
        "node",
        [
          "experiments/pattern-analysis/extract-pattern-results.js",
          approach,
          patternName,
          logDir,
        ],
        { stdio: "inherit" },
      );

      proc.on("close", (code) => {
        if (code === 0) {
          console.log(`✓ Extraction completed`);
          console.log(
            `[lifecycle ${new Date().toISOString()}] extraction.finish approach=${approach} pattern=${patternName} iteration=${iterationNum} attempt=${attemptNumber} status=success code=${code}`,
          );
          resolve({ status: "success" });
        } else {
          console.log(`⚠️  Extraction had issues (code ${code})`);
          console.log(
            `[lifecycle ${new Date().toISOString()}] extraction.finish approach=${approach} pattern=${patternName} iteration=${iterationNum} attempt=${attemptNumber} status=failed code=${code}`,
          );
          resolve({ status: "failed" });
        }
      });

      proc.on("error", (err) => {
        console.log(`✗ Extraction error: ${err.message}`);
        console.log(
          `[lifecycle ${new Date().toISOString()}] extraction.finish approach=${approach} pattern=${patternName} iteration=${iterationNum} attempt=${attemptNumber} status=failed error=${err.message}`,
        );
        resolve({ status: "failed" });
      });
    });
  }

  finalizeTestResult(baseResult, extractionResult) {
    const extractionStatus = extractionResult?.status || "failed";
    const success = extractionStatus === "success" || extractionStatus === "skipped";
    const terminationReason = success
      ? (baseResult?.terminationReason || "process_exit")
      : "extraction_failed";

    return {
      ...baseResult,
      extractionStatus,
      finalExtractionStatus: extractionStatus,
      finalStatus: success ? "success" : "failed",
      terminationReason,
      success,
    };
  }

  buildAttemptFailureReason(result) {
    if (result?.success) {
      return null;
    }
    if (result?.error) {
      return result.error;
    }
    if (result?.extractionStatus && !["success", "skipped"].includes(result.extractionStatus)) {
      return `Extraction ${result.extractionStatus}`;
    }
    if (result?.benchmarkStatus && result.benchmarkStatus !== "completed") {
      return `Benchmark ${result.benchmarkStatus}`;
    }
    if (result?.terminationReason && result?.terminationReason !== "duration_limit_reached") {
      return `Termination ${result.terminationReason}`;
    }
    return null;
  }

  normalizeAttemptResult(result, attemptNumber) {
    const extractionStatus = result?.extractionStatus || "failed";
    const success = Boolean(result?.success);
    const benchmarkStatus = result?.benchmarkStatus || (success ? "completed" : "failed");
    const durationMs = Number.isFinite(result?.durationMs) ? result.durationMs : (result?.duration ?? null);
    const configuredTimeoutMs = Number.isFinite(result?.configuredTimeoutMs)
      ? result.configuredTimeoutMs
      : null;
    const reachedDurationLimit = Boolean(result?.reachedDurationLimit);
    const terminationReason = result?.terminationReason
      || inferTerminationReason({ benchmarkStatus, extractionStatus, success, reachedDurationLimit });

    return {
      approach: result?.approach || null,
      pattern: result?.pattern || null,
      patternDisplayName: result?.patternDisplayName || null,
      patternParams: result?.patternParams || null,
      iteration: result?.iteration || null,
      attempt: attemptNumber,
      success,
      benchmarkStatus,
      terminationReason,
      extractionStatus,
      finalStatus: result?.finalStatus || (success ? "success" : "failed"),
      timedOut: Boolean(result?.timedOut),
      reachedDurationLimit,
      exitCode: result?.exitCode ?? null,
      duration: durationMs,
      durationMs,
      configuredTimeoutMs,
      logDir: result?.logDir || null,
      error: result?.error || null,
      failureReason: this.buildAttemptFailureReason({
        ...result,
        extractionStatus,
        benchmarkStatus,
        terminationReason,
        success,
      }),
    };
  }

  buildCaseResult(pattern, approach, iterationNum, attemptResults) {
    const finalAttempt = attemptResults[attemptResults.length - 1] || {};
    const firstFailedAttempt = attemptResults.find((attempt) => !attempt.success) || null;

    return {
      approach,
      pattern: pattern.type,
      patternDisplayName: pattern.name,
      patternParams: pattern.params,
      iteration: iterationNum,
      success: Boolean(finalAttempt.success),
      finalStatus: finalAttempt.success ? "success" : "failed",
      benchmarkStatus: finalAttempt.benchmarkStatus || (finalAttempt.success ? "completed" : "failed"),
      terminationReason: finalAttempt.terminationReason
        || inferTerminationReason(finalAttempt),
      extractionStatus: finalAttempt.extractionStatus || "failed",
      finalExtractionStatus: finalAttempt.extractionStatus || "failed",
      timedOut: Boolean(finalAttempt.timedOut),
      reachedDurationLimit: Boolean(finalAttempt.reachedDurationLimit),
      exitCode: finalAttempt.exitCode ?? null,
      duration: finalAttempt.duration ?? null,
      durationMs: finalAttempt.durationMs ?? finalAttempt.duration ?? null,
      configuredTimeoutMs: finalAttempt.configuredTimeoutMs ?? this.timeout,
      logDir: finalAttempt.logDir || null,
      finalLogDir: finalAttempt.logDir || null,
      finalAttemptNumber: finalAttempt.attempt || attemptResults.length,
      attemptCount: attemptResults.length,
      retriesConfigured: this.retries,
      retryUsed: attemptResults.length > 1,
      firstFailureReason: firstFailedAttempt?.failureReason || null,
      error: finalAttempt.error || null,
      attempts: attemptResults,
    };
  }

  async runCaseWithRetries(approach, pattern, iterationNum) {
    const attemptResults = [];
    const maxAttempts = this.retries + 1;

    for (let attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber += 1) {
      try {
        const benchmarkResult = await this.runSingleTest(approach, pattern, iterationNum, attemptNumber);
        const extractionResult = await this.extractResults(
          approach,
          pattern,
          iterationNum,
          attemptNumber,
          benchmarkResult.logDir,
        );
        const finalizedResult = this.finalizeTestResult(benchmarkResult, extractionResult);
        attemptResults.push(this.normalizeAttemptResult(finalizedResult, attemptNumber));

        if (finalizedResult.success) {
          break;
        }
      } catch (error) {
        attemptResults.push(this.normalizeAttemptResult({
          approach,
          pattern: pattern.type,
          patternDisplayName: pattern.name,
          patternParams: pattern.params,
          iteration: iterationNum,
          success: false,
          benchmarkStatus: "failed",
          terminationReason: "process_error",
          extractionStatus: "failed",
          error: error.message,
          logDir: this.getAttemptLogDir(approach, pattern.type, iterationNum, attemptNumber),
          configuredTimeoutMs: this.timeout,
        }, attemptNumber));
      }

      if (attemptNumber < maxAttempts) {
        console.log(
          `↻ Retrying ${approach} - ${pattern.name} - iteration ${iterationNum} (attempt ${attemptNumber + 1}/${maxAttempts})`,
        );
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }

    return this.buildCaseResult(pattern, approach, iterationNum, attemptResults);
  }

  async runAllPatterns() {
    console.log(`\n${"█".repeat(80)}`);
    console.log("CUSTOM PATTERN COMPARISON - ALL APPROACHES");
    console.log("█".repeat(80));
    console.log(`Total patterns: ${this.patterns.length}`);
    console.log(`Approaches: ${this.approaches.join(", ")}`);
    console.log(`Iterations per pattern-approach: ${this.iterations}`);
    console.log(
      `Total tests: ${this.patterns.length * this.approaches.length * this.iterations}`,
    );
    console.log("█".repeat(80));

    const results = [];

    for (const pattern of this.patterns) {
      console.log(`\n${"─".repeat(80)}`);
      console.log(`Pattern: ${pattern.name}`);
      console.log(`Parameters: ${pattern.params}`);
      console.log("─".repeat(80));

      for (const approach of this.approaches) {
        console.log(`\n  Approach: ${approach.toUpperCase()}`);

        for (let iter = 1; iter <= this.iterations; iter++) {
          const caseResult = await this.runCaseWithRetries(approach, pattern, iter);
          results.push(caseResult);

          // Wait between iterations
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }

        // Wait between approaches
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }

      // Wait between patterns
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    return results;
  }

  async runSpecificPattern(patternType) {
    const pattern = this.patterns.find((p) => p.type === patternType);

    if (!pattern) {
      throw new Error(
        `Unknown pattern type: ${patternType}. Available: ${this.patterns.map((p) => p.type).join(", ")}`,
      );
    }

    console.log("\n" + "█".repeat(80));
    console.log(`SINGLE PATTERN TEST: ${pattern.name}`);
    console.log(`Parameters: ${pattern.params}`);
    console.log("█".repeat(80));

    const results = [];

    for (const approach of this.approaches) {
      for (let iter = 1; iter <= this.iterations; iter++) {
        const caseResult = await this.runCaseWithRetries(approach, pattern, iter);
        results.push(caseResult);
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }

    return results;
  }

  generateSummary(results) {
    console.log("\n" + "█".repeat(80));
    console.log("FINAL SUMMARY");
    console.log("█".repeat(80));

    const successful = results.filter((r) => r.success);
    const failed = results.filter((r) => !r.success);
    const retried = results.filter((r) => r.retryUsed);
    const totalAttempts = results.reduce((sum, result) => sum + (result.attemptCount || 0), 0);

    console.log(`\nTotal tests: ${results.length}`);
    console.log(`Successful: ${successful.length}`);
    console.log(`Failed: ${failed.length}`);
    console.log(`Iterations per test: ${this.iterations}`);
    console.log(`Retries configured: ${this.retries}`);
    console.log(`Retried cases: ${retried.length}`);
    console.log(`Total attempts: ${totalAttempts}`);

    // Group by approach
    const byApproach = {};
    this.approaches.forEach((approach) => {
      byApproach[approach] = {
        total: results.filter((r) => r.approach === approach).length,
        success: results.filter((r) => r.approach === approach && r.success)
          .length,
      };
    });

    console.log("\n" + "─".repeat(80));
    console.log("Results by Approach:");
    Object.entries(byApproach).forEach(([approach, stats]) => {
      console.log(`  ${approach}: ${stats.success}/${stats.total} successful`);
    });

    // Group by pattern
    const byPattern = {};
    this.patterns.forEach((pattern) => {
      const patternResults = results.filter((r) => r.pattern === pattern.type);
      byPattern[pattern.name] = {
        total: patternResults.length,
        success: patternResults.filter((r) => r.success).length,
      };
    });

    console.log("\n" + "─".repeat(80));
    console.log("Results by Pattern:");
    Object.entries(byPattern).forEach(([name, stats]) => {
      console.log(`  ${name}: ${stats.success}/${stats.total} successful`);
    });

    if (failed.length > 0) {
      console.log("\n" + "─".repeat(80));
      console.log("Failed Tests:");
      failed.forEach((r) => {
        console.log(
          `  ✗ ${r.approach} - ${r.patternDisplayName || r.pattern} - iteration ${r.iteration || "?"}`,
        );
        if (r.firstFailureReason) console.log(`    First failure: ${r.firstFailureReason}`);
        if (r.error) console.log(`    Final error: ${r.error}`);
      });
    }

    if (retried.length > 0) {
      console.log("\n" + "─".repeat(80));
      console.log("Retried Cases:");
      retried.forEach((r) => {
        console.log(
          `  ↻ ${r.approach} - ${r.patternDisplayName || r.pattern} - iteration ${r.iteration}: ${r.attemptCount} attempt(s), final=${r.finalStatus}`,
        );
      });
    }

    // Save summary
    const summaryPath = path.join(
      this.baseLogDir,
      "custom_pattern_comparison_summary.json",
    );
    const summary = {
      timestamp: new Date().toISOString(),
      totalTests: results.length,
      iterations: this.iterations,
      retries: this.retries,
      smokeMode: this.smokeMode,
      selectedPatterns: this.patterns.map((pattern) => pattern.type),
      selectedApproaches: this.approaches,
      successful: successful.length,
      failed: failed.length,
      totalAttempts,
      retryCountConfigured: this.retries,
      retryUsed: retried.length > 0,
      retriedCases: retried.map((result) => ({
        approach: result.approach,
        pattern: result.pattern,
        iteration: result.iteration,
        attemptCount: result.attemptCount,
        finalStatus: result.finalStatus,
        finalLogDir: result.finalLogDir,
      })),
      failedAfterRetries: failed.map((result) => ({
        approach: result.approach,
        pattern: result.pattern,
        iteration: result.iteration,
        attemptCount: result.attemptCount,
        finalStatus: result.finalStatus,
        benchmarkStatus: result.benchmarkStatus,
        terminationReason: result.terminationReason,
        firstFailureReason: result.firstFailureReason,
        finalLogDir: result.finalLogDir,
        finalExtractionStatus: result.finalExtractionStatus,
      })),
      byApproach: byApproach,
      byPattern: byPattern,
      results: results.map((result) => ({
        ...result,
        extractionStatus: result.extractionStatus || "failed",
        finalExtractionStatus: result.finalExtractionStatus || result.extractionStatus || "failed",
        benchmarkStatus: result.benchmarkStatus || (result.success ? "completed" : "failed"),
        finalStatus: result.finalStatus || (result.success ? "success" : "failed"),
        terminationReason: result.terminationReason
          || inferTerminationReason(result),
        durationMs: result.durationMs ?? result.duration ?? null,
        configuredTimeoutMs: result.configuredTimeoutMs ?? this.timeout,
        reachedDurationLimit: Boolean(result.reachedDurationLimit),
      })),
    };

    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
    console.log(`\n📄 Summary saved to: ${summaryPath}`);
    console.log("█".repeat(80));
  }

  async runAnalysis() {
    console.log("\n" + "=".repeat(80));
    console.log("RUNNING COMPREHENSIVE ANALYSIS");
    console.log("=".repeat(80));

    // Note: You'll need to create a custom analysis script for these patterns
    // For now, just print instructions
    console.log("\n⚠️  Custom analysis script needed");
    console.log(
      "Create: analysis/accuracy/custom-pattern-accuracy-comparison.js",
    );
    console.log("This script should:");
    console.log("  1. Read all iteration data for each pattern-approach");
    console.log("  2. Compute mean ± std for MAPE, MAE, RMSE");
    console.log("  3. Compute mean ± std for latency and memory");
    console.log("  4. Generate aggregated CSV and JSON outputs");
    console.log("\nFor now, results are in individual iteration directories.");
    console.log("=".repeat(80));

    return true;
  }
}

function resolvePatternTestTimeoutMs(explicitTimeoutMs, smokeMode = false) {
  const defaultTimeoutMs = 240000;

  if (smokeMode && !Number.isFinite(explicitTimeoutMs)) {
    return 120000;
  }

  if (Number.isFinite(explicitTimeoutMs)) {
    return explicitTimeoutMs;
  }

  const envTimeoutMs = process.env.CUSTOM_PATTERN_TEST_TIMEOUT_MS;
  if (envTimeoutMs !== undefined) {
    const parsedTimeoutMs = Number.parseInt(envTimeoutMs, 10);
    if (Number.isFinite(parsedTimeoutMs)) {
      return parsedTimeoutMs;
    }
  }

  return defaultTimeoutMs;
}

function resolveRetryCount(explicitRetries, envRetries) {
  if (Number.isFinite(explicitRetries)) {
    return Math.max(0, explicitRetries);
  }

  if (envRetries !== undefined) {
    const parsedRetries = Number.parseInt(envRetries, 10);
    if (Number.isFinite(parsedRetries)) {
      return Math.max(0, parsedRetries);
    }
  }

  return 0;
}

// Main execution
async function main() {
  const args = process.argv.slice(2);

  // Check for iterations flag
  let iterations = 35; // default
  let patternTestTimeoutMs;
  let retries = 0;
  let filteredArgs = args;

  const iterFlag = args.findIndex(
    (arg) => arg === "--iterations" || arg === "-i",
  );
  if (iterFlag !== -1 && args[iterFlag + 1]) {
    iterations = parseInt(args[iterFlag + 1], 10);
    filteredArgs = args.filter(
      (_, idx) => idx !== iterFlag && idx !== iterFlag + 1,
    );
  }

  const timeoutFlag = filteredArgs.findIndex(
    (arg) => arg === "--pattern-test-timeout" || arg === "--test-timeout",
  );
  if (timeoutFlag !== -1 && filteredArgs[timeoutFlag + 1]) {
    patternTestTimeoutMs = Number.parseInt(filteredArgs[timeoutFlag + 1], 10);
    filteredArgs = filteredArgs.filter(
      (_, idx) => idx !== timeoutFlag && idx !== timeoutFlag + 1,
    );
  }

  const retriesFlag = filteredArgs.findIndex((arg) => arg === "--retries");
  if (retriesFlag !== -1 && filteredArgs[retriesFlag + 1]) {
    retries = Number.parseInt(filteredArgs[retriesFlag + 1], 10);
    filteredArgs = filteredArgs.filter(
      (_, idx) => idx !== retriesFlag && idx !== retriesFlag + 1,
    );
  }

  const runner = new CustomPatternComparisonRunner(iterations, {
    patternTestTimeoutMs,
    retries,
    smokeMode: process.env.PAPER_BENCHMARK_SMOKE === "1",
  });

  console.log(`\n${"=".repeat(80)}`);
  console.log(`Configuration: Running ${iterations} iteration(s) per test`);
  console.log(`Pattern test timeout: ${runner.timeout} ms`);
  console.log(`Retries per failed case: ${runner.retries}`);
  console.log(`Smoke mode: ${runner.smokeMode ? "enabled" : "disabled"}`);
  console.log(`Total patterns: ${runner.patterns.length} custom pattern(s)`);
  console.log(`Approaches: ${runner.approaches.join(", ")}`);
  console.log(`Selected patterns: ${runner.patterns.map((pattern) => pattern.type).join(", ") || "none"}`);
  console.log(`Selected approaches: ${runner.approaches.join(", ") || "none"}`);
  console.log(
    `Expected total tests: ${runner.patterns.length * runner.approaches.length * iterations} (${runner.patterns.length} patterns × ${runner.approaches.length} approaches × ${iterations} iterations)`,
  );
  const estimatedRuntimeMs = runner.patterns.length * runner.approaches.length * iterations * runner.timeout;
  const estimatedRuntimeMinutes = estimatedRuntimeMs / 60000;
  console.log(
    `Expected runtime: ~${estimatedRuntimeMinutes >= 60 ? `${(estimatedRuntimeMinutes / 60).toFixed(1)} hours` : `${Math.ceil(estimatedRuntimeMinutes)} minutes`} (${estimatedRuntimeMs} ms total)`,
  );
  console.log("=".repeat(80));

  try {
    if (filteredArgs.length === 0) {
      // Run all patterns
      const results = await runner.runAllPatterns();
      runner.generateSummary(results);
      await runner.runAnalysis();
    } else if (filteredArgs.length === 1) {
      // Run specific pattern
      const patternType = filteredArgs[0];
      const results = await runner.runSpecificPattern(patternType);
      runner.generateSummary(results);
    } else {
      console.log("Usage:");
      console.log(
        "  node run-custom-patterns-comparison.js [--iterations N]           # Run all patterns",
      );
      console.log(
        "  node run-custom-patterns-comparison.js low_variability [-i N]     # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js step_pattern [-i N]        # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js spike_pattern [-i N]       # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js low_freq_oscillation [-i N]  # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js high_freq_oscillation [-i N] # Run specific pattern",
      );
      console.log("\nOptions:");
      console.log(
        "  --iterations, -i N    Number of iterations per test (default: 35)",
      );
      console.log(
        "  --pattern-test-timeout MS  Per-test timeout in milliseconds (default: 240000)",
      );
      console.log(
        "  --retries N    Retry failed cases up to N additional attempts (default: 0)",
      );
      process.exit(1);
    }

    console.log("\n✓ All experiments completed!");
    process.exit(0);
  } catch (error) {
    console.error("\n✗ Experiment failed:", error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = CustomPatternComparisonRunner;
