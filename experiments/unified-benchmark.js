#!/usr/bin/env node

/**
 * UNIFIED BENCHMARK SCRIPT
 *
 * Comprehensive benchmarking for all three approaches across any data source.
 *
 * Metrics:
 * - First-event latency (query registration to first result)
 * - Resource usage (CPU %, Memory)
 * - Accuracy (vs Fetching baseline)
 *
 * Usage:
 *   # Real data (default)
 *   node unified-benchmark.js
 *   node unified-benchmark.js --iterations 1
 *   node unified-benchmark.js --iterations 35
 *
 *   # Pattern-based data
 *   node unified-benchmark.js --pattern exponential_growth_rate_0.1
 *   node unified-benchmark.js --pattern noise_0.5 --iterations 5
 *   node unified-benchmark.js --pattern exponential_decay_rate_100 --iterations 35
 */

const { spawn, execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const { parse } = require("csv-parse/sync");

const PROJECT_ROOT = path.resolve(__dirname, "..");

// Parse command line arguments
const args = process.argv.slice(2);
let iterations = 1;
let pattern = null;
let aggregationFunc = "AVG";
let publishMode = "uniform";
let subWindowRange = "60000";
let subWindowStep = "30000";
let wearableFrequency = "4";

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--iterations" || args[i] === "-i") {
    iterations = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === "--pattern" || args[i] === "-p") {
    pattern = args[i + 1];
    i++;
  } else if (args[i] === "--aggregation" || args[i] === "-a") {
    aggregationFunc = args[i + 1].toUpperCase();
    i++;
  } else if (args[i] === "--publish-mode") {
    publishMode = args[i + 1];
    i++;
  } else if (args[i] === "--sub-window-range") {
    subWindowRange = args[i + 1];
    i++;
  } else if (args[i] === "--sub-window-step") {
    subWindowStep = args[i + 1];
    i++;
  } else if (args[i] === "--wearable-freq") {
    wearableFrequency = args[i + 1];
    i++;
  }
}

// Resolve the DATA_PATH for the publisher based on pattern name
function resolveDataPath(patternName) {
  if (!patternName) return "."; // Real data at root

  const dataBase = path.join(PROJECT_ROOT, "src/streamer/data");
  const candidates = [
    `noisy_datasets/${patternName}`,
    `rate_comparison/${patternName}`,
    `pattern_comparison/${patternName}`,
    patternName, // Allow direct path as well
  ];

  for (const candidate of candidates) {
    const fullPath = path.join(
      dataBase,
      candidate,
      "smartphone.acceleration.x/data.nt",
    );
    if (fs.existsSync(fullPath)) {
      return candidate;
    }
  }

  console.error(`Pattern data not found for: ${patternName}`);
  console.error(
    `Searched in: ${candidates.map((c) => path.join(dataBase, c)).join(", ")}`,
  );
  process.exit(1);
}

const CONFIG = {
  mqttBroker: "mqtt://localhost:1883",
  windowWidth: 120000, // 120 seconds
  windowSlide: 60000, // 60 seconds
  timeout: 300000, // 5 minutes per approach
  startupDelay: 5000, // 5 seconds
  pattern: pattern,
  iterations: iterations,
  aggregationFunc: aggregationFunc,
  publishMode: publishMode,
  subWindowRange: subWindowRange,
  subWindowStep: subWindowStep,
  wearableFrequency: wearableFrequency,
  dataPath: resolveDataPath(pattern),
};

const OUTPUT_TOPICS = {
  fetching: ["client_operation_output"],
  approximation: ["approximation/output"],
  chunked: ["chunked/output", "output"],
};

const ORCHESTRATORS = {
  fetching: path.join(
    PROJECT_ROOT,
    "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
  ),
  approximation: path.join(
    PROJECT_ROOT,
    "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
  ),
  chunked: path.join(
    PROJECT_ROOT,
    "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
  ),
};

class UnifiedBenchmark {
  constructor() {
    const timestamp = Date.now();
    const dataLabel = CONFIG.pattern ? CONFIG.pattern : "real-data";
    const aggLabel =
      CONFIG.aggregationFunc !== "AVG" ? `-${CONFIG.aggregationFunc}` : "";
    const modeLabel =
      CONFIG.publishMode !== "uniform" ? `-${CONFIG.publishMode}` : "";
    const windowLabel =
      CONFIG.subWindowRange !== "60000"
        ? `-${CONFIG.subWindowRange}r${CONFIG.subWindowStep}s`
        : "";
    const freqLabel =
      CONFIG.wearableFrequency !== "4"
        ? `-wfreq${CONFIG.wearableFrequency}`
        : "";
    this.logsBaseDir = path.join(
      __dirname,
      "benchmark-results",
      `${dataLabel}${aggLabel}${modeLabel}${windowLabel}${freqLabel}-${timestamp}`,
    );

    if (!fs.existsSync(this.logsBaseDir)) {
      fs.mkdirSync(this.logsBaseDir, { recursive: true });
    }

    this.allResults = {
      config: CONFIG,
      byIteration: {},
      summary: {},
    };
    this.processes = [];
    this.mqttClient = null;
  }

  log(message) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${message}`);
  }

  async getProcessStats(pid) {
    return new Promise((resolve) => {
      const cmd = `ps -p ${pid} -o %cpu,rss`;
      const { exec } = require("child_process");

      exec(cmd, (error, stdout, stderr) => {
        if (error || stderr) {
          resolve(null);
          return;
        }

        try {
          const lines = stdout.trim().split("\n");
          if (lines.length < 2) {
            resolve(null);
            return;
          }

          const values = lines[lines.length - 1].trim().split(/\s+/);
          if (values.length >= 2) {
            resolve({
              cpu: parseFloat(values[0]),
              memory: parseInt(values[1], 10) * 1024, // RSS in bytes (convert from KB)
            });
          } else {
            resolve(null);
          }
        } catch (e) {
          resolve(null);
        }
      });
    });
  }

  cleanup() {
    for (const proc of this.processes) {
      try {
        proc.kill("SIGTERM");
      } catch (e) {
        // Already dead
      }
    }
    this.processes = [];

    if (this.mqttClient) {
      try {
        this.mqttClient.end(true);
      } catch (e) {
        // Ignore
      }
      this.mqttClient = null;
    }

    // Kill stale processes
    try {
      execSync('pkill -f "StreamingQuery.*Orchestrator" 2>/dev/null || true', {
        stdio: "ignore",
      });
      execSync('pkill -f "node.*publish.js" 2>/dev/null || true', {
        stdio: "ignore",
      });
      execSync('pkill -f "BeeWorker" 2>/dev/null || true', { stdio: "ignore" });
    } catch (e) {
      // Ignore
    }
  }

  async clearMqttState(topics) {
    return new Promise((resolve) => {
      const cleanupClient = mqtt.connect(CONFIG.mqttBroker, {
        clientId: `cleanup-${Date.now()}`,
        clean: true,
      });

      let messagesCleared = 0;
      const chunkedTopicsSeen = new Set();

      cleanupClient.on("connect", () => {
        const allTopics = [
          ...topics,
          "chunked/#",
          "output",
          "wearableX",
          "smartphoneX",
        ];

        for (const topic of allTopics) {
          cleanupClient.subscribe(topic, { qos: 0 });
        }

        cleanupClient.on("message", (topic, message) => {
          messagesCleared++;
          // Track individual chunked/* topics so we can clear their retained messages
          if (topic.startsWith("chunked/")) {
            chunkedTopicsSeen.add(topic);
          }
        });

        // Drain phase: wait until no new messages arrive for 1 second,
        // or up to 5 seconds max. This handles the case where chunked
        // sub-query processes were killed but had already published messages.
        let lastMessageCount = -1;
        let stableChecks = 0;
        const stabilityInterval = setInterval(() => {
          if (messagesCleared === lastMessageCount) {
            stableChecks++;
          } else {
            stableChecks = 0;
            lastMessageCount = messagesCleared;
          }
          // Consider drained when no new messages for 2 checks (1 second)
          if (stableChecks >= 2) {
            clearInterval(stabilityInterval);
            clearTimeout(maxTimeout);
            finishCleanup();
          }
        }, 500);

        const maxTimeout = setTimeout(() => {
          clearInterval(stabilityInterval);
          finishCleanup();
        }, 5000);

        const finishCleanup = () => {
          // Clear retained messages on output topics
          for (const topic of topics) {
            cleanupClient.publish(topic, "", { retain: true });
          }
          // Also clear retained messages on all discovered chunked/* topics
          for (const topic of chunkedTopicsSeen) {
            cleanupClient.publish(topic, "", { retain: true });
          }
          // Clear the generic output topic too
          cleanupClient.publish("output", "", { retain: true });

          this.log(
            `MQTT cleanup: drained ${messagesCleared} messages, cleared ${chunkedTopicsSeen.size} chunked topics`,
          );

          setTimeout(() => {
            cleanupClient.end(true);
            resolve();
          }, 500);
        };
      });

      cleanupClient.on("error", (err) => {
        cleanupClient.end(true);
        resolve();
      });
    });
  }

  extractValueFromMessage(message) {
    const msgStr = message.toString();

    // Try JSON object format
    try {
      const json = JSON.parse(msgStr);
      if (typeof json.value === "number") return json.value;
      if (typeof json.avgValue === "number") return json.avgValue;
      if (typeof json.result === "number") return json.result;
    } catch (e) {
      // Not JSON object
    }

    // Handle JSON-stringified RDF (Fetching approach wraps RDF in JSON.stringify)
    // This produces messages like: "\"<...> hasValue \\\"-10.75\\\"^^... .\""
    let rdfStr = msgStr;
    try {
      const parsed = JSON.parse(msgStr);
      if (typeof parsed === "string") {
        rdfStr = parsed; // Unwrap the JSON string to get raw RDF
      }
    } catch (e) {
      // Not a JSON string, use as-is
    }

    // Match hasValue in RDF: <...hasValue> "-10.75"^^<...>
    const rdfMatch = rdfStr.match(/hasValue[>]?\s+"(-?[0-9.eE+]+)"/);
    if (rdfMatch) return parseFloat(rdfMatch[1]);

    // Match any quoted negative or positive number
    const quotedMatch = rdfStr.match(
      /"(-?[0-9]+\.?[0-9]*(?:[eE][+-]?[0-9]+)?)"/,
    );
    if (quotedMatch) return parseFloat(quotedMatch[1]);

    // Fallback: match a signed decimal number anywhere
    const numMatch = rdfStr.match(/(-?[0-9]+\.[0-9]+)/);
    if (numMatch) return parseFloat(numMatch[1]);

    return null;
  }

  async runSingleApproach(approachName, iterationNum) {
    this.log(`\n${"=".repeat(60)}`);
    this.log(
      `[Iteration ${iterationNum}/${CONFIG.iterations}] ${approachName.toUpperCase()}`,
    );
    this.log(`${"=".repeat(60)}`);

    const topics = OUTPUT_TOPICS[approachName];

    // Phase 1: Subscribe early and drain any stale messages from previous runs.
    // By subscribing BEFORE cleanup, we guarantee that any in-flight messages
    // from a dying orchestrator are captured and discarded here, leaving
    // no window for stale messages to sneak through.
    let staleMessagesDrained = 0;
    let acceptingResults = false;

    this.mqttClient = mqtt.connect(CONFIG.mqttBroker, {
      clientId: `benchmark-${approachName}-${Date.now()}`,
      clean: true,
    });

    await new Promise((res, rej) => {
      this.mqttClient.on("connect", () => {
        this.log(`MQTT subscriber connected (drain phase)`);
        // Subscribe to output topics, raw data topics, and chunked wildcard
        // to drain any stale messages from previous iterations
        const drainTopics = [
          ...topics,
          "chunked/#",
          "wearableX",
          "smartphoneX",
        ];
        for (const topic of drainTopics) {
          this.mqttClient.subscribe(topic, { qos: 0 });
        }
        res();
      });

      this.mqttClient.on("error", (err) => {
        this.log(`MQTT error: ${err}`);
        rej(err);
      });
    });

    // Drain phase: wait until no new stale messages arrive for 1 second, up to 5s max
    await new Promise((res) => {
      let lastCount = -1;
      let stableChecks = 0;

      const drainHandler = (topic, message) => {
        staleMessagesDrained++;
      };
      this.mqttClient.on("message", drainHandler);

      const checkInterval = setInterval(() => {
        if (staleMessagesDrained === lastCount) {
          stableChecks++;
        } else {
          stableChecks = 0;
          lastCount = staleMessagesDrained;
        }
        if (stableChecks >= 2) {
          clearInterval(checkInterval);
          clearTimeout(maxTimeout);
          finish();
        }
      }, 500);

      const maxTimeout = setTimeout(() => {
        clearInterval(checkInterval);
        finish();
      }, 5000);

      const finish = () => {
        this.mqttClient.removeListener("message", drainHandler);
        // Clear retained messages on all relevant topics
        for (const topic of topics) {
          this.mqttClient.publish(topic, "", { retain: true });
        }
        this.mqttClient.publish("output", "", { retain: true });
        this.mqttClient.publish("wearableX", "", { retain: true });
        this.mqttClient.publish("smartphoneX", "", { retain: true });
        // Unsubscribe from drain-only topics so they don't interfere
        // with result collection during the actual run
        this.mqttClient.unsubscribe("chunked/#");
        this.mqttClient.unsubscribe("wearableX");
        this.mqttClient.unsubscribe("smartphoneX");
        // Re-subscribe to output topics with proper QoS for result collection
        for (const topic of topics) {
          this.mqttClient.subscribe(topic, { qos: 1 });
        }
        this.log(
          `MQTT drain phase complete: discarded ${staleMessagesDrained} stale messages`,
        );
        res();
      };
    });

    // Phase 2: Now accept real results
    return new Promise(async (resolve, reject) => {
      let queryRegistrationTime = null;
      let firstResultTime = null;
      let firstResultValue = null;
      let resolved = false;
      acceptingResults = true;

      this.mqttClient.on("message", (topic, message) => {
        if (resolved || !acceptingResults) return;

        const receiveTime = Date.now();
        const value = this.extractValueFromMessage(message);

        if (value === null || isNaN(value)) {
          return;
        }

        // Ignore results arriving before the publisher has started
        if (queryRegistrationTime === null) {
          this.log(
            `⚠️ Ignoring pre-registration message: value=${value.toFixed(4)} on topic=${topic}`,
          );
          return;
        }

        // Minimum latency threshold: a valid result cannot arrive before
        // at least one sub-window step has elapsed. This adapts to any
        // window configuration (--sub-window-step) and rejects stale
        // results from a previous iteration's dying orchestrator.
        const elapsed = receiveTime - queryRegistrationTime;
        const minValidLatency = parseInt(CONFIG.subWindowStep, 10);
        if (elapsed < minValidLatency) {
          this.log(
            `⚠️ Ignoring stale result (${elapsed}ms < ${minValidLatency}ms threshold): value=${value.toFixed(4)} on topic=${topic}`,
          );
          return;
        }

        if (firstResultTime === null) {
          firstResultTime = receiveTime;
          firstResultValue = value;
          const latency = firstResultTime - queryRegistrationTime;

          this.log(
            `✅ First result received: ${latency}ms, value: ${value.toFixed(4)}, topic: ${topic}`,
          );
          this.log(
            `   Raw message (first 300 chars): ${message.toString().substring(0, 300)}`,
          );

          resolved = true;
          clearInterval(monitorInterval);
          this.cleanup();

          const avgCpu =
            stats.cpuSamples.length > 0
              ? stats.cpuSamples.reduce((a, b) => a + b, 0) /
                stats.cpuSamples.length
              : 0;

          const maxCpu =
            stats.cpuSamples.length > 0 ? Math.max(...stats.cpuSamples) : 0;

          const avgMemory =
            stats.memorySamples.length > 0
              ? stats.memorySamples.reduce((a, b) => a + b, 0) /
                stats.memorySamples.length
              : 0;

          const maxMemory =
            stats.memorySamples.length > 0
              ? Math.max(...stats.memorySamples)
              : 0;

          resolve({
            approach: approachName,
            iteration: iterationNum,
            queryRegistrationTime,
            firstResultTime,
            latency,
            value: firstResultValue,
            resources: {
              avgCpu,
              maxCpu,
              avgMemory,
              maxMemory,
              samples: stats.cpuSamples.length,
            },
          });
        }
      });

      // Start orchestrator
      this.log(`Starting orchestrator...`);
      const orchestratorPath = ORCHESTRATORS[approachName];

      if (!fs.existsSync(orchestratorPath)) {
        reject(new Error(`Orchestrator not found: ${orchestratorPath}`));
        return;
      }

      const env = {
        ...process.env,
        DATA_PATH: CONFIG.dataPath,
        AGGREGATION_FUNC: CONFIG.aggregationFunc,
        PUBLISH_MODE: CONFIG.publishMode,
        SUB_WINDOW_RANGE: CONFIG.subWindowRange,
        SUB_WINDOW_STEP: CONFIG.subWindowStep,
        WEARABLE_FREQUENCY: CONFIG.wearableFrequency,
        SESSION_ID: `${approachName}-${iterationNum}-${Date.now()}`,
      };

      const orchestratorProc = spawn("node", [orchestratorPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: env,
      });

      this.processes.push(orchestratorProc);
      const orchestratorPid = orchestratorProc.pid;
      this.log(`Orchestrator PID: ${orchestratorPid}`);

      // Capture orchestrator output for debugging
      let orchestratorOutput = "";
      orchestratorProc.stdout.on("data", (data) => {
        orchestratorOutput += data.toString();
      });
      orchestratorProc.stderr.on("data", (data) => {
        const text = data.toString();
        orchestratorOutput += text;
        if (text.includes("Error") || text.includes("error")) {
          this.log(`⚠️ Orchestrator stderr: ${text.trim().substring(0, 200)}`);
        }
      });

      // Detect orchestrator crash
      orchestratorProc.on("exit", (code, signal) => {
        if (!resolved) {
          this.log(
            `⚠️ Orchestrator exited early (code: ${code}, signal: ${signal})`,
          );
          resolved = true;
          clearInterval(monitorInterval);
          clearTimeout(timeoutId);
          this.cleanup();
          resolve({
            approach: approachName,
            iteration: iterationNum,
            queryRegistrationTime,
            firstResultTime: null,
            latency: null,
            value: null,
            error: `Orchestrator exited (code: ${code}, signal: ${signal})`,
          });
        }
      });

      // Resource monitoring
      const stats = {
        cpuSamples: [],
        memorySamples: [],
        timestamps: [],
      };

      const monitorInterval = setInterval(async () => {
        if (resolved) return;

        const procStats = await this.getProcessStats(orchestratorPid);
        if (procStats) {
          stats.cpuSamples.push(procStats.cpu);
          stats.memorySamples.push(procStats.memory);
          stats.timestamps.push(Date.now());
        }
      }, 500);

      // Start publisher
      this.log(
        `Waiting ${CONFIG.startupDelay / 1000}s for orchestrator to initialize...`,
      );
      await new Promise((res) => setTimeout(res, CONFIG.startupDelay));

      queryRegistrationTime = Date.now();
      this.log(
        `Query registered at t0: ${new Date(queryRegistrationTime).toISOString()}`,
      );

      const publisherPath = path.join(
        PROJECT_ROOT,
        "dist/streamer/src/publish.js",
      );

      this.log("Starting publisher...");
      const publisherProc = spawn("node", [publisherPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: env,
      });

      this.processes.push(publisherProc);

      let publishCount = 0;
      publisherProc.stdout.on("data", (data) => {
        const text = data.toString();
        if (text.includes("Published observation")) {
          publishCount++;
          if (publishCount % 50 === 0) {
            this.log(`  Published ${publishCount} observations...`);
          }
        }
      });
      publisherProc.stderr.on("data", (data) => {
        const text = data.toString();
        if (text.includes("Error") || text.includes("error")) {
          this.log(`⚠️ Publisher stderr: ${text.trim().substring(0, 200)}`);
        }
      });

      // Detect publisher crash
      publisherProc.on("exit", (code, signal) => {
        if (!resolved) {
          this.log(
            `Publisher finished (code: ${code}). Published ${publishCount} observations.`,
          );
        }
      });

      // Timeout
      const timeoutId = setTimeout(() => {
        if (!resolved) {
          this.log(`⏰ TIMEOUT: No result within ${CONFIG.timeout / 1000}s`);
          this.log(
            `  Orchestrator output (last 500 chars): ${orchestratorOutput.slice(-500)}`,
          );
          resolved = true;
          clearInterval(monitorInterval);
          this.cleanup();
          resolve({
            approach: approachName,
            iteration: iterationNum,
            queryRegistrationTime,
            firstResultTime: null,
            latency: null,
            value: null,
            error: "TIMEOUT",
          });
        }
      }, CONFIG.timeout);
    });
  }

  async runAllIterations() {
    this.log(`\n${"█".repeat(70)}`);
    this.log(
      `UNIFIED BENCHMARK - ${CONFIG.pattern ? `Pattern: ${CONFIG.pattern}` : "Real Data"} | Aggregation: ${CONFIG.aggregationFunc} | PublishMode: ${CONFIG.publishMode} | SubWindow: ${CONFIG.subWindowRange}/${CONFIG.subWindowStep} | WearableFreq: ${CONFIG.wearableFrequency}Hz`,
    );
    this.log(`Iterations: ${CONFIG.iterations}`);
    this.log(`${"█".repeat(70)}\n`);

    const approaches = ["fetching", "approximation", "chunked"];

    for (let iter = 1; iter <= CONFIG.iterations; iter++) {
      this.log(`\n${"─".repeat(70)}`);
      this.log(`ITERATION ${iter}/${CONFIG.iterations}`);
      this.log(`${"─".repeat(70)}`);

      const iterationResults = {
        timestamp: new Date().toISOString(),
        results: {},
      };

      for (const approach of approaches) {
        try {
          const result = await this.runSingleApproach(approach, iter);
          iterationResults.results[approach] = result;

          // Small delay between approaches
          await new Promise((res) => setTimeout(res, 2000));
        } catch (error) {
          this.log(`❌ Error: ${error.message}`);
          iterationResults.results[approach] = {
            approach,
            iteration: iter,
            error: error.message,
          };
        }
      }

      this.allResults.byIteration[iter] = iterationResults;

      // Save iteration results immediately
      const iterFile = path.join(this.logsBaseDir, `iteration-${iter}.json`);
      fs.writeFileSync(iterFile, JSON.stringify(iterationResults, null, 2));

      // Delay between iterations
      if (iter < CONFIG.iterations) {
        this.log(`\n⏳ Pausing 5s before next iteration...`);
        await new Promise((res) => setTimeout(res, 5000));
      }
    }
  }

  calculateSummary() {
    const approaches = ["fetching", "approximation", "chunked"];
    const summary = {};

    for (const approach of approaches) {
      const latencies = [];
      const cpuValues = [];
      const memoryValues = [];
      const values = [];

      for (let iter = 1; iter <= CONFIG.iterations; iter++) {
        const result = this.allResults.byIteration[iter]?.results[approach];
        if (result && !result.error && result.latency) {
          latencies.push(result.latency);
          if (result.resources) {
            cpuValues.push(result.resources.avgCpu);
            memoryValues.push(result.resources.maxMemory / 1024 / 1024); // MB
          }
          values.push(result.value);
        }
      }

      if (latencies.length > 0) {
        const sortedLatencies = latencies.sort((a, b) => a - b);
        const avgLat = latencies.reduce((a, b) => a + b) / latencies.length;
        const stdDevLat = Math.sqrt(
          latencies.reduce((sq, n) => sq + Math.pow(n - avgLat, 2), 0) /
            latencies.length,
        );

        summary[approach] = {
          successCount: latencies.length,
          failureCount: CONFIG.iterations - latencies.length,
          latency: {
            avg: avgLat,
            stdDev: stdDevLat,
            min: sortedLatencies[0],
            max: sortedLatencies[sortedLatencies.length - 1],
            median: sortedLatencies[Math.floor(sortedLatencies.length / 2)],
          },
          resources: {
            avgCpu:
              cpuValues.length > 0
                ? cpuValues.reduce((a, b) => a + b) / cpuValues.length
                : 0,
            maxMemoryMB:
              memoryValues.length > 0 ? Math.max(...memoryValues) : 0,
          },
          values: values,
        };
      } else {
        summary[approach] = {
          successCount: 0,
          failureCount: CONFIG.iterations,
          error: "No successful runs",
        };
      }
    }

    this.allResults.summary = summary;
  }

  generateReport() {
    this.calculateSummary();

    let report = `\n${"=".repeat(80)}\n`;
    report += `UNIFIED BENCHMARK REPORT\n`;
    report += `${"=".repeat(80)}\n\n`;

    report += `Data: ${CONFIG.pattern ? `Pattern: ${CONFIG.pattern}` : "Real Data (smartphone.acceleration.x, wearable.acceleration.x)"}\n`;
    report += `Aggregation: ${CONFIG.aggregationFunc}\n`;
    report += `Publish Mode: ${CONFIG.publishMode}\n`;
    report += `Sub-Window: RANGE ${CONFIG.subWindowRange} STEP ${CONFIG.subWindowStep}\n`;
    report += `Wearable Frequency: ${CONFIG.wearableFrequency}Hz (Smartphone: 4Hz)\n`;
    report += `Iterations: ${CONFIG.iterations}\n`;
    report += `Timestamp: ${new Date().toISOString()}\n`;
    report += `Results Dir: ${this.logsBaseDir}\n\n`;

    // Summary table
    report += `${"─".repeat(80)}\n`;
    report += `FIRST EVENT LATENCY SUMMARY\n`;
    report += `${"─".repeat(80)}\n\n`;
    report += `Approach        | Avg (ms) | StdDev (ms) | Min (ms) | Max (ms) | Median (ms) | Success\n`;
    report += `${"-".repeat(80)}\n`;

    for (const approach of ["fetching", "approximation", "chunked"]) {
      const data = this.allResults.summary[approach];

      if (data.error) {
        report += `${approach.padEnd(15)} | ERROR: ${data.error}\n`;
      } else {
        const avg = data.latency.avg.toFixed(2).padStart(8);
        const stdDev = data.latency.stdDev.toFixed(2).padStart(10);
        const min = data.latency.min.toFixed(2).padStart(8);
        const max = data.latency.max.toFixed(2).padStart(8);
        const median = data.latency.median.toFixed(2).padStart(11);
        const success = `${data.successCount}/${CONFIG.iterations}`.padStart(7);

        report += `${approach.padEnd(15)} | ${avg} | ${stdDev} | ${min} | ${max} | ${median} | ${success}\n`;
      }
    }

    // Resource usage
    report += `\n${"─".repeat(80)}\n`;
    report += `RESOURCE USAGE (AVERAGE)\n`;
    report += `${"─".repeat(80)}\n\n`;
    report += `Approach        | Avg CPU (%) | Max Memory (MB)\n`;
    report += `${"-".repeat(45)}\n`;

    for (const approach of ["fetching", "approximation", "chunked"]) {
      const data = this.allResults.summary[approach];

      if (data.error) {
        report += `${approach.padEnd(15)} | N/A         | N/A\n`;
      } else {
        const cpu = data.resources.avgCpu.toFixed(1).padStart(11);
        const mem = data.resources.maxMemoryMB.toFixed(1).padStart(14);
        report += `${approach.padEnd(15)} | ${cpu} | ${mem}\n`;
      }
    }

    // Comparison
    report += `\n${"─".repeat(80)}\n`;
    report += `COMPARATIVE ANALYSIS\n`;
    report += `${"─".repeat(80)}\n\n`;

    const fetchingData = this.allResults.summary.fetching;
    if (fetchingData && !fetchingData.error) {
      const approxData = this.allResults.summary.approximation;
      const chunkedData = this.allResults.summary.chunked;

      if (approxData && !approxData.error) {
        const approxDiff = (
          ((approxData.latency.avg - fetchingData.latency.avg) /
            fetchingData.latency.avg) *
          100
        ).toFixed(1);
        report += `Approximation vs Fetching: ${approxDiff}% (${approxDiff > 0 ? "+" : ""}${approxDiff}%)\n`;
      }

      if (chunkedData && !chunkedData.error) {
        const chunkedDiff = (
          ((chunkedData.latency.avg - fetchingData.latency.avg) /
            fetchingData.latency.avg) *
          100
        ).toFixed(1);
        report += `Chunked vs Fetching: ${chunkedDiff}% (${chunkedDiff > 0 ? "+" : ""}${chunkedDiff}%)\n`;
      }
    }

    report += `\n${"=".repeat(80)}\n`;

    this.log(report);

    // Save reports
    const jsonPath = path.join(this.logsBaseDir, "benchmark-report.json");
    const txtPath = path.join(this.logsBaseDir, "benchmark-report.txt");

    fs.writeFileSync(jsonPath, JSON.stringify(this.allResults, null, 2));
    fs.writeFileSync(txtPath, report);

    this.log(`\n✅ Reports saved to:\n  ${jsonPath}\n  ${txtPath}`);
  }

  async run() {
    try {
      await this.runAllIterations();
      this.generateReport();
      this.log(`\n🎉 Benchmark complete!\n`);
    } catch (error) {
      this.log(`💥 Benchmark failed: ${error.message}`);
      process.exit(1);
    }
  }
}

// Main
if (require.main === module) {
  const benchmark = new UnifiedBenchmark();
  benchmark.run().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
}

module.exports = UnifiedBenchmark;
