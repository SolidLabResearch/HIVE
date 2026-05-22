#!/usr/bin/env node

/**
 * Comprehensive Comparison Script with Window Close Latency and Accuracy Measurement
 *
 * This script runs all three approaches (fetching, approximation, chunked) and compares:
 * 1. Accuracy - comparing approximation and chunked against fetching (baseline)
 * 2. Window Close Latency - time from window end to result emission (first window only)
 *
 * Usage: node run-comparison-with-latency.js
 */

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");

// Project root directory
const PROJECT_ROOT = path.resolve(__dirname, "../..");

// Configuration
const CONFIG = {
  mqttBroker: "mqtt://localhost:1883",
  windowWidth: 120000,  // 120 seconds (from query: RANGE 120000)
  windowSlide: 60000,   // 60 seconds (from query: STEP 60000)
  timeout: 12 * 60 * 1000, // 12 minutes
  settleTime: 5000,     // Time to wait after publisher finishes
  startupDelay: 3000,   // Delay before starting publisher
};

// Topics each approach publishes to
const OUTPUT_TOPICS = {
  fetching: "client_operation_output",
  approximation: "approximation/output",
  chunked: ["output", "chunked/output"],
};

// Orchestrator paths
const ORCHESTRATORS = {
  fetching: path.join(PROJECT_ROOT, "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js"),
  approximation: path.join(PROJECT_ROOT, "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js"),
  chunked: path.join(PROJECT_ROOT, "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js"),
};

// Results storage
const results = {
  fetching: { values: [], timestamps: [], firstWindowLatency: null },
  approximation: { values: [], timestamps: [], firstWindowLatency: null },
  chunked: { values: [], timestamps: [], firstWindowLatency: null },
};

class ComparisonRunner {
  constructor() {
    this.logsDir = path.join(__dirname, "logs", `comparison_${Date.now()}`);
    fs.mkdirSync(this.logsDir, { recursive: true });

    this.processes = [];
    this.mqttClient = null;
    this.startTime = null;
    this.firstWindowEnd = null;
    this.publisherFinished = false;
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);
    this.runEnv = this.replayEnv.withBenchmarkReplayEnv(process.env);
  }

  log(message) {
    const timestamp = new Date().toISOString();
    const logLine = `[${timestamp}] ${message}`;
    console.log(logLine);
    fs.appendFileSync(path.join(this.logsDir, "comparison.log"), logLine + "\n");
  }

  cleanup() {
    this.log("Cleaning up processes...");

    for (const proc of this.processes) {
      if (proc && !proc.killed) {
        proc.kill("SIGTERM");
      }
    }

    if (this.mqttClient) {
      this.mqttClient.end();
    }
  }

  extractValueFromMessage(message, approach) {
    try {
      const msgStr = message.toString();

      // Try parsing as JSON first
      try {
        const json = JSON.parse(msgStr);
        if (json.unifiedResult !== undefined) return parseFloat(json.unifiedResult);
        if (json.unifiedAverage !== undefined) return parseFloat(json.unifiedAverage);
        if (json.value !== undefined) return parseFloat(json.value);
        if (json.avgValue !== undefined) return parseFloat(json.avgValue);
      } catch (e) {
        // Not JSON, try other formats
      }

      // Try RDF/Turtle format: hasValue "X.XXX"^^xsd:float
      const rdfMatch = msgStr.match(/hasValue[>"]?\s*["']?(-?[\d.]+)/);
      if (rdfMatch) return parseFloat(rdfMatch[1]);

      // Try plain number
      const numMatch = msgStr.match(/(-?[\d.]+)/);
      if (numMatch) return parseFloat(numMatch[1]);

      return null;
    } catch (e) {
      this.log(`Error extracting value for ${approach}: ${e.message}`);
      return null;
    }
  }

  calculateFirstWindowLatency(receiveTime, approach) {
    // First window closes at: startTime + windowWidth
    // But we use the actual first window end time calculated from when data started
    if (this.firstWindowEnd === null) {
      // Calculate when first window should close
      // First window: [startTime, startTime + windowWidth]
      this.firstWindowEnd = this.startTime + CONFIG.windowWidth;
    }

    // Latency = time result received - time window should have closed
    const latency = receiveTime - this.firstWindowEnd;
    return latency;
  }

  setupMQTTSubscriber() {
    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(CONFIG.mqttBroker, {
        clientId: `comparison-subscriber-${Date.now()}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        this.log("MQTT subscriber connected");

        // Subscribe to all output topics
        const allTopics = [
          OUTPUT_TOPICS.fetching,
          OUTPUT_TOPICS.approximation,
          ...OUTPUT_TOPICS.chunked,
        ];

        for (const topic of allTopics) {
          this.mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
              this.log(`Failed to subscribe to ${topic}: ${err}`);
            } else {
              this.log(`Subscribed to ${topic}`);
            }
          });
        }

        resolve();
      });

      this.mqttClient.on("message", (topic, message) => {
        const receiveTime = Date.now();
        let approach = null;

        // Determine which approach this message is from
        if (topic === OUTPUT_TOPICS.fetching) {
          approach = "fetching";
        } else if (topic === OUTPUT_TOPICS.approximation) {
          approach = "approximation";
        } else if (OUTPUT_TOPICS.chunked.includes(topic)) {
          approach = "chunked";
        }

        if (!approach) return;

        const value = this.extractValueFromMessage(message, approach);
        if (value === null || isNaN(value)) {
          this.log(`Could not extract value from ${approach} message on ${topic}`);
          return;
        }

        this.log(`[${approach}] Received value: ${value} on topic ${topic}`);

        // Store result
        results[approach].values.push(value);
        results[approach].timestamps.push(receiveTime);

        // Calculate first window latency (only for the first result)
        if (results[approach].firstWindowLatency === null && this.startTime !== null) {
          const latency = this.calculateFirstWindowLatency(receiveTime, approach);
          results[approach].firstWindowLatency = latency;
          this.log(`[${approach}] First window latency: ${latency}ms`);
        }
      });

      this.mqttClient.on("error", (err) => {
        this.log(`MQTT error: ${err}`);
        reject(err);
      });
    });
  }

  startOrchestrator(approach) {
    return new Promise((resolve, reject) => {
      this.log(`Starting ${approach} orchestrator...`);

      const orchestratorPath = ORCHESTRATORS[approach];
      if (!fs.existsSync(orchestratorPath)) {
        reject(new Error(`Orchestrator not found: ${orchestratorPath}`));
        return;
      }

      const proc = spawn("node", [orchestratorPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: this.runEnv,
      });

      this.processes.push(proc);

      const logFile = fs.createWriteStream(path.join(this.logsDir, `${approach}_orchestrator.log`));

      proc.stdout.on("data", (data) => {
        logFile.write(data);
      });

      proc.stderr.on("data", (data) => {
        logFile.write(data);
      });

      proc.on("error", (err) => {
        this.log(`${approach} orchestrator error: ${err}`);
        reject(err);
      });

      // Give orchestrator time to initialize
      setTimeout(() => {
        this.log(`${approach} orchestrator started`);
        resolve(proc);
      }, 2000);
    });
  }

  startPublisher() {
    return new Promise((resolve, reject) => {
      this.log("Starting data publisher...");

      const publisherPath = path.join(PROJECT_ROOT, "dist/streamer/src/publish.js");
      if (!fs.existsSync(publisherPath)) {
        reject(new Error(`Publisher not found: ${publisherPath}`));
        return;
      }

      const proc = spawn("node", [publisherPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: this.runEnv,
      });

      this.processes.push(proc);

      const logFile = fs.createWriteStream(path.join(this.logsDir, "publisher.log"));
      let publishCount = 0;

      proc.stdout.on("data", (data) => {
        logFile.write(data);
        const text = data.toString();
        if (text.includes("Published observation")) {
          publishCount++;
          if (publishCount % 100 === 0) {
            process.stdout.write(`\r  Published ${publishCount} observations...`);
          }
        }
      });

      proc.stderr.on("data", (data) => {
        logFile.write(data);
      });

      proc.on("close", (code) => {
        console.log("");
        this.log(`Publisher finished with code ${code}, published ${publishCount} observations`);
        this.publisherFinished = true;
        resolve(code);
      });

      proc.on("error", (err) => {
        this.log(`Publisher error: ${err}`);
        reject(err);
      });
    });
  }

  calculateAccuracy() {
    const analysis = {
      baseline: results.fetching,
      approaches: {},
    };

    // Use fetching as baseline
    const baselineValues = results.fetching.values;

    if (baselineValues.length === 0) {
      this.log("WARNING: No baseline (fetching) values received");
      return analysis;
    }

    for (const approach of ["approximation", "chunked"]) {
      const approachValues = results[approach].values;

      if (approachValues.length === 0) {
        analysis.approaches[approach] = {
          valuesReceived: 0,
          accuracy: null,
          mae: null,
          mape: null,
          exactMatches: 0,
          exactMatchRate: 0,
        };
        continue;
      }

      // Calculate metrics
      let exactMatches = 0;
      let sumAbsError = 0;
      let sumAbsPercentError = 0;
      const tolerance = 0.001;

      // Compare window by window (up to the minimum length)
      const compareCount = Math.min(baselineValues.length, approachValues.length);

      for (let i = 0; i < compareCount; i++) {
        const baseline = baselineValues[i];
        const approachVal = approachValues[i];

        const absError = Math.abs(approachVal - baseline);
        sumAbsError += absError;

        if (baseline !== 0) {
          sumAbsPercentError += (absError / Math.abs(baseline)) * 100;
        }

        if (absError <= tolerance) {
          exactMatches++;
        }
      }

      const mae = compareCount > 0 ? sumAbsError / compareCount : null;
      const mape = compareCount > 0 ? sumAbsPercentError / compareCount : null;
      const exactMatchRate = compareCount > 0 ? (exactMatches / compareCount) * 100 : 0;

      analysis.approaches[approach] = {
        valuesReceived: approachValues.length,
        baselineValuesCompared: compareCount,
        accuracy: exactMatchRate,
        mae: mae,
        mape: mape,
        exactMatches: exactMatches,
        exactMatchRate: exactMatchRate,
        values: approachValues,
      };
    }

    analysis.baseline.valuesReceived = baselineValues.length;

    return analysis;
  }

  generateReport(duration) {
    const analysis = this.calculateAccuracy();

    const report = {
      timestamp: new Date().toISOString(),
      duration: duration,
      config: CONFIG,
      results: {
        fetching: {
          label: "Fetching Client Side (Baseline)",
          valuesReceived: results.fetching.values.length,
          values: results.fetching.values,
          firstWindowLatency: results.fetching.firstWindowLatency,
        },
        approximation: {
          label: "Approximation",
          valuesReceived: results.approximation.values.length,
          values: results.approximation.values,
          firstWindowLatency: results.approximation.firstWindowLatency,
          accuracy: analysis.approaches.approximation,
        },
        chunked: {
          label: "Chunked",
          valuesReceived: results.chunked.values.length,
          values: results.chunked.values,
          firstWindowLatency: results.chunked.firstWindowLatency,
          accuracy: analysis.approaches.chunked,
        },
      },
    };

    // Save JSON report
    fs.writeFileSync(
      path.join(this.logsDir, "report.json"),
      JSON.stringify(report, null, 2)
    );

    // Print summary
    console.log("\n" + "=".repeat(80));
    console.log("COMPARISON RESULTS");
    console.log("=".repeat(80));

    console.log(`\nDuration: ${(duration / 1000).toFixed(1)}s`);
    console.log(`Logs saved to: ${this.logsDir}`);

    console.log("\n--- VALUES RECEIVED ---");
    console.log(`Fetching (baseline):  ${results.fetching.values.length} values`);
    console.log(`  Values: ${results.fetching.values.map(v => v.toFixed(6)).join(", ") || "none"}`);
    console.log(`Approximation:        ${results.approximation.values.length} values`);
    console.log(`  Values: ${results.approximation.values.map(v => v.toFixed(6)).join(", ") || "none"}`);
    console.log(`Chunked:              ${results.chunked.values.length} values`);
    console.log(`  Values: ${results.chunked.values.map(v => v.toFixed(6)).join(", ") || "none"}`);

    console.log("\n--- FIRST WINDOW LATENCY ---");
    console.log(`Fetching (baseline):  ${results.fetching.firstWindowLatency !== null ? results.fetching.firstWindowLatency + "ms" : "N/A"}`);
    console.log(`Approximation:        ${results.approximation.firstWindowLatency !== null ? results.approximation.firstWindowLatency + "ms" : "N/A"}`);
    console.log(`Chunked:              ${results.chunked.firstWindowLatency !== null ? results.chunked.firstWindowLatency + "ms" : "N/A"}`);

    console.log("\n--- ACCURACY (vs Fetching Baseline) ---");

    for (const approach of ["approximation", "chunked"]) {
      const acc = analysis.approaches[approach];
      if (acc && acc.mae !== null) {
        console.log(`\n${approach.charAt(0).toUpperCase() + approach.slice(1)}:`);
        console.log(`  Exact Match Rate: ${acc.exactMatchRate.toFixed(1)}% (${acc.exactMatches}/${acc.baselineValuesCompared})`);
        console.log(`  Mean Absolute Error (MAE): ${acc.mae.toFixed(6)}`);
        console.log(`  Mean Absolute Percentage Error (MAPE): ${acc.mape !== null ? acc.mape.toFixed(2) + "%" : "N/A"}`);
      } else {
        console.log(`\n${approach.charAt(0).toUpperCase() + approach.slice(1)}: No data to compare`);
      }
    }

    console.log("\n" + "=".repeat(80));

    return report;
  }

  async run() {
    console.log("=".repeat(80));
    console.log("COMPARISON RUNNER WITH LATENCY MEASUREMENT");
    console.log("=".repeat(80));
    console.log(`\nThis will run all three approaches simultaneously and compare:`);
    console.log(`  1. Accuracy (vs fetching baseline)`);
    console.log(`  2. First Window Close Latency`);
    console.log(`\nTimeout: ${CONFIG.timeout / 1000}s`);
    console.log(`Logs: ${this.logsDir}\n`);

    const overallStart = Date.now();

    try {
      // Step 1: Set up MQTT subscriber
      await this.setupMQTTSubscriber();

      // Step 2: Start all orchestrators
      this.log("Starting all orchestrators...");
      await Promise.all([
        this.startOrchestrator("fetching"),
        this.startOrchestrator("approximation"),
        this.startOrchestrator("chunked"),
      ]);

      // Step 3: Wait for orchestrators to initialize
      this.log(`Waiting ${CONFIG.startupDelay / 1000}s for orchestrators to initialize...`);
      await new Promise(resolve => setTimeout(resolve, CONFIG.startupDelay));

      // Step 4: Record start time (for latency calculation)
      this.startTime = Date.now();
      this.log(`Recording start time: ${this.startTime}`);

      // Step 5: Start publisher
      await this.startPublisher();

      // Step 6: Wait for final results to arrive
      this.log(`Waiting ${CONFIG.settleTime / 1000}s for final results...`);
      await new Promise(resolve => setTimeout(resolve, CONFIG.settleTime));

    } catch (error) {
      this.log(`Error during comparison: ${error.message}`);
      console.error(error);
    } finally {
      const duration = Date.now() - overallStart;

      // Generate report
      this.generateReport(duration);

      // Cleanup
      this.cleanup();
    }
  }
}

// Main execution
async function main() {
  console.log("\n🔬 Starting Comparison with Latency Measurement\n");
  console.log("Prerequisites:");
  console.log("  ✓ MQTT broker running (mosquitto)");
  console.log("  ✓ Project built (npm run build)");
  console.log("  ✓ Data files exist in src/streamer/data/\n");

  const runner = new ComparisonRunner();

  // Handle Ctrl+C gracefully
  process.on("SIGINT", () => {
    console.log("\n\nReceived SIGINT, cleaning up...");
    runner.cleanup();
    process.exit(130);
  });

  process.on("SIGTERM", () => {
    console.log("\n\nReceived SIGTERM, cleaning up...");
    runner.cleanup();
    process.exit(143);
  });

  await runner.run();
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
