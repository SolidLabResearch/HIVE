#!/usr/bin/env node

/**
 * Sequential Comparison Script with Window Close Latency and Accuracy Measurement
 *
 * This script runs each approach (fetching, approximation, chunked) ONE AT A TIME
 * to avoid port conflicts, then compares:
 * 1. Accuracy - comparing approximation and chunked against fetching (baseline)
 * 2. Window Close Latency - time from window end to result emission (first window only)
 *
 * Usage: node run-sequential-comparison.js
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
  windowWidth: 120000, // 120 seconds (from query: RANGE 120000)
  windowSlide: 60000, // 60 seconds (from query: STEP 60000)
  timeout: 5 * 60 * 1000, // 5 minutes per approach
  settleTime: 10000, // Time to wait after publisher finishes
  startupDelay: 3000, // Delay before starting publisher
};

// Topics each approach publishes to
const OUTPUT_TOPICS = {
  fetching: ["client_operation_output"],
  approximation: ["approximation/output"],
  chunked: ["output", "chunked/output"],
};

// Latency log files for each approach
const LATENCY_LOG_FILES = {
  fetching: "fetching_latency_log.csv",
  approximation: "approximation_latency_log.csv",
  chunked: "chunked_latency_log.csv",
};

// Orchestrator paths
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

// Results storage
const allResults = {
  fetching: {
    values: [],
    timestamps: [],
    firstWindowLatency: null,
    duration: 0,
  },
  approximation: {
    values: [],
    timestamps: [],
    firstWindowLatency: null,
    duration: 0,
  },
  chunked: {
    values: [],
    timestamps: [],
    firstWindowLatency: null,
    duration: 0,
  },
};

class SequentialComparisonRunner {
  constructor() {
    this.logsDir = path.join(
      __dirname,
      "logs",
      `sequential_comparison_${Date.now()}`,
    );
    fs.mkdirSync(this.logsDir, { recursive: true });

    this.processes = [];
    this.mqttClient = null;
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);
    this.runEnv = this.replayEnv.withBenchmarkReplayEnv(process.env);
  }

  /**
   * Read latency measurements from the latency log file
   */
  readLatencyLog(approachName) {
    const latencyLogFile = path.join(
      PROJECT_ROOT,
      LATENCY_LOG_FILES[approachName],
    );
    const latencies = [];

    if (!fs.existsSync(latencyLogFile)) {
      this.log(
        `No latency log file found for ${approachName}: ${latencyLogFile}`,
      );
      return latencies;
    }

    try {
      const content = fs.readFileSync(latencyLogFile, "utf8");
      const lines = content.trim().split("\n");

      // Skip header line
      // CSV format varies by approach:
      // Fetching/Approximation: window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_obs_ms,result_value
      // Chunked: window_number,query_registered_at,first_data_received_at,expected_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,interval_wait_ms,computation_ms,result_value
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(",");

        // Handle chunked format (12 columns)
        if (parts.length >= 12) {
          const windowNumber = parseInt(parts[0]);
          const queryRegisteredAt = parseInt(parts[1]);
          const firstDataReceivedAt = parseInt(parts[2]);
          const expectedWindowClose = parseInt(parts[3]);
          const lastObsReceivedAt = parseInt(parts[4]);
          const intervalTriggerAt = parseInt(parts[5]);
          const resultEmittedAt = parseInt(parts[6]);
          const latencyFromQueryReg = parseInt(parts[7]);
          const latencyFromDataStart = parseInt(parts[8]);
          const intervalWaitTime = parseInt(parts[9]);
          const computationTime = parseInt(parts[10]);
          const value = parts[11];

          latencies.push({
            windowNumber,
            queryRegisteredAt,
            firstDataReceivedAt,
            expectedWindowClose,
            lastObsReceivedAt,
            intervalTriggerAt,
            resultEmittedAt,
            latencyFromQueryReg,
            latencyFromDataStart,
            intervalWaitTime,
            computationTime,
            latencyFromLastObs: intervalWaitTime + computationTime, // Total processing for compatibility
            value,
          });
        }
        // Handle fetching/approximation format (10 columns)
        else if (parts.length >= 10) {
          const windowNumber = parseInt(parts[0]);
          const queryRegisteredAt = parseInt(parts[1]);
          const firstDataReceivedAt = parseInt(parts[2]);
          const expectedWindowClose = parseInt(parts[3]);
          const lastObsReceivedAt = parseInt(parts[4]);
          const resultEmittedAt = parseInt(parts[5]);
          const latencyFromQueryReg = parseInt(parts[6]);
          const latencyFromDataStart = parseInt(parts[7]);
          const latencyFromLastObs = parseInt(parts[8]);
          const value = parts[9];

          latencies.push({
            windowNumber,
            queryRegisteredAt,
            firstDataReceivedAt,
            expectedWindowClose,
            lastObsReceivedAt,
            resultEmittedAt,
            latencyFromQueryReg,
            latencyFromDataStart,
            latencyFromLastObs,
            intervalWaitTime: null,
            computationTime: latencyFromLastObs, // For non-chunked, processing = computation
            value,
          });
        }
      }

      this.log(
        `Read ${latencies.length} latency measurements from ${approachName} log`,
      );

      // Copy latency log to logs directory
      const destPath = path.join(
        this.logsDir,
        `${approachName}_latency_log.csv`,
      );
      fs.copyFileSync(latencyLogFile, destPath);

      // Clean up the original file for next run
      fs.unlinkSync(latencyLogFile);
    } catch (err) {
      this.log(`Error reading latency log for ${approachName}: ${err.message}`);
    }

    return latencies;
  }

  log(message) {
    const timestamp = new Date().toISOString();
    const logLine = `[${timestamp}] ${message}`;
    console.log(logLine);
    fs.appendFileSync(
      path.join(this.logsDir, "comparison.log"),
      logLine + "\n",
    );
  }

  cleanup() {
    this.log("Cleaning up processes...");

    for (const proc of this.processes) {
      if (proc && !proc.killed) {
        try {
          proc.kill("SIGTERM");
        } catch (e) {
          // Ignore
        }
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
  }

  extractValueFromMessage(message) {
    try {
      const msgStr = message.toString();

      // Try parsing as JSON first
      try {
        const json = JSON.parse(msgStr);
        if (json.unifiedResult !== undefined)
          return parseFloat(json.unifiedResult);
        if (json.unifiedAverage !== undefined)
          return parseFloat(json.unifiedAverage);
        if (json.value !== undefined) return parseFloat(json.value);
        if (json.avgValue !== undefined) return parseFloat(json.avgValue);
      } catch (e) {
        // Not JSON, try other formats
      }

      // Try RDF/Turtle format with escaped quotes: hasValue \"4.234195\"^^
      // This handles: <https://saref.etsi.org/core/hasValue> \"4.234195\"^^<http://...
      const escapedRdfMatch = msgStr.match(
        /hasValue[>"]?\s*\\?"(-?[\d.]+)\\?"/,
      );
      if (escapedRdfMatch) return parseFloat(escapedRdfMatch[1]);

      // Try RDF/Turtle format without escaped quotes: hasValue "4.234195"^^
      const rdfMatch = msgStr.match(/hasValue[>"]?\s*"(-?[\d.]+)"/);
      if (rdfMatch) return parseFloat(rdfMatch[1]);

      // Try quoted number pattern: "4.234195"
      const quotedMatch = msgStr.match(/"(-?[\d.]+)"/);
      if (quotedMatch) return parseFloat(quotedMatch[1]);

      // Try escaped quoted number pattern: \"4.234195\"
      const escapedQuotedMatch = msgStr.match(/\\"(-?[\d.]+)\\"/);
      if (escapedQuotedMatch) return parseFloat(escapedQuotedMatch[1]);

      // Try plain number at end of line or string
      const plainMatch = msgStr.match(/(-?[\d.]+)\s*(?:\^|$|")/);
      if (plainMatch) return parseFloat(plainMatch[1]);

      // Last resort: find any float-like number
      const numMatch = msgStr.match(/(-?\d+\.\d+)/);
      if (numMatch) return parseFloat(numMatch[1]);

      return null;
    } catch (e) {
      this.log(`Error extracting value: ${e.message}`);
      return null;
    }
  }

  async runSingleApproach(approachName) {
    this.log(`\n${"=".repeat(60)}`);
    this.log(`RUNNING: ${approachName.toUpperCase()}`);
    this.log(`${"=".repeat(60)}`);

    const results = {
      values: [],
      timestamps: [],
      firstWindowLatency: null,
    };

    let startTime = null;
    let firstWindowEnd = null;

    return new Promise(async (resolve, reject) => {
      const approachStart = Date.now();

      // Set up MQTT subscriber for this approach
      const topics = OUTPUT_TOPICS[approachName];

      this.mqttClient = mqtt.connect(CONFIG.mqttBroker, {
        clientId: `comparison-${approachName}-${Date.now()}`,
        clean: true,
      });

      await new Promise((res, rej) => {
        this.mqttClient.on("connect", () => {
          this.log(`MQTT subscriber connected for ${approachName}`);

          for (const topic of topics) {
            this.mqttClient.subscribe(topic, { qos: 1 }, (err) => {
              if (err) {
                this.log(`Failed to subscribe to ${topic}: ${err}`);
              } else {
                this.log(`Subscribed to ${topic}`);
              }
            });
          }
          res();
        });

        this.mqttClient.on("error", (err) => {
          this.log(`MQTT error: ${err}`);
          rej(err);
        });
      });

      this.mqttClient.on("message", (topic, message) => {
        const receiveTime = Date.now();

        const value = this.extractValueFromMessage(message);
        if (value === null || isNaN(value)) {
          this.log(
            `Could not extract value from message on ${topic}: ${message.toString().substring(0, 100)}`,
          );
          return;
        }

        this.log(
          `[${approachName}] Received value: ${value} on topic ${topic}`,
        );

        results.values.push(value);
        results.timestamps.push(receiveTime);

        // Note: Actual latency will be read from latency log files after the run
        // This is just for tracking received values
      });

      // Start orchestrator
      this.log(`Starting ${approachName} orchestrator...`);
      const orchestratorPath = ORCHESTRATORS[approachName];

      if (!fs.existsSync(orchestratorPath)) {
        reject(new Error(`Orchestrator not found: ${orchestratorPath}`));
        return;
      }

      const orchestratorProc = spawn("node", [orchestratorPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: this.runEnv,
      });

      this.processes.push(orchestratorProc);

      const orchLogFile = fs.createWriteStream(
        path.join(this.logsDir, `${approachName}_orchestrator.log`),
      );
      orchestratorProc.stdout.on("data", (data) => orchLogFile.write(data));
      orchestratorProc.stderr.on("data", (data) => orchLogFile.write(data));

      orchestratorProc.on("error", (err) => {
        this.log(`${approachName} orchestrator error: ${err}`);
      });

      // Wait for orchestrator to initialize
      this.log(
        `Waiting ${CONFIG.startupDelay / 1000}s for orchestrator to initialize...`,
      );
      await new Promise((res) => setTimeout(res, CONFIG.startupDelay));

      // Record start time for latency calculation
      startTime = Date.now();
      this.log(`Recording start time: ${startTime}`);

      // Start publisher
      this.log("Starting data publisher...");
      const publisherPath = path.join(
        PROJECT_ROOT,
        "dist/streamer/src/publish.js",
      );

      const publisherProc = spawn("node", [publisherPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: this.runEnv,
      });

      this.processes.push(publisherProc);

      const pubLogFile = fs.createWriteStream(
        path.join(this.logsDir, `${approachName}_publisher.log`),
      );
      let publishCount = 0;

      publisherProc.stdout.on("data", (data) => {
        pubLogFile.write(data);
        const text = data.toString();
        if (text.includes("Published observation")) {
          publishCount++;
          if (publishCount % 100 === 0) {
            process.stdout.write(
              `\r  [${approachName}] Published ${publishCount} observations...`,
            );
          }
        }
      });

      publisherProc.stderr.on("data", (data) => pubLogFile.write(data));

      // Set timeout
      const timeoutId = setTimeout(() => {
        this.log(`Timeout reached for ${approachName}`);
        finishApproach();
      }, CONFIG.timeout);

      const finishApproach = () => {
        clearTimeout(timeoutId);
        console.log(""); // New line after progress dots

        const duration = Date.now() - approachStart;
        this.log(
          `${approachName} finished in ${(duration / 1000).toFixed(1)}s`,
        );
        this.log(
          `${approachName} received ${results.values.length} values: ${results.values.join(", ")}`,
        );

        // Read latency measurements from the instrumented log file
        const latencyMeasurements = this.readLatencyLog(approachName);

        // Get first window latency from the log file (using latencyFromDataStart as the primary metric)
        let firstWindowLatency = null;
        if (latencyMeasurements.length > 0) {
          firstWindowLatency = latencyMeasurements[0].latencyFromDataStart;
          this.log(
            `[${approachName}] First window latency from log (from data start): ${firstWindowLatency}ms`,
          );
          this.log(
            `[${approachName}] First window processing time: ${latencyMeasurements[0].latencyFromLastObs}ms`,
          );
        }

        // Store results
        allResults[approachName] = {
          ...results,
          duration: duration,
          firstWindowLatency: firstWindowLatency,
          latencyMeasurements: latencyMeasurements,
        };

        // Cleanup this approach's processes
        this.cleanup();

        // Wait a bit before resolving to ensure port is freed
        setTimeout(() => resolve(results), 3000);
      };

      publisherProc.on("close", (code) => {
        this.log(
          `Publisher finished with code ${code}, published ${publishCount} observations`,
        );

        // Wait for settle time then finish
        this.log(`Waiting ${CONFIG.settleTime / 1000}s for final results...`);
        setTimeout(finishApproach, CONFIG.settleTime);
      });

      publisherProc.on("error", (err) => {
        this.log(`Publisher error: ${err}`);
        finishApproach();
      });
    });
  }

  calculateAccuracy() {
    const analysis = {
      baseline: allResults.fetching,
      approaches: {},
    };

    const baselineValues = allResults.fetching.values;

    if (baselineValues.length === 0) {
      this.log("WARNING: No baseline (fetching) values received");
      return analysis;
    }

    for (const approach of ["approximation", "chunked"]) {
      const approachValues = allResults[approach].values;

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

      let exactMatches = 0;
      let sumAbsError = 0;
      let sumAbsPercentError = 0;
      const tolerance = 0.001;

      const compareCount = Math.min(
        baselineValues.length,
        approachValues.length,
      );

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
      const exactMatchRate =
        compareCount > 0 ? (exactMatches / compareCount) * 100 : 0;

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

  generateReport() {
    const analysis = this.calculateAccuracy();

    const report = {
      timestamp: new Date().toISOString(),
      config: CONFIG,
      results: {
        fetching: {
          label: "Fetching Client Side (Baseline)",
          valuesReceived: allResults.fetching.values.length,
          values: allResults.fetching.values,
          firstWindowLatency: allResults.fetching.firstWindowLatency,
          duration: allResults.fetching.duration,
        },
        approximation: {
          label: "Approximation",
          valuesReceived: allResults.approximation.values.length,
          values: allResults.approximation.values,
          firstWindowLatency: allResults.approximation.firstWindowLatency,
          duration: allResults.approximation.duration,
          accuracy: analysis.approaches.approximation,
        },
        chunked: {
          label: "Chunked",
          valuesReceived: allResults.chunked.values.length,
          values: allResults.chunked.values,
          firstWindowLatency: allResults.chunked.firstWindowLatency,
          duration: allResults.chunked.duration,
          accuracy: analysis.approaches.chunked,
        },
      },
    };

    // Save JSON report
    fs.writeFileSync(
      path.join(this.logsDir, "report.json"),
      JSON.stringify(report, null, 2),
    );

    // Print summary
    console.log("\n" + "=".repeat(80));
    console.log("FINAL COMPARISON RESULTS");
    console.log("=".repeat(80));

    console.log(`\nLogs saved to: ${this.logsDir}`);

    console.log("\n--- VALUES RECEIVED ---");
    console.log(
      `Fetching (baseline):  ${allResults.fetching.values.length} values`,
    );
    console.log(
      `  Values: ${allResults.fetching.values.map((v) => v.toFixed(6)).join(", ") || "none"}`,
    );
    console.log(
      `  Duration: ${(allResults.fetching.duration / 1000).toFixed(1)}s`,
    );

    console.log(
      `\nApproximation:        ${allResults.approximation.values.length} values`,
    );
    console.log(
      `  Values: ${allResults.approximation.values.map((v) => v.toFixed(6)).join(", ") || "none"}`,
    );
    console.log(
      `  Duration: ${(allResults.approximation.duration / 1000).toFixed(1)}s`,
    );

    console.log(
      `\nChunked:              ${allResults.chunked.values.length} values`,
    );
    console.log(
      `  Values: ${allResults.chunked.values.map((v) => v.toFixed(6)).join(", ") || "none"}`,
    );
    console.log(
      `  Duration: ${(allResults.chunked.duration / 1000).toFixed(1)}s`,
    );

    console.log("\n--- LATENCY METRICS ---");
    console.log("Three metrics are measured:");
    console.log(
      "  1. From Query Registration: result_time - (query_reg_time + RANGE + (N-1)*STEP)",
    );
    console.log(
      "  2. From Data Start: result_time - (first_data_time + RANGE + (N-1)*STEP)",
    );
    console.log(
      "  3. Processing Time: result_time - last_observation_received_time",
    );

    // Print detailed latency measurements
    console.log("\n--- DETAILED LATENCY MEASUREMENTS ---");
    for (const approach of ["fetching", "approximation", "chunked"]) {
      const measurements = allResults[approach].latencyMeasurements || [];
      if (measurements.length > 0) {
        console.log(
          `\n${approach.charAt(0).toUpperCase() + approach.slice(1)}:`,
        );
        measurements.forEach((m) => {
          console.log(`  Window ${m.windowNumber} (value: ${m.value}):`);
          const formatLatency = (lat) => (lat >= 0 ? `+${lat}ms` : `${lat}ms`);
          console.log(
            `    From query registration: ${formatLatency(m.latencyFromQueryReg)}`,
          );
          console.log(
            `    From data start:         ${formatLatency(m.latencyFromDataStart)}`,
          );
          if (m.intervalWaitTime !== null && m.intervalWaitTime !== undefined) {
            // Chunked approach - show interval wait and computation separately
            console.log(
              `    Interval wait time:      ${formatLatency(m.intervalWaitTime)}`,
            );
            console.log(
              `    Computation time:        ${formatLatency(m.computationTime)}`,
            );
          } else {
            // Fetching/Approximation - just show processing time
            console.log(
              `    Processing time:         ${formatLatency(m.latencyFromLastObs)}`,
            );
          }
        });

        // Calculate averages for each metric
        const avgFromQueryReg =
          measurements.reduce((sum, m) => sum + m.latencyFromQueryReg, 0) /
          measurements.length;
        const avgFromDataStart =
          measurements.reduce((sum, m) => sum + m.latencyFromDataStart, 0) /
          measurements.length;
        const avgProcessing =
          measurements.reduce((sum, m) => sum + m.latencyFromLastObs, 0) /
          measurements.length;

        // Check if this is chunked (has intervalWaitTime)
        const hasIntervalMetrics = measurements.some(
          (m) =>
            m.intervalWaitTime !== null && m.intervalWaitTime !== undefined,
        );

        console.log(`  Averages:`);
        console.log(
          `    From query registration: ${avgFromQueryReg.toFixed(2)}ms`,
        );
        console.log(
          `    From data start:         ${avgFromDataStart.toFixed(2)}ms`,
        );

        if (hasIntervalMetrics) {
          const avgIntervalWait =
            measurements.reduce(
              (sum, m) => sum + (m.intervalWaitTime || 0),
              0,
            ) / measurements.length;
          const avgComputation =
            measurements.reduce((sum, m) => sum + (m.computationTime || 0), 0) /
            measurements.length;
          console.log(
            `    Interval wait time:      ${avgIntervalWait.toFixed(2)}ms`,
          );
          console.log(
            `    Computation time:        ${avgComputation.toFixed(2)}ms`,
          );
        } else {
          console.log(
            `    Processing time:         ${avgProcessing.toFixed(2)}ms`,
          );
        }
      } else {
        console.log(
          `\n${approach.charAt(0).toUpperCase() + approach.slice(1)}: No latency measurements`,
        );
      }
    }

    // Summary table for easy comparison
    console.log("\n--- LATENCY SUMMARY TABLE ---");
    console.log(
      "| Approach      | From Query Reg (avg) | From Data Start (avg) | Processing (avg) |",
    );
    console.log(
      "|---------------|----------------------|-----------------------|------------------|",
    );
    for (const approach of ["fetching", "approximation", "chunked"]) {
      const measurements = allResults[approach].latencyMeasurements || [];
      if (measurements.length > 0) {
        const avgFromQueryReg =
          measurements.reduce((sum, m) => sum + m.latencyFromQueryReg, 0) /
          measurements.length;
        const avgFromDataStart =
          measurements.reduce((sum, m) => sum + m.latencyFromDataStart, 0) /
          measurements.length;
        const avgProcessing =
          measurements.reduce((sum, m) => sum + m.latencyFromLastObs, 0) /
          measurements.length;
        const hasIntervalMetrics = measurements.some(
          (m) =>
            m.intervalWaitTime !== null && m.intervalWaitTime !== undefined,
        );
        const avgIntervalWait = hasIntervalMetrics
          ? measurements.reduce(
              (sum, m) => sum + (m.intervalWaitTime || 0),
              0,
            ) / measurements.length
          : null;
        const avgComputation = hasIntervalMetrics
          ? measurements.reduce((sum, m) => sum + (m.computationTime || 0), 0) /
            measurements.length
          : null;

        const name = approach.charAt(0).toUpperCase() + approach.slice(1);
        const intervalStr =
          avgIntervalWait !== null ? `${avgIntervalWait.toFixed(0)}ms` : "N/A";
        const computeStr =
          avgComputation !== null
            ? `${avgComputation.toFixed(0)}ms`
            : `${avgProcessing.toFixed(0)}ms`;
        console.log(
          `| ${name.padEnd(13)} | ${avgFromQueryReg.toFixed(0).padStart(20)}ms | ${avgFromDataStart.toFixed(0).padStart(21)}ms | ${avgProcessing.toFixed(0).padStart(16)}ms | ${intervalStr.padStart(13)} | ${computeStr.padStart(11)} |`,
        );
      } else {
        const name = approach.charAt(0).toUpperCase() + approach.slice(1);
        console.log(
          `| ${name.padEnd(13)} | ${"N/A".padStart(20)}   | ${"N/A".padStart(21)}   | ${"N/A".padStart(16)}   | ${"N/A".padStart(13)} | ${"N/A".padStart(11)} |`,
        );
      }
    }

    console.log("\n--- ACCURACY (vs Fetching Baseline) ---");

    for (const approach of ["approximation", "chunked"]) {
      const acc = analysis.approaches[approach];
      if (acc && acc.mae !== null) {
        console.log(
          `\n${approach.charAt(0).toUpperCase() + approach.slice(1)}:`,
        );
        console.log(
          `  Exact Match Rate: ${acc.exactMatchRate.toFixed(1)}% (${acc.exactMatches}/${acc.baselineValuesCompared})`,
        );
        console.log(`  Mean Absolute Error (MAE): ${acc.mae.toFixed(6)}`);
        console.log(
          `  Mean Absolute Percentage Error (MAPE): ${acc.mape !== null ? acc.mape.toFixed(2) + "%" : "N/A"}`,
        );
      } else {
        console.log(
          `\n${approach.charAt(0).toUpperCase() + approach.slice(1)}: No data to compare`,
        );
      }
    }

    console.log("\n" + "=".repeat(80));

    return report;
  }

  async run() {
    console.log("=".repeat(80));
    console.log("SEQUENTIAL COMPARISON RUNNER");
    console.log("=".repeat(80));
    console.log(
      `\nThis will run each approach ONE AT A TIME to avoid port conflicts.`,
    );
    console.log(`Then compare accuracy and first window close latency.`);
    console.log(`\nTimeout per approach: ${CONFIG.timeout / 1000}s`);
    console.log(`Logs: ${this.logsDir}\n`);

    try {
      // Run each approach sequentially
      for (const approach of ["fetching", "approximation", "chunked"]) {
        await this.runSingleApproach(approach);

        // Extra wait between approaches to ensure ports are freed
        this.log("Waiting 5s before next approach...");
        await new Promise((res) => setTimeout(res, 5000));
      }

      // Generate final report
      this.generateReport();
    } catch (error) {
      this.log(`Error during comparison: ${error.message}`);
      console.error(error);
      this.cleanup();
    }
  }
}

// Main execution
async function main() {
  console.log("\n🔬 Starting Sequential Comparison\n");
  console.log("Prerequisites:");
  console.log("  - MQTT broker running (mosquitto)");
  console.log("  - Project built (npm run build)");
  console.log("  - Data files exist in src/streamer/data/\n");

  const runner = new SequentialComparisonRunner();

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
