/**
 * Focused Latency Experiment: Query Registration to First Result
 *
 * This experiment measures the time from query registration to receiving the first result
 * for all three approaches: Fetching, Approximation, and Chunked.
 *
 * Key metrics:
 * - Query registration timestamp (t0)
 * - First result received timestamp (t1)
 * - Latency = t1 - t0
 */

const { spawn, execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");

const PROJECT_ROOT = path.resolve(__dirname, "../..");

const CONFIG = {
  mqttBroker: "mqtt://localhost:1883",
  windowWidth: 120000, // 120 seconds RANGE
  windowSlide: 60000, // 60 seconds STEP
  timeout: 300000, // 5 minutes max per approach
  startupDelay: 5000, // 5 seconds for orchestrator to initialize
};

// Output topics for each approach (corrected to match actual orchestrator output topics)
const OUTPUT_TOPICS = {
  fetching: ["client_operation_output"],
  approximation: ["approximation/output"],
  chunked: ["chunked/output", "output"],
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

class FirstResultLatencyExperiment {
  constructor() {
    this.logsDir = path.join(
      __dirname,
      "logs",
      `first-result-latency-${Date.now()}`,
    );
    fs.mkdirSync(this.logsDir, { recursive: true });
    this.logStream = fs.createWriteStream(
      path.join(this.logsDir, "experiment.log"),
    );
    this.processes = [];
    this.mqttClient = null;
    this.results = {};
  }

  log(message) {
    const timestamp = new Date().toISOString();
    const logLine = `[${timestamp}] ${message}`;
    console.log(logLine);
    this.logStream.write(logLine + "\n");
  }

  cleanup() {
    this.log("Cleaning up processes...");
    for (const proc of this.processes) {
      try {
        proc.kill("SIGTERM");
      } catch (e) {
        // Process may already be dead
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

    // Kill any stale node processes from previous runs
    try {
      execSync('pkill -f "StreamingQuery.*Orchestrator" 2>/dev/null || true', {
        stdio: "ignore",
      });
      execSync('pkill -f "node.*publish.js" 2>/dev/null || true', {
        stdio: "ignore",
      });
      execSync('pkill -f "BeeWorker" 2>/dev/null || true', { stdio: "ignore" });
    } catch (e) {
      // Ignore errors
    }
  }

  /**
   * Clear retained MQTT messages and drain any stale messages from topics
   */
  async clearMqttState(topics) {
    this.log("Clearing MQTT state and stale messages...");

    return new Promise((resolve) => {
      const cleanupClient = mqtt.connect(CONFIG.mqttBroker, {
        clientId: `cleanup-${Date.now()}`,
        clean: true,
      });

      let messagesCleared = 0;
      const startTime = Date.now();
      const drainTimeout = 3000; // 3 seconds to drain stale messages

      cleanupClient.on("connect", () => {
        // Subscribe to all relevant topics to drain stale messages
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

        // Drain messages for a few seconds
        cleanupClient.on("message", (topic, message) => {
          messagesCleared++;
          // Just consume and discard
        });

        // After drain timeout, clear retained messages and close
        setTimeout(() => {
          // Clear retained messages by publishing empty payloads
          for (const topic of topics) {
            cleanupClient.publish(topic, "", { retain: true });
          }

          setTimeout(() => {
            cleanupClient.end(true);
            this.log(`Cleared ${messagesCleared} stale messages from MQTT`);
            resolve();
          }, 500);
        }, drainTimeout);
      });

      cleanupClient.on("error", (err) => {
        this.log(`MQTT cleanup error: ${err}`);
        cleanupClient.end(true);
        resolve();
      });
    });
  }

  extractValueFromMessage(message) {
    const msgStr = message.toString();

    // Try JSON format
    try {
      const json = JSON.parse(msgStr);
      if (typeof json.value === "number") return json.value;
      if (typeof json.avgValue === "number") return json.avgValue;
      if (typeof json.result === "number") return json.result;
    } catch (e) {
      // Not JSON
    }

    // Try RDF format with escaped quotes
    const escapedRdfMatch = msgStr.match(
      /saref:hasValue\s+\\?"([0-9.eE+-]+)\\?"/,
    );
    if (escapedRdfMatch) return parseFloat(escapedRdfMatch[1]);

    // Try standard RDF format
    const rdfMatch = msgStr.match(/saref:hasValue\s+"?([0-9.eE+-]+)"?/);
    if (rdfMatch) return parseFloat(rdfMatch[1]);

    // Try quoted number
    const quotedMatch = msgStr.match(/"([0-9.eE+-]+)"/);
    if (quotedMatch) return parseFloat(quotedMatch[1]);

    // Try plain number
    const plainMatch = msgStr.match(/^([0-9.eE+-]+)$/);
    if (plainMatch) return parseFloat(plainMatch[1]);

    // Try any number in the string
    const numMatch = msgStr.match(/([0-9]+\.[0-9]+)/);
    if (numMatch) return parseFloat(numMatch[1]);

    return null;
  }

  async runSingleApproach(approachName) {
    this.log(`\n${"=".repeat(60)}`);
    this.log(`RUNNING: ${approachName.toUpperCase()}`);
    this.log(`${"=".repeat(60)}`);

    // Clear MQTT state before starting
    const topics = OUTPUT_TOPICS[approachName];
    await this.clearMqttState(topics);

    return new Promise(async (resolve, reject) => {
      let queryRegistrationTime = null;
      let firstResultTime = null;
      let firstResultValue = null;
      let resolved = false;

      const topics = OUTPUT_TOPICS[approachName];

      // Set up MQTT subscriber
      this.mqttClient = mqtt.connect(CONFIG.mqttBroker, {
        clientId: `latency-exp-${approachName}-${Date.now()}`,
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
        if (resolved) return;

        const receiveTime = Date.now();
        const value = this.extractValueFromMessage(message);

        if (value === null || isNaN(value)) {
          this.log(`Could not extract value from message on ${topic}`);
          return;
        }

        if (firstResultTime === null) {
          firstResultTime = receiveTime;
          firstResultValue = value;

          const latency = firstResultTime - queryRegistrationTime;

          this.log(`[${approachName}] FIRST RESULT RECEIVED!`);
          this.log(`  Query Registration Time: ${queryRegistrationTime}`);
          this.log(`  First Result Time: ${firstResultTime}`);
          this.log(`  Latency: ${latency}ms (${(latency / 1000).toFixed(2)}s)`);
          this.log(`  Value: ${value}`);

          resolved = true;
          this.cleanup();

          resolve({
            approach: approachName,
            queryRegistrationTime,
            firstResultTime,
            latency,
            value: firstResultValue,
          });
        }
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
        env: process.env,
      });

      this.processes.push(orchestratorProc);

      const orchLogFile = fs.createWriteStream(
        path.join(this.logsDir, `${approachName}_orchestrator.log`),
      );
      orchestratorProc.stdout.on("data", (data) => orchLogFile.write(data));
      orchestratorProc.stderr.on("data", (data) => orchLogFile.write(data));

      // Wait for orchestrator to initialize
      this.log(
        `Waiting ${CONFIG.startupDelay / 1000}s for orchestrator to initialize...`,
      );
      await new Promise((res) => setTimeout(res, CONFIG.startupDelay));

      // RECORD QUERY REGISTRATION TIME - This is t0
      // Note: For proper window semantics, the first result should arrive at approximately:
      // - t0 + STEP (60s) for sliding windows with RANGE > STEP
      // - t0 + RANGE (120s) for tumbling windows
      queryRegistrationTime = Date.now();
      this.log(`Query Registration Time (t0): ${queryRegistrationTime}`);
      this.log(
        `Expected first result at: t0 + ${CONFIG.windowSlide}ms = ${queryRegistrationTime + CONFIG.windowSlide} (for sliding window)`,
      );

      // Start publisher (data replay) - this is when actual data starts flowing
      this.log("Starting data publisher...");
      const publisherPath = path.join(
        PROJECT_ROOT,
        "dist/streamer/src/publish.js",
      );

      const publisherProc = spawn("node", [publisherPath], {
        stdio: ["inherit", "pipe", "pipe"],
        cwd: PROJECT_ROOT,
        env: process.env,
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
        if (!resolved) {
          this.log(
            `TIMEOUT: No result received for ${approachName} within ${CONFIG.timeout / 1000}s`,
          );
          resolved = true;
          this.cleanup();
          resolve({
            approach: approachName,
            queryRegistrationTime,
            firstResultTime: null,
            latency: null,
            value: null,
            error: "TIMEOUT",
          });
        }
      }, CONFIG.timeout);

      orchestratorProc.on("exit", () => {
        clearTimeout(timeoutId);
      });
    });
  }

  generateReport() {
    const report = {
      timestamp: new Date().toISOString(),
      config: CONFIG,
      results: this.results,
    };

    // Generate summary
    let summary = "\n" + "=".repeat(70) + "\n";
    summary += "FIRST RESULT LATENCY EXPERIMENT - SUMMARY\n";
    summary += "=".repeat(70) + "\n\n";
    summary += `Experiment Time: ${report.timestamp}\n`;
    summary += `Window Width: ${CONFIG.windowWidth}ms (${CONFIG.windowWidth / 1000}s)\n`;
    summary += `Window Slide: ${CONFIG.windowSlide}ms (${CONFIG.windowSlide / 1000}s)\n\n`;

    summary += "-".repeat(70) + "\n";
    summary += "LATENCY: Query Registration to First Result\n";
    summary += "-".repeat(70) + "\n\n";

    const approaches = ["fetching", "approximation", "chunked"];

    for (const approach of approaches) {
      const result = this.results[approach];
      if (!result) {
        summary += `${approach.toUpperCase()}: Not run\n\n`;
        continue;
      }

      summary += `${approach.toUpperCase()}:\n`;
      if (result.error) {
        summary += `  Status: ${result.error}\n`;
      } else {
        summary += `  Query Registration (t0): ${result.queryRegistrationTime}\n`;
        summary += `  First Result (t1): ${result.firstResultTime}\n`;
        summary += `  Latency (t1 - t0): ${result.latency}ms (${(result.latency / 1000).toFixed(2)}s)\n`;
        summary += `  First Value: ${result.value}\n`;
      }
      summary += "\n";
    }

    // Comparison table
    summary += "-".repeat(70) + "\n";
    summary += "COMPARISON TABLE\n";
    summary += "-".repeat(70) + "\n\n";
    summary +=
      "Approach        | Latency (ms)  | Latency (s)   | First Value\n";
    summary +=
      "----------------|---------------|---------------|---------------\n";

    for (const approach of approaches) {
      const result = this.results[approach];
      const name = approach.padEnd(15);
      if (!result || result.error) {
        summary += `${name} | N/A           | N/A           | N/A\n`;
      } else {
        const latencyMs = result.latency.toString().padEnd(13);
        const latencyS = (result.latency / 1000).toFixed(2).padEnd(13);
        const value = result.value
          ? result.value.toFixed(6).padEnd(13)
          : "N/A".padEnd(13);
        summary += `${name} | ${latencyMs} | ${latencyS} | ${value}\n`;
      }
    }

    // Find fastest
    const validResults = approaches
      .filter((a) => this.results[a] && !this.results[a].error)
      .map((a) => ({ approach: a, latency: this.results[a].latency }));

    if (validResults.length > 0) {
      validResults.sort((a, b) => a.latency - b.latency);
      summary += "\n";
      summary += `Fastest: ${validResults[0].approach.toUpperCase()} (${validResults[0].latency}ms)\n`;

      if (validResults.length > 1) {
        const baseline = validResults[0].latency;
        summary += "\nRelative to fastest:\n";
        for (const r of validResults) {
          const diff = r.latency - baseline;
          const pct = baseline > 0 ? ((diff / baseline) * 100).toFixed(1) : 0;
          summary += `  ${r.approach}: +${diff}ms (+${pct}%)\n`;
        }
      }
    }

    summary += "\n" + "=".repeat(70) + "\n";

    this.log(summary);

    // Save report
    const reportPath = path.join(this.logsDir, "latency-report.json");
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
    this.log(`Report saved to: ${reportPath}`);

    const summaryPath = path.join(this.logsDir, "latency-summary.txt");
    fs.writeFileSync(summaryPath, summary);
    this.log(`Summary saved to: ${summaryPath}`);
  }

  async run() {
    this.log("Starting First Result Latency Experiment");
    this.log(`Logs directory: ${this.logsDir}`);

    const approaches = ["fetching", "approximation", "chunked"];

    for (const approach of approaches) {
      try {
        // Clean up any stale processes before each run
        this.cleanup();
        this.log("Waiting 10s for processes to fully terminate...");
        await new Promise((res) => setTimeout(res, 10000));

        const result = await this.runSingleApproach(approach);
        this.results[approach] = result;

        // Wait between approaches to ensure clean state
        this.log("Waiting 10s before next approach...");
        await new Promise((res) => setTimeout(res, 10000));
      } catch (error) {
        this.log(`Error running ${approach}: ${error.message}`);
        this.results[approach] = { approach, error: error.message };
        this.cleanup();
      }
    }

    this.generateReport();
    this.log("Experiment complete!");
  }
}

async function main() {
  const experiment = new FirstResultLatencyExperiment();

  process.on("SIGINT", () => {
    console.log("\nInterrupted, cleaning up...");
    experiment.cleanup();
    process.exit(1);
  });

  try {
    await experiment.run();
    process.exit(0);
  } catch (error) {
    console.error("Experiment failed:", error);
    experiment.cleanup();
    process.exit(1);
  }
}

main();
