#!/usr/bin/env ts-node

/**
 * Comprehensive test script to verify ALL 3 streaming query approaches:
 * 1. Approximation Approach
 * 2. Chunked Query Approach
 * 3. Fetching Client Side Approach
 *
 * This test subscribes to all relevant output topics and publishes test data
 * for long enough to allow windows to trigger.
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as fs from "fs";

const MQTT_BROKER = "mqtt://localhost:1883";

// Input topics
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";

// Output topics for each approach
const OUTPUT_TOPICS = {
  approximation: "approximation/output",
  chunked: "chunked/output",
  chunkedAlt: "output",
  fetching: "client_operation_output",
};

const DATA_RATE_HZ = 4; // 4Hz data rate
const EXPERIMENT_DURATION_S = 150; // 150 seconds to allow multiple window triggers

interface ApproachResult {
  name: string;
  topic: string;
  results: Array<{ timestamp: number; content: any }>;
  errors: string[];
}

class AllApproachesTester {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private approachResults: Map<string, ApproachResult> = new Map();
  private dataPublishCount = 0;
  private allMQTTMessages: Array<{ topic: string; timestamp: number; content: string }> = [];

  constructor() {
    // Initialize result tracking for each approach
    this.approachResults.set("approximation", {
      name: "Approximation Approach",
      topic: OUTPUT_TOPICS.approximation,
      results: [],
      errors: [],
    });
    this.approachResults.set("chunked", {
      name: "Chunked Query Approach",
      topic: OUTPUT_TOPICS.chunked,
      results: [],
      errors: [],
    });
    this.approachResults.set("fetching", {
      name: "Fetching Client Side",
      topic: OUTPUT_TOPICS.fetching,
      results: [],
      errors: [],
    });
  }

  async run(): Promise<void> {
    console.log("=".repeat(70));
    console.log("COMPREHENSIVE TEST: ALL 3 STREAMING QUERY APPROACHES");
    console.log("=".repeat(70));
    console.log("Testing:");
    console.log("  1. Approximation Approach (output: approximation/output)");
    console.log("  2. Chunked Query Approach (output: chunked/output)");
    console.log("  3. Fetching Client Side (output: client_operation_output)");
    console.log("=".repeat(70));
    console.log(`Duration: ${EXPERIMENT_DURATION_S}s data stream + 30s wait`);
    console.log("=".repeat(70));

    try {
      await this.clearLogs();
      await this.setupMQTTMonitoring();
      await this.launchAllOrchestrators();

      console.log("\n[WAIT] 15 seconds for all orchestrators to initialize...");
      await this.sleep(15000);

      await this.publishTestData();

      console.log("\n[WAIT] 30 seconds for final results processing...");
      await this.sleep(30000);

      this.showComprehensiveResults();
    } catch (error) {
      console.error("\n[ERROR]", error);
    } finally {
      await this.cleanup();
    }
  }

  private async clearLogs(): Promise<void> {
    console.log("\n[STEP 1] Clearing old log files...");
    const logFiles = [
      "approximation_approach_log.csv",
      "approximation_approach_resource_usage.csv",
      "chunked_query_approach_log.csv",
      "chunked_query_approach_resource_log.csv",
      "fetching_client_side_log.csv",
      "fetching_client_side_resource_usage.csv",
    ];

    logFiles.forEach((file) => {
      try {
        if (fs.existsSync(file)) fs.unlinkSync(file);
      } catch (e) {
        // Ignore
      }
    });
    console.log("  Logs cleared");
  }

  private async setupMQTTMonitoring(): Promise<void> {
    console.log("\n[STEP 2] Setting up MQTT monitoring for all output topics...");

    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: "all-approaches-test-" + Math.random().toString(16).substr(2, 8),
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("  Connected to MQTT broker");

        // Subscribe to all output topics
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
          OUTPUT_TOPICS.chunkedAlt,
          OUTPUT_TOPICS.fetching,
          "chunked/#", // Also subscribe to chunked subquery topics
        ];

        this.mqttClient!.subscribe(topics, { qos: 1 }, (err) => {
          if (err) {
            console.error("  Failed to subscribe:", err);
            reject(err);
          } else {
            console.log("  Subscribed to output topics:");
            topics.forEach((t) => console.log(`    - ${t}`));
            resolve();
          }
        });
      });

      this.mqttClient.on("message", (topic, message) => {
        const timestamp = Date.now();
        const content = message.toString();

        // Store all messages
        this.allMQTTMessages.push({ topic, timestamp, content });

        // Categorize by approach
        if (topic === OUTPUT_TOPICS.approximation) {
          this.handleApproximationResult(timestamp, content);
        } else if (topic === OUTPUT_TOPICS.chunked || topic === OUTPUT_TOPICS.chunkedAlt) {
          this.handleChunkedResult(timestamp, content);
        } else if (topic === OUTPUT_TOPICS.fetching) {
          this.handleFetchingResult(timestamp, content);
        } else if (topic.startsWith("chunked/") && topic !== OUTPUT_TOPICS.chunked) {
          // Chunked subquery result
          console.log(`\n[CHUNKED SUBQUERY] Topic: ${topic}`);
          console.log(`  Content preview: ${content.substring(0, 100)}...`);
        }
      });

      this.mqttClient.on("error", (error) => {
        console.error("  MQTT Error:", error);
        reject(error);
      });

      setTimeout(() => reject(new Error("MQTT connection timeout")), 10000);
    });
  }

  private handleApproximationResult(timestamp: number, content: string): void {
    const result = this.approachResults.get("approximation")!;
    try {
      const parsed = JSON.parse(content);
      result.results.push({ timestamp, content: parsed });
      console.log(`\n[APPROX RESULT #${result.results.length}]`);
      console.log(`  Unified Result: ${parsed.unifiedResult}`);
      console.log(`  Aggregation Type: ${parsed.aggregationType}`);
      if (parsed.window) {
        console.log(`  Window: ${parsed.window.start} to ${parsed.window.end}`);
      }
    } catch (e) {
      result.results.push({ timestamp, content: content.substring(0, 200) });
      console.log(`\n[APPROX RESULT #${result.results.length}] (raw): ${content.substring(0, 100)}...`);
    }
  }

  private handleChunkedResult(timestamp: number, content: string): void {
    const result = this.approachResults.get("chunked")!;
    try {
      const parsed = JSON.parse(content);
      result.results.push({ timestamp, content: parsed });
      console.log(`\n[CHUNKED RESULT #${result.results.length}]`);
      console.log(`  Content: ${JSON.stringify(parsed).substring(0, 150)}`);
    } catch (e) {
      // Try RDF parsing
      const valueMatch = content.match(/hasValue>\s*"([^"]+)"/);
      if (valueMatch) {
        result.results.push({ timestamp, content: { value: valueMatch[1] } });
        console.log(`\n[CHUNKED RESULT #${result.results.length}] Value: ${valueMatch[1]}`);
      } else {
        result.results.push({ timestamp, content: content.substring(0, 200) });
        console.log(`\n[CHUNKED RESULT #${result.results.length}] (raw): ${content.substring(0, 100)}...`);
      }
    }
  }

  private handleFetchingResult(timestamp: number, content: string): void {
    const result = this.approachResults.get("fetching")!;
    const valueMatch = content.match(/hasValue>\s*"([^"]+)"/);
    if (valueMatch) {
      result.results.push({ timestamp, content: { value: parseFloat(valueMatch[1]) } });
      console.log(`\n[FETCHING RESULT #${result.results.length}] Value: ${valueMatch[1]}`);
    } else {
      try {
        const parsed = JSON.parse(content);
        result.results.push({ timestamp, content: parsed });
        console.log(`\n[FETCHING RESULT #${result.results.length}] ${JSON.stringify(parsed).substring(0, 100)}`);
      } catch (e) {
        result.results.push({ timestamp, content: content.substring(0, 200) });
        console.log(`\n[FETCHING RESULT #${result.results.length}] (raw): ${content.substring(0, 100)}...`);
      }
    }
  }

  private async launchAllOrchestrators(): Promise<void> {
    console.log("\n[STEP 3] Launching all 3 orchestrators...");

    // Approximation Approach
    const approxOrch = spawn(
      "npx",
      ["ts-node", "src/approaches/ApproximationApproachOrchestrator.ts"],
      { env: { ...process.env, HTTP_PORT: "8081" } }
    );
    this.setupOrchestratorLogging(approxOrch, "APPROX");
    this.orchestrators.set("approximation", approxOrch);
    console.log("  Launched Approximation Approach (HTTP_PORT=8081)");

    // Chunked Query Approach
    const chunkedOrch = spawn(
      "npx",
      ["ts-node", "src/approaches/ChunkedQueryApproachOrchestrator.ts"],
      { env: { ...process.env, HTTP_PORT: "8082" } }
    );
    this.setupOrchestratorLogging(chunkedOrch, "CHUNKED");
    this.orchestrators.set("chunked", chunkedOrch);
    console.log("  Launched Chunked Query Approach (HTTP_PORT=8082)");

    // Fetching Client Side Approach
    const fetchingOrch = spawn("npx", [
      "ts-node",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
    ]);
    this.setupOrchestratorLogging(fetchingOrch, "FETCHING");
    this.orchestrators.set("fetching", fetchingOrch);
    console.log("  Launched Fetching Client Side Approach");

    console.log("  All 3 orchestrators launched");
  }

  private setupOrchestratorLogging(orch: ChildProcess, prefix: string): void {
    orch.stdout?.on("data", (data) => {
      const lines = data.toString().trim().split("\n");
      lines.forEach((line: string) => {
        // Only log important messages to avoid clutter
        if (
          line.includes("ERROR") ||
          line.includes("result") ||
          line.includes("published") ||
          line.includes("Result") ||
          line.includes("initialized") ||
          line.includes("connected")
        ) {
          console.log(`  [${prefix}] ${line.substring(0, 120)}`);
        }
      });
    });

    orch.stderr?.on("data", (data) => {
      const msg = data.toString().trim();
      if (!msg.includes("ExperimentalWarning") && !msg.includes("fetch")) {
        console.error(`  [${prefix} ERR] ${msg.substring(0, 120)}`);
        const result = this.approachResults.get(prefix.toLowerCase());
        if (result) {
          result.errors.push(msg);
        }
      }
    });
  }

  private async publishTestData(): Promise<void> {
    console.log("\n[STEP 4] Publishing test data...");
    console.log(`  Duration: ${EXPERIMENT_DURATION_S}s at ${DATA_RATE_HZ}Hz per sensor`);
    console.log(`  Total expected data points: ${EXPERIMENT_DURATION_S * DATA_RATE_HZ * 2}`);

    const totalEvents = EXPERIMENT_DURATION_S * DATA_RATE_HZ;
    const intervalMs = 1000 / DATA_RATE_HZ;

    for (let i = 0; i < totalEvents; i++) {
      const timestamp = new Date().toISOString();
      const wearableValue = 10 + 5 * Math.sin((2 * Math.PI * i) / (DATA_RATE_HZ * 10));
      const smartphoneValue = 15 + 3 * Math.cos((2 * Math.PI * i) / (DATA_RATE_HZ * 8));

      const wearableQuad = `<http://example.org/wearable/${i}> <https://saref.etsi.org/core/hasValue> "${wearableValue.toFixed(2)}"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.org/wearable/${i}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/wearable/${i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Sensor> .
<http://example.org/wearable/${i}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .`;

      const smartphoneQuad = `<http://example.org/smartphone/${i}> <https://saref.etsi.org/core/hasValue> "${smartphoneValue.toFixed(2)}"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.org/smartphone/${i}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/smartphone/${i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Sensor> .
<http://example.org/smartphone/${i}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/smartphoneX> .`;

      this.mqttClient!.publish(WEARABLE_TOPIC, wearableQuad, { qos: 1 });
      this.mqttClient!.publish(SMARTPHONE_TOPIC, smartphoneQuad, { qos: 1 });

      this.dataPublishCount += 2;

      // Progress update every 20 seconds
      if ((i + 1) % (DATA_RATE_HZ * 20) === 0) {
        const progressPct = ((i + 1) / totalEvents) * 100;
        const elapsedSec = (i + 1) / DATA_RATE_HZ;
        console.log(
          `  Progress: ${progressPct.toFixed(0)}% (${elapsedSec}s elapsed, ${this.dataPublishCount} data points)`
        );
        this.printIntermediateResults();
      }

      await this.sleep(intervalMs);
    }

    console.log(`  Published ${this.dataPublishCount} total data points`);
  }

  private printIntermediateResults(): void {
    const approx = this.approachResults.get("approximation")!;
    const chunked = this.approachResults.get("chunked")!;
    const fetching = this.approachResults.get("fetching")!;

    console.log(
      `    Results so far: Approx=${approx.results.length}, Chunked=${chunked.results.length}, Fetching=${fetching.results.length}`
    );
  }

  private showComprehensiveResults(): void {
    console.log("\n" + "=".repeat(70));
    console.log("COMPREHENSIVE TEST RESULTS");
    console.log("=".repeat(70));

    console.log(`\nData Published: ${this.dataPublishCount} data points`);
    console.log(`Total MQTT Messages Captured: ${this.allMQTTMessages.length}`);

    console.log("\n" + "-".repeat(70));
    console.log("RESULTS BY APPROACH:");
    console.log("-".repeat(70));

    this.approachResults.forEach((result, key) => {
      const status = result.results.length > 0 ? "PASS" : "FAIL";
      const statusIcon = result.results.length > 0 ? "PASS" : "FAIL";

      console.log(`\n[${statusIcon}] ${result.name}`);
      console.log(`    Output Topic: ${result.topic}`);
      console.log(`    Results Received: ${result.results.length}`);
      console.log(`    Errors: ${result.errors.length}`);

      if (result.results.length > 0) {
        console.log("    Latest 3 results:");
        result.results.slice(-3).forEach((r, idx) => {
          const contentStr =
            typeof r.content === "object"
              ? JSON.stringify(r.content).substring(0, 80)
              : String(r.content).substring(0, 80);
          console.log(`      ${idx + 1}. ${contentStr}`);
        });
      }

      if (result.errors.length > 0) {
        console.log("    Errors (first 3):");
        result.errors.slice(0, 3).forEach((err, idx) => {
          console.log(`      ${idx + 1}. ${err.substring(0, 80)}`);
        });
      }
    });

    console.log("\n" + "-".repeat(70));
    console.log("LOG FILES:");
    console.log("-".repeat(70));

    const logFiles = [
      { file: "approximation_approach_log.csv", approach: "Approximation" },
      { file: "chunked_query_approach_log.csv", approach: "Chunked Query" },
      { file: "fetching_client_side_log.csv", approach: "Fetching Client Side" },
    ];

    logFiles.forEach(({ file, approach }) => {
      if (fs.existsSync(file)) {
        const stats = fs.statSync(file);
        const lines = fs.readFileSync(file, "utf8").split("\n").length - 1;
        console.log(`  [EXISTS] ${file}: ${(stats.size / 1024).toFixed(2)} KB, ${lines} lines`);
      } else {
        console.log(`  [MISSING] ${file}`);
      }
    });

    console.log("\n" + "=".repeat(70));
    console.log("SUMMARY:");
    console.log("=".repeat(70));

    let passCount = 0;
    let failCount = 0;

    this.approachResults.forEach((result) => {
      if (result.results.length > 0) {
        passCount++;
        console.log(`  [PASS] ${result.name}: ${result.results.length} results`);
      } else {
        failCount++;
        console.log(`  [FAIL] ${result.name}: No results received`);
      }
    });

    console.log(`\nOverall: ${passCount}/3 approaches produced results`);

    if (failCount > 0) {
      console.log("\nDIAGNOSIS for failed approaches:");
      console.log("  - Check if orchestrator initialized correctly (look for errors above)");
      console.log("  - Window sizes: subqueries=60s/30s, main=120s/60s");
      console.log("  - Ensure sufficient data was published (need >60s for first window)");
      console.log("  - Check RSPAgent MQTT publishing and topic mapping");
    }

    console.log("\n" + "=".repeat(70));
  }

  private async cleanup(): Promise<void> {
    console.log("\n[CLEANUP] Shutting down...");

    this.orchestrators.forEach((orch, name) => {
      orch.kill("SIGTERM");
      console.log(`  Stopped ${name} orchestrator`);
    });

    if (this.mqttClient) {
      this.mqttClient.end();
      console.log("  Disconnected MQTT client");
    }

    console.log("  Cleanup complete");
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

if (require.main === module) {
  const tester = new AllApproachesTester();
  tester
    .run()
    .then(() => {
      console.log("\nTest completed!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("\nTest failed:", error);
      process.exit(1);
    });
}

export { AllApproachesTester };
