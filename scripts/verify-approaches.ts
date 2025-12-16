#!/usr/bin/env ts-node

/**
 * Focused verification test for all 3 streaming query approaches.
 * This script verifies that each approach correctly processes data and produces output.
 *
 * Uses shorter timeouts for quicker verification while still testing the full pipeline.
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as path from "path";

const MQTT_BROKER = "mqtt://localhost:1883";

// Input topics
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";

// Output topics for each approach
const OUTPUT_TOPICS = {
  approximation: "approximation/output",
  chunked: "output",
  fetching: "client_operation_output",
};

// Test parameters - shorter duration for quick verification
const DATA_RATE_HZ = 4;
const EXPERIMENT_DURATION_S = 90; // 90 seconds to allow window triggers
const INIT_WAIT_S = 10;
const FINAL_WAIT_S = 20;

interface ApproachResult {
  name: string;
  topic: string;
  results: Array<{ timestamp: number; content: string }>;
  started: boolean;
  errors: string[];
}

/**
 * Main verification class for testing all three approaches
 */
class ApproachVerifier {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private publisherClient?: mqtt.MqttClient;
  private approachResults: Map<string, ApproachResult> = new Map();
  private dataPublishCount = 0;
  private allMessages: Array<{
    topic: string;
    timestamp: number;
    content: string;
  }> = [];

  constructor() {
    this.initializeResults();
  }

  /**
   * Initialize result tracking for each approach
   */
  private initializeResults(): void {
    this.approachResults.set("approximation", {
      name: "Approximation Approach",
      topic: OUTPUT_TOPICS.approximation,
      results: [],
      started: false,
      errors: [],
    });
    this.approachResults.set("chunked", {
      name: "Chunked Query Approach",
      topic: OUTPUT_TOPICS.chunked,
      results: [],
      started: false,
      errors: [],
    });
    this.approachResults.set("fetching", {
      name: "Fetching Client Side",
      topic: OUTPUT_TOPICS.fetching,
      results: [],
      started: false,
      errors: [],
    });
  }

  /**
   * Run the verification test
   */
  async run(): Promise<void> {
    console.log("=".repeat(70));
    console.log("VERIFICATION TEST: ALL 3 STREAMING QUERY APPROACHES");
    console.log("=".repeat(70));
    console.log("Testing:");
    console.log(
      "  1. Approximation Approach -> " + OUTPUT_TOPICS.approximation,
    );
    console.log("  2. Chunked Query Approach -> " + OUTPUT_TOPICS.chunked);
    console.log("  3. Fetching Client Side   -> " + OUTPUT_TOPICS.fetching);
    console.log("=".repeat(70));
    console.log(
      `Duration: ${EXPERIMENT_DURATION_S}s data + ${FINAL_WAIT_S}s wait`,
    );
    console.log("=".repeat(70));

    try {
      await this.setupMQTTMonitoring();
      await this.launchOrchestrators();

      console.log(
        `\n[WAIT] ${INIT_WAIT_S}s for orchestrators to initialize...`,
      );
      await this.sleep(INIT_WAIT_S * 1000);

      await this.publishTestData();

      console.log(`\n[WAIT] ${FINAL_WAIT_S}s for final results...`);
      await this.sleep(FINAL_WAIT_S * 1000);

      this.showResults();
    } catch (error) {
      console.error("\n[ERROR]", error);
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Setup MQTT monitoring for all output topics
   */
  private async setupMQTTMonitoring(): Promise<void> {
    console.log("\n[STEP 1] Setting up MQTT monitoring...");

    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: "verifier-monitor-" + Math.random().toString(16).substr(2, 8),
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("  Connected to MQTT broker");

        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
          OUTPUT_TOPICS.fetching,
          "chunked/#",
        ];

        this.mqttClient!.subscribe(topics, { qos: 1 }, (err) => {
          if (err) {
            console.error("  Failed to subscribe:", err);
            reject(err);
          } else {
            console.log("  Subscribed to:", topics.join(", "));
            resolve();
          }
        });
      });

      this.mqttClient.on("message", (topic: string, message: Buffer) => {
        const content = message.toString();
        const timestamp = Date.now();

        this.allMessages.push({ topic, timestamp, content });

        // Match to approach
        if (topic === OUTPUT_TOPICS.approximation) {
          this.recordResult("approximation", content);
        } else if (topic === OUTPUT_TOPICS.chunked) {
          this.recordResult("chunked", content);
        } else if (topic === OUTPUT_TOPICS.fetching) {
          this.recordResult("fetching", content);
        } else if (topic.startsWith("chunked/")) {
          // Subquery output for chunked approach
          console.log(
            `  [CHUNKED SUBQUERY] ${topic}: ${content.substring(0, 80)}...`,
          );
        }
      });

      this.mqttClient.on("error", (err) => {
        console.error("  MQTT error:", err);
        reject(err);
      });
    });
  }

  /**
   * Record a result for an approach
   */
  private recordResult(approach: string, content: string): void {
    const result = this.approachResults.get(approach);
    if (result) {
      result.results.push({ timestamp: Date.now(), content });
      console.log(
        `  [${result.name.toUpperCase()}] Result #${result.results.length}: ${content.substring(0, 100)}...`,
      );
    }
  }

  /**
   * Launch all three orchestrators
   */
  private async launchOrchestrators(): Promise<void> {
    console.log("\n[STEP 2] Launching orchestrators...");

    // Approximation Approach
    await this.launchOrchestrator(
      "approximation",
      "src/approaches/ApproximationApproachOrchestrator.ts",
      { HTTP_PORT: "8081" },
    );

    // Chunked Query Approach
    await this.launchOrchestrator(
      "chunked",
      "src/approaches/ChunkedQueryApproachOrchestrator.ts",
      { HTTP_PORT: "8082" },
    );

    // Fetching Client Side Approach
    await this.launchOrchestrator(
      "fetching",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
      { HTTP_PORT: "8083" },
    );

    console.log("  All orchestrators launched");
  }

  /**
   * Launch a single orchestrator
   */
  private launchOrchestrator(
    name: string,
    scriptPath: string,
    env: Record<string, string>,
  ): Promise<void> {
    return new Promise((resolve) => {
      const projectRoot = path.resolve(__dirname, "..");
      const fullPath = path.resolve(projectRoot, scriptPath);

      const proc = spawn("npx", ["ts-node", fullPath], {
        env: { ...process.env, ...env },
        stdio: ["pipe", "pipe", "pipe"],
      });

      this.orchestrators.set(name, proc);

      proc.stdout?.on("data", (data: Buffer) => {
        const lines = data
          .toString()
          .split("\n")
          .filter((l: string) => l.trim());
        for (const line of lines) {
          if (line.includes("error") || line.includes("Error")) {
            console.log(`  [${name.toUpperCase()}:ERR] ${line}`);
          } else if (
            line.includes("Result") ||
            line.includes("output") ||
            line.includes("Published")
          ) {
            console.log(`  [${name.toUpperCase()}] ${line}`);
          }
        }
      });

      proc.stderr?.on("data", (data: Buffer) => {
        const msg = data.toString().trim();
        if (msg && !msg.includes("ExperimentalWarning")) {
          console.log(
            `  [${name.toUpperCase()}:STDERR] ${msg.substring(0, 100)}`,
          );
        }
      });

      proc.on("error", (err) => {
        console.error(`  [${name.toUpperCase()}] Process error:`, err);
        const result = this.approachResults.get(name);
        if (result) result.errors.push(err.message);
      });

      proc.on("exit", (code) => {
        console.log(`  [${name.toUpperCase()}] Exited with code ${code}`);
      });

      // Mark as started
      const result = this.approachResults.get(name);
      if (result) result.started = true;

      // Give it a moment to start
      setTimeout(resolve, 1000);
    });
  }

  /**
   * Publish test data to input topics
   */
  private async publishTestData(): Promise<void> {
    console.log("\n[STEP 3] Publishing test data...");
    console.log(`  Duration: ${EXPERIMENT_DURATION_S}s at ${DATA_RATE_HZ}Hz`);

    return new Promise((resolve) => {
      this.publisherClient = mqtt.connect(MQTT_BROKER, {
        clientId:
          "verifier-publisher-" + Math.random().toString(16).substr(2, 8),
        clean: true,
      });

      this.publisherClient.on("connect", () => {
        console.log("  Publisher connected");

        const totalEvents = EXPERIMENT_DURATION_S * DATA_RATE_HZ;
        const intervalMs = 1000 / DATA_RATE_HZ;
        let count = 0;

        const interval = setInterval(() => {
          if (count >= totalEvents) {
            clearInterval(interval);
            console.log(`\n  Published ${count} events total`);
            resolve();
            return;
          }

          const timestamp = new Date().toISOString();
          const wearableValue = Math.sin(count * 0.1) * 10 + 20;
          const smartphoneValue = Math.cos(count * 0.1) * 5 + 15;

          const wearableData = `
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasValue> "${wearableValue}"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .
          `.trim();

          const smartphoneData = `
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasValue> "${smartphoneValue}"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/smartphoneX> .
          `.trim();

          this.publisherClient!.publish(WEARABLE_TOPIC, wearableData, {
            qos: 1,
          });
          this.publisherClient!.publish(SMARTPHONE_TOPIC, smartphoneData, {
            qos: 1,
          });

          count++;
          this.dataPublishCount = count;

          // Progress update every 10 seconds
          if (count % (DATA_RATE_HZ * 10) === 0) {
            const elapsed = count / DATA_RATE_HZ;
            const approxResults =
              this.approachResults.get("approximation")?.results.length || 0;
            const chunkedResults =
              this.approachResults.get("chunked")?.results.length || 0;
            const fetchingResults =
              this.approachResults.get("fetching")?.results.length || 0;
            console.log(
              `  [${elapsed}s] Events: ${count} | Results: Approx=${approxResults}, Chunked=${chunkedResults}, Fetching=${fetchingResults}`,
            );
          }
        }, intervalMs);
      });
    });
  }

  /**
   * Display final results
   */
  private showResults(): void {
    console.log("\n" + "=".repeat(70));
    console.log("VERIFICATION RESULTS");
    console.log("=".repeat(70));

    let allPassed = true;

    for (const [key, result] of this.approachResults) {
      const status = result.results.length > 0 ? "PASS" : "FAIL";
      const icon = result.results.length > 0 ? "[OK]" : "[X]";

      if (result.results.length === 0) allPassed = false;

      console.log(`\n${icon} ${result.name}`);
      console.log(`   Topic: ${result.topic}`);
      console.log(`   Started: ${result.started ? "Yes" : "No"}`);
      console.log(`   Results: ${result.results.length}`);
      console.log(`   Status: ${status}`);

      if (result.errors.length > 0) {
        console.log(`   Errors: ${result.errors.join(", ")}`);
      }

      if (result.results.length > 0) {
        console.log(
          `   Last result: ${result.results[result.results.length - 1].content.substring(0, 80)}...`,
        );
      }
    }

    console.log("\n" + "-".repeat(70));
    console.log(`Total MQTT messages received: ${this.allMessages.length}`);
    console.log(`Total events published: ${this.dataPublishCount}`);
    console.log("-".repeat(70));

    console.log("\n" + "=".repeat(70));
    if (allPassed) {
      console.log("OVERALL: ALL APPROACHES VERIFIED SUCCESSFULLY");
    } else {
      console.log("OVERALL: SOME APPROACHES FAILED TO PRODUCE RESULTS");
      console.log(
        "\nNote: Window sizes are 60s/120s, so results require ~60-90s of data.",
      );
      console.log(
        "If test ran for less time, this may be a timing issue rather than a bug.",
      );
    }
    console.log("=".repeat(70));
  }

  /**
   * Cleanup resources
   */
  private async cleanup(): Promise<void> {
    console.log(
      "\n[CLEANUP] Stopping orchestrators and closing connections...",
    );

    // Kill orchestrator processes
    for (const [name, proc] of this.orchestrators) {
      try {
        proc.kill("SIGTERM");
        console.log(`  Stopped ${name}`);
      } catch (e) {
        // Ignore
      }
    }

    // Close MQTT connections
    if (this.mqttClient) {
      this.mqttClient.end(true);
    }
    if (this.publisherClient) {
      this.publisherClient.end(true);
    }

    console.log("  Cleanup complete");
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Run the verification
const verifier = new ApproachVerifier();
verifier
  .run()
  .then(() => {
    process.exit(0);
  })
  .catch((err) => {
    console.error("Verification failed:", err);
    process.exit(1);
  });
