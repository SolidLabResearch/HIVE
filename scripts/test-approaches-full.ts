#!/usr/bin/env ts-node

/**
 * Test script to run both streaming approaches with test data
 * This publishes test data to MQTT and monitors results from both approaches
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as path from "path";
import * as fs from "fs";

const MQTT_BROKER = "mqtt://localhost:1883";
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";
const APPROX_OUTPUT_TOPIC = "approximation/output";
const FETCHING_OUTPUT_TOPIC = "client_operation_output";

const DATA_RATE_HZ = 4; // 4Hz data rate
const EXPERIMENT_DURATION_S = 150; // 2.5 minutes

class ApproachTester {
  private approxOrchestrator?: ChildProcess;
  private fetchingOrchestrator?: ChildProcess;
  private mqttClient?: mqtt.MqttClient;
  private approxResults: any[] = [];
  private fetchingResults: any[] = [];
  private dataPublishCount = 0;

  /**
   * Main test execution
   */
  async run(): Promise<void> {
    console.log("=".repeat(60));
    console.log("Testing Streaming Query Approaches");
    console.log("=".repeat(60));

    try {
      // Step 1: Clear old logs
      await this.clearLogs();

      // Step 2: Start MQTT monitoring
      await this.setupMQTTMonitoring();

      // Step 3: Launch orchestrators
      await this.launchOrchestrators();

      // Step 4: Wait for initialization
      console.log(
        "\n[INFO] Waiting 10 seconds for orchestrators to initialize...",
      );
      await this.sleep(10000);

      // Step 5: Start publishing test data
      await this.publishTestData();

      // Step 6: Wait for results processing
      console.log(
        "\n[INFO] Waiting 30 seconds for final results processing...",
      );
      await this.sleep(30000);

      // Step 7: Show results summary
      this.showResults();
    } catch (error) {
      console.error("\n[ERROR] Test failed:", error);
    } finally {
      // Cleanup
      await this.cleanup();
    }
  }

  /**
   * Clear old log files
   */
  private async clearLogs(): Promise<void> {
    console.log("\n[STEP 1] Clearing old log files...");

    const logFiles = [
      "approximation_approach_log.csv",
      "approximation_approach_resource_usage.csv",
      "fetching_client_side_log.csv",
      "fetching_client_side_resource_usage.csv",
    ];

    for (const logFile of logFiles) {
      try {
        if (fs.existsSync(logFile)) {
          fs.unlinkSync(logFile);
          console.log(`  Deleted: ${logFile}`);
        }
      } catch (error) {
        console.log(`  Could not delete ${logFile}: ${error}`);
      }
    }
    console.log("  Log files cleared.");
  }

  /**
   * Setup MQTT monitoring to capture results
   */
  private async setupMQTTMonitoring(): Promise<void> {
    console.log("\n[STEP 2] Setting up MQTT monitoring...");

    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: "test-monitor-" + Math.random().toString(16).substr(2, 8),
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("  Connected to MQTT broker");

        // Subscribe to result topics
        this.mqttClient!.subscribe(
          [APPROX_OUTPUT_TOPIC, FETCHING_OUTPUT_TOPIC],
          (err) => {
            if (err) {
              console.error("  Failed to subscribe to result topics:", err);
              reject(err);
            } else {
              console.log(`  Subscribed to result topics:`);
              console.log(`    - ${APPROX_OUTPUT_TOPIC}`);
              console.log(`    - ${FETCHING_OUTPUT_TOPIC}`);
              resolve();
            }
          },
        );
      });

      this.mqttClient.on("message", (topic, message) => {
        const timestamp = Date.now();
        const messageStr = message.toString();

        if (topic === APPROX_OUTPUT_TOPIC) {
          try {
            const result = JSON.parse(messageStr);
            this.approxResults.push({ timestamp, result });
            console.log(
              `\n[APPROX RESULT] Unified Result: ${result.unifiedResult}, Type: ${result.aggregationType}`,
            );
          } catch (e) {
            console.log(`\n[APPROX RESULT] ${messageStr.substring(0, 100)}`);
          }
        } else if (topic === FETCHING_OUTPUT_TOPIC) {
          try {
            // Parse RDF/Turtle format
            const valueMatch = messageStr.match(/hasValue>\s*"([^"]+)"/);
            if (valueMatch) {
              const value = parseFloat(valueMatch[1]);
              this.fetchingResults.push({ timestamp, value });
              console.log(`\n[FETCHING RESULT] Value: ${value}`);
            } else {
              console.log(
                `\n[FETCHING RESULT] ${messageStr.substring(0, 100)}`,
              );
            }
          } catch (e) {
            console.log(`\n[FETCHING RESULT] ${messageStr.substring(0, 100)}`);
          }
        }
      });

      this.mqttClient.on("error", (error) => {
        console.error("  MQTT Client Error:", error);
        reject(error);
      });

      // Timeout after 10 seconds
      setTimeout(() => {
        reject(new Error("MQTT connection timeout"));
      }, 10000);
    });
  }

  /**
   * Launch both orchestrator processes
   */
  private async launchOrchestrators(): Promise<void> {
    console.log("\n[STEP 3] Launching orchestrators...");

    // Launch Approximation Approach
    console.log("  Starting Approximation Approach...");
    const projectRoot = path.resolve(__dirname, "..");
    const approxPath = path.resolve(
      projectRoot,
      "src/approaches/ApproximationApproachOrchestrator.ts",
    );
    this.approxOrchestrator = spawn("npx", ["ts-node", approxPath], {
      env: { ...process.env, HTTP_PORT: "8081" },
    });

    this.approxOrchestrator.stdout?.on("data", (data) => {
      const lines = data
        .toString()
        .split("\n")
        .filter((l: string) => l.trim());
      lines.forEach((line: string) => console.log(`  [APPROX] ${line}`));
    });

    this.approxOrchestrator.stderr?.on("data", (data) => {
      console.error(`  [APPROX ERROR] ${data.toString().trim()}`);
    });

    // Launch Fetching Client Side Approach
    console.log("  Starting Fetching Client Side Approach...");
    const fetchingPath = path.resolve(
      projectRoot,
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
    );
    this.fetchingOrchestrator = spawn("npx", ["ts-node", fetchingPath]);

    this.fetchingOrchestrator.stdout?.on("data", (data) => {
      const lines = data
        .toString()
        .split("\n")
        .filter((l: string) => l.trim());
      lines.forEach((line: string) => console.log(`  [FETCHING] ${line}`));
    });

    this.fetchingOrchestrator.stderr?.on("data", (data) => {
      console.error(`  [FETCHING ERROR] ${data.toString().trim()}`);
    });

    console.log("  Both orchestrators launched.");
  }

  /**
   * Publish test data to MQTT topics
   */
  private async publishTestData(): Promise<void> {
    console.log("\n[STEP 4] Publishing test data...");
    console.log(`  Duration: ${EXPERIMENT_DURATION_S}s at ${DATA_RATE_HZ}Hz`);

    const totalEvents = EXPERIMENT_DURATION_S * DATA_RATE_HZ;
    const intervalMs = 1000 / DATA_RATE_HZ;

    for (let i = 0; i < totalEvents; i++) {
      const timestamp = new Date().toISOString();

      // Generate varying sensor values (simulating real sensor data)
      const wearableValue =
        10 + 5 * Math.sin((2 * Math.PI * i) / (DATA_RATE_HZ * 10));
      const smartphoneValue =
        15 + 3 * Math.cos((2 * Math.PI * i) / (DATA_RATE_HZ * 8));

      // Publish wearable data
      const wearableQuad = `<http://example.org/wearable/${i}> <https://saref.etsi.org/core/hasValue> "${wearableValue.toFixed(2)}"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.org/wearable/${i}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/wearable/${i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Sensor> .
<http://example.org/wearable/${i}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .`;

      // Publish smartphone data
      const smartphoneQuad = `<http://example.org/smartphone/${i}> <https://saref.etsi.org/core/hasValue> "${smartphoneValue.toFixed(2)}"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://example.org/smartphone/${i}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://example.org/smartphone/${i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Sensor> .
<http://example.org/smartphone/${i}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/smartphoneX> .`;

      this.mqttClient!.publish(WEARABLE_TOPIC, wearableQuad, { qos: 1 });
      this.mqttClient!.publish(SMARTPHONE_TOPIC, smartphoneQuad, { qos: 1 });

      this.dataPublishCount += 2;

      // Progress indicator every 10 seconds
      if ((i + 1) % (DATA_RATE_HZ * 10) === 0) {
        console.log(
          `  Published ${this.dataPublishCount} data points (${i + 1}/${totalEvents} iterations, ${(((i + 1) / totalEvents) * 100).toFixed(1)}%)`,
        );
      }

      await this.sleep(intervalMs);
    }

    console.log(
      `  Data publishing complete: ${this.dataPublishCount} total data points published.`,
    );
  }

  /**
   * Show results summary
   */
  private showResults(): void {
    console.log("\n" + "=".repeat(60));
    console.log("RESULTS SUMMARY");
    console.log("=".repeat(60));

    console.log(`\nData Published: ${this.dataPublishCount} data points`);

    console.log(
      `\nApproximation Approach Results: ${this.approxResults.length}`,
    );
    if (this.approxResults.length > 0) {
      console.log("  Sample results:");
      this.approxResults.slice(0, 5).forEach((r, i) => {
        console.log(
          `    ${i + 1}. Unified Result: ${r.result.unifiedResult}, Type: ${r.result.aggregationType}`,
        );
      });
      if (this.approxResults.length > 5) {
        console.log(
          `    ... and ${this.approxResults.length - 5} more results`,
        );
      }
    } else {
      console.log("  ⚠️  NO RESULTS RECEIVED - Check logs for errors");
    }

    console.log(
      `\nFetching Client Side Approach Results: ${this.fetchingResults.length}`,
    );
    if (this.fetchingResults.length > 0) {
      console.log("  Sample results:");
      this.fetchingResults.slice(0, 5).forEach((r, i) => {
        console.log(`    ${i + 1}. Value: ${r.value}`);
      });
      if (this.fetchingResults.length > 5) {
        console.log(
          `    ... and ${this.fetchingResults.length - 5} more results`,
        );
      }
    } else {
      console.log("  ⚠️  NO RESULTS RECEIVED - Check logs for errors");
    }

    console.log("\nLog Files:");
    const logFiles = [
      "approximation_approach_log.csv",
      "approximation_approach_resource_usage.csv",
      "fetching_client_side_log.csv",
      "fetching_client_side_resource_usage.csv",
    ];

    logFiles.forEach((logFile) => {
      if (fs.existsSync(logFile)) {
        const stats = fs.statSync(logFile);
        const sizeKB = (stats.size / 1024).toFixed(2);
        console.log(`  - ${logFile}: ${sizeKB} KB`);
      } else {
        console.log(`  - ${logFile}: NOT CREATED`);
      }
    });

    console.log("\n" + "=".repeat(60));
  }

  /**
   * Cleanup resources
   */
  private async cleanup(): Promise<void> {
    console.log("\n[CLEANUP] Shutting down...");

    if (this.approxOrchestrator) {
      console.log("  Stopping Approximation Approach...");
      this.approxOrchestrator.kill("SIGTERM");
    }

    if (this.fetchingOrchestrator) {
      console.log("  Stopping Fetching Client Side Approach...");
      this.fetchingOrchestrator.kill("SIGTERM");
    }

    if (this.mqttClient) {
      console.log("  Disconnecting MQTT client...");
      this.mqttClient.end();
    }

    console.log("  Cleanup complete.");
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Run the test
if (require.main === module) {
  const tester = new ApproachTester();
  tester
    .run()
    .then(() => {
      console.log("\n✅ Test completed successfully!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("\n❌ Test failed:", error);
      process.exit(1);
    });
}

export { ApproachTester };
