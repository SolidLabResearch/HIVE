#!/usr/bin/env ts-node

/**
 * Quick 30-second test script to run both streaming approaches with test data
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as fs from "fs";

const MQTT_BROKER = "mqtt://localhost:1883";
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";
const APPROX_OUTPUT_TOPIC = "approximation/output";
const FETCHING_OUTPUT_TOPIC = "client_operation_output";

const DATA_RATE_HZ = 4; // 4Hz data rate
const EXPERIMENT_DURATION_S = 140; // 140 seconds to allow windows to trigger

class QuickApproachTester {
  private approxOrchestrator?: ChildProcess;
  private fetchingOrchestrator?: ChildProcess;
  private mqttClient?: mqtt.MqttClient;
  private approxResults: any[] = [];
  private fetchingResults: any[] = [];
  private dataPublishCount = 0;

  async run(): Promise<void> {
    console.log("=".repeat(60));
    console.log("Quick 140-Second Test of Streaming Query Approaches");
    console.log("=".repeat(60));

    try {
      await this.clearLogs();
      await this.setupMQTTMonitoring();
      await this.launchOrchestrators();

      console.log("\n[INFO] Waiting 10 seconds for initialization...");
      await this.sleep(10000);

      await this.publishTestData();

      console.log("\n[INFO] Waiting 30 seconds for results processing...");
      await this.sleep(30000);

      this.showResults();
    } catch (error) {
      console.error("\n[ERROR] Test failed:", error);
    } finally {
      await this.cleanup();
    }
  }

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
        }
      } catch (error) {
        // Ignore errors
      }
    }
    console.log("  Logs cleared.");
  }

  private async setupMQTTMonitoring(): Promise<void> {
    console.log("\n[STEP 2] Setting up MQTT monitoring...");

    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: "quick-test-" + Math.random().toString(16).substr(2, 8),
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("  ✓ Connected to MQTT broker");

        this.mqttClient!.subscribe(
          [APPROX_OUTPUT_TOPIC, FETCHING_OUTPUT_TOPIC],
          (err) => {
            if (err) {
              reject(err);
            } else {
              console.log(`  ✓ Subscribed to result topics`);
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
              `\n[✓ APPROX RESULT #${this.approxResults.length}] Unified: ${result.unifiedResult}, Type: ${result.aggregationType}`,
            );
          } catch (e) {
            this.approxResults.push({
              timestamp,
              raw: messageStr.substring(0, 50),
            });
          }
        } else if (topic === FETCHING_OUTPUT_TOPIC) {
          try {
            const valueMatch = messageStr.match(/hasValue>\s*"([^"]+)"/);
            if (valueMatch) {
              const value = parseFloat(valueMatch[1]);
              this.fetchingResults.push({ timestamp, value });
              console.log(
                `\n[✓ FETCHING RESULT #${this.fetchingResults.length}] Value: ${value}`,
              );
            }
          } catch (e) {
            this.fetchingResults.push({
              timestamp,
              raw: messageStr.substring(0, 50),
            });
          }
        }
      });

      setTimeout(() => reject(new Error("MQTT timeout")), 10000);
    });
  }

  private async launchOrchestrators(): Promise<void> {
    console.log("\n[STEP 3] Launching orchestrators...");

    this.approxOrchestrator = spawn(
      "npx",
      ["ts-node", "src/approaches/ApproximationApproachOrchestrator.ts"],
      {
        env: { ...process.env, HTTP_PORT: "8081" },
      },
    );

    this.fetchingOrchestrator = spawn("npx", [
      "ts-node",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
    ]);

    console.log("  ✓ Both orchestrators launched");
  }

  private async publishTestData(): Promise<void> {
    console.log("\n[STEP 4] Publishing test data for 140 seconds...");

    const totalEvents = EXPERIMENT_DURATION_S * DATA_RATE_HZ;
    const intervalMs = 1000 / DATA_RATE_HZ;

    for (let i = 0; i < totalEvents; i++) {
      const timestamp = new Date().toISOString();
      const wearableValue =
        10 + 5 * Math.sin((2 * Math.PI * i) / (DATA_RATE_HZ * 10));
      const smartphoneValue =
        15 + 3 * Math.cos((2 * Math.PI * i) / (DATA_RATE_HZ * 8));

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

      if ((i + 1) % (DATA_RATE_HZ * 10) === 0) {
        console.log(
          `  Progress: ${this.dataPublishCount} data points published (${(((i + 1) / totalEvents) * 100).toFixed(0)}%)`,
        );
      }

      await this.sleep(intervalMs);
    }

    console.log(`  ✓ Published ${this.dataPublishCount} total data points`);
  }

  private showResults(): void {
    console.log("\n" + "=".repeat(60));
    console.log("RESULTS SUMMARY");
    console.log("=".repeat(60));

    console.log(`\n📊 Data Published: ${this.dataPublishCount} data points`);

    console.log(
      `\n📈 Approximation Approach: ${this.approxResults.length} results received`,
    );
    if (this.approxResults.length > 0) {
      console.log("  Latest 3 results:");
      this.approxResults.slice(-3).forEach((r, i) => {
        if (r.result) {
          console.log(
            `    ${i + 1}. Unified: ${r.result.unifiedResult}, Type: ${r.result.aggregationType}`,
          );
        }
      });
    } else {
      console.log("  ⚠️  NO RESULTS - Check logs!");
    }

    console.log(
      `\n📈 Fetching Client Side: ${this.fetchingResults.length} results received`,
    );
    if (this.fetchingResults.length > 0) {
      console.log("  Latest 3 results:");
      this.fetchingResults.slice(-3).forEach((r, i) => {
        console.log(`    ${i + 1}. Value: ${r.value}`);
      });
    } else {
      console.log("  ⚠️  NO RESULTS - Check logs!");
    }

    console.log("\n📁 Log Files:");
    ["approximation_approach_log.csv", "fetching_client_side_log.csv"].forEach(
      (logFile) => {
        if (fs.existsSync(logFile)) {
          const stats = fs.statSync(logFile);
          const lines = fs.readFileSync(logFile, "utf8").split("\n").length - 1;
          console.log(
            `  ✓ ${logFile}: ${(stats.size / 1024).toFixed(2)} KB, ${lines} lines`,
          );
        } else {
          console.log(`  ✗ ${logFile}: NOT CREATED`);
        }
      },
    );

    console.log("\n" + "=".repeat(60));
  }

  private async cleanup(): Promise<void> {
    console.log("\n[CLEANUP] Shutting down...");

    if (this.approxOrchestrator) {
      this.approxOrchestrator.kill("SIGTERM");
    }

    if (this.fetchingOrchestrator) {
      this.fetchingOrchestrator.kill("SIGTERM");
    }

    if (this.mqttClient) {
      this.mqttClient.end();
    }

    console.log("  ✓ Cleanup complete");
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

if (require.main === module) {
  const tester = new QuickApproachTester();
  tester
    .run()
    .then(() => {
      console.log("\n✅ Quick test completed!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("\n❌ Test failed:", error);
      process.exit(1);
    });
}

export { QuickApproachTester };
