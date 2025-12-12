#!/usr/bin/env ts-node

/**
 * Comprehensive test script with full MQTT monitoring
 * This subscribes to ALL MQTT topics to see exactly what's being published
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as fs from "fs";

const MQTT_BROKER = "mqtt://localhost:1883";
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";

const DATA_RATE_HZ = 4;
const EXPERIMENT_DURATION_S = 140;

interface MQTTMessage {
  topic: string;
  timestamp: number;
  content: string;
}

class ComprehensiveTester {
  private approxOrchestrator?: ChildProcess;
  private fetchingOrchestrator?: ChildProcess;
  private mqttClient?: mqtt.MqttClient;
  private allMessages: MQTTMessage[] = [];
  private dataPublishCount = 0;
  private topicStats: Map<string, number> = new Map();

  async run(): Promise<void> {
    console.log("=".repeat(70));
    console.log("COMPREHENSIVE MQTT MONITORING TEST");
    console.log("=".repeat(70));
    console.log("This test subscribes to ALL MQTT topics (#) to capture everything");
    console.log("=".repeat(70));

    try {
      await this.clearLogs();
      await this.setupComprehensiveMQTTMonitoring();
      await this.launchOrchestrators();

      console.log("\n[WAIT] 10 seconds for orchestrators to initialize...");
      await this.sleep(10000);

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
    console.log("\n[STEP 1] Clearing logs...");
    const logFiles = [
      "approximation_approach_log.csv",
      "approximation_approach_resource_usage.csv",
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
    console.log("  ✓ Logs cleared");
  }

  private async setupComprehensiveMQTTMonitoring(): Promise<void> {
    console.log("\n[STEP 2] Setting up COMPREHENSIVE MQTT monitoring...");

    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: "comprehensive-test-" + Math.random().toString(16).substr(2, 8),
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("  ✓ Connected to MQTT broker");

        // Subscribe to EVERYTHING
        this.mqttClient!.subscribe("#", { qos: 2 }, (err) => {
          if (err) {
            console.error("  ✗ Failed to subscribe:", err);
            reject(err);
          } else {
            console.log("  ✓ Subscribed to ALL topics (#)");
            console.log("  ℹ️  Will capture every MQTT message");
            resolve();
          }
        });
      });

      this.mqttClient.on("message", (topic, message) => {
        const timestamp = Date.now();
        const content = message.toString();

        // Skip input data topics to reduce noise
        if (topic === WEARABLE_TOPIC || topic === SMARTPHONE_TOPIC) {
          // Count but don't log every data message
          this.topicStats.set(topic, (this.topicStats.get(topic) || 0) + 1);
          return;
        }

        // Store all non-data messages
        this.allMessages.push({ topic, timestamp, content });

        // Update topic stats
        this.topicStats.set(topic, (this.topicStats.get(topic) || 0) + 1);

        // Log interesting messages in real-time
        console.log(`\n[📨 MQTT MESSAGE]`);
        console.log(`  Topic: ${topic}`);
        console.log(`  Time: ${new Date(timestamp).toISOString()}`);

        // Try to parse and pretty-print the content
        try {
          const parsed = JSON.parse(content);
          console.log(`  Content (JSON):`);
          if (parsed.unifiedResult !== undefined) {
            console.log(`    🎯 Unified Result: ${parsed.unifiedResult}`);
            console.log(`    📊 Type: ${parsed.aggregationType}`);
            console.log(`    🪟 Window: ${parsed.window?.start} to ${parsed.window?.end}`);
            if (parsed.individualTopics) {
              console.log(`    📍 Individual Topics:`, parsed.individualTopics);
            }
          } else {
            console.log(`    ${JSON.stringify(parsed, null, 2).substring(0, 200)}`);
          }
        } catch (e) {
          // Not JSON, try RDF parsing
          const valueMatch = content.match(/hasValue>\s*"([^"]+)"/);
          if (valueMatch) {
            console.log(`  Content (RDF): hasValue="${valueMatch[1]}"`);
          } else {
            console.log(`  Content: ${content.substring(0, 150)}${content.length > 150 ? '...' : ''}`);
          }
        }
      });

      this.mqttClient.on("error", (error) => {
        console.error("  ✗ MQTT Error:", error);
        reject(error);
      });

      setTimeout(() => reject(new Error("MQTT connection timeout")), 10000);
    });
  }

  private async launchOrchestrators(): Promise<void> {
    console.log("\n[STEP 3] Launching orchestrators...");

    this.approxOrchestrator = spawn(
      "npx",
      ["ts-node", "src/approaches/ApproximationApproachOrchestrator.ts"],
      { env: { ...process.env, HTTP_PORT: "8081" } }
    );

    this.approxOrchestrator.stdout?.on("data", (data) => {
      const lines = data.toString().trim().split("\n");
      lines.forEach((line: string) => {
        if (line.includes("ERROR") || line.includes("result") || line.includes("published")) {
          console.log(`  [APPROX] ${line}`);
        }
      });
    });

    this.approxOrchestrator.stderr?.on("data", (data) => {
      console.error(`  [APPROX ERROR] ${data.toString().trim()}`);
    });

    this.fetchingOrchestrator = spawn("npx", [
      "ts-node",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
    ]);

    this.fetchingOrchestrator.stdout?.on("data", (data) => {
      const lines = data.toString().trim().split("\n");
      lines.forEach((line: string) => {
        if (line.includes("ERROR") || line.includes("result") || line.includes("published")) {
          console.log(`  [FETCHING] ${line}`);
        }
      });
    });

    this.fetchingOrchestrator.stderr?.on("data", (data) => {
      console.error(`  [FETCHING ERROR] ${data.toString().trim()}`);
    });

    console.log("  ✓ Both orchestrators launched");
  }

  private async publishTestData(): Promise<void> {
    console.log("\n[STEP 4] Publishing test data...");
    console.log(`  Duration: ${EXPERIMENT_DURATION_S}s at ${DATA_RATE_HZ}Hz per sensor`);

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

      if ((i + 1) % (DATA_RATE_HZ * 20) === 0) {
        console.log(`  Progress: ${((i + 1) / totalEvents * 100).toFixed(0)}% (${this.dataPublishCount} data points)`);
      }

      await this.sleep(intervalMs);
    }

    console.log(`  ✓ Published ${this.dataPublishCount} data points`);
  }

  private showComprehensiveResults(): void {
    console.log("\n" + "=".repeat(70));
    console.log("COMPREHENSIVE RESULTS");
    console.log("=".repeat(70));

    console.log(`\n📊 Data Published: ${this.dataPublishCount} data points`);

    console.log(`\n📨 MQTT Messages Captured: ${this.allMessages.length} non-data messages`);

    console.log(`\n📈 Topic Statistics:`);
    const sortedTopics = Array.from(this.topicStats.entries()).sort((a, b) => b[1] - a[1]);
    sortedTopics.forEach(([topic, count]) => {
      console.log(`  ${topic.padEnd(40)} : ${count} messages`);
    });

    if (this.allMessages.length > 0) {
      console.log(`\n📋 All Non-Data Messages (${this.allMessages.length} total):`);
      this.allMessages.forEach((msg, idx) => {
        console.log(`\n  Message ${idx + 1}:`);
        console.log(`    Topic: ${msg.topic}`);
        console.log(`    Time: ${new Date(msg.timestamp).toISOString()}`);
        console.log(`    Content: ${msg.content.substring(0, 150)}${msg.content.length > 150 ? '...' : ''}`);
      });
    } else {
      console.log(`\n⚠️  NO NON-DATA MESSAGES CAPTURED`);
      console.log(`   This means the approaches may not be publishing results to MQTT`);
    }

    console.log(`\n📁 Log Files Analysis:`);
    const logFiles = [
      "approximation_approach_log.csv",
      "fetching_client_side_log.csv",
    ];

    logFiles.forEach((logFile) => {
      if (fs.existsSync(logFile)) {
        const content = fs.readFileSync(logFile, "utf8");
        const lines = content.split("\n");
        const stats = fs.statSync(logFile);

        console.log(`\n  ${logFile}:`);
        console.log(`    Size: ${(stats.size / 1024).toFixed(2)} KB`);
        console.log(`    Lines: ${lines.length - 1}`);

        // Count specific events
        const resultCount = content.match(/result/gi)?.length || 0;
        const publishedCount = content.match(/published/gi)?.length || 0;
        const errorCount = content.match(/error/gi)?.length || 0;

        console.log(`    Results mentioned: ${resultCount}`);
        console.log(`    Published mentioned: ${publishedCount}`);
        console.log(`    Errors mentioned: ${errorCount}`);

        // Show last few log entries
        const lastEntries = lines.slice(-5).filter(l => l.trim());
        if (lastEntries.length > 0) {
          console.log(`    Last entries:`);
          lastEntries.forEach(entry => {
            const shortened = entry.length > 100 ? entry.substring(0, 100) + '...' : entry;
            console.log(`      ${shortened}`);
          });
        }
      } else {
        console.log(`\n  ${logFile}: NOT CREATED ❌`);
      }
    });

    console.log("\n" + "=".repeat(70));
    console.log("DIAGNOSIS:");
    console.log("=".repeat(70));

    if (this.allMessages.length === 0) {
      console.log("❌ No result messages were published to MQTT");
      console.log("   Possible causes:");
      console.log("   1. Orchestrators not processing data correctly");
      console.log("   2. BeeWorker processes not spawning");
      console.log("   3. Window sizes too large for test duration");
      console.log("   4. Query operators not initializing");
    } else {
      console.log("✅ Result messages were captured!");
      console.log("   Check the messages above to verify correctness");
    }

    console.log("\n" + "=".repeat(70));
  }

  private async cleanup(): Promise<void> {
    console.log("\n[CLEANUP] Shutting down...");

    if (this.approxOrchestrator) {
      this.approxOrchestrator.kill("SIGTERM");
      console.log("  ✓ Stopped Approximation Approach");
    }

    if (this.fetchingOrchestrator) {
      this.fetchingOrchestrator.kill("SIGTERM");
      console.log("  ✓ Stopped Fetching Client Side Approach");
    }

    if (this.mqttClient) {
      this.mqttClient.end();
      console.log("  ✓ Disconnected MQTT client");
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

if (require.main === module) {
  const tester = new ComprehensiveTester();
  tester
    .run()
    .then(() => {
      console.log("\n✅ Comprehensive test completed!");
      console.log("Check the output above for detailed MQTT message analysis.");
      process.exit(0);
    })
    .catch((error) => {
      console.error("\n❌ Test failed:", error);
      process.exit(1);
    });
}

export { ComprehensiveTester };
