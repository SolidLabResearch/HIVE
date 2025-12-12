#!/usr/bin/env ts-node

/**
 * Single Run Test Script for Debugging
 * Runs one iteration with full verbose output to diagnose issues
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as path from "path";
import * as fs from "fs";
import { StreamToMQTT } from "../src/streamer/src/publishing/StreamToMQTT";

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

// Test parameters
const DATA_RATE_HZ = 4;
const EXPERIMENT_DURATION_S = 30; // Shorter duration for testing
const INIT_WAIT_S = 10;
const FINAL_WAIT_S = 15;

class SingleRunDebugger {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private resultCounts: Map<string, number> = new Map();
  private wearablePublisher: StreamToMQTT | null = null;
  private smartphonePublisher: StreamToMQTT | null = null;

  constructor() {
    this.resultCounts.set("approximation", 0);
    this.resultCounts.set("chunked", 0);
    this.resultCounts.set("fetching", 0);
  }

  async run(): Promise<void> {
    console.log("\n" + "=".repeat(70));
    console.log("SINGLE RUN DEBUG TEST");
    console.log("=".repeat(70));

    try {
      console.log("\n[1/6] Clearing previous MQTT data...");
      await this.clearPreviousData();

      console.log("\n[2/6] Setting up MQTT monitoring...");
      await this.setupMQTTMonitoring();

      console.log("\n[3/6] Launching orchestrators...");
      await this.launchOrchestrators();

      console.log(
        `\n[4/6] Waiting ${INIT_WAIT_S}s for orchestrator initialization...`,
      );
      await this.sleep(INIT_WAIT_S * 1000);

      console.log("\n[5/6] Publishing test data...");
      await this.publishTestData();

      console.log(`\n[6/6] Waiting ${FINAL_WAIT_S}s for final results...`);
      await this.sleep(FINAL_WAIT_S * 1000);

      this.printResults();
    } catch (error) {
      console.error("\n[ERROR] Test failed:", error);
    } finally {
      await this.cleanup();
    }
  }

  private async clearPreviousData(): Promise<void> {
    return new Promise<void>((resolve) => {
      const clearClient = mqtt.connect(MQTT_BROKER, {
        clientId: `clearer-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      clearClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
          OUTPUT_TOPICS.fetching,
        ];

        topics.forEach((topic) => {
          clearClient.publish(topic, "", { qos: 1, retain: true });
        });

        console.log("  ✓ Cleared retained messages from output topics");
        clearClient.end(false, {}, () => {
          setTimeout(resolve, 1000);
        });
      });
    });
  }

  private async setupMQTTMonitoring(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: `monitor-debug-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("  ✓ Connected to MQTT broker");

        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
          OUTPUT_TOPICS.fetching,
        ];

        this.mqttClient!.subscribe(topics, { qos: 1 }, (err) => {
          if (err) {
            console.error("  ✗ Failed to subscribe to topics:", err);
            reject(err);
          } else {
            console.log(`  ✓ Subscribed to ${topics.length} output topics`);
            resolve();
          }
        });
      });

      this.mqttClient.on("error", (err) => {
        console.error("  ✗ MQTT error:", err);
      });

      this.mqttClient.on("message", (topic: string, message: Buffer) => {
        const content = message.toString();
        if (!content || content.trim() === "") return;

        let approachName = "unknown";
        if (topic === OUTPUT_TOPICS.approximation) {
          approachName = "approximation";
        } else if (topic === OUTPUT_TOPICS.chunked) {
          approachName = "chunked";
        } else if (topic === OUTPUT_TOPICS.fetching) {
          approachName = "fetching";
        }

        const count = (this.resultCounts.get(approachName) || 0) + 1;
        this.resultCounts.set(approachName, count);
        console.log(`  📊 [${approachName.toUpperCase()}] Result #${count}`);

        // Log first result content for debugging
        if (count === 1) {
          console.log(
            `      First result preview: ${content.substring(0, 100)}...`,
          );
        }
      });
    });
  }

  private async launchOrchestrators(): Promise<void> {
    await this.launchOrchestrator(
      "approximation",
      "../src/approaches/ApproximationApproachOrchestrator.ts",
      { HTTP_PORT: "8081" },
    );

    await this.launchOrchestrator(
      "chunked",
      "../src/approaches/ChunkedQueryApproachOrchestrator.ts",
      { HTTP_PORT: "8082" },
    );

    await this.launchOrchestrator(
      "fetching",
      "../src/approaches/FetchingClientSideApproachOrchestrator.ts",
      { HTTP_PORT: "8083" },
    );
  }

  private launchOrchestrator(
    name: string,
    scriptPath: string,
    env: Record<string, string>,
  ): Promise<void> {
    return new Promise((resolve) => {
      const fullPath = path.resolve(__dirname, scriptPath);
      console.log(`  → Launching ${name}...`);
      console.log(`    Path: ${fullPath}`);
      console.log(`    Env: ${JSON.stringify(env)}`);

      if (!fs.existsSync(fullPath)) {
        console.error(`    ✗ File not found: ${fullPath}`);
        resolve();
        return;
      }

      const proc = spawn("npx", ["ts-node", fullPath], {
        env: { ...process.env, ...env },
        stdio: ["pipe", "pipe", "pipe"],
      });

      this.orchestrators.set(name, proc);
      console.log(`    ✓ Process spawned (PID: ${proc.pid})`);

      proc.stdout?.on("data", (data: Buffer) => {
        const lines = data.toString().split("\n");
        lines.forEach((line) => {
          if (line.trim()) {
            console.log(`    [${name}] ${line.trim()}`);
          }
        });
      });

      proc.stderr?.on("data", (data: Buffer) => {
        const msg = data.toString().trim();
        if (msg && !msg.includes("ExperimentalWarning")) {
          console.error(`    [${name} ERR] ${msg}`);
        }
      });

      proc.on("error", (err) => {
        console.error(`    ✗ [${name}] Process error: ${err.message}`);
      });

      proc.on("exit", (code, signal) => {
        if (code !== null && code !== 0) {
          console.error(`    ✗ [${name}] Exited with code ${code}`);
        } else if (signal) {
          console.error(`    ✗ [${name}] Killed by signal ${signal}`);
        }
      });

      setTimeout(resolve, 2000);
    });
  }

  private async publishTestData(): Promise<void> {
    const projectRoot = path.resolve(__dirname, "..");
    const wearableDataPath = path.join(
      projectRoot,
      "src/streamer/data/wearable.acceleration.x/data.nt",
    );
    const smartphoneDataPath = path.join(
      projectRoot,
      "src/streamer/data/smartphone.acceleration.x/data.nt",
    );

    console.log(`  → Wearable data: ${wearableDataPath}`);
    console.log(`  → Smartphone data: ${smartphoneDataPath}`);

    if (!fs.existsSync(wearableDataPath)) {
      throw new Error(`Wearable data file not found: ${wearableDataPath}`);
    }
    if (!fs.existsSync(smartphoneDataPath)) {
      throw new Error(`Smartphone data file not found: ${smartphoneDataPath}`);
    }

    console.log("  ✓ Data files exist");

    // Check orchestrator status before publishing
    let runningCount = 0;
    for (const [name, proc] of this.orchestrators) {
      if (proc.exitCode === null) {
        runningCount++;
        console.log(`  ✓ [${name}] Still running`);
      } else {
        console.warn(`  ✗ [${name}] Already exited with code ${proc.exitCode}`);
      }
    }
    console.log(
      `  → ${runningCount}/${this.orchestrators.size} orchestrators running`,
    );

    if (runningCount === 0) {
      console.error(
        "\n  ⚠️  WARNING: No orchestrators are running! Skipping data publish.",
      );
      return;
    }

    console.log("\n  → Starting data replay...");

    const wearableClientId = `pub-wearable-debug-${Math.random().toString(16).substr(2, 8)}`;
    const smartphoneClientId = `pub-smartphone-debug-${Math.random().toString(16).substr(2, 8)}`;

    this.wearablePublisher = new StreamToMQTT(
      MQTT_BROKER,
      DATA_RATE_HZ,
      wearableDataPath,
      WEARABLE_TOPIC,
      { clientId: wearableClientId, clean: true },
    );

    this.smartphonePublisher = new StreamToMQTT(
      MQTT_BROKER,
      DATA_RATE_HZ,
      smartphoneDataPath,
      SMARTPHONE_TOPIC,
      { clientId: smartphoneClientId, clean: true },
    );

    console.log(
      `  → Publishing at ${DATA_RATE_HZ}Hz for ~${EXPERIMENT_DURATION_S}s`,
    );

    await Promise.all([
      this.wearablePublisher.replay_streams(),
      this.smartphonePublisher.replay_streams(),
    ]);

    console.log("  ✓ Data replay completed");
  }

  private printResults(): void {
    console.log("\n" + "=".repeat(70));
    console.log("TEST RESULTS");
    console.log("=".repeat(70));

    const approxCount = this.resultCounts.get("approximation") || 0;
    const chunkedCount = this.resultCounts.get("chunked") || 0;
    const fetchingCount = this.resultCounts.get("fetching") || 0;

    console.log(
      `\n  Approximation: ${approxCount} results ${approxCount > 0 ? "✓" : "✗"}`,
    );
    console.log(
      `  Chunked:       ${chunkedCount} results ${chunkedCount > 0 ? "✓" : "✗"}`,
    );
    console.log(
      `  Fetching:      ${fetchingCount} results ${fetchingCount > 0 ? "✓" : "✗"}`,
    );

    console.log("\n  Orchestrator Status:");
    for (const [name, proc] of this.orchestrators) {
      const status =
        proc.exitCode === null ? "Running ✓" : `Exited (${proc.exitCode}) ✗`;
      console.log(`    ${name.padEnd(15)}: ${status}`);
    }

    console.log("\n" + "=".repeat(70));
  }

  private async cleanup(): Promise<void> {
    console.log("\n[CLEANUP] Shutting down...");

    for (const [name, proc] of this.orchestrators) {
      try {
        if (proc.exitCode === null) {
          console.log(`  → Killing ${name}...`);
          proc.kill("SIGTERM");
          await this.sleep(500);
          if (!proc.killed) {
            proc.kill("SIGKILL");
          }
        }
      } catch (e) {
        // Ignore
      }
    }

    if (this.mqttClient) {
      this.mqttClient.end(true);
    }

    console.log("  ✓ Cleanup complete");
    await this.sleep(1000);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Run the debug test
const tester = new SingleRunDebugger();
tester
  .run()
  .then(() => {
    console.log("\n[INFO] Debug test complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("\n[ERROR] Debug test failed:", err);
    process.exit(1);
  });
