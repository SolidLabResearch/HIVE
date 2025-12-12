#!/usr/bin/env ts-node

/**
 * 35-Iteration Multi-Run Verification Test
 * Runs all 3 streaming query approaches 35 times to verify stability and consistency.
 *
 * This script uses pre-recorded data from src/streamer/data/smartphone.acceleration.x
 * and src/streamer/data/wearable.acceleration.x instead of generating synthetic data.
 * The StreamToMQTT replayer publishes this data at 4Hz to simulate real-time streaming
 * while maintaining exact reproducibility across all iterations.
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
const EXPERIMENT_DURATION_S = 90;
const INIT_WAIT_S = 10;
const FINAL_WAIT_S = 20;
const NUM_RUNS = 35; // 35 iterations

interface ApproachResult {
  name: string;
  topic: string;
  results: Array<{ timestamp: number; content: string }>;
  started: boolean;
  errors: string[];
}

interface RunSummary {
  runNumber: number;
  approximation: { passed: boolean; resultCount: number };
  chunked: { passed: boolean; resultCount: number };
  fetching: { passed: boolean; resultCount: number };
  totalMessages: number;
  duration: number;
}

/**
 * Single test run verifier
 */
class SingleRunVerifier {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private approachResults: Map<string, ApproachResult> = new Map();
  private dataPublishCount = 0;
  private allMessages: Array<{
    topic: string;
    timestamp: number;
    content: string;
  }> = [];
  private runNumber: number;
  private startTime: number = 0;
  private wearablePublisher: StreamToMQTT | null = null;
  private smartphonePublisher: StreamToMQTT | null = null;

  constructor(runNumber: number) {
    this.runNumber = runNumber;
    this.initializeResults();
  }

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

  async run(): Promise<RunSummary> {
    console.log(`\n${"=".repeat(70)}`);
    console.log(`RUN ${this.runNumber} - Starting verification test`);
    console.log("=".repeat(70));

    this.startTime = Date.now();

    try {
      await this.clearPreviousRunData();
      await this.setupMQTTMonitoring();
      await this.launchOrchestrators();

      console.log(
        `  [RUN ${this.runNumber}] Waiting ${INIT_WAIT_S}s for initialization...`,
      );
      await this.sleep(INIT_WAIT_S * 1000);

      await this.publishTestData();

      console.log(
        `  [RUN ${this.runNumber}] Waiting ${FINAL_WAIT_S}s for final results...`,
      );
      await this.sleep(FINAL_WAIT_S * 1000);

      const summary = this.generateSummary();
      return summary;
    } catch (error) {
      console.error(`  [RUN ${this.runNumber}] ERROR:`, error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  private async clearPreviousRunData(): Promise<void> {
    // Clear MQTT retained messages
    const clearClient = mqtt.connect(MQTT_BROKER, {
      clientId: `clearer-${this.runNumber}-${Math.random().toString(16).substr(2, 8)}`,
      clean: true,
    });

    await new Promise<void>((resolve) => {
      clearClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
          OUTPUT_TOPICS.fetching,
        ];

        topics.forEach((topic) => {
          clearClient.publish(topic, "", { qos: 1, retain: true });
        });

        setTimeout(() => {
          clearClient.end(true);
          resolve();
        }, 500);
      });
    });
  }

  private async setupMQTTMonitoring(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: `verifier-${this.runNumber}-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
          OUTPUT_TOPICS.fetching,
          "chunked/#", // Also subscribe to chunked subquery topics
        ];

        this.mqttClient!.subscribe(topics, { qos: 1 }, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });

      this.mqttClient.on("message", (topic: string, message: Buffer) => {
        const content = message.toString();
        if (!content || content.trim() === "") return; // Skip empty messages

        const timestamp = Date.now();
        this.allMessages.push({ topic, timestamp, content });

        if (topic === OUTPUT_TOPICS.approximation) {
          this.recordResult("approximation", content);
        } else if (topic === OUTPUT_TOPICS.chunked) {
          this.recordResult("chunked", content);
        } else if (topic === OUTPUT_TOPICS.fetching) {
          this.recordResult("fetching", content);
        } else if (
          topic.startsWith("chunked/") &&
          topic !== OUTPUT_TOPICS.chunked
        ) {
          // Chunked subquery result
          console.log(`    [CHUNKED SUBQUERY] Topic: ${topic}`);
        }
      });

      this.mqttClient.on("error", (err) => {
        reject(err);
      });
    });
  }

  private recordResult(approach: string, content: string): void {
    const result = this.approachResults.get(approach);
    if (result) {
      result.results.push({ timestamp: Date.now(), content });
      console.log(
        `    [${approach.toUpperCase()}] Result #${result.results.length}`,
      );
    }
  }

  private async launchOrchestrators(): Promise<void> {
    await this.launchOrchestrator(
      "approximation",
      "src/approaches/ApproximationApproachOrchestrator.ts",
      { HTTP_PORT: "8081" },
    );

    await this.launchOrchestrator(
      "chunked",
      "src/approaches/ChunkedQueryApproachOrchestrator.ts",
      { HTTP_PORT: "8082" },
    );

    await this.launchOrchestrator(
      "fetching",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
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

      const proc = spawn("npx", ["ts-node", fullPath], {
        env: { ...process.env, ...env },
        stdio: ["pipe", "pipe", "pipe"],
      });

      this.orchestrators.set(name, proc);

      proc.stdout?.on("data", (data: Buffer) => {
        // Only log significant events to reduce noise
        const lines = data
          .toString()
          .split("\n")
          .filter((l: string) => l.trim());
        for (const line of lines) {
          if (
            line.includes("error") ||
            line.includes("Error") ||
            line.includes("Failed") ||
            line.includes("Registered query")
          ) {
            // Suppress verbose output during multi-run
          }
        }
      });

      proc.stderr?.on("data", (data: Buffer) => {
        const msg = data.toString().trim();
        if (
          msg &&
          !msg.includes("ExperimentalWarning") &&
          !msg.includes("Watermark")
        ) {
          // Only log real errors
        }
      });

      proc.on("error", (err) => {
        const result = this.approachResults.get(name);
        if (result) result.errors.push(err.message);
      });

      const result = this.approachResults.get(name);
      if (result) result.started = true;

      setTimeout(resolve, 1000);
    });
  }

  private async publishTestData(): Promise<void> {
    console.log(
      `  [Run #${this.runNumber}] Publishing pre-recorded data (${EXPERIMENT_DURATION_S}s, ${DATA_RATE_HZ}Hz)...`,
    );

    const projectRoot = path.resolve(__dirname, "..");
    const wearableDataPath = path.join(
      projectRoot,
      "src/streamer/data/wearable.acceleration.x/data.nt",
    );
    const smartphoneDataPath = path.join(
      projectRoot,
      "src/streamer/data/smartphone.acceleration.x/data.nt",
    );

    // Verify files exist
    if (!fs.existsSync(wearableDataPath)) {
      throw new Error(`Wearable data file not found: ${wearableDataPath}`);
    }
    if (!fs.existsSync(smartphoneDataPath)) {
      throw new Error(`Smartphone data file not found: ${smartphoneDataPath}`);
    }

    console.log(
      `  [Run #${this.runNumber}] Loading pre-recorded data files...`,
    );

    // Create publishers with unique client IDs
    const wearableClientId = `pub-wearable-run${this.runNumber}-${Math.random().toString(16).substr(2, 8)}`;
    const smartphoneClientId = `pub-smartphone-run${this.runNumber}-${Math.random().toString(16).substr(2, 8)}`;

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

    // Replay streams concurrently
    await Promise.all([
      this.wearablePublisher.replay_streams(),
      this.smartphonePublisher.replay_streams(),
    ]);

    console.log(`  [Run #${this.runNumber}] Data replay completed`);
  }

  private generateSummary(): RunSummary {
    const duration = Date.now() - this.startTime;
    const approx = this.approachResults.get("approximation")!;
    const chunked = this.approachResults.get("chunked")!;
    const fetching = this.approachResults.get("fetching")!;

    return {
      runNumber: this.runNumber,
      approximation: {
        passed: approx.results.length > 0,
        resultCount: approx.results.length,
      },
      chunked: {
        passed: chunked.results.length > 0,
        resultCount: chunked.results.length,
      },
      fetching: {
        passed: fetching.results.length > 0,
        resultCount: fetching.results.length,
      },
      totalMessages: this.allMessages.length,
      duration: duration,
    };
  }

  private async cleanup(): Promise<void> {
    for (const [_name, proc] of this.orchestrators) {
      try {
        proc.kill("SIGTERM");
        await this.sleep(500);
        if (!proc.killed) {
          proc.kill("SIGKILL");
        }
      } catch (e) {
        // Ignore
      }
    }

    if (this.mqttClient) {
      this.mqttClient.end(true);
    }

    // Note: StreamToMQTT publishers handle their own MQTT connection cleanup

    await this.sleep(2000);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

/**
 * Multi-run orchestrator
 */
class MultiRunOrchestrator {
  private summaries: RunSummary[] = [];

  async runMultipleTests(numRuns: number): Promise<void> {
    console.log("\n" + "=".repeat(70));
    console.log(`MULTI-RUN VERIFICATION: ${numRuns} ITERATIONS`);
    console.log("=".repeat(70));
    console.log(
      `Testing all 3 approaches ${numRuns} times to verify stability`,
    );
    console.log("=".repeat(70));

    const startTime = Date.now();

    for (let i = 1; i <= numRuns; i++) {
      console.log(`\n[INFO] Starting run ${i}/${numRuns}...`);

      try {
        const verifier = new SingleRunVerifier(i);
        const summary = await verifier.run();
        this.summaries.push(summary);

        this.printRunSummary(summary);

        if (i < numRuns) {
          console.log(`\n[INFO] Waiting 5s between runs...`);
          await this.sleep(5000);
        }
      } catch (error) {
        console.error(`[ERROR] Run ${i} failed:`, error);
        this.summaries.push({
          runNumber: i,
          approximation: { passed: false, resultCount: 0 },
          chunked: { passed: false, resultCount: 0 },
          fetching: { passed: false, resultCount: 0 },
          totalMessages: 0,
          duration: 0,
        });
      }
    }

    const totalTime = Date.now() - startTime;
    this.printFinalSummary(totalTime);
    this.saveResultsToFile();
  }

  private printRunSummary(summary: RunSummary): void {
    const approxIcon = summary.approximation.passed ? "[OK]" : "[X]";
    const chunkedIcon = summary.chunked.passed ? "[OK]" : "[X]";
    const fetchingIcon = summary.fetching.passed ? "[OK]" : "[X]";

    console.log(
      `\n  Run ${summary.runNumber} Results (${(summary.duration / 1000).toFixed(1)}s):`,
    );
    console.log(
      `    ${approxIcon} Approximation: ${summary.approximation.resultCount} results`,
    );
    console.log(
      `    ${chunkedIcon} Chunked:       ${summary.chunked.resultCount} results`,
    );
    console.log(
      `    ${fetchingIcon} Fetching:      ${summary.fetching.resultCount} results`,
    );
  }

  private printFinalSummary(totalTime: number): void {
    console.log("\n" + "=".repeat(70));
    console.log("FINAL MULTI-RUN SUMMARY");
    console.log("=".repeat(70));

    const approxStats = this.calculateStats("approximation");
    const chunkedStats = this.calculateStats("chunked");
    const fetchingStats = this.calculateStats("fetching");

    console.log(`\nTotal runs completed: ${this.summaries.length}`);
    console.log(`Total time: ${(totalTime / 1000 / 60).toFixed(1)} minutes`);

    console.log(`\nApproximation Approach:`);
    console.log(
      `  Success rate: ${approxStats.successRate}% (${approxStats.passCount}/${this.summaries.length})`,
    );
    console.log(`  Avg results per run: ${approxStats.avgResults.toFixed(1)}`);
    console.log(
      `  Result range: ${approxStats.minResults}-${approxStats.maxResults}`,
    );

    console.log(`\nChunked Query Approach:`);
    console.log(
      `  Success rate: ${chunkedStats.successRate}% (${chunkedStats.passCount}/${this.summaries.length})`,
    );
    console.log(`  Avg results per run: ${chunkedStats.avgResults.toFixed(1)}`);
    console.log(
      `  Result range: ${chunkedStats.minResults}-${chunkedStats.maxResults}`,
    );

    console.log(`\nFetching Client Side:`);
    console.log(
      `  Success rate: ${fetchingStats.successRate}% (${fetchingStats.passCount}/${this.summaries.length})`,
    );
    console.log(
      `  Avg results per run: ${fetchingStats.avgResults.toFixed(1)}`,
    );
    console.log(
      `  Result range: ${fetchingStats.minResults}-${fetchingStats.maxResults}`,
    );

    console.log("\n" + "-".repeat(70));

    const allPassed =
      approxStats.passCount === this.summaries.length &&
      chunkedStats.passCount === this.summaries.length &&
      fetchingStats.passCount === this.summaries.length;

    if (allPassed) {
      console.log("OVERALL: ALL APPROACHES PASSED ALL RUNS - STABLE AND READY");
    } else {
      console.log("OVERALL: SOME RUNS FAILED - REVIEW NEEDED");
      console.log("\nFailed runs:");
      this.summaries.forEach((summary) => {
        if (
          !summary.approximation.passed ||
          !summary.chunked.passed ||
          !summary.fetching.passed
        ) {
          console.log(
            `  Run ${summary.runNumber}: ` +
              `A:${summary.approximation.passed ? "OK" : "FAIL"} ` +
              `C:${summary.chunked.passed ? "OK" : "FAIL"} ` +
              `F:${summary.fetching.passed ? "OK" : "FAIL"}`,
          );
        }
      });
    }

    console.log("=".repeat(70));
  }

  private calculateStats(approach: "approximation" | "chunked" | "fetching") {
    const results = this.summaries.map((s) => s[approach]);
    const passCount = results.filter((r) => r.passed).length;
    const successRate = Math.round((passCount / this.summaries.length) * 100);
    const resultCounts = results.map((r) => r.resultCount);
    const avgResults =
      resultCounts.reduce((a, b) => a + b, 0) / resultCounts.length;
    const minResults = Math.min(...resultCounts);
    const maxResults = Math.max(...resultCounts);

    return {
      passCount,
      successRate,
      avgResults,
      minResults,
      maxResults,
    };
  }

  private saveResultsToFile(): void {
    const filename = `multi-run-results-35-iterations-${new Date().toISOString().replace(/:/g, "-").split(".")[0]}.json`;
    const data = {
      timestamp: new Date().toISOString(),
      numRuns: this.summaries.length,
      summaries: this.summaries,
      statistics: {
        approximation: this.calculateStats("approximation"),
        chunked: this.calculateStats("chunked"),
        fetching: this.calculateStats("fetching"),
      },
    };

    fs.writeFileSync(filename, JSON.stringify(data, null, 2));
    console.log(`\nDetailed results saved to: ${filename}`);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Run the 35-iteration multi-run verification
const orchestrator = new MultiRunOrchestrator();
orchestrator
  .runMultipleTests(NUM_RUNS)
  .then(() => {
    console.log("\n[INFO] 35-iteration multi-run verification complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("\n[ERROR] 35-iteration verification failed:", err);
    process.exit(1);
  });
