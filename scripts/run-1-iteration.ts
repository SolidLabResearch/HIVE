#!/usr/bin/env ts-node

/**
 * Single Iteration Experiment Script
 * Runs one iteration of all three approaches and saves results to CSV files
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as path from "path";
import * as fs from "fs";
import { StreamToMQTT } from "../src/streamer/src/publishing/StreamToMQTT";

// ============================================================================
// CONFIGURATION
// ============================================================================

const MQTT_BROKER = "mqtt://localhost:1883";

// Input topics
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";

// Output topics for each approach
const OUTPUT_TOPICS = {
  approximation: "approximation/output",
  chunked: "chunked/output", // Aggregated results from StreamingQueryChunkAggregatorOperator
  fetching: "client_operation_output",
};

// Test parameters
const DATA_RATE_HZ = 4;
const EXPERIMENT_DURATION_S = 90;
const INIT_WAIT_S = 20;
const FINAL_WAIT_S = 30;

// Output files
const RESULTS_DIR = path.join(__dirname, "../results");
const FETCHING_FILE = path.join(
  RESULTS_DIR,
  "fetching_client_side_results.csv",
);
const CHUNKED_FILE = path.join(RESULTS_DIR, "chunked_query_results.csv");
const APPROXIMATION_FILE = path.join(RESULTS_DIR, "approximation_results.csv");

// ============================================================================
// INTERFACES
// ============================================================================

interface ResultRecord {
  queryRegisteredTimestamp: number;
  resultTimestamp: number;
  result: number;
}

interface ApproachData {
  name: string;
  topic: string;
  results: ResultRecord[];
  started: number;
  errors: number;
}

// ============================================================================
// MAIN CLASS
// ============================================================================

class SingleIterationRunner {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private approachResults: Map<string, ApproachData> = new Map();
  private dataPublishCount = 0;
  private startTime = Date.now();
  private wearablePublisher: StreamToMQTT | null = null;
  private smartphonePublisher: StreamToMQTT | null = null;

  constructor() {
    this.initializeResults();
  }

  private initializeResults(): void {
    this.approachResults.set("approximation", {
      name: "Approximation",
      topic: OUTPUT_TOPICS.approximation,
      results: [],
      started: Date.now(),
      errors: 0,
    });

    this.approachResults.set("chunked", {
      name: "Chunked Query",
      topic: OUTPUT_TOPICS.chunked,
      results: [],
      started: Date.now(),
      errors: 0,
    });

    this.approachResults.set("fetching", {
      name: "Fetching Client-Side",
      topic: OUTPUT_TOPICS.fetching,
      results: [],
      started: Date.now(),
      errors: 0,
    });
  }

  async run(): Promise<void> {
    console.log("\n" + "=".repeat(70));
    console.log("SINGLE ITERATION EXPERIMENT");
    console.log("=".repeat(70));
    console.log(`Duration: ${EXPERIMENT_DURATION_S}s`);
    console.log(`Data Rate: ${DATA_RATE_HZ}Hz`);
    console.log("=".repeat(70));

    try {
      console.log("\n[1/7] Clearing previous MQTT data...");
      await this.clearPreviousData();

      console.log("\n[2/7] Clearing previous CSV files...");
      this.clearPreviousCSVFiles();

      console.log("\n[3/7] Setting up MQTT monitoring...");
      await this.setupMQTTMonitoring();

      console.log("\n[4/7] Launching orchestrators...");
      await this.launchOrchestrators();

      console.log(`\n[5/7] Waiting ${INIT_WAIT_S}s for initialization...`);
      await this.sleep(INIT_WAIT_S * 1000);

      console.log("\n[6/7] Publishing test data...");
      await this.publishTestData();

      console.log(`\n[7/7] Waiting ${FINAL_WAIT_S}s for final results...`);
      await this.sleep(FINAL_WAIT_S * 1000);

      await this.sendFlushEvents();
      await this.sleep(5000);

      this.printResults();
      this.saveResultsToCSV();
    } catch (error) {
      console.error("\n[ERROR] Experiment failed:", error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  private clearPreviousCSVFiles(): void {
    const files = [FETCHING_FILE, CHUNKED_FILE, APPROXIMATION_FILE];
    files.forEach((file) => {
      if (fs.existsSync(file)) {
        fs.unlinkSync(file);
        console.log(`  ✓ Deleted ${path.basename(file)}`);
      }
    });
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
          WEARABLE_TOPIC,
          SMARTPHONE_TOPIC,
        ];

        topics.forEach((topic) => {
          clearClient.publish(topic, "", { qos: 1, retain: true });
        });

        console.log("  ✓ Cleared retained messages");
        clearClient.end(false, {}, () => {
          setTimeout(resolve, 1000);
        });
      });
    });
  }

  private async setupMQTTMonitoring(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: `monitor-single-${Math.random().toString(16).substr(2, 8)}`,
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
            console.error("  ✗ Failed to subscribe:", err);
            reject(err);
          } else {
            console.log(`  ✓ Subscribed to ${topics.length} topics`);
            resolve();
          }
        });
      });

      this.mqttClient.on("message", (topic: string, message: Buffer) => {
        const content = message.toString();
        if (!content || content.trim() === "") return;

        this.recordResult(topic, content);
      });
    });
  }

  private recordResult(topic: string, content: string): void {
    const timestamp = Date.now();

    let approachKey = "";
    if (topic === OUTPUT_TOPICS.approximation) approachKey = "approximation";
    else if (topic === OUTPUT_TOPICS.chunked) approachKey = "chunked";
    else if (topic === OUTPUT_TOPICS.fetching) approachKey = "fetching";
    else return;

    const approachData = this.approachResults.get(approachKey);
    if (!approachData) return;

    try {
      let value: number | null = null;

      // Try parsing as JSON first
      try {
        const jsonData = JSON.parse(content);

        // Case 1: JSON object with numeric properties (chunked approach)
        if (jsonData && typeof jsonData === "object") {
          if (typeof jsonData.avgValue === "number") {
            value = jsonData.avgValue;
          } else if (typeof jsonData.value === "number") {
            value = jsonData.value;
          } else if (typeof jsonData.result === "number") {
            value = jsonData.result;
          }
        }

        // Case 2: JSON-wrapped RDF string (fetching approach)
        // e.g., "<https://rsp.js/aggregation_event/...> <hasValue> \"3.96\"^^<float> ."
        if (typeof jsonData === "string" && value === null) {
          const hasValueMatch = jsonData.match(
            /<https:\/\/saref\.etsi\.org\/core\/hasValue>\s+"?(-?\d+\.?\d*)"?\^\^/,
          );
          if (hasValueMatch) {
            value = parseFloat(hasValueMatch[1]);
          }
        }
      } catch (jsonError) {
        // Not JSON, try parsing as raw RDF
      }

      // If not JSON, parse as RDF N-Triples
      if (value === null) {
        // Look for hasValue predicate with numeric literal
        const hasValueMatch = content.match(
          /<https:\/\/saref\.etsi\.org\/core\/hasValue>\s+"?(-?\d+\.?\d*)"?\^\^/,
        );
        if (hasValueMatch) {
          value = parseFloat(hasValueMatch[1]);
        } else {
          // Fallback: extract any numeric value that looks like a sensor reading (not a timestamp)
          const numericMatches = content.match(/"(-?\d+\.?\d*)"\^\^/g);
          if (numericMatches && numericMatches.length > 0) {
            // Get the first numeric value that's not a timestamp (< 10000)
            for (const match of numericMatches) {
              const numMatch = match.match(/"(-?\d+\.?\d*)"/);
              if (numMatch) {
                const num = parseFloat(numMatch[1]);
                if (Math.abs(num) < 10000) {
                  value = num;
                  break;
                }
              }
            }
          }
        }
      }

      // Filter out error values (-9 is used by RSP engines to indicate no data/error)
      if (value !== null && !isNaN(value) && value !== -9) {
        const record: ResultRecord = {
          queryRegisteredTimestamp: approachData.started,
          resultTimestamp: timestamp,
          result: value,
        };
        approachData.results.push(record);

        console.log(
          `  📊 [${approachKey.toUpperCase()}] Result #${approachData.results.length}: ${value.toFixed(4)}`,
        );
      } else if (value === -9) {
        console.log(
          `  ⚠️  [${approachKey.toUpperCase()}] Skipping error value (-9)`,
        );
      } else {
        console.log(
          `  ⚠️  [${approachKey.toUpperCase()}] Could not extract value from message`,
        );
      }
    } catch (e) {
      approachData.errors++;
      console.error(
        `  ✗ [${approachKey.toUpperCase()}] Error parsing result:`,
        e,
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
      const projectRoot = path.resolve(__dirname, "..");
      const fullPath = path.resolve(projectRoot, scriptPath);

      console.log(`  → ${name}...`);

      const proc = spawn("npx", ["ts-node", fullPath], {
        env: { ...process.env, ...env },
        stdio: ["pipe", "pipe", "pipe"],
      });

      this.orchestrators.set(name, proc);

      proc.stdout?.on("data", (data: Buffer) => {
        const lines = data.toString().split("\n");
        lines.forEach((line) => {
          if (line.trim() && !line.includes("Experimental")) {
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

      setTimeout(resolve, 3000);
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

    console.log(`  → Data rate: ${DATA_RATE_HZ}Hz`);
    console.log(`  → Duration: ~${EXPERIMENT_DURATION_S}s`);

    const wearableClientId = `pub-wearable-${Math.random().toString(16).substr(2, 8)}`;
    const smartphoneClientId = `pub-smartphone-${Math.random().toString(16).substr(2, 8)}`;

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

    await Promise.all([
      this.wearablePublisher.replay_streams(),
      this.smartphonePublisher.replay_streams(),
    ]);

    console.log("  ✓ Data replay completed");
  }

  private async sendFlushEvents(): Promise<void> {
    console.log("\n[FLUSH] Sending flush events to trigger final windows...");

    const flushClient = mqtt.connect(MQTT_BROKER, {
      clientId: `flusher-${Math.random().toString(16).substr(2, 8)}`,
      clean: true,
    });

    await new Promise<void>((resolve) => {
      flushClient.on("connect", async () => {
        const futureTimestamp = Date.now() + 3600000; // +1 hour

        const flushEventWearable = `
<https://dahcc.idlab.ugent.be/Protego/_participant1/flush> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Measurement> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/flush> <https://saref.etsi.org/core/hasValue> "999"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/flush> <https://saref.etsi.org/core/hasTimestamp> "${futureTimestamp}"^^<http://www.w3.org/2001/XMLSchema#integer> .
`.trim();

        const flushEventSmartphone = `
<https://dahcc.idlab.ugent.be/Protego/_participant1/flush> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://saref.etsi.org/core/Measurement> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/flush> <https://saref.etsi.org/core/hasValue> "999"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/flush> <https://saref.etsi.org/core/hasTimestamp> "${futureTimestamp}"^^<http://www.w3.org/2001/XMLSchema#integer> .
`.trim();

        await new Promise<void>((res) => {
          flushClient.publish(
            WEARABLE_TOPIC,
            flushEventWearable,
            { qos: 2 },
            () => {
              console.log("  ✓ Sent flush to wearable topic");
              res();
            },
          );
        });

        await new Promise<void>((res) => {
          flushClient.publish(
            SMARTPHONE_TOPIC,
            flushEventSmartphone,
            { qos: 2 },
            () => {
              console.log("  ✓ Sent flush to smartphone topic");
              res();
            },
          );
        });

        flushClient.end(false, {}, () => resolve());
      });
    });
  }

  private printResults(): void {
    console.log("\n" + "=".repeat(70));
    console.log("ITERATION RESULTS");
    console.log("=".repeat(70));

    for (const [key, data] of this.approachResults) {
      const icon = data.results.length > 0 ? "✓" : "✗";
      console.log(`\n  ${data.name}:`);
      console.log(`    Results: ${data.results.length} ${icon}`);
      console.log(`    Errors:  ${data.errors}`);
      console.log(`    Topic:   ${data.topic}`);
    }

    console.log("\n" + "=".repeat(70));
  }

  private saveResultsToCSV(): void {
    console.log("\n[SAVE] Writing results to CSV files...");

    // Ensure results directory exists
    if (!fs.existsSync(RESULTS_DIR)) {
      fs.mkdirSync(RESULTS_DIR, { recursive: true });
    }

    const header = "query_registered_timestamp,result_timestamp,result\n";

    // Save approximation results
    const approxData = this.approachResults.get("approximation");
    if (approxData) {
      const approxCSV =
        header +
        approxData.results
          .map(
            (r) =>
              `${r.queryRegisteredTimestamp},${r.resultTimestamp},${r.result}`,
          )
          .join("\n");
      fs.writeFileSync(APPROXIMATION_FILE, approxCSV);
      console.log(
        `  ✓ ${path.basename(APPROXIMATION_FILE)} (${approxData.results.length} results)`,
      );
    }

    // Save chunked results
    const chunkedData = this.approachResults.get("chunked");
    if (chunkedData) {
      const chunkedCSV =
        header +
        chunkedData.results
          .map(
            (r) =>
              `${r.queryRegisteredTimestamp},${r.resultTimestamp},${r.result}`,
          )
          .join("\n");
      fs.writeFileSync(CHUNKED_FILE, chunkedCSV);
      console.log(
        `  ✓ ${path.basename(CHUNKED_FILE)} (${chunkedData.results.length} results)`,
      );
    }

    // Save fetching results
    const fetchingData = this.approachResults.get("fetching");
    if (fetchingData) {
      const fetchingCSV =
        header +
        fetchingData.results
          .map(
            (r) =>
              `${r.queryRegisteredTimestamp},${r.resultTimestamp},${r.result}`,
          )
          .join("\n");
      fs.writeFileSync(FETCHING_FILE, fetchingCSV);
      console.log(
        `  ✓ ${path.basename(FETCHING_FILE)} (${fetchingData.results.length} results)`,
      );
    }

    console.log(`\n  Results saved to: ${RESULTS_DIR}`);
  }

  private async cleanup(): Promise<void> {
    console.log("\n[CLEANUP] Shutting down...");

    for (const [name, proc] of this.orchestrators) {
      try {
        if (proc.exitCode === null) {
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

// ============================================================================
// MAIN EXECUTION
// ============================================================================

const runner = new SingleIterationRunner();
runner
  .run()
  .then(() => {
    console.log("\n✓ Single iteration complete!");
    console.log("\nNext step: Run analysis with:");
    console.log("  npx ts-node scripts/analyze-5-iterations-results.ts\n");
    process.exit(0);
  })
  .catch((err) => {
    console.error("\n✗ Iteration failed:", err);
    process.exit(1);
  });
