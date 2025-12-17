/**
 * Simple Benchmark Script - Self-contained experiment runner
 *
 * This script runs each approach in a subprocess and collects results via MQTT.
 * Uses shorter window times for faster testing.
 */

import * as mqtt from "mqtt";
import * as path from "path";
import * as fs from "fs";
import { spawn, ChildProcess } from "child_process";

// ============================================================================
// Configuration
// ============================================================================

const CONFIG = {
  mqttBroker: "mqtt://localhost:1883",
  dataFrequency: 4, // Hz
  windowWidthMs: 120000, // 2 minutes (matches orchestrator)
  windowSlideMs: 60000, // 1 minute (matches orchestrator)
  runDurationMs: 300000, // 5 minutes per approach
  warmupWindowCount: 1,
  dataBasePath: path.resolve(__dirname, "../../src/streamer/data"),
};

// ============================================================================
// Types
// ============================================================================

interface WindowResult {
  windowNumber: number;
  windowCloseTime: number;
  resultTime: number;
  latencyMs: number;
  value: number;
}

interface ApproachResult {
  name: string;
  windows: WindowResult[];
  avgLatencyMs: number;
  stdDevLatencyMs: number;
  minLatencyMs: number;
  maxLatencyMs: number;
  avgValue: number;
}

// ============================================================================
// Utility Functions
// ============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function calculateStats(values: number[]): {
  mean: number;
  stdDev: number;
  min: number;
  max: number;
} {
  if (values.length === 0) return { mean: 0, stdDev: 0, min: 0, max: 0 };

  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const squaredDiffs = values.map((v) => Math.pow(v - mean, 2));
  const stdDev = Math.sqrt(
    squaredDiffs.reduce((a, b) => a + b, 0) / values.length,
  );

  return {
    mean,
    stdDev,
    min: Math.min(...values),
    max: Math.max(...values),
  };
}

// ============================================================================
// Data Publisher (runs in subprocess)
// ============================================================================

class DataPublisher {
  private process: ChildProcess | null = null;

  async start(): Promise<void> {
    console.log("[DataPublisher] Starting data streams...");

    const wearablePath = path.join(
      CONFIG.dataBasePath,
      "wearable.acceleration.x",
      "data.nt",
    );
    const smartphonePath = path.join(
      CONFIG.dataBasePath,
      "smartphone.acceleration.x",
      "data.nt",
    );

    // Check if files exist, fall back to noisy_datasets if not
    let actualWearablePath = wearablePath;
    let actualSmartphonePath = smartphonePath;

    if (!fs.existsSync(wearablePath)) {
      actualWearablePath = path.join(
        CONFIG.dataBasePath,
        "noisy_datasets/noise_0.5/wearable.acceleration.x",
        "data.nt",
      );
      actualSmartphonePath = path.join(
        CONFIG.dataBasePath,
        "noisy_datasets/noise_0.5/smartphone.acceleration.x",
        "data.nt",
      );
    }

    console.log(`[DataPublisher] Using wearable data: ${actualWearablePath}`);

    const script = `
            const { StreamToMQTT } = require('${path.resolve(__dirname, "../../src/streamer/src/publishing/StreamToMQTT").replace(/\\/g, "\\\\")}');

            async function main() {
                const wearable = new StreamToMQTT(
                    '${CONFIG.mqttBroker}',
                    ${CONFIG.dataFrequency},
                    '${actualWearablePath.replace(/\\/g, "\\\\")}',
                    'wearableX',
                    { clientId: 'benchmark-wearable-' + Date.now(), clean: true }
                );

                const smartphone = new StreamToMQTT(
                    '${CONFIG.mqttBroker}',
                    ${CONFIG.dataFrequency},
                    '${actualSmartphonePath.replace(/\\/g, "\\\\")}',
                    'smartphoneX',
                    { clientId: 'benchmark-smartphone-' + Date.now(), clean: true }
                );

                await Promise.all([
                    wearable.replay_streams(),
                    smartphone.replay_streams()
                ]);
            }

            main().catch(console.error);
        `;

    this.process = spawn("npx", ["ts-node", "-e", script], {
      cwd: path.resolve(__dirname, "../.."),
      stdio: ["pipe", "pipe", "pipe"],
    });

    this.process.stderr?.on("data", (data) => {
      const msg = data.toString();
      if (msg.includes("Error") && !msg.includes("ExperimentalWarning")) {
        console.error("[DataPublisher Error]", msg);
      }
    });

    // Wait for data to start flowing
    await sleep(3000);
    console.log("[DataPublisher] Data streams started");
  }

  stop(): void {
    if (this.process) {
      this.process.kill("SIGTERM");
      this.process = null;
    }
    console.log("[DataPublisher] Stopped");
  }
}

// ============================================================================
// Result Collector (MQTT Subscriber)
// ============================================================================

class ResultCollector {
  private client: mqtt.MqttClient;
  private results: WindowResult[] = [];
  private windowNumber: number = 0;
  private startTime: number = 0;
  private topic: string;
  private connected: boolean = false;
  private warmupDone: boolean = false;

  constructor(topic: string) {
    this.topic = topic;
    this.client = mqtt.connect(CONFIG.mqttBroker, {
      clientId: "benchmark-collector-" + Date.now(),
      clean: true,
    });
  }

  async start(): Promise<void> {
    this.startTime = Date.now();
    this.results = [];
    this.windowNumber = 0;
    this.warmupDone = false;

    return new Promise((resolve, reject) => {
      this.client.on("connect", () => {
        this.connected = true;
        console.log(
          `[ResultCollector] Connected, subscribing to: ${this.topic}`,
        );
        this.client.subscribe(this.topic, { qos: 1 }, (err) => {
          if (err) {
            console.error("[ResultCollector] Subscribe error:", err);
            reject(err);
          } else {
            console.log(`[ResultCollector] Subscribed to: ${this.topic}`);
            resolve();
          }
        });
      });

      this.client.on("message", (_topic, message) => {
        this.handleMessage(message);
      });

      this.client.on("error", (err) => {
        console.error("[ResultCollector] MQTT error:", err);
      });

      // Timeout if connection takes too long
      setTimeout(() => {
        if (!this.connected) {
          reject(new Error("MQTT connection timeout"));
        }
      }, 10000);
    });
  }

  private handleMessage(message: Buffer): void {
    const resultTime = Date.now();
    this.windowNumber++;

    // Skip warmup window
    if (!this.warmupDone && this.windowNumber <= CONFIG.warmupWindowCount) {
      console.log(
        `[ResultCollector] Skipping warmup window ${this.windowNumber}`,
      );
      if (this.windowNumber === CONFIG.warmupWindowCount) {
        // Reset timing after warmup
        this.startTime = Date.now();
        this.windowNumber = 0;
        this.warmupDone = true;
      }
      return;
    }

    // Calculate expected window close time
    // Window N closes at: startTime + (N * windowSlideMs)
    const windowCloseTime =
      this.startTime + this.windowNumber * CONFIG.windowSlideMs;
    const latencyMs = resultTime - windowCloseTime;

    // Parse the result value
    let value = 0;
    try {
      const msgStr = message.toString();
      if (msgStr.startsWith("{")) {
        const parsed = JSON.parse(msgStr);
        value =
          parsed.unifiedResult ??
          parsed.unifiedAverage ??
          parsed.value ??
          parsed.result ??
          0;
      } else {
        // RDF/Turtle format - look for hasValue
        const match =
          msgStr.match(/hasValue>\s*"([^"]*)"/) ||
          msgStr.match(/"(-?\d+\.?\d*)"/) ||
          msgStr.match(/(-?\d+\.?\d+)/);
        if (match) {
          value = parseFloat(match[1]);
        }
      }
    } catch (e) {
      console.error("[ResultCollector] Parse error:", e);
    }

    const result: WindowResult = {
      windowNumber: this.windowNumber,
      windowCloseTime,
      resultTime,
      latencyMs,
      value,
    };

    this.results.push(result);
    console.log(
      `[Window ${this.windowNumber}] value=${value.toFixed(4)}, latency=${latencyMs}ms`,
    );
  }

  getResults(): WindowResult[] {
    return this.results;
  }

  stop(): void {
    this.client.end();
  }
}

// ============================================================================
// Approach Runners (spawn subprocesses to avoid module side effects)
// ============================================================================

async function runApproach(
  name: string,
  scriptPath: string,
  resultTopic: string,
  durationMs: number,
): Promise<ApproachResult> {
  console.log("\n" + "=".repeat(60));
  console.log(`Running: ${name}`);
  console.log("=".repeat(60));

  const resultCollector = new ResultCollector(resultTopic);
  await resultCollector.start();

  // Start data publisher
  const dataPublisher = new DataPublisher();
  await dataPublisher.start();

  // Start the approach process
  console.log(`[${name}] Starting approach...`);
  const approachProcess = spawn("npx", ["ts-node", scriptPath], {
    cwd: path.resolve(__dirname, "../.."),
    stdio: ["pipe", "pipe", "pipe"],
  });

  approachProcess.stdout?.on("data", (data) => {
    const msg = data.toString();
    if (
      msg.includes("RStream") ||
      msg.includes("Aggregation") ||
      msg.includes("result")
    ) {
      // Show important messages
      // console.log(`[${name}]`, msg.trim().substring(0, 100));
    }
  });

  approachProcess.stderr?.on("data", (data) => {
    const msg = data.toString();
    if (
      msg.includes("Error") &&
      !msg.includes("ExperimentalWarning") &&
      !msg.includes("Watermark")
    ) {
      console.error(`[${name} Error]`, msg.substring(0, 200));
    }
  });

  // Wait for the specified duration
  console.log(`[${name}] Running for ${durationMs / 1000} seconds...`);
  await sleep(durationMs);

  // Cleanup
  approachProcess.kill("SIGTERM");
  dataPublisher.stop();

  // Give a moment for final messages
  await sleep(1000);
  resultCollector.stop();

  const results = resultCollector.getResults();
  const latencies = results.map((r) => r.latencyMs);
  const values = results.map((r) => r.value);
  const latencyStats = calculateStats(latencies);
  const valueStats = calculateStats(values);

  return {
    name,
    windows: results,
    avgLatencyMs: latencyStats.mean,
    stdDevLatencyMs: latencyStats.stdDev,
    minLatencyMs: latencyStats.min,
    maxLatencyMs: latencyStats.max,
    avgValue: valueStats.mean,
  };
}

async function runClientSideApproach(
  durationMs: number,
): Promise<ApproachResult> {
  return runApproach(
    "Client-Side Processing",
    path.resolve(
      __dirname,
      "../../src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts",
    ),
    "client_operation_output",
    durationMs,
  );
}

async function runChunkedApproach(durationMs: number): Promise<ApproachResult> {
  return runApproach(
    "Chunked Query",
    path.resolve(
      __dirname,
      "../../src/approaches/StreamingQueryChunkedApproachOrchestrator.ts",
    ),
    "output",
    durationMs,
  );
}

async function runApproximationApproach(
  durationMs: number,
): Promise<ApproachResult> {
  return runApproach(
    "Approximation",
    path.resolve(
      __dirname,
      "../../src/approaches/StreamingQueryApproximationApproachOrchestrator.ts",
    ),
    "approximation/output",
    durationMs,
  );
}

// ============================================================================
// Results Output
// ============================================================================

function printResults(
  results: ApproachResult[],
  groundTruthValue: number,
): void {
  console.log("\n" + "=".repeat(80));
  console.log("BENCHMARK RESULTS");
  console.log("=".repeat(80));

  console.log("\n--- Latency Results ---\n");
  console.log(
    "| Approach                  | Avg Latency (ms) | Std Dev (ms) | Windows |",
  );
  console.log(
    "|---------------------------|------------------|--------------|---------|",
  );

  for (const result of results) {
    console.log(
      `| ${result.name.padEnd(25)} | ${result.avgLatencyMs.toFixed(1).padStart(16)} | ${result.stdDevLatencyMs.toFixed(1).padStart(12)} | ${result.windows.length.toString().padStart(7)} |`,
    );
  }

  console.log("\n--- Accuracy Results (vs Ground Truth) ---\n");
  console.log("| Approach                  | Avg Value     | Accuracy % |");
  console.log("|---------------------------|---------------|------------|");

  for (const result of results) {
    let accuracy = 100;
    if (groundTruthValue !== 0 && result.avgValue !== 0) {
      const error =
        Math.abs((result.avgValue - groundTruthValue) / groundTruthValue) * 100;
      accuracy = Math.max(0, 100 - error);
    }
    const accuracyStr =
      result.name === "Client-Side Processing"
        ? "100% (GT)"
        : `${accuracy.toFixed(1)}%`;
    console.log(
      `| ${result.name.padEnd(25)} | ${result.avgValue.toFixed(4).padStart(13)} | ${accuracyStr.padStart(10)} |`,
    );
  }

  console.log("\n--- Reference: August 2024 Benchmark ---\n");
  console.log("| Approach                  | Latency (ms)     | Accuracy |");
  console.log("|---------------------------|------------------|----------|");
  console.log("| Chunked Query             | 414 +/- 12.3     | 100%     |");
  console.log("| Approximation             | 359 +/- 31.2     | 89.5%    |");
  console.log("| Client-Side Processing    | 2543 +/- 213.3   | 100% (GT)|");
}

function saveResults(results: ApproachResult[]): void {
  const outputDir = path.resolve(__dirname, "results");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputPath = path.join(outputDir, `simple-benchmark_${timestamp}.json`);

  fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
  console.log(`\nResults saved to: ${outputPath}`);
}

// ============================================================================
// Main
// ============================================================================

async function main(): Promise<void> {
  console.log("=".repeat(60));
  console.log("  Streaming Query Hive - Simple Benchmark");
  console.log("  Latency = result_time - window_close_time");
  console.log("=".repeat(60));
  console.log("\nConfiguration:");
  console.log(`  Data Frequency: ${CONFIG.dataFrequency} Hz`);
  console.log(`  Window Width: ${CONFIG.windowWidthMs}ms`);
  console.log(`  Window Slide: ${CONFIG.windowSlideMs}ms`);
  console.log(`  Run Duration: ${CONFIG.runDurationMs / 1000}s per approach`);
  console.log(`  Warmup Windows: ${CONFIG.warmupWindowCount}`);

  const results: ApproachResult[] = [];

  // Run client-side first (ground truth)
  const clientSideResult = await runClientSideApproach(CONFIG.runDurationMs);
  results.push(clientSideResult);

  // Cooldown between approaches
  console.log("\n[Benchmark] Cooldown between approaches...");
  await sleep(5000);

  // Run chunked approach
  const chunkedResult = await runChunkedApproach(CONFIG.runDurationMs);
  results.push(chunkedResult);

  await sleep(5000);

  // Run approximation approach
  const approxResult = await runApproximationApproach(CONFIG.runDurationMs);
  results.push(approxResult);

  // Print and save results
  const groundTruthValue = clientSideResult.avgValue;
  printResults(results, groundTruthValue);
  saveResults(results);

  // Force exit since some MQTT connections may keep the process alive
  process.exit(0);
}

// Handle command line arguments for running specific approaches
const args = process.argv.slice(2);
const approach = args[0] || "all";

if (approach === "client-side" || approach === "client") {
  runClientSideApproach(CONFIG.runDurationMs).then((result) => {
    console.log("\n--- Single Approach Result ---");
    console.log(`Approach: ${result.name}`);
    console.log(`Windows collected: ${result.windows.length}`);
    console.log(
      `Average latency: ${result.avgLatencyMs.toFixed(1)} +/- ${result.stdDevLatencyMs.toFixed(1)} ms`,
    );
    console.log(`Average value: ${result.avgValue.toFixed(4)}`);
    process.exit(0);
  });
} else if (approach === "chunked") {
  runChunkedApproach(CONFIG.runDurationMs).then((result) => {
    console.log("\n--- Single Approach Result ---");
    console.log(`Approach: ${result.name}`);
    console.log(`Windows collected: ${result.windows.length}`);
    console.log(
      `Average latency: ${result.avgLatencyMs.toFixed(1)} +/- ${result.stdDevLatencyMs.toFixed(1)} ms`,
    );
    console.log(`Average value: ${result.avgValue.toFixed(4)}`);
    process.exit(0);
  });
} else if (approach === "approximation" || approach === "approx") {
  runApproximationApproach(CONFIG.runDurationMs).then((result) => {
    console.log("\n--- Single Approach Result ---");
    console.log(`Approach: ${result.name}`);
    console.log(`Windows collected: ${result.windows.length}`);
    console.log(
      `Average latency: ${result.avgLatencyMs.toFixed(1)} +/- ${result.stdDevLatencyMs.toFixed(1)} ms`,
    );
    console.log(`Average value: ${result.avgValue.toFixed(4)}`);
    process.exit(0);
  });
} else {
  main().catch((err) => {
    console.error("Benchmark failed:", err);
    process.exit(1);
  });
}
