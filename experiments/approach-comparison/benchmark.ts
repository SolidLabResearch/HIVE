/**
 * Standalone Benchmark Script
 *
 * This script runs each approach independently (one at a time) and measures:
 * - Latency: time from window close to result available
 * - Resource usage: CPU and memory
 * - Accuracy: compared to client-side processing (ground truth)
 *
 * Usage:
 *   npx ts-node experiments/approach-comparison/benchmark.ts [approach]
 *
 * Where [approach] is one of:
 *   - client-side (ground truth)
 *   - chunked
 *   - approximation
 *   - all (runs all approaches sequentially)
 */

import * as fs from "fs";
import * as path from "path";
import * as mqtt from "mqtt";
import { spawn, ChildProcess } from "child_process";

// ============================================================================
// Configuration
// ============================================================================

interface BenchmarkConfig {
  mqttBroker: string;
  dataFrequency: number;
  windowWidthMs: number;
  windowSlideMs: number;
  warmupWindowCount: number;
  outputDir: string;
}

interface WindowResult {
  windowNumber: number;
  queryRegisteredTime: number;
  windowCloseTime: number;
  resultAvailableTime: number;
  latencyMs: number;
  resultValue: number;
}

interface BenchmarkResult {
  approach: string;
  windows: WindowResult[];
  latency: {
    mean: number;
    stdDev: number;
    min: number;
    max: number;
  };
  resources: {
    cpuPercent: number;
    memoryMB: number;
    peakMemoryMB: number;
  };
}

const CONFIG: BenchmarkConfig = {
  mqttBroker: "mqtt://localhost:1883",
  dataFrequency: 4,
  windowWidthMs: 120000,
  windowSlideMs: 60000,
  warmupWindowCount: 1,
  outputDir: path.resolve(__dirname, "results"),
};

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
  if (values.length === 0) {
    return { mean: 0, stdDev: 0, min: 0, max: 0 };
  }

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

function formatLatency(stats: { mean: number; stdDev: number }): string {
  return `${stats.mean.toFixed(0)}ms +/- ${stats.stdDev.toFixed(1)}ms`;
}

// ============================================================================
// Data Replayer
// ============================================================================

class DataReplayer {
  private process: ChildProcess | null = null;

  async start(): Promise<void> {
    console.log("[DataReplayer] Starting data replay at 4Hz...");

    const wearablePath = path.resolve(
      __dirname,
      "../../src/streamer/data/wearable.acceleration.x/data.nt",
    );
    const smartphonePath = path.resolve(
      __dirname,
      "../../src/streamer/data/smartphone.acceleration.x/data.nt",
    );

    // Use spawn to run the data replayer as a subprocess
    this.process = spawn(
      "npx",
      [
        "ts-node",
        "-e",
        `
        const { StreamToMQTT } = require('${path.resolve(__dirname, "../../src/streamer/src/publishing/StreamToMQTT").replace(/\\/g, "\\\\")}');

        async function main() {
          const wearableClientId = 'wearable-benchmark-' + Math.random().toString(16).substr(2, 8);
          const smartphoneClientId = 'smartphone-benchmark-' + Math.random().toString(16).substr(2, 8);

          const wearable = new StreamToMQTT(
            '${CONFIG.mqttBroker}',
            ${CONFIG.dataFrequency},
            '${wearablePath.replace(/\\/g, "\\\\")}',
            'wearableX',
            { clientId: wearableClientId, clean: true }
          );

          const smartphone = new StreamToMQTT(
            '${CONFIG.mqttBroker}',
            ${CONFIG.dataFrequency},
            '${smartphonePath.replace(/\\/g, "\\\\")}',
            'smartphoneX',
            { clientId: smartphoneClientId, clean: true }
          );

          await Promise.all([
            wearable.replay_streams(),
            smartphone.replay_streams()
          ]);
        }

        main().catch(console.error);
        `,
      ],
      {
        cwd: path.resolve(__dirname, "../.."),
        stdio: ["pipe", "pipe", "pipe"],
      },
    );

    this.process.stdout?.on("data", () => {
      // Data publishing in progress
    });

    this.process.stderr?.on("data", (data) => {
      const msg = data.toString();
      if (msg.includes("Error") && !msg.includes("ExperimentalWarning")) {
        console.error("[DataReplayer Error]", msg);
      }
    });

    // Wait for data to start flowing
    await sleep(2000);
  }

  stop(): void {
    if (this.process) {
      this.process.kill("SIGTERM");
      this.process = null;
    }
  }
}

// ============================================================================
// Result Collector
// ============================================================================

class ResultCollector {
  private mqttClient: mqtt.MqttClient;
  private results: WindowResult[] = [];
  private queryRegisteredTime: number = 0;
  private windowNumber: number = 0;
  private topic: string;
  private warmupComplete: boolean = false;
  private onResultCallback: ((_result: WindowResult) => void) | null = null;

  constructor(topic: string) {
    this.topic = topic;
    this.mqttClient = mqtt.connect(CONFIG.mqttBroker, {
      clientId: `benchmark-collector-${Date.now()}`,
      clean: true,
    });
  }

  async start(queryRegisteredTime: number): Promise<void> {
    this.queryRegisteredTime = queryRegisteredTime;
    this.results = [];
    this.windowNumber = 0;
    this.warmupComplete = false;

    return new Promise((resolve) => {
      this.mqttClient.on("connect", () => {
        console.log(`[ResultCollector] Subscribing to: ${this.topic}`);
        this.mqttClient.subscribe(this.topic, { qos: 1 }, (err) => {
          if (err) {
            console.error("[ResultCollector] Subscribe error:", err);
          }
          resolve();
        });
      });

      this.mqttClient.on("message", (topic, message) => {
        this.handleMessage(message);
      });
    });
  }

  private handleMessage(message: Buffer): void {
    const resultAvailableTime = Date.now();
    this.windowNumber++;

    // Skip warmup windows
    if (this.windowNumber <= CONFIG.warmupWindowCount) {
      console.log(
        `[ResultCollector] Skipping warmup window ${this.windowNumber}`,
      );
      // After warmup, reset the query registered time
      if (this.windowNumber === CONFIG.warmupWindowCount) {
        this.warmupComplete = true;
        this.queryRegisteredTime = Date.now();
        this.windowNumber = 0;
      }
      return;
    }

    // Calculate window close time based on window number
    // Window N closes at: queryRegisteredTime + (N * windowSlideMs)
    const windowCloseTime =
      this.queryRegisteredTime + this.windowNumber * CONFIG.windowSlideMs;

    // Latency = result available time - window close time
    const latencyMs = resultAvailableTime - windowCloseTime;

    // Parse result value
    let resultValue = 0;
    try {
      const msgStr = message.toString();

      if (msgStr.startsWith("{")) {
        const parsed = JSON.parse(msgStr);
        resultValue =
          parsed.unifiedResult ??
          parsed.unifiedAverage ??
          parsed.value ??
          parsed.result ??
          0;
      } else {
        // RDF format
        const valueMatch =
          msgStr.match(/hasValue>\s*"([^"]*)"/) ||
          msgStr.match(/"(-?\d+\.?\d*)"/) ||
          msgStr.match(/(-?\d+\.?\d+)/);
        if (valueMatch) {
          resultValue = parseFloat(valueMatch[1]);
        }
      }
    } catch (e) {
      console.error("[ResultCollector] Parse error:", e);
    }

    const result: WindowResult = {
      windowNumber: this.windowNumber,
      queryRegisteredTime: this.queryRegisteredTime,
      windowCloseTime,
      resultAvailableTime,
      latencyMs,
      resultValue,
    };

    this.results.push(result);
    console.log(
      `[Window ${this.windowNumber}] value=${resultValue.toFixed(4)}, latency=${latencyMs}ms`,
    );

    if (this.onResultCallback) {
      this.onResultCallback(result);
    }
  }

  onResult(callback: (_result: WindowResult) => void): void {
    this.onResultCallback = callback;
  }

  getResults(): WindowResult[] {
    return this.results;
  }

  stop(): void {
    this.mqttClient.end();
  }
}

// ============================================================================
// Resource Monitor
// ============================================================================

class ResourceMonitor {
  private samples: { memoryMB: number; cpuMs: number }[] = [];
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private startCpu: { user: number; system: number } | null = null;
  private startTime: number = 0;

  start(): void {
    this.samples = [];
    this.startCpu = process.cpuUsage();
    this.startTime = Date.now();

    this.intervalId = setInterval(() => {
      const mem = process.memoryUsage();
      const cpu = process.cpuUsage(this.startCpu!);

      this.samples.push({
        memoryMB: mem.heapUsed / 1024 / 1024,
        cpuMs: (cpu.user + cpu.system) / 1000,
      });
    }, 100);
  }

  stop(): { cpuPercent: number; memoryMB: number; peakMemoryMB: number } {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }

    if (this.samples.length === 0) {
      return { cpuPercent: 0, memoryMB: 0, peakMemoryMB: 0 };
    }

    const elapsedMs = Date.now() - this.startTime;
    const totalCpuMs = this.samples[this.samples.length - 1]?.cpuMs ?? 0;
    const cpuPercent = elapsedMs > 0 ? (totalCpuMs / elapsedMs) * 100 : 0;

    const avgMemoryMB =
      this.samples.reduce((sum, s) => sum + s.memoryMB, 0) /
      this.samples.length;
    const peakMemoryMB = Math.max(...this.samples.map((s) => s.memoryMB));

    return { cpuPercent, memoryMB: avgMemoryMB, peakMemoryMB };
  }
}

// ============================================================================
// Approach Runners
// ============================================================================

async function runApproach(
  name: string,
  scriptPath: string,
  resultTopic: string,
  runDurationMs: number,
): Promise<BenchmarkResult> {
  console.log("\n" + "=".repeat(60));
  console.log(`Running: ${name}`);
  console.log("=".repeat(60));

  const resourceMonitor = new ResourceMonitor();
  const resultCollector = new ResultCollector(resultTopic);

  resourceMonitor.start();

  // Start the approach process
  const approachProcess = spawn("npx", ["ts-node", scriptPath], {
    cwd: path.resolve(__dirname, "../.."),
    stdio: ["pipe", "pipe", "pipe"],
  });

  const queryRegisteredTime = Date.now();
  await resultCollector.start(queryRegisteredTime);

  // Log output (optional, for debugging)
  approachProcess.stdout?.on("data", () => {
    // Silent - uncomment below to see approach output
    // console.log(`[${name}]`, data.toString().trim());
  });

  approachProcess.stderr?.on("data", (data) => {
    const msg = data.toString();
    if (msg.includes("Error") && !msg.includes("ExperimentalWarning")) {
      console.error(`[${name} Error]`, msg);
    }
  });

  console.log(`Running for ${(runDurationMs / 1000).toFixed(0)} seconds...`);
  await sleep(runDurationMs);

  // Stop everything
  approachProcess.kill("SIGTERM");
  resultCollector.stop();
  const resources = resourceMonitor.stop();

  const results = resultCollector.getResults();
  const latencies = results.map((r) => r.latencyMs);
  const latencyStats = calculateStats(latencies);

  return {
    approach: name,
    windows: results,
    latency: latencyStats,
    resources,
  };
}

async function runClientSide(runDurationMs: number): Promise<BenchmarkResult> {
  return runApproach(
    "Client-Side Processing",
    path.resolve(
      __dirname,
      "../../src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts",
    ),
    "client_operation_output",
    runDurationMs,
  );
}

async function runChunked(runDurationMs: number): Promise<BenchmarkResult> {
  return runApproach(
    "Chunked Query",
    path.resolve(
      __dirname,
      "../../src/approaches/StreamingQueryChunkedApproachOrchestrator.ts",
    ),
    "output",
    runDurationMs,
  );
}

async function runApproximation(
  runDurationMs: number,
): Promise<BenchmarkResult> {
  return runApproach(
    "Approximation",
    path.resolve(
      __dirname,
      "../../src/approaches/StreamingQueryApproximationApproachOrchestrator.ts",
    ),
    "approximation/output",
    runDurationMs,
  );
}

// ============================================================================
// Results Output
// ============================================================================

function printResults(results: BenchmarkResult[]): void {
  console.log("\n" + "=".repeat(80));
  console.log("BENCHMARK RESULTS");
  console.log("=".repeat(80));

  console.log("\n--- Latency Results ---\n");
  console.log("| Approach                  | Latency (ms)         | Windows |");
  console.log("|---------------------------|----------------------|---------|");

  for (const result of results) {
    const latencyStr = formatLatency(result.latency);
    console.log(
      `| ${result.approach.padEnd(25)} | ${latencyStr.padEnd(20)} | ${result.windows.length.toString().padStart(7)} |`,
    );
  }

  console.log("\n--- Resource Usage ---\n");
  console.log(
    "| Approach                  | CPU %     | Memory (MB)         |",
  );
  console.log(
    "|---------------------------|-----------|---------------------|",
  );

  for (const result of results) {
    const memStr = `${result.resources.memoryMB.toFixed(2)} (peak: ${result.resources.peakMemoryMB.toFixed(2)})`;
    console.log(
      `| ${result.approach.padEnd(25)} | ${result.resources.cpuPercent.toFixed(2).padStart(8)} | ${memStr.padEnd(19)} |`,
    );
  }

  // Calculate accuracy if we have ground truth
  const groundTruth = results.find((r) =>
    r.approach.toLowerCase().includes("client"),
  );

  if (groundTruth && results.length > 1) {
    console.log("\n--- Accuracy vs Ground Truth ---\n");
    console.log("| Approach                  | Accuracy  |");
    console.log("|---------------------------|-----------|");

    for (const result of results) {
      if (result === groundTruth) {
        console.log(`| ${result.approach.padEnd(25)} | 100% (GT) |`);
        continue;
      }

      let matchCount = 0;
      let totalCompared = 0;

      for (const window of result.windows) {
        const gtWindow = groundTruth.windows.find(
          (w) => w.windowNumber === window.windowNumber,
        );
        if (gtWindow) {
          totalCompared++;
          const diff = Math.abs(window.resultValue - gtWindow.resultValue);
          const pctDiff =
            gtWindow.resultValue !== 0
              ? (diff / Math.abs(gtWindow.resultValue)) * 100
              : diff === 0
                ? 0
                : 100;

          if (pctDiff < 1 || diff < 0.01) {
            matchCount++;
          }
        }
      }

      const accuracy =
        totalCompared > 0
          ? ((matchCount / totalCompared) * 100).toFixed(1)
          : "N/A";
      console.log(
        `| ${result.approach.padEnd(25)} | ${accuracy.toString().padStart(7)}% |`,
      );
    }
  }

  console.log("\n" + "=".repeat(80));
}

function saveResults(results: BenchmarkResult[]): void {
  if (!fs.existsSync(CONFIG.outputDir)) {
    fs.mkdirSync(CONFIG.outputDir, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const filePath = path.join(CONFIG.outputDir, `benchmark_${timestamp}.json`);

  fs.writeFileSync(filePath, JSON.stringify(results, null, 2));
  console.log(`\nResults saved to: ${filePath}`);
}

// ============================================================================
// Main
// ============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const approach = args[0] || "all";

  console.log("============================================================");
  console.log("  Streaming Query Hive - Benchmark");
  console.log("  Latency = result_time - window_close_time");
  console.log("============================================================");

  // Calculate run duration based on data file size
  // Approximately 500 observations per file at 4Hz = 125 seconds
  // Plus buffer for window completion
  const estimatedDataDuration = (500 / CONFIG.dataFrequency) * 1000;
  const runDurationMs = estimatedDataDuration + CONFIG.windowWidthMs * 2;

  console.log(`\nConfiguration:`);
  console.log(`  Data Frequency: ${CONFIG.dataFrequency} Hz`);
  console.log(`  Window Width: ${CONFIG.windowWidthMs}ms`);
  console.log(`  Window Slide: ${CONFIG.windowSlideMs}ms`);
  console.log(
    `  Run Duration: ${(runDurationMs / 1000).toFixed(0)}s per approach`,
  );

  const results: BenchmarkResult[] = [];

  // Start data replayer
  const dataReplayer = new DataReplayer();

  try {
    switch (approach.toLowerCase()) {
      case "client-side":
      case "clientside":
      case "client":
        await dataReplayer.start();
        results.push(await runClientSide(runDurationMs));
        break;

      case "chunked":
        await dataReplayer.start();
        results.push(await runChunked(runDurationMs));
        break;

      case "approximation":
      case "approx":
        await dataReplayer.start();
        results.push(await runApproximation(runDurationMs));
        break;

      case "all":
      default: {
        // Run each approach separately with its own data replay
        console.log("\n[Benchmark] Running all approaches sequentially...\n");

        // Client-Side (Ground Truth)
        await dataReplayer.start();
        results.push(await runClientSide(runDurationMs));
        dataReplayer.stop();
        await sleep(5000); // Cooldown

        // Chunked
        const dataReplayer2 = new DataReplayer();
        await dataReplayer2.start();
        results.push(await runChunked(runDurationMs));
        dataReplayer2.stop();
        await sleep(5000);

        // Approximation
        const dataReplayer3 = new DataReplayer();
        await dataReplayer3.start();
        results.push(await runApproximation(runDurationMs));
        dataReplayer3.stop();
        break;
      }
    }
  } catch (error) {
    console.error("Benchmark error:", error);
  }

  dataReplayer.stop();

  // Print and save results
  if (results.length > 0) {
    printResults(results);
    saveResults(results);
  }

  // Compare with August benchmark
  console.log("\n--- August 2024 Benchmark (Reference) ---\n");
  console.log(
    "| Approach                  | Latency (ms)         | Accuracy |",
  );
  console.log(
    "|---------------------------|----------------------|----------|",
  );
  console.log(
    "| Chunked Query             | 414ms +/- 12.3ms     | 100%     |",
  );
  console.log(
    "| Approximation             | 359ms +/- 31.2ms     | 89.5%    |",
  );
  console.log(
    "| Client-Side Processing    | 2543ms +/- 213.3ms   | 100% (GT)|",
  );
  console.log("");

  process.exit(0);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
