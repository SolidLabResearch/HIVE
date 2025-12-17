/**
 * Approach Comparison Experiment
 *
 * This experiment compares the three streaming query processing approaches:
 * 1. Client-Side Processing (Fetching all data client-side)
 * 2. Chunked Query Approach (StreamingQueryChunkAggregatorOperator)
 * 3. Approximation Approach (RateBasedApproximationApproachOperator)
 *
 * Latency Definition:
 * Latency = result_available_time - window_close_time
 * Where window_close_time = query_registered_time + window_width
 *
 * This matches the August 2024 benchmark methodology.
 */

import * as fs from "fs";
import * as path from "path";
import * as mqtt from "mqtt";
import { spawn, ChildProcess } from "child_process";

// ============================================================================
// Configuration
// ============================================================================

interface ExperimentConfig {
  mqttBroker: string;
  dataFrequency: number; // Hz - data publish frequency
  windowWidthMs: number; // Main query window width
  windowSlideMs: number; // Main query window slide
  subQueryWindowWidthMs: number;
  subQueryWindowSlideMs: number;
  iterations: number; // Number of complete data replays
  dataPath: string;
  warmupMs: number; // Warmup period before recording results
}

interface WindowResult {
  windowNumber: number;
  queryRegisteredTime: number;
  windowCloseTime: number;
  resultAvailableTime: number;
  latencyMs: number;
  resultValue: number;
}

interface ApproachResults {
  approach: string;
  results: WindowResult[];
  avgLatencyMs: number;
  stdDevLatencyMs: number;
  minLatencyMs: number;
  maxLatencyMs: number;
  resourceUsage: ResourceUsage;
}

interface ResourceUsage {
  avgCpuPercent: number;
  avgMemoryMB: number;
  peakMemoryMB: number;
}

// ============================================================================
// Default Configuration
// ============================================================================

const DEFAULT_CONFIG: ExperimentConfig = {
  mqttBroker: "mqtt://localhost:1883",
  dataFrequency: 4, // 4 Hz as per your August experiment
  windowWidthMs: 120000, // 2 minutes
  windowSlideMs: 60000, // 1 minute
  subQueryWindowWidthMs: 60000, // 1 minute
  subQueryWindowSlideMs: 30000, // 30 seconds
  iterations: 1,
  dataPath: path.resolve(__dirname, "../../src/streamer/data"),
  warmupMs: 5000, // 5 second warmup
};

// ============================================================================
// Queries (matching your August experiment)
// ============================================================================

// Queries are defined in the approach orchestrators - these are for reference only
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const _MAIN_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
`;

// ============================================================================
// CSV Logger for Results
// ============================================================================

class ExperimentLogger {
  private outputDir: string;
  private timestamp: string;

  constructor(outputDir: string) {
    this.outputDir = outputDir;
    this.timestamp = new Date().toISOString().replace(/[:.]/g, "-");

    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
  }

  logResults(approach: string, results: WindowResult[]): void {
    const filePath = path.join(
      this.outputDir,
      `${approach}_results_${this.timestamp}.csv`,
    );
    const header =
      "window_number,query_registered_time,window_close_time,result_available_time,latency_ms,result_value\n";

    const rows = results
      .map(
        (r) =>
          `${r.windowNumber},${r.queryRegisteredTime},${r.windowCloseTime},${r.resultAvailableTime},${r.latencyMs},${r.resultValue}`,
      )
      .join("\n");

    fs.writeFileSync(filePath, header + rows + "\n");
    console.log(`Results written to: ${filePath}`);
  }

  logSummary(allResults: ApproachResults[]): void {
    const summaryPath = path.join(
      this.outputDir,
      `summary_${this.timestamp}.csv`,
    );
    const header =
      "approach,avg_latency_ms,std_dev_ms,min_latency_ms,max_latency_ms,avg_cpu_percent,avg_memory_mb,peak_memory_mb,num_windows\n";

    const rows = allResults
      .map(
        (r) =>
          `${r.approach},${r.avgLatencyMs.toFixed(2)},${r.stdDevLatencyMs.toFixed(2)},${r.minLatencyMs.toFixed(2)},${r.maxLatencyMs.toFixed(2)},${r.resourceUsage.avgCpuPercent.toFixed(2)},${r.resourceUsage.avgMemoryMB.toFixed(2)},${r.resourceUsage.peakMemoryMB.toFixed(2)},${r.results.length}`,
      )
      .join("\n");

    fs.writeFileSync(summaryPath, header + rows + "\n");
    console.log(`Summary written to: ${summaryPath}`);
  }

  logAccuracy(
    groundTruth: WindowResult[],
    approachResults: Map<string, WindowResult[]>,
  ): void {
    const accuracyPath = path.join(
      this.outputDir,
      `accuracy_${this.timestamp}.csv`,
    );
    const header =
      "approach,window_number,ground_truth_value,approach_value,absolute_error,percentage_error\n";

    const rows: string[] = [];

    for (const [approach, results] of approachResults.entries()) {
      for (const result of results) {
        const gtMatch = groundTruth.find(
          (gt) => gt.windowNumber === result.windowNumber,
        );
        if (gtMatch) {
          const absoluteError = Math.abs(
            result.resultValue - gtMatch.resultValue,
          );
          const percentageError =
            gtMatch.resultValue !== 0
              ? (absoluteError / Math.abs(gtMatch.resultValue)) * 100
              : absoluteError === 0
                ? 0
                : 100;

          rows.push(
            `${approach},${result.windowNumber},${gtMatch.resultValue},${result.resultValue},${absoluteError.toFixed(6)},${percentageError.toFixed(2)}`,
          );
        }
      }
    }

    fs.writeFileSync(accuracyPath, header + rows.join("\n") + "\n");
    console.log(`Accuracy written to: ${accuracyPath}`);
  }
}

// ============================================================================
// Resource Monitor
// ============================================================================

class ResourceMonitor {
  private samples: {
    timestamp: number;
    cpuUser: number;
    cpuSystem: number;
    memoryMB: number;
  }[] = [];
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private startCpuUsage: { user: number; system: number } | null = null;
  private startTime: number = 0;

  start(intervalMs: number = 100): void {
    this.samples = [];
    this.startCpuUsage = process.cpuUsage();
    this.startTime = Date.now();

    this.intervalId = setInterval(() => {
      const mem = process.memoryUsage();
      const cpu = process.cpuUsage(this.startCpuUsage!);
      const elapsedMs = Date.now() - this.startTime;

      this.samples.push({
        timestamp: Date.now(),
        cpuUser: cpu.user / 1000,
        cpuSystem: cpu.system / 1000,
        memoryMB: mem.heapUsed / 1024 / 1024,
      });
    }, intervalMs);
  }

  stop(): ResourceUsage {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    if (this.samples.length === 0) {
      return { avgCpuPercent: 0, avgMemoryMB: 0, peakMemoryMB: 0 };
    }

    const totalCpuTime = this.samples.reduce(
      (sum, s) => sum + s.cpuUser + s.cpuSystem,
      0,
    );
    const elapsedMs = Date.now() - this.startTime;
    const avgCpuPercent = elapsedMs > 0 ? (totalCpuTime / elapsedMs) * 100 : 0;

    const avgMemoryMB =
      this.samples.reduce((sum, s) => sum + s.memoryMB, 0) /
      this.samples.length;
    const peakMemoryMB = Math.max(...this.samples.map((s) => s.memoryMB));

    return { avgCpuPercent, avgMemoryMB, peakMemoryMB };
  }
}

// ============================================================================
// Data Publisher (using StreamToMQTT)
// ============================================================================

class DataPublisher {
  private process: ChildProcess | null = null;
  private config: ExperimentConfig;

  constructor(config: ExperimentConfig) {
    this.config = config;
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      // Check if replayer script exists, if not use inline approach
      console.log("[DataPublisher] Starting data replay...");

      // We'll use ts-node to run the StreamToMQTT directly
      this.process = spawn(
        "npx",
        [
          "ts-node",
          "-e",
          `
                const { StreamToMQTT } = require('${path.resolve(__dirname, "../../src/streamer/src/publishing/StreamToMQTT").replace(/\\/g, "\\\\")}');

                async function main() {
                    const wearable = new StreamToMQTT(
                        '${this.config.mqttBroker}',
                        ${this.config.dataFrequency},
                        '${path.join(this.config.dataPath, "wearable.acceleration.x", "data.nt").replace(/\\/g, "\\\\")}',
                        'wearableX'
                    );

                    const smartphone = new StreamToMQTT(
                        '${this.config.mqttBroker}',
                        ${this.config.dataFrequency},
                        '${path.join(this.config.dataPath, "smartphone.acceleration.x", "data.nt").replace(/\\/g, "\\\\")}',
                        'smartphoneX'
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

      this.process.stdout?.on("data", (data) => {
        const msg = data.toString();
        if (msg.includes("Published observation")) {
          // Data is flowing
        }
      });

      this.process.stderr?.on("data", (data) => {
        console.error("[DataPublisher Error]", data.toString());
      });

      // Give it time to initialize
      setTimeout(resolve, 2000);
    });
  }

  stop(): void {
    if (this.process) {
      this.process.kill("SIGTERM");
      this.process = null;
    }
  }
}

// ============================================================================
// Result Collector - Listens for results on MQTT topics
// ============================================================================

class ResultCollector {
  private mqttClient: mqtt.MqttClient;
  private results: WindowResult[] = [];
  private queryRegisteredTime: number = 0;
  private windowNumber: number = 0;
  private config: ExperimentConfig;
  private topic: string;
  private isCollecting: boolean = false;
  private warmupComplete: boolean = false;

  constructor(config: ExperimentConfig, topic: string) {
    this.config = config;
    this.topic = topic;
    this.mqttClient = mqtt.connect(config.mqttBroker, {
      clientId: `result-collector-${topic}-${Date.now()}`,
      clean: true,
    });
  }

  async start(queryRegisteredTime: number): Promise<void> {
    this.queryRegisteredTime = queryRegisteredTime;
    this.results = [];
    this.windowNumber = 0;
    this.isCollecting = true;
    this.warmupComplete = false;

    return new Promise((resolve) => {
      this.mqttClient.on("connect", () => {
        console.log(
          `[ResultCollector] Connected, subscribing to ${this.topic}`,
        );
        this.mqttClient.subscribe(this.topic, { qos: 1 }, (err) => {
          if (err) {
            console.error(`[ResultCollector] Subscribe error:`, err);
          }
          resolve();
        });
      });

      this.mqttClient.on("message", (topic, message) => {
        if (!this.isCollecting) return;

        const resultAvailableTime = Date.now();

        // Skip results during warmup period
        if (!this.warmupComplete) {
          if (
            resultAvailableTime - this.queryRegisteredTime <
            this.config.warmupMs
          ) {
            console.log(`[ResultCollector] Skipping warmup result`);
            return;
          }
          this.warmupComplete = true;
          // Reset the query registered time to after warmup for accurate latency
          this.queryRegisteredTime = Date.now();
        }

        this.windowNumber++;

        // Calculate when this window should have closed
        // Window N closes at: queryRegisteredTime + (N * windowSlideMs) + windowWidthMs
        // But for sliding windows, the first window closes at queryRegisteredTime + windowWidthMs
        // Subsequent windows close every windowSlideMs thereafter
        const windowCloseTime =
          this.queryRegisteredTime +
          this.windowNumber * this.config.windowSlideMs;

        // Latency = when result appeared - when window should have closed
        const latencyMs = resultAvailableTime - windowCloseTime;

        // Parse result value
        let resultValue = 0;
        try {
          const msgStr = message.toString();

          // Try JSON format first
          if (msgStr.startsWith("{")) {
            const parsed = JSON.parse(msgStr);
            resultValue =
              parsed.unifiedResult ??
              parsed.unifiedAverage ??
              parsed.value ??
              0;
          } else {
            // Try RDF format: hasValue> "number"
            const valueMatch =
              msgStr.match(/hasValue>\s*"([^"]*)"/) ||
              msgStr.match(/"(-?\d+\.?\d*)"/);
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
          `[ResultCollector] Window ${this.windowNumber}: value=${resultValue.toFixed(4)}, latency=${latencyMs}ms`,
        );
      });
    });
  }

  getResults(): WindowResult[] {
    return this.results;
  }

  stop(): void {
    this.isCollecting = false;
    this.mqttClient.end();
  }
}

// ============================================================================
// Approach Runner - Runs each approach as a separate process
// ============================================================================

class ApproachRunner {
  private config: ExperimentConfig;
  private logger: ExperimentLogger;

  constructor(config: ExperimentConfig, logger: ExperimentLogger) {
    this.config = config;
    this.logger = logger;
  }

  async runClientSideApproach(): Promise<ApproachResults> {
    console.log("\n" + "=".repeat(60));
    console.log("Running: Client-Side Processing Approach");
    console.log("=".repeat(60));

    const resourceMonitor = new ResourceMonitor();
    const resultCollector = new ResultCollector(
      this.config,
      "client_operation_output",
    );

    // Start resource monitoring
    resourceMonitor.start(100);

    // Start the client-side approach as a subprocess
    const approachProcess = spawn(
      "npx",
      [
        "ts-node",
        path.resolve(
          __dirname,
          "../../src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts",
        ),
      ],
      {
        cwd: path.resolve(__dirname, "../.."),
        stdio: ["pipe", "pipe", "pipe"],
      },
    );

    const queryRegisteredTime = Date.now();
    await resultCollector.start(queryRegisteredTime);

    // Let it run for the duration of data replay + some buffer
    // With 4Hz and ~500 data points per stream, that's ~125 seconds per stream
    const runDurationMs = this.calculateRunDuration();
    console.log(`[ClientSide] Running for ${runDurationMs / 1000} seconds...`);

    approachProcess.stdout?.on("data", () => {
      // Result generated - silent unless debugging
    });

    approachProcess.stderr?.on("data", (data) => {
      // Only log actual errors, not debug info
      const msg = data.toString();
      if (msg.includes("Error") || msg.includes("error")) {
        console.error("[ClientSide Error]", msg);
      }
    });

    await this.sleep(runDurationMs);

    // Stop everything
    approachProcess.kill("SIGTERM");
    resultCollector.stop();
    const resourceUsage = resourceMonitor.stop();

    const results = resultCollector.getResults();
    const stats = this.calculateStats(results);

    return {
      approach: "client_side_processing",
      results,
      ...stats,
      resourceUsage,
    };
  }

  async runChunkedApproach(): Promise<ApproachResults> {
    console.log("\n" + "=".repeat(60));
    console.log("Running: Chunked Query Approach");
    console.log("=".repeat(60));

    const resourceMonitor = new ResourceMonitor();
    // Chunked approach publishes to both 'output' and 'chunked/output' topics
    const resultCollector = new ResultCollector(this.config, "output");

    resourceMonitor.start(100);

    const approachProcess = spawn(
      "npx",
      [
        "ts-node",
        path.resolve(
          __dirname,
          "../../src/approaches/StreamingQueryChunkedApproachOrchestrator.ts",
        ),
      ],
      {
        cwd: path.resolve(__dirname, "../.."),
        stdio: ["pipe", "pipe", "pipe"],
      },
    );

    const queryRegisteredTime = Date.now();
    await resultCollector.start(queryRegisteredTime);

    const runDurationMs = this.calculateRunDuration();
    console.log(`[Chunked] Running for ${runDurationMs / 1000} seconds...`);

    approachProcess.stdout?.on("data", (data) => {
      // Silent unless debugging
    });

    approachProcess.stderr?.on("data", (data) => {
      const msg = data.toString();
      if (msg.includes("Error") || msg.includes("error")) {
        console.error("[Chunked Error]", msg);
      }
    });

    await this.sleep(runDurationMs);

    approachProcess.kill("SIGTERM");
    resultCollector.stop();
    const resourceUsage = resourceMonitor.stop();

    const results = resultCollector.getResults();
    const stats = this.calculateStats(results);

    return {
      approach: "chunked_query",
      results,
      ...stats,
      resourceUsage,
    };
  }

  async runApproximationApproach(): Promise<ApproachResults> {
    console.log("\n" + "=".repeat(60));
    console.log("Running: Approximation Approach");
    console.log("=".repeat(60));

    const resourceMonitor = new ResourceMonitor();
    const resultCollector = new ResultCollector(
      this.config,
      "approximation/output",
    );

    resourceMonitor.start(100);

    const approachProcess = spawn(
      "npx",
      [
        "ts-node",
        path.resolve(
          __dirname,
          "../../src/approaches/StreamingQueryApproximationApproachOrchestrator.ts",
        ),
      ],
      {
        cwd: path.resolve(__dirname, "../.."),
        stdio: ["pipe", "pipe", "pipe"],
      },
    );

    const queryRegisteredTime = Date.now();
    await resultCollector.start(queryRegisteredTime);

    const runDurationMs = this.calculateRunDuration();
    console.log(
      `[Approximation] Running for ${runDurationMs / 1000} seconds...`,
    );

    approachProcess.stdout?.on("data", () => {
      // Silent unless debugging
    });

    approachProcess.stderr?.on("data", (data) => {
      const msg = data.toString();
      if (msg.includes("Error") || msg.includes("error")) {
        console.error("[Approximation Error]", msg);
      }
    });

    await this.sleep(runDurationMs);

    approachProcess.kill("SIGTERM");
    resultCollector.stop();
    const resourceUsage = resourceMonitor.stop();

    const results = resultCollector.getResults();
    const stats = this.calculateStats(results);

    return {
      approach: "approximation",
      results,
      ...stats,
      resourceUsage,
    };
  }

  private calculateRunDuration(): number {
    // Estimate based on data file size and frequency
    // Each data file has approximately 500 observations
    // At 4Hz, that's 500/4 = 125 seconds
    // Add buffer for startup and final windows
    const estimatedDataDurationMs = (500 / this.config.dataFrequency) * 1000;
    const bufferMs = this.config.windowWidthMs * 2; // Wait for final windows
    return estimatedDataDurationMs + bufferMs + this.config.warmupMs;
  }

  private calculateStats(results: WindowResult[]): {
    avgLatencyMs: number;
    stdDevLatencyMs: number;
    minLatencyMs: number;
    maxLatencyMs: number;
  } {
    if (results.length === 0) {
      return {
        avgLatencyMs: 0,
        stdDevLatencyMs: 0,
        minLatencyMs: 0,
        maxLatencyMs: 0,
      };
    }

    const latencies = results.map((r) => r.latencyMs);
    const avgLatencyMs =
      latencies.reduce((a, b) => a + b, 0) / latencies.length;

    const squaredDiffs = latencies.map((l) => Math.pow(l - avgLatencyMs, 2));
    const stdDevLatencyMs = Math.sqrt(
      squaredDiffs.reduce((a, b) => a + b, 0) / latencies.length,
    );

    const minLatencyMs = Math.min(...latencies);
    const maxLatencyMs = Math.max(...latencies);

    return { avgLatencyMs, stdDevLatencyMs, minLatencyMs, maxLatencyMs };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// Main Experiment Class
// ============================================================================

class ApproachComparisonExperiment {
  private config: ExperimentConfig;
  private logger: ExperimentLogger;
  private runner: ApproachRunner;

  constructor(
    outputDir: string = "./experiments/approach-comparison/results",
    config?: Partial<ExperimentConfig>,
  ) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.logger = new ExperimentLogger(outputDir);
    this.runner = new ApproachRunner(this.config, this.logger);
  }

  async run(): Promise<void> {
    console.log("\n" + "=".repeat(60));
    console.log("APPROACH COMPARISON EXPERIMENT");
    console.log("=".repeat(60));
    console.log(`Configuration:`);
    console.log(`  MQTT Broker: ${this.config.mqttBroker}`);
    console.log(`  Data Frequency: ${this.config.dataFrequency} Hz`);
    console.log(`  Window Width: ${this.config.windowWidthMs}ms`);
    console.log(`  Window Slide: ${this.config.windowSlideMs}ms`);
    console.log(`  Warmup Period: ${this.config.warmupMs}ms`);
    console.log("");

    const allResults: ApproachResults[] = [];
    const approachResultsMap = new Map<string, WindowResult[]>();

    // Start data publisher that will be shared across runs
    // Note: In the original August experiment, each approach was run separately
    // with its own data replay. We'll do the same here.

    try {
      // Run Client-Side Processing (Ground Truth)
      console.log(
        "\n[Experiment] Starting Client-Side Processing (Ground Truth)...",
      );
      const dataPublisher1 = new DataPublisher(this.config);
      await dataPublisher1.start();

      const clientSideResults = await this.runner.runClientSideApproach();
      allResults.push(clientSideResults);
      approachResultsMap.set(
        "client_side_processing",
        clientSideResults.results,
      );
      this.logger.logResults(
        "client_side_processing",
        clientSideResults.results,
      );

      dataPublisher1.stop();
      await this.sleep(5000); // Cooldown between approaches

      // Run Chunked Query Approach
      console.log("\n[Experiment] Starting Chunked Query Approach...");
      const dataPublisher2 = new DataPublisher(this.config);
      await dataPublisher2.start();

      const chunkedResults = await this.runner.runChunkedApproach();
      allResults.push(chunkedResults);
      approachResultsMap.set("chunked_query", chunkedResults.results);
      this.logger.logResults("chunked_query", chunkedResults.results);

      dataPublisher2.stop();
      await this.sleep(5000);

      // Run Approximation Approach
      console.log("\n[Experiment] Starting Approximation Approach...");
      const dataPublisher3 = new DataPublisher(this.config);
      await dataPublisher3.start();

      const approximationResults = await this.runner.runApproximationApproach();
      allResults.push(approximationResults);
      approachResultsMap.set("approximation", approximationResults.results);
      this.logger.logResults("approximation", approximationResults.results);

      dataPublisher3.stop();
    } catch (error) {
      console.error("[Experiment] Error:", error);
    }

    // Log summary and accuracy
    this.logger.logSummary(allResults);

    const groundTruth = approachResultsMap.get("client_side_processing") || [];
    if (groundTruth.length > 0) {
      const otherApproaches = new Map<string, WindowResult[]>();
      otherApproaches.set(
        "chunked_query",
        approachResultsMap.get("chunked_query") || [],
      );
      otherApproaches.set(
        "approximation",
        approachResultsMap.get("approximation") || [],
      );
      this.logger.logAccuracy(groundTruth, otherApproaches);
    }

    // Print summary to console
    this.printSummary(allResults, groundTruth, approachResultsMap);
  }

  private printSummary(
    allResults: ApproachResults[],
    groundTruth: WindowResult[],
    approachResultsMap: Map<string, WindowResult[]>,
  ): void {
    console.log("\n" + "=".repeat(80));
    console.log("EXPERIMENT RESULTS SUMMARY");
    console.log("=".repeat(80));

    console.log("\n--- Latency Results ---\n");
    console.log(
      "| Approach                | Latency (ms)      | CPU %  | Memory (MB)      |",
    );
    console.log(
      "|-------------------------|-------------------|--------|------------------|",
    );

    for (const result of allResults) {
      const latencyStr = `${result.avgLatencyMs.toFixed(0)} +/- ${result.stdDevLatencyMs.toFixed(1)}`;
      const memStr = `${result.resourceUsage.avgMemoryMB.toFixed(2)} +/- ${(result.resourceUsage.peakMemoryMB - result.resourceUsage.avgMemoryMB).toFixed(1)}`;
      console.log(
        `| ${result.approach.padEnd(23)} | ${latencyStr.padEnd(17)} | ${result.resourceUsage.avgCpuPercent.toFixed(2).padStart(5)} | ${memStr.padEnd(16)} |`,
      );
    }

    console.log("\n--- Accuracy Results ---\n");
    console.log("| Approach                | Windows | Accuracy |");
    console.log("|-------------------------|---------|----------|");

    for (const [approach, results] of approachResultsMap.entries()) {
      if (approach === "client_side_processing") {
        console.log(
          `| ${approach.padEnd(23)} | ${results.length.toString().padStart(7)} | 100% (GT)|`,
        );
        continue;
      }

      let matchCount = 0;
      for (const result of results) {
        const gtMatch = groundTruth.find(
          (gt) => gt.windowNumber === result.windowNumber,
        );
        if (gtMatch) {
          // Consider a match if within 0.01% or absolute difference < 0.001
          const diff = Math.abs(result.resultValue - gtMatch.resultValue);
          const pctDiff =
            gtMatch.resultValue !== 0
              ? (diff / Math.abs(gtMatch.resultValue)) * 100
              : diff === 0
                ? 0
                : 100;
          if (pctDiff < 0.01 || diff < 0.001) {
            matchCount++;
          }
        }
      }

      const accuracy =
        results.length > 0
          ? (matchCount / Math.min(results.length, groundTruth.length)) * 100
          : 0;
      console.log(
        `| ${approach.padEnd(23)} | ${results.length.toString().padStart(7)} | ${accuracy.toFixed(1).padStart(6)}%  |`,
      );
    }

    console.log("\n" + "=".repeat(80));
    console.log("Experiment complete. Results saved to output directory.");
    console.log("=".repeat(80) + "\n");
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// CLI Entry Point
// ============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const outputDir = args[0] || "./experiments/approach-comparison/results";

  // Parse optional config from command line
  const config: Partial<ExperimentConfig> = {};

  for (let i = 1; i < args.length; i += 2) {
    const key = args[i]?.replace("--", "");
    const value = args[i + 1];

    if (key && value) {
      switch (key) {
        case "frequency":
          config.dataFrequency = parseInt(value);
          break;
        case "warmup":
          config.warmupMs = parseInt(value);
          break;
        case "iterations":
          config.iterations = parseInt(value);
          break;
      }
    }
  }

  const experiment = new ApproachComparisonExperiment(outputDir, config);
  await experiment.run();
}

main().catch((error) => {
  console.error("Experiment failed:", error);
  process.exit(1);
});
