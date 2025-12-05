#!/usr/bin/env ts-node

/**
 * Real-World Frequency Experiment Runner
 *
 * This script runs experiments using the actual MQTT-based streaming infrastructure.
 * It starts the data replayer and the processing approaches, measuring real performance.
 */

import * as fs from "fs";
import * as path from "path";
import { spawn, ChildProcess } from "child_process";
import { performance } from "perf_hooks";
import * as mqtt from "mqtt";

interface ExperimentConfig {
  experiment: {
    name: string;
    description: string;
    timestamp: string;
    iterations: number;
  };
  frequencies: string[];
  deviceTypes: string[];
  approaches: string[];
  dataBasePath: string;
  outputPath: string;
  queries: any;
  metrics: any;
}

interface ExperimentResult {
  approach: string;
  frequency: string;
  deviceType: string;
  iteration: number;
  timestamp: string;
  metrics: {
    latency: number;
    memoryUsage: number;
    accuracy?: number;
    throughput: number;
    observationsProcessed: number;
    executionTime: number;
    publishedObservations: number;
    receivedResults: number;
  };
  queryResults: any[];
  publisherStats: any;
  error?: string;
}

class RealWorldFrequencyExperimentRunner {
  private readonly projectRoot: string;
  private readonly config: ExperimentConfig;
  private readonly results: ExperimentResult[] = [];
  private groundTruthResults: Map<string, any[]> = new Map();
  private mqttBroker = "mqtt://localhost:1883";

  constructor() {
    this.projectRoot = path.resolve(__dirname, "../..");

    // Load configuration
    const configPath = path.join(
      this.projectRoot,
      "scripts/benchmarks/frequency-experiment-config.json",
    );
    if (!fs.existsSync(configPath)) {
      throw new Error("Experiment configuration not found. Run setup first.");
    }

    this.config = JSON.parse(fs.readFileSync(configPath, "utf8"));
  }

  /**
   * Run the complete frequency experiment suite
   */
  public async runExperiments(): Promise<void> {
    console.log(
      "Starting Real-World Frequency-Based Streaming Query Experiments",
    );
    console.log("=".repeat(75));
    console.log(`Experiment: ${this.config.experiment.name}`);
    console.log(`Iterations: ${this.config.experiment.iterations}`);
    console.log(`Frequencies: ${this.config.frequencies.join(", ")}`);
    console.log(`Device Types: ${this.config.deviceTypes.join(", ")}`);
    console.log(`Approaches: ${this.config.approaches.join(", ")}`);
    console.log("");

    const startTime = performance.now();

    try {
      // Step 1: Build the project
      console.log("Building project...");
      await this.buildProject();

      // Step 2: Generate ground truth (fetching-client-side approach)
      console.log(
        "\nGenerating ground truth with fetching-client-side approach...",
      );
      await this.generateGroundTruth();

      // Step 3: Run experiments for all approaches
      console.log("\nRunning experiments for all approaches...");
      await this.runAllApproaches();

      // Step 4: Save results
      console.log("\nSaving experiment results...");
      await this.saveResults();

      const endTime = performance.now();
      const totalTime = (endTime - startTime) / 1000;

      console.log("\n" + "=".repeat(75));
      console.log("All experiments completed successfully!");
      console.log(`Total execution time: ${totalTime.toFixed(2)} seconds`);
      console.log(`Total experiments run: ${this.results.length}`);
      console.log(`Results saved to: ${this.config.outputPath}`);
    } catch (error) {
      console.error("\nExperiment failed:", error);
      await this.saveResults(); // Save partial results
      throw error;
    }
  }

  /**
   * Build the TypeScript project
   */
  private async buildProject(): Promise<void> {
    return new Promise((resolve, reject) => {
      const buildProcess = spawn("npm", ["run", "build"], {
        cwd: this.projectRoot,
        stdio: "pipe",
      });

      let buildOutput = "";
      buildProcess.stdout.on("data", (data) => {
        buildOutput += data.toString();
      });

      buildProcess.stderr.on("data", (data) => {
        buildOutput += data.toString();
      });

      buildProcess.on("close", (code) => {
        if (code === 0) {
          console.log("Project built successfully");
          resolve();
        } else {
          console.error("Build failed:", buildOutput);
          reject(new Error(`Build failed with code ${code}`));
        }
      });
    });
  }

  /**
   * Generate ground truth using fetching-client-side approach
   */
  private async generateGroundTruth(): Promise<void> {
    console.log(
      "Generating ground truth (fetching-client-side) for all frequencies with combined data...",
    );

    for (const frequency of this.config.frequencies) {
      const key = `${frequency}-combined`;
      console.log(
        `  Processing ${frequency} (smartphone + wearable combined)...`,
      );

      try {
        const result = await this.runSingleRealExperiment(
          "fetching-client-side",
          frequency,
          "combined",
          1,
        );

        if (result && !result.error && result.queryResults.length > 0) {
          this.groundTruthResults.set(key, result.queryResults);
          console.log(
            `    Ground truth generated for ${key} (${result.queryResults.length} results)`,
          );
        } else {
          console.log(
            `    Failed to generate ground truth for ${key}: ${result?.error || "No results"}`,
          );
        }
      } catch (error) {
        console.log(`    Error generating ground truth for ${key}: ${error}`);
      }

      // Wait between experiments to avoid conflicts
      await this.sleep(2000);
    }

    console.log(
      `Ground truth generated for ${this.groundTruthResults.size} frequency combinations`,
    );
  }

  /**
   * Run experiments for all approaches across all frequencies with combined device data
   */
  private async runAllApproaches(): Promise<void> {
    const totalExperiments =
      this.config.approaches.length *
      this.config.frequencies.length *
      this.config.experiment.iterations;

    console.log(`Running ${totalExperiments} total experiments...`);
    console.log(
      `Each experiment uses combined smartphone + wearable data streaming`,
    );

    let completedExperiments = 0;

    for (const approach of this.config.approaches) {
      console.log(`\n--- Running ${approach} approach ---`);

      for (const frequency of this.config.frequencies) {
        console.log(`  ${frequency} (smartphone + wearable combined):`);

        for (
          let iteration = 1;
          iteration <= this.config.experiment.iterations;
          iteration++
        ) {
          try {
            const result = await this.runSingleRealExperiment(
              approach,
              frequency,
              "combined",
              iteration,
            );

            if (result) {
              // Calculate accuracy against ground truth
              const groundTruthKey = `${frequency}-combined`;
              const groundTruth = this.groundTruthResults.get(groundTruthKey);

              if (groundTruth && result.queryResults.length > 0) {
                result.metrics.accuracy = this.calculateAccuracy(
                  groundTruth,
                  result.queryResults,
                );
              }

              this.results.push(result);
              console.log(
                `    Iteration ${iteration}: [OK] (${result.metrics.latency.toFixed(2)}ms, ${result.metrics.memoryUsage.toFixed(2)}MB, ${result.queryResults.length} results)`,
              );
            } else {
              console.log(
                `    Iteration ${iteration}: [FAIL] No result returned`,
              );
            }
          } catch (error) {
            console.log(`    Iteration ${iteration}: [FAIL] ${error}`);

            // Record failed experiment
            this.results.push({
              approach,
              frequency,
              deviceType: "combined",
              iteration,
              timestamp: new Date().toISOString(),
              metrics: {
                latency: -1,
                memoryUsage: -1,
                throughput: -1,
                observationsProcessed: -1,
                executionTime: -1,
                publishedObservations: -1,
                receivedResults: -1,
              },
              queryResults: [],
              publisherStats: null,
              error: String(error),
            });
          }

          completedExperiments++;
          const progress = (
            (completedExperiments / totalExperiments) *
            100
          ).toFixed(1);
          process.stdout.write(
            `\r    Progress: ${progress}% (${completedExperiments}/${totalExperiments})`,
          );

          // Wait between experiments to avoid conflicts
          await this.sleep(3000);
        }
        console.log(); // New line after progress
      }
    }
  }

  /**
   * Run a single experiment with real MQTT infrastructure
   */
  private async runSingleRealExperiment(
    approach: string,
    frequency: string,
    deviceType: string,
    iteration: number,
  ): Promise<ExperimentResult | null> {
    const startTime = performance.now();
    const startMemory = process.memoryUsage().heapUsed;

    try {
      // Set up data paths for this frequency and device type
      let dataPaths: string[];
      if (deviceType === "combined") {
        // For combined, use both smartphone and wearable data
        dataPaths = [
          `frequency_variants/2mins/smartphone/${frequency}`,
          `frequency_variants/2mins/wearable/${frequency}`,
        ];
      } else {
        // Single device type
        dataPaths = [`frequency_variants/2mins/${deviceType}/${frequency}`];
      }

      // Start the approach process
      const approachProcess = await this.startApproachProcess(approach);

      // Set up result collection
      const queryResults: any[] = [];
      const resultTopic = this.getResultTopic(approach);

      const resultCollector = await this.setupResultCollector(
        resultTopic,
        queryResults,
      );

      // Start the data publishers (one or more depending on device type)
      const publisherProcesses: ChildProcess[] = [];
      for (const dataPath of dataPaths) {
        const publisherProcess = await this.startPublisherProcess(dataPath);
        publisherProcesses.push(publisherProcess);
      }

      // Wait for experiment to complete
      const experimentDuration = 30000; // 30 seconds
      await this.sleep(experimentDuration);

      // Get publisher statistics
      const publisherStats = await this.getPublisherStats();

      // Clean up processes
      await this.cleanupProcesses([approachProcess, ...publisherProcesses]);
      resultCollector.end();

      const endTime = performance.now();
      const endMemory = process.memoryUsage().heapUsed;

      const executionTime = endTime - startTime;
      const memoryUsage = Math.max(0, (endMemory - startMemory) / 1024 / 1024); // Convert to MB
      const observationsProcessed = publisherStats?.successful || 0;
      const throughput = observationsProcessed / (experimentDuration / 1000); // obs/second

      return {
        approach,
        frequency,
        deviceType,
        iteration,
        timestamp: new Date().toISOString(),
        metrics: {
          latency:
            queryResults.length > 0
              ? executionTime / queryResults.length
              : executionTime,
          memoryUsage,
          throughput,
          observationsProcessed,
          executionTime,
          publishedObservations: publisherStats?.intended || 0,
          receivedResults: queryResults.length,
        },
        queryResults,
        publisherStats,
      };
    } catch (error) {
      const endTime = performance.now();
      const executionTime = endTime - startTime;

      return {
        approach,
        frequency,
        deviceType,
        iteration,
        timestamp: new Date().toISOString(),
        metrics: {
          latency: executionTime,
          memoryUsage: -1,
          throughput: -1,
          observationsProcessed: -1,
          executionTime,
          publishedObservations: -1,
          receivedResults: -1,
        },
        queryResults: [],
        publisherStats: null,
        error: String(error),
      };
    }
  }

  /**
   * Start approach process
   */
  private async startApproachProcess(approach: string): Promise<ChildProcess> {
    const approachScripts = {
      "fetching-client-side":
        "dist/approaches/FetchingClientSideApproachOrchestrator.js",
      "chunked-query-approach":
        "dist/approaches/ChunkedQueryApproachOrchestrator.js",
      "approximation-approach":
        "dist/approaches/ApproximationApproachOrchestrator.js",
    };

    const scriptPath =
      approachScripts[approach as keyof typeof approachScripts];
    if (!scriptPath) {
      throw new Error(`Unknown approach: ${approach}`);
    }

    const approachProcess = spawn("node", [scriptPath], {
      cwd: this.projectRoot,
      stdio: "pipe",
      env: { ...process.env },
    });

    // Give process time to start
    await this.sleep(2000);

    return approachProcess;
  }

  /**
   * Start publisher process with specific data path
   */
  private async startPublisherProcess(dataPath: string): Promise<ChildProcess> {
    // Determine device type from path to set appropriate topic
    const deviceType = dataPath.includes("/smartphone/")
      ? "smartphone"
      : dataPath.includes("/wearable/")
        ? "wearable"
        : "sensor";

    // Extract frequency from path (e.g., "frequency_variants/2mins/smartphone/64Hz" → "64")
    const frequencyMatch = dataPath.match(/(\d+)Hz/);
    const frequency = frequencyMatch ? parseInt(frequencyMatch[1]) : 4;

    const topicName =
      deviceType === "smartphone"
        ? "smartphoneX"
        : deviceType === "wearable"
          ? "wearableX"
          : "sensorX";
    const fullDataPath = `src/streamer/data/${dataPath}/data.nt`;

    console.log(
      `      Starting publisher: ${fullDataPath} → topic: ${topicName} (${frequency}Hz)`,
    );

    const publisherProcess = spawn(
      "node",
      [
        "dist/streamer/src/experiment-publisher.js",
        fullDataPath,
        topicName,
        frequency.toString(),
      ],
      {
        cwd: this.projectRoot,
        stdio: "pipe",
        env: { ...process.env },
      },
    );

    // Give publisher time to start
    await this.sleep(1000);

    return publisherProcess;
  }

  /**
   * Set up MQTT result collector
   */
  private async setupResultCollector(
    topic: string,
    results: any[],
  ): Promise<mqtt.MqttClient> {
    return new Promise((resolve, reject) => {
      const client = mqtt.connect(this.mqttBroker);

      client.on("connect", () => {
        client.subscribe(topic, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve(client);
          }
        });
      });

      client.on("message", (receivedTopic, message) => {
        if (receivedTopic === topic) {
          try {
            const result = JSON.parse(message.toString());
            results.push({
              timestamp: new Date().toISOString(),
              data: result,
            });
          } catch (error) {
            // Handle non-JSON messages
            results.push({
              timestamp: new Date().toISOString(),
              data: message.toString(),
            });
          }
        }
      });

      client.on("error", reject);
    });
  }

  /**
   * Get result topic for approach
   */
  private getResultTopic(approach: string): string {
    const topicMap = {
      "fetching-client-side": "fetching_client_side_output",
      "streaming-query-hive": "streaming_query_hive_output",
      "chunked-approach": "chunked_approach_output",
      "approximation-approach": "approximation_approach_output",
    };

    return topicMap[approach as keyof typeof topicMap] || "default_output";
  }

  /**
   * Get publisher statistics from log file
   */
  private async getPublisherStats(): Promise<any> {
    try {
      const logPath = path.join(this.projectRoot, "replayer-log.csv");
      if (!fs.existsSync(logPath)) {
        return null;
      }

      const logContent = fs.readFileSync(logPath, "utf8");
      const lines = logContent.trim().split("\n");
      if (lines.length < 2) {
        return null;
      }

      const lastLine = lines[lines.length - 1];
      const [timestamp, intended, successful, failed] = lastLine.split(",");

      return {
        timestamp: parseInt(timestamp),
        intended: parseInt(intended),
        successful: parseInt(successful),
        failed: parseInt(failed),
      };
    } catch (error) {
      console.warn("Could not read publisher stats:", error);
      return null;
    }
  }

  /**
   * Clean up processes
   */
  private async cleanupProcesses(processes: ChildProcess[]): Promise<void> {
    for (const process of processes) {
      if (process && !process.killed) {
        process.kill("SIGTERM");

        // Give process time to gracefully shut down
        await this.sleep(1000);

        if (!process.killed) {
          process.kill("SIGKILL");
        }
      }
    }
  }

  /**
   * Calculate accuracy compared to ground truth
   */
  private calculateAccuracy(groundTruth: any[], results: any[]): number {
    try {
      if (groundTruth.length === 0 || results.length === 0) {
        return 0;
      }

      // Simple accuracy calculation based on result count similarity
      const lengthSimilarity =
        Math.min(results.length, groundTruth.length) /
        Math.max(results.length, groundTruth.length);

      // For more sophisticated accuracy, you would compare actual values
      // This is a simplified implementation
      return lengthSimilarity * 100;
    } catch (error) {
      console.warn(`Could not calculate accuracy: ${error}`);
      return -1;
    }
  }

  /**
   * Save experiment results to files
   */
  private async saveResults(): Promise<void> {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");

    // Save detailed results as JSON
    const detailedResultsPath = path.join(
      this.config.outputPath,
      `real-world-results-${timestamp}.json`,
    );
    const detailedResults = {
      config: this.config,
      groundTruth: Object.fromEntries(this.groundTruthResults),
      results: this.results,
      summary: this.generateSummary(),
    };

    fs.writeFileSync(
      detailedResultsPath,
      JSON.stringify(detailedResults, null, 2),
    );
    console.log(`Detailed results saved to: ${detailedResultsPath}`);

    // Save CSV for easy analysis
    const csvPath = path.join(
      this.config.outputPath,
      `real-world-results-${timestamp}.csv`,
    );
    const csvContent = this.generateCSV();
    fs.writeFileSync(csvPath, csvContent);
    console.log(`CSV results saved to: ${csvPath}`);
  }

  /**
   * Generate experiment summary
   */
  private generateSummary(): any {
    const summary = {
      totalExperiments: this.results.length,
      successfulExperiments: this.results.filter((r) => !r.error).length,
      failedExperiments: this.results.filter((r) => r.error).length,
      approaches: this.config.approaches,
      frequencies: this.config.frequencies,
      deviceTypes: this.config.deviceTypes,
      averageResults:
        this.results.reduce((acc, r) => acc + r.queryResults.length, 0) /
        this.results.length,
    };

    return summary;
  }

  /**
   * Generate CSV content from results
   */
  private generateCSV(): string {
    const headers = [
      "approach",
      "frequency",
      "deviceType",
      "iteration",
      "timestamp",
      "latency_ms",
      "memory_mb",
      "accuracy_percent",
      "throughput_obs_sec",
      "observations_processed",
      "execution_time_ms",
      "published_observations",
      "received_results",
      "error",
    ];

    const rows = this.results.map((result) => [
      result.approach,
      result.frequency,
      result.deviceType,
      result.iteration,
      result.timestamp,
      result.metrics.latency.toFixed(2),
      result.metrics.memoryUsage.toFixed(2),
      result.metrics.accuracy?.toFixed(2) || "N/A",
      result.metrics.throughput.toFixed(2),
      result.metrics.observationsProcessed,
      result.metrics.executionTime.toFixed(2),
      result.metrics.publishedObservations,
      result.metrics.receivedResults,
      result.error || "",
    ]);

    return [headers.join(","), ...rows.map((row) => row.join(","))].join("\n");
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Run the experiments if this file is executed directly
if (require.main === module) {
  const runner = new RealWorldFrequencyExperimentRunner();
  runner.runExperiments().catch((error) => {
    console.error("Experiment failed:", error);
    process.exit(1);
  });
}

export { RealWorldFrequencyExperimentRunner };
