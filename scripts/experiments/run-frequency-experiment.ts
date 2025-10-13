#!/usr/bin/env ts-node

/**
 * Frequency-Based Streaming Query Experiment Runner
 *
 * This script runs comprehensive experiments comparing different streaming query approaches
 * across multiple frequencies (4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz) for both smartphone
 * and wearable acceleration X-axis data.
 */

import * as fs from "fs";
import * as path from "path";
import { performance } from "perf_hooks";

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
  };
  queryResult: any;
  error?: string;
}

interface ApproachOrchestrator {
  runExperiment(dataPath: string, config: any): Promise<any>;
  getName(): string;
}

class FrequencyExperimentRunner {
  private readonly projectRoot: string;
  private readonly config: ExperimentConfig;
  private readonly results: ExperimentResult[] = [];
  private groundTruthResults: Map<string, any> = new Map();

  constructor() {
    this.projectRoot = path.resolve(__dirname, "../..");

    // Load configuration
    const configPath = path.join(
      this.projectRoot,
      "scripts/experiments/frequency-experiment-config.json",
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
    console.log("🚀 Starting Frequency-Based Streaming Query Experiments");
    console.log("=".repeat(70));
    console.log(`Experiment: ${this.config.experiment.name}`);
    console.log(`Iterations: ${this.config.experiment.iterations}`);
    console.log(`Frequencies: ${this.config.frequencies.join(", ")}`);
    console.log(`Device Types: ${this.config.deviceTypes.join(", ")}`);
    console.log(`Approaches: ${this.config.approaches.join(", ")}`);
    console.log("");

    const startTime = performance.now();

    try {
      // Step 1: Generate ground truth (fetching-client-side approach)
      console.log(
        "📊 Generating ground truth with fetching-client-side approach...",
      );
      await this.generateGroundTruth();

      // Step 2: Run experiments for all other approaches
      console.log("\n🎯 Running experiments for all approaches...");
      await this.runAllApproaches();

      // Step 3: Save results
      console.log("\n💾 Saving experiment results...");
      await this.saveResults();

      const endTime = performance.now();
      const totalTime = (endTime - startTime) / 1000;

      console.log("\n" + "=".repeat(70));
      console.log("✅ All experiments completed successfully!");
      console.log(`Total execution time: ${totalTime.toFixed(2)} seconds`);
      console.log(`Total experiments run: ${this.results.length}`);
      console.log(`Results saved to: ${this.config.outputPath}`);
    } catch (error) {
      console.error("\n❌ Experiment failed:", error);
      await this.saveResults(); // Save partial results
      throw error;
    }
  }

  /**
   * Generate ground truth using fetching-client-side approach
   */
  private async generateGroundTruth(): Promise<void> {
    console.log(
      "Generating ground truth (fetching-client-side) for all frequency-device combinations...",
    );

    for (const frequency of this.config.frequencies) {
      for (const deviceType of this.config.deviceTypes) {
        const key = `${frequency}-${deviceType}`;
        console.log(`  Processing ${key}...`);

        try {
          const dataPath = path.join(
            this.config.dataBasePath,
            deviceType,
            frequency,
            "data.nt",
          );
          const result = await this.runSingleExperiment(
            "fetching-client-side",
            frequency,
            deviceType,
            1,
            dataPath,
          );

          if (result && !result.error) {
            this.groundTruthResults.set(key, result.queryResult);
            console.log(`    ✅ Ground truth generated for ${key}`);
          } else {
            console.log(
              `    ❌ Failed to generate ground truth for ${key}: ${result?.error}`,
            );
          }
        } catch (error) {
          console.log(
            `    ❌ Error generating ground truth for ${key}: ${error}`,
          );
        }
      }
    }

    console.log(
      `Ground truth generated for ${this.groundTruthResults.size} combinations`,
    );
  }

  /**
   * Run experiments for all approaches across all frequencies and device types
   */
  private async runAllApproaches(): Promise<void> {
    const totalExperiments =
      this.config.approaches.length *
      this.config.frequencies.length *
      this.config.deviceTypes.length *
      this.config.experiment.iterations;

    console.log(`Running ${totalExperiments} total experiments...`);

    let completedExperiments = 0;

    for (const approach of this.config.approaches) {
      console.log(`\n--- Running ${approach} approach ---`);

      for (const frequency of this.config.frequencies) {
        for (const deviceType of this.config.deviceTypes) {
          console.log(`  ${frequency} ${deviceType}:`);

          for (
            let iteration = 1;
            iteration <= this.config.experiment.iterations;
            iteration++
          ) {
            const dataPath = path.join(
              this.config.dataBasePath,
              deviceType,
              frequency,
              "data.nt",
            );

            try {
              const result = await this.runSingleExperiment(
                approach,
                frequency,
                deviceType,
                iteration,
                dataPath,
              );

              if (result) {
                // Calculate accuracy against ground truth
                const groundTruthKey = `${frequency}-${deviceType}`;
                const groundTruth = this.groundTruthResults.get(groundTruthKey);

                if (groundTruth && result.queryResult) {
                  result.metrics.accuracy = this.calculateAccuracy(
                    groundTruth,
                    result.queryResult,
                  );
                }

                this.results.push(result);
                console.log(
                  `    Iteration ${iteration}: ✅ (${result.metrics.latency.toFixed(2)}ms, ${result.metrics.memoryUsage.toFixed(2)}MB)`,
                );
              } else {
                console.log(
                  `    Iteration ${iteration}: ❌ No result returned`,
                );
              }
            } catch (error) {
              console.log(`    Iteration ${iteration}: ❌ ${error}`);

              // Record failed experiment
              this.results.push({
                approach,
                frequency,
                deviceType,
                iteration,
                timestamp: new Date().toISOString(),
                metrics: {
                  latency: -1,
                  memoryUsage: -1,
                  throughput: -1,
                  observationsProcessed: -1,
                  executionTime: -1,
                },
                queryResult: null,
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
          }
          console.log(); // New line after progress
        }
      }
    }
  }

  /**
   * Run a single experiment
   */
  private async runSingleExperiment(
    approach: string,
    frequency: string,
    deviceType: string,
    iteration: number,
    dataPath: string,
  ): Promise<ExperimentResult | null> {
    const startTime = performance.now();
    const startMemory = process.memoryUsage().heapUsed;

    try {
      // Load the appropriate orchestrator
      const orchestrator = await this.loadOrchestrator(approach);

      // Count observations in dataset
      const observationsCount = this.countObservations(dataPath);

      // Run the experiment
      const queryResult = await orchestrator.runExperiment(dataPath, {
        query: this.config.queries.avgAcceleration,
        windowSize: this.config.queries.windowSize,
      });

      const endTime = performance.now();
      const endMemory = process.memoryUsage().heapUsed;

      const executionTime = endTime - startTime;
      const memoryUsage = (endMemory - startMemory) / 1024 / 1024; // Convert to MB
      const throughput = observationsCount / (executionTime / 1000); // obs/second

      return {
        approach,
        frequency,
        deviceType,
        iteration,
        timestamp: new Date().toISOString(),
        metrics: {
          latency: executionTime,
          memoryUsage: Math.max(0, memoryUsage),
          throughput,
          observationsProcessed: observationsCount,
          executionTime,
        },
        queryResult,
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
        },
        queryResult: null,
        error: String(error),
      };
    }
  }

  /**
   * Load the appropriate orchestrator for an approach
   */
  private async loadOrchestrator(
    approach: string,
  ): Promise<ApproachOrchestrator> {
    const orchestratorMap = {
      "fetching-client-side":
        "../approaches/StreamingQueryFetchingClientSideApproachOrchestrator",
      "streaming-query-hive":
        "../approaches/StreamingQueryHiveApproachOrchestrator",
      "chunked-approach":
        "../approaches/StreamingQueryChunkedApproachOrchestrator",
      "approximation-approach":
        "../approaches/StreamingQueryApproximationApproachOrchestrator",
    };

    const orchestratorPath =
      orchestratorMap[approach as keyof typeof orchestratorMap];
    if (!orchestratorPath) {
      throw new Error(`Unknown approach: ${approach}`);
    }

    try {
      const OrchestratorClass = await import(orchestratorPath);
      return new OrchestratorClass.default();
    } catch (error) {
      throw new Error(`Failed to load orchestrator for ${approach}: ${error}`);
    }
  }

  /**
   * Count observations in a data file
   */
  private countObservations(dataPath: string): number {
    if (!fs.existsSync(dataPath)) {
      throw new Error(`Data file not found: ${dataPath}`);
    }

    const content = fs.readFileSync(dataPath, "utf8");
    return content.split("\n").filter((line) => line.trim().length > 0).length;
  }

  /**
   * Calculate accuracy compared to ground truth
   */
  private calculateAccuracy(groundTruth: any, result: any): number {
    try {
      // This is a simplified accuracy calculation
      // You may need to adjust based on your specific query result structure
      if (typeof groundTruth === "number" && typeof result === "number") {
        const percentageError =
          Math.abs((result - groundTruth) / groundTruth) * 100;
        return Math.max(0, 100 - percentageError);
      }

      // For more complex results, implement custom comparison logic
      return JSON.stringify(groundTruth) === JSON.stringify(result) ? 100 : 0;
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
      `detailed-results-${timestamp}.json`,
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
      `results-${timestamp}.csv`,
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
      result.error || "",
    ]);

    return [headers.join(","), ...rows.map((row) => row.join(","))].join("\n");
  }
}

// Run the experiments if this file is executed directly
if (require.main === module) {
  const runner = new FrequencyExperimentRunner();
  runner.runExperiments().catch((error) => {
    console.error("Experiment failed:", error);
    process.exit(1);
  });
}

export { FrequencyExperimentRunner };
