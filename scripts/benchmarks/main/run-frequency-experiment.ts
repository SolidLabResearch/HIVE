#!/usr/bin/env ts-node

/**
 * Frequency-Based Streaming Query Experiment Runner.
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
    warmupIterations?: number;
    cooldownIterations?: number;
  };
  frequencies: string[];
  deviceTypes: string[];
  approaches: string[];
  dataBasePath: string;
  outputPath: string;
  saveIterationsSeparately?: boolean;
  queries: any;
  metrics: any;
}

interface ExperimentResult {
  approach: string;
  frequency: string;
  deviceType: string;
  iteration: number;
  isWarmup: boolean;
  isCooldown: boolean;
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
  runExperiment(): Promise<any>;
  getName(): string;
}
 

/**
 *
 */
class FrequencyExperimentRunner {
  private readonly projectRoot: string;
  private readonly config: ExperimentConfig;
  private readonly results: ExperimentResult[] = [];
  private groundTruthResults: Map<string, any> = new Map();
  private readonly warmupIterations: number;
  private readonly cooldownIterations: number;
  private readonly validIterations: number;

  /**
   *
   */
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

    // Set warmup and cooldown iterations
    this.warmupIterations = this.config.experiment.warmupIterations || 0;
    this.cooldownIterations = this.config.experiment.cooldownIterations || 0;
    this.validIterations =
      this.config.experiment.iterations -
      this.warmupIterations -
      this.cooldownIterations;

    console.log(`Warmup iterations: ${this.warmupIterations}`);
    console.log(`Valid iterations: ${this.validIterations}`);
    console.log(`Cooldown iterations: ${this.cooldownIterations}`);
    console.log(`Total iterations: ${this.config.experiment.iterations}`);
  }

  /**
   * Run the complete frequency experiment suite.
   */
  public async runExperiments(): Promise<void> {
    console.log("Starting Frequency-Based Streaming Query Experiments");
    console.log("=".repeat(70));
    console.log(`Experiment: ${this.config.experiment.name}`);
    console.log(`Total Iterations: ${this.config.experiment.iterations}`);
    console.log(
      `Warmup Iterations: ${this.warmupIterations} (excluded from analysis)`,
    );
    console.log(
      `Valid Iterations: ${this.validIterations} (used for analysis)`,
    );
    console.log(
      `Cooldown Iterations: ${this.cooldownIterations} (excluded from analysis)`,
    );
    console.log(`Frequencies: ${this.config.frequencies.join(", ")}`);
    console.log(`Device Types: ${this.config.deviceTypes.join(", ")}`);
    console.log(`Approaches: ${this.config.approaches.join(", ")}`);
    console.log("");

    const startTime = performance.now();

    try {
      // Step 1: Generate ground truth (fetching-client-side approach)
      console.log(
        "Generating ground truth with fetching-client-side approach...",
      );
      await this.generateGroundTruth();

      // Step 2: Run experiments for all other approaches
      console.log("\nRunning experiments for all approaches...");
      await this.runAllApproaches();

      // Step 3: Save results
      console.log("\nSaving experiment results...");
      await this.saveResults();

      const endTime = performance.now();
      const totalTime = (endTime - startTime) / 1000;

      console.log("\n" + "=".repeat(70));
      console.log("[OK] All experiments completed successfully!");
      console.log(`Total execution time: ${totalTime.toFixed(2)} seconds`);
      console.log(`Total experiments run: ${this.results.length}`);
      console.log(`Results saved to: ${this.config.outputPath}`);
    } catch (error) {
      console.error("\n[FAIL] Experiment failed:", error);
      await this.saveResults(); // Save partial results
      throw error;
    }
  }

  /**
   * Generate ground truth using fetching-client-side approach.
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
            false,
            false,
            dataPath,
          );

          if (result && !result.error) {
            this.groundTruthResults.set(key, result.queryResult);
            console.log(`    [OK] Ground truth generated for ${key}`);
          } else {
            console.log(
              `    [FAIL] Failed to generate ground truth for ${key}: ${result?.error}`,
            );
          }
        } catch (error) {
          console.log(
            `    [FAIL] Error generating ground truth for ${key}: ${error}`,
          );
        }
      }
    }

    console.log(
      `Ground truth generated for ${this.groundTruthResults.size} combinations`,
    );
  }

  /**
   * Run experiments for all approaches across all frequencies and device types.
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

            // Determine if this is warmup or cooldown
            const isWarmup = iteration <= this.warmupIterations;
            const isCooldown =
              iteration >
              this.config.experiment.iterations - this.cooldownIterations;

            try {
              const result = await this.runSingleExperiment(
                approach,
                frequency,
                deviceType,
                iteration,
                isWarmup,
                isCooldown,
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

                const iterationType = isWarmup
                  ? "[WARMUP]"
                  : isCooldown
                    ? "[COOLDOWN]"
                    : "[VALID]";
                console.log(
                  `    Iteration ${iteration} ${iterationType}: [OK] (${result.metrics.latency.toFixed(2)}ms, ${result.metrics.memoryUsage.toFixed(2)}MB)`,
                );

                // Save individual iteration if configured
                if (this.config.saveIterationsSeparately) {
                  await this.saveIterationResult(result);
                }
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
                deviceType,
                iteration,
                isWarmup,
                isCooldown,
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
   * Run a single experiment.
   * @param {string} approach - The approach name.
   * @param {string} frequency - The data frequency.
   * @param {string} deviceType - The device type.
   * @param {number} iteration - The iteration number.
   * @param {boolean} isWarmup - Whether this is a warmup iteration.
   * @param {boolean} isCooldown - Whether this is a cooldown iteration.
   * @param {string} dataPath - The path to the data file.
   * @returns {Promise<ExperimentResult | null>} The experiment result or null if failed.
   */
  private async runSingleExperiment(
    approach: string,
    frequency: string,
    deviceType: string,
    iteration: number,
    isWarmup: boolean,
    isCooldown: boolean,
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
      const queryResult = await orchestrator.runExperiment();

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
        isWarmup,
        isCooldown,
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
        isWarmup,
        isCooldown,
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
   * Load the appropriate orchestrator for an approach.
   * @param {string} approach - The approach name.
   * @returns {Promise<ApproachOrchestrator>} The orchestrator instance.
   */
  private async loadOrchestrator(
    approach: string,
  ): Promise<ApproachOrchestrator> {
    const orchestratorMap = {
      "fetching-client-side":
        "../approaches/FetchingClientSideApproachOrchestrator",
      "chunked-query-approach":
        "../approaches/ChunkedQueryApproachOrchestrator",
      "approximation-approach":
        "../approaches/ApproximationApproachOrchestrator",
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
   * Count observations in a data file.
   * @param {string} dataPath - The path to the data file.
   * @returns {number} The number of observations.
   */
  private countObservations(dataPath: string): number {
    if (!fs.existsSync(dataPath)) {
      throw new Error(`Data file not found: ${dataPath}`);
    }

    const content = fs.readFileSync(dataPath, "utf8");
    return content.split("\n").filter((line) => line.trim().length > 0).length;
  }

  /**
   * Calculate accuracy compared to ground truth.
   * @param {any} groundTruth - The ground truth result.
   * @param {any} result - The experiment result.
   * @returns {number} The accuracy percentage.
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
   * Save individual iteration result to separate folder.
   * @param {ExperimentResult} result - The experiment result to save.
   * @returns {Promise<void>}
   */
  private async saveIterationResult(result: ExperimentResult): Promise<void> {
    // Create iteration folder structure: approach/frequency/deviceType/iteration_X/
    const iterationPath = path.join(
      this.config.outputPath,
      result.approach,
      result.frequency,
      result.deviceType,
      `iteration_${result.iteration}`,
    );

    // Create directory if it doesn't exist
    if (!fs.existsSync(iterationPath)) {
      fs.mkdirSync(iterationPath, { recursive: true });
    }

    // Save iteration result as JSON
    const resultPath = path.join(iterationPath, "result.json");
    fs.writeFileSync(resultPath, JSON.stringify(result, null, 2));
  }

  /**
   * Save experiment results to files.
   * @returns {Promise<void>}
   */
  private async saveResults(): Promise<void> {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");

    // Filter results for analysis (exclude warmup and cooldown)
    const validResults = this.results.filter(
      (r) => !r.isWarmup && !r.isCooldown,
    );

    // Save detailed results as JSON
    const detailedResultsPath = path.join(
      this.config.outputPath,
      `detailed-results-${timestamp}.json`,
    );
    const detailedResults = {
      config: this.config,
      warmupIterations: this.warmupIterations,
      cooldownIterations: this.cooldownIterations,
      validIterations: this.validIterations,
      groundTruth: Object.fromEntries(this.groundTruthResults),
      allResults: this.results,
      validResults: validResults,
      summary: this.generateSummary(),
    };

    fs.writeFileSync(
      detailedResultsPath,
      JSON.stringify(detailedResults, null, 2),
    );
    console.log(`Detailed results saved to: ${detailedResultsPath}`);

    // Save CSV for all results
    const csvPath = path.join(
      this.config.outputPath,
      `results-all-${timestamp}.csv`,
    );
    const csvContent = this.generateCSV(this.results);
    fs.writeFileSync(csvPath, csvContent);
    console.log(`All results CSV saved to: ${csvPath}`);

    // Save CSV for valid results only (excluding warmup/cooldown)
    const validCsvPath = path.join(
      this.config.outputPath,
      `results-valid-${timestamp}.csv`,
    );
    const validCsvContent = this.generateCSV(validResults);
    fs.writeFileSync(validCsvPath, validCsvContent);
    console.log(`Valid results CSV saved to: ${validCsvPath}`);
    console.log(`Valid iterations used for analysis: ${validResults.length}`);
  }

  /**
   * Generate experiment summary.
   * @returns {any} The summary object.
   */
  private generateSummary(): any {
    const validResults = this.results.filter(
      (r) => !r.isWarmup && !r.isCooldown,
    );
    const warmupResults = this.results.filter((r) => r.isWarmup);
    const cooldownResults = this.results.filter((r) => r.isCooldown);

    const summary = {
      totalIterations: this.config.experiment.iterations,
      warmupIterations: this.warmupIterations,
      validIterations: this.validIterations,
      cooldownIterations: this.cooldownIterations,
      totalExperiments: this.results.length,
      warmupExperiments: warmupResults.length,
      validExperiments: validResults.length,
      cooldownExperiments: cooldownResults.length,
      successfulExperiments: this.results.filter((r) => !r.error).length,
      failedExperiments: this.results.filter((r) => r.error).length,
      validSuccessful: validResults.filter((r) => !r.error).length,
      validFailed: validResults.filter((r) => r.error).length,
      approaches: this.config.approaches,
      frequencies: this.config.frequencies,
      deviceTypes: this.config.deviceTypes,
    };

    return summary;
  }

  /**
   * Generate CSV content from results.
   * @param {ExperimentResult[]} results - The experiment results.
   * @returns {string} The CSV content.
   */
  private generateCSV(results: ExperimentResult[]): string {
    const headers = [
      "approach",
      "frequency",
      "deviceType",
      "iteration",
      "is_warmup",
      "is_cooldown",
      "timestamp",
      "latency_ms",
      "memory_mb",
      "accuracy_percent",
      "throughput_obs_sec",
      "observations_processed",
      "execution_time_ms",
      "error",
    ];

    const rows = results.map((result) => [
      result.approach,
      result.frequency,
      result.deviceType,
      result.iteration,
      result.isWarmup,
      result.isCooldown,
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
