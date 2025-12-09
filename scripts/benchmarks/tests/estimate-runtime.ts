#!/usr/bin/env ts-node

/**
 * Runtime Estimation Script
 *
 * Estimates total runtime for experiments based on configuration
 * and optionally runs a test iteration to get accurate timing.
 */

import * as fs from "fs";
import * as path from "path";

interface ExperimentConfig {
  experiment: {
    name: string;
    iterations: number;
    warmupIterations?: number;
    cooldownIterations?: number;
  };
  frequencies: string[];
  deviceTypes: string[];
  approaches: string[];
}

class RuntimeEstimator {
  private config: ExperimentConfig;
  private readonly projectRoot: string;

  constructor() {
    this.projectRoot = path.resolve(__dirname, "../..");
    const configPath = path.join(
      this.projectRoot,
      "scripts/benchmarks/frequency-experiment-config.json",
    );

    if (!fs.existsSync(configPath)) {
      throw new Error("Experiment configuration not found.");
    }

    this.config = JSON.parse(fs.readFileSync(configPath, "utf8"));
  }

  /**
   * Calculate total number of experiments
   */
  private calculateTotalExperiments(): {
    total: number;
    perApproach: number;
    perFrequency: number;
    perDevice: number;
  } {
    const total =
      this.config.approaches.length *
      this.config.frequencies.length *
      this.config.deviceTypes.length *
      this.config.experiment.iterations;

    const perApproach =
      this.config.frequencies.length *
      this.config.deviceTypes.length *
      this.config.experiment.iterations;

    const perFrequency =
      this.config.approaches.length *
      this.config.deviceTypes.length *
      this.config.experiment.iterations;

    const perDevice =
      this.config.approaches.length *
      this.config.frequencies.length *
      this.config.experiment.iterations;

    return { total, perApproach, perFrequency, perDevice };
  }

  /**
   * Estimate runtime based on average iteration time
   */
  private estimateRuntime(avgIterationSeconds: number): {
    totalSeconds: number;
    totalMinutes: number;
    totalHours: number;
    totalDays: number;
    perApproachHours: number;
  } {
    const counts = this.calculateTotalExperiments();
    const totalSeconds = counts.total * avgIterationSeconds;
    const totalMinutes = totalSeconds / 60;
    const totalHours = totalMinutes / 60;
    const totalDays = totalHours / 24;
    const perApproachHours = (counts.perApproach * avgIterationSeconds) / 3600;

    return {
      totalSeconds,
      totalMinutes,
      totalHours,
      totalDays,
      perApproachHours,
    };
  }

  /**
   * Format time duration
   */
  private formatDuration(seconds: number): string {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    const parts: string[] = [];
    if (days > 0) parts.push(`${days}d`);
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);

    return parts.join(" ");
  }

  /**
   * Display runtime estimation
   */
  public displayEstimation(): void {
    console.log("\n" + "=".repeat(70));
    console.log("EXPERIMENT RUNTIME ESTIMATION");
    console.log("=".repeat(70));

    console.log("\nConfiguration:");
    console.log(`  Approaches: ${this.config.approaches.length} (${this.config.approaches.join(", ")})`);
    console.log(`  Frequencies: ${this.config.frequencies.length} (${this.config.frequencies.join(", ")})`);
    console.log(`  Device Types: ${this.config.deviceTypes.length} (${this.config.deviceTypes.join(", ")})`);
    console.log(`  Total Iterations: ${this.config.experiment.iterations}`);

    const warmup = this.config.experiment.warmupIterations || 0;
    const cooldown = this.config.experiment.cooldownIterations || 0;
    const valid = this.config.experiment.iterations - warmup - cooldown;

    if (warmup > 0 || cooldown > 0) {
      console.log(`    - Warmup: ${warmup}`);
      console.log(`    - Valid: ${valid}`);
      console.log(`    - Cooldown: ${cooldown}`);
    }

    const counts = this.calculateTotalExperiments();
    console.log("\nExperiment Counts:");
    console.log(`  Total Experiments: ${counts.total.toLocaleString()}`);
    console.log(`  Per Approach: ${counts.perApproach.toLocaleString()}`);
    console.log(`  Per Frequency: ${counts.perFrequency.toLocaleString()}`);
    console.log(`  Per Device: ${counts.perDevice.toLocaleString()}`);

    console.log("\n" + "-".repeat(70));
    console.log("ESTIMATED RUNTIME (based on historical data)");
    console.log("-".repeat(70));

    // Conservative estimate (63 seconds based on performance analysis)
    const conservative = this.estimateRuntime(63);
    console.log("\nConservative Estimate (63s per iteration):");
    console.log(`  Total Time: ${this.formatDuration(conservative.totalSeconds)}`);
    console.log(`             (${conservative.totalHours.toFixed(1)} hours)`);
    console.log(`  Per Approach: ${conservative.perApproachHours.toFixed(1)} hours`);

    // Optimistic estimate (45 seconds)
    const optimistic = this.estimateRuntime(45);
    console.log("\nOptimistic Estimate (45s per iteration):");
    console.log(`  Total Time: ${this.formatDuration(optimistic.totalSeconds)}`);
    console.log(`             (${optimistic.totalHours.toFixed(1)} hours)`);
    console.log(`  Per Approach: ${optimistic.perApproachHours.toFixed(1)} hours`);

    // Pessimistic estimate (90 seconds)
    const pessimistic = this.estimateRuntime(90);
    console.log("\nPessimistic Estimate (90s per iteration):");
    console.log(`  Total Time: ${this.formatDuration(pessimistic.totalSeconds)}`);
    console.log(`             (${pessimistic.totalHours.toFixed(1)} hours)`);
    console.log(`  Per Approach: ${pessimistic.perApproachHours.toFixed(1)} hours`);

    console.log("\n" + "-".repeat(70));
    console.log("RECOMMENDATIONS");
    console.log("-".repeat(70));

    console.log("\n1. Run a test iteration first:");
    console.log("   - Temporarily set iterations to 1");
    console.log("   - Time how long it takes");
    console.log("   - Use actual timing for accurate estimate");

    console.log("\n2. Consider running approaches separately:");
    console.log(`   - Each approach: ~${conservative.perApproachHours.toFixed(1)} hours`);
    console.log("   - Edit config to include only one approach at a time");
    console.log("   - Reduces risk and allows monitoring per approach");

    console.log("\n3. Monitor execution:");
    console.log("   - Check console output for timing");
    console.log("   - Watch for errors early");
    console.log("   - Results saved per iteration (can resume if needed)");

    if (conservative.totalHours > 24) {
      console.log("\n4. Plan for long runtime:");
      console.log(`   - Estimated ${conservative.totalDays.toFixed(1)} days total`);
      console.log("   - Consider running overnight/weekend");
      console.log("   - Ensure system won't sleep/hibernate");
      console.log("   - Check disk space for results");
    }

    console.log("\n" + "=".repeat(70));
    console.log("To run experiments: npm run experiment:run");
    console.log("=".repeat(70) + "\n");
  }

  /**
   * Generate breakdown by approach
   */
  public displayApproachBreakdown(): void {
    console.log("\nPer-Approach Breakdown (conservative estimate):");
    console.log("-".repeat(70));

    const experimentsPerApproach =
      this.config.frequencies.length *
      this.config.deviceTypes.length *
      this.config.experiment.iterations;

    for (const approach of this.config.approaches) {
      const timeHours = (experimentsPerApproach * 63) / 3600;
      console.log(`  ${approach}:`);
      console.log(`    Experiments: ${experimentsPerApproach}`);
      console.log(`    Estimated Time: ${timeHours.toFixed(1)} hours (${(timeHours / 24).toFixed(1)} days)`);
    }
  }
}

// Run the estimator
if (require.main === module) {
  try {
    const estimator = new RuntimeEstimator();
    estimator.displayEstimation();
    estimator.displayApproachBreakdown();
  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  }
}

export { RuntimeEstimator };
