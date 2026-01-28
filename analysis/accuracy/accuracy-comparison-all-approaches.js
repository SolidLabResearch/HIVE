#!/usr/bin/env node

/**
 * Comprehensive Accuracy and Latency Comparison - All Three Approaches
 *
 * This script compares accuracy and first-event latency across:
 * 1. Fetching (client-side) - baseline/ground truth
 * 2. Approximation (rate-based)
 * 3. Chunked (aggregation)
 *
 * Metrics calculated:
 * - Absolute Error
 * - Mean Absolute Percentage Error (MAPE)
 * - Mean Absolute Error (MAE)
 * - Root Mean Square Error (RMSE)
 * - First-Event Latency
 * - Latency Difference vs Fetching
 *
 * Usage:
 *   node accuracy-comparison-all-approaches.js
 */

const fs = require("fs");
const path = require("path");

class AllApproachesComparison {
  constructor() {
    this.baseLogDir = "./logs";
    this.approaches = ["fetching", "approximation", "chunked"];
    this.frequencies = [0.1, 0.5, 1.0, 1.5, 2.0];
    this.oscillationType = "complex_oscillation";
  }

  /**
   * Read metadata from a specific approach/frequency
   */
  readMetadata(approach, frequency) {
    const formattedFreq =
      parseFloat(frequency) % 1 === 0
        ? parseFloat(frequency).toFixed(1)
        : frequency.toString();

    const logDirName = `frequency-comparison-${approach}`;
    const metadataPath = path.join(
      this.baseLogDir,
      logDirName,
      `${this.oscillationType}_freq_${formattedFreq}`,
      "iteration1",
      `${approach}_metadata.json`,
    );

    if (!fs.existsSync(metadataPath)) {
      console.warn(`Metadata not found: ${metadataPath}`);
      return null;
    }

    try {
      const content = fs.readFileSync(metadataPath, "utf8");
      return JSON.parse(content);
    } catch (error) {
      console.error(`Error reading metadata from ${metadataPath}:`, error.message);
      return null;
    }
  }

  /**
   * Read results CSV from a specific approach/frequency
   */
  readResults(approach, frequency) {
    const formattedFreq =
      parseFloat(frequency) % 1 === 0
        ? parseFloat(frequency).toFixed(1)
        : frequency.toString();

    const logDirName = `frequency-comparison-${approach}`;
    const resultsPath = path.join(
      this.baseLogDir,
      logDirName,
      `${this.oscillationType}_freq_${formattedFreq}`,
      "iteration1",
      `${approach}_results.csv`,
    );

    if (!fs.existsSync(resultsPath)) {
      console.warn(`Results not found: ${resultsPath}`);
      return null;
    }

    try {
      const content = fs.readFileSync(resultsPath, "utf8");
      const lines = content.trim().split("\n");

      if (lines.length < 2) {
        console.warn(`No data in results file: ${resultsPath}`);
        return null;
      }

      // Parse CSV (skip header)
      const results = [];
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(",");
        if (parts.length >= 3) {
          results.push({
            timestamp: parseInt(parts[0]),
            windowNumber: parseInt(parts[1]),
            resultValue: parseFloat(parts[2]),
            latency: parts[3] !== "N/A" ? parseFloat(parts[3]) : null,
          });
        }
      }

      return results;
    } catch (error) {
      console.error(`Error reading results from ${resultsPath}:`, error.message);
      return null;
    }
  }

  /**
   * Calculate accuracy metrics comparing approach results to fetching baseline
   */
  calculateAccuracyMetrics(baselineResults, approachResults) {
    if (!baselineResults || !approachResults) {
      return null;
    }

    // Match windows (assume same ordering and count)
    const minLength = Math.min(baselineResults.length, approachResults.length);

    if (minLength === 0) {
      return null;
    }

    let sumAbsoluteError = 0;
    let sumSquaredError = 0;
    let sumPercentageError = 0;
    const errors = [];

    for (let i = 0; i < minLength; i++) {
      const baseline = baselineResults[i].resultValue;
      const approach = approachResults[i].resultValue;

      const absoluteError = Math.abs(approach - baseline);
      const squaredError = Math.pow(approach - baseline, 2);
      const percentageError = baseline !== 0 ? (absoluteError / Math.abs(baseline)) * 100 : 0;

      sumAbsoluteError += absoluteError;
      sumSquaredError += squaredError;
      sumPercentageError += percentageError;

      errors.push({
        windowNumber: i + 1,
        baseline,
        approach,
        absoluteError,
        percentageError,
      });
    }

    const mae = sumAbsoluteError / minLength;
    const rmse = Math.sqrt(sumSquaredError / minLength);
    const mape = sumPercentageError / minLength;

    return {
      windowCount: minLength,
      mae,
      rmse,
      mape,
      errors,
    };
  }

  /**
   * Compare all approaches for a single frequency
   */
  compareFrequency(frequency) {
    console.log(`\nAnalyzing frequency: ${frequency} Hz`);
    console.log("─".repeat(80));

    // Read fetching (baseline) data
    const fetchingMetadata = this.readMetadata("fetching", frequency);
    const fetchingResults = this.readResults("fetching", frequency);

    if (!fetchingMetadata || !fetchingResults) {
      console.log(`  ✗ Fetching data not available - skipping ${frequency} Hz`);
      return null;
    }

    const comparison = {
      frequency,
      fetching: {
        firstEventLatency: fetchingMetadata.firstEventLatency,
        firstEventLatencySeconds: fetchingMetadata.firstEventLatencySeconds,
        resultCount: fetchingResults.length,
        avgValue: fetchingResults.length > 0
          ? fetchingResults.reduce((sum, r) => sum + r.resultValue, 0) / fetchingResults.length
          : null,
      },
      approximation: null,
      chunked: null,
    };

    // Compare approximation
    const approxMetadata = this.readMetadata("approximation", frequency);
    const approxResults = this.readResults("approximation", frequency);

    if (approxMetadata && approxResults) {
      const accuracyMetrics = this.calculateAccuracyMetrics(fetchingResults, approxResults);

      comparison.approximation = {
        firstEventLatency: approxMetadata.firstEventLatency,
        firstEventLatencySeconds: approxMetadata.firstEventLatencySeconds,
        latencyDifference: approxMetadata.firstEventLatency - fetchingMetadata.firstEventLatency,
        resultCount: approxResults.length,
        avgValue: approxResults.length > 0
          ? approxResults.reduce((sum, r) => sum + r.resultValue, 0) / approxResults.length
          : null,
        accuracy: accuracyMetrics,
      };

      console.log(`  Approximation:`);
      console.log(`    First-event latency: ${approxMetadata.firstEventLatencySeconds}s (Δ ${(comparison.approximation.latencyDifference / 1000).toFixed(2)}s)`);
      if (accuracyMetrics) {
        console.log(`    MAPE: ${accuracyMetrics.mape.toFixed(4)}%`);
        console.log(`    MAE: ${accuracyMetrics.mae.toFixed(6)}`);
        console.log(`    RMSE: ${accuracyMetrics.rmse.toFixed(6)}`);
      }
    } else {
      console.log(`  Approximation: ✗ Data not available`);
    }

    // Compare chunked
    const chunkedMetadata = this.readMetadata("chunked", frequency);
    const chunkedResults = this.readResults("chunked", frequency);

    if (chunkedMetadata && chunkedResults) {
      const accuracyMetrics = this.calculateAccuracyMetrics(fetchingResults, chunkedResults);

      comparison.chunked = {
        firstEventLatency: chunkedMetadata.firstEventLatency,
        firstEventLatencySeconds: chunkedMetadata.firstEventLatencySeconds,
        latencyDifference: chunkedMetadata.firstEventLatency - fetchingMetadata.firstEventLatency,
        resultCount: chunkedResults.length,
        avgValue: chunkedResults.length > 0
          ? chunkedResults.reduce((sum, r) => sum + r.resultValue, 0) / chunkedResults.length
          : null,
        accuracy: accuracyMetrics,
      };

      console.log(`  Chunked:`);
      console.log(`    First-event latency: ${chunkedMetadata.firstEventLatencySeconds}s (Δ ${(comparison.chunked.latencyDifference / 1000).toFixed(2)}s)`);
      if (accuracyMetrics) {
        console.log(`    MAPE: ${accuracyMetrics.mape.toFixed(4)}%`);
        console.log(`    MAE: ${accuracyMetrics.mae.toFixed(6)}`);
        console.log(`    RMSE: ${accuracyMetrics.rmse.toFixed(6)}`);
      }
    } else {
      console.log(`  Chunked: ✗ Data not available`);
    }

    return comparison;
  }

  /**
   * Generate comparison tables
   */
  generateComparisonTables(allComparisons) {
    console.log("\n" + "═".repeat(80));
    console.log("ACCURACY COMPARISON TABLE");
    console.log("═".repeat(80));
    console.log("Frequency | Approach      | MAPE (%)   | MAE        | RMSE       | Windows");
    console.log("─".repeat(80));

    allComparisons.forEach((comp) => {
      if (!comp) return;

      const freq = comp.frequency.toString().padEnd(9);

      // Fetching (baseline)
      console.log(`${freq} | Fetching      | Baseline   | Baseline   | Baseline   | ${comp.fetching.resultCount}`);

      // Approximation
      if (comp.approximation && comp.approximation.accuracy) {
        const acc = comp.approximation.accuracy;
        console.log(
          `${" ".repeat(9)} | Approximation | ${acc.mape.toFixed(4).padEnd(10)} | ` +
          `${acc.mae.toFixed(6).padEnd(10)} | ${acc.rmse.toFixed(6).padEnd(10)} | ${comp.approximation.resultCount}`,
        );
      } else {
        console.log(`${" ".repeat(9)} | Approximation | N/A        | N/A        | N/A        | N/A`);
      }

      // Chunked
      if (comp.chunked && comp.chunked.accuracy) {
        const acc = comp.chunked.accuracy;
        console.log(
          `${" ".repeat(9)} | Chunked       | ${acc.mape.toFixed(4).padEnd(10)} | ` +
          `${acc.mae.toFixed(6).padEnd(10)} | ${acc.rmse.toFixed(6).padEnd(10)} | ${comp.chunked.resultCount}`,
        );
      } else {
        console.log(`${" ".repeat(9)} | Chunked       | N/A        | N/A        | N/A        | N/A`);
      }

      console.log("─".repeat(80));
    });

    console.log("\n" + "═".repeat(80));
    console.log("LATENCY COMPARISON TABLE");
    console.log("═".repeat(80));
    console.log("Frequency | Approach      | First-Event Latency | Difference vs Fetching");
    console.log("─".repeat(80));

    allComparisons.forEach((comp) => {
      if (!comp) return;

      const freq = comp.frequency.toString().padEnd(9);

      // Fetching
      console.log(`${freq} | Fetching      | ${comp.fetching.firstEventLatencySeconds.padEnd(19)}s | Baseline`);

      // Approximation
      if (comp.approximation) {
        const diff = (comp.approximation.latencyDifference / 1000).toFixed(2);
        const sign = comp.approximation.latencyDifference >= 0 ? "+" : "";
        console.log(
          `${" ".repeat(9)} | Approximation | ${comp.approximation.firstEventLatencySeconds.padEnd(19)}s | ${sign}${diff}s`,
        );
      } else {
        console.log(`${" ".repeat(9)} | Approximation | N/A                 | N/A`);
      }

      // Chunked
      if (comp.chunked) {
        const diff = (comp.chunked.latencyDifference / 1000).toFixed(2);
        const sign = comp.chunked.latencyDifference >= 0 ? "+" : "";
        console.log(
          `${" ".repeat(9)} | Chunked       | ${comp.chunked.firstEventLatencySeconds.padEnd(19)}s | ${sign}${diff}s`,
        );
      } else {
        console.log(`${" ".repeat(9)} | Chunked       | N/A                 | N/A`);
      }

      console.log("─".repeat(80));
    });
  }

  /**
   * Save detailed results to CSV files
   */
  saveDetailedResults(allComparisons) {
    // Accuracy CSV
    const accuracyPath = path.join(this.baseLogDir, "accuracy_comparison_all_approaches.csv");
    let accuracyCsv = "frequency,approach,mape_percent,mae,rmse,window_count,avg_value\n";

    allComparisons.forEach((comp) => {
      if (!comp) return;

      // Fetching baseline
      accuracyCsv += `${comp.frequency},fetching,0.0,0.0,0.0,${comp.fetching.resultCount},${comp.fetching.avgValue}\n`;

      // Approximation
      if (comp.approximation && comp.approximation.accuracy) {
        const acc = comp.approximation.accuracy;
        accuracyCsv += `${comp.frequency},approximation,${acc.mape},${acc.mae},${acc.rmse},${comp.approximation.resultCount},${comp.approximation.avgValue}\n`;
      }

      // Chunked
      if (comp.chunked && comp.chunked.accuracy) {
        const acc = comp.chunked.accuracy;
        accuracyCsv += `${comp.frequency},chunked,${acc.mape},${acc.mae},${acc.rmse},${comp.chunked.resultCount},${comp.chunked.avgValue}\n`;
      }
    });

    fs.writeFileSync(accuracyPath, accuracyCsv);
    console.log(`\n📄 Accuracy comparison saved to: ${accuracyPath}`);

    // Latency CSV
    const latencyPath = path.join(this.baseLogDir, "latency_comparison_all_approaches.csv");
    let latencyCsv = "frequency,approach,first_event_latency_ms,first_event_latency_s,difference_vs_fetching_ms\n";

    allComparisons.forEach((comp) => {
      if (!comp) return;

      // Fetching
      latencyCsv += `${comp.frequency},fetching,${comp.fetching.firstEventLatency},${comp.fetching.firstEventLatencySeconds},0\n`;

      // Approximation
      if (comp.approximation) {
        latencyCsv += `${comp.frequency},approximation,${comp.approximation.firstEventLatency},${comp.approximation.firstEventLatencySeconds},${comp.approximation.latencyDifference}\n`;
      }

      // Chunked
      if (comp.chunked) {
        latencyCsv += `${comp.frequency},chunked,${comp.chunked.firstEventLatency},${comp.chunked.firstEventLatencySeconds},${comp.chunked.latencyDifference}\n`;
      }
    });

    fs.writeFileSync(latencyPath, latencyCsv);
    console.log(`📄 Latency comparison saved to: ${latencyPath}`);

    // JSON summary
    const summaryPath = path.join(this.baseLogDir, "comparison_summary_all_approaches.json");
    const summary = {
      timestamp: new Date().toISOString(),
      oscillationType: this.oscillationType,
      frequencies: this.frequencies,
      approaches: this.approaches,
      comparisons: allComparisons,
    };

    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
    console.log(`📄 JSON summary saved to: ${summaryPath}`);
  }

  /**
   * Run complete comparison analysis
   */
  run() {
    console.log("═".repeat(80));
    console.log("COMPREHENSIVE ACCURACY & LATENCY COMPARISON - ALL APPROACHES");
    console.log("═".repeat(80));
    console.log("Approaches: Fetching (baseline), Approximation, Chunked");
    console.log(`Frequencies: ${this.frequencies.join(", ")} Hz`);
    console.log("═".repeat(80));

    const allComparisons = [];

    for (const frequency of this.frequencies) {
      const comparison = this.compareFrequency(frequency);
      allComparisons.push(comparison);
    }

    // Generate summary tables
    this.generateComparisonTables(allComparisons);

    // Save detailed results
    this.saveDetailedResults(allComparisons);

    console.log("\n" + "═".repeat(80));
    console.log("ANALYSIS COMPLETE");
    console.log("═".repeat(80));
    console.log("\nKey Findings:");
    console.log("  - Fetching approach is the baseline (ground truth)");
    console.log("  - MAPE shows percentage accuracy difference vs baseline");
    console.log("  - First-event latency shows time to first result");
    console.log("  - All approaches should have similar latency (~60s for 60s STEP window)");
    console.log("═".repeat(80));
  }
}

// Main execution
if (require.main === module) {
  const comparison = new AllApproachesComparison();
  comparison.run();
}

module.exports = AllApproachesComparison;
