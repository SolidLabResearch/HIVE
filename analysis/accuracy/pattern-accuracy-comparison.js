#!/usr/bin/env node

/**
 * Pattern Accuracy Comparison Analysis
 *
 * Analyzes accuracy and performance across different data patterns:
 * - Exponential growth/decay patterns
 * - Noisy datasets
 *
 * Compares: Fetching (baseline), Approximation, Chunked
 *
 * Generates:
 * - Accuracy metrics (MAPE, MAE, RMSE) per pattern
 * - First-event latency comparison
 * - Pattern-specific analysis (where does approximation fail?)
 * - CSV outputs and summary reports
 *
 * Usage:
 *   node pattern-accuracy-comparison.js
 */

const fs = require("fs");
const path = require("path");

class PatternAccuracyComparison {
  constructor() {
    this.baseLogDir = "./logs/pattern-comparison";
    this.approaches = ["fetching", "approximation", "chunked"];

    // Define all patterns
    this.exponentialRates = [0.001, 0.01, 0.1, 1, 10, 100];
    this.noiseLevels = [0.1, 0.2, 0.5, 1.0, 2.0];
  }

  getAllPatterns() {
    const patterns = [];

    // Exponential growth
    this.exponentialRates.forEach((rate) => {
      patterns.push({
        type: "exponential_growth",
        value: rate,
        name: `exponential_growth_rate_${rate}`,
      });
    });

    // Exponential decay
    this.exponentialRates.forEach((rate) => {
      patterns.push({
        type: "exponential_decay",
        value: rate,
        name: `exponential_decay_rate_${rate}`,
      });
    });

    // Noisy datasets
    this.noiseLevels.forEach((level) => {
      patterns.push({
        type: "noise",
        value: level,
        name: `noise_${level}`,
      });
    });

    return patterns;
  }

  readMetadata(approach, patternName) {
    const metadataPath = path.join(
      this.baseLogDir,
      approach,
      patternName,
      "iteration1",
      `${approach}_metadata.json`,
    );

    if (!fs.existsSync(metadataPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(metadataPath, "utf8");
      return JSON.parse(content);
    } catch (error) {
      console.error(`Error reading ${metadataPath}:`, error.message);
      return null;
    }
  }

  readResults(approach, patternName) {
    const resultsPath = path.join(
      this.baseLogDir,
      approach,
      patternName,
      "iteration1",
      `${approach}_results.csv`,
    );

    if (!fs.existsSync(resultsPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(resultsPath, "utf8");
      const lines = content.trim().split("\n");

      if (lines.length < 2) {
        return null;
      }

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
      console.error(`Error reading ${resultsPath}:`, error.message);
      return null;
    }
  }

  readResourceUsage(approach, patternName) {
    const resourceFileMap = {
      fetching: "fetching_resource_usage.csv",
      approximation: "approximation_approach_resource_usage.csv",
      chunked: "streaming_query_hive_resource_log.csv",
    };

    const resourcePath = path.join(
      this.baseLogDir,
      approach,
      patternName,
      "iteration1",
      resourceFileMap[approach],
    );

    if (!fs.existsSync(resourcePath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(resourcePath, "utf8");
      const lines = content.trim().split("\n");

      if (lines.length < 2) {
        return null;
      }

      const measurements = [];
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(",");
        if (parts.length >= 7) {
          measurements.push({
            timestamp: parseInt(parts[0]),
            cpuUser: parseFloat(parts[1]),
            cpuSystem: parseFloat(parts[2]),
            rss: parseInt(parts[3]),
            heapTotal: parseInt(parts[4]),
            heapUsed: parseInt(parts[5]),
            heapUsedMB: parseFloat(parts[6]),
          });
        }
      }

      if (measurements.length === 0) {
        return null;
      }

      // Calculate statistics
      const heapUsedMBValues = measurements.map((m) => m.heapUsedMB);
      const avgHeapMB =
        heapUsedMBValues.reduce((a, b) => a + b, 0) / heapUsedMBValues.length;
      const maxHeapMB = Math.max(...heapUsedMBValues);

      return {
        avgHeapMB,
        maxHeapMB,
        measurementCount: measurements.length,
      };
    } catch (error) {
      console.error(`Error reading ${resourcePath}:`, error.message);
      return null;
    }
  }

  calculateAccuracyMetrics(baselineResults, approachResults) {
    if (!baselineResults || !approachResults) {
      return null;
    }

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
      const percentageError =
        baseline !== 0 ? (absoluteError / Math.abs(baseline)) * 100 : 0;

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

  analyzePattern(pattern) {
    console.log(`\nAnalyzing: ${pattern.name}`);
    console.log("─".repeat(80));

    // Read fetching (baseline) data
    const fetchingMetadata = this.readMetadata("fetching", pattern.name);
    const fetchingResults = this.readResults("fetching", pattern.name);
    const fetchingResources = this.readResourceUsage("fetching", pattern.name);

    if (!fetchingMetadata || !fetchingResults) {
      console.log(`  ✗ Fetching data not available - skipping ${pattern.name}`);
      return null;
    }

    const comparison = {
      pattern: pattern.name,
      patternType: pattern.type,
      patternValue: pattern.value,
      fetching: {
        firstEventLatency: fetchingMetadata.firstEventLatency,
        firstEventLatencySeconds: fetchingMetadata.firstEventLatencySeconds,
        resultCount: fetchingResults.length,
        avgValue:
          fetchingResults.length > 0
            ? fetchingResults.reduce((sum, r) => sum + r.resultValue, 0) /
              fetchingResults.length
            : null,
        resources: fetchingResources,
      },
      approximation: null,
      chunked: null,
    };

    // Compare approximation
    const approxMetadata = this.readMetadata("approximation", pattern.name);
    const approxResults = this.readResults("approximation", pattern.name);
    const approxResources = this.readResourceUsage(
      "approximation",
      pattern.name,
    );

    if (approxMetadata && approxResults) {
      const accuracyMetrics = this.calculateAccuracyMetrics(
        fetchingResults,
        approxResults,
      );

      comparison.approximation = {
        firstEventLatency: approxMetadata.firstEventLatency,
        firstEventLatencySeconds: approxMetadata.firstEventLatencySeconds,
        latencyDifference:
          approxMetadata.firstEventLatency -
          fetchingMetadata.firstEventLatency,
        resultCount: approxResults.length,
        avgValue:
          approxResults.length > 0
            ? approxResults.reduce((sum, r) => sum + r.resultValue, 0) /
              approxResults.length
            : null,
        accuracy: accuracyMetrics,
        resources: approxResources,
      };

      console.log(`  Approximation:`);
      console.log(
        `    Latency: ${approxMetadata.firstEventLatencySeconds}s (Δ ${(comparison.approximation.latencyDifference / 1000).toFixed(2)}s)`,
      );
      if (accuracyMetrics) {
        console.log(`    MAPE: ${accuracyMetrics.mape.toFixed(4)}%`);
        console.log(`    MAE: ${accuracyMetrics.mae.toFixed(6)}`);
        console.log(`    RMSE: ${accuracyMetrics.rmse.toFixed(6)}`);
      }
      if (approxResources) {
        console.log(`    Memory: ${approxResources.avgHeapMB.toFixed(2)} MB avg, ${approxResources.maxHeapMB.toFixed(2)} MB max`);
      }
    } else {
      console.log(`  Approximation: ✗ Data not available`);
    }

    // Compare chunked
    const chunkedMetadata = this.readMetadata("chunked", pattern.name);
    const chunkedResults = this.readResults("chunked", pattern.name);
    const chunkedResources = this.readResourceUsage("chunked", pattern.name);

    if (chunkedMetadata && chunkedResults) {
      const accuracyMetrics = this.calculateAccuracyMetrics(
        fetchingResults,
        chunkedResults,
      );

      comparison.chunked = {
        firstEventLatency: chunkedMetadata.firstEventLatency,
        firstEventLatencySeconds: chunkedMetadata.firstEventLatencySeconds,
        latencyDifference:
          chunkedMetadata.firstEventLatency - fetchingMetadata.firstEventLatency,
        resultCount: chunkedResults.length,
        avgValue:
          chunkedResults.length > 0
            ? chunkedResults.reduce((sum, r) => sum + r.resultValue, 0) /
              chunkedResults.length
            : null,
        accuracy: accuracyMetrics,
        resources: chunkedResources,
      };

      console.log(`  Chunked:`);
      console.log(
        `    Latency: ${chunkedMetadata.firstEventLatencySeconds}s (Δ ${(comparison.chunked.latencyDifference / 1000).toFixed(2)}s)`,
      );
      if (accuracyMetrics) {
        console.log(`    MAPE: ${accuracyMetrics.mape.toFixed(4)}%`);
        console.log(`    MAE: ${accuracyMetrics.mae.toFixed(6)}`);
        console.log(`    RMSE: ${accuracyMetrics.rmse.toFixed(6)}`);
      }
      if (chunkedResources) {
        console.log(`    Memory: ${chunkedResources.avgHeapMB.toFixed(2)} MB avg, ${chunkedResources.maxHeapMB.toFixed(2)} MB max`);
      }
    } else {
      console.log(`  Chunked: ✗ Data not available`);
    }

    return comparison;
  }

  generateAccuracyTable(comparisons) {
    console.log("\n" + "═".repeat(120));
    console.log("ACCURACY COMPARISON BY PATTERN");
    console.log("═".repeat(120));
    console.log(
      "Pattern Type       | Pattern Value | Approach      | MAPE (%)   | MAE        | RMSE       | Memory (MB)",
    );
    console.log("─".repeat(120));

    const validComparisons = comparisons.filter((c) => c !== null);

    validComparisons.forEach((comp) => {
      const patternTypeStr = comp.patternType.padEnd(18);
      const patternValueStr = String(comp.patternValue).padEnd(13);

      // Fetching (baseline)
      const fetchingMem = comp.fetching.resources
        ? `${comp.fetching.resources.avgHeapMB.toFixed(1)}`
        : "N/A";
      console.log(
        `${patternTypeStr} | ${patternValueStr} | Fetching      | Baseline   | Baseline   | Baseline   | ${fetchingMem}`,
      );

      // Approximation
      if (comp.approximation && comp.approximation.accuracy) {
        const acc = comp.approximation.accuracy;
        const approxMem = comp.approximation.resources
          ? `${comp.approximation.resources.avgHeapMB.toFixed(1)}`
          : "N/A";
        console.log(
          `${" ".repeat(18)} | ${" ".repeat(13)} | Approximation | ${acc.mape.toFixed(4).padEnd(10)} | ` +
            `${acc.mae.toFixed(6).padEnd(10)} | ${acc.rmse.toFixed(6).padEnd(10)} | ${approxMem}`,
        );
      } else {
        console.log(
          `${" ".repeat(18)} | ${" ".repeat(13)} | Approximation | N/A        | N/A        | N/A        | N/A`,
        );
      }

      // Chunked
      if (comp.chunked && comp.chunked.accuracy) {
        const acc = comp.chunked.accuracy;
        const chunkedMem = comp.chunked.resources
          ? `${comp.chunked.resources.avgHeapMB.toFixed(1)}`
          : "N/A";
        console.log(
          `${" ".repeat(18)} | ${" ".repeat(13)} | Chunked       | ${acc.mape.toFixed(4).padEnd(10)} | ` +
            `${acc.mae.toFixed(6).padEnd(10)} | ${acc.rmse.toFixed(6).padEnd(10)} | ${chunkedMem}`,
        );
      } else {
        console.log(
          `${" ".repeat(18)} | ${" ".repeat(13)} | Chunked       | N/A        | N/A        | N/A        | N/A`,
        );
      }

      console.log("─".repeat(120));
    });
  }

  analyzeApproximationBreakdown(comparisons) {
    console.log("\n" + "═".repeat(80));
    console.log("APPROXIMATION ACCURACY BREAKDOWN");
    console.log("═".repeat(80));

    const validComparisons = comparisons.filter(
      (c) => c !== null && c.approximation && c.approximation.accuracy,
    );

    // Group by pattern type
    const exponentialGrowth = validComparisons.filter(
      (c) => c.patternType === "exponential_growth",
    );
    const exponentialDecay = validComparisons.filter(
      (c) => c.patternType === "exponential_decay",
    );
    const noisy = validComparisons.filter((c) => c.patternType === "noise");

    // Exponential Growth
    if (exponentialGrowth.length > 0) {
      console.log("\nExponential Growth:");
      console.log("Rate    | MAPE (%)  | Status");
      console.log("─".repeat(40));
      exponentialGrowth
        .sort((a, b) => a.patternValue - b.patternValue)
        .forEach((c) => {
          const mape = c.approximation.accuracy.mape;
          const status =
            mape < 1 ? "✓ Good" : mape < 5 ? "⚠ Fair" : "✗ Poor";
          console.log(
            `${String(c.patternValue).padEnd(7)} | ${mape.toFixed(4).padEnd(9)} | ${status}`,
          );
        });
    }

    // Exponential Decay
    if (exponentialDecay.length > 0) {
      console.log("\nExponential Decay:");
      console.log("Rate    | MAPE (%)  | Status");
      console.log("─".repeat(40));
      exponentialDecay
        .sort((a, b) => a.patternValue - b.patternValue)
        .forEach((c) => {
          const mape = c.approximation.accuracy.mape;
          const status =
            mape < 1 ? "✓ Good" : mape < 5 ? "⚠ Fair" : "✗ Poor";
          console.log(
            `${String(c.patternValue).padEnd(7)} | ${mape.toFixed(4).padEnd(9)} | ${status}`,
          );
        });
    }

    // Noisy Datasets
    if (noisy.length > 0) {
      console.log("\nNoisy Datasets:");
      console.log("Noise   | MAPE (%)  | Status");
      console.log("─".repeat(40));
      noisy
        .sort((a, b) => a.patternValue - b.patternValue)
        .forEach((c) => {
          const mape = c.approximation.accuracy.mape;
          const status =
            mape < 1 ? "✓ Good" : mape < 5 ? "⚠ Fair" : "✗ Poor";
          console.log(
            `${String(c.patternValue).padEnd(7)} | ${mape.toFixed(4).padEnd(9)} | ${status}`,
          );
        });
    }

    // Summary
    console.log("\n" + "═".repeat(80));
    console.log("KEY FINDINGS:");
    console.log("─".repeat(80));

    const allMapes = validComparisons
      .map((c) => c.approximation.accuracy.mape)
      .filter((m) => !isNaN(m));
    const avgMape = allMapes.reduce((a, b) => a + b, 0) / allMapes.length;
    const maxMape = Math.max(...allMapes);
    const minMape = Math.min(...allMapes);

    console.log(`Average MAPE: ${avgMape.toFixed(4)}%`);
    console.log(`Min MAPE: ${minMape.toFixed(4)}%`);
    console.log(`Max MAPE: ${maxMape.toFixed(4)}%`);

    const goodCount = allMapes.filter((m) => m < 1).length;
    const fairCount = allMapes.filter((m) => m >= 1 && m < 5).length;
    const poorCount = allMapes.filter((m) => m >= 5).length;

    console.log(`\nAccuracy Distribution:`);
    console.log(`  Good (< 1%): ${goodCount}/${allMapes.length}`);
    console.log(`  Fair (1-5%): ${fairCount}/${allMapes.length}`);
    console.log(`  Poor (≥ 5%): ${poorCount}/${allMapes.length}`);
  }

  saveResults(comparisons) {
    const validComparisons = comparisons.filter((c) => c !== null);

    // Accuracy CSV
    const accuracyPath = path.join(
      this.baseLogDir,
      "pattern_accuracy_comparison.csv",
    );
    let accuracyCsv =
      "pattern_type,pattern_value,approach,mape_percent,mae,rmse,avg_value,memory_mb\n";

    validComparisons.forEach((comp) => {
      // Fetching
      const fetchingMem = comp.fetching.resources
        ? comp.fetching.resources.avgHeapMB.toFixed(2)
        : "N/A";
      accuracyCsv += `${comp.patternType},${comp.patternValue},fetching,0.0,0.0,0.0,${comp.fetching.avgValue},${fetchingMem}\n`;

      // Approximation
      if (comp.approximation && comp.approximation.accuracy) {
        const acc = comp.approximation.accuracy;
        const approxMem = comp.approximation.resources
          ? comp.approximation.resources.avgHeapMB.toFixed(2)
          : "N/A";
        accuracyCsv += `${comp.patternType},${comp.patternValue},approximation,${acc.mape},${acc.mae},${acc.rmse},${comp.approximation.avgValue},${approxMem}\n`;
      }

      // Chunked
      if (comp.chunked && comp.chunked.accuracy) {
        const acc = comp.chunked.accuracy;
        const chunkedMem = comp.chunked.resources
          ? comp.chunked.resources.avgHeapMB.toFixed(2)
          : "N/A";
        accuracyCsv += `${comp.patternType},${comp.patternValue},chunked,${acc.mape},${acc.mae},${acc.rmse},${comp.chunked.avgValue},${chunkedMem}\n`;
      }
    });

    fs.writeFileSync(accuracyPath, accuracyCsv);
    console.log(`\n📄 Accuracy comparison saved to: ${accuracyPath}`);

    // Summary JSON
    const summaryPath = path.join(
      this.baseLogDir,
      "pattern_analysis_summary.json",
    );
    const summary = {
      timestamp: new Date().toISOString(),
      totalPatterns: validComparisons.length,
      comparisons: validComparisons,
    };

    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
    console.log(`📄 Summary saved to: ${summaryPath}`);
  }

  run() {
    console.log("═".repeat(80));
    console.log("PATTERN ACCURACY COMPARISON - ALL APPROACHES");
    console.log("═".repeat(80));

    const allPatterns = this.getAllPatterns();
    console.log(`Total patterns to analyze: ${allPatterns.length}`);

    const comparisons = [];

    for (const pattern of allPatterns) {
      const comparison = this.analyzePattern(pattern);
      comparisons.push(comparison);
    }

    // Generate reports
    this.generateAccuracyTable(comparisons);
    this.analyzeApproximationBreakdown(comparisons);

    // Save results
    this.saveResults(comparisons);

    console.log("\n" + "═".repeat(80));
    console.log("ANALYSIS COMPLETE");
    console.log("═".repeat(80));
  }
}

// Main execution
if (require.main === module) {
  const comparison = new PatternAccuracyComparison();
  comparison.run();
}

module.exports = PatternAccuracyComparison;
