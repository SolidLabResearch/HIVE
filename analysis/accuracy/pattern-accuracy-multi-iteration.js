#!/usr/bin/env node

/**
 * Pattern Accuracy Multi-Iteration Analysis
 *
 * Analyzes accuracy, latency, and resource usage across multiple iterations.
 *
 * Comparisons:
 * 1. Latency (First-event latency)
 * 2. Resources (Heap Usage)
 * 3. Accuracy (MAPE, MAE, RMSE) vs Ground Truth (Fetching)
 * 4. Pattern robustness
 *
 * Usage:
 *   node analysis/accuracy/pattern-accuracy-multi-iteration.js
 */

const fs = require("fs");
const path = require("path");

class PatternMultiIterationAnalysis {
  constructor() {
    this.baseLogDir = "./logs/pattern-comparison";
    this.approaches = ["fetching", "approximation", "chunked"];
    this.exponentialRates = [0.001, 0.01, 0.1, 1, 10, 100];
    this.noiseLevels = [0.1, 0.2, 0.5, 1.0, 2.0];
  }

  getAllPatterns() {
    const patterns = [];
    this.exponentialRates.forEach((rate) => {
      patterns.push({ type: "exponential_growth", value: rate, name: `exponential_growth_rate_${rate}` });
      patterns.push({ type: "exponential_decay", value: rate, name: `exponential_decay_rate_${rate}` });
    });
    this.noiseLevels.forEach((level) => {
      patterns.push({ type: "noise", value: level, name: `noise_${level}` });
    });
    return patterns;
  }

  getIterationDirs(approach, patternName) {
    const patternDir = path.join(this.baseLogDir, approach, patternName);
    if (!fs.existsSync(patternDir)) return [];

    return fs.readdirSync(patternDir)
      .filter(dir => dir.startsWith('iteration'))
      .map(dir => parseInt(dir.replace('iteration', '')))
      .sort((a, b) => a - b);
  }

  readMetadata(approach, patternName, iteration) {
    const metadataPath = path.join(this.baseLogDir, approach, patternName, `iteration${iteration}`, `${approach}_metadata.json`);
    try {
      if (fs.existsSync(metadataPath)) return JSON.parse(fs.readFileSync(metadataPath, "utf8"));
    } catch (e) { /* ignore */ }
    return null;
  }

  readResults(approach, patternName, iteration) {
    const resultsPath = path.join(this.baseLogDir, approach, patternName, `iteration${iteration}`, `${approach}_results.csv`);
    try {
      if (!fs.existsSync(resultsPath)) return null;
      const content = fs.readFileSync(resultsPath, "utf8");
      const lines = content.trim().split("\n");
      const results = [];
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(",");
        if (parts.length >= 3) {
          results.push({
            windowNumber: parseInt(parts[1]),
            resultValue: parseFloat(parts[2]),
          });
        }
      }
      return results;
    } catch (e) { return null; }
  }

  readResources(approach, patternName, iteration) {
    const resourceFileMap = {
      fetching: "fetching_resource_usage.csv",
      approximation: "approximation_approach_resource_usage.csv",
      chunked: "streaming_query_hive_resource_log.csv",
    };
    const resourcePath = path.join(this.baseLogDir, approach, patternName, `iteration${iteration}`, resourceFileMap[approach]);
    try {
      if (!fs.existsSync(resourcePath)) return null;
      const content = fs.readFileSync(resourcePath, "utf8");
      const lines = content.trim().split("\n");
      const heaps = [];
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(",");
        if (parts.length >= 7) heaps.push(parseFloat(parts[6])); // heapUsedMB
      }
      if (heaps.length === 0) return null;
      return {
        avgHeap: heaps.reduce((a, b) => a + b, 0) / heaps.length,
        maxHeap: Math.max(...heaps)
      };
    } catch (e) { return null; }
  }

  calculateAccuracy(baseline, actual) {
    if (!baseline || !actual || baseline.length === 0 || actual.length === 0) return null;
    const n = Math.min(baseline.length, actual.length);
    let sumAbs = 0, sumSq = 0, sumPct = 0;

    for (let i = 0; i < n; i++) {
      const baseVal = baseline[i].resultValue;
      const actVal = actual[i].resultValue;
      const err = Math.abs(actVal - baseVal);
      sumAbs += err;
      sumSq += err * err;
      if (baseVal !== 0) sumPct += (err / Math.abs(baseVal));
    }

    return {
      mae: sumAbs / n,
      rmse: Math.sqrt(sumSq / n),
      mape: (sumPct / n) * 100
    };
  }

  calculateStats(values) {
    if (values.length === 0) return { mean: 0, std: 0, min: 0, max: 0, count: 0 };
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
    return {
      mean,
      std: Math.sqrt(variance),
      min: Math.min(...values),
      max: Math.max(...values),
      count: values.length
    };
  }

  analyze() {
    const patterns = this.getAllPatterns();
    const finalResults = [];

    console.log("Analyzing patterns...");

    for (const pattern of patterns) {
      // Find common iterations (iterations that exist for ALL approaches)
      const iterSets = this.approaches.map(app => this.getIterationDirs(app, pattern.name));
      // Intersection of all iterations
      const commonIterations = iterSets.reduce((a, b) => a.filter(c => b.includes(c)));

      if (commonIterations.length === 0) {
        // console.log(`Skipping ${pattern.name} (no complete iterations)`);
        continue;
      }

      console.log(`Analyzing ${pattern.name} (${commonIterations.length} iterations)`);

      const metrics = {
        fetching: { latency: [], avgHeap: [], maxHeap: [], mae: [], mape: [], rmse: [] },
        approximation: { latency: [], avgHeap: [], maxHeap: [], mae: [], mape: [], rmse: [] },
        chunked: { latency: [], avgHeap: [], maxHeap: [], mae: [], mape: [], rmse: [] }
      };

      for (const iter of commonIterations) {
        const fetchingRes = this.readResults("fetching", pattern.name, iter);

        for (const approach of this.approaches) {
          const meta = this.readMetadata(approach, pattern.name, iter);
          const res = this.readResults(approach, pattern.name, iter);
          const resources = this.readResources(approach, pattern.name, iter);

          // Latency
          if (meta && meta.firstEventLatency !== null) {
            metrics[approach].latency.push(meta.firstEventLatency);
          }

          // Resources
          if (resources) {
            metrics[approach].avgHeap.push(resources.avgHeap);
            metrics[approach].maxHeap.push(resources.maxHeap);
          }

          // Accuracy
          if (approach === "fetching") {
            // Baseline has 0 error
            metrics[approach].mae.push(0);
            metrics[approach].mape.push(0);
            metrics[approach].rmse.push(0);
          } else {
            const acc = this.calculateAccuracy(fetchingRes, res);
            if (acc) {
              metrics[approach].mae.push(acc.mae);
              metrics[approach].mape.push(acc.mape);
              metrics[approach].rmse.push(acc.rmse);
            }
          }
        }
      }

      // Aggregate
      for (const approach of this.approaches) {
        finalResults.push({
          pattern: pattern.name,
          type: pattern.type,
          value: pattern.value,
          approach: approach,
          iterations: commonIterations.length,
          latency: this.calculateStats(metrics[approach].latency),
          avgHeap: this.calculateStats(metrics[approach].avgHeap),
          maxHeap: this.calculateStats(metrics[approach].maxHeap),
          mae: this.calculateStats(metrics[approach].mae),
          mape: this.calculateStats(metrics[approach].mape),
          rmse: this.calculateStats(metrics[approach].rmse)
        });
      }
    }

    this.writeReports(finalResults);
  }

  writeReports(results) {
    // 1. CSV Report
    const csvHeader = [
      "Pattern", "Type", "Value", "Approach", "Iterations",
      "Latency_Mean_ms", "Latency_Std",
      "Heap_Avg_Mean_MB", "Heap_Avg_Std",
      "Heap_Max_Mean_MB", "Heap_Max_Std",
      "MAPE_Mean_%", "MAPE_Std",
      "MAE_Mean", "MAE_Std",
      "RMSE_Mean", "RMSE_Std"
    ].join(",");

    const csvRows = results.map(r => [
      r.pattern, r.type, r.value, r.approach, r.iterations,
      r.latency.mean.toFixed(2), r.latency.std.toFixed(2),
      r.avgHeap.mean.toFixed(2), r.avgHeap.std.toFixed(2),
      r.maxHeap.mean.toFixed(2), r.maxHeap.std.toFixed(2),
      r.mape.mean.toFixed(4), r.mape.std.toFixed(4),
      r.mae.mean.toFixed(6), r.mae.std.toFixed(6),
      r.rmse.mean.toFixed(6), r.rmse.std.toFixed(6)
    ].join(","));

    const csvPath = path.join(this.baseLogDir, "pattern_accuracy_multi_iteration_summary.csv");
    fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join("\n"));
    console.log(`\nCSV Report saved to: ${csvPath}`);

    // 2. Summary JSON
    const jsonPath = path.join(this.baseLogDir, "pattern_accuracy_multi_iteration_summary.json");
    fs.writeFileSync(jsonPath, JSON.stringify(results, null, 2));
    console.log(`JSON Report saved to: ${jsonPath}`);

    // 3. Console Table (Latency & Accuracy Focus)
    console.log("\n" + "=".repeat(100));
    console.log("MULTI-ITERATION RESULTS SUMMARY (Mean values)");
    console.log("=".repeat(100));
    console.log("Pattern".padEnd(30) + "Approach".padEnd(15) + "Latency (ms)".padEnd(15) + "MAPE (%)".padEnd(15) + "Heap Avg (MB)".padEnd(15));
    console.log("-".repeat(90));

    results.forEach(r => {
      console.log(
        r.pattern.padEnd(30) +
        r.approach.padEnd(15) +
        `${r.latency.mean.toFixed(0)} ±${r.latency.std.toFixed(0)}`.padEnd(15) +
        `${r.mape.mean.toFixed(3)} ±${r.mape.std.toFixed(3)}`.padEnd(15) +
        `${r.avgHeap.mean.toFixed(1)} ±${r.avgHeap.std.toFixed(1)}`.padEnd(15)
      );
      if (r.approach === "chunked") console.log("-".repeat(90)); // Separator after set
    });
  }
}

if (require.main === module) {
  new PatternMultiIterationAnalysis().analyze();
}
