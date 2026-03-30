#!/usr/bin/env node

/**
 * Custom Pattern Analysis - All Approaches
 *
 * Tests all three approaches (Fetching, Approximation, Chunked) across 5 custom patterns:
 * 1. Low Variability (μ=-23.0, σ=0.25)
 * 2. Step Pattern (v1=-23.0, v2=-15.0, t_step=60s)
 * 3. Spike Pattern (v_base=-23.0, v_spike=-5.0, Δt=1.25s)
 * 4. Low Freq. Oscillation (μ=-23.0, A=5.0, f=0.05Hz)
 * 5. High Freq. Oscillation (μ=-23.0, A=3.0, f=0.5Hz)
 *
 * Measures:
 * - Accuracy (MAPE, MAE, RMSE)
 * - First-event latency
 * - Resource usage (CPU, memory)
 *
 * Usage:
 *   node run-custom-patterns-comparison.js                    # Run all patterns (default 35 iterations)
 *   node run-custom-patterns-comparison.js --iterations 10    # Run with 10 iterations
 *   node run-custom-patterns-comparison.js -i 35              # Short flag
 *   node run-custom-patterns-comparison.js low_variability    # Run specific pattern
 */

const { spawn, execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

class CustomPatternComparisonRunner {
  constructor(iterations = 35) {
    this.iterations = iterations;
    this.approaches = ["fetching", "naive_distributed", "approximation", "chunked"];

    // Custom patterns matching the table specification
    this.patterns = [
      {
        type: "low_variability",
        name: "Low Variability",
        params: "μ=-23.0, σ=0.25",
      },
      {
        type: "step_pattern",
        name: "Step Pattern",
        params: "v₁=-23.0, v₂=-15.0, t_step=60s",
      },
      {
        type: "spike_pattern",
        name: "Spike Pattern",
        params: "v_base=-23.0, v_spike=-5.0, Δt=1.25s",
      },
      {
        type: "low_freq_oscillation",
        name: "Low Freq. Oscillation",
        params: "μ=-23.0, A=5.0, f=0.05Hz",
      },
      {
        type: "high_freq_oscillation",
        name: "High Freq. Oscillation",
        params: "μ=-23.0, A=3.0, f=0.5Hz",
      },
    ];

    this.baseLogDir = "./logs/custom-pattern-comparison";
    this.timeout = 240000; // 4 minutes per test
  }

  getDataPath(pattern) {
    return `custom_patterns/${pattern.type}`;
  }

  getPatternName(pattern) {
    return pattern.type;
  }

  getApproachScript(approach) {
    const scripts = {
      fetching:
        "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
      approximation:
        "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
      chunked: "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
      naive_distributed: "dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js",
    };
    return scripts[approach];
  }

  cleanupStaleProcesses() {
    try { execSync('pkill -f "dist/approaches" 2>/dev/null || true', { stdio: 'ignore' }); } catch (_) {}
    try { execSync('pkill -f "dist/services/BeeWorker" 2>/dev/null || true', { stdio: 'ignore' }); } catch (_) {}
    try { execSync('lsof -ti:8080 | xargs kill -9 2>/dev/null || true', { stdio: 'ignore' }); } catch (_) {}
    return new Promise(resolve => setTimeout(resolve, 1500));
  }

  async runSingleTest(approach, pattern, iterationNum = 1) {
    await this.cleanupStaleProcesses();
    const patternName = this.getPatternName(pattern);
    const dataPath = this.getDataPath(pattern);

    console.log(`\n${"=".repeat(80)}`);
    console.log(
      `TESTING: ${approach.toUpperCase()} - ${pattern.name} - Iteration ${iterationNum}/${this.iterations}`,
    );
    console.log(`Pattern: ${pattern.params}`);
    console.log(`Data: ${dataPath}`);
    console.log("=".repeat(80));

    const logDir = path.join(
      this.baseLogDir,
      approach,
      patternName,
      `iteration${iterationNum}`,
    );
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }

    // Check if data exists
    const smartphoneDataPath = path.join(
      "src/streamer/data",
      dataPath,
      "smartphone.acceleration.x",
      "data.nt",
    );
    if (!fs.existsSync(smartphoneDataPath)) {
      console.log(`⚠️  Data not found: ${smartphoneDataPath}`);
      console.log(
        `\n💡 Generate data first: node scripts/generate-custom-patterns.js\n`,
      );
      return { success: false, error: "Data not found" };
    }

    const env = {
      ...process.env,
      DATA_PATH: dataPath,
      LOG_PATH: logDir,
    };

    return new Promise((resolve) => {
      const startTime = Date.now();

      // Start approach
      console.log(`Starting ${approach} approach...`);
      const approachProc = spawn("node", [this.getApproachScript(approach)], {
        env,
        stdio: "pipe",
      });

      // Capture logs
      const approachLogPath = path.join(logDir, `${approach}_orchestrator.log`);
      const approachLogStream = fs.createWriteStream(approachLogPath);
      approachProc.stdout.pipe(approachLogStream);
      approachProc.stderr.pipe(approachLogStream);

      // Start publisher after delay
      setTimeout(() => {
        console.log("Starting data publisher...");
        const publisherProc = spawn("node", ["dist/streamer/src/publish.js"], {
          env,
          stdio: "pipe",
        });

        const publisherLogPath = path.join(logDir, "publisher.log");
        const publisherLogStream = fs.createWriteStream(publisherLogPath);
        publisherProc.stdout.pipe(publisherLogStream);
        publisherProc.stderr.pipe(publisherLogStream);

        const timeout = setTimeout(() => {
          console.log("⏰ Test timeout reached");
          publisherProc.kill();
          approachProc.kill();
        }, this.timeout);

        publisherProc.on("close", (code) => {
          clearTimeout(timeout);
          const duration = Date.now() - startTime;

          // Wait for final processing
          setTimeout(() => {
            approachProc.kill();

            // Move log files
            this.moveLogFiles(approach, logDir);

            const result = {
              approach,
              pattern: patternName,
              patternDisplayName: pattern.name,
              patternParams: pattern.params,
              iteration: iterationNum,
              success: code === 0,
              exitCode: code,
              duration: duration,
              logDir: logDir,
            };

            console.log(`✓ Test completed in ${(duration / 1000).toFixed(1)}s`);
            resolve(result);
          }, 2000);
        });

        publisherProc.on("error", (err) => {
          clearTimeout(timeout);
          approachProc.kill();
          console.log(`✗ Publisher error: ${err.message}`);
          resolve({
            approach,
            pattern: patternName,
            patternDisplayName: pattern.name,
            iteration: iterationNum,
            success: false,
            error: err.message,
          });
        });
      }, 2000);
    });
  }

  moveLogFiles(approach, logDir) {
    const logFileMap = {
      fetching: [
        "fetching_client_side_log.csv",
        "fetching_latency_log.csv",
        "fetching_resource_usage.csv",
        "replayer-log.csv",
      ],
      approximation: [
        "approximation_approach_log.csv",
        "approximation_latency_log.csv",
        "approximation_approach_resource_usage.csv",
        "replayer-log.csv",
      ],
      chunked: [
        "streaming_query_chunk_aggregator_log.csv",
        "chunked_latency_log.csv",
        "streaming_query_hive_resource_log.csv",
        "replayer-log.csv",
      ],
      naive_distributed: [
        "naive_distributed_approach_log.csv",
        "naive_distributed_latency_log.csv",
        "naive_distributed_approach_resource_usage.csv",
        "replayer-log.csv",
      ],
    };

    const logFiles = logFileMap[approach] || [];

    logFiles.forEach((logFile) => {
      const srcPath = path.join(".", logFile);
      const destPath = path.join(logDir, logFile);

      if (fs.existsSync(srcPath)) {
        try {
          fs.copyFileSync(srcPath, destPath);
          fs.unlinkSync(srcPath);
          console.log(`  Moved ${logFile}`);
        } catch (err) {
          console.log(`  Failed to move ${logFile}: ${err.message}`);
        }
      }
    });
  }

  async extractResults(approach, pattern, iterationNum = 1) {
    const patternName = this.getPatternName(pattern);
    console.log(
      `\n📊 Extracting results for ${approach} - ${pattern.name} - Iteration ${iterationNum}...`,
    );

    return new Promise((resolve) => {
      const logDir = path.join(
        this.baseLogDir,
        approach,
        patternName,
        `iteration${iterationNum}`,
      );

      // Call extraction script with custom parameters
      const proc = spawn(
        "node",
        [
          "experiments/pattern-analysis/extract-pattern-results.js",
          approach,
          patternName,
          logDir,
        ],
        { stdio: "inherit" },
      );

      proc.on("close", (code) => {
        if (code === 0) {
          console.log(`✓ Extraction completed`);
          resolve(true);
        } else {
          console.log(`⚠️  Extraction had issues (code ${code})`);
          resolve(false);
        }
      });

      proc.on("error", (err) => {
        console.log(`✗ Extraction error: ${err.message}`);
        resolve(false);
      });
    });
  }

  async runAllPatterns() {
    console.log(`\n${"█".repeat(80)}`);
    console.log("CUSTOM PATTERN COMPARISON - ALL APPROACHES");
    console.log("█".repeat(80));
    console.log(`Total patterns: ${this.patterns.length}`);
    console.log(`Approaches: ${this.approaches.join(", ")}`);
    console.log(`Iterations per pattern-approach: ${this.iterations}`);
    console.log(
      `Total tests: ${this.patterns.length * this.approaches.length * this.iterations}`,
    );
    console.log("█".repeat(80));

    const results = [];

    for (const pattern of this.patterns) {
      console.log(`\n${"─".repeat(80)}`);
      console.log(`Pattern: ${pattern.name}`);
      console.log(`Parameters: ${pattern.params}`);
      console.log("─".repeat(80));

      for (const approach of this.approaches) {
        console.log(`\n  Approach: ${approach.toUpperCase()}`);

        for (let iter = 1; iter <= this.iterations; iter++) {
          try {
            const result = await this.runSingleTest(approach, pattern, iter);
            results.push(result);

            // Extract results
            await this.extractResults(approach, pattern, iter);

            // Wait between iterations
            await new Promise((resolve) => setTimeout(resolve, 2000));
          } catch (error) {
            console.error(
              `Failed ${approach} on ${pattern.name} iteration ${iter}:`,
              error.message,
            );
            results.push({
              approach,
              pattern: pattern.type,
              patternDisplayName: pattern.name,
              iteration: iter,
              success: false,
              error: error.message,
            });
          }
        }

        // Wait between approaches
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }

      // Wait between patterns
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    return results;
  }

  async runSpecificPattern(patternType) {
    const pattern = this.patterns.find((p) => p.type === patternType);

    if (!pattern) {
      throw new Error(
        `Unknown pattern type: ${patternType}. Available: ${this.patterns.map((p) => p.type).join(", ")}`,
      );
    }

    console.log("\n" + "█".repeat(80));
    console.log(`SINGLE PATTERN TEST: ${pattern.name}`);
    console.log(`Parameters: ${pattern.params}`);
    console.log("█".repeat(80));

    const results = [];

    for (const approach of this.approaches) {
      for (let iter = 1; iter <= this.iterations; iter++) {
        try {
          const result = await this.runSingleTest(approach, pattern, iter);
          results.push(result);
          await this.extractResults(approach, pattern, iter);
          await new Promise((resolve) => setTimeout(resolve, 2000));
        } catch (error) {
          console.error(
            `Failed ${approach} iteration ${iter}:`,
            error.message,
          );
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }

    return results;
  }

  generateSummary(results) {
    console.log("\n" + "█".repeat(80));
    console.log("FINAL SUMMARY");
    console.log("█".repeat(80));

    const successful = results.filter((r) => r.success);
    const failed = results.filter((r) => !r.success);

    console.log(`\nTotal tests: ${results.length}`);
    console.log(`Successful: ${successful.length}`);
    console.log(`Failed: ${failed.length}`);
    console.log(`Iterations per test: ${this.iterations}`);

    // Group by approach
    const byApproach = {};
    this.approaches.forEach((approach) => {
      byApproach[approach] = {
        total: results.filter((r) => r.approach === approach).length,
        success: results.filter((r) => r.approach === approach && r.success)
          .length,
      };
    });

    console.log("\n" + "─".repeat(80));
    console.log("Results by Approach:");
    Object.entries(byApproach).forEach(([approach, stats]) => {
      console.log(`  ${approach}: ${stats.success}/${stats.total} successful`);
    });

    // Group by pattern
    const byPattern = {};
    this.patterns.forEach((pattern) => {
      const patternResults = results.filter((r) => r.pattern === pattern.type);
      byPattern[pattern.name] = {
        total: patternResults.length,
        success: patternResults.filter((r) => r.success).length,
      };
    });

    console.log("\n" + "─".repeat(80));
    console.log("Results by Pattern:");
    Object.entries(byPattern).forEach(([name, stats]) => {
      console.log(`  ${name}: ${stats.success}/${stats.total} successful`);
    });

    if (failed.length > 0) {
      console.log("\n" + "─".repeat(80));
      console.log("Failed Tests:");
      failed.forEach((r) => {
        console.log(
          `  ✗ ${r.approach} - ${r.patternDisplayName || r.pattern} - iteration ${r.iteration || "?"}`,
        );
        if (r.error) console.log(`    Error: ${r.error}`);
      });
    }

    // Save summary
    const summaryPath = path.join(
      this.baseLogDir,
      "custom_pattern_comparison_summary.json",
    );
    const summary = {
      timestamp: new Date().toISOString(),
      totalTests: results.length,
      iterations: this.iterations,
      successful: successful.length,
      failed: failed.length,
      byApproach: byApproach,
      byPattern: byPattern,
      results: results,
    };

    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
    console.log(`\n📄 Summary saved to: ${summaryPath}`);
    console.log("█".repeat(80));
  }

  async runAnalysis() {
    console.log("\n" + "=".repeat(80));
    console.log("RUNNING COMPREHENSIVE ANALYSIS");
    console.log("=".repeat(80));

    // Note: You'll need to create a custom analysis script for these patterns
    // For now, just print instructions
    console.log("\n⚠️  Custom analysis script needed");
    console.log(
      "Create: analysis/accuracy/custom-pattern-accuracy-comparison.js",
    );
    console.log("This script should:");
    console.log("  1. Read all iteration data for each pattern-approach");
    console.log("  2. Compute mean ± std for MAPE, MAE, RMSE");
    console.log("  3. Compute mean ± std for latency and memory");
    console.log("  4. Generate aggregated CSV and JSON outputs");
    console.log("\nFor now, results are in individual iteration directories.");
    console.log("=".repeat(80));

    return true;
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);

  // Check for iterations flag
  let iterations = 35; // default
  let filteredArgs = args;

  const iterFlag = args.findIndex(
    (arg) => arg === "--iterations" || arg === "-i",
  );
  if (iterFlag !== -1 && args[iterFlag + 1]) {
    iterations = parseInt(args[iterFlag + 1], 10);
    filteredArgs = args.filter(
      (_, idx) => idx !== iterFlag && idx !== iterFlag + 1,
    );
  }

  const runner = new CustomPatternComparisonRunner(iterations);

  console.log(`\n${"=".repeat(80)}`);
  console.log(`Configuration: Running ${iterations} iteration(s) per test`);
  console.log(`Total patterns: 5 custom patterns`);
  console.log(
    `Expected total tests: ${5 * 4 * iterations} (5 patterns × 4 approaches × ${iterations} iterations)`,
  );
  console.log(`Expected runtime: ~${Math.ceil((5 * 4 * iterations * 3) / 60)} minutes`);
  console.log("=".repeat(80));

  try {
    if (filteredArgs.length === 0) {
      // Run all patterns
      const results = await runner.runAllPatterns();
      runner.generateSummary(results);
      await runner.runAnalysis();
    } else if (filteredArgs.length === 1) {
      // Run specific pattern
      const patternType = filteredArgs[0];
      const results = await runner.runSpecificPattern(patternType);
      runner.generateSummary(results);
    } else {
      console.log("Usage:");
      console.log(
        "  node run-custom-patterns-comparison.js [--iterations N]           # Run all patterns",
      );
      console.log(
        "  node run-custom-patterns-comparison.js low_variability [-i N]     # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js step_pattern [-i N]        # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js spike_pattern [-i N]       # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js low_freq_oscillation [-i N]  # Run specific pattern",
      );
      console.log(
        "  node run-custom-patterns-comparison.js high_freq_oscillation [-i N] # Run specific pattern",
      );
      console.log("\nOptions:");
      console.log(
        "  --iterations, -i N    Number of iterations per test (default: 35)",
      );
      process.exit(1);
    }

    console.log("\n✓ All experiments completed!");
  } catch (error) {
    console.error("\n✗ Experiment failed:", error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = CustomPatternComparisonRunner;
