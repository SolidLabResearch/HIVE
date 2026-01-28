#!/usr/bin/env node

/**
 * Comprehensive Pattern Analysis - All Approaches
 *
 * Tests all three approaches (Fetching, Approximation, Chunked) across:
 * - Exponential growth/decay patterns (various rates)
 * - Noisy datasets (various noise levels)
 *
 * Measures:
 * - Accuracy (MAPE, MAE, RMSE)
 * - First-event latency
 * - Resource usage (CPU, memory)
 *
 * Usage:
 *   node run-all-patterns-comparison.js                    # Run all patterns
 *   node run-all-patterns-comparison.js exponential 1      # Run specific pattern
 *   node run-all-patterns-comparison.js noisy 0.5          # Run specific noise level
 */

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");

class PatternComparisonRunner {
  constructor(iterations = 35) {
    this.iterations = iterations;
    this.approaches = ["fetching", "approximation", "chunked"];

    // Exponential patterns
    this.exponentialPatterns = [
      { type: "exponential_growth", rate: 0.001 },
      { type: "exponential_growth", rate: 0.01 },
      { type: "exponential_growth", rate: 0.1 },
      { type: "exponential_growth", rate: 1 },
      { type: "exponential_growth", rate: 10 },
      { type: "exponential_growth", rate: 100 },
      { type: "exponential_decay", rate: 0.001 },
      { type: "exponential_decay", rate: 0.01 },
      { type: "exponential_decay", rate: 0.1 },
      { type: "exponential_decay", rate: 1 },
      { type: "exponential_decay", rate: 10 },
      { type: "exponential_decay", rate: 100 },
    ];

    // Noisy patterns
    this.noisyPatterns = [
      { type: "noise", level: 0.1 },
      { type: "noise", level: 0.2 },
      { type: "noise", level: 0.5 },
      { type: "noise", level: 1.0 },
      { type: "noise", level: 2.0 },
    ];

    this.baseLogDir = "./logs/pattern-comparison";
    this.timeout = 360000; // 6 minutes per test
  }

  getDataPath(pattern) {
    if (pattern.type === "noise") {
      return `noisy_datasets/noise_${pattern.level}`;
    } else {
      return `rate_comparison/${pattern.type}_rate_${pattern.rate}`;
    }
  }

  getPatternName(pattern) {
    if (pattern.type === "noise") {
      return `noise_${pattern.level}`;
    } else {
      return `${pattern.type}_rate_${pattern.rate}`;
    }
  }

  getApproachScript(approach) {
    const scripts = {
      fetching:
        "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
      approximation:
        "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
      chunked: "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
    };
    return scripts[approach];
  }

  async runSingleTest(approach, pattern, iterationNum = 1) {
    const patternName = this.getPatternName(pattern);
    const dataPath = this.getDataPath(pattern);

    console.log(`\n${"=".repeat(80)}`);
    console.log(
      `TESTING: ${approach.toUpperCase()} - ${patternName} - Iteration ${iterationNum}/${this.iterations}`,
    );
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
              patternType: pattern.type,
              patternValue: pattern.rate || pattern.level,
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
      `\n📊 Extracting results for ${approach} - ${patternName} - Iteration ${iterationNum}...`,
    );

    // Create a custom extraction script call
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

  async runPatternSet(patterns, patternType) {
    console.log(`\n${"█".repeat(80)}`);
    console.log(`PATTERN SET: ${patternType.toUpperCase()}`);
    console.log(`Total patterns: ${patterns.length}`);
    console.log(`Iterations per pattern-approach: ${this.iterations}`);
    console.log(
      `Total tests: ${patterns.length * this.approaches.length * this.iterations}`,
    );
    console.log("█".repeat(80));

    const results = [];

    for (const pattern of patterns) {
      console.log(`\n${"─".repeat(80)}`);
      console.log(`Pattern: ${this.getPatternName(pattern)}`);
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
              `Failed ${approach} on ${this.getPatternName(pattern)} iteration ${iter}:`,
              error.message,
            );
            results.push({
              approach,
              pattern: this.getPatternName(pattern),
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

  async runAll() {
    console.log("\n" + "█".repeat(80));
    console.log("COMPREHENSIVE PATTERN COMPARISON - ALL APPROACHES");
    console.log("█".repeat(80));
    console.log(`Exponential patterns: ${this.exponentialPatterns.length}`);
    console.log(`Noisy patterns: ${this.noisyPatterns.length}`);
    console.log(`Approaches: ${this.approaches.join(", ")}`);
    console.log(`Iterations per test: ${this.iterations}`);
    console.log(
      `Total tests: ${(this.exponentialPatterns.length + this.noisyPatterns.length) * this.approaches.length * this.iterations}`,
    );
    console.log("█".repeat(80));

    const allResults = [];

    // Run exponential patterns
    const exponentialResults = await this.runPatternSet(
      this.exponentialPatterns,
      "Exponential",
    );
    allResults.push(...exponentialResults);

    // Run noisy patterns
    const noisyResults = await this.runPatternSet(this.noisyPatterns, "Noisy");
    allResults.push(...noisyResults);

    // Generate summary
    this.generateSummary(allResults);

    return allResults;
  }

  async runSpecificPattern(patternType, value) {
    let pattern;

    if (
      patternType === "exponential_growth" ||
      patternType === "exponential_decay"
    ) {
      pattern = { type: patternType, rate: parseFloat(value) };
    } else if (patternType === "noise") {
      pattern = { type: "noise", level: parseFloat(value) };
    } else {
      throw new Error(`Unknown pattern type: ${patternType}`);
    }

    console.log("\n" + "█".repeat(80));
    console.log(`SINGLE PATTERN TEST: ${this.getPatternName(pattern)}`);
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
          console.error(`Failed ${approach} iteration ${iter}:`, error.message);
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }

    // Run analysis
    await this.runAnalysis();

    return results;
  }

  async runAnalysis() {
    console.log("\n" + "=".repeat(80));
    console.log("RUNNING COMPREHENSIVE ANALYSIS");
    console.log("=".repeat(80));

    return new Promise((resolve) => {
      const proc = spawn(
        "node",
        ["analysis/accuracy/pattern-accuracy-comparison.js"],
        { stdio: "inherit" },
      );

      proc.on("close", (code) => {
        if (code === 0) {
          console.log("✓ Analysis completed");
        } else {
          console.log("⚠️  Analysis completed with warnings");
        }
        resolve(code === 0);
      });
    });
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

    // Group by pattern type
    const exponential = results.filter(
      (r) => r.patternType && r.patternType.includes("exponential"),
    );
    const noisy = results.filter((r) => r.patternType === "noise");

    console.log("\n" + "─".repeat(80));
    console.log("Results by Pattern Type:");
    console.log(
      `  Exponential: ${exponential.filter((r) => r.success).length}/${exponential.length} successful`,
    );
    console.log(
      `  Noisy: ${noisy.filter((r) => r.success).length}/${noisy.length} successful`,
    );

    if (failed.length > 0) {
      console.log("\n" + "─".repeat(80));
      console.log("Failed Tests:");
      failed.forEach((r) => {
        console.log(`  ✗ ${r.approach} - ${r.pattern}`);
        if (r.error) console.log(`    Error: ${r.error}`);
      });
    }

    // Save summary
    const summaryPath = path.join(
      this.baseLogDir,
      "pattern_comparison_summary.json",
    );
    const summary = {
      timestamp: new Date().toISOString(),
      totalTests: results.length,
      successful: successful.length,
      failed: failed.length,
      byApproach: byApproach,
      results: results,
    };

    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
    console.log(`\n📄 Summary saved to: ${summaryPath}`);
    console.log("█".repeat(80));
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

  const runner = new PatternComparisonRunner(iterations);

  console.log(`\nConfiguration: Running ${iterations} iteration(s) per test\n`);

  try {
    if (filteredArgs.length === 0) {
      // Run all patterns
      await runner.runAll();
      await runner.runAnalysis();
    } else if (filteredArgs.length === 2) {
      // Run specific pattern
      const [patternType, value] = filteredArgs;
      await runner.runSpecificPattern(patternType, value);
    } else {
      console.log("Usage:");
      console.log(
        "  node run-all-patterns-comparison.js [--iterations N]                          # Run all patterns",
      );
      console.log(
        "  node run-all-patterns-comparison.js exponential_growth 1 [--iterations N]     # Run specific pattern",
      );
      console.log(
        "  node run-all-patterns-comparison.js exponential_decay 100 [--iterations N]    # Run specific pattern",
      );
      console.log(
        "  node run-all-patterns-comparison.js noise 0.5 [--iterations N]                # Run specific noise level",
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
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = PatternComparisonRunner;
