#!/usr/bin/env node

/**
 * Simplified Single-Run Test for Real Data Comparison
 * Runs ONE approach at a time with proper process management and completion detection
 * Usage: node run-single-test.js [fetching|approximation|chunked]
 */

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");

// Get project root directory
const PROJECT_ROOT = path.resolve(__dirname, "../..");

const APPROACHES = {
  fetching: {
    name: "fetching",
    label: "Fetching Client Side",
    orchestrator: path.join(
      PROJECT_ROOT,
      "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
    ),
    logFiles: {
      main: "fetching_client_side_log.csv",
      resource: "fetching_client_side_resource_usage.csv",
      replayer: "replayer-log.csv",
    },
  },
  approximation: {
    name: "approximation",
    label: "Approximation",
    orchestrator: path.join(
      PROJECT_ROOT,
      "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
    ),
    logFiles: {
      main: "approximation_approach_log.csv",
      resource: "approximation_approach_resource_usage.csv",
      replayer: "replayer-log.csv",
    },
  },
  chunked: {
    name: "chunked",
    label: "Chunked",
    orchestrator: path.join(
      PROJECT_ROOT,
      "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
    ),
    logFiles: {
      main: "streaming_query_chunk_aggregator_log.csv",
      resource: "streaming_query_hive_resource_log.csv",
      replayer: "replayer-log.csv",
    },
  },
};

const TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes - enough for data streaming
const LOGS_DIR = path.join(PROJECT_ROOT, "logs/real_data_single_test");
const CHECK_INTERVAL = 2000; // Check every 2 seconds if publisher is done

class SingleTestRunner {
  constructor(approachName) {
    this.approach = APPROACHES[approachName];
    if (!this.approach) {
      throw new Error(
        `Unknown approach: ${approachName}. Use: fetching, approximation, or chunked`,
      );
    }

    this.logDir = path.join(LOGS_DIR, this.approach.name);
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }

    this.orchestratorProcess = null;
    this.publisherProcess = null;
    this.checkTimer = null;
    this.timeoutTimer = null;
  }

  cleanup() {
    if (this.checkTimer) clearInterval(this.checkTimer);
    if (this.timeoutTimer) clearTimeout(this.timeoutTimer);

    if (this.orchestratorProcess) {
      console.log("Killing orchestrator process...");
      this.orchestratorProcess.kill("SIGTERM");
    }
    if (this.publisherProcess) {
      console.log("Killing publisher process...");
      this.publisherProcess.kill("SIGTERM");
    }
  }

  copyLogFiles() {
    console.log("\nCopying log files...");
    for (const logFile of Object.values(this.approach.logFiles)) {
      const srcPath = path.join(PROJECT_ROOT, logFile);
      const destPath = path.join(this.logDir, logFile);

      if (fs.existsSync(srcPath)) {
        fs.copyFileSync(srcPath, destPath);
        fs.unlinkSync(srcPath);
        console.log(`  ✓ Copied ${logFile}`);
      } else {
        console.log(`  ⚠ Missing ${logFile}`);
      }
    }
  }

  checkPublisherCompletion() {
    const replayerLog = path.join(
      PROJECT_ROOT,
      this.approach.logFiles.replayer,
    );

    if (!fs.existsSync(replayerLog)) {
      return false;
    }

    try {
      const content = fs.readFileSync(replayerLog, "utf8");
      const lines = content.trim().split("\n");

      // Check if we have the completion summary line
      // Format: timestamp,intended,successful,failed
      if (lines.length >= 2) {
        const lastLine = lines[lines.length - 1];
        // If last line has comma-separated numbers, it's the summary
        if (lastLine.match(/^\d+,\d+,\d+,\d+$/)) {
          const parts = lastLine.split(",");
          console.log(
            `\nPublisher Summary: Intended=${parts[1]}, Successful=${parts[2]}, Failed=${parts[3]}`,
          );
          return true;
        }
      }

      // Also check for "All observations published" message
      if (content.includes("All observations published")) {
        console.log("\nPublisher completed (found completion message)");
        return true;
      }
    } catch (err) {
      // File might be locked, try again next interval
      return false;
    }

    return false;
  }

  async run() {
    const startTime = Date.now();

    console.log("=".repeat(80));
    console.log(`SINGLE TEST: ${this.approach.label}`);
    console.log("=".repeat(80));
    console.log(`Data: smartphone.acceleration.x & wearable.acceleration.x`);
    console.log(`Logs: ${this.logDir}`);
    console.log(`Timeout: ${TIMEOUT_MS / 1000}s\n`);

    return new Promise((resolve, reject) => {
      const env = {
        ...process.env,
        LOG_PATH: this.logDir,
      };

      // Start orchestrator
      console.log(`Starting ${this.approach.label} orchestrator...`);
      this.orchestratorProcess = spawn("node", [this.approach.orchestrator], {
        stdio: ["inherit", "pipe", "pipe"],
        env: env,
        cwd: PROJECT_ROOT,
      });

      let orchestratorOutput = "";
      this.orchestratorProcess.stdout.on("data", (data) => {
        const text = data.toString();
        orchestratorOutput += text;
        process.stdout.write(text);
      });

      this.orchestratorProcess.stderr.on("data", (data) => {
        process.stderr.write(data);
      });

      this.orchestratorProcess.on("error", (err) => {
        console.error("Orchestrator error:", err);
        this.cleanup();
        reject(err);
      });

      // Start publisher after delay
      setTimeout(() => {
        console.log("\nStarting data publisher...");
        console.log("This will stream data at 4Hz. Please wait...\n");

        this.publisherProcess = spawn(
          "node",
          [path.join(PROJECT_ROOT, "dist/streamer/src/publish.js")],
          {
            stdio: ["inherit", "pipe", "pipe"],
            env: env,
            cwd: PROJECT_ROOT,
          },
        );

        let publisherOutput = "";
        this.publisherProcess.stdout.on("data", (data) => {
          const text = data.toString();
          publisherOutput += text;
          // Show progress periodically
          if (text.includes("Published observation:") && Math.random() < 0.05) {
            process.stdout.write(".");
          }
        });

        this.publisherProcess.stderr.on("data", (data) => {
          const text = data.toString();
          // Only show errors, not normal output
          if (text.toLowerCase().includes("error")) {
            process.stderr.write(text);
          }
        });

        this.publisherProcess.on("close", (code) => {
          const endTime = Date.now();
          const duration = (endTime - startTime) / 1000;

          console.log(`\n\nPublisher exited with code ${code}`);
          console.log(`Total duration: ${duration.toFixed(1)}s`);

          // Give orchestrator a moment to process final data
          setTimeout(() => {
            this.cleanup();
            this.copyLogFiles();
            this.analyzeResults(duration);
            resolve({ success: code === 0, duration });
          }, 3000);
        });

        this.publisherProcess.on("error", (err) => {
          console.error("Publisher error:", err);
          this.cleanup();
          reject(err);
        });

        // Set up periodic check for completion
        this.checkTimer = setInterval(() => {
          if (this.checkPublisherCompletion()) {
            console.log(
              "\nDetected publisher completion, waiting for final processing...",
            );
            clearInterval(this.checkTimer);

            // Wait a bit for final windows to close, then kill processes
            setTimeout(() => {
              const endTime = Date.now();
              const duration = (endTime - startTime) / 1000;

              console.log(
                `\nStopping processes after ${duration.toFixed(1)}s...`,
              );
              this.cleanup();
              this.copyLogFiles();
              this.analyzeResults(duration);
              resolve({ success: true, duration });
            }, 5000); // 5 seconds for final processing
          }
        }, CHECK_INTERVAL);

        // Set up timeout
        this.timeoutTimer = setTimeout(() => {
          console.log("\n⏰ Timeout reached!");
          const endTime = Date.now();
          const duration = (endTime - startTime) / 1000;

          this.cleanup();
          this.copyLogFiles();
          this.analyzeResults(duration);
          resolve({ success: false, duration, timeout: true });
        }, TIMEOUT_MS);
      }, 2000);
    });
  }

  analyzeResults(duration) {
    console.log("\n" + "=".repeat(80));
    console.log("RESULTS ANALYSIS");
    console.log("=".repeat(80));

    const mainLogPath = path.join(this.logDir, this.approach.logFiles.main);
    const replayerLogPath = path.join(
      this.logDir,
      this.approach.logFiles.replayer,
    );

    // Analyze replayer log
    if (fs.existsSync(replayerLogPath)) {
      try {
        const content = fs.readFileSync(replayerLogPath, "utf8");
        const lines = content.trim().split("\n");

        console.log("\nPublisher Statistics:");
        if (lines.length >= 2) {
          const lastLine = lines[lines.length - 1];
          if (lastLine.match(/^\d+,\d+,\d+,\d+$/)) {
            const [timestamp, intended, successful, failed] =
              lastLine.split(",");
            console.log(`  Intended observations: ${intended}`);
            console.log(`  Successful publishes: ${successful}`);
            console.log(`  Failed publishes: ${failed}`);
            console.log(
              `  Success rate: ${((successful / intended) * 100).toFixed(1)}%`,
            );
          }
        }
      } catch (err) {
        console.log(`  ⚠ Could not parse replayer log: ${err.message}`);
      }
    }

    // Analyze main log
    if (fs.existsSync(mainLogPath)) {
      try {
        const content = fs.readFileSync(mainLogPath, "utf8");
        const lines = content.trim().split("\n");

        console.log(`\nApproach Log Statistics:`);
        console.log(`  Total log entries: ${lines.length - 1}`); // -1 for header

        // Count window close events
        const windowCloses = content.match(/Window closed/gi) || [];
        console.log(`  Window close events: ${windowCloses.length}`);

        // Look for results
        const results =
          content.match(/avgValue|avgWearableX|avgSmartphoneX/gi) || [];
        console.log(`  Result events: ${results.length}`);
      } catch (err) {
        console.log(`  ⚠ Could not parse main log: ${err.message}`);
      }
    }

    console.log(
      `\nTest Duration: ${duration.toFixed(1)}s (${(duration / 60).toFixed(1)} minutes)`,
    );
    console.log(`Log files saved to: ${this.logDir}`);
    console.log("\n" + "=".repeat(80));
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0) {
    console.log("Usage: node run-single-test.js [approach]");
    console.log("\nAvailable approaches:");
    console.log("  fetching      - Fetching Client Side (baseline)");
    console.log("  approximation - Approximation approach");
    console.log("  chunked       - Chunked approach");
    console.log("\nExample:");
    console.log("  node run-single-test.js fetching");
    process.exit(1);
  }

  const approachName = args[0].toLowerCase();

  console.log("\n🚀 Starting Single Test Runner");
  console.log("Prerequisites:");
  console.log("  ✓ MQTT broker running (mosquitto)");
  console.log("  ✓ Project built (npm run build)");
  console.log("  ✓ Data files exist in src/streamer/data/\n");

  try {
    const runner = new SingleTestRunner(approachName);
    const result = await runner.run();

    console.log("\n🎉 Test completed!");
    if (result.timeout) {
      console.log("⚠️  Test ended due to timeout");
    }
    console.log(`Success: ${result.success}`);
    console.log(`Duration: ${result.duration.toFixed(1)}s\n`);

    process.exit(result.success ? 0 : 1);
  } catch (error) {
    console.error("\n💥 Test failed:", error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

if (require.main === module) {
  // Handle Ctrl+C gracefully
  process.on("SIGINT", () => {
    console.log("\n\nReceived SIGINT, cleaning up...");
    process.exit(130);
  });

  main();
}

module.exports = SingleTestRunner;
