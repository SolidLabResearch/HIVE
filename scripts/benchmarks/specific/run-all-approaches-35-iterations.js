#!/usr/bin/env node

/**
 * Simple orchestration script to run all streaming approaches sequentially
 * Each approach will run 1 iteration with 4Hz data for testing
 */

const { spawn } = require("child_process");
const path = require("path");

console.log(" Starting Sequential Approach Execution with 1 iteration each\n");
console.log("This will run 3 approaches sequentially:");
console.log("1. Independent Stream Processing Approach");
console.log("2. Approximation Approach");
console.log("3. Chunked Query Approach\n");

const approaches = [
  {
    name: "Independent Stream Processing",
    command: "npx",
    args: [
      "ts-node",
      "scripts/benchmarks/experiment-evaluation-independent-stream-processing.ts",
      "--frequency",
      "4Hz",
      "--iterations",
      "1",
    ],
  },
  {
    name: "Approximation Approach",
    command: "node",
    args: ["scripts/legacy/experiment-evaluation-approximation-approach.js"],
  },
  {
    name: "Chunked Query Approach",
    command: "node",
    args: ["scripts/legacy/experiment-evaluation-streaming-query-hive.js"],
  },
];

async function runApproach(approach) {
  return new Promise((resolve, reject) => {
    console.log(`\n Starting ${approach.name} Approach...`);
    console.log(`Command: ${approach.command} ${approach.args.join(" ")}\n`);

    const childProcess = spawn(approach.command, approach.args, {
      cwd: path.resolve(__dirname, "../.."),
      stdio: "inherit",
      env: { ...process.env },
    });

    childProcess.on("close", (code) => {
      if (code === 0) {
        console.log(`\n[OK] ${approach.name} completed successfully!\n`);
        resolve();
      } else {
        console.log(`\n[FAIL] ${approach.name} failed with code ${code}\n`);
        reject(new Error(`${approach.name} failed with exit code ${code}`));
      }
    });

    childProcess.on("error", (error) => {
      console.error(`\n[FAIL] Error starting ${approach.name}:`, error);
      reject(error);
    });
  });
}

async function runAllApproaches() {
  const startTime = Date.now();

  try {
    for (let i = 0; i < approaches.length; i++) {
      const approach = approaches[i];
      console.log(
        `\n Running approach ${i + 1}/${approaches.length}: ${approach.name}`,
      );
      await runApproach(approach);
    }

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
    console.log(`\n All approaches completed successfully!`);
    console.log(`Total execution time: ${totalTime} minutes`);
    console.log("\n Check the logs/ directory for all generated log files");
  } catch (error) {
    console.error("\n Execution stopped due to error:", error.message);
    process.exit(1);
  }
}

// Start execution
runAllApproaches().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
