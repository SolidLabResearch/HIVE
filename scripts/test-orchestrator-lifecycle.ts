#!/usr/bin/env ts-node
/**
 * Test Orchestrator Lifecycle
 *
 * This script tests that orchestrators stay alive long enough to process results.
 * It runs a single orchestrator and verifies it doesn't exit prematurely.
 */

import { spawn } from "child_process";
import * as path from "path";

const TEST_DURATION = 30000; // 30 seconds - enough to see if it stays alive
const ORCHESTRATOR_PATH = "src/approaches/ApproximationApproachOrchestrator.ts";

console.log("=".repeat(70));
console.log("ORCHESTRATOR LIFECYCLE TEST");
console.log("=".repeat(70));
console.log(`Testing: ${ORCHESTRATOR_PATH}`);
console.log(`Duration: ${TEST_DURATION / 1000}s`);
console.log("");

const projectRoot = path.resolve(__dirname, "..");
const fullPath = path.resolve(projectRoot, ORCHESTRATOR_PATH);

console.log(`[INFO] Starting orchestrator at: ${fullPath}`);

const proc = spawn("npx", ["ts-node", fullPath], {
  env: { ...process.env, HTTP_PORT: "8081", HEALTH_PORT: "9091" },
  stdio: ["pipe", "pipe", "pipe"],
});

console.log(`[INFO] Process spawned (PID: ${proc.pid})`);

let hasExited = false;
let exitCode: number | null = null;

proc.stdout?.on("data", (data: Buffer) => {
  const lines = data
    .toString()
    .split("\n")
    .filter((l: string) => l.trim());
  for (const line of lines) {
    console.log(`[STDOUT] ${line}`);
  }
});

proc.stderr?.on("data", (data: Buffer) => {
  const lines = data
    .toString()
    .split("\n")
    .filter((l: string) => l.trim());
  for (const line of lines) {
    console.log(`[STDERR] ${line}`);
  }
});

proc.on("exit", (code) => {
  hasExited = true;
  exitCode = code;
  console.log(`[EXIT] Orchestrator exited with code ${code}`);
});

// Check status every 5 seconds
const checkInterval = setInterval(() => {
  if (hasExited) {
    console.log(
      `[ERROR] Orchestrator exited prematurely after ${(Date.now() - startTime) / 1000}s`,
    );
    clearInterval(checkInterval);
    clearTimeout(testTimeout);
    process.exit(1);
  } else {
    console.log(
      `[STATUS] Orchestrator still running... (${Math.floor((Date.now() - startTime) / 1000)}s elapsed)`,
    );
  }
}, 5000);

const startTime = Date.now();

// End test after duration
const testTimeout = setTimeout(() => {
  clearInterval(checkInterval);

  if (hasExited) {
    console.log("");
    console.log("=".repeat(70));
    console.log(`[FAIL] Orchestrator exited early (code: ${exitCode})`);
    console.log(
      "The orchestrator should stay alive until explicitly terminated.",
    );
    console.log("=".repeat(70));
    process.exit(1);
  } else {
    console.log("");
    console.log("=".repeat(70));
    console.log(
      `[PASS] Orchestrator stayed alive for ${TEST_DURATION / 1000}s`,
    );
    console.log("Terminating orchestrator...");
    console.log("=".repeat(70));
    proc.kill("SIGTERM");

    setTimeout(() => {
      if (!hasExited) {
        console.log("[WARN] Process didn't respond to SIGTERM, using SIGKILL");
        proc.kill("SIGKILL");
      }
      process.exit(0);
    }, 2000);
  }
}, TEST_DURATION);

// Handle cleanup on script termination
process.on("SIGINT", () => {
  console.log("\n[INFO] Test interrupted, cleaning up...");
  clearInterval(checkInterval);
  clearTimeout(testTimeout);
  proc.kill("SIGKILL");
  process.exit(130);
});

process.on("SIGTERM", () => {
  console.log("\n[INFO] Test terminated, cleaning up...");
  clearInterval(checkInterval);
  clearTimeout(testTimeout);
  proc.kill("SIGKILL");
  process.exit(143);
});
