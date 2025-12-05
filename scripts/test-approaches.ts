#!/usr/bin/env ts-node

/**
 * Test script to verify all approach orchestrators work as standalone executables
 * after the refactoring
 */

import { spawn, ChildProcess } from "child_process";
import * as path from "path";

interface TestResult {
  name: string;
  status: "PASS" | "FAIL";
  message: string;
}

const results: TestResult[] = [];

console.log("=".repeat(70));
console.log("Testing Approach Orchestrators as Standalone Executables");
console.log("=".repeat(70));
console.log("");
console.log(
  "These approaches run as standalone scripts, not importable modules.",
);
console.log("Testing that they can start and initialize correctly...");
console.log("");

async function testApproach(
  name: string,
  scriptPath: string,
  args: string[] = [],
  timeoutMs: number = 5000,
): Promise<TestResult> {
  return new Promise((resolve) => {
    console.log(`Testing: ${name}...`);

    const child = spawn("npx", ["ts-node", scriptPath, ...args], {
      cwd: path.resolve(__dirname, ".."),
      stdio: "pipe",
    });

    let output = "";
    let errorOutput = "";
    let hasError = false;

    child.stdout?.on("data", (data) => {
      output += data.toString();
    });

    child.stderr?.on("data", (data) => {
      errorOutput += data.toString();
      // Check for actual errors vs debug output
      if (
        data.toString().includes("Error:") ||
        data.toString().includes("error TS")
      ) {
        hasError = true;
      }
    });

    // Kill after timeout (success means it started)
    const timer = setTimeout(() => {
      child.kill("SIGTERM");

      if (!hasError && output.length > 0) {
        console.log(
          `  [OK] Started successfully (killed after ${timeoutMs}ms)`,
        );
        console.log(`  Output preview: ${output.substring(0, 100).trim()}...`);
        resolve({
          name,
          status: "PASS",
          message: "Started successfully",
        });
      } else if (hasError) {
        console.log(`  [FAIL] Error during execution`);
        console.log(`  Error: ${errorOutput.substring(0, 200)}`);
        resolve({
          name,
          status: "FAIL",
          message: `Error: ${errorOutput.substring(0, 100)}`,
        });
      } else {
        console.log(`  [FAIL] No output produced`);
        resolve({
          name,
          status: "FAIL",
          message: "No output produced",
        });
      }
    }, timeoutMs);

    child.on("exit", (code) => {
      clearTimeout(timer);

      if (
        code !== null &&
        code !== 0 &&
        !output.includes("Started") &&
        !output.includes("Approach")
      ) {
        console.log(`  [FAIL] Exited with code ${code}`);
        console.log(`  Error: ${errorOutput.substring(0, 200)}`);
        resolve({
          name,
          status: "FAIL",
          message: `Exited with code ${code}`,
        });
      }
    });

    child.on("error", (error) => {
      clearTimeout(timer);
      console.log(`  [FAIL] Failed to start: ${error.message}`);
      resolve({
        name,
        status: "FAIL",
        message: error.message,
      });
    });
  });
}

async function testIndependentStreamProcessing(): Promise<TestResult> {
  console.log("Testing: IndependentStreamProcessingApproach (as module)...");

  try {
    const { IndependentStreamProcessingApproach } =
      await import("../src/approaches/IndependentStreamProcessingApproach");
    const approach = new IndependentStreamProcessingApproach();

    console.log("  [OK] Module imports and instantiates correctly");
    console.log("  Type:", typeof approach);

    return {
      name: "IndependentStreamProcessingApproach",
      status: "PASS",
      message: "Module-based approach works correctly",
    };
  } catch (error) {
    console.log(`  [FAIL] ${(error as Error).message}`);
    return {
      name: "IndependentStreamProcessingApproach",
      status: "FAIL",
      message: (error as Error).message,
    };
  }
}

async function runTests() {
  // Test 1: Independent Stream Processing (module-based)
  const independentResult = await testIndependentStreamProcessing();
  results.push(independentResult);
  console.log("");

  // Test 2: Chunked Approach (script-based)
  const chunkedResult = await testApproach(
    "StreamingQueryChunkedApproach",
    "src/approaches/StreamingQueryChunkedApproachOrchestrator.ts",
    [],
    5000,
  );
  results.push(chunkedResult);
  console.log("");

  // Note: The legacy experiments in scripts/legacy/ contain the actual
  // runnable versions of approximation and fetching approaches
  console.log(
    "Note: Approximation and Fetching approaches are in scripts/legacy/",
  );
  console.log("      These were moved there during reorganization as they are");
  console.log("      older standalone experiment scripts.");
  console.log("");

  // Summary
  console.log("=".repeat(70));
  console.log("Test Summary");
  console.log("=".repeat(70));

  const passed = results.filter((r) => r.status === "PASS").length;
  const failed = results.filter((r) => r.status === "FAIL").length;

  results.forEach((result) => {
    const status = result.status === "PASS" ? "[OK]  " : "[FAIL]";
    console.log(`${status} ${result.name}`);
    if (result.status === "FAIL") {
      console.log(`       ${result.message}`);
    }
  });

  console.log("");
  console.log(`Total:  ${results.length}`);
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  console.log("");

  if (failed === 0) {
    console.log("[OK] All tested approaches work correctly!");
    console.log("");
    console.log("Working Approaches:");
    console.log(
      "  1. IndependentStreamProcessing - Module-based (tested in experiments)",
    );
    console.log("  2. StreamingQueryChunked - Script-based orchestrator");
    console.log("");
    console.log("Legacy Approaches (in scripts/legacy/):");
    console.log("  - Approximation approach experiment scripts");
    console.log("  - Fetching client-side experiment scripts");
    console.log("");
    process.exit(0);
  } else {
    console.log("[FAIL] Some approaches failed");
    process.exit(1);
  }
}

runTests().catch((error) => {
  console.error("Test execution failed:", error);
  process.exit(1);
});
