#!/usr/bin/env ts-node

/**
 * Test script to verify all approaches can be imported and instantiated
 */

import path from "path";

async function testAllApproaches() {
  console.log("Testing all approach orchestrators...\n");
  console.log("=".repeat(70));

  const approaches = [
    {
      name: "Fetching Client Side",
      path: "../src/approaches/FetchingClientSideApproachOrchestrator",
    },
    {
      name: "Chunked Query Approach",
      path: "../src/approaches/ChunkedQueryApproachOrchestrator",
    },
    {
      name: "Approximation Approach",
      path: "../src/approaches/ApproximationApproachOrchestrator",
    },
  ];

  let allPassed = true;

  for (const approach of approaches) {
    try {
      console.log(`\nTesting: ${approach.name}`);
      console.log("-".repeat(70));

      // Try to import the module
      const Module = await import(approach.path);
      console.log(`  [OK] Module imported successfully`);

      // Check for default export
      if (!Module.default) {
        console.log(`  [FAIL] No default export found`);
        allPassed = false;
        continue;
      }
      console.log(`  [OK] Default export exists`);

      // Try to instantiate
      const instance = new Module.default();
      console.log(`  [OK] Instance created successfully`);

      // Check for required methods
      if (typeof instance.getName !== "function") {
        console.log(`  [FAIL] Missing getName() method`);
        allPassed = false;
        continue;
      }
      console.log(`  [OK] getName() method exists`);

      if (typeof instance.runExperiment !== "function") {
        console.log(`  [FAIL] Missing runExperiment() method`);
        allPassed = false;
        continue;
      }
      console.log(`  [OK] runExperiment() method exists`);

      // Test getName
      const name = instance.getName();
      console.log(`  [OK] getName() returns: "${name}"`);

      // Check cleanup method
      if (typeof instance.cleanup !== "function") {
        console.log(`  [WARN] Missing cleanup() method (optional)`);
      } else {
        console.log(`  [OK] cleanup() method exists`);
      }

      console.log(`  [SUCCESS] ${approach.name} passed all tests`);
    } catch (error) {
      console.log(`  [FAIL] Error testing ${approach.name}:`, error);
      allPassed = false;
    }
  }

  console.log("\n" + "=".repeat(70));
  if (allPassed) {
    console.log("[SUCCESS] All approaches passed tests!");
    console.log("\nAll orchestrators are ready to run in experiments.");
    process.exit(0);
  } else {
    console.log("[FAIL] Some approaches failed tests");
    process.exit(1);
  }
}

testAllApproaches().catch((error) => {
  console.error("Test script failed:", error);
  process.exit(1);
});
