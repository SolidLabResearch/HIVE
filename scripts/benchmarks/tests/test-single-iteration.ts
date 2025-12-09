import * as fs from "fs";
import * as path from "path";
import { FetchingClientSideApproachOrchestrator } from "../../src/approaches/FetchingClientSideApproachOrchestrator";
import { ChunkedQueryApproachOrchestrator } from "../../src/approaches/ChunkedQueryApproachOrchestrator";
import { ApproximationApproachOrchestrator } from "../../src/approaches/ApproximationApproachOrchestrator";

interface TestResult {
  approach: string;
  frequency: string;
  device: string;
  success: boolean;
  error?: string;
  metrics?: {
    latency: number;
    memoryUsage: number;
    throughput: number;
  };
}

async function runSingleIterationTest() {
  console.log("=".repeat(80));
  console.log("SINGLE ITERATION TEST");
  console.log("Verifying MQTT connectivity, SPARQL endpoints, and data flow");
  console.log("=".repeat(80));
  console.log();

  const configPath = path.join(
    __dirname,
    "frequency-experiment-config-test.json",
  );

  if (!fs.existsSync(configPath)) {
    console.error(`Test configuration not found: ${configPath}`);
    console.error("Please ensure frequency-experiment-config-test.json exists");
    process.exit(1);
  }

  const config = JSON.parse(fs.readFileSync(configPath, "utf-8"));

  console.log("Test Configuration:");
  console.log(`  - Approaches: ${config.approaches.join(", ")}`);
  console.log(`  - Frequencies: ${config.frequencies.join(", ")}`);
  console.log(`  - Device Types: ${config.deviceTypes.join(", ")}`);
  console.log(`  - Iterations: ${config.experiment.iterations}`);
  console.log(`  - Output: ${config.outputPath}`);
  console.log();

  const results: TestResult[] = [];
  const orchestratorMap: Record<string, any> = {
    "fetching-client-side": FetchingClientSideApproachOrchestrator,
    "chunked-query-approach": ChunkedQueryApproachOrchestrator,
    "approximation-approach": ApproximationApproachOrchestrator,
  };

  let totalTests = 0;
  let passedTests = 0;
  let failedTests = 0;

  for (const approach of config.approaches) {
    for (const frequency of config.frequencies) {
      for (const device of config.deviceTypes) {
        totalTests++;
        const testName = `${approach} @ ${frequency} (${device})`;
        console.log(`[${totalTests}] Testing: ${testName}`);
        console.log("-".repeat(80));

        const dataPath = path.join(
          config.dataBasePath,
          device,
          frequency,
          "data.nt",
        );

        if (!fs.existsSync(dataPath)) {
          console.error(`  ✗ Data file not found: ${dataPath}`);
          failedTests++;
          results.push({
            approach,
            frequency,
            device,
            success: false,
            error: `Data file not found: ${dataPath}`,
          });
          console.log();
          continue;
        }

        try {
          const startTime = Date.now();
          const startMemory = process.memoryUsage().heapUsed / 1024 / 1024;

          const OrchestratorClass = orchestratorMap[approach];
          if (!OrchestratorClass) {
            throw new Error(`Unknown approach: ${approach}`);
          }

          const orchestrator = new OrchestratorClass(dataPath);

          console.log(`  → Initializing ${orchestrator.getName()}...`);

          const queryResult = await orchestrator.runExperiment({
            query: config.queries.avgAcceleration.sparql,
            windowSize: config.queries.windowSize,
          });

          await orchestrator.cleanup();

          const endTime = Date.now();
          const endMemory = process.memoryUsage().heapUsed / 1024 / 1024;

          const executionTime = endTime - startTime;
          const memoryUsage = endMemory - startMemory;
          const observationsCount = countObservations(dataPath);
          const throughput = observationsCount / (executionTime / 1000);

          console.log(`  ✓ MQTT connection successful`);
          console.log(`  ✓ SPARQL query executed`);
          console.log(`  ✓ Data streaming completed`);
          console.log(`  ✓ Metrics collected:`);
          console.log(`    - Execution Time: ${executionTime.toFixed(2)} ms`);
          console.log(`    - Memory Usage: ${memoryUsage.toFixed(2)} MB`);
          console.log(`    - Throughput: ${throughput.toFixed(2)} obs/s`);
          if (queryResult) {
            const resultStr = JSON.stringify(queryResult);
            console.log(
              `    - Query Result: ${resultStr.substring(0, Math.min(100, resultStr.length))}${resultStr.length > 100 ? "..." : ""}`,
            );
          } else {
            console.log(`    - Query Result: (no result returned)`);
          }

          passedTests++;
          results.push({
            approach,
            frequency,
            device,
            success: true,
            metrics: {
              latency: executionTime,
              memoryUsage,
              throughput,
            },
          });
        } catch (error: any) {
          console.error(`  ✗ Test failed: ${error.message}`);
          console.error(`  Stack trace: ${error.stack}`);
          failedTests++;
          results.push({
            approach,
            frequency,
            device,
            success: false,
            error: error.message,
          });
        }

        console.log();
      }
    }
  }

  console.log("=".repeat(80));
  console.log("TEST SUMMARY");
  console.log("=".repeat(80));
  console.log();
  console.log(`Total Tests: ${totalTests}`);
  console.log(`Passed: ${passedTests}`);
  console.log(`Failed: ${failedTests}`);
  console.log();

  if (failedTests > 0) {
    console.log("Failed Tests:");
    results
      .filter((r) => !r.success)
      .forEach((r) => {
        console.log(
          `  - ${r.approach} @ ${r.frequency} (${r.device}): ${r.error}`,
        );
      });
    console.log();
  }

  const outputDir = config.outputPath;
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/:/g, "-").split(".")[0];
  const resultPath = path.join(outputDir, `test-results-${timestamp}.json`);
  fs.writeFileSync(
    resultPath,
    JSON.stringify(
      {
        timestamp: new Date().toISOString(),
        totalTests,
        passedTests,
        failedTests,
        results,
      },
      null,
      2,
    ),
  );

  console.log(`Results saved to: ${resultPath}`);
  console.log();

  if (failedTests === 0) {
    console.log("=".repeat(80));
    console.log("ALL TESTS PASSED");
    console.log("=".repeat(80));
    console.log();
    console.log("All approaches were able to:");
    console.log("  ✓ Connect to MQTT server");
    console.log("  ✓ Query SPARQL endpoint");
    console.log("  ✓ Process streaming data");
    console.log("  ✓ Collect performance metrics");
    console.log();
    console.log("You can now proceed with full benchmarking runs.");
    console.log();
    process.exit(0);
  } else {
    console.log("=".repeat(80));
    console.log("SOME TESTS FAILED");
    console.log("=".repeat(80));
    console.log();
    console.log(
      "Please fix the issues above before proceeding with full benchmarking.",
    );
    console.log();
    process.exit(1);
  }
}

function countObservations(dataPath: string): number {
  try {
    const content = fs.readFileSync(dataPath, "utf-8");
    // Count non-empty lines (RDF triples)
    return content.split("\n").filter((line) => line.trim().length > 0).length;
  } catch (error) {
    return 0;
  }
}

runSingleIterationTest().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
