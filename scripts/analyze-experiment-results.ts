#!/usr/bin/env ts-node

/**
 * Analyze 5-iteration experiment results and generate comparison tables
 * Reads CSV files from results/ directory and computes statistics
 */

import * as fs from "fs";
import * as path from "path";

interface ResultRow {
  query_registered_timestamp: number;
  result_timestamp: number;
  result: number;
}

interface ApproachStats {
  name: string;
  latencies: number[];
  avgLatency: number;
  stdDevLatency: number;
  results: number[];
  uniqueResults: Set<number>;
}

interface ResourceMetrics {
  cpu: number;
  memory: number;
}

function parseCSV(filepath: string): ResultRow[] {
  const content = fs.readFileSync(filepath, "utf-8");
  const lines = content.trim().split("\n");
  const rows: ResultRow[] = [];

  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const [query_registered_timestamp, result_timestamp, result] =
      lines[i].split(",");
    rows.push({
      query_registered_timestamp: parseInt(query_registered_timestamp),
      result_timestamp: parseInt(result_timestamp),
      result: parseFloat(result),
    });
  }

  return rows;
}

function calculateStats(rows: ResultRow[]): ApproachStats {
  const results: number[] = [];
  const uniqueResults = new Set<number>();

  // Group by query registration timestamp to identify iterations
  const iterationMap = new Map<number, ResultRow[]>();

  for (const row of rows) {
    results.push(row.result);
    uniqueResults.add(parseFloat(row.result.toFixed(7)));

    if (!iterationMap.has(row.query_registered_timestamp)) {
      iterationMap.set(row.query_registered_timestamp, []);
    }
    iterationMap.get(row.query_registered_timestamp)!.push(row);
  }

  // Calculate time-to-first-result for each iteration
  const latencies: number[] = [];
  for (const [queryTimestamp, iterationRows] of iterationMap) {
    // Sort by result timestamp and take the first
    iterationRows.sort((a, b) => a.result_timestamp - b.result_timestamp);
    const firstResult = iterationRows[0];
    const latency =
      firstResult.result_timestamp - firstResult.query_registered_timestamp;
    latencies.push(latency);
  }

  const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const variance =
    latencies.reduce((sum, val) => sum + Math.pow(val - avgLatency, 2), 0) /
    latencies.length;
  const stdDevLatency = Math.sqrt(variance);

  return {
    name: "",
    latencies,
    avgLatency,
    stdDevLatency,
    results,
    uniqueResults,
  };
}

function calculateAccuracy(
  approachResults: number[],
  groundTruthResults: number[],
): number {
  // Compare against ground truth (Fetching Client Side)
  if (groundTruthResults.length === 0) {
    return 0;
  }

  const tolerance = 0.0001;
  let matchCount = 0;

  // For each approach result, check if it matches any ground truth result within tolerance
  for (const result of approachResults) {
    const hasMatch = groundTruthResults.some(
      (gtResult) => Math.abs(result - gtResult) < tolerance,
    );
    if (hasMatch) {
      matchCount++;
    }
  }

  return (matchCount / approachResults.length) * 100;
}

function getMostFrequentResults(results: number[], topN: number = 2): number[] {
  const frequency = new Map<number, number>();

  for (const result of results) {
    const rounded = parseFloat(result.toFixed(7));
    frequency.set(rounded, (frequency.get(rounded) || 0) + 1);
  }

  return Array.from(frequency.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([value]) => value);
}

function main() {
  console.log(
    "\n======================================================================",
  );
  console.log("5-ITERATION EXPERIMENT ANALYSIS");
  console.log(
    "======================================================================\n",
  );

  const resultsDir = path.join(__dirname, "..", "results");

  const approaches = [
    { file: "approximation_results.csv", name: "Approximation Approach" },
    { file: "chunked_query_results.csv", name: "Chunked Query Approach" },
    {
      file: "fetching_client_side_results.csv",
      name: "Client Side Processing",
    },
  ];

  const allStats: { [key: string]: ApproachStats } = {};

  // Parse and calculate statistics for each approach
  for (const approach of approaches) {
    const filepath = path.join(resultsDir, approach.file);
    if (!fs.existsSync(filepath)) {
      console.log(`Warning: ${approach.file} not found`);
      continue;
    }

    const rows = parseCSV(filepath);
    const stats = calculateStats(rows);
    stats.name = approach.name;
    allStats[approach.name] = stats;

    console.log(`${approach.name}:`);
    console.log(`  Total results: ${rows.length}`);
    console.log(`  Number of iterations: ${stats.latencies.length}`);
    console.log(
      `  Unique result values: ${Array.from(stats.uniqueResults).join(", ")}`,
    );
    console.log(
      `  Avg time-to-first-result: ${(stats.avgLatency / 1000).toFixed(2)}s (${stats.avgLatency.toFixed(0)}ms)`,
    );
    console.log(
      `  Std dev: ${(stats.stdDevLatency / 1000).toFixed(2)}s (${stats.stdDevLatency.toFixed(1)}ms)`,
    );
    console.log();
  }

  // Generate tables
  console.log(
    "\n======================================================================",
  );
  console.log("BENCHMARKING RESULTS");
  console.log(
    "======================================================================\n",
  );

  // Latency Table
  console.log("**Benchmarking the Latency (Time-to-First-Result)**\n");
  console.log("| **Approaches** | **Latency** |");
  console.log("| --- | --- |");

  for (const approach of approaches) {
    const stats = allStats[approach.name];
    if (stats) {
      console.log(
        `| ${approach.name} | ${(stats.avgLatency / 1000).toFixed(2)}s +/- ${(stats.stdDevLatency / 1000).toFixed(2)}s |`,
      );
    }
  }

  // Resource Usage Table (placeholder - would need actual monitoring)
  console.log("\n**Benchmarking the Resources Used**\n");
  console.log("| **Approaches** | **CPU%** | **Memory (in MB)** |");
  console.log("| --- | --- | --- |");
  console.log(
    "| Chunked Query Approach | [Monitor Required] | [Monitor Required] |",
  );
  console.log(
    "| Approximation Approach | [Monitor Required] | [Monitor Required] |",
  );
  console.log(
    "| Client Side Processing | [Monitor Required] | [Monitor Required] |",
  );

  // Accuracy Table - Compare against Fetching Client Side as ground truth
  console.log("\n**Benchmarking the Accuracy**\n");
  console.log("| **Approaches** | **Results** | **Accuracy** |");
  console.log("| --- | --- | --- |");

  // Use Fetching Client Side as ground truth
  const groundTruthStats = allStats["Client Side Processing"];

  if (groundTruthStats) {
    const groundTruthTop2 = getMostFrequentResults(groundTruthStats.results, 2);
    const gtResultStr = groundTruthTop2.map((v) => v.toFixed(7)).join(", ");

    for (const approach of approaches) {
      const stats = allStats[approach.name];
      if (stats) {
        const topResults = getMostFrequentResults(stats.results, 2);
        const resultStr = topResults.map((v) => v.toFixed(7)).join(", ");

        if (approach.name === "Client Side Processing") {
          // Ground truth
          console.log(
            `| ${approach.name} | ${resultStr} | 100% (ground truth) |`,
          );
        } else {
          // Calculate accuracy compared to ground truth
          const accuracy = calculateAccuracy(
            stats.results,
            groundTruthStats.results,
          );
          console.log(
            `| ${approach.name} | ${resultStr} | ${accuracy.toFixed(1)}% |`,
          );
        }
      }
    }
  }

  console.log(
    "\n======================================================================",
  );
  console.log("SUMMARY");
  console.log(
    "======================================================================\n",
  );
  console.log("Experiment Configuration:");
  console.log("  - Number of iterations: 5");
  console.log(
    "  - Data: Pre-recorded smartphone and wearable acceleration data",
  );
  console.log("  - Publishing rate: 4 Hz");
  console.log("  - Total observations per stream: ~481");
  console.log("\nGround Truth: Client Side Processing (fetching approach)");
  console.log("  - This approach fetches all data and processes client-side");
  console.log("  - Used as the baseline for accuracy comparison");
  console.log("\nAccuracy Calculation:");
  console.log(
    "  - Compares each approach's results against Client Side Processing",
  );
  console.log(
    "  - Percentage represents how many results match the ground truth",
  );
  console.log("\nLatency Measurement:");
  console.log(
    "  - Time-to-first-result: Time from query registration to first result",
  );
  console.log(
    "  - Note: High latencies (~60-80s) reflect the streaming experiment",
  );
  console.log(
    "    duration where data is published over time before aggregation",
  );
  console.log("\nResource Monitoring:");
  console.log("  - CPU and Memory metrics require runtime instrumentation");
  console.log("  - Not captured in current CSV results");
  console.log(
    "  - Run with resource monitoring tools (e.g., pidstat, top) for these metrics",
  );
  console.log("\n");
}

main();
