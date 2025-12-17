#!/usr/bin/env ts-node

/**
 * Detailed accuracy comparison between approaches
 * Ground truth = Client-side fetching approach
 */

import * as fs from "fs";
import * as path from "path";

interface ResultRow {
  query_registered_timestamp: number;
  result_timestamp: number;
  result: number;
  iteration?: number;
}

interface IterationResults {
  iterationNumber: number;
  queryRegistered: number;
  results: number[];
  timeToFirstResult: number;
}

function parseCSV(filepath: string): ResultRow[] {
  const content = fs.readFileSync(filepath, "utf-8");
  const lines = content.trim().split("\n");
  const rows: ResultRow[] = [];

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

function groupByIteration(rows: ResultRow[]): IterationResults[] {
  const iterationMap = new Map<number, ResultRow[]>();

  for (const row of rows) {
    if (!iterationMap.has(row.query_registered_timestamp)) {
      iterationMap.set(row.query_registered_timestamp, []);
    }
    iterationMap.get(row.query_registered_timestamp)!.push(row);
  }

  const iterations: IterationResults[] = [];
  let iterationNumber = 1;

  const sortedTimestamps = Array.from(iterationMap.keys()).sort((a, b) => a - b);

  for (const timestamp of sortedTimestamps) {
    const rows = iterationMap.get(timestamp)!;
    rows.sort((a, b) => a.result_timestamp - b.result_timestamp);

    const timeToFirstResult = rows[0].result_timestamp - rows[0].query_registered_timestamp;
    const results = rows.map((r) => r.result);

    iterations.push({
      iterationNumber,
      queryRegistered: timestamp,
      results,
      timeToFirstResult,
    });

    iterationNumber++;
  }

  return iterations;
}

function getUniqueResults(results: number[], tolerance = 0.0001): number[] {
  const unique: number[] = [];

  for (const result of results) {
    const exists = unique.some((u) => Math.abs(u - result) < tolerance);
    if (!exists) {
      unique.push(result);
    }
  }

  return unique.sort((a, b) => b - a);
}

function compareResults(
  approach: number[],
  groundTruth: number[],
  tolerance = 0.0001
): { matches: number[]; missing: number[]; extra: number[] } {
  const matches: number[] = [];
  const missing: number[] = [];
  const extra: number[] = [];

  const gtUnique = getUniqueResults(groundTruth, tolerance);
  const approachUnique = getUniqueResults(approach, tolerance);

  for (const gt of gtUnique) {
    const found = approachUnique.some((a) => Math.abs(a - gt) < tolerance);
    if (found) {
      matches.push(gt);
    } else {
      missing.push(gt);
    }
  }

  for (const a of approachUnique) {
    const found = gtUnique.some((gt) => Math.abs(a - gt) < tolerance);
    if (!found) {
      extra.push(a);
    }
  }

  return { matches, missing, extra };
}

function calculateAccuracy(matches: number, total: number): number {
  if (total === 0) return 0;
  return (matches / total) * 100;
}

function main() {
  console.log("\n" + "=".repeat(80));
  console.log("DETAILED ACCURACY COMPARISON - 5 ITERATION EXPERIMENT");
  console.log("=".repeat(80) + "\n");

  const resultsDir = path.join(__dirname, "..", "results");

  const approaches = [
    { file: "fetching_client_side_results.csv", name: "Client-Side Fetching (Ground Truth)" },
    { file: "approximation_results.csv", name: "Approximation Approach" },
    { file: "chunked_query_results.csv", name: "Chunked Query Approach" },
  ];

  const allIterations: { [key: string]: IterationResults[] } = {};

  // Parse all approaches
  for (const approach of approaches) {
    const filepath = path.join(resultsDir, approach.file);
    if (!fs.existsSync(filepath)) {
      console.log(`ERROR: ${approach.file} not found\n`);
      continue;
    }

    const rows = parseCSV(filepath);
    const iterations = groupByIteration(rows);
    allIterations[approach.name] = iterations;
  }

  const groundTruthName = "Client-Side Fetching (Ground Truth)";
  const groundTruth = allIterations[groundTruthName];

  if (!groundTruth) {
    console.log("ERROR: Ground truth data not found\n");
    return;
  }

  console.log("GROUND TRUTH ANALYSIS");
  console.log("-".repeat(80) + "\n");

  for (const iteration of groundTruth) {
    const unique = getUniqueResults(iteration.results);
    console.log(`Iteration ${iteration.iterationNumber}:`);
    console.log(`  Total results: ${iteration.results.length}`);
    console.log(`  Unique values: [${unique.map(v => v.toFixed(7)).join(", ")}]`);
    console.log(`  Time to first result: ${(iteration.timeToFirstResult / 1000).toFixed(2)}s`);
    console.log();
  }

  // Overall ground truth summary
  const allGTResults = groundTruth.flatMap((it) => it.results);
  const gtUnique = getUniqueResults(allGTResults);
  const avgGTLatency = groundTruth.reduce((sum, it) => sum + it.timeToFirstResult, 0) / groundTruth.length;

  console.log("Overall Ground Truth:");
  console.log(`  Total iterations: ${groundTruth.length}`);
  console.log(`  Total results: ${allGTResults.length}`);
  console.log(`  Unique values: [${gtUnique.map(v => v.toFixed(7)).join(", ")}]`);
  console.log(`  Avg latency: ${(avgGTLatency / 1000).toFixed(2)}s\n`);

  console.log("\n" + "=".repeat(80));
  console.log("APPROACH COMPARISON");
  console.log("=".repeat(80) + "\n");

  // Compare each approach
  for (const approach of approaches) {
    if (approach.name === groundTruthName) continue;

    const iterations = allIterations[approach.name];
    if (!iterations) continue;

    console.log(`\n### ${approach.name.toUpperCase()} ###\n`);

    let totalMatches = 0;
    let totalMissing = 0;
    let totalExtra = 0;

    // Per-iteration comparison
    for (let i = 0; i < Math.min(iterations.length, groundTruth.length); i++) {
      const approachIteration = iterations[i];
      const gtIteration = groundTruth[i];

      const approachUnique = getUniqueResults(approachIteration.results);
      const gtUnique = getUniqueResults(gtIteration.results);

      const comparison = compareResults(approachIteration.results, gtIteration.results);

      console.log(`Iteration ${i + 1}:`);
      console.log(`  Approach results: ${approachIteration.results.length} (unique: [${approachUnique.map(v => v.toFixed(7)).join(", ")}])`);
      console.log(`  Ground truth: ${gtIteration.results.length} (unique: [${gtUnique.map(v => v.toFixed(7)).join(", ")}])`);
      console.log(`  Matches: [${comparison.matches.map(v => v.toFixed(7)).join(", ")}]`);

      if (comparison.missing.length > 0) {
        console.log(`  MISSING: [${comparison.missing.map(v => v.toFixed(7)).join(", ")}]`);
      }

      if (comparison.extra.length > 0) {
        console.log(`  EXTRA: [${comparison.extra.map(v => v.toFixed(7)).join(", ")}]`);
      }

      console.log(`  Latency: ${(approachIteration.timeToFirstResult / 1000).toFixed(2)}s (GT: ${(gtIteration.timeToFirstResult / 1000).toFixed(2)}s)`);
      console.log();

      totalMatches += comparison.matches.length;
      totalMissing += comparison.missing.length;
      totalExtra += comparison.extra.length;
    }

    // Overall comparison for this approach
    const allApproachResults = iterations.flatMap((it) => it.results);
    const approachUnique = getUniqueResults(allApproachResults);
    const overallComparison = compareResults(allApproachResults, allGTResults);
    const avgLatency = iterations.reduce((sum, it) => sum + it.timeToFirstResult, 0) / iterations.length;

    const accuracyByUniqueValues = calculateAccuracy(
      overallComparison.matches.length,
      gtUnique.length
    );

    console.log("Overall Summary:");
    console.log(`  Total results: ${allApproachResults.length}`);
    console.log(`  Unique values: [${approachUnique.map(v => v.toFixed(7)).join(", ")}]`);
    console.log(`  Matches with GT: [${overallComparison.matches.map(v => v.toFixed(7)).join(", ")}]`);

    if (overallComparison.missing.length > 0) {
      console.log(`  MISSING from GT: [${overallComparison.missing.map(v => v.toFixed(7)).join(", ")}]`);
    }

    if (overallComparison.extra.length > 0) {
      console.log(`  EXTRA (not in GT): [${overallComparison.extra.map(v => v.toFixed(7)).join(", ")}]`);
    }

    console.log(`  Accuracy (unique values): ${accuracyByUniqueValues.toFixed(1)}%`);
    console.log(`  Avg latency: ${(avgLatency / 1000).toFixed(2)}s (GT: ${(avgGTLatency / 1000).toFixed(2)}s)`);
    console.log(`  Latency overhead: +${((avgLatency - avgGTLatency) / 1000).toFixed(2)}s`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("FINAL SUMMARY TABLE");
  console.log("=".repeat(80) + "\n");

  console.log("| Approach | Avg Latency | Latency vs GT | Unique Results | Accuracy |");
  console.log("|----------|-------------|---------------|----------------|----------|");

  console.log(`| Ground Truth | ${(avgGTLatency / 1000).toFixed(2)}s | - | ${gtUnique.length} values | 100.0% |`);

  for (const approach of approaches) {
    if (approach.name === groundTruthName) continue;

    const iterations = allIterations[approach.name];
    if (!iterations) continue;

    const allApproachResults = iterations.flatMap((it) => it.results);
    const approachUnique = getUniqueResults(allApproachResults);
    const overallComparison = compareResults(allApproachResults, allGTResults);
    const avgLatency = iterations.reduce((sum, it) => sum + it.timeToFirstResult, 0) / iterations.length;
    const latencyDiff = avgLatency - avgGTLatency;
    const accuracyByUniqueValues = calculateAccuracy(
      overallComparison.matches.length,
      gtUnique.length
    );

    console.log(
      `| ${approach.name} | ${(avgLatency / 1000).toFixed(2)}s | +${(latencyDiff / 1000).toFixed(2)}s | ${approachUnique.length} values | ${accuracyByUniqueValues.toFixed(1)}% |`
    );
  }

  console.log("\n");
}

main();
