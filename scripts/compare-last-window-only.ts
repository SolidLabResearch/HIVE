#!/usr/bin/env ts-node

/**
 * Compare only the LAST window result from each iteration
 * The last window has the most data, so this gives the most accurate comparison
 */

import * as fs from "fs";
import * as path from "path";

interface ResultRow {
  query_registered_timestamp: number;
  result_timestamp: number;
  result: number;
}

interface LastWindowResult {
  iterationNumber: number;
  queryRegistered: number;
  lastResultTimestamp: number;
  lastResultValue: number;
  timeToLastResult: number;
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

function getLastWindowResults(rows: ResultRow[]): LastWindowResult[] {
  const iterationMap = new Map<number, ResultRow[]>();

  // Group by query registration timestamp
  for (const row of rows) {
    if (!iterationMap.has(row.query_registered_timestamp)) {
      iterationMap.set(row.query_registered_timestamp, []);
    }
    iterationMap.get(row.query_registered_timestamp)!.push(row);
  }

  const lastResults: LastWindowResult[] = [];
  let iterationNumber = 1;

  const sortedTimestamps = Array.from(iterationMap.keys()).sort((a, b) => a - b);

  for (const timestamp of sortedTimestamps) {
    const rows = iterationMap.get(timestamp)!;

    // Sort by result timestamp and take the LAST one
    rows.sort((a, b) => a.result_timestamp - b.result_timestamp);
    const lastRow = rows[rows.length - 1];

    const timeToLastResult = lastRow.result_timestamp - lastRow.query_registered_timestamp;

    lastResults.push({
      iterationNumber,
      queryRegistered: timestamp,
      lastResultTimestamp: lastRow.result_timestamp,
      lastResultValue: lastRow.result,
      timeToLastResult,
    });

    iterationNumber++;
  }

  return lastResults;
}

function compareValues(
  val1: number,
  val2: number,
  tolerance = 0.0001
): boolean {
  return Math.abs(val1 - val2) < tolerance;
}

function main() {
  console.log("\n" + "=".repeat(80));
  console.log("LAST WINDOW COMPARISON - 5 ITERATION EXPERIMENT");
  console.log("Comparing only the final result from each iteration");
  console.log("=".repeat(80) + "\n");

  const resultsDir = path.join(__dirname, "..", "results");

  const approaches = [
    { file: "fetching_client_side_results.csv", name: "Client-Side Fetching (Ground Truth)" },
    { file: "approximation_results.csv", name: "Approximation Approach" },
    { file: "chunked_query_results.csv", name: "Chunked Query Approach" },
  ];

  const allLastResults: { [key: string]: LastWindowResult[] } = {};

  // Parse all approaches and extract last window results
  for (const approach of approaches) {
    const filepath = path.join(resultsDir, approach.file);
    if (!fs.existsSync(filepath)) {
      console.log(`ERROR: ${approach.file} not found\n`);
      continue;
    }

    const rows = parseCSV(filepath);
    const lastResults = getLastWindowResults(rows);
    allLastResults[approach.name] = lastResults;
  }

  const groundTruthName = "Client-Side Fetching (Ground Truth)";
  const groundTruth = allLastResults[groundTruthName];

  if (!groundTruth) {
    console.log("ERROR: Ground truth data not found\n");
    return;
  }

  // Display ground truth
  console.log("GROUND TRUTH (Last Window Results):");
  console.log("-".repeat(80) + "\n");

  for (const result of groundTruth) {
    console.log(`Iteration ${result.iterationNumber}:`);
    console.log(`  Final result value: ${result.lastResultValue.toFixed(7)}`);
    console.log(`  Time to final result: ${(result.timeToLastResult / 1000).toFixed(2)}s`);
    console.log();
  }

  const gtValues = groundTruth.map(r => r.lastResultValue);
  const gtUniqueValues = [...new Set(gtValues.map(v => parseFloat(v.toFixed(7))))];
  const avgGTLatency = groundTruth.reduce((sum, r) => sum + r.timeToLastResult, 0) / groundTruth.length;

  console.log("Ground Truth Summary:");
  console.log(`  Total iterations: ${groundTruth.length}`);
  console.log(`  Unique final values: [${gtUniqueValues.map(v => v.toFixed(7)).join(", ")}]`);
  console.log(`  Avg time to final result: ${(avgGTLatency / 1000).toFixed(2)}s\n`);

  // Compare each approach
  console.log("\n" + "=".repeat(80));
  console.log("APPROACH COMPARISON (Last Window Only)");
  console.log("=".repeat(80) + "\n");

  for (const approach of approaches) {
    if (approach.name === groundTruthName) continue;

    const results = allLastResults[approach.name];
    if (!results) continue;

    console.log(`\n### ${approach.name.toUpperCase()} ###\n`);

    let matchCount = 0;
    const mismatches: { iteration: number; expected: number; actual: number }[] = [];

    // Per-iteration comparison
    for (let i = 0; i < Math.min(results.length, groundTruth.length); i++) {
      const approachResult = results[i];
      const gtResult = groundTruth[i];

      const matches = compareValues(approachResult.lastResultValue, gtResult.lastResultValue);

      console.log(`Iteration ${i + 1}:`);
      console.log(`  Ground Truth: ${gtResult.lastResultValue.toFixed(7)}`);
      console.log(`  Approach:     ${approachResult.lastResultValue.toFixed(7)}`);

      if (matches) {
        console.log(`  ✓ MATCH`);
        matchCount++;
      } else {
        console.log(`  ✗ MISMATCH`);
        mismatches.push({
          iteration: i + 1,
          expected: gtResult.lastResultValue,
          actual: approachResult.lastResultValue
        });
      }

      console.log(`  Latency: ${(approachResult.timeToLastResult / 1000).toFixed(2)}s (GT: ${(gtResult.timeToLastResult / 1000).toFixed(2)}s)`);
      console.log();
    }

    // Overall summary
    const approachValues = results.map(r => r.lastResultValue);
    const approachUniqueValues = [...new Set(approachValues.map(v => parseFloat(v.toFixed(7))))];
    const avgLatency = results.reduce((sum, r) => sum + r.timeToLastResult, 0) / results.length;
    const accuracy = (matchCount / Math.min(results.length, groundTruth.length)) * 100;

    console.log("Overall Summary:");
    console.log(`  Iterations compared: ${Math.min(results.length, groundTruth.length)}`);
    console.log(`  Matches: ${matchCount}/${Math.min(results.length, groundTruth.length)}`);
    console.log(`  Accuracy: ${accuracy.toFixed(1)}%`);
    console.log(`  Unique final values: [${approachUniqueValues.map(v => v.toFixed(7)).join(", ")}]`);
    console.log(`  Avg time to final result: ${(avgLatency / 1000).toFixed(2)}s`);
    console.log(`  Latency vs GT: ${avgLatency > avgGTLatency ? '+' : ''}${((avgLatency - avgGTLatency) / 1000).toFixed(2)}s`);

    if (mismatches.length > 0) {
      console.log("\n  Mismatches:");
      for (const mm of mismatches) {
        console.log(`    Iteration ${mm.iteration}: Expected ${mm.expected.toFixed(7)}, Got ${mm.actual.toFixed(7)}`);
      }
    }

    // Check for missing and extra values
    const missing = gtUniqueValues.filter(
      gtVal => !approachUniqueValues.some(appVal => compareValues(gtVal, appVal))
    );
    const extra = approachUniqueValues.filter(
      appVal => !gtUniqueValues.some(gtVal => compareValues(gtVal, appVal))
    );

    if (missing.length > 0) {
      console.log(`\n  Missing from GT: [${missing.map(v => v.toFixed(7)).join(", ")}]`);
    }
    if (extra.length > 0) {
      console.log(`  Extra (not in GT): [${extra.map(v => v.toFixed(7)).join(", ")}]`);
    }
  }

  // Final summary table
  console.log("\n" + "=".repeat(80));
  console.log("FINAL SUMMARY TABLE (Last Window Only)");
  console.log("=".repeat(80) + "\n");

  console.log("| Approach | Accuracy | Avg Latency | Latency vs GT | Unique Values |");
  console.log("|----------|----------|-------------|---------------|---------------|");

  console.log(`| Ground Truth | 100.0% | ${(avgGTLatency / 1000).toFixed(2)}s | - | ${gtUniqueValues.length} |`);

  for (const approach of approaches) {
    if (approach.name === groundTruthName) continue;

    const results = allLastResults[approach.name];
    if (!results) continue;

    const approachValues = results.map(r => r.lastResultValue);
    const approachUniqueValues = [...new Set(approachValues.map(v => parseFloat(v.toFixed(7))))];
    const avgLatency = results.reduce((sum, r) => sum + r.timeToLastResult, 0) / results.length;

    let matchCount = 0;
    for (let i = 0; i < Math.min(results.length, groundTruth.length); i++) {
      if (compareValues(results[i].lastResultValue, groundTruth[i].lastResultValue)) {
        matchCount++;
      }
    }
    const accuracy = (matchCount / Math.min(results.length, groundTruth.length)) * 100;
    const latencyDiff = avgLatency - avgGTLatency;

    const approachShortName = approach.name.replace(" Approach", "").replace(" (Ground Truth)", "");
    console.log(
      `| ${approachShortName} | ${accuracy.toFixed(1)}% | ${(avgLatency / 1000).toFixed(2)}s | ${latencyDiff > 0 ? '+' : ''}${(latencyDiff / 1000).toFixed(2)}s | ${approachUniqueValues.length} |`
    );
  }

  console.log("\n");
}

main();
