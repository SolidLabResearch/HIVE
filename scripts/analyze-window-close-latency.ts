#!/usr/bin/env ts-node

/**
 * Window-Close Latency Analysis
 *
 * This script properly calculates latency as the time between window close
 * and result emission, not total query execution time.
 *
 * For a window with RANGE 120000ms and STEP 60000ms:
 * - Window 1: [0, 120000] closes at query_start + 120000ms
 * - Window 2: [60000, 180000] closes at query_start + 180000ms
 * - etc.
 */

import * as fs from "fs";
import * as path from "path";

// ============================================================================
// INTERFACES
// ============================================================================

interface ResultEntry {
  queryRegisteredTimestamp: number;
  resultTimestamp: number;
  result: number;
  windowCloseTime?: number;
  windowNumber?: number;
  windowCloseLatency?: number;
}

interface WindowConfig {
  rangeMs: number;
  stepMs: number;
  dataStartOffset: number; // Time from query registration to first data point
}

interface LatencyStats {
  approach: string;
  totalResults: number;
  uniqueResults: number;
  duplicates: number;
  avgWindowCloseLatency: number;
  minWindowCloseLatency: number;
  maxWindowCloseLatency: number;
  medianWindowCloseLatency: number;
  stdDevLatency: number;
  resultsPerIteration: {
    min: number;
    max: number;
    avg: number;
  };
}

// ============================================================================
// CONFIGURATION
// ============================================================================

const RESULTS_DIR = path.join(__dirname, "../results");
const FETCHING_FILE = path.join(
  RESULTS_DIR,
  "fetching_client_side_results.csv",
);
const CHUNKED_FILE = path.join(RESULTS_DIR, "chunked_query_results.csv");
const APPROXIMATION_FILE = path.join(RESULTS_DIR, "approximation_results.csv");

// Window configuration from the queries
const WINDOW_CONFIG: WindowConfig = {
  rangeMs: 120000, // 120 seconds
  stepMs: 60000, // 60 seconds
  dataStartOffset: 10000, // Estimated ~10s from query registration to first data
};

const OUTPUT_FILE = path.join(RESULTS_DIR, "window-close-latency-report.txt");

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function loadCSVResults(filePath: string): ResultEntry[] {
  if (!fs.existsSync(filePath)) {
    console.warn(`File not found: ${filePath}`);
    return [];
  }

  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.trim().split("\n").slice(1); // Skip header

  return lines
    .map((line) => {
      const parts = line.split(",");
      return {
        queryRegisteredTimestamp: parseInt(parts[0]),
        resultTimestamp: parseInt(parts[1]),
        result: parseFloat(parts[2]),
      };
    })
    .filter((entry) => !isNaN(entry.result) && entry.result !== -9);
}

function groupByIteration(results: ResultEntry[]): Map<number, ResultEntry[]> {
  const iterations = new Map<number, ResultEntry[]>();

  results.forEach((entry) => {
    const queryStart = entry.queryRegisteredTimestamp;
    if (!iterations.has(queryStart)) {
      iterations.set(queryStart, []);
    }
    iterations.get(queryStart)!.push(entry);
  });

  return iterations;
}

function calculateWindowCloseTime(
  queryStart: number,
  resultTimestamp: number,
  config: WindowConfig,
): { closeTime: number; windowNumber: number } {
  const dataStart = queryStart + config.dataStartOffset;
  const timeSinceDataStart = resultTimestamp - dataStart;

  // Determine which window this result belongs to
  // Window 1 closes at rangeMs, Window 2 at rangeMs + stepMs, etc.
  let windowNumber = 1;
  let windowCloseTime = dataStart + config.rangeMs;

  // Find the window that would have just closed
  while (windowCloseTime + config.stepMs <= resultTimestamp) {
    windowNumber++;
    windowCloseTime += config.stepMs;
  }

  return { closeTime: windowCloseTime, windowNumber };
}

function removeDuplicates(results: ResultEntry[]): {
  unique: ResultEntry[];
  duplicateCount: number;
} {
  const seen = new Set<string>();
  const unique: ResultEntry[] = [];
  let duplicateCount = 0;

  results.forEach((entry) => {
    // Consider entries duplicate if they have same timestamp and value
    const key = `${entry.resultTimestamp}-${entry.result.toFixed(6)}`;
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(entry);
    } else {
      duplicateCount++;
    }
  });

  return { unique, duplicateCount };
}

function calculateWindowCloseLatencies(
  results: ResultEntry[],
  config: WindowConfig,
): ResultEntry[] {
  return results.map((entry) => {
    const { closeTime, windowNumber } = calculateWindowCloseTime(
      entry.queryRegisteredTimestamp,
      entry.resultTimestamp,
      config,
    );

    return {
      ...entry,
      windowCloseTime: closeTime,
      windowNumber,
      windowCloseLatency: entry.resultTimestamp - closeTime,
    };
  });
}

function calculateStatistics(values: number[]): {
  avg: number;
  min: number;
  max: number;
  median: number;
  stdDev: number;
} {
  if (values.length === 0) {
    return { avg: 0, min: 0, max: 0, median: 0, stdDev: 0 };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const avg = values.reduce((sum, val) => sum + val, 0) / values.length;
  const median =
    values.length % 2 === 0
      ? (sorted[values.length / 2 - 1] + sorted[values.length / 2]) / 2
      : sorted[Math.floor(values.length / 2)];

  const variance =
    values.reduce((sum, val) => sum + Math.pow(val - avg, 2), 0) /
    values.length;
  const stdDev = Math.sqrt(variance);

  return {
    avg,
    min: sorted[0],
    max: sorted[sorted.length - 1],
    median,
    stdDev,
  };
}

function analyzeApproach(
  approachName: string,
  results: ResultEntry[],
  config: WindowConfig,
): LatencyStats {
  // Remove duplicates
  const { unique, duplicateCount } = removeDuplicates(results);

  // Calculate window close latencies
  const withLatencies = calculateWindowCloseLatencies(unique, config);

  // Extract latency values
  const latencies = withLatencies
    .map((e) => e.windowCloseLatency!)
    .filter((l) => !isNaN(l) && l >= 0); // Filter out invalid latencies

  const stats = calculateStatistics(latencies);

  // Analyze results per iteration
  const iterations = groupByIteration(unique);
  const resultsPerIter = Array.from(iterations.values()).map(
    (iter) => iter.length,
  );
  const iterStats = calculateStatistics(resultsPerIter);

  return {
    approach: approachName,
    totalResults: results.length,
    uniqueResults: unique.length,
    duplicates: duplicateCount,
    avgWindowCloseLatency: stats.avg,
    minWindowCloseLatency: stats.min,
    maxWindowCloseLatency: stats.max,
    medianWindowCloseLatency: stats.median,
    stdDevLatency: stats.stdDev,
    resultsPerIteration: {
      min: iterStats.min,
      max: iterStats.max,
      avg: iterStats.avg,
    },
  };
}

function generateReport(
  fetchingStats: LatencyStats,
  chunkedStats: LatencyStats,
  approximationStats: LatencyStats,
): string {
  const lines: string[] = [];

  lines.push("=".repeat(80));
  lines.push("WINDOW-CLOSE LATENCY ANALYSIS");
  lines.push("=".repeat(80));
  lines.push("");
  lines.push(`Report Generated: ${new Date().toISOString()}`);
  lines.push(
    `Window Configuration: RANGE ${WINDOW_CONFIG.rangeMs}ms, STEP ${WINDOW_CONFIG.stepMs}ms`,
  );
  lines.push("");
  lines.push(
    "Latency Definition: Time between window close and result emission",
  );
  lines.push("  (NOT total query execution time)");
  lines.push("");

  // Results summary
  lines.push("=".repeat(80));
  lines.push("RESULTS SUMMARY");
  lines.push("=".repeat(80));
  lines.push("");

  const approaches = [
    { name: "Fetching Client-Side", stats: fetchingStats },
    { name: "Chunked Query", stats: chunkedStats },
    { name: "Approximation", stats: approximationStats },
  ];

  for (const { name, stats } of approaches) {
    lines.push(`${name}:`);
    lines.push(`  Total Results:        ${stats.totalResults}`);
    lines.push(`  Unique Results:       ${stats.uniqueResults}`);
    lines.push(`  Duplicates Removed:   ${stats.duplicates}`);
    lines.push(`  Results/Iteration:`);
    lines.push(
      `    Average:            ${stats.resultsPerIteration.avg.toFixed(1)}`,
    );
    lines.push(`    Min:                ${stats.resultsPerIteration.min}`);
    lines.push(`    Max:                ${stats.resultsPerIteration.max}`);
    lines.push("");
  }

  // Latency comparison
  lines.push("=".repeat(80));
  lines.push("WINDOW-CLOSE LATENCY COMPARISON (milliseconds)");
  lines.push("=".repeat(80));
  lines.push("");

  for (const { name, stats } of approaches) {
    if (stats.uniqueResults === 0) {
      lines.push(`${name}: No results`);
      lines.push("");
      continue;
    }

    lines.push(`${name}:`);
    lines.push(
      `  Average Latency:      ${stats.avgWindowCloseLatency.toFixed(2)} ms`,
    );
    lines.push(
      `  Median Latency:       ${stats.medianWindowCloseLatency.toFixed(2)} ms`,
    );
    lines.push(
      `  Min Latency:          ${stats.minWindowCloseLatency.toFixed(2)} ms`,
    );
    lines.push(
      `  Max Latency:          ${stats.maxWindowCloseLatency.toFixed(2)} ms`,
    );
    lines.push(`  Std Dev:              ${stats.stdDevLatency.toFixed(2)} ms`);
    lines.push("");
  }

  // Ranking
  const validApproaches = approaches.filter((a) => a.stats.uniqueResults > 0);
  const ranked = validApproaches.sort(
    (a, b) => a.stats.avgWindowCloseLatency - b.stats.avgWindowCloseLatency,
  );

  lines.push("Latency Ranking (Fastest to Slowest):");
  ranked.forEach((item, idx) => {
    lines.push(
      `  ${idx + 1}. ${item.name}: ${item.stats.avgWindowCloseLatency.toFixed(2)} ms`,
    );
  });
  lines.push("");

  // Expected vs actual results
  lines.push("=".repeat(80));
  lines.push("EXPECTED VS ACTUAL RESULTS");
  lines.push("=".repeat(80));
  lines.push("");
  lines.push("With 120-second data replay and window config:");
  lines.push("  RANGE: 120000ms (120s)");
  lines.push("  STEP:  60000ms (60s)");
  lines.push("");
  lines.push("Expected windows per iteration:");
  lines.push("  Window 1: [0, 120s] closes at 120s");
  lines.push("  Window 2: [60s, 180s] closes at 180s (but data ends at 120s)");
  lines.push(
    "  Expected: 1-2 results per iteration (depending on flush timing)",
  );
  lines.push("");

  for (const { name, stats } of approaches) {
    const expected = "1-2";
    const actual = stats.resultsPerIteration.avg.toFixed(1);
    const status =
      stats.resultsPerIteration.avg <= 2.5
        ? "✓ CORRECT"
        : "✗ EXCESSIVE (check for duplicates or accumulation)";
    lines.push(`${name}:`);
    lines.push(`  Expected: ${expected} results/iteration`);
    lines.push(`  Actual:   ${actual} results/iteration`);
    lines.push(`  Status:   ${status}`);
    lines.push("");
  }

  // Issues detected
  lines.push("=".repeat(80));
  lines.push("ISSUES DETECTED");
  lines.push("=".repeat(80));
  lines.push("");

  let issuesFound = false;

  for (const { name, stats } of approaches) {
    if (stats.duplicates > 0) {
      lines.push(`⚠️  ${name}: ${stats.duplicates} duplicate results detected`);
      issuesFound = true;
    }
    if (stats.resultsPerIteration.avg > 2.5) {
      lines.push(
        `⚠️  ${name}: Excessive results per iteration (${stats.resultsPerIteration.avg.toFixed(1)}, expected 1-2)`,
      );
      issuesFound = true;
    }
  }

  if (!issuesFound) {
    lines.push("✓ No issues detected");
  }

  lines.push("");
  lines.push("=".repeat(80));

  return lines.join("\n");
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function main(): Promise<void> {
  console.log("=".repeat(80));
  console.log("WINDOW-CLOSE LATENCY ANALYSIS");
  console.log("=".repeat(80));
  console.log("");

  // Load results
  console.log("Loading results from CSV files...");
  const fetchingResults = loadCSVResults(FETCHING_FILE);
  const chunkedResults = loadCSVResults(CHUNKED_FILE);
  const approximationResults = loadCSVResults(APPROXIMATION_FILE);

  console.log(`  Fetching:      ${fetchingResults.length} results`);
  console.log(`  Chunked:       ${chunkedResults.length} results`);
  console.log(`  Approximation: ${approximationResults.length} results`);
  console.log("");

  // Analyze each approach
  console.log("Analyzing window-close latencies...");
  const fetchingStats = analyzeApproach(
    "Fetching Client-Side",
    fetchingResults,
    WINDOW_CONFIG,
  );
  const chunkedStats = analyzeApproach(
    "Chunked Query",
    chunkedResults,
    WINDOW_CONFIG,
  );
  const approximationStats = analyzeApproach(
    "Approximation",
    approximationResults,
    WINDOW_CONFIG,
  );

  // Generate report
  const report = generateReport(
    fetchingStats,
    chunkedStats,
    approximationStats,
  );

  // Save to file
  console.log(`\nSaving report to: ${OUTPUT_FILE}`);
  fs.writeFileSync(OUTPUT_FILE, report);

  // Display report
  console.log("\n");
  console.log(report);

  console.log("\n✓ Analysis complete!");
  console.log(`  Report saved: ${OUTPUT_FILE}`);
}

// Run the analysis
main().catch((error) => {
  console.error("Error during analysis:", error);
  process.exit(1);
});
