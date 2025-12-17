/**
 * Analyze 5 Iterations Results - Accuracy and Latency Comparison
 *
 * This script analyzes the results from the 5-iteration experiment,
 * comparing accuracy (against fetching client-side as ground truth)
 * and latency across all three approaches.
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
}

interface LatencyMetrics {
  approach: string;
  avgLatency: number;
  minLatency: number;
  maxLatency: number;
  medianLatency: number;
  stdDevLatency: number;
  totalResults: number;
  latencies: number[];
}

interface AccuracyMetrics {
  approach: string;
  matchedResults: number;
  totalResults: number;
  mae: number; // Mean Absolute Error
  rmse: number; // Root Mean Square Error
  mape: number; // Mean Absolute Percentage Error
  maxError: number;
  minError: number;
  avgError: number;
  perfectMatches: number;
  accuracyPercent: number;
}

interface ComparisonReport {
  timestamp: string;
  totalIterations: number;
  groundTruthApproach: string;

  latencyComparison: {
    approximation: LatencyMetrics;
    chunked: LatencyMetrics;
    fetching: LatencyMetrics;
    ranking: { approach: string; avgLatency: number }[];
  };

  accuracyComparison: {
    approximation: AccuracyMetrics;
    chunked: AccuracyMetrics;
    ranking: { approach: string; accuracyPercent: number }[];
  };

  summary: {
    fastestApproach: string;
    fastestLatency: number;
    mostAccurateApproach: string;
    highestAccuracy: number;
    recommendedApproach: string;
    recommendation: string;
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
const OUTPUT_FILE = path.join(
  RESULTS_DIR,
  "5-iterations-comparison-report.json",
);
const OUTPUT_TXT_FILE = path.join(
  RESULTS_DIR,
  "5-iterations-comparison-report.txt",
);

// Tolerance for considering values as "matching"
const VALUE_TOLERANCE = 0.01;

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
    .filter((entry) => !isNaN(entry.result) && entry.result !== -9); // Filter out error values
}

function calculateLatency(entry: ResultEntry): number {
  return entry.resultTimestamp - entry.queryRegisteredTimestamp;
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

// ============================================================================
// LATENCY ANALYSIS
// ============================================================================

function analyzeLatency(
  approach: string,
  results: ResultEntry[],
): LatencyMetrics {
  const latencies = results.map(calculateLatency);
  const stats = calculateStatistics(latencies);

  return {
    approach,
    avgLatency: stats.avg,
    minLatency: stats.min,
    maxLatency: stats.max,
    medianLatency: stats.median,
    stdDevLatency: stats.stdDev,
    totalResults: results.length,
    latencies,
  };
}

// ============================================================================
// ACCURACY ANALYSIS
// ============================================================================

function compareAccuracy(
  groundTruth: ResultEntry[],
  testApproach: ResultEntry[],
  approachName: string,
): AccuracyMetrics {
  const errors: number[] = [];
  const percentageErrors: number[] = [];
  let perfectMatches = 0;
  let matchedResults = 0;

  // Group ground truth by timestamp window (±120 seconds to account for different registration times)
  const WINDOW_MS = 120000;

  for (const testEntry of testApproach) {
    // Find all ground truth results with the same value (within tolerance)
    const matchingGroundTruth = groundTruth.filter(
      (gt) => Math.abs(gt.result - testEntry.result) <= VALUE_TOLERANCE,
    );

    if (matchingGroundTruth.length > 0) {
      // Found exact value match
      matchedResults++;
      perfectMatches++;
      errors.push(0);
      percentageErrors.push(0);
    } else {
      // Find closest ground truth result by value
      const closestByValue = groundTruth.reduce((closest, current) => {
        const currentDiff = Math.abs(current.result - testEntry.result);
        const closestDiff = Math.abs(closest.result - testEntry.result);
        return currentDiff < closestDiff ? current : closest;
      }, groundTruth[0]);

      if (closestByValue) {
        matchedResults++;
        const error = Math.abs(testEntry.result - closestByValue.result);
        errors.push(error);

        // Calculate percentage error (avoid division by zero)
        if (Math.abs(closestByValue.result) > VALUE_TOLERANCE) {
          const percentError = (error / Math.abs(closestByValue.result)) * 100;
          percentageErrors.push(percentError);
        }
      }
    }
  }

  // Calculate metrics
  const mae =
    errors.length > 0
      ? errors.reduce((sum, e) => sum + e, 0) / errors.length
      : 0;

  const rmse =
    errors.length > 0
      ? Math.sqrt(errors.reduce((sum, e) => sum + e * e, 0) / errors.length)
      : 0;

  const mape =
    percentageErrors.length > 0
      ? percentageErrors.reduce((sum, e) => sum + e, 0) /
        percentageErrors.length
      : 0;

  const maxError = errors.length > 0 ? Math.max(...errors) : 0;
  const minError = errors.length > 0 ? Math.min(...errors) : 0;
  const avgError = mae;

  const accuracyPercent = matchedResults > 0 ? 100 - mape : 0;

  return {
    approach: approachName,
    matchedResults,
    totalResults: testApproach.length,
    mae,
    rmse,
    mape,
    maxError,
    minError,
    avgError,
    perfectMatches,
    accuracyPercent: Math.max(0, accuracyPercent),
  };
}

// ============================================================================
// REPORT GENERATION
// ============================================================================

function generateReport(
  fetchingResults: ResultEntry[],
  chunkedResults: ResultEntry[],
  approximationResults: ResultEntry[],
): ComparisonReport {
  // Latency Analysis
  const fetchingLatency = analyzeLatency(
    "Fetching Client-Side",
    fetchingResults,
  );
  const chunkedLatency = analyzeLatency("Chunked Query", chunkedResults);
  const approximationLatency = analyzeLatency(
    "Approximation",
    approximationResults,
  );

  const latencyRanking = [
    { approach: "Approximation", avgLatency: approximationLatency.avgLatency },
    { approach: "Chunked Query", avgLatency: chunkedLatency.avgLatency },
    {
      approach: "Fetching Client-Side",
      avgLatency: fetchingLatency.avgLatency,
    },
  ].sort((a, b) => a.avgLatency - b.avgLatency);

  // Accuracy Analysis (using Fetching as ground truth)
  const chunkedAccuracy = compareAccuracy(
    fetchingResults,
    chunkedResults,
    "Chunked Query",
  );
  const approximationAccuracy = compareAccuracy(
    fetchingResults,
    approximationResults,
    "Approximation",
  );

  const accuracyRanking = [
    {
      approach: "Chunked Query",
      accuracyPercent: chunkedAccuracy.accuracyPercent,
    },
    {
      approach: "Approximation",
      accuracyPercent: approximationAccuracy.accuracyPercent,
    },
  ].sort((a, b) => b.accuracyPercent - a.accuracyPercent);

  // Determine recommendations
  const fastestApproach = latencyRanking[0].approach;
  const fastestLatency = latencyRanking[0].avgLatency;
  const mostAccurateApproach = accuracyRanking[0].approach;
  const highestAccuracy = accuracyRanking[0].accuracyPercent;

  let recommendedApproach = "";
  let recommendation = "";

  if (mostAccurateApproach === fastestApproach) {
    recommendedApproach = mostAccurateApproach;
    recommendation = `${recommendedApproach} is both the fastest and most accurate approach.`;
  } else if (highestAccuracy > 95) {
    recommendedApproach = fastestApproach;
    recommendation = `${fastestApproach} is recommended for its speed (${fastestLatency.toFixed(2)}ms avg latency) while maintaining good accuracy (${highestAccuracy.toFixed(2)}%).`;
  } else {
    recommendedApproach = mostAccurateApproach;
    recommendation = `${mostAccurateApproach} is recommended for its superior accuracy (${highestAccuracy.toFixed(2)}%) despite slightly higher latency.`;
  }

  return {
    timestamp: new Date().toISOString(),
    totalIterations: 5,
    groundTruthApproach: "Fetching Client-Side",

    latencyComparison: {
      approximation: approximationLatency,
      chunked: chunkedLatency,
      fetching: fetchingLatency,
      ranking: latencyRanking,
    },

    accuracyComparison: {
      approximation: approximationAccuracy,
      chunked: chunkedAccuracy,
      ranking: accuracyRanking,
    },

    summary: {
      fastestApproach,
      fastestLatency,
      mostAccurateApproach,
      highestAccuracy,
      recommendedApproach,
      recommendation,
    },
  };
}

// ============================================================================
// TEXT REPORT FORMATTING
// ============================================================================

function generateTextReport(report: ComparisonReport): string {
  const lines: string[] = [];

  lines.push("=".repeat(80));
  lines.push("5 ITERATIONS EXPERIMENT - ACCURACY & LATENCY COMPARISON");
  lines.push("=".repeat(80));
  lines.push("");
  lines.push(`Report Generated: ${report.timestamp}`);
  lines.push(`Total Iterations: ${report.totalIterations}`);
  lines.push(`Ground Truth: ${report.groundTruthApproach}`);
  lines.push("");

  // Latency Comparison
  lines.push("=".repeat(80));
  lines.push("LATENCY COMPARISON (milliseconds)");
  lines.push("=".repeat(80));
  lines.push("");

  const approaches = [
    { name: "Approximation", data: report.latencyComparison.approximation },
    { name: "Chunked Query", data: report.latencyComparison.chunked },
    { name: "Fetching Client-Side", data: report.latencyComparison.fetching },
  ];

  for (const { name, data } of approaches) {
    lines.push(`${name}:`);
    lines.push(`  Average Latency:    ${data.avgLatency.toFixed(2)} ms`);
    lines.push(`  Median Latency:     ${data.medianLatency.toFixed(2)} ms`);
    lines.push(`  Min Latency:        ${data.minLatency.toFixed(2)} ms`);
    lines.push(`  Max Latency:        ${data.maxLatency.toFixed(2)} ms`);
    lines.push(`  Std Dev:            ${data.stdDevLatency.toFixed(2)} ms`);
    lines.push(`  Total Results:      ${data.totalResults}`);
    lines.push("");
  }

  lines.push("Latency Ranking (Fastest to Slowest):");
  report.latencyComparison.ranking.forEach((item, idx) => {
    lines.push(
      `  ${idx + 1}. ${item.approach}: ${item.avgLatency.toFixed(2)} ms`,
    );
  });
  lines.push("");

  // Accuracy Comparison
  lines.push("=".repeat(80));
  lines.push("ACCURACY COMPARISON (vs Fetching Client-Side Ground Truth)");
  lines.push("=".repeat(80));
  lines.push("");

  const accuracyApproaches = [
    { name: "Approximation", data: report.accuracyComparison.approximation },
    { name: "Chunked Query", data: report.accuracyComparison.chunked },
  ];

  for (const { name, data } of accuracyApproaches) {
    lines.push(`${name}:`);
    lines.push(`  Accuracy:           ${data.accuracyPercent.toFixed(2)}%`);
    lines.push(`  MAE:                ${data.mae.toFixed(6)}`);
    lines.push(`  RMSE:               ${data.rmse.toFixed(6)}`);
    lines.push(`  MAPE:               ${data.mape.toFixed(2)}%`);
    lines.push(`  Max Error:          ${data.maxError.toFixed(6)}`);
    lines.push(`  Min Error:          ${data.minError.toFixed(6)}`);
    lines.push(
      `  Perfect Matches:    ${data.perfectMatches}/${data.matchedResults}`,
    );
    lines.push(`  Total Results:      ${data.totalResults}`);
    lines.push("");
  }

  lines.push("Accuracy Ranking (Most to Least Accurate):");
  report.accuracyComparison.ranking.forEach((item, idx) => {
    lines.push(
      `  ${idx + 1}. ${item.approach}: ${item.accuracyPercent.toFixed(2)}%`,
    );
  });
  lines.push("");

  // Summary
  lines.push("=".repeat(80));
  lines.push("SUMMARY & RECOMMENDATIONS");
  lines.push("=".repeat(80));
  lines.push("");
  lines.push(
    `Fastest Approach:        ${report.summary.fastestApproach} (${report.summary.fastestLatency.toFixed(2)} ms)`,
  );
  lines.push(
    `Most Accurate Approach:  ${report.summary.mostAccurateApproach} (${report.summary.highestAccuracy.toFixed(2)}%)`,
  );
  lines.push("");
  lines.push(`Recommended Approach:    ${report.summary.recommendedApproach}`);
  lines.push(`Recommendation:          ${report.summary.recommendation}`);
  lines.push("");
  lines.push("=".repeat(80));

  return lines.join("\n");
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function main(): Promise<void> {
  console.log("=".repeat(80));
  console.log("ANALYZING 5 ITERATIONS EXPERIMENT RESULTS");
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

  if (fetchingResults.length === 0) {
    console.error(
      "ERROR: No fetching results found. Cannot use as ground truth.",
    );
    return;
  }

  // Generate report
  console.log("Calculating latency metrics...");
  console.log("Calculating accuracy metrics...");
  const report = generateReport(
    fetchingResults,
    chunkedResults,
    approximationResults,
  );

  // Save JSON report
  console.log(`\nSaving JSON report to: ${OUTPUT_FILE}`);
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(report, null, 2));

  // Generate and save text report
  const textReport = generateTextReport(report);
  console.log(`Saving text report to: ${OUTPUT_TXT_FILE}`);
  fs.writeFileSync(OUTPUT_TXT_FILE, textReport);

  // Display text report
  console.log("\n");
  console.log(textReport);

  console.log("\n✓ Analysis complete!");
  console.log(`  JSON report: ${OUTPUT_FILE}`);
  console.log(`  Text report: ${OUTPUT_TXT_FILE}`);
}

// Run the analysis
main().catch((error) => {
  console.error("Error during analysis:", error);
  process.exit(1);
});
