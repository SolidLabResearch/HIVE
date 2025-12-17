/**
 * Ground Truth Calculator and Accuracy Comparison Script
 *
 * This script:
 * 1. Loads raw sensor data from both streams (wearableX and smartphoneX)
 * 2. Calculates ground truth using the same windowing logic as the streaming queries
 * 3. Compares results from all three approaches (Approximation, Chunked, Fetching)
 * 4. Generates accuracy metrics (MAE, RMSE, percentage error)
 *
 * Note: Since the data is replayed with different wall-clock timestamps during experiments,
 * we compare based on the expected result values rather than timestamp matching.
 */

import * as fs from "fs";
import * as path from "path";

// ============================================================================
// Interfaces and Types
// ============================================================================

interface Observation {
  timestamp: Date;
  value: number;
  source: "wearableX" | "smartphoneX";
}

interface WindowResult {
  windowStart: number;
  windowEnd: number;
  maxValue: number;
  observations: number;
  source: string;
}

interface ApproachResult {
  timestamp: number;
  value: number;
}

interface AccuracyMetrics {
  approach: string;
  mae: number; // Mean Absolute Error
  rmse: number; // Root Mean Squared Error
  mape: number; // Mean Absolute Percentage Error
  maxError: number;
  minError: number;
  avgError: number;
  totalGroundTruthWindows: number;
  totalApproachResults: number;
  matchedResults: number;
  perfectMatches: number;
  accuracy: number; // Percentage of results within tolerance
}

// ============================================================================
// Configuration
// ============================================================================

const WINDOW_RANGE_MS = 120000; // 120 seconds
const WINDOW_STEP_MS = 60000; // 60 seconds
const DATA_PATH = process.env.DATA_PATH || "noisy_datasets/noise_0.5";
const VALUE_TOLERANCE = 0.01; // Tolerance for considering values as "perfect match"

// ============================================================================
// Data Loading Functions
// ============================================================================

/**
 * Parse N-Triples data file and extract observations
 */
function parseNTriplesData(
  filePath: string,
  source: "wearableX" | "smartphoneX",
): Observation[] {
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.split("\n").filter((line) => line.trim().length > 0);

  const observations: Observation[] = [];

  // Parse line by line - each observation is on a single line with all triples
  for (const line of lines) {
    let timestamp: Date | null = null;
    let value: number | null = null;

    // Extract timestamp
    const timestampMatch = line.match(/hasTimestamp>\s+"([^"]+)"\^\^/);
    if (timestampMatch) {
      timestamp = new Date(timestampMatch[1]);
    }

    // Extract value
    const valueMatch = line.match(/hasValue>\s+"([^"]+)"\^\^/);
    if (valueMatch) {
      value = parseFloat(valueMatch[1]);
    }

    if (timestamp && value !== null) {
      observations.push({ timestamp, value, source });
    }
  }

  return observations;
}

/**
 * Load all observations from both streams
 */
function loadAllObservations(): Observation[] {
  const wearablePath = path.join(
    "src/streamer/data",
    DATA_PATH,
    "wearable.acceleration.x/data.nt",
  );
  const smartphonePath = path.join(
    "src/streamer/data",
    DATA_PATH,
    "smartphone.acceleration.x/data.nt",
  );

  console.log("Loading wearable data from:", wearablePath);
  const wearableObs = parseNTriplesData(wearablePath, "wearableX");
  console.log(`Loaded ${wearableObs.length} wearable observations`);

  console.log("Loading smartphone data from:", smartphonePath);
  const smartphoneObs = parseNTriplesData(smartphonePath, "smartphoneX");
  console.log(`Loaded ${smartphoneObs.length} smartphone observations`);

  // Combine and sort by timestamp
  const allObs = [...wearableObs, ...smartphoneObs].sort(
    (a, b) => a.timestamp.getTime() - b.timestamp.getTime(),
  );

  console.log(`Total observations: ${allObs.length}`);
  return allObs;
}

// ============================================================================
// Ground Truth Calculation
// ============================================================================

/**
 * Simulate streaming replay at 4 Hz and calculate ground truth windows
 * This simulates what happens during the actual experiment
 */
function calculateGroundTruthWithSimulation(
  observations: Observation[],
): WindowResult[] {
  if (observations.length === 0) {
    return [];
  }

  const results: WindowResult[] = [];
  const REPLAY_FREQUENCY = 4; // Hz
  const MS_PER_OBSERVATION = 1000 / REPLAY_FREQUENCY;

  console.log("\nGround Truth Calculation (Simulating Replay at 4 Hz):");
  console.log(`Replay frequency: ${REPLAY_FREQUENCY} Hz`);
  console.log(`MS per observation: ${MS_PER_OBSERVATION}`);

  // Simulate replay - assign new timestamps based on 4 Hz replay
  const replayStart = 0; // Start at time 0
  const replayedObs = observations.map((obs, idx) => ({
    ...obs,
    replayTimestamp: replayStart + idx * MS_PER_OBSERVATION,
  }));

  const totalReplayTime = replayedObs[replayedObs.length - 1].replayTimestamp;
  console.log(`Total replay time: ${(totalReplayTime / 1000).toFixed(2)}s`);

  // Generate windows with STEP size
  let windowStart = 0;
  let windowNumber = 0;

  while (windowStart <= totalReplayTime) {
    const windowEnd = windowStart + WINDOW_RANGE_MS;

    // Find all observations within this window
    const windowObs = replayedObs.filter((obs) => {
      const t = obs.replayTimestamp;
      return t >= windowStart && t < windowEnd;
    });

    if (windowObs.length > 0) {
      // Calculate MAX value across both streams (as per the main query)
      const maxValue = Math.max(...windowObs.map((o) => o.value));

      windowNumber++;
      results.push({
        windowStart,
        windowEnd,
        maxValue,
        observations: windowObs.length,
        source: `window_${windowNumber}`,
      });
    }

    windowStart += WINDOW_STEP_MS;
  }

  console.log(`Generated ${results.length} ground truth windows`);
  return results;
}

/**
 * Calculate the single expected MAX value across all data
 * This is what the query should ultimately compute
 */
function calculateExpectedMaxValue(observations: Observation[]): number {
  if (observations.length === 0) {
    return 0;
  }
  return Math.max(...observations.map((o) => o.value));
}

// ============================================================================
// Approach Results Loading
// ============================================================================

/**
 * Load results from a CSV file
 */
function loadApproachResults(csvPath: string): ApproachResult[] {
  if (!fs.existsSync(csvPath)) {
    console.warn(`Warning: Results file not found: ${csvPath}`);
    return [];
  }

  const content = fs.readFileSync(csvPath, "utf-8");
  const lines = content.split("\n").filter((line) => line.trim().length > 0);

  const results: ApproachResult[] = [];

  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(",");
    if (parts.length >= 3) {
      const timestamp = parseInt(parts[1].trim());
      const value = parseFloat(parts[2].trim());

      // Filter out -9 values (these appear to be placeholder/error values)
      if (
        !isNaN(timestamp) &&
        !isNaN(value) &&
        value !== -9 &&
        value !== -9.0
      ) {
        results.push({ timestamp, value });
      }
    }
  }

  return results;
}

// ============================================================================
// Accuracy Calculation
// ============================================================================

/**
 * Calculate accuracy metrics by comparing approach results to ground truth
 */
function calculateAccuracy(
  approachName: string,
  groundTruth: WindowResult[],
  approachResults: ApproachResult[],
  expectedMaxValue: number,
): AccuracyMetrics {
  const errors: number[] = [];
  const percentageErrors: number[] = [];
  let perfectMatches = 0;
  let matchedResults = 0;

  console.log(`\n--- Analyzing ${approachName} ---`);
  console.log(`Ground truth windows: ${groundTruth.length}`);
  console.log(`Approach results: ${approachResults.length}`);
  console.log(`Expected MAX value: ${expectedMaxValue.toFixed(6)}`);

  // Deduplicate results (same value at slightly different timestamps)
  const uniqueValues = new Set<number>();
  approachResults.forEach((r) => uniqueValues.add(r.value));
  console.log(`Unique result values: ${uniqueValues.size}`);

  // For each result, calculate error against expected max
  for (const result of approachResults) {
    const error = Math.abs(result.value - expectedMaxValue);
    errors.push(error);

    if (expectedMaxValue !== 0) {
      const percentError = (error / Math.abs(expectedMaxValue)) * 100;
      percentageErrors.push(percentError);
    }

    if (error <= VALUE_TOLERANCE) {
      perfectMatches++;
    }

    matchedResults++;
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
  const accuracy =
    matchedResults > 0 ? (perfectMatches / matchedResults) * 100 : 0;

  return {
    approach: approachName,
    mae,
    rmse,
    mape,
    maxError,
    minError,
    avgError,
    totalGroundTruthWindows: groundTruth.length,
    totalApproachResults: approachResults.length,
    matchedResults,
    perfectMatches,
    accuracy,
  };
}

// ============================================================================
// Reporting Functions
// ============================================================================

/**
 * Display accuracy metrics in a formatted table
 */
function displayAccuracyReport(
  metrics: AccuracyMetrics[],
  expectedMaxValue: number,
  groundTruth: WindowResult[],
): void {
  console.log("\n");
  console.log("=".repeat(110));
  console.log("ACCURACY COMPARISON REPORT");
  console.log("=".repeat(110));
  console.log("\n");

  console.log("Query Configuration:");
  console.log(`  Window Range: ${WINDOW_RANGE_MS / 1000}s`);
  console.log(`  Window Step: ${WINDOW_STEP_MS / 1000}s`);
  console.log(`  Aggregation: MAX(?value) across both streams`);
  console.log(`  Data Path: ${DATA_PATH}`);
  console.log(`  Replay Frequency: 4 Hz`);
  console.log("\n");

  console.log("Ground Truth:");
  console.log(`  Expected MAX value: ${expectedMaxValue.toFixed(6)}`);
  console.log(`  Total windows: ${groundTruth.length}`);
  console.log("\n");

  // Sample windows
  console.log("Sample Ground Truth Windows:");
  groundTruth.slice(0, Math.min(5, groundTruth.length)).forEach((gt, idx) => {
    console.log(
      `  Window ${idx + 1}: [${(gt.windowStart / 1000).toFixed(1)}s - ${(gt.windowEnd / 1000).toFixed(1)}s] MAX = ${gt.maxValue.toFixed(6)} (${gt.observations} obs)`,
    );
  });
  console.log("\n");

  // Table header
  console.log(
    "┌────────────────────┬──────────┬──────────┬──────────┬───────────┬───────────┬──────────┬──────────────┐",
  );
  console.log(
    "│ Approach           │   MAE    │   RMSE   │   MAPE   │ Max Error │ Min Error │ Accuracy │ Perfect/Total│",
  );
  console.log(
    "├────────────────────┼──────────┼──────────┼──────────┼───────────┼───────────┼──────────┼──────────────┤",
  );

  for (const m of metrics) {
    console.log(
      `│ ${m.approach.padEnd(18)} │ ` +
        `${m.mae.toFixed(4).padStart(8)} │ ` +
        `${m.rmse.toFixed(4).padStart(8)} │ ` +
        `${m.mape.toFixed(2).padStart(7)}% │ ` +
        `${m.maxError.toFixed(4).padStart(9)} │ ` +
        `${m.minError.toFixed(4).padStart(9)} │ ` +
        `${m.accuracy.toFixed(1).padStart(7)}% │ ` +
        `${m.perfectMatches.toString().padStart(7)}/${m.totalApproachResults.toString().padEnd(5)} │`,
    );
  }

  console.log(
    "└────────────────────┴──────────┴──────────┴──────────┴───────────┴───────────┴──────────┴──────────────┘",
  );
  console.log("\n");

  // Detailed statistics
  console.log("Detailed Statistics:");
  console.log("-".repeat(110));
  for (const m of metrics) {
    console.log(`\n${m.approach}:`);
    console.log(`  Total results: ${m.totalApproachResults}`);
    console.log(
      `  Perfect matches (within ${VALUE_TOLERANCE}): ${m.perfectMatches}`,
    );
    console.log(`  Accuracy: ${m.accuracy.toFixed(2)}%`);
    console.log(`  Mean Absolute Error (MAE): ${m.mae.toFixed(6)}`);
    console.log(`  Root Mean Squared Error (RMSE): ${m.rmse.toFixed(6)}`);
    console.log(
      `  Mean Absolute Percentage Error (MAPE): ${m.mape.toFixed(2)}%`,
    );
    console.log(`  Max Error: ${m.maxError.toFixed(6)}`);
    console.log(`  Min Error: ${m.minError.toFixed(6)}`);
  }
  console.log("\n");

  // Summary and interpretation
  console.log("Interpretation:");
  console.log("-".repeat(110));
  const sortedByAccuracy = [...metrics].sort((a, b) => b.accuracy - a.accuracy);
  console.log(
    `\nMost Accurate: ${sortedByAccuracy[0].approach} (${sortedByAccuracy[0].accuracy.toFixed(1)}% perfect matches)`,
  );

  const sortedByMAE = [...metrics].sort((a, b) => a.mae - b.mae);
  console.log(
    `Lowest Error (MAE): ${sortedByMAE[0].approach} (${sortedByMAE[0].mae.toFixed(6)})`,
  );

  console.log("\nApproach Characteristics:");
  console.log(
    "  - Approximation: Uses MAX approximation, expects fewer but potentially less accurate results",
  );
  console.log(
    "  - Chunked: Aggregates chunks via MQTT, expects more frequent results",
  );
  console.log(
    "  - Fetching: Fetches subquery results via HTTP, expects moderate frequency results",
  );
  console.log("\n");
}

/**
 * Save detailed results to JSON file
 */
function saveDetailedResults(
  groundTruth: WindowResult[],
  metrics: AccuracyMetrics[],
  expectedMaxValue: number,
  outputPath: string,
): void {
  const report = {
    timestamp: new Date().toISOString(),
    configuration: {
      windowRangeMs: WINDOW_RANGE_MS,
      windowStepMs: WINDOW_STEP_MS,
      dataPath: DATA_PATH,
      valueTolerance: VALUE_TOLERANCE,
      replayFrequencyHz: 4,
    },
    expectedMaxValue,
    groundTruth: groundTruth.map((gt) => ({
      windowStartMs: gt.windowStart,
      windowEndMs: gt.windowEnd,
      windowStartS: (gt.windowStart / 1000).toFixed(2),
      windowEndS: (gt.windowEnd / 1000).toFixed(2),
      maxValue: gt.maxValue,
      observations: gt.observations,
    })),
    accuracyMetrics: metrics,
    ranking: {
      byAccuracy: [...metrics]
        .sort((a, b) => b.accuracy - a.accuracy)
        .map((m) => ({
          approach: m.approach,
          accuracy: m.accuracy,
        })),
      byMAE: [...metrics]
        .sort((a, b) => a.mae - b.mae)
        .map((m) => ({
          approach: m.approach,
          mae: m.mae,
        })),
      byRMSE: [...metrics]
        .sort((a, b) => a.rmse - b.rmse)
        .map((m) => ({
          approach: m.approach,
          rmse: m.rmse,
        })),
    },
  };

  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2));
  console.log(`Detailed results saved to: ${outputPath}`);
}

// ============================================================================
// Main Execution
// ============================================================================

async function main() {
  console.log("Starting Ground Truth Calculation and Accuracy Analysis...\n");

  try {
    // Step 1: Load raw observations
    console.log("Step 1: Loading raw observations...");
    const observations = loadAllObservations();

    // Step 2: Calculate expected max value
    console.log("\nStep 2: Calculating expected MAX value...");
    const expectedMaxValue = calculateExpectedMaxValue(observations);
    console.log(
      `Expected MAX value across all data: ${expectedMaxValue.toFixed(6)}`,
    );

    // Step 3: Calculate ground truth windows (simulating replay)
    console.log(
      "\nStep 3: Calculating ground truth windows (simulating 4 Hz replay)...",
    );
    const groundTruth = calculateGroundTruthWithSimulation(observations);

    // Step 4: Load approach results
    console.log("\nStep 4: Loading approach results...");
    const approximationResults = loadApproachResults(
      "results/approximation_results.csv",
    );
    const chunkedResults = loadApproachResults(
      "results/chunked_query_results.csv",
    );
    const fetchingResults = loadApproachResults(
      "results/fetching_client_side_results.csv",
    );

    console.log(`Approximation results loaded: ${approximationResults.length}`);
    console.log(`Chunked results loaded: ${chunkedResults.length}`);
    console.log(`Fetching results loaded: ${fetchingResults.length}`);

    // Step 5: Calculate accuracy for each approach
    console.log("\nStep 5: Calculating accuracy metrics...");
    const metrics: AccuracyMetrics[] = [
      calculateAccuracy(
        "Approximation",
        groundTruth,
        approximationResults,
        expectedMaxValue,
      ),
      calculateAccuracy(
        "Chunked",
        groundTruth,
        chunkedResults,
        expectedMaxValue,
      ),
      calculateAccuracy(
        "Fetching",
        groundTruth,
        fetchingResults,
        expectedMaxValue,
      ),
    ];

    // Step 6: Display and save results
    console.log("\nStep 6: Generating report...");
    displayAccuracyReport(metrics, expectedMaxValue, groundTruth);

    const timestamp = new Date().toISOString().replace(/:/g, "-").split(".")[0];
    const outputPath = `results/accuracy-report-${timestamp}.json`;
    saveDetailedResults(groundTruth, metrics, expectedMaxValue, outputPath);

    console.log("Analysis complete!");
  } catch (error) {
    console.error("Error during analysis:", error);
    process.exit(1);
  }
}

// Run the script
main().catch(console.error);
