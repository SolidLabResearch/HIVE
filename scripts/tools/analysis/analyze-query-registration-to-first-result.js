#!/usr/bin/env node

/**
 * Query Registration to First Result Analyzer
 *
 * This script analyzes the time difference between query registration and first result
 * for all three approaches across different frequencies, similar to pattern analysis.
 */

const fs = require("fs");
const path = require("path");

console.log("Analyzing Query Registration to First Result Latency...\n");

const LOGS_BASE_DIR = path.join(__dirname, "../../experiments/logs");
const APPROACHES = [
  "fetching-client-side",
  "streaming-query-hive",
  "approximation-approach",
];
const FREQUENCIES = ["4Hz", "8Hz", "16Hz", "32Hz", "64Hz", "128Hz"];

/**
 * Extract query registration timestamp from log
 */
function extractQueryRegistrationTimestamp(logPath, approach) {
  if (!fs.existsSync(logPath)) return null;

  const content = fs.readFileSync(logPath, "utf8");
  const lines = content.split("\n");

  const registrationPatterns = {
    "fetching-client-side": [
      "fetching_query_registered",
      "Query registered successfully",
      "Registering query",
    ],
    "streaming-query-hive": [
      "Registering main streaming query hive query",
      "Starting streaming query hive execution",
      "Sub-queries added",
    ],
    "approximation-approach": [
      "Registered query:",
      "Sub-queries added:",
      "Starting approximation approach",
    ],
  };

  const patterns = registrationPatterns[approach] || [];

  for (const line of lines) {
    if (line.includes("timestamp,message")) continue; // Skip header

    const parts = line.split(",");
    if (parts.length < 2) continue;

    const timestamp = parseInt(parts[0]);
    const message = parts[1] ? parts[1].replace(/"/g, "") : "";

    if (isNaN(timestamp)) continue;

    for (const pattern of patterns) {
      if (message.includes(pattern)) {
        return { timestamp, message: message.substring(0, 100) };
      }
    }
  }

  return null;
}

/**
 * Extract first result timestamp from log
 */
function extractFirstResultTimestamp(logPath, approach) {
  if (!fs.existsSync(logPath)) return null;

  const content = fs.readFileSync(logPath, "utf8");
  const lines = content.split("\n");

  const resultPatterns = {
    "fetching-client-side": [
      "RStream result generated:",
      "Processing valid result:",
      "Successfully published result:",
    ],
    "streaming-query-hive": [
      "Successfully published result:",
      "Query result received:",
      "Result generated:",
    ],
    "approximation-approach": [
      "Successfully published unified cross-sensor average:",
      "Updated global latest value",
      "Publishing approximation result:",
    ],
  };

  const patterns = resultPatterns[approach] || [];

  for (const line of lines) {
    if (line.includes("timestamp,message")) continue; // Skip header

    const parts = line.split(",");
    if (parts.length < 2) continue;

    const timestamp = parseInt(parts[0]);
    const message = parts[1] ? parts[1].replace(/"/g, "") : "";

    if (isNaN(timestamp)) continue;

    for (const pattern of patterns) {
      if (message.includes(pattern)) {
        return { timestamp, message: message.substring(0, 100) };
      }
    }
  }

  return null;
}

/**
 * Analyze memory and CPU usage from resource file
 */
function analyzeResourceUsage(resourcePath) {
  if (!fs.existsSync(resourcePath)) return null;

  const content = fs.readFileSync(resourcePath, "utf8");
  const lines = content.split("\n");

  let cpuUser = [];
  let cpuSystem = [];
  let memoryRSS = [];
  let memoryHeap = [];

  for (let i = 1; i < lines.length; i++) {
    // Skip header
    const parts = lines[i].split(",");
    if (parts.length < 8) continue;

    try {
      cpuUser.push(parseFloat(parts[1]));
      cpuSystem.push(parseFloat(parts[2]));
      memoryRSS.push(parseInt(parts[3]) / (1024 * 1024)); // Convert to MB
      memoryHeap.push(parseFloat(parts[6])); // heapUsedMB
    } catch (e) {
      // Skip invalid lines
    }
  }

  if (cpuUser.length === 0) return null;

  return {
    cpu: {
      avgUser: cpuUser.reduce((a, b) => a + b, 0) / cpuUser.length,
      avgSystem: cpuSystem.reduce((a, b) => a + b, 0) / cpuSystem.length,
      maxUser: Math.max(...cpuUser),
      maxSystem: Math.max(...cpuSystem),
    },
    memory: {
      avgRSS: memoryRSS.reduce((a, b) => a + b, 0) / memoryRSS.length,
      avgHeap: memoryHeap.reduce((a, b) => a + b, 0) / memoryHeap.length,
      maxRSS: Math.max(...memoryRSS),
      maxHeap: Math.max(...memoryHeap),
    },
  };
}

/**
 * Get log file paths for an approach
 */
function getLogPaths(approach, frequency, iteration = "iteration1") {
  const basePath = path.join(
    LOGS_BASE_DIR,
    approach,
    `${frequency}_combined`,
    iteration,
  );

  const logFilePatterns = {
    "fetching-client-side": "fetching_client_side_log.csv",
    "streaming-query-hive": "streaming_query_hive_log.csv",
    "approximation-approach": "approximation_approach_log.csv",
  };

  const resourceFilePatterns = {
    "fetching-client-side": "fetching_client_side_resource_usage.csv",
    "streaming-query-hive": "streaming_query_hive_resource_usage.csv",
    "approximation-approach": "approximation_approach_resource_usage.csv",
  };

  return {
    mainLog: path.join(basePath, logFilePatterns[approach]),
    resourceLog: path.join(basePath, resourceFilePatterns[approach]),
  };
}

/**
 * Analyze a single approach-frequency combination
 */
function analyzeApproachFrequency(approach, frequency) {
  console.log(`  Analyzing ${approach} @ ${frequency}...`);

  const results = [];

  // Analyze all available iterations
  for (let iteration = 1; iteration <= 3; iteration++) {
    const iterationName = `iteration${iteration}`;
    const paths = getLogPaths(approach, frequency, iterationName);

    if (!fs.existsSync(paths.mainLog)) {
      console.log(`    ${iterationName}: Log file not found`);
      continue;
    }

    const registrationInfo = extractQueryRegistrationTimestamp(
      paths.mainLog,
      approach,
    );
    const firstResultInfo = extractFirstResultTimestamp(
      paths.mainLog,
      approach,
    );
    const resourceUsage = analyzeResourceUsage(paths.resourceLog);

    if (registrationInfo && firstResultInfo) {
      const latency = firstResultInfo.timestamp - registrationInfo.timestamp;

      results.push({
        iteration: iteration,
        registrationTimestamp: registrationInfo.timestamp,
        firstResultTimestamp: firstResultInfo.timestamp,
        latencyMs: latency,
        registrationMessage: registrationInfo.message,
        firstResultMessage: firstResultInfo.message,
        resourceUsage: resourceUsage,
      });

      console.log(`    ${iterationName}: ${latency}ms latency`);
    } else {
      console.log(
        `    ${iterationName}: Missing data (reg: ${!!registrationInfo}, result: ${!!firstResultInfo})`,
      );
    }
  }

  if (results.length === 0) {
    return { error: "No valid data found" };
  }

  // Calculate statistics
  const latencies = results.map((r) => r.latencyMs);
  const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const minLatency = Math.min(...latencies);
  const maxLatency = Math.max(...latencies);

  // Average resource usage across iterations
  const validResourceUsage = results
    .filter((r) => r.resourceUsage)
    .map((r) => r.resourceUsage);
  let avgResourceUsage = null;

  if (validResourceUsage.length > 0) {
    avgResourceUsage = {
      cpu: {
        avgUser:
          validResourceUsage.reduce((sum, r) => sum + r.cpu.avgUser, 0) /
          validResourceUsage.length,
        avgSystem:
          validResourceUsage.reduce((sum, r) => sum + r.cpu.avgSystem, 0) /
          validResourceUsage.length,
        maxUser: Math.max(...validResourceUsage.map((r) => r.cpu.maxUser)),
        maxSystem: Math.max(...validResourceUsage.map((r) => r.cpu.maxSystem)),
      },
      memory: {
        avgRSS:
          validResourceUsage.reduce((sum, r) => sum + r.memory.avgRSS, 0) /
          validResourceUsage.length,
        avgHeap:
          validResourceUsage.reduce((sum, r) => sum + r.memory.avgHeap, 0) /
          validResourceUsage.length,
        maxRSS: Math.max(...validResourceUsage.map((r) => r.memory.maxRSS)),
        maxHeap: Math.max(...validResourceUsage.map((r) => r.memory.maxHeap)),
      },
    };
  }

  return {
    iterations: results.length,
    avgLatencyMs: avgLatency,
    minLatencyMs: minLatency,
    maxLatencyMs: maxLatency,
    latencies: latencies,
    avgResourceUsage: avgResourceUsage,
    detailedResults: results,
  };
}

/**
 * Main analysis function
 */
function runAnalysis() {
  const allResults = {};

  console.log("=".repeat(100));
  console.log("QUERY REGISTRATION TO FIRST RESULT LATENCY ANALYSIS");
  console.log("=".repeat(100));

  for (const approach of APPROACHES) {
    console.log(
      `\nAnalyzing ${approach.toUpperCase().replace(/-/g, " ")} approach:`,
    );
    console.log("-".repeat(60));

    allResults[approach] = {};

    for (const frequency of FREQUENCIES) {
      const result = analyzeApproachFrequency(approach, frequency);
      allResults[approach][frequency] = result;
    }
  }

  return allResults;
}

/**
 * Generate summary table
 */
function generateSummaryTable(results) {
  console.log("\n" + "=".repeat(120));
  console.log("LATENCY SUMMARY TABLE");
  console.log("=".repeat(120));

  console.log(
    "┌─────────────────────────┬──────────┬─────────────┬─────────────┬─────────────┬──────────┬─────────────┐",
  );
  console.log(
    "│ Approach                │ Frequency│ Avg Latency │ Min Latency │ Max Latency │ CPU Avg  │ Memory Avg  │",
  );
  console.log(
    "│                         │          │ (ms)        │ (ms)        │ (ms)        │ (%)      │ (MB)        │",
  );
  console.log(
    "├─────────────────────────┼──────────┼─────────────┼─────────────┼─────────────┼──────────┼─────────────┤",
  );

  for (const approach of APPROACHES) {
    if (!results[approach]) continue;

    for (const frequency of FREQUENCIES) {
      const data = results[approach][frequency];
      if (!data || data.error) continue;

      const approachName = approach
        .replace(/-/g, " ")
        .substring(0, 23)
        .padEnd(23);
      const freq = frequency.padEnd(8);
      const avgLat = data.avgLatencyMs.toFixed(1).padStart(11);
      const minLat = data.minLatencyMs.toFixed(1).padStart(11);
      const maxLat = data.maxLatencyMs.toFixed(1).padStart(11);

      let cpuAvg = "N/A".padStart(8);
      let memAvg = "N/A".padStart(11);

      if (data.avgResourceUsage) {
        const totalCpu =
          data.avgResourceUsage.cpu.avgUser +
          data.avgResourceUsage.cpu.avgSystem;
        cpuAvg = totalCpu.toFixed(1).padStart(8);
        memAvg = data.avgResourceUsage.memory.avgRSS.toFixed(1).padStart(11);
      }

      console.log(
        `│ ${approachName} │ ${freq} │ ${avgLat} │ ${minLat} │ ${maxLat} │ ${cpuAvg} │ ${memAvg} │`,
      );
    }
  }

  console.log(
    "└─────────────────────────┴──────────┴─────────────┴─────────────┴─────────────┴──────────┴─────────────┘",
  );
}

/**
 * Generate frequency comparison
 */
function generateFrequencyComparison(results) {
  console.log("\n" + "=".repeat(100));
  console.log("FREQUENCY-BASED PERFORMANCE COMPARISON");
  console.log("=".repeat(100));

  for (const frequency of FREQUENCIES) {
    console.log(`\n${frequency} FREQUENCY COMPARISON:`);
    console.log("-".repeat(50));

    const frequencyResults = [];

    for (const approach of APPROACHES) {
      if (
        results[approach] &&
        results[approach][frequency] &&
        !results[approach][frequency].error
      ) {
        const data = results[approach][frequency];
        frequencyResults.push({
          approach: approach,
          avgLatency: data.avgLatencyMs,
          iterations: data.iterations,
          cpu: data.avgResourceUsage
            ? data.avgResourceUsage.cpu.avgUser +
              data.avgResourceUsage.cpu.avgSystem
            : null,
          memory: data.avgResourceUsage
            ? data.avgResourceUsage.memory.avgRSS
            : null,
        });
      }
    }

    // Sort by latency (best performance first)
    frequencyResults.sort((a, b) => a.avgLatency - b.avgLatency);

    frequencyResults.forEach((result, index) => {
      const rank = index + 1;
      const name = result.approach.replace(/-/g, " ");
      const cpu = result.cpu ? result.cpu.toFixed(1) + "%" : "N/A";
      const memory = result.memory ? result.memory.toFixed(1) + "MB" : "N/A";

      console.log(
        `  ${rank}. ${name}: ${result.avgLatency.toFixed(1)}ms (${result.iterations} iterations, CPU: ${cpu}, Memory: ${memory})`,
      );
    });
  }
}

/**
 * Generate best/worst performers summary
 */
function generatePerformanceSummary(results) {
  console.log("\n" + "=".repeat(100));
  console.log("OVERALL PERFORMANCE SUMMARY");
  console.log("=".repeat(100));

  const allCombinations = [];

  for (const approach of APPROACHES) {
    if (!results[approach]) continue;

    for (const frequency of FREQUENCIES) {
      const data = results[approach][frequency];
      if (!data || data.error) continue;

      allCombinations.push({
        approach: approach,
        frequency: frequency,
        avgLatency: data.avgLatencyMs,
        iterations: data.iterations,
        cpu: data.avgResourceUsage
          ? data.avgResourceUsage.cpu.avgUser +
            data.avgResourceUsage.cpu.avgSystem
          : null,
        memory: data.avgResourceUsage
          ? data.avgResourceUsage.memory.avgRSS
          : null,
      });
    }
  }

  // Sort by latency
  allCombinations.sort((a, b) => a.avgLatency - b.avgLatency);

  console.log("\nBEST PERFORMERS (Lowest Latency):");
  allCombinations.slice(0, 5).forEach((combo, index) => {
    const name = `${combo.approach.replace(/-/g, " ")} @ ${combo.frequency}`;
    const cpu = combo.cpu ? combo.cpu.toFixed(1) + "%" : "N/A";
    const memory = combo.memory ? combo.memory.toFixed(1) + "MB" : "N/A";
    console.log(
      `  ${index + 1}. ${name}: ${combo.avgLatency.toFixed(1)}ms (CPU: ${cpu}, Memory: ${memory})`,
    );
  });

  console.log("\nWORST PERFORMERS (Highest Latency):");
  allCombinations
    .slice(-5)
    .reverse()
    .forEach((combo, index) => {
      const name = `${combo.approach.replace(/-/g, " ")} @ ${combo.frequency}`;
      const cpu = combo.cpu ? combo.cpu.toFixed(1) + "%" : "N/A";
      const memory = combo.memory ? combo.memory.toFixed(1) + "MB" : "N/A";
      console.log(
        `  ${index + 1}. ${name}: ${combo.avgLatency.toFixed(1)}ms (CPU: ${cpu}, Memory: ${memory})`,
      );
    });

  // Calculate approach averages
  console.log("\nAPPROACH AVERAGES:");
  for (const approach of APPROACHES) {
    const approachData = allCombinations.filter((c) => c.approach === approach);
    if (approachData.length === 0) continue;

    const avgLatency =
      approachData.reduce((sum, c) => sum + c.avgLatency, 0) /
      approachData.length;
    const validCpu = approachData.filter((c) => c.cpu !== null);
    const validMemory = approachData.filter((c) => c.memory !== null);

    const avgCpu =
      validCpu.length > 0
        ? validCpu.reduce((sum, c) => sum + c.cpu, 0) / validCpu.length
        : null;
    const avgMemory =
      validMemory.length > 0
        ? validMemory.reduce((sum, c) => sum + c.memory, 0) / validMemory.length
        : null;

    const name = approach.replace(/-/g, " ");
    const cpu = avgCpu ? avgCpu.toFixed(1) + "%" : "N/A";
    const memory = avgMemory ? avgMemory.toFixed(1) + "MB" : "N/A";

    console.log(
      `  ${name}: ${avgLatency.toFixed(1)}ms average (CPU: ${cpu}, Memory: ${memory})`,
    );
  }
}

// Run the analysis
const results = runAnalysis();

// Generate reports
generateSummaryTable(results);
generateFrequencyComparison(results);
generatePerformanceSummary(results);

// Export results
const outputFile = "query-registration-to-first-result-analysis.json";
fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));

console.log("\n" + "=".repeat(100));
console.log(`Analysis complete! Detailed results exported to: ${outputFile}`);
console.log("=".repeat(100));
