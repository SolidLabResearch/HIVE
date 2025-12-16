#!/usr/bin/env ts-node

/**
 * Cleanup Logs Script (TypeScript version for cross-platform compatibility)
 *
 * Removes all log files, CSV outputs, and experimental results for a fresh start.
 * This script cleans up:
 * - CSV log files (orchestrator logs, resource usage)
 * - Results directories
 * - Unified log directories
 * - Publisher logs
 * - Temporary experiment files
 */

import * as fs from "fs";
import * as path from "path";

// Color codes for terminal output
const colors = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
};

/**
 * Simple glob matching for file patterns
 */
function matchesPattern(filename: string, pattern: string): boolean {
  // Convert glob pattern to regex
  const regexPattern = pattern
    .replace(/\./g, "\\.")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".");
  const regex = new RegExp(`^${regexPattern}$`);
  return regex.test(filename);
}

/**
 * Find files matching a pattern in a directory
 */
function findMatchingFiles(dir: string, pattern: string): string[] {
  const matches: string[] = [];

  try {
    if (!fs.existsSync(dir)) {
      return matches;
    }

    const files = fs.readdirSync(dir);
    for (const file of files) {
      if (matchesPattern(file, pattern)) {
        matches.push(path.join(dir, file));
      }
    }
  } catch (error) {
    // Ignore errors (e.g., permission denied)
  }

  return matches;
}

/**
 * Removes files or directories matching the given patterns
 */
function removeItems(description: string, patterns: string[]): void {
  console.log(`${colors.yellow}Checking ${description}...${colors.reset}`);

  let found = 0;
  const projectRoot = process.cwd();

  for (const pattern of patterns) {
    // Check if it's a pattern or direct path
    if (pattern.includes("*")) {
      // Extract directory and filename pattern
      const dir = path.dirname(pattern);
      const filePattern = path.basename(pattern);
      const searchDir = dir === "." ? projectRoot : path.join(projectRoot, dir);

      const matches = findMatchingFiles(searchDir, filePattern);
      for (const match of matches) {
        found++;
        const relativePath = path.relative(projectRoot, match);
        console.log(
          `  ${colors.red}✗${colors.reset} Removing: ${relativePath}`,
        );
        fs.rmSync(match, { recursive: true, force: true });
      }
    } else {
      // Direct path
      const fullPath = path.join(projectRoot, pattern);
      if (fs.existsSync(fullPath)) {
        found++;
        console.log(`  ${colors.red}✗${colors.reset} Removing: ${pattern}`);
        fs.rmSync(fullPath, { recursive: true, force: true });
      }
    }
  }

  if (found === 0) {
    console.log(`  ${colors.green}✓${colors.reset} No files found`);
  }
  console.log("");
}

/**
 * Main cleanup function
 */
function cleanupLogs(): void {
  console.log(`${colors.blue}================================${colors.reset}`);
  console.log(
    `${colors.blue}Streaming Query Hive - Log Cleanup${colors.reset}`,
  );
  console.log(`${colors.blue}================================${colors.reset}`);
  console.log("");

  // 1. Clean CSV logs in project root
  console.log(`${colors.blue}[1/8] CSV Log Files${colors.reset}`);
  removeItems("CSV log files", [
    "approximation_approach_log.csv",
    "chunked_query_approach_log.csv",
    "streaming_query_chunk_aggregator_log.csv",
    "naive_approximation_approach_log.csv",
    "fetching_client_side_log.csv",
  ]);

  // 2. Clean resource usage logs
  console.log(`${colors.blue}[2/8] Resource Usage Logs${colors.reset}`);
  removeItems("resource usage logs", [
    "approximation_approach_resource_usage.csv",
    "chunked_query_approach_resource_log.csv",
    "fetching_client_side_resource_usage.csv",
  ]);

  // 3. Clean replayer logs
  console.log(`${colors.blue}[3/8] Publisher/Replayer Logs${colors.reset}`);
  removeItems("publisher logs", [
    "replayer-log.csv",
    "publisher-log.csv",
    "streamer-log.csv",
  ]);

  // 4. Clean results directories and files
  console.log(`${colors.blue}[4/8] Results Directories${colors.reset}`);
  removeItems("results directories", [
    "results/chunked_query_results.csv",
    "results/approximation_results.csv",
    "results/fetching_client_side_results.csv",
    "results/multi-run-verification-*.json",
    "results/pattern-test-*.json",
    "results/pattern-test-*.csv",
    "results/frequency-experiments",
  ]);

  // 5. Clean unified log directories
  console.log(`${colors.blue}[5/8] Unified Log Directories${colors.reset}`);
  removeItems("unified logs", [
    "logs/approximation",
    "logs/fetching",
    "logs/chunked",
  ]);

  // 6. Clean experiment-specific logs
  console.log(`${colors.blue}[6/8] Experiment Logs${colors.reset}`);
  removeItems("experiment logs", [
    "experiment-*.log",
    "experiment-*.json",
    "benchmark-*.json",
    "test-*.log",
  ]);

  // 7. Clean temporary files
  console.log(`${colors.blue}[7/8] Temporary Files${colors.reset}`);
  removeItems("temporary files", [
    "*.tmp",
    "*.temp",
    ".experiment-state",
    "pid-*.txt",
  ]);

  // 8. Clean any orphaned result files
  console.log(`${colors.blue}[8/8] Orphaned Result Files${colors.reset}`);
  removeItems("orphaned result files", [
    "output-*.csv",
    "metrics-*.json",
    "summary-*.txt",
  ]);

  // Summary
  console.log(`${colors.green}================================${colors.reset}`);
  console.log(`${colors.green}Cleanup Complete!${colors.reset}`);
  console.log(`${colors.green}================================${colors.reset}`);
  console.log("");
  console.log("All log files and experimental results have been removed.");
  console.log("You can now run experiments with a clean slate.");
  console.log("");
  console.log(`${colors.blue}Next steps:${colors.reset}`);
  console.log(
    `  1. Build the project: ${colors.yellow}npm run build${colors.reset}`,
  );
  console.log(
    `  2. Test MQTT: ${colors.yellow}npm run experiment:test-mqtt${colors.reset}`,
  );
  console.log(
    `  3. Run experiment: ${colors.yellow}npm run experiment:5-iterations${colors.reset}`,
  );
  console.log("");
}

// Run cleanup if executed directly
if (require.main === module) {
  try {
    cleanupLogs();
    process.exit(0);
  } catch (error) {
    console.error(`${colors.red}Error during cleanup:${colors.reset}`, error);
    process.exit(1);
  }
}

export { cleanupLogs };
