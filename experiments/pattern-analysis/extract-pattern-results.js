#!/usr/bin/env node

/**
 * Extract Pattern Results from Logs
 *
 * Extracts query results and metadata from pattern comparison experiment logs.
 * Supports fetching, approximation, and chunked approaches.
 *
 * Usage:
 *   node extract-pattern-results.js <approach> <pattern_name> <log_dir>
 *
 * Example:
 *   node extract-pattern-results.js fetching exponential_growth_rate_1 ./logs/pattern-comparison/fetching/exponential_growth_rate_1/iteration1
 */

const fs = require("fs");
const path = require("path");

class PatternResultExtractor {
  constructor(approach, patternName, logDir) {
    this.approach = approach;
    this.patternName = patternName;
    this.logDir = logDir;

    // Determine log file names based on approach
    const logFileMap = {
      fetching: "fetching_orchestrator.log",
      approximation: "approximation_approach_log.csv",
      chunked: "streaming_query_chunk_aggregator_log.csv",
    };

    const latencyLogMap = {
      fetching: "fetching_latency_log.csv",
      approximation: "approximation_latency_log.csv",
      chunked: "chunked_latency_log.csv",
    };

    this.logFile = path.join(logDir, logFileMap[approach]);
    this.latencyFile = path.join(logDir, latencyLogMap[approach]);
    this.outputFile = path.join(logDir, `${approach}_results.csv`);
    this.metadataFile = path.join(logDir, `${approach}_metadata.json`);
  }

  extractResults() {
    console.log(`Extracting results from: ${this.logFile}`);

    if (!fs.existsSync(this.logFile)) {
      console.error(`Log file not found: ${this.logFile}`);
      return null;
    }

    const content = fs.readFileSync(this.logFile, "utf8");
    const results = [];
    let firstResultTime = null;
    let queryRegisteredTime = null;

    // Try to extract metadata from latency log first
    if (fs.existsSync(this.latencyFile)) {
      try {
        console.log(`Reading latency log: ${this.latencyFile}`);
        const latencyContent = fs.readFileSync(this.latencyFile, "utf8");
        const latencyLines = latencyContent.trim().split("\n");

        // Skip header, read first data line
        if (latencyLines.length > 1) {
          const parts = latencyLines[1].split(",");
          // CSV format: window_number,query_registered_at,first_data_received_at,...
          if (parts.length >= 2) {
            queryRegisteredTime = parseInt(parts[1]);
            console.log(
              `Found query registration time from latency log: ${queryRegisteredTime}`,
            );
          }

          // Result emitted at is usually index 5 or 6 depending on approach
          // Fetching/Approx: index 5 (result_emitted_at)
          // Chunked: might be different, let's check
          // Chunked headers: window_number,query_registered_at,first_data_received_at,expected_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at
          // So result_emitted_at is index 6 for chunked

          let resultTimeIndex = 5;
          if (this.approach === "chunked") resultTimeIndex = 6;

          if (parts.length > resultTimeIndex) {
            firstResultTime = parseInt(parts[resultTimeIndex]);
            console.log(
              `Found first result time from latency log: ${firstResultTime}`,
            );
          }
        }
      } catch (e) {
        console.warn(`Failed to parse latency log: ${e.message}`);
      }
    }

    // Fetching approach
    if (this.approach === "fetching") {
      // Extract query registration time
      const registrationMatch = content.match(
        /LOG:\s*(\d+)\s*-\s*fetching_query_registered/,
      );
      if (registrationMatch) {
        queryRegisteredTime = parseInt(registrationMatch[1]);
        console.log(`Found query registration time: ${queryRegisteredTime}`);
      }

      // Parse RStream events
      const lines = content.split("\n");
      for (const line of lines) {
        if (line.includes("DEBUG: RStream event received:")) {
          try {
            const jsonStart = line.indexOf("{");
            if (jsonStart === -1) continue;

            const jsonStr = line.substring(jsonStart);
            const data = JSON.parse(jsonStr);

            if (data.bindings && data.bindings.entries) {
              const avgValue = data.bindings.entries.avgValue;

              if (avgValue && avgValue.value) {
                const resultValue = parseFloat(avgValue.value);
                const timestamp = data.timestamp_to || Date.now();

                if (!firstResultTime) {
                  firstResultTime = timestamp;
                }

                results.push({
                  timestamp,
                  resultValue,
                  windowNumber: results.length + 1,
                });

                console.log(
                  `  Window ${results.length}: ${resultValue.toFixed(6)} at ${timestamp}`,
                );
              }
            }
          } catch (e) {
            continue;
          }
        }
      }
    }
    // Approximation approach
    else if (this.approach === "approximation") {
      const lines = content.split("\n");

      // Extract query registration time from latency logs
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        const latencyMatch = line.match(
          /From query registration:\s*(-?\d+)ms\s*\(expected close:\s*(\d+),\s*result:\s*(\d+)\)/,
        );

        if (latencyMatch) {
          const expectedCloseTime = parseInt(latencyMatch[2]);
          const resultTimestamp = parseInt(latencyMatch[3]);

          // For first window: expectedClose = queryRegisteredTime + RANGE
          const windowRangeMs = 120000;
          const calculatedRegistrationTime = expectedCloseTime - windowRangeMs;

          if (!queryRegisteredTime) {
            queryRegisteredTime = calculatedRegistrationTime;
            console.log(
              `Found query registration time: ${queryRegisteredTime}`,
            );
          }

          if (!firstResultTime) {
            firstResultTime = resultTimestamp;
            console.log(`Found first result time: ${firstResultTime}`);
          }
        }

        // Extract published results
        if (line.includes("Successfully published unified cross-sensor")) {
          const match = line.match(
            /Successfully published unified cross-sensor \w+:\s*([\d.]+)/,
          );
          if (match) {
            const resultValue = parseFloat(match[1]);

            let timestamp = firstResultTime || Date.now();

            // Search backwards for result timestamp
            for (let j = Math.max(0, i - 20); j < i; j++) {
              const tsMatch = lines[j].match(/result:\s*(\d{13})/);
              if (tsMatch) {
                timestamp = parseInt(tsMatch[1]);
              }
            }

            results.push({
              timestamp,
              resultValue,
              windowNumber: results.length + 1,
            });

            console.log(
              `  Window ${results.length}: ${resultValue.toFixed(6)} at ${timestamp}`,
            );
          }
        }
      }

      // Fallback estimation if no registration time found
      if (!queryRegisteredTime && results.length > 0 && firstResultTime) {
        queryRegisteredTime = firstResultTime - 60000;
        console.log(
          `Estimated query registration time: ${queryRegisteredTime}`,
        );
      }
    }
    // Chunked approach
    else if (this.approach === "chunked") {
      const lines = content.split("\n");

      // Extract query registration time from latency logs
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        const latencyMatch = line.match(
          /From query registration:\s*(-?\d+)ms\s*\(expected close:\s*(\d+),\s*result:\s*(\d+)\)/,
        );

        if (latencyMatch) {
          const expectedCloseTime = parseInt(latencyMatch[2]);
          const resultTimestamp = parseInt(latencyMatch[3]);

          const windowRangeMs = 120000;
          const calculatedRegistrationTime = expectedCloseTime - windowRangeMs;

          if (!queryRegisteredTime) {
            queryRegisteredTime = calculatedRegistrationTime;
            console.log(
              `Found query registration time: ${queryRegisteredTime}`,
            );
          }

          if (!firstResultTime) {
            firstResultTime = resultTimestamp;
            console.log(`Found first result time: ${firstResultTime}`);
          }
        }

        // Extract published results
        if (line.includes("Successfully published unified cross-sensor")) {
          const match = line.match(
            /Successfully published unified cross-sensor \w+:\s*([\d.]+)/,
          );
          if (match) {
            const resultValue = parseFloat(match[1]);

            let timestamp = firstResultTime || Date.now();

            for (let j = Math.max(0, i - 20); j < i; j++) {
              const tsMatch = lines[j].match(/result:\s*(\d{13})/);
              if (tsMatch) {
                timestamp = parseInt(tsMatch[1]);
              }
            }

            results.push({
              timestamp,
              resultValue,
              windowNumber: results.length + 1,
            });

            console.log(
              `  Window ${results.length}: ${resultValue.toFixed(6)} at ${timestamp}`,
            );
          }
        }
      }

      // Fallback estimation
      if (!queryRegisteredTime && results.length > 0 && firstResultTime) {
        queryRegisteredTime = firstResultTime - 60000;
        console.log(
          `Estimated query registration time: ${queryRegisteredTime}`,
        );
      }
    }

    console.log(`\nExtracted ${results.length} results`);

    if (results.length === 0) {
      console.warn("No results found in log file");
      return null;
    }

    return {
      results,
      queryRegisteredTime,
      firstResultTime,
    };
  }

  saveResults(extractedData) {
    if (
      !extractedData ||
      !extractedData.results ||
      extractedData.results.length === 0
    ) {
      console.error("No data to save");
      return false;
    }

    const { results, queryRegisteredTime, firstResultTime } = extractedData;

    // Write results CSV
    let csvContent =
      "timestamp,window_number,result_value,latency_from_registration_ms\n";

    results.forEach((result) => {
      const latency = queryRegisteredTime
        ? result.timestamp - queryRegisteredTime
        : "N/A";
      csvContent += `${result.timestamp},${result.windowNumber},${result.resultValue},${latency}\n`;
    });

    fs.writeFileSync(this.outputFile, csvContent);
    console.log(`\nSaved results to: ${this.outputFile}`);

    // Calculate statistics
    const values = results.map((r) => r.resultValue);
    const avgValue = values.reduce((a, b) => a + b, 0) / values.length;
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);

    // Calculate first-event latency
    const firstEventLatency =
      queryRegisteredTime && firstResultTime
        ? firstResultTime - queryRegisteredTime
        : null;

    // Write metadata
    const metadata = {
      approach: this.approach,
      pattern: this.patternName,
      queryRegisteredTime: queryRegisteredTime,
      firstResultTime: firstResultTime,
      totalResults: results.length,
      firstEventLatency: firstEventLatency,
      firstEventLatencySeconds: firstEventLatency
        ? (firstEventLatency / 1000).toFixed(2)
        : null,
      statistics: {
        avgValue: avgValue,
        minValue: minValue,
        maxValue: maxValue,
        range: maxValue - minValue,
      },
      extractionMethod: "log_parsing",
      extractionDate: new Date().toISOString(),
    };

    fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
    console.log(`Saved metadata to: ${this.metadataFile}`);

    // Print summary
    console.log("\n=== Summary ===");
    console.log(`Approach: ${this.approach}`);
    console.log(`Pattern: ${this.patternName}`);
    console.log(
      `Query registered at: ${queryRegisteredTime ? new Date(queryRegisteredTime).toISOString() : "N/A"}`,
    );
    console.log(
      `First result at: ${firstResultTime ? new Date(firstResultTime).toISOString() : "N/A"}`,
    );
    console.log(
      `First-event latency: ${firstEventLatency ? `${firstEventLatency} ms (${(firstEventLatency / 1000).toFixed(2)} s)` : "N/A"}`,
    );
    console.log(`Total results: ${results.length}`);
    console.log(`Average value: ${avgValue.toFixed(6)}`);
    console.log(`Value range: ${minValue.toFixed(6)} - ${maxValue.toFixed(6)}`);

    return true;
  }

  run() {
    console.log("Pattern Result Extractor");
    console.log("=======================");
    console.log(`Approach: ${this.approach}`);
    console.log(`Pattern: ${this.patternName}`);
    console.log(`Log dir: ${this.logDir}`);
    console.log("");

    const extractedData = this.extractResults();

    if (extractedData) {
      this.saveResults(extractedData);
      console.log("\n✓ Extraction completed successfully");
      return true;
    } else {
      console.log("\n✗ Extraction failed");
      return false;
    }
  }
}

// Main execution
const args = process.argv.slice(2);

if (args.length < 3) {
  console.error(
    "Usage: node extract-pattern-results.js <approach> <pattern_name> <log_dir>",
  );
  console.error(
    "Example: node extract-pattern-results.js fetching exponential_growth_rate_1 ./logs/...",
  );
  process.exit(1);
}

const [approach, patternName, logDir] = args;

if (
  approach !== "fetching" &&
  approach !== "approximation" &&
  approach !== "chunked"
) {
  console.error(
    'Error: Approach must be "fetching", "approximation", or "chunked"',
  );
  process.exit(1);
}

const extractor = new PatternResultExtractor(approach, patternName, logDir);
const success = extractor.run();

process.exit(success ? 0 : 1);
