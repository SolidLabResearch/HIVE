#!/usr/bin/env node

/**
 * Extract Results from Approach Logs
 *
 * When MQTT capture fails, this script extracts query results directly from
 * the approach log files by parsing RStream output.
 *
 * Updated to extract query registration timestamps for accurate first-event latency calculation.
 *
 * Usage:
 *   node extract-results-from-logs.js <approach> <frequency>
 *
 * Example:
 *   node extract-results-from-logs.js fetching 0.1
 *   node extract-results-from-logs.js approximation 0.1
 */

const fs = require("fs");
const path = require("path");

class LogResultExtractor {
  constructor(approach, frequency) {
    this.approach = approach;
    this.frequency = frequency;

    // Determine log directory
    const logBaseName =
      approach === "fetching"
        ? "frequency-comparison-fetching"
        : approach === "approximation"
          ? "frequency-comparison-approximation"
          : approach === "naive-distributed"
            ? "frequency-comparison-naive-distributed"
            : "frequency-comparison-chunked";

    const formattedFreq =
      parseFloat(frequency) % 1 === 0
        ? parseFloat(frequency).toFixed(1)
        : frequency.toString();

    this.logDir = path.join(
      "./logs",
      logBaseName,
      `complex_oscillation_freq_${formattedFreq}`,
      "iteration1",
    );

    // Determine log file names
    this.logFile =
      approach === "fetching"
        ? path.join(this.logDir, "fetching_client_side_log.csv")
        : approach === "approximation"
          ? path.join(this.logDir, "approximation_approach_log.csv")
          : approach === "naive-distributed"
            ? path.join(this.logDir, "naive_distributed_approach_log.csv")
            : path.join(this.logDir, "streaming_query_chunk_aggregator_log.csv");

    this.outputFile = path.join(this.logDir, `${approach}_results.csv`);
    this.metadataFile = path.join(this.logDir, `${approach}_metadata.json`);
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

    // For fetching approach
    if (this.approach === "fetching") {      // Extract query registration time from "fetching_query_registered" log
      const registrationMatch = content.match(
        /LOG:\s*(\d+)\s*-\s*fetching_query_registered/,
      );
      if (registrationMatch) {
        queryRegisteredTime = parseInt(registrationMatch[1]);
        console.log(`Found query registration time: ${queryRegisteredTime}`);
      } else {
        console.warn(
          "Could not find query registration timestamp in fetching log",
        );
      }

      // Parse RStream events
      const lines = content.split("\n");

      for (const line of lines) {
        if (line.includes("DEBUG: RStream event received:")) {
          try {
            // Extract JSON from the line
            const jsonStart = line.indexOf("{");
            if (jsonStart === -1) continue;

            const jsonStr = line.substring(jsonStart);
            const data = JSON.parse(jsonStr);

            // Extract avgValue from nested structure
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
            // Skip lines that can't be parsed
            continue;
          }
        }
      }
    }
    // For naive-distributed approach
    // Log format is identical to the fetching approach: the super-query result is
    // logged as "DEBUG: RStream event received: {JSON}" and the registration time
    // is recorded with the "naive_distributed_query_registered" marker.
    else if (this.approach === "naive-distributed") {
      const registrationMatch = content.match(
        /LOG:\s*(\d+)\s*-\s*naive_distributed_query_registered/,
      );
      if (registrationMatch) {
        queryRegisteredTime = parseInt(registrationMatch[1]);
        console.log(`Found query registration time: ${queryRegisteredTime}`);
      } else {
        console.warn(
          "Could not find query registration timestamp in naive-distributed log",
        );
      }

      // Parse super-query RStream events (same JSON structure as fetching)
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
            // Skip lines that can't be parsed
            continue;
          }
        }
      }
    }
    // For chunked approach
    else if (this.approach === "chunked") {
      const lines = content.split("\n");

      // Look for latency log lines from the chunked operator
      // Format similar to approximation: "- From query registration: Xms (expected close: TIMESTAMP, result: TIMESTAMP)"
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        // Extract result timestamp and query registration info from latency logs
        const latencyMatch = line.match(
          /From query registration:\s*(-?\d+)ms\s*\(expected close:\s*(\d+),\s*result:\s*(\d+)\)/,
        );

        if (latencyMatch) {
          const expectedCloseTime = parseInt(latencyMatch[2]);
          const resultTimestamp = parseInt(latencyMatch[3]);

          // For the FIRST window: expectedClose = queryRegisteredTime + RANGE
          const windowRangeMs = 120000;
          const calculatedRegistrationTime = expectedCloseTime - windowRangeMs;

          if (!queryRegisteredTime) {
            queryRegisteredTime = calculatedRegistrationTime;
            console.log(
              `Found query registration time from latency log: ${queryRegisteredTime}`,
            );
            console.log(
              `  Expected close: ${expectedCloseTime} (registration + ${windowRangeMs}ms RANGE)`,
            );
          }

          if (!firstResultTime) {
            firstResultTime = resultTimestamp;
            console.log(`Found first result time: ${firstResultTime}`);
          }
        }

        // Extract published results from chunked operator
        if (line.includes("Successfully published unified cross-sensor")) {
          const match = line.match(
            /Successfully published unified cross-sensor \w+:\s*([\d.]+)/,
          );
          if (match) {
            const resultValue = parseFloat(match[1]);

            // Find the actual result timestamp by looking backwards for latency logs
            let timestamp = firstResultTime || Date.now();

            // Search backwards for the result timestamp in latency logs near this line
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

      // Fallback: if we didn't find latency logs, estimate based on first data arrival
      if (!queryRegisteredTime && results.length > 0) {
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i];

          if (line.includes("Running registered query:")) {
            for (let j = i; j < Math.min(lines.length, i + 500); j++) {
              const dataMatch = lines[j].match(
                /Adding.*at time\s*:\s*(\d{13})/,
              );
              if (dataMatch) {
                if (firstResultTime) {
                  queryRegisteredTime = firstResultTime - 60000;
                  console.log(
                    `Estimated query registration time: ${queryRegisteredTime} (based on first result and window config)`,
                  );
                }
                break;
              }
            }
            break;
          }
        }
      }
    }
    // For approximation approach
    else if (this.approach === "approximation") {
      const lines = content.split("\n");

      // Look for latency log lines from the approximation operator
      // Format: "- From query registration: Xms (expected close: TIMESTAMP, result: TIMESTAMP)"
      // For window 1: expectedClose = queryRegisteredTime + RANGE (120000ms)
      // For window N: expectedClose = queryRegisteredTime + RANGE + (N-1) * STEP
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        // Extract result timestamp and query registration info from latency logs
        const latencyMatch = line.match(
          /From query registration:\s*(-?\d+)ms\s*\(expected close:\s*(\d+),\s*result:\s*(\d+)\)/,
        );

        if (latencyMatch) {
          const expectedCloseTime = parseInt(latencyMatch[2]);
          const resultTimestamp = parseInt(latencyMatch[3]);

          // For the FIRST window (window 1):
          // expectedClose = queryRegisteredTime + RANGE
          // Therefore: queryRegisteredTime = expectedClose - RANGE
          // Window configuration: RANGE = 120000ms, STEP = 60000ms
          const windowRangeMs = 120000;
          const calculatedRegistrationTime = expectedCloseTime - windowRangeMs;

          if (!queryRegisteredTime) {
            queryRegisteredTime = calculatedRegistrationTime;
            console.log(
              `Found query registration time from latency log: ${queryRegisteredTime}`,
            );
            console.log(
              `  Expected close: ${expectedCloseTime} (registration + ${windowRangeMs}ms RANGE)`,
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

            // Find the actual result timestamp by looking backwards for latency logs
            let timestamp = firstResultTime || Date.now();

            // Search backwards for the result timestamp in latency logs near this line
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

      // Fallback: if we didn't find latency logs, look for first data arrival
      if (!queryRegisteredTime && results.length > 0) {
        // Look for "Running registered query:" and first data timestamp
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i];

          if (line.includes("Running registered query:")) {
            // Look forward for first "Adding" message with timestamp
            for (let j = i; j < Math.min(lines.length, i + 500); j++) {
              const dataMatch = lines[j].match(
                /Adding.*at time\s*:\s*(\d{13})/,
              );
              if (dataMatch) {
                const firstDataTime = parseInt(dataMatch[1]);
                // Query registration should be slightly before first data
                // Use first result time and work backwards with window config (120s window)
                if (firstResultTime) {
                  queryRegisteredTime = firstResultTime - 60000; // Conservative 60s estimate
                  console.log(
                    `Estimated query registration time: ${queryRegisteredTime} (based on first result and window config)`,
                  );
                }
                break;
              }
            }
            break;
          }
        }
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

    // Calculate first-event latency
    const firstEventLatency =
      queryRegisteredTime && firstResultTime
        ? firstResultTime - queryRegisteredTime
        : null;

    // Write metadata
    const metadata = {
      approach: this.approach,
      frequency: this.frequency,
      queryRegisteredTime: queryRegisteredTime,
      firstResultTime: firstResultTime,
      totalResults: results.length,
      firstEventLatency: firstEventLatency,
      firstEventLatencySeconds: firstEventLatency
        ? (firstEventLatency / 1000).toFixed(2)
        : null,
      extractionMethod: "log_parsing",
      extractionDate: new Date().toISOString(),
      notes:
        this.approach === "approximation" && !queryRegisteredTime
          ? "Query registration time estimated based on first result time"
          : "Query registration time extracted from logs",
    };

    fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
    console.log(`Saved metadata to: ${this.metadataFile}`);

    // Print summary
    console.log("\n=== Summary ===");
    console.log(`Approach: ${this.approach}`);
    console.log(`Frequency: ${this.frequency} Hz`);
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

    return true;
  }

  run() {
    console.log("Log Result Extractor");
    console.log("===================");
    console.log(`Approach: ${this.approach}`);
    console.log(`Frequency: ${this.frequency} Hz`);
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

if (args.length < 2) {
  console.error(
    "Usage: node extract-results-from-logs.js <approach> <frequency>",
  );
  console.error("Example: node extract-results-from-logs.js fetching 0.1");
  process.exit(1);
}

const [approach, frequency] = args;

if (
  approach !== "fetching" &&
  approach !== "approximation" &&
  approach !== "chunked" &&
  approach !== "naive-distributed"
) {
  console.error(
    'Error: Approach must be "fetching", "approximation", "chunked", or "naive-distributed"',
  );
  process.exit(1);
}

const extractor = new LogResultExtractor(approach, frequency);
const success = extractor.run();

process.exit(success ? 0 : 1);
