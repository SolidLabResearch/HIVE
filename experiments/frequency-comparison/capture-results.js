#!/usr/bin/env node

/**
 * Results Capture Utility for Frequency Comparison Experiments
 *
 * This script subscribes to MQTT result topics and captures query results to CSV files.
 * It captures results from both fetching and approximation approaches for comparison.
 *
 * Usage:
 *   node capture-results.js <approach> <frequency> <output_dir>
 *
 * Example:
 *   node capture-results.js fetching 0.1 ./logs/frequency-comparison-fetching/complex_oscillation_freq_0.1/iteration1
 */

const mqtt = require("mqtt");
const fs = require("fs");
const path = require("path");

class ResultsCapture {
  constructor(approach, frequency, outputDir) {
    this.approach = approach;
    this.frequency = frequency;
    this.outputDir = outputDir;
    this.results = [];
    this.startTime = null;
    this.firstResultTime = null;
    this.client = null;

    // Determine which topics to subscribe to based on approach
    if (approach === "fetching") {
      this.resultTopic = "output"; // Fetching publishes to 'output'
    } else if (approach === "approximation") {
      this.resultTopic = "approximation/output"; // Approximation publishes here
    } else if (approach === "chunked") {
      this.resultTopic = "chunked/output"; // Chunked publishes here
    } else if (approach === "naive-distributed") {
      this.resultTopic = "naive_distributed/output"; // Naive Distributed publishes here
    } else {
      throw new Error(`Unknown approach: ${approach}`);
    }

    // Ensure output directory exists
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    this.resultsFile = path.join(outputDir, `${approach}_results.csv`);
    this.metadataFile = path.join(outputDir, `${approach}_metadata.json`);

    // Initialize CSV file with headers
    this.initializeCSV();
  }

  initializeCSV() {
    const headers =
      "timestamp,window_number,result_value,latency_from_start_ms\n";
    fs.writeFileSync(this.resultsFile, headers);
    console.log(`Initialized results file: ${this.resultsFile}`);
  }

  connect() {
    console.log(`Connecting to MQTT broker at mqtt://localhost:1883/`);
    this.client = mqtt.connect("mqtt://localhost:1883/", {
      clientId: `results_capture_${this.approach}_${Date.now()}`,
      clean: true,
    });

    this.client.on("connect", () => {
      console.log(`Connected to MQTT broker`);
      console.log(`Subscribing to topic: ${this.resultTopic}`);

      this.client.subscribe(this.resultTopic, { qos: 1 }, (err) => {
        if (err) {
          console.error(`Failed to subscribe to ${this.resultTopic}:`, err);
        } else {
          console.log(`Successfully subscribed to ${this.resultTopic}`);
          this.startTime = Date.now();
        }
      });
    });

    this.client.on("message", (topic, message) => {
      this.handleMessage(topic, message);
    });

    this.client.on("error", (err) => {
      console.error("MQTT client error:", err);
    });

    this.client.on("close", () => {
      console.log("MQTT connection closed");
    });
  }

  handleMessage(topic, message) {
    try {
      const timestamp = Date.now();

      if (!this.firstResultTime) {
        this.firstResultTime = timestamp;
        console.log(`First result received at ${timestamp}`);
      }

      const messageStr = message.toString();
      let resultValue = null;

      // Try to parse as JSON first
      try {
        const data = JSON.parse(messageStr);

        // Handle nested RDF term structures from RSP-JS
        // Format: { bindings: { entries: { avgValue: { value: "123.45" } } } }
        if (data.bindings && data.bindings.entries) {
          const entries = data.bindings.entries;

          // Try avgValue first (most common)
          if (entries.avgValue && entries.avgValue.value !== undefined) {
            resultValue = parseFloat(entries.avgValue.value);
          } else if (entries.value && entries.value.value !== undefined) {
            resultValue = parseFloat(entries.value.value);
          } else if (entries.result && entries.result.value !== undefined) {
            resultValue = parseFloat(entries.result.value);
          }
        }
        // Handle simple JSON formats
        else if (data.avgValue !== undefined) {
          resultValue = parseFloat(data.avgValue);
        } else if (data.value !== undefined) {
          resultValue = parseFloat(data.value);
        } else if (data.result !== undefined) {
          resultValue = parseFloat(data.result);
        } else if (typeof data === "number") {
          resultValue = data;
        }
      } catch (e) {
        // If not JSON, try to parse as a number directly
        const parsed = parseFloat(messageStr);
        if (!isNaN(parsed)) {
          resultValue = parsed;
        }
      }

      if (resultValue !== null && !isNaN(resultValue)) {
        const latencyFromStart = timestamp - this.startTime;
        const windowNumber = this.results.length + 1;

        const resultRecord = {
          timestamp,
          windowNumber,
          resultValue,
          latencyFromStart,
        };

        this.results.push(resultRecord);

        // Append to CSV
        const csvLine = `${timestamp},${windowNumber},${resultValue},${latencyFromStart}\n`;
        fs.appendFileSync(this.resultsFile, csvLine);

        console.log(
          `[${this.approach}] Window ${windowNumber}: ${resultValue.toFixed(6)} (latency: ${latencyFromStart}ms)`,
        );
      } else {
        console.warn(
          `Could not extract numeric result from message: ${messageStr.substring(0, 200)}`,
        );
      }
    } catch (error) {
      console.error("Error handling message:", error);
    }
  }

  saveMetadata() {
    const metadata = {
      approach: this.approach,
      frequency: this.frequency,
      startTime: this.startTime,
      firstResultTime: this.firstResultTime,
      totalResults: this.results.length,
      firstEventLatency: this.firstResultTime
        ? this.firstResultTime - this.startTime
        : null,
      resultTopic: this.resultTopic,
      captureDate: new Date().toISOString(),
    };

    fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
    console.log(`Saved metadata to: ${this.metadataFile}`);
  }

  stop() {
    console.log(`\nStopping results capture...`);
    console.log(`Total results captured: ${this.results.length}`);

    this.saveMetadata();

    if (this.client) {
      this.client.end();
    }

    console.log(`Results saved to: ${this.resultsFile}`);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);

if (args.length < 3) {
  console.error(
    "Usage: node capture-results.js <approach> <frequency> <output_dir>",
  );
  console.error(
    "Example: node capture-results.js fetching 0.1 ./logs/frequency-comparison-fetching/complex_oscillation_freq_0.1/iteration1",
  );
  process.exit(1);
}

const [approach, frequency, outputDir] = args;

console.log("Results Capture Utility");
console.log("=======================");
console.log(`Approach: ${approach}`);
console.log(`Frequency: ${frequency} Hz`);
console.log(`Output Directory: ${outputDir}`);
console.log("");

const capture = new ResultsCapture(approach, frequency, outputDir);
capture.connect();

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\nReceived SIGINT, shutting down...");
  capture.stop();
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("\nReceived SIGTERM, shutting down...");
  capture.stop();
  process.exit(0);
});

// Auto-stop after 3 minutes (same as experiment timeout)
setTimeout(
  () => {
    console.log("\nTimeout reached (3 minutes), stopping capture...");
    capture.stop();
    process.exit(0);
  },
  3 * 60 * 1000,
);
