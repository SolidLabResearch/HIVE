import { EventEmitter } from "events";
import * as fs from "fs";
import * as path from "path";
import mqtt from "mqtt";
import { RSPEngine, RDFStream } from "rsp-js";
import { v4 as uuidv4 } from "uuid";

const N3 = require("n3");
const { DataFactory } = N3;

// ============================================================================
// Types and Interfaces
// ============================================================================

interface ExperimentResult {
  approach: string;
  windowNumber: number;
  windowCloseTime: number;
  resultAvailableTime: number;
  firstEventLatencyMs: number;
  resultValue: number;
  timestamp: number;
}

interface AccuracyResult {
  approach: string;
  windowNumber: number;
  groundTruthValue: number;
  approachValue: number;
  absoluteError: number;
  percentageError: number;
}

interface ExperimentConfig {
  mqttBroker: string;
  dataFrequency: number; // events per second
  experimentDurationMs: number;
  windowWidthMs: number;
  windowSlideMs: number;
  subQueryWindowWidthMs: number;
  subQueryWindowSlideMs: number;
}

// ============================================================================
// Queries
// ============================================================================

// Sub-queries are documented in README.md but the experiment uses MAIN_QUERY directly
// since we're comparing approaches that all receive the same raw stream data

const MAIN_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
`;

// ============================================================================
// CSV Logger for Experiments
// ============================================================================

class ExperimentLogger {
  private latencyStream: fs.WriteStream;
  private accuracyStream: fs.WriteStream;
  private outputDir: string;

  constructor(outputDir: string) {
    this.outputDir = outputDir;
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const latencyPath = path.join(
      outputDir,
      `latency_results_${timestamp}.csv`,
    );
    const accuracyPath = path.join(
      outputDir,
      `accuracy_results_${timestamp}.csv`,
    );

    this.latencyStream = fs.createWriteStream(latencyPath, { flags: "w" });
    this.accuracyStream = fs.createWriteStream(accuracyPath, { flags: "w" });

    // Write headers
    this.latencyStream.write(
      "approach,window_number,window_close_time,result_available_time,first_event_latency_ms,result_value,timestamp\n",
    );
    this.accuracyStream.write(
      "approach,window_number,ground_truth_value,approach_value,absolute_error,percentage_error\n",
    );
  }

  logLatency(result: ExperimentResult): void {
    this.latencyStream.write(
      `${result.approach},${result.windowNumber},${result.windowCloseTime},${result.resultAvailableTime},${result.firstEventLatencyMs},${result.resultValue},${result.timestamp}\n`,
    );
  }

  logAccuracy(result: AccuracyResult): void {
    this.accuracyStream.write(
      `${result.approach},${result.windowNumber},${result.groundTruthValue},${result.approachValue},${result.absoluteError},${result.percentageError}\n`,
    );
  }

  close(): void {
    this.latencyStream.end();
    this.accuracyStream.end();
  }
}

// ============================================================================
// Mock Data Generator
// ============================================================================

class MockDataGenerator {
  private mqttClient: mqtt.MqttClient;
  private isRunning: boolean = false;
  private publishedData: Map<
    string,
    Array<{ value: number; timestamp: number }>
  > = new Map();

  constructor(private config: ExperimentConfig) {
    this.mqttClient = mqtt.connect(config.mqttBroker, {
      clientId: `mock-generator-${uuidv4().slice(0, 8)}`,
      clean: true,
    });
    this.publishedData.set("wearableX", []);
    this.publishedData.set("smartphoneX", []);
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.mqttClient.on("connect", () => {
        console.log("[MockDataGenerator] Connected to MQTT broker");
        this.isRunning = true;
        this.startPublishing();
        resolve();
      });
    });
  }

  private startPublishing(): void {
    const intervalMs = 1000 / this.config.dataFrequency;

    const publishEvent = (topic: string, sensorProperty: string) => {
      if (!this.isRunning) return;

      const value = Math.random() * 100 - 50; // Random value between -50 and 50
      const timestamp = Date.now();
      const isoTimestamp = new Date(timestamp).toISOString();

      const eventId = `https://rsp.js/event/${uuidv4()}`;
      const rdfData = `
<${eventId}> <https://saref.etsi.org/core/hasValue> "${value}"^^<http://www.w3.org/2001/XMLSchema#float> .
<${eventId}> <https://saref.etsi.org/core/hasTimestamp> "${isoTimestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<${eventId}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${sensorProperty}> .
<${eventId}> <https://saref.etsi.org/core/measurementMadeBy> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${sensorProperty}Sensor> .
      `.trim();

      this.mqttClient.publish(topic, rdfData, { qos: 1 });
      this.publishedData
        .get(topic.replace("mqtt://localhost:1883/", ""))
        ?.push({ value, timestamp });
    };

    // Stagger the publishing for the two streams
    setInterval(() => publishEvent("wearableX", "wearableX"), intervalMs);
    setTimeout(() => {
      setInterval(() => publishEvent("smartphoneX", "smartphoneX"), intervalMs);
    }, intervalMs / 2);
  }

  getPublishedData(): Map<string, Array<{ value: number; timestamp: number }>> {
    return this.publishedData;
  }

  stop(): void {
    this.isRunning = false;
    this.mqttClient.end();
  }
}

// ============================================================================
// Ground Truth Calculator (Fetching Client-Side Approach)
// ============================================================================

class FetchingClientSideApproach {
  private rspEngine: RSPEngine;
  private rstreamEmitter: EventEmitter;
  private results: ExperimentResult[] = [];
  private windowNumber: number = 0;
  private windowCloseTimeTracker: Map<number, number> = new Map();
  private mqttClient!: mqtt.MqttClient;
  private isRunning: boolean = false;
  private config: ExperimentConfig;
  private logger: ExperimentLogger;

  constructor(config: ExperimentConfig, logger: ExperimentLogger) {
    this.config = config;
    this.logger = logger;
    this.rspEngine = new RSPEngine(MAIN_QUERY);
    this.rstreamEmitter = this.rspEngine.register();
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.mqttClient = mqtt.connect(this.config.mqttBroker, {
        clientId: `fetching-client-${uuidv4().slice(0, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("[FetchingClientSide] Connected to MQTT broker");
        this.isRunning = true;

        // Subscribe to both streams
        this.mqttClient.subscribe(
          ["wearableX", "smartphoneX"],
          { qos: 1 },
          (err) => {
            if (err) {
              console.error("[FetchingClientSide] Subscription error:", err);
            } else {
              console.log("[FetchingClientSide] Subscribed to streams");
            }
          },
        );

        this.setupStreamProcessing();
        this.setupResultHandler();
        resolve();
      });
    });
  }

  private setupStreamProcessing(): void {
    const streamMap: { [key: string]: RDFStream } = {
      wearableX: this.rspEngine.getStream(
        "mqtt://localhost:1883/wearableX",
      ) as RDFStream,
      smartphoneX: this.rspEngine.getStream(
        "mqtt://localhost:1883/smartphoneX",
      ) as RDFStream,
    };

    this.mqttClient.on("message", async (topic: string, message: Buffer) => {
      if (!this.isRunning) return;

      try {
        const messageStr = message.toString();
        const store = new N3.Store();
        const parser = new N3.Parser();

        const quads = parser.parse(messageStr);
        store.addQuads(quads);

        // Extract timestamp from the RDF data
        const timestampQuads = store.getQuads(
          null,
          DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"),
          null,
          null,
        );

        let timestamp = Date.now();
        if (timestampQuads.length > 0) {
          const timestampValue = timestampQuads[0].object.value;
          timestamp = Date.parse(timestampValue);
        }

        // Track window close time (approximation based on slide interval)
        const windowNum = Math.floor(timestamp / this.config.windowSlideMs);
        if (!this.windowCloseTimeTracker.has(windowNum)) {
          this.windowCloseTimeTracker.set(
            windowNum,
            timestamp + this.config.windowSlideMs,
          );
        }

        const rdfStream = streamMap[topic];
        if (rdfStream) {
          const graphNode = DataFactory.namedNode(
            `mqtt://localhost:1883/${topic}`,
          );
          for (const quad of quads) {
            const quadWithGraph = DataFactory.quad(
              quad.subject,
              quad.predicate,
              quad.object,
              graphNode,
            );
            rdfStream.add(quadWithGraph, timestamp);
          }
        }
      } catch (error) {
        console.error("[FetchingClientSide] Error processing message:", error);
      }
    });
  }

  private setupResultHandler(): void {
    this.rstreamEmitter.on("RStream", (event: any) => {
      const resultAvailableTime = Date.now();

      if (!event || !event.bindings) return;

      for (const binding of event.bindings.values()) {
        const value = parseFloat(binding.value);
        this.windowNumber++;

        // Estimate window close time
        const estimatedWindowCloseTime =
          resultAvailableTime -
          (resultAvailableTime % this.config.windowSlideMs);

        const result: ExperimentResult = {
          approach: "fetching_client_side",
          windowNumber: this.windowNumber,
          windowCloseTime: estimatedWindowCloseTime,
          resultAvailableTime: resultAvailableTime,
          firstEventLatencyMs: resultAvailableTime - estimatedWindowCloseTime,
          resultValue: value,
          timestamp: resultAvailableTime,
        };

        this.results.push(result);
        this.logger.logLatency(result);
        console.log(
          `[FetchingClientSide] Window ${this.windowNumber}: Value=${value.toFixed(4)}, Latency=${result.firstEventLatencyMs}ms`,
        );
      }
    });
  }

  getResults(): ExperimentResult[] {
    return this.results;
  }

  stop(): void {
    this.isRunning = false;
    this.mqttClient.end();
  }
}

// ============================================================================
// Approximation Approach
// ============================================================================

class ApproximationApproach {
  private mqttClient!: mqtt.MqttClient;
  private results: ExperimentResult[] = [];
  private windowNumber: number = 0;
  private isRunning: boolean = false;
  private config: ExperimentConfig;
  private logger: ExperimentLogger;

  // Buffers for storing sub-query results
  private subQueryBuffers: Map<
    string,
    Array<{ value: number; timestamp: number }>
  > = new Map();

  constructor(config: ExperimentConfig, logger: ExperimentLogger) {
    this.config = config;
    this.logger = logger;
    this.subQueryBuffers.set("wearableX", []);
    this.subQueryBuffers.set("smartphoneX", []);
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.mqttClient = mqtt.connect(this.config.mqttBroker, {
        clientId: `approximation-${uuidv4().slice(0, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("[Approximation] Connected to MQTT broker");
        this.isRunning = true;

        // Subscribe to raw streams
        this.mqttClient.subscribe(
          ["wearableX", "smartphoneX"],
          { qos: 1 },
          (err) => {
            if (err) {
              console.error("[Approximation] Subscription error:", err);
            } else {
              console.log("[Approximation] Subscribed to streams");
            }
          },
        );

        this.setupMessageHandler();
        this.startApproximationTimer();
        resolve();
      });
    });
  }

  private setupMessageHandler(): void {
    this.mqttClient.on("message", (topic: string, message: Buffer) => {
      if (!this.isRunning) return;

      try {
        const messageStr = message.toString();

        // Parse value from RDF
        const valueMatch = messageStr.match(
          /hasValue>\s*"([^"]*)"(?:\^\^<[^>]*>)?/,
        );
        if (!valueMatch) return;

        const value = parseFloat(valueMatch[1]);
        if (isNaN(value)) return;

        const timestamp = Date.now();
        this.subQueryBuffers.get(topic)?.push({ value, timestamp });
      } catch (error) {
        console.error("[Approximation] Error processing message:", error);
      }
    });
  }

  private startApproximationTimer(): void {
    // Trigger approximation at each slide interval
    setInterval(() => {
      if (!this.isRunning) return;

      const now = Date.now();
      const windowCloseTime = now;

      // Get values within the window width for each stream
      const windowStart = now - this.config.windowWidthMs;

      const wearableValues = this.subQueryBuffers
        .get("wearableX")!
        .filter((d) => d.timestamp >= windowStart)
        .map((d) => d.value);

      const smartphoneValues = this.subQueryBuffers
        .get("smartphoneX")!
        .filter((d) => d.timestamp >= windowStart)
        .map((d) => d.value);

      // Clean up old values
      this.subQueryBuffers.set(
        "wearableX",
        this.subQueryBuffers
          .get("wearableX")!
          .filter((d) => d.timestamp >= windowStart),
      );
      this.subQueryBuffers.set(
        "smartphoneX",
        this.subQueryBuffers
          .get("smartphoneX")!
          .filter((d) => d.timestamp >= windowStart),
      );

      // Combine and compute MAX (matching the query)
      const allValues = [...wearableValues, ...smartphoneValues];
      if (allValues.length === 0) return;

      const maxValue = Math.max(...allValues);
      const resultAvailableTime = Date.now();

      this.windowNumber++;

      const result: ExperimentResult = {
        approach: "approximation",
        windowNumber: this.windowNumber,
        windowCloseTime: windowCloseTime,
        resultAvailableTime: resultAvailableTime,
        firstEventLatencyMs: resultAvailableTime - windowCloseTime,
        resultValue: maxValue,
        timestamp: resultAvailableTime,
      };

      this.results.push(result);
      this.logger.logLatency(result);
      console.log(
        `[Approximation] Window ${this.windowNumber}: Value=${maxValue.toFixed(4)}, Latency=${result.firstEventLatencyMs}ms`,
      );
    }, this.config.windowSlideMs);
  }

  getResults(): ExperimentResult[] {
    return this.results;
  }

  stop(): void {
    this.isRunning = false;
    this.mqttClient.end();
  }
}

// ============================================================================
// Chunked Query Approach
// ============================================================================

class ChunkedQueryApproach {
  private mqttClient!: mqtt.MqttClient;
  private results: ExperimentResult[] = [];
  private windowNumber: number = 0;
  private isRunning: boolean = false;
  private config: ExperimentConfig;
  private logger: ExperimentLogger;

  // Chunk-based buffers
  private chunkBuffers: Map<
    string,
    Array<{ value: number; timestamp: number; chunkId: number }>
  > = new Map();
  private chunkSize: number;

  constructor(config: ExperimentConfig, logger: ExperimentLogger) {
    this.config = config;
    this.logger = logger;
    this.chunkBuffers.set("wearableX", []);
    this.chunkBuffers.set("smartphoneX", []);
    // GCD of sub-query windows (60000) and main query window (120000) is 60000
    this.chunkSize = this.gcd(
      config.subQueryWindowWidthMs,
      config.windowWidthMs,
    );
  }

  private gcd(a: number, b: number): number {
    return b === 0 ? a : this.gcd(b, a % b);
  }

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.mqttClient = mqtt.connect(this.config.mqttBroker, {
        clientId: `chunked-${uuidv4().slice(0, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        console.log("[ChunkedQuery] Connected to MQTT broker");
        this.isRunning = true;

        // Subscribe to raw streams
        this.mqttClient.subscribe(
          ["wearableX", "smartphoneX"],
          { qos: 1 },
          (err) => {
            if (err) {
              console.error("[ChunkedQuery] Subscription error:", err);
            } else {
              console.log("[ChunkedQuery] Subscribed to streams");
            }
          },
        );

        this.setupMessageHandler();
        this.startChunkedAggregation();
        resolve();
      });
    });
  }

  private setupMessageHandler(): void {
    this.mqttClient.on("message", (topic: string, message: Buffer) => {
      if (!this.isRunning) return;

      try {
        const messageStr = message.toString();

        // Parse value from RDF
        const valueMatch = messageStr.match(
          /hasValue>\s*"([^"]*)"(?:\^\^<[^>]*>)?/,
        );
        if (!valueMatch) return;

        const value = parseFloat(valueMatch[1]);
        if (isNaN(value)) return;

        const timestamp = Date.now();
        const chunkId = Math.floor(timestamp / this.chunkSize);

        this.chunkBuffers.get(topic)?.push({ value, timestamp, chunkId });
      } catch (error) {
        console.error("[ChunkedQuery] Error processing message:", error);
      }
    });
  }

  private startChunkedAggregation(): void {
    // Trigger aggregation at each slide interval
    setInterval(() => {
      if (!this.isRunning) return;

      const now = Date.now();
      const windowCloseTime = now;

      // Process chunks within the window
      const chunksNeeded = Math.ceil(
        this.config.windowWidthMs / this.chunkSize,
      );
      const currentChunkId = Math.floor(now / this.chunkSize);
      const startChunkId = currentChunkId - chunksNeeded;

      // Get aggregated values per chunk
      const allValues: number[] = [];

      for (const [topic, buffer] of this.chunkBuffers.entries()) {
        // Filter to relevant chunks
        const relevantData = buffer.filter(
          (d) => d.chunkId >= startChunkId && d.chunkId <= currentChunkId,
        );

        // Per-chunk MAX (simulating sub-query aggregation)
        const chunkMaxes = new Map<number, number>();
        for (const d of relevantData) {
          const currentMax = chunkMaxes.get(d.chunkId) ?? -Infinity;
          chunkMaxes.set(d.chunkId, Math.max(currentMax, d.value));
        }

        allValues.push(...Array.from(chunkMaxes.values()));

        // Clean up old chunks
        this.chunkBuffers.set(
          topic,
          buffer.filter((d) => d.chunkId >= startChunkId),
        );
      }

      if (allValues.length === 0) return;

      // Final aggregation: MAX of all chunk MAXes
      const maxValue = Math.max(...allValues);
      const resultAvailableTime = Date.now();

      this.windowNumber++;

      const result: ExperimentResult = {
        approach: "chunked_query",
        windowNumber: this.windowNumber,
        windowCloseTime: windowCloseTime,
        resultAvailableTime: resultAvailableTime,
        firstEventLatencyMs: resultAvailableTime - windowCloseTime,
        resultValue: maxValue,
        timestamp: resultAvailableTime,
      };

      this.results.push(result);
      this.logger.logLatency(result);
      console.log(
        `[ChunkedQuery] Window ${this.windowNumber}: Value=${maxValue.toFixed(4)}, Latency=${result.firstEventLatencyMs}ms`,
      );
    }, this.config.windowSlideMs);
  }

  getResults(): ExperimentResult[] {
    return this.results;
  }

  stop(): void {
    this.isRunning = false;
    this.mqttClient.end();
  }
}

// ============================================================================
// Main Experiment Runner
// ============================================================================

export class ApproachComparisonExperiment {
  private config: ExperimentConfig;
  private logger: ExperimentLogger;
  private dataGenerator: MockDataGenerator;
  private fetchingApproach: FetchingClientSideApproach;
  private approximationApproach: ApproximationApproach;
  private chunkedApproach: ChunkedQueryApproach;

  constructor(outputDir: string = "./experiments/approach-comparison/results") {
    this.config = {
      mqttBroker: "mqtt://localhost:1883",
      dataFrequency: 4, // 4 events per second per stream
      experimentDurationMs: 180000, // 3 minutes
      windowWidthMs: 120000, // 120 seconds (main query)
      windowSlideMs: 60000, // 60 seconds
      subQueryWindowWidthMs: 60000, // 60 seconds
      subQueryWindowSlideMs: 60000, // 60 seconds
    };

    this.logger = new ExperimentLogger(outputDir);
    this.dataGenerator = new MockDataGenerator(this.config);
    this.fetchingApproach = new FetchingClientSideApproach(
      this.config,
      this.logger,
    );
    this.approximationApproach = new ApproximationApproach(
      this.config,
      this.logger,
    );
    this.chunkedApproach = new ChunkedQueryApproach(this.config, this.logger);
  }

  async run(): Promise<void> {
    console.log("============================================================");
    console.log("  Approach Comparison Experiment");
    console.log("============================================================");
    console.log(`Configuration:`);
    console.log(`  - MQTT Broker: ${this.config.mqttBroker}`);
    console.log(`  - Data Frequency: ${this.config.dataFrequency} events/sec`);
    console.log(
      `  - Experiment Duration: ${this.config.experimentDurationMs / 1000} seconds`,
    );
    console.log(
      `  - Window Width: ${this.config.windowWidthMs / 1000} seconds`,
    );
    console.log(
      `  - Window Slide: ${this.config.windowSlideMs / 1000} seconds`,
    );
    console.log(
      "============================================================\n",
    );

    try {
      // Start all components
      console.log("Starting data generator...");
      await this.dataGenerator.start();

      console.log("Starting Fetching Client-Side approach (Ground Truth)...");
      await this.fetchingApproach.start();

      console.log("Starting Approximation approach...");
      await this.approximationApproach.start();

      console.log("Starting Chunked Query approach...");
      await this.chunkedApproach.start();

      console.log("\nAll approaches started. Running experiment...\n");

      // Wait for experiment duration
      await this.sleep(this.config.experimentDurationMs);

      // Stop all components
      console.log("\nStopping experiment...");
      this.dataGenerator.stop();
      this.fetchingApproach.stop();
      this.approximationApproach.stop();
      this.chunkedApproach.stop();

      // Calculate accuracy
      await this.calculateAccuracy();

      // Generate summary
      this.generateSummary();

      this.logger.close();
      console.log("\nExperiment completed successfully!");
    } catch (error) {
      console.error("Experiment failed:", error);
      throw error;
    }
  }

  private async calculateAccuracy(): Promise<void> {
    console.log(
      "\n============================================================",
    );
    console.log("  Calculating Accuracy");
    console.log(
      "============================================================\n",
    );

    const groundTruthResults = this.fetchingApproach.getResults();
    const approximationResults = this.approximationApproach.getResults();
    const chunkedResults = this.chunkedApproach.getResults();

    // Match results by window number
    for (const gtResult of groundTruthResults) {
      // Find matching approximation result
      const approxMatch = approximationResults.find(
        (r) => r.windowNumber === gtResult.windowNumber,
      );
      if (approxMatch) {
        const absoluteError = Math.abs(
          gtResult.resultValue - approxMatch.resultValue,
        );
        const percentageError =
          gtResult.resultValue !== 0
            ? (absoluteError / Math.abs(gtResult.resultValue)) * 100
            : absoluteError === 0
              ? 0
              : 100;

        this.logger.logAccuracy({
          approach: "approximation",
          windowNumber: gtResult.windowNumber,
          groundTruthValue: gtResult.resultValue,
          approachValue: approxMatch.resultValue,
          absoluteError,
          percentageError,
        });
      }

      // Find matching chunked result
      const chunkedMatch = chunkedResults.find(
        (r) => r.windowNumber === gtResult.windowNumber,
      );
      if (chunkedMatch) {
        const absoluteError = Math.abs(
          gtResult.resultValue - chunkedMatch.resultValue,
        );
        const percentageError =
          gtResult.resultValue !== 0
            ? (absoluteError / Math.abs(gtResult.resultValue)) * 100
            : absoluteError === 0
              ? 0
              : 100;

        this.logger.logAccuracy({
          approach: "chunked_query",
          windowNumber: gtResult.windowNumber,
          groundTruthValue: gtResult.resultValue,
          approachValue: chunkedMatch.resultValue,
          absoluteError,
          percentageError,
        });
      }
    }
  }

  private generateSummary(): void {
    console.log(
      "\n============================================================",
    );
    console.log("  Experiment Summary");
    console.log(
      "============================================================\n",
    );

    const fetchingResults = this.fetchingApproach.getResults();
    const approximationResults = this.approximationApproach.getResults();
    const chunkedResults = this.chunkedApproach.getResults();

    const calcStats = (results: ExperimentResult[]) => {
      if (results.length === 0)
        return { count: 0, avgLatency: 0, minLatency: 0, maxLatency: 0 };
      const latencies = results.map((r) => r.firstEventLatencyMs);
      return {
        count: results.length,
        avgLatency: latencies.reduce((a, b) => a + b, 0) / latencies.length,
        minLatency: Math.min(...latencies),
        maxLatency: Math.max(...latencies),
      };
    };

    const fetchingStats = calcStats(fetchingResults);
    const approxStats = calcStats(approximationResults);
    const chunkedStats = calcStats(chunkedResults);

    console.log("Fetching Client-Side (Ground Truth):");
    console.log(`  - Results: ${fetchingStats.count}`);
    console.log(`  - Avg Latency: ${fetchingStats.avgLatency.toFixed(2)} ms`);
    console.log(`  - Min Latency: ${fetchingStats.minLatency.toFixed(2)} ms`);
    console.log(`  - Max Latency: ${fetchingStats.maxLatency.toFixed(2)} ms`);

    console.log("\nApproximation Approach:");
    console.log(`  - Results: ${approxStats.count}`);
    console.log(`  - Avg Latency: ${approxStats.avgLatency.toFixed(2)} ms`);
    console.log(`  - Min Latency: ${approxStats.minLatency.toFixed(2)} ms`);
    console.log(`  - Max Latency: ${approxStats.maxLatency.toFixed(2)} ms`);

    console.log("\nChunked Query Approach:");
    console.log(`  - Results: ${chunkedStats.count}`);
    console.log(`  - Avg Latency: ${chunkedStats.avgLatency.toFixed(2)} ms`);
    console.log(`  - Min Latency: ${chunkedStats.minLatency.toFixed(2)} ms`);
    console.log(`  - Max Latency: ${chunkedStats.maxLatency.toFixed(2)} ms`);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// ============================================================================
// CLI Entry Point
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const outputDir = args[0] || "./experiments/approach-comparison/results";

  const experiment = new ApproachComparisonExperiment(outputDir);
  await experiment.run();
  process.exit(0);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
