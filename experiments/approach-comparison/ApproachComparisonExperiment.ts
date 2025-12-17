import { EventEmitter } from "events";
import * as fs from "fs";
import * as path from "path";
import mqtt from "mqtt";
import { RSPEngine, RDFStream } from "rsp-js";
import { v4 as uuidv4 } from "uuid";
import { StreamToMQTT } from "../../src/streamer/src/publishing/StreamToMQTT";

const N3 = require("n3");
const { DataFactory } = N3;

// ============================================================================
// Types and Interfaces
// ============================================================================

interface ExperimentResult {
  approach: string;
  windowNumber: number;
  lastDataArrivalTime: number;
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
  dataFrequency: number;
  experimentDurationMs: number;
  windowWidthMs: number;
  windowSlideMs: number;
  subQueryWindowWidthMs: number;
  subQueryWindowSlideMs: number;
  dataPath: string;
}

// ============================================================================
// Queries
// ============================================================================

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

  constructor(outputDir: string) {
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
      "approach,window_number,last_data_arrival_time,result_available_time,first_event_latency_ms,result_value,timestamp\n",
    );
    this.accuracyStream.write(
      "approach,window_number,ground_truth_value,approach_value,absolute_error,percentage_error\n",
    );
  }

  logLatency(result: ExperimentResult): void {
    this.latencyStream.write(
      `${result.approach},${result.windowNumber},${result.lastDataArrivalTime},${result.resultAvailableTime},${result.firstEventLatencyMs},${result.resultValue},${result.timestamp}\n`,
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
// Data Publisher using existing StreamToMQTT
// ============================================================================

class DataPublisher {
  private wearablePublisher: StreamToMQTT;
  private smartphonePublisher: StreamToMQTT;
  private experimentStartTime: number = 0;

  constructor(private config: ExperimentConfig) {
    const wearableDataPath = path.join(
      config.dataPath,
      "wearable.acceleration.x",
      "data.nt",
    );
    const smartphoneDataPath = path.join(
      config.dataPath,
      "smartphone.acceleration.x",
      "data.nt",
    );

    this.wearablePublisher = new StreamToMQTT(
      config.mqttBroker,
      config.dataFrequency,
      wearableDataPath,
      "wearableX",
      { clientId: `wearable-pub-${uuidv4().slice(0, 8)}`, clean: true },
    );

    this.smartphonePublisher = new StreamToMQTT(
      config.mqttBroker,
      config.dataFrequency,
      smartphoneDataPath,
      "smartphoneX",
      { clientId: `smartphone-pub-${uuidv4().slice(0, 8)}`, clean: true },
    );
  }

  async start(): Promise<number> {
    console.log(
      "[DataPublisher] Starting data replay from existing datasets...",
    );
    this.experimentStartTime = Date.now();

    // Start both publishers in parallel
    Promise.all([
      this.wearablePublisher.replay_streams(),
      this.smartphonePublisher.replay_streams(),
    ])
      .then(() => {
        console.log("[DataPublisher] All streams finished replaying");
      })
      .catch((err) => {
        console.error("[DataPublisher] Error during replay:", err);
      });

    return this.experimentStartTime;
  }

  getExperimentStartTime(): number {
    return this.experimentStartTime;
  }
}

// ============================================================================
// Shared Data Arrival Tracker
// ============================================================================

class DataArrivalTracker {
  private lastArrivalTime: number = 0;
  private dataCount: number = 0;

  recordArrival(): void {
    this.lastArrivalTime = Date.now();
    this.dataCount++;
  }

  getLastArrivalTime(): number {
    return this.lastArrivalTime;
  }

  getDataCount(): number {
    return this.dataCount;
  }

  reset(): void {
    this.lastArrivalTime = Date.now();
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
  private mqttClient!: mqtt.MqttClient;
  private isRunning: boolean = false;
  private config: ExperimentConfig;
  private logger: ExperimentLogger;
  private lastResultValue: number | null = null;
  private lastResultTime: number = 0;
  private dataTracker: DataArrivalTracker = new DataArrivalTracker();

  // Track seen result values to deduplicate UNION emissions
  private recentResults: Map<number, number> = new Map(); // value -> timestamp

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

        // Track data arrival time
        this.dataTracker.recordArrival();

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
      const lastDataArrivalTime = this.dataTracker.getLastArrivalTime();

      if (!event || !event.bindings) return;

      for (const binding of event.bindings.values()) {
        const value = parseFloat(binding.value);

        // Deduplicate results from UNION clause
        // RSP-JS may emit the same result multiple times due to UNION
        const isDuplicate = this.isDuplicateResult(value, resultAvailableTime);
        if (isDuplicate) {
          console.log(
            `[FetchingClientSide] Skipping duplicate result: ${value.toFixed(6)}`,
          );
          continue;
        }

        this.lastResultTime = resultAvailableTime;
        this.lastResultValue = value;
        this.windowNumber++;

        // Calculate latency as time from last data arrival to result emission
        const firstEventLatencyMs = Math.max(
          0,
          resultAvailableTime - lastDataArrivalTime,
        );

        const result: ExperimentResult = {
          approach: "fetching_client_side",
          windowNumber: this.windowNumber,
          lastDataArrivalTime: lastDataArrivalTime,
          resultAvailableTime: resultAvailableTime,
          firstEventLatencyMs: firstEventLatencyMs,
          resultValue: value,
          timestamp: resultAvailableTime,
        };

        this.results.push(result);
        this.logger.logLatency(result);
        console.log(
          `[FetchingClientSide] Window ${this.windowNumber}: Value=${value.toFixed(6)}, Latency=${firstEventLatencyMs}ms`,
        );
      }
    });
  }

  private isDuplicateResult(value: number, timestamp: number): boolean {
    const DEDUP_WINDOW_MS = 1000; // Consider results within 1 second as potential duplicates

    // Clean up old entries
    for (const [val, ts] of this.recentResults.entries()) {
      if (timestamp - ts > DEDUP_WINDOW_MS) {
        this.recentResults.delete(val);
      }
    }

    // Check if we've seen this exact value recently
    if (this.recentResults.has(value)) {
      const lastSeen = this.recentResults.get(value)!;
      if (timestamp - lastSeen < DEDUP_WINDOW_MS) {
        return true; // This is a duplicate
      }
    }

    // Record this result
    this.recentResults.set(value, timestamp);
    return false;
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
// Approximation Approach - FIXED: Emit immediately when data complete
// ============================================================================

class ApproximationApproach {
  private mqttClient!: mqtt.MqttClient;
  private results: ExperimentResult[] = [];
  private windowNumber: number = 0;
  private isRunning: boolean = false;
  private config: ExperimentConfig;
  private logger: ExperimentLogger;
  private dataTracker: DataArrivalTracker = new DataArrivalTracker();

  // Buffers for storing values
  private subQueryBuffers: Map<
    string,
    Array<{ value: number; timestamp: number }>
  > = new Map();

  // Track when we last emitted a result to avoid re-emitting
  private lastEmissionTime: number = 0;
  private lastWindowEndTime: number = 0;

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
        this.dataTracker.recordArrival();

        // FIXED: Try to emit immediately when we have enough data
        this.tryEmitResult();
      } catch (error) {
        console.error("[Approximation] Error processing message:", error);
      }
    });
  }

  private startApproximationTimer(): void {
    // Still use timer as fallback, but emit immediately when possible
    setInterval(() => this.tryEmitResult(), this.config.windowSlideMs);
  }

  private tryEmitResult(): void {
    if (!this.isRunning) return;

    const now = Date.now();
    const lastDataArrivalTime = this.dataTracker.getLastArrivalTime();

    // Only emit if we have recent data
    if (lastDataArrivalTime === 0) return;

    // Calculate window boundaries
    const windowEndTime =
      Math.floor(now / this.config.windowSlideMs) * this.config.windowSlideMs;
    const windowStartTime = windowEndTime - this.config.windowWidthMs;

    // Avoid duplicate emissions for the same window
    if (windowEndTime === this.lastWindowEndTime) {
      return;
    }

    // Check if we have enough data for this window
    const wearableValues = this.subQueryBuffers
      .get("wearableX")!
      .filter(
        (d) => d.timestamp >= windowStartTime && d.timestamp < windowEndTime,
      )
      .map((d) => d.value);

    const smartphoneValues = this.subQueryBuffers
      .get("smartphoneX")!
      .filter(
        (d) => d.timestamp >= windowStartTime && d.timestamp < windowEndTime,
      )
      .map((d) => d.value);

    const allValues = [...wearableValues, ...smartphoneValues];

    // Need data to emit
    if (allValues.length === 0) return;

    // Only emit if we have data that's recent enough (within last slide period)
    const timeSinceLastData = now - lastDataArrivalTime;
    if (timeSinceLastData > this.config.windowSlideMs * 0.5) {
      // Data is stale, wait for more
      return;
    }

    // Compute result
    const maxValue = Math.max(...allValues);
    const resultAvailableTime = Date.now();

    this.windowNumber++;
    this.lastWindowEndTime = windowEndTime;
    this.lastEmissionTime = resultAvailableTime;

    // Calculate latency as time from last data arrival to result emission
    const firstEventLatencyMs = Math.max(
      0,
      resultAvailableTime - lastDataArrivalTime,
    );

    const result: ExperimentResult = {
      approach: "approximation",
      windowNumber: this.windowNumber,
      lastDataArrivalTime: lastDataArrivalTime,
      resultAvailableTime: resultAvailableTime,
      firstEventLatencyMs: firstEventLatencyMs,
      resultValue: maxValue,
      timestamp: resultAvailableTime,
    };

    this.results.push(result);
    this.logger.logLatency(result);
    console.log(
      `[Approximation] Window ${this.windowNumber}: Value=${maxValue.toFixed(6)}, Latency=${firstEventLatencyMs}ms`,
    );

    // Clean up old data
    const cleanupThreshold = now - this.config.windowWidthMs * 2;
    this.subQueryBuffers.set(
      "wearableX",
      this.subQueryBuffers
        .get("wearableX")!
        .filter((d) => d.timestamp >= cleanupThreshold),
    );
    this.subQueryBuffers.set(
      "smartphoneX",
      this.subQueryBuffers
        .get("smartphoneX")!
        .filter((d) => d.timestamp >= cleanupThreshold),
    );
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
  private dataTracker: DataArrivalTracker = new DataArrivalTracker();
  private experimentStartTime: number = 0;

  // Chunk-based buffers
  private chunkBuffers: Map<
    string,
    Array<{ value: number; timestamp: number; chunkId: number }>
  > = new Map();
  private chunkSize: number;
  private lastWindowEndTime: number = 0;

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

  setExperimentStartTime(startTime: number): void {
    this.experimentStartTime = startTime;
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
        const chunkId = Math.floor(
          (timestamp - this.experimentStartTime) / this.chunkSize,
        );

        this.chunkBuffers.get(topic)?.push({ value, timestamp, chunkId });
        this.dataTracker.recordArrival();

        // Try to emit immediately when we have enough chunks
        this.tryEmitResult();
      } catch (error) {
        console.error("[ChunkedQuery] Error processing message:", error);
      }
    });
  }

  private startChunkedAggregation(): void {
    // Use timer as fallback, but emit immediately when possible
    setInterval(() => this.tryEmitResult(), this.config.windowSlideMs);
  }

  private tryEmitResult(): void {
    if (!this.isRunning) return;

    const now = Date.now();
    const lastDataArrivalTime = this.dataTracker.getLastArrivalTime();

    if (lastDataArrivalTime === 0) return;

    // Calculate window boundaries
    const windowEndTime =
      Math.floor(now / this.config.windowSlideMs) * this.config.windowSlideMs;

    // Avoid duplicate emissions
    if (windowEndTime === this.lastWindowEndTime) {
      return;
    }

    // Check if we have recent data
    const timeSinceLastData = now - lastDataArrivalTime;
    if (timeSinceLastData > this.config.windowSlideMs * 0.5) {
      return;
    }

    // Process chunks within the window
    const chunksNeeded = Math.ceil(this.config.windowWidthMs / this.chunkSize);
    const currentChunkId = Math.floor(
      (now - this.experimentStartTime) / this.chunkSize,
    );
    const startChunkId = currentChunkId - chunksNeeded;

    // Get aggregated values per chunk
    const allValues: number[] = [];

    for (const [topic, buffer] of this.chunkBuffers.entries()) {
      // Filter to relevant chunks
      const relevantData = buffer.filter(
        (d) => d.chunkId >= startChunkId && d.chunkId <= currentChunkId,
      );

      // Need data to proceed
      if (relevantData.length === 0) continue;

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
        buffer.filter((d) => d.chunkId >= startChunkId - chunksNeeded),
      );
    }

    if (allValues.length === 0) return;

    // Final aggregation: MAX of all chunk MAXes
    const maxValue = Math.max(...allValues);
    const resultAvailableTime = Date.now();

    this.windowNumber++;
    this.lastWindowEndTime = windowEndTime;

    // Calculate latency as time from last data arrival to result emission
    const firstEventLatencyMs = Math.max(
      0,
      resultAvailableTime - lastDataArrivalTime,
    );

    const result: ExperimentResult = {
      approach: "chunked_query",
      windowNumber: this.windowNumber,
      lastDataArrivalTime: lastDataArrivalTime,
      resultAvailableTime: resultAvailableTime,
      firstEventLatencyMs: firstEventLatencyMs,
      resultValue: maxValue,
      timestamp: resultAvailableTime,
    };

    this.results.push(result);
    this.logger.logLatency(result);
    console.log(
      `[ChunkedQuery] Window ${this.windowNumber}: Value=${maxValue.toFixed(6)}, Latency=${firstEventLatencyMs}ms`,
    );
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
  private dataPublisher: DataPublisher;
  private fetchingApproach: FetchingClientSideApproach;
  private approximationApproach: ApproximationApproach;
  private chunkedApproach: ChunkedQueryApproach;

  constructor(outputDir: string = "./experiments/approach-comparison/results") {
    this.config = {
      mqttBroker: "mqtt://localhost:1883",
      dataFrequency: 4, // 4 events per second per stream
      experimentDurationMs: 300000, // 5 minutes - increased to collect more windows
      windowWidthMs: 120000, // 120 seconds (main query)
      windowSlideMs: 60000, // 60 seconds
      subQueryWindowWidthMs: 60000, // 60 seconds
      subQueryWindowSlideMs: 60000, // 60 seconds
      dataPath: "src/streamer/data/noisy_datasets/noise_0.5",
    };

    this.logger = new ExperimentLogger(outputDir);
    this.dataPublisher = new DataPublisher(this.config);
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
    console.log(`  - Data Path: ${this.config.dataPath}`);
    console.log(
      "============================================================\n",
    );

    try {
      // Start data publisher first and get experiment start time
      console.log("Starting data publisher (using existing StreamToMQTT)...");
      const experimentStartTime = await this.dataPublisher.start();
      console.log(`Experiment start time: ${experimentStartTime}`);

      // Set start time for chunked approach
      this.chunkedApproach.setExperimentStartTime(experimentStartTime);

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
      this.fetchingApproach.stop();
      this.approximationApproach.stop();
      this.chunkedApproach.stop();

      // Wait a moment for final results
      await this.sleep(2000);

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

    console.log(`Ground truth results: ${groundTruthResults.length}`);
    console.log(`Approximation results: ${approximationResults.length}`);
    console.log(`Chunked results: ${chunkedResults.length}`);

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

        console.log(
          `Window ${gtResult.windowNumber}: Approximation error = ${percentageError.toFixed(2)}%`,
        );
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

        console.log(
          `Window ${gtResult.windowNumber}: Chunked error = ${percentageError.toFixed(2)}%`,
        );
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
