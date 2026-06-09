import { EventEmitter } from "events";
import fs from "fs";
import { RDFStream, RSPEngine, RSPQLParser } from "rsp-js";
import { v4 as uuidv4 } from "uuid";
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import {
  AggregationFunction,
  buildBenchmarkStreamIri,
  buildBenchmarkTopicName,
  buildBenchmarkResultPayload,
  buildOutputSelectClause,
  getBenchmarkEventTimeAnchor,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getResultTopic,
  getSessionId,
  getTimestampDomainMax,
  getTimestampDomainMin,
  useCleanMqttSessionsForBenchmark,
} from "../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
const N3 = require("n3");
const mqtt = require("mqtt");
const { DataFactory } = N3;

type DerivedLogicalWindow = {
  start: number;
  end: number;
  isComplete: boolean;
  key: string;
};

type FetchingWindowCandidate = {
  logicalWindow: DerivedLogicalWindow;
  eventCount: number;
  sumValue: number;
  avgValue: number;
  firstEventTimestamp: string;
  lastEventTimestamp: string;
  resultValue: string;
  completenessStatus: string;
  completenessReason: string;
  rspWindowStart: number | null;
  rspWindowEnd: number | null;
};

type StreamObservation = {
  timestamp: number;
  value: number;
};

/**
 *
 */
export class FetchingAllDataClientSide {
  public query: string;
  public r2s_topic: string;
  public rspql_parser: RSPQLParser;
  public rsp_engine: RSPEngine;
  public rstream_emitter: EventEmitter;
  private logStream!: fs.WriteStream;
  private latencyLogStream!: fs.WriteStream;
  private windowStreamMap: { [key: string]: string } = {
    "mqtt://localhost:1883/wearableX": "https://rsp.jsw1",
    "mqtt://localhost:1883/smartphoneX": "https://rsp.jsw1",
  };
  private expectedWindowInterval: number = 60000;
  private windowRange: number = 120000;
  private tolerance: number = 5000; // 5 second tolerance
  private startTime: number = 0; // Track when processing started
  private lastValidResultTime: number = 0; // Track last valid result timing
  private queryRegisteredTime: number = 0; // Track when query was registered
  private windowCount: number = 0; // Track window count for latency logging
  private firstDataReceivedTime: number = 0; // Track when first data arrives (wall-clock)
  private lastObservationReceivedTime: number = 0; // Track when last observation was received
  private aggregationFunction: AggregationFunction;
  private sessionId: string;
  private diagnosticsLogStream!: fs.WriteStream;
  private emittedLogicalWindows: Set<string> = new Set<string>();
  private logicalWindowCandidates: Map<string, FetchingWindowCandidate> = new Map();
  private expectedEventCount: number | null = null;
  private expectedEventCountTolerance: number = 0;
  private latestObservationTimestampByStream: Map<string, number> = new Map();
  private observationsByStream: Map<string, StreamObservation[]> = new Map();
  private benchmarkEventTimeAnchor: number | null = null;
  private timestampDomainMin: number | null = null;
  private timestampDomainMax: number | null = null;
  private rejectedContaminatedTimestampCount: number = 0;
  private firstObservedEventTimestampByStream: Map<string, number> = new Map();
  private benchmarkFiniteReplayMode: boolean = ["1", "true", "yes", "on"].includes(
    (process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY || "").trim().toLowerCase(),
  );
  private benchmarkReplayComplete: boolean = false;
  private benchmarkControlTopic: string = buildBenchmarkTopicName("__benchmark_control__");

  /**
   *
   * @param query
   * @param r2s_topic
   */
  constructor(
    query: string,
    r2s_topic: string,
    aggregationFunction: AggregationFunction,
  ) {
    this.query = query;
    this.r2s_topic = r2s_topic;
    this.aggregationFunction = aggregationFunction;
    this.sessionId = getSessionId();
    this.rspql_parser = new RSPQLParser();
    this.rsp_engine = new RSPEngine(query);
    this.rstream_emitter = this.rsp_engine.register();
    this.startTime = 0; // Will be set when first result arrives
    this.queryRegisteredTime = Date.now(); // Track when query was registered
    this.windowRange = getOutputWindowRange();
    this.expectedWindowInterval = getOutputWindowStep();
    this.benchmarkEventTimeAnchor = getBenchmarkEventTimeAnchor();
    this.timestampDomainMin = getTimestampDomainMin();
    this.timestampDomainMax = getTimestampDomainMax();
    this.expectedEventCount = this.deriveExpectedEventCount();
    this.expectedEventCountTolerance = this.deriveExpectedEventCountTolerance(
      this.expectedEventCount,
    );

    // Initialize CSV logging for this approach
    this.initializeLogging();
    this.log("fetching_query_registered");

    this.subscribeRStream();
    this.startResourceUsageLogging();
  }

  /**
   * Initialize CSV logging for this approach
   */
  private initializeLogging() {
    const logFilePath = "fetching_client_side_log.csv";
    const writeHeader = !fs.existsSync(logFilePath);
    this.logStream = fs.createWriteStream(logFilePath, { flags: "a" });

    if (writeHeader) {
      this.logStream.write("timestamp,message\n");
    }

    // Initialize latency log
    const latencyLogFilePath = "fetching_latency_log.csv";
    const writeLatencyHeader = !fs.existsSync(latencyLogFilePath);
    this.latencyLogStream = fs.createWriteStream(latencyLogFilePath, {
      flags: "a",
    });

    if (writeLatencyHeader) {
      this.latencyLogStream.write(
        "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,delay_past_expected_close_ms,delay_past_data_start_ms,delay_past_last_obs_ms,result_value\n",
      );
    }

    const diagnosticsLogFilePath = "fetching_window_diagnostics.csv";
    const writeDiagnosticsHeader = !fs.existsSync(diagnosticsLogFilePath);
    this.diagnosticsLogStream = fs.createWriteStream(diagnosticsLogFilePath, {
      flags: "a",
    });

    if (writeDiagnosticsHeader) {
      this.diagnosticsLogStream.write(
        "benchmark_event_time_anchor,window_number,window_start,window_end,event_count,expected_event_count,sum,avg,first_event_timestamp,last_event_timestamp,completeness_status,accepted_or_suppressed,reason,result_value\n",
      );
    }
  }

  private deriveExpectedEventCount(): number | null {
    const frequency = Number.parseFloat(process.env.WEARABLE_FREQUENCY || "");
    if (!Number.isFinite(frequency) || frequency <= 0) {
      return null;
    }

    const streamCount = this.returnStreams().length;
    if (!Number.isFinite(streamCount) || streamCount <= 0) {
      return null;
    }

    return streamCount * frequency * (this.windowRange / 1000);
  }

  private deriveExpectedEventCountTolerance(expectedEventCount: number | null): number {
    if (!Number.isFinite(expectedEventCount ?? NaN)) {
      return 0;
    }

    return Math.max(2, Math.ceil((expectedEventCount as number) * 0.01));
  }

  private extractBindingValue(binding: any, candidateKeys: string[]): string | null {
    if (!binding) {
      return null;
    }

    if (binding instanceof Map) {
      for (const key of candidateKeys) {
        const value = binding.get(`?${key}`)?.value ?? binding.get(key)?.value;
        if (value !== undefined) {
          return value;
        }
      }
      return null;
    }

    if (binding.entries) {
      if (typeof binding.entries.get === "function") {
        for (const key of candidateKeys) {
          const term = binding.entries.get(key) ?? binding.entries.get(`?${key}`);
          if (term?.value !== undefined) {
            return term.value;
          }
        }
      } else {
        for (const key of candidateKeys) {
          const entry = binding.entries[key] ?? binding.entries[`?${key}`];
          if (entry?.value !== undefined) {
            return entry.value;
          }
        }
      }
    }

    for (const key of candidateKeys) {
      const direct = binding[key] ?? binding[`?${key}`];
      if (direct?.value !== undefined) {
        return direct.value;
      }
    }

    return null;
  }

  private extractWindowBounds(rstreamObject: any): { start: number; end: number } | null {
    const candidates: Array<{ start?: number; end?: number }> = [
      { start: rstreamObject?.window?.open, end: rstreamObject?.window?.close },
      { start: rstreamObject?.windowOpen, end: rstreamObject?.windowClose },
      { start: rstreamObject?.open, end: rstreamObject?.close },
      { start: rstreamObject?.start, end: rstreamObject?.end },
      { start: rstreamObject?.timestamp_from, end: rstreamObject?.timestamp_to },
    ];

    for (const candidate of candidates) {
      if (Number.isFinite(candidate.start) && Number.isFinite(candidate.end)) {
        return { start: candidate.start as number, end: candidate.end as number };
      }
    }

    return null;
  }

  /**
   * Log latency measurement with multiple metrics
   */
  private logLatency(
    windowNumber: number,
    expectedWindowClose: number,
    lastObsReceivedAt: number,
    resultTime: number,
    value: string,
  ) {
    // Metric 1: Delay past the expected close time
    const latencyFromQueryReg = resultTime - expectedWindowClose;

    // Metric 2: Time from first data received to result (wall-clock)
    // For window N: expected time = firstDataReceivedTime + RANGE + (N-1) * STEP
    const expectedFromDataStart =
      this.firstDataReceivedTime +
      this.windowRange +
      (windowNumber - 1) * this.expectedWindowInterval;
    const latencyFromDataStart = resultTime - expectedFromDataStart;

    // Metric 3: Time from last observation received to result emitted (processing latency)
    const latencyFromLastObs = resultTime - lastObsReceivedAt;

    if (this.latencyLogStream) {
      this.latencyLogStream.write(
        `${windowNumber},${this.queryRegisteredTime},${this.firstDataReceivedTime},${expectedWindowClose},${lastObsReceivedAt},${resultTime},${latencyFromQueryReg},${latencyFromDataStart},${latencyFromLastObs},${value}\n`,
      );
    }
    console.log(`LATENCY: Window ${windowNumber}:`);
    console.log(
      `  - Delay past expected close: ${latencyFromQueryReg}ms (expected close: ${expectedWindowClose}, result: ${resultTime})`,
    );
    console.log(
      `  - Delay past data start: ${latencyFromDataStart}ms (first data: ${this.firstDataReceivedTime}, expected: ${expectedFromDataStart}, result: ${resultTime})`,
    );
    console.log(
      `  - Processing time (last obs to result): ${latencyFromLastObs}ms`,
    );
    console.log(`  - Value: ${value}`);
  }

  /**
   * Calculate expected window close time for a given window number
   * Window N closes at: queryRegisteredTime + RANGE + (N-1) * STEP
   */
  private getExpectedWindowCloseTime(windowNumber: number): number {
    // First window closes at: queryRegisteredTime + RANGE
    // Subsequent windows close at: queryRegisteredTime + RANGE + (windowNumber - 1) * STEP
    return (
      this.queryRegisteredTime +
      this.windowRange +
      (windowNumber - 1) * this.expectedWindowInterval
    );
  }

  /**
   * Log a message with timestamp
   */
  public log(message: string) {
    const timestamp = Date.now();
    if (this.logStream) {
      this.logStream.write(`${timestamp},"${message}"\n`);
    }
    console.log(`LOG: ${timestamp} - ${message}`);
  }

  /**
   *
   */
  process_streams() {
    const streams = this.returnStreams();
    console.log("Processing streams:", streams);
    for (const stream of streams) {
      const stream_name = stream.stream_name;
      const mqtt_broker = this.returnMQTTBroker(stream_name);
      // Generate a unique clientId for persistent session
      const clientId = "client-" + Math.random().toString(16).substr(2, 8);
      const rsp_client = mqtt.connect(mqtt_broker, {
        clean: useCleanMqttSessionsForBenchmark(),
        clientId,
      });
      const rsp_stream_object = this.rsp_engine.getStream(stream_name);
      const topic = new URL(stream_name).pathname.slice(1);

      rsp_client.on("connect", () => {
        console.log(`Connected to MQTT broker at ${mqtt_broker}`);
        rsp_client.subscribe(topic, { qos: 1 }, (err: any) => {
          if (err) {
            console.error(`Failed to subscribe to topic ${topic}:`, err);
          } else {
            console.log(`Subscribed to topic ${topic} with QoS 1`);
          }
        });
        if (this.benchmarkFiniteReplayMode) {
          rsp_client.subscribe(this.benchmarkControlTopic, { qos: 1 }, (err: any) => {
            if (err) {
              console.error(`Failed to subscribe to benchmark control topic ${this.benchmarkControlTopic}:`, err);
            } else {
              console.log(`Subscribed to benchmark control topic ${this.benchmarkControlTopic} with QoS 1`);
            }
          });
        }
      });

      rsp_client.on("message", async (topic: any, message: any) => {
        if (this.benchmarkFiniteReplayMode && topic === this.benchmarkControlTopic) {
          try {
            const parsed = JSON.parse(message.toString());
            if (parsed?.type === "finite_replay_complete") {
              this.benchmarkReplayComplete = true;
              this.log(
                `Finite replay complete signal received: ${JSON.stringify({
                  topic: this.benchmarkControlTopic,
                  source: parsed?.source ?? null,
                })}`,
              );
              this.flushPendingFinalizedWindows();
            }
          } catch (error) {
            console.error("Error parsing benchmark control message:", error);
          }
          return;
        }

        try {
          const message_string = message.toString();

          // Track when data is received for latency calculations
          const now = Date.now();
          if (this.firstDataReceivedTime === 0) {
            this.firstDataReceivedTime = now;
            this.log(
              `First data received at wall-clock time: ${this.firstDataReceivedTime}`,
            );
          }
          this.lastObservationReceivedTime = now;

          const latest_event_store = await turtleStringToStore(message_string);
          const timestamp = latest_event_store.getQuads(
            null,
            DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"),
            null,
            null,
          )[0].object.value;
          const timestamp_epoch = Date.parse(timestamp);
          if (this.isContaminatedTimestamp(timestamp_epoch, stream_name)) {
            return;
          }
          const value = latest_event_store.getQuads(
            null,
            DataFactory.namedNode("https://saref.etsi.org/core/hasValue"),
            null,
            null,
          )[0]?.object?.value;
          const numericValue = Number.parseFloat(value ?? "NaN");

          if (Number.isFinite(numericValue)) {
            const observations = this.observationsByStream.get(stream_name) ?? [];
            observations.push({
              timestamp: timestamp_epoch,
              value: numericValue,
            });
            this.observationsByStream.set(stream_name, observations);
          }
          if (!this.firstObservedEventTimestampByStream.has(stream_name)) {
            this.firstObservedEventTimestampByStream.set(stream_name, timestamp_epoch);
            this.log(
              `First observed event timestamp for ${stream_name}: ${timestamp_epoch}`,
            );
          }
          this.latestObservationTimestampByStream.set(stream_name, timestamp_epoch);

          if (rsp_stream_object) {
            await this.add_event_store_to_rsp_engine(
              latest_event_store,
              rsp_stream_object,
              timestamp_epoch,
            );
          }
        } catch (error) {
          console.error("Error processing message:", error);
          this.log(`Error processing message: ${error}`);
        }
      });
    }
  }

  private isContaminatedTimestamp(timestamp: number, streamName: string): boolean {
    if (!Number.isFinite(timestamp)) {
      return true;
    }

    if (
      this.timestampDomainMin !== null &&
      timestamp < this.timestampDomainMin
    ) {
      this.rejectedContaminatedTimestampCount += 1;
      this.log(
        `Rejected contaminated timestamp: ${JSON.stringify({
          stream: streamName,
          timestamp,
          timestampDomainMin: this.timestampDomainMin,
          timestampDomainMax: this.timestampDomainMax,
          rejectedContaminatedTimestampCount:
            this.rejectedContaminatedTimestampCount,
        })}`,
      );
      return true;
    }

    if (
      this.timestampDomainMax !== null &&
      timestamp > this.timestampDomainMax
    ) {
      this.rejectedContaminatedTimestampCount += 1;
      this.log(
        `Rejected contaminated timestamp: ${JSON.stringify({
          stream: streamName,
          timestamp,
          timestampDomainMin: this.timestampDomainMin,
          timestampDomainMax: this.timestampDomainMax,
          rejectedContaminatedTimestampCount:
            this.rejectedContaminatedTimestampCount,
        })}`,
      );
      return true;
    }

    return false;
  }

  /**
   *
   */
  returnStreams() {
    const parsedQuery = this.rspql_parser.parse(this.query);
    const streams: any[] = [...parsedQuery.s2r];
    return streams;
  }

  /**
   *
   * @param stream_name
   */
  public returnMQTTBroker(stream_name: string): string {
    const url = new URL(stream_name);
    return `${url.protocol}//${url.hostname}:${url.port}/`;
  }

  /**
   *
   * @param event_store
   * @param stream_name
   * @param timestamp
   */
  public async add_event_store_to_rsp_engine(
    event_store: any,
    stream_name: RDFStream,
    timestamp: number,
  ) {
    const quads = event_store.getQuads(null, null, null, null);
    const graph = DataFactory.namedNode(stream_name.name);
    // Add each quad to the stream with the correct graph node
    for (const q of quads) {
      // Set the graph node
      const quadWithGraph = DataFactory.quad(
        q.subject,
        q.predicate,
        q.object,
        graph,
      );
      // Debug: print every quad being added
      console.log(
        `DEBUG: Adding quad to stream ${stream_name.name} at ${timestamp}:`,
        quadWithGraph.subject.value,
        quadWithGraph.predicate.value,
        quadWithGraph.object.value,
        quadWithGraph.graph.value,
      );
      stream_name.add(quadWithGraph, timestamp);
    }
  }

  /**
   * Filter results based on expected RSP-QL window timing to ignore extra dynamic windows
   * @param timestamp Current timestamp
   * @returns true if result should be processed, false if it should be ignored
   */
  private isWithinExpectedWindowTiming(timestamp: number): boolean {
    if (this.startTime === 0) {
      this.startTime = timestamp;
      this.lastValidResultTime = timestamp;
      return true; // Always accept first result
    }

    const timeSinceStart = timestamp - this.startTime;
    const timeSinceLastValid = timestamp - this.lastValidResultTime;

    // Check if this result aligns with expected window intervals (60 seconds)
    const expectedResultNumber = Math.floor(
      timeSinceStart / this.expectedWindowInterval,
    );
    const expectedTime =
      this.startTime + expectedResultNumber * this.expectedWindowInterval;
    const timeDeviation = Math.abs(timestamp - expectedTime);

    // Also check if enough time has passed since last valid result
    const isIntervalValid =
      timeSinceLastValid >= this.expectedWindowInterval - this.tolerance;

    const isValid = timeDeviation <= this.tolerance && isIntervalValid;

    if (isValid) {
      console.log(
        `FILTER: VALID result at ${timestamp}, deviation: ${timeDeviation}ms, interval: ${timeSinceLastValid}ms`,
      );
      this.lastValidResultTime = timestamp;
    } else {
      console.log(
        `FILTER: IGNORING extra window result at ${timestamp}, deviation: ${timeDeviation}ms, interval: ${timeSinceLastValid}ms`,
      );
    }

    return isValid;
  }

  private deriveLogicalWindow(
    firstEventTimestamp: string,
    lastEventTimestamp: string,
  ): DerivedLogicalWindow | null {
    const first = Date.parse(firstEventTimestamp);
    const last = Date.parse(lastEventTimestamp);
    if (!Number.isFinite(first) || !Number.isFinite(last) || last < first) {
      return null;
    }

    const observedSpan = last - first;
    const completionTolerance = 1000;
    const isComplete = observedSpan >= this.windowRange - completionTolerance;
    let start = first;
    if (this.benchmarkEventTimeAnchor !== null) {
      const relativeOffset = first - this.benchmarkEventTimeAnchor;
      const windowIndex = Math.floor(
        relativeOffset / this.expectedWindowInterval,
      );
      start =
        this.benchmarkEventTimeAnchor +
        Math.max(0, windowIndex) * this.expectedWindowInterval;
    }
    const end = start + this.windowRange;
    return {
      start,
      end,
      isComplete,
      key: `${start}:${end}`,
    };
  }

  private assessWindowCompleteness(
    logicalWindow: DerivedLogicalWindow | null,
    eventCount: number,
  ): { status: string; reason: string; isSettled: boolean } {
    if (!logicalWindow) {
      return {
        status: "invalid_window_bounds",
        reason: "missing_or_invalid_logical_window_bounds",
        isSettled: false,
      };
    }

    if (!logicalWindow.isComplete) {
      return {
        status: "incomplete_span",
        reason: "observed_timestamp_span_shorter_than_window_range",
        isSettled: false,
      };
    }

    if (Number.isFinite(this.expectedEventCount ?? NaN) && Number.isFinite(eventCount)) {
      const minimumAcceptedCount =
        (this.expectedEventCount as number) - this.expectedEventCountTolerance;
      if (eventCount < minimumAcceptedCount) {
        return {
          status: "incomplete_count",
          reason: `event_count_below_expected_threshold_${minimumAcceptedCount}`,
          isSettled: false,
        };
      }
    }

    return {
      status: "complete",
      reason: Number.isFinite(this.expectedEventCount ?? NaN)
        ? "event_count_meets_expected_threshold"
        : "logical_window_span_complete",
      isSettled: true,
    };
  }

  private canFinalizeLogicalWindow(windowEnd: number): boolean {
    const latestObservationSatisfied = this.returnStreams().every((stream) => {
      const latestTimestamp = this.latestObservationTimestampByStream.get(
        stream.stream_name,
      );
      return Number.isFinite(latestTimestamp) && (latestTimestamp as number) >= windowEnd;
    });

    return latestObservationSatisfied || (this.benchmarkFiniteReplayMode && this.benchmarkReplayComplete);
  }

  private flushPendingFinalizedWindows() {
    if (!this.benchmarkFiniteReplayMode || !this.benchmarkReplayComplete) {
      return;
    }

    const pendingCandidates = Array.from(this.logicalWindowCandidates.values())
      .filter((candidate) => candidate.logicalWindow && !this.emittedLogicalWindows.has(candidate.logicalWindow.key))
      .sort((a, b) => a.logicalWindow.start - b.logicalWindow.start);

    for (const candidate of pendingCandidates) {
      const windowStartIso = new Date(candidate.logicalWindow.start).toISOString();
      const windowEndIso = new Date(candidate.logicalWindow.end).toISOString();
      this.rstream_emitter.emit("RStream", {
        window: {
          open: candidate.logicalWindow.start,
          close: candidate.logicalWindow.end,
        },
        bindings: [
          new Map([
            ["?resultValue", { value: candidate.resultValue }],
            ["?eventCount", { value: String(candidate.eventCount) }],
            ["?sumValue", { value: String(candidate.sumValue) }],
            ["?avgValue", { value: String(candidate.avgValue) }],
            ["?firstEventTimestamp", { value: windowStartIso }],
            ["?lastEventTimestamp", { value: windowEndIso }],
          ]),
        ],
      });
    }
  }

  private computeSettledWindowAggregate(logicalWindow: DerivedLogicalWindow): {
    eventCount: number;
    sumValue: number;
    avgValue: number;
  } {
    let eventCount = 0;
    let sumValue = 0;

    for (const observations of this.observationsByStream.values()) {
      for (const observation of observations) {
        if (
          observation.timestamp >= logicalWindow.start &&
          observation.timestamp < logicalWindow.end
        ) {
          eventCount += 1;
          sumValue += observation.value;
        }
      }
    }

    return {
      eventCount,
      sumValue,
      avgValue: eventCount > 0 ? sumValue / eventCount : 0,
    };
  }

  private writeWindowDiagnostics(
    windowNumber: number | "" | null,
    logicalWindow: DerivedLogicalWindow | null,
    eventCount: number,
    sumValue: number,
    avgValue: number,
    firstEventTimestamp: string,
    lastEventTimestamp: string,
    completenessStatus: string,
    acceptedOrSuppressed: string,
    reason: string,
    resultValue: string,
  ) {
    if (!this.diagnosticsLogStream || !logicalWindow) {
      return;
    }

    this.diagnosticsLogStream.write(
      `${this.benchmarkEventTimeAnchor ?? ""},${windowNumber ?? ""},${logicalWindow.start},${logicalWindow.end},${Number.isFinite(eventCount) ? eventCount : ""},${Number.isFinite(this.expectedEventCount ?? NaN) ? this.expectedEventCount : ""},${Number.isFinite(sumValue) ? sumValue : ""},${Number.isFinite(avgValue) ? avgValue : ""},${firstEventTimestamp},${lastEventTimestamp},${completenessStatus},${acceptedOrSuppressed},${reason},${resultValue}\n`,
    );
  }

  /**
   *
   */
  public async subscribeRStream() {
    console.log("Subscribing to RStream...");
    if (!this.rstream_emitter) {
      console.error("RStream emitter is not initialized.");
      return;
    }
    this.rstream_emitter.on("error", (err: any) => {
      console.error("Error in RStream emitter:", err);
    });
    this.rstream_emitter.on("RStream", (object: any) => {
      console.log("DEBUG: RStream event received:", JSON.stringify(object));
      if (!object || !object.bindings) {
        console.error("Received invalid RStream object:", object);
        return;
      }

      // Handle bindings (rsp-js returns array of bindings or single binding map)
      // Since we have multiple variables now (?avgValue, ?countValue), we need to extract them by name.
      // object.bindings can be a single binding object or array of binding objects

      // Normalize to array
      const bindings = Array.isArray(object.bindings)
        ? object.bindings
        : [object.bindings];

      for (const binding of bindings) {
        let resultValue = null;

        if (binding instanceof Map) {
          resultValue =
            binding.get("?resultValue")?.value ??
            binding.get("?avgValue")?.value ??
            binding.get("?countValue")?.value;
        } else if (Array.isArray(binding)) {
          for (const [v, t] of binding) {
            if (
              v.value === "resultValue" ||
              v.value === "avgValue" ||
              v.value === "countValue"
            ) {
              resultValue = t.value;
            }
          }
        } else if (binding.entries) {
          try {
            if (typeof binding.entries.get === "function") {
              const resultTerm =
                binding.entries.get("resultValue") ||
                binding.entries.get("avgValue") ||
                binding.entries.get("countValue");
              if (resultTerm) resultValue = resultTerm.value;
            } else {
              for (const [key, value] of Object.entries(binding.entries)) {
                if (
                  key === "resultValue" ||
                  key === "avgValue" ||
                  key === "countValue"
                ) {
                  resultValue = (value as any).value;
                }
              }
            }
          } catch (e) {
            console.log("Error parsing binding entries:", e);
          }
        } else {
          try {
            if (binding.resultValue) resultValue = binding.resultValue.value;
            if (!resultValue && binding.avgValue) resultValue = binding.avgValue.value;
            if (!resultValue && binding.countValue) {
              resultValue = binding.countValue.value;
            }
          } catch (e) {
            console.log("Error parsing binding:", e);
          }
        }

        if (!resultValue) {
          console.log("DEBUG: Could not parse resultValue from:", binding);
          continue;
        }

        const data = resultValue;
        const currentTimestamp = Date.now();
        const windowBounds = this.extractWindowBounds(object);
        const eventCount = Number.parseFloat(
          this.extractBindingValue(binding, ["eventCount", "countValue"]) ?? "NaN",
        );
        const sumValue = Number.parseFloat(
          this.extractBindingValue(binding, ["sumValue"]) ?? "NaN",
        );
        const avgValue = Number.parseFloat(
          this.extractBindingValue(binding, ["avgValue", "resultValue"]) ?? "NaN",
        );
        const firstEventTimestamp =
          this.extractBindingValue(binding, ["firstEventTimestamp"]) ?? "";
        const lastEventTimestamp =
          this.extractBindingValue(binding, ["lastEventTimestamp"]) ?? "";
        const logicalWindow = this.deriveLogicalWindow(
          firstEventTimestamp,
          lastEventTimestamp,
        );
        const completeness = this.assessWindowCompleteness(logicalWindow, eventCount);

        this.log(
          `RStream result generated: ${data} at timestamp: ${currentTimestamp}`,
        );

        this.log(
          `Fetching candidate row seen: ${JSON.stringify({
            logicalWindowKey: logicalWindow?.key ?? null,
            windowStart: logicalWindow?.start ?? null,
            windowEnd: logicalWindow?.end ?? null,
            eventCount: Number.isFinite(eventCount) ? eventCount : null,
            expectedEventCount: this.expectedEventCount,
            completenessStatus: completeness.status,
            reason: completeness.reason,
          })}`,
        );

        if (!logicalWindow) {
          this.log(
            `Delayed because incomplete: ${JSON.stringify({
              reason: completeness.reason,
              firstEventTimestamp,
              lastEventTimestamp,
            })}`,
          );
          continue;
        }

        const previousCandidate = this.logicalWindowCandidates.get(logicalWindow.key);
        const shouldReplaceCandidate =
          !previousCandidate ||
          (Number.isFinite(eventCount) &&
            (!Number.isFinite(previousCandidate.eventCount) ||
              eventCount >= previousCandidate.eventCount));

        if (shouldReplaceCandidate) {
          this.logicalWindowCandidates.set(logicalWindow.key, {
            logicalWindow,
            eventCount,
            sumValue,
            avgValue,
            firstEventTimestamp,
            lastEventTimestamp,
            resultValue: data,
            completenessStatus: completeness.status,
            completenessReason: completeness.reason,
            rspWindowStart: windowBounds?.start ?? null,
            rspWindowEnd: windowBounds?.end ?? null,
          });
          this.log(
            `Updated candidate for same window: ${JSON.stringify({
              logicalWindowKey: logicalWindow.key,
              previousEventCount: previousCandidate?.eventCount ?? null,
              nextEventCount: Number.isFinite(eventCount) ? eventCount : null,
            })}`,
          );
        }

        if (!this.canFinalizeLogicalWindow(logicalWindow.end)) {
          this.writeWindowDiagnostics(
            "",
            logicalWindow,
            eventCount,
            sumValue,
            avgValue,
            firstEventTimestamp,
            lastEventTimestamp,
            completeness.status,
            "suppressed",
            "waiting_for_all_streams_to_progress_past_window_end",
            data,
          );
          this.log(
            `Delayed because incomplete: ${JSON.stringify({
              logicalWindowKey: logicalWindow.key,
              eventCount: Number.isFinite(eventCount) ? eventCount : null,
              expectedEventCount: this.expectedEventCount,
              completenessStatus: completeness.status,
              reason: "waiting_for_all_streams_to_progress_past_window_end",
            })}`,
          );
          continue;
        }

        const settledAggregate = this.computeSettledWindowAggregate(logicalWindow);
        const settledCompleteness = this.assessWindowCompleteness(
          logicalWindow,
          settledAggregate.eventCount,
        );

        if (!settledCompleteness.isSettled) {
          this.writeWindowDiagnostics(
            "",
            logicalWindow,
            settledAggregate.eventCount,
            settledAggregate.sumValue,
            settledAggregate.avgValue,
            firstEventTimestamp,
            lastEventTimestamp,
            settledCompleteness.status,
            "suppressed",
            settledCompleteness.reason,
            data,
          );
          this.log(
            `Delayed because incomplete: ${JSON.stringify({
              logicalWindowKey: logicalWindow.key,
              eventCount: settledAggregate.eventCount,
              expectedEventCount: this.expectedEventCount,
              completenessStatus: settledCompleteness.status,
              reason: settledCompleteness.reason,
            })}`,
          );
          continue;
        }

        if (this.emittedLogicalWindows.has(logicalWindow.key)) {
          this.writeWindowDiagnostics(
            "",
            logicalWindow,
            settledAggregate.eventCount,
            settledAggregate.sumValue,
            settledAggregate.avgValue,
            firstEventTimestamp,
            lastEventTimestamp,
            settledCompleteness.status,
            "suppressed",
            "duplicate_after_finalization",
            String(settledAggregate.avgValue),
          );
          this.log(
            `Suppressed duplicate only after finalization: ${JSON.stringify({
              logicalWindowKey: logicalWindow.key,
              eventCount: settledAggregate.eventCount,
            })}`,
          );
          continue;
        }

        // Apply timing filter to ignore extra dynamic windows
        if (!this.isWithinExpectedWindowTiming(currentTimestamp)) {
          // Skip this result - it's from an extra dynamic window
          this.log(`Filtered out result due to timing: ${data}`);
          continue;
        }

        this.log(`Processing valid result: ${settledAggregate.avgValue}`);

        // Calculate and log latency with multiple metrics
        this.windowCount++;
        this.emittedLogicalWindows.add(logicalWindow.key);
        const resultEmittedAt = Date.now();
        const expectedWindowClose = this.getExpectedWindowCloseTime(
          this.windowCount,
        );
        this.logLatency(
          this.windowCount,
          expectedWindowClose,
          this.lastObservationReceivedTime,
          resultEmittedAt,
          String(settledAggregate.avgValue),
        );
        this.writeWindowDiagnostics(
          this.windowCount,
          logicalWindow,
          settledAggregate.eventCount,
          settledAggregate.sumValue,
          settledAggregate.avgValue,
          firstEventTimestamp,
          lastEventTimestamp,
          settledCompleteness.status,
          "accepted",
          "finalized_settled_window",
          String(settledAggregate.avgValue),
        );
        this.log(
          `Accepted/finalized: ${JSON.stringify({
            window_number: this.windowCount,
            benchmark_event_time_anchor: this.benchmarkEventTimeAnchor,
            window_start: logicalWindow.start,
            window_end: logicalWindow.end,
            rsp_window_start: windowBounds?.start ?? null,
            rsp_window_end: windowBounds?.end ?? null,
            event_count: settledAggregate.eventCount,
            expected_event_count: this.expectedEventCount,
            sum: settledAggregate.sumValue,
            avg: settledAggregate.avgValue,
            first_event_timestamp: firstEventTimestamp || null,
            last_event_timestamp: lastEventTimestamp || null,
            completeness_status: settledCompleteness.status,
          })}`,
        );

        // Debug: print the full binding object
        // console.log("DEBUG: RStream binding:", binding);

        const numericValue = settledAggregate.avgValue;
        const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
        const aggregation_object_string = useBenchmarkPayload
          ? JSON.stringify(
                buildBenchmarkResultPayload(
                  "fetching",
                  this.aggregationFunction,
                  this.sessionId,
                  numericValue,
                  this.windowCount,
                  {
                    benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
                    windowStart: logicalWindow.start,
                    windowEnd: logicalWindow.end,
                    rspWindowStart: windowBounds?.start ?? null,
                    rspWindowEnd: windowBounds?.end ?? null,
                    eventCount: settledAggregate.eventCount,
                    sumValue: settledAggregate.sumValue,
                    avgValue: settledAggregate.avgValue,
                    firstEventTimestamp: firstEventTimestamp || null,
                    lastEventTimestamp: lastEventTimestamp || null,
                  },
                ),
              )
          : JSON.stringify(this.generate_aggregation_event(data));
        console.log(
          `Aggregation event generated: ${aggregation_object_string}`,
        );

        this.log(`Generated aggregation event for result: ${data}`);

        // Generate a unique clientId for persistent session
        const clientId = hash_string_md5(aggregation_object_string);
        // const clientId = 'pub-' + Math.random().toString(16).substr(2, 8);
        const pubClient = mqtt.connect("mqtt://localhost:1883", {
          clean: false,
          clientId,
        });
        pubClient.on("connect", () => {
          const publishStartTime = Date.now();
          pubClient.publish(
            this.r2s_topic,
            aggregation_object_string,
            { qos: 2 },
            (err: any) => {
              if (err) {
                console.error(
                  "Error publishing aggregation event with QoS 2:",
                  err,
                );
                this.log(`Error publishing result: ${err}`);
              } else {
                const publishEndTime = Date.now();
                recordPublishedMqttMessage({
                  topic: this.r2s_topic,
                  payload: aggregation_object_string,
                  messageType: "superquery_result",
                  warmup: this.windowCount === 1,
                });
                console.log("Aggregation event published with QoS 2");
                this.log(
                  `Successfully published result: ${data}, publish latency: ${publishEndTime - publishStartTime}ms`,
                );
              }
              pubClient.end();
            },
          );
        });
      }
    });
  }

  /**
   *
   * @param data
   */
  public generate_aggregation_event(data: any): string {
    const uuid_random = uuidv4();

    const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
    return aggregation_event.trim();
  }

  /**
   * Clean up resources
   */
  public cleanup() {
    if (this.logStream) {
      this.logStream.end();
    }
    if (this.latencyLogStream) {
      this.latencyLogStream.end();
    }
    if (this.diagnosticsLogStream) {
      this.diagnosticsLogStream.end();
    }
  }

  /**
   *
   * @param filePath
   * @param intervalMs
   */
  startResourceUsageLogging(
    filePath = "fetching_client_side_resource_usage.csv",
    intervalMs = 100,
  ) {
    const writeHeader = !fs.existsSync(filePath);
    const logStream = fs.createWriteStream(filePath, { flags: "a" });
    if (writeHeader) {
      logStream.write(
        "timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n",
      );
    }
    setInterval(() => {
      const mem = process.memoryUsage();
      const cpu = process.cpuUsage();
      const now = Date.now();
      const line =
        [
          now,
          (cpu.user / 1000).toFixed(2),
          (cpu.system / 1000).toFixed(2),
          mem.rss,
          mem.heapTotal,
          mem.heapUsed,
          (mem.heapUsed / 1024 / 1024).toFixed(2),
          mem.external,
        ].join(",") + "\n";
      logStream.write(line);
    }, intervalMs);
  }
}

/**
 *
 */
async function clientSideProcessing() {
  const aggregationFunction = getConfiguredAggregation();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  const wearableTopicName = buildBenchmarkTopicName("wearableX");
  const smartphoneTopicName = buildBenchmarkTopicName("smartphoneX");
  const wearableStreamIri = buildBenchmarkStreamIri("wearableX");
  const smartphoneStreamIri = buildBenchmarkStreamIri("smartphoneX");
  const query = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT ${buildOutputSelectClause(aggregationFunction)}
FROM NAMED WINDOW <${wearableStreamIri}> ON STREAM mqtt_broker:${wearableTopicName} [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
FROM NAMED WINDOW <${smartphoneStreamIri}> ON STREAM mqtt_broker:${smartphoneTopicName} [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
WHERE {
    {
        WINDOW <${wearableStreamIri}> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:hasTimestamp ?ts .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <${smartphoneStreamIri}> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:hasTimestamp ?ts .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
  `;

  console.log(new RSPQLParser().parse(query).sparql);

  const r2s_topic = getResultTopic("client_operation_output");
  const client = new FetchingAllDataClientSide(
    query,
    r2s_topic,
    aggregationFunction,
  );

  // Add cleanup handlers
  process.on("exit", () => client.cleanup());
  process.on("SIGINT", () => {
    client.log("Process interrupted, cleaning up...");
    client.cleanup();
    process.exit(0);
  });
  process.on("SIGTERM", () => {
    client.log("Process terminated, cleaning up...");
    client.cleanup();
    process.exit(0);
  });

  client.process_streams();
}

clientSideProcessing().catch((error) => {
  console.error("Error during client-side processing:", error);
});
