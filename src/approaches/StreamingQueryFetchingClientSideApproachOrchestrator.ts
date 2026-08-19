import { EventEmitter, once } from "events";
import fs from "fs";
import path from "path";
import { RDFStream, RSPEngine, RSPQLParser } from "rsp-js";
import { v4 as uuidv4 } from "uuid";
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import {
  AggregationFunction,
  buildBenchmarkTopicName,
  buildBenchmarkResultPayload,
  buildBenchmarkWindowMetadata,
  getConfiguredWindowSemantics,
  getBenchmarkEventTimeAnchor,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getResultTopic,
  getSessionId,
  getBenchmarkTargetWindowCount,
  getTimestampDomainMax,
  getTimestampDomainMin,
  useCleanMqttSessionsForBenchmark,
} from "../util/runtimeConfig";
import {
  buildQueryTargetScalingSuperQuery,
  getConfiguredBenchmarkTargets,
} from "../util/queryTargets";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
import { profileCount, profileSync, writeProfileArtifact } from "../util/profiling";
import { resourceTraceSnapshot } from "../util/resourceTrace";
import {
  appendFetchingArtifactTrace,
  appendFetchingPipelineTrace,
  buildFetchingArtifactTracePath,
  buildFetchingPipelineTracePath,
  buildFetchingConsumerSummaryPath,
  FetchingConsumerSummary,
  FetchingTraceEvent,
  writeAtomicFile,
  writeFetchingConsumerSummaryAtomic,
} from "./fetchingKScalingArtifacts";
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

type ComparableWindowFlags = {
  windowDurationMs: number | null;
  coverageComplete: boolean;
  isPartialWindow: boolean;
  isComparableWindow: boolean;
};

type QueryWindowConfiguration = {
  rangeMs: number;
  stepMs: number;
};

type StreamObservation = {
  timestamp: number;
  value: number;
};

const BENCHMARK_REPLAY_QUIESCENCE_MS = 1000;
const BENCHMARK_REPLAY_SETTLE_TIMEOUT_MS = 5000;
const BENCHMARK_REPLAY_POLL_INTERVAL_MS = 50;
const FETCHING_LATENCY_HEADER =
  "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,delay_past_expected_close_ms,delay_past_data_start_ms,delay_past_last_obs_ms,window_semantics,logical_trigger_time,window_start,window_end,window_duration_ms,window_data_close_time,latency_from_logical_trigger_ms,latency_from_window_close_ms,coverage_complete,is_partial_window,is_comparable_window,metadata_source,result_value\n";

type FetchingArtifactState = {
  stateObjectId: string;
  finalizedWindowNumbers: number[];
  benchmarkTargetWindowReached: boolean;
  benchmarkStopReason:
    | "target_window_count_reached"
    | "finite_replay_duration_reached"
    | "other";
};

type FetchingLifecycleHooks = {
  onReady?: (payload: { consumerIndex: number | null; stateObjectId: string }) => void;
  onDurableArtifacts?: (payload: {
    consumerIndex: number | null;
    stateObjectId: string;
    windowNumber: number;
    summaryPath: string;
    latencyPath: string;
  }) => void;
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
  private startupFirstEmittedMode: boolean = ["1", "true", "yes", "on"].includes(
    (process.env.STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE || "").trim().toLowerCase(),
  );
  private deterministicEventTimeMode: boolean = ["1", "true", "yes", "on"].includes(
    (process.env.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME || "").trim().toLowerCase(),
  );
  private disableCadenceFilterMode: boolean = ["1", "true", "yes", "on"].includes(
    (process.env.STREAMING_QUERY_HIVE_FETCHING_DISABLE_CADENCE_FILTER || "").trim().toLowerCase(),
  );
  private timingFilterEnabled: boolean = true;
  private timingFilterBypassedReason: string | null = null;
  private filteredDueToTimingCount: number = 0;
  private acceptedCompleteWindowCount: number = 0;
  private benchmarkReplayComplete: boolean = false;
  private benchmarkTargetWindowCount: number | null =
    getBenchmarkTargetWindowCount();
  private benchmarkControlTopic: string = buildBenchmarkTopicName("__benchmark_control__");
  private mqttClients: any[] = [];
  private resourceUsageInterval: NodeJS.Timeout | null = null;
  private resourceUsageLogStream: fs.WriteStream | null = null;
  private cleanupRegistered: boolean = false;
  private cleanupPromise: Promise<void> | null = null;
  private consumerIdx: string;
  private consumerIndex: number | null;
  private readonly logRoot: string;
  private readonly latencyLogPath: string;
  private readonly consumerSummaryPath: string;
  private readonly tracePath: string;
  private readonly pipelineTracePath: string;
  private readonly artifactTracingEnabled: boolean;
  private readonly artifactState: FetchingArtifactState;
  private traceSequence: number = 0;
  private latencyRows: string[] = [];
  private durableSummaryWritten = false;
  private completionPromise: Promise<void>;
  private resolveCompletion!: () => void;
  private rejectCompletion!: (error: Error) => void;
  private completionSettled = false;
  private replayDrainStarted = false;
  /** Final result MQTT publishes must settle before finite replay completion. */
  private pendingFinalPublications = 0;
  /** Async RStream callbacks may still be processing after MQTT replay completion. */
  private pendingRStreamHandlers = 0;
  private readonly lifecycleHooks: FetchingLifecycleHooks;

  private deriveQueryWindowConfiguration(): QueryWindowConfiguration {
    const parsedQuery = this.rspql_parser.parse(this.query) as {
      s2r?: Array<{ width?: number; slide?: number }>;
    };
    const firstWindow = parsedQuery?.s2r?.[0];
    const rangeMs = Number(firstWindow?.width);
    const stepMs = Number(firstWindow?.slide);

    return {
      rangeMs:
        Number.isFinite(rangeMs) && rangeMs > 0
          ? rangeMs
          : getOutputWindowRange(),
      stepMs:
        Number.isFinite(stepMs) && stepMs > 0
          ? stepMs
          : getOutputWindowStep(),
    };
  }

  /**
   *
   * @param query
   * @param r2s_topic
   */
  constructor(
    query: string,
    r2s_topic: string,
    aggregationFunction: AggregationFunction,
    consumerIndex?: string | number,
    lifecycleHooks: FetchingLifecycleHooks = {},
  ) {
    process.env.HIVE_PROCESS_ROLE =
      process.env.HIVE_PROCESS_ROLE || "fetching_orchestrator";
    this.consumerIdx = consumerIndex ? `_consumer_${consumerIndex}` : (process.env.K_SCALING_CONSUMER_INDEX ? `_consumer_${process.env.K_SCALING_CONSUMER_INDEX}` : "");
    this.consumerIndex = consumerIndex === undefined
      ? (process.env.K_SCALING_CONSUMER_INDEX ? Number.parseInt(process.env.K_SCALING_CONSUMER_INDEX, 10) : null)
      : Number.parseInt(String(consumerIndex), 10);
    this.query = query;
    this.r2s_topic = r2s_topic;
    this.aggregationFunction = aggregationFunction;
    this.sessionId = getSessionId();
    this.rspql_parser = new RSPQLParser();
    const queryWindowConfiguration = this.deriveQueryWindowConfiguration();
    this.rsp_engine = new RSPEngine(query);
    profileCount("rsp_engines_created");
    this.rstream_emitter = this.rsp_engine.register();
    this.startTime = 0; // Will be set when first result arrives
    this.queryRegisteredTime = Date.now(); // Track when query was registered
    this.windowRange = queryWindowConfiguration.rangeMs;
    this.expectedWindowInterval = queryWindowConfiguration.stepMs;
    this.benchmarkEventTimeAnchor = getBenchmarkEventTimeAnchor();
    this.timestampDomainMin = getTimestampDomainMin();
    this.timestampDomainMax = getTimestampDomainMax();
    this.expectedEventCount = this.deriveExpectedEventCount();
    this.expectedEventCountTolerance = this.deriveExpectedEventCountTolerance(
      this.expectedEventCount,
    );
    this.timingFilterBypassedReason = this.deriveTimingFilterBypassedReason();
    this.timingFilterEnabled = this.timingFilterBypassedReason === null;
    this.logRoot = process.env.LOG_PATH || ".";
    this.latencyLogPath = path.join(
      this.logRoot,
      `fetching_latency_log${this.consumerIdx}.csv`,
    );
    this.consumerSummaryPath = buildFetchingConsumerSummaryPath(
      this.logRoot,
      this.consumerIndex ?? 0,
    );
    this.tracePath = buildFetchingArtifactTracePath(this.logRoot);
    this.pipelineTracePath = buildFetchingPipelineTracePath(this.logRoot);
    this.artifactTracingEnabled = ["1", "true", "yes", "on"].includes(
      (process.env.STREAMING_QUERY_HIVE_FETCHING_ARTIFACT_TRACE || "").trim().toLowerCase(),
    );
    this.lifecycleHooks = lifecycleHooks;
    this.artifactState = {
      stateObjectId: `${process.pid}:${this.consumerIndex ?? "standalone"}:${Date.now()}`,
      finalizedWindowNumbers: [],
      benchmarkTargetWindowReached: false,
      benchmarkStopReason: "other",
    };
    this.completionPromise = new Promise<void>((resolve, reject) => {
      this.resolveCompletion = resolve;
      this.rejectCompletion = reject;
    });

    // Initialize CSV logging for this approach
    this.initializeLogging();
    this.tracePipelineEvent("fetching_execution_created", {
      finalOutputTopic: this.r2s_topic,
      replayAnchor: this.benchmarkEventTimeAnchor,
    });
    this.log("fetching_query_registered");
    this.log(
      `Benchmark target window cap ${this.benchmarkTargetWindowCount !== null ? `enabled target=${this.benchmarkTargetWindowCount}` : "disabled"}`,
    );

    this.registerCleanupHook();
    this.subscribeRStream();
    this.startResourceUsageLogging();
    resourceTraceSnapshot("startup", "fetching orchestrator initialized");
  }

  /**
   * Initialize CSV logging for this approach
   */
  private initializeLogging() {
    fs.mkdirSync(this.logRoot, { recursive: true });
    const consumerIdx = this.consumerIdx;

    const logFilePath = path.join(this.logRoot, `fetching_client_side_log${consumerIdx}.csv`);
    this.logStream = fs.createWriteStream(logFilePath, { flags: "w" });
    this.logStream.write("timestamp,message\n");
    this.latencyRows = [];

    const diagnosticsLogFilePath = path.join(this.logRoot, `fetching_window_diagnostics${consumerIdx}.csv`);
    this.diagnosticsLogStream = fs.createWriteStream(diagnosticsLogFilePath, {
      flags: "w",
    });
    this.diagnosticsLogStream.write(
      "benchmark_event_time_anchor,window_number,window_start,window_end,event_count,expected_event_count,sum,avg,first_event_timestamp,last_event_timestamp,completeness_status,accepted_or_suppressed,reason,result_value,timing_filter_enabled,timing_filter_bypassed_reason,filtered_due_to_timing_count,accepted_complete_window_count\n",
    );
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

  private deriveTimingFilterBypassedReason(): string | null {
    if (this.disableCadenceFilterMode) {
      return "STREAMING_QUERY_HIVE_FETCHING_DISABLE_CADENCE_FILTER=1";
    }

    if (this.deterministicEventTimeMode) {
      return "STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1";
    }

    return null;
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

  private buildWindowMetadata(
    rstreamObject: any,
    logicalWindow: DerivedLogicalWindow,
    resultEmittedAt: number,
    expectedWindowClose: number,
  ) {
    const directLogicalTriggerTime = Number(rstreamObject?.logical_trigger_time);
    const directWindowDataCloseTime = Number(rstreamObject?.window_data_close_time);
    const directResultEmittedAt = Number(rstreamObject?.result_emitted_at);
    const directLatencyFromLogicalTriggerMs = Number(rstreamObject?.latency_from_logical_trigger_ms);
    const directLatencyFromWindowCloseMs = Number(rstreamObject?.latency_from_window_close_ms);
    const windowSemantics = String(
      rstreamObject?.window_semantics || getConfiguredWindowSemantics(),
    ).toLowerCase();
    const metadataSource = Number.isFinite(directLogicalTriggerTime) &&
      Number.isFinite(directWindowDataCloseTime) &&
      Number.isFinite(directResultEmittedAt)
      ? "direct"
      : "reconstructed";

    return buildBenchmarkWindowMetadata({
      windowSemantics,
      logicalTriggerTime: Number.isFinite(directLogicalTriggerTime)
        ? directLogicalTriggerTime
        : expectedWindowClose - (this.windowRange / 2),
      windowStart: logicalWindow.start,
      windowEnd: logicalWindow.end,
      windowDataCloseTime: Number.isFinite(directWindowDataCloseTime)
        ? directWindowDataCloseTime
        : expectedWindowClose,
      resultEmittedAt: Number.isFinite(directResultEmittedAt)
        ? directResultEmittedAt
        : resultEmittedAt,
      latencyFromLogicalTriggerMs: Number.isFinite(directLatencyFromLogicalTriggerMs)
        ? directLatencyFromLogicalTriggerMs
        : undefined,
      latencyFromWindowCloseMs: Number.isFinite(directLatencyFromWindowCloseMs)
        ? directLatencyFromWindowCloseMs
        : undefined,
      metadataSource,
    });
  }

  public awaitCompletion(): Promise<void> {
    return this.completionPromise;
  }

  private settleCompletion(error?: Error): void {
    if (this.completionSettled) {
      return;
    }
    this.completionSettled = true;
    if (error) {
      this.rejectCompletion(error);
      return;
    }
    this.resolveCompletion();
  }

  private traceArtifactEvent(
    event: FetchingTraceEvent,
    details: Partial<Omit<FetchingConsumerSummary, "summaryVersion">> & {
      windowNumber?: number | null;
      finalWindowCount?: number | null;
      coverageComplete?: boolean | null;
      comparable?: boolean | null;
      targetPath?: string | null;
    } = {},
  ): void {
    if (!this.artifactTracingEnabled) {
      return;
    }
    appendFetchingArtifactTrace(this.tracePath, {
      sequence: ++this.traceSequence,
      timestamp: Date.now(),
      pid: process.pid,
      consumerIndex: this.consumerIndex,
      event,
      windowNumber: details.windowNumber ?? null,
      finalWindowCount: details.finalWindowCount ?? this.artifactState.finalizedWindowNumbers.length,
      coverageComplete: details.coverageComplete ?? null,
      comparable: details.comparable ?? null,
      targetPath: details.targetPath ?? null,
      stateObjectId: this.artifactState.stateObjectId,
    });
  }

  private tracePipelineEvent(
    eventType: string,
    details: Record<string, unknown> = {},
  ): void {
    appendFetchingPipelineTrace(this.pipelineTracePath, {
      scenarioId: process.env.BENCHMARK_SCENARIO_ID || null,
      executionId: this.sessionId,
      consumerId: this.consumerIndex ?? "standalone",
      finalOutputTopic: this.r2s_topic,
      windowRangeMs: this.windowRange,
      windowStepMs: this.expectedWindowInterval,
      replayAnchor: this.benchmarkEventTimeAnchor,
      timestamp: Date.now(),
      eventType,
      ...details,
    });
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
    metadata: ReturnType<typeof buildBenchmarkWindowMetadata>,
    windowFlags: ComparableWindowFlags,
  ): string {
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

    const row = `${windowNumber},${this.queryRegisteredTime},${this.firstDataReceivedTime},${expectedWindowClose},${lastObsReceivedAt},${resultTime},${latencyFromQueryReg},${latencyFromDataStart},${latencyFromLastObs},${metadata.windowSemantics},${metadata.logicalTriggerTime ?? ""},${metadata.windowStart ?? ""},${metadata.windowEnd ?? ""},${windowFlags.windowDurationMs ?? ""},${metadata.windowDataCloseTime ?? ""},${metadata.latencyFromLogicalTriggerMs ?? ""},${metadata.latencyFromWindowCloseMs ?? ""},${windowFlags.coverageComplete},${windowFlags.isPartialWindow},${windowFlags.isComparableWindow},${metadata.metadataSource},${value}`;
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
    return row;
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
    const readinessPromises: Array<Promise<void>> = [];
    for (const stream of streams) {
      const stream_name = stream.stream_name;
      const mqtt_broker = this.returnMQTTBroker(stream_name);
      // Generate a unique clientId for persistent session
      const clientId = "client-" + Math.random().toString(16).substr(2, 8);
      const rsp_client = mqtt.connect(mqtt_broker, {
        clean: useCleanMqttSessionsForBenchmark(),
        clientId,
      });
      this.mqttClients.push(rsp_client);
      profileCount("mqtt_clients_created");
      const rsp_stream_object = this.rsp_engine.getStream(stream_name);
      const topic = new URL(stream_name).pathname.slice(1);
      readinessPromises.push(new Promise<void>((resolve, reject) => {
        let streamSubscribed = false;
        let controlSubscribed = !this.benchmarkFiniteReplayMode;
        let resolved = false;
        const markReady = () => {
          if (resolved || !streamSubscribed || !controlSubscribed) {
            return;
          }
          resolved = true;
          resolve();
        };
        rsp_client.once("error", (error: any) => {
          if (!resolved) {
            reject(error);
          }
        });
        rsp_client.on("connect", () => {
          console.log(`Connected to MQTT broker at ${mqtt_broker}`);
          rsp_client.subscribe(topic, { qos: 1 }, (err: any) => {
            if (err) {
              console.error(`Failed to subscribe to topic ${topic}:`, err);
              if (!resolved) {
                reject(err);
              }
            } else {
              console.log(`Subscribed to topic ${topic} with QoS 1`);
              this.tracePipelineEvent("stream_subscribed", {
                streamTopic: topic,
              });
              streamSubscribed = true;
              markReady();
            }
          });
          if (this.benchmarkFiniteReplayMode) {
            rsp_client.subscribe(this.benchmarkControlTopic, { qos: 1 }, (err: any) => {
              if (err) {
                console.error(`Failed to subscribe to benchmark control topic ${this.benchmarkControlTopic}:`, err);
                if (!resolved) {
                  reject(err);
                }
              } else {
                console.log(`Subscribed to benchmark control topic ${this.benchmarkControlTopic} with QoS 1`);
                controlSubscribed = true;
                markReady();
              }
            });
          }
        });
      }));

      rsp_client.on("message", async (topic: any, message: any) => {
        if (this.benchmarkFiniteReplayMode && topic === this.benchmarkControlTopic) {
          try {
            const parsed = JSON.parse(message.toString());
            if (parsed?.type === "finite_replay_complete") {
              this.benchmarkReplayComplete = true;
              if (
                this.benchmarkTargetWindowCount !== null &&
                !this.artifactState.benchmarkTargetWindowReached
              ) {
                this.artifactState.benchmarkStopReason = "finite_replay_duration_reached";
              }
              this.log(
                `Finite replay complete signal received: ${JSON.stringify({
                  topic: this.benchmarkControlTopic,
                  source: parsed?.source ?? null,
                  stateObjectId: this.artifactState.stateObjectId,
                })}`,
              );
              this.traceArtifactEvent("shutdown_requested");
              void this.handleFiniteReplayCompletion();
            }
          } catch (error) {
            console.error("Error parsing benchmark control message:", error);
            this.settleCompletion(error instanceof Error ? error : new Error(String(error)));
          }
          return;
        }

        try {
          const message_string = message.toString();
          profileCount("mqtt_messages_received");
          this.tracePipelineEvent("raw_event_received", {
            streamTopic: topic,
            payloadBytes: Buffer.byteLength(message_string, "utf8"),
          });

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
            this.tracePipelineEvent("event_timestamp_rejected", {
              streamTopic: topic,
              rawTimestampField: timestamp,
              parsedEventTimestamp: timestamp_epoch,
              candidateReason: "contaminated_timestamp",
            });
            return;
          }
          this.tracePipelineEvent("event_timestamp_extracted", {
            streamTopic: topic,
            rawTimestampField: timestamp,
            parsedEventTimestamp: timestamp_epoch,
          });
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
    return Promise.all(readinessPromises).then(() => {
      this.log("mqtt_subscription_ready");
      this.lifecycleHooks.onReady?.({
        consumerIndex: this.consumerIndex,
        stateObjectId: this.artifactState.stateObjectId,
      });
      resourceTraceSnapshot("after_mqtt_subscriptions_ready", "fetching subscriptions ready", {
        streamCount: streams.length,
      });
    });
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

  private countPendingLogicalWindows(): number {
    return Array.from(this.logicalWindowCandidates.values()).filter((candidate) => (
      candidate.logicalWindow &&
      !this.emittedLogicalWindows.has(candidate.logicalWindow.key)
    )).length;
  }

  private async handleFiniteReplayCompletion(): Promise<void> {
    if (this.replayDrainStarted) {
      return;
    }
    this.replayDrainStarted = true;
    try {
      await this.waitForReplayDrain();
      if (!this.completionSettled) {
        this.settleCompletion(new Error(
          `Fetching consumer ${this.consumerIndex ?? "standalone"} did not durably finalize a complete comparable window before replay completion`,
        ));
      }
    } catch (error) {
      this.settleCompletion(error instanceof Error ? error : new Error(String(error)));
    }
  }

  private async waitForReplayDrain(): Promise<void> {
    const startedAt = Date.now();

    while ((Date.now() - startedAt) < BENCHMARK_REPLAY_SETTLE_TIMEOUT_MS) {
      const sinceLastObservation = this.lastObservationReceivedTime === 0
        ? Number.POSITIVE_INFINITY
        : Date.now() - this.lastObservationReceivedTime;
      if (sinceLastObservation >= BENCHMARK_REPLAY_QUIESCENCE_MS) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, BENCHMARK_REPLAY_POLL_INTERVAL_MS));
    }

    this.flushPendingFinalizedWindows();

    const flushStartedAt = Date.now();
    while ((Date.now() - flushStartedAt) < BENCHMARK_REPLAY_SETTLE_TIMEOUT_MS) {
      if (this.countPendingLogicalWindows() === 0 && this.pendingFinalPublications === 0 && this.pendingRStreamHandlers === 0) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, BENCHMARK_REPLAY_POLL_INTERVAL_MS));
      this.flushPendingFinalizedWindows();
    }

    if (this.pendingFinalPublications > 0 || this.pendingRStreamHandlers > 0) {
      throw new Error(
        `Fetching consumer ${this.consumerIndex ?? "standalone"} final RStream processing/publication did not settle before replay completion`,
      );
    }

    await new Promise((resolve) => setTimeout(resolve, BENCHMARK_REPLAY_POLL_INTERVAL_MS));
  }

  private async closeWritableStream(
    stream: fs.WriteStream | null | undefined,
  ): Promise<void> {
    if (!stream || stream.destroyed) {
      return;
    }
    const finishPromise = once(stream, "finish").catch(() => undefined);
    stream.end();
    await finishPromise;
  }

  private recordFinalizedWindow(windowNumber: number): void {
    if (!this.artifactState.finalizedWindowNumbers.includes(windowNumber)) {
      this.artifactState.finalizedWindowNumbers.push(windowNumber);
      this.artifactState.finalizedWindowNumbers.sort((left, right) => left - right);
    }

    if (
      this.benchmarkTargetWindowCount !== null &&
      this.artifactState.finalizedWindowNumbers.length >= this.benchmarkTargetWindowCount
    ) {
      this.artifactState.benchmarkTargetWindowReached = true;
      this.artifactState.benchmarkStopReason = "target_window_count_reached";
      this.log(
        `Benchmark target window reached: target=${this.benchmarkTargetWindowCount} finalWindows=${this.artifactState.finalizedWindowNumbers.join(",")} stateObjectId=${this.artifactState.stateObjectId}`,
      );
    }
  }

  private computeSettledWindowAggregate(logicalWindow: DerivedLogicalWindow): {
    eventCount: number;
    sumValue: number;
    avgValue: number;
    minValue: number | null;
    maxValue: number | null;
  } {
    let eventCount = 0;
    let sumValue = 0;
    let minValue: number | null = null;
    let maxValue: number | null = null;

    for (const observations of this.observationsByStream.values()) {
      for (const observation of observations) {
        if (
          observation.timestamp >= logicalWindow.start &&
          observation.timestamp < logicalWindow.end
        ) {
          eventCount += 1;
          sumValue += observation.value;
          if (minValue === null || observation.value < minValue) {
            minValue = observation.value;
          }
          if (maxValue === null || observation.value > maxValue) {
            maxValue = observation.value;
          }
        }
      }
    }

    return {
      eventCount,
      sumValue,
      avgValue: eventCount > 0 ? sumValue / eventCount : 0,
      minValue,
      maxValue,
    };
  }

  private deriveBenchmarkWindowNumber(
    logicalWindow: DerivedLogicalWindow | null,
  ): number | null {
    if (
      !logicalWindow ||
      this.benchmarkEventTimeAnchor === null ||
      !Number.isFinite(this.expectedWindowInterval) ||
      this.expectedWindowInterval <= 0
    ) {
      return null;
    }

    const relativeOffset = logicalWindow.start - this.benchmarkEventTimeAnchor;
    const windowIndex = Math.round(relativeOffset / this.expectedWindowInterval);
    if (!Number.isFinite(windowIndex) || windowIndex < 0) {
      return null;
    }

    return windowIndex + 1;
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
      `${this.benchmarkEventTimeAnchor ?? ""},${windowNumber ?? ""},${logicalWindow.start},${logicalWindow.end},${Number.isFinite(eventCount) ? eventCount : ""},${Number.isFinite(this.expectedEventCount ?? NaN) ? this.expectedEventCount : ""},${Number.isFinite(sumValue) ? sumValue : ""},${Number.isFinite(avgValue) ? avgValue : ""},${firstEventTimestamp},${lastEventTimestamp},${completenessStatus},${acceptedOrSuppressed},${reason},${resultValue},${this.timingFilterEnabled},${this.timingFilterBypassedReason ?? ""},${this.filteredDueToTimingCount},${this.acceptedCompleteWindowCount}\n`,
    );
  }

  private buildComparableWindowFlags(
    logicalWindow: DerivedLogicalWindow,
    coverageComplete: boolean,
  ): ComparableWindowFlags {
    const windowDurationMs =
      Number.isFinite(logicalWindow.start) && Number.isFinite(logicalWindow.end)
        ? logicalWindow.end - logicalWindow.start
        : null;
    const isComparableWindow =
      coverageComplete && windowDurationMs === this.windowRange;

    return {
      windowDurationMs,
      coverageComplete,
      isPartialWindow: !isComparableWindow,
      isComparableWindow,
    };
  }

  private buildBenchmarkPayloadDetails(args: {
    windowNumber: number;
    numericValue: number;
    logicalWindow: DerivedLogicalWindow;
    windowBounds: { start: number; end: number } | null;
    eventCount: number;
    sumValue: number;
    avgValue: number;
    firstEventTimestamp: string | null;
    lastEventTimestamp: string | null;
    centeredWindowMetadata: ReturnType<typeof buildBenchmarkWindowMetadata>;
    windowFlags: ComparableWindowFlags;
  }) {
    return buildBenchmarkResultPayload(
      "fetching",
      this.aggregationFunction,
      this.sessionId,
      args.numericValue,
      args.windowNumber,
      {
        benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
        benchmarkWindowStart: args.logicalWindow.start,
        benchmarkWindowEnd: args.logicalWindow.end,
        rspWindowStart: args.windowBounds?.start ?? null,
        rspWindowEnd: args.windowBounds?.end ?? null,
        eventCount: args.eventCount,
        sumValue: args.sumValue,
        avgValue: args.avgValue,
        count: args.eventCount,
        sum: args.sumValue,
        average: args.avgValue,
        rangeMs: this.windowRange,
        stepMs: this.expectedWindowInterval,
        firstEventTimestamp: args.firstEventTimestamp,
        lastEventTimestamp: args.lastEventTimestamp,
        comparableWindow: args.windowFlags.isComparableWindow,
        coverageComplete: args.windowFlags.coverageComplete,
        isPartialWindow: args.windowFlags.isPartialWindow,
        isComparableWindow: args.windowFlags.isComparableWindow,
      },
      {
        windowSemantics: args.centeredWindowMetadata.windowSemantics,
        logicalTriggerTime: args.centeredWindowMetadata.logicalTriggerTime,
        windowStart: args.centeredWindowMetadata.windowStart,
        windowEnd: args.centeredWindowMetadata.windowEnd,
        windowDataCloseTime: args.centeredWindowMetadata.windowDataCloseTime,
        resultEmittedAt: args.centeredWindowMetadata.resultEmittedAt,
        latencyFromLogicalTriggerMs:
          args.centeredWindowMetadata.latencyFromLogicalTriggerMs,
        latencyFromWindowCloseMs:
          args.centeredWindowMetadata.latencyFromWindowCloseMs,
        windowDurationMs: args.windowFlags.windowDurationMs,
        metadataSource: args.centeredWindowMetadata.metadataSource,
      },
    );
  }

  private buildLatencyCsvContent(): string {
    return `${FETCHING_LATENCY_HEADER}${this.latencyRows.join("\n")}${this.latencyRows.length > 0 ? "\n" : ""}`;
  }

  public getLatencySnapshot(): {
    queryRegisteredAt: number;
    firstDataReceivedAt: number | null;
    lastObservationReceivedAt: number | null;
  } {
    return {
      queryRegisteredAt: this.queryRegisteredTime,
      firstDataReceivedAt: this.firstDataReceivedTime > 0 ? this.firstDataReceivedTime : null,
      lastObservationReceivedAt:
        this.lastObservationReceivedTime > 0 ? this.lastObservationReceivedTime : null,
    };
  }

  private async persistDurableArtifacts({
    windowNumber,
    windowFlags,
    latencyRow,
    resultValue,
    resultEmittedAt,
    logicalWindow,
  }: {
    windowNumber: number;
    windowFlags: ComparableWindowFlags;
    latencyRow: string;
    resultValue: number;
    resultEmittedAt: number;
    logicalWindow: DerivedLogicalWindow;
  }): Promise<void> {
    this.traceArtifactEvent("final_window_counter_incremented", {
      windowNumber,
      finalWindowCount: this.artifactState.finalizedWindowNumbers.length,
      coverageComplete: windowFlags.coverageComplete,
      comparable: windowFlags.isComparableWindow,
      targetPath: this.latencyLogPath,
    });

    this.latencyRows.push(latencyRow);
    this.traceArtifactEvent("latency_write_started", {
      windowNumber,
      coverageComplete: windowFlags.coverageComplete,
      comparable: windowFlags.isComparableWindow,
      targetPath: this.latencyLogPath,
    });
    const writeCompletedAt = new Date().toISOString();
    const summary: FetchingConsumerSummary = {
      summaryVersion: 1,
      consumerIndex: this.consumerIndex ?? 0,
      stateObjectId: this.artifactState.stateObjectId,
      queryRegisteredAt: this.queryRegisteredTime,
      firstDataReceivedAt: this.firstDataReceivedTime,
      emittedFinalWindowCount: this.artifactState.finalizedWindowNumbers.length,
      windowNumber,
      windowStart: logicalWindow.start,
      windowEnd: logicalWindow.end,
      coverageComplete: windowFlags.coverageComplete,
      isPartialWindow: windowFlags.isPartialWindow,
      isComparableWindow: windowFlags.isComparableWindow,
      resultValue,
      resultEmittedAt,
      writeCompletedAt,
      stoppedAfterTargetWindows: this.artifactState.benchmarkTargetWindowReached,
      stopReason: this.artifactState.benchmarkStopReason,
      finalWindowNumbers: [...this.artifactState.finalizedWindowNumbers],
    };
    await writeAtomicFile(this.latencyLogPath, this.buildLatencyCsvContent());
    this.traceArtifactEvent("latency_write_completed", {
      windowNumber,
      coverageComplete: windowFlags.coverageComplete,
      comparable: windowFlags.isComparableWindow,
      targetPath: this.latencyLogPath,
    });
    this.traceArtifactEvent("consumer_summary_write_started", {
      windowNumber,
      coverageComplete: windowFlags.coverageComplete,
      comparable: windowFlags.isComparableWindow,
      targetPath: this.consumerSummaryPath,
    });
    await writeFetchingConsumerSummaryAtomic(this.consumerSummaryPath, summary);
    this.traceArtifactEvent("consumer_summary_write_completed", {
      windowNumber,
      coverageComplete: windowFlags.coverageComplete,
      comparable: windowFlags.isComparableWindow,
      targetPath: this.consumerSummaryPath,
    });
    this.durableSummaryWritten = true;
    this.log(
      `Benchmark artifacts durable: consumer=${this.consumerIndex ?? "standalone"} window=${windowNumber} stateObjectId=${this.artifactState.stateObjectId}`,
    );
    this.lifecycleHooks.onDurableArtifacts?.({
      consumerIndex: this.consumerIndex,
      stateObjectId: this.artifactState.stateObjectId,
      windowNumber,
      summaryPath: this.consumerSummaryPath,
      latencyPath: this.latencyLogPath,
    });
    // In finite replay, completion is settled by the final publication
    // callback.  The durable local summary alone is not sufficient because
    // the runner's result subscriber still depends on that publication.
    if (!this.benchmarkFiniteReplayMode) {
      this.settleCompletion();
    }
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
      this.pendingRStreamHandlers += 1;
      void (async () => {
      console.log("DEBUG: RStream event received:", JSON.stringify(object));
      this.tracePipelineEvent("rstream_result_received", {
        payloadSummary: {
          hasWindow: Boolean(object?.window),
          bindingCount: Array.isArray(object?.bindings) ? object.bindings.length : 0,
        },
      });
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
        if (logicalWindow) {
          this.tracePipelineEvent("window_bounds_derived", {
            logicalWindowStart: logicalWindow.start,
            logicalWindowEnd: logicalWindow.end,
            rawTimestampField: {
              firstEventTimestamp,
              lastEventTimestamp,
            },
          });
        } else {
          this.tracePipelineEvent("window_bounds_rejected", {
            rawTimestampField: {
              firstEventTimestamp,
              lastEventTimestamp,
            },
            candidateReason: "missing_or_invalid_logical_window_bounds",
          });
        }
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
          this.tracePipelineEvent("candidate_rejected", {
            candidateReason: completeness.reason,
            rawTimestampField: {
              firstEventTimestamp,
              lastEventTimestamp,
            },
          });
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

        const benchmarkWindowNumber = this.deriveBenchmarkWindowNumber(logicalWindow);
        this.traceArtifactEvent("candidate_received", {
          windowNumber: benchmarkWindowNumber ?? undefined,
          coverageComplete: completeness.isSettled,
          comparable: completeness.isSettled,
        });

        if (this.startupFirstEmittedMode) {
          if (!Number.isFinite(benchmarkWindowNumber ?? NaN)) {
            this.log(
              `Delayed because incomplete: ${JSON.stringify({
                logicalWindowKey: logicalWindow.key,
                reason: "unable_to_derive_benchmark_window_number",
              })}`,
            );
            continue;
          }

          if (this.emittedLogicalWindows.has(logicalWindow.key)) {
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
              "duplicate_after_first_emitted_acceptance",
              data,
            );
            this.traceArtifactEvent("candidate_suppressed", {
              windowNumber: benchmarkWindowNumber ?? undefined,
              coverageComplete: false,
              comparable: false,
            });
            continue;
          }

          if (!this.canFinalizeLogicalWindow(logicalWindow.end)) {
            this.tracePipelineEvent("candidate_rejected", {
              logicalWindowStart: logicalWindow.start,
              logicalWindowEnd: logicalWindow.end,
              candidateReason: "waiting_for_all_streams_to_progress_past_window_end",
            });
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
            this.traceArtifactEvent("candidate_suppressed", {
              windowNumber: benchmarkWindowNumber ?? undefined,
              coverageComplete: false,
              comparable: false,
            });
            continue;
          }

          const settledAggregate = this.computeSettledWindowAggregate(logicalWindow);
          const settledCompleteness = this.assessWindowCompleteness(
            logicalWindow,
            settledAggregate.eventCount,
          );
          if (!settledCompleteness.isSettled) {
            this.tracePipelineEvent("candidate_rejected", {
              logicalWindowStart: logicalWindow.start,
              logicalWindowEnd: logicalWindow.end,
              candidateReason: settledCompleteness.reason,
            });
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
            continue;
          }

          if (
            this.benchmarkTargetWindowCount !== null &&
            this.acceptedCompleteWindowCount >= this.benchmarkTargetWindowCount
          ) {
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
              "target_window_count_reached",
              data,
            );
            this.artifactState.benchmarkTargetWindowReached = true;
            this.artifactState.benchmarkStopReason = "target_window_count_reached";
            this.traceArtifactEvent("candidate_suppressed", {
              windowNumber: benchmarkWindowNumber ?? undefined,
              coverageComplete: true,
              comparable: true,
            });
            continue;
          }

          let startupNumericValue = settledAggregate.avgValue;
          if (this.aggregationFunction === "SUM") {
            startupNumericValue = settledAggregate.sumValue;
          } else if (this.aggregationFunction === "COUNT") {
            startupNumericValue = settledAggregate.eventCount;
          } else if (this.aggregationFunction === "MIN") {
            if (
              settledAggregate.eventCount === 0 ||
              settledAggregate.minValue === null
            ) {
              continue;
            }
            startupNumericValue = settledAggregate.minValue;
          } else if (this.aggregationFunction === "MAX") {
            if (
              settledAggregate.eventCount === 0 ||
              settledAggregate.maxValue === null
            ) {
              continue;
            }
            startupNumericValue = settledAggregate.maxValue;
          }

          if (!Number.isFinite(startupNumericValue)) {
            this.log(
              `Delayed because incomplete: ${JSON.stringify({
                logicalWindowKey: logicalWindow.key,
                reason: "startup_numeric_value_not_finite",
              })}`,
            );
            continue;
          }

          this.acceptedCompleteWindowCount++;
          this.tracePipelineEvent("candidate_accepted", {
            logicalWindowStart: logicalWindow.start,
            logicalWindowEnd: logicalWindow.end,
            eventCount: settledAggregate.eventCount,
          });
          this.windowCount = Math.max(this.windowCount, benchmarkWindowNumber as number);
          this.emittedLogicalWindows.add(logicalWindow.key);
          this.recordFinalizedWindow(benchmarkWindowNumber as number);
          this.traceArtifactEvent("complete_window_finalized", {
            windowNumber: benchmarkWindowNumber as number,
            coverageComplete: true,
            comparable: true,
          });

          const resultEmittedAt = currentTimestamp;
          const expectedWindowClose = this.getExpectedWindowCloseTime(
            benchmarkWindowNumber as number,
          );
          const centeredWindowMetadata = this.buildWindowMetadata(
            object,
            logicalWindow,
            resultEmittedAt,
            expectedWindowClose,
          );
          const windowFlags = this.buildComparableWindowFlags(
            logicalWindow,
            settledCompleteness.isSettled,
          );
          const latencyRow = this.logLatency(
            benchmarkWindowNumber as number,
            expectedWindowClose,
            this.lastObservationReceivedTime,
            resultEmittedAt,
            String(startupNumericValue),
            centeredWindowMetadata,
            windowFlags,
          );
          await this.persistDurableArtifacts({
            windowNumber: benchmarkWindowNumber as number,
            windowFlags,
            latencyRow,
            resultValue: startupNumericValue,
            resultEmittedAt,
            logicalWindow,
          });
          this.writeWindowDiagnostics(
            benchmarkWindowNumber as number,
            logicalWindow,
            settledAggregate.eventCount,
            settledAggregate.sumValue,
            settledAggregate.avgValue,
            firstEventTimestamp,
            lastEventTimestamp,
            settledCompleteness.status,
            "accepted",
            "accepted_first_comparable_complete_window",
            String(startupNumericValue),
          );
          this.log(
            `Accepted/finalized: ${JSON.stringify({
              window_number: benchmarkWindowNumber,
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
              coverage_complete: windowFlags.coverageComplete,
              is_partial_window: windowFlags.isPartialWindow,
              is_comparable_window: windowFlags.isComparableWindow,
              timing_filter_enabled: this.timingFilterEnabled,
              timing_filter_bypassed_reason: this.timingFilterBypassedReason,
              filtered_due_to_timing_count: this.filteredDueToTimingCount,
              accepted_complete_window_count: this.acceptedCompleteWindowCount,
            })}`,
          );
          this.tracePipelineEvent("exact_result_computed", {
            logicalWindowStart: logicalWindow.start,
            logicalWindowEnd: logicalWindow.end,
            eventCount: settledAggregate.eventCount,
            resultValue: startupNumericValue,
          });

          const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
          const aggregation_object_string = useBenchmarkPayload
            ? JSON.stringify(
                  this.buildBenchmarkPayloadDetails({
                    windowNumber: benchmarkWindowNumber as number,
                    numericValue: startupNumericValue,
                    logicalWindow,
                    windowBounds,
                    eventCount: settledAggregate.eventCount,
                    sumValue: settledAggregate.sumValue,
                    avgValue: settledAggregate.avgValue,
                    firstEventTimestamp: firstEventTimestamp || null,
                    lastEventTimestamp: lastEventTimestamp || null,
                    centeredWindowMetadata,
                    windowFlags,
                  }),
                )
            : JSON.stringify(this.generate_aggregation_event(String(startupNumericValue)));
          this.log(`Generated aggregation event for result: ${data}`);

          const clientId = hash_string_md5(aggregation_object_string);
          const pubClient = mqtt.connect("mqtt://localhost:1883", {
            clean: false,
            clientId,
          });
          this.mqttClients.push(pubClient);
          profileCount("mqtt_clients_created");
          this.pendingFinalPublications += 1;
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
                  profileCount("mqtt_messages_published");
                  profileCount("emitted_results");
                  recordPublishedMqttMessage({
                    topic: this.r2s_topic,
                    payload: aggregation_object_string,
                    messageType: "superquery_result",
                    warmup: (benchmarkWindowNumber as number) === 1,
                  });
                  this.log(
                    `Successfully published result: ${data}, publish latency: ${publishEndTime - publishStartTime}ms`,
                  );
                  this.tracePipelineEvent("final_result_published", {
                    logicalWindowStart: logicalWindow.start,
                    logicalWindowEnd: logicalWindow.end,
                    resultValue: startupNumericValue,
                    finalOutputTopic: this.r2s_topic,
                  });
                }
                pubClient.end();
                this.pendingFinalPublications = Math.max(0, this.pendingFinalPublications - 1);
                if (this.benchmarkFiniteReplayMode && this.pendingFinalPublications === 0) {
                  this.settleCompletion(err ? new Error(String(err)) : undefined);
                }
              },
            );
          });
          continue;
        }

        if (!this.canFinalizeLogicalWindow(logicalWindow.end)) {
          this.tracePipelineEvent("candidate_rejected", {
            logicalWindowStart: logicalWindow.start,
            logicalWindowEnd: logicalWindow.end,
            candidateReason: "waiting_for_all_streams_to_progress_past_window_end",
          });
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
          this.traceArtifactEvent("candidate_suppressed", {
            windowNumber: benchmarkWindowNumber ?? undefined,
            coverageComplete: false,
            comparable: false,
          });
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
          this.tracePipelineEvent("candidate_rejected", {
            logicalWindowStart: logicalWindow.start,
            logicalWindowEnd: logicalWindow.end,
            candidateReason: settledCompleteness.reason,
          });
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
          this.traceArtifactEvent("candidate_suppressed", {
            windowNumber: benchmarkWindowNumber ?? undefined,
            coverageComplete: false,
            comparable: false,
          });
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
          this.traceArtifactEvent("candidate_suppressed", {
            windowNumber: benchmarkWindowNumber ?? undefined,
            coverageComplete: true,
            comparable: true,
          });
          continue;
        }

        if (
          this.benchmarkTargetWindowCount !== null &&
          this.acceptedCompleteWindowCount >= this.benchmarkTargetWindowCount
        ) {
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
            "target_window_count_reached",
            data,
          );
          this.log(
            `Suppressed complete window after benchmark target reached: ${JSON.stringify({
              logicalWindowKey: logicalWindow.key,
              targetWindowCount: this.benchmarkTargetWindowCount,
              acceptedCompleteWindowCount: this.acceptedCompleteWindowCount,
            })}`,
          );
          this.artifactState.benchmarkTargetWindowReached = true;
          this.artifactState.benchmarkStopReason = "target_window_count_reached";
          this.traceArtifactEvent("candidate_suppressed", {
            windowNumber: benchmarkWindowNumber ?? undefined,
            coverageComplete: true,
            comparable: true,
          });
          continue;
        }

        // Apply timing filter only in legacy/live mode. Deterministic benchmark
        // mode finalizes based on window completeness and logical window keys.
        if (this.timingFilterEnabled && !this.isWithinExpectedWindowTiming(currentTimestamp)) {
          this.tracePipelineEvent("candidate_rejected", {
            logicalWindowStart: logicalWindow.start,
            logicalWindowEnd: logicalWindow.end,
            candidateReason: "filtered_due_to_timing",
          });
          this.filteredDueToTimingCount += 1;
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
            "filtered_due_to_timing",
            data,
          );
          // Skip this result - it's from an extra dynamic window
          this.log(`Filtered out result due to timing: ${data}`);
          continue;
        }

        this.log(`Processing valid result: ${settledAggregate.avgValue}`);

        let numericValue = settledAggregate.avgValue;
        if (this.aggregationFunction === "SUM") {
          numericValue = settledAggregate.sumValue;
        } else if (this.aggregationFunction === "COUNT") {
          numericValue = settledAggregate.eventCount;
        } else if (this.aggregationFunction === "MIN") {
          if (settledAggregate.eventCount === 0 || settledAggregate.minValue === null) {
            this.log(`Empty window skipped for MIN`);
            continue;
          }
          numericValue = settledAggregate.minValue;
        } else if (this.aggregationFunction === "MAX") {
          if (settledAggregate.eventCount === 0 || settledAggregate.maxValue === null) {
            this.log(`Empty window skipped for MAX`);
            continue;
          }
          numericValue = settledAggregate.maxValue;
        }

        const valueStr = String(numericValue);

        // Calculate and log latency with multiple metrics
        this.windowCount++;
        this.acceptedCompleteWindowCount++;
        this.tracePipelineEvent("candidate_accepted", {
          logicalWindowStart: logicalWindow.start,
          logicalWindowEnd: logicalWindow.end,
          eventCount: settledAggregate.eventCount,
        });
        this.emittedLogicalWindows.add(logicalWindow.key);
        this.recordFinalizedWindow(this.windowCount);
        this.traceArtifactEvent("complete_window_finalized", {
          windowNumber: this.windowCount,
          coverageComplete: true,
          comparable: true,
        });
        const resultEmittedAt = Date.now();
        const expectedWindowClose = this.getExpectedWindowCloseTime(
          this.windowCount,
        );
        const centeredWindowMetadata = this.buildWindowMetadata(
          object,
          logicalWindow,
          resultEmittedAt,
          expectedWindowClose,
        );
        const windowFlags = this.buildComparableWindowFlags(
          logicalWindow,
          settledCompleteness.isSettled,
        );
        const latencyRow = this.logLatency(
          this.windowCount,
          expectedWindowClose,
          this.lastObservationReceivedTime,
          resultEmittedAt,
          valueStr,
          centeredWindowMetadata,
          windowFlags,
        );
        await this.persistDurableArtifacts({
          windowNumber: this.windowCount,
          windowFlags,
          latencyRow,
          resultValue: numericValue,
          resultEmittedAt,
          logicalWindow,
        });
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
          valueStr,
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
            coverage_complete: windowFlags.coverageComplete,
            is_partial_window: windowFlags.isPartialWindow,
            is_comparable_window: windowFlags.isComparableWindow,
            timing_filter_enabled: this.timingFilterEnabled,
            timing_filter_bypassed_reason: this.timingFilterBypassedReason,
            filtered_due_to_timing_count: this.filteredDueToTimingCount,
            accepted_complete_window_count: this.acceptedCompleteWindowCount,
          })}`,
        );
        this.tracePipelineEvent("exact_result_computed", {
          logicalWindowStart: logicalWindow.start,
          logicalWindowEnd: logicalWindow.end,
          eventCount: settledAggregate.eventCount,
          resultValue: numericValue,
        });

        // Debug: print the full binding object
        // console.log("DEBUG: RStream binding:", binding);

        const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
        const aggregation_object_string = useBenchmarkPayload
          ? JSON.stringify(
                this.buildBenchmarkPayloadDetails({
                  windowNumber: this.windowCount,
                  numericValue,
                  logicalWindow,
                  windowBounds,
                  eventCount: settledAggregate.eventCount,
                  sumValue: settledAggregate.sumValue,
                  avgValue: settledAggregate.avgValue,
                  firstEventTimestamp: firstEventTimestamp || null,
                  lastEventTimestamp: lastEventTimestamp || null,
                  centeredWindowMetadata,
                  windowFlags,
                }),
              )
          : JSON.stringify(this.generate_aggregation_event(valueStr));
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
        this.mqttClients.push(pubClient);
        profileCount("mqtt_clients_created");
        this.pendingFinalPublications += 1;
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
                profileCount("mqtt_messages_published");
                profileCount("emitted_results");
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
                this.tracePipelineEvent("final_result_published", {
                  logicalWindowStart: logicalWindow.start,
                  logicalWindowEnd: logicalWindow.end,
                  resultValue: numericValue,
                  finalOutputTopic: this.r2s_topic,
                });
              }
              pubClient.end();
              this.pendingFinalPublications = Math.max(0, this.pendingFinalPublications - 1);
              if (this.benchmarkFiniteReplayMode && this.pendingFinalPublications === 0) {
                this.settleCompletion(err ? new Error(String(err)) : undefined);
              }
            },
          );
        });
      }
      })().finally(() => {
        this.pendingRStreamHandlers = Math.max(0, this.pendingRStreamHandlers - 1);
      });
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
  public async cleanup(): Promise<void> {
    if (this.cleanupPromise) {
      return this.cleanupPromise;
    }

    this.cleanupPromise = profileSync("cleanup_time_ms", () => {
      if (this.resourceUsageInterval) {
        clearInterval(this.resourceUsageInterval);
        this.resourceUsageInterval = null;
      }
      this.traceArtifactEvent("stream_end_started", {
        targetPath: this.consumerSummaryPath,
      });
      for (const client of this.mqttClients.splice(0)) {
        try {
          client.end(true);
        } catch (error) {
          console.error("Failed to clean up fetching MQTT client:", error);
        }
      }
      return Promise.all([
        this.closeWritableStream(this.logStream),
        this.closeWritableStream(this.diagnosticsLogStream),
        this.closeWritableStream(this.resourceUsageLogStream),
      ]).then(() => {
        this.traceArtifactEvent("stream_end_completed", {
          targetPath: this.consumerSummaryPath,
        });
      });
    });
    await this.cleanupPromise;
    writeProfileArtifact();
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
    const logRoot = process.env.LOG_PATH || ".";
    fs.mkdirSync(logRoot, { recursive: true });
    const consumerIdx = this.consumerIdx;
    const targetPath = path.join(logRoot, filePath.replace(".csv", `${consumerIdx}.csv`));

    const writeHeader = !fs.existsSync(targetPath);
    this.resourceUsageLogStream = fs.createWriteStream(targetPath, { flags: "a" });
    if (writeHeader) {
      this.resourceUsageLogStream.write(
        "timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n",
      );
    }
    this.resourceUsageInterval = setInterval(() => {
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
      this.resourceUsageLogStream?.write(line);
    }, intervalMs);
    this.resourceUsageInterval.unref?.();
  }

  private registerCleanupHook() {
    if (this.cleanupRegistered) {
      return;
    }

    this.cleanupRegistered = true;
    process.once("exit", () => {
      void this.cleanup();
    });
  }
}

/**
 *
 */
async function clientSideProcessing() {
  const aggregationFunction = getConfiguredAggregation();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  const targets = getConfiguredBenchmarkTargets();
  const query = buildQueryTargetScalingSuperQuery(
    targets,
    aggregationFunction,
    outputWindowRange,
    outputWindowStep,
  );

  console.log(new RSPQLParser().parse(query).sparql);

  const r2s_topic = getResultTopic("client_operation_output");
  const client = new FetchingAllDataClientSide(
    query,
    r2s_topic,
    aggregationFunction,
  );

  // Add cleanup handlers
  process.on("exit", () => {
    void client.cleanup();
  });
  process.on("SIGINT", () => {
    client.log("Process interrupted, cleaning up...");
    void client.cleanup().finally(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    client.log("Process terminated, cleaning up...");
    void client.cleanup().finally(() => process.exit(0));
  });

  await client.process_streams();
  await client.awaitCompletion();
  await client.cleanup();
}

if (require.main === module) {
  clientSideProcessing().catch((error) => {
    console.error("Error during client-side processing:", error);
  });
}
