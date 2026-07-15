import { IStreamQueryOperator } from "../../util/Interfaces";
import CONFIG from "../../config/httpServerConfig.json";
import { RSPQLParser } from "rsp-js";
import fs from "fs";
import * as path from "path";
import { ExtractedQuery, QueryMap } from "../../util/Types";
import { CSVLogger } from "../../util/logger/CSVLogger";
import mqtt from "mqtt";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  buildBenchmarkWindowMetadata,
  getBenchmarkStartTime,
  getBenchmarkTargetWindowCount,
  getApproximationCompletedWindowMode,
  getApproximationEarlyTriggerMode,
  getBenchmarkEventTimeAnchor,
  getResultTopic,
  getSessionId,
  isApproximationDebugEnabled,
} from "../../util/runtimeConfig";
import { PartialChunkResult } from "../../util/chunkTypes";
import { hash_string_md5 } from "../../util/Util";
import { recordPublishedMqttMessage } from "../../util/mqttTraffic";
import { getCachedParsedQuery } from "../../util/queryCache";
import {
  endStageTimer,
  profileCount,
  profileStageSync,
  profileSync,
  startStageTimer,
  writeProfileArtifact,
  writeStageProfileArtifact,
} from "../../util/profiling";
import { ApproximationDiagnosticsWriter } from "./approximation/ApproximationDiagnosticsWriter";
import { ApproximationResultPublisher } from "./approximation/ApproximationResultPublisher";
import {
  appendTopicResult,
  cleanupOldWindows,
  computeTopicLevelApproximationResult,
  getActiveWindowCount,
  getLatestTopicValue,
  TopicWindowBuffers,
} from "./approximation/ApproximationWindowBuffer";

/**
 * Configuration interface for inactivity detection
 */
interface InactivityConfig {
  /** Minimum samples to calculate average interval */
  minSamplesForInterval?: number;
  /** Multiplier for average interval to determine timeout (adaptive) */
  inactivityMultiplier?: number;
  /** Fallback timeout for when we can't calculate interval (ms) */
  fallbackTimeoutMs?: number;
  /** Maximum timeout to prevent infinite waiting (ms) */
  maxTimeoutMs?: number;
}

const VALUE_EXTRACT_REGEX =
  /saref:hasValue>\s*"([^"]*)"(?:\^\^<[^>]*>)?/;
const OUTPUT_AGGREGATION_REGEX = /SELECT\s*\((\w+)\(/i;

type ApproximationWindowMessage = {
  kind: "structured" | "adapted_legacy";
  windowStart: number;
  windowEnd: number;
  value: number;
  aggregationType: "SUM" | "AVG" | "COUNT" | "MIN" | "MAX";
  sourceTopic: string;
};

/**
 *
 */
export class ApproximationApproachOperator implements IStreamQueryOperator {
  private logger: CSVLogger = new CSVLogger("approximation_approach_log.csv");
  private readonly debugEnabled: boolean = isApproximationDebugEnabled();
  private subQueries: string[] = [];
  private outputQuery: string = "";
  private queryMQTTTopicMap: Map<string, string> = new Map<string, string>();
  private queryFetchLocation: string = CONFIG.queryFetchLocation;
  private extractedQueries: ExtractedQuery[] = [];
  private parser: RSPQLParser = new RSPQLParser();
  private inactivityConfig: InactivityConfig;
  private diagnosticsWriter: ApproximationDiagnosticsWriter;
  private topicWindowParameters: Record<
    string,
    { width: number; aggregation: string }
  > = {};
  private windowCount: number = 0;
  private queryRegisteredTime: number = 0;
  private windowRange: number = 120000; // 120 seconds based on RANGE 120000
  private windowSlide: number = 60000; // 60 seconds based on STEP 60000
  private firstDataReceivedTime: number = 0; // Track when first data arrives (wall-clock)
  private lastDataReceivedTime: number = 0; // Track when last data was received
  private sessionId: string = getSessionId();
  private outputAggregationType: AggregationFunction = "AVG";
  private resultPublisher: ApproximationResultPublisher;
  private activeMqttClients: any[] = [];
  private cleanupRegistered: boolean = false;
  private cachedOutputQuery: string = "";
  private cachedOutputQueryParsed: any = null;
  private benchmarkEventTimeAnchor: number | null =
    getBenchmarkEventTimeAnchor();
  private runtimeReplayStartWallClockTime: number | null =
    getBenchmarkStartTime();
  private readonly completedWindowMode: boolean =
    getApproximationCompletedWindowMode();
  private readonly earlyTriggerMode: boolean =
    getApproximationEarlyTriggerMode();
  private readonly messageCounters = {
    legacy_messages_seen: 0,
    structured_messages_seen: 0,
    adapted_legacy_messages_seen: 0,
    suppressed_missing_window_metadata: 0,
  };
  private readonly benchmarkTargetWindowCount: number | null =
    getBenchmarkTargetWindowCount();
  private finalizedWindowNumbers: number[] = [];
  private benchmarkTargetWindowReached: boolean = false;
  private benchmarkStopReason:
    | "target_window_count_reached"
    | "finite_replay_duration_reached"
    | "other" = "other";
  private benchmarkWindowSummaryPath: string = path.join(
    process.env.LOG_PATH || ".",
    "benchmark_window_cap_summary.json",
  );

  /**
   * The constructor class with optional inactivity configuration.
   * @param inactivityConfig Optional configuration for data inactivity detection
   */
  constructor(inactivityConfig?: InactivityConfig) {
    // Set default configuration with optional overrides
    this.inactivityConfig = {
      minSamplesForInterval: 3, // Reduced from 10 - need faster detection
      inactivityMultiplier: 0.5, // Much more aggressive - 0.5x average instead of 5x
      fallbackTimeoutMs: 5000, // Reduced from 10s to 5s
      maxTimeoutMs: 15000, // Reduced from 3min to 15s - much more aggressive
      ...inactivityConfig,
    };
    this.queryRegisteredTime = Date.now(); // Record when query is registered
    this.diagnosticsWriter = new ApproximationDiagnosticsWriter(
      this.queryRegisteredTime,
      this.windowRange,
      this.windowSlide,
    );
    this.diagnosticsWriter.updateTimeAnchors({
      runtimeReplayStartWallClockTime: this.runtimeReplayStartWallClockTime,
      benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
    });
    this.resultPublisher = new ApproximationResultPublisher(
      CONFIG.mqttBroker,
      this.activeMqttClients,
      (error) => {
        console.error("Failed to publish aggregated results:", error);
        this.logger.log(`Failed to publish aggregated results: ${error}`);
      },
    );
    this.registerCleanupHook();
  }

  private registerCleanupHook(): void {
    if (this.cleanupRegistered) {
      return;
    }

    this.cleanupRegistered = true;
    process.once("exit", () => {
      this.cleanup();
    });
  }

  public cleanup(): void {
    profileSync("cleanup_time_ms", () => {
      this.writeBenchmarkWindowSummary();
      this.resultPublisher.cleanup();
      for (const client of this.activeMqttClients.splice(0)) {
        try {
          client.end(true);
        } catch (error) {
          console.error("Failed to close approximation MQTT client:", error);
        }
      }
      this.diagnosticsWriter.cleanup();
    });
    this.logger.log(
      `Approximation message counters: ${JSON.stringify(this.messageCounters)}`,
    );
    writeProfileArtifact();
    writeStageProfileArtifact();
  }

  private recordFinalizedWindow(windowNumber: number): void {
    if (!this.finalizedWindowNumbers.includes(windowNumber)) {
      this.finalizedWindowNumbers.push(windowNumber);
      this.finalizedWindowNumbers.sort((left, right) => left - right);
    }

    if (
      this.benchmarkTargetWindowCount !== null &&
      this.finalizedWindowNumbers.length >= this.benchmarkTargetWindowCount
    ) {
      this.benchmarkTargetWindowReached = true;
      this.benchmarkStopReason = "target_window_count_reached";
      this.logger.log(
        `Benchmark target window reached: target=${this.benchmarkTargetWindowCount} finalWindows=${this.finalizedWindowNumbers.join(",")}`,
      );
      this.writeBenchmarkWindowSummary();
      setTimeout(() => {
        this.cleanup();
        process.exit(0);
      }, 50);
    }
  }

  private writeBenchmarkWindowSummary(): void {
    if (this.benchmarkTargetWindowCount === null) {
      return;
    }

    try {
      fs.mkdirSync(path.dirname(this.benchmarkWindowSummaryPath), {
        recursive: true,
      });
      fs.writeFileSync(
        this.benchmarkWindowSummaryPath,
        JSON.stringify(
          {
            targetWindowCount: this.benchmarkTargetWindowCount,
            emittedFinalWindowCount: this.finalizedWindowNumbers.length,
            finalWindowNumbers: this.finalizedWindowNumbers,
            stoppedAfterTargetWindows: this.benchmarkTargetWindowReached,
            stopReason: this.benchmarkStopReason,
            approach: "approximation",
          },
          null,
          2,
        ),
      );
    } catch (error) {
      console.error("Error writing benchmark window summary:", error);
    }
  }

  /**
   *
   * The method adds a query to the subQueries array.
   * @param {string} query - The query to be added
   * @memberof ApproximationApproachOperator
   */
  addSubQuery(query: string): void {
    this.subQueries.push(query);
    console.log(`Sub-query added: ${query}`);
  }

  /**
   * The method returns the string array containing the subQueries.
   * @return {string[]} - The subQueries string array.
   * @memberof ApproximationApproachOperator
   */
  getSubQueries(): string[] {
    return this.subQueries;
  }

  /**
   * The method assigns the outputQuery string to the variable.
   * @param {string} query - The output query.
   * @returns {void}
   */
  addOutputQuery(query: string): void {
    this.outputQuery = query;
  }

  /**
   * Initializes different variables related to the MQTT topics and extracted queries.
   * It also initializes the method which fetches the existing queries and assigns the topic of their
   * Results stream into the map.
   * @returns {Promise<void>}
   */
  async init(): Promise<void> {
    this.queryMQTTTopicMap = new Map<string, string>();
    // Don't clear subQueries here - they should be preserved from addSubQuery() calls
    // this.subQueries = [];
    this.extractedQueries = [];
    await this.setMQTTTopicMap();
    this.topicWindowParameters = await this.createTopicWindowParameters(
      this.extractedQueries,
    );
    console.log(
      `Init completed. Current subqueries count: ${this.subQueries.length}`,
    );
  }

  /**
   * The method creates a Record of the result R2S topic of each query with the width and the aggregation function used in the RSP-QL Query.
   * @param {Array<{r2s_topic : string, rspql_query : string}>} topics - The topics array which contains the result topic where the results are being posted along with the RSP-QL Query.
   * @returns {Record<string, {width: number, aggregation : string}} - A record containing the R2S topic, width of the query and the aggregation function.
   */
  async createTopicWindowParameters(
    topics: Array<{ r2s_topic: string; rspql_query: string }>,
  ) {
    const topicWindowParameters: Record<
      string,
      { width: number; aggregation: string }
    > = {};

    for (const topic of topics) {
      try {
        const parsed = getCachedParsedQuery<any>(this.parser, topic.rspql_query);

        const width = parsed.s2r[0]?.width;
        const aggregationMatch = topic.rspql_query.match(
          OUTPUT_AGGREGATION_REGEX,
        );
        const aggregation = aggregationMatch
          ? aggregationMatch[1].toUpperCase()
          : "AVG";
        if (topic.r2s_topic && width) {
          topicWindowParameters[topic.r2s_topic] = {
            width: width,
            aggregation: aggregation,
          };
        }
      } catch (error) {
        console.log("Failed to parse query:", topic.rspql_query, error);
      }
    }

    return topicWindowParameters;
  }

  /**
   * The method does a GET request to the HTTP server to fetch the existing queries which are being executed
   * In the Query Network.
   * @returns {Promise<void>} - Returns nothing.
   */
  async setMQTTTopicMap(): Promise<void> {
    // Prefer the active benchmark attempt subqueries passed in by BeeWorker.
    // Their MQTT result topics are deterministic: chunked/<md5(query)>.
    if (this.subQueries.length > 0) {
      this.queryMQTTTopicMap = new Map<string, string>();
      this.extractedQueries = this.subQueries.map((query) => {
        const queryHash = hash_string_md5(query);
        const r2sTopic = `chunked/${queryHash}`;
        this.queryMQTTTopicMap.set(queryHash, r2sTopic);
        return {
          rspql_query: query,
          r2s_topic: r2sTopic,
        };
      });

      console.log(
        `Derived ${this.extractedQueries.length} active reusable-result topics from provided subqueries.`,
      );
      if (this.debugEnabled) {
        console.log(
          `Active approximation MQTT topics: ${JSON.stringify(Array.from(this.queryMQTTTopicMap.values()))}`,
        );
      }
      return;
    }

    const response = await fetch(this.queryFetchLocation);
    if (!response.ok) {
      throw new Error("Failed to fetch subqueries");
    }

    const data = await response.json();

    for (const [queryHash, mqttTopic] of Object.entries(data)) {
      this.queryMQTTTopicMap.set(queryHash, mqttTopic as string);
    }

    this.extractedQueries = await this.extractQueriesWithTopics(
      data as QueryMap,
    );
    console.log(
      `Extracted ${this.extractedQueries.length} queries with topics.`,
    );
  }

  /**
   * The method extracts the RSP-QL queries with their respective R2S topics from the fetched data of the HTTP Server.
   * @param {QueryMap} data - The fetched queries running in the network which are registered in the Query Service.
   * @returns {ExtractedQuery[]} - Extracted Query Array with RSP-QL query and the R2S topic location.
   */
  async extractQueriesWithTopics(data: QueryMap): Promise<ExtractedQuery[]> {
    const extractedQueries: ExtractedQuery[] = [];
    console.log(`Extracting queries with topics from data:`, data);

    for (const key in data) {
      if (data.hasOwnProperty(key)) {
        const entry = data[key];
        if (entry.rspql_query && entry.r2s_topic) {
          console.log(
            `Extracting query: ${entry.rspql_query} with topic: ${entry.r2s_topic}`,
          );

          extractedQueries.push({
            rspql_query: entry.rspql_query,
            r2s_topic: entry.r2s_topic,
          });
        }
      }
    }

    return extractedQueries;
  }
  /**
   * The method handles the aggregation with the subqueries, and aggregated based on approximating the resultant window based on the
   * Initial subqueries aggregation values.
   * @returns {Promise<void>} - Returns a void Promise.
   */
  async handleAggregation(): Promise<void> {
    if (this.subQueries.length === 0) {
      throw new Error("No subqueries to aggregate.");
    }

    const window_parameters =
      Object.keys(this.topicWindowParameters).length > 0
        ? this.topicWindowParameters
        : await this.createTopicWindowParameters(this.extractedQueries);

    if (this.queryMQTTTopicMap.size === 0) {
      console.log("No MQTT topics found for the subqueries.");
      return;
    }

    // Parsing the output RSP-QL query with it's width and the slide.
    const outputQueryParsed = getCachedParsedQuery<any>(this.parser, this.outputQuery);
    this.cachedOutputQuery = this.outputQuery;
    this.cachedOutputQueryParsed = outputQueryParsed;
    const outputQueryWidth = outputQueryParsed.s2r[0].width;
    const outputQuerySlide = outputQueryParsed.s2r[0].slide;
    const resultTopic = getResultTopic("approximation/output");
    this.windowRange = outputQueryWidth;
    this.windowSlide = outputQuerySlide;
    this.diagnosticsWriter.updateWindowConfig(
      this.windowRange,
      this.windowSlide,
    );
    this.diagnosticsWriter.updateTimeAnchors({
      runtimeReplayStartWallClockTime: this.runtimeReplayStartWallClockTime,
      benchmarkEventTimeAnchor: this.benchmarkEventTimeAnchor,
    });

    // Extract aggregation type from output query
    const outputAggregationMatch = this.outputQuery.match(
      OUTPUT_AGGREGATION_REGEX,
    );
    const outputAggregationType = outputAggregationMatch
      ? (outputAggregationMatch[1].toUpperCase() as AggregationFunction)
      : "AVG";
    this.outputAggregationType = outputAggregationType;
    this.logger.log(
      `Output query aggregation type detected: ${outputAggregationType}`,
    );
    this.logger.log(
      `Benchmark target window cap ${this.benchmarkTargetWindowCount !== null ? `enabled target=${this.benchmarkTargetWindowCount}` : "disabled"}`,
    );

    if (outputQueryParsed === null || outputQueryParsed === undefined) {
      throw new Error("Failed to parse output query.");
    }

    if (!this.extractedQueries) {
      throw new Error("No extracted queries found for aggregation.");
    }

    const s2rQueries = this.extractedQueries.map((query) =>
      getCachedParsedQuery<any>(this.parser, query.rspql_query).s2r[0],
    );
    if (s2rQueries.length === 0) {
      throw new Error("No valid s2r queries found for aggregation.");
    }

    if (outputQuerySlide <= 0 || outputQueryWidth <= 0) {
      throw new Error(
        "Invalid output query parameters: slide and width must be greater than zero. Cannot proceed with approximation approach.",
      );
    }

    // Create MQTT client for communication with the broker with different MQTT client IDs.
    const rsp_client = mqtt.connect(CONFIG.mqttBroker, {
      clientId:
        "approximation-operator-" + Math.random().toString(16).substr(2, 8),
      clean: true,
      keepalive: 60,
      reconnectPeriod: 1000,
    });
    this.activeMqttClients.push(rsp_client);
    profileCount("mqtt_clients_created");

    rsp_client.on("error", (error: any) => {
      console.error("MQTT Client Error:", error);
      this.logger.log(`MQTT Client Error: ${error}`);
    });

    rsp_client.on("offline", () => {
      console.log(
        "MQTT Client Offline. Please check the connection of the broker.",
      );
      this.logger.log(
        "MQTT Client Offline. Please check the connection of the broker.",
      );
    });

    rsp_client.on("reconnect", () => {
      console.log("Reconnecting to MQTT broker");
      this.logger.log("Reconnecting to MQTT broker");
    });

    rsp_client.on("connect", () => {
      // Successfully connected to the MQTT broker
      this.logger.log("MQTT Client Connected for Approximation Operator");
      this.logger.log(
        `Approximation mode configuration: ${JSON.stringify({
          completedWindowMode: this.completedWindowMode,
          earlyTriggerMode: this.earlyTriggerMode,
        })}`,
      );

      // Subscribe to relevant topics
      const topics = Array.from(this.queryMQTTTopicMap.values());
      if (topics.length === 0) {
        console.log("No topics to subscribe to for the values.");
        return;
      }

      const r2sTopics = topics.map((item: any) =>
        typeof item === "object" && item !== null ? item.r2s_topic : item,
      );

      // Error handling if the R2S topics are invalid
      if (r2sTopics.length === 0) {
        console.log("No valid r2s topics found for the values.");
        return;
      }

      // Subscribing to the different subQueries for the R2S result values to approximate for the output query.
      rsp_client.subscribe(r2sTopics, { qos: 1 }, (err: any) => {
        if (err) {
          console.error(`Failed to subscribe to topics ${r2sTopics.join(",")}:`, err);
          return;
        }

        console.log(
          `Successfully subscribed to ${r2sTopics.length} approximation topics`,
        );
      });

      // Use separate buffers for each topic/variable
      const windowBuffers: TopicWindowBuffers = new Map();
      const latestStructuredWindowEndByTopic: Map<string, number> = new Map();

      // Global tracking of latest values from all topics - this persists across windows
      const globalLatestValues: Map<
        string,
        { value: number; timestamp: number }
      > = new Map();
      let nextOutputWindowNumber = 1;
      let structuredWindowAnchor: number | null = this.benchmarkEventTimeAnchor;

      let lastTriggerTime = Date.now();
      let lastDataReceivedTime = Date.now(); // Track when we last received any data
      let dataReceiveCount = 0; // Count data messages received
      let averageDataInterval = 0; // Average time between data messages
      let previousDataTime = Date.now();
      let streamStartTime = Date.now(); // Track when the stream started
      const MAX_STREAM_DURATION = 240000; // Allow enough time for three 60s output windows plus buffer

      const emitStructuredReadyWindows = () => {
        const readinessStartedAt = startStageTimer();
        if (structuredWindowAnchor === null) {
          endStageTimer(
            "approximation.completed_window_readiness_check_ms",
            readinessStartedAt,
          );
          return;
        }

        while (true) {
          if (
            this.benchmarkTargetWindowCount !== null &&
            nextOutputWindowNumber > this.benchmarkTargetWindowCount
          ) {
            endStageTimer(
              "approximation.completed_window_readiness_check_ms",
              readinessStartedAt,
            );
            return;
          }

          const windowStart =
            structuredWindowAnchor +
            (nextOutputWindowNumber - 1) * outputQuerySlide;
          const windowEnd = windowStart + outputQueryWidth;
          const allTopicsClosed = r2sTopics.every((expectedTopic) => {
            const latestEnd = latestStructuredWindowEndByTopic.get(expectedTopic);
            return latestEnd !== undefined && latestEnd >= windowEnd;
          });

          if (!allTopicsClosed) {
            endStageTimer(
              "approximation.completed_window_readiness_check_ms",
              readinessStartedAt,
            );
            return;
          }

          const aggregationResults: Record<string, number | string> = {};
          const latestValues: Record<string, number> = {};
          let totalValidBuffers = 0;

          windowBuffers.forEach((buffer, topicKey) => {
            cleanupOldWindows(buffer, windowStart);
            const activeWindowCount = getActiveWindowCount(buffer);
            if (activeWindowCount > 0) {
              totalValidBuffers += 1;
              const target = { start: windowStart, end: windowEnd };
              const topicResult = profileStageSync(
                "approximation.aggregation_math_ms",
                () =>
                  computeTopicLevelApproximationResult(
                    buffer,
                    target,
                  ),
              );
              aggregationResults[topicKey] = topicResult;
              if (typeof topicResult === "number") {
                latestValues[topicKey] = topicResult;
              }
            } else {
              const mostRecentValue = getLatestTopicValue(windowBuffers.get(topicKey));
              if (mostRecentValue !== undefined) {
                latestValues[topicKey] = mostRecentValue;
              }
            }
          });

          r2sTopics.forEach((expectedTopic) => {
            if (
              latestValues[expectedTopic] === undefined &&
              windowBuffers.has(expectedTopic)
            ) {
              const mostRecentValue = getLatestTopicValue(
                windowBuffers.get(expectedTopic),
              );
              if (mostRecentValue !== undefined) {
                latestValues[expectedTopic] = mostRecentValue;
              }
            }
          });

          const allAvailableValues = Object.values(latestValues);
          if (allAvailableValues.length === 0) {
            endStageTimer(
              "approximation.completed_window_readiness_check_ms",
              readinessStartedAt,
            );
            return;
          }

          let unifiedResult: number;
          switch (outputAggregationType) {
            case "MAX":
              unifiedResult = Math.max(...allAvailableValues);
              break;
            case "MIN":
              unifiedResult = Math.min(...allAvailableValues);
              break;
            case "SUM":
              unifiedResult = allAvailableValues.reduce((sum, val) => sum + val, 0);
              break;
            case "COUNT":
              unifiedResult = allAvailableValues.length;
              break;
            case "AVG":
            default:
              unifiedResult =
                allAvailableValues.reduce((sum, val) => sum + val, 0) /
                allAvailableValues.length;
              break;
          }

          const individualTopicsResults: Record<string, number | string> = {
            ...aggregationResults,
          };
          Object.keys(latestValues).forEach((topicKey) => {
            if (individualTopicsResults[topicKey] === undefined) {
              individualTopicsResults[topicKey] = latestValues[topicKey];
            }
          });

          const currentOutputWindowNumber = nextOutputWindowNumber;
          this.windowCount = currentOutputWindowNumber;
          const resultEmittedAt = Date.now();
          const registrationAnchoredExpectedClose =
            this.diagnosticsWriter.getExpectedWindowCloseTime(
              currentOutputWindowNumber,
            );
          const alignedWindowMetadata = buildBenchmarkWindowMetadata({
            windowSemantics: process.env.RSP_WINDOW_SEMANTICS || "trailing",
            logicalTriggerTime: windowEnd - outputQueryWidth / 2,
            windowStart,
            windowEnd,
            windowDataCloseTime: windowEnd,
            resultEmittedAt,
            metadataSource: "reconstructed",
          });

          this.diagnosticsWriter.logLatency(
            currentOutputWindowNumber,
            this.firstDataReceivedTime,
            registrationAnchoredExpectedClose,
            this.lastDataReceivedTime,
            resultEmittedAt,
            String(unifiedResult),
            {
              ...alignedWindowMetadata,
              approximationStatus: "completed_window_approximation",
              coverageComplete: true,
              isPartialWindow: false,
              isComparableWindow: true,
            } as ReturnType<typeof buildBenchmarkWindowMetadata>,
          );

          const finalResult = {
            approach: "approximation",
            aggregationType: this.outputAggregationType,
            sessionId: this.sessionId,
            timestamp: resultEmittedAt,
            window: { start: windowStart, end: windowEnd },
            unifiedResult,
            value: unifiedResult,
            unifiedAverage:
              this.outputAggregationType === "AVG" ? unifiedResult : undefined,
            individualTopics: individualTopicsResults,
            metadata: {
              validBuffers: totalValidBuffers,
              expectedTopics: r2sTopics.length,
              availableTopics: Object.keys(individualTopicsResults),
              topicCount: allAvailableValues.length,
              hasMultipleTopics: allAvailableValues.length >= 2,
              latestValuesUsed: Object.keys(latestValues),
              globalValuesAvailable: globalLatestValues.size,
              usingGlobalValues: false,
              approximationStatus: "completed_window_approximation",
              registrationAnchoredExpectedClose,
              eventTimeWindowClose: windowEnd,
            },
          };

          if (rsp_client.connected) {
            const publishedPayload = profileStageSync(
              "approximation.final_payload_json_stringify_ms",
              () =>
                JSON.stringify({
                  ...finalResult,
                  ...buildBenchmarkResultPayload(
                    "approximation",
                    this.outputAggregationType,
                    this.sessionId,
                    unifiedResult,
                    currentOutputWindowNumber,
                    {},
                    {
                      windowSemantics: alignedWindowMetadata.windowSemantics,
                      logicalTriggerTime: alignedWindowMetadata.logicalTriggerTime,
                      windowStart: alignedWindowMetadata.windowStart,
                      windowEnd: alignedWindowMetadata.windowEnd,
                      windowDataCloseTime: alignedWindowMetadata.windowDataCloseTime,
                      resultEmittedAt: alignedWindowMetadata.resultEmittedAt,
                      latencyFromLogicalTriggerMs:
                        alignedWindowMetadata.latencyFromLogicalTriggerMs,
                      latencyFromWindowCloseMs:
                        alignedWindowMetadata.latencyFromWindowCloseMs,
                      windowDurationMs: windowEnd - windowStart,
                      coverageComplete: true,
                      isPartialWindow: false,
                      isComparableWindow: true,
                      metadataSource: alignedWindowMetadata.metadataSource,
                      registrationAnchoredExpectedClose,
                      eventTimeWindowClose: windowEnd,
                      approximationStatus: "completed_window_approximation",
                    },
                  ),
                }),
            );

            const publishStartedAt = startStageTimer();
            rsp_client.publish(
              resultTopic,
              publishedPayload,
              { qos: 1 },
              (error) => {
                endStageTimer(
                  "approximation.final_mqtt_publish_total_ms",
                  publishStartedAt,
                );
                if (error) {
                  console.error("Failed to publish aggregated results:", error);
                  this.logger.log(`Failed to publish aggregated results: ${error}`);
                } else {
                  recordPublishedMqttMessage({
                    topic: resultTopic,
                    payload: publishedPayload,
                    messageType: "superquery_result",
                    warmup: currentOutputWindowNumber === 1,
                  });
                  profileCount("mqtt_messages_published");
                  profileCount("emitted_results");
                  this.recordFinalizedWindow(currentOutputWindowNumber);
                }
              },
            );
          }

          nextOutputWindowNumber += 1;
        }
      };

      rsp_client.on("message", (topic: string, message: any) => {
        const callbackStartedAt = startStageTimer();
        // this.logger.log(`Received message on topic ${topic}: ${message.toString()}`);

        // Track when data is received for latency calculations
        const nowForLatency = Date.now();
        if (this.firstDataReceivedTime === 0) {
          this.firstDataReceivedTime = nowForLatency;
          this.logger.log(
            `First data received at wall-clock time: ${this.firstDataReceivedTime}`,
          );
        }
        this.lastDataReceivedTime = nowForLatency;

        try {
          const data = message.toString();
          profileCount("mqtt_messages_received");
          const structuredWindowMessage = this.parseApproximationWindowMessage(
            data,
            topic,
            outputAggregationType,
          );
          if (structuredWindowMessage) {
            profileStageSync("approximation.structured_branch_decision_ms", () => {
              this.logger.log(
                `Approximation branch decision: topic=${topic} branch=${structuredWindowMessage.kind} window_start=${structuredWindowMessage.windowStart} window_end=${structuredWindowMessage.windowEnd}`,
              );
            });
            if (structuredWindowAnchor === null) {
              structuredWindowAnchor = structuredWindowMessage.windowStart;
            }

            profileStageSync("approximation.buffer_update_ms", () => {
              appendTopicResult(windowBuffers, topic, {
                start: structuredWindowMessage.windowStart,
                end: structuredWindowMessage.windowEnd,
                value: structuredWindowMessage.value,
                agg: structuredWindowMessage.aggregationType,
              });
              latestStructuredWindowEndByTopic.set(
                topic,
                structuredWindowMessage.windowEnd,
              );
              globalLatestValues.set(topic, {
                value: structuredWindowMessage.value,
                timestamp: structuredWindowMessage.windowEnd,
              });
            });
            emitStructuredReadyWindows();
            endStageTimer(
              "approximation.mqtt_message_callback_total_ms",
              callbackStartedAt,
            );
            return;
          }

          if (this.completedWindowMode && !this.earlyTriggerMode) {
            this.messageCounters.legacy_messages_seen += 1;
            this.messageCounters.suppressed_missing_window_metadata += 1;
            this.logger.log(
              `Approximation branch decision: topic=${topic} branch=suppressed_missing_window_metadata payload=${data}`,
            );
            endStageTimer(
              "approximation.mqtt_message_callback_total_ms",
              callbackStartedAt,
            );
            return;
          }

          // Parse the RDF triple to extract the numeric value
          // Look for patterns like: hasValue> "number"^^<type>
          const valueMatch = data.match(VALUE_EXTRACT_REGEX);
          let value: number;

          if (valueMatch && valueMatch[1]) {
            value = parseFloat(valueMatch[1]);
          } else {
            // Fallback: try to parse as direct number
            value = parseFloat(data);
          }

          if (isNaN(value)) {
            this.logger.log(`Failed to parse numeric value from: ${data}`);
            return;
          }

          const now = Date.now();
          lastDataReceivedTime = now; // Update last data received time

          // Calculate adaptive data interval for inactivity detection
          if (dataReceiveCount > 0) {
            const currentInterval = now - previousDataTime;
            if (dataReceiveCount === 1) {
              averageDataInterval = currentInterval;
            } else {
              // Running average of data intervals
              averageDataInterval =
                (averageDataInterval * (dataReceiveCount - 1) +
                  currentInterval) /
                dataReceiveCount;
            }

            // Log interval calculation progress every 10 messages
            if (dataReceiveCount % 10 === 0) {
              this.logger.log(
                `Data interval stats: count=${dataReceiveCount}, avgInterval=${averageDataInterval.toFixed(2)}ms, currentInterval=${currentInterval}ms`,
              );
            }
          }
          dataReceiveCount++;
          previousDataTime = now;

          const params = window_parameters[topic];

          if (!params) {
            console.log(
              `[DEBUG] No window parameters found for topic ${topic}. Available topics in window_parameters:`,
              Object.keys(window_parameters),
            );
            this.logger.log(
              `No window parameters found for topic ${topic}. Available topics: ${JSON.stringify(Object.keys(window_parameters))}`,
            );
            console.error(`No window parameters found for topic ${topic}`);
            return;
          }

          const windowEnd = now;
          const windowStart = windowEnd - params.width;

          const result = {
            start: windowStart,
            end: windowEnd,
            value: value,
            agg: params.aggregation as "SUM" | "AVG" | "COUNT" | "MIN" | "MAX",
          };

          // this.logger.log(`Pushed window result to buffer: ${JSON.stringify(result)}`);
          if (!result.value && result.value !== 0) {
            this.logger.log(
              `Received null or undefined value for topic ${topic}. Skipping.`,
            );
            // Still add to buffer for this topic
            appendTopicResult(windowBuffers, topic, result);
            profileCount("buffered_subwindow_results");
            return;
          }

          appendTopicResult(windowBuffers, topic, result);
          profileCount("buffered_subwindow_results");

          // ALWAYS update global latest values for ALL topics receiving data
          globalLatestValues.set(topic, { value: value, timestamp: now });
          this.logger.log(
            `Updated global latest value for topic ${topic}: ${value} at ${now}`,
          );

          if (this.debugEnabled) {
            const allTopicBufferSizes: Record<string, number> = {};
            windowBuffers.forEach((buffer, topicKey) => {
              allTopicBufferSizes[topicKey] = getActiveWindowCount(buffer);
            });
            this.logger.log(
              `After adding result - Topic ${topic} buffer size: ${getActiveWindowCount(windowBuffers.get(topic))}; all topic buffer sizes: ${JSON.stringify(allTopicBufferSizes)}`,
            );
          }

          if (Date.now() - lastTriggerTime >= outputQuerySlide) {
            // Check maximum stream duration first
            const totalStreamDuration = Date.now() - streamStartTime;
            if (totalStreamDuration > MAX_STREAM_DURATION) {
              this.logger.log(
                `Stopping aggregation due to maximum stream duration reached: ${totalStreamDuration}ms (max: ${MAX_STREAM_DURATION}ms)`,
              );
              return; // Stop after maximum duration regardless of data activity
            }

            // Calculate adaptive timeout based on data frequency
            let adaptiveTimeout: number;

            if (
              dataReceiveCount >=
                this.inactivityConfig.minSamplesForInterval! &&
              averageDataInterval > 0
            ) {
              // Use adaptive timeout based on actual data frequency
              adaptiveTimeout = Math.min(
                averageDataInterval *
                  this.inactivityConfig.inactivityMultiplier!,
                this.inactivityConfig.maxTimeoutMs!,
              );
              this.logger.log(
                `Using adaptive timeout: ${adaptiveTimeout}ms (avg interval: ${averageDataInterval}ms, multiplier: ${this.inactivityConfig.inactivityMultiplier})`,
              );
            } else {
              // Use fallback timeout when we don't have enough samples
              adaptiveTimeout = this.inactivityConfig.fallbackTimeoutMs!;
              this.logger.log(
                `Using fallback timeout: ${adaptiveTimeout}ms (insufficient samples: ${dataReceiveCount})`,
              );
            }

            // Check if we haven't received data for too long (data stream ended)
            const timeSinceLastData = Date.now() - lastDataReceivedTime;
            if (timeSinceLastData > adaptiveTimeout) {
              this.logger.log(
                `Stopping aggregation due to data inactivity. Time since last data: ${timeSinceLastData}ms (adaptive timeout: ${adaptiveTimeout}ms)`,
              );
              return; // Stop triggering when no data received for extended period
            }

            const windowStartGlobal = now - outputQueryWidth;

            // Check how many topics have valid data in the current window BEFORE cleanup
            let topicsWithValidData = 0;
            windowBuffers.forEach((buffer) => {
              const activeWindowCount = getActiveWindowCount(buffer);
              if (
                activeWindowCount > 0 &&
                buffer.windows[buffer.windows.length - 1].end >= windowStartGlobal
              ) {
                topicsWithValidData++;
              }
            });

            // More flexible waiting strategy - wait a bit longer for all topics
            const bufferTimeMs = 3000; // 3 second buffer to allow for timing differences
            const shouldWaitForMoreTopics =
              topicsWithValidData < r2sTopics.length &&
              Date.now() - lastTriggerTime < outputQuerySlide + bufferTimeMs;

            if (shouldWaitForMoreTopics) {
              this.logger.log(
                `Waiting for more topics. Topics with valid data: ${topicsWithValidData}, Expected: ${r2sTopics.length}, Time since trigger: ${Date.now() - lastTriggerTime}ms`,
              );
              return; // Wait a bit more for other topics
            }

            this.logger.log(
              `Triggering aggregation for window [${windowStartGlobal}, ${now}]`,
            );

            // Log buffer sizes before cleanup for each topic
            const topicBufferSizes: Record<string, number> = {};
            windowBuffers.forEach((buffer, topicKey) => {
              topicBufferSizes[topicKey] = getActiveWindowCount(buffer);
            });
            this.logger.log(
              `Current window buffer sizes before cleanup: ${JSON.stringify(topicBufferSizes)}`,
            );

            // Clean up old windows for each topic buffer and collect latest values
            const aggregationResults: Record<string, number | string> = {};
            const latestValues: Record<string, number> = {}; // Keep track of latest values from each topic
            let totalValidBuffers = 0;

            windowBuffers.forEach((buffer, topicKey) => {
              cleanupOldWindows(buffer, windowStartGlobal);

              this.logger.log(
                `Topic ${topicKey} buffer size after cleanup: ${getActiveWindowCount(buffer)}`,
              );
              if (this.debugEnabled) {
                this.logger.log(
                  `Topic ${topicKey} buffer contents after cleanup: ${JSON.stringify(buffer.windows.slice(buffer.headIndex))}`,
                );
              }

              const activeWindowCount = getActiveWindowCount(buffer);
              if (activeWindowCount > 0) {
                totalValidBuffers++;
                const target = { start: windowStartGlobal, end: now };
                const topicResult = computeTopicLevelApproximationResult(
                  buffer,
                  target,
                );
                aggregationResults[topicKey] = topicResult;

                // Store the latest value for cross-sensor averaging
                if (typeof topicResult === "number") {
                  latestValues[topicKey] = topicResult;
                }

                this.logger.log(
                  `Aggregation result for topic ${topicKey}: ${topicResult}`,
                );
              } else {
                const allTopicData = windowBuffers.get(topicKey);
                const mostRecentValue = getLatestTopicValue(allTopicData);
                if (mostRecentValue !== undefined) {
                  latestValues[topicKey] = mostRecentValue;
                  this.logger.log(
                    `Using most recent value for topic ${topicKey}: ${mostRecentValue} (outside current window)`,
                  );
                }
              }
            });

            // Also check if we have any recent data for expected topics that might not be in windowBuffers yet
            r2sTopics.forEach((expectedTopic) => {
              if (
                !latestValues[expectedTopic] &&
                windowBuffers.has(expectedTopic)
              ) {
                const mostRecentValue = getLatestTopicValue(
                  windowBuffers.get(expectedTopic),
                );
                if (mostRecentValue !== undefined) {
                  latestValues[expectedTopic] = mostRecentValue;
                  this.logger.log(
                    `Added latest value for expected topic ${expectedTopic}: ${mostRecentValue}`,
                  );
                }
              }
            });

            lastTriggerTime = Date.now();

            // Publish results if we have at least one valid buffer or latest values from multiple topics
            const hasMultipleTopicData = Object.keys(latestValues).length >= 2;
            const hasAnyValidData =
              totalValidBuffers > 0 || Object.keys(latestValues).length > 0;

            if (
              this.benchmarkTargetWindowCount !== null &&
              this.windowCount >= this.benchmarkTargetWindowCount
            ) {
              this.logger.log(
                `Benchmark target window cap reached before legacy aggregation: target=${this.benchmarkTargetWindowCount} emitted=${this.windowCount}`,
              );
              this.benchmarkTargetWindowReached = true;
              this.benchmarkStopReason = "target_window_count_reached";
              this.writeBenchmarkWindowSummary();
              setTimeout(() => {
                this.cleanup();
                process.exit(0);
              }, 50);
              return;
            }

            if (hasAnyValidData) {
              // Calculate unified cross-sensor average using latest values from all available topics
              // First priority: use the latest global values from all topics that have ever sent data
              const allAvailableValuesFromGlobal: number[] = [];
              const allTopicsWithData: string[] = [];

              globalLatestValues.forEach((valData, topicKey) => {
                allAvailableValuesFromGlobal.push(valData.value);
                allTopicsWithData.push(topicKey);
              });

              // If we don't have global data, fallback to latestValues from windowing
              const allAvailableValues =
                allAvailableValuesFromGlobal.length >= 2
                  ? allAvailableValuesFromGlobal
                  : Object.values(latestValues);

              const topicsUsedForAverage =
                allAvailableValuesFromGlobal.length >= 2
                  ? allTopicsWithData
                  : Object.keys(latestValues);

              // Calculate unified result based on output query aggregation type
              let unifiedResult: number;
              switch (outputAggregationType) {
                case "MAX":
                  unifiedResult =
                    allAvailableValues.length > 0
                      ? Math.max(...allAvailableValues)
                      : 0;
                  break;
                case "MIN":
                  unifiedResult =
                    allAvailableValues.length > 0
                      ? Math.min(...allAvailableValues)
                      : 0;
                  break;
                case "SUM":
                  unifiedResult =
                    allAvailableValues.length > 0
                      ? allAvailableValues.reduce((sum, val) => sum + val, 0)
                      : 0;
                  break;
                case "COUNT":
                  unifiedResult = allAvailableValues.length; // Count of available sensors
                  break;
                case "AVG":
                default:
                  unifiedResult =
                    allAvailableValues.length > 0
                      ? allAvailableValues.reduce((sum, val) => sum + val, 0) /
                        allAvailableValues.length
                      : 0;
                  break;
              }

            this.logger.log(
              `Computing unified cross-sensor ${outputAggregationType.toLowerCase()} using ${allAvailableValuesFromGlobal.length >= 2 ? "global" : "windowed"} values: ${this.debugEnabled ? JSON.stringify(allAvailableValues) : `[${allAvailableValues.length} values]`} from topics: ${this.debugEnabled ? JSON.stringify(topicsUsedForAverage) : `[${topicsUsedForAverage.length} topics]`} -> ${unifiedResult}`,
            ); // Prepare individual topics results - prefer aggregation results, fallback to latest values, then global values
              const individualTopicsResults: Record<string, number | string> = {
                ...aggregationResults,
              };
              Object.keys(latestValues).forEach((topic) => {
                if (!individualTopicsResults[topic]) {
                  individualTopicsResults[topic] = latestValues[topic];
                }
              });

              // Also add global values if they're not already included
              globalLatestValues.forEach((valData, topic) => {
                if (!individualTopicsResults[topic]) {
                  individualTopicsResults[topic] = valData.value;
                }
              });

              // Publish unified result similar to other approaches
              const finalResult = {
                approach: "approximation",
                aggregationType: this.outputAggregationType,
                sessionId: this.sessionId,
                timestamp: now,
                window: { start: windowStartGlobal, end: now },
                unifiedResult: unifiedResult,
                value: unifiedResult,
                unifiedAverage:
                  this.outputAggregationType === "AVG" ? unifiedResult : undefined, // Keep for backward compatibility
                individualTopics: individualTopicsResults,
                metadata: {
                  validBuffers: totalValidBuffers,
                  expectedTopics: r2sTopics.length,
                  availableTopics: Object.keys(individualTopicsResults),
                  topicCount: allAvailableValues.length,
                  hasMultipleTopics: allAvailableValues.length >= 2,
                  latestValuesUsed: topicsUsedForAverage,
                  globalValuesAvailable: globalLatestValues.size,
                  usingGlobalValues: allAvailableValuesFromGlobal.length >= 2,
                },
              };

              if (this.debugEnabled) {
                this.logger.log(
                  `Final aggregation results: ${JSON.stringify(finalResult)}`,
                );
              } else {
                this.logger.log(
                  `Final aggregation results ready for publishing: window=${this.windowCount + 1}, topics=${Object.keys(individualTopicsResults).length}, aggregation=${this.outputAggregationType}`,
                );
              }

              // Check if client is connected before publishing
              if (rsp_client.connected) {
                // Calculate and log latency with multiple metrics
                this.windowCount++;
                const resultEmittedAt = Date.now();
                const expectedWindowClose =
                  this.diagnosticsWriter.getExpectedWindowCloseTime(
                    this.windowCount,
                  );
                const centeredWindowMetadata = buildBenchmarkWindowMetadata({
                  windowSemantics: process.env.RSP_WINDOW_SEMANTICS || "trailing",
                  logicalTriggerTime:
                    expectedWindowClose - this.windowRange / 2,
                  windowStart: expectedWindowClose - this.windowRange,
                  windowEnd: expectedWindowClose,
                  windowDataCloseTime: expectedWindowClose,
                  resultEmittedAt,
                  metadataSource: "reconstructed",
                });
                this.diagnosticsWriter.logLatency(
                  this.windowCount,
                  this.firstDataReceivedTime,
                  expectedWindowClose,
                  this.lastDataReceivedTime,
                  resultEmittedAt,
                  String(unifiedResult),
                  {
                    ...centeredWindowMetadata,
                    coverageComplete: false,
                    isPartialWindow: true,
                    isComparableWindow: false,
                  } as ReturnType<typeof buildBenchmarkWindowMetadata>,
                );
                const publishedPayload = JSON.stringify({
                  ...finalResult,
                  ...buildBenchmarkResultPayload(
                    "approximation",
                    this.outputAggregationType,
                    this.sessionId,
                    unifiedResult,
                    this.windowCount,
                    {
                      windowSemantics: centeredWindowMetadata.windowSemantics,
                      logicalTriggerTime:
                        centeredWindowMetadata.logicalTriggerTime,
                      windowStart: centeredWindowMetadata.windowStart,
                      windowEnd: centeredWindowMetadata.windowEnd,
                      windowDataCloseTime:
                        centeredWindowMetadata.windowDataCloseTime,
                      resultEmittedAt: centeredWindowMetadata.resultEmittedAt,
                      latencyFromLogicalTriggerMs:
                        centeredWindowMetadata.latencyFromLogicalTriggerMs,
                      latencyFromWindowCloseMs:
                        centeredWindowMetadata.latencyFromWindowCloseMs,
                      windowDurationMs: this.windowRange,
                      coverageComplete: false,
                      isPartialWindow: true,
                      isComparableWindow: false,
                      metadataSource: centeredWindowMetadata.metadataSource,
                    },
                  ),
                });

                // Publish with QoS 1 and error handling

                rsp_client.publish(
                  resultTopic,
                  publishedPayload,
                  { qos: 1 },
                  (error) => {
                    if (error) {
                      console.error(
                        "Failed to publish aggregated results:",
                        error,
                      );
                      this.logger.log(
                        `Failed to publish aggregated results: ${error}`,
                      );
                    } else {
                      recordPublishedMqttMessage({
                        topic: resultTopic,
                        payload: publishedPayload,
                        messageType: "superquery_result",
                        warmup: this.windowCount === 1,
                      });
                      profileCount("mqtt_messages_published");
                      profileCount("emitted_results");
                      this.recordFinalizedWindow(this.windowCount);
                      console.log(
                        `Successfully published unified cross-sensor ${outputAggregationType.toLowerCase()}: ${unifiedResult} (from ${allAvailableValues.length} topics)`,
                      );
                      this.logger.log(
                        `Successfully published unified cross-sensor ${outputAggregationType.toLowerCase()}: ${unifiedResult} (from ${allAvailableValues.length} topics)`,
                      );
                    }
                  },
                );
              } else {
                console.warn(
                  "MQTT client not connected, cannot publish results",
                );
                this.logger.log(
                  "MQTT client not connected, cannot publish results",
                );
              }
            } else {
              this.logger.log(
                `No valid data available for publishing. ValidBuffers: ${totalValidBuffers}, LatestValues: ${Object.keys(latestValues).length}`,
              );
            }
          }
        } catch (error) {
          this.logger.log(
            `Error processing message from topic ${topic}: ${error}`,
          );
        }
        endStageTimer(
          "approximation.mqtt_message_callback_total_ms",
          callbackStartedAt,
        );
      });
    });
  }

  private parseApproximationWindowMessage(
    rawData: string,
    topic: string,
    fallbackAggregationType: AggregationFunction,
  ): ApproximationWindowMessage | null {
    let parsed: any;
    try {
      parsed = profileStageSync(
        "approximation.structured_json_parse_ms",
        () => JSON.parse(rawData),
      );
    } catch {
      return null;
    }

    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    const structuredChunkResult = parsed as PartialChunkResult;
    const directWindowStart = Number(
      structuredChunkResult.window?.start ?? parsed.window_start,
    );
    const directWindowEnd = Number(
      structuredChunkResult.window?.end ?? parsed.window_end,
    );
    const timestampFrom = Number(parsed.timestamp_from);
    const timestampTo = Number(parsed.timestamp_to);
    const windowStart = Number.isFinite(directWindowStart)
      ? directWindowStart
      : Number.isFinite(timestampFrom)
        ? timestampFrom
        : NaN;
    const windowEnd = Number.isFinite(directWindowEnd)
      ? directWindowEnd
      : Number.isFinite(timestampTo)
        ? timestampTo
        : NaN;
    const value = Number(
      structuredChunkResult.value ??
        parsed.value ??
        parsed.resultValue ??
        structuredChunkResult.avg ??
        parsed.avg ??
        structuredChunkResult.sum ??
        parsed.sum ??
        structuredChunkResult.min ??
        parsed.min ??
        structuredChunkResult.max ??
        parsed.max,
    );
    const aggregationType = String(
      structuredChunkResult.aggregateFunction ??
        parsed.aggregateFunction ??
        parsed.aggregationType ??
        fallbackAggregationType,
    ).toUpperCase() as "SUM" | "AVG" | "COUNT" | "MIN" | "MAX";

    if (
      Number.isFinite(windowStart) &&
      Number.isFinite(windowEnd) &&
      Number.isFinite(value)
    ) {
      const hasStructuredEnvelope =
        parsed.message_format === "structured_reusable_result";
      const hasNestedWindow =
        structuredChunkResult.window &&
        Number.isFinite(Number(structuredChunkResult.window.start)) &&
        Number.isFinite(Number(structuredChunkResult.window.end));
      const hasFlatStructuredWindow =
        Number.isFinite(Number(parsed.window_start)) &&
        Number.isFinite(Number(parsed.window_end));
      const kind =
        hasStructuredEnvelope && (hasNestedWindow || hasFlatStructuredWindow)
          ? "structured"
          : "adapted_legacy";
      if (kind === "structured") {
        this.messageCounters.structured_messages_seen += 1;
      } else {
        this.messageCounters.adapted_legacy_messages_seen += 1;
      }

      return {
        kind,
        windowStart,
        windowEnd,
        value,
        aggregationType,
        sourceTopic:
          String(parsed.source_topic || parsed.reusable_result_topic || topic),
      };
    }

    return null;
  }
}
