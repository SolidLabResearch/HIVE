import { IStreamQueryOperator } from "../../util/Interfaces";
import CONFIG from "../../config/httpServerConfig.json";
import { RSPQLParser } from "rsp-js";
import { ExtractedQuery, QueryMap } from "../../util/Types";
import { CSVLogger } from "../../util/logger/CSVLogger";
import mqtt from "mqtt";
import fs from "fs";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  getResultTopic,
  getSessionId,
} from "../../util/runtimeConfig";
import { hash_string_md5 } from "../../util/Util";
import { recordPublishedMqttMessage } from "../../util/mqttTraffic";
import { getCachedParsedQuery } from "../../util/queryCache";
import { profileCount, profileSync, writeProfileArtifact } from "../../util/profiling";

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

/**
 *
 */
export class ApproximationApproachOperator implements IStreamQueryOperator {
  private logger: CSVLogger = new CSVLogger("approximation_approach_log.csv");
  private subQueries: string[] = [];
  private outputQuery: string = "";
  private queryMQTTTopicMap: Map<string, string> = new Map<string, string>();
  private queryFetchLocation: string = CONFIG.queryFetchLocation;
  private extractedQueries: ExtractedQuery[] = [];
  private parser: RSPQLParser = new RSPQLParser();
  private inactivityConfig: InactivityConfig;
  private latencyLogStream!: fs.WriteStream;
  private windowCount: number = 0;
  private queryRegisteredTime: number = 0;
  private windowRange: number = 120000; // 120 seconds based on RANGE 120000
  private windowSlide: number = 60000; // 60 seconds based on STEP 60000
  private firstDataReceivedTime: number = 0; // Track when first data arrives (wall-clock)
  private lastDataReceivedTime: number = 0; // Track when last data was received
  private sessionId: string = getSessionId();
  private mqttPublisherClient: any = null;
  private activeMqttClients: any[] = [];
  private cleanupRegistered: boolean = false;
  private cachedOutputQuery: string = "";
  private cachedOutputQueryParsed: any = null;

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
    this.initializeLatencyLogging();
    this.queryRegisteredTime = Date.now(); // Record when query is registered
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
      if (this.mqttPublisherClient) {
        try {
          this.mqttPublisherClient.end(true);
        } catch (error) {
          console.error("Failed to close approximation publisher client:", error);
        }
        this.mqttPublisherClient = null;
      }

      for (const client of this.activeMqttClients.splice(0)) {
        try {
          client.end(true);
        } catch (error) {
          console.error("Failed to close approximation MQTT client:", error);
        }
      }

      if (this.latencyLogStream) {
        this.latencyLogStream.end();
      }
    });
    writeProfileArtifact();
  }

  private getOrCreatePublisherClient(): any {
    if (this.mqttPublisherClient) {
      return this.mqttPublisherClient;
    }

    this.mqttPublisherClient = mqtt.connect(CONFIG.mqttBroker, {
      clientId: "approximation-operator-" + Math.random().toString(16).substr(2, 8),
      clean: true,
      keepalive: 60,
      reconnectPeriod: 1000,
    });
    this.activeMqttClients.push(this.mqttPublisherClient);
    profileCount("mqtt_clients_created");
    return this.mqttPublisherClient;
  }

  private publishWithSharedClient(topic: string, payload: string, qos = 1): Promise<void> {
    const client = this.getOrCreatePublisherClient();
    return new Promise((resolve) => {
      const publish = () => {
        client.publish(topic, payload, { qos }, (error: any) => {
          if (error) {
            console.error("Failed to publish aggregated results:", error);
            this.logger.log(`Failed to publish aggregated results: ${error}`);
          } else {
            profileCount("mqtt_messages_published");
          }
          resolve();
        });
      };

      if (client.connected) {
        publish();
      } else {
        client.once("connect", publish);
      }
    });
  }

  /**
   * Initialize latency logging
   */
  private initializeLatencyLogging() {
    const latencyLogFilePath = "approximation_latency_log.csv";
    const writeLatencyHeader = !fs.existsSync(latencyLogFilePath);
    this.latencyLogStream = fs.createWriteStream(latencyLogFilePath, {
      flags: "a",
    });

    if (writeLatencyHeader) {
      this.latencyLogStream.write(
        "window_number,query_registered_at,first_data_received_at,expected_window_close,last_data_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_data_ms,result_value\n",
      );
    }
  }

  /**
   * Log latency measurement with multiple metrics
   */
  private logLatency(
    windowNumber: number,
    expectedWindowClose: number,
    lastDataReceivedAt: number,
    resultTime: number,
    value: string,
  ) {
    // Metric 1: Latency from query registration (original calculation)
    const latencyFromQueryReg = resultTime - expectedWindowClose;

    // Metric 2: Time from first data received to result (wall-clock)
    // For window N: expected time = firstDataReceivedTime + RANGE + (N-1) * STEP
    const expectedFromDataStart =
      this.firstDataReceivedTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide;
    const latencyFromDataStart = resultTime - expectedFromDataStart;

    // Metric 3: Time from last data received to result emitted (processing latency)
    const latencyFromLastData = resultTime - lastDataReceivedAt;

    if (this.latencyLogStream) {
      this.latencyLogStream.write(
        `${windowNumber},${this.queryRegisteredTime},${this.firstDataReceivedTime},${expectedWindowClose},${lastDataReceivedAt},${resultTime},${latencyFromQueryReg},${latencyFromDataStart},${latencyFromLastData},${value}\n`,
      );
    }
    console.log(`LATENCY: Window ${windowNumber}:`);
    console.log(
      `  - From query registration: ${latencyFromQueryReg}ms (expected close: ${expectedWindowClose}, result: ${resultTime})`,
    );
    console.log(
      `  - From data start: ${latencyFromDataStart}ms (first data: ${this.firstDataReceivedTime}, expected: ${expectedFromDataStart}, result: ${resultTime})`,
    );
    console.log(
      `  - Processing time (last data to result): ${latencyFromLastData}ms`,
    );
    console.log(`  - Value: ${value}`);
  }

  /**
   * Calculate expected window close time for a given window number
   * Window N closes at: queryRegisteredTime + RANGE + (N-1) * STEP
   */
  private getExpectedWindowCloseTime(windowNumber: number): number {
    return (
      this.queryRegisteredTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide
    );
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
        const aggregationMatch = topic.rspql_query.match(/SELECT\s*\((\w+)\(/i);
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
      console.log(
        `Active approximation MQTT topics: ${JSON.stringify(Array.from(this.queryMQTTTopicMap.values()))}`,
      );
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

    const window_parameters = await this.createTopicWindowParameters(
      this.extractedQueries,
    );

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

    // Extract aggregation type from output query
    const outputAggregationMatch =
      this.outputQuery.match(/SELECT\s*\((\w+)\(/i);
    const outputAggregationType = outputAggregationMatch
      ? outputAggregationMatch[1].toUpperCase()
      : "AVG";
    this.logger.log(
      `Output query aggregation type detected: ${outputAggregationType}`,
    );

    if (outputQueryParsed === null || outputQueryParsed === undefined) {
      throw new Error("Failed to parse output query.");
    }

    if (!this.extractedQueries) {
      throw new Error("No extracted queries found for aggregation.");
    }

    const s2rQueries = this.extractedQueries.map(
      (query) => getCachedParsedQuery<any>(this.parser, query.rspql_query).s2r[0],
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

    const that = this;

    rsp_client.on("connect", () => {
      // Successfully connected to the MQTT broker
      this.logger.log("MQTT Client Connected for Approximation Operator");

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

      for (const mqttTopic of r2sTopics) {
        rsp_client.subscribe(mqttTopic, (err: any) => {
          if (err) {
            console.error(`Failed to subscribe to topic ${mqttTopic}:`, err);
          } else {
            console.log(`Successfully subscribed to topic ${mqttTopic}`);
          }
        });
      }

      // Use separate buffers for each topic/variable
      const windowBuffers: Map<
        string,
        Array<{
          start: number;
          end: number;
          value: number;
          agg: "SUM" | "AVG" | "COUNT" | "MIN" | "MAX";
        }>
      > = new Map();

      // Global tracking of latest values from all topics - this persists across windows
      const globalLatestValues: Map<
        string,
        { value: number; timestamp: number }
      > = new Map();

      let lastTriggerTime = Date.now();
      let lastDataReceivedTime = Date.now(); // Track when we last received any data
      let dataReceiveCount = 0; // Count data messages received
      let averageDataInterval = 0; // Average time between data messages
      let previousDataTime = Date.now();
      let streamStartTime = Date.now(); // Track when the stream started
      const MAX_STREAM_DURATION = 240000; // Allow enough time for three 60s output windows plus buffer

      rsp_client.on("message", (topic: string, message: any) => {
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
          // Parse the RDF triple to extract the numeric value
          // Look for patterns like: hasValue> "number"^^<type>
          const valueMatch = data.match(
            /saref:hasValue>\s*"([^"]*)"(?:\^\^<[^>]*>)?/,
          );
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
            if (!windowBuffers.has(topic)) {
              windowBuffers.set(topic, []);
            }
            windowBuffers.get(topic)!.push(result);
            profileCount("buffered_subwindow_results");
            return;
          }

          // Get or create buffer for this specific topic
          if (!windowBuffers.has(topic)) {
            windowBuffers.set(topic, []);
          }
          windowBuffers.get(topic)!.push(result);
          profileCount("buffered_subwindow_results");

          // ALWAYS update global latest values for ALL topics receiving data
          globalLatestValues.set(topic, { value: value, timestamp: now });
          this.logger.log(
            `Updated global latest value for topic ${topic}: ${value} at ${now}`,
          );

          if (process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1") {
            const allTopicBufferSizes: Record<string, number> = {};
            windowBuffers.forEach((buffer, topicKey) => {
              allTopicBufferSizes[topicKey] = buffer.length;
            });
            this.logger.log(
              `After adding result - Topic ${topic} buffer size: ${windowBuffers.get(topic)!.length}; all topic buffer sizes: ${JSON.stringify(allTopicBufferSizes)}`,
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
              if (buffer.length > 0 && buffer[buffer.length - 1].end >= windowStartGlobal) {
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
              topicBufferSizes[topicKey] = buffer.length;
            });
            this.logger.log(
              `Current window buffer sizes before cleanup: ${JSON.stringify(topicBufferSizes)}`,
            );

            // Clean up old windows for each topic buffer and collect latest values
            const aggregationResults: Record<string, number | string> = {};
            const latestValues: Record<string, number> = {}; // Keep track of latest values from each topic
            let totalValidBuffers = 0;

            windowBuffers.forEach((buffer, topicKey) => {
              // Clean up old entries for this topic
              while (buffer.length && buffer[0].end < windowStartGlobal) {
                buffer.shift();
              }

              this.logger.log(
                `Topic ${topicKey} buffer size after cleanup: ${buffer.length}`,
              );
              this.logger.log(
                `Topic ${topicKey} buffer contents after cleanup: ${JSON.stringify(buffer)}`,
              );

              if (buffer.length > 0) {
                totalValidBuffers++;
                // Calculate aggregation for this specific topic
                const target = { start: windowStartGlobal, end: now };
                const aggregation = buffer[buffer.length - 1].agg;

                const topicResult = mergeMultipleSlidingWindowResults(
                  buffer,
                  target,
                  aggregation,
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
                // If no current window data, try to use the most recent value from this topic
                // Look through all buffered data for this topic to find the most recent value
                const allTopicData = windowBuffers.get(topicKey);
                if (allTopicData && allTopicData.length > 0) {
                  // Get the most recent value regardless of window
                  const mostRecentValue =
                    allTopicData[allTopicData.length - 1].value;
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
                const buffer = windowBuffers.get(expectedTopic)!;
                if (buffer.length > 0) {
                  const mostRecentValue = buffer[buffer.length - 1].value;
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
                `Computing unified cross-sensor ${outputAggregationType.toLowerCase()} using ${allAvailableValuesFromGlobal.length >= 2 ? "global" : "windowed"} values: ${JSON.stringify(allAvailableValues)} from topics: ${JSON.stringify(topicsUsedForAverage)} -> ${unifiedResult}`,
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
                aggregationType: outputAggregationType,
                sessionId: this.sessionId,
                timestamp: now,
                window: { start: windowStartGlobal, end: now },
                unifiedResult: unifiedResult,
                value: unifiedResult,
                unifiedAverage:
                  outputAggregationType === "AVG" ? unifiedResult : undefined, // Keep for backward compatibility
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

              this.logger.log(
                `Final aggregation results: ${JSON.stringify(finalResult)}`,
              );

              // Check if client is connected before publishing
              if (rsp_client.connected) {
                // Calculate and log latency with multiple metrics
                this.windowCount++;
                const resultEmittedAt = Date.now();
                const expectedWindowClose = this.getExpectedWindowCloseTime(
                  this.windowCount,
                );
                this.logLatency(
                  this.windowCount,
                  expectedWindowClose,
                  this.lastDataReceivedTime,
                  resultEmittedAt,
                  String(unifiedResult),
                );
                const publishedPayload = JSON.stringify({
                  ...finalResult,
                  ...buildBenchmarkResultPayload(
                    "approximation",
                    outputAggregationType as AggregationFunction,
                    this.sessionId,
                    unifiedResult,
                    this.windowCount,
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
      });
    });
  }
}

/**
 * Merges two window results with overlap subtraction for sliding windows.
 * @param win1 - First window result {start, end, value}.
 * @param win2 - Second window result {start, end, value}.
 * @param overlap - Overlap window result {start, end, value}.
 * @param target - Target window {start, end}.
 * @param win1.start
 * @param win1.end
 * @param win1.value
 * @param agg - Aggregation function: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX'.
 * @param win2.start
 * @param win2.end
 * @param win2.value
 * @param overlap.start
 * @param overlap.end
 * @param overlap.value
 * @param target.start
 * @param target.end
 * @returns The approximate aggregation for the target window.
 */
export function mergeSlidingWindowResults(
  win1: { start: number; end: number; value: number },
  win2: { start: number; end: number; value: number },
  overlap: { start: number; end: number; value: number },
  target: { start: number; end: number },
  agg: "SUM" | "AVG" | "COUNT" | "MIN" | "MAX",
): number | string {
  const dur1 = win1.end - win1.start;
  const dur2 = win2.end - win2.start;
  const dur3 = target.end - target.start;
  const overlap_dur = Math.max(
    0,
    Math.min(win1.end, win2.end) - Math.max(win1.start, win2.start),
  );

  if (agg === "SUM" || agg === "AVG") {
    const combined_sum =
      win1.value * dur1 + win2.value * dur2 - overlap.value * overlap_dur;
    return agg === "AVG" ? combined_sum / dur3 : combined_sum;
  } else if (agg === "COUNT") {
    const combined = win1.value + win2.value - overlap.value;
    return combined;
  } else if (agg === "MIN") {
    return Math.min(win1.value, win2.value, overlap.value);
  } else if (agg === "MAX") {
    return Math.max(win1.value, win2.value, overlap.value);
  } else {
    return "Not Supported";
  }
}

export function mergeMultipleSlidingWindowResults(
  windows: Array<{ start: number; end: number; value: number }>,
  target: { start: number; end: number },
  agg: "SUM" | "AVG" | "COUNT" | "MIN" | "MAX",
): number | string {
  console.log(`DEBUG: mergeMultipleSlidingWindowResults called with:`);
  console.log(`  - windows:`, JSON.stringify(windows));
  console.log(`  - target:`, JSON.stringify(target));
  console.log(`  - aggregation:`, agg);

  // Filter windows that overlap target
  const overlapping = windows.filter(
    (w) => w.end > target.start && w.start < target.end,
  );
  console.log(
    `DEBUG: Found ${overlapping.length} overlapping windows:`,
    JSON.stringify(overlapping),
  );

  if (overlapping.length === 0) return 0;

  // MIN/MAX aggregation - straightforward from overlapping window values
  if (agg === "MIN") {
    const result = Math.min(...overlapping.map((w) => w.value));
    console.log(`DEBUG: MIN result:`, result);
    return result;
  }
  if (agg === "MAX") {
    const result = Math.max(...overlapping.map((w) => w.value));
    console.log(`DEBUG: MAX result:`, result);
    return result;
  }

  // For AVG: Calculate weighted average based on overlap duration with target window
  if (agg === "AVG") {
    let weightedSum = 0;
    let totalWeight = 0;

    console.log(`DEBUG: Calculating weighted average:`);
    overlapping.forEach((w, idx) => {
      // Calculate overlap duration between window and target
      const overlapStart = Math.max(w.start, target.start);
      const overlapEnd = Math.min(w.end, target.end);
      const overlapDuration = overlapEnd - overlapStart;

      console.log(
        `DEBUG: Window ${idx}: value=${w.value}, overlapDuration=${overlapDuration}`,
      );

      if (overlapDuration > 0) {
        // For averages, weight by overlap duration
        const contribution = w.value * overlapDuration;
        weightedSum += contribution;
        totalWeight += overlapDuration;
        console.log(
          `DEBUG: Added contribution=${contribution}, totalWeight=${totalWeight}, weightedSum=${weightedSum}`,
        );
      }
    });

    const result = totalWeight > 0 ? weightedSum / totalWeight : 0;
    console.log(
      `DEBUG: Final AVG calculation: weightedSum=${weightedSum} / totalWeight=${totalWeight} = ${result}`,
    );
    return result;
  }

  // For SUM and COUNT: partition target window by all unique boundaries from overlapping windows
  const boundaries = new Set<number>();
  boundaries.add(target.start);
  boundaries.add(target.end);
  overlapping.forEach((w) => {
    if (w.start > target.start && w.start < target.end) boundaries.add(w.start);
    if (w.end > target.start && w.end < target.end) boundaries.add(w.end);
  });

  const sortedBoundaries = Array.from(boundaries).sort((a, b) => a - b);

  let totalSum = 0;

  for (let i = 0; i < sortedBoundaries.length - 1; i++) {
    const subStart = sortedBoundaries[i];
    const subEnd = sortedBoundaries[i + 1];
    const subDuration = subEnd - subStart;
    if (subDuration <= 0) continue;

    // Identify windows covering this subinterval
    const coveringWindows = overlapping.filter(
      (w) => w.start <= subStart && w.end >= subEnd,
    );
    if (coveringWindows.length === 0) continue;

    if (agg === "SUM") {
      // For SUM: treat values as rates and integrate over time
      const subValueRateSum = coveringWindows.reduce((acc, w) => {
        const windowDuration = w.end - w.start;
        if (windowDuration <= 0) return acc;
        const rate = w.value / windowDuration;
        return acc + rate;
      }, 0);
      totalSum += subValueRateSum * subDuration;
    } else if (agg === "COUNT") {
      // For COUNT: sum the values directly
      const subValueSum = coveringWindows.reduce((acc, w) => acc + w.value, 0);
      totalSum += subValueSum;
    }
  }

  return totalSum;
}
