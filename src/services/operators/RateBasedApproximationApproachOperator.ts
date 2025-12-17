import { IStreamQueryOperator } from "../../util/Interfaces";
import CONFIG from "../../config/httpServerConfig.json";
import { RSPQLParser } from "rsp-js";
import { ExtractedQuery, QueryMap } from "../../util/Types";
import { CSVLogger } from "../../util/logger/CSVLogger";
import { hash_string_md5 } from "../../util/Util";
import mqtt from "mqtt";
import * as fs from "fs";

/**
 * Configuration interface for inactivity detection.
 */
interface InactivityConfig {
  /** Minimum samples to calculate average interval. */
  minSamplesForInterval?: number;
  /** Multiplier for average interval to determine timeout (adaptive). */
  inactivityMultiplier?: number;
  /** Fallback timeout for when we can't calculate interval (ms). */
  fallbackTimeoutMs?: number;
  /** Maximum timeout to prevent infinite waiting (ms). */
  maxTimeoutMs?: number;
}

/**
 * Operator implementation for the Approximation Approach.
 * It subscribes to sub-query streams and approximates the result for the main query.
 */
export class ApproximationApproachOperator implements IStreamQueryOperator {
  private logger: CSVLogger = new CSVLogger("approximation_approach_log.csv");
  private subQueries: string[] = [];
  private outputQuery: string = "";
  private queryMQTTTopicMap: Map<string, string> = new Map<string, string>();
  private queryFetchLocation: string;
  private extractedQueries: ExtractedQuery[] = [];
  private parser: RSPQLParser = new RSPQLParser();
  private inactivityConfig: InactivityConfig;
  private resultsCsvStream?: fs.WriteStream;
  private queryRegisteredTimestamp: number | null = null;

  /**
   * The constructor class with optional inactivity configuration.
   * @param {InactivityConfig} [inactivityConfig] - Optional configuration for data inactivity detection.
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

    // Use HTTP_PORT environment variable if set, otherwise use config port
    const port = process.env.HTTP_PORT
      ? parseInt(process.env.HTTP_PORT)
      : CONFIG.port;
    this.queryFetchLocation = `http://localhost:${port}/fetchQueries`;

    // Initialize results CSV in results/ folder
    const resultsDir = "results";
    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true });
    }
    const resultsFileName = "approximation_results.csv";
    const resultsFilePath = `${resultsDir}/${resultsFileName}`;
    const writeHeader = !fs.existsSync(resultsFilePath);
    this.resultsCsvStream = fs.createWriteStream(resultsFilePath, {
      flags: "a",
    });
    if (writeHeader) {
      this.resultsCsvStream.write(
        "query_registered_timestamp,result_timestamp,result\n",
      );
    }
  }

  /**
   * Adds a query to the subQueries array.
   * @param {string} query - The query to be added.
   * @memberof ApproximationApproachOperator
   */
  addSubQuery(query: string): void {
    this.subQueries.push(query);
    console.log(`Sub-query added: ${query}`);
  }

  /**
   * Returns the array containing the subQueries.
   * @returns {string[]} The subQueries string array.
   * @memberof ApproximationApproachOperator
   */
  getSubQueries(): string[] {
    return this.subQueries;
  }

  /**
   * Assigns the outputQuery string to the variable.
   * @param {string} query - The output query.
   * @returns {void}
   */
  addOutputQuery(query: string): void {
    this.outputQuery = query;
    // Set query registration timestamp when output query is added
    if (!this.queryRegisteredTimestamp) {
      this.queryRegisteredTimestamp = Date.now();
    }
  }

  /**
   * Initializes variables related to MQTT topics and extracted queries.
   * Fetches existing queries and assigns result stream topics.
   * @returns {Promise<void>} A promise that resolves when initialization is complete.
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
   * Creates a Record of result R2S topics with window width and aggregation function.
   * @param {Array<{r2s_topic: string, rspql_query: string}>} topics - Array of topics containing the result topic and RSP-QL Query.
   * @returns {Promise<Record<string, {width: number, aggregation: string}>>} A record containing the R2S topic, width, and aggregation function.
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
        const parsed = this.parser.parse(topic.rspql_query);

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
   * Performs a GET request to the HTTP server to fetch existing queries being executed.
   * @returns {Promise<void>} Returns nothing.
   */
  async setMQTTTopicMap(): Promise<void> {
    // Try to fetch with retries, but don't fail if HTTP server is not available
    let retries = 3;
    let lastError: Error | null = null;

    while (retries > 0) {
      try {
        const response = await fetch(this.queryFetchLocation);
        if (!response.ok) {
          throw new Error(`Failed to fetch subqueries: ${response.status}`);
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
        return; // Success!
      } catch (error) {
        lastError = error as Error;
        retries--;
        if (retries > 0) {
          console.log(
            `Failed to fetch from HTTP server, retrying... (${retries} attempts left)`,
          );
          await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1s before retry
        }
      }
    }

    // If we get here, all retries failed
    console.warn(
      `Could not fetch queries from HTTP server at ${this.queryFetchLocation}: ${lastError?.message}`,
    );
    console.warn(
      "Continuing with locally added queries only (if any were added via addSubQuery)",
    );

    // Build extractedQueries and queryMQTTTopicMap from subQueries that were added directly
    if (this.subQueries.length > 0) {
      console.log(
        `Using ${this.subQueries.length} locally added subqueries instead`,
      );
      // Use the same hashing logic as Orchestrator to compute chunked/${hash} topics
      this.extractedQueries = this.subQueries.map((query) => {
        // Compute the hash the same way as Orchestrator.addSubQuery()
        const query_hash = hash_string_md5(query);
        const r2s_topic = `chunked/${query_hash}`;
        return {
          rspql_query: query,
          r2s_topic: r2s_topic,
        };
      });

      // CRITICAL FIX: Also populate queryMQTTTopicMap so handleAggregation doesn't exit early
      for (const extracted of this.extractedQueries) {
        const query_hash = hash_string_md5(extracted.rspql_query);
        this.queryMQTTTopicMap.set(query_hash, extracted.r2s_topic);
        console.log(
          `Mapped query hash ${query_hash} to topic ${extracted.r2s_topic}`,
        );
      }

      console.log(
        `Created ${this.extractedQueries.length} extracted queries from local subqueries`,
      );
      console.log(
        `queryMQTTTopicMap now has ${this.queryMQTTTopicMap.size} entries`,
      );
    } else {
      console.warn(
        "No subqueries available - neither from HTTP server nor locally added",
      );
    }
  }

  /**
   * Extracts RSP-QL queries with their respective R2S topics from the fetched data.
   * @param {QueryMap} data - The fetched queries running in the network.
   * @returns {Promise<ExtractedQuery[]>} Array containing RSP-QL queries and R2S topic locations.
   */
  async extractQueriesWithTopics(data: QueryMap): Promise<ExtractedQuery[]> {
    const extractedQueries: ExtractedQuery[] = [];
    console.log(`Extracting queries with topics from data:`, data);

    for (const key in data) {
      if (Object.prototype.hasOwnProperty.call(data, key)) {
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
   * Handles aggregation with subqueries, approximating the resultant window based on subquery aggregation values.
   * @returns {Promise<void>} Returns a void Promise.
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
    const outputQueryParsed = this.parser.parse(this.outputQuery);
    const outputQueryWidth = outputQueryParsed.s2r[0].width;
    const outputQuerySlide = outputQueryParsed.s2r[0].slide;

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
      (query) => this.parser.parse(query.rspql_query).s2r[0],
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

    const _that = this;

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
      const streamStartTime = Date.now(); // Track when the stream started
      const MAX_STREAM_DURATION = 150000; // Maximum 150 seconds (2.5 minutes) total runtime

      rsp_client.on("message", (topic: string, message: any) => {
        // this.logger.log(`Received message on topic ${topic}: ${message.toString()}`);

        try {
          const data = message.toString();
          // Parse the RDF triple to extract the numeric value
          // Look for patterns like:
          //   - saref:hasValue> "number"^^<type> (prefix notation)
          //   - <https://saref.etsi.org/core/hasValue> "number"^^<type> (full URI from RSPAgent)
          const valueMatch = data.match(
            /(?:saref:hasValue>|<https:\/\/saref\.etsi\.org\/core\/hasValue>)\s*"([^"]*)"(?:\^\^<[^>]*>)?/,
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
            return;
          }

          // Get or create buffer for this specific topic
          if (!windowBuffers.has(topic)) {
            windowBuffers.set(topic, []);
          }
          windowBuffers.get(topic)!.push(result);

          // ALWAYS update global latest values for ALL topics receiving data
          globalLatestValues.set(topic, { value: value, timestamp: now });
          this.logger.log(
            `Updated global latest value for topic ${topic}: ${value} at ${now}`,
          );

          // Log current buffer state for all topics
          this.logger.log(
            `After adding result - Topic ${topic} buffer size: ${windowBuffers.get(topic)!.length}`,
          );
          const allTopicBufferSizes: Record<string, number> = {};
          windowBuffers.forEach((buffer, topicKey) => {
            allTopicBufferSizes[topicKey] = buffer.length;
          });
          this.logger.log(
            `All topic buffer sizes after message: ${JSON.stringify(allTopicBufferSizes)}`,
          );

          // Log all global latest values
          const globalLatestValuesObj: Record<string, any> = {};
          globalLatestValues.forEach((valData, topicKey) => {
            globalLatestValuesObj[topicKey] = {
              value: valData.value,
              timestamp: valData.timestamp,
            };
          });
          // this.logger.log(`Global latest values: ${JSON.stringify(globalLatestValuesObj)}`);

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
            const topicsWithData: string[] = [];
            windowBuffers.forEach((buffer, topicKey) => {
              const validWindowData = buffer.filter(
                (w) => w.end >= windowStartGlobal,
              );
              if (validWindowData.length > 0) {
                topicsWithValidData++;
                topicsWithData.push(topicKey);
              }
            });

            // Also count topics with recent global values (within the window)
            let globalTopicsWithRecentData = 0;
            const globalTopicsWithData: string[] = [];
            globalLatestValues.forEach((valData, topicKey) => {
              // Consider data "recent" if it arrived within the window duration
              if (now - valData.timestamp <= outputQueryWidth) {
                globalTopicsWithRecentData++;
                if (!globalTopicsWithData.includes(topicKey)) {
                  globalTopicsWithData.push(topicKey);
                }
              }
            });

            // Use the maximum of buffer-based and global-based topic counts
            const effectiveTopicsWithData = Math.max(
              topicsWithValidData,
              globalTopicsWithRecentData,
            );

            // More robust waiting strategy with adaptive buffer time
            // Use 20% of the window slide time, minimum 5 seconds, maximum 15 seconds
            const adaptiveBufferMs = Math.min(
              Math.max(outputQuerySlide * 0.2, 5000),
              15000,
            );

            const timeSinceLastTrigger = Date.now() - lastTriggerTime;
            const hasAllTopics = effectiveTopicsWithData >= r2sTopics.length;
            const withinBufferPeriod =
              timeSinceLastTrigger < outputQuerySlide + adaptiveBufferMs;

            // Wait for more topics if we don't have all and we're still within buffer period
            const shouldWaitForMoreTopics = !hasAllTopics && withinBufferPeriod;

            if (shouldWaitForMoreTopics) {
              this.logger.log(
                `Waiting for more topics. Topics with valid data: ${effectiveTopicsWithData} (buffer: ${topicsWithValidData}, global: ${globalTopicsWithRecentData}), Expected: ${r2sTopics.length}, Time since trigger: ${timeSinceLastTrigger}ms, Buffer: ${adaptiveBufferMs}ms`,
              );
              return; // Wait a bit more for other topics
            }

            // CRITICAL: Only produce unified result if we have data from ALL expected topics
            // This prevents incorrect results from partial data
            if (!hasAllTopics) {
              this.logger.log(
                `Skipping unified result: only have data from ${effectiveTopicsWithData}/${r2sTopics.length} topics. Topics with data: [${[...new Set([...topicsWithData, ...globalTopicsWithData])].join(", ")}], Expected: [${r2sTopics.join(", ")}]`,
              );
              lastTriggerTime = Date.now(); // Reset trigger time to avoid immediate re-trigger
              return; // Don't produce partial results
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
            const _hasMultipleTopicData = Object.keys(latestValues).length >= 2;
            const hasAnyValidData =
              totalValidBuffers > 0 || Object.keys(latestValues).length > 0;

            if (hasAnyValidData) {
              // Calculate unified cross-sensor aggregation using latest values from all topics
              // IMPORTANT: Only use recent values (within window) to avoid stale data
              const allAvailableValuesFromGlobal: number[] = [];
              const allTopicsWithRecentData: string[] = [];

              globalLatestValues.forEach((valData, topicKey) => {
                // Only use values that are recent (within 2x window width for safety margin)
                const maxAge = outputQueryWidth * 2;
                if (now - valData.timestamp <= maxAge) {
                  allAvailableValuesFromGlobal.push(valData.value);
                  allTopicsWithRecentData.push(topicKey);
                } else {
                  this.logger.log(
                    `Skipping stale global value for topic ${topicKey}: age=${now - valData.timestamp}ms, maxAge=${maxAge}ms`,
                  );
                }
              });

              // Verify we have data from all expected topics before proceeding
              if (allAvailableValuesFromGlobal.length < r2sTopics.length) {
                this.logger.log(
                  `Insufficient recent data for unified result: have ${allAvailableValuesFromGlobal.length} topics, need ${r2sTopics.length}`,
                );
                return;
              }

              // If we don't have enough global data, fallback to latestValues from windowing
              const allAvailableValues =
                allAvailableValuesFromGlobal.length >= r2sTopics.length
                  ? allAvailableValuesFromGlobal
                  : Object.values(latestValues);

              const topicsUsedForAverage =
                allAvailableValuesFromGlobal.length >= r2sTopics.length
                  ? allTopicsWithRecentData
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
                timestamp: now,
                window: { start: windowStartGlobal, end: now },
                unifiedResult: unifiedResult,
                unifiedAverage:
                  outputAggregationType === "AVG" ? unifiedResult : undefined, // Keep for backward compatibility
                aggregationType: outputAggregationType,
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
                // Publish with QoS 1 and error handling

                rsp_client.publish(
                  "approximation/output",
                  JSON.stringify(finalResult),
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
                      console.log(
                        `Successfully published unified cross-sensor ${outputAggregationType.toLowerCase()}: ${unifiedResult} (from ${allAvailableValues.length} topics)`,
                      );
                      this.logger.log(
                        `Successfully published unified result: ${unifiedResult}`,
                      );

                      // Write to results CSV
                      if (
                        this.resultsCsvStream &&
                        this.queryRegisteredTimestamp
                      ) {
                        const csvLine = `${this.queryRegisteredTimestamp},${now},${unifiedResult}\n`;
                        this.resultsCsvStream.write(csvLine);
                      }

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
 * @param {object} win1 - First window result containing start, end, and value.
 * @param {number} win1.start - Start time of first window.
 * @param {number} win1.end - End time of first window.
 * @param {number} win1.value - Value of first window.
 * @param {object} win2 - Second window result containing start, end, and value.
 * @param {number} win2.start - Start time of second window.
 * @param {number} win2.end - End time of second window.
 * @param {number} win2.value - Value of second window.
 * @param {object} overlap - Overlap window result containing start, end, and value.
 * @param {number} overlap.start - Start time of overlap window.
 * @param {number} overlap.end - End time of overlap window.
 * @param {number} overlap.value - Value of overlap window.
 * @param {object} target - Target window containing start and end.
 * @param {number} target.start - Start time of target window.
 * @param {number} target.end - End time of target window.
 * @param {"SUM" | "AVG" | "COUNT" | "MIN" | "MAX"} agg - Aggregation function to use.
 * @returns {number | string} The approximate aggregation for the target window.
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

/**
 * Merges results from multiple sliding windows to approximate the value for a target window.
 * @param {Array<{ start: number; end: number; value: number }>} windows - Array of window results.
 * @param {object} target - Target window containing start and end.
 * @param {number} target.start - Start time of target window.
 * @param {number} target.end - End time of target window.
 * @param {"SUM" | "AVG" | "COUNT" | "MIN" | "MAX"} agg - Aggregation function to use.
 * @returns {number | string} The approximate aggregation result.
 */
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
