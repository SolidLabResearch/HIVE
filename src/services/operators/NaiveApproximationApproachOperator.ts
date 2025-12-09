import { RSPQLParser } from "rspql-containment-checker";
import { IStreamQueryOperator } from "../../util/Interfaces";
import { CSVLogger } from "../../util/logger/CSVLogger";
import { ExtractedQuery, QueryMap } from "../../util/Types";
import CONFIG from "../../config/httpServerConfig.json";
import mqtt from "mqtt";

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
 * A naive approximation approach operator for stream queries.
 * The naive approximation approach approximates the results by not aligning the different
 * windows but rather by just combining the approximation results of
 * the different sized windows together.
 * @export
 * @class NaiveApproximationApproachOperator
 * @implements {IStreamQueryOperator}
 */
export class NaiveApproximationApproachOperator implements IStreamQueryOperator {
  private logger: CSVLogger = new CSVLogger(
    "naive_approximation_approach_log.csv",
  );
  private subQueries: string[] = [];
  private outputQuery = "";
  private extractedQueries: ExtractedQuery[] = [];
  private parser: RSPQLParser = new RSPQLParser();
  private queryMQTTTopicMap: Map<string, string> = new Map<string, string>();
  private queryFetchLocation: string = CONFIG.queryFetchLocation;
  private inactivityConfig: InactivityConfig;

  /**
   *
   */
  constructor(inactivityConfig?: InactivityConfig) {
    this.inactivityConfig = {
      minSamplesForInterval: 3,
      inactivityMultiplier: 0.5,
      fallbackTimeoutMs: 5000,
      maxTimeoutMs: 15000,
      ...inactivityConfig,
    };
  }
  /**
   *
   */
  addSubQuery(query: string): void {
    this.subQueries.push(query);
  }

  /**
   *
   */
  addOutputQuery(query: string): void {
    this.outputQuery = query;
  }

  /**
   *
   */
  getSubQueries(): string[] {
    return this.subQueries;
  }

  /**
   *
   */
  async init(): Promise<void> {
    this.queryMQTTTopicMap = new Map<string, string>();
    this.extractedQueries = [];
    await this.setMQTTTopicMap();
  }

  /**
   *
   */
  async setMQTTTopicMap(): Promise<void> {
    const response = await fetch(this.queryFetchLocation);
    if (!response.ok) {
      throw new Error("Failed to fetch the subqueries.");
    }

    const data = await response.json();

    for (const [queryHash, mqttTopic] of Object.entries(data)) {
      this.queryMQTTTopicMap.set(queryHash, mqttTopic as string);
    }
    this.extractedQueries = await this.extractQueriesWithTopics(
      data as QueryMap,
    );
  }

  /**
   *
   */
  async extractQueriesWithTopics(data: QueryMap): Promise<ExtractedQuery[]> {
    const extractedQueries: ExtractedQuery[] = [];

    for (const key in data) {
      if (Object.prototype.hasOwnProperty.call(data, key)) {
        const entry = data[key];

        if (entry.rspql_query && entry.r2s_topic) {
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
   *
   */
  async naiveApproximationApproach(
    windows: Array<{
      start: number;
      end: number;
      value: number;
    }>,
    target: {
      start: number;
      end: number;
    },
    aggregation: "SUM" | "AVG" | "COUNT" | "MAX" | "MIN",
  ): Promise<number | string> {
    if (!windows || windows.length === 0) {
      return 0;
    }

    // Filter windows that overlap with the target window
    const relevantWindows = windows.filter(
      (window) => window.end > target.start && window.start < target.end,
    );

    if (relevantWindows.length === 0) {
      return 0;
    }

    const values = relevantWindows.map((window) => window.value);

    switch (aggregation) {
      case "SUM":
        return values.reduce((sum, val) => sum + val, 0);
      case "AVG":
        return values.reduce((sum, val) => sum + val, 0) / values.length;
      case "COUNT":
        return values.length;
      case "MAX":
        return Math.max(...values);
      case "MIN":
        return Math.min(...values);
      default:
        return values.reduce((sum, val) => sum + val, 0) / values.length;
    }
  }

  /**
   *
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
   *
   */
  async handleAggregation(): Promise<void> {
    if (this.subQueries.length === 0) {
      throw new Error("No subqueries are available to aggregate.");
    }

    const window_parameters = await this.createTopicWindowParameters(
      this.extractedQueries,
    );

    if (this.queryMQTTTopicMap.size === 0) {
      throw new Error("No MQTT topics are available for the queries.");
    }

    const outputQueryParsed = this.parser.parse(this.outputQuery);
    const outputQueryWidth = outputQueryParsed.s2r[0].width;
    const outputQuerySlide = outputQueryParsed.s2r[0].slide;

    const outputAggregationMatch =
      this.outputQuery.match(/SELECT\s*\((\w+)\(/i);
    const outputAggregationType = outputAggregationMatch
      ? outputAggregationMatch[1].toUpperCase()
      : "AVG";

    if (outputQueryParsed === null || outputQueryParsed === undefined) {
      throw new Error("Failed to parse output query.");
    }

    if (!this.extractedQueries) {
      throw new Error("No extracted queries available for aggregation.");
    }

    const s2rQueries = this.extractedQueries.map(
      (query) => this.parser.parse(query.rspql_query).s2r[0],
    );

    if (s2rQueries.length === 0) {
      throw new Error("No S2R queries found in the extracted queries.");
    }

    if (outputQuerySlide <= 0 || outputQueryWidth <= 0) {
      throw new Error(
        "Invalid output query parameters : slide and width must be greater than 0",
      );
    }

    const rsp_client = mqtt.connect(CONFIG.mqttBroker, {
      clientId:
        "naive-approximation-approach-" +
        Math.random().toString(16).substr(2, 8),
      clean: true,
      keepalive: 60,
      reconnectPeriod: 1000,
    });

    rsp_client.on("error", (error) => {
      console.error("MQTT Client Error:", error);
    });

    rsp_client.on("connect", () => {
      console.log("MQTT Client Connected");
      this.logger.log("MQTT Client Connected");
      const topics = Array.from(this.queryMQTTTopicMap.values());
      if (topics.length === 0) {
        this.logger.log("No MQTT topics available for the queries.");
        return;
      }

      const r2s_topics = topics.map((item: any) =>
        typeof item === "object" && item !== null ? item.r2s_topic : item,
      );

      if (r2s_topics.length === 0) {
        console.log("No valid R2S topics available for the values.");
        this.logger.log("No valid R2S topics available for the values.");
        return;
      }

      for (const topic of r2s_topics) {
        rsp_client.subscribe(topic, { qos: 0 }, (err: any) => {
          if (err) {
            console.error("Failed to subscribe to topic:", topic, err);
          } else {
            console.log("Successfully subscribed to topic:", topic);
          }
        });
      }

      const windowBuffers: Map<
        string,
        Array<{
          start: number;
          end: number;
          value: number;
          agg: "SUM" | "AVG" | "COUNT" | "MAX" | "MIN";
        }>
      > = new Map();

      const globalLatestValues: Map<
        string,
        {
          value: number;
          timestamp: number;
        }
      > = new Map();

      let lastTriggerTime = Date.now();
      let lastDataReceivedTime = Date.now();
      let dataReceiveCount = 0;
      let averageDataInterval = 0;
      let previousDataTime = Date.now();
      const streamStartTime = Date.now();
      const MAX_STREAM_DURATION = 150000;

      rsp_client.on("message", async (topic: string, message: Buffer) => {
        try {
          const data = message.toString();

          const valueMatch = data.match(
            /saref:hasValue>\s*"([^"]*)"(?:\^\^<[^>]*>)?/,
          );

          let value: number;

          if (valueMatch && valueMatch[1]) {
            value = parseFloat(valueMatch[1]);
          } else {
            value = parseFloat(data);
          }

          if (isNaN(value)) {
            this.logger.log(
              `Received NaN value from topic ${topic}, ignoring.`,
            );
            return;
          }

          const now = Date.now();
          lastDataReceivedTime = now;

          if (dataReceiveCount > 0) {
            const currentInterval = now - previousDataTime;

            if (dataReceiveCount === 1) {
              averageDataInterval = currentInterval;
            } else {
              averageDataInterval =
                (averageDataInterval * (dataReceiveCount - 1) +
                  currentInterval) /
                dataReceiveCount;
            }

            if (dataReceiveCount % 10 === 0) {
              this.logger.log(
                `Average data interval after ${dataReceiveCount} messages: ${averageDataInterval} ms`,
              );
            }
          }

          dataReceiveCount++;
          previousDataTime = now;

          const params = window_parameters[topic];

          if (!params) {
            console.log(`No window parameters found for topic ${topic}`);
            this.logger.log(`No window parameters found for topic ${topic}`);
            return;
          }

          const windowEnd = now;
          const windowStart = windowEnd - params.width;

          const result = {
            start: windowStart,
            end: windowEnd,
            value: value,
            agg: params.aggregation as "SUM" | "AVG" | "COUNT" | "MAX" | "MIN",
          };

          if (!result.value && result.value !== 0) {
            this.logger.log(
              `Invalid value received from topic ${topic}, ignoring.`,
            );

            if (!windowBuffers.has(topic)) {
              windowBuffers.set(topic, []);
            }

            windowBuffers.get(topic)!.push(result);
            return;
          }

          if (!windowBuffers.has(topic)) {
            windowBuffers.set(topic, []);
          }

          windowBuffers.get(topic)!.push(result);
          globalLatestValues.set(topic, {
            value: value,
            timestamp: now,
          });
          this.logger.log(
            `Updated latest value for topic ${topic}: ${JSON.stringify(globalLatestValues.get(topic))}`,
          );

          const allTopicBufferSizes: Record<string, number> = {};
          windowBuffers.forEach((buffer, key) => {
            allTopicBufferSizes[key] = buffer.length;
          });

          this.logger.log(
            `Current buffer sizes for all topics: ${JSON.stringify(allTopicBufferSizes)}`,
          );

          const globalLatestValueObject: Record<string, any> = {};

          globalLatestValues.forEach((val, key) => {
            globalLatestValueObject[key] = {
              value: val.value,
              timestamp: val.timestamp,
            };
          });

          this.logger.log(
            `Current latest values for all topics: ${JSON.stringify(globalLatestValueObject)}`,
          );

          if (Date.now() - lastTriggerTime >= outputQuerySlide) {
            const totalStreamDuration = Date.now() - streamStartTime;
            if (totalStreamDuration >= MAX_STREAM_DURATION) {
              return;
            }

            let adaptiveTimeout: number;

            if (
              dataReceiveCount >=
                this.inactivityConfig.minSamplesForInterval! &&
              averageDataInterval > 0
            ) {
              adaptiveTimeout = Math.min(
                averageDataInterval *
                  this.inactivityConfig.inactivityMultiplier!,
                this.inactivityConfig.maxTimeoutMs!,
              );
            } else {
              adaptiveTimeout = this.inactivityConfig.fallbackTimeoutMs!;
            }

            const timeSinceLastData = Date.now() - lastDataReceivedTime;

            if (timeSinceLastData >= adaptiveTimeout) {
              this.logger.log(
                `No data received for ${timeSinceLastData}ms, triggering inactivity timeout.`,
              );
              return;
            }

            const windowStartGlobal = now - outputQueryWidth;
            let topicsWithValidData = 0;

            windowBuffers.forEach((buffer, _topic) => {
              const validWindowData = buffer.filter(
                (window) => window.end > windowStartGlobal,
              );

              if (validWindowData.length > 0) {
                topicsWithValidData++;
              }
            });

            const bufferTimeMs = 3000;

            const shouldWaitForMoreTopics =
              topicsWithValidData < r2s_topics.length &&
              Date.now() - lastTriggerTime < outputQuerySlide + bufferTimeMs;

            if (shouldWaitForMoreTopics) {
              this.logger.log(`Waiting for more topics to have valid data.`);
              return;
            }

            this.logger.log(
              `Triggering aggregation at ${new Date().toISOString()}`,
            );

            const topicBufferSizes: Record<string, number> = {};

            windowBuffers.forEach((buffer, key) => {
              topicBufferSizes[key] = buffer.length;
            });

            const aggregationResults: Record<string, number | string> = {};
            const latestValues: Record<string, number> = {};

            let totalValidBuffers = 0;

            for (const [topic, buffer] of windowBuffers.entries()) {
              while (buffer.length && buffer[0].end <= windowStartGlobal) {
                buffer.shift();
              }

              if (buffer.length > 0) {
                totalValidBuffers++;

                const target = {
                  start: windowStartGlobal,
                  end: now,
                };

                const aggregation = buffer[buffer.length - 1].agg;
                const topicResult = await this.naiveApproximationApproach(
                  buffer,
                  target,
                  aggregation,
                );

                aggregationResults[topic] = topicResult;

                if (typeof topicResult === "number") {
                  latestValues[topic] = topicResult;
                }
                this.logger.log(
                  `Aggregation result for topic ${topic}: ${topicResult}`,
                );
              } else {
                const allTopicData = windowBuffers.get(topic);
                if (allTopicData && allTopicData.length > 0) {
                  const mostRecentValue =
                    allTopicData[allTopicData.length - 1].value;

                  latestValues[topic] = mostRecentValue;
                  this.logger.log(
                    `Using most recent value for topic ${topic}: ${mostRecentValue}`,
                  );
                }
              }
            }

            r2s_topics.forEach((expectedTopic) => {
              if (
                !latestValues[expectedTopic] &&
                windowBuffers.has(expectedTopic)
              ) {
                const buffer = windowBuffers.get(expectedTopic)!;

                if (buffer.length > 0) {
                  const mostRecentValue = buffer[buffer.length - 1].value;

                  latestValues[expectedTopic] = mostRecentValue;
                  this.logger.log(
                    `Using most recent value for topic ${expectedTopic}: ${mostRecentValue}`,
                  );
                }
              }
            });

            lastTriggerTime = Date.now();

            const hasMultipleTopicData = Object.keys(latestValues).length >= 2;
            if (hasMultipleTopicData) {
              this.logger.log(
                `Multiple topic data available for aggregation: ${JSON.stringify(latestValues)}`,
              );
            }

            const hasAnyValidData =
              totalValidBuffers > 0 || Object.keys(latestValues).length > 0;

            if (hasAnyValidData) {
              const allAvailableValuesFromGlobal: number[] = [];
              const allTopicsWithData: string[] = [];

              globalLatestValues.forEach((val, key) => {
                allAvailableValuesFromGlobal.push(val.value);
                allTopicsWithData.push(key);
              });

              const allAvailableValues =
                allAvailableValuesFromGlobal.length >= 2
                  ? allAvailableValuesFromGlobal
                  : Object.values(latestValues);
              const topicsUsedForAverage =
                allAvailableValuesFromGlobal.length >= 2
                  ? allTopicsWithData
                  : Object.keys(latestValues);

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
                `Unified result for ${outputAggregationType}: ${unifiedResult}`,
              );

              const individualTopicsResults: Record<string, number | string> = {
                ...aggregationResults,
              };
              Object.keys(latestValues).forEach((topic) => {
                if (!individualTopicsResults[topic]) {
                  individualTopicsResults[topic] = latestValues[topic];
                }
              });

              globalLatestValues.forEach((val, key) => {
                if (!individualTopicsResults[key]) {
                  individualTopicsResults[key] = val.value;
                }
              });

              const finalResult = {
                timestamp: now,
                windows: {
                  start: windowStartGlobal,
                  end: now,
                },
                unifiedResult: unifiedResult,
                unifiedAverage:
                  outputAggregationType === "AVG" ? unifiedResult : undefined,
                aggregationType: outputAggregationType,
                individualResults: individualTopicsResults,
                metadata: {
                  validBuffers: totalValidBuffers,
                  expectedTopics: r2s_topics.length,
                  allAvailableTopics: Object.keys(individualTopicsResults),
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

              if (rsp_client.connected) {
                rsp_client.publish(
                  "naiveApproximation/output",
                  JSON.stringify(finalResult),
                  {
                    qos: 0,
                  },
                  (error) => {
                    if (error) {
                      this.logger.log(
                        `Failed to publish aggregation result: ${error}`,
                      );
                    } else {
                      this.logger.log(
                        `Successfully published aggregation result`,
                      );
                    }
                  },
                );
              } else {
                this.logger.log(
                  `MQTT Client is not connected. Therefore we cannot publish aggregation results.`,
                );
              }
            }
          }
        } catch (error) {
          this.logger.log(
            `Error processing message for topic ${topic}: ${error}`,
          );
        }
      });

      rsp_client.on("close", () => {
        console.log("MQTT Client Disconnected");
      });

      rsp_client.on("offline", () => {
        console.log("MQTT Client Offline");
      });

      rsp_client.on("reconnect", () => {
        console.log("MQTT Client Reconnecting");
      });
    });
  }
}
