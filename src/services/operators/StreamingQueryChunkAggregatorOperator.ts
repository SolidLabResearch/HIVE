import fs from "fs";
import { RewriteChunkQuery } from "hive-thought-rewriter";
import mqtt from "mqtt";
import { RSPQLParser } from "rsp-js";
import { RSPQueryProcess } from "../../rsp/RSPQueryProcess";
import { IStreamQueryOperator } from "../../util/Interfaces";
import { CSVLogger } from "../../util/logger/CSVLogger";
import { hash_string_md5, storeToString } from "../../util/Util";
import { R2ROperator } from "./r2r";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  getResultTopic,
  getSessionId,
} from "../../util/runtimeConfig";
import { PartialChunkResult } from "../../util/chunkTypes";
const N3 = require("n3");

type SubqueryIdentity = {
  subqueryId: string;
  topic: string;
};

/**
 *
 */
export class StreamingQueryChunkAggregatorOperator implements IStreamQueryOperator {
  public subQueries: string[];
  public outputQuery: string = "";
  private parser: RSPQLParser;
  private queryMQTTTopicMap!: Map<string, string>;
  private subQueryMQTTTopicMap: Map<string, string> = new Map<string, string>();
  private chunkGCD: number;
  private logger: CSVLogger;
  private mqttBroker: string = "mqtt://localhost:1883"; // Default MQTT broker URL, can be changed if needed
  private sessionId: string; // Unique session ID to isolate MQTT topics across iterations
  private latencyLogStream!: fs.WriteStream;
  private windowCount: number = 0; // Track window count for latency logging
  private queryRegisteredTime: number = 0; // Track when query was registered
  private windowRange: number = 120000; // 120 seconds based on RANGE 120000
  private windowSlide: number = 60000; // 60 seconds based on STEP 60000
  private firstDataReceivedTime: number = 0; // Track when first data arrives (wall-clock)
  private lastChunkReceivedTime: number = 0; // Track when last chunk was received
  private intervalTriggerTime: number = 0; // Track when the setInterval fires
  private lastProcessedTime: number = 0; // Track last time we processed (for immediate trigger)
  private processingInProgress: boolean = false; // Prevent concurrent processing
  private useImmediateTrigger: boolean = true; // Enable immediate trigger optimization
  private debugChunksEnabled: boolean = process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1";
  private ignoredLegacyChunkCount: number = 0;
  /**
   *
   */
  constructor() {
    this.subQueries = [];
    this.parser = new RSPQLParser();
    this.chunkGCD = 0;
    this.logger = new CSVLogger("streaming_query_chunk_aggregator_log.csv");
    this.sessionId = getSessionId();
    this.initializeLatencyLogging();
    this.queryRegisteredTime = Date.now(); // Record when query is registered
  }

  /**
   * Initialize latency logging
   */
  private initializeLatencyLogging() {
    const latencyLogFilePath = "chunked_latency_log.csv";
    const writeLatencyHeader = !fs.existsSync(latencyLogFilePath);
    this.latencyLogStream = fs.createWriteStream(latencyLogFilePath, {
      flags: "a",
    });

    if (writeLatencyHeader) {
      this.latencyLogStream.write(
        "window_number,query_registered_at,first_data_received_at,expected_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,interval_wait_ms,computation_ms,result_value\n",
      );
    }
  }

  /**
   * Log latency measurement with multiple metrics
   */
  private logLatency(
    windowNumber: number,
    expectedWindowClose: number,
    lastChunkReceivedAt: number,
    intervalTriggerAt: number,
    resultTime: number,
    value: string,
  ) {
    // Metric 1: Latency from query registration
    // Expected window close = queryRegisteredTime + RANGE + (N-1) * STEP
    const latencyFromQueryReg = resultTime - expectedWindowClose;

    // Metric 2: Time from first data received to result (wall-clock)
    // For window N: expected time = firstDataReceivedTime + RANGE + (N-1) * STEP
    const expectedFromDataStart =
      this.firstDataReceivedTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide;
    const latencyFromDataStart = resultTime - expectedFromDataStart;

    // Metric 3: Interval wait time - time from last chunk received to when interval fires
    const intervalWaitTime = intervalTriggerAt - lastChunkReceivedAt;

    // Metric 4: Actual computation time - time from interval trigger to result emission
    const computationTime = resultTime - intervalTriggerAt;

    if (this.latencyLogStream) {
      this.latencyLogStream.write(
        `${windowNumber},${this.queryRegisteredTime},${this.firstDataReceivedTime},${expectedWindowClose},${lastChunkReceivedAt},${intervalTriggerAt},${resultTime},${latencyFromQueryReg},${latencyFromDataStart},${intervalWaitTime},${computationTime},${value}\n`,
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
      `  - Interval wait time (last chunk to interval trigger): ${intervalWaitTime}ms`,
    );
    console.log(
      `  - Actual computation time (interval trigger to result): ${computationTime}ms`,
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


  private debugChunkLog(message: string): void {
    if (this.debugChunksEnabled) {
      this.logger.log(`[DEBUG_CHUNKS] ${message}`);
    }
  }

  private normalizeChunkPayload(message: string): PartialChunkResult | null {
    try {
      const parsed = JSON.parse(message);
      if (
        parsed &&
        typeof parsed.queryId === "string" &&
        typeof parsed.subqueryId === "string" &&
        parsed.window &&
        Number.isFinite(parsed.window.start) &&
        Number.isFinite(parsed.window.end)
      ) {
        const chunkGroupId = `${parsed.queryId}:${parsed.window.windowName}:${parsed.window.start}:${parsed.window.end}`;
        return {
          queryId: parsed.queryId,
          subqueryId: parsed.subqueryId,
          window: parsed.window,
          chunkId: parsed.chunkId || `${chunkGroupId}:${parsed.subqueryId}`,
          aggregateFunction: parsed.aggregateFunction,
          value: Number.isFinite(Number(parsed.value))
            ? Number(parsed.value)
            : undefined,
          count: Number.isFinite(Number(parsed.count))
            ? Number(parsed.count)
            : undefined,
          rdfPayload: typeof parsed.rdfPayload === "string" ? parsed.rdfPayload : undefined,
        };
      }
    } catch {
      // legacy payload, keep null to skip structured aggregation
    }
    return null;
  }

  public collectChunkByWindow(
    chunksByWindow: Map<string, Map<string, PartialChunkResult>>,
    partial: PartialChunkResult,
    expectedSubqueryIds: string[],
  ): { chunkGroupId: string; missingSubqueryIds: string[]; isComplete: boolean } {
    const chunkGroupId = `${partial.queryId}:${partial.window.windowName}:${partial.window.start}:${partial.window.end}`;
    if (!chunksByWindow.has(chunkGroupId)) {
      chunksByWindow.set(chunkGroupId, new Map<string, PartialChunkResult>());
    }
    const bySubquery = chunksByWindow.get(chunkGroupId)!;
    bySubquery.set(partial.subqueryId, partial);
    const missingSubqueryIds = expectedSubqueryIds.filter(
      (subqueryId) => !bySubquery.has(subqueryId),
    );
    return {
      chunkGroupId,
      missingSubqueryIds,
      isComplete: missingSubqueryIds.length === 0,
    };
  }

  /**
   *
   */
  public async init() {
    this.logger.log("init() called");
    await this.setMQTTTopicMap();
    this.logger.log("StreamingQueryChunkAggregatorOperator initialized.");
  }

  /**
   *
   * @param query
   */
  addOutputQuery(query: string): void {
    this.outputQuery = query;
  }

  /**
   *
   */
  async setMQTTTopicMap(): Promise<void> {
    this.logger.log("setMQTTTopicMap() called");
    this.queryMQTTTopicMap = new Map<string, string>();
    this.logger.log(
      `MQTT Topic Map set for subqueries: ${JSON.stringify(this.queryMQTTTopicMap)}`,
    );
    const response = await fetch("http://localhost:8080/fetchQueries");
    if (!response.ok) {
      console.error("Failed to fetch queries from the server.");
      return;
    }
    const data = await response.json();
    // Log the full structure for debugging
    this.logger.log(
      `Fetched data from server (full JSON): ${JSON.stringify(data, null, 2)}`,
    );

    for (const [queryHash, mqttTopic] of Object.entries(data)) {
      const topicString = mqttTopic;

      this.queryMQTTTopicMap.set(queryHash as string, topicString as string);
      this.logger.log(
        `DEBUG: queryMQTTTopicMap after set: ${JSON.stringify(Array.from(this.queryMQTTTopicMap.entries()))}`,
      );
      this.logger.log(
        `Subquery ${queryHash} mapped to MQTT Topic ${topicString}`,
      );
    }
  }

  /**
   *
   */
  async handleAggregation(): Promise<void> {
    this.logger.log("Starting aggregation process for subqueries.");
    await this.initializeSubQueryProcesses();
    this.logger.log("SubQuery Processes initialized for aggregation.");

    if (this.subQueries.length === 0) {
      this.logger.log("No subqueries available for aggregation.");
      console.error("No subqueries available for aggregation.");
      return;
    }
    if (this.outputQuery === "") {
      this.logger.log("Output query is not set for aggregation.");
      console.error("Output query is not set for aggregation.");
      return;
    }
    if (this.chunkGCD <= 0) {
      this.logger.log("Chunk GCD is not valid for aggregation.");
      console.error("Chunk GCD is not valid for aggregation.");
      return;
    }
    if (this.queryMQTTTopicMap.size === 0) {
      this.logger.log("No MQTT topics mapped for subqueries.");
      return;
    }

    this.logger.log(
      `Starting aggregation of subqueries with GCD chunk size: ${this.chunkGCD}`,
    );

    if (!this.outputQuery) {
      console.error("Output query is not set or is undefined.");
      return;
    }
    const outputQueryParsed = this.parser.parse(this.outputQuery);
    if (!outputQueryParsed) {
      console.error(`Failed to parse output query: ${this.outputQuery}`);
      return;
    }

    const outputQueryWidth = outputQueryParsed.s2r[0].width;
    const outputQuerySlide = outputQueryParsed.s2r[0].slide;
    const resultTopic = getResultTopic("output");
    this.windowRange = outputQueryWidth;
    this.windowSlide = outputQuerySlide;
    if (outputQueryWidth <= 0 || outputQuerySlide <= 0) {
      console.error(
        `Invalid width or slide in output query: ${this.outputQuery}`,
      );
      return;
    }

    const rsp_client = mqtt.connect(this.mqttBroker);
    this.logger.log(`Connecting to MQTT broker at ${this.mqttBroker}...`);
    rsp_client.on("error", (err) => {
      console.error("MQTT connection error:", err);
    });
    rsp_client.on("offline", () => {
      console.error(
        "MQTT client is offline. Please check the broker connection.",
      );
    });
    rsp_client.on("reconnect", () => {
      this.logger.log("Reconnecting to MQTT broker...");
    });

    const that = this;

    rsp_client.on("connect", () => {
      this.logger.log(
        `subQueryTopicMap : ${JSON.stringify(that.subQueryMQTTTopicMap)}`,
      );
      const topics = Array.from(that.subQueryMQTTTopicMap.values());
      const subqueryIdentities: SubqueryIdentity[] = Array.from(
        that.subQueryMQTTTopicMap.entries(),
      ).map(([subqueryId, topic]) => ({ subqueryId, topic }));
      const expectedSubqueryIds = subqueryIdentities.map((entry) => entry.subqueryId);
      const topicToSubqueryId = new Map(
        subqueryIdentities.map((entry) => [entry.topic, entry.subqueryId]),
      );
      this.logger.log(`topics to subscribe: ${topics}`);
      // TODO : Remove hardcoded topics, use the topics from subQueryMQTTTopicMap but currently there is a bug in the subQueryMQTTTopicMap that prevents it from being used correctly.
      // TODO : Such that only one topic is subscribed to at a time.
      let topicsOfProcesses: string[] = [
        "chunked/f8eec45a01e39e93d117673df8915525",
        "chunked/b22681cadced9975b3b35cb47f82bb40",
      ];

      topicsOfProcesses = topics;

      this.logger.log(
        `DEBUG: topicsOfProcesses after loop: ${topicsOfProcesses}, length: ${topicsOfProcesses.length}`,
      );
      if (topicsOfProcesses.length === 0) {
        this.logger.log(
          "No valid MQTT topics to subscribe to. Please check the subQueryMQTTTopicMap.",
        );
        return;
      }
      for (const mqttTopic of topicsOfProcesses) {
        rsp_client.subscribe(`${mqttTopic}`, (err) => {
          if (err) {
            this.logger.log(
              `Failed to subscribe to topic ${mqttTopic}: ${err}`,
            );
          } else {
            this.logger.log(`Subscribed to topic: ${mqttTopic}`);
          }
        });
      }

      const chunksByWindow: Map<string, Map<string, PartialChunkResult>> = new Map();
      this.logger.log(
        `Output Query Width: ${outputQueryWidth}, Chunk GCD: ${this.chunkGCD}, SubQueries Length: ${this.subQueries.length}`,
      );

      // Helper function to process chunks - used by both immediate trigger and interval fallback
      const processChunks = async (triggerSource: string) => {
        // Prevent concurrent processing
        if (this.processingInProgress) {
          this.logger.log(
            `Processing already in progress, skipping ${triggerSource} trigger`,
          );
          return;
        }

        this.processingInProgress = true;

        // Record when processing is triggered (before any processing)
        this.intervalTriggerTime = Date.now();

        const readyGroups = Array.from(chunksByWindow.entries()).filter(
          ([, bySubquery]) => expectedSubqueryIds.every((subqueryId) => bySubquery.has(subqueryId)),
        );

        if (readyGroups.length === 0) {
          for (const [chunkGroupId, bySubquery] of chunksByWindow.entries()) {
            const missing = expectedSubqueryIds.filter((subqueryId) => !bySubquery.has(subqueryId));
            this.debugChunkLog(
              `${triggerSource}: waiting chunkGroupId=${chunkGroupId}; received=${Array.from(bySubquery.keys()).join(",") || "none"}; missing=${missing.join(",") || "none"}`,
            );
          }
          this.processingInProgress = false;
          return;
        }

        readyGroups.sort((a, b) => {
          const aStart = a[1].values().next().value?.window?.start ?? 0;
          const bStart = b[1].values().next().value?.window?.start ?? 0;
          return aStart - bStart;
        });

        for (const [chunkGroupId, bySubquery] of readyGroups) {
          const chunksForAggregation: string[] = [];
          for (const subqueryId of expectedSubqueryIds) {
            const partial = bySubquery.get(subqueryId);
            if (!partial) continue;
            if (partial.rdfPayload) {
              chunksForAggregation.push(partial.rdfPayload);
            }
          }

          if (chunksForAggregation.length === expectedSubqueryIds.length) {
            this.debugChunkLog(
              `final emission chunkGroupId=${chunkGroupId}; includedSubqueries=${Array.from(bySubquery.keys()).join(",")}`,
            );
            await this.executeR2ROperator(chunksForAggregation, chunkGroupId);
            chunksByWindow.delete(chunkGroupId);
          } else {
            this.logger.log(
              `${triggerSource}: chunkGroupId=${chunkGroupId} skipped due to incomplete rdfPayload coverage (${chunksForAggregation.length}/${expectedSubqueryIds.length})`,
            );
          }
        }

        this.processingInProgress = false;
      };

      // Interval handle — will be started (and restarted) aligned to first data arrival
      let intervalHandle: ReturnType<typeof setInterval> | null = null;

      rsp_client.on("message", async (topic, message) => {
        this.logger.log(
          `Received message on topic ${topic}: ${message.toString()}`,
        );

        // Track when data is received for latency calculations
        const now = Date.now();
        if (this.firstDataReceivedTime === 0) {
          this.firstDataReceivedTime = now;
          this.lastProcessedTime = 0; // Reset to allow first processing
          this.logger.log(
            `First data received at wall-clock time: ${this.firstDataReceivedTime}`,
          );

          // Restart the interval aligned to queryRegisteredTime so ticks fire at
          // t0 + N * outputQuerySlide rather than firstDataReceivedTime + N *
          // outputQuerySlide. This removes the artificial sub-window-step delay
          // that inflated chunked latency measurements (BUG-001).
          if (intervalHandle) clearInterval(intervalHandle);
          const elapsed = now - this.queryRegisteredTime;
          const firstTickDelay =
            outputQuerySlide - (elapsed % outputQuerySlide);
          setTimeout(() => {
            processChunks("Interval");
            intervalHandle = setInterval(async () => {
              await processChunks("Interval");
            }, outputQuerySlide);
          }, firstTickDelay);
          this.logger.log(
            `Interval restarted aligned to queryRegisteredTime. First tick in ${firstTickDelay}ms, then every ${outputQuerySlide}ms.`,
          );
        }
        this.lastChunkReceivedTime = now;

        const normalized = this.normalizeChunkPayload(message.toString());
        if (normalized) {
          const expectedSubqueryId = topicToSubqueryId.get(topic);
          const effectiveSubqueryId = expectedSubqueryId || normalized.subqueryId;
          const chunkGroupId = `${normalized.queryId}:${normalized.window.windowName}:${normalized.window.start}:${normalized.window.end}`;
          const bySubquery = chunksByWindow.get(chunkGroupId) ?? new Map<string, PartialChunkResult>();
          const existing = bySubquery.get(effectiveSubqueryId);
          if (!existing) {
            const outcome = this.collectChunkByWindow(
              chunksByWindow,
              {
              ...normalized,
              subqueryId: effectiveSubqueryId,
              },
              expectedSubqueryIds,
            );
            this.debugChunkLog(
              `received chunkId=${normalized.chunkId}; subqueryId=${effectiveSubqueryId}; chunkGroupId=${outcome.chunkGroupId}; completeness=${expectedSubqueryIds.length - outcome.missingSubqueryIds.length}/${expectedSubqueryIds.length}; missing=${outcome.missingSubqueryIds.join(",") || "none"}`,
            );
          } else {
            this.debugChunkLog(
              `duplicate chunk ignored chunkId=${normalized.chunkId}; subqueryId=${effectiveSubqueryId}; chunkGroupId=${chunkGroupId}; existingChunkId=${existing.chunkId}`,
            );
          }
        } else {
          this.ignoredLegacyChunkCount++;
          this.debugChunkLog(
            `ignored legacy/unstructured chunk on topic=${topic}; ignoredLegacyChunkCount=${this.ignoredLegacyChunkCount}`,
          );
        }

        // IMMEDIATE TRIGGER OPTIMIZATION:
        // Process immediately when message arrives; only complete chunk groups emit.
        if (this.useImmediateTrigger) {
          await processChunks("Immediate");
        }
      });

      // Initial interval — runs at startup as a safety net before first data arrives.
      // Once the first chunk is received, this is cleared and restarted aligned to
      // firstDataReceivedTime so that subsequent ticks fire at exact STEP boundaries.
      intervalHandle = setInterval(async () => {
        await processChunks("Interval");
      }, outputQuerySlide);
    });
  }

  /**
   *
   * @param chunks
   */
  async executeR2ROperator(
    chunks: string[],
    chunkGroupId?: string,
  ): Promise<void> {
    this.logger.log(`Executing the R2R Operator with results: ${chunks}`);

    /*
For example, the allResults object might look like this:
          chunks: [
    '"<https://rsp.js/aggregation_event/89302616-a8d1-4a99-8b97-1f7efac51d88> <https://saref.etsi.org/core/hasTimestamp> \\"1749592410235\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/89302616-a8d1-4a99-8b97-1f7efac51d88> <https://saref.etsi.org/core/hasValue> \\"-22.666666666666668\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/b31b1867-f310-4c39-8379-893044ab517d> <https://saref.etsi.org/core/hasTimestamp> \\"1749592410517\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/b31b1867-f310-4c39-8379-893044ab517d> <https://saref.etsi.org/core/hasValue> \\"-4.2\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/65223e5b-711e-4c8a-95ab-878df02fec83> <https://saref.etsi.org/core/hasTimestamp> \\"1749592710780\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/65223e5b-711e-4c8a-95ab-878df02fec83> <https://saref.etsi.org/core/hasValue> \\"-22.857142857142858\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/6848a43f-b852-4914-81d0-c40c3f3840bc> <https://saref.etsi.org/core/hasTimestamp> \\"1749592710869\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/6848a43f-b852-4914-81d0-c40c3f3840bc> <https://saref.etsi.org/core/hasValue> \\"-4.285714285714286\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/6e9d0962-02ac-4424-8211-e0e44c609a12> <https://saref.etsi.org/core/hasTimestamp> \\"1749592740597\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/6e9d0962-02ac-4424-8211-e0e44c609a12> <https://saref.etsi.org/core/hasValue> \\"-22.7\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/0d4e9551-fe52-4bb1-a186-c342c091fe6d> <https://saref.etsi.org/core/hasTimestamp> \\"1749592740995\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/0d4e9551-fe52-4bb1-a186-c342c091fe6d> <https://saref.etsi.org/core/hasValue> \\"-4.2\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/0a3bcc3b-acb8-4d52-985b-185a2db9b4dd> <https://saref.etsi.org/core/hasTimestamp> \\"1749592770111\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/0a3bcc3b-acb8-4d52-985b-185a2db9b4dd> <https://saref.etsi.org/core/hasValue> \\"-4.103448275862069\\"^^<http://www.w3.org/2001/XMLSchema#float> ."',
    '"<https://rsp.js/aggregation_event/327ae8b1-52a1-48f8-9749-324000a75a45> <https://saref.etsi.org/core/hasTimestamp> \\"1749592770747\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/327ae8b1-52a1-48f8-9749-324000a75a45> <https://saref.etsi.org/core/hasValue> \\"-23\\"^^<http://www.w3.org/2001/XMLSchema#float> ."'
  ]
        */
    const store = new N3.Store();
    const parser = new N3.Parser();
    for (const chunk of chunks) {
      let chunkString = chunk;
      // If chunk is a JSON string, parse it
      try {
        if (chunkString.startsWith('"') && chunkString.endsWith('"')) {
          chunkString = JSON.parse(chunkString);
        }
      } catch (e) {
        this.logger.log(`DEBUG: Could not JSON.parse chunk: ${chunkString}`);
      }
      try {
        const quads = parser.parse(chunkString);
        store.addQuads(quads);
      } catch (e) {
        this.logger.log(
          `DEBUG: Could not parse chunk as Turtle: ${chunkString}`,
        );
      }
    }
    this.logger.log(storeToString(store));
    const detectAggregationFunction = this.detectAggregationFunction(
      this.outputQuery,
    );
    if (!detectAggregationFunction) {
      console.error("No aggregation function detected in the output query.");
      return;
    }
    const aggregationSPARQLQuery = this.getAggregationSPARQLQuery(
      detectAggregationFunction,
      "o",
    );
    if (!aggregationSPARQLQuery) {
      console.error("Failed to generate aggregation SPARQL query.");
      return;
    }
    this.logger.log(
      `Generated Aggregation SPARQL Query: ${aggregationSPARQLQuery}`,
    );
    const r2rOperator = new R2ROperator(aggregationSPARQLQuery);
    const bindingStream = await r2rOperator.execute(store);
    if (!bindingStream) {
      console.error("Failed to execute R2R Operator.");
      return;
    }
    await new Promise<void>((resolve) => {
      let completed = false;
      const finalize = () => {
        if (completed) return;
        completed = true;
        resolve();
      };

      bindingStream.on("data", (data: any) => {
        if (completed) return;

        this.logger.log(`R2R Operator Data Received: ${data}`);
        const resultValue = data.get("result").value;
        const outputQueryEvent = this.generateOutputQueryEvent(resultValue);
        this.logger.log(`Generated Output Query Event: ${outputQueryEvent}`);
        if (chunkGroupId) {
          this.debugChunkLog(
            `final emission chunkGroupId=${chunkGroupId}; recomposedResult=${resultValue}`,
          );
        }

        // Calculate and log latency with multiple metrics
        this.windowCount++;
        const resultEmittedAt = Date.now();
        const expectedWindowClose = this.getExpectedWindowCloseTime(
          this.windowCount,
        );
        this.logLatency(
          this.windowCount,
          expectedWindowClose,
          this.lastChunkReceivedTime,
          this.intervalTriggerTime,
          resultEmittedAt,
          resultValue,
        );

        // Publish the output query event to the MQTT broker
        const rsp_client = mqtt.connect(this.mqttBroker);
        rsp_client.on("connect", () => {
          const resultTopic = getResultTopic("output");
          const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
          const payload = useBenchmarkPayload
            ? JSON.stringify(
                buildBenchmarkResultPayload(
                  "chunked",
                  detectAggregationFunction as AggregationFunction,
                  this.sessionId,
                  Number.parseFloat(resultValue),
                  this.windowCount,
                ),
              )
            : outputQueryEvent;
          this.logger.log(`calculated result ${payload}`);

          rsp_client.publish(resultTopic, payload, (err: any) => {
            if (err) {
              console.error(
                `Error publishing output query event to topic ${resultTopic}:`,
                err,
              );
            } else {
              this.logger.log(
                `Output query event published to topic ${resultTopic}`,
              );
            }
          });
        });
        rsp_client.on("error", (err) => {
          console.error("MQTT connection error:", err);
        });
        rsp_client.on("offline", () => {
          console.error(
            "MQTT client is offline. Please check the broker connection.",
          );
        });
        rsp_client.on("reconnect", () => {
          this.logger.log("Reconnecting to MQTT broker...");
        });

        finalize();
      });

      bindingStream.on("end", () => {
        this.logger.log(
          "R2R Operator binding stream ended without additional results.",
        );
        finalize();
      });

      bindingStream.on("error", (err: any) => {
        console.error("R2R Operator binding stream error:", err);
        finalize();
      });
    });
  }

  /**
   *
   * @param data
   */
  generateOutputQueryEvent(data: any): string {
    const uuid_random = uuidv4();
    return ` <https://rsp.js/outputQueryEvent/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .`;
  }

  /**
   *
   */
  async initializeSubQueryProcesses(): Promise<void> {
    this.logger.log(`Initializing subquery processes.`);
    this.logger.log(`DEBUG: subQueries length: ${this.subQueries.length}`);
    this.logger.log(`DEBUG: subQueries: ${JSON.stringify(this.subQueries)}`);
    this.logger.log(`DEBUG: outputQuery: ${this.outputQuery}`);
    const chunkSize = this.findGCDChunk(this.subQueries, this.outputQuery);
    this.logger.log(`Calculated GCD Chunk Size: ${chunkSize}`);
    this.chunkGCD = chunkSize;
    const rewrittenChunkQueries: string[] = [];
    if (chunkSize > 0) {
      const rewriteChunkQuery = new RewriteChunkQuery(chunkSize, chunkSize);
      for (let i = 0; i < this.subQueries.length; i++) {
        const subQuery = this.subQueries[i];
        this.logger.log(`DEBUG: Rewriting subQuery ${i}: ${subQuery}`);
        const rewrittenQuery =
          rewriteChunkQuery.rewriteQueryWithNewChunkSize(subQuery);
        this.logger.log(`Rewritten SubQuery ${i}: ${rewrittenQuery}`);
        rewrittenChunkQueries.push(rewrittenQuery);
      }

      // Collect all promises
      const allPromises: Promise<void>[] = [];
      for (let i = 0; i < rewrittenChunkQueries.length; i++) {
        const hash_subQuery = hash_string_md5(rewrittenChunkQueries[i]);
        const topicName = `chunked/${this.sessionId}/${hash_subQuery}`;
        this.logger.log(
          `DEBUG: Setting subQueryMQTTTopicMap[${hash_subQuery}] = ${topicName}`,
        );
        this.subQueryMQTTTopicMap.set(hash_subQuery, topicName);
        this.logger.log(
          `DEBUG: subQueryMQTTTopicMap after set: ${JSON.stringify(Array.from(this.subQueryMQTTTopicMap.entries()))}`,
        );
        const rspQueryProcess = new RSPQueryProcess(
          rewrittenChunkQueries[i],
          topicName,
          this.sessionId,
          hash_subQuery,
        );
        this.logger.log(
          `${topicName} topic created for rewrittenChunkQueries: ${rewrittenChunkQueries[i]}: ${rewrittenChunkQueries[i]}`,
        );
        const p = rspQueryProcess
          .stream_process()
          .then(() => {
            this.logger.log(
              `Topic chunked/${hash_subQuery} created for subquery ${i}`,
            );
            this.logger.log(
              `RSP Query Process started for subquery ${i}: ${rewrittenChunkQueries[i]}`,
            );
          })
          .catch((error) => {
            console.error(
              `Error starting RSP Query Process for subquery ${i}: ${rewrittenChunkQueries[i]}`,
              error,
            );
          });
        allPromises.push(p);
      }
      // Wait for all subquery processes to finish initializing
      await Promise.all(allPromises);
      this.logger.log(
        `DEBUG: Final subQueryMQTTTopicMap: ${JSON.stringify(Array.from(this.subQueryMQTTTopicMap.entries()))}`,
      );
    } else {
      console.error("Failed to find a valid chunk size for the aggregation.");
      this.logger.log("Failed to find a valid chunk size for the aggregation.");
    }
  }

  /**
   *
   * @param subQueries
   * @param outputQuery
   */
  findGCDChunk(subQueries: string[], outputQuery: string): number {
    const window_parameters: number[] = [];
    for (let i = 0; i < subQueries.length; i++) {
      const subQueryParsed = this.parser.parse(subQueries[i]);
      this.logger.log(
        `Parsed subquery ${i}: ${JSON.stringify(subQueryParsed)}`,
      );

      if (subQueryParsed) {
        for (const s2r of subQueryParsed.s2r) {
          window_parameters.push(s2r.width);
          window_parameters.push(s2r.slide);
        }
      } else {
        console.error(`Failed to parse subquery: ${subQueries[i]}`);
      }
    }

    const outputQueryParsed = this.parser.parse(outputQuery);
    this.logger.log(
      `Parsed output query: ${JSON.stringify(outputQueryParsed)}`,
    );

    if (outputQueryParsed) {
      for (const s2r of outputQueryParsed.s2r) {
        window_parameters.push(s2r.width);
        window_parameters.push(s2r.slide);
      }
    } else {
      console.error(`Failed to parse output query: ${outputQuery}`);
    }
    // Find the GCD of the window parameters
    return this.findGCD(window_parameters);
  }

  /**
   *
   * @param arr
   */
  findGCD(arr: number[]): number {
    if (arr.length === 0) {
      return 1;
    }
    const gcd = (a: number, b: number): number => {
      return b === 0 ? a : gcd(b, a % b);
    };

    return arr.reduce((acc, val) => gcd(acc, val), arr[0]);
  }

  /**
   *
   * @param arr
   */
  findLCM(arr: number[]): number {
    const lcm = (a: number, b: number): number => {
      return (a * b) / this.findGCD([a, b]);
    };

    return arr.reduce((acc, val) => lcm(acc, val), 1);
  }

  /**
   *
   * @param query
   */
  addSubQuery(query: string): void {
    this.subQueries.push(query);
    if (this.logger) {
      this.logger.log(
        `addSubQuery called. Current subQueries length: ${this.subQueries.length}`,
      );
      this.logger.log(
        `addSubQuery called. Current subQueries: ${JSON.stringify(this.subQueries)}`,
      );
    }
  }

  /**
   *
   * @param query
   */
  setOutputQuery(query: string): void {
    this.outputQuery = query;
    this.logger.log(`Output query set: ${this.outputQuery}`);
    if (this.outputQuery === "") {
      console.error("Output query is empty. Please set a valid output query.");
    }
  }
  /**
   *
   */
  getOutputQuery(): string {
    return this.outputQuery ?? "";
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
  clearSubQueries(): void {
    this.subQueries = [];
  }

  /**
   *
   * @param query
   */
  detectAggregationFunction(query: string): string | null {
    const aggregationFunctions = ["SUM", "AVG", "COUNT", "MIN", "MAX"];
    for (const func of aggregationFunctions) {
      if (query.includes(func)) {
        return func;
      }
    }
    return null;
  }

  /**
   *
   * @param aggregationFunction
   * @param variable
   */
    getAggregationSPARQLQuery(
    aggregationFunction: string,
    variable: string,
  ): string {
    const allowedFunctions = ["AVG", "SUM", "COUNT", "MIN", "MAX"];

    if (!aggregationFunction || !variable) {
      console.error("Missing aggregation function or variable.");
      return "";
    }

    aggregationFunction = aggregationFunction.toUpperCase();
    if (!allowedFunctions.includes(aggregationFunction)) {
      console.error("Invalid aggregation function.");
      return "";
    }

    if (!variable.startsWith("?")) {
      variable = "?" + variable;
    }

    // Weighted Average Logic for AVG
    if (aggregationFunction === "AVG") {
       return `
        PREFIX saref: <https://saref.etsi.org/core/>
        SELECT ((SUM(?val * ?cnt) / SUM(?cnt)) AS ?result)
        WHERE {
          ?s saref:hasValue ?val .
          ?s saref:hasCount ?cnt .
        }
       `;
    }

    // COUNT: each sub-query emits per-sub-window count as saref:hasValue.
    // Summing those values yields the total count for the output window.
    if (aggregationFunction === "COUNT") {
      return `
        PREFIX saref: <https://saref.etsi.org/core/>
        SELECT (SUM(?val) AS ?result)
        WHERE {
          ?s saref:hasValue ?val .
        }
      `;
    }

    // Default behavior for other functions (assuming simple hasValue for now, but really only AVG is critical here)
    // Note: This simplifies the dynamic variable name usage because the RDF data is now consistently using saref:hasValue
    return `
      PREFIX saref: <https://saref.etsi.org/core/>
      SELECT (${aggregationFunction}(?val) AS ?result)
      WHERE {
        ?s saref:hasValue ?val .
      }
    `;
  }

  /**
   *
   * @param ms
   */
  sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Execute R2R Operator for a specific topic's chunks
   * @param topic
   * @param chunks
   */
  async executeR2ROperatorForTopic(
    topic: string,
    chunks: string[],
  ): Promise<number | null> {
    this.logger.log(
      `Executing the R2R Operator for topic ${topic} with ${chunks.length} chunks`,
    );

    const store = new N3.Store();
    const parser = new N3.Parser();

    // Filter out plain numeric values that might be duplicates of RDF values
    // Only process chunks that look like RDF (contain URIs and predicates)
    const rdfChunks = chunks.filter((chunk) => {
      let chunkString = chunk;
      // Handle JSON-wrapped chunks
      try {
        if (chunkString.startsWith('"') && chunkString.endsWith('"')) {
          chunkString = JSON.parse(chunkString);
        }
      } catch (e) {
        // If it's not JSON, use as-is
      }

      // Check if chunk contains RDF patterns (URIs and predicates)
      // Skip plain numeric values that are likely duplicates
      const isRDF =
        chunkString.includes("https://") && chunkString.includes("hasValue");
      const isPlainNumber = /^-?\d+\.?\d*$/.test(chunkString.trim());

      if (isPlainNumber) {
        this.logger.log(
          `DEBUG: Filtering out plain numeric duplicate for ${topic}: ${chunkString}`,
        );
        return false; // Skip plain numbers
      }

      return isRDF; // Only process RDF chunks
    });

    this.logger.log(
      `DEBUG: Filtered ${chunks.length} chunks down to ${rdfChunks.length} RDF chunks for topic ${topic}`,
    );

    for (const chunk of rdfChunks) {
      let chunkString = chunk;
      // If chunk is a JSON string, parse it
      try {
        if (chunkString.startsWith('"') && chunkString.endsWith('"')) {
          chunkString = JSON.parse(chunkString);
        }
      } catch (e) {
        this.logger.log(
          `DEBUG: Could not JSON.parse chunk for ${topic}: ${chunkString}`,
        );
      }
      try {
        const quads = parser.parse(chunkString);
        store.addQuads(quads);
      } catch (e) {
        this.logger.log(
          `DEBUG: Could not parse chunk as Turtle for ${topic}: ${chunkString}`,
        );
      }
    }

    if (store.size === 0) {
      this.logger.log(`No valid RDF data found for topic ${topic}`);
      return null;
    }

    this.logger.log(
      `Topic ${topic} RDF store contents: ${storeToString(store)}`,
    );
    const detectAggregationFunction = this.detectAggregationFunction(
      this.outputQuery,
    );
    if (!detectAggregationFunction) {
      console.error(
        `No aggregation function detected in the output query for topic ${topic}.`,
      );
      return null;
    }
    const aggregationSPARQLQuery = this.getAggregationSPARQLQuery(
      detectAggregationFunction,
      "o",
    );
    if (!aggregationSPARQLQuery) {
      console.error(
        `Failed to generate aggregation SPARQL query for topic ${topic}.`,
      );
      return null;
    }
    this.logger.log(
      `Generated Aggregation SPARQL Query for ${topic}: ${aggregationSPARQLQuery}`,
    );

    return new Promise<number | null>((resolve) => {
      const r2rOperator = new R2ROperator(aggregationSPARQLQuery);
      r2rOperator
        .execute(store)
        .then((bindingStream) => {
          if (!bindingStream) {
            console.error(`Failed to execute R2R Operator for topic ${topic}.`);
            resolve(null);
            return;
          }

          let completed = false;
          const finalize = (value: number | null) => {
            if (completed) return;
            completed = true;
            resolve(value);
          };

          bindingStream.on("data", (data: any) => {
            if (completed) return;
            const resultValue = data.get("result").value;
            this.logger.log(
              `R2R Operator Data Received for ${topic}: ${JSON.stringify(data)}`,
            );
            const numericResult = parseFloat(resultValue);
            finalize(numericResult);
          });
          bindingStream.on("end", () => {
            this.logger.log(
              `R2R Operator stream ended for ${topic} without additional results.`,
            );
            finalize(null);
          });
          bindingStream.on("error", (error: any) => {
            console.error(`R2R Operator error for topic ${topic}:`, error);
            finalize(null);
          });
        })
        .catch((error) => {
          console.error(
            `R2R Operator execution failed for topic ${topic}:`,
            error,
          );
          resolve(null);
        });
    });
  }

  /**
   * Publish combined results from all topics
   * @param finalResult
   */
  publishCombinedResults(finalResult: any): void {
    const rsp_client = mqtt.connect(this.mqttBroker);
    rsp_client.on("connect", () => {
      const resultTopic = getResultTopic("chunked/output");
      rsp_client.publish(
        resultTopic,
        JSON.stringify(finalResult),
        { qos: 1 },
        (error) => {
          if (error) {
            console.error("Failed to publish chunked results:", error);
            this.logger.log(`Failed to publish chunked results: ${error}`);
          } else {
            console.log(
              "Successfully published chunked results to chunked/output",
            );
            this.logger.log(
              "Successfully published chunked results to chunked/output",
            );
          }
          rsp_client.end();
        },
      );
    });
  }
}

/**
 *
 */
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
