import { RSPQLParser } from "rsp-js";
import { RewriteChunkQuery } from "hive-thought-rewriter";
import { RSPQueryProcess } from "../../rsp/RSPQueryProcess";
import { hash_string_md5, storeToString } from "../../util/Util";
import { R2ROperator } from "./r2r";
import mqtt from "mqtt";
import { CSVLogger } from "../../util/logger/CSVLogger";
import { IStreamQueryOperator } from "../../util/Interfaces";
import * as fs from "fs";
const N3 = require("n3");

/**
 * Operator that aggregates streaming query results using chunk-based processing.
 * It rewrites queries to operate on smaller time chunks and aggregates the partial results.
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
  private resultsCsvStream?: fs.WriteStream;
  private queryRegisteredTimestamp: number | null = null;
  // Timer and MQTT client references for proper cleanup
  private windowEvaluationTimer?: NodeJS.Timeout;
  private aggregationMqttClient?: mqtt.MqttClient;
  /**
   * Creates a new StreamingQueryChunkAggregatorOperator instance.
   */
  constructor() {
    this.subQueries = [];
    this.parser = new RSPQLParser();
    this.chunkGCD = 0;
    this.logger = new CSVLogger("streaming_query_chunk_aggregator_log.csv");

    // Initialize results CSV in results/ folder
    const resultsDir = "results";
    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true });
    }
    const resultsFileName = "chunked_query_results.csv";
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
   * Initializes the operator by setting up the MQTT topic map.
   * @returns {Promise<void>} A promise that resolves when initialization is complete.
   */
  public async init(): Promise<void> {
    this.logger.log("init() called");
    await this.setMQTTTopicMap();
    this.logger.log("StreamingQueryChunkAggregatorOperator initialized.");
  }

  /**
   * Sets the output query for the aggregation.
   * @param {string} query - The output query string.
   */
  addOutputQuery(query: string): void {
    this.outputQuery = query;
    // Set query registration timestamp when output query is added
    if (!this.queryRegisteredTimestamp) {
      this.queryRegisteredTimestamp = Date.now();
    }
  }

  /**
   * Fetches existing queries from the server and maps them to MQTT topics.
   * Falls back to locally added subqueries if HTTP fetch fails.
   * @returns {Promise<void>} A promise that resolves when the topic map is set.
   */
  async setMQTTTopicMap(): Promise<void> {
    this.logger.log("setMQTTTopicMap() called");
    this.queryMQTTTopicMap = new Map<string, string>();

    // Try to fetch with retries, but don't fail if HTTP server is not available
    let retries = 3;
    let lastError: Error | null = null;

    while (retries > 0) {
      try {
        const response = await fetch("http://localhost:8080/fetchQueries");
        if (!response.ok) {
          throw new Error(`Failed to fetch queries: ${response.status}`);
        }
        const data = await response.json();
        // Log the full structure for debugging
        this.logger.log(
          `Fetched data from server (full JSON): ${JSON.stringify(data, null, 2)}`,
        );

        for (const [queryHash, mqttTopic] of Object.entries(data)) {
          const topicString = mqttTopic;
          this.queryMQTTTopicMap.set(
            queryHash as string,
            topicString as string,
          );
          this.logger.log(
            `DEBUG: queryMQTTTopicMap after set: ${JSON.stringify(Array.from(this.queryMQTTTopicMap.entries()))}`,
          );
          this.logger.log(
            `Subquery ${queryHash} mapped to MQTT Topic ${topicString}`,
          );
        }
        this.logger.log(
          `MQTT Topic Map set for subqueries: ${JSON.stringify(Object.fromEntries(this.queryMQTTTopicMap))}`,
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

    // If we get here, all retries failed - use fallback
    console.warn(
      `Could not fetch queries from HTTP server: ${lastError?.message}`,
    );
    console.warn(
      "Continuing with locally added queries only (if any were added via addSubQuery)",
    );

    // Build queryMQTTTopicMap from subQueries that were added directly
    if (this.subQueries.length > 0) {
      console.log(
        `Using ${this.subQueries.length} locally added subqueries instead`,
      );
      for (const query of this.subQueries) {
        const queryHash = hash_string_md5(query);
        const mqttTopic = `chunked/${queryHash}`;
        this.queryMQTTTopicMap.set(queryHash, mqttTopic);
        this.logger.log(
          `Fallback: Mapped query hash ${queryHash} to topic ${mqttTopic}`,
        );
      }
      this.logger.log(
        `Fallback MQTT Topic Map set: ${JSON.stringify(Object.fromEntries(this.queryMQTTTopicMap))}`,
      );
    } else {
      this.logger.log("No subqueries available for fallback mapping");
    }
  }

  /**
   * Orchestrates the aggregation process by initializing sub-queries and processing results.
   * @returns {Promise<void>} A promise that resolves when the aggregation process is set up.
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

    // Wait for MQTT connection before proceeding
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("MQTT connection timeout after 10 seconds"));
      }, 10000);

      rsp_client.on("connect", () => {
        clearTimeout(timeout);
        this.logger.log(
          `subQueryTopicMap : ${JSON.stringify(that.subQueryMQTTTopicMap)}`,
        );
        const topics = Array.from(that.subQueryMQTTTopicMap.values());
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

        // Data structure to collect chunks by topic with timestamps
        const chunksByTopic: Map<
          string,
          { data: string; timestamp: number }[]
        > = new Map();
        const chunksRequired =
          Math.ceil(outputQueryWidth / this.chunkGCD) * this.subQueries.length;
        this.logger.log(`Chunks required for aggregation: ${chunksRequired}`);
        this.logger.log(
          `Output Query Width: ${outputQueryWidth}, Chunk GCD: ${this.chunkGCD}, SubQueries Length: ${this.subQueries.length}`,
        );

        // Track last aggregation time for deduplication
        let lastAggregationTime = 0;
        const minAggregationInterval = 500; // Minimum 500ms between aggregations to avoid duplicates
        let isAggregating = false; // Prevent concurrent aggregations

        // Store MQTT client reference for cleanup
        this.aggregationMqttClient = rsp_client;

        // Event-driven aggregation function - triggers when conditions are met
        const tryAggregation = async () => {
          const now = Date.now();

          // Prevent concurrent aggregations and respect minimum interval
          if (
            isAggregating ||
            now - lastAggregationTime < minAggregationInterval
          ) {
            return;
          }

          const windowStart = now - outputQueryWidth;

          // Collect all chunks from all topics within the window
          const allWindowChunks: string[] = [];
          let totalTopicsWithData = 0;

          for (const [topic, chunks] of Array.from(chunksByTopic.entries())) {
            const windowChunks = chunks.filter(
              (chunk) => chunk.timestamp >= windowStart,
            );

            if (windowChunks.length > 0) {
              totalTopicsWithData++;
              allWindowChunks.push(...windowChunks.map((chunk) => chunk.data));
            }
          }

          // Check if we have enough data to aggregate
          // Need data from all expected topics (subQueries.length)
          const expectedTopics = this.subQueries.length;
          const hasEnoughChunks = allWindowChunks.length >= chunksRequired;
          const hasAllTopics = totalTopicsWithData >= expectedTopics;

          if (hasAllTopics && hasEnoughChunks && allWindowChunks.length > 0) {
            isAggregating = true;
            lastAggregationTime = now;

            this.logger.log(
              `EVENT-DRIVEN aggregation triggered. Topics: ${totalTopicsWithData}/${expectedTopics}, Chunks: ${allWindowChunks.length}/${chunksRequired}`,
            );

            try {
              // Process all chunks together
              await this.executeR2ROperator(allWindowChunks);

              // Clean up old chunks after successful aggregation
              for (const [topic, chunks] of Array.from(
                chunksByTopic.entries(),
              )) {
                chunksByTopic.set(
                  topic,
                  chunks.filter((chunk) => chunk.timestamp >= windowStart),
                );
              }
            } finally {
              isAggregating = false;
            }
          }
        };

        rsp_client.on("message", async (topic, message) => {
          this.logger.log(
            `Received message on topic ${topic}: ${message.toString()}`,
          );

          // Initialize topic array if it doesn't exist
          if (!chunksByTopic.has(topic)) {
            chunksByTopic.set(topic, []);
          }

          // Add chunk to the appropriate topic
          chunksByTopic
            .get(topic)!
            .push({ data: message.toString(), timestamp: Date.now() });

          // EVENT-DRIVEN: Try to aggregate immediately when new data arrives
          await tryAggregation();
        });

        // Clear any existing timer before creating new one (prevents memory leak)
        if (this.windowEvaluationTimer) {
          clearInterval(this.windowEvaluationTimer);
          this.windowEvaluationTimer = undefined;
          this.logger.log("Cleared existing window evaluation timer");
        }

        // Backup timer: also check periodically in case event-driven misses edge cases
        // Use a shorter interval for faster response
        this.windowEvaluationTimer = setInterval(async () => {
          await tryAggregation();
        }, 1000); // Check every 1 second as backup

        resolve();
      });

      rsp_client.on("error", (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    this.logger.log("MQTT connection established and subscriptions ready");
    console.log("[CHUNKED] Connected to MQTT broker");
  }

  /**
   * Executes the R2R operator on a set of data chunks.
   * @param {string[]} chunks - The data chunks to process.
   * @returns {Promise<void>} A promise that resolves when execution is complete.
   */
  async executeR2ROperator(chunks: string[]): Promise<void> {
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
    bindingStream.on("data", (data: any) => {
      this.logger.log(`R2R Operator Data Received: ${data}`);
      const outputQueryEvent = this.generateOutputQueryEvent(
        data.get("result").value,
      );
      this.logger.log(`Generated Output Query Event: ${outputQueryEvent}`);
      // Publish the output query event to the MQTT broker
      const rsp_client = mqtt.connect(this.mqttBroker);
      rsp_client.on("connect", () => {
        const outputTopic = `chunked/output`;
        this.logger.log(`calculated result ${outputQueryEvent}`);

        rsp_client.publish(outputTopic, outputQueryEvent, (err: any) => {
          if (err) {
            console.error(
              `Error publishing output query event to topic ${outputTopic}:`,
              err,
            );
          } else {
            this.logger.log(
              `Output query event published to topic ${outputTopic}`,
            );

            // Write to results CSV
            if (this.resultsCsvStream && this.queryRegisteredTimestamp) {
              const resultTimestamp = Date.now();
              const resultValue = data.get("result").value;
              const csvLine = `${this.queryRegisteredTimestamp},${resultTimestamp},${resultValue}\n`;
              this.resultsCsvStream.write(csvLine);
            }
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
    });
  }

  /**
   * Generates a unique output query event string.
   * @param {any} data - The data value to include in the event.
   * @returns {string} The formatted event string.
   */
  generateOutputQueryEvent(data: any): string {
    const uuid_random = uuidv4();
    return ` <https://rsp.js/outputQueryEvent/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .`;
  }

  /**
   * Initializes the sub-query processes for chunked processing.
   * @returns {Promise<void>} A promise that resolves when initialization is complete.
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
        this.logger.log(
          `DEBUG: Setting subQueryMQTTTopicMap[${hash_subQuery}] = chunked/${hash_subQuery}`,
        );
        this.subQueryMQTTTopicMap.set(
          hash_subQuery,
          `chunked/${hash_subQuery}`,
        );
        this.logger.log(
          `DEBUG: subQueryMQTTTopicMap after set: ${JSON.stringify(Array.from(this.subQueryMQTTTopicMap.entries()))}`,
        );
        const rspQueryProcess = new RSPQueryProcess(
          rewrittenChunkQueries[i],
          `chunked/${hash_subQuery}`,
        );
        this.logger.log(
          `chunked/${hash_subQuery} topic created for rewrittenChunkQueries: ${rewrittenChunkQueries[i]}: ${rewrittenChunkQueries[i]}`,
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
   * Calculates the GCD of window parameters from sub-queries and output query.
   * @param {string[]} subQueries - The list of sub-query strings.
   * @param {string} outputQuery - The output query string.
   * @returns {number} The greatest common divisor of the window sizes.
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
   * Calculates the Greatest Common Divisor of an array of numbers.
   * @param {number[]} arr - Array of numbers to process.
   * @returns {number} The GCD of the numbers.
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
   * Calculates the Least Common Multiple of an array of numbers.
   * @param {number[]} arr - Array of numbers to process.
   * @returns {number} The LCM of the numbers.
   */
  findLCM(arr: number[]): number {
    const lcm = (a: number, b: number): number => {
      return (a * b) / this.findGCD([a, b]);
    };

    return arr.reduce((acc, val) => lcm(acc, val), 1);
  }

  /**
   * Adds a sub-query to the list.
   * @param {string} query - The sub-query string to add.
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
   * Sets the output query string.
   * @param {string} query - The output query string.
   */
  setOutputQuery(query: string): void {
    this.outputQuery = query;
    this.logger.log(`Output query set: ${this.outputQuery}`);
    if (this.outputQuery === "") {
      console.error("Output query is empty. Please set a valid output query.");
    }
  }
  /**
   * Gets the current output query string.
   * @returns {string} The output query string.
   */
  getOutputQuery(): string {
    return this.outputQuery ?? "";
  }
  /**
   * Gets the list of sub-queries.
   * @returns {string[]} An array of sub-query strings.
   */
  getSubQueries(): string[] {
    return this.subQueries;
  }
  /**
   * Clears all registered sub-queries.
   * @returns {void}
   */
  clearSubQueries(): void {
    this.subQueries = [];
  }

  /**
   * Detects the aggregation function used in a query string.
   * @param {string} query - The query string to analyze.
   * @returns {string | null} The detected aggregation function name or null if not found.
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
   * Generates a SPARQL query for aggregating results.
   * @param {string} aggregationFunction - The aggregation function to use (e.g., AVG, SUM).
   * @param {string} variable - The variable name to aggregate.
   * @returns {string} The generated SPARQL query string.
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

    return `SELECT (${aggregationFunction}(${variable}) AS ?result) WHERE { ?s ?p ${variable} }`;
  }

  /**
   * Pauses execution for a specified duration.
   * @param {number} ms - The duration to sleep in milliseconds.
   * @returns {Promise<void>} A promise that resolves after the specified duration.
   */
  sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Cleans up resources used by the operator.
   * This should be called when the operator is no longer needed to prevent memory leaks.
   */
  public cleanup(): void {
    this.logger.log(
      "Cleaning up StreamingQueryChunkAggregatorOperator resources",
    );

    // Clear the window evaluation timer
    if (this.windowEvaluationTimer) {
      clearInterval(this.windowEvaluationTimer);
      this.windowEvaluationTimer = undefined;
      this.logger.log("Window evaluation timer cleared");
    }

    // Close MQTT client connection
    if (this.aggregationMqttClient) {
      this.aggregationMqttClient.end(true);
      this.aggregationMqttClient = undefined;
      this.logger.log("MQTT client connection closed");
    }

    // Close results CSV stream
    if (this.resultsCsvStream) {
      this.resultsCsvStream.end();
      this.resultsCsvStream = undefined;
      this.logger.log("Results CSV stream closed");
    }

    this.logger.log("StreamingQueryChunkAggregatorOperator cleanup completed");
  }

  /**
   * Execute R2R Operator for a specific topic's chunks.
   * @param {string} topic - The topic associated with the chunks.
   * @param {string[]} chunks - The data chunks to process.
   * @returns {Promise<number | null>} A promise resolving to the numeric result or null on failure.
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
          bindingStream.on("data", (data: any) => {
            const resultValue = data.get("result").value;
            this.logger.log(
              `R2R Operator Data Received for ${topic}: ${JSON.stringify(data)}`,
            );
            const numericResult = parseFloat(resultValue);
            resolve(numericResult);
          });
          bindingStream.on("error", (error: any) => {
            console.error(`R2R Operator error for topic ${topic}:`, error);
            resolve(null);
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
   * Publish combined results from all topics.
   * @param {any} finalResult - The result object to publish.
   */
  publishCombinedResults(finalResult: any): void {
    const rsp_client = mqtt.connect(this.mqttBroker);
    rsp_client.on("connect", () => {
      // Publish to chunked/output topic to differentiate from other approaches
      rsp_client.publish(
        "chunked/output",
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
 * Generates a random UUID v4 string.
 * @returns {string} A random UUID string.
 */
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
