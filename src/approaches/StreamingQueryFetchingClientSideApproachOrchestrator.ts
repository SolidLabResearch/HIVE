import { EventEmitter } from "events";
import fs from "fs";
import { RDFStream, RSPEngine, RSPQLParser } from "rsp-js";
import { v4 as uuidv4 } from "uuid";
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  buildOutputSelectClause,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getResultTopic,
  getSessionId,
} from "../util/runtimeConfig";
const N3 = require("n3");
const mqtt = require("mqtt");
const { DataFactory } = N3;

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
        "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_obs_ms,result_value\n",
      );
    }
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
    // Metric 1: Latency from query registration (original calculation)
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
      `  - From query registration: ${latencyFromQueryReg}ms (expected close: ${expectedWindowClose}, result: ${resultTime})`,
    );
    console.log(
      `  - From data start: ${latencyFromDataStart}ms (first data: ${this.firstDataReceivedTime}, expected: ${expectedFromDataStart}, result: ${resultTime})`,
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
      const rsp_client = mqtt.connect(mqtt_broker, { clean: false, clientId });
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
      });

      rsp_client.on("message", async (topic: any, message: any) => {
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

        this.log(
          `RStream result generated: ${data} at timestamp: ${currentTimestamp}`,
        );

        // Apply timing filter to ignore extra dynamic windows
        if (!this.isWithinExpectedWindowTiming(currentTimestamp)) {
          // Skip this result - it's from an extra dynamic window
          this.log(`Filtered out result due to timing: ${data}`);
          continue;
        }

        this.log(`Processing valid result: ${data}`);

        // Calculate and log latency with multiple metrics
        this.windowCount++;
        const resultEmittedAt = Date.now();
        const expectedWindowClose = this.getExpectedWindowCloseTime(
          this.windowCount,
        );
        this.logLatency(
          this.windowCount,
          expectedWindowClose,
          this.lastObservationReceivedTime,
          resultEmittedAt,
          data,
        );

        // Debug: print the full binding object
        // console.log("DEBUG: RStream binding:", binding);

        const numericValue = Number.parseFloat(data);
        const useBenchmarkPayload = Boolean(process.env.RESULT_TOPIC);
        const aggregation_object_string = useBenchmarkPayload
          ? JSON.stringify(
              buildBenchmarkResultPayload(
                "fetching",
                this.aggregationFunction,
                this.sessionId,
                numericValue,
                this.windowCount,
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
  const query = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT ${buildOutputSelectClause(aggregationFunction)}
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
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
