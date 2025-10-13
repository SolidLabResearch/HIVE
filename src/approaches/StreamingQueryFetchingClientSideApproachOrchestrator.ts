import { EventEmitter } from "events";
import { RDFStream, RSPEngine, RSPQLParser } from "rsp-js";
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import { v4 as uuidv4 } from "uuid";
import fs from "fs";
import path from "path";
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
  private windowStreamMap: { [key: string]: string } = {
    "mqtt://localhost:1883/wearableX": "https://rsp.jsw1",
    "mqtt://localhost:1883/smartphoneX": "https://rsp.jsw1",
  };
  private expectedWindowInterval: number = 60000; // 60 seconds based on STEP 60000
  private tolerance: number = 5000; // 5 second tolerance
  private startTime: number = 0; // Track when processing started
  private lastValidResultTime: number = 0; // Track last valid result timing
  private queryRegisteredTime: number = 0; // Track when query was registered

  /**
   * Constructor for the FetchingAllDataClientSide class.
   * @param {string} query - The RSP-QL query to be executed.
   * @param {string} r2s_topic - The MQTT topic to publish results to.
   * @param {string} [logDirectory] - Optional directory to save logs.
   */
  constructor(query: string, r2s_topic: string, logDirectory?: string) {
    this.query = query;
    this.r2s_topic = r2s_topic;
    this.rspql_parser = new RSPQLParser();
    this.rsp_engine = new RSPEngine(query);
    this.rstream_emitter = this.rsp_engine.register();
    this.startTime = Date.now(); // Initialize start time for filtering
    this.queryRegisteredTime = Date.now(); // Track when query was registered

    // Initialize CSV logging for this approach
    this.initializeLogging(logDirectory);
    this.log("fetching_query_registered");

    this.subscribeRStream();
    this.startResourceUsageLogging();
  }

  /**
   * Initialize logging to a CSV file.
   * @param {string} [logDirectory] - Optional directory to save logs.
   */
  private initializeLogging(logDirectory?: string) {
    const baseName = "fetching_client_side_log.csv";
    const logFilePath = logDirectory
      ? path.join(logDirectory, baseName)
      : baseName;
    const writeHeader = !fs.existsSync(logFilePath);
    this.logStream = fs.createWriteStream(logFilePath, { flags: "a" });

    if (writeHeader) {
      this.logStream.write("timestamp,message\n");
    }
  }

  /**
   * Log a message with timestamp.
   * @param {string} message - The message to log.
   */
  public log(message: string) {
    const timestamp = Date.now();
    if (this.logStream) {
      this.logStream.write(`${timestamp},"${message}"\n`);
    }
    console.log(`LOG: ${timestamp} - ${message}`);
  }

  /**
   * Process streams by connecting to MQTT brokers and subscribing to topics.
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
   * Returns the list of streams involved in the query.
   * @returns {any[]} Array of stream objects.
   */
  returnStreams(): any[] {
    const parsedQuery = this.rspql_parser.parse(this.query);
    const streams: any[] = [...parsedQuery.s2r];
    return streams;
  }

  /**
   * Returns the MQTT broker URL from a stream name.
   * @param {string} stream_name - The stream name (e.g., mqtt://localhost:1883/wearableX).
   * @returns {string} The MQTT broker URL (e.g., mqtt://localhost:1883/).
   */
  public returnMQTTBroker(stream_name: string): string {
    const url = new URL(stream_name);
    return `${url.protocol}//${url.hostname}:${url.port}/`;
  }

  /**
   * Add events from an event store to the RSP engine's stream with correct graph node and timestamp.
   * @param {string} event_store - The N3 Store containing the events.
   * @param {RDFStream} stream_name - The RDFStream to add events to.
   * @param {number} timestamp - The timestamp to associate with the events.
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
   * Filter results based on expected RSP-QL window timing to ignore extra dynamic windows.
   * @param {number} timestamp - Current timestamp.
   * @returns {boolean} - If result should be processed, false if it should be ignored.
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
   * Subscribe to the RStream emitter to process results and publish aggregation events to MQTT.
   * @returns {Promise<void>} - A promise that resolves when subscription is set up.
   */
  public async subscribeRStream(): Promise<void> {
    console.log("Subscribing to RStream...");
    if (!this.rstream_emitter) {
      console.error("RStream emitter is not initialized.");
      return;
    }
    this.rstream_emitter.on("error", (err: any) => {
      console.error("Error in RStream emitter:", err);
    });
    this.rstream_emitter.on("RStream", (object: any) => {
      if (!object || !object.bindings) {
        console.error("Received invalid RStream object:", object);
        return;
      }

      const iterables = object.bindings.values();

      for (const item of iterables) {
        const data = item.value;
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

        // Debug: print the full binding object
        console.log("DEBUG: RStream binding:", item);
        const aggregation_event = this.generate_aggregation_event(data);
        const aggregation_object_string = JSON.stringify(aggregation_event);
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
                console.log("Aggregation event published with QoS 2");
                this.log(`Successfully published result: ${data}`);
              }
              pubClient.end();
            },
          );
        });
      }
    });
  }

  /**
   * Generate an aggregation event in Turtle format with a unique identifier and the given data value with a simple ontology.
   * @param {any} data - The data value to include in the aggregation event.
   * @returns {string} The aggregation event in Turtle format.
   */
  public generate_aggregation_event(data: any): string {
    const uuid_random = uuidv4();

    const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
    return aggregation_event.trim();
  }

  /**
   * Clean up resources like log streams.
   * @returns {void} - Nothing, just cleans up the log stream.
   */
  public cleanup(): void {
    if (this.logStream) {
      this.logStream.end();
    }
  }

  /**
   * Logs CPU and memory usage to a CSV file at regular intervals.
   * @param {string} filePath - Path to the CSV file. Default is 'fetching_client_side_resource_usage.csv'.
   * @param {number} intervalMs - Interval in milliseconds for logging. Default is 100ms.
   * @returns {void} - Nothing, just starts the logging interval.
   */
  startResourceUsageLogging(
    filePath = "fetching_client_side_resource_usage.csv",
    intervalMs = 100,
  ): void {
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
 * Client-side processing function to execute the RSP-QL query and handle results.
 * @returns {Promise<void>} - A promise that resolves when processing is complete.
 */
async function clientSideProcessing(): Promise<void> {
  const query = `
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

  console.log(new RSPQLParser().parse(query).sparql);

  const r2s_topic = "client_operation_output";
  const logDirectory = process.env.CUSTOM_LOG_DIR;
  const client = new FetchingAllDataClientSide(query, r2s_topic, logDirectory);

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
