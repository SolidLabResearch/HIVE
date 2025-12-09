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
 * FetchingAllDataClientSide processor.
 * Fetches all necessary data from streams and processes it locally.
 */
class FetchingAllDataClientSide {
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
  private tolerance: number = 5000;
  private startTime: number = 0;
  private lastValidResultTime: number = 0;
  private queryRegisteredTime: number = 0;

  /**
   * Creates a new FetchingAllDataClientSide instance.
   * @param {string} query - The RSPQL query to execute.
   * @param {string} r2s_topic - The MQTT topic to publish results to.
   * @param {string} [logDirectory] - Optional directory for log files.
   */
  constructor(query: string, r2s_topic: string, logDirectory?: string) {
    this.query = query;
    this.r2s_topic = r2s_topic;
    this.rspql_parser = new RSPQLParser();
    this.rsp_engine = new RSPEngine(query);
    this.rstream_emitter = this.rsp_engine.register();
    this.startTime = Date.now();
    this.queryRegisteredTime = Date.now();

    this.initializeLogging(logDirectory);
    this.log("fetching_query_registered");

    this.subscribeRStream();
  }

  /**
   * Initializes logging mechanism.
   * @param {string} [logDirectory] - Optional directory for log files.
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
   * Logs a message to the CSV file and console.
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
   * Processes the streams defined in the query.
   * Connects to MQTT brokers and subscribes to topics.
   */
  process_streams() {
    const streams = this.returnStreams();
    console.log("Processing streams:", streams);
    for (const stream of streams) {
      const stream_name = stream.stream_name;
      const mqtt_broker = this.returnMQTTBroker(stream_name);
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
   * Returns the list of streams from the parsed query.
   * @returns {any[]} Array of stream objects.
   */
  returnStreams(): any[] {
    const parsedQuery = this.rspql_parser.parse(this.query);
    const streams: any[] = [...parsedQuery.s2r];
    return streams;
  }

  /**
   * Extracts the MQTT broker URL from the stream name.
   * @param {string} stream_name - The name/IRI of the stream.
   * @returns {string} The MQTT broker URL.
   */
  public returnMQTTBroker(stream_name: string): string {
    const url = new URL(stream_name);
    return `${url.protocol}//${url.hostname}:${url.port}/`;
  }

  /**
   * Adds an event store to the RSP engine.
   * @param {any} event_store - The N3 store containing the event data.
   * @param {RDFStream} stream_name - The RSP-JS RDFStream object.
   * @param {number} timestamp - The timestamp of the event.
   * @returns {Promise<void>}
   */
  public async add_event_store_to_rsp_engine(
    event_store: any,
    stream_name: RDFStream,
    timestamp: number,
  ) {
    const quads = event_store.getQuads(null, null, null, null);
    const graph = DataFactory.namedNode(stream_name.name);
    for (const q of quads) {
      const quadWithGraph = DataFactory.quad(
        q.subject,
        q.predicate,
        q.object,
        graph,
      );
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
   * Checks if the timestamp is within the expected window timing.
   * @param {number} _timestamp - The timestamp to check.
   * @returns {boolean} True if within window (currently always true).
   */
  private isWithinExpectedWindowTiming(_timestamp: number): boolean {
    // Timing filter disabled to allow all results during experiment replay
    return true;
  }

  /**
   * Subscribes to the RStream emitter to handle query results.
   * @returns {Promise<void>}
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

        if (!this.isWithinExpectedWindowTiming(currentTimestamp)) {
          this.log(`Filtered out result due to timing: ${data}`);
          continue;
        }

        this.log(`Processing valid result: ${data}`);

        console.log("DEBUG: RStream binding:", item);
        const aggregation_event = this.generate_aggregation_event(data);
        const aggregation_object_string = JSON.stringify(aggregation_event);
        console.log(
          `Aggregation event generated: ${aggregation_object_string}`,
        );

        this.log(`Generated aggregation event for result: ${data}`);

        const clientId = hash_string_md5(aggregation_object_string);
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
   * Generates an aggregation event string (RDF).
   * @param {any} data - The data value.
   * @returns {string} The RDF string representing the event.
   */
  public generate_aggregation_event(data: any): string {
    const uuid_random = uuidv4();

    const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
    return aggregation_event.trim();
  }

  /**
   * Cleans up resources.
   * @returns {void}
   */
  public cleanup(): void {
    if (this.logStream) {
      this.logStream.end();
    }
  }
}

/**
 * Fetching Client Side Approach Orchestrator
 * This orchestrator fetches all data on the client side for processing.
 */
export class FetchingClientSideApproachOrchestrator {
  private client?: FetchingAllDataClientSide;
  private resourceLogStream?: fs.WriteStream;
  private resourceLogInterval?: ReturnType<typeof setInterval>;

  /**
   * Creates a new FetchingClientSideApproachOrchestrator instance.
   */
  constructor() {}

  /**
   * Get the name of this approach.
   * @returns {string} The name of the approach.
   */
  public getName(): string {
    return "fetching-client-side";
  }

  /**
   * Initialize and run the experiment.
   * @returns {Promise<any>} A promise that resolves with the experiment status.
   */
  public async runExperiment(): Promise<any> {
    console.log(`[FetchingClientSide] Starting experiment`);

    try {
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
      this.client = new FetchingAllDataClientSide(
        query,
        r2s_topic,
        logDirectory,
      );

      this.startResourceUsageLogging();

      this.client.process_streams();

      console.log(
        `[FetchingClientSide] Experiment started, processing streams...`,
      );

      return { status: "running" };
    } catch (error) {
      console.error(`[FetchingClientSide] Error during experiment:`, error);
      throw error;
    }
  }

  /**
   * Logs CPU and memory usage to a CSV file.
   * @param {string} filePath - Path to log file.
   * @param {number} intervalMs - Logging interval in milliseconds.
   * @returns {void}
   */
  private startResourceUsageLogging(
    filePath = "fetching_client_side_resource_usage.csv",
    intervalMs = 100,
  ): void {
    const writeHeader = !fs.existsSync(filePath);
    this.resourceLogStream = fs.createWriteStream(filePath, { flags: "a" });
    if (writeHeader) {
      this.resourceLogStream.write(
        "timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n",
      );
    }
    this.resourceLogInterval = setInterval(() => {
      if (!this.resourceLogStream) return;

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
      this.resourceLogStream.write(line);
    }, intervalMs);
  }

  /**
   * Clean up resources.
   * @returns {void}
   */
  public cleanup(): void {
    if (this.resourceLogInterval) {
      clearInterval(this.resourceLogInterval);
    }
    if (this.resourceLogStream) {
      this.resourceLogStream.end();
    }
    if (this.client) {
      this.client.cleanup();
    }
    console.log("[FetchingClientSide] Cleanup completed");
  }
}

/**
 * Runs the FetchingClientSide approach in standalone mode.
 * @returns {Promise<void>}
 */
async function runStandaloneFetchingClientSide(): Promise<void> {
  const orchestrator = new FetchingClientSideApproachOrchestrator();

  process.on("exit", () => orchestrator.cleanup());
  process.on("SIGINT", () => {
    console.log("Process interrupted, cleaning up...");
    orchestrator.cleanup();
    process.exit(0);
  });
  process.on("SIGTERM", () => {
    console.log("Process terminated, cleaning up...");
    orchestrator.cleanup();
    process.exit(0);
  });

  try {
    await orchestrator.runExperiment();
  } catch (error) {
    console.error("Error during client-side processing:", error);
    orchestrator.cleanup();
    process.exit(1);
  }
}

if (require.main === module) {
  runStandaloneFetchingClientSide();
}

export default FetchingClientSideApproachOrchestrator;
