import { RDFStream, RSPEngine } from "rsp-js";
import { RSPQLParser } from "hive-thought-rewriter";
import { EventEmitter } from "events";
const { DataFactory } = require("n3");
import { v4 as uuidv4 } from "uuid";
import { hash_string_md5, turtleStringToStore } from "../util/Util";
const mqtt = require("mqtt");

/**
 * Represents an RSP Agent that executes SPARQL/RSPQL queries.
 * It manages query registration, stream processing, and result publication.
 */
export class RSPAgent {
  public query: string;
  public r2s_topic: string;
  public rstream_emitter: EventEmitter;
  public rsp_engine: RSPEngine;
  public rspql_parser: RSPQLParser;
  public http_server_location: string;

  /**
   * Creates a new RSPAgent instance.
   * @param {string} query - The RSPQL query string to execute.
   * @param {string} r2s_topic - The MQTT topic to publish results to.
   */
  constructor(query: string, r2s_topic: string) {
    this.query = query;
    this.r2s_topic = r2s_topic;
    this.rspql_parser = new RSPQLParser();
    this.rsp_engine = new RSPEngine(query);
    this.rstream_emitter = this.rsp_engine.register();
    // Use HTTP_PORT from environment if available, otherwise default to 8080
    const httpPort = process.env.HTTP_PORT || "8080";
    this.http_server_location = `http://localhost:${httpPort}/`;
    // Register async but don't block - handle errors gracefully
    this.registerToQueryRegistry().catch((error) => {
      console.warn(
        `[RSPAgent] Could not register query to registry: ${error.message}. Continuing without registration.`,
      );
    });
    this.subscribeRStream();
  }

  /**
   * Registers the query with the central Query Registry via HTTP.
   * @returns {Promise<any>} The response from the query registry.
   * @throws {Error} If registration fails.
   */
  public async registerToQueryRegistry(): Promise<any> {
    console.log(`Registering query: ${this.query} to the query registry.`);
    const register_location = `${this.http_server_location}register`;

    // Try to register with retries
    let retries = 3;
    let lastError: Error | null = null;

    while (retries > 0) {
      try {
        const request = await fetch(register_location, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            rspql_query: this.query,
            r2s_topic: this.r2s_topic,
            data_topic: this.r2s_topic,
            id: hash_string_md5(this.query),
          }),
        });
        if (!request.ok) {
          throw new Error(
            `Failed to register query: ${this.query}. Status: ${request.status}`,
          );
        }
        const response = await request.json();
        if (response.error) {
          throw new Error(`Error registering query: ${response.error}`);
        }
        console.log(`Successfully registered query: ${this.query}`);
        return response;
      } catch (error) {
        lastError = error as Error;
        retries--;
        if (retries > 0) {
          console.log(
            `[RSPAgent] Failed to register to query registry, retrying... (${retries} attempts left)`,
          );
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }

    // If we get here, all retries failed - don't throw, just warn
    console.warn(
      `[RSPAgent] Could not register query to HTTP registry after retries: ${lastError?.message}`,
    );
    console.warn(
      `[RSPAgent] Continuing without HTTP registration - agent will still function via MQTT.`,
    );
    return null;
  }

  /**
   * Processes input streams defined in the query.
   * Connects to MQTT brokers and feeds data into the RSP engine.
   * @returns {Promise<void>}
   */
  public async process_streams(): Promise<void> {
    const streams = this.returnStreams();
    for (const stream of streams) {
      const stream_name = stream.stream_name;
      const mqtt_broker: string = this.returnMQTTBroker(stream_name);
      const rsp_client = mqtt.connect(mqtt_broker);
      const rsp_stream_object = this.rsp_engine.getStream(stream_name);
      const topic = new URL(stream_name).pathname.slice(1);

      rsp_client.on("connect", () => {
        console.log(`Connected to MQTT broker at ${mqtt_broker}`);

        rsp_client.subscribe(topic, (err: any) => {
          if (err) {
            console.error(`Failed to subscribe to stream ${stream_name}:`, err);
          } else {
            console.log(`Subscribed to stream ${stream_name}`);
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
              [rsp_stream_object],
              timestamp_epoch,
            );
          }
        } catch (error) {
          console.error(
            `Error processing message from stream ${stream_name}:`,
            error,
          );
        }
      });

      rsp_client.on("error", (error: any) => {
        console.error(
          `Error with MQTT client for stream ${stream_name}:`,
          error,
        );
      });
    }
  }

  /**
   * Extracts the MQTT broker URL from a stream name (IRI).
   * @param {string} stream_name - The stream name/IRI.
   * @returns {string} The MQTT broker URL.
   */
  public returnMQTTBroker(stream_name: string): string {
    const url = new URL(stream_name);
    return `${url.protocol}//${url.hostname}:${url.port}/`;
  }

  /**
   * Parses the query and returns the list of input streams.
   * @returns {any[]} Array of stream objects.
   */
  public returnStreams(): any[] {
    const parsed_query = this.rspql_parser.parse(this.query);
    const streams: any[] = [...parsed_query.s2r];
    return streams;
  }

  /**
   * Subscribes to the RSP engine's output stream and publishes results to MQTT.
   * @returns {Promise<void>}
   */
  public async subscribeRStream(): Promise<void> {
    const mqtt_broker = "mqtt://localhost:1883";
    const rstream_publisher = mqtt.connect(mqtt_broker);

    rstream_publisher.on("connect", () => {
      console.log("Connected to MQTT broker for publishing");

      this.rstream_emitter.on("RStream", async (object: any) => {
        if (!object || !object.bindings) {
          console.log(`No bindings found in the RStream object.`);
          return;
        }

        const iterables = object.bindings.values();

        for (const item of iterables) {
          const data = item.value;
          console.log("Binding data received:", data);
          // Format the output as RDF using generate_aggregation_event so operators can parse it
          const rdfFormattedData = this.generate_aggregation_event(
            data,
            Date.now(),
          );
          console.log(
            `Publishing RDF formatted data to ${this.r2s_topic}:`,
            rdfFormattedData,
          );
          rstream_publisher.publish(this.r2s_topic, rdfFormattedData);
        }
      });
    });

    rstream_publisher.on("error", (err: any) => {
      console.error("MQTT publisher error:", err);
    });
  }

  /**
   * Generates a unique aggregation event string.
   * @param {any} data - The data value.
   * @param {number} _timestamp - The timestamp (unused).
   * @returns {string} The aggregation event RDF string.
   */
  public generate_aggregation_event(data: any, _timestamp: number): string {
    const uuid_random = uuidv4();

    const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
    return aggregation_event.trim();
  }

  /**
   * Adds an event store (set of quads) to the RSP engine's streams.
   * @param {any} event_store - The N3 store containing event data.
   * @param {RDFStream[]} stream_name - Array of RDFStream objects to add data to.
   * @param {number} timestamp - The timestamp of the event.
   * @returns {Promise<void>}
   */
  public async add_event_store_to_rsp_engine(
    event_store: any,
    streams: RDFStream[],
    timestamp: number,
  ): Promise<void> {
    for (const stream of streams) {
      const quads = event_store.getQuads(null, null, null, null);
      for (const quad of quads) {
        // Use the stream's name property for the graph component
        const streamName = (stream as any).name || "default";
        const quadWithGraph = DataFactory.quad(
          quad.subject,
          quad.predicate,
          quad.object,
          DataFactory.namedNode(streamName),
        );
        stream.add(quadWithGraph, timestamp);
      }
    }
  }
}
