import { EventEmitter } from "events";
import { RDFStream, RSPEngine } from "rsp-js";
const mqtt = require("mqtt");
const { DataFactory } = require("n3");
import { v4 as uuidv4 } from "uuid";
import { RSPQLParser } from "rsp-js";
import { turtleStringToStore } from "../util/Util";

/**
 * Represents a process that executes an RSPQL query and streams the results.
 * It connects to MQTT brokers for input streams and publishes output to a result stream.
 */
export class RSPQueryProcess {
  public query: string;
  public rstream_topic: string;
  public rstream_emitter: EventEmitter;
  public rsp_engine: RSPEngine;
  public rspql_parser: RSPQLParser;

  /**
   * Creates a new RSPQueryProcess instance.
   * @param {string} query - The RSPQL query to execute.
   * @param {string} rstream_topic - The MQTT topic for the result stream.
   */
  constructor(query: string, rstream_topic: string) {
    this.query = query;
    this.rstream_topic = rstream_topic;
    this.rsp_engine = new RSPEngine(query);
    this.rstream_emitter = this.rsp_engine.register();
    this.rspql_parser = new RSPQLParser();
    this.subscribeToResultStream();
  }

  /**
   * Starts processing the query by connecting to input streams.
   * @returns {Promise<void>}
   */
  public async stream_process(): Promise<void> {
    console.log(`Processing query in RSPQueryProcess: ${this.query}`);
    if (!this.query || this.query.trim() === "") {
      console.error(`Query is empty or undefined.`);
      return;
    }
    const parsed_query = this.rspql_parser.parse(this.query);
    if (parsed_query) {
      const streams: any[] = [...parsed_query.s2r];
      console.log(
        `Parsed query successfully. Found ${streams.length} streams.`,
      );
      console.log(`The streams are: ${JSON.stringify(streams)}`);
      for (const stream of streams) {
        const stream_name = stream.stream_name;
        const stream_url = new URL(stream_name);
        const mqtt_broker: string = `${stream_url.protocol}//${stream_url.hostname}:${stream_url.port}/`;
        const rsp_client = mqtt.connect(mqtt_broker);
        const rsp_stream_object = this.rsp_engine.getStream(stream_name);
        const topic = stream_url.pathname.slice(1);
        console.log(
          `Connecting to MQTT broker at ${mqtt_broker} for stream ${stream_name}`,
        );
        rsp_client.on("connect", () => {
          console.log(`Connected to MQTT broker`);
          rsp_client.subscribe(topic, (err: any) => {
            if (err) {
              console.error(
                `Failed to subscribe to stream ${stream_name}:`,
                err,
              );
            } else {
              console.log(`Subscribed to stream ${stream_name}`);
            }
          });
        });

        rsp_client.on("message", async (topic: any, message: any) => {
          if (!message || message.length === 0) {
            console.error(`Received empty message on topic ${topic}`);
            return;
          }

          try {
            const message_string = message.toString();
            const latest_event_store =
              await turtleStringToStore(message_string);
            const timestamp = latest_event_store.getQuads(
              null,
              DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"),
              null,
              null,
            )[0]?.object.value;
            if (!timestamp) {
              console.error(
                `No timestamp found in the message for stream ${stream_name}`,
              );
              return;
            }
            const timestamp_epoch = Date.parse(timestamp);
            if (rsp_stream_object) {
              await this.add_event_store_to_rsp_engine(
                latest_event_store,
                [rsp_stream_object],
                timestamp_epoch,
              );
            } else {
              console.error(
                `Stream object not found for stream ${stream_name}`,
              );
              return;
            }
          } catch (error) {
            console.error(
              `Error processing message for stream ${stream_name}:`,
              error,
            );
            return;
          }
        });

        rsp_client.on("error", (err: any) => {
          console.error(`Error in MQTT client for stream ${stream_name}:`, err);
        });
        rsp_client.on("close", () => {
          console.log(`Connection closed for stream ${stream_name}`);
        });
      }
    } else {
      console.log(`Failed to parse query: ${this.query}`);
    }
  }

  /**
   * Subscribes to the RSP engine's result emitter and publishes to MQTT.
   * @returns {Promise<void>}
   */
  public async subscribeToResultStream(): Promise<void> {
    console.log(`Subscribing to result stream: ${this.rstream_topic}`);
    if (!this.rstream_topic || this.rstream_topic.trim() === "") {
      console.error(`RStream topic is empty or undefined.`);
      return;
    }

    const mqtt_broker = "mqtt://localhost:1883";
    const rstream_publisher = mqtt.connect(mqtt_broker);

    this.rstream_emitter.on("RStream", async (object: any) => {
      console.log(`Received RStream object: ${JSON.stringify(object)}`);

      if (!object || !object.bindings) {
        console.log(`No bindings found in the RStream object.`);
        return;
      }

      const iterables = object.bindings.values();
      for await (const iterable of iterables) {
        const event_timestamp = new Date().getTime();
        const data = iterable.value;
        console.log(
          `Processing data: ${data} at timestamp: ${event_timestamp}`,
        );
        const aggregation_event = this.generate_aggregation_event(
          data,
          event_timestamp,
        );
        // Publish RDF as plain string, not JSON-wrapped (operators expect raw RDF)
        rstream_publisher.publish(
          this.rstream_topic,
          aggregation_event,
          (err: any) => {
            if (err) {
              console.error(`Error publishing aggregation event: ${err}`);
            } else {
              console.log(
                `Successfully published aggregation event: ${aggregation_event}`,
              );
            }
          },
        );
        console.log(`Published aggregation event: ${aggregation_event}`);
      }
    });
  }

  /**
   * Generates a unique aggregation event string.
   * @param {any} data - The result data value.
   * @param {number} _timestamp - The timestamp of the event (unused in RDF generation).
   * @returns {string} The RDF string for the event.
   */
  public generate_aggregation_event(data: any, _timestamp: number): string {
    const uuid_random = uuidv4();

    const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
    return aggregation_event.trim();
  }

  /**
   * Adds an event store to the RSP engine.
   * @param {any} event_store - The N3 store containing event quads.
   * @param {RDFStream[]} streams - Array of RDF streams to add to.
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
