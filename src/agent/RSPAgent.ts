import { RDFStream, RSPEngine } from "rsp-js";
import { RSPQLParser } from "hive-thought-rewriter";
import { EventEmitter } from "events";
const { DataFactory } = require("n3");
import { v4 as uuidv4 } from 'uuid';
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import { buildBenchmarkTopicName } from "../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
const mqtt = require('mqtt');


/**
 *
 */
export class RSPAgent {

    public query: string;
    public r2s_topic: string;
    public rstream_emitter: EventEmitter;
    public rsp_engine: RSPEngine;
    public rspql_parser: RSPQLParser;
    public http_server_location: string;

    /**
     *
     * @param query
     * @param r2s_topic
     */
    constructor(query: string, r2s_topic: string) {
        this.query = query;
        this.r2s_topic = r2s_topic;
        this.rspql_parser = new RSPQLParser();
        this.rsp_engine = new RSPEngine(query);
        this.rstream_emitter = this.rsp_engine.register();
        this.http_server_location = "http://localhost:8080/";
        this.registerToQueryRegistry();
        this.subscribeRStream();


    }

    /**
     *
     */
    public async registerToQueryRegistry() {
        console.log(`Registering query: ${this.query} to the query registry.`);
        const register_location = `${this.http_server_location}register`;
        const request = await fetch(register_location, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                rspql_query: this.query,
                r2s_topic: this.r2s_topic,
                data_topic: this.r2s_topic,
                id: hash_string_md5(this.query)
            })
        });
        if (!request.ok) {
            throw new Error(`Failed to register query: ${this.query}. Status: ${request.status}`);
        }
        const response = await request.json();
        if (response.error) {
            throw new Error(`Error registering query: ${response.error}`);
        }
        console.log(`Successfully registered query: ${this.query}`);
        return response;
    }

    /**
     *
     */
    public async process_streams() {
        const streams = this.returnStreams();
        for (const stream of streams) {
            const stream_name = stream.stream_name;
            const mqtt_broker: string = this.returnMQTTBroker(stream_name);
            const rsp_client = mqtt.connect(mqtt_broker);
            const rsp_stream_object = this.rsp_engine.getStream(stream_name);
            const rawTopic = new URL(stream_name).pathname.replace(/^\/+/, "");
            const topic = rawTopic.startsWith("bench/")
              ? rawTopic
              : buildBenchmarkTopicName(rawTopic);

            rsp_client.on("connect", () => {
                console.log(`Connected to MQTT broker at ${mqtt_broker}`);

                rsp_client.subscribe(topic, (err: any) => {
                    if (err) {
                        console.error(`Failed to subscribe to stream ${stream_name}:`, err);
                    } else {
                        console.log(`Subscribed to stream ${topic}`);
                    }
                });
            });

            rsp_client.on("message", async (topic: any, message: any) => {
                try {
                    const message_string = message.toString();
                    const latest_event_store = await turtleStringToStore(message_string);
                    const timestamp = latest_event_store.getQuads(null, DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"), null, null)[0].object.value;
                    const timestamp_epoch = Date.parse(timestamp);
                    if (rsp_stream_object) {
                        await this.add_event_store_to_rsp_engine(latest_event_store, [rsp_stream_object], timestamp_epoch);
                    }
                } catch (error) {
                    console.error(`Error processing message from stream ${stream_name}:`, error);
                }
            });

            rsp_client.on("error", (error: any) => {
                console.error(`Error with MQTT client for stream ${stream_name}:`, error);
            });
        }
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
     */
    public returnStreams() {
        const parsed_query = this.rspql_parser.parse(this.query);
        const streams: any[] = [...parsed_query.s2r];
        return streams;
    }

    /**
     *
     */
    public async subscribeRStream() {
        const mqtt_broker = "mqtt://localhost:1883";
        const rstream_publisher = mqtt.connect(mqtt_broker);
        const query_hash = hash_string_md5(this.query);

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
                    rstream_publisher.publish(this.r2s_topic, data, (err: any) => {
                        if (err) {
                            console.error("MQTT publisher error:", err);
                            return;
                        }

                        recordPublishedMqttMessage({
                            topic: this.r2s_topic,
                            payload: data,
                            messageType: "reusable_result",
                        });
                    });
                }
            });
        });

        rstream_publisher.on("error", (err: any) => {
            console.error("MQTT publisher error:", err);
        });
    }

    /**
     *
     * @param data
     * @param timestamp
     */
    public generate_aggregation_event(data: any, timestamp: number) {
        const uuid_random = uuidv4();

        const aggregation_event = `
    <https://rsp.js/aggregation_event/${uuid_random}> <https://saref.etsi.org/core/hasValue> "${data}"^^<http://www.w3.org/2001/XMLSchema#float> .
    `;
        return aggregation_event.trim();

    }


    /**
     *
     * @param event_store
     * @param stream_name
     * @param timestamp
     */
    public async add_event_store_to_rsp_engine(event_store: any, stream_name: RDFStream[], timestamp: number) {
        stream_name.forEach(async (stream: RDFStream) => {
            const quads = event_store.getQuads(null, null, null, null);
            for (const quad of quads) {
                const quadWithGraph = DataFactory.quad(
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    DataFactory.namedNode(stream_name)
                );
                stream.add(quadWithGraph, timestamp);
            }
        });
    }


}
