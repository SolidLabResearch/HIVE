import { RDFStream, RSPEngine } from "rsp-js";
import { RSPQLParser } from "hive-thought-rewriter";
import { EventEmitter } from "events";
const { DataFactory } = require("n3");
import { v4 as uuidv4 } from 'uuid';
import { hash_string_md5, turtleStringToStore } from "../util/Util";
import {
    buildBenchmarkTopicName,
    isApproximationDebugEnabled,
    useCompactReusableResultPayload,
} from "../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
import {
    endStageTimer,
    profileCount,
    profileStageSync,
    profileSync,
    startStageTimer,
    writeProfileArtifact,
    writeStageProfileArtifact,
} from "../util/profiling";
const mqtt = require('mqtt');

type StructuredReusableResultPayload = {
    message_format: "structured_reusable_result";
    source_query_id: string;
    source_topic?: string | null;
    reusable_result_topic?: string;
    aggregationType: string | null;
    value: number | null;
    resultValue?: number | null;
    count: number | null;
    sum: number | null;
    avg: number | null;
    min: number | null;
    max: number | null;
    raw_bindings?: Record<string, string>;
    window_start: number | null;
    window_end: number | null;
    window_data_close_time: number | null;
    logical_trigger_time?: number | null;
    timestamp_from?: number | null;
    timestamp_to?: number | null;
    result_emitted_at?: number;
    window?: {
        start: number | null;
        end: number | null;
        range: number | null;
        step: number | null;
        semantics: "[start,end)";
        windowSemantics: string;
        logicalTriggerTime: number | null;
        windowDataCloseTime: number | null;
        resultEmittedAt: number;
        metadataSource: "reconstructed";
    };
};


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
    private mqttClients: any[] = [];
    private cleanupRegistered: boolean = false;
    private readonly queryId: string;
    private readonly parsedQuery: any;
    private readonly aggregationType: string | null;
    private readonly sourceTopic: string | null;
    private readonly range: number | null;
    private readonly step: number | null;
    private readonly debugStructuredReusableResults: boolean;
    private readonly compactReusableResultPayload: boolean;

    /**
     *
     * @param query
     * @param r2s_topic
     */
    constructor(query: string, r2s_topic: string) {
        this.query = query;
        this.r2s_topic = r2s_topic;
        this.queryId = hash_string_md5(query);
        this.rspql_parser = new RSPQLParser();
        this.parsedQuery = this.rspql_parser.parse(this.query);
        this.aggregationType = this.detectAggregationType();
        this.sourceTopic = this.extractSourceTopic();
        this.range = Number(this.parsedQuery?.s2r?.[0]?.width);
        this.step = Number(this.parsedQuery?.s2r?.[0]?.slide);
        this.debugStructuredReusableResults = isApproximationDebugEnabled();
        this.compactReusableResultPayload = useCompactReusableResultPayload();
        this.rsp_engine = new RSPEngine(query);
        profileCount("rsp_engines_created");
        this.rstream_emitter = this.rsp_engine.register();
        this.http_server_location = "http://localhost:8080/";
        this.registerCleanupHook();
        this.registerToQueryRegistry();
        this.subscribeRStream();


    }

    /**
     *
     */
    public async registerToQueryRegistry() {
        console.log(`Registering query: ${this.query} to the query registry.`);
        const response = await RSPAgent.registerQueryDefinition(
            this.query,
            this.r2s_topic,
            {},
            this.http_server_location,
        );
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
            this.mqttClients.push(rsp_client);
            profileCount("mqtt_clients_created");
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
                    profileCount("mqtt_messages_received");
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
        this.mqttClients.push(rstream_publisher);
        profileCount("mqtt_clients_created");

        rstream_publisher.on("connect", () => {
            console.log("Connected to MQTT broker for publishing");

            this.rstream_emitter.on("RStream", async (object: any) => {
                const callbackStartedAt = startStageTimer();
                profileCount("rsp_agent_rstream_callbacks");
                if (!object || !object.bindings) {
                    endStageTimer("rsp_agent.rstream_callback_total_ms", callbackStartedAt);
                    console.log(`No bindings found in the RStream object.`);
                    return;
                }

                const bindingExtractionStartedAt = startStageTimer();
                const bindings = this.normalizeBindings(object);
                endStageTimer(
                    "rsp_agent.binding_extraction_ms",
                    bindingExtractionStartedAt,
                );
                profileCount("rsp_agent_binding_rows", Object.keys(bindings).length);

                const payload = profileStageSync("rsp_agent.reusable_payload_build_ms", () =>
                    this.buildReusableResultPayload(object, bindings),
                );
                profileCount("rsp_agent_output_construction_calls");
                if (!payload) {
                    endStageTimer("rsp_agent.rstream_callback_total_ms", callbackStartedAt);
                    console.error("Failed to build structured reusable_result payload.");
                    return;
                }

                const data = profileStageSync(
                    "rsp_agent.reusable_json_stringify_ms",
                    () => JSON.stringify(payload),
                );
                profileCount("rsp_agent_json_serializations");
                profileCount("rsp_agent_reusable_payload_bytes", Buffer.byteLength(data, "utf8"));
                if (this.debugStructuredReusableResults) {
                    const debugLogStartedAt = startStageTimer();
                    console.log("Structured reusable_result payload:", data);
                    endStageTimer("rsp_agent.debug_log_write_ms", debugLogStartedAt);
                }
                profileCount("mqtt_messages_published");
                const publishStartedAt = startStageTimer();
                rstream_publisher.publish(this.r2s_topic, data, (err: any) => {
                    endStageTimer("rsp_agent.reusable_mqtt_publish_total_ms", publishStartedAt);
                    endStageTimer("rsp_agent.rstream_callback_total_ms", callbackStartedAt);
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

    private normalizeBindings(rstreamObject: any): Record<string, string> {
        const normalized: Record<string, string> = {};
        const bindings = rstreamObject?.bindings;
        if (!bindings) {
            return normalized;
        }

        if (typeof bindings[Symbol.iterator] === "function") {
            for (const binding of bindings) {
                if (Array.isArray(binding) && binding.length >= 2) {
                    const varName = String(binding[0]?.value ?? binding[0] ?? "").replace(/^\?/, "");
                    const varValue = String(binding[1]?.value ?? binding[1] ?? "");
                    if (varName) {
                        normalized[varName] = varValue;
                    }
                }
            }
        }

        if (Object.keys(normalized).length > 0) {
            return normalized;
        }

        if (typeof bindings.values === "function" && typeof bindings.keys === "function") {
            const keys = Array.from(bindings.keys());
            const values = Array.from(bindings.values());
            for (let index = 0; index < Math.min(keys.length, values.length); index += 1) {
                const key = String((keys[index] as any)?.value ?? keys[index] ?? "").replace(/^\?/, "");
                const value = String((values[index] as any)?.value ?? values[index] ?? "");
                if (key) {
                    normalized[key] = value;
                }
            }
        }

        return normalized;
    }

    private extractNumericBinding(
        bindings: Record<string, string>,
        prefixes: string[],
    ): number | null {
        for (const prefix of prefixes) {
            for (const [key, value] of Object.entries(bindings)) {
                if (!key.toLowerCase().startsWith(prefix.toLowerCase())) {
                    continue;
                }
                const numeric = Number(value);
                if (Number.isFinite(numeric)) {
                    return numeric;
                }
            }
        }
        return null;
    }

    private detectAggregationType(): string | null {
        const match = this.query.match(/SELECT\s*\((\w+)\(/i);
        return match?.[1]?.toUpperCase() ?? null;
    }

    private extractWindowBounds(rstreamObject: any): { start: number; end: number } | null {
        const candidates: Array<{ start?: number; end?: number }> = [
            { start: rstreamObject?.window?.open, end: rstreamObject?.window?.close },
            { start: rstreamObject?.windowOpen, end: rstreamObject?.windowClose },
            { start: rstreamObject?.open, end: rstreamObject?.close },
            { start: rstreamObject?.start, end: rstreamObject?.end },
            { start: rstreamObject?.timestamp_from, end: rstreamObject?.timestamp_to },
        ];

        for (const candidate of candidates) {
            if (Number.isFinite(candidate.start) && Number.isFinite(candidate.end)) {
                return { start: Number(candidate.start), end: Number(candidate.end) };
            }
        }

        const anchor = Number(rstreamObject?.timestamp ?? rstreamObject?.tick);
        if (!Number.isFinite(anchor) || !Number.isFinite(this.range) || !Number.isFinite(this.step) || (this.step as number) <= 0) {
            return null;
        }
        const end = Math.floor(anchor / (this.step as number)) * (this.step as number);
        return { start: end - (this.range as number), end };
    }

    private extractSourceTopic(): string | null {
        const streams: any[] = [...(this.parsedQuery?.s2r ?? [])];
        const streamName = streams[0]?.stream_name;
        if (!streamName) {
            return null;
        }
        return new URL(streamName).pathname.replace(/^\/+/, "");
    }

    private buildReusableResultPayload(
        rstreamObject: any,
        bindings: Record<string, string>,
    ): StructuredReusableResultPayload | null {
        const windowBounds = this.extractWindowBounds(rstreamObject);
        const resultEmittedAt = Date.now();
        const value =
            this.extractNumericBinding(bindings, ["agg", "result", "avg", "sum", "min", "max"]) ??
            null;

        if (!windowBounds || value === null) {
            return null;
        }

        const logicalTriggerTime =
            Number.isFinite(this.range) ? windowBounds.end - (this.range as number) / 2 : null;

        const basePayload: StructuredReusableResultPayload = {
            message_format: "structured_reusable_result",
            source_query_id: this.queryId,
            aggregationType: this.aggregationType,
            value,
            count: this.extractNumericBinding(bindings, ["count"]),
            sum: this.extractNumericBinding(bindings, ["sum"]),
            avg: this.extractNumericBinding(bindings, ["avg", "agg", "result"]),
            min: this.extractNumericBinding(bindings, ["min"]),
            max: this.extractNumericBinding(bindings, ["max"]),
            window_start: windowBounds.start,
            window_end: windowBounds.end,
            window_data_close_time: windowBounds.end,
        };

        if (this.compactReusableResultPayload) {
            if (this.sourceTopic) {
                basePayload.source_topic = this.sourceTopic;
            }
            basePayload.logical_trigger_time = logicalTriggerTime;
            return basePayload;
        }

        basePayload.source_topic = this.sourceTopic;
        basePayload.reusable_result_topic = this.r2s_topic;
        basePayload.resultValue = value;
        basePayload.raw_bindings = bindings;
        basePayload.logical_trigger_time = logicalTriggerTime;
        basePayload.timestamp_from = windowBounds.start;
        basePayload.timestamp_to = windowBounds.end;
        basePayload.result_emitted_at = resultEmittedAt;
        basePayload.window = {
            start: windowBounds.start,
            end: windowBounds.end,
            range: Number.isFinite(this.range) ? this.range : null,
            step: Number.isFinite(this.step) ? this.step : null,
            semantics: "[start,end)",
            windowSemantics: process.env.RSP_WINDOW_SEMANTICS || "trailing",
            logicalTriggerTime: logicalTriggerTime,
            windowDataCloseTime: windowBounds.end,
            resultEmittedAt,
            metadataSource: "reconstructed",
        };

        return basePayload;
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

    private registerCleanupHook(): void {
        if (this.cleanupRegistered) {
            return;
        }

        this.cleanupRegistered = true;
        process.once("exit", () => {
            this.cleanup();
        });
    }

    public cleanup(): void {
        profileSync("cleanup_time_ms", () => {
            for (const client of this.mqttClients.splice(0)) {
                try {
                    client.end(true);
                } catch (error) {
                    console.error("Failed to clean up RSPAgent MQTT client:", error);
                }
            }
        });
        writeProfileArtifact();
        writeStageProfileArtifact();
    }

    public static async registerQueryDefinition(
        query: string,
        r2s_topic: string,
        metadata: Record<string, unknown> = {},
        httpServerLocation = "http://localhost:8080/",
    ) {
        const register_location = `${httpServerLocation}register`;
        const request = await fetch(register_location, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                rspql_query: query,
                r2s_topic,
                data_topic: r2s_topic,
                id: hash_string_md5(query),
                ...metadata,
            })
        });
        if (!request.ok) {
            throw new Error(`Failed to register query: ${query}. Status: ${request.status}`);
        }
        const response = await request.json();
        if (response.error) {
            throw new Error(`Error registering query: ${response.error}`);
        }
        return response;
    }


}
