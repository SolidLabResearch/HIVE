import { EventEmitter } from 'events';
import { RDFStream, RSPEngine, RSPQLParser } from 'rsp-js';
import { v4 as uuidv4 } from 'uuid';
import { turtleStringToStore } from '../util/Util';
import { PartialChunkResult } from '../util/chunkTypes';
import { recordPublishedMqttMessage } from '../util/mqttTraffic';
import { getTimestampDomainMax, getTimestampDomainMin, useCleanMqttSessionsForBenchmark } from '../util/runtimeConfig';
import { profileCount, profileSync, writeProfileArtifact } from "../util/profiling";
import { resourceTraceSnapshot } from "../util/resourceTrace";
import { detectCompatibleChunkReuse } from '../util/chunkStateReuse';
const mqtt = require('mqtt');
const { DataFactory } = require('n3');

/**
 *
 */
export class RSPQueryProcess {
    public query: string;
    public rstream_topic: string;
    public rstream_emitter: EventEmitter;
    public rsp_engine: RSPEngine;
    public rspql_parser: RSPQLParser;
    private queryId: string;
    private subqueryId: string;
    private debugChunksEnabled: boolean;
    private benchmarkEventTimeAnchor: number | null;
    private firstObservedWindowStart: number | null;
    private timestampDomainMin: number | null;
    private timestampDomainMax: number | null;
    private rejectedContaminatedTimestampCount: number;
    private mqttClients: any[] = [];
    private cleanupRegistered: boolean = false;

    /**
     *
     * @param query
     * @param rstream_topic
     */
    constructor(query: string, rstream_topic: string, queryId: string = "default-query", subqueryId: string = "default-subquery") {
        this.query = query;
        this.rstream_topic = rstream_topic;
        this.rsp_engine = new RSPEngine(query);
        profileCount("rsp_engines_created");
        this.rstream_emitter = this.rsp_engine.register();
        this.rspql_parser = new RSPQLParser();
        this.queryId = queryId;
        this.subqueryId = subqueryId;
        this.debugChunksEnabled = process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1";
        const anchorRaw = process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
        const parsedAnchor = Number.parseInt(anchorRaw || "", 10);
        this.benchmarkEventTimeAnchor = Number.isFinite(parsedAnchor) ? parsedAnchor : null;
        this.firstObservedWindowStart = null;
        this.timestampDomainMin = getTimestampDomainMin();
        this.timestampDomainMax = getTimestampDomainMax();
        this.rejectedContaminatedTimestampCount = 0;
        this.registerCleanupHook();
        this.subscribeToResultStream();
        resourceTraceSnapshot("startup", "rsp query process constructed", {
            queryId: this.queryId,
            subqueryId: this.subqueryId,
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
        resourceTraceSnapshot("before_cleanup", "rsp query process cleanup", {
            queryId: this.queryId,
            subqueryId: this.subqueryId,
        });
        profileSync("cleanup_time_ms", () => {
            for (const client of this.mqttClients.splice(0)) {
                try {
                    client.end(true);
                } catch (error) {
                    console.error("Failed to clean up MQTT client:", error);
                }
            }
        });
        writeProfileArtifact();
        resourceTraceSnapshot("after_cleanup", "rsp query process cleanup complete", {
            queryId: this.queryId,
            subqueryId: this.subqueryId,
        });
    }

    /**
     *
     */
    public async stream_process() {
        console.log(`Processing query in RSPQueryProcess: ${this.query}`);
        if (!this.query || this.query.trim() === "") {
            console.error(`Query is empty or undefined.`);
            return;
        }
        const parsed_query = this.rspql_parser.parse(this.query);
        if (parsed_query) {
            const streams: any[] = [...parsed_query.s2r];
            console.log(`Parsed query successfully. Found ${streams.length} streams.`);
            for (const stream of streams) {
                const stream_name = stream.stream_name;
                const stream_url = new URL(stream_name);
                const mqtt_broker: string = `${stream_url.protocol}//${stream_url.hostname}:${stream_url.port}/`;
                const rsp_client = mqtt.connect(mqtt_broker, {
                    clean: useCleanMqttSessionsForBenchmark(),
                });
                this.mqttClients.push(rsp_client);
                profileCount("mqtt_clients_created");
                const rsp_stream_object = this.rsp_engine.getStream(stream_name);
                const topic = stream_url.pathname.slice(1);
                rsp_client.on("connect", () => {
                    resourceTraceSnapshot("after_mqtt_subscription_connect", "rsp stream subscriber connected", {
                        queryId: this.queryId,
                        subqueryId: this.subqueryId,
                        topic,
                    });
                    rsp_client.subscribe(topic, (err: any) => {
                        if (err) {
                            console.error(`Failed to subscribe to stream ${stream_name}:`, err);
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
                        profileCount("mqtt_messages_received");
                        const latest_event_store = await turtleStringToStore(message_string);
                        const timestamp = latest_event_store.getQuads(null, DataFactory.namedNode('https://saref.etsi.org/core/hasTimestamp'), null, null)[0]?.object.value;
                        if (!timestamp) {
                            console.error(`No timestamp found in the message for stream ${stream_name}`);
                            return;
                        }
                        const timestamp_epoch = Date.parse(timestamp);
                        if (this.isContaminatedTimestamp(timestamp_epoch, stream_name)) {
                            return;
                        }
                        if (rsp_stream_object) {
                            await this.add_event_store_to_rsp_engine(latest_event_store, [rsp_stream_object], timestamp_epoch);
                        }
                        else {
                            console.error(`Stream object not found for stream ${stream_name}`);
                            return;
                        }
                    } catch (error) {
                        console.error(`Error processing message for stream ${stream_name}:`, error);
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
        }
        else {
            console.log(`Failed to parse query: ${this.query}`);
        }
    }

    private isContaminatedTimestamp(timestamp: number, streamName: string): boolean {
        if (!Number.isFinite(timestamp)) {
            return true;
        }

        if (this.timestampDomainMin !== null && timestamp < this.timestampDomainMin) {
            this.rejectedContaminatedTimestampCount += 1;
            console.log(
                `[CONTAMINATION] stream=${streamName} timestamp=${timestamp} timestampDomainMin=${this.timestampDomainMin} timestampDomainMax=${this.timestampDomainMax} rejectedCount=${this.rejectedContaminatedTimestampCount}`,
            );
            return true;
        }

        if (this.timestampDomainMax !== null && timestamp > this.timestampDomainMax) {
            this.rejectedContaminatedTimestampCount += 1;
            console.log(
                `[CONTAMINATION] stream=${streamName} timestamp=${timestamp} timestampDomainMin=${this.timestampDomainMin} timestampDomainMax=${this.timestampDomainMax} rejectedCount=${this.rejectedContaminatedTimestampCount}`,
            );
            return true;
        }

        return false;
    }


    /**
     *
     */
    public async subscribeToResultStream() {
        console.log(`Subscribing to result stream: ${this.rstream_topic}`);
        if (!this.rstream_topic || this.rstream_topic.trim() === "") {
            console.error(`RStream topic is empty or undefined.`);
            return;
        }

        const mqtt_broker = "mqtt://localhost:1883";
        const rstream_publisher = mqtt.connect(mqtt_broker);
        this.mqttClients.push(rstream_publisher);
        profileCount("mqtt_clients_created");
        resourceTraceSnapshot("after_result_publisher_created", "rsp result publisher created", {
            queryId: this.queryId,
            subqueryId: this.subqueryId,
        });

        this.rstream_emitter.on("RStream", async (object: any) => {
            if (!object || !object.bindings || object.bindings.length === 0) {
                return;
            }

            // Merge all bindings into one object.
            // rsp-js returns bindings as an iterable of entries [Variable, Term] (effectively a Map).
            // We need to flatten this into a { varName: varValue } object.
            let mergedBinding: {[key: string]: any} = {};
            
            // Check if object.bindings is iterable
            if (object.bindings && typeof object.bindings[Symbol.iterator] === 'function') {
                for (const binding of object.bindings) {
                     // binding is expected to be [Variable, Term]
                     if (Array.isArray(binding) && binding.length >= 2) {
                         const varName = binding[0].value;
                         const varValue = binding[1].value;
                         mergedBinding[varName] = varValue;
                     }
                }
            }

            const partialChunkResult = this.generate_partial_chunk_result(mergedBinding, object);

            if (partialChunkResult) {
                if (this.debugChunksEnabled) {
                    console.log(
                        `[DEBUG_CHUNKS] subquery subqueryId=${partialChunkResult.subqueryId} windowName=${partialChunkResult.window.windowName} windowStart=${partialChunkResult.window.start} windowEnd=${partialChunkResult.window.end} chunkId=${partialChunkResult.chunkId} value=${partialChunkResult.value} count=${partialChunkResult.count}`,
                    );
                }
                const aggregation_object_string = JSON.stringify(partialChunkResult);
                rstream_publisher.publish(this.rstream_topic, aggregation_object_string, (err: any) => {
                    if (err) {
                        console.error(`Error publishing aggregation event: ${err}`);
                    } else {
                        profileCount("mqtt_messages_published");
                        profileCount("chunk_state_messages_published");
                        recordPublishedMqttMessage({
                            topic: this.rstream_topic,
                            payload: aggregation_object_string,
                            messageType: "chunk_result",
                        });
                    }
                });
            }
        });
    }


    /**
     *
     * @param bindings Map of variable names to values
     * @param timestamp
     */
    private generate_partial_chunk_result(bindings: any, rstreamObject: any): PartialChunkResult | null {
        const parsedQuery = this.rspql_parser.parse(this.query);
        if (!parsedQuery || !parsedQuery.s2r || parsedQuery.s2r.length === 0) {
            return null;
        }
        const s2rWindow = parsedQuery.s2r[0];
        const windowName = s2rWindow.window_name || s2rWindow.stream_name || "window";
        const range = s2rWindow.width;
        const step = s2rWindow.slide;
        const windowBounds = this.extractWindowBounds(rstreamObject, range, step);
        if (!windowBounds) {
            if (this.debugChunksEnabled) {
                console.log(
                    `[DEBUG_CHUNKS] subquery partial result dropped: unable to extract window bounds from keys=${Object.keys(rstreamObject || {}).join(",") || "none"}`,
                );
            }
            return null;
        }

        const aggregateFunctionMatch = this.query.match(/\b(AVG|SUM|COUNT|MIN|MAX)\s*\(/i);
        const aggregateFunction = aggregateFunctionMatch?.[1]?.toUpperCase();
        const value = this.extractNumericBinding(bindings, ["agg", "avg", "sum", "min", "max"]);
        const count = this.extractNumericBinding(bindings, ["count"]);
        const sum = this.extractNumericBinding(bindings, ["sum"]);
        const avg = this.extractNumericBinding(bindings, ["avg"]);
        const min = this.extractNumericBinding(bindings, ["min"]);
        const max = this.extractNumericBinding(bindings, ["max"]);
        const event_timestamp = new Date().getTime();
        const rdfPayload = this.generate_aggregation_event(bindings, event_timestamp);
        const chunkGroupId = `${this.queryId}:${windowBounds.start}:${windowBounds.end}`;
        const chunkId = `${chunkGroupId}:${this.subqueryId}`;
        const compatibleReuse = detectCompatibleChunkReuse(this.query);
        const directLogicalTriggerTime = Number(rstreamObject?.logical_trigger_time);
        const directWindowDataCloseTime = Number(rstreamObject?.window_data_close_time);
        const directResultEmittedAt = Number(rstreamObject?.result_emitted_at);
        const directLatencyFromLogicalTriggerMs = Number(rstreamObject?.latency_from_logical_trigger_ms);
        const directLatencyFromWindowCloseMs = Number(rstreamObject?.latency_from_window_close_ms);
        const windowSemantics = String(
            rstreamObject?.window_semantics ||
            process.env.RSP_WINDOW_SEMANTICS ||
            "trailing",
        ).toLowerCase();
        const metadataSource = Number.isFinite(directLogicalTriggerTime) &&
            Number.isFinite(directWindowDataCloseTime) &&
            Number.isFinite(directResultEmittedAt)
            ? "direct"
            : "reconstructed";
        const logicalTriggerTime = Number.isFinite(directLogicalTriggerTime)
            ? directLogicalTriggerTime
            : (windowBounds.end - (range / 2));
        const windowDataCloseTime = Number.isFinite(directWindowDataCloseTime)
            ? directWindowDataCloseTime
            : windowBounds.end;
        const resultEmittedAt = Number.isFinite(directResultEmittedAt)
            ? directResultEmittedAt
            : windowDataCloseTime;
        const latencyFromLogicalTriggerMs = Number.isFinite(directLatencyFromLogicalTriggerMs)
            ? directLatencyFromLogicalTriggerMs
            : resultEmittedAt - logicalTriggerTime;
        const latencyFromWindowCloseMs = Number.isFinite(directLatencyFromWindowCloseMs)
            ? directLatencyFromWindowCloseMs
            : resultEmittedAt - windowDataCloseTime;

        return {
            queryId: this.queryId,
            subqueryId: this.subqueryId,
            window: {
                windowName,
                start: windowBounds.start,
                end: windowBounds.end,
                range,
                step,
                semantics: "[start,end)",
                windowSemantics,
                logicalTriggerTime,
                windowDataCloseTime,
                resultEmittedAt,
                latencyFromLogicalTriggerMs,
                latencyFromWindowCloseMs,
                metadataSource,
            },
            chunkId,
            reuseClassKey: compatibleReuse?.reuseClassKey,
            sourceStreamId: compatibleReuse?.sourceStreamId,
            sourceTopic: compatibleReuse?.sourceTopic,
            aggregateFunction,
            value,
            count,
            sum,
            avg,
            min,
            max,
            state: compatibleReuse
                ? {
                    ...(compatibleReuse.aggregationStateSignature.includes("count") ? { count } : {}),
                    ...(compatibleReuse.aggregationStateSignature.includes("sum") ? { sum } : {}),
                    ...(compatibleReuse.aggregationStateSignature.includes("min") ? { min } : {}),
                    ...(compatibleReuse.aggregationStateSignature.includes("max") ? { max } : {}),
                  }
                : {
                    count,
                    sum,
                    min,
                    max,
                  },
            rdfPayload,
        };
    }

    private extractNumericBinding(bindings: any, prefixes: string[]): number | undefined {
        for (const prefix of prefixes) {
            for (const [key, value] of Object.entries(bindings)) {
                const keyLc = key.toLowerCase();
                if (!keyLc.startsWith(prefix)) continue;
                const numeric = Number(value);
                if (Number.isFinite(numeric)) {
                    return numeric;
                }
            }
        }
        return undefined;
    }

    private extractWindowBounds(rstreamObject: any, range: number, step: number): { start: number; end: number } | null {
        const candidates: Array<{ start?: number; end?: number }> = [
            { start: rstreamObject?.window?.open, end: rstreamObject?.window?.close },
            { start: rstreamObject?.windowOpen, end: rstreamObject?.windowClose },
            { start: rstreamObject?.open, end: rstreamObject?.close },
            { start: rstreamObject?.start, end: rstreamObject?.end },
            { start: rstreamObject?.timestamp_from, end: rstreamObject?.timestamp_to },
        ];
        for (const candidate of candidates) {
            if (Number.isFinite(candidate.start) && Number.isFinite(candidate.end)) {
                const explicit = { start: candidate.start as number, end: candidate.end as number };
                return this.remapWindowBoundsToBenchmarkAnchor(explicit, range, step);
            }
        }

        const anchorCandidate = rstreamObject?.timestamp ?? rstreamObject?.tick;
        const anchor = Number(anchorCandidate);
        if (!Number.isFinite(anchor) || !Number.isFinite(step) || step <= 0 || !Number.isFinite(range) || range <= 0) {
            return null;
        }
        const end = Math.floor(anchor / step) * step;
        return this.remapWindowBoundsToBenchmarkAnchor(
            { start: end - range, end },
            range,
            step,
        );
    }

    private remapWindowBoundsToBenchmarkAnchor(
        bounds: { start: number; end: number },
        range: number,
        step: number,
    ): { start: number; end: number } {
        if (
            this.benchmarkEventTimeAnchor === null ||
            !Number.isFinite(step) ||
            step <= 0 ||
            !Number.isFinite(range) ||
            range <= 0
        ) {
            return bounds;
        }

        if (this.firstObservedWindowStart === null) {
            this.firstObservedWindowStart = bounds.start;
        }

        const windowIndex = Math.round((bounds.start - this.firstObservedWindowStart) / step);
        const remappedStart = this.benchmarkEventTimeAnchor + (windowIndex * step);
        return {
            start: remappedStart,
            end: remappedStart + range,
        };
    }

    public generate_aggregation_event(bindings: any, timestamp: number) {
        const uuid_random = uuidv4();
        let triples = "";
        const queryAggregation = this.query.match(/\b(AVG|SUM|COUNT|MIN|MAX)\s*\(/i)?.[1]?.toUpperCase();

        // Iterate over keys in the binding object (e.g. avgWearableX, countWearableX)
        // Note: rsp-js bindings might be a Map or an Object. Adjusting for Object behavior based on JSON.stringify output.
        // Assuming binding is { varName: value, ... } based on previous logs.
        
        for (const [key, value] of Object.entries(bindings)) {
             console.log(`DEBUG: Generating triple for key: ${key}, value: ${value}`);
             let predicate = "";
             if (key.startsWith("avg") || key.startsWith("agg") || key.startsWith("sum") || key.startsWith("min") || key.startsWith("max")) {
                 predicate = "<https://saref.etsi.org/core/hasValue>";
             } else if (key.startsWith("count")) {
                 predicate = "<https://saref.etsi.org/core/hasCount>";
             } else {
                 continue; // specific interest only
             }

             const isCountBinding =
                key.startsWith("count") ||
                (queryAggregation === "COUNT" && (key.startsWith("avg") || key.startsWith("agg")));
             const datatype = isCountBinding
                ? "http://www.w3.org/2001/XMLSchema#integer"
                : "http://www.w3.org/2001/XMLSchema#double";

             triples += `<https://rsp.js/aggregation_event/${uuid_random}> ${predicate} "${value}"^^<${datatype}> .\n`;
        }
        
        return triples.trim();
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
