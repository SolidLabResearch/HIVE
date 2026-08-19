import fs from "fs";
import { RSPEngine, RSPQLParser } from "rsp-js";
import { turtleStringToStore } from "../util/Util";
import {
    AggregationFunction,
    buildBenchmarkTopicName,
    buildBenchmarkResultPayload,
    buildOutputSelectClause,
    buildSubQuerySelectClause,
    getConfiguredAggregation,
    getBenchmarkTargetWindowCount,
    getOutputWindowRange,
    getOutputWindowStep,
    getResultTopic,
    getSessionId,
    getSubWindowRange,
    getSubWindowStep,
} from "../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
const N3 = require("n3");
const mqtt = require("mqtt");
const { DataFactory } = N3;

// ─── Query definitions ────────────────────────────────────────────────────────
// These are the same queries used by Approximation and Chunked approaches so
// that all four approaches are evaluated on identical workloads.

function buildSubQuery1(
    aggregationFunction: AggregationFunction,
    subWindowRange: number,
    subWindowStep: number,
) {
const wearableTopic = buildBenchmarkTopicName("wearableX");
return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <subquery1_output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "WearableX")}
FROM NAMED WINDOW <mqtt://localhost:1883/${wearableTopic}> ON STREAM <mqtt://localhost:1883/${wearableTopic}> [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/${wearableTopic}> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:hasTimestamp ?ts .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
`;
}

function buildSubQuery2(
    aggregationFunction: AggregationFunction,
    subWindowRange: number,
    subWindowStep: number,
) {
const smartphoneTopic = buildBenchmarkTopicName("smartphoneX");
return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <subquery2_output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "SmartphoneX")}
FROM NAMED WINDOW <mqtt://localhost:1883/${smartphoneTopic}> ON STREAM <mqtt://localhost:1883/${smartphoneTopic}> [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/${smartphoneTopic}> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:hasTimestamp ?ts .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
`;
}

function buildSuperQuery(
    aggregationFunction: AggregationFunction,
    outputWindowRange: number,
    outputWindowStep: number,
) {
const wearableTopic = buildBenchmarkTopicName("wearableX");
const smartphoneTopic = buildBenchmarkTopicName("smartphoneX");
return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <sensor_averages> AS
SELECT ${buildOutputSelectClause(aggregationFunction)}
FROM NAMED WINDOW <mqtt://localhost:1883/${wearableTopic}> ON STREAM <mqtt://localhost:1883/${wearableTopic}> [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
FROM NAMED WINDOW <mqtt://localhost:1883/${smartphoneTopic}> ON STREAM <mqtt://localhost:1883/${smartphoneTopic}> [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/${wearableTopic}> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:hasTimestamp ?ts .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/${smartphoneTopic}> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:hasTimestamp ?ts .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
`;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Initialise a CSV log file and return a log() function that writes both to the
 * file and to stdout (in the same "LOG: <ts> - <msg>" format used by the
 * fetching approach so that extract-results-from-logs.js can parse it).
 */
function initLog(logFilePath: string): (message: string) => void {
    const writeHeader = !fs.existsSync(logFilePath);
    const stream = fs.createWriteStream(logFilePath, { flags: "a" });
    if (writeHeader) {
        stream.write("timestamp,message\n");
    }
    return (message: string) => {
        const ts = Date.now();
        stream.write(`${ts},"${message}"\n`);
        console.log(`LOG: ${ts} - ${message}`);
    };
}

/**
 * Initialise the per-window latency CSV (same column format as the other operators).
 * Returns a function that appends one row per window.
 */
function initLatencyLog(filePath: string): (
    row: Record<string, string | number | null>,
) => void {
    const writeHeader = !fs.existsSync(filePath);
    const stream = fs.createWriteStream(filePath, { flags: "a" });
    if (writeHeader) {
        stream.write(
            "window_number,query_registered_at,first_data_received_at," +
            "expected_window_close,last_obs_received_at,result_emitted_at," +
            "latency_from_query_reg_ms,latency_from_data_start_ms," +
            "latency_from_last_obs_ms,window_semantics,logical_trigger_time," +
            "window_start,window_end,window_data_close_time," +
            "latency_from_logical_trigger_ms,latency_from_window_close_ms," +
            "metadata_source,result_value,event_count,sum_value,avg_value," +
            "first_event_timestamp,last_event_timestamp\n",
        );
    }

    const columns = [
        "window_number",
        "query_registered_at",
        "first_data_received_at",
        "expected_window_close",
        "last_obs_received_at",
        "result_emitted_at",
        "latency_from_query_reg_ms",
        "latency_from_data_start_ms",
        "latency_from_last_obs_ms",
        "window_semantics",
        "logical_trigger_time",
        "window_start",
        "window_end",
        "window_data_close_time",
        "latency_from_logical_trigger_ms",
        "latency_from_window_close_ms",
        "metadata_source",
        "result_value",
        "event_count",
        "sum_value",
        "avg_value",
        "first_event_timestamp",
        "last_event_timestamp",
    ];

    return (row) => {
        stream.write(
            `${columns.map((column) => row[column] ?? "").join(",")}\n`,
        );
    };
}

type ParsedBindingFields = {
    resultValue: number | null;
    eventCount: number | null;
    sumValue: number | null;
    avgValue: number | null;
    firstEventTimestamp: string | null;
    lastEventTimestamp: string | null;
};

type WindowPartialAggregate = {
    logicalTriggerTime: number | null;
    windowStart: number | null;
    windowEnd: number | null;
    windowDataCloseTime: number | null;
    windowSemantics: string;
    metadataSource: string;
    expectedWindowClose: number | null;
    firstEventTimestamp: string | null;
    lastEventTimestamp: string | null;
    streamKeys: Set<string>;
    partialCount: number;
    totalEventCount: number;
    totalSumValue: number;
    avgValues: number[];
    resultEmittedAt: number | null;
    lastObservedAt: number | null;
    latencyFromLogicalTriggerMs: number | null;
    latencyFromWindowCloseMs: number | null;
};

export function maybeFinalizeNaiveBenchmarkTargetWindowCount(params: {
    finalizedWindowNumbers: Set<number>;
    benchmarkTargetWindowCount: number | null;
    benchmarkWindowSummaryPath: string;
    scheduleShutdown: () => void;
}): boolean {
    const {
        finalizedWindowNumbers,
        benchmarkTargetWindowCount,
        benchmarkWindowSummaryPath,
        scheduleShutdown,
    } = params;

    if (
        !Number.isFinite(benchmarkTargetWindowCount) ||
        finalizedWindowNumbers.size < (benchmarkTargetWindowCount as number)
    ) {
        return false;
    }

    fs.writeFileSync(
        benchmarkWindowSummaryPath,
        `${JSON.stringify({
            targetWindowCount: benchmarkTargetWindowCount,
            emittedFinalWindowCount: finalizedWindowNumbers.size,
            finalWindowNumbers: [...finalizedWindowNumbers].sort((left, right) => left - right),
            stoppedAfterTargetWindows: true,
            stopReason: "target_window_count_reached",
            approach: "naive_distributed",
        }, null, 2)}\n`,
    );
    scheduleShutdown();
    return true;
}

function parseBindingFields(binding: any): ParsedBindingFields {
    const entry = (key: string) => {
        if (binding instanceof Map) {
            return binding.get(`?${key}`)?.value ?? binding.get(key)?.value ?? null;
        }
        if (binding?.entries) {
            if (typeof binding.entries.get === "function") {
                return binding.entries.get(key)?.value ?? null;
            }
            return binding.entries[key]?.value ?? null;
        }
        return binding?.[key]?.value ?? null;
    };

    const toNumber = (value: unknown): number | null => {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    };

    const toStringOrNull = (value: unknown): string | null => {
        if (value === undefined || value === null) {
            return null;
        }
        const stringValue = String(value).trim();
        return stringValue === "" ? null : stringValue;
    };

    return {
        resultValue: toNumber(entry("resultValue") ?? entry("avgValue")),
        eventCount: toNumber(entry("eventCount")),
        sumValue: toNumber(entry("sumValue")),
        avgValue: toNumber(entry("avgValue")),
        firstEventTimestamp: toStringOrNull(entry("firstEventTimestamp")),
        lastEventTimestamp: toStringOrNull(entry("lastEventTimestamp")),
    };
}

/**
 * Connect an RSPEngine to all MQTT streams declared in `query`.
 * Each stream gets its own MQTT client so that concurrent subscriptions do not
 * interfere. Data is fed directly into the engine with no result reuse.
 * `onData` is called with the wall-clock time of every message received.
 */
async function subscribeEngineToMQTT(
    engine: RSPEngine,
    query: string,
    log: (msg: string) => void,
    onData?: (wallClockTime: number) => void,
): Promise<void> {
    const parser = new RSPQLParser();
    const parsed = parser.parse(query);
    const streams: any[] = [...parsed.s2r];

    for (const s of streams) {
        const streamName: string = s.stream_name;
        const url = new URL(streamName);
        const broker = `${url.protocol}//${url.hostname}:${url.port}/`;
        const topic = url.pathname.slice(1);
        const rdfStream = engine.getStream(streamName);

        if (!rdfStream) {
            log(`Warning: no stream object registered for ${streamName}`);
            continue;
        }

        const clientId = "naive-sub-" + Math.random().toString(16).substr(2, 8);
        const client = mqtt.connect(broker, { clean: false, clientId });

        client.on("connect", () => {
            log(`Connected to ${broker}, subscribing to ${topic}`);
            client.subscribe(topic, { qos: 1 }, (err: any) => {
                if (err) {
                    log(`Subscribe error on ${topic}: ${err}`);
                } else {
                    log(`Subscribed to ${topic}`);
                }
            });
        });

        client.on("message", async (_topic: string, message: Buffer) => {
            try {
                const receivedAt = Date.now();
                const messageStr = message.toString();
                const store = await turtleStringToStore(messageStr);

                const tsQuads = store.getQuads(
                    null,
                    DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"),
                    null,
                    null,
                );
                if (tsQuads.length === 0) return;
                const timestamp = Date.parse(tsQuads[0].object.value);

                const quads = store.getQuads(null, null, null, null);
                const graph = DataFactory.namedNode(rdfStream.name);
                for (const q of quads) {
                    rdfStream.add(
                        DataFactory.quad(q.subject, q.predicate, q.object, graph),
                        timestamp,
                    );
                }

                if (onData) onData(receivedAt);
            } catch (e) {
                log(`Error processing MQTT message on ${topic}: ${e}`);
            }
        });

        client.on("error", (err: any) => {
            log(`MQTT client error on ${broker}: ${err}`);
        });
    }
}

// ─── Main orchestrator ────────────────────────────────────────────────────────

/**
 * Naive Distributed Execution baseline.
 *
 * Runs three RSP engines simultaneously against raw MQTT streams with no
 * result reuse between them:
 *   1. SubQuery 1  – wearableX  (RANGE 60 s / STEP 30 s)
 *   2. SubQuery 2  – smartphoneX (RANGE 60 s / STEP 30 s)
 *   3. Super-query – wearableX + smartphoneX (RANGE 120 s / STEP 60 s)
 *
 * This models the scenario where pre-existing subqueries cannot be stopped
 * (they are consumed by other processes), so a new super-query is registered
 * independently alongside them — bearing the full resource cost of all three
 * without any optimisation from the Streaming Query Hive architecture.
 */
async function NaiveDistributedApproachOrchestrator(): Promise<void> {
    const log = initLog("naive_distributed_approach_log.csv");
    const logLatency = initLatencyLog("naive_distributed_latency_log.csv");
    const aggregationFunction = getConfiguredAggregation();
    const subWindowRange = getSubWindowRange();
    const subWindowStep = getSubWindowStep();
    const outputWindowRange = getOutputWindowRange();
    const outputWindowStep = getOutputWindowStep();
    const sessionId = getSessionId();
    const resultTopic = getResultTopic("naive_distributed/output");
    const benchmarkTargetWindowCount = getBenchmarkTargetWindowCount();
    const benchmarkWindowSummaryPath = `${process.env.LOG_PATH || "."}/benchmark_window_cap_summary.json`;
    let benchmarkShutdownInitiated = false;
    const scheduleBenchmarkShutdown = () => {
        if (benchmarkShutdownInitiated) {
            return;
        }
        benchmarkShutdownInitiated = true;
        setTimeout(() => {
            process.exit(0);
        }, 50);
    };
    const subQuery1 = buildSubQuery1(
        aggregationFunction,
        subWindowRange,
        subWindowStep,
    );
    const subQuery2 = buildSubQuery2(
        aggregationFunction,
        subWindowRange,
        subWindowStep,
    );
    const superQuery = buildSuperQuery(
        aggregationFunction,
        outputWindowRange,
        outputWindowStep,
    );

    const queryRegisteredTime = Date.now();
    log("naive_distributed_query_registered");
    console.log(`Naive Distributed: query registered at ${queryRegisteredTime}`);

    // Shared timing state — updated by all three engines' MQTT callbacks
    let firstDataReceivedTime = 0;
    let lastObsReceivedTime   = 0;
    const finalizedWindowNumbers = new Set<number>();
    const partialsByWindowNumber = new Map<number, WindowPartialAggregate>();

    const onData = (wallClock: number) => {
        if (firstDataReceivedTime === 0) firstDataReceivedTime = wallClock;
        lastObsReceivedTime = wallClock;
    };

    // ── Three independent RSP engines — no result reuse ──────────────────────
    const subEngine1 = new RSPEngine(subQuery1);
    const subEngine2 = new RSPEngine(subQuery2);
    const superEngine = new RSPEngine(superQuery);

    const subEmitter1 = subEngine1.register();
    const subEmitter2 = subEngine2.register();
    const superEmitter = superEngine.register();

    // Subquery result handlers – engines run and consume resources;
    // their results are NOT forwarded to the super-query (no reuse).
    subEmitter1.on("RStream", (obj: any) => {
        log(`SubQuery1 (wearableX) result: ${JSON.stringify(obj)}`);
    });

    subEmitter2.on("RStream", (obj: any) => {
        log(`SubQuery2 (smartphoneX) result: ${JSON.stringify(obj)}`);
    });

    // ── Super-query result handler ────────────────────────────────────────────
    superEmitter.on("RStream", (object: any) => {
        // Log in the same format as FetchingClientSide so that
        // extract-results-from-logs.js can parse this file identically.
        console.log("DEBUG: RStream event received:", JSON.stringify(object));

        if (!object || !object.bindings) {
            console.error("Received invalid RStream object:", object);
            return;
        }

        const bindings = Array.isArray(object.bindings)
            ? object.bindings
            : [object.bindings];

        for (const binding of bindings) {
            if (benchmarkShutdownInitiated) {
                continue;
            }
            const parsed = parseBindingFields(binding);
            const emittedWindowNumber = Number(object?.window_number);
            const logicalTriggerTime = Number(object?.logical_trigger_time);
            const windowStart = Number(object?.window_start);
            const windowEnd = Number(object?.window_end);
            const windowDataCloseTime = Number(object?.window_data_close_time);
            const emittedAt = Number(object?.result_emitted_at);
            const latencyFromLogicalTriggerMs = Number(object?.latency_from_logical_trigger_ms);
            const latencyFromWindowCloseMs = Number(object?.latency_from_window_close_ms);
            const metadataSource = object?.metadata_source ? String(object.metadata_source) : "direct";
            const windowSemantics = object?.window_semantics ? String(object.window_semantics) : "trailing";
            const windowName = object?.window_name ? String(object.window_name) : "unknown";

            if (
                !Number.isFinite(emittedWindowNumber) ||
                !Number.isFinite(windowStart) ||
                !Number.isFinite(windowEnd) ||
                !Number.isFinite(windowDataCloseTime) ||
                !Number.isFinite(logicalTriggerTime) ||
                !Number.isFinite(emittedAt) ||
                !Number.isFinite(parsed.resultValue) ||
                !Number.isFinite(parsed.eventCount) ||
                !Number.isFinite(parsed.sumValue)
            ) {
                console.log("DEBUG: Could not parse resultValue from binding:", binding);
                continue;
            }
            const windowNumber = emittedWindowNumber;
            const eventCount = parsed.eventCount as number;
            const sumValue = parsed.sumValue as number;
            const existing = partialsByWindowNumber.get(windowNumber) || {
                logicalTriggerTime,
                windowStart,
                windowEnd,
                windowDataCloseTime,
                windowSemantics,
                metadataSource,
                expectedWindowClose: windowDataCloseTime,
                firstEventTimestamp: parsed.firstEventTimestamp,
                lastEventTimestamp: parsed.lastEventTimestamp,
                streamKeys: new Set<string>(),
                partialCount: 0,
                totalEventCount: 0,
                totalSumValue: 0,
                avgValues: [],
                resultEmittedAt: emittedAt,
                lastObservedAt: lastObsReceivedTime || emittedAt,
                latencyFromLogicalTriggerMs,
                latencyFromWindowCloseMs,
            };

            if (existing.streamKeys.has(windowName)) {
                continue;
            }

            existing.streamKeys.add(windowName);
            existing.partialCount += 1;
            existing.totalEventCount += eventCount;
            existing.totalSumValue += sumValue;
            if (Number.isFinite(parsed.avgValue)) {
                existing.avgValues.push(parsed.avgValue as number);
            }
            existing.resultEmittedAt = Number.isFinite(existing.resultEmittedAt as number)
                ? Math.max(existing.resultEmittedAt as number, emittedAt)
                : emittedAt;
            existing.lastObservedAt = lastObsReceivedTime || emittedAt;
            existing.firstEventTimestamp = existing.firstEventTimestamp || parsed.firstEventTimestamp;
            existing.lastEventTimestamp = parsed.lastEventTimestamp || existing.lastEventTimestamp;
            partialsByWindowNumber.set(windowNumber, existing);

            if (existing.partialCount < 2 || finalizedWindowNumbers.has(windowNumber)) {
                continue;
            }

            const resultTime = existing.resultEmittedAt as number;
            const expectedClose = existing.expectedWindowClose as number;
            const resolvedResultValue = aggregationFunction === "AVG"
                ? existing.totalSumValue / existing.totalEventCount
                : existing.totalSumValue;
            finalizedWindowNumbers.add(windowNumber);

            log(`LATENCY: Window ${windowNumber}:`);
            log(`  - From logical trigger: ${existing.latencyFromLogicalTriggerMs}ms (expected close: ${expectedClose}, result: ${resultTime})`);
            log(`Successfully published unified cross-sensor resultValue: ${resolvedResultValue}`);
            console.log(
                `Window ${windowNumber}: resultValue = ${resolvedResultValue}, latency from logical trigger = ${existing.latencyFromLogicalTriggerMs}ms`,
            );

            logLatency({
                window_number: windowNumber,
                query_registered_at: queryRegisteredTime,
                first_data_received_at: firstDataReceivedTime || queryRegisteredTime,
                expected_window_close: expectedClose,
                last_obs_received_at: existing.lastObservedAt,
                result_emitted_at: resultTime,
                latency_from_query_reg_ms: resultTime - queryRegisteredTime,
                latency_from_data_start_ms: resultTime - (firstDataReceivedTime || queryRegisteredTime),
                latency_from_last_obs_ms: resultTime - (existing.lastObservedAt || resultTime),
                window_semantics: existing.windowSemantics,
                logical_trigger_time: existing.logicalTriggerTime,
                window_start: existing.windowStart,
                window_end: existing.windowEnd,
                window_data_close_time: existing.windowDataCloseTime,
                latency_from_logical_trigger_ms: existing.latencyFromLogicalTriggerMs,
                latency_from_window_close_ms: existing.latencyFromWindowCloseMs,
                metadata_source: existing.metadataSource,
                result_value: resolvedResultValue,
                event_count: existing.totalEventCount,
                sum_value: existing.totalSumValue,
                avg_value: resolvedResultValue,
                first_event_timestamp: existing.firstEventTimestamp,
                last_event_timestamp: existing.lastEventTimestamp,
            });

            // Publish result to the naive_distributed/output MQTT topic so that
            // capture-results.js can subscribe to it as a secondary capture path.
            const pubClientId = "naive-pub-" + Math.random().toString(16).substr(2, 8);
            const pubClient = mqtt.connect("mqtt://localhost:1883", {
                clean: false,
                clientId: pubClientId,
            });
            const resultPayload = JSON.stringify({
                ...buildBenchmarkResultPayload(
                    "naive_distributed",
                    aggregationFunction,
                    sessionId,
                    resolvedResultValue,
                    windowNumber,
                ),
                windowNumber,
                eventCount: existing.totalEventCount,
                sumValue: existing.totalSumValue,
                avgValue: resolvedResultValue,
                firstEventTimestamp: existing.firstEventTimestamp,
                lastEventTimestamp: existing.lastEventTimestamp,
                windowStart: existing.windowStart,
                windowEnd: existing.windowEnd,
                logicalTriggerTime: existing.logicalTriggerTime,
            });
            pubClient.on("connect", () => {
                pubClient.publish(
                    resultTopic,
                    resultPayload,
                    { qos: 1 },
                    (err: any) => {
                        if (err) {
                            log(`Error publishing to ${resultTopic}: ${err}`);
                        } else {
                            recordPublishedMqttMessage({
                                topic: resultTopic,
                                payload: resultPayload,
                                messageType: "superquery_result",
                                warmup: windowNumber === 1,
                            });
                        }
                        pubClient.end();
                    },
                );
            });

            maybeFinalizeNaiveBenchmarkTargetWindowCount({
                finalizedWindowNumbers,
                benchmarkTargetWindowCount,
                benchmarkWindowSummaryPath,
                scheduleShutdown: scheduleBenchmarkShutdown,
            });
        }
    });

    // ── Subscribe all three engines directly to raw MQTT streams ─────────────
    log("Starting SubQuery 1 engine (wearableX, direct MQTT)...");
    await subscribeEngineToMQTT(subEngine1, subQuery1, log, onData);

    log("Starting SubQuery 2 engine (smartphoneX, direct MQTT)...");
    await subscribeEngineToMQTT(subEngine2, subQuery2, log, onData);

    log("Starting super-query engine (wearableX + smartphoneX, direct MQTT, no result reuse)...");
    await subscribeEngineToMQTT(superEngine, superQuery, log, onData);

    log("All three RSP engines started. Naive Distributed Execution running.");
    console.log(
        "Naive Distributed: 3 RSP engines active simultaneously on raw streams (no result reuse).",
    );
}

// ─── Resource usage logging ───────────────────────────────────────────────────

function startResourceUsageLogging(
    filePath = "naive_distributed_approach_resource_usage.csv",
    intervalMs = 100,
): void {
    const writeHeader = !fs.existsSync(filePath);
    const logStream = fs.createWriteStream(filePath, { flags: "a" });
    if (writeHeader) {
        logStream.write(
            "timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n",
        );
    }
    const timer = setInterval(() => {
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
    timer.unref?.();
}

if (require.main === module) {
    startResourceUsageLogging();
    NaiveDistributedApproachOrchestrator().catch((error) => {
        console.error("Error in Naive Distributed orchestrator:", error);
    });
}
