import fs from "fs";
import { RSPEngine, RSPQLParser } from "rsp-js";
import { turtleStringToStore } from "../util/Util";
import {
    AggregationFunction,
    buildBenchmarkResultPayload,
    buildOutputSelectClause,
    buildSubQuerySelectClause,
    getConfiguredAggregation,
    getOutputWindowRange,
    getOutputWindowStep,
    getResultTopic,
    getSessionId,
    getSubWindowRange,
    getSubWindowStep,
} from "../util/runtimeConfig";
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
return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <subquery1_output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "WearableX")}
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
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
return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <subquery2_output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "SmartphoneX")}
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
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
return `
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
            ?s1 saref:hasTimestamp ?ts .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
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
    windowNumber: number,
    queryRegisteredAt: number,
    firstDataReceivedAt: number,
    expectedWindowClose: number,
    lastObsReceivedAt: number,
    resultEmittedAt: number,
    value: string,
) => void {
    const writeHeader = !fs.existsSync(filePath);
    const stream = fs.createWriteStream(filePath, { flags: "a" });
    if (writeHeader) {
        stream.write(
            "window_number,query_registered_at,first_data_received_at," +
            "expected_window_close,last_obs_received_at,result_emitted_at," +
            "latency_from_query_reg_ms,latency_from_data_start_ms," +
            "latency_from_last_obs_ms,result_value\n",
        );
    }
    return (
        windowNumber, queryRegisteredAt, firstDataReceivedAt,
        expectedWindowClose, lastObsReceivedAt, resultEmittedAt, value,
    ) => {
        const latFromReg      = resultEmittedAt - queryRegisteredAt;
        const latFromDataStart = resultEmittedAt - (firstDataReceivedAt + (windowNumber - 1) * 60000);
        const latFromLastObs  = resultEmittedAt - lastObsReceivedAt;
        stream.write(
            `${windowNumber},${queryRegisteredAt},${firstDataReceivedAt},` +
            `${expectedWindowClose},${lastObsReceivedAt},${resultEmittedAt},` +
            `${latFromReg},${latFromDataStart},${latFromLastObs},${value}\n`,
        );
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
    const windowRange = outputWindowRange;
    const windowStep  = outputWindowStep;
    let windowCount = 0;

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
            let resultValue: string | null = null;

            if (binding instanceof Map) {
                resultValue =
                    (binding as Map<string, any>).get("?resultValue")?.value ??
                    (binding as Map<string, any>).get("?avgValue")?.value ??
                    null;
            } else if (binding.entries) {
                try {
                    if (typeof binding.entries.get === "function") {
                        resultValue =
                            binding.entries.get("resultValue")?.value ??
                            binding.entries.get("avgValue")?.value ??
                            null;
                    } else {
                        resultValue =
                            (binding.entries as any).resultValue?.value ??
                            (binding.entries as any).avgValue?.value ??
                            null;
                    }
                } catch (_) { /* ignore */ }
            } else {
                try {
                    resultValue =
                        (binding as any).resultValue?.value ??
                        (binding as any).avgValue?.value ??
                        null;
                } catch (_) { /* ignore */ }
            }

            if (!resultValue) {
                console.log("DEBUG: Could not parse resultValue from binding:", binding);
                continue;
            }
            const resolvedResultValue = resultValue;

            windowCount++;
            const resultTime = Date.now();
            const expectedClose =
                queryRegisteredTime + windowRange + (windowCount - 1) * windowStep;
            const latency = resultTime - expectedClose;

            log(`LATENCY: Window ${windowCount}:`);
            log(`  - From query registration: ${latency}ms (expected close: ${expectedClose}, result: ${resultTime})`);
            log(`Successfully published unified cross-sensor resultValue: ${resolvedResultValue}`);
            console.log(
                `Window ${windowCount}: resultValue = ${resolvedResultValue}, latency from registration = ${latency}ms`,
            );

            // Write to per-window latency CSV (same format as other approaches)
            logLatency(
                windowCount,
                queryRegisteredTime,
                firstDataReceivedTime || queryRegisteredTime,
                expectedClose,
                lastObsReceivedTime  || resultTime,
                resultTime,
                resolvedResultValue,
            );

            // Publish result to the naive_distributed/output MQTT topic so that
            // capture-results.js can subscribe to it as a secondary capture path.
            const pubClientId = "naive-pub-" + Math.random().toString(16).substr(2, 8);
            const pubClient = mqtt.connect("mqtt://localhost:1883", {
                clean: false,
                clientId: pubClientId,
            });
            pubClient.on("connect", () => {
                pubClient.publish(
                    resultTopic,
                    JSON.stringify({
                        ...buildBenchmarkResultPayload(
                            "naive_distributed",
                            aggregationFunction,
                            sessionId,
                            Number.parseFloat(resolvedResultValue),
                            windowCount,
                        ),
                        windowNumber: windowCount,
                    }),
                    { qos: 1 },
                    (err: any) => {
                        if (err) {
                            log(`Error publishing to ${resultTopic}: ${err}`);
                        }
                        pubClient.end();
                    },
                );
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

startResourceUsageLogging();
NaiveDistributedApproachOrchestrator().catch((error) => {
    console.error("Error in Naive Distributed orchestrator:", error);
});
