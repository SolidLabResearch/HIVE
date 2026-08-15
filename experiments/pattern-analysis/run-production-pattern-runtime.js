#!/usr/bin/env node

// Production entry point used by the custom-pattern harness for reusable
// approaches.  It deliberately mirrors the scalability runners: the HTTP
// server owns managed producers and the shared reconstruction runtime, while
// this small harness only registers a query and consumes its final output.

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const mqtt = require("mqtt");
const { delay, terminateChildProcessTree } = require("../utils/processCleanup");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const PORT = 8080;
const RANGE_MS = 120000;
const STEP_MS = 60000;
const SUB_RANGE_MS = 60000;
const SUB_STEP_MS = 30000;

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function appendNdjson(filePath, value) {
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`);
}

function buildQuery(prefix, outputIri) {
  const stream = (name) => `mqtt://localhost:1883/${prefix}/${name}`;
  const topic = (name) => `${prefix}/${name}`;
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <${outputIri}> AS
SELECT (AVG(?value) AS ?resultValue)
FROM NAMED WINDOW <${stream("wearableX")}> ON STREAM mqtt_broker:${topic("wearableX")} [RANGE ${RANGE_MS} STEP ${STEP_MS}]
FROM NAMED WINDOW <${stream("smartphoneX")}> ON STREAM mqtt_broker:${topic("smartphoneX")} [RANGE ${RANGE_MS} STEP ${STEP_MS}]
WHERE {
  { WINDOW <${stream("wearableX")}> { ?s1 saref:hasValue ?value . ?s1 saref:hasTimestamp ?ts . ?s1 saref:relatesToProperty dahccsensors:wearableX . } }
  UNION
  { WINDOW <${stream("smartphoneX")}> { ?s2 saref:hasValue ?value . ?s2 saref:hasTimestamp ?ts . ?s2 saref:relatesToProperty dahccsensors:smartphoneX . } }
}`.trim();
}

async function waitForReady(serverLog) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (fs.existsSync(serverLog) && fs.readFileSync(serverLog, "utf8").includes("HTTP server has started")) return;
    try {
      const response = await fetch(`http://localhost:${PORT}/register`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      if (response.status === 400) return;
    } catch { /* startup polling */ }
    await delay(250);
  }
  throw new Error("Timed out waiting for production HIVE /register");
}

function parsePayload(text) {
  const raw = JSON.parse(text);
  const number = (value) => Number.isFinite(Number(value)) ? Number(value) : null;
  return {
    raw,
    value: number(raw.value ?? raw.avgValue ?? raw.recomposedAvg),
    windowStart: number(raw.windowStart ?? raw.window_start),
    windowEnd: number(raw.windowEnd ?? raw.window_end),
    timestamp: number(raw.timestamp),
    rangeMs: number(raw.rangeMs),
    stepMs: number(raw.stepMs),
    isComparableWindow: raw.isComparableWindow === true || raw.comparableWindow === true,
    coverageComplete: raw.coverageComplete === true,
    executionId: raw.executionId || null,
  };
}

async function main() {
  const approach = String(process.argv[2] || "").trim();
  if (!["approximation", "chunked"].includes(approach)) throw new Error("Usage: run-production-pattern-runtime.js <approximation|chunked>");
  const logDir = process.env.LOG_PATH;
  const prefix = process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX;
  const anchor = Number(process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR);
  const targetWindows = Number(process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS || "1");
  if (!logDir || !prefix || !Number.isFinite(anchor) || !Number.isInteger(targetWindows) || targetWindows < 1) throw new Error("Missing benchmark log/topic/anchor/target-window configuration");

  const serverOut = path.join(logDir, "server.stdout.log");
  const serverErr = path.join(logDir, "server.stderr.log");
  const registrationPath = path.join(logDir, "registration_events.ndjson");
  const deliveryPath = path.join(logDir, "production_delivery_events.ndjson");
  const outputIri = `mqtt://localhost:1883/${prefix}/final-output`;
  const server = spawn("node", [path.join(REPO_ROOT, "dist", "startHTTPServer.js")], {
    cwd: REPO_ROOT,
    env: { ...process.env, HIVE_PROCESS_ROLE: "pattern_production_server" },
    detached: true,
    stdio: ["ignore", fs.openSync(serverOut, "a"), fs.openSync(serverErr, "a")],
  });
  let client;
  try {
    await waitForReady(serverOut);
    const requestBody = {
      id: `${approach}-pattern-final`, consumer_id: `${approach}-pattern-consumer`, approach,
      rspql_query: buildQuery(prefix, outputIri), r2s_topic: outputIri, data_topic: outputIri,
      approximation_config: approach === "approximation" ? { policy: "rate-based-completed-window", completedWindowMode: true, earlyTriggerMode: false } : undefined,
    };
    const response = await fetch(`http://localhost:${PORT}/register`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(requestBody) });
    const payload = await response.json();
    if (!response.ok || typeof payload.outputTopic !== "string") throw new Error(`Registration failed: ${response.status} ${JSON.stringify(payload)}`);
    const registration = { ...payload, requestBody, requestedAt: Date.now(), expectedWindowStart: anchor, expectedWindowEnd: anchor + RANGE_MS, rangeMs: RANGE_MS, stepMs: STEP_MS };
    appendNdjson(registrationPath, registration);

    const delivery = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timed out waiting for a complete comparable production result")), 240000);
      client = mqtt.connect("mqtt://localhost:1883", { clean: true, clientId: `${requestBody.consumer_id}-${Math.random().toString(16).slice(2, 10)}` });
      client.on("connect", () => client.subscribe(payload.outputTopic, { qos: 1 }, (error) => error && reject(error)));
      client.on("error", reject);
      client.on("message", (topic, buffer) => {
        if (topic !== payload.outputTopic) return;
        let parsed;
        try { parsed = parsePayload(buffer.toString("utf8")); } catch { return; }
        // The production result contract carries executionId when available;
        // older final payload serializers may omit it, as accounted for by the
        // canonical scalability delivery gate.  A present ID must still match.
        const accepted = parsed.isComparableWindow === true && parsed.coverageComplete === true && (!parsed.executionId || parsed.executionId === payload.executionId) && parsed.windowStart === anchor && parsed.windowEnd === anchor + RANGE_MS && (parsed.rangeMs === null || parsed.rangeMs === RANGE_MS) && (parsed.stepMs === null || parsed.stepMs === STEP_MS);
        const event = { receivedAt: Date.now(), accepted, registration, ...parsed };
        appendNdjson(deliveryPath, event);
        if (accepted) { clearTimeout(timeout); resolve(event); }
      });
    });
    writeJson(path.join(logDir, "production_delivery_summary.json"), { approach, registration, delivery, outputRangeMs: RANGE_MS, outputStepMs: STEP_MS, subWindowRangeMs: SUB_RANGE_MS, subWindowStepMs: SUB_STEP_MS });
    writeJson(path.join(logDir, "benchmark_window_cap_summary.json"), { stoppedAfterTargetWindows: true, emittedFinalWindowCount: targetWindows, executionPath: "production-register-shared-runtime" });
  } finally {
    if (client) client.end(true);
    await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 3000, killWaitMs: 3000 });
  }
}

main().catch((error) => { console.error(error.stack || error.message); process.exitCode = 1; });
