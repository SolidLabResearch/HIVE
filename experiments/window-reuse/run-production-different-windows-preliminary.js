#!/usr/bin/env node

const { execSync, spawn } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const {
  cleanupStaleBenchmarkProcesses,
  delay,
  terminateChildProcessTree,
} = require("../utils/processCleanup");
const {
  startProcessTreeResourceLogging,
} = require("../../scripts/analysis-js/process-tree-resource-sampler");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const CONTROL_PORT = 8080;
const BUILD_MANIFEST_NAME = "production-build-manifest.json";
const SERVER_EXECUTABLE_PATH = path.join(REPO_ROOT, "dist", "startHTTPServer.js");
const PUBLISHER_EXECUTABLE_PATH = path.join(
  REPO_ROOT,
  "dist",
  "streamer",
  "src",
  "publish.js",
);
const POLL_INTERVAL_MS = 250;
const STARTUP_TIMEOUT_MS = 30_000;
const SUBSCRIBE_TIMEOUT_MS = 15_000;
const DELIVERY_TIMEOUT_MS = 8 * 60 * 1000;
const REPLAY_DURATION_SECONDS = 305;
const ARTIFACT_SETTLE_MS = 2_000;
const CHUNK_RANGE_MS = 60_000;
const CHUNK_STEP_MS = 30_000;
const FINAL_STEP_MS = 60_000;
const FLOAT_TOLERANCE = 1e-9;
const DATA_PATH = "custom_patterns/low_variability";
const FINAL_QUERIES = [
  { label: "Q120", rangeMs: 120_000 },
  { label: "Q180", rangeMs: 180_000 },
];
const ALL_APPROACHES = ["fetching", "approximation", "chunked"];

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function sanitizeTimestamp(date) {
  return date.toISOString().replace(/[:.]/g, "-");
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function appendNdjson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`);
}

function runCommand(command, cwd = REPO_ROOT) {
  return execSync(command, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function buildResultRoot() {
  return path.join(
    REPO_ROOT,
    "results",
    "window-reuse",
    `production-different-windows-preliminary-${sanitizeTimestamp(new Date())}`,
  );
}

function getSourceFilesForBuildGuard() {
  const output = runCommand(
    "rg --files src experiments/window-reuse package.json tsconfig.json 2>/dev/null || true",
  );
  return output
    .split(/\r?\n/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => path.join(REPO_ROOT, entry))
    .filter((entry) => fs.existsSync(entry));
}

function getLatestSourceMtimeMs() {
  return getSourceFilesForBuildGuard().reduce((latest, filePath) => {
    const stat = fs.statSync(filePath);
    return Math.max(latest, stat.mtimeMs);
  }, 0);
}

function buildRepoStateSnapshot() {
  const branch = runCommand("git branch --show-current");
  const commit = runCommand("git rev-parse HEAD");
  const statusShort = runCommand("git status --short || true");
  const dirty = statusShort.trim().length > 0;
  return {
    branch,
    commit,
    statusShort,
    dirty,
    diffHash: sha256(statusShort),
  };
}

function writeBuildManifest(manifest) {
  const manifestPath = path.join(REPO_ROOT, "dist", BUILD_MANIFEST_NAME);
  writeJson(manifestPath, manifest);
  return manifestPath;
}

function readBuildManifest() {
  const manifestPath = path.join(REPO_ROOT, "dist", BUILD_MANIFEST_NAME);
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Missing build manifest: ${manifestPath}`);
  }
  return {
    manifestPath,
    manifest: JSON.parse(fs.readFileSync(manifestPath, "utf8")),
  };
}

function ensureFreshProductionBuild(resultRoot) {
  const repoState = buildRepoStateSnapshot();
  const buildStartedAt = new Date().toISOString();
  const latestSourceMtimeMsBeforeBuild = getLatestSourceMtimeMs();
  execSync("npm run build", {
    cwd: REPO_ROOT,
    stdio: "inherit",
  });
  const buildCompletedAt = new Date().toISOString();
  if (!fs.existsSync(SERVER_EXECUTABLE_PATH)) {
    throw new Error(`Missing server executable after build: ${SERVER_EXECUTABLE_PATH}`);
  }
  const manifest = {
    commit: repoState.commit,
    branch: repoState.branch,
    dirty: repoState.dirty,
    statusShort: repoState.statusShort,
    diffHash: repoState.diffHash,
    buildStartedAt,
    buildCompletedAt,
    latestSourceMtimeMsBeforeBuild,
    serverExecutable: SERVER_EXECUTABLE_PATH,
    sourceFingerprint: sha256(
      JSON.stringify({
        commit: repoState.commit,
        diffHash: repoState.diffHash,
        latestSourceMtimeMsBeforeBuild,
      }),
    ),
  };
  const manifestPath = writeBuildManifest(manifest);
  writeJson(path.join(resultRoot, "build_identity.json"), {
    ...manifest,
    manifestPath,
  });
  return {
    repoState,
    buildManifestPath: manifestPath,
    buildManifest: manifest,
  };
}

function verifyBuildManifestOrThrow(expectedManifest) {
  const { manifestPath, manifest } = readBuildManifest();
  const latestSourceMtimeMs = getLatestSourceMtimeMs();
  const buildCompletedMs = Date.parse(manifest.buildCompletedAt || "");
  if (!Number.isFinite(buildCompletedMs)) {
    throw new Error(`Invalid buildCompletedAt in manifest: ${manifestPath}`);
  }
  if (buildCompletedMs < latestSourceMtimeMs) {
    throw new Error(
      `Build manifest predates source changes: build=${manifest.buildCompletedAt} latestSourceMtimeMs=${latestSourceMtimeMs}`,
    );
  }
  if (expectedManifest && manifest.sourceFingerprint !== expectedManifest.sourceFingerprint) {
    throw new Error(
      `Runtime build fingerprint mismatch: expected=${expectedManifest.sourceFingerprint} actual=${manifest.sourceFingerprint}`,
    );
  }
  return { manifestPath, manifest };
}

function buildQuery({ inputTopicPrefix, outputIri, rangeMs }) {
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX rspjs: <https://rsp.js>

REGISTER RStream <${outputIri}> AS
SELECT (AVG(?value) AS ?resultValue) (COUNT(?value) AS ?eventCount) (SUM(?value) AS ?sumValue) (AVG(?value) AS ?avgValue) (MIN(?ts) AS ?firstEventTimestamp) (MAX(?ts) AS ?lastEventTimestamp)
FROM NAMED WINDOW <mqtt://localhost:1883/${inputTopicPrefix}/wearableX> ON STREAM mqtt_broker:${inputTopicPrefix}/wearableX [RANGE ${rangeMs} STEP ${FINAL_STEP_MS}]
FROM NAMED WINDOW <mqtt://localhost:1883/${inputTopicPrefix}/smartphoneX> ON STREAM mqtt_broker:${inputTopicPrefix}/smartphoneX [RANGE ${rangeMs} STEP ${FINAL_STEP_MS}]
WHERE {
  {
    WINDOW <mqtt://localhost:1883/${inputTopicPrefix}/wearableX> {
      ?wearableObservation saref:hasValue ?value .
      ?wearableObservation saref:hasTimestamp ?ts .
      ?wearableObservation saref:relatesToProperty dahccsensors:wearableX .
    }
  }
  UNION
  {
    WINDOW <mqtt://localhost:1883/${inputTopicPrefix}/smartphoneX> {
      ?smartphoneObservation saref:hasValue ?value .
      ?smartphoneObservation saref:hasTimestamp ?ts .
      ?smartphoneObservation saref:relatesToProperty dahccsensors:smartphoneX .
    }
  }
}
`;
}

function parseArgs(argv) {
  const args = {
    timeoutMs: DELIVERY_TIMEOUT_MS,
    baseAnchorMs: null,
    approaches: [...ALL_APPROACHES],
    oracleRoot: null,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--timeout-ms":
        args.timeoutMs = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--base-anchor-ms":
        args.baseAnchorMs = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--approach":
      case "--approaches":
        args.approaches = String(next || "")
          .split(",")
          .map((entry) => entry.trim().toLowerCase())
          .filter(Boolean);
        index += 1;
        break;
      case "--oracle-root":
        args.oracleRoot = next ? path.resolve(next) : null;
        index += 1;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (args.approaches.length === 0) {
    throw new Error("At least one approach must be selected");
  }
  for (const approach of args.approaches) {
    if (!ALL_APPROACHES.includes(approach)) {
      throw new Error(`Unsupported approach: ${approach}`);
    }
  }
  return args;
}

function buildWindowPlan(anchorMs, queries) {
  const requiredWatermark = anchorMs + Math.max(...queries.map((entry) => entry.rangeMs));
  return {
    replayAnchor: anchorMs,
    requiredWatermark,
    queries: queries.map((entry) => ({
      ...entry,
      stepMs: FINAL_STEP_MS,
      firstFinalWindowStart: anchorMs,
      firstFinalWindowEnd: anchorMs + entry.rangeMs,
      expectedIntermediateChunkIds: ["wearableX", "smartphoneX"],
      expectedFinalReconstructionPlan: entry.rangeMs / FINAL_STEP_MS,
      requiredEventTimeWatermark: requiredWatermark,
    })),
  };
}

function getLatestPreviousResultRoots() {
  const resultsRoot = path.join(REPO_ROOT, "results", "window-reuse");
  if (!fs.existsSync(resultsRoot)) {
    return [];
  }
  return fs.readdirSync(resultsRoot)
    .filter((entry) => entry.startsWith("production-different-windows-preliminary-"))
    .map((entry) => path.join(resultsRoot, entry))
    .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);
}

function parseResultPayload(payloadText) {
  const parsed = JSON.parse(payloadText);
  const toNumber = (value) =>
    Number.isFinite(Number(value)) ? Number(value) : null;
  return {
    raw: parsed,
    value: toNumber(parsed.value),
    windowStart: toNumber(parsed.windowStart ?? parsed.window_start),
    windowEnd: toNumber(parsed.windowEnd ?? parsed.window_end),
    publicationTimestamp: toNumber(parsed.timestamp),
    eventCount: toNumber(parsed.eventCount ?? parsed.count),
    sumValue: toNumber(parsed.sumValue ?? parsed.sum),
    avgValue: toNumber(parsed.avgValue ?? parsed.average),
    recomposedCount: toNumber(parsed.recomposedCount),
    recomposedSum: toNumber(parsed.recomposedSum),
    recomposedAvg: toNumber(parsed.recomposedAvg),
    rangeMs: toNumber(parsed.rangeMs),
    stepMs: toNumber(parsed.stepMs),
    isComparableWindow:
      parsed.isComparableWindow === true || parsed.comparableWindow === true,
    coverageComplete: parsed.coverageComplete === true,
  };
}

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadFetchingOracleSummary({ oracleRoot, excludeRoot } = {}) {
  const candidateRoots = [];
  if (oracleRoot) {
    candidateRoots.push(oracleRoot);
  }
  for (const root of getLatestPreviousResultRoots()) {
    if (excludeRoot && path.resolve(root) === path.resolve(excludeRoot)) {
      continue;
    }
    candidateRoots.push(root);
  }

  for (const root of candidateRoots) {
    const summaryPath = path.join(root, "fetching", "approach_summary.json");
    if (!fs.existsSync(summaryPath)) {
      continue;
    }
    const summary = loadJson(summaryPath);
    if (Array.isArray(summary?.deliveries) && summary.deliveries.length === FINAL_QUERIES.length) {
      return {
        oracleRoot: root,
        summary,
      };
    }
  }

  throw new Error("Missing saved Fetching oracle summary for Chunked comparison");
}

function normalizeDelivery(approach, registration, parsed) {
  return {
    queryLabel: registration.queryLabel,
    rangeMs: registration.rangeMs,
    executionId: registration.executionId,
    outputTopic: registration.outputTopic,
    windowStart: parsed.windowStart,
    windowEnd: parsed.windowEnd,
    sum:
      approach === "chunked"
        ? parsed.recomposedSum
        : parsed.sumValue,
    count:
      approach === "chunked"
        ? parsed.recomposedCount
        : parsed.eventCount,
    avg:
      approach === "chunked"
        ? parsed.recomposedAvg ?? parsed.value
        : parsed.avgValue ?? parsed.value,
    value: parsed.value,
    coverageComplete: parsed.coverageComplete,
    isComparableWindow: parsed.isComparableWindow,
    publicationTimestamp: parsed.publicationTimestamp,
    receptionTimestamp: Date.now(),
    payload: parsed.raw,
  };
}

function evaluateComparableDelivery(registration, parsed) {
  if (parsed.isComparableWindow !== true) {
    return {
      accepted: false,
      reason: "isComparableWindow=false",
    };
  }

  if (registration.executionId && parsed.raw?.executionId && parsed.raw.executionId !== registration.executionId) {
    return {
      accepted: false,
      reason: "executionId_mismatch",
    };
  }

  if (
    Number.isFinite(registration.expectedWindowStart) &&
    parsed.windowStart !== registration.expectedWindowStart
  ) {
    return {
      accepted: false,
      reason: "windowStart_mismatch",
    };
  }

  if (
    Number.isFinite(registration.expectedWindowEnd) &&
    parsed.windowEnd !== registration.expectedWindowEnd
  ) {
    return {
      accepted: false,
      reason: "windowEnd_mismatch",
    };
  }

  if (Number.isFinite(registration.rangeMs) && parsed.rangeMs !== null && parsed.rangeMs !== registration.rangeMs) {
    return {
      accepted: false,
      reason: "rangeMs_mismatch",
    };
  }

  if (Number.isFinite(registration.stepMs) && parsed.stepMs !== null && parsed.stepMs !== registration.stepMs) {
    return {
      accepted: false,
      reason: "stepMs_mismatch",
    };
  }

  return {
    accepted: true,
    reason: "accepted",
  };
}

async function waitForServerLog(filePath, timeoutMs = STARTUP_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, "utf8");
      if (content.includes("HTTP server has started")) {
        return;
      }
    }
    await delay(POLL_INTERVAL_MS);
  }
  throw new Error("Timed out waiting for HTTP server startup");
}

async function waitForRegisterReady(port, timeoutMs = STARTUP_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://localhost:${port}/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      if (response.status === 400) {
        return;
      }
    } catch {
      // keep polling
    }
    await delay(POLL_INTERVAL_MS);
  }
  throw new Error("Timed out waiting for register endpoint");
}

async function postRegistration(port, requestBody) {
  const requestStartedAt = Date.now();
  const response = await fetch(`http://localhost:${port}/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });
  const payload = await response.json();
  return {
    requestStartedAt,
    responseReceivedAt: Date.now(),
    status: response.status,
    requestBody,
    responseBody: payload,
    queryLabel: requestBody.query_label,
    rangeMs: requestBody.range_ms,
    consumerId: payload.consumerId,
    canonicalQueryId: payload.canonicalQueryId,
    executionId: payload.executionId,
    executionCreated: payload.executionCreated,
    reuseHit: payload.reuseHit,
    outputTopic: payload.outputTopic,
    reuseDecision: payload.reuseDecision,
    producerSnapshots: payload.producerSnapshots || [],
  };
}

async function subscribeConsumers({ registrations, deliveryPath }) {
  const clients = [];
  const comparableByLabel = new Map();
  const allDeliveries = [];
  const deliveryDecisions = [];
  await Promise.all(
    registrations.map((registration) => new Promise((resolve, reject) => {
      const client = mqtt.connect("mqtt://localhost:1883", {
        clean: true,
        clientId: `${registration.queryLabel}-${Math.random().toString(16).slice(2, 10)}`,
      });
      clients.push(client);
      const timeout = setTimeout(() => {
        reject(new Error(`Timed out subscribing ${registration.queryLabel}`));
      }, SUBSCRIBE_TIMEOUT_MS);
      client.on("connect", () => {
        client.subscribe(registration.outputTopic, { qos: 1 }, (error) => {
          clearTimeout(timeout);
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
      client.on("message", (topic, payloadBuffer) => {
        if (topic !== registration.outputTopic) {
          return;
        }
        const parsed = parseResultPayload(payloadBuffer.toString("utf8"));
        const delivery = normalizeDelivery("unknown", registration, parsed);
        const decision = evaluateComparableDelivery(registration, parsed);
        allDeliveries.push(delivery);
        appendNdjson(deliveryPath, delivery);
        deliveryDecisions.push({
          queryLabel: registration.queryLabel,
          executionId: registration.executionId,
          outputTopic: registration.outputTopic,
          windowStart: delivery.windowStart,
          windowEnd: delivery.windowEnd,
          rangeMs: parsed.rangeMs,
          stepMs: parsed.stepMs,
          isComparableWindow: parsed.isComparableWindow,
          accepted: decision.accepted,
          reason: decision.reason,
        });
        if (decision.accepted && !comparableByLabel.has(registration.queryLabel)) {
          comparableByLabel.set(registration.queryLabel, delivery);
        }
      });
      client.on("error", reject);
    })),
  );

  return {
    async waitForComparableDeliveries(timeoutMs, approach) {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        if (comparableByLabel.size === registrations.length) {
          return registrations.map((registration) => {
            const delivery = comparableByLabel.get(registration.queryLabel);
            if (!delivery) {
              throw new Error(`Missing comparable delivery for ${registration.queryLabel}`);
            }
            return {
              ...delivery,
              ...normalizeDelivery(approach, registration, {
                ...delivery.payload,
                value: delivery.value,
                windowStart: delivery.windowStart,
                windowEnd: delivery.windowEnd,
                publicationTimestamp: delivery.publicationTimestamp,
                eventCount: delivery.count,
                sumValue: delivery.sum,
                avgValue: delivery.avg,
                recomposedCount: delivery.count,
                recomposedSum: delivery.sum,
                recomposedAvg: delivery.avg,
                isComparableWindow: true,
                coverageComplete: delivery.coverageComplete,
              }),
            };
          });
        }
        await delay(POLL_INTERVAL_MS);
      }
      throw new Error(
        `Timed out waiting for ${registrations.length} comparable deliveries; got ${comparableByLabel.size}`,
      );
    },
    async close() {
      for (const client of clients) {
        try {
          client.end(true);
        } catch {
          // ignore cleanup errors
        }
      }
    },
    getDeliveryDecisions() {
      return [...deliveryDecisions];
    },
  };
}

function parseResourceMetrics(csvPath) {
  if (!fs.existsSync(csvPath)) {
    return {
      averageCpuPct: null,
      peakCpuPct: null,
      peakRssMb: null,
      peakProcessCount: null,
      sampleCount: 0,
    };
  }
  const lines = fs.readFileSync(csvPath, "utf8").trim().split(/\r?\n/).slice(1);
  if (lines.length === 0) {
    return {
      averageCpuPct: null,
      peakCpuPct: null,
      peakRssMb: null,
      peakProcessCount: null,
      sampleCount: 0,
    };
  }
  const samples = lines
    .map((line) => line.split(","))
    .map((parts) => ({
      processCount: Number(parts[2]),
      rssMb: Number(parts[3]) / (1024 * 1024),
      cpuPct: Number(parts[7]),
    }))
    .filter((entry) => Number.isFinite(entry.processCount));
  const totalCpu = samples.reduce((sum, entry) => sum + (Number.isFinite(entry.cpuPct) ? entry.cpuPct : 0), 0);
  return {
    averageCpuPct: samples.length > 0 ? totalCpu / samples.length : null,
    peakCpuPct: samples.reduce((peak, entry) => Math.max(peak, entry.cpuPct || 0), 0),
    peakRssMb: samples.reduce((peak, entry) => Math.max(peak, entry.rssMb || 0), 0),
    peakProcessCount: samples.reduce((peak, entry) => Math.max(peak, entry.processCount || 0), 0),
    sampleCount: samples.length,
  };
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function checkPortStatus(port) {
  const output = runCommand(`lsof -ti:${port} || true`);
  return {
    port,
    activePids: output
      .split(/\s+/)
      .map((entry) => Number.parseInt(entry, 10))
      .filter((value) => Number.isFinite(value)),
  };
}

function buildApproachEnv({
  approach,
  runRoot,
  replayAnchor,
  topicPrefix,
}) {
  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const benchmarkMaxTimestamp = replayAnchor + (REPLAY_DURATION_SECONDS * 1000);
  return replayEnv.withBenchmarkReplayEnv({
    ...process.env,
    LOG_PATH: runRoot,
    DATA_PATH,
    SESSION_ID: `experiment1_${approach}_${path.basename(runRoot)}`,
    BENCHMARK_SCENARIO: "experiment1-production-different-windows-preliminary",
    BENCHMARK_APPROACH: approach,
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(
      REPLAY_DURATION_SECONDS,
    ),
    STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: String(replayAnchor),
    STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(replayAnchor),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(replayAnchor),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(benchmarkMaxTimestamp),
    RESULT_TOPIC: "benchmark-payload-enabled",
    OUTPUT_WINDOW_STEP: String(FINAL_STEP_MS),
    SUB_WINDOW_RANGE: String(CHUNK_RANGE_MS),
    SUB_WINDOW_STEP: String(CHUNK_STEP_MS),
    STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
    STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "1",
    STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
    STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
    HIVE_PROFILE: "1",
  });
}

function buildRegistrationBody({
  approach,
  inputTopicPrefix,
  outputTopicBase,
  querySpec,
}) {
  const outputIri = `${outputTopicBase}/${approach}/${querySpec.label.toLowerCase()}`;
  return {
    id: `${approach}-${querySpec.label.toLowerCase()}`,
    consumer_id: `${approach}-${querySpec.label.toLowerCase()}-consumer`,
    query_label: querySpec.label,
    range_ms: querySpec.rangeMs,
    approach,
    rspql_query: buildQuery({
      inputTopicPrefix,
      outputIri,
      rangeMs: querySpec.rangeMs,
    }),
    r2s_topic: outputIri,
    data_topic: outputIri,
    approximation_config:
      approach === "approximation"
        ? {
            policy: "rate-based-completed-window",
            completedWindowMode: true,
            earlyTriggerMode: false,
          }
        : undefined,
  };
}

function validateRegistrationSet(approach, registrations) {
  assert(registrations.length > 0, `${approach}: missing registrations`);
  const executionIds = new Set(registrations.map((entry) => entry.executionId));
  const canonicalQueryIds = new Set(registrations.map((entry) => entry.canonicalQueryId));
  const outputTopics = new Set(registrations.map((entry) => entry.outputTopic));
  assert(
    executionIds.size === registrations.length,
    `${approach}: different RANGE queries shared one final execution`,
  );
  assert(
    canonicalQueryIds.size === registrations.length,
    `${approach}: different RANGE queries shared one canonical final-query ID`,
  );
  assert(
    outputTopics.size === registrations.length,
    `${approach}: final MQTT topics are not distinct`,
  );
  for (const registration of registrations) {
    assert(registration.status === 200, `${approach}/${registration.queryLabel}: register failed`);
    assert(registration.executionCreated === true, `${approach}/${registration.queryLabel}: executionCreated=false`);
    assert(registration.reuseHit === false, `${approach}/${registration.queryLabel}: final-result reuseHit=true`);
    assert(
      registration.reuseDecision?.mutuallyContained === false,
      `${approach}/${registration.queryLabel}: mutuallyContained=true`,
    );
  }
}

async function runApproach({
  approach,
  queries,
  replayAnchor,
  resultRoot,
  deliveryTimeoutMs,
  expectedBuildManifest,
}) {
  const runRoot = path.join(resultRoot, approach);
  ensureDir(runRoot);
  const registrationPath = path.join(runRoot, "registration_events.ndjson");
  const deliveryPath = path.join(runRoot, "consumer_delivery_events.ndjson");
  const serverOutPath = path.join(runRoot, "server.stdout.log");
  const serverErrPath = path.join(runRoot, "server.stderr.log");
  const publisherOutPath = path.join(runRoot, "publisher.stdout.log");
  const publisherErrPath = path.join(runRoot, "publisher.stderr.log");
  const resourcePath = path.join(runRoot, "process_tree_resource_usage.csv");
  const sharedTopicPrefix = `experiment1/${path.basename(resultRoot)}`;
  const env = buildApproachEnv({
    approach,
    runRoot,
    replayAnchor,
    topicPrefix: sharedTopicPrefix,
  });

  await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  verifyBuildManifestOrThrow(expectedBuildManifest);

  const server = spawn("node", [SERVER_EXECUTABLE_PATH], {
    cwd: REPO_ROOT,
    env,
    detached: true,
    stdio: [
      "ignore",
      fs.openSync(serverOutPath, "a"),
      fs.openSync(serverErrPath, "a"),
    ],
  });

  let resourceSampler = null;
  let publisher = null;
  let subscribers = null;
  try {
    await waitForServerLog(serverOutPath);
    await waitForRegisterReady(CONTROL_PORT);

    resourceSampler = startProcessTreeResourceLogging(resourcePath, server.pid, 200);

    const registrations = [];
    for (const querySpec of queries) {
      const registration = await postRegistration(
        CONTROL_PORT,
        buildRegistrationBody({
          approach,
          inputTopicPrefix: sharedTopicPrefix,
          outputTopicBase: `mqtt://localhost:1883/${sharedTopicPrefix}/results`,
          querySpec,
        }),
      );
      registration.expectedWindowStart = replayAnchor;
      registration.expectedWindowEnd = replayAnchor + querySpec.rangeMs;
      registration.stepMs = FINAL_STEP_MS;
      appendNdjson(registrationPath, registration);
      registrations.push(registration);
    }
    validateRegistrationSet(approach, registrations);

    subscribers = await subscribeConsumers({
      registrations,
      deliveryPath,
    });

    publisher = spawn("node", [PUBLISHER_EXECUTABLE_PATH], {
      cwd: REPO_ROOT,
      env,
      detached: true,
      stdio: [
        "ignore",
        fs.openSync(publisherOutPath, "a"),
        fs.openSync(publisherErrPath, "a"),
      ],
    });

    const deliveries = await subscribers.waitForComparableDeliveries(
      deliveryTimeoutMs,
      approach,
    );

    await terminateChildProcessTree(publisher, {
      logger: () => undefined,
      termWaitMs: 2_000,
      killWaitMs: 2_000,
    });
    publisher = null;

    await delay(ARTIFACT_SETTLE_MS);
    const resourceMetrics = parseResourceMetrics(resourcePath);
    const latestProducerSnapshots = registrations.at(-1)?.producerSnapshots || [];
    if (approach === "chunked") {
      assert(
        latestProducerSnapshots.length === 2,
        `chunked created ${latestProducerSnapshots.length} producers instead of two`,
      );
      for (const producer of latestProducerSnapshots) {
        assert(
          producer.referenceCount === queries.length,
          `chunked producer ${producer.producerId} referenceCount=${producer.referenceCount}`,
        );
      }
    }

    const summary = {
      approach,
      runRoot,
      replayAnchor,
      sharedTopicPrefix,
      registrations,
      deliveries,
      resourceMetrics,
      latestProducerSnapshots,
      readiness: {
        registerEndpointReady: true,
        registrationsReady: true,
        consumersSubscribed: true,
        replayStartedAfterReadiness: true,
      },
      deliveryDecisions: subscribers.getDeliveryDecisions(),
    };
    writeJson(path.join(runRoot, "approach_summary.json"), summary);
    return summary;
  } finally {
    await subscribers?.close?.().catch?.(() => undefined);
    if (publisher) {
      await terminateChildProcessTree(publisher, {
        logger: () => undefined,
        termWaitMs: 1_000,
        killWaitMs: 1_000,
      });
    }
    if (resourceSampler) {
      resourceSampler.stop();
    }
    await terminateChildProcessTree(server, {
      logger: () => undefined,
      termWaitMs: 2_000,
      killWaitMs: 2_000,
    });
    await delay(500);
  }
}

function indexByLabel(entries) {
  return new Map(entries.map((entry) => [entry.queryLabel, entry]));
}

function compareAgainstFetching(fetchingSummary, otherSummary, approach) {
  const fetchingByLabel = indexByLabel(fetchingSummary.deliveries);
  return otherSummary.deliveries.map((delivery) => {
    const oracle = fetchingByLabel.get(delivery.queryLabel);
    if (!oracle) {
      throw new Error(`${approach}/${delivery.queryLabel}: missing fetching oracle`);
    }
    const avgError = Math.abs((delivery.avg ?? 0) - (oracle.avg ?? 0));
    const percentError =
      Math.abs(oracle.avg ?? 0) > 1e-12 ? avgError / Math.abs(oracle.avg) : 0;
    return {
      queryLabel: delivery.queryLabel,
      windowStartMatch: delivery.windowStart === oracle.windowStart,
      windowEndMatch: delivery.windowEnd === oracle.windowEnd,
      countMatch:
        approach === "chunked" ? delivery.count === oracle.count : undefined,
      sumError:
        approach === "chunked" && delivery.sum !== null && oracle.sum !== null
          ? Math.abs(delivery.sum - oracle.sum)
          : null,
      avgError,
      percentError,
      oracle,
      delivery,
    };
  });
}

function validateChunkedChecks(chunkedChecks) {
  for (const check of chunkedChecks) {
    assert(check.windowStartMatch, `chunked/${check.queryLabel}: windowStart mismatch`);
    assert(check.windowEndMatch, `chunked/${check.queryLabel}: windowEnd mismatch`);
    assert(check.countMatch === true, `chunked/${check.queryLabel}: count mismatch`);
    assert(
      (check.sumError ?? Infinity) <= FLOAT_TOLERANCE,
      `chunked/${check.queryLabel}: sum mismatch ${check.sumError}`,
    );
    assert(
      check.avgError <= FLOAT_TOLERANCE,
      `chunked/${check.queryLabel}: avg mismatch ${check.avgError}`,
    );
    assert(
      check.delivery.coverageComplete === true,
      `chunked/${check.queryLabel}: final payload coverageComplete=false`,
    );
    assert(
      check.delivery.isComparableWindow === true,
      `chunked/${check.queryLabel}: final payload isComparableWindow=false`,
    );
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const queries = FINAL_QUERIES;
  const resultRoot = buildResultRoot();
  ensureDir(resultRoot);

  const replayAnchor = args.baseAnchorMs || Date.UTC(2026, 7, 5, 10, 0, 0, 0);
  const windowPlan = buildWindowPlan(replayAnchor, queries);
  writeJson(path.join(resultRoot, "window_plan.json"), windowPlan);

  const buildInfo = ensureFreshProductionBuild(resultRoot);
  const approachSummaries = {};
  for (const approach of args.approaches) {
    approachSummaries[approach] = await runApproach({
      approach,
      queries,
      replayAnchor,
      resultRoot,
      deliveryTimeoutMs: args.timeoutMs,
      expectedBuildManifest: buildInfo.buildManifest,
    });
  }

  let fetchingSummary = approachSummaries.fetching || null;
  let oracleSourceRoot = null;
  if (!fetchingSummary && (approachSummaries.chunked || approachSummaries.approximation)) {
    const oracle = loadFetchingOracleSummary({
      oracleRoot: args.oracleRoot,
      excludeRoot: resultRoot,
    });
    fetchingSummary = oracle.summary;
    oracleSourceRoot = oracle.oracleRoot;
    writeJson(path.join(resultRoot, "loaded_fetching_oracle.json"), {
      oracleSourceRoot,
      fetchingSummary,
    });
  }

  const approximationSummary = approachSummaries.approximation || null;
  const chunkedSummary = approachSummaries.chunked || null;
  const approximationChecks =
    fetchingSummary && approximationSummary
      ? compareAgainstFetching(fetchingSummary, approximationSummary, "approximation")
      : [];
  const chunkedChecks =
    fetchingSummary && chunkedSummary
      ? compareAgainstFetching(fetchingSummary, chunkedSummary, "chunked")
      : [];

  if (chunkedChecks.length > 0) {
    validateChunkedChecks(chunkedChecks);
  }

  const portStatus = checkPortStatus(CONTROL_PORT);
  const summary = {
    resultRoot,
    fallbackMode: "two-window",
    selectedApproaches: args.approaches,
    oracleSourceRoot,
    buildInfo,
    windowPlan,
    fetchingSummary,
    approximationSummary,
    chunkedSummary,
    approximationChecks,
    chunkedChecks,
    controlPortStatus: portStatus,
    orphanProcessStatus: {
      controlPortFree: portStatus.activePids.length === 0,
    },
  };
  writeJson(path.join(resultRoot, "preliminary_summary.json"), summary);

  console.log(JSON.stringify({
    resultRoot,
    fallbackMode: summary.fallbackMode,
    selectedApproaches: summary.selectedApproaches,
    fetchingDeliveries: fetchingSummary?.deliveries?.length ?? null,
    approximationDeliveries: approximationSummary?.deliveries?.length ?? null,
    chunkedDeliveries: chunkedSummary?.deliveries?.length ?? null,
    chunkedProducerCount: chunkedSummary?.latestProducerSnapshots?.length ?? null,
    controlPortFree: summary.orphanProcessStatus.controlPortFree,
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack || error.message : String(error));
    process.exit(1);
  });
}

module.exports = {
  ALL_APPROACHES,
  FINAL_QUERIES,
  FINAL_STEP_MS,
  buildWindowPlan,
  evaluateComparableDelivery,
  normalizeDelivery,
  parseArgs,
  parseResultPayload,
};
