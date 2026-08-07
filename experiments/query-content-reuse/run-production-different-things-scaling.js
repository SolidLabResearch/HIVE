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
const {
  ALIGNMENT_ORIGIN_MS,
  ALL_APPROACHES,
  CHUNK_RANGE_MS,
  CHUNK_STEP_MS,
  FLOAT_TOLERANCE,
  OUTPUT_RANGE_MS,
  OUTPUT_STEP_MS,
  PRELIMINARY_THING_COUNTS,
  buildFixture,
  buildProducerExpectations,
  buildScenarioMetrics,
  buildScenarioOracle,
  buildScenarioQueryDefinitions,
} = require("./different-things-scaling-common");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const CONTROL_PORT = 8080;
const BUILD_MANIFEST_NAME = "production-build-manifest.json";
const SERVER_EXECUTABLE_PATH = path.join(REPO_ROOT, "dist", "startHTTPServer.js");
const POLL_INTERVAL_MS = 250;
const STARTUP_TIMEOUT_MS = 30000;
const SUBSCRIBE_TIMEOUT_MS = 15000;
const DELIVERY_TIMEOUT_MS = 4 * 60 * 1000;
const ARTIFACT_SETTLE_MS = 2000;

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

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function buildResultRoot() {
  return path.join(
    REPO_ROOT,
    "results",
    "query-content-reuse",
    `production-different-things-scaling-${sanitizeTimestamp(new Date())}`,
  );
}

function getSourceFilesForBuildGuard() {
  const output = runCommand(
    "rg --files src experiments/query-content-reuse package.json tsconfig.json 2>/dev/null || true",
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

function parseArgs(argv) {
  const args = {
    approaches: [...ALL_APPROACHES],
    things: [...PRELIMINARY_THING_COUNTS],
    timeoutMs: DELIVERY_TIMEOUT_MS,
    baseAnchorMs: ALIGNMENT_ORIGIN_MS,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--approach":
      case "--approaches":
        args.approaches = String(next || "")
          .split(",")
          .map((entry) => entry.trim().toLowerCase())
          .filter(Boolean);
        index += 1;
        break;
      case "--things":
        args.things = String(next || "")
          .split(",")
          .map((entry) => Number.parseInt(entry.trim(), 10))
          .filter((value) => Number.isFinite(value));
        index += 1;
        break;
      case "--timeout-ms":
        args.timeoutMs = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--base-anchor-ms":
        args.baseAnchorMs = Number.parseInt(next || "", 10);
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
  if (args.things.length === 0) {
    throw new Error("At least one thing count must be selected");
  }
  for (const thingCount of args.things) {
    if (!Number.isInteger(thingCount) || thingCount <= 0 || thingCount > 10) {
      throw new Error(`Unsupported thing count: ${thingCount}`);
    }
  }
  return args;
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
    internalChunks: Array.isArray(parsed.internalChunks) ? parsed.internalChunks : [],
    internalChunkIds: Array.isArray(parsed.internalChunkIds) ? parsed.internalChunkIds : [],
    executionId: parsed.executionId || null,
    producerIdentityMappings: Array.isArray(parsed.producerIdentityMappings)
      ? parsed.producerIdentityMappings
      : [],
    requiredCanonicalProducerIds: Array.isArray(parsed.requiredCanonicalProducerIds)
      ? parsed.requiredCanonicalProducerIds
      : [],
    receivedCanonicalProducerIds: Array.isArray(parsed.receivedCanonicalProducerIds)
      ? parsed.receivedCanonicalProducerIds
      : [],
    missingCanonicalProducerIds: Array.isArray(parsed.missingCanonicalProducerIds)
      ? parsed.missingCanonicalProducerIds
      : [],
    requiredRuntimeProducerIds: Array.isArray(parsed.requiredRuntimeProducerIds)
      ? parsed.requiredRuntimeProducerIds
      : [],
    receivedRuntimeProducerIds: Array.isArray(parsed.receivedRuntimeProducerIds)
      ? parsed.receivedRuntimeProducerIds
      : [],
    missingRuntimeProducerIds: Array.isArray(parsed.missingRuntimeProducerIds)
      ? parsed.missingRuntimeProducerIds
      : [],
    latestWatermarkByRequiredRuntimeProducer:
      parsed.latestWatermarkByRequiredRuntimeProducer &&
      typeof parsed.latestWatermarkByRequiredRuntimeProducer === "object"
        ? parsed.latestWatermarkByRequiredRuntimeProducer
        : {},
    derivedReconstructionWatermark: toNumber(parsed.derivedReconstructionWatermark),
    requiredChunkContributions: toNumber(parsed.requiredChunkContributions),
    receivedChunkContributions: toNumber(parsed.receivedChunkContributions),
    missingContributionCount: toNumber(parsed.missingContributionCount),
    localProducerSpawnCount: toNumber(parsed.localProducerSpawnCount),
    managedProducerMode: parsed.managedProducerMode === true,
  };
}

function normalizeDelivery(approach, registration, parsed) {
  return {
    queryLabel: registration.queryLabel,
    includedThings: registration.includedThings,
    executionId: registration.executionId,
    outputTopic: registration.outputTopic,
    windowStart: parsed.windowStart,
    windowEnd: parsed.windowEnd,
    count:
      approach === "chunked" ? parsed.recomposedCount : parsed.eventCount,
    sum:
      approach === "chunked" ? parsed.recomposedSum : parsed.sumValue,
    average:
      approach === "chunked"
        ? parsed.recomposedAvg ?? parsed.value
        : parsed.avgValue ?? parsed.value,
    coverageComplete: parsed.coverageComplete,
    isComparableWindow: parsed.isComparableWindow,
    publicationTimestamp: parsed.publicationTimestamp,
    internalChunks: parsed.internalChunks,
    internalChunkIds: parsed.internalChunkIds,
    producerIdentityMappings: parsed.producerIdentityMappings,
    requiredCanonicalProducerIds: parsed.requiredCanonicalProducerIds,
    receivedCanonicalProducerIds: parsed.receivedCanonicalProducerIds,
    missingCanonicalProducerIds: parsed.missingCanonicalProducerIds,
    requiredRuntimeProducerIds: parsed.requiredRuntimeProducerIds,
    receivedRuntimeProducerIds: parsed.receivedRuntimeProducerIds,
    missingRuntimeProducerIds: parsed.missingRuntimeProducerIds,
    latestWatermarkByRequiredRuntimeProducer:
      parsed.latestWatermarkByRequiredRuntimeProducer,
    derivedReconstructionWatermark: parsed.derivedReconstructionWatermark,
    requiredChunkContributions: parsed.requiredChunkContributions,
    receivedChunkContributions: parsed.receivedChunkContributions,
    missingContributionCount: parsed.missingContributionCount,
    localProducerSpawnCount: parsed.localProducerSpawnCount,
    managedProducerMode: parsed.managedProducerMode,
    payload: parsed.raw,
  };
}

function evaluateComparableDelivery(registration, parsed) {
  if (parsed.isComparableWindow !== true) {
    return { accepted: false, reason: "isComparableWindow=false" };
  }
  if (parsed.coverageComplete !== true) {
    return { accepted: false, reason: "coverageComplete=false" };
  }
  if (registration.executionId && parsed.executionId && parsed.executionId !== registration.executionId) {
    return { accepted: false, reason: "executionId_mismatch" };
  }
  if (parsed.windowStart !== registration.expectedWindowStart) {
    return { accepted: false, reason: "windowStart_mismatch" };
  }
  if (parsed.windowEnd !== registration.expectedWindowEnd) {
    return { accepted: false, reason: "windowEnd_mismatch" };
  }
  if (parsed.rangeMs !== null && parsed.rangeMs !== registration.rangeMs) {
    return { accepted: false, reason: "rangeMs_mismatch" };
  }
  if (parsed.stepMs !== null && parsed.stepMs !== registration.stepMs) {
    return { accepted: false, reason: "stepMs_mismatch" };
  }
  return { accepted: true, reason: "accepted" };
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
    includedThings: requestBody.included_things,
    rangeMs: OUTPUT_RANGE_MS,
    stepMs: OUTPUT_STEP_MS,
    consumerId: payload.consumerId,
    canonicalQueryId: payload.canonicalQueryId,
    executionId: payload.executionId,
    executionCreated: payload.executionCreated,
    reuseHit: payload.reuseHit,
    outputTopic: payload.outputTopic,
    reuseDecision: payload.reuseDecision,
    producerSnapshots: payload.producerSnapshots || [],
    producerIdentityMappings: payload.producerIdentityMappings || [],
    workerIds: payload.workerIds || [],
    localProducerSpawnCount: payload.localProducerSpawnCount,
    managedProducerMode: payload.managedProducerMode,
  };
}

async function subscribeConsumers({ registrations, deliveryPath }) {
  const clients = [];
  const comparableByLabel = new Map();
  const deliveryDecisions = [];
  await Promise.all(
    registrations.map((registration) => new Promise((resolve, reject) => {
      const client = mqtt.connect("mqtt://localhost:1883", {
        clean: true,
        clientId: `${registration.consumerId}-${Math.random().toString(16).slice(2, 10)}`,
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
        appendNdjson(deliveryPath, delivery);
        deliveryDecisions.push({
          queryLabel: registration.queryLabel,
          executionId: registration.executionId,
          outputTopic: registration.outputTopic,
          windowStart: delivery.windowStart,
          windowEnd: delivery.windowEnd,
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
              approach,
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
  const totalCpu = samples.reduce(
    (sum, entry) => sum + (Number.isFinite(entry.cpuPct) ? entry.cpuPct : 0),
    0,
  );
  return {
    averageCpuPct: samples.length > 0 ? totalCpu / samples.length : null,
    peakCpuPct: samples.reduce((peak, entry) => Math.max(peak, entry.cpuPct || 0), 0),
    peakRssMb: samples.reduce((peak, entry) => Math.max(peak, entry.rssMb || 0), 0),
    peakProcessCount: samples.reduce((peak, entry) => Math.max(peak, entry.processCount || 0), 0),
    sampleCount: samples.length,
  };
}

function buildApproachEnv({
  approach,
  runRoot,
  replayAnchor,
  topicPrefix,
}) {
  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  return replayEnv.withBenchmarkReplayEnv({
    ...process.env,
    LOG_PATH: runRoot,
    SESSION_ID: `experiment2_${approach}_${path.basename(runRoot)}`,
    BENCHMARK_SCENARIO: "experiment2-production-different-things-scaling",
    BENCHMARK_APPROACH: approach,
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
    STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: String(replayAnchor),
    STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(replayAnchor),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(replayAnchor),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(replayAnchor + OUTPUT_RANGE_MS + OUTPUT_STEP_MS),
    RESULT_TOPIC: "benchmark-payload-enabled",
    OUTPUT_WINDOW_RANGE: String(OUTPUT_RANGE_MS),
    OUTPUT_WINDOW_STEP: String(OUTPUT_STEP_MS),
    SUB_WINDOW_RANGE: String(CHUNK_RANGE_MS),
    SUB_WINDOW_STEP: String(CHUNK_STEP_MS),
    STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "1",
    STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
    STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
    STREAMING_QUERY_HIVE_DEBUG_CHUNKS: "1",
    HIVE_WATERMARK_DEBUG: "true",
    HIVE_PROFILE: "1",
  });
}

function buildRegistrationBodies({ approach, thingCount, topicPrefix }) {
  return buildScenarioQueryDefinitions(thingCount, {
    topicPrefix,
    outputIriBuilder: (label) =>
      `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${label.toLowerCase()}`,
  }).map((queryDefinition) => ({
    id: `${approach}-${thingCount}-${queryDefinition.queryLabel.toLowerCase()}`,
    consumer_id: `${approach}-${thingCount}-${queryDefinition.queryLabel.toLowerCase()}-consumer`,
    query_label: queryDefinition.queryLabel,
    included_things: queryDefinition.includedThings,
    approach,
    rspql_query: queryDefinition.query,
    r2s_topic: `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${queryDefinition.queryLabel.toLowerCase()}`,
    data_topic: `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${queryDefinition.queryLabel.toLowerCase()}`,
    approximation_config:
      approach === "approximation"
        ? {
            policy: "rate-based-completed-window",
            completedWindowMode: true,
            earlyTriggerMode: false,
          }
        : undefined,
    expectedWindowStart: queryDefinition.expectedWindowStart,
    expectedWindowEnd: queryDefinition.expectedWindowEnd,
  }));
}

function validateRegistrationSet(approach, registrations) {
  assert(registrations.length > 0, `${approach}: missing registrations`);
  const executionIds = new Set(registrations.map((entry) => entry.executionId));
  const canonicalQueryIds = new Set(registrations.map((entry) => entry.canonicalQueryId));
  const outputTopics = new Set(registrations.map((entry) => entry.outputTopic));
  assert(
    executionIds.size === registrations.length,
    `${approach}: final queries shared one execution`,
  );
  assert(
    canonicalQueryIds.size === registrations.length,
    `${approach}: final queries shared one canonical final-query ID`,
  );
  assert(
    outputTopics.size === registrations.length,
    `${approach}: final MQTT topics are not distinct`,
  );
  for (const registration of registrations) {
    assert(registration.status === 200, `${approach}/${registration.queryLabel}: register failed`);
    assert(registration.executionCreated === true, `${approach}/${registration.queryLabel}: executionCreated=false`);
    assert(registration.reuseHit === false, `${approach}/${registration.queryLabel}: reuseHit=true`);
    assert(
      registration.reuseDecision?.reuseHit === false,
      `${approach}/${registration.queryLabel}: reuseDecision.reuseHit=true`,
    );
    if (approach !== "fetching") {
      validateProducerIdentityMappings(
        registration.producerIdentityMappings,
        `${approach}/${registration.queryLabel}: producerIdentityMappings`,
      );
    }
    if (approach === "chunked") {
      assert(registration.localProducerSpawnCount === 0, `${approach}/${registration.queryLabel}: localProducerSpawnCount must be 0`);
      assert(registration.managedProducerMode === true, `${approach}/${registration.queryLabel}: managedProducerMode must be true`);
    }
  }
}

function validateWatermarkSentinels(fixture, thingCount) {
  return fixture.things.slice(0, thingCount).map((thing) => {
    const sentinel = thing.watermarkSentinel;
    assert(sentinel?.isWatermarkSentinel === true, `${thing.thingName}: watermark sentinel is not marked`);
    assert(sentinel.sentinelStream === thing.thingName, `${thing.thingName}: sentinel stream mismatch`);
    assert(sentinel.sentinelExcludedFromOracle === true, `${thing.thingName}: sentinel is not excluded from oracle`);
    assert(
      sentinel.sentinelTimestamp < fixture.windowStart || sentinel.sentinelTimestamp >= fixture.windowEnd,
      `${thing.thingName}: watermark sentinel entered half-open target interval`,
    );
    return {
      isWatermarkSentinel: true,
      sentinelTimestamp: sentinel.sentinelTimestamp,
      sentinelStream: sentinel.sentinelStream,
      sentinelExcludedFromOracle: true,
      targetWindowStart: fixture.windowStart,
      targetWindowEnd: fixture.windowEnd,
    };
  });
}

function buildOrderedPublishEvents({ fixture, thingCount, topicPrefix }) {
  const selectedThings = fixture.things.slice(0, thingCount);
  const normalizedPrefix = String(topicPrefix || "").trim().replace(/^\/+|\/+$/g, "");
  const events = [];
  for (const thing of selectedThings) {
    for (const event of thing.events) {
      events.push({
        thingName: thing.thingName,
        topic: normalizedPrefix ? `${normalizedPrefix}/${thing.topicName}` : thing.topicName,
        payload: event.payload,
        offsetMs: event.offsetMs,
      });
    }

    events.push({
      thingName: thing.thingName,
      topic: normalizedPrefix ? `${normalizedPrefix}/${thing.topicName}` : thing.topicName,
      payload: thing.watermarkSentinel.payload,
      offsetMs: thing.watermarkSentinel.offsetMs,
      isWatermarkSentinel: true,
      sentinelTimestamp: thing.watermarkSentinel.sentinelTimestamp,
      sentinelStream: thing.watermarkSentinel.sentinelStream,
      sentinelExcludedFromOracle: true,
    });
  }
  events.sort((left, right) => left.offsetMs - right.offsetMs);
  return events;
}

async function connectMqttClient(clientId) {
  const client = mqtt.connect("mqtt://localhost:1883", {
    clean: true,
    clientId,
  });
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timed out connecting MQTT client ${clientId}`));
    }, 10000);
    client.on("connect", () => {
      clearTimeout(timeout);
      resolve();
    });
    client.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
  return client;
}

async function publishMessage(client, topic, payload) {
  await new Promise((resolve, reject) => {
    client.publish(topic, payload, { qos: 1, retain: false }, (error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

async function publishFixture({
  fixture,
  thingCount,
  topicPrefix,
  publishEventsPath,
}) {
  const events = buildOrderedPublishEvents({ fixture, thingCount, topicPrefix });
  const client = await connectMqttClient(`experiment2-publisher-${Date.now().toString(16)}`);
  const startTime = Date.now();
  try {
    for (const event of events) {
      const remainingMs = event.offsetMs - (Date.now() - startTime);
      if (remainingMs > 0) {
        await delay(remainingMs);
      }
      await publishMessage(client, event.topic, event.payload);
      appendNdjson(publishEventsPath, {
        thingName: event.thingName,
        topic: event.topic,
        offsetMs: event.offsetMs,
        publishedAt: Date.now(),
        isWatermarkSentinel: event.isWatermarkSentinel === true,
        sentinelTimestamp: event.sentinelTimestamp ?? null,
        sentinelStream: event.sentinelStream ?? null,
        sentinelExcludedFromOracle: event.sentinelExcludedFromOracle === true,
      });
    }
    await delay(250);
    await publishMessage(
      client,
      `${topicPrefix}/__benchmark_control__`,
      JSON.stringify({
        type: "finite_replay_complete",
        source: "experiment2-runner",
        topicPrefix,
        timestamp: Date.now(),
      }),
    );
  } finally {
    await new Promise((resolve) => client.end(true, resolve));
  }
}

function sortedUniqueStrings(values, context) {
  assert(Array.isArray(values), `${context}: expected an array`);
  assert(values.every((value) => typeof value === "string" && value.length > 0), `${context}: expected non-empty string IDs`);
  const unique = [...new Set(values)].sort();
  assert(unique.length === values.length, `${context}: duplicate IDs`);
  return unique;
}

function assertSameStringSet(actual, expected, context) {
  const normalizedActual = sortedUniqueStrings(actual, `${context} actual`);
  const normalizedExpected = sortedUniqueStrings(expected, `${context} expected`);
  assert(
    JSON.stringify(normalizedActual) === JSON.stringify(normalizedExpected),
    `${context}: actual=${JSON.stringify(normalizedActual)} expected=${JSON.stringify(normalizedExpected)}`,
  );
}

function validateProducerIdentityMappings(mappings, context = "producerIdentityMappings") {
  assert(Array.isArray(mappings), `${context}: expected an array`);
  const canonicalIds = new Set();
  const runtimeIds = new Set();
  const topics = new Set();
  return mappings.map((mapping, index) => {
    const prefix = `${context}[${index}]`;
    assert(mapping && typeof mapping === "object", `${prefix}: expected an object`);
    for (const field of ["canonicalProducerId", "runtimeProducerId", "topic"]) {
      assert(typeof mapping[field] === "string" && mapping[field].length > 0, `${prefix}: missing ${field}`);
    }
    assert(!canonicalIds.has(mapping.canonicalProducerId), `${context}: duplicate canonicalProducerId ${mapping.canonicalProducerId}`);
    assert(!runtimeIds.has(mapping.runtimeProducerId), `${context}: duplicate runtimeProducerId ${mapping.runtimeProducerId}`);
    assert(!topics.has(mapping.topic), `${context}: duplicate topic ${mapping.topic}`);
    canonicalIds.add(mapping.canonicalProducerId);
    runtimeIds.add(mapping.runtimeProducerId);
    topics.add(mapping.topic);
    return {
      canonicalProducerId: mapping.canonicalProducerId,
      runtimeProducerId: mapping.runtimeProducerId,
      topic: mapping.topic,
    };
  });
}

function buildNestedDependencyTopology({ registrations, thingCount }) {
  assert(registrations.length === thingCount, `expected ${thingCount} registrations, got ${registrations.length}`);
  let previousMappings = [];
  let previousThings = [];
  const nodes = [];

  for (const [index, registration] of registrations.entries()) {
    const context = `${registration.queryLabel}: producerIdentityMappings`;
    const mappings = validateProducerIdentityMappings(registration.producerIdentityMappings, context);
    assert(mappings.length === index + 1, `${registration.queryLabel}: expected ${index + 1} mappings, got ${mappings.length}`);

    const previousByCanonical = new Map(
      previousMappings.map((mapping) => [mapping.canonicalProducerId, mapping]),
    );
    for (const mapping of mappings) {
      const previous = previousByCanonical.get(mapping.canonicalProducerId);
      if (previous) {
        assert(
          previous.runtimeProducerId === mapping.runtimeProducerId && previous.topic === mapping.topic,
          `${registration.queryLabel}: canonical producer ${mapping.canonicalProducerId} changed runtime identity or topic`,
        );
      }
    }
    const addedMappings = mappings.filter(
      (mapping) => !previousByCanonical.has(mapping.canonicalProducerId),
    );
    assert(addedMappings.length === 1, `${registration.queryLabel}: expected exactly one producer mapping set difference`);

    const currentThings = sortedUniqueStrings(registration.includedThings, `${registration.queryLabel}: includedThings`);
    const previousThingSet = new Set(previousThings);
    const addedThings = currentThings.filter((thingName) => !previousThingSet.has(thingName));
    assert(addedThings.length === 1, `${registration.queryLabel}: expected exactly one included thing set difference`);
    assert(
      previousThings.every((thingName) => currentThings.includes(thingName)),
      `${registration.queryLabel}: nested thing dependency was removed`,
    );

    nodes.push({
      registrationIndex: index,
      queryLabel: registration.queryLabel,
      thingName: addedThings[0],
      ...addedMappings[0],
    });
    registration.producerIdentityMappings = mappings;
    previousMappings = mappings;
    previousThings = currentThings;
  }

  return {
    nodes,
    finalProducerIdentityMappings: previousMappings,
  };
}

function buildProducerReferenceRows({ producerSnapshots, registrations, dependencyTopology }) {
  const snapshots = Array.isArray(producerSnapshots) ? producerSnapshots : [];
  const snapshotByCanonical = new Map();
  for (const snapshot of snapshots) {
    assert(typeof snapshot.canonicalProducerId === "string", "producer snapshot missing canonicalProducerId");
    assert(typeof snapshot.runtimeProducerId === "string", "producer snapshot missing runtimeProducerId");
    assert(!snapshotByCanonical.has(snapshot.canonicalProducerId), `duplicate producer snapshot ${snapshot.canonicalProducerId}`);
    snapshotByCanonical.set(snapshot.canonicalProducerId, snapshot);
  }

  return dependencyTopology.nodes.map((node) => {
    const snapshot = snapshotByCanonical.get(node.canonicalProducerId);
    assert(snapshot, `missing producer snapshot for canonical producer ${node.canonicalProducerId}`);
    assert(snapshot.runtimeProducerId === node.runtimeProducerId, `${node.thingName}: snapshot runtimeProducerId mismatch`);
    assert((snapshot.topic || snapshot.producerTopic || snapshot.outputTopic) === node.topic, `${node.thingName}: snapshot topic mismatch`);
    const dependentRegistrations = registrations.filter((registration) =>
      registration.producerIdentityMappings.some(
        (mapping) => mapping.canonicalProducerId === node.canonicalProducerId,
      ),
    );
    return {
      thingName: node.thingName,
      canonicalProducerId: node.canonicalProducerId,
      runtimeProducerId: node.runtimeProducerId,
      producerTopic: node.topic,
      pid: snapshot.pid ?? null,
      parentPid: snapshot.parentPid ?? null,
      processCommandLine: snapshot.processCommandLine ?? null,
      state: snapshot.state,
      referenceCount: snapshot.referenceCount,
      dependentExecutionIds: dependentRegistrations.map((registration) => registration.executionId),
      dependentQueryLabels: dependentRegistrations.map((registration) => registration.queryLabel),
    };
  });
}

function validateProducerTopology({ thingCount, registrations, latestProducerSnapshots }) {
  const dependencyTopology = buildNestedDependencyTopology({ registrations, thingCount });
  assert(latestProducerSnapshots.length === thingCount, `expected ${thingCount} unique producers, got ${latestProducerSnapshots.length}`);
  const rows = buildProducerReferenceRows({
    producerSnapshots: latestProducerSnapshots,
    registrations,
    dependencyTopology,
  });
  const expectations = buildProducerExpectations(thingCount);
  for (const expectation of expectations) {
    const row = rows.find((entry) => entry.thingName === expectation.thingName);
    assert(row, `missing producer topology node for ${expectation.thingName}`);
    assert(row.referenceCount === expectation.expectedReferenceCount, `${expectation.thingName}: referenceCount=${row.referenceCount} expected=${expectation.expectedReferenceCount}`);
    assertSameStringSet(row.dependentQueryLabels, expectation.dependentQueryLabels, `${expectation.thingName}: dependent queries`);
  }
  return { dependencyTopology, producerReferenceRows: rows };
}

function buildApproachScenarioMetrics(approach, thingCount) {
  const metrics = buildScenarioMetrics(thingCount);
  if (approach === "fetching") {
    return {
      ...metrics,
      uniqueProducers: 0,
      reusedProducerAcquisitions: 0,
      producerReusePercentage: 0,
    };
  }
  return metrics;
}

function compareToOracle(approach, deliveries, oracleRows) {
  const oracleByLabel = new Map(oracleRows.map((row) => [row.queryLabel, row]));
  return deliveries.map((delivery) => {
    const oracle = oracleByLabel.get(delivery.queryLabel);
    if (!oracle) {
      throw new Error(`missing oracle row for ${delivery.queryLabel}`);
    }
    const absoluteError = Math.abs((delivery.average ?? 0) - oracle.average);
    const percentageError =
      Math.abs(oracle.average) > 1e-12
        ? (absoluteError / Math.abs(oracle.average)) * 100
        : 0;
    const countError =
      delivery.count !== null && Number.isFinite(oracle.count)
        ? Math.abs(delivery.count - oracle.count)
        : null;
    const sumError =
      delivery.sum !== null && Number.isFinite(oracle.sum)
        ? Math.abs(delivery.sum - oracle.sum)
        : null;
    return {
      approach,
      queryLabel: delivery.queryLabel,
      oracle,
      delivery,
      absoluteError,
      percentageError,
      countError,
      sumError,
    };
  });
}

function validateExactApproach(approach, checks) {
  for (const check of checks) {
    assert(
      check.delivery.windowStart === check.oracle.windowStart,
      `${approach}/${check.queryLabel}: windowStart mismatch`,
    );
    assert(
      check.delivery.windowEnd === check.oracle.windowEnd,
      `${approach}/${check.queryLabel}: windowEnd mismatch`,
    );
    assert(
      (check.countError ?? Infinity) <= FLOAT_TOLERANCE,
      `${approach}/${check.queryLabel}: count mismatch`,
    );
    assert(
      (check.sumError ?? Infinity) <= FLOAT_TOLERANCE,
      `${approach}/${check.queryLabel}: sum mismatch ${check.sumError}`,
    );
    assert(
      check.absoluteError <= FLOAT_TOLERANCE,
      `${approach}/${check.queryLabel}: avg mismatch ${check.absoluteError}`,
    );
  }
}

function validateDeliveryProducerIdentities(delivery, registration) {
  const mappings = validateProducerIdentityMappings(
    delivery.producerIdentityMappings,
    `${delivery.queryLabel}: delivery producerIdentityMappings`,
  );
  const expectedMappings = validateProducerIdentityMappings(
    registration.producerIdentityMappings,
    `${delivery.queryLabel}: registration producerIdentityMappings`,
  );
  const expectedCanonicalIds = expectedMappings.map((mapping) => mapping.canonicalProducerId);
  const expectedRuntimeIds = expectedMappings.map((mapping) => mapping.runtimeProducerId);
  assertSameStringSet(
    mappings.map((mapping) => mapping.canonicalProducerId),
    expectedCanonicalIds,
    `${delivery.queryLabel}: mapped canonical IDs`,
  );
  for (const mapping of mappings) {
    const expected = expectedMappings.find(
      (candidate) => candidate.canonicalProducerId === mapping.canonicalProducerId,
    );
    assert(
      expected.runtimeProducerId === mapping.runtimeProducerId && expected.topic === mapping.topic,
      `${delivery.queryLabel}: canonical/runtime/topic mapping mismatch for ${mapping.canonicalProducerId}`,
    );
  }
  assertSameStringSet(delivery.requiredCanonicalProducerIds, expectedCanonicalIds, `${delivery.queryLabel}: required canonical IDs`);
  assertSameStringSet(delivery.receivedCanonicalProducerIds, expectedCanonicalIds, `${delivery.queryLabel}: received canonical IDs`);
  assertSameStringSet(delivery.missingCanonicalProducerIds, [], `${delivery.queryLabel}: missing canonical IDs`);
  assertSameStringSet(delivery.requiredRuntimeProducerIds, expectedRuntimeIds, `${delivery.queryLabel}: required runtime IDs`);
  assertSameStringSet(delivery.receivedRuntimeProducerIds, expectedRuntimeIds, `${delivery.queryLabel}: received runtime IDs`);
  assertSameStringSet(delivery.missingRuntimeProducerIds, [], `${delivery.queryLabel}: missing runtime IDs`);
  assert(delivery.localProducerSpawnCount === 0, `${delivery.queryLabel}: localProducerSpawnCount must be 0`);
  assert(delivery.managedProducerMode === true, `${delivery.queryLabel}: managedProducerMode must be true`);
  return { mappings, expectedCanonicalIds, expectedRuntimeIds };
}

function buildChunkedContributionEvidence({ deliveries, producerRows, registrations }) {
  const fixture = buildFixture();
  const oracleByThing = new Map(
    fixture.things.map((thing) => [thing.thingName, { count: thing.oracle.count, sum: thing.oracle.sum }]),
  );
  const producerByRuntimeId = new Map(
    producerRows.map((row) => [row.runtimeProducerId, row]),
  );
  const registrationByLabel = new Map(
    registrations.map((registration) => [registration.queryLabel, registration]),
  );

  return deliveries.map((delivery) => {
    const registration = registrationByLabel.get(delivery.queryLabel);
    assert(registration, `${delivery.queryLabel}: missing registration`);
    const identity = validateDeliveryProducerIdentities(delivery, registration);
    const receivedRuntimeIds = new Set();
    const receivedChunkIdsByRuntimeProducer = {};

    for (const chunk of delivery.internalChunks) {
      const chunkIdentities = Array.isArray(chunk.producerIdentities)
        ? chunk.producerIdentities
        : [];
      assert(chunkIdentities.length > 0, `${delivery.queryLabel}: chunk omitted producerIdentities`);
      for (const chunkIdentity of chunkIdentities) {
        const mapping = identity.mappings.find(
          (candidate) => candidate.runtimeProducerId === chunkIdentity.runtimeProducerId,
        );
        assert(mapping, `${delivery.queryLabel}: unknown runtimeProducerId ${chunkIdentity.runtimeProducerId}`);
        assert(
          mapping.canonicalProducerId === chunkIdentity.canonicalProducerId,
          `${delivery.queryLabel}: mapped canonical/runtime mismatch for ${chunkIdentity.runtimeProducerId}`,
        );
        receivedRuntimeIds.add(mapping.runtimeProducerId);
        receivedChunkIdsByRuntimeProducer[mapping.runtimeProducerId] = [
          ...(receivedChunkIdsByRuntimeProducer[mapping.runtimeProducerId] || []),
          ...(chunk.receivedChunkIdsBySubquery?.[mapping.runtimeProducerId] || []),
        ];
      }
    }

    assertSameStringSet([...receivedRuntimeIds], identity.expectedRuntimeIds, `${delivery.queryLabel}: chunk runtime IDs`);
    const contributionByRuntimeProducer = {};
    for (const runtimeProducerId of identity.expectedRuntimeIds) {
      const producer = producerByRuntimeId.get(runtimeProducerId);
      assert(producer, `${delivery.queryLabel}: unknown topology runtimeProducerId ${runtimeProducerId}`);
      const oracleContribution = oracleByThing.get(producer.thingName);
      assert(oracleContribution, `${delivery.queryLabel}: missing oracle for ${producer.thingName}`);
      contributionByRuntimeProducer[runtimeProducerId] = {
        thingName: producer.thingName,
        canonicalProducerId: producer.canonicalProducerId,
        count: oracleContribution.count,
        sum: oracleContribution.sum,
        receivedChunkIds: [...new Set(receivedChunkIdsByRuntimeProducer[runtimeProducerId] || [])],
      };
    }
    return {
      queryLabel: delivery.queryLabel,
      producerIdentityMappings: identity.mappings,
      requiredCanonicalProducerIds: delivery.requiredCanonicalProducerIds,
      receivedCanonicalProducerIds: delivery.receivedCanonicalProducerIds,
      missingCanonicalProducerIds: delivery.missingCanonicalProducerIds,
      requiredRuntimeProducerIds: delivery.requiredRuntimeProducerIds,
      receivedRuntimeProducerIds: delivery.receivedRuntimeProducerIds,
      missingRuntimeProducerIds: delivery.missingRuntimeProducerIds,
      expectedThingNames: registration.includedThings,
      actualThingNames: Object.values(contributionByRuntimeProducer).map((entry) => entry.thingName),
      countContributionPerRuntimeProducer: Object.fromEntries(
        Object.entries(contributionByRuntimeProducer).map(([runtimeId, entry]) => [runtimeId, entry.count]),
      ),
      sumContributionPerRuntimeProducer: Object.fromEntries(
        Object.entries(contributionByRuntimeProducer).map(([runtimeId, entry]) => [runtimeId, entry.sum]),
      ),
      receivedChunkIdsByRuntimeProducer: Object.fromEntries(
        Object.entries(contributionByRuntimeProducer).map(([runtimeId, entry]) => [runtimeId, entry.receivedChunkIds]),
      ),
      combinedCount: delivery.count,
      combinedSum: delivery.sum,
    };
  });
}

function validateChunkedContributionEvidence(evidenceRows) {
  for (const row of evidenceRows) {
    assertSameStringSet(row.actualThingNames, row.expectedThingNames, `${row.queryLabel}: actual thing producer set`);
    for (const runtimeProducerId of row.requiredRuntimeProducerIds) {
      const chunkIds = row.receivedChunkIdsByRuntimeProducer[runtimeProducerId] || [];
      assert(chunkIds.length === 2, `${row.queryLabel}: runtime producer ${runtimeProducerId} did not contribute exactly two chunks`);
      assert(
        new Set(chunkIds).size === chunkIds.length,
        `${row.queryLabel}: runtime producer ${runtimeProducerId} duplicated one chunk`,
      );
    }
    const expectedCount = Object.values(row.countContributionPerRuntimeProducer).reduce((sum, value) => sum + value, 0);
    const expectedSum = Object.values(row.sumContributionPerRuntimeProducer).reduce((sum, value) => sum + value, 0);
    assert(row.combinedCount === expectedCount, `${row.queryLabel}: combined count is not the producer count sum`);
    assert(Math.abs(row.combinedSum - expectedSum) <= FLOAT_TOLERANCE, `${row.queryLabel}: combined sum is not the producer sum`);
  }
}

function parseProcessRows(psOutput) {
  return String(psOutput || "")
    .split(/\r?\n/)
    .map((line) => line.match(/^\s*(\d+)\s+(\d+)\s+(.+)$/))
    .filter(Boolean)
    .map((match) => ({
      pid: Number(match[1]),
      ppid: Number(match[2]),
      command: match[3],
    }));
}

function classifyProcessTopology({ processRows, serverPid, producerSnapshots = [], reconstructionWorkerIds = [] }) {
  assert(Number.isInteger(serverPid) && serverPid > 0, "authoritative server PID is required");
  const rowsByPid = new Map(processRows.map((row) => [Number(row.pid), {
    pid: Number(row.pid),
    ppid: Number(row.ppid),
    command: String(row.command || ""),
  }]));
  const producerByPid = new Map();
  for (const snapshot of producerSnapshots) {
    if (Number.isInteger(Number(snapshot.pid)) && Number(snapshot.pid) > 0) {
      producerByPid.set(Number(snapshot.pid), snapshot);
    }
  }
  const numericWorkerIds = reconstructionWorkerIds
    .map((workerId) => Number(workerId))
    .filter((workerId) => Number.isInteger(workerId) && workerId > 0);
  const workerPidSet = new Set(numericWorkerIds);
  const relatedPids = new Set([serverPid, ...producerByPid.keys(), ...workerPidSet]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const row of rowsByPid.values()) {
      if (relatedPids.has(row.ppid) && !relatedPids.has(row.pid)) {
        relatedPids.add(row.pid);
        changed = true;
      }
    }
  }

  const processes = [...relatedPids]
    .map((pid) => {
      const row = rowsByPid.get(pid);
      const producer = producerByPid.get(pid);
      let classification = "descendant";
      if (pid === serverPid) classification = "server";
      else if (producer) classification = "managed_producer";
      else if (workerPidSet.has(pid)) classification = "reconstruction_worker";
      return {
        pid,
        ppid: row?.ppid ?? null,
        command: row?.command ?? null,
        classification,
        alive: Boolean(row),
        canonicalProducerId: producer?.canonicalProducerId ?? null,
        runtimeProducerId: producer?.runtimeProducerId ?? null,
      };
    })
    .sort((left, right) => left.pid - right.pid);
  return {
    authoritativeServerPid: serverPid,
    producerSnapshotPids: [...producerByPid.keys()].sort((left, right) => left - right),
    reconstructionWorkerIds: reconstructionWorkerIds.map(String),
    unresolvedReconstructionWorkerIds: reconstructionWorkerIds
      .map(String)
      .filter((workerId) => !Number.isInteger(Number(workerId)) || Number(workerId) <= 0),
    processes,
    edges: processes
      .filter((process) => process.ppid !== null && relatedPids.has(process.ppid))
      .map((process) => ({ parentPid: process.ppid, childPid: process.pid })),
  };
}

function captureProcessTopology({ serverPid, producerSnapshots, reconstructionWorkerIds }) {
  return {
    capturedAt: Date.now(),
    ...classifyProcessTopology({
      processRows: parseProcessRows(runCommand("ps -axo pid=,ppid=,command=")),
      serverPid,
      producerSnapshots,
      reconstructionWorkerIds,
    }),
  };
}

function validateChunkedWatermarkAndCoverage(deliveries, registrations) {
  const registrationByLabel = new Map(
    registrations.map((registration) => [registration.queryLabel, registration]),
  );
  return deliveries.map((delivery) => {
    const registration = registrationByLabel.get(delivery.queryLabel);
    assert(registration, `${delivery.queryLabel}: missing registration`);
    const identity = validateDeliveryProducerIdentities(delivery, registration);
    const latestWatermarks = delivery.latestWatermarkByRequiredRuntimeProducer;
    assertSameStringSet(
      Object.keys(latestWatermarks),
      identity.expectedRuntimeIds,
      `${delivery.queryLabel}: latest runtime watermark IDs`,
    );
    const watermarkValues = identity.expectedRuntimeIds.map(
      (runtimeProducerId) => latestWatermarks[runtimeProducerId],
    );
    assert(watermarkValues.every(Number.isFinite), `${delivery.queryLabel}: missing latest runtime producer watermark`);
    const expectedReconstructionWatermark = Math.min(...watermarkValues);
    assert(
      delivery.derivedReconstructionWatermark === expectedReconstructionWatermark,
      `${delivery.queryLabel}: reconstruction watermark is not the minimum runtime producer watermark`,
    );
    assert(delivery.coverageComplete === true, `${delivery.queryLabel}: coverageComplete=false`);
    assert(delivery.isComparableWindow === true, `${delivery.queryLabel}: isComparableWindow=false`);
    assert(delivery.missingContributionCount === 0, `${delivery.queryLabel}: missing contributions`);
    assert(
      delivery.requiredChunkContributions === delivery.receivedChunkContributions,
      `${delivery.queryLabel}: received/required contribution mismatch`,
    );
    return {
      queryLabel: delivery.queryLabel,
      producerIdentityMappings: identity.mappings,
      requiredCanonicalProducerIds: delivery.requiredCanonicalProducerIds,
      receivedCanonicalProducerIds: delivery.receivedCanonicalProducerIds,
      missingCanonicalProducerIds: delivery.missingCanonicalProducerIds,
      requiredRuntimeProducerIds: delivery.requiredRuntimeProducerIds,
      receivedRuntimeProducerIds: delivery.receivedRuntimeProducerIds,
      missingRuntimeProducerIds: delivery.missingRuntimeProducerIds,
      latestWatermarkByRequiredRuntimeProducer: latestWatermarks,
      derivedReconstructionWatermark: delivery.derivedReconstructionWatermark,
      requiredChunkContributions: delivery.requiredChunkContributions,
      receivedChunkContributions: delivery.receivedChunkContributions,
      missingContributionCount: delivery.missingContributionCount,
      coverageComplete: delivery.coverageComplete,
      localProducerSpawnCount: delivery.localProducerSpawnCount,
      managedProducerMode: delivery.managedProducerMode,
    };
  });
}

async function runApproach({
  approach,
  thingCount,
  fixture,
  scenarioRoot,
  deliveryTimeoutMs,
  expectedBuildManifest,
}) {
  const runRoot = path.join(scenarioRoot, approach);
  ensureDir(runRoot);
  const topicPrefix = `experiment2/${path.basename(scenarioRoot)}/n${thingCount}`;
  const registrationPath = path.join(runRoot, "registration_events.ndjson");
  const deliveryPath = path.join(runRoot, "consumer_delivery_events.ndjson");
  const publishEventsPath = path.join(runRoot, "published_fixture_events.ndjson");
  const serverOutPath = path.join(runRoot, "server.stdout.log");
  const serverErrPath = path.join(runRoot, "server.stderr.log");
  const resourcePath = path.join(runRoot, "process_tree_resource_usage.csv");
  const processTopologyPath = path.join(runRoot, "process_topology.json");
  const processLifecyclePath = path.join(runRoot, "process_lifecycle_evidence.json");
  const sentinelVerification = validateWatermarkSentinels(fixture, thingCount);
  const env = buildApproachEnv({
    approach,
    runRoot,
    replayAnchor: fixture.anchorMs,
    topicPrefix,
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
  let subscribers = null;
  const registrations = [];
  let latestProducerSnapshots = [];
  let reconstructionWorkerIds = [];
  const lifecycleEvidence = [];
  const processTopologyCaptures = [];
  try {
    await waitForServerLog(serverOutPath);
    await waitForRegisterReady(CONTROL_PORT);
    resourceSampler = startProcessTreeResourceLogging(resourcePath, server.pid, 200);

    for (const requestBody of buildRegistrationBodies({ approach, thingCount, topicPrefix })) {
      const registration = await postRegistration(CONTROL_PORT, requestBody);
      registration.expectedWindowStart = requestBody.expectedWindowStart;
      registration.expectedWindowEnd = requestBody.expectedWindowEnd;
      appendNdjson(registrationPath, registration);
      registrations.push(registration);
    }
    validateRegistrationSet(approach, registrations);
    latestProducerSnapshots = registrations.at(-1)?.producerSnapshots || [];
    reconstructionWorkerIds = [...new Set(registrations.flatMap((registration) => registration.workerIds || []))];
    const registrationProcessTopology = captureProcessTopology({
      serverPid: server.pid,
      producerSnapshots: latestProducerSnapshots,
      reconstructionWorkerIds,
    });
    processTopologyCaptures.push({ phase: "registrations_complete", ...registrationProcessTopology });
    lifecycleEvidence.push({
      phase: "registrations_complete",
      capturedAt: registrationProcessTopology.capturedAt,
      serverPid: server.pid,
      serverAlive: registrationProcessTopology.processes.some((process) => process.pid === server.pid && process.alive),
      producerPidsAlive: registrationProcessTopology.processes
        .filter((process) => process.classification === "managed_producer" && process.alive)
        .map((process) => process.pid),
      reconstructionWorkerPidsAlive: registrationProcessTopology.processes
        .filter((process) => process.classification === "reconstruction_worker" && process.alive)
        .map((process) => process.pid),
    });
    assert(
      registrationProcessTopology.processes.some((process) => process.pid === server.pid && process.alive),
      `authoritative server PID ${server.pid} was not alive during capture`,
    );
    if (approach !== "fetching") {
      assert(
        latestProducerSnapshots.every((snapshot) =>
          registrationProcessTopology.processes.some(
            (process) => process.pid === Number(snapshot.pid) && process.alive,
          ),
        ),
        `${approach}: one or more producer snapshot PIDs were not alive during capture`,
      );
    }
    assert(
      registrationProcessTopology.processes
        .filter((process) => process.classification === "reconstruction_worker")
        .every((process) => process.alive),
      `${approach}: one or more exposed reconstruction worker PIDs were not alive during capture`,
    );
    writeJson(processTopologyPath, { captures: processTopologyCaptures });
    writeJson(processLifecyclePath, lifecycleEvidence);

    subscribers = await subscribeConsumers({
      registrations,
      deliveryPath,
    });

    await publishFixture({
      fixture,
      thingCount,
      topicPrefix,
      publishEventsPath,
    });

    const deliveries = await subscribers.waitForComparableDeliveries(
      deliveryTimeoutMs,
      approach,
    );

    await delay(ARTIFACT_SETTLE_MS);
    const resourceMetrics = parseResourceMetrics(resourcePath);
    let dependencyTopology = {
      nodes: [],
      finalProducerIdentityMappings: [],
    };
    let producerReferenceRows = [];
    if (approach !== "fetching") {
      const validatedTopology = validateProducerTopology({
        thingCount,
        registrations,
        latestProducerSnapshots,
      });
      dependencyTopology = validatedTopology.dependencyTopology;
      producerReferenceRows = validatedTopology.producerReferenceRows;
    }
    const oracleRows = buildScenarioOracle(thingCount, fixture);
    const checks = compareToOracle(approach, deliveries, oracleRows);
    if (approach === "fetching" || approach === "chunked") {
      validateExactApproach(approach, checks);
      assert(
        checks.every((check) => check.countError === 0 && check.sumError <= FLOAT_TOLERANCE),
        `${approach}: watermark sentinel entered target count or sum`,
      );
    }
    const chunkedContributionEvidence =
      approach === "chunked"
        ? buildChunkedContributionEvidence({
            deliveries,
            producerRows: producerReferenceRows,
            registrations,
          })
        : [];
    const watermarkAndCoverage =
      approach === "chunked"
        ? validateChunkedWatermarkAndCoverage(deliveries, registrations)
        : [];
    if (approach === "chunked") {
      validateChunkedContributionEvidence(chunkedContributionEvidence);
    }
    const finalQueryLabel = registrations.at(-1)?.queryLabel;
    const finalProducerIdentityEvidence = approach === "chunked"
      ? watermarkAndCoverage.find((entry) => entry.queryLabel === finalQueryLabel)
      : null;
    if (approach === "chunked") {
      assert(finalProducerIdentityEvidence, `${approach}: missing final producer identity evidence`);
    }

    const summary = {
      approach,
      thingCount,
      runRoot,
      topicPrefix,
      registrations,
      deliveries,
      oracleRows,
      checks,
      dependencyTopology,
      finalProducerIdentityMappings: dependencyTopology.finalProducerIdentityMappings,
      finalProducerIdentityEvidence,
      producerReferenceRows,
      managedModeEvidence: approach === "chunked"
        ? {
            localProducerSpawnCount: 0,
            managedProducerMode: true,
            registrationEvidence: registrations.map((registration) => ({
              queryLabel: registration.queryLabel,
              localProducerSpawnCount: registration.localProducerSpawnCount,
              managedProducerMode: registration.managedProducerMode,
            })),
            deliveryEvidence: deliveries.map((delivery) => ({
              queryLabel: delivery.queryLabel,
              localProducerSpawnCount: delivery.localProducerSpawnCount,
              managedProducerMode: delivery.managedProducerMode,
            })),
          }
        : null,
      processTopologyPath,
      processLifecyclePath,
      chunkedContributionEvidence,
      watermarkAndCoverage,
      sentinelVerification: sentinelVerification.map((entry) => ({
        ...entry,
        sentinelExcludedFromTargetResult: true,
      })),
      resourceMetrics,
      scenarioMetrics: buildApproachScenarioMetrics(approach, thingCount),
      deliveryDecisions: subscribers.getDeliveryDecisions(),
    };
    writeJson(path.join(runRoot, "approach_summary.json"), summary);
    return summary;
  } finally {
    const beforeTermination = captureProcessTopology({
      serverPid: server.pid,
      producerSnapshots: latestProducerSnapshots,
      reconstructionWorkerIds,
    });
    processTopologyCaptures.push({ phase: "before_termination", ...beforeTermination });
    lifecycleEvidence.push({
      phase: "before_termination",
      capturedAt: beforeTermination.capturedAt,
      alivePids: beforeTermination.processes.filter((process) => process.alive).map((process) => process.pid),
    });
    writeJson(processTopologyPath, { captures: processTopologyCaptures });
    writeJson(processLifecyclePath, lifecycleEvidence);

    await subscribers?.close?.().catch?.(() => undefined);
    if (resourceSampler) {
      resourceSampler.stop();
    }
    await terminateChildProcessTree(server, {
      logger: () => undefined,
      termWaitMs: 2000,
      killWaitMs: 2000,
    });
    await delay(500);

    const afterTermination = captureProcessTopology({
      serverPid: server.pid,
      producerSnapshots: latestProducerSnapshots,
      reconstructionWorkerIds,
    });
    processTopologyCaptures.push({ phase: "after_termination", ...afterTermination });
    lifecycleEvidence.push({
      phase: "after_termination",
      capturedAt: afterTermination.capturedAt,
      alivePids: afterTermination.processes.filter((process) => process.alive).map((process) => process.pid),
    });
    writeJson(processTopologyPath, { captures: processTopologyCaptures });
    writeJson(processLifecyclePath, lifecycleEvidence);
    assert(
      afterTermination.processes.every((process) => !process.alive),
      `managed process lifecycle incomplete; still alive: ${afterTermination.processes.filter((process) => process.alive).map((process) => process.pid).join(",")}`,
    );
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

function buildScenarioOrder(args) {
  const selectedApproaches = args.approaches.slice();
  return args.things.flatMap((thingCount) =>
    ["fetching", "chunked", "approximation"]
      .filter((approach) => selectedApproaches.includes(approach))
      .map((approach) => ({ thingCount, approach })),
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const resultRoot = buildResultRoot();
  ensureDir(resultRoot);
  const fixture = buildFixture(10, args.baseAnchorMs);
  writeJson(path.join(resultRoot, "fixture_oracle.json"), {
    fixture: {
      anchorMs: fixture.anchorMs,
      windowStart: fixture.windowStart,
      windowEnd: fixture.windowEnd,
      things: fixture.things.map((thing) => ({
        thingName: thing.thingName,
        topicName: thing.topicName,
        streamIri: thing.streamIri,
        oracle: thing.oracle,
        watermarkSentinel: thing.watermarkSentinel,
      })),
      watermarkSentinels: fixture.watermarkSentinels,
    },
  });

  const buildInfo = ensureFreshProductionBuild(resultRoot);
  const executionOrder = buildScenarioOrder(args);
  const scenarioSummaries = [];
  for (const { thingCount, approach } of executionOrder) {
    const scenarioRoot = path.join(resultRoot, `n${thingCount}`);
    ensureDir(scenarioRoot);
    const summary = await runApproach({
      approach,
      thingCount,
      fixture,
      scenarioRoot,
      deliveryTimeoutMs: args.timeoutMs,
      expectedBuildManifest: buildInfo.buildManifest,
    });
    scenarioSummaries.push({
      thingCount,
      approach,
      summary,
    });
  }

  const completeness = scenarioSummaries.map(({ thingCount, approach, summary }) => ({
    thingCount,
    approach,
    deliveries: summary.deliveries.length,
    registrations: summary.registrations.length,
    exactOrComparable:
      approach === "approximation"
        ? summary.checks.every((entry) => Number.isFinite(entry.absoluteError))
        : summary.checks.every((entry) => entry.absoluteError <= FLOAT_TOLERANCE),
  }));
  const portStatus = checkPortStatus(CONTROL_PORT);
  const finalSummary = {
    resultRoot,
    buildInfo,
    scenarioOrder: executionOrder,
    completeness,
    portStatus,
    orphanProcessStatus: {
      controlPortFree: portStatus.activePids.length === 0,
    },
  };
  writeJson(path.join(resultRoot, "preliminary_summary.json"), finalSummary);

  console.log(
    JSON.stringify({
      resultRoot,
      successfulCells: completeness.filter((entry) => entry.exactOrComparable).length,
      totalCells: completeness.length,
      controlPortFree: finalSummary.orphanProcessStatus.controlPortFree,
    }),
  );
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = {
  buildApproachScenarioMetrics,
  buildNestedDependencyTopology,
  buildOrderedPublishEvents,
  buildProducerReferenceRows,
  buildRegistrationBodies,
  buildScenarioOrder,
  classifyProcessTopology,
  compareToOracle,
  evaluateComparableDelivery,
  normalizeDelivery,
  parseArgs,
  parseProcessRows,
  parseResultPayload,
  validateChunkedContributionEvidence,
  validateChunkedWatermarkAndCoverage,
  validateDeliveryProducerIdentities,
  validateProducerIdentityMappings,
  validateProducerTopology,
  validateWatermarkSentinels,
};
