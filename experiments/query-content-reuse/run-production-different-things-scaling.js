#!/usr/bin/env node

const { execSync, spawn } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const {
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
  REUSE_DENSITY_MANIFEST,
  REUSE_DENSITY_PRODUCER_COUNT,
  REUSE_DENSITY_TARGET_COUNTS,
  buildFinalQuery,
  buildFixture,
  buildProducerExpectations,
  buildReuseDensityMetrics,
  buildReuseDensityOracle,
  buildReuseDensityProducerExpectations,
  buildReuseDensityQueryDefinitions,
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
    mode: "nested",
  };
  let countExplicitlySet = false;
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
        countExplicitlySet = true;
        break;
      case "--timeout-ms":
        args.timeoutMs = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--base-anchor-ms":
        args.baseAnchorMs = Number.parseInt(next || "", 10);
        index += 1;
        break;
      case "--mode":
        args.mode = String(next || "").trim().toLowerCase();
        index += 1;
        break;
      case "--targets":
        args.things = String(next || "")
          .split(",")
          .map((entry) => Number.parseInt(entry.trim(), 10))
          .filter((value) => Number.isFinite(value));
        index += 1;
        countExplicitlySet = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (args.approaches.length === 0) {
    throw new Error("At least one approach must be selected");
  }
  assert(["nested", "reuse-density", "existing-reuse-density"].includes(args.mode), `Unsupported mode: ${args.mode}`);
  if (["reuse-density", "existing-reuse-density"].includes(args.mode) && !countExplicitlySet) {
    args.things = [...REUSE_DENSITY_TARGET_COUNTS];
  }
  for (const approach of args.approaches) {
    if (!ALL_APPROACHES.includes(approach)) {
      throw new Error(`Unsupported approach: ${approach}`);
    }
  }
  if (args.things.length === 0) {
    throw new Error("At least one thing count must be selected");
  }
  const allowedCounts = ["reuse-density", "existing-reuse-density"].includes(args.mode)
    ? REUSE_DENSITY_TARGET_COUNTS
    : null;
  for (const thingCount of args.things) {
    if (
      !Number.isInteger(thingCount) ||
      thingCount <= 0 ||
      (args.mode === "nested" && thingCount > 10) ||
      (allowedCounts && !allowedCounts.includes(thingCount))
    ) {
      throw new Error(`Unsupported ${["reuse-density", "existing-reuse-density"].includes(args.mode) ? "target" : "thing"} count: ${thingCount}`);
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
      totalProcessTreeCpuSeconds: null,
      activeCpuSeconds: null,
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
      totalProcessTreeCpuSeconds: null,
      activeCpuSeconds: null,
    };
  }
  const samples = lines
    .map((line) => line.split(","))
    .map((parts) => ({
      timestamp: Number(parts[0]),
      processCount: Number(parts[2]),
      rssMb: Number(parts[3]) / (1024 * 1024),
      treeCpuSeconds: Number(parts[4]),
      treeCpuSecondsDelta: Number(parts[5]),
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
    totalProcessTreeCpuSeconds: samples.at(-1)?.treeCpuSeconds ?? null,
    activeCpuSeconds: samples.reduce(
      (sum, entry) => sum + (Number.isFinite(entry.treeCpuSecondsDelta) ? entry.treeCpuSecondsDelta : 0),
      0,
    ),
  };
}

function collectProfileSummaries(runRoot) {
  if (!fs.existsSync(runRoot)) {
    return { artifactCount: 0, counters: {}, timingsMs: {} };
  }
  const summaries = fs.readdirSync(runRoot, { withFileTypes: true })
    .filter((entry) => entry.isFile() && /^hive_profile_summary\..+\.json$/.test(entry.name))
    .map((entry) => {
      try {
        return JSON.parse(fs.readFileSync(path.join(runRoot, entry.name), "utf8"));
      } catch (error) {
        throw new Error(`Invalid profile artifact ${entry.name}: ${error.message}`);
      }
    });
  const aggregate = (field) => summaries.reduce((result, summary) => {
    for (const [key, value] of Object.entries(summary[field] || {})) {
      if (Number.isFinite(value)) {
        result[key] = (result[key] || 0) + value;
      }
    }
    return result;
  }, {});
  return {
    artifactCount: summaries.length,
    counters: aggregate("counters"),
    timingsMs: aggregate("timingsMs"),
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

function buildQueryDefinitions(mode, targetCount, options) {
  return ["reuse-density", "existing-reuse-density"].includes(mode)
    ? buildReuseDensityQueryDefinitions(targetCount, options)
    : buildScenarioQueryDefinitions(targetCount, options);
}

function buildScenarioOracleForMode(mode, targetCount, fixture) {
  return ["reuse-density", "existing-reuse-density"].includes(mode)
    ? buildReuseDensityOracle(targetCount, fixture)
    : buildScenarioOracle(targetCount, fixture);
}

function buildRegistrationBodies({ approach, thingCount, topicPrefix, mode = "nested" }) {
  return buildQueryDefinitions(mode, thingCount, {
    topicPrefix,
    outputIriBuilder: (label) =>
      `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${label.toLowerCase()}`,
  }).map((queryDefinition) => ({
    id: `${approach}-${mode}-${thingCount}-${queryDefinition.queryLabel.toLowerCase()}`,
    consumer_id: `${approach}-${mode}-${thingCount}-${queryDefinition.queryLabel.toLowerCase()}-consumer`,
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

// This intentionally uses the same query builder and production /register body as
// the target workload.  The only distinction is time: these seven registrations
// form the pre-existing workload before a composite is allowed to arrive.
function buildExistingPrimitiveRegistrationBodies({ approach, topicPrefix }) {
  return Array.from({ length: REUSE_DENSITY_PRODUCER_COUNT }, (_unused, index) => {
    const thingName = `thing${index + 1}`;
    const queryLabel = `P${index + 1}`;
    const query = buildScenarioQueryDefinitions(1, {
      topicPrefix,
      outputIriBuilder: () => `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${queryLabel.toLowerCase()}`,
    })[0];
    // buildScenarioQueryDefinitions always starts at thing1, so use the shared
    // final-query constructor indirectly through the fixed manifest definition.
    const definition = index === 0 ? query : {
      ...query,
      includedThings: [thingName],
      query: buildFinalQuery({
        includedThings: [thingName],
        topicPrefix,
        outputIri: `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${queryLabel.toLowerCase()}`,
        rangeMs: OUTPUT_RANGE_MS,
        stepMs: OUTPUT_STEP_MS,
      }),
    };
    return {
      id: `${approach}-existing-reuse-density-primitive-${queryLabel.toLowerCase()}`,
      consumer_id: `${approach}-existing-reuse-density-primitive-${queryLabel.toLowerCase()}-consumer`,
      query_label: queryLabel,
      included_things: definition.includedThings,
      approach,
      rspql_query: definition.query,
      r2s_topic: `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${queryLabel.toLowerCase()}`,
      data_topic: `mqtt://localhost:1883/${topicPrefix}/results/${approach}/${queryLabel.toLowerCase()}`,
      approximation_config:
        approach === "approximation"
          ? {
              policy: "rate-based-completed-window",
              completedWindowMode: true,
              earlyTriggerMode: false,
            }
          : undefined,
      expectedWindowStart: ALIGNMENT_ORIGIN_MS,
      expectedWindowEnd: ALIGNMENT_ORIGIN_MS + OUTPUT_RANGE_MS,
    };
  });
}

function latestResourceSnapshot(csvPath) {
  const metrics = parseResourceMetrics(csvPath);
  if (!fs.existsSync(csvPath)) return metrics;
  const lines = fs.readFileSync(csvPath, "utf8").trim().split(/\r?\n/).slice(1);
  const last = lines.at(-1)?.split(",") || [];
  return {
    ...metrics,
    timestamp: Number(last[0]) || null,
    processCount: Number(last[2]) || null,
    rssMb: Number(last[3]) / (1024 * 1024) || null,
    cumulativeProcessTreeCpuSeconds: Number(last[4]) || null,
  };
}

function sumRoleCounts(processes = []) {
  return processes.reduce((counts, process) => {
    const role = process.classification || "unknown";
    counts[role] = (counts[role] || 0) + 1;
    return counts;
  }, {});
}

function subtractNumericRecords(after = {}, before = {}) {
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  return Object.fromEntries([...keys].sort().map((key) => [
    key,
    (Number(after[key]) || 0) - (Number(before[key]) || 0),
  ]));
}

function buildPhaseProfileSnapshot(runRoot) {
  const summary = collectProfileSummaries(runRoot);
  return {
    capturedAt: Date.now(),
    scope: "finalized per-process profile artifacts currently present in this run directory",
    liveCounterAvailability: summary.artifactCount > 0
      ? "partial-finalized-artifacts-only"
      : "unavailable-until-worker-shutdown",
    ...summary,
  };
}

function buildRspEngineEvidence({ approach, registrations, producerMappings }) {
  if (approach === "chunked") {
    const runtimeProducerIds = [...new Set(producerMappings.map((mapping) => mapping.runtimeProducerId))].sort();
    return {
      scope: "manager-owned primitive producer runtimes reported by production /register",
      primitiveRspEngineCount: runtimeProducerIds.length,
      primitiveRuntimeProducerIds: runtimeProducerIds,
      reconstructionExecutionIds: registrations.map((registration) => registration.executionId),
    };
  }
  return {
    scope: "production /register execution records; Fetching is in-process and does not expose per-engine worker IDs",
    primitiveRspEngineCount: null,
    executionCount: registrations.length,
    executionIds: registrations.map((registration) => registration.executionId),
  };
}

function compareTopologyPhases(before, after) {
  const beforePids = new Set(before.processes.filter((process) => process.alive).map((process) => process.pid));
  const afterPids = new Set(after.processes.filter((process) => process.alive).map((process) => process.pid));
  return {
    processCountDelta: afterPids.size - beforePids.size,
    addedProcesses: after.processes.filter((process) => process.alive && !beforePids.has(process.pid)),
    removedProcesses: before.processes.filter((process) => process.alive && !afterPids.has(process.pid)),
    phase1RoleCounts: sumRoleCounts(before.processes.filter((process) => process.alive)),
    postAdditionRoleCounts: sumRoleCounts(after.processes.filter((process) => process.alive)),
  };
}

function validateRegistrationSet(approach, registrations, { allowFinalReuse = false } = {}) {
  assert(registrations.length > 0, `${approach}: missing registrations`);
  const executionIds = new Set(registrations.map((entry) => entry.executionId));
  const canonicalQueryIds = new Set(registrations.map((entry) => entry.canonicalQueryId));
  const outputTopics = new Set(registrations.map((entry) => entry.outputTopic));
  if (!allowFinalReuse) {
    assert(
      executionIds.size === registrations.length,
      `${approach}: final queries shared one execution`,
    );
  }
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
    if (!allowFinalReuse) {
      assert(registration.executionCreated === true, `${approach}/${registration.queryLabel}: executionCreated=false`);
      assert(registration.reuseHit === false, `${approach}/${registration.queryLabel}: reuseHit=true`);
      assert(
        registration.reuseDecision?.reuseHit === false,
        `${approach}/${registration.queryLabel}: reuseDecision.reuseHit=true`,
      );
    }
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

function validateReuseDensityProducerTopology({ targetCount, registrations, latestProducerSnapshots }) {
  assert(registrations.length === targetCount, `expected ${targetCount} registrations, got ${registrations.length}`);
  assert(
    latestProducerSnapshots.length === REUSE_DENSITY_PRODUCER_COUNT,
    `expected ${REUSE_DENSITY_PRODUCER_COUNT} unique producers, got ${latestProducerSnapshots.length}`,
  );
  const snapshotByCanonical = new Map();
  for (const snapshot of latestProducerSnapshots) {
    const thingName = String(snapshot.expectedInputStream || "").split("/").at(-1);
    assert(/^thing[1-7]$/.test(thingName), `invalid fixed-pool producer stream ${snapshot.expectedInputStream}`);
    assert(!snapshotByCanonical.has(snapshot.canonicalProducerId), `duplicate producer ${snapshot.canonicalProducerId}`);
    snapshotByCanonical.set(snapshot.canonicalProducerId, { ...snapshot, thingName });
  }
  assertSameStringSet(
    [...snapshotByCanonical.values()].map((snapshot) => snapshot.thingName),
    Array.from({ length: REUSE_DENSITY_PRODUCER_COUNT }, (_unused, index) => `thing${index + 1}`),
    "reuse-density fixed producer pool",
  );

  const identityByCanonical = new Map();
  for (const registration of registrations) {
    const mappings = validateProducerIdentityMappings(
      registration.producerIdentityMappings,
      `${registration.queryLabel}: producerIdentityMappings`,
    );
    assert(mappings.length === 4, `${registration.queryLabel}: expected four producer mappings`);
    const mappedThings = mappings.map((mapping) => {
      const snapshot = snapshotByCanonical.get(mapping.canonicalProducerId);
      assert(snapshot, `${registration.queryLabel}: unknown canonical producer ${mapping.canonicalProducerId}`);
      assert(
        snapshot.runtimeProducerId === mapping.runtimeProducerId &&
          (snapshot.topic || snapshot.producerTopic || snapshot.outputTopic) === mapping.topic,
        `${registration.queryLabel}: producer identity mismatch for ${mapping.canonicalProducerId}`,
      );
      const prior = identityByCanonical.get(mapping.canonicalProducerId);
      if (prior) {
        assert(
          prior.runtimeProducerId === mapping.runtimeProducerId && prior.topic === mapping.topic,
          `${registration.queryLabel}: canonical producer ${mapping.canonicalProducerId} changed identity`,
        );
      } else {
        identityByCanonical.set(mapping.canonicalProducerId, mapping);
      }
      return snapshot.thingName;
    });
    assertSameStringSet(mappedThings, registration.includedThings, `${registration.queryLabel}: fixed-pool dependencies`);
    registration.producerIdentityMappings = mappings;
  }

  const dependencyTopology = {
    nodes: [...snapshotByCanonical.values()].map((snapshot) => ({
      thingName: snapshot.thingName,
      canonicalProducerId: snapshot.canonicalProducerId,
      runtimeProducerId: snapshot.runtimeProducerId,
      topic: snapshot.topic || snapshot.producerTopic || snapshot.outputTopic,
    })),
    finalProducerIdentityMappings: [...identityByCanonical.values()],
  };
  const rows = buildProducerReferenceRows({
    producerSnapshots: latestProducerSnapshots,
    registrations,
    dependencyTopology,
  });
  const expectations = buildReuseDensityProducerExpectations(targetCount);
  for (const expectation of expectations) {
    const row = rows.find((entry) => entry.thingName === expectation.thingName);
    assert(row, `missing producer topology node for ${expectation.thingName}`);
    assert(row.referenceCount === expectation.expectedReferenceCount, `${expectation.thingName}: referenceCount=${row.referenceCount} expected=${expectation.expectedReferenceCount}`);
    assertSameStringSet(row.dependentQueryLabels, expectation.dependentQueryLabels, `${expectation.thingName}: dependent queries`);
  }
  return { dependencyTopology, producerReferenceRows: rows };
}

function validateProducerTopology({ thingCount, registrations, latestProducerSnapshots, mode = "nested" }) {
  if (mode === "reuse-density") {
    return validateReuseDensityProducerTopology({
      targetCount: thingCount,
      registrations,
      latestProducerSnapshots,
    });
  }
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

function buildApproachScenarioMetrics(approach, thingCount, mode = "nested") {
  const metrics = mode === "reuse-density"
    ? buildReuseDensityMetrics(thingCount)
    : buildScenarioMetrics(thingCount);
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

function summarizeCorrectness(checks) {
  const finite = (field) => checks.map((check) => check[field]).filter(Number.isFinite);
  const mean = (values) => values.length > 0
    ? values.reduce((sum, value) => sum + value, 0) / values.length
    : null;
  const max = (values) => values.length > 0 ? Math.max(...values) : null;
  return {
    checkCount: checks.length,
    countErrors: finite("countError"),
    sumErrors: finite("sumError"),
    averageErrors: finite("absoluteError"),
    mae: mean(finite("absoluteError")),
    mape: mean(finite("percentageError")),
    maxAbsoluteError: max(finite("absoluteError")),
  };
}

function summarizeRegistrations(registrations) {
  return {
    registrationCount: registrations.length,
    executionCount: new Set(registrations.map((entry) => entry.executionId).filter(Boolean)).size,
    reuseHits: registrations.filter((entry) => entry.reuseHit === true || entry.reuseDecision?.reuseHit === true).length,
  };
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

async function runExistingReuseDensityApproach({ approach, thingCount, fixture, scenarioRoot, deliveryTimeoutMs, expectedBuildManifest }) {
  const runRoot = path.join(scenarioRoot, approach);
  ensureDir(runRoot);
  const topicPrefix = `experiment2/${path.basename(scenarioRoot)}/existing-reuse-density/m${thingCount}`;
  const registrationPath = path.join(runRoot, "registration_events.ndjson");
  const deliveryPath = path.join(runRoot, "consumer_delivery_events.ndjson");
  const publishEventsPath = path.join(runRoot, "published_fixture_events.ndjson");
  const resourcePath = path.join(runRoot, "process_tree_resource_usage.csv");
  const serverOutPath = path.join(runRoot, "server.stdout.log");
  const serverErrPath = path.join(runRoot, "server.stderr.log");
  assertControlPortIsFree(CONTROL_PORT);
  verifyBuildManifestOrThrow(expectedBuildManifest);
  const server = spawn("node", [SERVER_EXECUTABLE_PATH], { cwd: REPO_ROOT, env: buildApproachEnv({ approach, runRoot, replayAnchor: fixture.anchorMs, topicPrefix }), detached: true, stdio: ["ignore", fs.openSync(serverOutPath, "a"), fs.openSync(serverErrPath, "a")] });
  let sampler = null;
  let subscribers = null;
  const baselineRegistrations = [];
  const compositeRegistrations = [];
  try {
    await waitForServerLog(serverOutPath);
    await waitForRegisterReady(CONTROL_PORT);
    sampler = startProcessTreeResourceLogging(resourcePath, server.pid, 200);
    for (const body of buildExistingPrimitiveRegistrationBodies({ approach, topicPrefix })) {
      const registration = await postRegistration(CONTROL_PORT, body);
      registration.expectedWindowStart = body.expectedWindowStart;
      registration.expectedWindowEnd = body.expectedWindowEnd;
      appendNdjson(registrationPath, { phase: "baseline", ...registration });
      baselineRegistrations.push(registration);
    }
    validateRegistrationSet(approach, baselineRegistrations, { allowFinalReuse: approach === "approximation" });
    assert(baselineRegistrations.length === REUSE_DENSITY_PRODUCER_COUNT, "existing-reuse-density requires exactly seven Phase-1 primitives");
    await delay(300);
    const baselineSnapshots = latestResourceSnapshot(resourcePath);
    const baselineProducerMappings = baselineRegistrations.flatMap((entry) => entry.producerIdentityMappings || []);
    if (approach === "chunked") {
      assert(baselineProducerMappings.length === REUSE_DENSITY_PRODUCER_COUNT, "Chunked Phase 1 must expose exactly seven primitive runtimes");
      assert(new Set(baselineProducerMappings.map((entry) => entry.runtimeProducerId)).size === REUSE_DENSITY_PRODUCER_COUNT, "Chunked Phase 1 primitive runtime IDs must be distinct");
    }
    const phase1BaselineTopology = captureProcessTopology({
      serverPid: server.pid,
      producerSnapshots: baselineRegistrations.flatMap((entry) => entry.producerSnapshots || []),
      reconstructionWorkerIds: baselineRegistrations.flatMap((entry) => entry.workerIds || []),
    });
    const phase1ProfileSnapshot = buildPhaseProfileSnapshot(runRoot);
    const phase1RspEngineEvidence = buildRspEngineEvidence({
      approach,
      registrations: baselineRegistrations,
      producerMappings: baselineProducerMappings,
    });
    writeJson(path.join(runRoot, "phase1_baseline_evidence.json"), {
      timestamp: Date.now(), resourceSnapshot: baselineSnapshots, processTopology: phase1BaselineTopology,
      profileSnapshot: phase1ProfileSnapshot, rspEngineEvidence: phase1RspEngineEvidence,
    });

    const cpuIntervalStartedAt = Date.now();
    const compositeStartSnapshot = latestResourceSnapshot(resourcePath);
    for (const body of buildRegistrationBodies({ approach, thingCount, topicPrefix, mode: "existing-reuse-density" })) {
      const registration = await postRegistration(CONTROL_PORT, body);
      registration.expectedWindowStart = body.expectedWindowStart;
      registration.expectedWindowEnd = body.expectedWindowEnd;
      appendNdjson(registrationPath, { phase: "post_addition", ...registration });
      compositeRegistrations.push(registration);
    }
    validateRegistrationSet(approach, compositeRegistrations, { allowFinalReuse: approach === "approximation" });
    if (approach === "chunked") {
      const baselineRuntimeIds = new Set(baselineProducerMappings.map((entry) => entry.runtimeProducerId));
      const compositeMappings = compositeRegistrations.flatMap((entry) => entry.producerIdentityMappings);
      assert(compositeMappings.length === thingCount * 4, "Chunked composite dependency count mismatch");
      assert(compositeMappings.every((entry) => baselineRuntimeIds.has(entry.runtimeProducerId)), "Chunked created a primitive runtime after composite registration");
      assert(new Set(compositeMappings.map((entry) => entry.runtimeProducerId)).size === REUSE_DENSITY_PRODUCER_COUNT, "Chunked composite dependencies do not resolve to the seven Phase-1 runtimes");
      assert(compositeRegistrations.every((entry) => entry.localProducerSpawnCount === 0), "Chunked local producer spawning must remain zero");
    }
    await delay(300);
    const postAdditionSnapshot = latestResourceSnapshot(resourcePath);
    const postAdditionTopology = captureProcessTopology({
      serverPid: server.pid,
      producerSnapshots: [...baselineRegistrations, ...compositeRegistrations].flatMap((entry) => entry.producerSnapshots || []),
      reconstructionWorkerIds: [...baselineRegistrations, ...compositeRegistrations].flatMap((entry) => entry.workerIds || []),
    });
    const postAdditionProfileSnapshot = buildPhaseProfileSnapshot(runRoot);
    const postAdditionRspEngineEvidence = buildRspEngineEvidence({
      approach,
      registrations: [...baselineRegistrations, ...compositeRegistrations],
      producerMappings: [...baselineProducerMappings, ...compositeRegistrations.flatMap((entry) => entry.producerIdentityMappings)],
    });
    const topologyDelta = compareTopologyPhases(phase1BaselineTopology, postAdditionTopology);
    const profileCounterDelta = subtractNumericRecords(postAdditionProfileSnapshot.counters, phase1ProfileSnapshot.counters);
    const profileTimingDeltaMs = subtractNumericRecords(postAdditionProfileSnapshot.timingsMs, phase1ProfileSnapshot.timingsMs);
    const phaseEvidenceDelta = {
      processTopology: topologyDelta,
      profileCounters: profileCounterDelta,
      profileTimingsMs: profileTimingDeltaMs,
      rspPrimitiveEngineDelta: approach === "chunked"
        ? postAdditionRspEngineEvidence.primitiveRspEngineCount - phase1RspEngineEvidence.primitiveRspEngineCount
        : null,
    };
    writeJson(path.join(runRoot, "post_addition_evidence.json"), {
      timestamp: Date.now(), resourceSnapshot: postAdditionSnapshot, processTopology: postAdditionTopology,
      profileSnapshot: postAdditionProfileSnapshot, rspEngineEvidence: postAdditionRspEngineEvidence,
      phase1ToPostAdditionDelta: phaseEvidenceDelta,
    });
    subscribers = await subscribeConsumers({ registrations: [...baselineRegistrations, ...compositeRegistrations], deliveryPath });
    await publishFixture({ fixture, thingCount: REUSE_DENSITY_PRODUCER_COUNT, topicPrefix, publishEventsPath });
    const allDeliveries = await subscribers.waitForComparableDeliveries(deliveryTimeoutMs, approach);
    const deliveries = allDeliveries.filter((entry) => /^Q\d+$/.test(entry.queryLabel));
    const baselineDeliveries = allDeliveries.filter((entry) => /^P\d+$/.test(entry.queryLabel));
    const checks = compareToOracle(approach, deliveries, buildReuseDensityOracle(thingCount, fixture));
    if (approach !== "approximation") {
      validateExactApproach(approach, checks);
    }
    assert(baselineDeliveries.length === REUSE_DENSITY_PRODUCER_COUNT, "all Phase-1 primitives must deliver before completion");
    await delay(ARTIFACT_SETTLE_MS);
    const completionSnapshot = latestResourceSnapshot(resourcePath);
    const incremental = {
      cpuIntervalStartedAt,
      cpuIntervalEndedAt: Date.now(),
      cpuSeconds: completionSnapshot.cumulativeProcessTreeCpuSeconds - compositeStartSnapshot.cumulativeProcessTreeCpuSeconds,
      rssMb: postAdditionSnapshot.rssMb - baselineSnapshots.rssMb,
      processCount: postAdditionSnapshot.processCount - baselineSnapshots.processCount,
    };
    assert(Number.isFinite(incremental.cpuSeconds) && incremental.cpuSeconds >= 0, "invalid incremental CPU-seconds accounting");
    assert(Number.isFinite(incremental.rssMb), "invalid incremental RSS accounting");
    const provenance = compositeRegistrations.flatMap((composite) => composite.producerIdentityMappings.map((mapping) => {
      const primitive = baselineRegistrations.find((entry) => entry.producerIdentityMappings.some((candidate) => candidate.runtimeProducerId === mapping.runtimeProducerId));
      return { compositeQueryLabel: composite.queryLabel, compositeRegisteredAt: composite.requestStartedAt, primitiveQueryLabel: primitive?.queryLabel, primitiveExecutionId: primitive?.executionId, primitiveRegisteredAt: primitive?.responseReceivedAt, runtimeProducerId: mapping.runtimeProducerId, topic: mapping.topic };
    }));
    assert(provenance.every((entry) => entry.primitiveRegisteredAt < entry.compositeRegisteredAt), "a composite dependency does not predate composite registration");
    const registrations = [...baselineRegistrations, ...compositeRegistrations];
    const summary = { approach, mode: "existing-reuse-density", thingCount, runRoot, topicPrefix, baseline: { registrations: baselineRegistrations, registrationMetrics: summarizeRegistrations(baselineRegistrations), deliveries: baselineDeliveries, resourceSnapshot: baselineSnapshots, producerMappings: baselineProducerMappings, processTopology: phase1BaselineTopology, profileSnapshot: phase1ProfileSnapshot, rspEngineEvidence: phase1RspEngineEvidence }, postAddition: { registrations: compositeRegistrations, registrationMetrics: summarizeRegistrations(compositeRegistrations), resourceSnapshot: postAdditionSnapshot, processTopology: postAdditionTopology, profileSnapshot: postAdditionProfileSnapshot, rspEngineEvidence: postAdditionRspEngineEvidence }, registrationMetrics: summarizeRegistrations(registrations), phase2EvidenceDelta: phaseEvidenceDelta, completion: { resourceSnapshot: completionSnapshot }, incremental: { ...incremental, rssMeasurementScope: "instantaneous summed RSS of the authoritative server process tree at each boundary; run-wide peak is retained separately" }, deliveries, checks, correctness: summarizeCorrectness(checks), workloadManifest: REUSE_DENSITY_MANIFEST.slice(0, thingCount).map((dependencies, index) => ({ queryLabel: `Q${index + 1}`, dependencies: [...dependencies] })), provenance, topology: { existingPrimitiveExecutions: REUSE_DENSITY_PRODUCER_COUNT, dependencyReferences: thingCount * 4, reusedDependencyReferences: thingCount * 4 - REUSE_DENSITY_PRODUCER_COUNT, additionalPrimitiveExecutions: approach === "chunked" ? 0 : null, additionalIndependentExecutions: approach === "fetching" ? thingCount : null, compositeReconstructionPaths: approach === "chunked" ? thingCount : 0 }, resourceMetrics: parseResourceMetrics(resourcePath), profileSummary: collectProfileSummaries(runRoot), deliveryDecisions: subscribers.getDeliveryDecisions() };
    writeJson(path.join(runRoot, "approach_summary.json"), summary);
    return summary;
  } finally {
    await subscribers?.close?.().catch?.(() => undefined);
    sampler?.stop();
    await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 2000, killWaitMs: 2000 });
    await delay(500);
    assertControlPortIsFree(CONTROL_PORT);
  }
}

async function runApproach({
  approach,
  thingCount,
  mode = "nested",
  fixture,
  scenarioRoot,
  deliveryTimeoutMs,
  expectedBuildManifest,
}) {
  if (mode === "existing-reuse-density") {
    return runExistingReuseDensityApproach({ approach, thingCount, fixture, scenarioRoot, deliveryTimeoutMs, expectedBuildManifest });
  }
  const runRoot = path.join(scenarioRoot, approach);
  ensureDir(runRoot);
  const topicPrefix = mode === "reuse-density"
    ? `experiment2/${path.basename(scenarioRoot)}/reuse-density/m${thingCount}`
    : `experiment2/${path.basename(scenarioRoot)}/n${thingCount}`;
  const registrationPath = path.join(runRoot, "registration_events.ndjson");
  const deliveryPath = path.join(runRoot, "consumer_delivery_events.ndjson");
  const publishEventsPath = path.join(runRoot, "published_fixture_events.ndjson");
  const serverOutPath = path.join(runRoot, "server.stdout.log");
  const serverErrPath = path.join(runRoot, "server.stderr.log");
  const resourcePath = path.join(runRoot, "process_tree_resource_usage.csv");
  const processTopologyPath = path.join(runRoot, "process_topology.json");
  const processLifecyclePath = path.join(runRoot, "process_lifecycle_evidence.json");
  const producerCount = mode === "reuse-density" ? REUSE_DENSITY_PRODUCER_COUNT : thingCount;
  const sentinelVerification = validateWatermarkSentinels(fixture, producerCount);
  const env = buildApproachEnv({
    approach,
    runRoot,
    replayAnchor: fixture.anchorMs,
    topicPrefix,
  });

  // Experiments 1 and 3 may be active independently. A busy port is an
  // ownership conflict, not permission to terminate an unrelated process.
  assertControlPortIsFree(CONTROL_PORT);
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

    for (const requestBody of buildRegistrationBodies({ approach, thingCount, topicPrefix, mode })) {
      const registration = await postRegistration(CONTROL_PORT, requestBody);
      registration.expectedWindowStart = requestBody.expectedWindowStart;
      registration.expectedWindowEnd = requestBody.expectedWindowEnd;
      appendNdjson(registrationPath, registration);
      registrations.push(registration);
    }
    validateRegistrationSet(approach, registrations);
    latestProducerSnapshots = [...new Map(
      registrations
        .flatMap((registration) => registration.producerSnapshots || [])
        .map((snapshot) => [snapshot.canonicalProducerId, snapshot]),
    ).values()];
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
      thingCount: producerCount,
      topicPrefix,
      publishEventsPath,
    });

    const deliveries = await subscribers.waitForComparableDeliveries(
      deliveryTimeoutMs,
      approach,
    );

    await delay(ARTIFACT_SETTLE_MS);
    const resourceMetrics = parseResourceMetrics(resourcePath);
    const profileSummary = collectProfileSummaries(runRoot);
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
        mode,
      });
      dependencyTopology = validatedTopology.dependencyTopology;
      producerReferenceRows = validatedTopology.producerReferenceRows;
    }
    const oracleRows = buildScenarioOracleForMode(mode, thingCount, fixture);
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
      mode,
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
      profileSummary,
      scenarioMetrics: buildApproachScenarioMetrics(approach, thingCount, mode),
      workloadManifest: mode === "reuse-density"
        ? REUSE_DENSITY_MANIFEST.slice(0, thingCount).map((dependencies, index) => ({
            queryLabel: `Q${index + 1}`,
            dependencies: [...dependencies],
          }))
        : null,
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
    assertControlPortIsFree(CONTROL_PORT);
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

function assertControlPortIsFree(port) {
  const status = checkPortStatus(port);
  assert(
    status.activePids.length === 0,
    `Refusing to start Experiment 2: control port ${port} is owned by existing PID(s) ${status.activePids.join(",")}. ` +
      "The runner never kills pre-existing processes; stop only the known owner and retry.",
  );
}

function buildScenarioOrder(args) {
  const selectedApproaches = args.approaches.slice();
  return args.things.flatMap((thingCount) =>
    ["fetching", "chunked", "approximation"]
      .filter((approach) => selectedApproaches.includes(approach))
      .map((approach) => ({ thingCount, approach, mode: args.mode })),
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const resultRoot = buildResultRoot();
  ensureDir(resultRoot);
  const fixture = buildFixture(
    ["reuse-density", "existing-reuse-density"].includes(args.mode) ? REUSE_DENSITY_PRODUCER_COUNT : 10,
    args.baseAnchorMs,
  );
  writeJson(path.join(resultRoot, "fixture_oracle.json"), {
    mode: args.mode,
    workloadManifest: ["reuse-density", "existing-reuse-density"].includes(args.mode)
      ? REUSE_DENSITY_MANIFEST.map((dependencies, index) => ({
          queryLabel: `Q${index + 1}`,
          dependencies: [...dependencies],
        }))
      : null,
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
  for (const { thingCount, approach, mode } of executionOrder) {
    const scenarioRoot = path.join(
      resultRoot,
      mode === "reuse-density" ? `reuse-density-m${thingCount}` : mode === "existing-reuse-density" ? `existing-reuse-density-m${thingCount}` : `n${thingCount}`,
    );
    ensureDir(scenarioRoot);
    const summary = await runApproach({
      approach,
      thingCount,
      mode,
      fixture,
      scenarioRoot,
      deliveryTimeoutMs: args.timeoutMs,
      expectedBuildManifest: buildInfo.buildManifest,
    });
    scenarioSummaries.push({
      mode,
      thingCount,
      approach,
      summary,
    });
  }

  const completeness = scenarioSummaries.map(({ mode, thingCount, approach, summary }) => ({
    mode,
    thingCount,
    approach,
    deliveries: summary.deliveries.length,
    registrations:
      summary.registrations?.length ??
      ((summary.baseline?.registrations?.length ?? 0) +
        (summary.postAddition?.registrations?.length ?? 0)),
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
  buildQueryDefinitions,
  buildNestedDependencyTopology,
  buildOrderedPublishEvents,
  buildProducerReferenceRows,
  buildRegistrationBodies,
  buildExistingPrimitiveRegistrationBodies,
  buildPhaseProfileSnapshot,
  buildRspEngineEvidence,
  compareTopologyPhases,
  buildScenarioOrder,
  assertControlPortIsFree,
  classifyProcessTopology,
  compareToOracle,
  summarizeCorrectness,
  summarizeRegistrations,
  evaluateComparableDelivery,
  normalizeDelivery,
  parseArgs,
  parseProcessRows,
  parseResourceMetrics,
  subtractNumericRecords,
  latestResourceSnapshot,
  parseResultPayload,
  validateChunkedContributionEvidence,
  validateChunkedWatermarkAndCoverage,
  validateDeliveryProducerIdentities,
  validateProducerIdentityMappings,
  validateProducerTopology,
  validateReuseDensityProducerTopology,
  validateWatermarkSentinels,
};
