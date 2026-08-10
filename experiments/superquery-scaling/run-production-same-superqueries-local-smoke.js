#!/usr/bin/env node

const { spawn, execSync } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");
const mqtt = require("mqtt");
const {
  cleanupStaleBenchmarkProcesses,
  delay,
  terminateChildProcessTree,
} = require("../utils/processCleanup");
const { createBenchmarkReplayRunEnv } = require("../utils/benchmarkReplayEnv");
const { startProcessTreeResourceLogging } = require("../../scripts/analysis-js/process-tree-resource-sampler");
const {
  OUTPUT_WINDOW_RANGE_MS,
  OUTPUT_WINDOW_STEP_MS,
  SUB_WINDOW_RANGE_MS,
  SUB_WINDOW_STEP_MS,
  TARGET_WINDOWS,
  buildCombinationMatrix,
  buildScenarioKey,
  createScenarioReplayAnchors,
  median,
  parseApproachSelection,
  parseKScalingSelection,
  readProcessTreeMetrics,
  sanitizeTimestamp,
} = require("../k-scaling/local-k-scaling-smoke-common");
const { buildOutputSelectClause } = require("../../dist/util/runtimeConfig");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const CONTROL_PORT = 8080;
const REQUIRED_REPLAY_DURATION_SECONDS = 185;
const STARTUP_READY_TIMEOUT_MS = 30_000;
const DELIVERY_TIMEOUT_MS = 6 * 60 * 1000;
const ARTIFACT_SETTLE_TIMEOUT_MS = 5_000;
const POLL_INTERVAL_MS = 250;
const DEFAULT_K_VALUES = [1, 2, 4, 8, 32];
const DEFAULT_APPROACHES = ["fetching", "approximation", "chunked"];
const DEFAULT_ITERATIONS = 1;
const DEFAULT_ADVERSARIAL_K_VALUES = [3, 5, 11];
const BUILD_MANIFEST_NAME = "production-build-manifest.json";
const SERVER_EXECUTABLE_PATH = path.join(REPO_ROOT, "dist", "startHTTPServer.js");

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function parseArgs(argv) {
  const args = {
    kValues: DEFAULT_K_VALUES,
    adversarialKValues: DEFAULT_ADVERSARIAL_K_VALUES,
    approaches: DEFAULT_APPROACHES,
    iterations: DEFAULT_ITERATIONS,
    timeoutMs: DELIVERY_TIMEOUT_MS,
    baseAnchorMs: null,
    skipAdversarial: false,
    skipMainMatrix: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--k-values":
        args.kValues = parseKScalingSelection(next, DEFAULT_K_VALUES);
        index += 1;
        break;
      case "--adversarial-k-values":
        args.adversarialKValues = parseKScalingSelection(
          next,
          DEFAULT_ADVERSARIAL_K_VALUES,
        );
        index += 1;
        break;
      case "--approaches":
        args.approaches = parseApproachSelection(next, DEFAULT_APPROACHES);
        index += 1;
        break;
      case "--iterations":
        args.iterations = Number.parseInt(next || "", 10);
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
      case "--skip-adversarial":
        args.skipAdversarial = true;
        break;
      case "--skip-main-matrix":
        args.skipMainMatrix = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (!Number.isFinite(args.iterations) || args.iterations <= 0) {
    throw new Error("--iterations must be a positive integer");
  }
  return args;
}

function runCommand(command, cwd = REPO_ROOT) {
  return execSync(command, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
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

function buildResultRoot() {
  return path.join(
    REPO_ROOT,
    "results",
    "paper-benchmarks",
    `experiment3-production-same-superqueries-1x-local-${sanitizeTimestamp(new Date())}`,
  );
}

function getSourceFilesForBuildGuard() {
  const candidates = runCommand(
    'rg --files src experiments/superquery-scaling package.json tsconfig.json 2>/dev/null || true',
  )
    .split(/\r?\n/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => path.join(REPO_ROOT, entry))
    .filter((entry) => fs.existsSync(entry));
  return candidates;
}

function getLatestSourceMtimeMs() {
  const files = getSourceFilesForBuildGuard();
  return files.reduce((latest, filePath) => {
    const stat = fs.statSync(filePath);
    return Math.max(latest, stat.mtimeMs);
  }, 0);
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
    serverExecutable: SERVER_EXECUTABLE_PATH,
    sourceFingerprint: sha256(
      JSON.stringify({
        commit: repoState.commit,
        diffHash: repoState.diffHash,
        latestSourceMtimeMsBeforeBuild,
      }),
    ),
    latestSourceMtimeMsBeforeBuild,
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
  if (!fs.existsSync(path.join(REPO_ROOT, "dist"))) {
    throw new Error("dist directory is missing");
  }
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

function buildRunRoot(resultRoot, scenarioClass, approach, kValue, iteration) {
  return path.join(
    resultRoot,
    scenarioClass,
    approach,
    `K${kValue}`,
    `iteration${iteration}`,
  );
}

function readNdjson(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  return fs
    .readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function waitForServerLog(filePath, timeoutMs = STARTUP_READY_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const poll = () => {
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, "utf8");
        if (content.includes("HTTP server has started")) {
          resolve();
          return;
        }
      }
      if (Date.now() >= deadline) {
        reject(new Error("Timed out waiting for HTTP server startup"));
        return;
      }
      setTimeout(poll, POLL_INTERVAL_MS);
    };
    poll();
  });
}

async function waitForRegisterReady(port, timeoutMs = STARTUP_READY_TIMEOUT_MS) {
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

function buildQuery({
  prefix,
  outputIri,
  outputAlias = "avgValue",
  approach = "approximation",
  aggregationFunction = "AVG",
  windowRangeMs = OUTPUT_WINDOW_RANGE_MS,
  windowStepMs = OUTPUT_WINDOW_STEP_MS,
  includeWearable = true,
  includeSmartphone = true,
  renamedVariables = false,
  comment = "",
  prefixAliases,
}) {
  const aliases = prefixAliases || {
    mqtt: "mqtt_broker",
    saref: "saref",
    sensors: "dahccsensors",
    local: "rspjs",
  };
  const vValue = renamedVariables ? "?measurementValue" : "?value";
  const vTimestamp = approach === "fetching"
    ? "?ts"
    : renamedVariables
    ? "?eventTimestamp"
    : "?ts";
  const wearableSubject = renamedVariables ? "?wearableObservation" : "?s1";
  const smartphoneSubject = renamedVariables ? "?smartphoneObservation" : "?s2";
  const unions = [];
  if (includeWearable) {
    unions.push(`{
    WINDOW <mqtt://localhost:1883/${prefix}/wearableX> {
      ${wearableSubject} ${aliases.saref}:hasValue ${vValue} .
      ${wearableSubject} ${aliases.saref}:hasTimestamp ${vTimestamp} .
      ${wearableSubject} ${aliases.saref}:relatesToProperty ${aliases.sensors}:wearableX .
    }
  }`);
  }
  if (includeSmartphone) {
    unions.push(`{
    WINDOW <mqtt://localhost:1883/${prefix}/smartphoneX> {
      ${smartphoneSubject} ${aliases.saref}:hasValue ${vValue} .
      ${smartphoneSubject} ${aliases.saref}:hasTimestamp ${vTimestamp} .
      ${smartphoneSubject} ${aliases.saref}:relatesToProperty ${aliases.sensors}:smartphoneX .
    }
  }`);
  }
  const windows = [];
  if (includeWearable) {
    windows.push(
      `FROM NAMED WINDOW <mqtt://localhost:1883/${prefix}/wearableX> ON STREAM ${aliases.mqtt}:${prefix}/wearableX [RANGE ${windowRangeMs} STEP ${windowStepMs}]`,
    );
  }
  if (includeSmartphone) {
    windows.push(
      `FROM NAMED WINDOW <mqtt://localhost:1883/${prefix}/smartphoneX> ON STREAM ${aliases.mqtt}:${prefix}/smartphoneX [RANGE ${windowRangeMs} STEP ${windowStepMs}]`,
    );
  }
  const prefixLines = [
    `PREFIX ${aliases.mqtt}: <mqtt://localhost:1883/>`,
    `PREFIX ${aliases.saref}: <https://saref.etsi.org/core/>`,
    `PREFIX ${aliases.sensors}: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>`,
    `PREFIX ${aliases.local}: <https://rsp.js>`,
  ];
  const selectClause =
    approach === "fetching"
      ? buildOutputSelectClause(aggregationFunction)
      : `(${aggregationFunction}(${vValue}) AS ?${outputAlias})`;
  return `
${comment}
${prefixLines.join("\n")}
REGISTER RStream <${outputIri}> AS
SELECT ${selectClause}
${windows.join("\n")}
WHERE {
  ${unions.join(" UNION ")}
}
`.trim();
}

function defaultApproximationConfig(overrides = {}) {
  return {
    policy: "rate-based-completed-window",
    completedWindowMode: true,
    earlyTriggerMode: false,
    ...overrides,
  };
}

function buildRegistrationBody({
  approach,
  topicPrefix,
  consumerIndex,
  variantMode = false,
  approximationConfig,
  queryOverrides = {},
}) {
  const outputIri = `consumer-${consumerIndex}-output-topic`;
  return {
    id: `${approach}-query-${consumerIndex}`,
    consumer_id: `${approach}-consumer-${consumerIndex}`,
    approach,
    rspql_query: variantMode
      ? buildEquivalentVariant(topicPrefix, outputIri, consumerIndex)
      : buildQuery({
          approach,
          prefix: topicPrefix,
          outputIri,
          ...queryOverrides,
        }),
    r2s_topic: outputIri,
    data_topic: outputIri,
    approximation_config: approximationConfig,
  };
}

async function postRegistration(port, requestBody) {
  const requestStartedAt = Date.now();
  const response = await fetch(`http://localhost:${port}/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });
  const responseText = await response.text();
  let payload;
  try {
    payload = JSON.parse(responseText);
  } catch {
    payload = responseText;
  }
  const outputTopic = payload && typeof payload === "object"
    ? payload.outputTopic
    : undefined;
  if (!response.ok || typeof outputTopic !== "string" || outputTopic.trim() === "") {
    throw new Error(
      `Registration failed for ${requestBody.consumer_id}: status=${response.status} payload=${JSON.stringify(payload)}`,
    );
  }
  return {
    consumerId: requestBody.consumer_id,
    requestStartedAt,
    responseReceivedAt: Date.now(),
    status: response.status,
    requestBody,
    responseBody: payload,
    executionId: payload.executionId,
    outputTopic,
    reuseHit: payload.reuseHit,
    executionCreated: payload.executionCreated,
    reuseDecision: payload.reuseDecision,
  };
}

function buildEquivalentVariant(prefix, outputIri, consumerIndex) {
  switch (consumerIndex % 5) {
    case 1:
      return buildQuery({ prefix, outputIri });
    case 2:
      return buildQuery({
        prefix,
        outputIri,
        comment: "# consumer-specific comment",
      });
    case 3:
      return buildQuery({
        prefix,
        outputIri,
        prefixAliases: {
          mqtt: "mb",
          saref: "sf",
          sensors: "sens",
          local: "rsp",
        },
      });
    case 4:
      return buildQuery({
        prefix,
        outputIri,
        renamedVariables: true,
      });
    default:
      return buildQuery({
        prefix,
        outputIri,
      })
        .split("\n")
        .filter(Boolean)
        .sort((left, right) => {
          if (!left.startsWith("PREFIX") || !right.startsWith("PREFIX")) {
            return 0;
          }
          return right.localeCompare(left);
        })
        .join("\n");
  }
}

function buildOneWayContainmentPair(prefix) {
  const narrower = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX rspjs: <https://rsp.js>
REGISTER RStream <one-way-narrower> AS
SELECT (COUNT(?value) AS ?resultValue)
FROM NAMED WINDOW <mqtt://localhost:1883/${prefix}/wearableX> ON STREAM mqtt_broker:${prefix}/wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/${prefix}/smartphoneX> ON STREAM mqtt_broker:${prefix}/smartphoneX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <mqtt://localhost:1883/${prefix}/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
  WINDOW <mqtt://localhost:1883/${prefix}/smartphoneX> {
    ?s2 saref:hasValue ?value .
    ?s2 saref:hasTimestamp ?ts .
    ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
  }
}
`.trim();
  const broader = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX rspjs: <https://rsp.js>
REGISTER RStream <one-way-broader> AS
SELECT (COUNT(?value) AS ?resultValue)
FROM NAMED WINDOW <mqtt://localhost:1883/${prefix}/wearableX> ON STREAM mqtt_broker:${prefix}/wearableX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <mqtt://localhost:1883/${prefix}/wearableX> {
    ?s1 saref:hasValue ?value .
    ?s1 saref:hasTimestamp ?ts .
    ?s1 saref:relatesToProperty dahccsensors:wearableX .
  }
}
`.trim();
  return { narrower, broader };
}

function parseResultPayload(payloadText) {
  let parsed;
  try {
    parsed = JSON.parse(payloadText);
  } catch {
    const typedLiteralMatch = payloadText.match(/"(-?\d+(?:\.\d+)?)"\^\^<http:\/\/www\.w3\.org\/2001\/XMLSchema#(?:float|double|decimal)>/i);
    return {
      raw: payloadText,
      value:
        typedLiteralMatch && Number.isFinite(Number(typedLiteralMatch[1]))
          ? Number(typedLiteralMatch[1])
          : null,
      windowStart: null,
      windowEnd: null,
      publicationTimestamp: null,
    };
  }
  return {
    raw: parsed,
    value:
      typeof parsed.value === "number"
        ? parsed.value
        : Number.isFinite(Number(parsed.value))
        ? Number(parsed.value)
        : null,
    windowStart:
      Number.isFinite(Number(parsed.windowStart))
        ? Number(parsed.windowStart)
        : Number.isFinite(Number(parsed.window_start))
        ? Number(parsed.window_start)
        : null,
    windowEnd:
      Number.isFinite(Number(parsed.windowEnd))
        ? Number(parsed.windowEnd)
        : Number.isFinite(Number(parsed.window_end))
        ? Number(parsed.window_end)
        : null,
    publicationTimestamp:
      Number.isFinite(Number(parsed.timestamp))
        ? Number(parsed.timestamp)
        : null,
  };
}

function readFetchingComparableSummary(runRoot) {
  const summaryPath = path.join(
    runRoot,
    "benchmark_window_cap_summary_consumer_0.json",
  );
  if (!fs.existsSync(summaryPath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(summaryPath, "utf8"));
}

function enrichFetchingDeliveries(runRoot, firstDeliveries, allDeliveries) {
  const summary = readFetchingComparableSummary(runRoot);
  if (!summary || !Number.isFinite(summary.resultValue)) {
    return { firstDeliveries, allDeliveries };
  }
  const enrich = (entry) => ({
    ...entry,
    windowStart: summary.windowStart,
    windowEnd: summary.windowEnd,
    resultValue: summary.resultValue,
    sourcePublicationTimestamp: summary.resultEmittedAt,
    payload: entry.payload,
    fetchingSummary: summary,
  });
  return {
    firstDeliveries: firstDeliveries.map(enrich),
    allDeliveries: allDeliveries.map(enrich),
  };
}

async function subscribeConsumers({ registrations, expectedCount, deliveryPath }) {
  const clientRecords = [];
  const firstDeliveryByConsumer = new Map();
  const allDeliveries = [];

  const closeClients = async () => {
    await Promise.all(clientRecords.map(async ({ client, timeout }) => {
      clearTimeout(timeout);
      client.removeAllListeners();
      await new Promise((resolve) => {
        let settled = false;
        const finish = () => {
          if (settled) {
            return;
          }
          settled = true;
          resolve();
        };
        const closeTimeout = setTimeout(finish, 1_000);
        try {
          client.end(true, {}, () => {
            clearTimeout(closeTimeout);
            finish();
          });
        } catch {
          clearTimeout(closeTimeout);
          finish();
        }
      });
    }));
  };

  try {
    await Promise.all(
      registrations.map(({ consumerId, executionId, outputTopic }) => new Promise((resolve, reject) => {
        if (typeof outputTopic !== "string" || outputTopic.trim() === "") {
          reject(new Error(`Invalid output topic for ${consumerId}: ${String(outputTopic)}`));
          return;
        }
        const client = mqtt.connect("mqtt://localhost:1883", {
          clean: true,
          clientId: `${consumerId}-${Math.random().toString(16).slice(2, 10)}`,
        });
        const timeout = setTimeout(() => {
          reject(new Error(`Timed out subscribing ${consumerId}`));
        }, 15_000);
        clientRecords.push({ client, timeout });
        client.on("connect", () => {
          client.subscribe(outputTopic, { qos: 1 }, (error) => {
            clearTimeout(timeout);
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          });
        });
        client.on("message", (topic, payloadBuffer) => {
          if (topic !== outputTopic) {
            return;
          }
          const parsed = parseResultPayload(payloadBuffer.toString("utf8"));
          const event = {
            consumerId,
            executionId,
            sharedOutputTopic: outputTopic,
            windowStart: parsed.windowStart,
            windowEnd: parsed.windowEnd,
            resultValue: parsed.value,
            sourcePublicationTimestamp: parsed.publicationTimestamp,
            consumerReceptionTimestamp: Date.now(),
            payload: parsed.raw,
          };
          allDeliveries.push(event);
          appendNdjson(deliveryPath, event);
          if (!firstDeliveryByConsumer.has(consumerId)) {
            firstDeliveryByConsumer.set(consumerId, event);
          }
        });
        client.on("error", reject);
      })),
    );
  } catch (error) {
    await closeClients();
    throw error;
  }

  return {
    clients: clientRecords.map(({ client }) => client),
    waitForAll: async (timeoutMs) => {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        if (firstDeliveryByConsumer.size >= expectedCount) {
          return {
            firstDeliveries: Array.from(firstDeliveryByConsumer.values()),
            allDeliveries,
          };
        }
        await delay(POLL_INTERVAL_MS);
      }
      throw new Error(
        `Timed out waiting for ${expectedCount} consumer deliveries; got ${firstDeliveryByConsumer.size}`,
      );
    },
    close: closeClients,
  };
}

function collectProfileSummaries(runRoot) {
  return fs
    .readdirSync(runRoot, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.startsWith("hive_profile_summary."))
    .map((entry) => {
      const profilePath = path.join(runRoot, entry.name);
      try {
        return JSON.parse(fs.readFileSync(profilePath, "utf8"));
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw new Error(`Failed to parse profile summary ${profilePath}: ${detail}`);
      }
    });
}

function summarizeProfiles(profiles, approach) {
  const beeProfiles = profiles.filter((entry) =>
    approach === "approximation"
      ? entry.processRole === "approximation_bee_worker"
      : approach === "chunked"
      ? entry.processRole === "chunked_bee_worker"
      : entry.processRoleGroup === "orchestrator",
  );
  const total = (field) =>
    beeProfiles.reduce((sum, profile) => sum + Number(profile[field] || 0), 0);
  return {
    mqttMessagesReceived: total("mqtt_messages_received"),
    emittedResults: total("emitted_results"),
    reconstructedSuperqueryResults: total("reconstructed_superquery_results"),
    comparableWindowsEmitted: total("comparable_windows_emitted"),
    chunkGroupsCompleted: total("chunk_groups_completed"),
  };
}

function readMqttTraffic(runRoot) {
  return readNdjson(path.join(runRoot, "mqtt_traffic.ndjson"));
}

function buildScenarioEnv({
  approach,
  kValue,
  iteration,
  runRoot,
  replayAnchor,
  scenarioId,
  targetWindows = TARGET_WINDOWS,
}) {
  const replayEnv = createBenchmarkReplayRunEnv(process.env);
  const benchmarkMinTimestamp = Number.parseInt(replayAnchor, 10);
  const benchmarkMaxTimestamp =
    benchmarkMinTimestamp + (REQUIRED_REPLAY_DURATION_SECONDS * 1000);
  const topicPrefix = [
    "production-experiment3",
    approach,
    `K${kValue}`,
    `iteration${iteration}`,
  ].join("/");
  return replayEnv.withBenchmarkReplayEnv({
    ...process.env,
    LOG_PATH: runRoot,
    DATA_PATH: "custom_patterns/low_variability",
    SESSION_ID: `prod_e3_${approach}_K${kValue}_iteration${iteration}`,
    BENCHMARK_SCENARIO: "experiment3-production-same-superqueries",
    BENCHMARK_SCENARIO_ID: scenarioId,
    BENCHMARK_SCALE: `K${kValue}`,
    BENCHMARK_APPROACH: approach,
    BENCHMARK_ITERATION: String(iteration),
    STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX: topicPrefix,
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
    STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(targetWindows),
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(
      REQUIRED_REPLAY_DURATION_SECONDS,
    ),
    STREAMING_QUERY_HIVE_BENCHMARK_START_TIME: replayAnchor,
    STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: replayAnchor,
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN: String(benchmarkMinTimestamp),
    STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX: String(benchmarkMaxTimestamp),
    OUTPUT_WINDOW_RANGE: String(OUTPUT_WINDOW_RANGE_MS),
    OUTPUT_WINDOW_STEP: String(OUTPUT_WINDOW_STEP_MS),
    SUB_WINDOW_RANGE: String(SUB_WINDOW_RANGE_MS),
    SUB_WINDOW_STEP: String(SUB_WINDOW_STEP_MS),
    STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
    STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "1",
    STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0",
    STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1",
    STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0",
    HIVE_PROFILE: "1",
  });
}

async function registerConsumers({
  approach,
  kValue,
  port,
  topicPrefix,
  registrationPath,
  variantMode = false,
  approximationConfig,
}) {
  const registrations = [];
  for (let consumerIndex = 1; consumerIndex <= kValue; consumerIndex += 1) {
    const requestBody = buildRegistrationBody({
      approach,
      topicPrefix,
      consumerIndex,
      variantMode,
      approximationConfig,
    });
    const registration = await postRegistration(port, requestBody);
    appendNdjson(registrationPath, registration);
    registrations.push(registration);
  }
  return registrations;
}

function computeCorrectness({
  approach,
  firstDeliveries,
  referenceValue,
}) {
  const resultValues = firstDeliveries
    .map((entry) => entry.resultValue)
    .filter(Number.isFinite);
  const comparableCount = resultValues.length;
  const errors = Number.isFinite(referenceValue)
    ? resultValues.map((value) => Math.abs(value - referenceValue))
    : [];
  const mae =
    errors.length > 0
      ? errors.reduce((sum, value) => sum + value, 0) / errors.length
      : null;
  const maxAbsoluteError = errors.length > 0 ? Math.max(...errors) : null;
  const mape =
    errors.length > 0 && Math.abs(referenceValue) > 1e-12
      ? errors.reduce((sum, value) => sum + (value / Math.abs(referenceValue)), 0) /
        errors.length
      : 0;
  return {
    approach,
    comparableCount,
    expectedCount: firstDeliveries.length,
    exactCount:
      approach === "chunked"
        ? errors.filter((value) => value <= 1e-9).length
        : null,
    mae,
    medianAbsoluteError: errors.length > 0 ? median(errors) : null,
    maxAbsoluteError,
    mape,
    maxPercentageError:
      errors.length > 0 && Math.abs(referenceValue) > 1e-12
        ? Math.max(...errors.map((value) => value / Math.abs(referenceValue)))
        : 0,
  };
}

function summarizeMqttTraffic(traffic, registrations) {
  const sharedTopics = new Set(registrations.map((entry) => entry.outputTopic));
  const producerPublications = traffic.filter((entry) =>
    entry.messageType === "reusable_result" || entry.messageType === "chunk_result",
  );
  const finalPublications = traffic.filter((entry) =>
    entry.messageType === "superquery_result" && sharedTopics.has(entry.topic),
  );
  return {
    rawInputPublications: traffic.filter((entry) => entry.messageType === "raw_input_stream").length,
    reusableResultPublications: traffic.filter((entry) => entry.messageType === "reusable_result").length,
    chunkResultPublications: traffic.filter((entry) => entry.messageType === "chunk_result").length,
    producerPublications: producerPublications.length,
    finalPublications: finalPublications.length,
    producerTopics: [...new Set(producerPublications.map((entry) => entry.topic))],
    finalTopics: [...new Set(finalPublications.map((entry) => entry.topic))],
  };
}

function buildRuntimeSummary({ registrations, firstDeliveries, processMetrics, mqttSummary, profiles, approach }) {
  const uniqueExecutionIds = [...new Set(registrations.map((entry) => entry.executionId).filter(Boolean))];
  const uniqueOutputTopics = [...new Set(registrations.map((entry) => entry.outputTopic).filter(Boolean))];
  const reuseHits = registrations.filter((entry) => entry.reuseHit === true).length;
  const profileSummary = summarizeProfiles(profiles, approach);
  return {
    cpuPct: processMetrics.averageCpuPct,
    rssMiB: processMetrics.peakRssMb,
    processCount: processMetrics.peakProcessCount,
    producerTopics: mqttSummary.producerTopics,
    producerTopicCount: mqttSummary.producerTopics.length,
    uniqueExecutionIds,
    executionCount: uniqueExecutionIds.length,
    reuseHits,
    uniqueOutputTopics,
    sharedTopicCount: uniqueOutputTopics.length,
    uniqueConsumersDelivered: new Set(firstDeliveries.map((entry) => entry.consumerId)).size,
    totalDeliveryEvents: firstDeliveries.length,
    finalPublications: mqttSummary.finalPublications,
    operatorReceipts: profileSummary.mqttMessagesReceived,
    resultsComputed:
      approach === "chunked"
        ? profileSummary.reconstructedSuperqueryResults
        : profileSummary.emittedResults,
    comparableWindowsEmitted: profileSummary.comparableWindowsEmitted,
    chunkGroupsCompleted: profileSummary.chunkGroupsCompleted,
  };
}

async function runMainCell({
  resultRoot,
  approach,
  kValue,
  iteration,
  replayAnchor,
  deliveryTimeoutMs,
}) {
  const scenarioId = buildScenarioKey(kValue, iteration);
  const runRoot = buildRunRoot(resultRoot, "raw", approach, kValue, iteration);
  ensureDir(runRoot);
  const registrationPath = path.join(runRoot, "registration_events.ndjson");
  const deliveryPath = path.join(runRoot, "consumer_delivery_events.ndjson");
  const env = buildScenarioEnv({
    approach,
    kValue,
    iteration,
    runRoot,
    replayAnchor,
    scenarioId,
  });
  const serverOutPath = path.join(runRoot, "server.stdout.log");
  const serverErrPath = path.join(runRoot, "server.stderr.log");
  const publisherOutPath = path.join(runRoot, "publisher.stdout.log");
  const publisherErrPath = path.join(runRoot, "publisher.stderr.log");

  await cleanupStaleBenchmarkProcesses({ logger: () => undefined });

  const server = spawn("node", [path.join(REPO_ROOT, "dist", "startHTTPServer.js")], {
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
    const runtimeBuild = verifyBuildManifestOrThrow();
    await waitForServerLog(serverOutPath);
    await waitForRegisterReady(CONTROL_PORT);

    resourceSampler = startProcessTreeResourceLogging(
      path.join(runRoot, "process_tree_resource_usage.csv"),
      server.pid,
      100,
    );

    const topicPrefix = env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX;
    const registrations = await registerConsumers({
      approach,
      kValue,
      port: CONTROL_PORT,
      topicPrefix,
      registrationPath,
      approximationConfig:
        approach === "approximation"
          ? {
              policy: "rate-based-completed-window",
              completedWindowMode: true,
              earlyTriggerMode: false,
            }
          : undefined,
    });

    subscribers = await subscribeConsumers({
      registrations,
      expectedCount: kValue,
      deliveryPath,
    });

    publisher = spawn("node", [path.join(REPO_ROOT, "dist", "streamer", "src", "publish.js")], {
      cwd: REPO_ROOT,
      env: {
        ...env,
        DATA_PATH: "custom_patterns/low_variability",
      },
      detached: true,
      stdio: [
        "ignore",
        fs.openSync(publisherOutPath, "a"),
        fs.openSync(publisherErrPath, "a"),
      ],
    });

    const deliveries = await subscribers.waitForAll(deliveryTimeoutMs);
    const {
      firstDeliveries,
      allDeliveries,
    } = approach === "fetching"
      ? enrichFetchingDeliveries(runRoot, deliveries.firstDeliveries, deliveries.allDeliveries)
      : deliveries;
    await terminateChildProcessTree(publisher, {
      logger: () => undefined,
      termWaitMs: 3_000,
      killWaitMs: 3_000,
    });
    publisher = null;
    await delay(ARTIFACT_SETTLE_TIMEOUT_MS);

    const processMetrics = readProcessTreeMetrics(runRoot);
    const mqttTraffic = readMqttTraffic(runRoot);
    const profiles = collectProfileSummaries(runRoot);
    const mqttSummary = summarizeMqttTraffic(mqttTraffic, registrations);
    const runtimeSummary = buildRuntimeSummary({
      registrations,
      firstDeliveries,
      processMetrics,
      mqttSummary,
      profiles,
      approach,
    });
    const cellSummary = {
      approach,
      kValue,
      iteration,
      scenarioId,
      replayAnchor,
      runRoot,
      registrations,
      firstDeliveries,
      allDeliveries,
      runtimeBuild,
      processMetrics,
      mqttSummary,
      runtimeSummary,
    };
    writeJson(path.join(runRoot, "cell_summary.json"), cellSummary);
    return cellSummary;
  } finally {
    await subscribers?.close().catch?.(() => undefined);
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
      termWaitMs: 1_000,
      killWaitMs: 1_000,
    });
    await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  }
}

async function runMainMatrix(args, resultRoot) {
  const anchors = createScenarioReplayAnchors({
    kValues: args.kValues,
    iterations: args.iterations,
    baseAnchorMs: args.baseAnchorMs ?? Date.now(),
  });
  const combinations = buildCombinationMatrix({
    approaches: args.approaches,
    kValues: args.kValues,
    iterations: args.iterations,
  });
  const cells = [];
  const fetchingByScenario = new Map();
  for (const { approach, kValue, iteration } of combinations) {
    const scenarioId = buildScenarioKey(kValue, iteration);
    const replayAnchor = anchors[scenarioId];
    const cell = await runMainCell({
      resultRoot,
      approach,
      kValue,
      iteration,
      replayAnchor,
      deliveryTimeoutMs: args.timeoutMs,
    });
    if (approach === "fetching") {
      const reference = cell.firstDeliveries[0]?.resultValue ?? null;
      fetchingByScenario.set(scenarioId, reference);
    }
    const referenceValue = fetchingByScenario.get(scenarioId) ?? null;
    cell.correctness = computeCorrectness({
      approach,
      firstDeliveries: cell.firstDeliveries,
      referenceValue,
    });
    writeJson(path.join(cell.runRoot, "cell_summary.json"), cell);
    cells.push(cell);
  }
  return { anchors, cells };
}

async function runAdversarialValidations(resultRoot) {
  const scenarioResults = [];
  const concurrencyResults = [];
  const anchors = createScenarioReplayAnchors({
    kValues: DEFAULT_ADVERSARIAL_K_VALUES,
    iterations: 1,
    baseAnchorMs: Date.now() + 60 * 60 * 1000,
  });

  for (const kValue of DEFAULT_ADVERSARIAL_K_VALUES) {
    const scenarioId = buildScenarioKey(kValue, 1);
    for (const approach of ["approximation", "chunked"]) {
      const runRoot = buildRunRoot(resultRoot, "adversarial", approach, kValue, 1);
      ensureDir(runRoot);
      const env = buildScenarioEnv({
        approach,
        kValue,
        iteration: 1,
        runRoot,
        replayAnchor: anchors[scenarioId],
        scenarioId,
      });
      await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
      const server = spawn("node", [path.join(REPO_ROOT, "dist", "startHTTPServer.js")], {
        cwd: REPO_ROOT,
        env,
        detached: true,
        stdio: [
          "ignore",
          fs.openSync(path.join(runRoot, "server.stdout.log"), "a"),
          fs.openSync(path.join(runRoot, "server.stderr.log"), "a"),
        ],
      });
      let publisher = null;
      let subscribers = null;
      try {
        await waitForRegisterReady(CONTROL_PORT);
        const registrations = await registerConsumers({
          approach,
          kValue,
          port: CONTROL_PORT,
          topicPrefix: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
          registrationPath: path.join(runRoot, "registration_events.ndjson"),
          variantMode: true,
          approximationConfig:
            approach === "approximation"
              ? defaultApproximationConfig()
              : undefined,
        });
        subscribers = await subscribeConsumers({
          registrations,
          expectedCount: kValue,
          deliveryPath: path.join(runRoot, "consumer_delivery_events.ndjson"),
        });
        publisher = spawn("node", [path.join(REPO_ROOT, "dist", "streamer", "src", "publish.js")], {
          cwd: REPO_ROOT,
          env,
          detached: true,
          stdio: [
            "ignore",
            fs.openSync(path.join(runRoot, "publisher.stdout.log"), "a"),
            fs.openSync(path.join(runRoot, "publisher.stderr.log"), "a"),
          ],
        });
        const { firstDeliveries } = await subscribers.waitForAll(DELIVERY_TIMEOUT_MS);
        scenarioResults.push({
          kValue,
          approach,
          reuseHits: registrations.filter((entry) => entry.reuseHit).length,
          executions: [...new Set(registrations.map((entry) => entry.executionId))].length,
          deliveries: firstDeliveries.length,
          mutuallyContained: registrations.slice(1).every((entry) => entry.reuseDecision?.mutuallyContained === true),
        });
      } finally {
        await subscribers?.close().catch?.(() => undefined);
        if (publisher) {
          await terminateChildProcessTree(publisher, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
        }
        await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
        await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
      }
    }
  }

  for (const approach of ["approximation", "chunked"]) {
    const runRoot = buildRunRoot(resultRoot, "adversarial-concurrency", approach, 10, 1);
    ensureDir(runRoot);
    const env = buildScenarioEnv({
      approach,
      kValue: 10,
      iteration: 1,
      runRoot,
      replayAnchor: `${Date.now() + 2 * 60 * 60 * 1000}`,
      scenarioId: `${approach}-concurrency`,
    });
    await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
    const server = spawn("node", [path.join(REPO_ROOT, "dist", "startHTTPServer.js")], {
      cwd: REPO_ROOT,
      env,
      detached: true,
      stdio: [
        "ignore",
        fs.openSync(path.join(runRoot, "server.stdout.log"), "a"),
        fs.openSync(path.join(runRoot, "server.stderr.log"), "a"),
      ],
    });
    let publisher = null;
    let subscribers = null;
    try {
      await waitForRegisterReady(CONTROL_PORT);
      const topicPrefix = env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX;
      const requests = Array.from({ length: 10 }, (_, index) => ({
        ...buildRegistrationBody({
          approach,
          topicPrefix,
          consumerIndex: index + 1,
          variantMode: true,
          approximationConfig:
            approach === "approximation"
              ? defaultApproximationConfig()
              : undefined,
        }),
        id: `${approach}-concurrent-query-${index + 1}`,
        consumer_id: `${approach}-concurrent-consumer-${index + 1}`,
        r2s_topic: `concurrent-consumer-${index + 1}-output-topic`,
        data_topic: `concurrent-consumer-${index + 1}-output-topic`,
        rspql_query: buildEquivalentVariant(
          topicPrefix,
          `concurrent-consumer-${index + 1}-output-topic`,
          index + 1,
        ),
      }));
      const responses = await Promise.all(
        requests.map((body) =>
          fetch(`http://localhost:${CONTROL_PORT}/register`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          }).then(async (response) => ({
            status: response.status,
            body: await response.json(),
          })),
        ),
      );
      const registrations = responses.map((response, index) => ({
        consumerId: requests[index].consumer_id,
        outputTopic: response.body.outputTopic,
        executionId: response.body.executionId,
        reuseHit: response.body.reuseHit,
      }));
      subscribers = await subscribeConsumers({
        registrations,
        expectedCount: 10,
        deliveryPath: path.join(runRoot, "consumer_delivery_events.ndjson"),
      });
      publisher = spawn("node", [path.join(REPO_ROOT, "dist", "streamer", "src", "publish.js")], {
        cwd: REPO_ROOT,
        env,
        detached: true,
        stdio: [
          "ignore",
          fs.openSync(path.join(runRoot, "publisher.stdout.log"), "a"),
          fs.openSync(path.join(runRoot, "publisher.stderr.log"), "a"),
        ],
      });
      const { firstDeliveries } = await subscribers.waitForAll(DELIVERY_TIMEOUT_MS);
      concurrencyResults.push({
        approach,
        executions: [...new Set(registrations.map((entry) => entry.executionId))].length,
        reuseHits: registrations.filter((entry) => entry.reuseHit).length,
        deliveries: firstDeliveries.length,
      });
    } finally {
      await subscribers?.close().catch?.(() => undefined);
      if (publisher) {
        await terminateChildProcessTree(publisher, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
      }
      await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
      await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
    }
  }

  return {
    structuralReuse: scenarioResults,
    concurrency: concurrencyResults,
  };
}

async function runLateSubscriberValidation(resultRoot, approach) {
  const runRoot = buildRunRoot(resultRoot, `late-subscriber-${approach}`, approach, 2, 1);
  ensureDir(runRoot);
  const replayAnchor = `${Date.now() + 3 * 60 * 60 * 1000}`;
  const env = buildScenarioEnv({
    approach,
    kValue: 2,
    iteration: 1,
    runRoot,
    replayAnchor,
    scenarioId: `${approach}-late-subscriber`,
    targetWindows: 2,
  });
  await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  const server = spawn("node", [SERVER_EXECUTABLE_PATH], {
    cwd: REPO_ROOT,
    env,
    detached: true,
    stdio: [
      "ignore",
      fs.openSync(path.join(runRoot, "server.stdout.log"), "a"),
      fs.openSync(path.join(runRoot, "server.stderr.log"), "a"),
    ],
  });
  let subscribers = null;
  let publisher = null;
  try {
    verifyBuildManifestOrThrow();
    await waitForServerLog(path.join(runRoot, "server.stdout.log"));
    await waitForRegisterReady(CONTROL_PORT);
    const firstRegs = await registerConsumers({
      approach,
      kValue: 1,
      port: CONTROL_PORT,
      topicPrefix: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
      registrationPath: path.join(runRoot, "registration_events.ndjson"),
      approximationConfig:
        approach === "approximation"
          ? defaultApproximationConfig()
          : undefined,
    });
    subscribers = await subscribeConsumers({
      registrations: firstRegs,
      expectedCount: 1,
      deliveryPath: path.join(runRoot, "consumer_delivery_events.ndjson"),
    });
    publisher = spawn("node", [path.join(REPO_ROOT, "dist", "streamer", "src", "publish.js")], {
      cwd: REPO_ROOT,
      env,
      detached: true,
      stdio: [
        "ignore",
        fs.openSync(path.join(runRoot, "publisher.stdout.log"), "a"),
        fs.openSync(path.join(runRoot, "publisher.stderr.log"), "a"),
      ],
    });
    const firstDelivery = await subscribers.waitForAll(DELIVERY_TIMEOUT_MS);
    const lateRegistration = await postRegistration(
      CONTROL_PORT,
      buildRegistrationBody({
        approach,
        topicPrefix: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
        consumerIndex: 2,
        approximationConfig:
          approach === "approximation"
            ? defaultApproximationConfig()
            : undefined,
      }),
    );
    appendNdjson(path.join(runRoot, "registration_events.ndjson"), lateRegistration);
    const lateSubscribers = await subscribeConsumers({
      registrations: [lateRegistration],
      expectedCount: 1,
      deliveryPath: path.join(runRoot, "consumer_delivery_events.ndjson"),
    });
    const lateDelivery = await lateSubscribers.waitForAll(DELIVERY_TIMEOUT_MS);
    await lateSubscribers.close();
    return {
      approach,
      firstExecutionId: firstRegs[0].executionId,
      secondExecutionId: lateRegistration.executionId,
      sameExecution: firstRegs[0].executionId === lateRegistration.executionId,
      sameTopic: firstRegs[0].outputTopic === lateRegistration.outputTopic,
      lateDeliveryCount: lateDelivery.firstDeliveries.length,
      firstDeliveryCount: firstDelivery.firstDeliveries.length,
    };
  } finally {
    await subscribers?.close().catch?.(() => undefined);
    if (publisher) {
      await terminateChildProcessTree(publisher, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
    }
    await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
    await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  }
}

async function runColdRestartValidation(resultRoot, approach) {
  const runRoot = buildRunRoot(resultRoot, `cold-restart-${approach}`, approach, 1, 1);
  ensureDir(runRoot);
  const replayAnchor = `${Date.now() + 4 * 60 * 60 * 1000}`;
  const env = buildScenarioEnv({
    approach,
    kValue: 1,
    iteration: 1,
    runRoot,
    replayAnchor,
    scenarioId: `${approach}-cold-restart`,
  });
  const startServer = () =>
    spawn("node", [SERVER_EXECUTABLE_PATH], {
      cwd: REPO_ROOT,
      env,
      detached: true,
      stdio: [
        "ignore",
        fs.openSync(path.join(runRoot, "server.stdout.log"), "a"),
        fs.openSync(path.join(runRoot, "server.stderr.log"), "a"),
      ],
    });
  await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  let server = startServer();
  try {
    verifyBuildManifestOrThrow();
    await waitForServerLog(path.join(runRoot, "server.stdout.log"));
    await waitForRegisterReady(CONTROL_PORT);
    const first = await registerConsumers({
      approach,
      kValue: 1,
      port: CONTROL_PORT,
      topicPrefix: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
      registrationPath: path.join(runRoot, "registration_events.ndjson"),
      approximationConfig:
        approach === "approximation"
          ? defaultApproximationConfig()
          : undefined,
    });
    await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
    await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
    server = startServer();
    await waitForServerLog(path.join(runRoot, "server.stdout.log"));
    await waitForRegisterReady(CONTROL_PORT);
    const second = await registerConsumers({
      approach,
      kValue: 1,
      port: CONTROL_PORT,
      topicPrefix: env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX,
      registrationPath: path.join(runRoot, "registration_events.ndjson"),
      approximationConfig:
        approach === "approximation"
          ? defaultApproximationConfig()
          : undefined,
    });
    return {
      approach,
      firstExecutionId: first[0].executionId,
      secondExecutionId: second[0].executionId,
      reusedStaleExecution: first[0].executionId === second[0].executionId,
      secondExecutionCreated: second[0].executionCreated,
      secondReuseHit: second[0].reuseHit,
    };
  } finally {
    await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
    await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  }
}

async function runNegativeControl(resultRoot, label, firstBody, secondBody) {
  const runRoot = buildRunRoot(resultRoot, `negative-${label}`, firstBody.approach, 2, 1);
  ensureDir(runRoot);
  const replayAnchor = `${Date.now() + 5 * 60 * 60 * 1000}`;
  const env = buildScenarioEnv({
    approach: firstBody.approach,
    kValue: 2,
    iteration: 1,
    runRoot,
    replayAnchor,
    scenarioId: `negative-${label}`,
  });
  await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  const server = spawn("node", [SERVER_EXECUTABLE_PATH], {
    cwd: REPO_ROOT,
    env,
    detached: true,
    stdio: [
      "ignore",
      fs.openSync(path.join(runRoot, "server.stdout.log"), "a"),
      fs.openSync(path.join(runRoot, "server.stderr.log"), "a"),
    ],
  });
  try {
    verifyBuildManifestOrThrow();
    await waitForServerLog(path.join(runRoot, "server.stdout.log"));
    await waitForRegisterReady(CONTROL_PORT);
    const responses = [];
    for (const body of [firstBody, secondBody]) {
      const response = await fetch(`http://localhost:${CONTROL_PORT}/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      responses.push(await response.json());
    }
    return {
      label,
      firstExecutionId: responses[0].executionId,
      secondExecutionId: responses[1].executionId,
      secondReuseHit: responses[1].reuseHit,
      secondExecutionCreated: responses[1].executionCreated,
      forwardContained: responses[1].reuseDecision?.forwardContained ?? null,
      reverseContained: responses[1].reuseDecision?.reverseContained ?? null,
      mutuallyContained: responses[1].reuseDecision?.mutuallyContained ?? null,
    };
  } finally {
    await terminateChildProcessTree(server, { logger: () => undefined, termWaitMs: 1_000, killWaitMs: 1_000 });
    await cleanupStaleBenchmarkProcesses({ logger: () => undefined });
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const resultRoot = buildResultRoot();
  ensureDir(resultRoot);

  const buildIdentity = ensureFreshProductionBuild(resultRoot);
  const repoState = {
    ...buildIdentity.repoState,
    createdAt: new Date().toISOString(),
  };
  writeJson(path.join(resultRoot, "repo_state.json"), repoState);

  const mainMatrix = args.skipMainMatrix
    ? { anchors: {}, cells: [] }
    : await runMainMatrix(args, resultRoot);
  const adversarial = args.skipAdversarial
    ? null
    : await runAdversarialValidations(resultRoot);
  const lateSubscriber = args.skipAdversarial
    ? null
    : {
        approximation: await runLateSubscriberValidation(resultRoot, "approximation"),
        chunked: await runLateSubscriberValidation(resultRoot, "chunked"),
      };
  const coldRestart = args.skipAdversarial
    ? null
    : {
        approximation: await runColdRestartValidation(resultRoot, "approximation"),
        chunked: await runColdRestartValidation(resultRoot, "chunked"),
      };

  const negativeControls = args.skipAdversarial
    ? null
    : await (async () => {
        const prefix = "production-experiment3/negative-control";
        const { narrower: oneWayNarrower, broader: oneWayBroader } =
          buildOneWayContainmentPair(prefix);
        const approxBase = {
          id: "negative-approx-base",
          consumer_id: "negative-approx-base",
          approach: "approximation",
          rspql_query: buildQuery({ approach: "approximation", prefix, outputIri: "base-output" }),
          r2s_topic: "base-output",
          data_topic: "base-output",
          approximation_config: defaultApproximationConfig(),
        };
        const approxDifferentPolicy = {
          ...approxBase,
          id: "negative-approx-different-policy",
          consumer_id: "negative-approx-different-policy",
          approximation_config: defaultApproximationConfig({
            policy: "different-policy",
          }),
        };
        const approxDifferentRate = {
          ...approxBase,
          id: "negative-approx-different-rate",
          consumer_id: "negative-approx-different-rate",
          approximation_config: {
            ...defaultApproximationConfig(),
            rate: 0.5,
          },
        };
        const chunkBase = {
          id: "negative-chunk-base",
          consumer_id: "negative-chunk-base",
          approach: "chunked",
          rspql_query: buildQuery({ approach: "chunked", prefix, outputIri: "base-output" }),
          r2s_topic: "base-output",
          data_topic: "base-output",
        };
        const chunkDifferentRange = {
          ...chunkBase,
          id: "negative-chunk-range",
          consumer_id: "negative-chunk-range",
          rspql_query: buildQuery({
            approach: "chunked",
            prefix,
            outputIri: "different-range-output",
            windowRangeMs: OUTPUT_WINDOW_RANGE_MS + 60000,
          }),
          r2s_topic: "different-range-output",
          data_topic: "different-range-output",
        };
        const chunkDifferentStep = {
          ...chunkBase,
          id: "negative-chunk-step",
          consumer_id: "negative-chunk-step",
          rspql_query: buildQuery({
            approach: "chunked",
            prefix,
            outputIri: "different-step-output",
            windowStepMs: OUTPUT_WINDOW_STEP_MS / 2,
          }),
          r2s_topic: "different-step-output",
          data_topic: "different-step-output",
        };
        const chunkDifferentStream = {
          ...chunkBase,
          id: "negative-chunk-stream",
          consumer_id: "negative-chunk-stream",
          rspql_query: buildQuery({
            approach: "chunked",
            prefix,
            outputIri: "different-stream-output",
            includeSmartphone: false,
          }),
          r2s_topic: "different-stream-output",
          data_topic: "different-stream-output",
        };
        const chunkDifferentAggregation = {
          ...chunkBase,
          id: "negative-chunk-aggregation",
          consumer_id: "negative-chunk-aggregation",
          rspql_query: buildQuery({
            approach: "chunked",
            prefix,
            outputIri: "different-aggregation-output",
            aggregationFunction: "SUM",
          }),
          r2s_topic: "different-aggregation-output",
          data_topic: "different-aggregation-output",
        };
        const oneWayBroad = {
          id: "negative-one-way-broad",
          consumer_id: "negative-one-way-broad",
          approach: "chunked",
          rspql_query: oneWayBroader,
          r2s_topic: "one-way-broad-output",
          data_topic: "one-way-broad-output",
        };
        const oneWayNarrow = {
          id: "negative-one-way-narrow",
          consumer_id: "negative-one-way-narrow",
          approach: "chunked",
          rspql_query: oneWayNarrower,
          r2s_topic: "one-way-narrow-output",
          data_topic: "one-way-narrow-output",
        };
        return [
          await runNegativeControl(resultRoot, "different-approximation-policy", approxBase, approxDifferentPolicy),
          await runNegativeControl(resultRoot, "different-approximation-rate", approxBase, approxDifferentRate),
          await runNegativeControl(resultRoot, "different-range", chunkBase, chunkDifferentRange),
          await runNegativeControl(resultRoot, "different-step", chunkBase, chunkDifferentStep),
          await runNegativeControl(resultRoot, "different-aggregation", chunkBase, chunkDifferentAggregation),
          await runNegativeControl(resultRoot, "different-stream", chunkBase, chunkDifferentStream),
          await runNegativeControl(resultRoot, "one-way-containment", oneWayBroad, oneWayNarrow),
        ];
      })();

  const summary = {
    resultRoot,
    repoState,
    buildIdentity: {
      manifestPath: buildIdentity.buildManifestPath,
      manifest: buildIdentity.buildManifest,
      serverExecutable: SERVER_EXECUTABLE_PATH,
    },
    mainMatrix: mainMatrix.cells.map((cell) => ({
      approach: cell.approach,
      kValue: cell.kValue,
      scenarioId: cell.scenarioId,
      cpuPct: cell.runtimeSummary.cpuPct,
      rssMiB: cell.runtimeSummary.rssMiB,
      processCount: cell.runtimeSummary.processCount,
      producerTopicCount: cell.runtimeSummary.producerTopicCount,
      executionCount: cell.runtimeSummary.executionCount,
      reuseHits: cell.runtimeSummary.reuseHits,
      deliveries: cell.runtimeSummary.uniqueConsumersDelivered,
      finalPublications: cell.runtimeSummary.finalPublications,
      operatorReceipts: cell.runtimeSummary.operatorReceipts,
      resultsComputed: cell.runtimeSummary.resultsComputed,
      forwardChecks: cell.registrations.slice(1).length,
      reverseChecks: cell.registrations.slice(1).length,
      cacheHits: cell.registrations.slice(1).filter((entry) => entry.reuseDecision?.cacheHit).length,
      correctness: cell.correctness,
    })),
    adversarial,
    lateSubscriber,
    coldRestart,
    negativeControls,
  };
  writeJson(path.join(resultRoot, "production_experiment3_summary.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
