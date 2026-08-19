import fs from "fs";
import path from "path";
import { Orchestrator } from "../orchestrator/Orchestrator";
import { CSVLogger } from "../util/logger/CSVLogger";
import { profileCount } from "../util/profiling";
import {
  QueryReuseRegistry,
  ReuseMode,
} from "../reuse/QueryReuseRegistry";
import { getCanonicalRSPQLQueryHash } from "../reuse/normalizeRSPQLForExactReuse";
import {
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getSubWindowRange,
  getSubWindowStep,
  buildBenchmarkTopicName,
  buildBenchmarkStreamIri,
  buildSubQuerySelectClause,
  buildOutputSelectClause,
  getKScalingReuseMode,
  isExactFinalResultReuseEnabled,
} from "../util/runtimeConfig";

function getSharedFinalResultTopic(canonicalQueryHash: string): string {
  return `hive/results/final/${canonicalQueryHash}`;
}

function ensureSymlinkAlias(targetPath: string, aliasPath: string): void {
  try {
    if (fs.existsSync(aliasPath) || fs.lstatSync(aliasPath)) {
      fs.unlinkSync(aliasPath);
    }
  } catch {
    // ignore missing alias
  }

  fs.mkdirSync(path.dirname(aliasPath), { recursive: true });
  fs.symlinkSync(path.basename(targetPath), aliasPath);
}

function ensureExactFinalBenchmarkAliases(
  logRoot: string,
  ownerConsumerIndex: number,
  consumerIndex: number,
): void {
  const ownerSuffix = `_consumer_${ownerConsumerIndex}`;
  const consumerSuffix = `_consumer_${consumerIndex}`;
  const aliases = [
    ["chunked_latency_log", ".csv"],
    ["chunked_window_diagnostics", ".csv"],
    ["chunked_parent_partial_latency_log", ".csv"],
  ];

  for (const [prefix, extension] of aliases) {
    ensureSymlinkAlias(
      path.join(logRoot, `${prefix}${ownerSuffix}${extension}`),
      path.join(logRoot, `${prefix}${consumerSuffix}${extension}`),
    );
  }
}

function logReuseDecision(params: {
  consumerId: string;
  reuseMode: ReuseMode;
  canonicalQueryHash: string;
  resultTopic: string;
  ownerQueryId: string;
}): void {
  console.log(
    `consumer=${params.consumerId} reuse_mode=${params.reuseMode} canonical_query_hash=${params.canonicalQueryHash} result_topic=${params.resultTopic} owner_query_id=${params.ownerQueryId}`,
  );
}

function startResourceUsageLogging(
  filePath = "streaming_query_hive_resource_log.csv",
  intervalMs = 100,
) {
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

async function runChunkedKScalingOrchestrator() {
  const logger = new CSVLogger("streaming_query_chunk_aggregator_log.csv");
  const orchestrator = new Orchestrator("StreamingQueryChunkAggregatorOperator");
  const queryReuseRegistry = new QueryReuseRegistry();

  const aggFunc = getConfiguredAggregation();
  const subWindowRange = getSubWindowRange();
  const subWindowStep = getSubWindowStep();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  const wearableTopicName = buildBenchmarkTopicName("wearableX");
  const smartphoneTopicName = buildBenchmarkTopicName("smartphoneX");
  const wearableStreamIri = buildBenchmarkStreamIri("wearableX");
  const smartphoneStreamIri = buildBenchmarkStreamIri("smartphoneX");

  const K = parseInt(process.env.K_SCALING_K || "1", 10);
  const baseResultTopic = process.env.RESULT_TOPIC || "output";
  const kScalingReuseMode = getKScalingReuseMode();
  const exactFinalReuseEnabled =
    isExactFinalResultReuseEnabled() && kScalingReuseMode === "exact-final";
  const logRoot = process.env.LOG_PATH || ".";

  console.log(`chunked_consumers_started = ${K}`);
  console.log(`K_SCALING_K = ${K}`);
  console.log(`BASE_RESULT_TOPIC = ${baseResultTopic}`);
  console.log(`K_SCALING_REUSE_MODE = ${kScalingReuseMode}`);
  console.log(
    `HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE = ${exactFinalReuseEnabled}`,
  );

  logger.log(
    `Chunked K-scaling orchestrator config: K=${K}, aggregation=${aggFunc}, subWindowRange=${subWindowRange}, subWindowStep=${subWindowStep}`,
  );

  // Add shared chunk-producing sub-queries
  const query1 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggFunc, "WearableX")}
FROM NAMED WINDOW <${wearableStreamIri}> ON STREAM mqtt_broker:${wearableTopicName} [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <${wearableStreamIri}> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:hasTimestamp ?ts .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
}
}
  `;

  const query2 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggFunc, "SmartphoneX")}
FROM NAMED WINDOW <${smartphoneStreamIri}> ON STREAM mqtt_broker:${smartphoneTopicName} [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <${smartphoneStreamIri}> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:hasTimestamp ?ts .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
  `;

  orchestrator.addSubQuery(query1);
  orchestrator.addSubQuery(query2);
  logger.log(
    `Sub-queries added: ${JSON.stringify(orchestrator.getSubQueries())}`,
  );

  // Register and execute K queries via BeeKeeper
  for (let i = 1; i <= K; i++) {
    const consumerId = `consumer_${i}`;
    const ownerQueryId = `sensor_averages_${i}`;
    const registeredQuery = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages_${i}> AS
SELECT ${buildOutputSelectClause(aggFunc)}
FROM NAMED WINDOW <${wearableStreamIri}> ON STREAM mqtt_broker:${wearableTopicName} [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
FROM NAMED WINDOW <${smartphoneStreamIri}> ON STREAM mqtt_broker:${smartphoneTopicName} [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
WHERE {
    {
        WINDOW <${wearableStreamIri}> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:hasTimestamp ?ts .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <${smartphoneStreamIri}> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:hasTimestamp ?ts .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
    `;

    if (exactFinalReuseEnabled) {
      const exactHit = await queryReuseRegistry.findExactFinalResult(registeredQuery);

      if (exactHit) {
        queryReuseRegistry.registerConsumer({
          queryId: exactHit.queryId,
          consumerId,
        });
        profileCount("exact_final_result_reuse_hits");
        profileCount("final_result_topics_reused");
        profileCount("final_result_subscribers_registered");
        profileCount("chunk_reuse_paths_skipped_due_to_exact_hit");
        profileCount("reconstruction_paths_skipped");
        ensureExactFinalBenchmarkAliases(logRoot, 1, i);
        logReuseDecision({
          consumerId,
          reuseMode: "final_result_reuse",
          canonicalQueryHash: exactHit.queryId,
          resultTopic: exactHit.resultTopic,
          ownerQueryId: exactHit.ownerQueryId,
        });
        logger.log(
          `Exact final-result reuse hit: consumer=${consumerId} owner=${exactHit.ownerQueryId} topic=${exactHit.resultTopic}`,
        );
        continue;
      }
    }

    const { canonicalQueryHash } =
      getCanonicalRSPQLQueryHash(registeredQuery);
    const topic = exactFinalReuseEnabled
      ? getSharedFinalResultTopic(canonicalQueryHash)
      : `${baseResultTopic}_consumer_${i}`;
    const workerEnv = {
      RESULT_TOPIC: topic,
      HIVE_PROCESS_ROLE: `chunked_bee_worker_${i}`,
      K_SCALING_CONSUMER_INDEX: String(i),
      HIVE_SKIP_CHUNK_PRODUCER_SPAWNING: i === 1 ? "false" : "true",
    };

    if (exactFinalReuseEnabled) {
      queryReuseRegistry.registerFinalResult({
        query: registeredQuery,
        resultTopic: topic,
        ownerQueryId,
        consumerId,
      });
      profileCount("canonical_query_hashes_seen");
      profileCount("final_result_topics_created");
      profileCount("final_result_subscribers_registered");
    }

    profileCount("chunk_reuse_paths_created");
    profileCount("reconstruction_paths_created");

    logReuseDecision({
      consumerId,
      reuseMode: "chunk_state_reuse",
      canonicalQueryHash,
      resultTopic: topic,
      ownerQueryId,
    });
    console.log(`Executing Chunked consumer ${i}: topic=${topic} queryId=${ownerQueryId} skipSpawning=${workerEnv.HIVE_SKIP_CHUNK_PRODUCER_SPAWNING}`);
    orchestrator["beeKeeper"].executeQuery(
      registeredQuery,
      topic,
      "StreamingQueryChunkAggregatorOperator",
      orchestrator.getSubQueries(),
      workerEnv
    );
  }

  logger.log(`Initialized K=${K} Chunked queries.`);

  let shuttingDown = false;
  async function shutdown(reason: string, exitCode = 0) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    try {
      await orchestrator.stop?.();
    } catch (err) {
      console.error("[shutdown] orchestrator.stop failed", err);
    }
    process.exit(exitCode);
  }

  process.on("SIGINT", () => void shutdown("SIGINT", 130));
  process.on("SIGTERM", () => void shutdown("SIGTERM", 143));
  process.on("uncaughtException", (err) => {
    console.error("[fatal] uncaughtException", err);
    void shutdown("uncaughtException", 1);
  });
  process.on("unhandledRejection", (err) => {
    console.error("[fatal] unhandledRejection", err);
    void shutdown("unhandledRejection", 1);
  });
  process.on("exit", (code) => {
    console.log(`[exit] code=${code}`);
  });
}

startResourceUsageLogging();
runChunkedKScalingOrchestrator().catch((error) => {
  console.error("Error in chunked K-scaling orchestrator:", error);
});
