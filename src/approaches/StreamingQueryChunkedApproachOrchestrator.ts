import fs from "fs";
import mqtt from "mqtt";
import { Orchestrator } from "../orchestrator/Orchestrator";

import { CSVLogger } from "../util/logger/CSVLogger";
import {
  buildBenchmarkTopicName,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getSubWindowRange,
  getSubWindowStep,
} from "../util/runtimeConfig";
import CONFIG from "../config/httpServerConfig.json";
import {
  buildQueryTargetScalingSubQuery,
  buildQueryTargetScalingSuperQuery,
  getConfiguredBenchmarkTargets,
} from "../util/queryTargets";
import { resourceTraceSnapshot } from "../util/resourceTrace";
import { writeStageProfileArtifact } from "../util/profiling";
/**
 *
 */
async function StreamingQueryHiveApproachOrchestrator() {
  process.env.HIVE_PROCESS_ROLE =
    process.env.HIVE_PROCESS_ROLE || "chunked_orchestrator";
  resourceTraceSnapshot("startup", "chunked orchestrator boot");
  const logger = new CSVLogger("streaming_query_chunk_aggregator_log.csv");
  const orchestrator = new Orchestrator(
    "StreamingQueryChunkAggregatorOperator",
  );

  const aggFunc = getConfiguredAggregation();
  const subWindowRange = getSubWindowRange();
  const subWindowStep = getSubWindowStep();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  const targets = getConfiguredBenchmarkTargets();

  logger.log(
    `Chunked orchestrator config: aggregation=${aggFunc}, subWindowRange=${subWindowRange}, subWindowStep=${subWindowStep}, targets=${targets.map((target) => target.name).join(",")}`,
  );

  for (const target of targets) {
    orchestrator.addSubQuery(
      buildQueryTargetScalingSubQuery(
        target,
        aggFunc,
        subWindowRange,
        subWindowStep,
      ),
    );
  }
  logger.log(
    `Sub-queries added: ${JSON.stringify(orchestrator.getSubQueries())}`,
  );
  const registeredQuery = buildQueryTargetScalingSuperQuery(
    targets,
    aggFunc,
    outputWindowRange,
    outputWindowStep,
  );
  logger.log("Registered Query");
  orchestrator.registerQuery(registeredQuery);
  console.log("Registered query:", orchestrator.getRegisteredQuery());
  // -------------------------------------------------------------
  // Run sub-queries
  // Run registered query
  orchestrator.runRegisteredQuery();
  if (["1", "true", "yes", "on"].includes((process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY || "").trim().toLowerCase())) {
    const controlTopic = buildBenchmarkTopicName("__benchmark_control__");
    const controlClient = mqtt.connect(CONFIG.mqttBroker);
    let shutdownStarted = false;

    const shutdown = async () => {
      if (shutdownStarted) {
        return;
      }
      shutdownStarted = true;
      try {
        await orchestrator.stop();
      } catch (error) {
        console.error("Error stopping chunked orchestrator:", error);
      } finally {
        writeStageProfileArtifact();
        try {
          controlClient.end(true);
        } catch (error) {
          console.error("Error closing chunked control client:", error);
        }
        process.exit(0);
      }
    };

    controlClient.on("connect", () => {
      controlClient.subscribe(controlTopic, { qos: 1 }, (err) => {
        if (err) {
          console.error(`Failed to subscribe to benchmark control topic ${controlTopic}:`, err);
        } else {
          console.log(`Subscribed to benchmark control topic ${controlTopic} with QoS 1`);
        }
      });
    });

    controlClient.on("message", (topic, message) => {
      if (topic !== controlTopic) {
        return;
      }
      try {
        const parsed = JSON.parse(message.toString());
        if (parsed?.type === "finite_replay_complete") {
          console.log(`Finite replay complete signal received: ${JSON.stringify({
            topic: controlTopic,
            source: parsed?.source ?? null,
          })}`);
          setTimeout(() => {
            void shutdown();
          }, 50);
        }
      } catch (error) {
        console.error("Error parsing benchmark control message:", error);
      }
    });
  }
  resourceTraceSnapshot(
    "registered_query_started",
    "chunked orchestrator handed off to BeeWorker",
  );
}

StreamingQueryHiveApproachOrchestrator().catch((error) => {
  console.error("Error in orchestrator:", error);
});

/**
 *
 * @param filePath
 * @param intervalMs
 */
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

startResourceUsageLogging();
