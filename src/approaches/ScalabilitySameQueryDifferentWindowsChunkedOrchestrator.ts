import fs from "fs";
import { Orchestrator } from "../orchestrator/Orchestrator";
import { CSVLogger } from "../util/logger/CSVLogger";
import { getConfiguredAggregation } from "../util/runtimeConfig";
import {
  buildScalabilitySubQuery,
  buildScalabilitySuperQuery,
  getConfiguredScale,
  getReusableWindowsForScale,
} from "./scalability/sameQueryDifferentWindows";

async function runScalabilityChunkedOrchestrator() {
  const logger = new CSVLogger("streaming_query_chunk_aggregator_log.csv");
  const orchestrator = new Orchestrator("StreamingQueryChunkAggregatorOperator");
  const aggregation = getConfiguredAggregation();
  const scale = getConfiguredScale();
  const reusableWindows = getReusableWindowsForScale(scale);

  reusableWindows.forEach((window, index) => {
    orchestrator.addSubQuery(buildScalabilitySubQuery(index + 1, window, aggregation));
  });
  logger.log(
    `Scalability chunked config: scale=${scale}, reusableWindows=${JSON.stringify(reusableWindows)}`,
  );

  const registeredQuery = buildScalabilitySuperQuery(aggregation);
  orchestrator.registerQuery(registeredQuery);
  logger.log(`Registered query: ${registeredQuery}`);
  orchestrator.runRegisteredQuery();
}

function startResourceUsageLogging(
  filePath = "streaming_query_hive_resource_log.csv",
  intervalMs = 100,
) {
  const writeHeader = !fs.existsSync(filePath);
  const logStream = fs.createWriteStream(filePath, { flags: "a" });
  if (writeHeader) {
    logStream.write("timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n");
  }

  const timer = setInterval(() => {
    const mem = process.memoryUsage();
    const cpu = process.cpuUsage();
    const now = Date.now();
    logStream.write(
      [
        now,
        (cpu.user / 1000).toFixed(2),
        (cpu.system / 1000).toFixed(2),
        mem.rss,
        mem.heapTotal,
        mem.heapUsed,
        (mem.heapUsed / 1024 / 1024).toFixed(2),
        mem.external,
      ].join(",") + "\n",
    );
  }, intervalMs);
  timer.unref?.();
}

startResourceUsageLogging();
runScalabilityChunkedOrchestrator().catch((error) => {
  console.error("Error in scalability chunked orchestrator:", error);
});
