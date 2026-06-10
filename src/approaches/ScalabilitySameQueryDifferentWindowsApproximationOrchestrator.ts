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

async function runScalabilityApproximationOrchestrator() {
  const logger = new CSVLogger("approximation_approach_log.csv");
  const orchestrator = new Orchestrator("ApproximationApproachOperator");
  const aggregation = getConfiguredAggregation();
  const scale = getConfiguredScale();
  const reusableWindows = getReusableWindowsForScale(scale);

  reusableWindows.forEach((window, index) => {
    orchestrator.addSubQuery(buildScalabilitySubQuery(index + 1, window, aggregation));
  });
  logger.log(
    `Scalability approximation config: scale=${scale}, reusableWindows=${JSON.stringify(reusableWindows)}`,
  );

  const registeredQuery = buildScalabilitySuperQuery(aggregation);
  orchestrator.registerQuery(registeredQuery);
  logger.log(`Registered query: ${registeredQuery}`);
  orchestrator.runRegisteredQuery();
}

function startResourceUsageLogging(
  filePath = "approximation_approach_resource_usage.csv",
  intervalMs = 100,
) {
  const writeHeader = !fs.existsSync(filePath);
  const logStream = fs.createWriteStream(filePath, { flags: "a" });
  if (writeHeader) {
    logStream.write("timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n");
  }

  setInterval(() => {
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
}

startResourceUsageLogging();
runScalabilityApproximationOrchestrator().catch((error) => {
  console.error("Error in scalability approximation orchestrator:", error);
});
