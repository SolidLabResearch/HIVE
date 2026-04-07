import fs from "fs";
import { Orchestrator } from "../orchestrator/Orchestrator";

import { CSVLogger } from "../util/logger/CSVLogger";
/**
 *
 */
async function StreamingQueryHiveApproachOrchestrator() {
  const logger = new CSVLogger("streaming_query_chunk_aggregator_log.csv");
  const orchestrator = new Orchestrator(
    "StreamingQueryChunkAggregatorOperator",
  );

  const configuredAgg = (
    process.env.AGGREGATION_FUNCTION || process.env.AGGREGATION_FUNC || "AVG"
  ).toUpperCase();
  const allowedAgg = ["AVG", "SUM", "COUNT", "MIN", "MAX"];
  const aggFunc = allowedAgg.includes(configuredAgg) ? configuredAgg : "AVG";
  const subWindowRange = process.env.SUB_WINDOW_RANGE || "60000";
  const subWindowStep = process.env.SUB_WINDOW_STEP || "30000";

  const aggSelectWearable =
    aggFunc === "COUNT"
      ? `(COUNT(?value) AS ?aggWearableX)`
      : `(${aggFunc}(?value) AS ?aggWearableX) (COUNT(?value) AS ?countWearableX)`;

  const aggSelectSmartphone =
    aggFunc === "COUNT"
      ? `(COUNT(?value) AS ?aggSmartphoneX)`
      : `(${aggFunc}(?value) AS ?aggSmartphoneX) (COUNT(?value) AS ?countSmartphoneX)`;

  logger.log(
    `Chunked orchestrator config: aggregation=${aggFunc}, subWindowRange=${subWindowRange}, subWindowStep=${subWindowStep}`,
  );

  // Add sub-queries
  const query1 = `
            PREFIX mqtt_broker: <mqtt://localhost:1883/>
    PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT ${aggSelectWearable}
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
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
    SELECT ${aggSelectSmartphone}
    FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
    `;

  orchestrator.addSubQuery(query1);
  orchestrator.addSubQuery(query2);
  logger.log(
    `Sub-queries added: ${JSON.stringify(orchestrator.getSubQueries())}`,
  );
  // Register a query
  const registeredQuery = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (${aggFunc}(?value) AS ?resultValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${subWindowRange} STEP ${subWindowStep}]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
    `;
  logger.log("Registered Query");
  orchestrator.registerQuery(registeredQuery);
  console.log("Registered query:", orchestrator.getRegisteredQuery());
  // -------------------------------------------------------------
  // Run sub-queries
  // Run registered query
  orchestrator.runRegisteredQuery();
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
