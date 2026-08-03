import path from "path";
import { Orchestrator } from "../orchestrator/Orchestrator";
import { CSVLogger } from "../util/logger/CSVLogger";
import {
  buildBenchmarkStreamIri,
  buildBenchmarkTopicName,
  buildOutputSelectClause,
  buildSubQuerySelectClause,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getSubWindowRange,
  getSubWindowStep,
} from "../util/runtimeConfig";

async function runApproximationKScalingOrchestrator() {
  const aggregationFunction = getConfiguredAggregation();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  const subWindowRange = getSubWindowRange();
  const subWindowStep = getSubWindowStep();
  const wearableTopicName = buildBenchmarkTopicName("wearableX");
  const smartphoneTopicName = buildBenchmarkTopicName("smartphoneX");
  const wearableStreamIri = buildBenchmarkStreamIri("wearableX");
  const smartphoneStreamIri = buildBenchmarkStreamIri("smartphoneX");

  const K = parseInt(process.env.K_SCALING_K || "1", 10);
  const baseResultTopic = process.env.RESULT_TOPIC || "output";
  const logRoot = process.env.LOG_PATH || ".";
  const logger = new CSVLogger(
    path.join(logRoot, "approximation_k_scaling_orchestrator.csv"),
  );
  const orchestrator = new Orchestrator("ApproximationApproachOperator");

  const subQueries = [
    `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "WearableX")}
FROM NAMED WINDOW <${wearableStreamIri}> ON STREAM mqtt_broker:${wearableTopicName} [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <${wearableStreamIri}> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:hasTimestamp ?ts .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
    `,
    `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggregationFunction, "SmartphoneX")}
FROM NAMED WINDOW <${smartphoneStreamIri}> ON STREAM mqtt_broker:${smartphoneTopicName} [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <${smartphoneStreamIri}> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:hasTimestamp ?ts .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
    `,
  ];

  console.log(`approximation_consumers_started = ${K}`);
  console.log(`K_SCALING_K = ${K}`);
  console.log(`BASE_RESULT_TOPIC = ${baseResultTopic}`);
  logger.log(
    `Approximation K-scaling orchestrator config: K=${K}, aggregation=${aggregationFunction}, subWindowRange=${subWindowRange}, subWindowStep=${subWindowStep}, outputWindowRange=${outputWindowRange}, outputWindowStep=${outputWindowStep}`,
  );

  for (const subQuery of subQueries) {
    orchestrator.addSubQuery(subQuery);
  }
  logger.log(`Sub-queries added: ${JSON.stringify(orchestrator.getSubQueries())}`);

  const queries = [];
  for (let i = 1; i <= K; i++) {
    const query = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages_${i}> AS
SELECT ${buildOutputSelectClause(aggregationFunction)}
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
    queries.push(query);

    const topic = `${baseResultTopic}_consumer_${i}`;
    console.log(
      `Instantiating Approximation consumer ${i}: topic=${topic} queryId=sensor_averages_${i}`,
    );
    logger.log(
      `Approximation consumer ${i}: topic=${topic} queryId=sensor_averages_${i}`,
    );
    orchestrator["beeKeeper"].executeQuery(query, topic, "ApproximationApproachOperator", subQueries, {
      RESULT_TOPIC: topic,
      HIVE_PROCESS_ROLE: `approximation_bee_worker_${i}`,
      K_SCALING_CONSUMER_INDEX: String(i),
      LOG_PATH: logRoot,
    });
  }

  let shuttingDown = false;
  async function shutdown(reason: string, exitCode = 0) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    try {
      await orchestrator.stop();
    } catch (error) {
      console.error("[shutdown] approximation orchestrator.stop failed", error);
    }
    process.exit(exitCode);
  }

  process.on("SIGINT", () => void shutdown("SIGINT", 130));
  process.on("SIGTERM", () => void shutdown("SIGTERM", 143));
  process.on("uncaughtException", (error) => {
    console.error("[fatal] uncaughtException", error);
    void shutdown("uncaughtException", 1);
  });
  process.on("unhandledRejection", (error) => {
    console.error("[fatal] unhandledRejection", error);
    void shutdown("unhandledRejection", 1);
  });
  process.on("exit", (code) => {
    console.log(`[exit] code=${code}`);
  });
}

runApproximationKScalingOrchestrator().catch((error) => {
  console.error("Error in approximation K-scaling orchestrator:", error);
});
