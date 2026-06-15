import { FetchingAllDataClientSide } from "./StreamingQueryFetchingClientSideApproachOrchestrator";
import {
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  buildBenchmarkTopicName,
  buildBenchmarkStreamIri,
  buildOutputSelectClause,
} from "../util/runtimeConfig";

async function runFetchingKScalingOrchestrator() {
  const aggregationFunction = getConfiguredAggregation();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  const wearableTopicName = buildBenchmarkTopicName("wearableX");
  const smartphoneTopicName = buildBenchmarkTopicName("smartphoneX");
  const wearableStreamIri = buildBenchmarkStreamIri("wearableX");
  const smartphoneStreamIri = buildBenchmarkStreamIri("smartphoneX");

  const K = parseInt(process.env.K_SCALING_K || "1", 10);
  const baseResultTopic = process.env.RESULT_TOPIC || "output";

  console.log(`fetching_consumers_started = ${K}`);
  console.log(`K_SCALING_K = ${K}`);
  console.log(`BASE_RESULT_TOPIC = ${baseResultTopic}`);

  const clients: FetchingAllDataClientSide[] = [];

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

    const r2s_topic = `${baseResultTopic}_consumer_${i}`;
    console.log(`Instantiating Fetching consumer ${i}: topic=${r2s_topic} queryId=sensor_averages_${i}`);

    const client = new FetchingAllDataClientSide(
      query,
      r2s_topic,
      aggregationFunction,
      i,
    );
    clients.push(client);
  }

  // Setup process wide cleanup
  let shuttingDown = false;
  async function shutdown(reason: string, exitCode = 0) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    for (const client of clients) {
      try {
        await client.cleanup();
      } catch (err) {
        console.error(`[shutdown] Failed to clean up fetching consumer:`, err);
      }
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

  // Start all consumers
  clients.forEach((client) => client.process_streams());
}

runFetchingKScalingOrchestrator().catch((error) => {
  console.error("Error in fetching K-scaling orchestrator:", error);
});
