import mqtt from "mqtt";
import path from "path";
import CONFIG from "../config/httpServerConfig.json";
import { Orchestrator } from "../orchestrator/Orchestrator";
import {
  aggregateApproximationConsumerSummaries,
  appendApproximationTrace,
  buildApproximationAggregateSummaryPath,
  buildApproximationStartupReadyPath,
  buildApproximationTracePath,
  writeAtomicJson,
} from "./approximationKScalingArtifacts";
import { waitForApproximationParentCompletion } from "./approximationKScalingParentCompletion";
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

type Deferred = {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
  settled: boolean;
};

function createDeferred(): Deferred {
  let resolvePromise!: () => void;
  let rejectPromise!: (error: Error) => void;
  const deferred: Deferred = {
    promise: new Promise<void>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    }),
    resolve: () => {
      if (deferred.settled) {
        return;
      }
      deferred.settled = true;
      resolvePromise();
    },
    reject: (error: Error) => {
      if (deferred.settled) {
        return;
      }
      deferred.settled = true;
      rejectPromise(error);
    },
    settled: false,
  };
  return deferred;
}

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
  const controlTopic = buildBenchmarkTopicName("__benchmark_control__");

  const K = parseInt(process.env.K_SCALING_K || "1", 10);
  const baseResultTopic = process.env.RESULT_TOPIC || "output";
  const logRoot = process.env.LOG_PATH || ".";
  const logger = new CSVLogger(
    path.join(logRoot, "approximation_k_scaling_orchestrator.csv"),
  );
  const orchestrator = new Orchestrator("ApproximationApproachOperator");
  const readyPath = buildApproximationStartupReadyPath(logRoot);
  const aggregateSummaryPath = buildApproximationAggregateSummaryPath(logRoot);
  const tracePath = buildApproximationTracePath(logRoot);

  const completionDeferreds = new Map<number, Deferred>();
  const completionPromisesResolved = new Set<number>();
  const completionEventsObserved = new Set<number>();
  const consumerSummaryValid = new Set<number>();
  let replayCompleteReceived = false;
  let shuttingDown = false;

  const appendParentTrace = (
    event: any,
    extra?: Record<string, unknown>,
  ) => {
    appendApproximationTrace(tracePath, {
      consumerIndex:
        typeof extra?.consumerIndex === "number" ? Number(extra.consumerIndex) : null,
      event,
      detail: extra,
    } as any);
  };

  const dumpParentTimeoutState = (reason: string) => {
    appendParentTrace("completion_wait_deadline_exceeded" as any, {
      reason,
      resolvedCount: completionPromisesResolved.size,
      completionEventsObserved: completionEventsObserved.size,
      consumerSummaryValid: consumerSummaryValid.size,
    });
  };

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

  const controlClient = mqtt.connect(CONFIG.mqttBroker, {
    clientId: `approximation-parent-${process.pid}-${Date.now()}`,
    clean: true,
    keepalive: 60,
    reconnectPeriod: 1000,
  });

  const controlReady = new Promise<void>((resolve, reject) => {
    controlClient.once("connect", () => {
      controlClient.subscribe(controlTopic, { qos: 1 }, (error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
    controlClient.once("error", reject);
  });

  controlClient.on("message", (_topic, message) => {
    try {
      const parsed = JSON.parse(message.toString());
      if (parsed?.type === "finite_replay_complete") {
        replayCompleteReceived = true;
        appendParentTrace("publisher_started", {
          controlTopic,
          replayCompleteReceived,
        });
      }
    } catch (error) {
      logger.log(`Approximation control parse error: ${String(error)}`);
    }
  });

  async function shutdown(reason: string, exitCode = 0) {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    appendParentTrace("shutdown_started", { reason, exitCode });
    try {
      controlClient.end(true);
    } catch (error) {
      console.error("[shutdown] approximation control client end failed", error);
    }
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
    appendParentTrace("parent_exit", { code });
    console.log(`[exit] code=${code}`);
  });

  for (let i = 1; i <= K; i += 1) {
    const deferred = createDeferred();
    completionDeferreds.set(i, deferred);
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

    const topic = `${baseResultTopic}_consumer_${i}`;
    console.log(
      `Instantiating Approximation consumer ${i}: topic=${topic} queryId=sensor_averages_${i}`,
    );
    logger.log(
      `Approximation consumer ${i}: topic=${topic} queryId=sensor_averages_${i}`,
    );

    orchestrator["beeKeeper"].executeQuery(
      query,
      topic,
      "ApproximationApproachOperator",
      subQueries,
      {
        RESULT_TOPIC: topic,
        HIVE_PROCESS_ROLE: `approximation_bee_worker_${i}`,
        K_SCALING_CONSUMER_INDEX: String(i),
        LOG_PATH: logRoot,
      },
      {
        onMessage: (message) => {
          const typed = message as Record<string, unknown>;
          if (typed?.type === "approximation_consumer_completion") {
            completionEventsObserved.add(i);
            completionPromisesResolved.add(i);
            appendParentTrace("completion_event_received", {
              consumerIndex: i,
              summaryPath: typed.summaryPath,
              latencyPath: typed.latencyPath,
            });
            deferred.resolve();
            return;
          }
          if (typed?.type === "approximation_consumer_failure") {
            deferred.reject(
              new Error(
                `Approximation consumer ${i} failed: ${String(typed.error || "unknown error")}`,
              ),
            );
          }
        },
        onExit: ({ code, signal }) => {
          appendParentTrace("worker_exit_observed" as any, {
            consumerIndex: i,
            code,
            signal,
          });
          if (!deferred.settled && code && code !== 0 && signal !== "SIGTERM") {
            deferred.reject(
              new Error(
                `Approximation consumer ${i} exited before durable completion: code=${code} signal=${signal ?? "null"}`,
              ),
            );
          }
        },
        onError: (error) => {
          deferred.reject(error);
        },
      },
    );
  }

  await controlReady;
  await writeAtomicJson(readyPath, {
    approach: "approximation",
    kValue: K,
    readyConsumerCount: K,
    readyAt: new Date().toISOString(),
  });
  appendParentTrace("startup_ready", {
    readyConsumerCount: K,
    targetPath: readyPath,
  });
  appendParentTrace("publisher_started", { readyConsumerCount: K });

  try {
    const completionWaitResult = await waitForApproximationParentCompletion({
      expectedConsumerCount: K,
      logRoot,
      completionPromises: [...completionDeferreds.values()].map((entry) => entry.promise),
      completionPromisesResolved,
      completionEventsObserved,
      consumerSummaryValid,
      reconciliationEligible: () =>
        replayCompleteReceived || completionEventsObserved.size > 0,
      appendParentTrace,
      dumpParentTimeoutState,
    });

    appendParentTrace("aggregate_write_started", {
      reconciledFromDurableState: completionWaitResult.reconciledFromDurableState,
      completionStateMismatch: completionWaitResult.completionStateMismatch,
      targetPath: aggregateSummaryPath,
    });
    const aggregateSummary = await aggregateApproximationConsumerSummaries({
      logRoot,
      expectedConsumerCount: K,
    });
    await writeAtomicJson(aggregateSummaryPath, aggregateSummary);
    appendParentTrace("aggregate_write_completed", {
      completedConsumerCount: aggregateSummary.completedConsumerCount,
      comparableConsumerCount: aggregateSummary.comparableConsumerCount,
      targetPath: aggregateSummaryPath,
    });
    await shutdown("benchmark_artifacts_durable", 0);
  } catch (error) {
    console.error("Error in approximation K-scaling orchestrator:", error);
    await shutdown("orchestrator_error", 1);
  }
}

runApproximationKScalingOrchestrator().catch((error) => {
  console.error("Error in approximation K-scaling orchestrator:", error);
  process.exit(1);
});
