import fs from "fs";
import path from "path";
import { FetchingAllDataClientSide } from "./StreamingQueryFetchingClientSideApproachOrchestrator";
import {
  aggregateFetchingConsumerSummaries,
  appendFetchingArtifactTrace,
  buildFetchingAggregateSummaryPath,
  buildFetchingArtifactTracePath,
  readJsonIfExists,
  FetchingConsumerSummary,
  writeAtomicJson,
} from "./fetchingKScalingArtifacts";
import { waitForFetchingParentCompletion } from "./fetchingKScalingParentCompletion";
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
  const logRoot = process.env.LOG_PATH || ".";
  const readyPath = path.join(logRoot, "startup_ready.json");
  const aggregateSummaryPath = buildFetchingAggregateSummaryPath(logRoot);
  const tracePath = buildFetchingArtifactTracePath(logRoot);
  const artifactTracingEnabled = ["1", "true", "yes", "on"].includes(
    (process.env.STREAMING_QUERY_HIVE_FETCHING_ARTIFACT_TRACE || "").trim().toLowerCase(),
  );
  const parentTracePath = path.join(logRoot, "fetching_parent_state_trace.ndjson");
  const parentTraceEnabled = artifactTracingEnabled || ["1", "true", "yes", "on"].includes(
    (process.env.STREAMING_QUERY_HIVE_FETCHING_PARENT_TRACE || "").trim().toLowerCase(),
  );
  const parentState = {
    parentStateObjectId: `fetching-parent:${process.pid}:${Date.now()}`,
    sequence: 0,
    consumersCreated: new Set<number>(),
    consumersReady: new Set<number>(),
    durableEventsObserved: new Set<number>(),
    completionPromisesResolved: new Set<number>(),
    completionPromisesPending: new Set<number>(),
    consumerSummaryValid: new Set<number>(),
    aggregatePhaseEntered: false,
    aggregateWritePhaseEntered: false,
  };
  const appendParentTrace = (event: string, extra: Record<string, unknown> = {}) => {
    if (!parentTraceEnabled) {
      return;
    }
    const pendingConsumers = [...parentState.completionPromisesPending].sort((a, b) => a - b);
    fs.mkdirSync(path.dirname(parentTracePath), { recursive: true });
    fs.appendFileSync(parentTracePath, `${JSON.stringify({
      sequence: ++parentState.sequence,
      timestamp: Date.now(),
      pid: process.pid,
      event,
      expectedCount: K,
      createdCount: parentState.consumersCreated.size,
      readyCount: parentState.consumersReady.size,
      durableCount: parentState.durableEventsObserved.size,
      resolvedCount: parentState.completionPromisesResolved.size,
      pendingConsumers,
      validSummaryCount: parentState.consumerSummaryValid.size,
      aggregatePhaseEntered: parentState.aggregatePhaseEntered,
      aggregateWritePhaseEntered: parentState.aggregateWritePhaseEntered,
      stateObjectId: parentState.parentStateObjectId,
      ...extra,
    })}\n`);
  };
  const traceAggregateEvent = (event: "aggregate_summary_write_started" | "aggregate_summary_write_completed" | "process_exit_requested") => {
    if (artifactTracingEnabled) {
      appendFetchingArtifactTrace(tracePath, {
        sequence: ++parentState.sequence,
        timestamp: Date.now(),
        pid: process.pid,
        consumerIndex: null,
        event,
        targetPath: aggregateSummaryPath,
        stateObjectId: parentState.parentStateObjectId,
      });
    }
    appendParentTrace(event);
  };
  const dumpParentTimeoutState = (reason: string) => {
    const activeHandles = typeof (process as any)._getActiveHandles === "function"
      ? (process as any)._getActiveHandles().map((handle: any) => handle?.constructor?.name || typeof handle)
      : [];
    const activeRequests = typeof (process as any)._getActiveRequests === "function"
      ? (process as any)._getActiveRequests().map((request: any) => request?.constructor?.name || typeof request)
      : [];
    appendParentTrace("parent_timeout_state_dump", {
      reason,
      activeHandles,
      activeRequests,
    });
  };
  const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
  const createDeferred = (consumerIndex: number) => {
    let resolvePromise!: () => void;
    let rejectPromise!: (error: Error) => void;
    let settled = false;
    const promise = new Promise<void>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    return {
      id: `parent-completion:${consumerIndex}`,
      promise,
      resolve: () => {
        if (settled) {
          return;
        }
        settled = true;
        parentState.completionPromisesPending.delete(consumerIndex);
        parentState.completionPromisesResolved.add(consumerIndex);
        appendParentTrace("consumer_completion_promise_resolved", { consumerIndex });
        resolvePromise();
      },
      reject: (error: Error) => {
        if (settled) {
          return;
        }
        settled = true;
        parentState.completionPromisesPending.delete(consumerIndex);
        appendParentTrace("promise_rejected", { consumerIndex, error: error.message });
        rejectPromise(error);
      },
    };
  };
  const completionDeferreds = new Map<number, ReturnType<typeof createDeferred>>();
  appendParentTrace("parent_started");

  for (let i = 1; i <= K; i++) {
    const deferred = createDeferred(i);
    completionDeferreds.set(i, deferred);
    parentState.completionPromisesPending.add(i);
    appendParentTrace("consumer_completion_promise_created", {
      consumerIndex: i,
      completionPromiseId: deferred.id,
    });
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
      {
        onReady: ({ consumerIndex, stateObjectId }) => {
          if (consumerIndex !== null) {
            parentState.consumersReady.add(consumerIndex);
            appendParentTrace("consumer_ready", { consumerIndex, stateObjectId });
          }
        },
        onDurableArtifacts: ({ consumerIndex, stateObjectId, summaryPath, latencyPath }) => {
          if (consumerIndex === null) {
            return;
          }
          parentState.durableEventsObserved.add(consumerIndex);
          appendParentTrace("consumer_artifacts_durable_event_received", {
            consumerIndex,
            stateObjectId,
            summaryPath,
            latencyPath,
          });
          completionDeferreds.get(consumerIndex)?.resolve();
        },
      },
    );
    clients.push(client);
    parentState.consumersCreated.add(i);
    appendParentTrace("consumer_created", { consumerIndex: i });
  }

  // Setup process wide cleanup
  let shuttingDown = false;
  async function shutdown(reason: string, exitCode = 0) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    appendParentTrace("shutdown_started", { reason, exitCode });
    traceAggregateEvent("process_exit_requested");
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
  process.on("multipleResolves", (type, _promise, value) => {
    appendParentTrace("promise_rejected", {
      reason: `multipleResolves:${type}`,
      value: String(value),
    });
  });
  process.on("exit", (code) => {
    appendParentTrace("parent_exit", { code });
    console.log(`[exit] code=${code}`);
  });

  try {
    // Start all consumers
    await Promise.all(clients.map((client) => client.process_streams()));
    fs.writeFileSync(
      readyPath,
      `${JSON.stringify({
        approach: "fetching",
        kValue: K,
        readyConsumerCount: clients.length,
        readyAt: new Date().toISOString(),
      }, null, 2)}\n`,
    );
    console.log(`startup_ready_path = ${readyPath}`);

    const completionWaitResult = await waitForFetchingParentCompletion({
      expectedConsumerCount: K,
      logRoot,
      completionPromises: [...completionDeferreds.values()].map((entry) => entry.promise),
      durableEventsObserved: parentState.durableEventsObserved,
      completionPromisesResolved: parentState.completionPromisesResolved,
      consumerSummaryValid: parentState.consumerSummaryValid,
      appendParentTrace: (event, extra = {}) => {
        if (event === "aggregate_read_started") {
          parentState.aggregatePhaseEntered = true;
        }
        appendParentTrace(event, extra);
      },
      dumpParentTimeoutState,
      delay,
    });

    if (completionWaitResult.reconciledFromDurableState) {
      parentState.aggregatePhaseEntered = true;
    }

    const aggregateSummary = await aggregateFetchingConsumerSummaries({
      logRoot,
      expectedConsumerCount: K,
    });
    appendParentTrace("aggregate_validation_completed", {
      completedConsumerCount: aggregateSummary.completedConsumerCount,
      comparableConsumerCount: aggregateSummary.comparableConsumerCount,
      reconciledFromDurableState: completionWaitResult.reconciledFromDurableState,
      completionStateMismatch: completionWaitResult.completionStateMismatch,
    });
    if (!aggregateSummary.allConsumersComparable) {
      dumpParentTimeoutState("aggregate_validation_failed");
      throw new Error(
        `Fetching aggregate summary incomplete: completed=${aggregateSummary.completedConsumerCount}/${aggregateSummary.expectedConsumerCount} comparable=${aggregateSummary.comparableConsumerCount}/${aggregateSummary.expectedConsumerCount} failedConsumers=${aggregateSummary.failedConsumerIndices.join(",")}`,
      );
    }
    parentState.aggregateWritePhaseEntered = true;
    appendParentTrace("aggregate_write_started");
    traceAggregateEvent("aggregate_summary_write_started");
    await writeAtomicJson(aggregateSummaryPath, aggregateSummary);
    traceAggregateEvent("aggregate_summary_write_completed");
    appendParentTrace("aggregate_write_completed");
    console.log(
      `Benchmark artifacts durable: aggregateCompleted=${aggregateSummary.completedConsumerCount}/${aggregateSummary.expectedConsumerCount} comparable=${aggregateSummary.comparableConsumerCount}/${aggregateSummary.expectedConsumerCount}`,
    );
    await shutdown("benchmark_artifacts_durable", 0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Error in fetching K-scaling orchestrator:", error);
    dumpParentTimeoutState(`orchestrator_error:${message}`);
    await shutdown("orchestrator_error", 1);
  }
}

runFetchingKScalingOrchestrator().catch((error) => {
  console.error("Fatal error in fetching K-scaling orchestrator bootstrap:", error);
  process.exit(1);
});
