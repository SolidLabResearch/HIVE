import fs from "fs";
import path from "path";
import mqtt from "mqtt";
import { Orchestrator } from "../orchestrator/Orchestrator";
import {
  AggregationFunction,
  buildBenchmarkStreamIri,
  buildBenchmarkTopicName,
  buildOutputSelectClause,
  buildSubQuerySelectClause,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getSessionId,
  getSubWindowRange,
  getSubWindowStep,
} from "../util/runtimeConfig";
import { profileCount, writeProfileArtifact } from "../util/profiling";
import {
  buildHierarchicalResultId,
  canonicalizeHierarchicalFinalQuery,
  HierarchicalDeliveryEvent,
  HierarchicalSummary,
  RegistryEntry,
  writeAtomicJson,
} from "./hierarchicalReuse";

type SubscriberRecord = {
  consumerId: string;
  subscriptionId: string;
  consumerQueryRegisteredAt: number;
  reuseHit: boolean;
};

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNullableNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function appendNdjson(filePath: string, value: unknown): void {
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`);
}

function writeLatencyHeader(filePath: string): void {
  fs.writeFileSync(
    filePath,
    "window_number,consumer_query_registered_at,reconstruction_worker_created_at,reconstruction_ready_at,last_required_observation_received_at,shared_result_created_at,cache_entry_created_at,consumer_result_delivered_at,consumer_query_to_result_ms,shared_reconstruction_to_result_ms,last_observation_to_shared_result_ms,fanout_delivery_latency_ms,reuse_lookup_latency_ms,subscriber_attachment_latency_ms,window_start,window_end,window_duration_ms,coverage_complete,is_partial_window,is_comparable_window,result_value,consumer_id,subscription_id,canonical_query_id,result_id\n",
  );
}

class ChunkedExactFinalFanOut {
  private readonly K: number;
  private readonly iteration: number | null;
  private readonly aggregation: AggregationFunction;
  private readonly logRoot: string;
  private readonly sessionId: string;
  private readonly scenarioId: string;
  private readonly orchestrator = new Orchestrator("StreamingQueryChunkAggregatorOperator");
  private readonly subscriberClients: any[] = [];
  private readonly subscribers = new Map<string, SubscriberRecord>();
  private readonly deliveredConsumers = new Set<string>();
  private readonly deliveryEvents: HierarchicalDeliveryEvent[] = [];
  private readonly finalResultCachePath: string;
  private readonly deliveryPath: string;
  private readonly subscriberPath: string;
  private readonly reconstructionPath: string;
  private readonly registryPath: string;
  private readonly topologyPath: string;
  private readonly summaryPath: string;
  private readonly capSummaryPath: string;
  private readonly readyPath: string;
  private readonly chunkProducerTopologyPath: string;
  private readonly finalQuery: string;
  private readonly subQueries: string[];
  private readonly canonicalQueryId: string;
  private readonly finalResultTopic: string;
  private readonly reconstructionWorkerId: string;
  private readonly activeFinalQueries = new Map<string, RegistryEntry>();
  private worker: any = null;
  private reconstructionWorkerCreatedAt = 0;
  private reconstructionReadyAt: number | null = null;
  private cachedResult: {
    result_id: string;
    result_value: number;
    source_result_timestamp: number;
    cached_at: number;
    window_start: number | null;
    window_end: number | null;
  } | null = null;
  private shuttingDown = false;
  private lastDeliveryTimestamp = 0;

  constructor(K: number, iteration: number | null, aggregation: AggregationFunction) {
    this.K = K;
    this.iteration = iteration;
    this.aggregation = aggregation;
    this.logRoot = process.env.LOG_PATH || ".";
    this.sessionId = getSessionId();
    this.scenarioId = process.env.BENCHMARK_SCENARIO_ID ||
      `H1-K${K}-iteration${iteration ?? "unknown"}`;
    fs.mkdirSync(this.logRoot, { recursive: true });

    this.finalQuery = this.buildFinalQuery("sensor_averages_hierarchical");
    this.subQueries = this.buildChunkSubQueries();
    for (const subQuery of this.subQueries) {
      this.orchestrator.addSubQuery(subQuery);
    }
    this.canonicalQueryId = canonicalizeHierarchicalFinalQuery(this.finalQuery).canonicalQueryId;
    this.reconstructionWorkerId = [
      "chunked_reconstruction",
      this.sessionId,
      `K${K}`,
      `iteration${iteration ?? "unknown"}`,
      this.canonicalQueryId.slice(0, 12),
    ].join("_");
    this.finalResultTopic = `paper-local-h1/chunked-exact-final/final/${this.reconstructionWorkerId}`;

    this.finalResultCachePath = path.join(this.logRoot, "exact_final_result_cache.ndjson");
    this.deliveryPath = path.join(this.logRoot, "exact_final_delivery_events.ndjson");
    this.subscriberPath = path.join(this.logRoot, "exact_final_subscriber_events.ndjson");
    this.reconstructionPath = path.join(this.logRoot, "chunk_reconstruction_events.ndjson");
    this.registryPath = path.join(this.logRoot, "active_final_query_registry.json");
    this.topologyPath = path.join(this.logRoot, "hierarchical_reuse_topology.json");
    this.summaryPath = path.join(this.logRoot, "hierarchical_reuse_summary.json");
    this.capSummaryPath = path.join(this.logRoot, "benchmark_window_cap_summary.json");
    this.readyPath = path.join(this.logRoot, "startup_ready.json");
    this.chunkProducerTopologyPath = path.join(this.logRoot, "chunk_producer_topology.json");

    for (const filePath of [
      this.finalResultCachePath,
      this.deliveryPath,
      this.subscriberPath,
      this.reconstructionPath,
    ]) {
      fs.writeFileSync(filePath, "");
    }
    for (let index = 1; index <= this.K; index += 1) {
      writeLatencyHeader(path.join(this.logRoot, `chunked_exact_final_latency_log_consumer_${index}.csv`));
    }
  }

  async run(): Promise<void> {
    console.log(`hierarchical_chunked_exact_final_consumers_started = ${this.K}`);
    console.log(`K_SCALING_K = ${this.K}`);
    console.log(`H1_FINAL_RESULT_TOPIC = ${this.finalResultTopic}`);

    await this.registerConsumer(1);
    await this.waitForReconstructionReady();
    for (let consumerIndex = 2; consumerIndex <= this.K; consumerIndex += 1) {
      await this.registerConsumer(consumerIndex);
    }
    await this.writeStartupReady();
    this.installShutdownHooks();
  }

  private buildChunkSubQueries(): string[] {
    const subWindowRange = getSubWindowRange();
    const subWindowStep = getSubWindowStep();
    const wearableTopicName = buildBenchmarkTopicName("wearableX");
    const smartphoneTopicName = buildBenchmarkTopicName("smartphoneX");
    const wearableStreamIri = buildBenchmarkStreamIri("wearableX");
    const smartphoneStreamIri = buildBenchmarkStreamIri("smartphoneX");
    return [
      `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(this.aggregation, "WearableX")}
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
SELECT ${buildSubQuerySelectClause(this.aggregation, "SmartphoneX")}
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
  }

  private buildFinalQuery(outputName: string): string {
    const outputWindowRange = getOutputWindowRange();
    const outputWindowStep = getOutputWindowStep();
    const wearableTopicName = buildBenchmarkTopicName("wearableX");
    const smartphoneTopicName = buildBenchmarkTopicName("smartphoneX");
    const wearableStreamIri = buildBenchmarkStreamIri("wearableX");
    const smartphoneStreamIri = buildBenchmarkStreamIri("smartphoneX");
    return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <${outputName}> AS
SELECT ${buildOutputSelectClause(this.aggregation)}
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
  }

  private async registerConsumer(consumerIndex: number): Promise<void> {
    const consumerId = `consumer_${consumerIndex}`;
    const registrationStartedAt = Date.now();
    const active = this.activeFinalQueries.get(this.canonicalQueryId);
    const reuseHit = Boolean(active);
    if (!active) {
      await this.createReconstruction(consumerId);
    } else {
      active.subscribers.push(consumerId);
    }

    await this.attachSubscriber(consumerIndex, consumerId, registrationStartedAt, reuseHit);
    appendNdjson(this.subscriberPath, {
      scenario_id: this.scenarioId,
      canonical_query_id: this.canonicalQueryId,
      consumer_id: consumerId,
      registration_time: registrationStartedAt,
      reuse_hit: reuseHit,
      attached_output_topic: this.finalResultTopic,
      reuse_lookup_latency_ms: Date.now() - registrationStartedAt,
    });
    profileCount(reuseHit ? "hierarchical_final_query_reuse_hits" : "hierarchical_final_query_reuse_misses");
    await this.writeRegistry();
    await this.writeTopology();
  }

  private async createReconstruction(consumerId: string): Promise<void> {
    this.reconstructionWorkerCreatedAt = Date.now();
    const entry: RegistryEntry = {
      canonicalQueryId: this.canonicalQueryId,
      source: "chunked-reconstruction",
      reconstructionWorkerId: this.reconstructionWorkerId,
      finalResultTopic: this.finalResultTopic,
      cacheEntryCount: 0,
      subscribers: [consumerId],
      createdAt: this.reconstructionWorkerCreatedAt,
    };
    this.activeFinalQueries.set(this.canonicalQueryId, entry);
    appendNdjson(this.reconstructionPath, {
      scenario_id: this.scenarioId,
      canonical_query_id: this.canonicalQueryId,
      worker_id: this.reconstructionWorkerId,
      consumer_that_created_it: consumerId,
      created_at: this.reconstructionWorkerCreatedAt,
      source_chunk_topics: this.subQueries.map((_query, index) => `chunked/${this.sessionId}/producer_${index + 1}`),
      output_topic: this.finalResultTopic,
      source: "chunked-reconstruction",
    });
    await writeAtomicJson(this.chunkProducerTopologyPath, {
      scenario_id: this.scenarioId,
      chunkProducerCount: this.subQueries.length,
      source: "shared-chunk-production",
      outputFinalQueryRangeMs: getOutputWindowRange(),
      outputFinalQueryStepMs: getOutputWindowStep(),
      subWindowRangeMs: getSubWindowRange(),
      subWindowStepMs: getSubWindowStep(),
      fixedAcrossK: true,
    });

    this.worker = this.orchestrator["beeKeeper"].executeQuery(
      this.finalQuery,
      this.finalResultTopic,
      "StreamingQueryChunkAggregatorOperator",
      this.orchestrator.getSubQueries(),
      {
        LOG_PATH: path.join(this.logRoot, "reconstruction-worker"),
        RESULT_TOPIC: this.finalResultTopic,
        HIVE_PROCESS_ROLE: "chunked_exact_final_reconstruction_worker",
        K_SCALING_CONSUMER_INDEX: "shared",
        HIVE_SKIP_CHUNK_PRODUCER_SPAWNING: "false",
        STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "1",
      },
    );
    profileCount("hierarchical_reconstruction_workers_created");
    profileCount("hierarchical_reconstruction_executions_started");
  }

  private async waitForReconstructionReady(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 2500));
    this.reconstructionReadyAt = Date.now();
    await this.writeTopology();
  }

  private async attachSubscriber(
    consumerIndex: number,
    consumerId: string,
    registrationStartedAt: number,
    reuseHit: boolean,
  ): Promise<void> {
    const subscriptionId = `${this.reconstructionWorkerId}_subscription_${consumerIndex}`;
    await new Promise<void>((resolve, reject) => {
      const client = mqtt.connect("mqtt://localhost:1883", {
        clean: true,
        clientId: `${subscriptionId}_${process.pid}`,
      });
      this.subscriberClients.push(client);
      client.on("connect", () => {
        client.subscribe(this.finalResultTopic, { qos: 2 }, (error: Error | null) => {
          if (error) {
            reject(error);
            return;
          }
          this.subscribers.set(consumerId, {
            consumerId,
            subscriptionId,
            consumerQueryRegisteredAt: registrationStartedAt,
            reuseHit,
          });
          resolve();
        });
      });
      client.on("message", (_topic: string, payload: Buffer) => {
        void this.recordDelivery(consumerIndex, consumerId, subscriptionId, payload);
      });
      client.on("error", reject);
    });
  }

  private async recordDelivery(
    consumerIndex: number,
    consumerId: string,
    subscriptionId: string,
    payload: Buffer,
  ): Promise<void> {
    if (this.deliveredConsumers.has(consumerId)) {
      return;
    }

    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(payload.toString("utf8"));
    } catch (error) {
      console.error(`Invalid hierarchical final payload for ${consumerId}:`, error);
      return;
    }

    const resultValue = Number(parsed.value);
    const sourceResultTimestamp = Number(parsed.timestamp);
    const windowStart = parseNullableNumber(parsed.windowStart);
    const windowEnd = parseNullableNumber(parsed.windowEnd);
    const windowNumber = parseNullableNumber(parsed.windowNumber);
    if (!Number.isFinite(resultValue) || !Number.isFinite(sourceResultTimestamp)) {
      console.error(`Invalid hierarchical final result fields for ${consumerId}`);
      return;
    }

    const resultId = buildHierarchicalResultId({
      canonicalQueryId: this.canonicalQueryId,
      value: resultValue,
      timestamp: sourceResultTimestamp,
      windowStart,
      windowEnd,
    });
    if (!this.cachedResult) {
      this.cachedResult = {
        result_id: resultId,
        result_value: resultValue,
        source_result_timestamp: sourceResultTimestamp,
        cached_at: Date.now(),
        window_start: windowStart,
        window_end: windowEnd,
      };
      appendNdjson(this.finalResultCachePath, {
        scenario_id: this.scenarioId,
        canonical_query_id: this.canonicalQueryId,
        source: "chunked-reconstruction",
        reconstruction_worker_id: this.reconstructionWorkerId,
        final_result_topic: this.finalResultTopic,
        ...this.cachedResult,
      });
      const active = this.activeFinalQueries.get(this.canonicalQueryId);
      if (active) {
        active.cacheEntryCount = 1;
      }
    }

    const rawDeliveryTimestamp = Date.now();
    const deliveryTimestamp = rawDeliveryTimestamp <= this.lastDeliveryTimestamp
      ? this.lastDeliveryTimestamp + 1
      : rawDeliveryTimestamp;
    this.lastDeliveryTimestamp = deliveryTimestamp;
    const subscriber = this.subscribers.get(consumerId);
    const event: HierarchicalDeliveryEvent = {
      scenario_id: this.scenarioId,
      canonical_query_id: this.canonicalQueryId,
      consumer_id: consumerId,
      subscription_id: subscriptionId,
      window_start: windowStart,
      window_end: windowEnd,
      result_value: resultValue,
      source_result_timestamp: sourceResultTimestamp,
      delivery_timestamp: deliveryTimestamp,
      shared_result_created_at: sourceResultTimestamp,
      cache_entry_created_at: this.cachedResult.cached_at,
      fanout_delivery_latency_ms: deliveryTimestamp - sourceResultTimestamp,
      last_required_observation_received_at: sourceResultTimestamp,
      consumer_query_registered_at:
        subscriber?.consumerQueryRegisteredAt ?? this.reconstructionWorkerCreatedAt,
      reconstruction_worker_created_at: this.reconstructionWorkerCreatedAt,
      reconstruction_ready_at: this.reconstructionReadyAt,
      source_result_topic: this.finalResultTopic,
      result_id: resultId,
    };

    this.deliveredConsumers.add(consumerId);
    this.deliveryEvents.push(event);
    appendNdjson(this.deliveryPath, event);
    this.writeLatencyCsv(consumerIndex, event, windowNumber);
    await this.writeRegistry();
    await this.writeTopology();

    if (this.deliveryEvents.length >= this.K) {
      await this.writeSummaries();
      void this.shutdown("all_hierarchical_deliveries_observed", 0);
    }
  }

  private writeLatencyCsv(
    consumerIndex: number,
    event: HierarchicalDeliveryEvent,
    windowNumber: number | null,
  ): void {
    const latencyPath = path.join(this.logRoot, `chunked_exact_final_latency_log_consumer_${consumerIndex}.csv`);
    const duration =
      event.window_start !== null && event.window_end !== null
        ? event.window_end - event.window_start
        : getOutputWindowRange();
    const subscriber = this.subscribers.get(event.consumer_id);
    fs.appendFileSync(
      latencyPath,
      [
        windowNumber ?? "",
        event.consumer_query_registered_at,
        event.reconstruction_worker_created_at,
        event.reconstruction_ready_at ?? "",
        event.last_required_observation_received_at ?? "",
        event.shared_result_created_at,
        event.cache_entry_created_at,
        event.delivery_timestamp,
        event.delivery_timestamp - event.consumer_query_registered_at,
        event.shared_result_created_at - event.reconstruction_worker_created_at,
        event.last_required_observation_received_at !== null
          ? event.shared_result_created_at - event.last_required_observation_received_at
          : "",
        event.fanout_delivery_latency_ms,
        subscriber?.reuseHit ? 0 : "",
        subscriber?.reuseHit ? 0 : "",
        event.window_start ?? "",
        event.window_end ?? "",
        duration,
        true,
        false,
        true,
        event.result_value,
        event.consumer_id,
        event.subscription_id,
        event.canonical_query_id,
        event.result_id,
      ].join(",") + "\n",
    );
  }

  private async writeRegistry(): Promise<void> {
    await writeAtomicJson(this.registryPath, {
      scenario_id: this.scenarioId,
      activeFinalQueries: [...this.activeFinalQueries.values()].map((entry) => ({
        ...entry,
        subscribers: [...entry.subscribers],
      })),
      updatedAt: new Date().toISOString(),
    });
  }

  private async writeTopology(): Promise<void> {
    await writeAtomicJson(this.topologyPath, this.buildSummary(false));
  }

  private async writeSummaries(): Promise<void> {
    const summary = this.buildSummary(true);
    await writeAtomicJson(this.summaryPath, summary);
    await writeAtomicJson(this.capSummaryPath, {
      targetWindowCount: 1,
      emittedFinalWindowCount: summary.emittedFinalWindowCount,
      finalWindowNumbers: [1],
      stoppedAfterTargetWindows: summary.stoppedAfterTargetWindows,
      stopReason: summary.stopReason,
      approach: "chunked-exact-final",
      deliveryEventCount: summary.deliveries,
      subscriberCount: summary.subscribers,
      finalResultSource: summary.finalResultSource,
      directFinalQueryExecutionCount: summary.directFinalQueryExecutionCount,
    });
  }

  private buildSummary(finalized: boolean): HierarchicalSummary {
    const subscribers = this.subscribers.size;
    const deliveries = this.deliveryEvents.length;
    return {
      experiment: "H1: Hierarchical Chunked and Exact-Final Reuse",
      approach: "chunked-exact-final",
      scenario_id: this.scenarioId,
      canonical_query_id: this.canonicalQueryId,
      source: "chunked-reconstruction",
      chunkProducerCount: this.subQueries.length,
      uniqueFinalQueryCount: this.activeFinalQueries.size,
      reconstructionWorkerCount: this.worker ? 1 : 0,
      reconstructionExecutionCount: this.worker ? 1 : 0,
      directFinalQueryExecutionCount: 0,
      cacheEntries: this.cachedResult ? 1 : 0,
      subscribers,
      reuseHits: Math.max(0, subscribers - 1),
      deliveries,
      exactResults: deliveries,
      expectedConsumers: this.K,
      allConsumersDelivered: deliveries >= this.K,
      finalResultSource: "chunked-reconstruction",
      targetWindowCount: 1,
      emittedFinalWindowCount: this.cachedResult ? 1 : 0,
      stoppedAfterTargetWindows: finalized && deliveries >= this.K,
      stopReason: finalized && deliveries >= this.K ? "target_window_count_reached" : "other",
      producedByDirectFinalQuery: false,
      aggregateWrittenAt: new Date().toISOString(),
    };
  }

  private async writeStartupReady(): Promise<void> {
    await writeAtomicJson(this.readyPath, {
      approach: "chunked-exact-final",
      kValue: this.K,
      registeredConsumers: this.subscribers.size,
      readyConsumers: this.subscribers.size,
      requiredSubscriptions: this.subscribers.size,
      uniqueFinalQueryCount: this.activeFinalQueries.size,
      reconstructionWorkerCount: this.worker ? 1 : 0,
      reconstructionReadyAt: this.reconstructionReadyAt,
      readyAt: new Date().toISOString(),
    });
  }

  private installShutdownHooks(): void {
    process.on("SIGINT", () => void this.shutdown("SIGINT", 130));
    process.on("SIGTERM", () => void this.shutdown("SIGTERM", 143));
    process.on("uncaughtException", (error) => {
      console.error("[fatal] uncaughtException", error);
      void this.shutdown("uncaughtException", 1);
    });
    process.on("unhandledRejection", (error) => {
      console.error("[fatal] unhandledRejection", error);
      void this.shutdown("unhandledRejection", 1);
    });
  }

  private async shutdown(reason: string, exitCode: number): Promise<void> {
    if (this.shuttingDown) {
      return;
    }
    this.shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    if (this.deliveryEvents.length >= this.K) {
      await this.writeSummaries();
    }
    await this.writeTopology();
    for (const client of this.subscriberClients.splice(0)) {
      try {
        client.end(true);
      } catch (error) {
        console.error("Failed to close hierarchical subscriber:", error);
      }
    }
    try {
      await this.orchestrator.stop();
    } catch (error) {
      console.error("Failed to stop hierarchical reconstruction worker:", error);
    }
    writeProfileArtifact();
    process.exit(exitCode);
  }
}

async function runChunkedExactFinalKScalingOrchestrator() {
  const K = parsePositiveInt(process.env.K_SCALING_K, 1);
  const iteration = process.env.BENCHMARK_ITERATION
    ? parsePositiveInt(process.env.BENCHMARK_ITERATION, 1)
    : null;
  const fanOut = new ChunkedExactFinalFanOut(K, iteration, getConfiguredAggregation());
  await fanOut.run();
}

runChunkedExactFinalKScalingOrchestrator().catch((error) => {
  console.error("Error in chunked exact-final K-scaling orchestrator:", error);
  process.exit(1);
});
