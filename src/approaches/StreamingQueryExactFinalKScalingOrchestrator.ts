import crypto from "crypto";
import fs from "fs";
import path from "path";
import mqtt from "mqtt";
import { FetchingAllDataClientSide } from "./StreamingQueryFetchingClientSideApproachOrchestrator";
import {
  AggregationFunction,
  buildBenchmarkStreamIri,
  buildBenchmarkTopicName,
  buildOutputSelectClause,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
  getSessionId,
} from "../util/runtimeConfig";
import { getCanonicalRSPQLQueryHash } from "../reuse/normalizeRSPQLForExactReuse";
import { profileCount, writeProfileArtifact } from "../util/profiling";

type DeliveryEvent = {
  consumerId: string;
  subscriptionId: string;
  sharedExecutionId: string;
  resultId: string;
  resultValue: number;
  deliveryTimestamp: number;
  sourceResultTimestamp: number;
  K: number;
  iteration: number | null;
  aggregation: AggregationFunction;
  inputStreams: string[];
  windowNumber: number | null;
  windowStart: number | null;
  windowEnd: number | null;
  sourceResultTopic: string;
};

type CachedFinalResult = {
  sharedExecutionId: string;
  resultId: string;
  canonicalQueryHash: string;
  resultTopic: string;
  resultValue: number;
  sourceResultTimestamp: number;
  cachedAt: number;
  aggregation: AggregationFunction;
  inputStreams: string[];
  windowNumber: number | null;
  windowStart: number | null;
  windowEnd: number | null;
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

function writeCsvHeader(filePath: string): void {
  fs.writeFileSync(
    filePath,
    "window_number,query_registered_at,source_result_timestamp,result_emitted_at,delay_past_expected_close_ms,window_start,window_end,window_duration_ms,coverage_complete,is_partial_window,is_comparable_window,result_value,consumer_id,subscription_id,shared_execution_id,result_id\n",
  );
}

class ExactFinalFanOut {
  private readonly K: number;
  private readonly iteration: number | null;
  private readonly logRoot: string;
  private readonly aggregation: AggregationFunction;
  private readonly sessionId: string;
  private readonly sharedResultTopic: string;
  private readonly sharedExecutionId: string;
  private readonly canonicalQueryHash: string;
  private readonly deliveryPath: string;
  private readonly cachePath: string;
  private readonly topologyPath: string;
  private readonly summaryPath: string;
  private readonly readyPath: string;
  private readonly subscriberClients: any[] = [];
  private readonly deliveredConsumers = new Set<string>();
  private readonly subscriptionIds = new Map<string, string>();
  private sharedClient: FetchingAllDataClientSide | null = null;
  private cachedResult: CachedFinalResult | null = null;
  private deliveryEvents: DeliveryEvent[] = [];
  private shuttingDown = false;
  private lastDeliveryTimestamp = 0;
  private sharedQueryExecutionCount = 0;

  constructor(K: number, iteration: number | null, aggregation: AggregationFunction) {
    this.K = K;
    this.iteration = iteration;
    this.aggregation = aggregation;
    this.logRoot = process.env.LOG_PATH || ".";
    this.sessionId = getSessionId();
    fs.mkdirSync(this.logRoot, { recursive: true });

    const query = this.buildSharedQuery();
    this.canonicalQueryHash = getCanonicalRSPQLQueryHash(query).canonicalQueryHash;
    this.sharedExecutionId = [
      "exact_final",
      this.sessionId,
      `K${K}`,
      `iteration${iteration ?? "unknown"}`,
      this.canonicalQueryHash.slice(0, 12),
    ].join("_");
    this.sharedResultTopic = `paper-local-k-scaling/exact-final/shared/${this.sharedExecutionId}`;
    this.deliveryPath = path.join(this.logRoot, "exact_final_delivery_events.ndjson");
    this.cachePath = path.join(this.logRoot, "exact_final_result_cache.ndjson");
    this.topologyPath = path.join(this.logRoot, "exact_final_topology.json");
    this.summaryPath = path.join(this.logRoot, "benchmark_window_cap_summary.json");
    this.readyPath = path.join(this.logRoot, "startup_ready.json");

    fs.writeFileSync(this.deliveryPath, "");
    fs.writeFileSync(this.cachePath, "");
    for (let index = 1; index <= this.K; index += 1) {
      writeCsvHeader(path.join(this.logRoot, `exact_final_latency_log_consumer_${index}.csv`));
    }
    this.writeTopology();
  }

  async run(): Promise<void> {
    console.log(`exact_final_consumers_started = ${this.K}`);
    console.log(`K_SCALING_K = ${this.K}`);
    console.log(`K_SCALING_REUSE_MODE = exact-final`);
    console.log(`SHARED_RESULT_TOPIC = ${this.sharedResultTopic}`);

    await this.startSubscribers();
    await this.startSharedExecution();
    this.writeReadySignal();
    this.installShutdownHooks();
  }

  private buildSharedQuery(): string {
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

REGISTER RStream <sensor_averages_shared> AS
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

  private async startSubscribers(): Promise<void> {
    await Promise.all(
      Array.from({ length: this.K }, (_, zeroIndex) => {
        const index = zeroIndex + 1;
        const consumerId = `consumer_${index}`;
        const subscriptionId = `${this.sharedExecutionId}_subscription_${index}`;
        this.subscriptionIds.set(consumerId, subscriptionId);
        profileCount("final_result_subscribers_registered");
        return new Promise<void>((resolve, reject) => {
          const client = mqtt.connect("mqtt://localhost:1883", {
            clean: true,
            clientId: `${subscriptionId}_${process.pid}`,
          });
          this.subscriberClients.push(client);
          client.on("connect", () => {
            client.subscribe(this.sharedResultTopic, { qos: 2 }, (error: Error | null) => {
              if (error) {
                reject(error);
                return;
              }
              resolve();
            });
          });
          client.on("message", (_topic: string, payload: Buffer) => {
            this.recordDelivery(index, consumerId, subscriptionId, payload);
          });
          client.on("error", reject);
        });
      }),
    );
    this.writeTopology();
  }

  private async startSharedExecution(): Promise<void> {
    const originalTargetWindows = process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS;
    delete process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS;
    const originalResultTopic = process.env.RESULT_TOPIC;
    process.env.RESULT_TOPIC = this.sharedResultTopic;

    try {
      this.sharedQueryExecutionCount = 1;
      profileCount("fresh_executions_started");
      profileCount("final_result_topics_created");
      this.sharedClient = new FetchingAllDataClientSide(
        this.buildSharedQuery(),
        this.sharedResultTopic,
        this.aggregation,
        "shared",
      );
      await this.sharedClient.process_streams();
    } finally {
      if (originalTargetWindows === undefined) {
        delete process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS;
      } else {
        process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS = originalTargetWindows;
      }
      if (originalResultTopic === undefined) {
        delete process.env.RESULT_TOPIC;
      } else {
        process.env.RESULT_TOPIC = originalResultTopic;
      }
    }
    this.writeTopology();
  }

  private writeReadySignal(): void {
    fs.writeFileSync(
      this.readyPath,
      `${JSON.stringify({
        approach: "exact-final",
        kValue: this.K,
        readySubscriberCount: this.subscriptionIds.size,
        readyAt: new Date().toISOString(),
      }, null, 2)}\n`,
    );
  }

  private recordDelivery(
    consumerIndex: number,
    consumerId: string,
    subscriptionId: string,
    payload: Buffer,
  ): void {
    if (this.deliveredConsumers.has(consumerId)) {
      return;
    }

    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(payload.toString("utf8"));
    } catch (error) {
      console.error(`Invalid exact-final payload for ${consumerId}:`, error);
      return;
    }

    const resultValue = Number(parsed.value);
    const sourceResultTimestamp = Number(parsed.timestamp);
    const windowNumber = parseNullableNumber(parsed.windowNumber);
    const windowStart = parseNullableNumber(parsed.windowStart);
    const windowEnd = parseNullableNumber(parsed.windowEnd);
    const resultId = crypto
      .createHash("sha256")
      .update(JSON.stringify({
        sharedExecutionId: this.sharedExecutionId,
        resultValue,
        sourceResultTimestamp,
        windowNumber,
        windowStart,
        windowEnd,
      }))
      .digest("hex");

    if (!this.cachedResult) {
      this.cachedResult = {
        sharedExecutionId: this.sharedExecutionId,
        resultId,
        canonicalQueryHash: this.canonicalQueryHash,
        resultTopic: this.sharedResultTopic,
        resultValue,
        sourceResultTimestamp,
        cachedAt: Date.now(),
        aggregation: this.aggregation,
        inputStreams: ["wearableX", "smartphoneX"],
        windowNumber,
        windowStart,
        windowEnd,
      };
      appendNdjson(this.cachePath, this.cachedResult);
    }

    const rawDeliveryTimestamp = Date.now();
    const deliveryTimestamp = rawDeliveryTimestamp <= this.lastDeliveryTimestamp
      ? this.lastDeliveryTimestamp + 1
      : rawDeliveryTimestamp;
    this.lastDeliveryTimestamp = deliveryTimestamp;

    const event: DeliveryEvent = {
      consumerId,
      subscriptionId,
      sharedExecutionId: this.sharedExecutionId,
      resultId,
      resultValue,
      deliveryTimestamp,
      sourceResultTimestamp,
      K: this.K,
      iteration: this.iteration,
      aggregation: this.aggregation,
      inputStreams: ["wearableX", "smartphoneX"],
      windowNumber,
      windowStart,
      windowEnd,
      sourceResultTopic: this.sharedResultTopic,
    };

    this.deliveredConsumers.add(consumerId);
    this.deliveryEvents.push(event);
    appendNdjson(this.deliveryPath, event);
    this.writeLatencyCsv(consumerIndex, event);
    profileCount("final_result_topics_reused");
    profileCount("emitted_results");
    this.writeTopology();

    if (this.deliveryEvents.length >= this.K) {
      void this.shutdown("all_deliveries_observed", 0);
    }
  }

  private writeLatencyCsv(consumerIndex: number, event: DeliveryEvent): void {
    const latencyPath = path.join(this.logRoot, `exact_final_latency_log_consumer_${consumerIndex}.csv`);
    const queryRegisteredAt = event.sourceResultTimestamp;
    const duration = event.windowStart !== null && event.windowEnd !== null
      ? event.windowEnd - event.windowStart
      : getOutputWindowRange();
    const delayPastExpectedClose = event.deliveryTimestamp - event.sourceResultTimestamp;
    fs.appendFileSync(
      latencyPath,
      [
        event.windowNumber ?? "",
        queryRegisteredAt,
        event.sourceResultTimestamp,
        event.deliveryTimestamp,
        delayPastExpectedClose,
        event.windowStart ?? "",
        event.windowEnd ?? "",
        duration,
        true,
        false,
        true,
        event.resultValue,
        event.consumerId,
        event.subscriptionId,
        event.sharedExecutionId,
        event.resultId,
      ].join(",") + "\n",
    );
  }

  private writeTopology(): void {
    const uniqueConsumerCount = this.deliveredConsumers.size;
    fs.writeFileSync(
      this.topologyPath,
      JSON.stringify(
        {
          mode: "exact-final",
          sharedQueryExecutionCount: this.sharedQueryExecutionCount,
          queryExecutionCount: this.sharedQueryExecutionCount,
          BeeWorkerCount: 0,
          subscriberCount: this.subscriptionIds.size,
          deliveryEventCount: this.deliveryEvents.length,
          uniqueConsumerCount,
          cachedFinalResultCount: this.cachedResult ? 1 : 0,
          sharedExecutionId: this.sharedExecutionId,
          canonicalQueryHash: this.canonicalQueryHash,
          resultTopic: this.sharedResultTopic,
          K: this.K,
          iteration: this.iteration,
          processId: process.pid,
          updatedAt: new Date().toISOString(),
        },
        null,
        2,
      ) + "\n",
    );
  }

  private writeBenchmarkSummary(): void {
    fs.writeFileSync(
      this.summaryPath,
      JSON.stringify(
        {
          targetWindowCount: 1,
          emittedFinalWindowCount: this.cachedResult ? 1 : 0,
          finalWindowNumbers: this.cachedResult?.windowNumber !== null && this.cachedResult?.windowNumber !== undefined
            ? [this.cachedResult.windowNumber]
            : [],
          stoppedAfterTargetWindows: this.cachedResult !== null && this.deliveryEvents.length >= this.K,
          stopReason: this.deliveryEvents.length >= this.K ? "target_window_count_reached" : "other",
          approach: "exact-final",
          deliveryEventCount: this.deliveryEvents.length,
          subscriberCount: this.subscriptionIds.size,
        },
        null,
        2,
      ) + "\n",
    );
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
    process.on("exit", (code) => {
      console.log(`[exit] code=${code}`);
    });
  }

  private async shutdown(reason: string, exitCode: number): Promise<void> {
    if (this.shuttingDown) {
      return;
    }
    this.shuttingDown = true;
    console.log(`[shutdown] reason=${reason}`);
    this.writeBenchmarkSummary();
    this.writeTopology();

    for (const client of this.subscriberClients.splice(0)) {
      try {
        client.end(true);
      } catch (error) {
        console.error("Failed to close exact-final subscriber:", error);
      }
    }
    if (this.sharedClient) {
      try {
        await this.sharedClient.cleanup();
      } catch (error) {
        console.error("Failed to clean up exact-final shared execution:", error);
      }
    }
    writeProfileArtifact();
    process.exit(exitCode);
  }
}

async function runExactFinalKScalingOrchestrator() {
  const K = parsePositiveInt(process.env.K_SCALING_K, 1);
  const iteration = process.env.BENCHMARK_ITERATION
    ? parsePositiveInt(process.env.BENCHMARK_ITERATION, 1)
    : null;
  const fanOut = new ExactFinalFanOut(K, iteration, getConfiguredAggregation());
  await fanOut.run();
}

runExactFinalKScalingOrchestrator().catch((error) => {
  console.error("Error in exact-final K-scaling orchestrator:", error);
  process.exit(1);
});
