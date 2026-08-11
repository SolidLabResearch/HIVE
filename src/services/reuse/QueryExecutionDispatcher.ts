import crypto from "crypto";
import { FetchingAllDataClientSide } from "../../approaches/StreamingQueryFetchingClientSideApproachOrchestrator";
import { HiveQueryBee } from "../HiveQueryBee";
import { BeeKeeper } from "../BeeKeeper";
import {
  ActiveExecutionHandle,
  buildCanonicalQueryId,
  ExecutionState,
} from "../../reuse/QueryReuseRegistry";
import {
  AggregationFunction,
  getConfiguredAggregation,
  getBenchmarkTopicPrefix,
  getOutputWindowRange,
  getOutputWindowStep,
  getSubWindowRange,
  getSubWindowStep,
} from "../../util/runtimeConfig";
import {
  buildQueryTargetScalingSubQuery,
  QueryTargetDefinition,
} from "../../util/queryTargets";
import {
  SubqueryProducerHandle,
  SubqueryProducerRuntimeSnapshot,
  SubqueryProducerManager,
} from "./SubqueryProducerManager";

export type RegistrationApproach = "fetching" | "approximation" | "chunked";

export type ExecutionCreationRequest = {
  approach: RegistrationApproach;
  query: string;
  approximationConfigHash?: string;
  canonicalQueryId: string;
  requestedOutputTopic?: string;
};

type DispatchListener = {
  onExecutionFailed?: (canonicalQueryId: string, reason: string) => void;
};

type DerivedSubqueryPlan = {
  aggregation: AggregationFunction;
  subQueries: string[];
};

type MutableExecutionHandle = ActiveExecutionHandle & {
  state: ExecutionState;
};

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function buildExecutionId(
  approach: RegistrationApproach,
  canonicalQueryId: string,
  approximationConfigHash?: string,
): string {
  const digest = crypto
    .createHash("sha256")
    .update(
      [
        approach,
        canonicalQueryId,
        approximationConfigHash ?? "",
        Date.now().toString(),
        crypto.randomBytes(8).toString("hex"),
      ].join(":"),
    )
    .digest("hex");
  return `${approach}_${digest.slice(0, 16)}`;
}

function buildFetchingExecutionId(
  requestedOutputTopic?: string,
): string {
  const digest = crypto
    .createHash("sha256")
    .update(
      [
        "fetching",
        requestedOutputTopic?.trim() ?? "",
        Date.now().toString(),
        crypto.randomBytes(8).toString("hex"),
      ].join(":"),
    )
    .digest("hex");
  return `fetching_${digest.slice(0, 16)}`;
}

function buildSharedOutputTopic(
  executionId: string,
  requestedOutputTopic?: string,
): string {
  if (requestedOutputTopic && requestedOutputTopic.trim() !== "") {
    return `shared/${executionId}/${requestedOutputTopic.trim().replace(/[^a-zA-Z0-9/_-]/g, "_")}`;
  }
  return `shared/${executionId}/results`;
}

function detectAggregation(query: string): AggregationFunction {
  const match = query.match(/\bSELECT\s*\((AVG|SUM|COUNT|MIN|MAX)\s*\(/i);
  const candidate = match?.[1]?.toUpperCase() as AggregationFunction | undefined;
  return candidate ?? getConfiguredAggregation();
}

function normalizeTargetTopicName(topicName: string): string {
  const normalized = topicName.trim().replace(/^\/+|\/+$/g, "");
  const prefix = getBenchmarkTopicPrefix().trim().replace(/^\/+|\/+$/g, "");
  if (!prefix) {
    return normalized;
  }

  return normalized.startsWith(`${prefix}/`)
    ? normalized.slice(prefix.length + 1)
    : normalized;
}

function extractWindowTargets(query: string): QueryTargetDefinition[] {
  const windowTargets: Array<{ windowIri: string; topicName: string }> = [];
  for (const match of query.matchAll(
    /FROM\s+NAMED\s+WINDOW\s+<([^>]+)>\s+ON\s+STREAM\s+mqtt_broker:([^\s\[]+)\s+\[RANGE\s+\d+\s+STEP\s+\d+\]/gi,
  )) {
    windowTargets.push({
      windowIri: match[1],
      topicName: match[2],
    });
  }

  const propertyByWindow = new Map<string, string>();
  for (const match of query.matchAll(
    /WINDOW\s+<([^>]+)>\s*\{[\s\S]*?relatesToProperty\s+dahccsensors:([A-Za-z0-9_]+)\s*\.[\s\S]*?\}/gi,
  )) {
    propertyByWindow.set(match[1], match[2]);
  }

  return windowTargets.map((entry) => {
    const propertyName = propertyByWindow.get(entry.windowIri);
    if (!propertyName) {
      throw new Error(`Unsupported query shape: unable to derive property for window ${entry.windowIri}`);
    }
    return {
      name: propertyName,
      topicName: normalizeTargetTopicName(entry.topicName),
      propertyName,
    };
  });
}

function deriveSubqueryPlan(query: string): DerivedSubqueryPlan {
  const targets = extractWindowTargets(query);
  if (targets.length === 0) {
    throw new Error("Unsupported query shape: no named windows found for reusable execution");
  }
  const aggregation = detectAggregation(query);
  const subWindowRange = getSubWindowRange();
  const subWindowStep = getSubWindowStep();

  return {
    aggregation,
    subQueries: targets.map((target) =>
      buildQueryTargetScalingSubQuery(
        target,
        aggregation,
        subWindowRange,
        subWindowStep,
      ),
    ),
  };
}

export class QueryExecutionDispatcher {
  private readonly beeKeeper: BeeKeeper;
  private readonly listener: DispatchListener;
  private readonly producerManager: SubqueryProducerManager;
  private readonly executionOwnerById = new Map<string, string>();
  // Multiple independently registered Fetching targets live in one server
  // process. Give their diagnostic artifacts distinct consumer slots; this is
  // observability only and does not alter query execution or result topics.
  private nextFetchingArtifactConsumerIndex = 1;

  constructor(
    beeKeeper = new BeeKeeper(),
    listener: DispatchListener = {},
    producerManager?: SubqueryProducerManager,
  ) {
    this.beeKeeper = beeKeeper;
    this.listener = listener;
    this.producerManager =
      producerManager ??
      new SubqueryProducerManager(undefined, {
        onProducerFailed: (dependentExecutionIds, _producerId, reason) => {
          for (const dependentExecutionId of dependentExecutionIds) {
            const canonicalQueryId =
              this.executionOwnerById.get(dependentExecutionId);
            if (canonicalQueryId) {
              this.executionOwnerById.delete(dependentExecutionId);
              this.listener.onExecutionFailed?.(canonicalQueryId, reason);
            }
          }
        },
      });
  }

  async createExecution(
    request: ExecutionCreationRequest,
  ): Promise<ActiveExecutionHandle> {
    const executionId =
      request.approach === "fetching"
        ? buildFetchingExecutionId(request.requestedOutputTopic)
        : buildExecutionId(
            request.approach,
            request.canonicalQueryId,
            request.approximationConfigHash,
          );
    const sharedOutputTopic = buildSharedOutputTopic(
      executionId,
      request.requestedOutputTopic,
    );

    if (request.approach === "fetching") {
      return this.createFetchingExecution(
        request,
        executionId,
        sharedOutputTopic,
      );
    }

    const runtimePlan = deriveSubqueryPlan(request.query);
    if (
      request.approach === "approximation" ||
      request.approach === "chunked"
    ) {
      this.executionOwnerById.set(executionId, request.canonicalQueryId);
      const producers = await this.producerManager.ensureProducers(
        runtimePlan.subQueries,
        executionId,
      );
      try {
        return this.createBeeExecution(
          request,
          executionId,
          sharedOutputTopic,
          runtimePlan.subQueries,
          producers,
        );
      } catch (error) {
        this.executionOwnerById.delete(executionId);
        await this.producerManager.releaseExecution(executionId);
        throw error;
      }
    }
    return this.createBeeExecution(
      request,
      executionId,
      sharedOutputTopic,
      runtimePlan.subQueries,
    );
  }

  private async createFetchingExecution(
    request: ExecutionCreationRequest,
    executionId: string,
    sharedOutputTopic: string,
  ): Promise<ActiveExecutionHandle> {
    const client = new FetchingAllDataClientSide(
      request.query,
      sharedOutputTopic,
      detectAggregation(request.query),
      this.nextFetchingArtifactConsumerIndex++,
    );
    const handle: MutableExecutionHandle = {
      executionId,
      approach: "fetching",
      canonicalQueryId: request.canonicalQueryId,
      sharedOutputTopic,
      workerIds: [],
      state: "starting",
      stop: async () => {
        handle.state = "stopping";
        await client.cleanup();
        handle.state = "stopped";
      },
    };
    try {
      await client.process_streams();
      handle.state = "active";
      return handle;
    } catch (error) {
      handle.state = "failed";
      this.listener.onExecutionFailed?.(
        request.canonicalQueryId,
        error instanceof Error ? error.message : String(error),
      );
      throw error;
    }
  }

  private async createBeeExecution(
    request: ExecutionCreationRequest,
    executionId: string,
    sharedOutputTopic: string,
    subQueries: string[],
    producerHandles: SubqueryProducerHandle[] = [],
  ): Promise<ActiveExecutionHandle> {
    const operator =
      request.approach === "approximation"
        ? "ApproximationApproachOperator"
        : "StreamingQueryChunkAggregatorOperator";
    const usesSharedProducers = producerHandles.length > 0;
    let resolved = false;
    let handle!: MutableExecutionHandle;
    const readySignal =
      request.approach === "chunked" ? createDeferred<void>() : null;
    const worker = this.beeKeeper.executeQuery(
      request.query,
      sharedOutputTopic,
      operator,
      subQueries,
      {
        RESULT_TOPIC: sharedOutputTopic,
        BENCHMARK_APPROACH: request.approach,
        EXECUTION_ID: executionId,
        HIVE_SKIP_CHUNK_PRODUCER_SPAWNING:
          request.approach === "chunked" && usesSharedProducers ? "true" : "false",
        HIVE_PRODUCER_IDENTITY_MAPPINGS: usesSharedProducers
          ? JSON.stringify(
              producerHandles.map((producer) => ({
                producerId: producer.producerId,
                canonicalProducerId: producer.canonicalProducerId,
                runtimeProducerId: producer.runtimeProducerId,
                topic: producer.producerTopic,
                canonicalProducerQuery: producer.canonicalProducerQuery,
                runtimeProducerQuery: producer.runtimeProducerQuery,
                expectedInputStream: producer.expectedInputStream,
                alignmentOriginMs: producer.alignmentOriginMs,
              })),
            )
          : "[]",
      },
      {
        onMessage: (message) => {
          if (request.approach !== "chunked" || !readySignal) {
            return;
          }
          const payload = message as
            | {
                type?: string;
                readyAt?: number;
                error?: string;
              }
            | undefined;
          if (payload?.type === "chunked_worker_ready") {
            readySignal.resolve();
            return;
          }
          if (payload?.type === "chunked_worker_ready_failed") {
            readySignal.reject(
              new Error(
                payload.error || "chunked worker readiness failed",
              ),
            );
          }
        },
        onExit: ({ code, signal }) => {
          if (!resolved) {
            handle.state = "failed";
            readySignal?.reject(
              new Error(
                `worker exited before activation code=${code} signal=${signal}`,
              ),
            );
            if (usesSharedProducers) {
              this.executionOwnerById.delete(executionId);
              void this.producerManager.releaseExecution(executionId);
            }
            this.listener.onExecutionFailed?.(
              request.canonicalQueryId,
              `worker exited before activation code=${code} signal=${signal}`,
            );
            return;
          }
          if (handle.state !== "stopping" && handle.state !== "stopped") {
            handle.state = "failed";
            if (usesSharedProducers) {
              this.executionOwnerById.delete(executionId);
              void this.producerManager.releaseExecution(executionId);
            }
            this.listener.onExecutionFailed?.(
              request.canonicalQueryId,
              `worker exited code=${code} signal=${signal}`,
            );
          }
        },
        onError: (error) => {
          handle.state = "failed";
          readySignal?.reject(error);
          if (usesSharedProducers) {
            this.executionOwnerById.delete(executionId);
            void this.producerManager.releaseExecution(executionId);
          }
          this.listener.onExecutionFailed?.(
            request.canonicalQueryId,
            error.message,
          );
        },
      },
    ) as HiveQueryBee;

    handle = {
      executionId,
      approach: request.approach,
      canonicalQueryId: request.canonicalQueryId,
      sharedOutputTopic,
      workerIds: worker.getPid() ? [String(worker.getPid())] : [],
      producerIds: producerHandles.map((handle) => handle.producerId),
      canonicalProducerIds: producerHandles.map((handle) => handle.canonicalProducerId),
      runtimeProducerIds: producerHandles.map((handle) => handle.runtimeProducerId),
      producerTopics: producerHandles.map((handle) => handle.outputTopic),
      producerSnapshots: this.buildProducerSnapshots(producerHandles),
      state: "starting",
      stop: async () => {
        handle.state = "stopping";
        worker.stop();
        if (usesSharedProducers) {
          this.executionOwnerById.delete(executionId);
          await this.producerManager.releaseExecution(executionId);
        }
        handle.state = "stopped";
      },
    };

    try {
      if (readySignal) {
        await Promise.race([
          readySignal.promise,
          new Promise<never>((_, reject) =>
            setTimeout(
              () =>
                reject(
                  new Error("Timed out waiting for chunked worker readiness"),
                ),
              30_000,
            ),
          ),
        ]);
      }
      resolved = true;
      handle.state = "active";
      return handle;
    } catch (error) {
      handle.state = "failed";
      worker.stop();
      if (usesSharedProducers) {
        this.executionOwnerById.delete(executionId);
        await this.producerManager.releaseExecution(executionId);
      }
      this.listener.onExecutionFailed?.(
        request.canonicalQueryId,
        error instanceof Error ? error.message : String(error),
      );
      throw error;
    }
  }

  async shutdown(): Promise<void> {
    await this.producerManager.stopAll();
  }

  private buildProducerSnapshots(
    producerHandles: SubqueryProducerHandle[],
  ): SubqueryProducerRuntimeSnapshot[] | undefined {
    if (producerHandles.length === 0) {
      return undefined;
    }
    return this.producerManager.getProducerSnapshots(
      producerHandles.map((handle) => handle.producerId),
    );
  }
}

export function deriveReusableSubqueriesForSupportedQuery(
  query: string,
): string[] {
  return deriveSubqueryPlan(query).subQueries;
}

export function deriveCanonicalQueryId(query: string): string {
  return buildCanonicalQueryId(query);
}
