import { ChildProcess, fork } from "child_process";
import path from "path";
import {
  ProducerIdentity,
  buildSubqueryRuntimeIdentity,
  createRuntimeProducerId,
  validateAlignmentOriginMs,
} from "./SubqueryRuntimeIdentity";
import { getBenchmarkEventTimeAnchor } from "../../util/runtimeConfig";

export type SubqueryProducerState =
  | "starting"
  | "ready"
  | "failed"
  | "stopping"
  | "stopped";

export type SubqueryProducerHandle = ProducerIdentity & {
  /** Legacy field: stable canonical producer identity. */
  producerId: string;
  canonicalSubqueryId: string;
  canonicalProducerQuery: string;
  runtimeProducerQuery: string;
  expectedInputStream: string;
  alignmentOriginMs: number;
  query: string;
  producerTopic: string;
  outputTopic: string;
  pid?: number;
  parentPid?: number;
  processCommandLine?: string;
  workerReadyAt?: number;
  workerShutdownAt?: number;
  state: SubqueryProducerState;
  ready: Promise<void>;
  stop(): Promise<void>;
};

export type SubqueryProducerRuntimeSnapshot = ProducerIdentity & {
  /** Legacy field: stable canonical producer identity. */
  producerId: string;
  canonicalSubqueryId: string;
  canonicalQuery: string;
  canonicalProducerQuery: string;
  runtimeProducerQuery: string;
  expectedInputStream: string;
  alignmentOriginMs: number;
  query: string;
  topic: string;
  producerTopic: string;
  outputTopic: string;
  pid?: number;
  parentPid?: number;
  processCommandLine?: string;
  workerReadyAt?: number;
  workerShutdownAt?: number;
  state: SubqueryProducerState;
  referenceCount: number;
  dependentExecutionIds: string[];
};

type RuntimeProducerHandle = {
  pid?: number;
  processCommandLine?: string;
  ready: Promise<void>;
  stop(): Promise<void>;
};

type RuntimeProducerFactory = {
  createProducer(request: {
    query: string;
    outputTopic: string;
    canonicalSubqueryId: string;
    canonicalProducerId: string;
    runtimeProducerId: string;
    expectedInputStream: string;
    alignmentOriginMs: number;
    onFailed: (reason: string) => void;
  }): Promise<RuntimeProducerHandle>;
};

type ProducerFailureListener = {
  onProducerFailed?: (
    dependentExecutionIds: string[],
    producerId: string,
    reason: string,
  ) => void;
};

type ProducerRecord = {
  handle: SubqueryProducerHandle;
  dependentExecutionIds: Set<string>;
};

class KeyedLock {
  private readonly active = new Set<string>();
  private readonly waiters = new Map<string, Array<() => void>>();

  async runExclusive<T>(key: string, task: () => Promise<T>): Promise<T> {
    await this.acquire(key);
    try {
      return await task();
    } finally {
      this.release(key);
    }
  }

  private async acquire(key: string): Promise<void> {
    if (!this.active.has(key)) {
      this.active.add(key);
      return;
    }

    await new Promise<void>((resolve) => {
      const queue = this.waiters.get(key) ?? [];
      queue.push(resolve);
      this.waiters.set(key, queue);
    });
    this.active.add(key);
  }

  private release(key: string): void {
    const queue = this.waiters.get(key);
    const next = queue?.shift();
    if (queue && queue.length > 0) {
      this.waiters.set(key, queue);
    } else {
      this.waiters.delete(key);
    }
    this.active.delete(key);
    next?.();
  }
}

class ForkedProducerFactory implements RuntimeProducerFactory {
  async createProducer(request: {
    query: string;
    outputTopic: string;
    canonicalSubqueryId: string;
    canonicalProducerId: string;
    runtimeProducerId: string;
    expectedInputStream: string;
    alignmentOriginMs: number;
    onFailed: (reason: string) => void;
  }): Promise<RuntimeProducerHandle> {
    const workerPath = path.resolve(__dirname, "SubqueryProducerWorker.js");
    const mqttClientId = `producer-${request.runtimeProducerId}`;
    const child = fork(workerPath, [], {
      env: {
        ...process.env,
        HIVE_PROCESS_ROLE: "manager_owned_subquery_producer",
        PRODUCER_QUERY: request.query,
        PRODUCER_TOPIC: request.outputTopic,
        CANONICAL_PRODUCER_ID: request.canonicalProducerId,
        RUNTIME_PRODUCER_ID: request.runtimeProducerId,
        PRODUCER_EXPECTED_INPUT_STREAM: request.expectedInputStream,
        PRODUCER_ALIGNMENT_ORIGIN: String(request.alignmentOriginMs),
        PRODUCER_MQTT_CLIENT_ID: mqttClientId,
      },
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    let stopping = false;
    const ready = new Promise<void>((resolve, reject) => {
      const fail = (reason: string) => {
        reject(new Error(reason));
        request.onFailed(reason);
      };
      child.on("message", (message: unknown) => {
        const event = message as { type?: string; reason?: string };
        if (event.type === "producer_ready") {
          resolve();
        } else if (event.type === "producer_failed") {
          fail(event.reason || "producer worker failed");
        }
      });
      child.once("error", (error) => fail(error.message));
      child.once("exit", (code, signal) => {
        if (!stopping && code !== 0) {
          fail(`producer worker exited before stop code=${code} signal=${signal}`);
        }
      });
    });

    return {
      pid: child.pid,
      processCommandLine: `${process.execPath} ${workerPath}`,
      ready,
      stop: () => stopChild(child, () => {
        stopping = true;
      }),
    };
  }
}

async function stopChild(child: ChildProcess, markStopping: () => void): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  markStopping();
  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
    }, 2_000);
    child.once("exit", () => {
      clearTimeout(timeout);
      resolve();
    });
    child.send?.({ type: "stop" });
    child.kill("SIGTERM");
  });
}

export class SubqueryProducerManager {
  private readonly entries = new Map<string, ProducerRecord>();
  private readonly lock = new KeyedLock();
  private readonly runtimeFactory: RuntimeProducerFactory;
  private readonly listener: ProducerFailureListener;

  constructor(
    runtimeFactory: RuntimeProducerFactory = new ForkedProducerFactory(),
    listener: ProducerFailureListener = {},
  ) {
    this.runtimeFactory = runtimeFactory;
    this.listener = listener;
  }

  async ensureProducers(
    queries: string[],
    dependentExecutionId: string,
  ): Promise<SubqueryProducerHandle[]> {
    const handles: SubqueryProducerHandle[] = [];
    try {
      for (const query of queries) {
        handles.push(await this.ensureProducer(query, dependentExecutionId));
      }
      return handles;
    } catch (error) {
      await this.releaseExecution(dependentExecutionId);
      throw error;
    }
  }

  async ensureProducer(
    query: string,
    dependentExecutionId: string,
  ): Promise<SubqueryProducerHandle> {
    const identity = buildSubqueryRuntimeIdentity(query);
    return this.lock.runExclusive(identity.canonicalId, async () => {
      const existing = this.entries.get(identity.canonicalId);
      if (existing && existing.handle.state !== "failed") {
        existing.dependentExecutionIds.add(dependentExecutionId);
        await existing.handle.ready;
        return existing.handle;
      }

      const runtimeProducerId = createRuntimeProducerId();
      if (identity.inputStreams.length !== 1) {
        throw new Error(
          `Manager-owned producer ${identity.canonicalId} must have exactly one authoritative input stream; received ${identity.inputStreams.length}`,
        );
      }
      const expectedInputStream = identity.inputStreams[0].streamName;
      const configuredAlignmentOrigin = getBenchmarkEventTimeAnchor();
      if (configuredAlignmentOrigin === null) {
        throw new Error(
          `Manager-owned producer ${identity.canonicalId} requires STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR`,
        );
      }
      const alignmentOriginMs = validateAlignmentOriginMs(configuredAlignmentOrigin);
      const runtime = await this.runtimeFactory.createProducer({
        query,
        outputTopic: identity.outputTopic,
        canonicalSubqueryId: identity.canonicalId,
        canonicalProducerId: identity.canonicalId,
        runtimeProducerId,
        expectedInputStream,
        alignmentOriginMs,
        onFailed: (reason) => {
          void this.handleProducerFailure(identity.canonicalId, reason);
        },
      });

      let resolveReady!: () => void;
      let rejectReady!: (error: Error) => void;
      const ready = new Promise<void>((resolve, reject) => {
        resolveReady = resolve;
        rejectReady = reject;
      });

      const handle: SubqueryProducerHandle = {
        producerId: identity.canonicalId,
        canonicalSubqueryId: identity.canonicalId,
        canonicalProducerId: identity.canonicalId,
        runtimeProducerId,
        canonicalProducerQuery: identity.canonicalQuery,
        runtimeProducerQuery: query,
        expectedInputStream,
        alignmentOriginMs,
        query,
        producerTopic: identity.outputTopic,
        outputTopic: identity.outputTopic,
        pid: runtime.pid,
        parentPid: process.pid,
        processCommandLine: runtime.processCommandLine,
        state: "starting",
        ready,
        stop: async () => {
          if (handle.state === "stopped" || handle.state === "stopping") {
            return;
          }
          const failedBeforeStop = handle.state === "failed";
          handle.state = "stopping";
          await runtime.stop();
          handle.workerShutdownAt = Date.now();
          handle.state = failedBeforeStop ? "failed" : "stopped";
        },
      };

      const record: ProducerRecord = {
        handle,
        dependentExecutionIds: new Set([dependentExecutionId]),
      };
      this.entries.set(identity.canonicalId, record);

      try {
        await runtime.ready;
        handle.workerReadyAt = Date.now();
        handle.state = "ready";
        resolveReady();
        return handle;
      } catch (error) {
        handle.state = "failed";
        rejectReady(error instanceof Error ? error : new Error(String(error)));
        this.entries.delete(identity.canonicalId);
        await runtime.stop().catch(() => undefined);
        throw error;
      }
    });
  }

  async releaseExecution(dependentExecutionId: string): Promise<void> {
    const producerIds = Array.from(this.entries.keys());
    for (const producerId of producerIds) {
      await this.lock.runExclusive(producerId, async () => {
        const record = this.entries.get(producerId);
        if (!record) {
          return;
        }
        record.dependentExecutionIds.delete(dependentExecutionId);
        if (record.dependentExecutionIds.size > 0) {
          return;
        }
        this.entries.delete(producerId);
        await record.handle.stop();
      });
    }
  }

  getActiveHandles(): SubqueryProducerHandle[] {
    return Array.from(this.entries.values()).map((entry) => entry.handle);
  }

  async stopAll(): Promise<void> {
    const records = Array.from(this.entries.values());
    this.entries.clear();
    await Promise.all(records.map((record) => record.handle.stop()));
  }

  getProducerSnapshots(
    producerIds?: string[],
  ): SubqueryProducerRuntimeSnapshot[] {
    const requestedIds = producerIds ? new Set(producerIds) : null;
    return Array.from(this.entries.values())
      .filter((entry) =>
        requestedIds ? requestedIds.has(entry.handle.producerId) : true,
      )
      .map((entry) => {
        return {
          producerId: entry.handle.producerId,
          canonicalSubqueryId: entry.handle.canonicalSubqueryId,
          canonicalProducerId: entry.handle.canonicalProducerId,
          runtimeProducerId: entry.handle.runtimeProducerId,
          canonicalQuery: entry.handle.canonicalProducerQuery,
          canonicalProducerQuery: entry.handle.canonicalProducerQuery,
          runtimeProducerQuery: entry.handle.runtimeProducerQuery,
          expectedInputStream: entry.handle.expectedInputStream,
          alignmentOriginMs: entry.handle.alignmentOriginMs,
          query: entry.handle.query,
          topic: entry.handle.producerTopic,
          producerTopic: entry.handle.producerTopic,
          outputTopic: entry.handle.outputTopic,
          pid: entry.handle.pid,
          parentPid: entry.handle.parentPid,
          processCommandLine: entry.handle.processCommandLine,
          workerReadyAt: entry.handle.workerReadyAt,
          workerShutdownAt: entry.handle.workerShutdownAt,
          state: entry.handle.state,
          referenceCount: entry.dependentExecutionIds.size,
          dependentExecutionIds: Array.from(entry.dependentExecutionIds).sort(),
        };
      })
      .sort((left, right) => left.producerId.localeCompare(right.producerId));
  }

  private async handleProducerFailure(
    producerId: string,
    reason: string,
  ): Promise<void> {
    await this.lock.runExclusive(producerId, async () => {
      const record = this.entries.get(producerId);
      if (!record) {
        return;
      }
      this.entries.delete(producerId);
      record.handle.state = "failed";
      this.listener.onProducerFailed?.(
        Array.from(record.dependentExecutionIds),
        producerId,
        reason,
      );
      await record.handle.stop().catch(() => undefined);
    });
  }
}
