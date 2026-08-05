import { RSPAgent } from "../../agent/RSPAgent";
import { buildSubqueryRuntimeIdentity } from "./SubqueryRuntimeIdentity";

export type SubqueryProducerState =
  | "starting"
  | "ready"
  | "failed"
  | "stopping"
  | "stopped";

export type SubqueryProducerHandle = {
  producerId: string;
  canonicalSubqueryId: string;
  query: string;
  outputTopic: string;
  pid?: number;
  state: SubqueryProducerState;
  ready: Promise<void>;
  stop(): Promise<void>;
};

export type SubqueryProducerRuntimeSnapshot = {
  producerId: string;
  canonicalSubqueryId: string;
  canonicalQuery: string;
  query: string;
  outputTopic: string;
  pid?: number;
  state: SubqueryProducerState;
  referenceCount: number;
  dependentExecutionIds: string[];
};

type RuntimeProducerHandle = {
  pid?: number;
  ready: Promise<void>;
  stop(): Promise<void>;
};

type RuntimeProducerFactory = {
  createProducer(request: {
    query: string;
    outputTopic: string;
    canonicalSubqueryId: string;
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

class RSPAgentProducerFactory implements RuntimeProducerFactory {
  async createProducer(request: {
    query: string;
    outputTopic: string;
    canonicalSubqueryId: string;
    onFailed: (reason: string) => void;
  }): Promise<RuntimeProducerHandle> {
    const agent = new RSPAgent(request.query, request.outputTopic, {
      registerQueryDefinition: false,
      onRuntimeError: (error) => {
        request.onFailed(error.message);
      },
    });

    const ready = agent.process_streams().catch((error) => {
      request.onFailed(error instanceof Error ? error.message : String(error));
      throw error;
    });

    return {
      pid: agent.getPid(),
      ready,
      stop: async () => {
        agent.stop();
      },
    };
  }
}

export class SubqueryProducerManager {
  private readonly entries = new Map<string, ProducerRecord>();
  private readonly lock = new KeyedLock();
  private readonly runtimeFactory: RuntimeProducerFactory;
  private readonly listener: ProducerFailureListener;

  constructor(
    runtimeFactory: RuntimeProducerFactory = new RSPAgentProducerFactory(),
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

      const runtime = await this.runtimeFactory.createProducer({
        query,
        outputTopic: identity.outputTopic,
        canonicalSubqueryId: identity.canonicalId,
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
        query,
        outputTopic: identity.outputTopic,
        pid: runtime.pid,
        state: "starting",
        ready,
        stop: async () => {
          if (handle.state === "stopped" || handle.state === "stopping") {
            return;
          }
          const failedBeforeStop = handle.state === "failed";
          handle.state = "stopping";
          await runtime.stop();
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

  getProducerSnapshots(
    producerIds?: string[],
  ): SubqueryProducerRuntimeSnapshot[] {
    const requestedIds = producerIds ? new Set(producerIds) : null;
    return Array.from(this.entries.values())
      .filter((entry) =>
        requestedIds ? requestedIds.has(entry.handle.producerId) : true,
      )
      .map((entry) => {
        const identity = buildSubqueryRuntimeIdentity(entry.handle.query);
        return {
          producerId: entry.handle.producerId,
          canonicalSubqueryId: entry.handle.canonicalSubqueryId,
          canonicalQuery: identity.canonicalQuery,
          query: entry.handle.query,
          outputTopic: entry.handle.outputTopic,
          pid: entry.handle.pid,
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
