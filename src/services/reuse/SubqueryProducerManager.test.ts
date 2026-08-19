import {
  SubqueryProducerHandle,
  SubqueryProducerManager,
} from "./SubqueryProducerManager";
import { buildQueryTargetScalingSubQuery } from "../../util/queryTargets";

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

class FakeRuntimeFactory {
  public calls: Array<{ query: string; outputTopic: string; canonicalSubqueryId: string }> = [];
  public stopCalls: string[] = [];
  public shouldFail = false;
  public lastFailureCallback?: (reason: string) => void;
  private readonly readyById = new Map<string, ReturnType<typeof createDeferred<void>>>();

  async createProducer(request: {
    query: string;
    outputTopic: string;
    canonicalSubqueryId: string;
    onFailed: (reason: string) => void;
  }) {
    this.calls.push(request);
    this.lastFailureCallback = request.onFailed;
    if (this.shouldFail) {
      throw new Error("producer startup failed");
    }
    const ready = createDeferred<void>();
    this.readyById.set(request.canonicalSubqueryId, ready);
    ready.resolve();
    return {
      pid: this.calls.length + 1000,
      ready: ready.promise,
      stop: async () => {
        this.stopCalls.push(request.canonicalSubqueryId);
      },
    };
  }
}

describe("SubqueryProducerManager", () => {
  const originalAlignmentOrigin =
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
  const query = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 30000]
WHERE {
  WINDOW <mqtt://localhost:1883/wearableX> {
    ?s saref:hasValue ?value .
    ?s saref:hasTimestamp ?ts .
    ?s saref:relatesToProperty dahccsensors:wearableX .
  }
}
`;

  beforeEach(() => {
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR = "1785924000000";
  });

  afterAll(() => {
    if (originalAlignmentOrigin === undefined) {
      delete process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
    } else {
      process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR =
        originalAlignmentOrigin;
    }
  });

  test("creates one runtime producer and reuses it for repeated requests", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    const first = await manager.ensureProducer(query, "execution-1");
    const second = await manager.ensureProducer(query, "execution-2");

    expect(runtimeFactory.calls).toHaveLength(1);
    expect(first.producerId).toBe(second.producerId);
    expect(first.outputTopic).toBe(second.outputTopic);
    expect(first.expectedInputStream).toBe("mqtt://localhost:1883/wearableX");
    expect(first.alignmentOriginMs).toBe(1785924000000);
    expect(manager.getActiveHandles()).toHaveLength(1);
  });

  test("serializes concurrent requests for the same subquery", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    const handles = await Promise.all(
      Array.from({ length: 10 }, (_, index) =>
        manager.ensureProducer(query, `execution-${index + 1}`),
      ),
    );

    expect(runtimeFactory.calls).toHaveLength(1);
    expect(new Set(handles.map((handle) => handle.producerId)).size).toBe(1);
  });

  test("creates different producers for different subqueries", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    const first = await manager.ensureProducer(query, "execution-1");
    const second = await manager.ensureProducer(
      query.replace(/wearableX/g, "smartphoneX"),
      "execution-2",
    );

    expect(runtimeFactory.calls).toHaveLength(2);
    expect(first.producerId).not.toBe(second.producerId);
    expect(first.outputTopic).not.toBe(second.outputTopic);
  });

  test("cleans up stale entry after producer creation failure", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    runtimeFactory.shouldFail = true;
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    await expect(manager.ensureProducer(query, "execution-1")).rejects.toThrow(
      "producer startup failed",
    );

    runtimeFactory.shouldFail = false;
    await manager.ensureProducer(query, "execution-1-retry");

    expect(runtimeFactory.calls).toHaveLength(2);
    expect(manager.getActiveHandles()).toHaveLength(1);
  });

  test("stops producer when last dependent execution is released", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    const handle = await manager.ensureProducer(query, "execution-1");
    await manager.ensureProducer(query, "execution-2");

    await manager.releaseExecution("execution-1");
    expect(runtimeFactory.stopCalls).toHaveLength(0);

    await manager.releaseExecution("execution-2");
    expect(runtimeFactory.stopCalls).toEqual([handle.producerId]);
    expect(manager.getActiveHandles()).toHaveLength(0);
  });

  test("reports dependent execution ids and reference count for shared producer", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    const handle = await manager.ensureProducer(query, "execution-1");
    await manager.ensureProducer(query, "execution-2");
    await manager.ensureProducer(query, "execution-3");

    const [snapshot] = manager.getProducerSnapshots([handle.producerId]);
    expect(snapshot.producerId).toBe(handle.producerId);
    expect(snapshot.referenceCount).toBe(3);
    expect(snapshot.dependentExecutionIds).toEqual([
      "execution-1",
      "execution-2",
      "execution-3",
    ]);

    await manager.releaseExecution("execution-2");
    const [afterSingleRelease] = manager.getProducerSnapshots([handle.producerId]);
    expect(afterSingleRelease.referenceCount).toBe(2);
    expect(afterSingleRelease.dependentExecutionIds).toEqual([
      "execution-1",
      "execution-3",
    ]);

    await manager.releaseExecution("execution-1");
    await manager.releaseExecution("execution-3");
    expect(manager.getProducerSnapshots([handle.producerId])).toEqual([]);
  });

  test("invalidates failed producer and notifies dependents", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const onProducerFailed = jest.fn();
    const manager = new SubqueryProducerManager(runtimeFactory as any, {
      onProducerFailed,
    });

    const handle = await manager.ensureProducer(query, "execution-1");
    runtimeFactory.lastFailureCallback?.("producer crashed");

    await new Promise((resolve) => setImmediate(resolve));

    expect(["failed", "stopped"]).toContain(handle.state);
    expect(onProducerFailed).toHaveBeenCalledWith(
      ["execution-1"],
      handle.producerId,
      "producer crashed",
    );
    expect(manager.getActiveHandles()).toHaveLength(0);
  });

  test("shares thing1 across every nested query and leaves thing10 only on Q10", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);
    const subqueriesByThing = Array.from({ length: 10 }, (_unused, index) =>
      buildQueryTargetScalingSubQuery(
        {
          name: `thing${index + 1}`,
          topicName: `thing${index + 1}`,
          propertyName: "sharedNumericProperty",
        },
        "AVG",
        60000,
        30000,
      ),
    );

    for (let queryIndex = 0; queryIndex < 10; queryIndex += 1) {
      await manager.ensureProducers(
        subqueriesByThing.slice(0, queryIndex + 1),
        `execution-q${queryIndex + 1}`,
      );
    }

    const snapshots = manager.getProducerSnapshots();
    expect(snapshots).toHaveLength(10);

    for (let index = 0; index < 10; index += 1) {
      const topic = `chunked/${snapshots[index].producerId}`;
      expect(topic).toBe(snapshots[index].outputTopic);
    }

    const snapshotByTopic = new Map(
      snapshots.map((snapshot) => {
        const topicNameMatch = snapshot.query.match(/ON STREAM mqtt_broker:([^\s\[]+)/i);
        return [topicNameMatch?.[1], snapshot];
      }),
    );

    const thing1 = snapshotByTopic.get("thing1");
    const thing10 = snapshotByTopic.get("thing10");
    expect(thing1?.referenceCount).toBe(10);
    expect(thing1?.dependentExecutionIds).toEqual([
      "execution-q1",
      "execution-q10",
      "execution-q2",
      "execution-q3",
      "execution-q4",
      "execution-q5",
      "execution-q6",
      "execution-q7",
      "execution-q8",
      "execution-q9",
    ]);
    expect(thing10?.referenceCount).toBe(1);
    expect(thing10?.dependentExecutionIds).toEqual(["execution-q10"]);

    for (let index = 0; index < 10; index += 1) {
      const snapshot = snapshotByTopic.get(`thing${index + 1}`);
      expect(snapshot?.referenceCount).toBe(10 - index);
    }
  });
});
