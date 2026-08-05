import {
  SubqueryProducerHandle,
  SubqueryProducerManager,
} from "./SubqueryProducerManager";

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

  test("creates one runtime producer and reuses it for repeated requests", async () => {
    const runtimeFactory = new FakeRuntimeFactory();
    const manager = new SubqueryProducerManager(runtimeFactory as any);

    const first = await manager.ensureProducer(query, "execution-1");
    const second = await manager.ensureProducer(query, "execution-2");

    expect(runtimeFactory.calls).toHaveLength(1);
    expect(first.producerId).toBe(second.producerId);
    expect(first.outputTopic).toBe(second.outputTopic);
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
});
