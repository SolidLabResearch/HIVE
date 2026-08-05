import { QueryExecutionDispatcher } from "./QueryExecutionDispatcher";
import { FetchingAllDataClientSide } from "../../approaches/StreamingQueryFetchingClientSideApproachOrchestrator";

jest.mock("../../approaches/StreamingQueryFetchingClientSideApproachOrchestrator", () => ({
  FetchingAllDataClientSide: jest.fn(),
}));

class FakeWorker {
  public pid = 4321;
  public callbacks: {
    onExit?: (info: { code: number | null; signal: NodeJS.Signals | null }) => void;
    onError?: (error: Error) => void;
  } = {};

  stop(): void {
    return;
  }

  getPid(): number {
    return this.pid;
  }
}

class FakeProducerManager {
  public ensureCalls: Array<{ queries: string[]; executionId: string }> = [];
  public releaseCalls: string[] = [];
  public shouldFail = false;
  public handles = [
    {
      producerId: "producer-1",
      canonicalSubqueryId: "producer-1",
      query: "subquery-1",
      outputTopic: "chunked/producer-1",
      pid: 9001,
      state: "ready" as const,
      ready: Promise.resolve(),
      stop: async () => undefined,
    },
  ];
  public snapshots = [
    {
      producerId: "producer-1",
      canonicalSubqueryId: "producer-1",
      canonicalQuery: "canonical-subquery-1",
      query: "subquery-1",
      outputTopic: "chunked/producer-1",
      pid: 9001,
      state: "ready" as const,
      referenceCount: 1,
      dependentExecutionIds: ["execution-1"],
    },
  ];

  async ensureProducers(
    queries: string[],
    executionId: string,
  ) {
    this.ensureCalls.push({ queries, executionId });
    if (this.shouldFail) {
      throw new Error("producer startup failed");
    }
    return this.handles;
  }

  async releaseExecution(executionId: string): Promise<void> {
    this.releaseCalls.push(executionId);
  }

  getProducerSnapshots() {
    return this.snapshots;
  }
}

describe("QueryExecutionDispatcher", () => {
  afterEach(() => {
    jest.restoreAllMocks();
    jest.clearAllMocks();
  });

  test("creates fetching execution without synthetic shared consumer index", async () => {
    const cleanup = jest.fn().mockResolvedValue(undefined);
    const process_streams = jest.fn().mockResolvedValue(undefined);
    (FetchingAllDataClientSide as unknown as jest.Mock).mockImplementation(() => ({
      cleanup,
      process_streams,
    }));
    const dispatcher = new QueryExecutionDispatcher({} as any, {});

    const handle = await dispatcher.createExecution({
      approach: "fetching",
      canonicalQueryId: "fetching-query",
      query: "SELECT (AVG(?value) AS ?avgValue) WHERE { ?s ?p ?value }",
      requestedOutputTopic: "consumer-topic",
    });

    expect(FetchingAllDataClientSide).toHaveBeenCalledTimes(1);
    const fetchingCall = (FetchingAllDataClientSide as unknown as jest.Mock).mock.calls[0];
    expect(fetchingCall[0]).toBe("SELECT (AVG(?value) AS ?avgValue) WHERE { ?s ?p ?value }");
    expect(fetchingCall[1]).toMatch(/^shared\/fetching_[0-9a-f]{16}\/consumer-topic$/);
    expect(fetchingCall[2]).toBe("AVG");
    expect(fetchingCall).toHaveLength(3);
    expect(handle.state).toBe("active");
  });

  test("creates distinct fetching executions for equivalent registrations", async () => {
    const cleanup = jest.fn().mockResolvedValue(undefined);
    const process_streams = jest.fn().mockResolvedValue(undefined);
    (FetchingAllDataClientSide as unknown as jest.Mock).mockImplementation(() => ({
      cleanup,
      process_streams,
    }));
    const dispatcher = new QueryExecutionDispatcher({} as any, {});

    const firstHandle = await dispatcher.createExecution({
      approach: "fetching",
      canonicalQueryId: "fetching-query",
      query: "SELECT (AVG(?value) AS ?avgValue) WHERE { ?s ?p ?value }",
      requestedOutputTopic: "consumer-1-topic",
    });
    const secondHandle = await dispatcher.createExecution({
      approach: "fetching",
      canonicalQueryId: "fetching-query",
      query: "SELECT (AVG(?value) AS ?avgValue) WHERE { ?s ?p ?value }",
      requestedOutputTopic: "consumer-2-topic",
    });

    expect(firstHandle.executionId).not.toBe(secondHandle.executionId);
    expect(firstHandle.sharedOutputTopic).not.toBe(secondHandle.sharedOutputTopic);
    expect((FetchingAllDataClientSide as unknown as jest.Mock).mock.calls[0][1]).toContain(
      firstHandle.executionId,
    );
    expect((FetchingAllDataClientSide as unknown as jest.Mock).mock.calls[1][1]).toContain(
      secondHandle.executionId,
    );
  });

  test("creates distinct reusable execution ids for fresh approximation runtimes", async () => {
    const worker = new FakeWorker();
    const producerManager = new FakeProducerManager();
    const beeKeeper = {
      executeQuery: jest.fn().mockImplementation(
        (
          _query: string,
          _topic: string,
          _operator: string,
          _subQueries: string[],
          _env: Record<string, string>,
          callbacks: {
            onExit?: (info: { code: number | null; signal: NodeJS.Signals | null }) => void;
            onError?: (error: Error) => void;
          },
        ) => {
          worker.callbacks = callbacks;
          return worker;
        },
      ),
    };
    const dispatcher = new QueryExecutionDispatcher(beeKeeper as any, {}, producerManager as any);
    const request = {
      approach: "approximation" as const,
      canonicalQueryId: "canonical-query",
      query: `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <mqtt://localhost:1883/wearableX> {
    ?s saref:hasValue ?value .
    ?s saref:hasTimestamp ?ts .
    ?s saref:relatesToProperty dahccsensors:wearableX .
  }
}
`,
      requestedOutputTopic: "consumer-topic",
    };

    const firstHandle = await dispatcher.createExecution(request);
    const secondHandle = await dispatcher.createExecution(request);

    expect(firstHandle.executionId).not.toBe(secondHandle.executionId);
    expect(firstHandle.sharedOutputTopic).not.toBe(secondHandle.sharedOutputTopic);
  });

  test("marks runtime handle failed and notifies listener when active worker exits", async () => {
    const worker = new FakeWorker();
    const producerManager = new FakeProducerManager();
    const beeKeeper = {
      executeQuery: jest.fn().mockImplementation(
        (
          _query: string,
          _topic: string,
          _operator: string,
          _subQueries: string[],
          _env: Record<string, string>,
          callbacks: {
            onExit?: (info: { code: number | null; signal: NodeJS.Signals | null }) => void;
            onError?: (error: Error) => void;
          },
        ) => {
          worker.callbacks = callbacks;
          return worker;
        },
      ),
    };
    const onExecutionFailed = jest.fn();
    const dispatcher = new QueryExecutionDispatcher(beeKeeper as any, {
      onExecutionFailed,
    }, producerManager as any);

    const handle = await dispatcher.createExecution({
      approach: "approximation",
      canonicalQueryId: "canonical-query",
      query: `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <mqtt://localhost:1883/wearableX> {
    ?s saref:hasValue ?value .
    ?s saref:relatesToProperty dahccsensors:wearableX .
  }
}
`,
      requestedOutputTopic: "consumer-topic",
    });

    expect(handle.state).toBe("active");
    expect(producerManager.ensureCalls).toHaveLength(1);
    expect(handle.producerIds).toEqual(["producer-1"]);
    expect(handle.producerTopics).toEqual(["chunked/producer-1"]);

    worker.callbacks.onExit?.({ code: 1, signal: null });

    expect(handle.state).toBe("failed");
    expect(producerManager.releaseCalls).toContain(handle.executionId);
    expect(onExecutionFailed).toHaveBeenCalledWith(
      "canonical-query",
      expect.stringContaining("worker exited code=1"),
    );
  });

  test("does not start approximation worker when producer startup fails", async () => {
    const beeKeeper = {
      executeQuery: jest.fn(),
    };
    const producerManager = new FakeProducerManager();
    producerManager.shouldFail = true;
    const dispatcher = new QueryExecutionDispatcher(beeKeeper as any, {}, producerManager as any);

    await expect(
      dispatcher.createExecution({
        approach: "approximation",
        canonicalQueryId: "canonical-query",
        query: `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <mqtt://localhost:1883/wearableX> {
    ?s saref:hasValue ?value .
    ?s saref:hasTimestamp ?ts .
    ?s saref:relatesToProperty dahccsensors:wearableX .
  }
}
`,
        requestedOutputTopic: "consumer-topic",
      }),
    ).rejects.toThrow("producer startup failed");

    expect(beeKeeper.executeQuery).not.toHaveBeenCalled();
  });

  test("shares chunked producers across distinct final ranges and keeps final executions separate", async () => {
    const worker = new FakeWorker();
    const producerManager = new FakeProducerManager();
    producerManager.handles = [
      {
        producerId: "producer-wearable",
        canonicalSubqueryId: "producer-wearable",
        query: "subquery-wearable",
        outputTopic: "chunked/producer-wearable",
        pid: 9001,
        state: "ready" as const,
        ready: Promise.resolve(),
        stop: async () => undefined,
      },
      {
        producerId: "producer-smartphone",
        canonicalSubqueryId: "producer-smartphone",
        query: "subquery-smartphone",
        outputTopic: "chunked/producer-smartphone",
        pid: 9002,
        state: "ready" as const,
        ready: Promise.resolve(),
        stop: async () => undefined,
      },
    ];
    (producerManager as any).getProducerSnapshots = jest.fn().mockReturnValue([
      {
        producerId: "producer-smartphone",
        canonicalSubqueryId: "producer-smartphone",
        canonicalQuery: "canonical-smartphone",
        query: "subquery-smartphone",
        outputTopic: "chunked/producer-smartphone",
        pid: 9002,
        state: "ready",
        referenceCount: 3,
        dependentExecutionIds: ["chunked-a", "chunked-b", "chunked-c"],
      },
      {
        producerId: "producer-wearable",
        canonicalSubqueryId: "producer-wearable",
        canonicalQuery: "canonical-wearable",
        query: "subquery-wearable",
        outputTopic: "chunked/producer-wearable",
        pid: 9001,
        state: "ready",
        referenceCount: 3,
        dependentExecutionIds: ["chunked-a", "chunked-b", "chunked-c"],
      },
    ]);
    const beeKeeper = {
      executeQuery: jest.fn().mockImplementation(
        (
          _query: string,
          _topic: string,
          _operator: string,
          _subQueries: string[],
          _env: Record<string, string>,
          callbacks: {
            onMessage?: (message: unknown) => void;
            onExit?: (info: { code: number | null; signal: NodeJS.Signals | null }) => void;
            onError?: (error: Error) => void;
          },
        ) => {
          callbacks.onMessage?.({ type: "chunked_worker_ready", readyAt: Date.now() });
          return worker;
        },
      ),
    };
    const dispatcher = new QueryExecutionDispatcher(beeKeeper as any, {}, producerManager as any);

    const q120 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <output-120> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
  { WINDOW <mqtt://localhost:1883/wearableX> { ?s1 saref:hasValue ?value . ?s1 saref:hasTimestamp ?ts . ?s1 saref:relatesToProperty dahccsensors:wearableX . } }
  UNION
  { WINDOW <mqtt://localhost:1883/smartphoneX> { ?s2 saref:hasValue ?value . ?s2 saref:hasTimestamp ?ts . ?s2 saref:relatesToProperty dahccsensors:smartphoneX . } }
}
`;
    const q180 = q120.replaceAll("120000", "180000").replace("output-120", "output-180");

    const firstHandle = await dispatcher.createExecution({
      approach: "chunked",
      canonicalQueryId: "canonical-120",
      query: q120,
      requestedOutputTopic: "consumer-topic-120",
    });
    const secondHandle = await dispatcher.createExecution({
      approach: "chunked",
      canonicalQueryId: "canonical-180",
      query: q180,
      requestedOutputTopic: "consumer-topic-180",
    });

    expect(producerManager.ensureCalls).toHaveLength(2);
    expect(firstHandle.executionId).not.toBe(secondHandle.executionId);
    expect(firstHandle.sharedOutputTopic).not.toBe(secondHandle.sharedOutputTopic);
    expect(firstHandle.producerIds).toEqual([
      "producer-wearable",
      "producer-smartphone",
    ]);
    expect(secondHandle.producerIds).toEqual([
      "producer-wearable",
      "producer-smartphone",
    ]);
    expect(firstHandle.producerSnapshots).toHaveLength(2);
    expect(beeKeeper.executeQuery).toHaveBeenCalledTimes(2);
  });

  test("normalizes benchmark-prefixed stream topics before deriving reusable subqueries", async () => {
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX = "bench-prefix";
    const worker = new FakeWorker();
    const producerManager = new FakeProducerManager();
    const beeKeeper = {
      executeQuery: jest.fn().mockImplementation(
        (
          _query: string,
          _topic: string,
          _operator: string,
          _subQueries: string[],
          _env: Record<string, string>,
          callbacks: {
            onExit?: (info: { code: number | null; signal: NodeJS.Signals | null }) => void;
            onError?: (error: Error) => void;
          },
        ) => {
          worker.callbacks = callbacks;
          return worker;
        },
      ),
    };
    const dispatcher = new QueryExecutionDispatcher(beeKeeper as any, {}, producerManager as any);

    await dispatcher.createExecution({
      approach: "approximation",
      canonicalQueryId: "canonical-query",
      query: `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <output> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/bench-prefix/wearableX> ON STREAM mqtt_broker:bench-prefix/wearableX [RANGE 5000 STEP 2500]
WHERE {
  WINDOW <mqtt://localhost:1883/bench-prefix/wearableX> {
    ?s saref:hasValue ?value .
    ?s saref:hasTimestamp ?ts .
    ?s saref:relatesToProperty dahccsensors:wearableX .
  }
}
`,
      requestedOutputTopic: "consumer-topic",
    });

    expect(producerManager.ensureCalls).toHaveLength(1);
    expect(producerManager.ensureCalls[0].queries[0]).toContain(
      "mqtt://localhost:1883/bench-prefix/wearableX",
    );
    expect(producerManager.ensureCalls[0].queries[0]).not.toContain(
      "mqtt://localhost:1883/bench-prefix/bench-prefix/wearableX",
    );
  });
});
