import { QueryReuseRegistry } from "./QueryReuseRegistry";
import { RSPQLContainmentService } from "../services/reuse/RSPQLContainmentService";

function buildQuery(options?: {
  output?: string;
  prefixes?: string[];
  range?: number;
  step?: number;
  aggregation?: "AVG" | "SUM" | "COUNT";
  stream?: string;
  streamVar?: string;
  valueVar?: string;
  filter?: string;
  projection?: string;
  whereExtra?: string;
  groupBy?: string;
}): string {
  const output = options?.output ?? "consumer-output";
  const prefixes = options?.prefixes ?? [
    "PREFIX mqtt_broker: <mqtt://localhost:1883/>",
    "PREFIX saref: <https://saref.etsi.org/core/>",
    "PREFIX ex: <https://example.org/>",
  ];
  const range = options?.range ?? 120000;
  const step = options?.step ?? 60000;
  const aggregation = options?.aggregation ?? "AVG";
  const stream = options?.stream ?? "mqtt_broker:wearableX";
  const streamVar = options?.streamVar ?? "s";
  const valueVar = options?.valueVar ?? "value";
  const projection =
    options?.projection ?? `(${aggregation}(?${valueVar}) AS ?resultValue)`;
  const filter = options?.filter ? `  ${options.filter}\n` : "";
  const whereExtra = options?.whereExtra ? `  ${options.whereExtra}\n` : "";
  const groupBy = options?.groupBy ? `\nGROUP BY ${options.groupBy}` : "";

  return `
${prefixes.join("\n")}

REGISTER RStream <${output}> AS
SELECT ${projection}
FROM NAMED WINDOW <https://example.org/window/main> ON STREAM ${stream} [RANGE ${range} STEP ${step}]
WHERE {
  WINDOW <https://example.org/window/main> {
    ?${streamVar} saref:hasValue ?${valueVar} .
    ?${streamVar} saref:hasTimestamp ?ts .
${whereExtra}${filter}  }
}${groupBy}
`;
}

class FakeContainmentService {
  constructor(
    private readonly equivalentDelayMs = 0,
  ) {}

  getNormalizedInputHash(query: string): string {
    return query.replace(/\s+/g, " ").trim();
  }

  async checkEquivalence(queryA: string, queryB: string) {
    if (this.equivalentDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.equivalentDelayMs));
    }
    const equivalent =
      queryA.replace(/<consumer-[^>]+>/g, "<x>") ===
      queryB.replace(/<consumer-[^>]+>/g, "<x>");
    return {
      equivalent,
      supported: true,
      durationMs: 1,
      forward: {
        contained: equivalent,
        supported: true,
        durationMs: 1,
        cacheHit: false,
        direction: "subquery_in_superquery" as const,
        checkerVersion: "fake",
      },
      reverse: {
        contained: equivalent,
        supported: true,
        durationMs: 1,
        cacheHit: false,
        direction: "subquery_in_superquery" as const,
        checkerVersion: "fake",
      },
    };
  }
}

describe("RSPQLContainmentService", () => {
  let service: RSPQLContainmentService;

  beforeEach(() => {
    service = new RSPQLContainmentService();
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  test("treats output-target differences as semantically equivalent only through mutual containment", async () => {
    const first = buildQuery({ output: "consumer-1-output" });
    const second = buildQuery({ output: "consumer-2-output" });

    const result = await service.checkEquivalence(first, second);

    expect(result.equivalent).toBe(true);
    expect(result.forward.contained).toBe(true);
    expect(result.reverse.contained).toBe(true);
  });

  test("accepts prefix alias renaming and variable alpha-renaming", async () => {
    const first = buildQuery();
    const second = buildQuery({
      output: "consumer-2-output",
      prefixes: [
        "PREFIX b: <mqtt://localhost:1883/>",
        "PREFIX s: <https://saref.etsi.org/core/>",
        "PREFIX e: <https://example.org/>",
      ],
      stream: "b:wearableX",
      streamVar: "obs",
      valueVar: "reading",
      projection: "(AVG(?reading) AS ?finalValue)",
    })
      .replace(/saref:/g, "s:")
      .replace(/ex:/g, "e:");

    const result = await service.checkEquivalence(first, second);

    expect(result.equivalent).toBe(true);
  });

  test("accepts comment-only query variants", async () => {
    const first = buildQuery();
    const second = `# consumer specific comment\n${buildQuery({ output: "consumer-2-output" })}`;

    const result = await service.checkEquivalence(first, second);

    expect(result.equivalent).toBe(true);
    expect(result.forward.contained).toBe(true);
    expect(result.reverse.contained).toBe(true);
  });

  test("rejects different window semantics", async () => {
    const baseline = buildQuery({ range: 120000, step: 60000 });
    const differentRange = buildQuery({ range: 180000, step: 60000 });

    const result = await service.checkEquivalence(baseline, differentRange);

    expect(result.equivalent).toBe(false);
    expect(result.supported).toBe(false);
  });

  test("records one-way containment from directional checker outcomes", async () => {
    const first = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output-a> AS
SELECT (COUNT(?x) AS ?count)
FROM NAMED WINDOW ex:w1 ON STREAM ex:stream1 [RANGE 10 STEP 5]
FROM NAMED WINDOW ex:w2 ON STREAM ex:stream2 [RANGE 10 STEP 5]
WHERE {
  WINDOW ex:w1 { ?x a ex:Person . }
  WINDOW ex:w2 { ?x ex:hasAge ex:One . }
}
`;
    const second = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output-b> AS
SELECT (COUNT(?x) AS ?count)
FROM NAMED WINDOW ex:w1 ON STREAM ex:stream1 [RANGE 10 STEP 5]
WHERE {
  WINDOW ex:w1 { ?x a ex:Person . }
}
`;
    const spy = jest
      .spyOn(service as any, "runChecker")
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false);

    const forward = await service.checkContainment(first, second);
    const reverse = await service.checkContainment(second, first);

    expect(forward.contained).toBe(true);
    expect(reverse.contained).toBe(false);
    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("fails closed for unsupported query features", async () => {
    const unsupported = buildQuery({
      filter: "FILTER(?value > 10)",
      projection: "?value",
    });
    const baseline = buildQuery({ projection: "?value" });

    const result = await service.checkEquivalence(unsupported, baseline);

    expect(result.equivalent).toBe(false);
    expect(result.forward.supported).toBe(false);
    expect(result.forward.failureKind).toBe("UNSUPPORTED_QUERY");
  });

  test("fails closed for malformed queries", async () => {
    const malformed = "REGISTER RStream <x> AS SELECT WHERE {";
    const baseline = buildQuery();

    const result = await service.checkEquivalence(malformed, baseline);

    expect(result.equivalent).toBe(false);
    expect(result.forward.supported).toBe(false);
  });

  test("fails closed for unknown extension functions", async () => {
    const unknownFunction = buildQuery({
      projection: "(customFn(?value) AS ?resultValue)",
    });
    const baseline = buildQuery();

    const result = await service.checkEquivalence(unknownFunction, baseline);

    expect(result.equivalent).toBe(false);
    expect(result.forward.failureKind).toBe("UNSUPPORTED_QUERY");
  });

  test("fails closed on timeout and caches the conservative result", async () => {
    const timeoutService = new RSPQLContainmentService();
    const spy = jest
      .spyOn(timeoutService as any, "runChecker")
      .mockImplementation(async () => {
        throw new Error("Process timeout after 30 seconds");
      });

    const first = await timeoutService.checkContainment(buildQuery(), buildQuery());
    const second = await timeoutService.checkContainment(buildQuery(), buildQuery());

    expect(first.supported).toBe(false);
    expect(first.failureKind).toBe("TIMEOUT");
    expect(second.cacheHit).toBe(true);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  test("fails closed on unexpected solver exceptions", async () => {
    const brokenService = new RSPQLContainmentService();
    jest
      .spyOn(brokenService as any, "runChecker")
      .mockImplementation(async () => {
        throw new Error("solver crashed");
      });

    const result = await brokenService.checkContainment(buildQuery(), buildQuery());

    expect(result.supported).toBe(false);
    expect(result.failureKind).toBe("SOLVER_FAILURE");
  });
});

describe("QueryReuseRegistry", () => {
  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  test("reuses mutually contained final queries", async () => {
    const registry = new QueryReuseRegistry(new RSPQLContainmentService());
    const first = buildQuery({ output: "consumer-1-output" });
    const second = buildQuery({ output: "consumer-2-output" });

    const firstDecision = await registry.resolveFinalResultRegistration({
      query: first,
      resultTopic: "hive/results/final/1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
      executionId: "execution-1",
    });
    const secondDecision = await registry.resolveFinalResultRegistration({
      query: second,
      resultTopic: "hive/results/final/2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
      executionId: "execution-2",
    });

    expect(firstDecision.decision.reuseHit).toBe(false);
    expect(secondDecision.decision.reuseHit).toBe(true);
    expect(secondDecision.entry.executionId).toBe("execution-1");
    expect(secondDecision.decision.mutuallyContained).toBe(true);
  });

  test("reuses equivalent queries across prefix alias renaming in stage-1 candidate lookup", async () => {
    const containmentService = {
      getNormalizedInputHash: (query: string) => query.replace(/\s+/g, " ").trim(),
      checkEquivalence: jest.fn().mockResolvedValue({
        equivalent: true,
        supported: true,
        durationMs: 1,
        forward: {
          contained: true,
          supported: true,
          durationMs: 1,
          cacheHit: false,
          direction: "subquery_in_superquery" as const,
          checkerVersion: "fake",
        },
        reverse: {
          contained: true,
          supported: true,
          durationMs: 1,
          cacheHit: false,
          direction: "subquery_in_superquery" as const,
          checkerVersion: "fake",
        },
      }),
    };
    const registry = new QueryReuseRegistry(containmentService as any);
    const first = buildQuery({ output: "consumer-1-output" });
    const second = buildQuery({
      output: "consumer-2-output",
      prefixes: [
        "PREFIX b: <mqtt://localhost:1883/>",
        "PREFIX s: <https://saref.etsi.org/core/>",
        "PREFIX e: <https://example.org/>",
      ],
      stream: "b:wearableX",
      streamVar: "obs",
      valueVar: "reading",
      projection: "(AVG(?reading) AS ?finalValue)",
    })
      .replace(/saref:/g, "s:")
      .replace(/ex:/g, "e:");

    await registry.resolveFinalResultRegistration({
      query: first,
      resultTopic: "hive/results/final/1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
      executionId: "execution-1",
    });
    const secondDecision = await registry.resolveFinalResultRegistration({
      query: second,
      resultTopic: "hive/results/final/2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
      executionId: "execution-2",
    });

    expect(secondDecision.decision.supported).toBe(true);
    expect(secondDecision.decision.reuseHit).toBe(true);
    expect(secondDecision.entry.executionId).toBe("execution-1");
    expect(containmentService.checkEquivalence).toHaveBeenCalledTimes(1);
  });

  test("rejects final-result reuse for one-way containment", async () => {
    const registry = new QueryReuseRegistry(new RSPQLContainmentService());
    const broader = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output-a> AS
SELECT (COUNT(?x) AS ?count)
FROM NAMED WINDOW ex:w1 ON STREAM ex:stream1 [RANGE 10 STEP 5]
WHERE {
  WINDOW ex:w1 { ?x a ex:Person . }
}
`;
    const narrower = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output-b> AS
SELECT (COUNT(?x) AS ?count)
FROM NAMED WINDOW ex:w1 ON STREAM ex:stream1 [RANGE 10 STEP 5]
FROM NAMED WINDOW ex:w2 ON STREAM ex:stream2 [RANGE 10 STEP 5]
WHERE {
  WINDOW ex:w1 { ?x a ex:Person . }
  WINDOW ex:w2 { ?x ex:hasAge ex:One . }
}
`;

    await registry.resolveFinalResultRegistration({
      query: broader,
      resultTopic: "hive/results/final/1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
      executionId: "execution-1",
    });
    const secondDecision = await registry.resolveFinalResultRegistration({
      query: narrower,
      resultTopic: "hive/results/final/2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
      executionId: "execution-2",
    });

    expect(secondDecision.decision.reuseHit).toBe(false);
    expect(secondDecision.entry.executionId).toBe("execution-2");
    expect(secondDecision.decision.forwardContained).toBe(true);
    expect(secondDecision.decision.reverseContained).toBe(false);
    expect(secondDecision.decision.mutuallyContained).toBe(false);
    expect(secondDecision.decision.supported).toBe(false);
  });

  test("reuses runtime registrations across comment-only variants", async () => {
    const registry = new QueryReuseRegistry(new RSPQLContainmentService());
    const first = buildQuery({ output: "consumer-1-output" });
    const second = `# consumer specific comment\n${buildQuery({ output: "consumer-2-output" })}`;
    const createExecution = jest
      .fn()
      .mockResolvedValueOnce({
        executionId: "execution-1",
        approach: "approximation" as const,
        canonicalQueryId: "canonical-query",
        sharedOutputTopic: "shared/execution-1",
        workerIds: ["worker-1"],
        state: "active" as const,
        stop: async () => undefined,
      })
      .mockResolvedValueOnce({
        executionId: "execution-2",
        approach: "approximation" as const,
        canonicalQueryId: "canonical-query",
        sharedOutputTopic: "shared/execution-2",
        workerIds: ["worker-2"],
        state: "active" as const,
        stop: async () => undefined,
      });

    const firstDecision = await registry.resolveReusableRuntimeRegistration({
      approach: "approximation",
      query: first,
      resultTopic: "ignored-1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
      createExecution,
    });
    const secondDecision = await registry.resolveReusableRuntimeRegistration({
      approach: "approximation",
      query: second,
      resultTopic: "ignored-2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
      createExecution,
    });

    expect(firstDecision.executionCreated).toBe(true);
    expect(secondDecision.executionCreated).toBe(false);
    expect(secondDecision.decision.reuseHit).toBe(true);
    expect(secondDecision.entry.executionId).toBe("execution-1");
    expect(createExecution).toHaveBeenCalledTimes(1);
  });

  test("separates approximation executions when configs differ", async () => {
    const registry = new QueryReuseRegistry(new RSPQLContainmentService());
    const query = buildQuery();
    const firstConfigHash = QueryReuseRegistry.buildApproximationConfigHash({
      policy: "rate-based",
      rate: 0.25,
    });
    const secondConfigHash = QueryReuseRegistry.buildApproximationConfigHash({
      policy: "rate-based",
      rate: 0.5,
    });

    await registry.resolveFinalResultRegistration({
      query,
      resultTopic: "topic-1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
      executionId: "execution-1",
      approximationConfigHash: firstConfigHash,
    });
    const decision = await registry.resolveFinalResultRegistration({
      query: query.replace("consumer-output", "consumer-output-2"),
      resultTopic: "topic-2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
      executionId: "execution-2",
      approximationConfigHash: secondConfigHash,
    });

    expect(decision.decision.reuseHit).toBe(false);
    expect(decision.entry.executionId).toBe("execution-2");
  });

  test("serializes concurrent equivalent registrations to one execution", async () => {
    const registry = new QueryReuseRegistry(
      new FakeContainmentService(20) as unknown as RSPQLContainmentService,
    );

    const [first, second] = await Promise.all([
      registry.resolveFinalResultRegistration({
        query: buildQuery({ output: "consumer-1-output" }),
        resultTopic: "topic-1",
        ownerQueryId: "query-1",
        consumerId: "consumer-1",
        executionId: "execution-1",
      }),
      registry.resolveFinalResultRegistration({
        query: buildQuery({ output: "consumer-2-output" }),
        resultTopic: "topic-2",
        ownerQueryId: "query-2",
        consumerId: "consumer-2",
        executionId: "execution-2",
      }),
    ]);

    expect(first.entry.executionId).toBe("execution-1");
    expect(second.entry.executionId).toBe("execution-1");
    expect(registry.getAllEntries()).toHaveLength(1);
  });
});
