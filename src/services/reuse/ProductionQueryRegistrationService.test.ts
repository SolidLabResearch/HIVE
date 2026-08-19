import {
  ActiveExecutionHandle,
  QueryReuseRegistry,
} from "../../reuse/QueryReuseRegistry";
import { RSPQLContainmentService } from "./RSPQLContainmentService";
import { ProductionQueryRegistrationService } from "./ProductionQueryRegistrationService";
import { QueryExecutionDispatcher } from "./QueryExecutionDispatcher";

function buildQuery(output: string, extraWhere = ""): string {
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX ex: <https://example.org/>

REGISTER RStream <${output}> AS
SELECT (AVG(?value) AS ?resultValue)
FROM NAMED WINDOW <https://example.org/window/main> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
WHERE {
  WINDOW <https://example.org/window/main> {
    ?s saref:hasValue ?value .
    ?s saref:hasTimestamp ?ts .
    ${extraWhere}
  }
}
`;
}

function buildRangeVariantQuery(output: string, rangeMs: number): string {
  return buildQuery(output).replace("[RANGE 120000 STEP 60000]", `[RANGE ${rangeMs} STEP 60000]`);
}

function buildNestedThingQuery(output: string, thingCount: number): string {
  const thingNames = Array.from({ length: thingCount }, (_unused, index) => `thing${index + 1}`);
  const fromClauses = thingNames
    .map(
      (thingName) =>
        `FROM NAMED WINDOW <mqtt://localhost:1883/${thingName}> ON STREAM mqtt_broker:${thingName} [RANGE 120000 STEP 60000]`,
    )
    .join("\n");
  const unionClauses = thingNames
    .map(
      (thingName) => `{
    WINDOW <mqtt://localhost:1883/${thingName}> {
      ?obs_${thingName} saref:hasValue ?value .
      ?obs_${thingName} saref:hasTimestamp ?ts .
      ?obs_${thingName} saref:relatesToProperty <https://example.org/property/sharedNumericProperty> .
    }
  }`,
    )
    .join(" UNION ");
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>

REGISTER RStream <${output}> AS
SELECT (AVG(?value) AS ?resultValue)
${fromClauses}
WHERE {
  ${unionClauses}
}
`;
}

function buildVariantQuery(
  output: string,
  variant: "base" | "comment" | "aliases" | "alpha" = "base",
): string {
  const base = buildQuery(output);
  switch (variant) {
    case "comment":
      return `# consumer specific comment\n${base}`;
    case "aliases":
      return base
        .replace("PREFIX mqtt_broker:", "PREFIX mb:")
        .replace(/mqtt_broker:/g, "mb:")
        .replace("PREFIX saref:", "PREFIX sf:")
        .replace(/saref:/g, "sf:");
    case "alpha":
      return base
        .replace(/\?s\b/g, "?obs")
        .replace(/\?value\b/g, "?reading")
        .replace(/\?ts\b/g, "?timestamp");
    default:
      return base;
  }
}

class FakeDispatcher {
  public calls: Array<{ approach: string; canonicalQueryId: string; requestedOutputTopic?: string }> = [];
  public createdHandles: ActiveExecutionHandle[] = [];
  public shouldFail = false;

  async createExecution(request: {
    approach: "fetching" | "approximation" | "chunked";
    canonicalQueryId: string;
    requestedOutputTopic?: string;
  }): Promise<ActiveExecutionHandle> {
    this.calls.push(request);
    if (this.shouldFail) {
      throw new Error("dispatcher creation failed");
    }
    const handle: ActiveExecutionHandle = {
      executionId: `${request.approach}-execution-${this.calls.length}`,
      approach: request.approach,
      canonicalQueryId: request.canonicalQueryId,
      sharedOutputTopic: `shared/${request.approach}/${this.calls.length}`,
      workerIds: [`worker-${this.calls.length}`],
      state: "active",
      stop: async () => undefined,
    };
    this.createdHandles.push(handle);
    return handle;
  }
}

describe("ProductionQueryRegistrationService", () => {
  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  test("creates one reusable runtime on first approximation registration and reuses it on hit", async () => {
    const dispatcher = new FakeDispatcher();
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(new RSPQLContainmentService()),
      dispatcher as unknown as QueryExecutionDispatcher,
    );

    const first = await service.register({
      approach: "approximation",
      query: buildQuery("consumer-1-output"),
      requestedOutputTopic: "consumer-topic-1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
    });
    const second = await service.register({
      approach: "approximation",
      query: buildQuery("consumer-2-output"),
      requestedOutputTopic: "consumer-topic-2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
    });

    expect(dispatcher.calls).toHaveLength(1);
    expect(first.executionCreated).toBe(true);
    expect(first.reuseHit).toBe(false);
    expect(second.executionCreated).toBe(false);
    expect(second.reuseHit).toBe(true);
    expect(second.executionId).toBe(first.executionId);
    expect(second.sharedOutputTopic).toBe(first.sharedOutputTopic);
  });

  test("serializes concurrent equivalent registrations to one runtime execution", async () => {
    const dispatcher = new FakeDispatcher();
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(new RSPQLContainmentService()),
      dispatcher as unknown as QueryExecutionDispatcher,
    );

    const results = await Promise.all(
      Array.from({ length: 10 }, (_, index) =>
        service.register({
          approach: "chunked",
          query: buildQuery(`consumer-${index + 1}-output`),
          requestedOutputTopic: `consumer-topic-${index + 1}`,
          ownerQueryId: `query-${index + 1}`,
          consumerId: `consumer-${index + 1}`,
        }),
      ),
    );

    expect(dispatcher.calls).toHaveLength(1);
    expect(results.filter((result) => result.reuseHit)).toHaveLength(9);
    expect(new Set(results.map((result) => result.executionId)).size).toBe(1);
  });

  test("serializes concurrent textual variants to one runtime execution", async () => {
    const dispatcher = new FakeDispatcher();
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(new RSPQLContainmentService()),
      dispatcher as unknown as QueryExecutionDispatcher,
    );
    const variants: Array<"base" | "comment" | "aliases" | "alpha"> = [
      "base",
      "comment",
      "aliases",
      "alpha",
    ];

    const results = await Promise.all(
      variants.map((variant, index) =>
        service.register({
          approach: "approximation",
          query: buildVariantQuery(`consumer-${index + 1}-output`, variant),
          requestedOutputTopic: `consumer-topic-${index + 1}`,
          ownerQueryId: `query-${index + 1}`,
          consumerId: `consumer-${index + 1}`,
        }),
      ),
    );

    expect(dispatcher.calls).toHaveLength(1);
    expect(results.filter((result) => result.reuseHit)).toHaveLength(3);
    expect(new Set(results.map((result) => result.executionId)).size).toBe(1);
  });

  test("removes stale reservations after runtime creation failure", async () => {
    const dispatcher = new FakeDispatcher();
    dispatcher.shouldFail = true;
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(new RSPQLContainmentService()),
      dispatcher as unknown as QueryExecutionDispatcher,
    );

    await expect(
      service.register({
        approach: "approximation",
        query: buildQuery("consumer-1-output"),
        requestedOutputTopic: "consumer-topic-1",
        ownerQueryId: "query-1",
        consumerId: "consumer-1",
      }),
    ).rejects.toThrow("dispatcher creation failed");

    dispatcher.shouldFail = false;
    const retry = await service.register({
      approach: "approximation",
      query: buildQuery("consumer-1-output"),
      requestedOutputTopic: "consumer-topic-1",
      ownerQueryId: "query-1-retry",
      consumerId: "consumer-1-retry",
    });

    expect(retry.executionCreated).toBe(true);
    expect(dispatcher.calls).toHaveLength(2);
  });

  test("creates separate execution when mutual containment fails", async () => {
    const dispatcher = new FakeDispatcher();
    const containmentService = {
      getNormalizedInputHash: (query: string) => query.replace(/\s+/g, " ").trim(),
      checkEquivalence: jest.fn().mockResolvedValue({
        equivalent: false,
        supported: true,
        durationMs: 1,
        forward: {
          contained: false,
          supported: true,
          durationMs: 1,
          cacheHit: false,
          direction: "subquery_in_superquery" as const,
          checkerVersion: "fake",
        },
        reverse: {
          contained: false,
          supported: true,
          durationMs: 1,
          cacheHit: false,
          direction: "subquery_in_superquery" as const,
          checkerVersion: "fake",
        },
      }),
    };
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(containmentService as any),
      dispatcher as unknown as QueryExecutionDispatcher,
    );

    const first = await service.register({
      approach: "chunked",
      query: buildQuery("consumer-1-output"),
      requestedOutputTopic: "consumer-topic-1",
      ownerQueryId: "query-1",
      consumerId: "consumer-1",
    });
    const second = await service.register({
      approach: "chunked",
      query: buildQuery(
        "consumer-2-output",
        "?s saref:relatesToProperty <https://example.org/device/wearableX> .",
      ),
      requestedOutputTopic: "consumer-topic-2",
      ownerQueryId: "query-2",
      consumerId: "consumer-2",
    });

    expect(dispatcher.calls).toHaveLength(2);
    expect(second.executionId).not.toBe(first.executionId);
    expect(second.reuseHit).toBe(false);
    expect(containmentService.checkEquivalence).toHaveBeenCalledTimes(1);
  });

  test("keeps ten nested final queries as separate executions with no final-result reuse", async () => {
    const dispatcher = new FakeDispatcher();
    const containmentService = {
      getNormalizedInputHash: (query: string) => query.replace(/\s+/g, " ").trim(),
      checkEquivalence: jest.fn().mockResolvedValue({
        equivalent: false,
        supported: true,
        durationMs: 1,
        forward: {
          contained: false,
          supported: true,
          durationMs: 1,
          cacheHit: false,
          direction: "subquery_in_superquery" as const,
          checkerVersion: "fake",
        },
        reverse: {
          contained: false,
          supported: true,
          durationMs: 1,
          cacheHit: false,
          direction: "subquery_in_superquery" as const,
          checkerVersion: "fake",
        },
      }),
    };
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(containmentService as any),
      dispatcher as unknown as QueryExecutionDispatcher,
    );

    const registrations = [];
    for (let thingCount = 1; thingCount <= 10; thingCount += 1) {
      registrations.push(
        await service.register({
          approach: "chunked",
          query: buildNestedThingQuery(`consumer-${thingCount}-output`, thingCount),
          requestedOutputTopic: `consumer-topic-${thingCount}`,
          ownerQueryId: `query-${thingCount}`,
          consumerId: `consumer-${thingCount}`,
        }),
      );
    }

    expect(dispatcher.calls).toHaveLength(10);
    expect(new Set(registrations.map((entry) => entry.canonicalQueryId)).size).toBe(10);
    expect(new Set(registrations.map((entry) => entry.executionId)).size).toBe(10);
    expect(new Set(registrations.map((entry) => entry.sharedOutputTopic)).size).toBe(10);
    for (const registration of registrations) {
      expect(registration.executionCreated).toBe(true);
      expect(registration.reuseHit).toBe(false);
      expect(registration.containmentDecision.reuseHit).toBe(false);
    }
    expect(containmentService.checkEquivalence).toHaveBeenCalledTimes(45);
  });

  test("keeps different final ranges in separate final executions with no final reuse hit", async () => {
    const dispatcher = new FakeDispatcher();
    const service = new ProductionQueryRegistrationService(
      new QueryReuseRegistry(new RSPQLContainmentService()),
      dispatcher as unknown as QueryExecutionDispatcher,
    );

    const q120 = await service.register({
      approach: "chunked",
      query: buildRangeVariantQuery("consumer-120-output", 120000),
      requestedOutputTopic: "consumer-topic-120",
      ownerQueryId: "query-120",
      consumerId: "consumer-120",
    });
    const q180 = await service.register({
      approach: "chunked",
      query: buildRangeVariantQuery("consumer-180-output", 180000),
      requestedOutputTopic: "consumer-topic-180",
      ownerQueryId: "query-180",
      consumerId: "consumer-180",
    });

    expect(dispatcher.calls).toHaveLength(2);
    expect(q120.canonicalQueryId).not.toBe(q180.canonicalQueryId);
    expect(q120.executionId).not.toBe(q180.executionId);
    expect(q120.executionCreated).toBe(true);
    expect(q180.executionCreated).toBe(true);
    expect(q120.reuseHit).toBe(false);
    expect(q180.reuseHit).toBe(false);
    expect(q120.containmentDecision.mutuallyContained).toBe(false);
    expect(q180.containmentDecision.mutuallyContained).toBe(false);
  });
});
