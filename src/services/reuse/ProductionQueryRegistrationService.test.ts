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
});
