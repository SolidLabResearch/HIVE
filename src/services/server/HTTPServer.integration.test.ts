import { HTTPServer, RSPAgentQueryRecord } from "./HTTPServer";
import { POSTHandler } from "./POSTHandler";
import { ProductionQueryRegistrationService } from "../reuse/ProductionQueryRegistrationService";
import { QueryReuseRegistry } from "../../reuse/QueryReuseRegistry";
import { RSPQLContainmentService } from "../reuse/RSPQLContainmentService";
import {
  ActiveExecutionHandle,
} from "../../reuse/QueryReuseRegistry";
import { QueryExecutionDispatcher } from "../reuse/QueryExecutionDispatcher";

function buildQuery(output: string): string {
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
  }
}
`;
}

class FakeDispatcher {
  public calls = 0;
  public shouldFail = false;

  async createExecution(request: {
    approach: "fetching" | "approximation" | "chunked";
    canonicalQueryId: string;
  }): Promise<ActiveExecutionHandle> {
    this.calls += 1;
    if (this.shouldFail) {
      throw new Error("dispatcher creation failed");
    }
    return {
      executionId: `${request.approach}-execution-${this.calls}`,
      approach: request.approach,
      canonicalQueryId: request.canonicalQueryId,
      sharedOutputTopic: `shared/${request.approach}/${this.calls}`,
      workerIds: [`worker-${this.calls}`],
      state: "active",
      stop: async () => undefined,
    };
  }
}

describe("HTTPServer production registration integration", () => {
  let server: HTTPServer;
  let port: number;

  beforeEach(() => {
    for (const key of Object.keys(RSPAgentQueryRecord)) {
      delete RSPAgentQueryRecord[key];
    }
    port = 19080 + Math.floor(Math.random() * 500);
  });

  afterEach(async () => {
    server?.close();
    await new Promise((resolve) => setTimeout(resolve, 25));
    jest.restoreAllMocks();
  });

  test("reuses one execution across equivalent HTTP registrations", async () => {
    const dispatcher = new FakeDispatcher();
    POSTHandler.setRegistrationService(
      new ProductionQueryRegistrationService(
        new QueryReuseRegistry(new RSPQLContainmentService()),
        dispatcher as unknown as QueryExecutionDispatcher,
      ),
    );
    server = new HTTPServer(port, console);

    const firstResponse = await fetch(`http://localhost:${port}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: "query-1",
        consumer_id: "consumer-1",
        approach: "approximation",
        rspql_query: buildQuery("consumer-1-output"),
        r2s_topic: "consumer-topic-1",
        data_topic: "consumer-topic-1",
      }),
    });
    const secondResponse = await fetch(`http://localhost:${port}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: "query-2",
        consumer_id: "consumer-2",
        approach: "approximation",
        rspql_query: buildQuery("consumer-2-output"),
        r2s_topic: "consumer-topic-2",
        data_topic: "consumer-topic-2",
      }),
    });

    const firstPayload = await firstResponse.json();
    const secondPayload = await secondResponse.json();

    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);
    expect(dispatcher.calls).toBe(1);
    expect(firstPayload.executionCreated).toBe(true);
    expect(secondPayload.reuseHit).toBe(true);
    expect(secondPayload.executionId).toBe(firstPayload.executionId);
    expect(secondPayload.outputTopic).toBe(firstPayload.outputTopic);
  });

  test("returns 400 for missing approach", async () => {
    POSTHandler.setRegistrationService(
      new ProductionQueryRegistrationService(
        new QueryReuseRegistry(new RSPQLContainmentService()),
        new FakeDispatcher() as unknown as QueryExecutionDispatcher,
      ),
    );
    server = new HTTPServer(port, console);

    const response = await fetch(`http://localhost:${port}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: "query-1",
        rspql_query: buildQuery("consumer-1-output"),
        r2s_topic: "consumer-topic-1",
        data_topic: "consumer-topic-1",
      }),
    });

    expect(response.status).toBe(400);
  });

  test("returns 500 and leaves no registry record when runtime creation fails", async () => {
    const dispatcher = new FakeDispatcher();
    dispatcher.shouldFail = true;
    POSTHandler.setRegistrationService(
      new ProductionQueryRegistrationService(
        new QueryReuseRegistry(new RSPQLContainmentService()),
        dispatcher as unknown as QueryExecutionDispatcher,
      ),
    );
    server = new HTTPServer(port, console);

    const response = await fetch(`http://localhost:${port}/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: "query-fail",
        consumer_id: "consumer-fail",
        approach: "approximation",
        rspql_query: buildQuery("consumer-fail-output"),
        r2s_topic: "consumer-topic-fail",
        data_topic: "consumer-topic-fail",
      }),
    });
    const payload = await response.json();

    expect(response.status).toBe(500);
    expect(payload.error).toBe("Internal Server Error");
    expect(RSPAgentQueryRecord["query-fail"]).toBeUndefined();
  });
});
