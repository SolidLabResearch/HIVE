type MockMqttClient = {
  handlers: Record<string, Function>;
  on: jest.Mock;
  subscribe: jest.Mock;
  publish: jest.Mock;
  end: jest.Mock;
  emit: (event: string, ...args: any[]) => void;
};

const mqttClients: MockMqttClient[] = [];
const turtleStringToStoreMock = jest.fn();

function createMockMqttClient(): MockMqttClient {
  const handlers: Record<string, Function> = {};
  const client: MockMqttClient = {
    handlers,
    on: jest.fn().mockImplementation((event: string, handler: Function) => {
      handlers[event] = handler;
      return client;
    }),
    subscribe: jest.fn(),
    publish: jest.fn((_topic, _payload, callback) => {
      if (typeof callback === 'function') {
        callback();
      }
    }),
    end: jest.fn(),
    emit(event: string, ...args: any[]) {
      handlers[event]?.(...args);
    },
  };
  return client;
}

function timestampStore(timestamp: string): { getQuads: jest.Mock } {
  return { getQuads: jest.fn().mockReturnValue([{ object: { value: timestamp } }]) };
}

jest.mock("mqtt", () => ({
  connect: jest.fn().mockImplementation(() => {
    const client = createMockMqttClient();
    mqttClients.push(client);
    return client;
  }),
}));

jest.mock("../util/Util", () => ({
  ...jest.requireActual("../util/Util"),
  turtleStringToStore: (...args: unknown[]) => turtleStringToStoreMock(...args),
}));

global.fetch = jest.fn().mockResolvedValue({
  ok: true,
  json: jest.fn().mockResolvedValue({}),
}) as jest.Mock;

import { RSPAgent } from "./RSPAgent";

describe("RSPAgent", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.clearAllMocks();
    mqttClients.length = 0;
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  test("should create an instance of RSPAgent", () => {
    const query = "SELECT ?s WHERE { ?s ?p ?o }";
    const rstream_topic = "test_topic";
    const agent = new RSPAgent(query, rstream_topic);
    expect(agent).toBeInstanceOf(RSPAgent);
  });

  test("should set child query and rstream topic", () => {
    const query = "SELECT ?s WHERE { ?s ?p ?o }";
    const rstream_topic = "test_topic";
    const agent = new RSPAgent(query, rstream_topic);
    expect(agent.query).toBe(query);
    expect(agent.r2s_topic).toBe(rstream_topic);
  });

  test("should initialize RSPEngine", () => {
    const query = "SELECT ?s WHERE { ?s ?p ?o }";
    const rstream_topic = "test_topic";
    const agent = new RSPAgent(query, rstream_topic);
    expect(agent.rsp_engine).toBeDefined();
  });

  test("returnStreams should return streams", () => {
    const query = `
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;
    const rstream_topic = "test_topic";
    const agent = new RSPAgent(query, rstream_topic);
    const streams = agent.returnStreams();
    expect(streams).toStrictEqual(JSON.parse(`[{"slide": 2, "stream_name": "https://rsp.js/stream1", "width": 10, "window_name": "https://rsp.js/w1"}]`));
  });

  test("shouldReturn MQTT broker with a topic as a stream name", () => {
    const query = `
        PREFIX mqtt_broker: <mqtt://localhost:1883/>
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:topic [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;

    const rstream_topic = "rstream_topic";
    const agent = new RSPAgent(query, rstream_topic);
    const streams = agent.returnStreams();
    expect(streams).toStrictEqual(JSON.parse(`[{"slide": 2, "stream_name": "mqtt://localhost:1883/topic", "width": 10, "window_name": "https://rsp.js/w1"}]`));
  });

  test("should return MQTT broker URL", () => {
    const query = `
        PREFIX mqtt_broker: <mqtt://localhost:1883/>
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:topic [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;
    const rstream_topic = "rstream_topic";
    const agent = new RSPAgent(query, rstream_topic);

    expect(agent.returnMQTTBroker("mqtt://localhost:1883/topic")).toBe("mqtt://localhost:1883/");
  });

  test("should return MQTT broker URL for more than one topic hierarchy with slash semantics", () => {
    const query = `
        PREFIX mqtt_broker: <mqtt://localhost:1883/>
        PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:topic [RANGE 10 STEP 2]
        WHERE{
            WINDOW :w1 { ?s ?p ?o}
        }
        `;

    const rstream_topic = "rstream_topic";
    const agent = new RSPAgent(query, rstream_topic);
    expect(agent.returnMQTTBroker("mqtt://localhost:1883/topic/random/sensor/room/temperature")).toBe("mqtt://localhost:1883/");
  });

  test("inserts ordered MQTT messages into RSP-JS even when an earlier parse is slower", async () => {
    const query = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX : <https://rsp.js/>
      REGISTER RStream <output> AS
      SELECT *
      FROM NAMED WINDOW :w ON STREAM mqtt_broker:ordered/input [RANGE 60000 STEP 60000]
      WHERE { WINDOW :w { ?s ?p ?o } }
    `;
    let releaseFirst: (() => void) | undefined;
    const firstParsed = new Promise<any>((resolve) => { releaseFirst = () => resolve(timestampStore("2026-01-01T00:00:05.000Z")); });
    turtleStringToStoreMock.mockImplementation((payload: string) =>
      payload === "first" ? firstParsed : Promise.resolve(timestampStore("2026-01-01T00:02:05.001Z")),
    );
    const agent = new RSPAgent(query, "chunked/ordered-test");
    const inserted: number[] = [];
    jest.spyOn(agent, "add_event_store_to_rsp_engine").mockImplementation(async (_store, _streams, timestamp) => {
      inserted.push(timestamp);
    });
    const started = agent.process_streams();
    const inputClient = mqttClients[1];
    inputClient.subscribe.mockImplementation((_topic: string, callback: (error?: Error) => void) => callback());
    inputClient.emit("connect");
    await started;

    inputClient.emit("message", "ordered/input", Buffer.from("first"));
    inputClient.emit("message", "ordered/input", Buffer.from("sentinel"));
    await Promise.resolve();
    expect(inserted).toEqual([]);
    releaseFirst?.();
    await new Promise<void>((resolve) => setImmediate(resolve));
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(inserted).toEqual([
      Date.parse("2026-01-01T00:00:05.000Z"),
      Date.parse("2026-01-01T00:02:05.001Z"),
    ]);
  });

  test("publishes structured reusable_result payloads with reconstructed window metadata", () => {
    const query = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX : <https://rsp.js/>
      REGISTER RStream <output> AS
      SELECT (AVG(?v) AS ?avgTemp)
      FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:wearable/temperature [RANGE 120000 STEP 60000]
      WHERE {
        WINDOW :w1 { ?sensor :value ?v }
      }
    `;
    const agent = new RSPAgent(query, "chunked/test-hash");
    const publisher = mqttClients[0];
    expect(publisher).toBeDefined();

    publisher.emit("connect");
    agent.rstream_emitter.emit("RStream", {
      timestamp_from: 0,
      timestamp_to: 120000,
      bindings: new Map([
        [{ value: "?avgTemp" }, { value: "21.5" }],
      ]),
    });

    expect(publisher.publish).toHaveBeenCalledTimes(1);
    const [topic, rawPayload] = publisher.publish.mock.calls[0];
    expect(topic).toBe("chunked/test-hash");

    const payload = JSON.parse(rawPayload);
    expect(payload.message_format).toBe("structured_reusable_result");
    expect(payload.source_query_id).toBeDefined();
    expect(payload.source_topic).toBe("wearable/temperature");
    expect(payload.reusable_result_topic).toBe("chunked/test-hash");
    expect(payload.aggregationType).toBe("AVG");
    expect(payload.value).toBe(21.5);
    expect(payload.window_start).toBe(0);
    expect(payload.window_end).toBe(120000);
    expect(payload.window_data_close_time).toBe(120000);
    expect(payload.timestamp_from).toBe(0);
    expect(payload.timestamp_to).toBe(120000);
    expect(payload.window).toMatchObject({
      start: 0,
      end: 120000,
      range: 120000,
      step: 60000,
      semantics: "[start,end)",
      metadataSource: "reconstructed",
    });
  });

  test("publishes compact structured reusable_result payloads when enabled", () => {
    process.env.STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD = "1";
    const query = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX : <https://rsp.js/>
      REGISTER RStream <output> AS
      SELECT (AVG(?v) AS ?avgTemp)
      FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:wearable/temperature [RANGE 120000 STEP 60000]
      WHERE {
        WINDOW :w1 { ?sensor :value ?v }
      }
    `;
    const agent = new RSPAgent(query, "chunked/test-hash");
    const publisher = mqttClients[0];
    expect(publisher).toBeDefined();

    publisher.emit("connect");
    agent.rstream_emitter.emit("RStream", {
      timestamp_from: 0,
      timestamp_to: 120000,
      bindings: new Map([
        [{ value: "?avgTemp" }, { value: "21.5" }],
      ]),
    });

    expect(publisher.publish).toHaveBeenCalledTimes(1);
    const [, rawPayload] = publisher.publish.mock.calls[0];
    const payload = JSON.parse(rawPayload);
    expect(payload).toMatchObject({
      message_format: "structured_reusable_result",
      source_query_id: expect.any(String),
      source_topic: "wearable/temperature",
      aggregationType: "AVG",
      value: 21.5,
      window_start: 0,
      window_end: 120000,
      window_data_close_time: 120000,
      logical_trigger_time: 60000,
    });
    expect(payload.reusable_result_topic).toBeUndefined();
    expect(payload.resultValue).toBeUndefined();
    expect(payload.raw_bindings).toBeUndefined();
    expect(payload.timestamp_from).toBeUndefined();
    expect(payload.timestamp_to).toBeUndefined();
    expect(payload.window).toBeUndefined();
  });

  test("raw producer mode does not register through HTTP", () => {
    const query = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX : <https://rsp.js/>
      REGISTER RStream <output> AS
      SELECT (AVG(?v) AS ?avgTemp)
      FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:wearable/temperature [RANGE 120000 STEP 60000]
      WHERE {
        WINDOW :w1 { ?sensor :value ?v }
      }
    `;

    new RSPAgent(query, "chunked/test-hash", {
      registerQueryDefinition: false,
    });

    expect(global.fetch).not.toHaveBeenCalled();
  });

  test("Case D managed producer requires an authoritative coverage origin", () => {
    const query = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX : <https://rsp.js/>
      REGISTER RStream <output> AS
      SELECT (AVG(?v) AS ?avgTemp)
      FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:wearable/temperature [RANGE 60000 STEP 30000]
      WHERE { WINDOW :w1 { ?sensor :value ?v } }
    `;

    expect(() => new RSPAgent(query, "chunked/test-hash", {
      registerQueryDefinition: false,
      managedProducer: true,
      producerIdentity: {
        canonicalProducerId: "canonical",
        runtimeProducerId: "runtime",
      },
    } as any)).toThrow("coverage origin must be finite");
  });

  test("Case C marks sparse managed windows complete from raw bounds and the input watermark", () => {
    const origin = 1_000_000;
    const query = `
      PREFIX mqtt_broker: <mqtt://localhost:1883/>
      PREFIX : <https://rsp.js/>
      REGISTER RStream <output> AS
      SELECT (AVG(?v) AS ?avgTemp)
      FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:wearable/temperature [RANGE 60000 STEP 30000]
      WHERE { WINDOW :w1 { ?sensor :value ?v } }
    `;
    const agent = new RSPAgent(query, "chunked/test-hash", {
      registerQueryDefinition: false,
      managedProducer: true,
      producerCoverageOrigin: origin,
      producerIdentity: {
        canonicalProducerId: "canonical",
        runtimeProducerId: "runtime",
      },
    } as any);
    const publisher = mqttClients[0];
    publisher.emit("connect");

    agent.rstream_emitter.emit("RStream", {
      timestamp_from: origin,
      timestamp_to: origin + 60_000,
      result_emitted_at: origin + 60_000,
      bindings: new Map([
        [{ value: "?avgTemp" }, { value: "12.5" }],
      ]),
    });

    const payload = JSON.parse(publisher.publish.mock.calls[0][1]);
    expect(payload).toMatchObject({
      rawWindowStart: origin,
      rawWindowEnd: origin + 60_000,
      inputWatermark: origin + 60_000,
      producerCoverageOrigin: origin,
      temporallyComplete: true,
    });
  });
});
