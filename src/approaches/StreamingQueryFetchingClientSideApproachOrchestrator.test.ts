import { EventEmitter } from "events";
import os from "os";
import path from "path";

type MockMqttClient = {
  handlers: Record<string, Function>;
  on: jest.Mock;
  once: jest.Mock;
  subscribe: jest.Mock;
  publish: jest.Mock;
  end: jest.Mock;
  emit: (event: string, ...args: any[]) => void;
};

const mqttClients: MockMqttClient[] = [];
let lastRStreamEmitter: EventEmitter | null = null;
let logRoot: string;
const createdOperators: FetchingAllDataClientSide[] = [];

function createMockMqttClient(): MockMqttClient {
  const handlers: Record<string, Function> = {};
  const client: MockMqttClient = {
    handlers,
    on: jest.fn().mockImplementation((event: string, handler: Function) => {
      handlers[event] = handler;
      return client;
    }),
    once: jest.fn().mockImplementation((event: string, handler: Function) => {
      handlers[event] = handler;
      return client;
    }),
    subscribe: jest.fn(),
    publish: jest.fn((_topic, _payload, _optionsOrCb, maybeCb) => {
      const callback =
        typeof _optionsOrCb === "function" ? _optionsOrCb : maybeCb;
      if (callback) {
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

jest.mock("mqtt", () => ({
  connect: jest.fn().mockImplementation(() => {
    const client = createMockMqttClient();
    mqttClients.push(client);
    return client;
  }),
}));

jest.mock("rsp-js", () => {
  const { EventEmitter } = require("events");
  return {
    RSPEngine: jest.fn().mockImplementation(() => ({
      register: jest.fn().mockImplementation(() => {
        lastRStreamEmitter = new EventEmitter();
        return lastRStreamEmitter;
      }),
      getStream: jest.fn().mockReturnValue({ add: jest.fn() }),
    })),
    RSPQLParser: jest.fn().mockImplementation(() => ({
      parse: jest.fn().mockReturnValue({
        s2r: [
          { stream_name: "mqtt://localhost:1883/wearableX" },
          { stream_name: "mqtt://localhost:1883/smartphoneX" },
        ],
      }),
    })),
    RDFStream: jest.fn(),
  };
});

jest.mock("../util/logger/CSVLogger", () => ({
  CSVLogger: jest.fn().mockImplementation(() => ({ log: jest.fn() })),
}));

jest.mock("fs", () => ({
  ...jest.requireActual("fs"),
  existsSync: jest.fn().mockReturnValue(true),
  writeFileSync: jest.fn(),
  createWriteStream: jest.fn().mockReturnValue({ write: jest.fn(), end: jest.fn() }),
}));

import { FetchingAllDataClientSide } from "./StreamingQueryFetchingClientSideApproachOrchestrator";

const QUERY = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avg)
FROM NAMED WINDOW :wout ON STREAM :streamOut [RANGE 120000 STEP 60000]
WHERE { WINDOW :wout { ?sensor :value ?v } }
`;

const WINDOW_START = 1756123145256;
const WINDOW_END = 1756123265256;
const FIRST_EVENT_ISO = new Date(WINDOW_START).toISOString();
const LAST_EVENT_ISO = new Date(WINDOW_END - 1000).toISOString();

function buildCompleteRStreamObject(eventCount = 2400, avgValue = 1.0160224011111112) {
  return {
    window: {
      open: WINDOW_START,
      close: WINDOW_END,
    },
    bindings: [
      new Map([
        ["?resultValue", { value: String(avgValue) }],
        ["?eventCount", { value: String(eventCount) }],
        ["?sumValue", { value: String(avgValue * eventCount) }],
        ["?avgValue", { value: String(avgValue) }],
        ["?firstEventTimestamp", { value: FIRST_EVENT_ISO }],
        ["?lastEventTimestamp", { value: LAST_EVENT_ISO }],
      ]),
    ],
  };
}

function buildPartialRStreamObject(eventCount = 1200, avgValue = 1) {
  return {
    window: {
      open: WINDOW_START,
      close: WINDOW_END,
    },
    bindings: [
      new Map([
        ["?resultValue", { value: String(avgValue) }],
        ["?eventCount", { value: String(eventCount) }],
        ["?sumValue", { value: String(avgValue * eventCount) }],
        ["?avgValue", { value: String(avgValue) }],
        ["?firstEventTimestamp", { value: FIRST_EVENT_ISO }],
        ["?lastEventTimestamp", { value: new Date(WINDOW_START + 59_000).toISOString() }],
      ]),
    ],
  };
}

function configureEnv(overrides: Record<string, string>) {
  process.env = {
    ...process.env,
    DATA_PATH: "approximation_test/challenging/exponential_growth",
    WEARABLE_FREQUENCY: "10",
    OUTPUT_WINDOW_RANGE: "120000",
    OUTPUT_WINDOW_STEP: "60000",
    LOG_PATH: logRoot,
    ...overrides,
  };
}

describe("StreamingQueryFetchingClientSideApproachOrchestrator timing filter", () => {
  const originalEnv = process.env;
  const originalNow = Date.now;

  beforeEach(() => {
    jest.clearAllMocks();
    mqttClients.length = 0;
    createdOperators.length = 0;
    lastRStreamEmitter = null;
    Date.now = jest.fn(() => 1782246735560);
    logRoot = jest.requireActual("fs").mkdtempSync(path.join(os.tmpdir(), "fetching-client-test-"));
  });

  afterEach(async () => {
    await Promise.all(createdOperators.map((operator) => operator.cleanup()));
    process.env = originalEnv;
    Date.now = originalNow;
  });

  function createOperator(envOverrides: Record<string, string>) {
    configureEnv(envOverrides);
    const operator = new FetchingAllDataClientSide(
      QUERY,
      "mqtt://localhost:1883/result",
      "AVG",
    );
    createdOperators.push(operator);
    expect(lastRStreamEmitter).toBeTruthy();
    return operator;
  }

  async function waitForCondition(
    predicate: () => boolean,
    attempts = 50,
  ) {
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      if (predicate()) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }

  async function waitForAsyncProcessing(predicate?: () => boolean) {
    if (predicate) {
      await waitForCondition(predicate);
    }
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
  }

  function primeCompleteWindow(operator: FetchingAllDataClientSide) {
    const instance = operator as any;
    const wearableObservations = Array.from({ length: 1200 }, (_value, index) => ({
      timestamp: WINDOW_START + index * 100,
      value: 1,
    }));
    const smartphoneObservations = Array.from({ length: 1200 }, (_value, index) => ({
      timestamp: WINDOW_START + index * 100,
      value: 1,
    }));
    instance.observationsByStream.set("mqtt://localhost:1883/wearableX", wearableObservations);
    instance.observationsByStream.set("mqtt://localhost:1883/smartphoneX", smartphoneObservations);
    instance.latestObservationTimestampByStream.set("mqtt://localhost:1883/wearableX", WINDOW_END + 1);
    instance.latestObservationTimestampByStream.set("mqtt://localhost:1883/smartphoneX", WINDOW_END + 1);
    instance.benchmarkReplayComplete = true;
    instance.benchmarkFiniteReplayMode = true;
    instance.firstDataReceivedTime = 1782246366110;
    instance.lastObservationReceivedTime = 1782246673778;
    instance.queryRegisteredTime = 1782246364099;
  }

  test("deterministic benchmark mode bypasses cadence filtering for complete aligned windows", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_FETCHING_DISABLE_CADENCE_FILTER: "0",
    });
    primeCompleteWindow(operator);

    const timingSpy = jest.spyOn(operator as any, "isWithinExpectedWindowTiming");
    lastRStreamEmitter?.emit("RStream", buildCompleteRStreamObject());
    await waitForAsyncProcessing(() => mqttClients.length === 1);

    expect(timingSpy).not.toHaveBeenCalled();
    expect((operator as any).acceptedCompleteWindowCount).toBe(1);
    expect((operator as any).filteredDueToTimingCount).toBe(0);
    expect(mqttClients).toHaveLength(1);
    mqttClients[0].emit("connect");
    expect(mqttClients[0].publish).toHaveBeenCalledTimes(1);
  });

  test("legacy mode still applies cadence filtering", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "0",
      STREAMING_QUERY_HIVE_FETCHING_DISABLE_CADENCE_FILTER: "0",
    });
    primeCompleteWindow(operator);

    const timingSpy = jest.spyOn(operator as any, "isWithinExpectedWindowTiming").mockReturnValue(false);
    lastRStreamEmitter?.emit("RStream", buildCompleteRStreamObject());
    await waitForAsyncProcessing(() => (operator as any).filteredDueToTimingCount === 1);

    expect(timingSpy).toHaveBeenCalledTimes(1);
    expect((operator as any).acceptedCompleteWindowCount).toBe(0);
    expect((operator as any).filteredDueToTimingCount).toBe(1);
    expect(mqttClients).toHaveLength(0);
  });

  test("incomplete windows are still suppressed in deterministic mode", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_FETCHING_DISABLE_CADENCE_FILTER: "0",
    });
    primeCompleteWindow(operator);

    jest.spyOn(operator as any, "computeSettledWindowAggregate").mockReturnValue({
      eventCount: 1800,
      sumValue: 1800,
      avgValue: 1,
      minValue: 1,
      maxValue: 1,
    });

    lastRStreamEmitter?.emit("RStream", buildCompleteRStreamObject(1800));
    await waitForAsyncProcessing(() => (operator as any).logicalWindowCandidates.size === 1);

    expect((operator as any).acceptedCompleteWindowCount).toBe(0);
    expect((operator as any).filteredDueToTimingCount).toBe(0);
    expect(mqttClients).toHaveLength(0);
  });

  test("startup-first-emitted mode suppresses partial startup rows before full range coverage", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE: "1",
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(WINDOW_START),
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "5",
    });
    primeCompleteWindow(operator);
    lastRStreamEmitter?.emit("RStream", buildPartialRStreamObject());
    await waitForAsyncProcessing(() => (operator as any).logicalWindowCandidates.size === 1);

    expect((operator as any).acceptedCompleteWindowCount).toBe(0);
    expect((operator as any).windowCount).toBe(0);
    expect((operator as any).artifactState.finalizedWindowNumbers).toEqual([]);
    expect(mqttClients).toHaveLength(0);
  });

  test("startup-first-emitted mode counts the first settled complete 120-second window", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE: "1",
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: String(WINDOW_START),
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "5",
    });
    primeCompleteWindow(operator);

    lastRStreamEmitter?.emit("RStream", buildCompleteRStreamObject(2400, 1));
    await waitForAsyncProcessing(() => mqttClients.length === 1);

    expect((operator as any).acceptedCompleteWindowCount).toBe(1);
    expect((operator as any).windowCount).toBe(1);
    expect((operator as any).artifactState.finalizedWindowNumbers).toEqual([1]);
    expect(mqttClients).toHaveLength(1);
    mqttClients[0].emit("connect");
    expect(mqttClients[0].publish).toHaveBeenCalledTimes(1);
  });

  test("duplicate windows are still suppressed in deterministic mode", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_FETCHING_DISABLE_CADENCE_FILTER: "0",
    });
    primeCompleteWindow(operator);

    const key = `${WINDOW_START}:${WINDOW_END}`;
    (operator as any).emittedLogicalWindows.add(key);
    lastRStreamEmitter?.emit("RStream", buildCompleteRStreamObject());
    await waitForAsyncProcessing(() => (operator as any).acceptedCompleteWindowCount === 0);

    expect((operator as any).acceptedCompleteWindowCount).toBe(0);
    expect(mqttClients).toHaveLength(0);
  });

  test("benchmark target window cap stops after the requested number of finalized windows", () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "3",
    });
    const recordSpy = jest.spyOn(operator as any, "recordFinalizedWindow");

    (operator as any).recordFinalizedWindow(1);
    (operator as any).recordFinalizedWindow(2);
    (operator as any).recordFinalizedWindow(3);

    expect(recordSpy).toHaveBeenCalledTimes(3);
    expect((operator as any).artifactState.finalizedWindowNumbers).toEqual([1, 2, 3]);
    expect((operator as any).artifactState.benchmarkTargetWindowReached).toBe(true);
    expect((operator as any).artifactState.benchmarkStopReason).toBe("target_window_count_reached");
  });

  test("benchmark target window cap suppresses complete windows beyond the cap", async () => {
    const operator = createOperator({
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME: "1",
      STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "3",
    });
    primeCompleteWindow(operator);
    (operator as any).acceptedCompleteWindowCount = 3;

    lastRStreamEmitter?.emit("RStream", buildCompleteRStreamObject());
    await waitForAsyncProcessing(() => (operator as any).artifactState.benchmarkTargetWindowReached === true);

    expect((operator as any).acceptedCompleteWindowCount).toBe(3);
    expect((operator as any).artifactState.benchmarkTargetWindowReached).toBe(true);
    expect(mqttClients).toHaveLength(0);
  });
});
