type MockMqttClient = {
  handlers: Record<string, Function>;
  on: jest.Mock;
  once: jest.Mock;
  subscribe: jest.Mock;
  publish: jest.Mock;
  end: jest.Mock;
  connected: boolean;
  emit: (event: string, ...args: any[]) => void;
};

const mqttClients: MockMqttClient[] = [];

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
    connected: true,
    emit(event: string, ...args: any[]) {
      handlers[event]?.(...args);
    },
  };
  return client;
}

jest.mock('mqtt', () => ({
  connect: jest.fn().mockImplementation(() => {
    const client = createMockMqttClient();
    mqttClients.push(client);
    return client;
  }),
}));

const csvLoggerLog = jest.fn();

jest.mock('../../util/logger/CSVLogger', () => ({
  CSVLogger: jest.fn().mockImplementation(() => ({ log: csvLoggerLog })),
}));

jest.mock('fs', () => ({
  ...jest.requireActual('fs'),
  existsSync: jest.fn().mockReturnValue(true),
  writeFileSync: jest.fn(),
  createWriteStream: jest.fn().mockReturnValue({ write: jest.fn(), end: jest.fn() }),
}));

import {
  ApproximationApproachOperator,
} from './RateBasedApproximationApproachOperator';
import { mergeMultipleSlidingWindowResults } from './approximation/RateBasedApproximationMath';
import {
  appendTopicResult,
  cleanupOldWindows,
  computeTopicLevelApproximationResult,
  getActiveWindowCount,
  getLatestTopicValue,
  TopicWindowBuffers,
} from './approximation/ApproximationWindowBuffer';
import { buildSubqueryRuntimeIdentity } from '../reuse/SubqueryRuntimeIdentity';

const OUTPUT_QUERY = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avg)
FROM NAMED WINDOW :wout ON STREAM :streamOut [RANGE 120000 STEP 60000]
WHERE { WINDOW :wout { ?sensor :value ?v } }
`;

const SUBQUERY_A = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 120000 STEP 60000]
WHERE { WINDOW :w1 { ?sensor :value ?v } }
`;

const SUBQUERY_B = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w2 ON STREAM :stream2 [RANGE 120000 STEP 60000]
WHERE { WINDOW :w2 { ?sensor :value ?v } }
`;

const TOPIC_A = buildSubqueryRuntimeIdentity(SUBQUERY_A).outputTopic;
const TOPIC_B = buildSubqueryRuntimeIdentity(SUBQUERY_B).outputTopic;
const PRODUCTION_STYLE_OUTPUT_QUERY = `
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX om: <http://www.ontology-of-units-of-measure.org/resource/om-2/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX : <http://example.com/>
PREFIX mqtt_broker: <http://example.com/mqtt/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
REGISTER RStream <consumer-output-topic> AS
SELECT (AVG(?value) AS ?avg)
FROM NAMED WINDOW <wearableX> ON STREAM mqtt_broker:wearableX [RANGE 5000 STEP 2500]
FROM NAMED WINDOW <smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 5000 STEP 2500]
WHERE {
  WINDOW <wearableX> {
    ?obs saref:relatesToProperty dahccsensors:wearableX .
    ?obs saref:hasValue ?wearableValue .
    BIND(xsd:float(?wearableValue) AS ?w)
  }
  WINDOW <smartphoneX> {
    ?obs saref:relatesToProperty dahccsensors:smartphoneX .
    ?obs saref:hasValue ?smartphoneValue .
    BIND(xsd:float(?smartphoneValue) AS ?s)
  }
  BIND((?w + ?s) / 2 AS ?value)
}
`;

describe('ApproximationApproachOperator', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.clearAllMocks();
    mqttClients.length = 0;
    process.env = {
      ...originalEnv,
      STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: '1',
      STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: '0',
      STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: '0',
    };
    delete process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS;
    delete process.env.LOG_PATH;
    delete process.env.K_SCALING_CONSUMER_INDEX;
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  function createOperator(): ApproximationApproachOperator {
    const operator = new ApproximationApproachOperator();
    operator.addSubQuery(SUBQUERY_A);
    operator.addSubQuery(SUBQUERY_B);
    operator.addOutputQuery(OUTPUT_QUERY);
    return operator;
  }

  function createOperatorWithOutputQuery(
    outputQuery: string,
  ): ApproximationApproachOperator {
    const operator = new ApproximationApproachOperator();
    operator.addSubQuery(SUBQUERY_A);
    operator.addSubQuery(SUBQUERY_B);
    operator.addOutputQuery(outputQuery);
    return operator;
  }

  async function startOperator() {
    const operator = createOperator();
    await operator.init();
    await operator.handleAggregation();
    const client = mqttClients[0];
    expect(client).toBeDefined();
    client.emit('connect');
    return { operator, client };
  }

  async function startOperatorWithOutputQuery(outputQuery: string) {
    const operator = createOperatorWithOutputQuery(outputQuery);
    await operator.init();
    await operator.handleAggregation();
    const client = mqttClients[0];
    expect(client).toBeDefined();
    client.emit('connect');
    return { operator, client };
  }

  test('reuses cached topic window parsing without changing output', async () => {
    const operator = new ApproximationApproachOperator();
    const topics = [
      {
        r2s_topic: 'chunked/a',
        rspql_query: `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
WHERE { WINDOW :w1 { ?sensor :value ?v } }
        `,
      },
      {
        r2s_topic: 'chunked/b',
        rspql_query: `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w2 ON STREAM :stream2 [RANGE 10 STEP 2]
WHERE { WINDOW :w2 { ?sensor :value ?v } }
        `,
      },
    ];

    const parseSpy = jest.spyOn((operator as any).parser, 'parse');
    const first = await operator.createTopicWindowParameters(topics);
    const callsAfterFirst = parseSpy.mock.calls.length;
    const second = await operator.createTopicWindowParameters(topics);

    expect(second).toEqual(first);
    expect(parseSpy.mock.calls.length).toBe(callsAfterFirst);
  });

  test('adapts legacy reusable_result payloads with timestamp metadata into structured windows', () => {
    const operator = new ApproximationApproachOperator();

    const parsed = (operator as any).parseApproximationWindowMessage(
      JSON.stringify({
        source_query_id: 'subquery-a',
        aggregationType: 'AVG',
        timestamp_from: 0,
        timestamp_to: 120000,
        value: 12.5,
      }),
      'chunked/a',
      'AVG',
    );

    expect(parsed).toEqual({
      kind: 'adapted_legacy',
      windowStart: 0,
      windowEnd: 120000,
      value: 12.5,
      aggregationType: 'AVG',
      sourceTopic: 'chunked/a',
    });
  });

  test('parses compact structured reusable_result payloads without legacy fallback', () => {
    const operator = new ApproximationApproachOperator();

    const parsed = (operator as any).parseApproximationWindowMessage(
      JSON.stringify({
        message_format: 'structured_reusable_result',
        source_query_id: 'subquery-a',
        source_topic: 'wearable/temperature',
        aggregationType: 'AVG',
        value: 12.5,
        window_start: 0,
        window_end: 120000,
        window_data_close_time: 120000,
      }),
      'chunked/a',
      'AVG',
    );

    expect(parsed).toEqual({
      kind: 'structured',
      windowStart: 0,
      windowEnd: 120000,
      value: 12.5,
      aggregationType: 'AVG',
      sourceTopic: 'wearable/temperature',
    });
  });

  test('completed-window mode emits correctly from compact structured reusable_result payloads', async () => {
    const { client } = await startOperator();

    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-a',
      source_topic: 'stream1',
      aggregationType: 'AVG',
      value: 10,
      window_start: 0,
      window_end: 120000,
      window_data_close_time: 120000,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-b',
      source_topic: 'stream2',
      aggregationType: 'AVG',
      value: 20,
      window_start: 0,
      window_end: 120000,
      window_data_close_time: 120000,
    })));
    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-a',
      source_topic: 'stream1',
      aggregationType: 'AVG',
      value: 30,
      window_start: 60000,
      window_end: 180000,
      window_data_close_time: 180000,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-b',
      source_topic: 'stream2',
      aggregationType: 'AVG',
      value: 40,
      window_start: 60000,
      window_end: 180000,
      window_data_close_time: 180000,
    })));
    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-a',
      source_topic: 'stream1',
      aggregationType: 'AVG',
      value: 50,
      window_start: 120000,
      window_end: 240000,
      window_data_close_time: 240000,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-b',
      source_topic: 'stream2',
      aggregationType: 'AVG',
      value: 60,
      window_start: 120000,
      window_end: 240000,
      window_data_close_time: 240000,
    })));

    expect(client.publish).toHaveBeenCalledTimes(3);
    const payloads = client.publish.mock.calls.map((call) => JSON.parse(call[1]));
    expect(payloads.map((payload) => payload.windowNumber)).toEqual([1, 2, 3]);
    expect(payloads.map((payload) => payload.window)).toEqual([
      { start: 0, end: 120000 },
      { start: 60000, end: 180000 },
      { start: 120000, end: 240000 },
    ]);
  });

  test('falls back to query-text window extraction for production-style final queries', async () => {
    process.env.RESULT_TOPIC = 'shared/approximation-test/results';
    const { client } = await startOperatorWithOutputQuery(
      PRODUCTION_STYLE_OUTPUT_QUERY,
    );

    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-a',
      source_topic: 'wearableX',
      aggregationType: 'AVG',
      value: 10,
      window_start: 0,
      window_end: 5000,
      window_data_close_time: 5000,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      message_format: 'structured_reusable_result',
      source_query_id: 'subquery-b',
      source_topic: 'smartphoneX',
      aggregationType: 'AVG',
      value: 20,
      window_start: 0,
      window_end: 5000,
      window_data_close_time: 5000,
    })));

    expect(client.publish).toHaveBeenCalledWith(
      'shared/approximation-test/results',
      expect.any(String),
      { qos: 1 },
      expect.any(Function),
    );
  });

  test('completed-window mode suppresses legacy messages that lack window metadata', async () => {
    const { client } = await startOperator();

    client.emit('message', TOPIC_A, Buffer.from('7'));

    expect(client.publish).not.toHaveBeenCalled();
    expect(csvLoggerLog).toHaveBeenCalledWith(
      expect.stringContaining(
        'branch=suppressed_missing_window_metadata',
      ),
    );
  });

  test('completed-window mode does not emit before aligned close and emits windows 1, 2, 3', async () => {
    const { client } = await startOperator();

    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 0,
      timestamp_to: 120000,
      value: 10,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 0,
      timestamp_to: 120000,
      value: 20,
    })));

    expect(client.publish).toHaveBeenCalledTimes(1);
    const firstPayload = JSON.parse(client.publish.mock.calls[0][1]);
    expect(firstPayload.windowNumber).toBe(1);
    expect(firstPayload.window).toEqual({ start: 0, end: 120000 });
    expect(firstPayload.eventTimeWindowClose).toBe(120000);
    expect(firstPayload.anchorAlignedWindowClose).toBeUndefined();
    expect(firstPayload.anchorAlignedWindowCloseToResultMs).toBeUndefined();

    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 60000,
      timestamp_to: 180000,
      value: 30,
    })));

    expect(client.publish).toHaveBeenCalledTimes(1);

    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 60000,
      timestamp_to: 180000,
      value: 40,
    })));

    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 120000,
      timestamp_to: 240000,
      value: 50,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 120000,
      timestamp_to: 240000,
      value: 60,
    })));

    expect(client.publish).toHaveBeenCalledTimes(3);

    const payloads = client.publish.mock.calls.map((call) => JSON.parse(call[1]));
    expect(payloads.map((payload) => payload.windowNumber)).toEqual([1, 2, 3]);
    expect(payloads.map((payload) => payload.window)).toEqual([
      { start: 0, end: 120000 },
      { start: 60000, end: 180000 },
      { start: 120000, end: 240000 },
    ]);
  });

  test('completed-window mode records finalized window numbers from the emitted publish callback window', async () => {
    const { client, operator } = await startOperator();
    client.publish.mockImplementation((_topic, _payload, _optionsOrCb, maybeCb) => {
      const callback =
        typeof _optionsOrCb === "function" ? _optionsOrCb : maybeCb;
      if (callback) {
        setTimeout(() => callback(), 0);
      }
    });

    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 0,
      timestamp_to: 120000,
      value: 10,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 0,
      timestamp_to: 120000,
      value: 20,
    })));
    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 60000,
      timestamp_to: 180000,
      value: 30,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 60000,
      timestamp_to: 180000,
      value: 40,
    })));
    client.emit('message', TOPIC_A, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 120000,
      timestamp_to: 240000,
      value: 50,
    })));
    client.emit('message', TOPIC_B, Buffer.from(JSON.stringify({
      aggregationType: 'AVG',
      timestamp_from: 120000,
      timestamp_to: 240000,
      value: 60,
    })));

    await new Promise((resolve) => setTimeout(resolve, 0));

    expect((operator as any).finalizedWindowNumbers).toEqual([1, 2, 3]);
  });

  test('early-trigger mode still accepts legacy scalar messages when explicitly enabled', async () => {
    process.env.STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE = '0';
    process.env.STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE = '1';
    const realNow = Date.now;
    let now = 0;
    Date.now = jest.fn(() => now);

    try {
      const { client } = await startOperator();

      now = 0;
      client.emit('message', TOPIC_A, Buffer.from('10'));
      client.emit('message', TOPIC_B, Buffer.from('20'));

      now = 61000;
      client.emit('message', TOPIC_A, Buffer.from('30'));

      expect(client.publish).toHaveBeenCalledTimes(1);
      const payload = JSON.parse(client.publish.mock.calls[0][1]);
      expect(payload.approach).toBe('approximation');
      expect(payload.windowNumber).toBe(1);
      expect(payload.window.start).toBeLessThan(payload.window.end);
      expect(csvLoggerLog).toHaveBeenCalledWith(
        expect.stringContaining('Triggering aggregation for window'),
      );
    } finally {
      Date.now = realNow;
    }
  });

  describe('mergeMultipleSlidingWindowResults characterization', () => {
    test('calculates AVG weighted by overlap duration', () => {
      const result = mergeMultipleSlidingWindowResults(
        [
          { start: 0, end: 10, value: 10 },
          { start: 8, end: 20, value: 40 },
        ],
        { start: 5, end: 15 },
        'AVG',
      );

      expect(result).toBe(27.5);
    });

    test('calculates SUM using rate-based integration over target subintervals', () => {
      const result = mergeMultipleSlidingWindowResults(
        [
          { start: 0, end: 10, value: 100 },
          { start: 5, end: 15, value: 50 },
        ],
        { start: 0, end: 15 },
        'SUM',
      );

      expect(result).toBe(150);
    });

    test('preserves COUNT behavior across subintervals', () => {
      const result = mergeMultipleSlidingWindowResults(
        [
          { start: 0, end: 10, value: 2 },
          { start: 5, end: 15, value: 3 },
        ],
        { start: 0, end: 15 },
        'COUNT',
      );

      expect(result).toBe(10);
    });

    test('preserves MIN/MAX behavior over overlapping windows', () => {
      const windows = [
        { start: 0, end: 10, value: 7 },
        { start: 5, end: 15, value: 3 },
        { start: 8, end: 12, value: 9 },
      ];
      const target = { start: 6, end: 11 };

      expect(mergeMultipleSlidingWindowResults(windows, target, 'MIN')).toBe(3);
      expect(mergeMultipleSlidingWindowResults(windows, target, 'MAX')).toBe(9);
    });

    test('returns 0 when no windows overlap the target interval', () => {
      const result = mergeMultipleSlidingWindowResults(
        [
          { start: 0, end: 10, value: 1 },
          { start: 20, end: 30, value: 2 },
        ],
        { start: 10, end: 20 },
        'SUM',
      );

      expect(result).toBe(0);
    });
  });

  describe('ApproximationWindowBuffer cursor cleanup', () => {
    test('preserves window order and latest value after cleanup', () => {
      const buffers: TopicWindowBuffers = new Map();
      appendTopicResult(buffers, 'chunked/a', {
        start: 0,
        end: 10,
        value: 10,
        agg: 'AVG',
      });
      appendTopicResult(buffers, 'chunked/a', {
        start: 10,
        end: 20,
        value: 20,
        agg: 'AVG',
      });
      appendTopicResult(buffers, 'chunked/a', {
        start: 20,
        end: 30,
        value: 30,
        agg: 'AVG',
      });

      const buffer = buffers.get('chunked/a');
      expect(buffer).toBeDefined();
      expect(getActiveWindowCount(buffer)).toBe(3);

      cleanupOldWindows(buffer!, 15);

      expect(getActiveWindowCount(buffer)).toBe(2);
      expect(getLatestTopicValue(buffer)).toBe(30);
      expect(
        computeTopicLevelApproximationResult(buffer!, { start: 15, end: 25 }),
      ).toBe(25);
    });
  });

  test('benchmark target window cap records finalized windows and target stop state', () => {
    const timeoutSpy = jest
      .spyOn(global, 'setTimeout')
      .mockImplementation(((callback: any) => 0 as any) as typeof setTimeout);
    try {
      const operator = new ApproximationApproachOperator();
      (operator as any).benchmarkTargetWindowCount = 3;
      (operator as any).recordFinalizedWindow(1);
      (operator as any).recordFinalizedWindow(2);
      (operator as any).recordFinalizedWindow(3);

      expect((operator as any).finalizedWindowNumbers).toEqual([1, 2, 3]);
      expect((operator as any).benchmarkTargetWindowReached).toBe(true);
      expect((operator as any).benchmarkStopReason).toBe('target_window_count_reached');
    } finally {
      timeoutSpy.mockRestore();
    }
  });
});
