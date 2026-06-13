jest.mock('mqtt', () => ({
  connect: jest.fn().mockReturnValue({
    on: jest.fn().mockReturnThis(),
    subscribe: jest.fn(),
    publish: jest.fn(),
    end: jest.fn(),
    once: jest.fn().mockReturnThis(),
    connected: true,
  }),
}));

jest.mock('../../util/logger/CSVLogger', () => ({
  CSVLogger: jest.fn().mockImplementation(() => ({ log: jest.fn() })),
}));

jest.mock('fs', () => ({
  ...jest.requireActual('fs'),
  existsSync: jest.fn().mockReturnValue(true),
  createWriteStream: jest.fn().mockReturnValue({ write: jest.fn(), end: jest.fn() }),
}));

import { ApproximationApproachOperator } from './RateBasedApproximationApproachOperator';

describe('ApproximationApproachOperator', () => {
  let operator: ApproximationApproachOperator;

  beforeEach(() => {
    jest.clearAllMocks();
    operator = new ApproximationApproachOperator();
  });

  test('reuses cached topic window parsing without changing output', async () => {
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
});
