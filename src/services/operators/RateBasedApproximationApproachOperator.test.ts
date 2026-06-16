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

import {
  ApproximationApproachOperator,
} from './RateBasedApproximationApproachOperator';
import { mergeMultipleSlidingWindowResults } from './approximation/RateBasedApproximationMath';

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
});
