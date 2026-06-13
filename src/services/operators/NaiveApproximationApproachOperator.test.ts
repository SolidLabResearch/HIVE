jest.mock('mqtt/*', () => ({
  connect: jest.fn().mockReturnValue({
    on: jest.fn().mockReturnThis(),
    subscribe: jest.fn(),
    publish: jest.fn(),
    end: jest.fn(),
  }),
}), { virtual: true });

jest.mock('mqtt', () => ({
  connect: jest.fn().mockReturnValue({
    on: jest.fn().mockReturnThis(),
    subscribe: jest.fn(),
    publish: jest.fn(),
    end: jest.fn(),
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

import { NaiveApproximationApproachOperator } from './NaiveApproximationApproachOperator';

describe('NaiveApproximationApproachOperator', () => {
  test('keeps AVG semantics for overlapping windows', async () => {
    const operator = new NaiveApproximationApproachOperator();
    const windows = [
      { start: 0, end: 10, value: 2 },
      { start: 5, end: 15, value: 4 },
      { start: 20, end: 30, value: 100 },
    ];

    const result = await operator.naiveApproximationApproach(
      windows,
      { start: 0, end: 12 },
      'AVG',
    );

    expect(result).toBeCloseTo(3, 8);
  });
});
