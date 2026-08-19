const {
  createRandomSource,
  generateLinearRampPattern,
  generateLowVariability,
  generateMultipleBurstsPattern,
  generateSegmentPattern,
  generateStepPattern,
  generateTimedBurstPattern,
  INTERVAL_MS,
  LAST_TIMESTAMP_MS,
  parseArgs,
  PATTERNS,
  TOTAL_POINTS,
} = require("./generate-custom-patterns.js");

function expectCommonSeriesShape(data) {
  expect(data).toHaveLength(TOTAL_POINTS);
  expect(data[0].timestamp).toBe(0);
  expect(data[data.length - 1].timestamp).toBe(LAST_TIMESTAMP_MS);
}

function getValueAtMs(data, timestamp) {
  const row = data.find((entry) => entry.timestamp === timestamp);
  expect(row).toBeDefined();
  return row.value;
}

function countValue(data, expectedValue) {
  return data.filter((entry) => entry.value === expectedValue).length;
}

describe("generate-custom-patterns seed handling", () => {
  test("parseArgs accepts a fixed seed", () => {
    expect(parseArgs(["--seed", "1234"])).toEqual({ seed: 1234 });
  });

  test("low variability generation is deterministic for the same seed", () => {
    const rngA = createRandomSource(42);
    const rngB = createRandomSource(42);
    const rngC = createRandomSource(43);

    const sampleA = generateLowVariability({ mu: -23, sigma: 0.25 }, rngA.next)
      .slice(0, 5)
      .map((row) => row.value);
    const sampleB = generateLowVariability({ mu: -23, sigma: 0.25 }, rngB.next)
      .slice(0, 5)
      .map((row) => row.value);
    const sampleC = generateLowVariability({ mu: -23, sigma: 0.25 }, rngC.next)
      .slice(0, 5)
      .map((row) => row.value);

    expect(sampleA).toEqual(sampleB);
    expect(sampleA).not.toEqual(sampleC);
  });
});

describe("stress-pattern generation", () => {
  test("spike_boundary_short has exact observation count, timestamps, transitions, and affected observations", () => {
    const data = generateTimedBurstPattern(PATTERNS.spike_boundary_short.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 58750)).toBe(-23);
    expect(getValueAtMs(data, 59000)).toBe(-5);
    expect(getValueAtMs(data, 62750)).toBe(-5);
    expect(getValueAtMs(data, 63000)).toBe(-23);
    expect(countValue(data, -5)).toBe(16);
  });

  test("spike_boundary_medium has exact transition edges and affected observations", () => {
    const data = generateTimedBurstPattern(PATTERNS.spike_boundary_medium.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 54750)).toBe(-23);
    expect(getValueAtMs(data, 55000)).toBe(-5);
    expect(getValueAtMs(data, 64750)).toBe(-5);
    expect(getValueAtMs(data, 65000)).toBe(-23);
    expect(countValue(data, -5)).toBe(40);
  });

  test("spike_asymmetric_long has exact transition edges and affected observations", () => {
    const data = generateTimedBurstPattern(PATTERNS.spike_asymmetric_long.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 49750)).toBe(-23);
    expect(getValueAtMs(data, 50000)).toBe(-5);
    expect(getValueAtMs(data, 69750)).toBe(-5);
    expect(getValueAtMs(data, 70000)).toBe(-23);
    expect(countValue(data, -5)).toBe(80);
  });

  test("late_burst has exact transition edges and affected observations", () => {
    const data = generateTimedBurstPattern(PATTERNS.late_burst.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 84750)).toBe(-23);
    expect(getValueAtMs(data, 85000)).toBe(-5);
    expect(getValueAtMs(data, 104750)).toBe(-5);
    expect(getValueAtMs(data, 105000)).toBe(-23);
    expect(countValue(data, -5)).toBe(80);
  });

  test("multiple_bursts has exact transition edges and affected observations", () => {
    const data = generateMultipleBurstsPattern(PATTERNS.multiple_bursts.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 24750)).toBe(-23);
    expect(getValueAtMs(data, 25000)).toBe(-5);
    expect(getValueAtMs(data, 34750)).toBe(-5);
    expect(getValueAtMs(data, 35000)).toBe(-23);
    expect(getValueAtMs(data, 84750)).toBe(-23);
    expect(getValueAtMs(data, 85000)).toBe(-5);
    expect(getValueAtMs(data, 94750)).toBe(-5);
    expect(getValueAtMs(data, 95000)).toBe(-23);
    expect(countValue(data, -5)).toBe(80);
  });

  test("step_misaligned_45 transitions at exactly 45 seconds", () => {
    const data = generateStepPattern(PATTERNS.step_misaligned_45.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 44750)).toBe(-23);
    expect(getValueAtMs(data, 45000)).toBe(-15);
    expect(getValueAtMs(data, 45250)).toBe(-15);
  });

  test("step_misaligned_75 transitions at exactly 75 seconds", () => {
    const data = generateStepPattern(PATTERNS.step_misaligned_75.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 74750)).toBe(-23);
    expect(getValueAtMs(data, 75000)).toBe(-15);
    expect(getValueAtMs(data, 75250)).toBe(-15);
  });

  test("linear_ramp spans the full sampled interval with exact endpoints", () => {
    const data = generateLinearRampPattern(PATTERNS.linear_ramp.params);
    expectCommonSeriesShape(data);
    expect(data[0].value).toBe(-23);
    expect(data[data.length - 1].value).toBe(-11);
    expect(getValueAtMs(data, INTERVAL_MS)).toBeGreaterThan(-23);
    expect(getValueAtMs(data, LAST_TIMESTAMP_MS - INTERVAL_MS)).toBeLessThan(-11 + 0.1);
  });

  test("asymmetric_activity transitions at every configured boundary", () => {
    const data = generateSegmentPattern(PATTERNS.asymmetric_activity.params);
    expectCommonSeriesShape(data);
    expect(getValueAtMs(data, 39750)).toBe(-23);
    expect(getValueAtMs(data, 40000)).toBe(-8);
    expect(getValueAtMs(data, 54750)).toBe(-8);
    expect(getValueAtMs(data, 55000)).toBe(-23);
    expect(getValueAtMs(data, 94750)).toBe(-23);
    expect(getValueAtMs(data, 95000)).toBe(-15);
    expect(getValueAtMs(data, LAST_TIMESTAMP_MS)).toBe(-15);
  });

  test("deterministic stress patterns reproduce exactly with the same seed context", () => {
    const rngA = createRandomSource(20260716);
    const rngB = createRandomSource(20260716);

    const lowVarA = generateLowVariability(PATTERNS.low_variability.params, rngA.next)
      .slice(0, 10);
    const lowVarB = generateLowVariability(PATTERNS.low_variability.params, rngB.next)
      .slice(0, 10);
    expect(lowVarA).toEqual(lowVarB);

    const burstA = generateTimedBurstPattern(PATTERNS.spike_boundary_medium.params);
    const burstB = generateTimedBurstPattern(PATTERNS.spike_boundary_medium.params);
    expect(burstA).toEqual(burstB);
  });
});
