const {
  evaluateComparableDelivery,
  parseArgs,
  parseResultPayload,
} = require("./run-production-different-windows-preliminary");

describe("run-production-different-windows-preliminary", () => {
  test("parses a single selected approach", () => {
    const args = parseArgs(["--approach", "approximation"]);

    expect(args.approaches).toEqual(["approximation"]);
  });

  test("accepts a Q180 approximation payload using execution-specific bounds", () => {
    const registration = {
      queryLabel: "Q180",
      executionId: "approximation_q180",
      rangeMs: 180000,
      stepMs: 60000,
      expectedWindowStart: 1785924000000,
      expectedWindowEnd: 1785924180000,
    };
    const parsed = parseResultPayload(JSON.stringify({
      executionId: "approximation_q180",
      windowStart: 1785924000000,
      windowEnd: 1785924180000,
      rangeMs: 180000,
      stepMs: 60000,
      average: -22.99331519360933,
      isComparableWindow: true,
      coverageComplete: true,
      timestamp: 1785954919348,
    }));

    expect(evaluateComparableDelivery(registration, parsed)).toEqual({
      accepted: true,
      reason: "accepted",
    });
  });

  test("rejects a Q120-sized comparable payload for Q180", () => {
    const registration = {
      queryLabel: "Q180",
      executionId: "approximation_q180",
      rangeMs: 180000,
      stepMs: 60000,
      expectedWindowStart: 1785924000000,
      expectedWindowEnd: 1785924180000,
    };
    const parsed = parseResultPayload(JSON.stringify({
      executionId: "approximation_q120",
      windowStart: 1785924000000,
      windowEnd: 1785924120000,
      rangeMs: 120000,
      stepMs: 60000,
      average: -22.98875688189093,
      isComparableWindow: true,
      coverageComplete: true,
      timestamp: 1785954859409,
    }));

    expect(evaluateComparableDelivery(registration, parsed)).toEqual({
      accepted: false,
      reason: "executionId_mismatch",
    });
  });
});
