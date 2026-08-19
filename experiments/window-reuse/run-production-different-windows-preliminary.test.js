const {
  REPLAY_ANCHOR_MS,
  buildMetricComparison,
  buildQueriesForRanges,
  calculateRequiredReplayDurationSeconds,
  collectChunkedDebugSummaries,
  evaluateComparableDelivery,
  normalizeDelivery,
  parseArgs,
  parseResultPayload,
  validateReconstructionChunks,
} = require("./run-production-different-windows-preliminary");
const fs = require("fs");
const os = require("os");
const path = require("path");

function writeDebugSummary(runRoot, id, rangeMs, overrides = {}) {
  fs.writeFileSync(path.join(runRoot, `chunked_debug_summary_chunked_${id}.json`), JSON.stringify({
    lastComparableWindowStart: REPLAY_ANCHOR_MS,
    lastComparableWindowEnd: REPLAY_ANCHOR_MS + rangeMs,
    managedProducerMode: true,
    localProducerSpawnCount: 0,
    managerOwnedSubscriptionCount: 2,
    reconstructedSuperqueryResultCount: 1,
    coverageCompleteEmissionCount: 1,
    emittedIncompleteWindowCount: 0,
    ...overrides,
  }));
}

describe("run-production-different-windows-preliminary", () => {
  let debugRunRoot;

  beforeEach(() => {
    debugRunRoot = fs.mkdtempSync(path.join(os.tmpdir(), "hive-chunked-debug-"));
  });

  afterEach(() => {
    fs.rmSync(debugRunRoot, { recursive: true, force: true });
  });

  test("collects valid per-plan Chunked debug summaries by configured query range", () => {
    writeDebugSummary(debugRunRoot, "q180", 180000);
    writeDebugSummary(debugRunRoot, "q120", 120000);

    expect(collectChunkedDebugSummaries(debugRunRoot, buildQueriesForRanges([120, 180]))).toEqual({
      Q120: expect.objectContaining({ fileName: "chunked_debug_summary_chunked_q120.json", rangeMs: 120000 }),
      Q180: expect.objectContaining({ fileName: "chunked_debug_summary_chunked_q180.json", rangeMs: 180000 }),
    });
  });

  test("rejects a missing per-plan Chunked debug summary", () => {
    writeDebugSummary(debugRunRoot, "q120", 120000);

    expect(() => collectChunkedDebugSummaries(debugRunRoot, buildQueriesForRanges([120, 180])))
      .toThrow("expected 2 operator debug summaries, got 1");
  });

  test("rejects duplicate comparable window ranges", () => {
    writeDebugSummary(debugRunRoot, "q120a", 120000);
    writeDebugSummary(debugRunRoot, "q120b", 120000);

    expect(() => collectChunkedDebugSummaries(debugRunRoot, buildQueriesForRanges([120, 180])))
      .toThrow("duplicate operator debug summary range 120000");
  });

  test("rejects an unexpected comparable window range", () => {
    writeDebugSummary(debugRunRoot, "q120", 120000);
    writeDebugSummary(debugRunRoot, "q300", 300000);

    expect(() => collectChunkedDebugSummaries(debugRunRoot, buildQueriesForRanges([120, 180])))
      .toThrow("unexpected comparable window range 300000");
  });

  test("rejects reconstruction-local producer spawning", () => {
    writeDebugSummary(debugRunRoot, "q120", 120000, { localProducerSpawnCount: 1 });

    expect(() => collectChunkedDebugSummaries(debugRunRoot, buildQueriesForRanges([120])))
      .toThrow("reconstruction-local producer spawning occurred");
  });

  test("rejects incomplete window emissions", () => {
    writeDebugSummary(debugRunRoot, "q120", 120000, { emittedIncompleteWindowCount: 1 });

    expect(() => collectChunkedDebugSummaries(debugRunRoot, buildQueriesForRanges([120])))
      .toThrow("incomplete window emission occurred");
  });

  test("parses a single selected approach", () => {
    const args = parseArgs(["--approach", "approximation"]);

    expect(args.approaches).toEqual(["approximation"]);
  });

  test("rejects a replay anchor other than the fixed Experiment 1 anchor", () => {
    expect(() => parseArgs(["--base-anchor-ms", "1785924000001"])).toThrow(
      `Experiment 1 requires replay anchor ${REPLAY_ANCHOR_MS}`,
    );
  });

  test("parameterizes the separate sensitivity ranges and requires a Q600-safe horizon", () => {
    const queries = buildQueriesForRanges([120, 180, 300, 600]);

    expect(queries.map((query) => query.label)).toEqual(["Q120", "Q180", "Q300", "Q600"]);
    expect(calculateRequiredReplayDurationSeconds(queries)).toBe(660);
    expect(parseArgs([
      "--ranges", "120,180,300,600",
      "--chunk-step-seconds", "60",
      "--replay-duration-seconds", "660",
      "--experiment-name", "window-size-sensitivity",
    ])).toMatchObject({
      rangesSeconds: [120, 180, 300, 600],
      chunkStepSeconds: 60,
      replayDurationSeconds: 660,
      experimentName: "window-size-sensitivity",
    });
  });

  test("reports aggregate error metrics without requiring approximation to be exact", () => {
    const comparison = buildMetricComparison(
      { count: 962, sum: -22120.722912, avg: -22.99451446153846 },
      { count: 961, sum: -22120, avg: -23 },
      false,
    );

    expect(comparison.expectedOutputCount).toBe(1);
    expect(comparison.comparableOutputCount).toBe(1);
    expect(comparison.exactOutputCount).toBe(0);
    expect(comparison.maxAbsoluteError).toBeGreaterThan(0);
    expect(comparison.mape).toBeGreaterThan(0);
    expect(comparison.exact).toBeUndefined();
  });

  test("reports approximation error from its available average when count and sum are absent", () => {
    const comparison = buildMetricComparison(
      { count: 962, sum: -22120.722912, avg: -22.99451446153846 },
      { count: null, sum: null, avg: -22.98875688189093 },
      false,
    );

    expect(comparison.absoluteErrors).toMatchObject({ count: null, sum: null });
    expect(comparison.mae).toBeCloseTo(0.00575757964753, 12);
    expect(comparison.maxAbsoluteError).toBe(comparison.mae);
    expect(comparison.mape).toBeGreaterThan(0);
  });

  test("preserves raw Chunked reconstruction evidence in a normalized delivery", () => {
    const parsed = parseResultPayload(JSON.stringify({
      recomposedCount: 962,
      recomposedSum: -22120.722912,
      recomposedAvg: -22.99451446153846,
      windowStart: REPLAY_ANCHOR_MS,
      windowEnd: REPLAY_ANCHOR_MS + 120000,
      coverageComplete: true,
      isComparableWindow: true,
      metadataSource: "reconstructed",
      internalChunks: [{ start: REPLAY_ANCHOR_MS, end: REPLAY_ANCHOR_MS + 60000 }],
    }));
    const delivery = normalizeDelivery("chunked", { queryLabel: "Q120", rangeMs: 120000 }, parsed);

    expect(delivery.payload.metadataSource).toBe("reconstructed");
    expect(delivery.payload.internalChunks).toHaveLength(1);
  });

  test("requires Q600 to use ten complete contiguous 60-second chunks", () => {
    const chunks = Array.from({ length: 10 }, (_, index) => ({
      start: REPLAY_ANCHOR_MS + (index * 60000),
      end: REPLAY_ANCHOR_MS + ((index + 1) * 60000),
      coverageComplete: true,
    }));
    const delivery = {
      queryLabel: "Q600",
      rangeMs: 600000,
      windowStart: REPLAY_ANCHOR_MS,
      payload: { metadataSource: "reconstructed", internalChunks: chunks },
    };

    expect(validateReconstructionChunks(delivery)).toHaveLength(10);
    expect(() => validateReconstructionChunks({
      ...delivery,
      payload: { ...delivery.payload, internalChunks: [...chunks.slice(0, 5), { ...chunks[5], coverageComplete: false }, ...chunks.slice(6)] },
    })).toThrow("incomplete internal chunk");
    expect(() => validateReconstructionChunks({
      ...delivery,
      payload: { ...delivery.payload, internalChunks: [...chunks.slice(0, 5), { ...chunks[5], start: chunks[5].start + 1 }, ...chunks.slice(6)] },
    })).toThrow("non-contiguous chunk start");
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
