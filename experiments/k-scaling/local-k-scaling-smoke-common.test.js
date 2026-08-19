const {
  LATENCY_TOLERANCE_MS,
  APPROACH_CONFIG,
  buildCheckpointKey,
  buildCombinationMatrix,
  buildScenarioKey,
  compareAgainstFetching,
  countConsumerLatencyFiles,
  createScenarioReplayAnchors,
  extractAllConsumerWindows,
  extractRepresentativeWindow,
  getExpectedBenchmarkSummaryCount,
  listBenchmarkWindowSummaryPaths,
  median,
  parseApproachSelection,
  parseCsv,
  parseKScalingSelection,
  REGISTRATION_ANCHORED_LATENCY_SOURCE,
  selectFirstCompleteRow,
} = require("./local-k-scaling-smoke-common");
const fs = require("fs");
const os = require("os");
const path = require("path");

describe("local-k-scaling-smoke-common", () => {
  test("parses K selections including 32", () => {
    expect(parseKScalingSelection("1,2,4,8,32", [1])).toEqual([1, 2, 4, 8, 32]);
  });

  test("parses all three approach identifiers", () => {
    expect(parseApproachSelection("fetching,approximation,chunked", ["fetching"])).toEqual([
      "fetching",
      "approximation",
      "chunked",
    ]);
  });

  test("extracts the first complete comparable row", () => {
    const row = selectFirstCompleteRow([
      { coverage_complete: "false", is_partial_window: "true", is_comparable_window: "false" },
      { coverage_complete: "true", is_partial_window: "false", is_comparable_window: "true", window_number: "1" },
    ]);
    expect(row.window_number).toBe("1");
  });

  test("parses CSV with quoted fields", () => {
    const rows = parseCsv("a,b\n1,\"2,3\"\n");
    expect(rows).toEqual([{ a: "1", b: "2,3" }]);
  });

  test("computes comparison exactness and MAE", () => {
    expect(compareAgainstFetching(5, 5)).toMatchObject({
      exactAgreement: true,
      absoluteError: 0,
      mae: 0,
      maxAbsoluteError: 0,
    });
    expect(compareAgainstFetching(5, 5.5)).toMatchObject({
      exactAgreement: false,
      absoluteError: 0.5,
      mae: 0.5,
      maxAbsoluteError: 0.5,
    });
  });

  test("builds checkpoint keys and matrix sizes", () => {
    expect(buildCheckpointKey("chunked", 32, 3)).toBe("chunked-K32-iteration3");
    const matrix = buildCombinationMatrix({
      approaches: ["fetching", "approximation", "chunked"],
      kValues: [1, 2, 4, 8, 32],
      iterations: 3,
    });
    expect(matrix).toHaveLength(45);
    expect(matrix.slice(0, 6)).toEqual([
      { approach: "fetching", kValue: 1, iteration: 1 },
      { approach: "approximation", kValue: 1, iteration: 1 },
      { approach: "chunked", kValue: 1, iteration: 1 },
      { approach: "fetching", kValue: 1, iteration: 2 },
      { approach: "approximation", kValue: 1, iteration: 2 },
      { approach: "chunked", kValue: 1, iteration: 2 },
    ]);
  });

  test("creates one replay anchor per K/iteration scenario", () => {
    const anchors = createScenarioReplayAnchors({
      kValues: [1, 32],
      iterations: 2,
      baseAnchorMs: 1000,
      spacingMs: 10,
    });
    expect(anchors).toEqual({
      [buildScenarioKey(1, 1)]: "1000",
      [buildScenarioKey(1, 2)]: "1010",
      [buildScenarioKey(32, 1)]: "1020",
      [buildScenarioKey(32, 2)]: "1030",
    });
  });

  test("median handles odd and empty arrays", () => {
    expect(median([3, 1, 2])).toBe(2);
    expect(median([])).toBeNull();
  });

  test("latency tolerance stays bounded at 1 ms", () => {
    expect(LATENCY_TOLERANCE_MS).toBe(1);
  });

  test("counts per-consumer latency artifacts for every concurrent registration", () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "k-scaling-latency-count-"));
    try {
      for (let consumerIndex = 1; consumerIndex <= 4; consumerIndex += 1) {
        fs.writeFileSync(
          path.join(tempDir, APPROACH_CONFIG.fetching.consumerLatencyFile(consumerIndex)),
          "window_number,coverage_complete,is_partial_window,is_comparable_window\n",
        );
      }
      expect(countConsumerLatencyFiles(tempDir, "fetching", 4)).toMatchObject({
        existing: 4,
        expected: 4,
      });
      expect(countConsumerLatencyFiles(tempDir, "fetching", 5)).toMatchObject({
        existing: 4,
        expected: 5,
      });
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  test("uses the shared chunked benchmark summary for bounded completion", () => {
    expect(listBenchmarkWindowSummaryPaths("/tmp/run", "chunked", 2)).toEqual([
      "/tmp/run/benchmark_window_cap_summary.json",
    ]);
    expect(getExpectedBenchmarkSummaryCount("chunked", 2)).toBe(1);
    expect(listBenchmarkWindowSummaryPaths("/tmp/run", "approximation", 2)).toEqual([
      "/tmp/run/benchmark_window_cap_summary_consumer_1.json",
      "/tmp/run/benchmark_window_cap_summary_consumer_2.json",
    ]);
    expect(getExpectedBenchmarkSummaryCount("approximation", 2)).toBe(2);
  });

  test("extractRepresentativeWindow computes registration-anchored latency", () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "k-scaling-latency-row-"));
    try {
      fs.writeFileSync(
        path.join(tempDir, APPROACH_CONFIG.fetching.consumerLatencyFile(1)),
        [
          "window_number,coverage_complete,is_partial_window,is_comparable_window,query_registered_at,result_emitted_at,window_start,window_end,window_duration_ms,result_value",
          "1,true,false,true,1000,121050,0,120000,120000,42.5",
        ].join("\n"),
      );
      expect(extractRepresentativeWindow(tempDir, "fetching", 1)).toMatchObject({
        ok: true,
        windowNumber: 1,
        resultValue: 42.5,
        queryToFirstResultMs: 120050,
        postWindowCloseLatencyMs: 50,
        latencyMetricSource: REGISTRATION_ANCHORED_LATENCY_SOURCE,
      });
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  test("extractRepresentativeWindow rejects missing or incomplete raw results", () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "k-scaling-missing-row-"));
    try {
      fs.writeFileSync(
        path.join(tempDir, APPROACH_CONFIG.chunked.consumerLatencyFile(1)),
        [
          "window_number,coverage_complete,is_partial_window,is_comparable_window,query_registered_at,result_emitted_at,window_start,window_end,window_duration_ms,result_value",
          "1,false,false,true,1000,121050,0,120000,120000,42.5",
        ].join("\n"),
      );
      expect(extractRepresentativeWindow(tempDir, "chunked", 1)).toMatchObject({
        ok: false,
        reason: "missing first complete comparable latency row",
      });
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  test("extractAllConsumerWindows requires one comparable result per consumer", () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "k-scaling-all-consumers-"));
    try {
      fs.writeFileSync(
        path.join(tempDir, APPROACH_CONFIG.fetching.consumerLatencyFile(1)),
        [
          "window_number,coverage_complete,is_partial_window,is_comparable_window,query_registered_at,result_emitted_at,window_start,window_end,window_duration_ms,result_value",
          "1,true,false,true,1000,121050,0,120000,120000,42.5",
        ].join("\n"),
      );
      fs.writeFileSync(
        path.join(tempDir, APPROACH_CONFIG.fetching.consumerLatencyFile(2)),
        [
          "window_number,coverage_complete,is_partial_window,is_comparable_window,query_registered_at,result_emitted_at,window_start,window_end,window_duration_ms,result_value",
          "1,true,false,true,1000,121060,0,120000,120000,42.5",
        ].join("\n"),
      );

      expect(extractAllConsumerWindows(tempDir, "fetching", 2)).toMatchObject({
        ok: true,
      });

      fs.unlinkSync(path.join(tempDir, APPROACH_CONFIG.fetching.consumerLatencyFile(2)));

      const invalid = extractAllConsumerWindows(tempDir, "fetching", 2);
      expect(invalid.ok).toBe(false);
      expect(invalid.consumers[1]).toMatchObject({
        ok: false,
        reason: "missing first complete comparable latency row",
      });
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
