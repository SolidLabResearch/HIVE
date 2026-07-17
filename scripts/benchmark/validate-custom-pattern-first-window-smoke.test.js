const fs = require("fs");
const os = require("os");
const path = require("path");
const childProcess = require("child_process");

const {
  renderMarkdown,
  summarizeSmokeValidation,
} = require("./validate-custom-pattern-first-window-smoke.js");
const {
  compareAggregateResultEquivalence,
  EXACT_AGGREGATE_ABSOLUTE_TOLERANCE,
  EXACT_AGGREGATE_COMPARISON_METHOD,
} = require("../../analysis/accuracy/accuracy-comparison-custom-patterns.js");

function writeCsv(filePath, headers, rows) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const formatValue = (value) => {
    const stringValue = String(value ?? "");
    if (!/[",\n]/.test(stringValue)) {
      return stringValue;
    }
    return `"${stringValue.replace(/"/g, "\"\"")}"`;
  };
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => formatValue(row[header] ?? "")).join(",")),
  ];
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeNdjson(filePath, records) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${records.map((row) => JSON.stringify(row)).join("\n")}\n`);
}

function writeApproachRun(rootDir, pattern, approach, options = {}) {
  const iteration = options.iteration ?? 1;
  const runDir = path.join(rootDir, approach, pattern, `iteration${iteration}`);
  fs.mkdirSync(runDir, { recursive: true });

  const baseWindow = {
    result_value: "-23.000000",
    window_number: "1",
    window_start: "1000",
    window_end: "121000",
    timestamp: "121500",
  };

  const resultFile = {
    fetching: "fetching_results.csv",
    approximation: "approximation_results.csv",
    chunked: "chunked_results.csv",
  }[approach];
  const latencyFile = {
    fetching: "fetching_latency_log.csv",
    approximation: "approximation_latency_log.csv",
    chunked: "chunked_latency_log.csv",
  }[approach];

  writeCsv(
    path.join(runDir, resultFile),
    [
      "timestamp",
      "window_number",
      "window_start",
      "window_end",
      "result_value",
      "elapsed_since_registration_ms",
      "delay_past_expected_close_ms",
    ],
    [{
      ...baseWindow,
      result_value: String(options.resultValue ?? -23),
      elapsed_since_registration_ms: "500",
      delay_past_expected_close_ms: "500",
    }],
  );

  if (approach === "fetching") {
    const fetchingLatencyRow = {
      window_number: "1",
      query_registered_at: "0",
      first_data_received_at: "1000",
      expected_window_close: "121000",
      last_obs_received_at: "121499",
      result_emitted_at: "121500",
      delay_past_expected_close_ms: "500",
      delay_past_data_start_ms: "0",
      delay_past_last_obs_ms: "1",
      window_semantics: "trailing",
      logical_trigger_time: "121000",
      window_start: "1000",
      window_end: "121000",
      window_data_close_time: "121000",
      latency_from_logical_trigger_ms: "500",
      latency_from_window_close_ms: "500",
      metadata_source: "direct",
      result_value: String(options.resultValue ?? -23),
      ...(options.fetchingLatencyRow || {}),
    };

    writeCsv(
      path.join(runDir, latencyFile),
      [
        "window_number",
        "query_registered_at",
        "first_data_received_at",
        "expected_window_close",
        "last_obs_received_at",
        "result_emitted_at",
        "delay_past_expected_close_ms",
        "delay_past_data_start_ms",
        "delay_past_last_obs_ms",
        "window_semantics",
        "logical_trigger_time",
        "window_start",
        "window_end",
        "window_data_close_time",
        "latency_from_logical_trigger_ms",
        "latency_from_window_close_ms",
        "metadata_source",
        "result_value",
      ],
      [{
        ...fetchingLatencyRow,
      }],
    );

    writeCsv(
      path.join(runDir, "fetching_window_diagnostics.csv"),
      [
        "window_number",
        "window_start",
        "window_end",
        "event_count",
        "expected_event_count",
        "sum",
        "avg",
        "first_event_timestamp",
        "last_event_timestamp",
        "completeness_status",
        "accepted_or_suppressed",
        "reason",
        "result_value",
      ],
      [{
        window_number: "1",
        window_start: "1000",
        window_end: "121000",
        event_count: "960",
        expected_event_count: "960",
        sum: "-22080",
        avg: String(options.resultValue ?? -23),
        first_event_timestamp: "1000",
        last_event_timestamp: "121000",
        completeness_status: "complete",
        accepted_or_suppressed: "accepted",
        reason: "finalized_settled_window",
        result_value: String(options.resultValue ?? -23),
      }],
    );
  } else if (approach === "approximation") {
    writeCsv(
      path.join(runDir, latencyFile),
      [
        "window_number",
        "query_registered_at",
        "first_data_received_at",
        "expected_window_close",
        "registration_anchored_expected_close",
        "event_time_window_close",
        "wall_clock_window_close",
        "last_data_received_at",
        "result_emitted_at",
        "latency_from_query_reg_ms",
        "latency_from_data_start_ms",
        "latency_from_last_data_ms",
        "wall_clock_close_to_result_ms",
        "latency_domain_status",
        "approximation_status",
        "window_semantics",
        "logical_trigger_time",
        "window_start",
        "window_end",
        "window_data_close_time",
        "latency_from_logical_trigger_ms",
        "latency_from_window_close_ms",
        "metadata_source",
        "result_value",
      ],
      [{
        window_number: "1",
        query_registered_at: "0",
        first_data_received_at: "1000",
        expected_window_close: "121000",
        registration_anchored_expected_close: "121000",
        event_time_window_close: "121000",
        wall_clock_window_close: "",
        last_data_received_at: "121499",
        result_emitted_at: "121500",
        latency_from_query_reg_ms: "500",
        latency_from_data_start_ms: "0",
        latency_from_last_data_ms: "1",
        wall_clock_close_to_result_ms: "",
        latency_domain_status: "domain_mismatch",
        approximation_status: "completed_window_approximation",
        window_semantics: "trailing",
        logical_trigger_time: "121000",
        window_start: "1000",
        window_end: "121000",
        window_data_close_time: "121000",
        latency_from_logical_trigger_ms: "",
        latency_from_window_close_ms: "",
        metadata_source: "reconstructed",
        result_value: String(options.resultValue ?? -23),
      }],
    );
  } else {
    writeCsv(
      path.join(runDir, latencyFile),
      [
        "window_number",
        "query_registered_at",
        "first_data_received_at",
        "expected_window_close",
        "registration_anchored_expected_close",
        "event_time_window_start",
        "event_time_window_end",
        "event_time_window_close",
        "wall_clock_window_close",
        "anchor_aligned_window_close",
        "last_chunk_received_at",
        "interval_trigger_at",
        "result_emitted_at",
        "delay_past_expected_close_ms",
        "delay_past_data_start_ms",
        "interval_wait_ms",
        "computation_ms",
        "result_value",
        "required_chunk_intervals",
        "last_required_chunk_received_at",
        "semantic_ready_at",
        "window_close_to_ready_ms",
        "ready_to_emit_ms",
        "wall_clock_close_to_result_ms",
        "anchor_aligned_window_close_to_result_ms",
        "latency_domain_status",
        "trigger_type",
        "emission_reason",
        "window_semantics",
        "logical_trigger_time",
        "window_start",
        "window_end",
        "window_data_close_time",
        "latency_from_logical_trigger_ms",
        "latency_from_window_close_ms",
        "metadata_source",
      ],
      [{
        window_number: "1",
        query_registered_at: "0",
        first_data_received_at: "1000",
        expected_window_close: "121000",
        registration_anchored_expected_close: "121000",
        event_time_window_start: "1000",
        event_time_window_end: "121000",
        event_time_window_close: "121000",
        wall_clock_window_close: "",
        anchor_aligned_window_close: "",
        last_chunk_received_at: "121499",
        interval_trigger_at: "121499",
        result_emitted_at: "121500",
        delay_past_expected_close_ms: "500",
        delay_past_data_start_ms: "0",
        interval_wait_ms: "0",
        computation_ms: "1",
        result_value: String(options.resultValue ?? -23),
        required_chunk_intervals: "1-2|2-3|3-4|4-5",
        last_required_chunk_received_at: "121499",
        semantic_ready_at: "121499",
        window_close_to_ready_ms: "499",
        ready_to_emit_ms: "1",
        wall_clock_close_to_result_ms: "",
        anchor_aligned_window_close_to_result_ms: "",
        latency_domain_status: "domain_mismatch",
        trigger_type: "immediate",
        emission_reason: "immediate_ready_check",
        window_semantics: "trailing",
        logical_trigger_time: "121000",
        window_start: "1000",
        window_end: "121000",
        window_data_close_time: "121000",
        latency_from_logical_trigger_ms: "",
        latency_from_window_close_ms: "",
        metadata_source: "reconstructed",
      }],
    );

    writeCsv(
      path.join(runDir, "chunked_window_diagnostics.csv"),
      [
        "benchmark_event_time_anchor",
        "external_window_number",
        "external_window_start",
        "external_window_end",
        "internal_chunk_ids",
        "internal_chunks_json",
        "recomposed_count",
        "recomposed_sum",
        "recomposed_avg",
        "recomposed_min",
        "recomposed_max",
        "result_value",
      ],
      [{
        benchmark_event_time_anchor: "1000",
        external_window_number: "1",
        external_window_start: "1000",
        external_window_end: "121000",
        internal_chunk_ids: "1|2|3|4",
        internal_chunks_json: JSON.stringify([
          { coverageComplete: true },
          { coverageComplete: true },
          { coverageComplete: true },
          { coverageComplete: true },
        ]),
        recomposed_count: "960",
        recomposed_sum: "-22080",
        recomposed_avg: String(options.resultValue ?? -23),
        recomposed_min: "",
        recomposed_max: "",
        result_value: String(options.resultValue ?? -23),
      }],
    );
  }

  writeJson(path.join(runDir, "resource_summary.json"), {
    meanCpuPct: options.meanCpuPct ?? 12.5,
    peakRssMb: options.peakRssMb ?? 256,
  });

  writeJson(path.join(runDir, "benchmark_window_cap_summary.json"), {
    targetWindowCount: 1,
    emittedFinalWindowCount: 1,
    finalWindowNumbers: [1],
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    approach,
  });

  writeJson(path.join(runDir, "attempt_metadata.json"), {
    benchmark_event_time_anchor: 1000,
    output_window_range: 120000,
    output_window_step: 60000,
    configuredTimeoutMs: options.configuredTimeoutMs ?? 300000,
  });

  writeNdjson(path.join(runDir, "mqtt_traffic.ndjson"), [
    {
      timestamp: 1000,
      messageType: "raw_input_stream",
    },
  ]);
}

function writeExecutionSummary(rootDir, entries) {
  writeJson(path.join(rootDir, "custom_pattern_comparison_summary.json"), {
    results: entries.map((entry) => ({
      approach: entry.approach,
      pattern: entry.pattern,
      iteration: entry.iteration,
      configuredTimeoutMs: entry.configuredTimeoutMs ?? 300000,
    })),
  });
}

function createMultiIterationFixture(rootDir, options = {}) {
  const pattern = options.pattern ?? "late_burst";
  const iterations = options.iterations ?? [1, 2, 3];
  const baseFetching = options.fetchingResults ?? { 1: -10, 2: -20, 3: -30 };
  const baseApproximation = options.approximationResults ?? { 1: -9.7, 2: -19.5, 3: -29.2 };
  const baseChunked = options.chunkedResults ?? { 1: -10, 2: -20, 3: -30 };
  const baseLatency = options.fetchingLatency ?? { 1: 100, 2: 200, 3: 300 };
  const baseCpu = options.averageCpuPct ?? {
    fetching: { 1: 10, 2: 20, 3: 30 },
    approximation: { 1: 11, 2: 21, 3: 31 },
    chunked: { 1: 12, 2: 22, 3: 32 },
  };
  const baseRss = options.peakRssMb ?? {
    fetching: { 1: 100, 2: 200, 3: 300 },
    approximation: { 1: 110, 2: 210, 3: 310 },
    chunked: { 1: 120, 2: 220, 3: 320 },
  };

  for (const iteration of iterations) {
    writeApproachRun(rootDir, pattern, "fetching", {
      iteration,
      resultValue: baseFetching[iteration],
      meanCpuPct: baseCpu.fetching[iteration],
      peakRssMb: baseRss.fetching[iteration],
      fetchingLatencyRow: {
        latency_from_window_close_ms: String(baseLatency[iteration]),
      },
    });
    writeApproachRun(rootDir, pattern, "approximation", {
      iteration,
      resultValue: baseApproximation[iteration],
      meanCpuPct: baseCpu.approximation[iteration],
      peakRssMb: baseRss.approximation[iteration],
    });
    writeApproachRun(rootDir, pattern, "chunked", {
      iteration,
      resultValue: baseChunked[iteration],
      meanCpuPct: baseCpu.chunked[iteration],
      peakRssMb: baseRss.chunked[iteration],
    });
  }

  writeExecutionSummary(
    rootDir,
    iterations.flatMap((iteration) => ([
      { approach: "fetching", pattern, iteration },
      { approach: "approximation", pattern, iteration },
      { approach: "chunked", pattern, iteration },
    ])),
  );

  return { pattern, iterations };
}

describe("custom-pattern first-window smoke validator", () => {
  test("floating-point aggregate equivalence handles identical, tolerant, and invalid values", () => {
    const identical = compareAggregateResultEquivalence(-23, -23);
    expect(identical.exactAgreement).toBe(true);
    expect(identical.rawAbsoluteError).toBe(0);
    expect(identical.comparisonTolerance).toBe(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE);
    expect(identical.comparisonMethod).toBe(EXACT_AGGREGATE_COMPARISON_METHOD);

    const roundoffEquivalent = compareAggregateResultEquivalence(-23, -23.00000000000001);
    expect(roundoffEquivalent.exactAgreement).toBe(true);
    expect(roundoffEquivalent.rawAbsoluteError).toBeCloseTo(1.0658141036401503e-14, 20);

    const atTolerance = compareAggregateResultEquivalence(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE, 0);
    expect(atTolerance.exactAgreement).toBe(true);
    expect(atTolerance.rawAbsoluteError).toBe(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE);

    const aboveTolerance = compareAggregateResultEquivalence(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE * 2, 0);
    expect(aboveTolerance.exactAgreement).toBe(false);
    expect(aboveTolerance.rawAbsoluteError).toBe(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE * 2);

    const nanComparison = compareAggregateResultEquivalence(Number.NaN, 10);
    expect(nanComparison.exactAgreement).toBe(false);
    expect(nanComparison.rawAbsoluteError).toBeNull();

    const infinityComparison = compareAggregateResultEquivalence(Number.POSITIVE_INFINITY, 10);
    expect(infinityComparison.exactAgreement).toBe(false);
    expect(infinityComparison.rawAbsoluteError).toBeNull();

    const missingComparison = compareAggregateResultEquivalence(undefined, 10);
    expect(missingComparison.exactAgreement).toBe(false);
    expect(missingComparison.rawAbsoluteError).toBeNull();
  });

  test("passes when all three approaches produce one complete comparable first window", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-pass-"));

    try {
      writeApproachRun(rootDir, "low_variability", "fetching", { resultValue: -23 });
      writeApproachRun(rootDir, "low_variability", "approximation", { resultValue: -22.9 });
      writeApproachRun(rootDir, "low_variability", "chunked", { resultValue: -23 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["low_variability"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
      expect(summary.failures).toEqual([]);
      expect(summary.patternResults[0].perApproach.fetching.semanticWindowCloseToResultMs).toBe(500);
      expect(summary.patternResults[0].perApproach.fetching.latencyMetricSource).toBe("fetching_latency_log");
      expect(summary.patternResults[0].perApproach.approximation.latencyMetricSource).toBe("semantic_window_metadata");
      expect(summary.tableRows[0]["Latency source"]).toBe("fetching_latency_log");
      expect(summary.patternResults[0].perApproach.fetching.absoluteError).toBe(0);
      expect(summary.patternResults[0].perApproach.chunked.absoluteError).toBe(0);
      expect(summary.patternResults[0].perApproach.chunked.exactAgreement).toBe(true);
      expect(summary.patternResults[0].perApproach.chunked.comparisonTolerance).toBe(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE);
      expect(summary.patternResults[0].perApproach.chunked.comparisonMethod).toBe(EXACT_AGGREGATE_COMPARISON_METHOD);
      expect(summary.patternResults[0].perApproach.approximation.mae).toBeCloseTo(0.1, 6);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("passes when chunked differs from fetching only by floating-point round-off within tolerance", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-roundoff-"));

    try {
      writeApproachRun(rootDir, "low_freq_oscillation", "fetching", { resultValue: -23.00000000000001 });
      writeApproachRun(rootDir, "low_freq_oscillation", "approximation", { resultValue: -22.85 });
      writeApproachRun(rootDir, "low_freq_oscillation", "chunked", { resultValue: -23 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["low_freq_oscillation"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
      expect(summary.failures).toEqual([]);
      expect(summary.patternResults[0].perApproach.chunked.exactAgreement).toBe(true);
      expect(summary.patternResults[0].perApproach.chunked.absoluteError).toBeCloseTo(1.0658141036401503e-14, 20);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails when chunked differs from fetching", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-fail-"));

    try {
      writeApproachRun(rootDir, "step_pattern", "fetching", { resultValue: -19 });
      writeApproachRun(rootDir, "step_pattern", "approximation", { resultValue: -18.75 });
      writeApproachRun(rootDir, "step_pattern", "chunked", { resultValue: -18.5 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["step_pattern"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("fail");
      expect(summary.failures).toContain("step_pattern iteration 1: chunked result is not exactly equal to fetching");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("uses precomputed fetching latency even when window_end is event-time", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-fetching-direct-"));

    try {
      writeApproachRun(rootDir, "spike_pattern", "fetching", {
        resultValue: -23,
        fetchingLatencyRow: {
          result_emitted_at: "1784310435297",
          window_end: "1716454224620",
          window_data_close_time: "1784310432409",
          latency_from_window_close_ms: "2888",
        },
      });
      writeApproachRun(rootDir, "spike_pattern", "approximation", { resultValue: -22.95 });
      writeApproachRun(rootDir, "spike_pattern", "chunked", { resultValue: -23 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["spike_pattern"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
      expect(summary.patternResults[0].perApproach.fetching.semanticWindowCloseToResultMs).toBe(2888);
      expect(summary.patternResults[0].perApproach.fetching.latencyMetricSource).toBe("fetching_latency_log");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("rejects negative fetching latency", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-fetching-negative-"));

    try {
      writeApproachRun(rootDir, "late_burst", "fetching", {
        fetchingLatencyRow: {
          latency_from_window_close_ms: "-5",
        },
      });
      writeApproachRun(rootDir, "late_burst", "approximation", { resultValue: -22.9 });
      writeApproachRun(rootDir, "late_burst", "chunked", { resultValue: -23 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("fail");
      expect(summary.failures).toContain("late_burst iteration 1: fetching: semantic window-close latency is negative");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("rejects missing fetching latency when neither direct nor fallback values are usable", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-fetching-missing-"));

    try {
      writeApproachRun(rootDir, "multiple_bursts", "fetching", {
        fetchingLatencyRow: {
          result_emitted_at: "",
          window_data_close_time: "",
          latency_from_window_close_ms: "",
        },
      });
      writeApproachRun(rootDir, "multiple_bursts", "approximation", { resultValue: -22.9 });
      writeApproachRun(rootDir, "multiple_bursts", "chunked", { resultValue: -23 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["multiple_bursts"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("fail");
      expect(summary.failures).toContain("multiple_bursts iteration 1: fetching: semantic window-close latency missing or invalid");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("falls back to result_emitted_at minus window_data_close_time for fetching latency", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-fetching-fallback-"));

    try {
      writeApproachRun(rootDir, "linear_ramp", "fetching", {
        fetchingLatencyRow: {
          result_emitted_at: "1784310435297",
          window_data_close_time: "1784310432409",
          latency_from_window_close_ms: "",
          window_end: "1716454224620",
        },
      });
      writeApproachRun(rootDir, "linear_ramp", "approximation", { resultValue: -22.9 });
      writeApproachRun(rootDir, "linear_ramp", "chunked", { resultValue: -23 });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["linear_ramp"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
      expect(summary.patternResults[0].perApproach.fetching.semanticWindowCloseToResultMs).toBe(2888);
      expect(summary.patternResults[0].perApproach.fetching.latencyMetricSource).toBe("fetching_latency_log");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("keeps one patternResult entry per requested iteration and one raw row per requested combination", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-multi-"));

    try {
      const { pattern } = createMultiIterationFixture(rootDir);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: [pattern],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      expect(summary.status).toBe("pass");
      expect(summary.patternResults).toHaveLength(3);
      expect(summary.patternResults.map((entry) => [entry.pattern, entry.iteration])).toEqual([
        [pattern, 1],
        [pattern, 2],
        [pattern, 3],
      ]);
      expect(summary.tableRows).toHaveLength(9);
      expect(summary.tableRows.filter((row) => row.Approach === "fetching")).toHaveLength(3);
      expect(summary.tableRows[0]).toMatchObject({
        Pattern: pattern,
        Iteration: 1,
        Approach: "fetching",
      });
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails when a requested middle iteration directory is missing", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-missing-middle-"));

    try {
      const { pattern } = createMultiIterationFixture(rootDir, { iterations: [1, 3] });

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: [pattern],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      expect(summary.status).toBe("fail");
      expect(summary.failures).toContain(`${pattern} iteration 2: fetching: missing iteration directory`);
      expect(summary.failures).toContain(`${pattern} iteration 2: approximation: missing iteration directory`);
      expect(summary.failures).toContain(`${pattern} iteration 2: chunked: missing iteration directory`);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("aggregates exact counts and MAE across requested iterations", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-aggregate-"));

    try {
      const { pattern } = createMultiIterationFixture(rootDir);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: [pattern],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      expect(summary.aggregatedTableRows).toHaveLength(3);
      expect(summary.aggregatedTableRows).toEqual([
        expect.objectContaining({
          Pattern: pattern,
          Approach: "fetching",
          "Complete windows": "3/3",
          "Exact vs Fetching": "3/3",
          "Median window-close latency (ms)": 200,
          "Median average CPU (%)": 20,
          "Median peak RSS (MiB)": 200,
          "Median result": -20,
          "Median absolute error": 0,
          MAE: 0,
        }),
        expect.objectContaining({
          Pattern: pattern,
          Approach: "approximation",
          "Complete windows": "3/3",
          "Exact vs Fetching": "0/3",
          "Median window-close latency (ms)": 500,
          "Median average CPU (%)": 21,
          "Median peak RSS (MiB)": 210,
          "Median result": -19.5,
          "Median absolute error": 0.5,
          MAE: 0.533333333333,
        }),
        expect.objectContaining({
          Pattern: pattern,
          Approach: "chunked",
          "Complete windows": "3/3",
          "Exact vs Fetching": "3/3",
          "Median window-close latency (ms)": 500,
          "Median average CPU (%)": 22,
          "Median peak RSS (MiB)": 220,
          "Median result": -20,
          "Median absolute error": 0,
          MAE: 0,
        }),
      ]);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("renders the required markdown column layout for per-iteration and aggregated tables", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-markdown-"));

    try {
      const { pattern } = createMultiIterationFixture(rootDir);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: [pattern],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      const markdown = renderMarkdown(summary);
      expect(markdown).toContain("| Pattern | Iteration | Approach | Complete window | Window-close latency (ms) | Latency source | Average CPU (%) | Peak RSS (MiB) | Result | Exact vs Fetching | Absolute error |");
      expect(markdown).toContain("| Pattern | Approach | Complete windows | Median window-close latency (ms) | Median average CPU (%) | Median peak RSS (MiB) | Exact vs Fetching | MAE |");
      const lateBurstRows = markdown.split("\n").filter((line) => line.startsWith(`| ${pattern} |`));
      expect(lateBurstRows).toHaveLength(12);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("writes the expected raw and aggregated CSV row counts from the CLI for three requested iterations", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-cli-"));

    try {
      const outputDir = path.join(rootDir, "analysis", "first-window-validation");
      const { pattern } = createMultiIterationFixture(rootDir);

      childProcess.execFileSync(
        process.execPath,
        [
          "scripts/benchmark/validate-custom-pattern-first-window-smoke.js",
          "--input-root",
          rootDir,
          "--output-dir",
          outputDir,
          "--patterns",
          pattern,
          "--approaches",
          "fetching,approximation,chunked",
          "--iterations",
          "1,2,3",
        ],
        { cwd: path.resolve(__dirname, "../..") },
      );

      const rawCsvLines = fs.readFileSync(path.join(outputDir, "summary.csv"), "utf8").trim().split("\n");
      const aggregatedCsvLines = fs.readFileSync(path.join(outputDir, "summary.aggregated.csv"), "utf8").trim().split("\n");

      expect(rawCsvLines[0]).toBe("Pattern,Iteration,Approach,Complete window,Window-close latency (ms),Latency source,Average CPU (%),Peak RSS (MiB),Result,Exact vs Fetching,Absolute error");
      expect(rawCsvLines).toHaveLength(10);
      expect(aggregatedCsvLines[0]).toBe("Pattern,Approach,Complete windows,Median window-close latency (ms),Median average CPU (%),Median peak RSS (MiB),Exact vs Fetching,MAE");
      expect(aggregatedCsvLines).toHaveLength(4);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });
});
