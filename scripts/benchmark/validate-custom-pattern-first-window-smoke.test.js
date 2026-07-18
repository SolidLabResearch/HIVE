const fs = require("fs");
const os = require("os");
const path = require("path");
const childProcess = require("child_process");

const {
  calculateRegistrationAnchoredLatencies,
  compareAggregateResultEquivalence,
  EXACT_AGGREGATE_ABSOLUTE_TOLERANCE,
  EXACT_AGGREGATE_COMPARISON_METHOD,
  REGISTRATION_ANCHORED_LATENCY_SOURCE,
} = require("../../analysis/accuracy/accuracy-comparison-custom-patterns.js");
const {
  parseArgs,
  renderMarkdown,
  summarizeSmokeValidation,
} = require("./validate-custom-pattern-first-window-smoke.js");

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
    ...rows.map((row) => headers.map((header) => formatValue(row[header])).join(",")),
  ];
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function buildLatencyRow(overrides = {}) {
  const defaults = {
    window_number: "1",
    query_registered_at: "0",
    result_emitted_at: "121500",
    elapsed_since_registration_ms: "121500",
    delay_past_expected_close_ms: "1500",
    latency_from_window_close_ms: "1500",
    window_start: "1000",
    window_end: "121000",
    window_data_close_time: "121000",
    result_value: "-23",
  };
  return { ...defaults, ...overrides };
}

function writeApproachRun(rootDir, pattern, approach, options = {}) {
  const iteration = options.iteration ?? 1;
  const runDir = path.join(rootDir, approach, pattern, `iteration${iteration}`);
  fs.mkdirSync(runDir, { recursive: true });

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

  const latencyRow = buildLatencyRow({
    result_value: String(options.resultValue ?? -23),
    ...(options.latencyRow || {}),
  });

  const resultRow = {
    timestamp: latencyRow.result_emitted_at || "121500",
    window_number: latencyRow.window_number || "1",
    window_start: latencyRow.window_start || "1000",
    window_end: latencyRow.window_end || "121000",
    result_value: String(options.resultValue ?? -23),
    elapsed_since_registration_ms: latencyRow.elapsed_since_registration_ms || "",
    delay_past_expected_close_ms: latencyRow.delay_past_expected_close_ms || "",
  };

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
    [resultRow],
  );

  const latencyHeadersByApproach = {
    fetching: [
      "window_number",
      "query_registered_at",
      "result_emitted_at",
      "elapsed_since_registration_ms",
      "delay_past_expected_close_ms",
      "latency_from_window_close_ms",
      "window_start",
      "window_end",
      "window_data_close_time",
      "result_value",
    ],
    approximation: [
      "window_number",
      "query_registered_at",
      "result_emitted_at",
      "elapsed_since_registration_ms",
      "delay_past_expected_close_ms",
      "window_start",
      "window_end",
      "window_data_close_time",
      "approximation_status",
      "result_value",
    ],
    chunked: [
      "window_number",
      "query_registered_at",
      "result_emitted_at",
      "elapsed_since_registration_ms",
      "delay_past_expected_close_ms",
      "window_start",
      "window_end",
      "window_data_close_time",
      "result_value",
    ],
  };

  if (approach === "approximation" && !Object.prototype.hasOwnProperty.call(latencyRow, "approximation_status")) {
    latencyRow.approximation_status = "completed_window_approximation";
  }

  writeCsv(
    path.join(runDir, latencyFile),
    latencyHeadersByApproach[approach],
    [latencyRow],
  );

  if (approach === "fetching") {
    writeCsv(
      path.join(runDir, "fetching_window_diagnostics.csv"),
      ["window_number", "completeness_status", "accepted_or_suppressed", "reason"],
      [{
        window_number: latencyRow.window_number || "1",
        completeness_status: "complete",
        accepted_or_suppressed: "accepted",
        reason: "finalized_settled_window",
      }],
    );
  }

  if (approach === "chunked") {
    writeCsv(
      path.join(runDir, "chunked_window_diagnostics.csv"),
      ["external_window_number", "internal_chunks_json"],
      [{
        external_window_number: latencyRow.window_number || "1",
        internal_chunks_json: JSON.stringify([
          { coverageComplete: true },
          { coverageComplete: true },
        ]),
      }],
    );
  }

  writeJson(path.join(runDir, "resource_summary.json"), {
    meanCpuPct: options.meanCpuPct ?? 12.5,
    peakRssMb: options.peakRssMb ?? 256,
  });

  writeJson(path.join(runDir, "benchmark_window_cap_summary.json"), {
    emittedFinalWindowCount: 1,
    stoppedAfterTargetWindows: true,
  });

  writeJson(path.join(runDir, "attempt_metadata.json"), {
    output_window_range: options.outputWindowRangeMs ?? 120000,
    output_window_step: options.outputWindowStepMs ?? 60000,
    configuredTimeoutMs: options.configuredTimeoutMs ?? 300000,
  });

  return runDir;
}

function writeExecutionSummary(rootDir, entries) {
  writeJson(path.join(rootDir, "custom_pattern_comparison_summary.json"), {
    results: entries.map((entry) => ({
      configuredTimeoutMs: 300000,
      ...entry,
    })),
  });
}

function createThreeApproachFixture(rootDir, pattern = "late_burst", iterations = [1]) {
  const entries = [];
  for (const iteration of iterations) {
    writeApproachRun(rootDir, pattern, "fetching", {
      iteration,
      resultValue: -23,
      latencyRow: {
        result_emitted_at: String(121500 + ((iteration - 1) * 1000)),
        elapsed_since_registration_ms: String(121500 + ((iteration - 1) * 1000)),
        delay_past_expected_close_ms: String(1500 + ((iteration - 1) * 1000)),
        latency_from_window_close_ms: String(1500 + ((iteration - 1) * 1000)),
      },
      meanCpuPct: 10 + iteration,
      peakRssMb: 100 + iteration,
    });
    writeApproachRun(rootDir, pattern, "approximation", {
      iteration,
      resultValue: -22.5 - iteration,
      latencyRow: {
        result_emitted_at: String(121400 + ((iteration - 1) * 1000)),
        elapsed_since_registration_ms: String(121400 + ((iteration - 1) * 1000)),
        delay_past_expected_close_ms: String(1400 + ((iteration - 1) * 1000)),
      },
      meanCpuPct: 20 + iteration,
      peakRssMb: 200 + iteration,
    });
    writeApproachRun(rootDir, pattern, "chunked", {
      iteration,
      resultValue: -23,
      latencyRow: {
        result_emitted_at: String(121300 + ((iteration - 1) * 1000)),
        elapsed_since_registration_ms: String(121300 + ((iteration - 1) * 1000)),
        delay_past_expected_close_ms: String(1300 + ((iteration - 1) * 1000)),
      },
      meanCpuPct: 30 + iteration,
      peakRssMb: 300 + iteration,
    });

    entries.push(
      { approach: "fetching", pattern, iteration },
      { approach: "approximation", pattern, iteration },
      { approach: "chunked", pattern, iteration },
    );
  }
  writeExecutionSummary(rootDir, entries);
}

describe("registration-anchored latency helper", () => {
  test("calculates first-window query-to-first-result latency", () => {
    const metrics = calculateRegistrationAnchoredLatencies({
      queryRegisteredAt: 1000,
      resultEmittedAt: 123950,
      windowNumber: 1,
      outputWindowRangeMs: 120000,
      outputWindowStepMs: 60000,
    });

    expect(metrics.queryToFirstResultMs).toBe(122950);
    expect(metrics.latencyMetricSource).toBe(REGISTRATION_ANCHORED_LATENCY_SOURCE);
  });

  test("calculates first-window post-window-close latency", () => {
    const metrics = calculateRegistrationAnchoredLatencies({
      queryRegisteredAt: 1000,
      resultEmittedAt: 123950,
      windowNumber: 1,
      outputWindowRangeMs: 120000,
      outputWindowStepMs: 60000,
    });

    expect(metrics.registrationAnchoredWindowCloseAt).toBe(121000);
    expect(metrics.postWindowCloseLatencyMs).toBe(2950);
    expect(metrics.queryToFirstResultMs - 120000).toBe(metrics.postWindowCloseLatencyMs);
  });

  test("calculates second-window latency using the configured step", () => {
    const metrics = calculateRegistrationAnchoredLatencies({
      queryRegisteredAt: 5000,
      resultEmittedAt: 186250,
      windowNumber: 2,
      outputWindowRangeMs: 120000,
      outputWindowStepMs: 60000,
    });

    expect(metrics.registrationAnchoredWindowCloseAt).toBe(185000);
    expect(metrics.queryToFirstResultMs).toBe(181250);
    expect(metrics.postWindowCloseLatencyMs).toBe(1250);
  });
});

describe("custom-pattern first-window smoke validator", () => {
  test("all three approaches use the same registration-anchored helper output", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-shared-"));

    try {
      createThreeApproachFixture(rootDir, "low_variability", [1]);
      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["low_variability"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
      for (const approach of ["fetching", "approximation", "chunked"]) {
        expect(summary.patternResults[0].perApproach[approach].latencyMetricSource).toBe(REGISTRATION_ANCHORED_LATENCY_SOURCE);
      }
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("verifies elapsed_since_registration_ms when present", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-elapsed-"));

    try {
      writeApproachRun(rootDir, "late_burst", "fetching", {
        latencyRow: { elapsed_since_registration_ms: "121501" },
      });
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("verifies delay_past_expected_close_ms when present", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-delay-"));

    try {
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "approximation", {
        latencyRow: { delay_past_expected_close_ms: "1501", result_emitted_at: "121501", elapsed_since_registration_ms: "121501" },
      });
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("verifies fetching latency_from_window_close_ms when present", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-fetching-raw-"));

    try {
      writeApproachRun(rootDir, "late_burst", "fetching", {
        latencyRow: { latency_from_window_close_ms: "1501", result_emitted_at: "121501", elapsed_since_registration_ms: "121501", delay_past_expected_close_ms: "1501" },
      });
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.status).toBe("pass");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on missing query registration timestamp", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-missing-registration-"));

    try {
      writeApproachRun(rootDir, "late_burst", "fetching", {
        latencyRow: { query_registered_at: "" },
      });
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: fetching: missing query registration timestamp");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on missing result-emission timestamp", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-missing-emission-"));

    try {
      writeApproachRun(rootDir, "late_burst", "approximation", {
        latencyRow: { result_emitted_at: "" },
      });
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: approximation: missing result-emission timestamp");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on invalid range", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-invalid-range-"));

    try {
      writeApproachRun(rootDir, "late_burst", "chunked", { outputWindowRangeMs: 0 });
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: chunked: missing or invalid range");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on invalid step", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-invalid-step-"));

    try {
      writeApproachRun(rootDir, "late_burst", "chunked", { outputWindowStepMs: -1 });
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: chunked: missing or invalid step");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on invalid window number", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-invalid-window-"));

    try {
      writeApproachRun(rootDir, "late_burst", "approximation", {
        latencyRow: { window_number: "0" },
      });
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: approximation: invalid window number");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on negative query-to-first-result latency", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-negative-e2e-"));

    try {
      writeApproachRun(rootDir, "late_burst", "fetching", {
        latencyRow: {
          query_registered_at: "5000",
          result_emitted_at: "4999",
          elapsed_since_registration_ms: "-1",
          delay_past_expected_close_ms: "-120001",
          latency_from_window_close_ms: "-120001",
        },
      });
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: fetching: negative query-to-first-result latency");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on negative post-window-close latency", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-negative-post-close-"));

    try {
      writeApproachRun(rootDir, "late_burst", "chunked", {
        latencyRow: {
          result_emitted_at: "119999",
          elapsed_since_registration_ms: "119999",
          delay_past_expected_close_ms: "-1",
        },
      });
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "approximation");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: chunked: negative post-window-close latency");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("fails on raw metric mismatch greater than 1 ms", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-raw-mismatch-"));

    try {
      writeApproachRun(rootDir, "late_burst", "approximation", {
        latencyRow: { delay_past_expected_close_ms: "1503" },
      });
      writeApproachRun(rootDir, "late_burst", "fetching");
      writeApproachRun(rootDir, "late_burst", "chunked");
      writeExecutionSummary(rootDir, [
        { approach: "fetching", pattern: "late_burst", iteration: 1 },
        { approach: "approximation", pattern: "late_burst", iteration: 1 },
        { approach: "chunked", pattern: "late_burst", iteration: 1 },
      ]);

      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1],
      });

      expect(summary.failures).toContain("late_burst iteration 1: approximation: delay_past_expected_close_ms mismatch greater than 1 ms");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("accepts comma-separated iterations in parseArgs", () => {
    const args = parseArgs([
      "--iterations", "1,2,3",
      "--patterns", "late_burst",
      "--approaches", "fetching,approximation,chunked",
    ]);

    expect(args.iterations).toEqual([1, 2, 3]);
    expect(args.patterns).toEqual(["late_burst"]);
    expect(args.approaches).toEqual(["fetching", "approximation", "chunked"]);
  });

  test("fails when requested combinations are missing", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-missing-combos-"));

    try {
      createThreeApproachFixture(rootDir, "late_burst", [1, 3]);
      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      expect(summary.failures).toContain("late_burst iteration 2: fetching: missing iteration directory");
      expect(summary.failures).toContain("late_burst iteration 2: approximation: missing iteration directory");
      expect(summary.failures).toContain("late_burst iteration 2: chunked: missing iteration directory");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("writes nine raw rows for three iterations and three approaches", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-nine-rows-"));

    try {
      createThreeApproachFixture(rootDir, "late_burst", [1, 2, 3]);
      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      expect(summary.tableRows).toHaveLength(9);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("aggregates medians for both latency metrics", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-medians-"));

    try {
      createThreeApproachFixture(rootDir, "late_burst", [1, 2, 3]);
      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir: path.join(rootDir, "analysis"),
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      expect(summary.aggregatedTableRows).toEqual([
        expect.objectContaining({
          Pattern: "late_burst",
          Approach: "fetching",
          "Median query-to-first-result latency (ms)": 122500,
          "Median post-window-close latency (ms)": 2500,
        }),
        expect.objectContaining({
          Pattern: "late_burst",
          Approach: "approximation",
          "Median query-to-first-result latency (ms)": 122400,
          "Median post-window-close latency (ms)": 2400,
        }),
        expect.objectContaining({
          Pattern: "late_burst",
          Approach: "chunked",
          "Median query-to-first-result latency (ms)": 122300,
          "Median post-window-close latency (ms)": 2300,
        }),
      ]);
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("renders markdown and csv column alignment for the new latency metrics", () => {
    const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-smoke-columns-"));

    try {
      const outputDir = path.join(rootDir, "analysis");
      createThreeApproachFixture(rootDir, "late_burst", [1, 2, 3]);
      const summary = summarizeSmokeValidation({
        inputRoot: rootDir,
        outputDir,
        patterns: ["late_burst"],
        approaches: ["fetching", "approximation", "chunked"],
        iterations: [1, 2, 3],
      });

      const markdown = renderMarkdown(summary);
      expect(markdown).toContain("| Pattern | Iteration | Approach | Complete window | Query-to-first-result latency (ms) | Post-window-close latency (ms) | Latency source | Average CPU (%) | Peak RSS (MiB) | Result | Exact vs Fetching | Absolute error |");
      expect(markdown).toContain("| Pattern | Approach | Complete windows | Median query-to-first-result latency (ms) | Median post-window-close latency (ms) | Median average CPU (%) | Median peak RSS (MiB) | Exact vs Fetching | MAE |");

      childProcess.execFileSync(
        process.execPath,
        [
          "scripts/benchmark/validate-custom-pattern-first-window-smoke.js",
          "--input-root",
          rootDir,
          "--output-dir",
          outputDir,
          "--patterns",
          "late_burst",
          "--approaches",
          "fetching,approximation,chunked",
          "--iterations",
          "1,2,3",
        ],
        { cwd: path.resolve(__dirname, "../..") },
      );

      const rawCsvLines = fs.readFileSync(path.join(outputDir, "summary.csv"), "utf8").trim().split("\n");
      const aggregatedCsvLines = fs.readFileSync(path.join(outputDir, "summary.aggregated.csv"), "utf8").trim().split("\n");

      expect(rawCsvLines[0]).toBe("Pattern,Iteration,Approach,Complete window,Query-to-first-result latency (ms),Post-window-close latency (ms),Latency source,Average CPU (%),Peak RSS (MiB),Result,Exact vs Fetching,Absolute error");
      expect(aggregatedCsvLines[0]).toBe("Pattern,Approach,Complete windows,Median query-to-first-result latency (ms),Median post-window-close latency (ms),Median average CPU (%),Median peak RSS (MiB),Exact vs Fetching,MAE");
    } finally {
      fs.rmSync(rootDir, { recursive: true, force: true });
    }
  });

  test("floating-point aggregate equivalence remains unchanged", () => {
    const identical = compareAggregateResultEquivalence(-23, -23);
    expect(identical.exactAgreement).toBe(true);
    expect(identical.rawAbsoluteError).toBe(0);
    expect(identical.comparisonTolerance).toBe(EXACT_AGGREGATE_ABSOLUTE_TOLERANCE);
    expect(identical.comparisonMethod).toBe(EXACT_AGGREGATE_COMPARISON_METHOD);
  });
});
