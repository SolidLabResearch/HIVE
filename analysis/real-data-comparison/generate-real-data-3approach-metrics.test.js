const fs = require("fs");
const os = require("os");
const path = require("path");

const { APPROACHES } = require("../../experiments/real-data-comparison/run-real-data-4-approaches.js");
const {
  analyzeRealDataResults,
} = require("./generate-real-data-3approach-metrics.js");

function writeCsv(filePath, headers, rows) {
  const body = rows.map((row) => headers.map((header) => row[header] ?? "").join(",")).join("\n");
  fs.writeFileSync(filePath, `${headers.join(",")}\n${body}\n`);
}

function buildLatencyRows(approachName, windowNumbers, options = {}) {
  const lastObservedField = approachName === "approximation"
    ? "last_data_received_at"
    : approachName === "chunked"
      ? "last_chunk_received_at"
      : "last_obs_received_at";

  return windowNumbers.map((windowNumber) => {
    const windowEnd = 1000 + 120000 + ((windowNumber - 1) * 60000);
    const closeToResultMs = (options.closeToResultMsByWindow && options.closeToResultMsByWindow[windowNumber])
      || options.closeToResultMs
      || 500;
    const row = {
      window_number: windowNumber,
      query_registered_at: 1000,
      first_data_received_at: 1100,
      expected_window_close: (options.expectedWindowCloseByWindow && options.expectedWindowCloseByWindow[windowNumber])
        ?? windowEnd,
      result_emitted_at: (options.resultEmittedAtByWindow && options.resultEmittedAtByWindow[windowNumber])
        ?? (windowEnd + closeToResultMs),
      window_semantics: "trailing",
      logical_trigger_time: (options.logicalTriggerTimeByWindow && options.logicalTriggerTimeByWindow[windowNumber])
        ?? windowEnd,
      window_start: windowEnd - 120000,
      window_end: (options.windowEndByWindow && options.windowEndByWindow[windowNumber]) ?? windowEnd,
      window_data_close_time: (options.windowDataCloseTimeByWindow && options.windowDataCloseTimeByWindow[windowNumber])
        ?? windowEnd,
      latency_from_logical_trigger_ms: (options.latencyFromLogicalTriggerMsByWindow && options.latencyFromLogicalTriggerMsByWindow[windowNumber])
        ?? closeToResultMs,
      latency_from_window_close_ms: (options.latencyFromWindowCloseMsByWindow && options.latencyFromWindowCloseMsByWindow[windowNumber])
        ?? closeToResultMs,
      latency_domain_status: (options.latencyDomainStatusByWindow && Object.prototype.hasOwnProperty.call(options.latencyDomainStatusByWindow, windowNumber))
        ? options.latencyDomainStatusByWindow[windowNumber]
        : (options.latencyDomainStatus ?? "wall_clock_mapped"),
      wall_clock_close_to_result_ms: (options.wallClockCloseToResultMsByWindow && Object.prototype.hasOwnProperty.call(options.wallClockCloseToResultMsByWindow, windowNumber))
        ? options.wallClockCloseToResultMsByWindow[windowNumber]
        : closeToResultMs,
      metadata_source: "direct",
      result_value: (windowNumber * 10) + (options.valueOffset || 0),
      warmup: options.warmupByWindow && Object.prototype.hasOwnProperty.call(options.warmupByWindow, windowNumber)
        ? String(Boolean(options.warmupByWindow[windowNumber]))
        : "",
    };
    row[lastObservedField] = windowEnd - 100;
    return row;
  });
}

function writeIteration(rootDir, approachName, iteration, options) {
  const approach = APPROACHES.find((entry) => entry.name === approachName);
  const iterationDir = path.join(rootDir, approachName, `iteration${iteration}`);
  fs.mkdirSync(iterationDir, { recursive: true });

  writeCsv(
    path.join(iterationDir, approach.logFiles.latency),
    [
      "window_number",
      "query_registered_at",
      "first_data_received_at",
      "expected_window_close",
      "last_obs_received_at",
      "last_data_received_at",
      "last_chunk_received_at",
      "result_emitted_at",
      "window_semantics",
      "logical_trigger_time",
      "window_start",
      "window_end",
      "window_data_close_time",
      "latency_from_logical_trigger_ms",
      "latency_from_window_close_ms",
      "latency_domain_status",
      "wall_clock_close_to_result_ms",
      "metadata_source",
      "result_value",
      "warmup",
    ],
    buildLatencyRows(approachName, options.windowNumbers, options),
  );

  writeCsv(
    path.join(iterationDir, approach.processTreeFile),
    ["timestamp", "tree_rss_bytes", "tree_cpu_seconds"],
    [
      { timestamp: 1000, tree_rss_bytes: 100 * 1024 * 1024, tree_cpu_seconds: 0 },
      { timestamp: 2000, tree_rss_bytes: (options.rssMiB || 120) * 1024 * 1024, tree_cpu_seconds: options.cpuSeconds || 5 },
    ],
  );

  fs.writeFileSync(
    path.join(iterationDir, "benchmark_window_cap_summary.json"),
    `${JSON.stringify({
      targetWindowCount: options.targetWindowCount ?? options.windowNumbers.length,
      emittedFinalWindowCount: options.emittedFinalWindowCount ?? options.windowNumbers.length,
      finalWindowNumbers: options.finalWindowNumbers ?? options.windowNumbers,
      stoppedAfterTargetWindows: options.stoppedAfterTargetWindows ?? true,
      stopReason: options.stopReason ?? "target_window_count_reached",
      approach: approachName,
    }, null, 2)}\n`,
  );

  if (!options.omitRunSummary) {
    fs.writeFileSync(
      path.join(iterationDir, "run_summary.json"),
      `${JSON.stringify({
        completionStatus: options.completionStatus ?? "completed",
        publisherExitReason: "finite_replay_duration_reached",
        failedPublishes: 0,
      }, null, 2)}\n`,
    );
  }

  fs.writeFileSync(
    path.join(iterationDir, "mqtt_traffic_summary.json"),
    `${JSON.stringify({
      published_application_bytes: 1000,
      estimated_delivery_bytes: 1000,
    }, null, 2)}\n`,
  );
}

describe("real-data 3-approach report helper", () => {
  test("steady-state mode partitions windows 1..35 and excludes approximation domain mismatches from the main latency table", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-steady-"));
    const windows = Array.from({ length: 35 }, (_, index) => index + 1);

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: windows,
        closeToResultMs: 500,
        cpuSeconds: 4,
        rssMiB: 150,
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: windows,
        closeToResultMs: 700,
        valueOffset: 1,
        latencyDomainStatus: "domain_mismatch",
        cpuSeconds: 5,
        rssMiB: 200,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: windows,
        closeToResultMs: 650,
        cpuSeconds: 4.5,
        rssMiB: 210,
      });

      const result = analyzeRealDataResults({
        mode: "steady-state",
        inputRoot: tempRoot,
      });

      expect(result.selectedApproachesExact).toBe(true);
      expect(result.errors).toEqual([]);
      expect(result.steadyStateLatency.find((row) => row.approach === "fetching").steady.count).toBe(30);
      expect(result.steadyStateLatency.find((row) => row.approach === "approximation").steady.count).toBe(0);
      expect(result.accuracyRows.find((row) => row.approach === "chunked").exactAgainstFetching).toBe(true);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-cost mode summarizes first-window startup cost across independent iterations", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-startup-"));

    try {
      for (let iteration = 1; iteration <= 2; iteration += 1) {
        writeIteration(tempRoot, "fetching", iteration, {
          windowNumbers: [1],
          closeToResultMs: 500 + iteration,
          cpuSeconds: 2 + (iteration * 0.1),
          rssMiB: 120 + iteration,
        });
        writeIteration(tempRoot, "approximation", iteration, {
          windowNumbers: [1],
          closeToResultMs: 550 + iteration,
          valueOffset: 1,
          cpuSeconds: 2.4 + (iteration * 0.1),
          rssMiB: 130 + iteration,
        });
        writeIteration(tempRoot, "chunked", iteration, {
          windowNumbers: [1],
          closeToResultMs: 500 + iteration,
          cpuSeconds: 2.2 + (iteration * 0.1),
          rssMiB: 140 + iteration,
        });
      }

      const result = analyzeRealDataResults({
        mode: "startup-cost",
        inputRoot: tempRoot,
        expectedIterations: 2,
        targetWindows: 1,
      });

      expect(result.selectedApproachesExact).toBe(true);
      expect(result.errors).toEqual([]);
      expect(result.startupLatency.find((row) => row.approach === "fetching").startupCostMs.count).toBe(2);
      expect(result.accuracyRows.find((row) => row.approach === "chunked").exactAgainstFetching).toBe(true);
      expect(result.perApproach.find((row) => row.approachName === "approximation").processResourceSummary.cpuSeconds.mean).toBeCloseTo(2.55, 6);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted mode passes when all approaches have a startup-valid first row even if one iteration only reaches windows 1,2,3", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-partial-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3],
        finalWindowNumbers: [1, 2, 3],
        targetWindowCount: 5,
        emittedFinalWindowCount: 3,
        stoppedAfterTargetWindows: false,
        stopReason: "other",
        closeToResultMsByWindow: { 1: 401, 2: 420 },
      });
      for (let iteration = 2; iteration <= 2; iteration += 1) {
        writeIteration(tempRoot, "fetching", iteration, {
          windowNumbers: [1, 2, 3, 4, 5],
          closeToResultMsByWindow: { 1: 401 + iteration, 2: 420 + iteration },
        });
      }
      for (let iteration = 1; iteration <= 2; iteration += 1) {
        writeIteration(tempRoot, "approximation", iteration, {
          windowNumbers: [1, 2, 3, 4, 5],
          closeToResultMsByWindow: { 1: 451 + iteration, 2: 470 + iteration },
          valueOffset: 1,
        });
        writeIteration(tempRoot, "chunked", iteration, {
          windowNumbers: [1, 2, 3, 4, 5],
          closeToResultMsByWindow: { 1: 401 + iteration, 2: 425 + iteration },
        });
      }

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 2,
        targetWindows: 5,
      });

      expect(result.errors).toEqual([]);
      expect(result.startupLatency.find((row) => row.approach === "fetching").usableFirstEmittedRows).toBe(2);
      expect(result.startupFirstEmittedRows.find((row) => row.approach === "fetching" && row.iteration === 1).startupValid).toBe(true);
      expect(result.startupFirstEmittedRows.find((row) => row.approach === "fetching" && row.iteration === 1).finalWindowNumbers).toEqual([1, 2, 3]);
      expect(result.accuracyRows.find((row) => row.approach === "chunked").matchedFirstEmittedWindowCount).toBe(2);
      expect(result.warnings).toContain("fetching/iteration1: diagnostic stop reason is other; startup-first-emitted only requires a usable first row");
      expect(result.warnings).toContain("fetching/iteration1: diagnostic final windows 1,2,3 did not reach the target-window upper bound 5");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted mode aligns approximation to window 2 when that is the first usable emitted row", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-window2-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 410, 2: 430 },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [2, 3, 4, 5],
        closeToResultMsByWindow: { 2: 460, 3: 480 },
        valueOffset: 2,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 410, 2: 435 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const approximationEntry = result.startupFirstEmittedRows.find((row) => row.approach === "approximation");
      const approximationAccuracy = result.accuracyRows.find((row) => row.approach === "approximation");

      expect(approximationEntry.firstEmittedWindowNumber).toBe(2);
      expect(approximationEntry.accuracyComparable).toBe(true);
      expect(approximationEntry.accuracyAlignment).toBe("matched_candidate_window_in_fetching");
      expect(approximationAccuracy.matchedCandidateWindowCount).toBe(1);
      expect(approximationAccuracy.nonComparableIterationCount).toBe(0);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted mode reports when approximation emits no usable result row", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-no-result-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 410, 2: 430 },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [],
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 410, 2: 435 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const approximationEntry = result.startupFirstEmittedRows.find((row) => row.approach === "approximation");
      const approximationAccuracy = result.accuracyRows.find((row) => row.approach === "approximation");

      expect(approximationEntry.usable).toBe(false);
      expect(approximationEntry.startupValid).toBe(false);
      expect(approximationEntry.noUsableResultReason).toBe("no_result_rows");
      expect(approximationEntry.accuracyComparable).toBe(false);
      expect(approximationAccuracy.comparableIterationCount).toBe(0);
      expect(result.errors).toContain("approximation/iteration1: no usable non-warmup result row (no_result_rows)");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted mode ignores a warmup row when a non-warmup row exists", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-warmup-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 405, 2: 425 },
        warmupByWindow: { 1: true, 2: false },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 455, 2: 475 },
        warmupByWindow: { 1: true, 2: false },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 405, 2: 430 },
        warmupByWindow: { 1: true, 2: false },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const approximationEntry = result.startupFirstEmittedRows.find((row) => row.approach === "approximation");

      expect(approximationEntry.firstEmittedWindowNumber).toBe(2);
      expect(approximationEntry.warmupRowsSkipped).toBe(1);
      expect(approximationEntry.accuracyAlignment).toBe("matched_first_emitted_window");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted accepts successful cap summaries as bounded-run completion evidence", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-cap-evidence-"));

    try {
      for (let iteration = 1; iteration <= 3; iteration += 1) {
        writeIteration(tempRoot, "fetching", iteration, {
          windowNumbers: [1, 2, 3, 4, 5],
          wallClockCloseToResultMsByWindow: { 1: "invalid" },
          expectedWindowCloseByWindow: { 1: 121480 },
          logicalTriggerTimeByWindow: { 1: 60520 },
          windowDataCloseTimeByWindow: { 1: 60520 },
          windowEndByWindow: { 1: 120520 },
          resultEmittedAtByWindow: { 1: 62910 },
          latencyFromLogicalTriggerMsByWindow: { 1: 0 },
          latencyFromWindowCloseMsByWindow: { 1: 0 },
          latencyDomainStatus: "",
          omitRunSummary: iteration > 1,
        });
        writeIteration(tempRoot, "approximation", iteration, {
          windowNumbers: [1, 2, 3, 4, 5],
          closeToResultMsByWindow: { 1: 2240 + iteration },
          valueOffset: 1,
        });
        writeIteration(tempRoot, "chunked", iteration, {
          windowNumbers: [1, 2, 3, 4, 5],
          closeToResultMsByWindow: { 1: 2260 + iteration },
        });
      }

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 3,
        targetWindows: 5,
      });

      const fetchingSummary = result.startupLatency.find((row) => row.approach === "fetching");
      const fetchingEntries = result.startupFirstEmittedRows.filter((row) => row.approach === "fetching");

      expect(result.errors).toEqual([]);
      expect(fetchingSummary.usableFirstEmittedRows).toBe(3);
      expect(fetchingSummary.missingFirstEmittedRows).toBe(0);
      expect(fetchingEntries).toHaveLength(3);
      expect(fetchingEntries.every((row) => row.startupValid)).toBe(true);
      expect(fetchingEntries.every((row) => row.runCompleted)).toBe(true);
      expect(fetchingEntries.every((row) => row.startupLatencyMs > 2000)).toBe(true);
      expect(fetchingEntries.every((row) => row.startupLatencySource === "resultEmittedAtMinusWindowEnd")).toBe(true);
      expect(fetchingEntries.every((row) => row.latencyDomainStatus === "wall_clock_mapped")).toBe(true);
      expect(fetchingEntries.every((row) => row.diagnosticDirectWindowCloseMs === 0)).toBe(true);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted uses explicit wall-clock-mapped fetching latency when raw fields are present", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-fetching-wall-clock-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        wallClockCloseToResultMsByWindow: { 1: 2245 },
        latencyDomainStatus: "wall_clock_mapped",
        latencyFromLogicalTriggerMsByWindow: { 1: 0 },
        latencyFromWindowCloseMsByWindow: { 1: 0 },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2246 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2267 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const fetchingEntry = result.startupFirstEmittedRows.find((row) => row.approach === "fetching");
      expect(fetchingEntry.startupValid).toBe(true);
      expect(fetchingEntry.startupLatencyMs).toBe(2245);
      expect(fetchingEntry.startupLatencySource).toBe("wallClockCloseToResultMs");
      expect(fetchingEntry.latencyDomainStatus).toBe("wall_clock_mapped");
      expect(fetchingEntry.diagnosticDirectWindowCloseMs).toBe(0);
      expect(result.errors).toEqual([]);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted rejects direct fetching latency as non-comparable when no mapped latency is available", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-direct-only-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1],
        resultEmittedAtByWindow: { 1: "invalid" },
        windowEndByWindow: { 1: "invalid" },
        latencyFromLogicalTriggerMsByWindow: { 1: 0 },
        latencyFromWindowCloseMsByWindow: { 1: 0 },
        latencyDomainStatus: "",
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1],
        closeToResultMsByWindow: { 1: 2245 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1],
        closeToResultMsByWindow: { 1: 2267 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 1,
      });

      const fetchingEntry = result.startupFirstEmittedRows.find((row) => row.approach === "fetching");
      expect(fetchingEntry.startupValid).toBe(false);
      expect(fetchingEntry.startupLatencySource).toBe("unavailable");
      expect(fetchingEntry.startupLatencyFailureReason).toBe("no_result_rows");
      expect(fetchingEntry.latencyDomainStatus).toBeNull();
      expect(result.errors).toContain(
        "fetching/iteration1: no usable non-warmup result row (no_result_rows)",
      );
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted rejects negative registration-anchored values instead of using them as comparable latency", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-negative-registration-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        resultEmittedAtByWindow: { 1: 62910 },
        expectedWindowCloseByWindow: { 1: 121480 },
        windowEndByWindow: { 1: "invalid" },
        wallClockCloseToResultMsByWindow: { 1: "invalid" },
        latencyFromLogicalTriggerMsByWindow: { 1: "invalid" },
        latencyFromWindowCloseMsByWindow: { 1: "invalid" },
        latencyDomainStatus: "",
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2245 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2267 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const fetchingEntry = result.startupFirstEmittedRows.find((row) => row.approach === "fetching");
      expect(fetchingEntry.startupValid).toBe(false);
      expect(fetchingEntry.startupLatencyFailureReason).toBe("missing_comparable_close_to_result_latency");
      expect(fetchingEntry.registrationToResultMs).toBeGreaterThanOrEqual(0);
      expect(result.errors).toContain(
        "fetching/iteration1: first usable row has invalid comparable startup latency (missing_comparable_close_to_result_latency)",
      );
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted flags mixed-domain startup comparison instead of silently comparing rows", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-mixed-domain-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1],
        closeToResultMsByWindow: { 1: 2245 },
        latencyDomainStatus: "wall_clock_mapped",
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1],
        closeToResultMsByWindow: { 1: 2246 },
        latencyDomainStatus: "wall_clock_mapped",
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1],
        closeToResultMsByWindow: { 1: 0 },
        resultEmittedAtByWindow: { 1: "invalid" },
        windowEndByWindow: { 1: "invalid" },
        wallClockCloseToResultMsByWindow: { 1: "invalid" },
        latencyDomainStatus: "",
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 1,
      });

      const chunkedEntry = result.startupFirstEmittedRows.find((row) => row.approach === "chunked");
      expect(chunkedEntry.startupValid).toBe(false);
      expect(chunkedEntry.latencyDomainStatus).toBeNull();
      expect(result.errors).toContain(
        "chunked/iteration1: no usable non-warmup result row (no_result_rows)",
      );
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted still rejects truly incomplete iterations when run summary and successful cap evidence are both missing", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-incomplete-evidence-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3],
        finalWindowNumbers: [1, 2, 3],
        targetWindowCount: 5,
        emittedFinalWindowCount: 3,
        stoppedAfterTargetWindows: false,
        stopReason: "other",
        closeToResultMsByWindow: { 1: 0 },
        latencyDomainStatus: "",
        omitRunSummary: true,
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2245 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2267 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      expect(result.errors).toContain(
        "fetching/iteration1: missing bounded-run completion evidence (run_summary.json or successful benchmark_window_cap_summary.json)",
      );
      const fetchingEntry = result.startupFirstEmittedRows.find((row) => row.approach === "fetching");
      expect(fetchingEntry.startupValid).toBe(false);
      expect(fetchingEntry.runCompleted).toBe(false);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted reports latency domain mismatch as a failure separate from final-window diagnostics", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-first-emitted-domain-mismatch-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3],
        finalWindowNumbers: [1, 2, 3],
        targetWindowCount: 5,
        emittedFinalWindowCount: 3,
        stoppedAfterTargetWindows: false,
        stopReason: "other",
        closeToResultMsByWindow: { 1: 410, 2: 430 },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3],
        finalWindowNumbers: [1, 2, 3],
        targetWindowCount: 5,
        emittedFinalWindowCount: 3,
        stoppedAfterTargetWindows: false,
        stopReason: "other",
        closeToResultMsByWindow: { 1: 460, 2: 480 },
        latencyDomainStatus: "domain_mismatch",
        valueOffset: 2,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 410, 2: 435 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const approximationEntry = result.startupFirstEmittedRows.find((row) => row.approach === "approximation");

      expect(approximationEntry.startupValid).toBe(false);
      expect(approximationEntry.startupLatencyFailureReason).toBe("latency_domain_mismatch");
      expect(result.errors).toContain("approximation/iteration1: first usable row has invalid comparable startup latency (latency_domain_mismatch)");
      expect(result.warnings).toContain("approximation/iteration1: diagnostic stop reason is other; startup-first-emitted only requires a usable first row");
      expect(result.warnings).toContain("approximation/iteration1: diagnostic final windows 1,2,3 did not reach the target-window upper bound 5");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted reconstructs fetching wall-clock mapped latency from window_end instead of using direct trigger latency", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-fetching-direct-window-close-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        latencyDomainStatus: "",
        wallClockCloseToResultMsByWindow: { 1: "invalid" },
        expectedWindowCloseByWindow: { 1: 121480 },
        logicalTriggerTimeByWindow: { 1: 60520 },
        windowDataCloseTimeByWindow: { 1: 60520 },
        windowEndByWindow: { 1: 120520 },
        resultEmittedAtByWindow: { 1: 62910 },
        latencyFromLogicalTriggerMsByWindow: { 1: 0 },
        latencyFromWindowCloseMsByWindow: { 1: 0 },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2263 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2259 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const fetchingEntry = result.startupFirstEmittedRows.find((row) => row.approach === "fetching");
      expect(fetchingEntry.startupLatencyMs).toBe(2390);
      expect(fetchingEntry.startupLatencySource).toBe("resultEmittedAtMinusWindowEnd");
      expect(fetchingEntry.latencyDomainStatus).toBe("wall_clock_mapped");
      expect(fetchingEntry.startupValid).toBe(true);
      expect(result.errors).toEqual([]);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted keeps approximation and chunked wall-clock mapped selection unchanged", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-wall-clock-selection-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 0 },
        latencyDomainStatus: "",
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2263 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2259 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const approximationEntry = result.startupFirstEmittedRows.find((row) => row.approach === "approximation");
      const chunkedEntry = result.startupFirstEmittedRows.find((row) => row.approach === "chunked");

      expect(approximationEntry.startupLatencyMs).toBe(2263);
      expect(approximationEntry.startupLatencySource).toBe("wallClockCloseToResultMs");
      expect(approximationEntry.latencyDomainStatus).toBe("wall_clock_mapped");
      expect(chunkedEntry.startupLatencyMs).toBe(2259);
      expect(chunkedEntry.startupLatencySource).toBe("wallClockCloseToResultMs");
      expect(chunkedEntry.latencyDomainStatus).toBe("wall_clock_mapped");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-first-emitted rejects rows with only registration-anchored fallback and no valid comparable close-to-result latency", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-helper-invalid-comparable-startup-"));

    try {
      writeIteration(tempRoot, "fetching", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        latencyDomainStatus: "",
        wallClockCloseToResultMsByWindow: { 1: "invalid" },
        resultEmittedAtByWindow: { 1: 62910 },
        expectedWindowCloseByWindow: { 1: 121480 },
        logicalTriggerTimeByWindow: { 1: 121480 },
        windowDataCloseTimeByWindow: { 1: 121480 },
        windowEndByWindow: { 1: 121480 },
        latencyFromLogicalTriggerMsByWindow: { 1: -58570 },
        latencyFromWindowCloseMsByWindow: { 1: -58570 },
      });
      writeIteration(tempRoot, "approximation", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2263 },
        valueOffset: 1,
      });
      writeIteration(tempRoot, "chunked", 1, {
        windowNumbers: [1, 2, 3, 4, 5],
        closeToResultMsByWindow: { 1: 2259 },
      });

      const result = analyzeRealDataResults({
        mode: "startup-first-emitted",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 5,
      });

      const fetchingEntry = result.startupFirstEmittedRows.find((row) => row.approach === "fetching");
      expect(fetchingEntry.startupValid).toBe(false);
      expect(fetchingEntry.startupLatencyMs).toBeNull();
      expect(fetchingEntry.startupLatencyFailureReason).toBe("missing_comparable_close_to_result_latency");
      expect(result.errors).toContain("fetching/iteration1: first usable row has invalid comparable startup latency (missing_comparable_close_to_result_latency)");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
