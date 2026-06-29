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
      expected_window_close: windowEnd,
      result_emitted_at: windowEnd + closeToResultMs,
      window_semantics: "trailing",
      logical_trigger_time: windowEnd,
      window_start: windowEnd - 120000,
      window_end: windowEnd,
      window_data_close_time: windowEnd,
      latency_from_logical_trigger_ms: closeToResultMs,
      latency_from_window_close_ms: closeToResultMs,
      latency_domain_status: options.latencyDomainStatus || "wall_clock_mapped",
      wall_clock_close_to_result_ms: closeToResultMs,
      metadata_source: "direct",
      result_value: (windowNumber * 10) + (options.valueOffset || 0),
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
      targetWindowCount: options.windowNumbers.length,
      emittedFinalWindowCount: options.windowNumbers.length,
      finalWindowNumbers: options.windowNumbers,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      approach: approachName,
    }, null, 2)}\n`,
  );

  fs.writeFileSync(
    path.join(iterationDir, "run_summary.json"),
    `${JSON.stringify({
      completionStatus: "completed",
      publisherExitReason: "finite_replay_duration_reached",
      failedPublishes: 0,
    }, null, 2)}\n`,
  );

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
});
