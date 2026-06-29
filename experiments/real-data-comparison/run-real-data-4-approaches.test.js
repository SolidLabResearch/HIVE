const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  APPROACHES,
  RealDataComparisonRunner,
  buildPaperConfig,
  computeReplayDurationSeconds,
  computeRunTimeoutMs,
  getApproachByName,
  parseCliArgs,
  resolveFiniteReplayDurationSeconds,
} = require("./run-real-data-4-approaches.js");

function writeCsv(filePath, headers, rows) {
  const body = rows.map((row) => headers.map((header) => row[header] ?? "").join(",")).join("\n");
  fs.writeFileSync(filePath, `${headers.join(",")}\n${body}\n`);
}

function buildLatencyRows(approachName, options = {}) {
  const rows = [];
  const omittedWindows = new Set(options.omittedWindows || []);
  const windowNumbers = options.windowNumbers || Array.from({ length: 35 }, (_, index) => index + 1);
  const lastObservedField = approachName === "approximation"
    ? "last_data_received_at"
    : approachName === "chunked"
      ? "last_chunk_received_at"
      : "last_obs_received_at";

  for (const windowNumber of windowNumbers) {
    if (omittedWindows.has(windowNumber)) {
      continue;
    }

    const windowEnd = 1000 + 120000 + ((windowNumber - 1) * 60000);
    const resultValue = (windowNumber * 10) + (options.valueOffset || 0);
    const row = {
      window_number: windowNumber,
      query_registered_at: 1000,
      first_data_received_at: 1100,
      expected_window_close: windowEnd,
      result_emitted_at: windowEnd + (options.closeToResultMs || 500),
      window_semantics: "trailing",
      logical_trigger_time: windowEnd,
      window_start: windowEnd - 120000,
      window_end: windowEnd,
      window_data_close_time: windowEnd,
      latency_from_logical_trigger_ms: options.closeToResultMs || 500,
      latency_from_window_close_ms: options.closeToResultMs || 500,
      latency_domain_status: "wall_clock_mapped",
      wall_clock_close_to_result_ms: options.closeToResultMs || 500,
      metadata_source: "direct",
      result_value: resultValue,
    };
    row[lastObservedField] = windowEnd - 100;
    rows.push(row);
  }

  return rows;
}

function writeApproachFixtures(rootDir, approachName, options = {}) {
  const approach = APPROACHES.find((entry) => entry.name === approachName);
  const iterationDir = path.join(rootDir, approachName, "iteration1");
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
    buildLatencyRows(approachName, options),
  );

  writeCsv(
    path.join(iterationDir, approach.processTreeFile),
    ["timestamp", "tree_rss_bytes", "tree_cpu_seconds"],
    [
      {
        timestamp: 1000,
        tree_rss_bytes: 100 * 1024 * 1024,
        tree_cpu_seconds: 0,
      },
      {
        timestamp: 2000,
        tree_rss_bytes: 120 * 1024 * 1024,
        tree_cpu_seconds: options.cpuSeconds ?? 7,
      },
    ],
  );

  writeCsv(
    path.join(iterationDir, "mqtt_traffic.csv"),
    ["timestamp", "messageType", "warmup"],
    [
      { timestamp: 1000, messageType: "raw_input_stream", warmup: "false" },
      { timestamp: 1100, messageType: "raw_input_stream", warmup: "false" },
      { timestamp: 1200, messageType: "superquery_result", warmup: "false" },
      { timestamp: 1300, messageType: "control", warmup: "false" },
    ],
  );

  fs.writeFileSync(
    path.join(iterationDir, "mqtt_traffic_summary.json"),
    `${JSON.stringify({
      published_application_bytes: options.publishedBytes ?? 1000,
      estimated_delivery_bytes: options.estimatedDeliveryBytes ?? 2000,
    }, null, 2)}\n`,
  );

  return iterationDir;
}

describe("real-data paper-ready analysis", () => {
  test("timeout scales with replay duration for 35-window paper runs", () => {
    const paperConfig = buildPaperConfig(1);

    expect(computeReplayDurationSeconds(paperConfig)).toBe(2340);
    expect(computeRunTimeoutMs({}, paperConfig)).toBe(2460000);
    expect(computeRunTimeoutMs({ STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS: "12345" }, paperConfig)).toBe(12345);
  });

  test("real-data runner ignores inherited finite replay durations that are shorter than required", () => {
    const paperConfig = buildPaperConfig(1);

    expect(resolveFiniteReplayDurationSeconds({}, paperConfig)).toBe(2340);
    expect(resolveFiniteReplayDurationSeconds({
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: "300",
    }, paperConfig)).toBe(2340);
    expect(resolveFiniteReplayDurationSeconds({
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: "2500",
    }, paperConfig)).toBe(2500);
  });

  test("parseCliArgs accepts the plural approach filter for the three paper-ready approaches", () => {
    expect(parseCliArgs([
      "--iterations",
      "3",
      "--approaches",
      "fetching,approximation,chunked",
    ])).toEqual({
      analyzeOnly: false,
      approachNames: ["fetching", "approximation", "chunked"],
      iterations: 3,
      targetWindowCount: null,
    });

    const runner = new RealDataComparisonRunner({
      iterations: 3,
      approachNames: ["fetching", "approximation", "chunked"],
    });
    expect(runner.selectedApproaches.map((approach) => approach.name)).toEqual([
      "fetching",
      "approximation",
      "chunked",
    ]);
    expect(runner.selectedApproaches.some((approach) => approach.name === "naive_distributed")).toBe(false);
    expect(getApproachByName("approximation")?.label).toBe("Approximation");
  });

  test("parseCliArgs keeps the singular approach flag as a compatibility alias", () => {
    expect(parseCliArgs(["--iterations", "1", "--approach", "approximation"])).toEqual({
      analyzeOnly: false,
      approachNames: ["approximation"],
      iterations: 1,
      targetWindowCount: null,
    });
  });

  test("parseCliArgs accepts target-window counts of 1 and 35", () => {
    expect(parseCliArgs(["--iterations", "2", "--target-windows", "1"])).toEqual({
      analyzeOnly: false,
      approachNames: null,
      iterations: 2,
      targetWindowCount: 1,
    });

    expect(parseCliArgs(["--iterations", "1", "--target-windows", "35"])).toEqual({
      analyzeOnly: false,
      approachNames: null,
      iterations: 1,
      targetWindowCount: 35,
    });
  });

  test("buildPaperConfig prefers an explicit target-window override and keeps the env fallback", () => {
    expect(buildPaperConfig(1, { targetWindowCount: 1 }).targetWindows).toBe(1);
    expect(buildPaperConfig(1, { targetWindowCount: 35 }).targetWindows).toBe(35);

    const original = process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS;
    process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS = "7";
    try {
      expect(buildPaperConfig(1).targetWindows).toBe(7);
    } finally {
      if (original === undefined) {
        delete process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS;
      } else {
        process.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS = original;
      }
    }
  });

  test("direct runner preserves selected approaches and target-window counts in run env", () => {
    const startupRunner = new RealDataComparisonRunner({
      iterations: 2,
      approachNames: ["fetching", "approximation", "chunked"],
      targetWindowCount: 1,
    });
    const startupEnv = startupRunner.buildRunEnv(APPROACHES[0], "/tmp/log", 1);
    expect(startupRunner.selectedApproaches.map((approach) => approach.name)).toEqual([
      "fetching",
      "approximation",
      "chunked",
    ]);
    expect(startupEnv.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("1");

    const steadyRunner = new RealDataComparisonRunner({
      iterations: 1,
      approachNames: ["fetching", "approximation", "chunked"],
      targetWindowCount: 35,
    });
    const steadyEnv = steadyRunner.buildRunEnv(APPROACHES[1], "/tmp/log", 1);
    expect(steadyEnv.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("35");
  });

  test("real-data runner uses the base smartphone and wearable datasets", () => {
    const runner = new RealDataComparisonRunner({ iterations: 1 });
    const env = runner.buildRunEnv(APPROACHES[0], "/tmp/log", 1);

    expect(env.DATA_PATH).toBe(".");
  });

  test("analyzeResults trims windows 4..33 and reports completeness, latency, CPU-seconds, and MQTT counts", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-paper-ready-"));

    try {
      const fetchingLogDir = writeApproachFixtures(tempRoot, "fetching");
      const approximationLogDir = writeApproachFixtures(tempRoot, "approximation", {
        omittedWindows: [20],
        valueOffset: 1,
        closeToResultMs: 700,
        cpuSeconds: 9,
        publishedBytes: 1400,
        estimatedDeliveryBytes: 2600,
      });
      const chunkedLogDir = writeApproachFixtures(tempRoot, "chunked", {
        closeToResultMs: 650,
        cpuSeconds: 8,
      });
      const naiveLogDir = writeApproachFixtures(tempRoot, "naive_distributed", {
        closeToResultMs: 800,
        cpuSeconds: 10,
      });

      const runner = new RealDataComparisonRunner({ iterations: 1 });
      runner.results = [
        { approach: "fetching", iteration: 1, success: true, logDir: fetchingLogDir },
        { approach: "approximation", iteration: 1, success: true, logDir: approximationLogDir },
        { approach: "chunked", iteration: 1, success: true, logDir: chunkedLogDir },
        { approach: "naive_distributed", iteration: 1, success: true, logDir: naiveLogDir },
      ];

      const analysis = runner.analyzeResults();

      expect(analysis.paper.methodology.targetWindows).toBe(35);
      expect(analysis.paper.methodology.analyzedWindows).toBe("4..33");

      expect(analysis.paper.byApproach.fetching.closeToResultLatencyMs.mean).toBe(500);
      expect(analysis.paper.byApproach.fetching.cpuSeconds.mean).toBe(7);
      expect(analysis.paper.byApproach.fetching.cpuSecondsPerWindow.mean).toBeCloseTo(0.2, 6);
      expect(analysis.paper.byApproach.fetching.mqttPublishedBytes.mean).toBe(1000);
      expect(analysis.paper.byApproach.fetching.mqttEstimatedDeliveryBytes.mean).toBe(2000);
      expect(analysis.paper.byApproach.fetching.mqttMessageCount.mean).toBe(4);
      expect(analysis.paper.byApproach.fetching.completeness.matchedWindowCount.mean).toBe(30);
      expect(analysis.paper.byApproach.fetching.accuracy.mae.mean).toBe(0);
      expect(analysis.paper.byApproach.fetching.accuracy.rmse.mean).toBe(0);

      expect(analysis.paper.byApproach.approximation.closeToResultLatencyMs.mean).toBe(700);
      expect(analysis.paper.byApproach.approximation.completeness.matchedWindowCount.mean).toBe(29);
      expect(analysis.paper.byApproach.approximation.completeness.fetchingOnlyWindows.mean).toBe(1);
      expect(analysis.paper.byApproach.approximation.completeness.approachOnlyWindows.mean).toBe(0);
      expect(analysis.paper.byApproach.approximation.completeness.missingWindows.mean).toBe(1);
      expect(analysis.paper.byApproach.approximation.accuracy.mae.mean).toBe(1);
      expect(analysis.paper.byApproach.approximation.accuracy.rmse.mean).toBe(1);
      expect(analysis.paper.byApproach.approximation.cpuSeconds.mean).toBe(9);
      expect(analysis.paper.byApproach.approximation.cpuSecondsPerWindow.mean).toBeCloseTo(9 / 34, 6);
      expect(analysis.paper.byApproach.approximation.mqttMessageCount.mean).toBe(4);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
