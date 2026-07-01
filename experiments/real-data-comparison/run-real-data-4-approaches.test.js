const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  APPROACHES,
  RealDataComparisonRunner,
  buildPaperConfig,
  computeRequiredEventTimeCoverageMs,
  computeReplayDurationSeconds,
  computeRunTimeoutMs,
  getApproachByName,
  hasSuccessfulTargetWindowCapSummary,
  isBoundedTargetStopExitAcceptable,
  parseCliArgs,
  prepareFreshIterationLogDir,
  readBenchmarkWindowSummary,
  resolveFiniteReplayDurationSeconds,
  shouldEnableFetchingStartupFirstEmitted,
  shouldStopPublisherAfterTargetReached,
  writeFallbackRunSummary,
} = require("./run-real-data-4-approaches.js");
const {
  attachComparableTiming,
} = require("../../scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js");

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

function writeText(filePath, content) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content);
}

describe("real-data paper-ready analysis", () => {
  test("timeout scales with replay duration for 35-window paper runs", () => {
    const paperConfig = buildPaperConfig(1);

    expect(computeRequiredEventTimeCoverageMs(paperConfig)).toBe(2160000);
    expect(computeReplayDurationSeconds(paperConfig)).toBe(2340);
    expect(computeRunTimeoutMs({}, paperConfig)).toBe(2460000);
    expect(computeRunTimeoutMs({ STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS: "12345" }, paperConfig)).toBe(12345);
  });

  test("5-window startup gates require 360 seconds of event-time coverage plus replay margin", () => {
    const paperConfig = buildPaperConfig(1, { targetWindowCount: 5 });

    expect(computeRequiredEventTimeCoverageMs(paperConfig)).toBe(360000);
    expect(computeReplayDurationSeconds(paperConfig)).toBe(540);
    expect(resolveFiniteReplayDurationSeconds({}, paperConfig)).toBe(540);
    expect(resolveFiniteReplayDurationSeconds({
      STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: "300",
    }, paperConfig)).toBe(540);
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
    expect(startupEnv.STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE).toBe("1");

    const steadyRunner = new RealDataComparisonRunner({
      iterations: 1,
      approachNames: ["fetching", "approximation", "chunked"],
      targetWindowCount: 35,
    });
    const steadyEnv = steadyRunner.buildRunEnv(APPROACHES[1], "/tmp/log", 1);
    expect(steadyEnv.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("35");
    expect(steadyEnv.STREAMING_QUERY_HIVE_FETCHING_STARTUP_FIRST_EMITTED_MODE).toBe("0");
  });

  test("startup-first-emitted helper only enables the fetching shortcut for bounded startup pilots", () => {
    expect(shouldEnableFetchingStartupFirstEmitted(1)).toBe(true);
    expect(shouldEnableFetchingStartupFirstEmitted(5)).toBe(true);
    expect(shouldEnableFetchingStartupFirstEmitted(6)).toBe(false);
    expect(shouldEnableFetchingStartupFirstEmitted(35)).toBe(false);
  });

  test("real-data runner uses the base smartphone and wearable datasets", () => {
    const runner = new RealDataComparisonRunner({ iterations: 1 });
    const env = runner.buildRunEnv(APPROACHES[0], "/tmp/log", 1);

    expect(env.DATA_PATH).toBe(".");
  });

  test("buildRunEnv refreshes benchmark replay anchors per approach iteration run", () => {
    const runner = new RealDataComparisonRunner({ iterations: 1 });
    const originalNow = Date.now;
    let currentNow = 1000;
    Date.now = jest.fn(() => currentNow);

    try {
      const env1 = runner.buildRunEnv(APPROACHES[0], "/tmp/log-a", 1);
      currentNow = 9000;
      const env2 = runner.buildRunEnv(APPROACHES[1], "/tmp/log-b", 1);

      expect(env1.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME).toBe("1000");
      expect(env1.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR).toBe("1000");
      expect(env2.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME).toBe("9000");
      expect(env2.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR).toBe("9000");
    } finally {
      Date.now = originalNow;
    }
  });

  test("buildRunEnv isolates benchmark MQTT topics per approach iteration", () => {
    const runner = new RealDataComparisonRunner({
      iterations: 3,
      approachNames: ["fetching", "approximation", "chunked"],
      targetWindowCount: 5,
    });

    const iteration1Env = runner.buildRunEnv(APPROACHES[0], "/tmp/log-1", 1);
    const iteration2Env = runner.buildRunEnv(APPROACHES[0], "/tmp/log-2", 2);
    const chunkedIteration3Env = runner.buildRunEnv(APPROACHES[2], "/tmp/log-3", 3);

    expect(iteration1Env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX).toBe(
      "real-data-paper-ready/fetching/iteration1",
    );
    expect(iteration2Env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX).toBe(
      "real-data-paper-ready/fetching/iteration2",
    );
    expect(chunkedIteration3Env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX).toBe(
      "real-data-paper-ready/chunked/iteration3",
    );
  });

  test("publisher-stop helper recognizes a successful target-window summary", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-window-summary-"));

    try {
      fs.writeFileSync(
        path.join(tempRoot, "benchmark_window_cap_summary.json"),
        `${JSON.stringify({
          targetWindowCount: 5,
          emittedFinalWindowCount: 5,
          finalWindowNumbers: [1, 2, 3, 4, 5],
          stoppedAfterTargetWindows: true,
          stopReason: "target_window_count_reached",
          approach: "approximation",
        }, null, 2)}\n`,
      );

      expect(readBenchmarkWindowSummary(tempRoot)).toMatchObject({
        stoppedAfterTargetWindows: true,
        stopReason: "target_window_count_reached",
      });
      expect(shouldStopPublisherAfterTargetReached(tempRoot)).toBe(true);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("publisher-stop helper ignores missing or non-target summaries", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-window-summary-miss-"));

    try {
      expect(readBenchmarkWindowSummary(tempRoot)).toBeNull();
      expect(shouldStopPublisherAfterTargetReached(tempRoot)).toBe(false);

      fs.writeFileSync(
        path.join(tempRoot, "benchmark_window_cap_summary.json"),
        `${JSON.stringify({
          stoppedAfterTargetWindows: false,
          stopReason: "other",
        }, null, 2)}\n`,
      );

      expect(shouldStopPublisherAfterTargetReached(tempRoot)).toBe(false);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("bounded target-stop exit helper accepts SIGTERM only after a successful cap summary", () => {
    expect(isBoundedTargetStopExitAcceptable(0, null, false)).toBe(true);
    expect(isBoundedTargetStopExitAcceptable(null, "SIGTERM", false)).toBe(false);
    expect(isBoundedTargetStopExitAcceptable(null, "SIGTERM", true)).toBe(true);
    expect(isBoundedTargetStopExitAcceptable(0, null, true)).toBe(true);
    expect(isBoundedTargetStopExitAcceptable(1, null, true)).toBe(false);
  });

  test("bounded target-stop writes a completed fallback run summary when the publisher summary is missing", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-fallback-summary-"));

    try {
      const logDir = path.join(tempRoot, "fetching", "iteration1");
      fs.mkdirSync(logDir, { recursive: true });

      const summary = writeFallbackRunSummary(logDir, {
        emittedFinalWindowCount: 5,
        targetWindowCount: 5,
        stopReason: "target_window_count_reached",
        stoppedAfterTargetWindows: true,
        finalWindowNumbers: [1, 2, 3, 4, 5],
      });

      expect(summary.completionStatus).toBe("completed");
      expect(summary.publisherExitReason).toBe("target_window_count_reached");
      expect(summary.emittedFinalWindowCount).toBe(5);
      expect(summary.finalWindowNumbers).toEqual([1, 2, 3, 4, 5]);

      const writtenSummary = JSON.parse(fs.readFileSync(path.join(logDir, "run_summary.json"), "utf8"));
      expect(writtenSummary.completionStatus).toBe("completed");
      expect(writtenSummary.stopReason).toBe("target_window_count_reached");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("successful target-window cap summaries are treated as completed bounded runs", () => {
    expect(hasSuccessfulTargetWindowCapSummary({
      targetWindowCount: 5,
      emittedFinalWindowCount: 5,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
    })).toBe(true);

    expect(hasSuccessfulTargetWindowCapSummary({
      targetWindowCount: 5,
      emittedFinalWindowCount: 4,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
    })).toBe(false);

    expect(hasSuccessfulTargetWindowCapSummary({
      targetWindowCount: 5,
      emittedFinalWindowCount: 5,
      stoppedAfterTargetWindows: false,
      stopReason: "target_window_count_reached",
    })).toBe(false);
  });

  test("bounded target-stop fallback summaries can be written for multiple iterations", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-fallback-summary-multi-"));

    try {
      const benchmarkSummary = {
        emittedFinalWindowCount: 5,
        targetWindowCount: 5,
        stopReason: "target_window_count_reached",
        stoppedAfterTargetWindows: true,
        finalWindowNumbers: [1, 2, 3, 4, 5],
      };

      for (const iteration of [1, 2]) {
        const logDir = path.join(tempRoot, "fetching", `iteration${iteration}`);
        fs.mkdirSync(logDir, { recursive: true });
        writeFallbackRunSummary(logDir, benchmarkSummary);
      }

      for (const iteration of [1, 2]) {
        const writtenSummary = JSON.parse(
          fs.readFileSync(path.join(tempRoot, "fetching", `iteration${iteration}`, "run_summary.json"), "utf8"),
        );
        expect(writtenSummary.completionStatus).toBe("completed");
        expect(writtenSummary.stopReason).toBe("target_window_count_reached");
        expect(writtenSummary.finalWindowNumbers).toEqual([1, 2, 3, 4, 5]);
      }
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("prepareFreshIterationLogDir removes stale files only from the targeted iteration directory", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-clean-"));

    try {
      const targetDir = path.join(tempRoot, "approximation", "iteration1");
      const siblingIterationDir = path.join(tempRoot, "approximation", "iteration2");
      const siblingApproachDir = path.join(tempRoot, "fetching", "iteration1");

      writeText(path.join(targetDir, "approximation_latency_log.csv"), "stale-latency\n");
      writeText(path.join(targetDir, "mqtt_traffic_summary.json"), "{\"stale\":true}\n");
      writeText(path.join(siblingIterationDir, "keep.txt"), "keep-iteration2\n");
      writeText(path.join(siblingApproachDir, "keep.txt"), "keep-fetching\n");

      prepareFreshIterationLogDir(targetDir);

      expect(fs.existsSync(targetDir)).toBe(true);
      expect(fs.readdirSync(targetDir)).toEqual([]);
      expect(fs.readFileSync(path.join(siblingIterationDir, "keep.txt"), "utf8")).toBe("keep-iteration2\n");
      expect(fs.readFileSync(path.join(siblingApproachDir, "keep.txt"), "utf8")).toBe("keep-fetching\n");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("prepareFreshIterationLogDir removes stale latency and MQTT summaries before a new iteration run", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-clean-stale-"));

    try {
      const targetDir = path.join(tempRoot, "approximation", "iteration1");
      writeText(path.join(targetDir, "approximation_latency_log.csv"), "old-row\n");
      writeText(path.join(targetDir, "mqtt_traffic_summary.json"), "{\"published_application_bytes\":123}\n");

      prepareFreshIterationLogDir(targetDir);

      expect(fs.existsSync(path.join(targetDir, "approximation_latency_log.csv"))).toBe(false);
      expect(fs.existsSync(path.join(targetDir, "mqtt_traffic_summary.json"))).toBe(false);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("prepareFreshIterationLogDir creates the iteration directory for a fresh run", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-clean-fresh-"));

    try {
      const targetDir = path.join(tempRoot, "chunked", "iteration1");
      expect(fs.existsSync(targetDir)).toBe(false);

      prepareFreshIterationLogDir(targetDir);

      expect(fs.existsSync(targetDir)).toBe(true);
      expect(fs.statSync(targetDir).isDirectory()).toBe(true);
      expect(fs.readdirSync(targetDir)).toEqual([]);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
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
      expect(analysis.paper.methodology.firstOutputTriggerOffsetMs).toBe(60000);
      expect(analysis.paper.methodology.steadyStateLatencyMetric).toBe(
        "result_emitted_at - mapped_output_trigger_wall_clock_ms",
      );

      expect(analysis.paper.byApproach.fetching.closeToResultLatencyMs.mean).toBe(500);
      expect(analysis.paper.byApproach.fetching.cpuSeconds.mean).toBe(7);
      expect(analysis.paper.byApproach.fetching.cpuSecondsPerWindow.mean).toBeCloseTo(0.2, 6);
      expect(analysis.paper.byApproach.fetching.mqttPublishedBytes.mean).toBe(1000);
      expect(analysis.paper.byApproach.fetching.mqttEstimatedDeliveryBytes.mean).toBe(2000);
      expect(analysis.paper.byApproach.fetching.mqttMessageCount.mean).toBe(4);
      expect(analysis.paper.byApproach.fetching.outputTriggerCadence.trimmed.ok).toBe(true);
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
      expect(analysis.paper.byApproach.approximation.outputTriggerCadence.trimmed.ok).toBe(false);
      expect(analysis.paper.byApproach.approximation.outputTriggerCadence.trimmed.issues).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            iteration: 1,
            type: "window_gap",
            previousWindowNumber: 19,
            currentWindowNumber: 21,
            windowDelta: 2,
          }),
        ]),
      );
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("fallback comparable timing maps window 1 to the first output step tick", () => {
    const rows = attachComparableTiming(
      [
        {
          windowNumber: 1,
          resultEmittedAt: 61500,
          latencyFromWindowCloseMs: null,
          latencyDomainStatus: "",
          wallClockCloseToResultMs: null,
        },
        {
          windowNumber: 2,
          resultEmittedAt: 121500,
          latencyFromWindowCloseMs: null,
          latencyDomainStatus: "",
          wallClockCloseToResultMs: null,
        },
      ],
      {
        csvRows: [
          { timestamp: "0", messageType: "raw_input_stream", warmup: "false" },
        ],
        firstTimestampsByType: {
          raw_input_stream: 0,
        },
      },
    );

    expect(rows[0].mappedOutputTriggerWallClockMs).toBe(60000);
    expect(rows[0].anchorAlignedExpectedWindowClose).toBe(60000);
    expect(rows[0].anchorAlignedWindowCloseToResultMs).toBe(1500);
    expect(rows[1].mappedOutputTriggerWallClockMs).toBe(120000);
    expect(rows[1].anchorAlignedWindowCloseToResultMs).toBe(1500);
  });
});
