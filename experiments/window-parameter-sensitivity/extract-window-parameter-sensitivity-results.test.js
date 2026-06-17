const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  applyValidation,
  buildAggregateRows,
  buildPerRunRows,
  computeErrorStats,
  loadRunRecords,
} = require("./extract-window-parameter-sensitivity-results");

describe("window-parameter-sensitivity extractor", () => {
  test("computeErrorStats reports zero error for fetching baseline", () => {
    const stats = computeErrorStats(
      {
        approach: "fetching",
        emitted_window_count: 3,
      },
      null,
    );

    expect(stats).toEqual({
      mean_error: 0,
      mae: 0,
      rmse: 0,
      mape: 0,
      max_abs_error: 0,
      matched_window_count: 3,
    });
  });

  test("applyValidation marks zero emitted windows invalid", () => {
    const records = [
      {
        experiment_name: "superquery-range-scaling",
        approach: "fetching",
        pattern: "low_variability",
        iteration: 1,
        superquery_range_seconds: 120,
        superquery_step_seconds: 60,
        chunk_size_seconds: 30,
        success: true,
        process_cleanup_ok: true,
        exact_final_reuse_enabled: false,
        exact_final_result_reuse_hits: 0,
        rsp_engines_created: 0,
        emitted_window_count: 0,
        emitted_result_count: 0,
        result_values_by_window: new Map(),
      },
    ];

    applyValidation(records);

    expect(records[0].is_valid).toBe(false);
    expect(records[0].validity_reason).toContain("no_result_windows_emitted");
  });

  test("per-result metrics stay blank when emitted count is zero", () => {
    const rows = buildPerRunRows([
      {
        experiment_name: "superquery-range-scaling",
        approach: "fetching",
        pattern: "low_variability",
        iteration: 1,
        aggregation_function: "AVG",
        superquery_range_seconds: 120,
        superquery_step_seconds: 60,
        chunk_size_seconds: 30,
        replay_duration_seconds: 240,
        exact_final_reuse_enabled: false,
        chunk_size_applies_to_approach: false,
        cpu_seconds: 10,
        cpu_seconds_per_emitted_result: null,
        peak_rss_mb: 100,
        peak_rss_mb_per_emitted_result: null,
        mean_rss_mb: 80,
        mean_window_adjusted_latency_ms: null,
        std_window_adjusted_latency_ms: null,
        median_window_adjusted_latency_ms: null,
        p95_window_adjusted_latency_ms: null,
        mean_ready_to_emit_ms: null,
        std_ready_to_emit_ms: null,
        median_ready_to_emit_ms: null,
        p95_ready_to_emit_ms: null,
        mean_computation_ms: null,
        std_computation_ms: null,
        median_computation_ms: null,
        p95_computation_ms: null,
        mean_error: null,
        mae: null,
        rmse: null,
        mape: null,
        max_abs_error: null,
        chunk_state_messages_published: 0,
        chunk_state_messages_per_emitted_result: null,
        shared_chunk_producers_created: 0,
        fallback_original_agent_rsps_started: 0,
        reconstructed_superquery_results: 0,
        rsp_engines_created: 0,
        mqtt_clients_created: 0,
        compatible_queries_detected: 0,
        original_agent_rsps_skipped: 0,
        original_agent_outputs_derived_from_chunks: 0,
        exact_final_result_reuse_hits: 0,
        emitted_result_count: 0,
        reconstructed_result_count: 0,
        superquery_result_count: 0,
        emitted_window_count: 0,
        matched_window_count: 0,
        chunk_states_consumed_per_result_mean: null,
        chunk_states_consumed_per_emitted_result: null,
        expected_chunk_states_per_result: 8,
        expected_chunk_states_total: 0,
        success: true,
        is_valid: false,
        process_cleanup_ok: true,
        validity_reason: "no_result_windows_emitted",
        log_dir: "/tmp/run",
      },
    ]);

    expect(rows[0].cpu_seconds_per_emitted_result).toBeNull();
    expect(rows[0].peak_rss_mb_per_emitted_result).toBeNull();
    expect(rows[0].chunk_state_messages_per_emitted_result).toBeNull();
  });

  test("chunk size applicability is false for fetching and true for chunked", () => {
    const aggregateRows = buildAggregateRows([
      {
        experiment_name: "chunk-granularity-sensitivity",
        approach: "fetching",
        pattern: "low_variability",
        aggregation_function: "AVG",
        superquery_range_seconds: 120,
        superquery_step_seconds: 60,
        chunk_size_seconds: 30,
        replay_duration_seconds: 240,
        exact_final_reuse_enabled: false,
        chunk_size_applies_to_approach: false,
        success: true,
        is_valid: true,
      },
      {
        experiment_name: "chunk-granularity-sensitivity",
        approach: "chunked",
        pattern: "low_variability",
        aggregation_function: "AVG",
        superquery_range_seconds: 120,
        superquery_step_seconds: 60,
        chunk_size_seconds: 30,
        replay_duration_seconds: 240,
        exact_final_reuse_enabled: false,
        chunk_size_applies_to_approach: true,
        success: true,
        is_valid: true,
      },
    ]);

    const fetching = aggregateRows.find((row) => row.approach === "fetching");
    const chunked = aggregateRows.find((row) => row.approach === "chunked");

    expect(fetching.chunk_size_applies_to_approach).toBe(false);
    expect(chunked.chunk_size_applies_to_approach).toBe(true);
  });

  test("fetching leaves chunk consumption blank while chunked reports it", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "window-parameter-sensitivity-"));

    try {
      const baseRun = {
        experiment_name: "superquery-range-scaling",
        pattern: "low_variability",
        iteration: 1,
        aggregation_function: "AVG",
        superquery_range_seconds: 120,
        superquery_step_seconds: 60,
        chunk_size_seconds: 30,
        replay_duration_seconds: 240,
        exact_final_reuse_enabled: false,
        process_cleanup_ok: true,
        success: true,
      };

      const sharedResourceSummary = {
        cpuSeconds: 12.5,
        peakRssMb: 256,
        meanRssMb: 192,
        wallTimeSec: 30,
        peakCpuPct: 78,
        peakProcessCount: 4,
      };

      const sharedProfileSummary = {
        summedCounters: {
          emitted_results: 1,
          chunk_state_messages_published: 0,
          shared_chunk_producers_created: 0,
          fallback_original_agent_rsps_started: 0,
          reconstructed_superquery_results: 0,
          rsp_engines_created: 0,
          mqtt_clients_created: 0,
          compatible_queries_detected: 0,
          original_agent_rsps_skipped: 0,
          original_agent_outputs_derived_from_chunks: 0,
          exact_final_result_reuse_hits: 0,
          chunk_consumers_registered: 0,
          chunk_groups_completed: 0,
          comparable_windows_emitted: 0,
        },
      };

      const writeRun = (approach, extraFiles = {}) => {
        const runDir = path.join(
          tempRoot,
          approach,
          "low_variability",
          "range-120s",
          "iteration1",
        );
        fs.mkdirSync(runDir, { recursive: true });
        fs.writeFileSync(
          path.join(runDir, "run_metadata.json"),
          `${JSON.stringify({ ...baseRun, approach }, null, 2)}\n`,
        );
        fs.writeFileSync(
          path.join(runDir, "resource_summary.json"),
          `${JSON.stringify(sharedResourceSummary, null, 2)}\n`,
        );
        fs.writeFileSync(
          path.join(runDir, "hive_profile_summary.aggregate.json"),
          `${JSON.stringify(sharedProfileSummary, null, 2)}\n`,
        );
        fs.writeFileSync(
          path.join(runDir, `${approach}_latency_log.csv`),
          [
            "window_number,result_value,delay_past_expected_close_ms,ready_to_emit_ms,computation_ms,delay_past_last_obs_ms,expected_window_close,result_emitted_at",
            "1,42,5,6,7,7,0,0",
          ].join("\n"),
        );

        for (const [fileName, fileContents] of Object.entries(extraFiles)) {
          fs.writeFileSync(path.join(runDir, fileName), fileContents);
        }
      };

      writeRun("fetching");
      writeRun(
        "chunked",
        {
          "chunked_emission_proof.json": JSON.stringify(
            [
              {
                receivedChunksUsedBySubquery: {
                  wearableX: [1, 2, 3, 4],
                  smartphoneX: [5, 6, 7, 8],
                },
              },
            ],
            null,
            2,
          ),
        },
      );

      const records = loadRunRecords({
        experimentName: "superquery-range-scaling",
        inputRoot: tempRoot,
        outputDir: path.join(tempRoot, "out"),
        patterns: ["low_variability"],
        approaches: ["fetching", "chunked"],
        ranges: [],
        chunkSizes: [],
      });

      const fetching = records.find((record) => record.approach === "fetching");
      const chunked = records.find((record) => record.approach === "chunked");

      expect(fetching.expected_chunk_states_per_result).toBe(8);
      expect(fetching.chunk_states_consumed_per_emitted_result).toBeNull();
      expect(chunked.expected_chunk_states_per_result).toBe(8);
      expect(chunked.chunk_states_consumed_per_emitted_result).toBe(8);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
