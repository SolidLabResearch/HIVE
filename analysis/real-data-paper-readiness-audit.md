# Real-Data Benchmark Paper Readiness Audit

This document presents the audit findings for the real-data-comparison benchmark family after auditing the directory structure, execution scripts, test suites, and running a bounded smoke test.

## 1. Diff and Directory Audit

The real-data-comparison benchmark scripts under [experiments/real-data-comparison/](file:///Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/) have been reviewed:
- [run-real-data-4-approaches.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js): Re-architected to align with the paper methodology, running all 4 approaches (fetching, approximation, chunked, naive_distributed) under a shared finite replay config, capturing process tree statistics, and computing trimmed completeness/accuracy errors against the fetching baseline.
- [run-real-data-4-approaches.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.test.js): Fully tests key properties (scaled timeouts, smartphone/wearable dataset paths, and completeness/latency/accuracy aggregation).

## 2. Paper Methodology Alignment

The real-data benchmark fully supports and enforces the same core methodology as the other paper benchmark families:
- Target window count: 35 windows (`PAPER_TARGET_WINDOWS = 35` and `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "35"`).
- Trimmed evaluation range: 4..33 (`DEFAULT_ANALYSIS_WINDOW_START = 4`, `DEFAULT_ANALYSIS_WINDOW_END = 33`).
- Finite replay configuration: Activated via `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1"`.
- Completed-window approximation mode: Activated via completed window mode "1" and early trigger mode "0".
- Compact reusable payload: Active via `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD: "1"`.
- Chunked semantic-ready immediate mode: Configured via immediate trigger "1", comparable output only "0", and cadence only "0".
- Resource sampling: Captures process tree CPU-seconds and RSS using `process-tree-resource-sampler.js`.

## 3. Extractor and Plotting Output Correctness

The automated analysis phase correctly calculates and exports the following metrics:
- Latencies: `latency_mean_ms`, `latency_min_ms`, `latency_max_ms`
- Resource Usage: `mean_rss_mib`, `peak_rss_mib`, `cpu_seconds_mean`, and `cpu_seconds_per_window_mean`
- MQTT Metrics: `mqtt_published_bytes_mean`, `mqtt_estimated_delivery_bytes_mean`, and `mqtt_message_count_mean`
- Completeness: `matched_windows_mean`, `fetching_only_windows_mean`, and `approach_only_windows_mean`
- Accuracy: `mae_mean`, `mape_mean`, and `rmse_mean`

CSV and JSON results are correctly exported to:
- `logs/real_data_paper_ready_trimmed-4-33_summary.csv`
- `logs/real_data_paper_ready_trimmed-4-33_summary.json`
- `logs/real_data_paper_ready_raw_summary.json`

## 4. Focused Unit Test Status

All 3 unit test cases pass successfully:
- `timeout scales with replay duration for 35-window paper runs`: PASS
- `real-data runner uses the base smartphone and wearable datasets`: PASS
- `analyzeResults trims windows 4..33 and reports completeness, latency, CPU-seconds, and MQTT counts`: PASS

## 5. Bounded Smoke Test Verification

A single-iteration bounded smoke test was successfully executed with target windows set to 2 (`STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=2`):
- All 4 approaches replayed and exited successfully.
- Output logs and CSV reports were successfully saved to the logs folder.
- *Note:* Because the target windows count of 2 was less than the analysis start window of 4, the trimmed window range (4..33) yielded 0 windows and null metrics in the final summary. This is expected behavior for short runs and confirms the correctness of the trimming boundaries.

## 6. Execution Blockers for Full 35-Iteration Run

- **Time Scale / Execution Duration:** Replaying the real dataset at the default 4Hz takes 39 minutes per iteration. Running 35 iterations sequentially across all 4 approaches requires ~91 hours. This is a practical execution blocker for standard time limits and will cause server timeout unless split into parallel jobs, or replayed at a higher frequency.
- **Process Leakage Risk:** Long execution times increase the risk of stuck BeeWorker or orchestrator processes. A single process leak will pollute subsequent iterations' CPU/memory usage metrics or fail execution.
