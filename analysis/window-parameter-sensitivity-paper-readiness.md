# Window-Parameter Sensitivity Paper Readiness Audit

This document presents the audit results for the window-parameter-sensitivity experiments after the recent low-risk wiring changes.

## 1. Diff Inspection of Experiments Directory

The current modifications in [experiments/window-parameter-sensitivity/](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/) have been reviewed:
- [common.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.js): Added helper methods, scenario anchors, default target configurations, and environment mappings for the target-scaling experiment.
- [common.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.test.js): Verified test coverage for scenario configuration builders.
- [extract-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js): Updated result extraction fields to track window-completeness counters, trimmed ranges, and reconstruction errors.
- [extract-window-parameter-sensitivity-results.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js): Verified extraction correctness.
- [plot-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js): Standardized plotting options.
- [run-window-parameter-sensitivity.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js): Integrated query-target-scaling scenarios and bounded-run success classification logic.
- [run-window-parameter-sensitivity.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js): Verified classification functionality.

## 2. Bounded-Run Failure Classification Fix Verification

The bounded-run failure classification fix has been verified in [run-window-parameter-sensitivity.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js#L513-L576). The function `classifyBoundedSuccess` correctly overrides unexpected exits to success when:
- The orchestrator exit is triggered by SIGTERM or exit code 143 after successfully outputting windows.
- The `benchmark_window_cap_summary.json` has `stoppedAfterTargetWindows: true`.
- At least one expected window result has been emitted.

## 3. Paper-Mode Flags Verification

The shared scenario configuration builder in [common.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.js) successfully configures the following paper-mode environment flags:
- target windows 35: `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: "35"`
- trim 4..33: `DEFAULT_PAPER_TRIMMED_WINDOW_START = 4`, `DEFAULT_PAPER_TRIMMED_WINDOW_END = 33`
- compact reusable payload: `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD: "1"`
- completed-window approximation: `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE: "1"` and `STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE: "0"`
- chunked semantic-ready immediate mode: `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER: "1"`, `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY: "0"`, and `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY: "0"`

## 4. Extractor Outputs Verification

The results extractor defined in [extract-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js#L950-L959) correctly populates all required metrics:
- `expected_window_count`
- `actual_window_count`
- `missing_window_count`
- `extra_window_count`
- `matched_window_count`
- trimmed equivalents:
  - `trimmed_expected_window_count`
  - `trimmed_matched_window_count`
  - `trimmed_missing_window_count`
  - `trimmed_extra_window_count`
  - `trimmed_actual_window_count`
- `cpu_seconds`
- correctness metrics: `mae`, `mape`, and `rmse`

## 5. Focused Test Results

All 19 test cases targeting the window parameter sensitivity runner, extractor, and helper logic pass successfully:
- [common.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.test.js): PASS
- [extract-window-parameter-sensitivity-results.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js): PASS
- [run-window-parameter-sensitivity.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js): PASS

## 6. Smoke Run Verification

A bounded smoke run was successfully completed for the `low_variability` pattern with `fetching` and `chunked` approaches (1 iterations, superquery-range-scaling, range-120s, duration 240s):
- `chunked`: Success (valid: true, emittedCount: 2)
- `fetching`: Success (valid: true, emittedCount: 2)
- All generated CSV outputs were successfully saved to [results/window-parameter-sensitivity/superquery-range-scaling/](file:///Users/kushbisen/Code/streaming-query-hive/results/window-parameter-sensitivity/superquery-range-scaling/).

## 7. Go/No-Go and Commits Plan

- Recommendation: GO
- Blockers: None
- Files to commit:
  - [experiments/window-parameter-sensitivity/README.md](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/README.md)
  - [experiments/window-parameter-sensitivity/common.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.js)
  - [experiments/window-parameter-sensitivity/common.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.test.js)
  - [experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js)
  - [experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js)
  - [experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js)
  - [experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js)
  - [experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js)
  - [analysis/window-parameter-sensitivity-paper-readiness.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/window-parameter-sensitivity-paper-readiness.md)
- Files to leave out:
  - All other unstaged files or modifications in core directories, scripts, and other experiment folders (e.g. frequency-comparison, k-scaling, real-data-comparison, src/, etc.).
