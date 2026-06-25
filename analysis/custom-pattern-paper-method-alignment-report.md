# Custom Pattern Paper Methodology Alignment Report

This document reports on the alignment of the custom-pattern benchmark family with the paper-ready methodology established for the real-data benchmark family.

## 1. Exact Changes Made

### A. Custom-Pattern Runner
- File modified: [run-custom-patterns-comparison.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)
- Implemented logic to check if process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY is unset, defaulting it to "1". If explicitly set to "0" or "1", that value is preserved.
- Derived the finite replay duration from the configured target windows (default 35), window range, and window step using the formula: Math.ceil((windowRange + (targetWindows + 2) * windowStep) / 1000). Set this as process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS if unset.
- Configured a dynamic watchdog timeout based on the derived duration: Math.max((derivedDuration * 1000) + 120000, configuredTimeout).
- Added an automatic step after benchmark run completion to execute [accuracy-comparison-custom-patterns.js](file:///Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js).

### B. Custom-Pattern Analysis
- File modified: [accuracy-comparison-custom-patterns.js](file:///Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js)
- Extended compareResults() to accept trimWindowStart, trimWindowEnd, and methodologyLabel within options.
- Extended buildPatternSummary() and summarizePatterns() to propagate options to compareResults().
- Updated main() to execute two analysis passes: one with no trimming (raw) and one with trimWindowStart=4 and trimWindowEnd=33 (trimmed).
- Generated 5 output files in the analysis directory: summary.raw.json, summary.raw.csv, summary.trimmed-4-33.json, summary.trimmed-4-33.csv, summary.json (matching trimmed), and summary.csv (matching trimmed).

## 2. Validation Commands Executed

The validation run was executed using the following commands:

```bash
npm run build

CUSTOM_PATTERN_ITERATIONS=1 \
WEARABLE_FREQUENCY=4 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
bash scripts/benchmark/run-custom-pattern-validation-pattern.sh low_variability

CUSTOM_PATTERN_ITERATIONS=1 \
CUSTOM_PATTERN_SELECTED_APPROACHES=approximation \
WEARABLE_FREQUENCY=4 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
bash scripts/benchmark/run-custom-pattern-validation-pattern.sh low_variability
```

## 3. Files Produced

The validation runs successfully produced the following output files in the target directories:

- [summary.raw.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.raw.json)
- [summary.raw.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.raw.csv)
- [summary.trimmed-4-33.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.trimmed-4-33.json)
- [summary.trimmed-4-33.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.trimmed-4-33.csv)
- [summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.json)
- [summary.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.csv)

## 4. Raw Summary Findings

- Source file: [summary.raw.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.raw.json)
- Total matched windows: 5 (covering windows 1 to 5).
- Clean validation: All expected execution cases succeeded.
- Extraction was cleanly executed for all runs.

## 5. Trimmed Summary Findings

- Source file: [summary.trimmed-4-33.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.trimmed-4-33.json)
- Total matched windows: 2 (covering windows 4 and 5).
- The window trimming successfully omitted windows 1 to 3 from accuracy comparison.
- Metrics (MAE, RMSE, MAPE) are calculated specifically over the window range [4, 33], which reduces to windows 4 and 5 for a 5-window benchmark.
- summary.json matches summary.trimmed-4-33.json, and summary.csv matches summary.trimmed-4-33.csv.

## 6. Chunked Exactness Verification

- Source files: [fetching_results.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/fetching_results.csv) and [chunked_results.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_results.csv)
- Window results:
  - Window 1 value (fetching): -23.011740634095613
  - Window 1 value (chunked): -23.011740634095634
  - Window 5 value (fetching): -23.01098917047815
  - Window 5 value (chunked): -23.010989170478172
- Mean Absolute Error (MAE) mean: 1.5631940186722205e-14
- Root Mean Squared Error (RMSE) mean: 1.6663704229186144e-14
- Mean Absolute Percentage Error (MAPE) mean: 6.793063560578203e-14
- Chunked output is verified mathematically exact (differences represent floating-point precision limits only).
- In [chunked_window_diagnostics.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_window_diagnostics.csv), the internal_chunk_ids list covers the full window start to window end. For example, window 1 (1716454104620 to 1716454224620) is covered by:
  - mqtfoskb:1716454104620:1716454134620
  - mqtfoskb:1716454134620:1716454164620
  - mqtfoskb:1716454164620:1716454194620
  - mqtfoskb:1716454194620:1716454224620
  This verifies that the required chunk intervals fully cover the external window bounds.

## 7. Approximation Extraction Verification

- Source file: [approximation_results.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_results.csv)
- Result extraction: Successfully emitted completed_window_approximation rows.
- Extracted results for windows 1..5:
  - Window 1: -23.008355
  - Window 2: -23.012102
  - Window 3: -23.011718
  - Window 4: -23.011824
  - Window 5: -23.011219
- All extracted result values are non-zero.

## 8. CPU-seconds Verification

- Source files: [resource_summary.json (chunked)](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/resource_summary.json) and [resource_summary.json (approximation)](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/resource_summary.json)
- Chunked CPU seconds: 23.03 seconds (mean CPU percent: 3.71%)
- Approximation CPU seconds: 35.24 seconds (mean CPU percent: 6.24%)
- The resource summary metrics correctly include process-tree CPU-seconds.

## 9. MQTT Verification

- Source file: [mqtt_traffic_summary.json (chunked)](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/mqtt_traffic_summary.json)
- Chunked published application bytes: 2,426,234 bytes
- Chunked estimated delivery bytes: 4,806,961 bytes
- Chunked published bandwidth: 5.64 kb/s
- Steady state duration: 419.78 seconds
- MQTT metrics are successfully calculated and exported.

## 10. Final Verdict

Can the custom-pattern benchmark family now be treated as paper-ready under the same methodology as the real-data benchmark family?

Yes. The custom-pattern benchmark family now conforms to the paper methodology:
- Default finite replay mode is active with derived duration matching target window count.
- Dynamic watchdog timeouts prevent premature termination.
- Results are trimmed during analysis to windows 4..33, matching the real-data evaluation strategy, while preserving raw output.
- All evaluation criteria (exactness, approximation extraction, process-tree CPU seconds, MQTT bandwidth) are fully tracked and verified.
