# Tier 1 Smoke Blocker Fix Report

## Scope

- Custom-pattern approximation extraction only
- Window-parameter-sensitivity bounded smoke classification only

No changes were made to real-data, K-scaling, scalability, frequency, unified-benchmark, or legacy shell wrappers.

## Fixes

### 1. Custom-pattern approximation extractor

Files changed:

- `/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/extract-pattern-results.js`
- `/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/extract-pattern-results.test.js`

Root cause:

- The approximation extractor already had the right high-level fallback path to read `approximation_latency_log.csv`.
- The actual failure was lower-level: `parseCsvLine()` dropped empty CSV fields.
- Real approximation latency rows contain empty columns such as `wall_clock_window_close` and `wall_clock_close_to_result_ms`.
- Once empty fields were dropped, the row shifted left. `result_emitted_at` was parsed as `latency_from_query_reg_ms`, `approximation_status` no longer lined up, and the extractor returned zero comparable results.

Patch:

- Replaced the regex CSV splitter with a simple parser that preserves empty fields and quoted values.
- Kept the approximation latency extraction logic scoped to:
  - `approximation_status=completed_window_approximation`
  - numeric `window_number`
  - numeric `result_value`
  - numeric `result_emitted_at`
- Preserved legacy behavior for fetching, chunked, naive, and older approximation extraction fallbacks.
- Added a regression test using a latency row with empty columns in the same positions as real benchmark output.

Validation:

- Focused tests passed:
  - `npx jest experiments/pattern-analysis/extract-pattern-results.test.js experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js --runInBand`
- Real completed-window artifact validation passed:
  - `node experiments/pattern-analysis/extract-pattern-results.js approximation low_variability logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1`
- Result:
  - extractor produced 15 approximation windows from real `completed_window_approximation` rows
  - `approximation_results.csv` and `approximation_metadata.json` were written successfully

Fresh bounded smoke caveat:

- A fresh bounded custom-pattern approximation run at:
  - `/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1`
  did not emit any completed approximation rows before publisher shutdown.
- Its `approximation_latency_log.csv` contains only the header row, so that run does not exercise the reported blocker.
- I did not broaden scope into the separate runner/publisher timing issue because the task was to fix the extractor path only.

### 2. Window-parameter-sensitivity bounded smoke classification

Files changed:

- `/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
- `/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js`

Root cause:

- The bounded-success classifier treated orchestrator exit code `143` as a hard failure.
- In bounded smoke runs, `143` can be an intentional cleanup termination after valid target-window artifacts already exist.
- That incorrectly preserved `FAILED` classification for runs that had already emitted valid bounded results.

Patch:

- Kept the bounded-success path restricted to the existing failure reason:
  - `Orchestrator exited unexpectedly before publisher completion`
- Allowed bounded success when:
  - required result files exist
  - emitted result count is positive
  - publisher termination is acceptable
  - orchestrator termination is cleanup-compatible (`SIGTERM` or exit code `143`)
- Preserved rejection for other nonzero or signaled failure shapes.
- Added a regression test covering `orchestratorExitCode=143`.

Validation:

- Focused tests passed:
  - `npx jest experiments/pattern-analysis/extract-pattern-results.test.js experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js --runInBand`
- Bounded smoke rerun:
  - `node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js --experiment superquery-range-scaling --iterations 1 --ranges 120 --patterns low_variability --approaches fetching --replay-duration-seconds 180 --timeout-ms 240000 --skip-build --log-root logs/window-parameter-sensitivity-smoke --result-root results/window-parameter-sensitivity-smoke`
- Observed artifact state after one bounded final window:
  - `/Users/kushbisen/Code/streaming-query-hive/logs/window-parameter-sensitivity-smoke/superquery-range-scaling/fetching/low_variability/range-120s/iteration1/fetching_latency_log.csv`
  - `/Users/kushbisen/Code/streaming-query-hive/logs/window-parameter-sensitivity-smoke/superquery-range-scaling/fetching/low_variability/range-120s/iteration1/run_metadata.json`
- Result:
  - `run_metadata.json` records `success: true`
  - `run_status: "SUCCESS"`
  - `emitted_result_count: 1`
  - `orchestrator_exit_code: 143`
  - trimmed metadata remains present:
    - `target_window_count: 35`
    - `trimmed_window_start: 4`
    - `trimmed_window_end: 33`

Direct classifier confirmation:

- Evaluating `classifyBoundedSuccess()` against the same bounded smoke artifact returns:
  - `boundedSuccess: true`
  - `reason: "bounded_smoke_windows_emitted"`

## Acceptance Status

### Custom-pattern approximation extractor no longer reports 0 when completed-window latency rows exist

Pass.

- Verified with the real completed-row artifact at:
  - `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1/approximation_latency_log.csv`

### Window-parameter-sensitivity smoke no longer reports FAILED when target-window completion is valid

Pass.

- Verified with:
  - `/Users/kushbisen/Code/streaming-query-hive/logs/window-parameter-sensitivity-smoke/superquery-range-scaling/fetching/low_variability/range-120s/iteration1/run_metadata.json`

### Existing CPU-seconds, MQTT, MAE/MAPE/RMSE, and completeness fields remain present

Pass for the touched scope.

- Window-sensitivity smoke still writes:
  - `resource_summary.json`
  - `resource_per_pid_summary.json`
  - `mqtt_traffic.ndjson`
- The extractor changes are limited to approximation result-row parsing and do not remove downstream accuracy/completeness fields.

## Commands Run

```bash
node --check experiments/pattern-analysis/extract-pattern-results.js
node --check experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js
npx jest experiments/pattern-analysis/extract-pattern-results.test.js experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js --runInBand
node experiments/pattern-analysis/extract-pattern-results.js approximation low_variability logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js --experiment superquery-range-scaling --iterations 1 --ranges 120 --patterns low_variability --approaches fetching --replay-duration-seconds 180 --timeout-ms 240000 --skip-build --log-root logs/window-parameter-sensitivity-smoke --result-root results/window-parameter-sensitivity-smoke
```

## Remaining Caveat

- The fresh bounded custom-pattern approximation runner instance used for smoke validation still shuts down before it emits any `completed_window_approximation` rows.
- That is a separate runner/runtime issue, not an extractor issue, and it was intentionally left out of this patch set.
