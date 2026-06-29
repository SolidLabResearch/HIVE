# Tier 1 Low-Risk Paper Wiring Report

Date: 2026-06-25
Scope: low-risk Tier 1 paper-methodology wiring only

Implemented scope:

1. `experiments/pattern-analysis/run-custom-patterns-comparison.js`
2. `experiments/window-parameter-sensitivity/common.js`
3. `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`

Out of scope and not changed:

- frequency runners
- scalability runners
- K-scaling runners
- `experiments/unified-benchmark.js`
- legacy shell wrappers
- real-data runner behavior

## Changes Made

### 1. Custom-pattern runner

Patched:

- `experiments/pattern-analysis/run-custom-patterns-comparison.js`

Added explicit paper-mode env wiring to the existing `withBenchmarkReplayEnv(...)` call:

- `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
- `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
- `STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0`
- `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`

Preserved:

- existing process-tree CPU collection
- MQTT artifact collection
- existing shared accuracy analysis path

Effect:

- custom-pattern runs no longer depend on the stale default in `benchmarkReplayEnv.js`
- chunked is now explicitly configured for semantic-ready immediate emission instead of inheriting comparable-output mode

### 2. Window-parameter-sensitivity shared env

Patched:

- `experiments/window-parameter-sensitivity/common.js`

Added explicit paper-mode env wiring to both:

- `buildScenarioConfig(...)`
- `buildQueryTargetScalingScenarioConfig(...)`

Added env flags:

- `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
- `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
- `STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0`
- `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`

Added metadata fields propagated into `run_metadata.json` through the existing runner:

- `target_window_count=35`
- `trimmed_window_start=4`
- `trimmed_window_end=33`

Effect:

- sub-window/window-parameter sensitivity now makes the paper method explicit instead of relying on helper defaults
- no benchmark semantics changed beyond explicit paper-mode configuration

### 3. Window-parameter-sensitivity extractor completeness/trim fields

Patched:

- `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`

Added per-run completeness fields:

- `expected_window_count`
- `actual_window_count`
- `missing_window_count`
- `extra_window_count`
- `matched_window_count`

Added trimmed completeness fields when target-window metadata is available:

- `trimmed_expected_window_count`
- `trimmed_matched_window_count`
- `trimmed_missing_window_count`
- `trimmed_extra_window_count`
- `trimmed_actual_window_count`

Details:

- full-window completeness uses expected windows `1..target_window_count`
- trimmed completeness uses `trimmed_window_start..trimmed_window_end`
- if target-window metadata is absent, trimmed/full optional counts remain null-compatible rather than inferred unsafely
- existing `MAE`, `MAPE`, `RMSE`, `cpu_seconds`, `ready_to_emit`, and chunk-state fields were kept

Also updated:

- per-run CSV schema
- aggregate CSV schema
- focused unit tests for common/env and extractor behavior

## Validation Run

Static checks:

```bash
node --check experiments/pattern-analysis/run-custom-patterns-comparison.js
node --check experiments/window-parameter-sensitivity/common.js
node --check experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js
```

Focused tests:

```bash
npx jest experiments/window-parameter-sensitivity/common.test.js experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js --runInBand
```

Result:

- 2 test suites passed
- 17 tests passed

Non-benchmark CLI validation:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js --help
```

Result:

- help output rendered successfully

What was intentionally not run:

- no full 35-window benchmark
- no full custom-pattern paper suite
- no full window-parameter sensitivity matrix

## Acceptance Check

- no new runner created
- existing helpers reused
- real-data runner behavior unchanged
- custom-pattern runner now explicitly uses semantic-ready full-window chunked mode
- window-sensitivity shared env now explicitly uses semantic-ready full-window chunked mode
- extractor now exposes paper completeness fields

## Remaining Caveat

The custom-pattern runner file already had unrelated local modifications in the worktree before this task. This patch only added the explicit paper-mode env block and did not attempt to normalize or revert any other changes in that file.
