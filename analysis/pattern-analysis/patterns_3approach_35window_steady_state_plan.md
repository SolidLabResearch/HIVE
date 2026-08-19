# Custom-Pattern 3-Approach 35-Window Steady-State Plan

## Scope

Reuse the existing custom-pattern benchmark runner and existing extraction/accuracy path. Do not rebuild the benchmark framework.

Selected approaches:
- `fetching`
- `approximation`
- `chunked`

Excluded approach:
- `naive_distributed`

Steady-state mode:
- iterations: `1`
- target windows per run: `35`
- analysis windows: `4..33`
- dropped warm-up windows: `1..3`
- dropped shutdown windows: `34..35`

## Existing Reusable Pieces

- Canonical runner: `node experiments/pattern-analysis/run-custom-patterns-comparison.js`
- Pattern names:
  - `low_variability`
  - `step_pattern`
  - `spike_pattern`
  - `low_freq_oscillation`
  - `high_freq_oscillation`
- Existing approaches in the runner:
  - `fetching`
  - `naive_distributed`
  - `approximation`
  - `chunked`
- Existing output root structure:
  - `<output-root>/<approach>/<pattern>/iterationN/...`
  - summary: `<output-root>/custom_pattern_comparison_summary.json`
- Existing accuracy aggregation:
  - `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- Existing extracted per-iteration resource metrics:
  - `resource_summary.json`
  - `resource_usage.csv`
  - `resource_per_pid_summary.json`
- Existing extracted latency/result columns:
  - `timestamp`
  - `window_number`
  - `window_start`
  - `window_end`
  - `result_value`
  - `elapsed_since_registration_ms`
  - `delay_past_expected_close_ms`

## Small Changes Applied

- Added CLI approach filtering to the existing runner:
  - `--approaches fetching,approximation,chunked`
- Added CLI target-window override to the existing runner:
  - `--target-windows 35`
- Added CLI output-root selection to the existing runner:
  - `--output-dir <path>`
- Kept env-based selection support intact for existing wrapper flows.
- Added an analysis-only helper for the stabilized 3-approach report shape:
  - `analysis/pattern-analysis/generate-custom-pattern-3approach-metrics.js`
- Added wrapper forwarding for custom-pattern `--patterns`, `--approaches`, and `--target-windows`.

## Exact Server Commands

Benchmark run:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js \
  --iterations 1 \
  --approaches fetching,approximation,chunked \
  --target-windows 35 \
  --output-dir /path/to/output/patterns_3approach_35window_steady_state
```

Optional single-pattern dry launch check:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js \
  low_variability \
  --iterations 1 \
  --approaches fetching,approximation,chunked \
  --target-windows 35 \
  --output-dir /path/to/output/patterns_3approach_35window_steady_state_low_variability
```

Steady-state report generation:

```bash
node analysis/pattern-analysis/generate-custom-pattern-3approach-metrics.js \
  --mode steady-state \
  --input-root /path/to/output/patterns_3approach_35window_steady_state \
  --output /path/to/output/patterns_3approach_35window_steady_state_report.md \
  --target-windows 35 \
  --expected-iterations 1 \
  --approaches fetching,approximation,chunked \
  --patterns low_variability
```

## Validation Performed

- Focused Jest coverage passed for:
  - custom-pattern CLI approach filtering
  - target-window counts `1` and `35`
  - selected pattern preservation
  - steady-state report trimming to windows `4..33`
  - startup-cost first-window-only reporting
  - paper wrapper forwarding
- Tiny steady-state structure smoke:
  - `1 iteration x 5 windows x 1 pattern x 3 approaches`
  - result: runner accepted the interface, but low-window smoke is not yet a reliable validation proxy

## Launch Risk

- The tiny `5-window` smoke was not clean:
  - fetching exited with `stoppedAfterTargetWindows is not true in summary`
  - approximation and chunked terminated before extracting comparable rows
- That risk is in the small smoke shape, not in the new CLI/report wiring itself.
- Before a long server run, prefer a single-pattern `35-window` launch over treating the `5-window` smoke as authoritative.
