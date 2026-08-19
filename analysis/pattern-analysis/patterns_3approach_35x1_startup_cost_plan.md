# Custom-Pattern 3-Approach 35x1 Startup-Cost Plan

## Scope

Reuse the existing custom-pattern benchmark runner and existing extraction/accuracy path. Do not rebuild the benchmark framework.

Selected approaches:
- `fetching`
- `approximation`
- `chunked`

Excluded approach:
- `naive_distributed`

Startup-cost mode:
- iterations: `35`
- target windows per run: `1`
- one independent first-window measurement per iteration

## Existing Reusable Pieces

- Canonical runner: `node experiments/pattern-analysis/run-custom-patterns-comparison.js`
- Existing extraction already writes:
  - `<iteration>/<approach>_results.csv`
  - `<iteration>/<approach>_metadata.json`
  - `<iteration>/resource_summary.json`
- Existing first-window timing fields already available:
  - `elapsed_since_registration_ms` in `*_results.csv`
  - `firstEventLatency` in `*_metadata.json`
- Existing accuracy baseline remains fetching-derived and reusable.

## Small Changes Applied

- Added CLI approach filtering to the existing runner:
  - `--approaches fetching,approximation,chunked`
- Added CLI target-window override to the existing runner:
  - `--target-windows 1`
- Added CLI output-root selection to the existing runner:
  - `--output-dir <path>`
- Added an analysis-only helper for startup-cost reporting from existing extracted outputs:
  - `analysis/pattern-analysis/generate-custom-pattern-3approach-metrics.js`

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
  --iterations 35 \
  --approaches fetching,approximation,chunked \
  --target-windows 1 \
  --output-dir /path/to/output/patterns_3approach_35x1_startup_cost
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
  --iterations 35 \
  --approaches fetching,approximation,chunked \
  --target-windows 1 \
  --output-dir /path/to/output/patterns_3approach_35x1_startup_cost_low_variability
```

Startup-cost report generation:

```bash
node analysis/pattern-analysis/generate-custom-pattern-3approach-metrics.js \
  --mode startup-cost \
  --input-root /path/to/output/patterns_3approach_35x1_startup_cost \
  --output /path/to/output/patterns_3approach_35x1_startup_cost_report.md \
  --target-windows 1 \
  --expected-iterations 35 \
  --approaches fetching,approximation,chunked \
  --patterns low_variability
```

## Validation Performed

- Focused Jest coverage passed for:
  - exact 3-approach selection
  - exclusion of `naive_distributed`
  - target-window count `1`
  - startup-cost report using first-window outputs only
- Tiny startup structure smoke:
  - `2 iterations x 1 window x 1 pattern x 3 approaches`
  - result: the interface is accepted, but this low-window smoke also showed incomplete comparable extraction on later runs

## Launch Risk

- The tiny startup smoke was not fully clean:
  - first fetching iteration completed and extracted one finalized window
  - later low-window cases hit early invalid exits or no comparable extracted rows
- That means the `2x1` smoke should not be treated as a go/no-go proxy for the full `35x1` launch.
- The safest next step is a single-pattern server launch with the exact `35x1` configuration, then inspect `custom_pattern_comparison_summary.json`, `benchmark_window_cap_summary.json`, and the first few per-iteration extraction outputs before scaling to all patterns.
