# Real-Data 3-Approach 35x1 Startup-Cost Plan

Status:
`rejected for 3-approach paper reporting on 2026-06-30`

Date:
`2026-06-29`

Mode:
startup-cost

Rejection reason:
- `target-windows=1` is not a valid common startup-cost shape across `fetching`, `approximation`, and `chunked`
- `chunked` emitted result-bearing window-1 rows across the run set, but `approximation` did not reliably persist a usable window-1 latency row
- the `35x1` output therefore cannot be used as a single comparable 3-approach paper artifact

Superseded by:
- [`analysis/real-data-comparison/real_data_3approach_35x5_startup_first_emitted_plan.md`](/Users/kushbisen/Code/streaming-query-hive/analysis/real-data-comparison/real_data_3approach_35x5_startup_first_emitted_plan.md)

## Local Validation Performed

- Focused Jest suites only:
  - `experiments/real-data-comparison/run-real-data-4-approaches.test.js`
  - `scripts/benchmark/run-all-paper-benchmarks.test.js`
  - `analysis/real-data-comparison/generate-real-data-3approach-metrics.test.js`
- Top-level wrapper dry-run only:
  - `node scripts/benchmark/run-all-paper-benchmarks.js --suite real-data --iterations 35 --approaches fetching,approximation,chunked --target-windows 1 --dry-run`
- No local live benchmark smoke was run in this turn.

Verified dry-run command line:

```bash
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 35 \
  --approaches fetching,approximation,chunked \
  --target-windows 1
```

## Historical Server Command

Use the top-level wrapper on the server so the run has a concrete archived output directory:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 35 \
  --approaches fetching,approximation,chunked \
  --target-windows 1 \
  --output-dir results/paper-benchmarks/real-data-3approach-startup-35x1-2026-06-29
```

## Historical Analysis Command

```bash
node analysis/real-data-comparison/generate-real-data-3approach-metrics.js \
  --mode startup-cost \
  --input-root results/paper-benchmarks/real-data-3approach-startup-35x1-2026-06-29 \
  --output analysis/real-data-comparison/real_data_3approach_35x1_startup_cost_report.md
```

## Why This Plan Is Rejected

- The helper assumed exactly one usable row per iteration and required that row to be window `1`.
- That assumption fails for approximation on the existing `35x1` output.
- A startup report that treats missing approximation rows as equivalent to valid first-window rows would understate incompleteness and overstate comparability.
- The replacement design uses bounded `target-windows=5` runs and takes the first usable non-warmup emitted row instead.

## Historical Methodology Notes

- This run is only for startup cost.
- The helper treats `registrationToResultMs` as the aggregate startup cost.
- No separate containment-check timing field is currently extracted, so any containment/reuse planning cost remains part of the aggregate first-window startup metric.
- Do not merge this experiment’s startup latency mean with the steady-state 35-window experiment.
