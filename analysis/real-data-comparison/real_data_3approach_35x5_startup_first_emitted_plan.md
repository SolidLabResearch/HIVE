# Real-Data 3-Approach 35x5 First-Emitted Startup Plan

Date:
`2026-06-30`

Mode:
`startup-first-emitted`

Decision:
- approaches: `fetching`, `approximation`, `chunked`
- excluded: `naive_distributed`
- iterations per approach: `35`
- target windows per iteration: `5`
- `target-windows=5` is only an upper bound / flush allowance
- startup metric is the latency of the first usable non-warmup emitted result row in each iteration
- the first usable row does not need to be window `1`

## Analysis Command

```bash
node analysis/real-data-comparison/generate-real-data-3approach-metrics.js \
  --mode startup-first-emitted \
  --input-root results/paper-benchmarks/real-data-3approach-startup-first-emitted-35x5-<timestamp> \
  --output analysis/real-data-comparison/real_data_3approach_35x5_startup_first_emitted_report.md
```

## Replacement 35-Run Server Command

Do not run from this document automatically. This is the exact replacement for the rejected `35x1` plan.

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
  --target-windows 5 \
  --output-dir results/paper-benchmarks/real-data-3approach-startup-first-emitted-35x5-<timestamp>
```

## Required Validation After the Server Run

- exactly these approach directories are present:
  - `fetching`
  - `approximation`
  - `chunked`
- `naive_distributed` does not appear in the real-data snapshot
- each selected approach has exactly `35` iteration directories
- each selected approach has a startup-valid first-emitted row in every iteration
- each selected approach reports first emitted window numbers
- each selected approach reports startup latency for each usable first-emitted row
- the first usable row may be window `1`, `2`, `3`, `4`, or `5`
- any iteration with no usable row is reported explicitly
- final-window reach and stop reason are reported as diagnostics only
- accuracy comparability is explicit for each non-fetching iteration
- accuracy aligns to `fetching` by iteration and first emitted window number where possible

## Methodology Notes

- This startup artifact remains separate from the steady-state `1x35` artifact.
- Reaching all `5` windows is not required for startup-first-emitted success.
- Warmup rows are ignored only when the latency row carries explicit `warmup=true`.
- The preferred comparable startup latency is `anchorAlignedWindowCloseToResultMs`; the helper falls back to other available latency columns only when needed.
- If the first usable row would rely on a `domain_mismatch` comparable latency, that iteration fails startup validity instead of being counted as successful.
- Under the current real-data approximation instrumentation, startup rows may emit valid result values while still lacking a same-domain comparable window-close latency. In that case, approximation startup latency should be excluded from the cross-approach startup-latency table rather than replaced with registration-based or data-start-based timings.
- Accuracy is computed only for matched windows against `fetching`.
