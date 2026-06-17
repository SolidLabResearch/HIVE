# Window-Parameter Sensitivity Benchmarks

This folder adds two controlled chunk-state reconstruction benchmarks on top of the existing HIVE runner/orchestrator pattern. Both experiments reuse the current `fetching` and `chunked` approach implementations, the shared finite-replay publisher, the shared process-cleanup flow, and the existing profiling counters.

## Experiment 2: Superquery Range Scaling

Purpose: hold the two reusable subqueries fixed while increasing the downstream superquery range.

- Base streams/properties: the existing `wearableX` and `smartphoneX` AVG subqueries
- Default subquery windows: `RANGE 60s STEP 30s`
- Superquery step: `60s`
- Superquery ranges: `120s,180s,240s,300s,360s,420s`
- Exact-final reuse: disabled explicitly with `HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE=false`

Smoke run:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js \
  --experiment superquery-range-scaling \
  --iterations 1 \
  --patterns low_variability \
  --approaches fetching,chunked \
  --ranges 120,180 \
  --replay-duration-seconds 240
```

Full run:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js \
  --experiment superquery-range-scaling \
  --iterations 5 \
  --patterns low_variability \
  --approaches fetching,chunked \
  --ranges 120,180,240,300,360,420 \
  --replay-duration-seconds 900
```

## Experiment 3: Chunk Granularity Sensitivity

Purpose: hold the superquery fixed and vary the chunk size used by the reusable subqueries.

- Base streams/properties: the existing `wearableX` and `smartphoneX` AVG subqueries
- Superquery window: `RANGE 120s STEP 60s`
- Chunk sizes: `1s,5s,15s,30s,60s`
- For this experiment the subquery windows are set to `RANGE C STEP C`, which makes the chunk plan GCD equal to the selected chunk size
- Exact-final reuse: disabled explicitly with `HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE=false`

Smoke run:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js \
  --experiment chunk-granularity-sensitivity \
  --iterations 1 \
  --patterns low_variability \
  --approaches fetching,chunked \
  --chunk-sizes 30,60 \
  --replay-duration-seconds 240
```

Full run:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js \
  --experiment chunk-granularity-sensitivity \
  --iterations 5 \
  --patterns low_variability \
  --approaches fetching,chunked \
  --chunk-sizes 1,5,15,30,60 \
  --replay-duration-seconds 900
```

## Outputs

Runner logs:

- `logs/window-parameter-sensitivity/<experiment>/<approach>/<pattern>/<scenario>/iterationN/`

Extracted CSVs:

- `results/window-parameter-sensitivity/<experiment>/*_per_run.csv`
- `results/window-parameter-sensitivity/<experiment>/*_aggregate.csv`
- `results/window-parameter-sensitivity/<experiment>/*_profile_counters.csv`

## Plotting Guidance

Per-result normalization is required for these experiments because a fixed replay duration does not guarantee the same number of emitted superquery windows across parameter settings. In particular, larger superquery ranges can emit fewer results, so total CPU, RSS, chunk-message, and latency totals alone are not comparable enough for paper plots.

Use these CSVs for plotting:

- `*_per_run.csv` for point plots, box plots, scatter plots, or any view that needs one row per run
- `*_aggregate.csv` for mean and standard-deviation summaries across iterations
- `*_profile_counters.csv` when plotting raw profiler counters directly

For Experiment 3 (`chunk-granularity-sensitivity`), treat `fetching` as a chunk-independent baseline. The extractor includes `chunk_size_applies_to_approach` for this purpose:

- `true` for `chunked`
- `false` for `fetching`

When plotting chunk size on the x-axis, fetching should be shown as a repeated baseline or collapsed baseline annotation, not interpreted as if fetching has a meaningful internal chunk size.

`expected_chunk_states_per_result` is scenario metadata and can be populated for all approaches. `chunk_states_consumed_per_emitted_result` is only meaningful for chunked runs. Fetching does not consume chunk states and should be treated as a non-chunked baseline.

`ready_to_emit_ms`-family metrics are chunked-only. Fetching rows may leave those fields blank.

Each per-run row includes:

- experiment metadata (`experiment_name`, `approach`, `pattern`, `iteration`, aggregation, superquery range/step, chunk size, replay duration, exact-final reuse flag)
- emitted-result metadata (`emitted_result_count`, `reconstructed_result_count`, `superquery_result_count`, `expected_chunk_states_per_result`, `expected_chunk_states_total`)
- resource metrics (`cpu_seconds`, `peak_rss_mb`, plus per-emitted-result normalized variants)
- latency metrics (`mean/std/median/p95` for adjusted latency and, where available, `ready_to_emit_ms` and `computation_ms`)
- correctness metrics (`mean_error`, `mae`, `rmse`, `mape`, `max_abs_error`)
- reuse counters (`shared_chunk_producers_created`, `chunk_state_messages_published`, `reconstructed_superquery_results`, `fallback_original_agent_rsps_started`)
- validity metadata (`success`, `is_valid`, `process_cleanup_ok`, `validity_reason`)
