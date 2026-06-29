# One-Pattern Centered Window Smoke

Smoke run for centered-window `rsp-js` on the `one-pattern-centered-smoke` benchmark. This is n=1 only. The centered configuration did not change the aggregated values; it changed the trigger timing and metadata shape. The normalized rows now carry centered metadata directly where the approach can surface it, and the report only reconstructs the fields for validation or comparison where the source data is still indirect.

## Environment

- RSP-JS branch: `feature/centered-window-semantics`
- RSP-JS commit: `3717272f1b7c6c11fa1776b78b161185bf28a15d`
- HIVE branch: `chunk-state-reuse-design`
- HIVE commit: `7268051fc7350b7934fcae0bf914ae47bbf5affb`
- HIVE working tree: dirty with pre-existing tracked and untracked changes; I did not overwrite them.
- RSP-JS working tree: dirty with pre-existing tracked and untracked changes.
- HIVE resolves `rsp-js` through `node_modules/rsp-js -> ../../RSP-JS`.

## Install / Build

```bash
cd /Users/kushbisen/Code/RSP-JS && npm install && npm run build && npx jest src/operators/s2r.test.ts src/rsp.test.ts --runInBand
cd /Users/kushbisen/Code/streaming-query-hive && npm install && npm run build && npx tsc --noEmit
cd /Users/kushbisen/Code/streaming-query-hive && npm install (resolved node_modules/rsp-js -> ../../RSP-JS symlink)
```

## Benchmark Commands

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-smoke/i1/fetching SESSION_ID=one-pattern-centered-smoke-fetching-i1 BENCHMARK_SCENARIO=one-pattern-centered-smoke BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/fetching/iteration1 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-smoke/i1/approximation SESSION_ID=one-pattern-centered-smoke-approximation-i1 BENCHMARK_SCENARIO=one-pattern-centered-smoke BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/approximation/iteration1 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-smoke/i1/chunked SESSION_ID=one-pattern-centered-smoke-chunked-i1 BENCHMARK_SCENARIO=one-pattern-centered-smoke BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/chunked/iteration1 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

## Process Visibility

- `RSP_WINDOW_SEMANTICS=centered` is visible in the raw latency logs and normalized rows.
- The centered schedule is emitted directly for fetching and chunked, and reconstructed for approximation because that path still does not receive the RSP engine result payload with centered metadata attached.
- The normalized CSVs at the paths below now carry the centered columns and `metadata_source`:
  - `/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/fetching/iteration1/normalized_final_rows.csv`
  - `/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/approximation/iteration1/normalized_final_rows.csv`
  - `/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/chunked/iteration1/normalized_final_rows.csv`

## Comparable Rows

| Approach | Window | window_start | window_end | logical_trigger_time | window_data_close_time | result_emitted_at | result_value |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 1 | 1756122905256 | 1756123025256 | 1756122965256 | 1756123025256 | 1782218986796 | 1.0028534916666654 |
| fetching | 2 | 1756122965256 | 1756123085256 | 1756123025256 | 1756123085256 | 1782219048745 | 1.0058389366666671 |
| fetching | 3 | 1756123025256 | 1756123145256 | 1756123085256 | 1756123145256 | 1782219110272 | 1.00911985 |
| approximation | 1 | 1756122905256 | 1756123025256 | 1756122965256 | 1756123025256 | 1782219237944 | 1.0011696916666666 |
| approximation | 2 | 1756122965256 | 1756123085256 | 1756123025256 | 1756123085256 | 1782219299781 | 1.0034087866666668 |
| approximation | 3 | 1756123025256 | 1756123145256 | 1756123085256 | 1756123145256 | 1782219361367 | 1.0067610441666668 |
| chunked | 1 | 1756122905256 | 1756123025256 | 1756122920256 | 1756122935256 | 1782219664858 | 1.0028534916666667 |
| chunked | 2 | 1756122965256 | 1756123085256 | 1756122980256 | 1756122995256 | 1782219724857 | 1.0058389366666667 |
| chunked | 3 | 1756123025256 | 1756123145256 | 1756123040256 | 1756123055256 | 1782219784858 | 1.00911985 |

## Latency Table

| Approach | Window | latency_from_logical_trigger_ms | latency_from_window_close_ms | registration_to_result_ms | last_data_to_result_ms |
| --- | --- | --- | --- | --- | --- |
| fetching | 1 | 60000 | 0 | 127108 | 5 |
| fetching | 2 | 60000 | 0 | 189057 | 49 |
| fetching | 3 | 60000 | 0 | 250584 | 13 |
| approximation | 1 | 3676 | -56324 | 63676 | 1 |
| approximation | 2 | 5513 | -54487 | 125513 | 1 |
| approximation | 3 | 7099 | -52901 | 187099 | 0 |
| chunked | 1 | 15000 | 0 | 180005 | 24086 |
| chunked | 2 | 15000 | 0 | 240004 | 22278 |
| chunked | 3 | 15000 | 0 | 300005 | 20654 |

## Accuracy vs Fetching

| Comparison | Window | Fetching | Other | Delta | Abs error | Pct error |
| --- | --- | --- | --- | --- | --- | --- |
| approximation vs fetching | 1 | 1.002853492 | 1.001169692 | -0.001683800 | 0.001683800 | -0.1680% |
| approximation vs fetching | 2 | 1.005838937 | 1.003408787 | -0.002430150 | 0.002430150 | -0.2416% |
| approximation vs fetching | 3 | 1.009119850 | 1.006761044 | -0.002358806 | 0.002358806 | -0.2337% |
| chunked vs fetching | 1 | 1.002853492 | 1.002853492 | 0.000000000 | 0.000000000 | 0.0000% |
| chunked vs fetching | 2 | 1.005838937 | 1.005838937 | -0.000000000 | 0.000000000 | -0.0000% |
| chunked vs fetching | 3 | 1.009119850 | 1.009119850 | 0.000000000 | 0.000000000 | 0.0000% |

Aggregate error summary:
- Approximation MAE: `0.002157585`
- Approximation RMSE: `0.002183634`
- Approximation MAPE: `0.2144%`
- Chunked MAE: `0.000000000`
- Chunked RMSE: `0.000000000`
- Chunked MAPE: `0.0000%`

## Resource Summary

Primary resource CSV summary:

The primary resource log reports a cumulative CPU metric in the source file. The process-tree CPU columns below are derived from wall-clock sampling.

| Approach | Primary peak RSS MiB | Primary peak heapUsed MB | Primary final CPU metric | Tree peak RSS MiB | Tree mean CPU % | Tree peak CPU % | Tree final CPU s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 347.281 | 206.530 | 14363.200 | 347.281 | 0.345 | 19.608 | 14.360 |
| approximation | 286.766 | 161.480 | 15191.870 | 364.453 | 0.427 | 18.824 | 15.770 |
| chunked | 154.359 | 40.460 | 1300.340 | 336.906 | 0.396 | 20.000 | 13.950 |

## MQTT Traffic Summary

| Approach | Published app bytes | Estimated delivery bytes | Raw input bytes | Superquery bytes | Control bytes | Chunk result bytes | Steady-state s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 2402841 | 2402841 | 2402240 | 396 | 205 | 0 | 175.952 |
| approximation | 3278957 | 3278742 | 3272613 | 1644 | 215 | 0 | 238.092 |
| chunked | 1673960 | 3326543 | 1655059 | 354 | 203 | 16071 | 121.665 |

MQTT summary files:
- `/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/fetching/iteration1/mqtt_traffic_summary.json`
- `/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/approximation/iteration1/mqtt_traffic_summary.json`
- `/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/chunked/iteration1/mqtt_traffic_summary.json`

## Centered Metadata Visibility

| Approach | Metadata source | Normalized row count | Normalized CSV |
| --- | --- | --- | --- |
| fetching | direct | 3 | /Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/fetching/iteration1/normalized_final_rows.csv |
| approximation | reconstructed | 3 | /Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/approximation/iteration1/normalized_final_rows.csv |
| chunked | direct | 3 | /Users/kushbisen/Code/streaming-query-hive/logs/centered-window-smoke/chunked/iteration1/normalized_final_rows.csv |

- Fetching emitted three comparable windows, and the normalized rows now carry centered metadata directly.
- Approximation emitted three comparable windows with early results, and its centered fields are reconstructed from benchmark geometry because the operator does not receive the direct RSP engine payload path.
- Chunked emitted three comparable windows and matched fetching window-for-window on the numeric result values, with centered metadata carried directly through the chunk summaries.
- The launcher exits cleanly after the finite replay complete signal; no manual `kill` is required for the smoke run.

## Status

- n=3 remains unrun.
- The centered smoke path is ready for the wider benchmark sweep once you want to collect it.
