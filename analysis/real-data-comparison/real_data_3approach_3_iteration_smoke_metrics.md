# Real-Data 3-Approach 3-Iteration Smoke Metrics

Date/time:
`2026-06-29T22:53:14+0200` local
`2026-06-29T20:53:14Z` UTC

Branch:
`chunk-state-reuse-design`

Commit hash:
`7d65c9a34f347293ca414bbc7418314e45ed4f56`

Dirty state:
`yes`

Exact command run:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 3 \
  --approaches fetching,approximation,chunked
```

Top-level equivalent command:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 3 \
  --approaches fetching,approximation,chunked
```

Selected approaches:
`fetching, approximation, chunked`

Excluded approaches:
`naive_distributed`

Output directory:
`/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/logs`

Run log:
`/Users/kushbisen/Code/streaming-query-hive/analysis/real-data-comparison/real_data_3approach_3_iteration_smoke_run.log`

## Completeness

- Exactly 3 selected approaches ran: `yes`
- Selected-approach artifacts touched in this run window: `yes`
- naive_distributed touched in this run window: `no`
- Captured run log mentions Naive Distributed: `no`

| Approach | Iterations complete | Final windows | Stop reason | Latency rows per iteration | Required result fields valid | Artifacts touched in run window |
| --- | --- | --- | --- | --- | --- | --- |
| `fetching` | 3/3 | [1,2,3,4,5] | `target_window_count_reached` | 5/5/5 | yes | yes |
| `approximation` | 3/3 | [1,2,3,4,5] | `target_window_count_reached` | 5/5/5 | yes | yes |
| `chunked` | 3/3 | [1,2,3,4,5] | `target_window_count_reached` | 5/5/5 | yes | yes |

## Latency

Comparable latency column used in the main table:
`anchorAlignedWindowCloseToResultMs`

Main comparable latency table:

| Approach | Comparable windows | Mean ms | Std ms | Min ms | Max ms |
| --- | --- | --- | --- | --- | --- |
| `fetching` | 15 | 144361.47 | 121469.12 | 0.00 | 287467.00 |
| `chunked` | 15 | 996673.40 | 119890.79 | 854812.00 | 1138540.00 |

Latency rows excluded from the main comparison because `latency_domain_status=domain_mismatch`:

| Approach | Excluded windows | Mean raw ms | Std raw ms | Min raw ms | Max raw ms |
| --- | --- | --- | --- | --- | --- |
| `approximation` | 15 | 571102.47 | 119891.55 | 429228.00 | 712974.00 |
## Accuracy

Accuracy baseline:
`fetching`

Alignment:
`iteration + window_number`

| Approach vs fetching | Matched windows | Baseline-only windows | Approach-only windows | MAE | RMSE | MAPE | MAPE windows | Max abs error | Chunked exact |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `approximation` | 15 | 0 | 0 | 0.001970 | 0.002432 | 0.015915 | 15 | 0.004220 | n/a |
| `chunked` | 15 | 0 | 0 | 0.000000 | 0.000000 | 0.000000 | 15 | 0.000000 | yes |

## Resource Usage

Preferred CPU metric:
`tree_cpu_seconds` from the process-tree resource logs

RSS metric:
`tree_rss_bytes`, reported here as MiB

| Approach | CPU seconds mean | CPU seconds std | CPU seconds min | CPU seconds max | Mean RSS MiB mean | Mean RSS MiB std | Peak RSS MiB mean | Peak RSS MiB std |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `fetching` | 3.560 | 0.095 | 3.460 | 3.650 | 162.49 | 1.61 | 237.75 | 0.28 |
| `approximation` | 4.333 | 0.145 | 4.190 | 4.480 | 222.99 | 14.31 | 323.52 | 5.56 |
| `chunked` | 3.283 | 0.105 | 3.180 | 3.390 | 222.07 | 19.79 | 334.03 | 26.82 |

## Warnings

- approximation: 15 latency rows reported latency_domain_status=domain_mismatch and were excluded from the comparable latency table.

## Errors

- none

## Recommendation

- 3-approach smoke passed: `yes`
- 35-iteration server run recommended: `yes`

