# One-pattern 10 Hz CPU diagnosis

Benchmark run:
- Pattern: `approximation_test/challenging/exponential_growth`
- Frequency: `WEARABLE_FREQUENCY=10`
- Finite replay duration: `300s`
- Output window: `RANGE 120000 STEP 60000`
- Subwindow / chunk window: `RANGE 60000 STEP 30000`
- Deterministic event time: enabled
- Debug logging: off
- Session prefix: `one-pattern-three-approach-10hz/<approach>`

## Patch Summary

Measurement-layer patch only:
- Added a process-tree resource sampler in the benchmark runner scripts.
- Kept the existing per-process CSVs for backward compatibility.
- Added tree-aware CSVs:
  - `fetching_client_side_process_tree_resource_usage.csv`
  - `approximation_approach_process_tree_resource_usage.csv`
  - `streaming_query_hive_process_tree_resource_log.csv`
- No algorithm logic changed.

The sampler uses the approach process PID as the root and walks descendant PIDs via `ps`, sampling every 100 ms.

## Exact Commands

### Fetching
```bash
DATA_PATH=approximation_test/challenging/exponential_growth \
WEARABLE_FREQUENCY=10 \
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 \
STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 \
STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-10hz/fetching \
SESSION_ID=one-pattern-three-approach-fetching-10hz \
BENCHMARK_SCENARIO=one-pattern-three-approach \
BENCHMARK_SCALE=challenging_exponential_growth \
BENCHMARK_APPROACH=fetching \
BENCHMARK_ITERATION=1 \
OUTPUT_WINDOW_RANGE=120000 \
OUTPUT_WINDOW_STEP=60000 \
SUB_WINDOW_RANGE=60000 \
SUB_WINDOW_STEP=30000 \
AGGREGATION_FUNCTION=AVG \
AGGREGATION_FUNC=AVG \
STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 \
STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 \
LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1 \
node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

### Approximation
```bash
DATA_PATH=approximation_test/challenging/exponential_growth \
WEARABLE_FREQUENCY=10 \
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 \
STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 \
STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-10hz/approximation \
SESSION_ID=one-pattern-three-approach-approximation-10hz \
BENCHMARK_SCENARIO=one-pattern-three-approach \
BENCHMARK_SCALE=challenging_exponential_growth \
BENCHMARK_APPROACH=approximation \
BENCHMARK_ITERATION=1 \
OUTPUT_WINDOW_RANGE=120000 \
OUTPUT_WINDOW_STEP=60000 \
SUB_WINDOW_RANGE=60000 \
SUB_WINDOW_STEP=30000 \
AGGREGATION_FUNCTION=AVG \
AGGREGATION_FUNC=AVG \
STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 \
STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 \
LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1 \
node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

### Chunked
```bash
DATA_PATH=approximation_test/challenging/exponential_growth \
WEARABLE_FREQUENCY=10 \
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 \
STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 \
STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-10hz/chunked \
SESSION_ID=one-pattern-three-approach-chunked-10hz \
BENCHMARK_SCENARIO=one-pattern-three-approach \
BENCHMARK_SCALE=challenging_exponential_growth \
BENCHMARK_APPROACH=chunked \
BENCHMARK_ITERATION=1 \
OUTPUT_WINDOW_RANGE=120000 \
OUTPUT_WINDOW_STEP=60000 \
SUB_WINDOW_RANGE=60000 \
SUB_WINDOW_STEP=30000 \
AGGREGATION_FUNCTION=AVG \
AGGREGATION_FUNC=AVG \
STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 \
STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 \
LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1 \
node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

## Process Coverage

Resource CSV source semantics:
- `cpu_user` and `cpu_system` in the primary CSVs are cumulative `process.cpuUsage()` samples in milliseconds.
- Tree CSVs are sampled from `ps` over the approach PID plus descendants and store cumulative tree CPU seconds and summed RSS.
- Publisher/replayer is launched as a separate benchmark process and is not included in the approach resource logs.
- MQTT broker is external and not included.
- Sampling interval: approximately 100 ms across all logs.

| Approach | Primary process entrypoint | Child processes spawned by the approach | Primary CSV coverage | Tree CSV coverage | `process_count` in tree log | Publisher/replayer included? | Broker included? | CPU source |
| --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| Fetching | `dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js` | No additional worker PID observed in the tree log | `process.cpuUsage()` on launcher PID | `ps` over launcher PID + descendants | 1 | No | No | `process.cpuUsage()` / `ps` |
| Approximation | `dist/approaches/StreamingQueryApproximationApproachOrchestrator.js` | `HiveQueryBee` worker PID appears in the tree log | `process.cpuUsage()` on launcher PID | `ps` over launcher PID + descendants | 1 to 2 | No | No | `process.cpuUsage()` / `ps` |
| Chunked | `dist/approaches/StreamingQueryChunkedApproachOrchestrator.js` | `HiveQueryBee` worker PID appears in the tree log | `process.cpuUsage()` on launcher PID | `ps` over launcher PID + descendants | 1 to 2 | No | No | `process.cpuUsage()` / `ps` |

Tree log evidence:
- Fetching tree log stayed at `process_count=1` for the whole run.
- Approximation tree log reached `process_count=2`.
- Chunked tree log reached `process_count=2`.

## Corrected Resource Table

Machine core count: `10`

### Primary process only

| Approach | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fetching | 121.03 MiB | 196.09 MiB | 0.41% | 27.39% | 17.29 s |
| Approximation | 129.86 MiB | 190.64 MiB | 0.60% | 27.46% | 19.90 s |
| Chunked | 60.74 MiB | 137.41 MiB | 0.02% | 4.31% | 0.77 s |

### Process tree

| Approach | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fetching | 120.93 MiB | 196.09 MiB | 0.42% | 29.59% | 17.67 s |
| Approximation | 209.09 MiB | 278.17 MiB | 0.63% | 31.11% | 20.88 s |
| Chunked | 182.91 MiB | 304.50 MiB | 0.59% | 30.43% | 19.54 s |

## MQTT Traffic

Observed traffic volume is similar between approximation and chunked:
- Approximation: `raw_input_stream=5755`, `reusable_result=108`, `superquery_result=3`
- Chunked: `raw_input_stream=5750`, `chunk_result=18`, `reusable_result=96`, `superquery_result=3`

That volume difference is too small to justify the earlier apparent primary-process CPU gap by itself.

## Diagnosis

The earlier "approximation much higher than chunked" CPU result was a measurement artifact.

Why:
- The primary CSVs are launcher-PID snapshots. They do not represent the same coverage boundary across approaches.
- The chunked launcher is extremely light in the primary CSV, because the heavy work sits in the worker/process tree.
- Once sampled as a process tree, approximation and chunked are close: `0.63%` vs `0.59%` mean CPU, with similar peak CPU and total CPU seconds.
- Fetching remains single-process in this benchmark, so its primary and tree metrics are nearly identical.

## Conclusion

This is not a meaningful algorithmic CPU win for chunked over approximation in the primary CSVs. The large gap came from process-boundary mismatch. The tree-aware metrics are the fair comparison here, and they show approximation and chunked in the same band.
