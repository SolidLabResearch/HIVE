# One-Pattern Three-Approach Comparison, Corrected

Benchmark controls:
- DATA_PATH: `approximation_test/challenging/exponential_growth`
- Aggregation: `AVG`
- Output window: `RANGE 120000 STEP 60000`
- Subwindow / chunk window: `RANGE 60000 STEP 30000`
- Finite replay duration: `300s`
- Deterministic event-time anchor: `1756122905256`
- Session naming convention: `one-pattern-three-approach-300s/<approach>`
- K / target count: default wearableX + smartphoneX
- Debug logging: off
- Chunked comparable-output mode: on

## Executive Summary

- Fetching did not emit any accepted final-output windows in this 300s run. It produced 41 provisional window candidates in diagnostics, but none were accepted/finalized, so it cannot serve as a fetching reference for accuracy.
- Approximation emitted 3 final rows total. Window 1 is warmup; windows 2-3 are the only non-warmup rows, so the series is incomplete relative to the replay horizon.
- Chunked emitted 41 final rows total and is the only approach that produced a full comparable final-output series in this run.
- Because the fetching baseline is missing final comparable rows, MAE / RMSE / MAPE vs fetching are not valid for this run and are intentionally not reported as decision-grade numbers.

## Commands

### fetching
```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=100 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-300s/fetching SESSION_ID=one-pattern-three-approach-fetching-300s BENCHMARK_SCENARIO=one-pattern-three-approach BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js > analysis/benchmark-logs/corrected-fetching-console.log 2>&1
```

### approximation
```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=100 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-300s/approximation SESSION_ID=one-pattern-three-approach-approximation-300s BENCHMARK_SCENARIO=one-pattern-three-approach BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 node scripts/analysis-js/experiment-evaluation-approximation-approach.js > analysis/benchmark-logs/corrected-approximation-console.log 2>&1
```

### chunked
```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=100 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-300s/chunked SESSION_ID=one-pattern-three-approach-chunked-300s BENCHMARK_SCENARIO=one-pattern-three-approach BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js > analysis/benchmark-logs/corrected-chunked-console.log 2>&1
```

### postprocess
```bash
node scripts/analysis-js/generate-one-pattern-three-approach-corrected-report.js
```

## Artifact Paths

### fetching
- Console log: [/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log](/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log)
- Raw result log: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_log.csv)
- Diagnostics CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv)
- Resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_resource_usage.csv)
- Normalized output CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/output.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/output.csv)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson)

### approximation
- Console log: [/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-approximation-console.log](/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-approximation-console.log)
- Raw result log: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_log.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/approximation_latency_log.csv)
- Resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv)
- Normalized output CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/output.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/output.csv)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/run_summary.json)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.ndjson)

### chunked
- Console log: [/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-chunked-console.log](/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-chunked-console.log)
- Raw result log: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_chunk_aggregator_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_chunk_aggregator_log.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv)
- Resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv)
- Normalized output CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/output.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/output.csv)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/run_summary.json)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.ndjson)

## Windowing

- RANGE `120000` means the first complete output window is only expected once 120 seconds of event-time coverage exist.
- STEP `60000` means subsequent output windows are scheduled every 60 seconds after that first full window.
- With a 300s replay, the theoretical comparable series length is about 4 windows, assuming the replay and event-time coverage are sufficient.

## Comparable Final Output Counts

| Approach | Final comparable windows | Warmup windows | Non-warmup windows |
| --- | ---: | ---: | ---: |
| fetching | 0 | 0 | 0 |
| approximation | 3 | 1 | 2 |
| chunked | 41 | 1 | 40 |

Fetching diagnostics also showed 41 provisional window starts, but none were accepted/finalized.

## Missing And Extra Windows

- Fetching has no comparable final windows, so there is no valid fetching baseline to compute missing or extra windows against.
- Approximation emitted windows 1-3 only, so it is missing the rest of the observed comparable cadence.
- Chunked emitted windows 1-41 and is complete for the observed cadence in this run.

## Latency Summary

### Fetching

- Final comparable registration-to-result latency: n/a
- Final comparable data-start-to-result latency: n/a
- Final comparable expected-window-close delay: n/a
- Reason: no accepted/finalized fetching output rows were produced.

### Approximation

- All rows: registration-to-result 122319.00 ms, std 49218.43 ms, p95 182568.00 ms
- All rows: data-start-to-result 116781.00 ms, std 49218.43 ms, p95 177030.00 ms
- All rows: post-window delay -57681.00 ms, std 232.78 ms, p95 -57432.00 ms
- Warmup row: registration-to-result 62008.00 ms, p95 62008.00 ms
- Non-warmup rows: registration-to-result 152474.50 ms, std 30093.50 ms, p95 182568.00 ms
- Non-warmup rows: post-window delay -57525.50 ms, std 93.50 ms, p95 -57432.00 ms

### Chunked

- All rows: registration-to-result 187325.15 ms, std 86082.38 ms, p95 300012.00 ms
- All rows: data-start-to-result 181656.15 ms, std 86082.38 ms, p95 294343.00 ms
- All rows: post-window delay -1132674.85 ms, std 625812.66 ms, p95 -179996.00 ms
- Warmup row: registration-to-result 60003.00 ms, p95 60003.00 ms
- Non-warmup rows: registration-to-result 190508.20 ms, std 84735.03 ms, p95 300012.00 ms
- Non-warmup rows: post-window delay -1159491.80 ms, std 609874.92 ms, p95 -239995.00 ms

Note: post-window delay is measured against query-registration + RANGE + (window_number - 1) * STEP. Negative values are possible under the deterministic anchor and should be treated as relative timing, not a correctness failure by themselves.

## Resource Summary

Resource samples are process-level snapshots. CPU is computed from process.cpuUsage() deltas between adjacent ~100 ms samples; RSS is sampled from process RSS.

| Approach | Sample count | Sample interval | Mean RSS | Peak RSS | Mean CPU | Peak CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 4175 | 100.90 ms | 173.37 MiB | 280.98 MiB | 22.18% | 243.83% |
| approximation | 4176 | 100.90 ms | 177.10 MiB | 252.19 MiB | 17.18% | 212.93% |
| chunked | 4176 | 100.88 ms | 66.89 MiB | 136.89 MiB | 0.24% | 50.33% |

## MQTT Traffic

| Approach | CSV available | Published bytes | Estimated delivered bytes | Notes |
| --- | --- | ---: | ---: | --- |
| fetching | yes | 35632123 | 35632123 | raw_input=35631914 |
| approximation | yes | 28785354 | 28785135 | superquery=1644 |
| chunked | yes | 29034073 | 57707170 | chunk_result=136 |

## Accuracy

- Fetching baseline final windows are missing, so MAE / RMSE / MAPE versus fetching are not valid for this run.
- Alignment by `window_number` could not be completed because the reference series is empty.
- Approximation and chunked both emitted comparable window numbers, but neither can be scored against fetching without inventing a baseline from provisional rows.

## Interpretation

- Fastest comparable final-output series: chunked, because it is the only approach that produced the full 41-window comparable run.
- Lowest memory: chunked.
- Lowest CPU: chunked.
- Approximation pays an accuracy risk through incomplete output coverage in this run, not through a measurable versus-fetching numeric error.
- Chunked is not proven exact against fetching here because the fetching baseline never finalized.

## Remaining Limitations

- Fetching produced only provisional suppressed candidates and no accepted final output rows.
- Approximation stopped after 3 windows, so the replay did not cover the full observed cadence.
- Accuracy against fetching is intentionally omitted because the baseline is missing the required comparable final series.
- MQTT traffic CSVs were generated from the NDJSON traces during postprocessing; no broker-internal delivery counts were available.
