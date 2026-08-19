# One-pattern three-approach benchmark comparison

Benchmark configuration:
- Pattern: `approximation_test/challenging/exponential_growth`
- Frequency: `WEARABLE_FREQUENCY=10`
- Aggregation: `AVG`
- Output window: `RANGE 120000 STEP 60000`
- Subwindow / chunk window: `RANGE 60000 STEP 30000`
- Deterministic event time: enabled
- Finite replay: `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1`
- Finite replay duration: `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300`
- Debug logging: off
- Alignment basis: `window_number`

Patch summary:
- Updated the publisher harness so finite replay exits cleanly when the configured replay duration is reached, even if the full source dataset was not exhausted.
- Added `run_summary.json` output with:
  - `publisherExitReason`
  - `publishedObservations`
  - `totalSourceObservations`
  - `finiteReplayDurationSeconds`
  - plus the existing completion metadata
- No fetching, approximation, or chunked algorithm logic was changed.

## Exact commands run

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

## Generated logs

### Fetching
- `/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_log.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_resource_usage.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson`
- `/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json`

### Approximation
- `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_log.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.ndjson`
- `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/replayer-log.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/run_summary.json`

### Chunked
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_window_diagnostics.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_debug_summary.json`
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_emission_proof.json`
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv`
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.ndjson`
- `/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/run_summary.json`

## Final comparable rows

Expected event count per 120s window:
- `120s * 10 Hz * 2 streams = 2400 observations`

| Approach | Final comparable rows | Window numbers |
| --- | ---: | --- |
| Fetching | 3 | 1, 2, 3 |
| Approximation | 3 | 1, 2, 3 |
| Chunked | 3 | 1, 2, 3 |

### Fetching accepted windows

| Window | Expected count | Actual count | Mean value |
| --- | ---: | ---: | ---: |
| 1 | 2400 | 2400 | 1.0028534916666654 |
| 2 | 2400 | 2400 | 1.0058389366666671 |
| 3 | 2400 | 2400 | 1.00911985 |

Missing windows: none in the overlap set.
Extra windows: none.

## Latency summary

Latency fields differ by approach, so the table below uses the native latency field emitted by each harness:
- Fetching: `delay_past_last_obs_ms`
- Approximation: publish delay between "final aggregation ready" and "successfully published"
- Chunked: `ready_to_emit_ms`

| Approach | Mean latency | Std dev | p95 |
| --- | ---: | ---: | ---: |
| Fetching | 39.33 ms | 1.25 ms | 41.00 ms |
| Approximation | 0.67 ms | 0.47 ms | 1.00 ms |
| Chunked | 50268.67 ms | 2145.49 ms | 52930.00 ms |

## Resource summary

CPU is computed from adjacent sample deltas: `(delta(cpu_user + cpu_system) / delta_wall_time) * 100`, with `cpu_user` and `cpu_system` read from the CSV as cumulative process CPU time in milliseconds since process start. The benchmark host had 10 CPU cores, so the percentages below are normalized to total machine capacity. Total CPU seconds are included separately for reference.

| Approach | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fetching | 115.94 MiB | 194.02 MiB | 0.64% | 28.34% | 26.69 s |
| Approximation | 122.90 MiB | 175.04 MiB | 0.90% | 26.65% | 29.80 s |
| Chunked | 81.60 MiB | 136.63 MiB | 0.04% | 10.19% | 1.40 s |

## MQTT traffic

| Approach | Messages | Published bytes | Delivered bytes | Message breakdown |
| --- | ---: | ---: | ---: | --- |
| Fetching | 5773 | 4,059,961 | 4,059,961 | raw_input_stream=5769, superquery_result=3, control=1 |
| Approximation | 5867 | 4,086,166 | 4,085,947 | raw_input_stream=5755, reusable_result=108, superquery_result=3, control=1 |
| Chunked | 5868 | 4,081,800 | 8,117,054 | raw_input_stream=5750, chunk_result=18, reusable_result=96, superquery_result=3, control=1 |

## Accuracy vs fetching

Aligned on `window_number` over the overlap set `{1, 2, 3}`.

| Comparison | MAE | RMSE | MAPE |
| --- | ---: | ---: | ---: |
| Approximation vs fetching | 0.0021575852777774665 | 0.0021836343117125206 | 0.21441800437701944% |
| Chunked vs fetching | 5.921189464667501e-16 | 8.107922592650824e-16 | 5.899960285278096e-14% |

Window-level values:

| Window | Fetching | Approximation | Chunked |
| --- | ---: | ---: | ---: |
| 1 | 1.0028534916666654 | 1.0011696916666666 | 1.0028534916666667 |
| 2 | 1.0058389366666671 | 1.0034087866666668 | 1.0058389366666667 |
| 3 | 1.00911985 | 1.0067610441666668 | 1.00911985 |

## Interpretation

- Fastest native latency: approximation
- Least memory: chunked
- Least CPU: chunked
- Accuracy cost of approximation: small but non-zero underestimation relative to fetching
- Chunked exactness: effectively exact over the overlapping windows, with only floating-point noise

## Run summary notes

The finite replay publisher now records controlled termination explicitly through `publisherExitReason=finite_replay_duration_reached`.

Observed `run_summary.json` values:
- Fetching: `publishedObservations=2885`, `totalSourceObservations=10000`, `finiteReplayDurationSeconds=300`
- Approximation: `publishedObservations=2878`, `totalSourceObservations=10000`, `finiteReplayDurationSeconds=300`
- Chunked: `publishedObservations=2875`, `totalSourceObservations=10000`, `finiteReplayDurationSeconds=300`

These runs terminated by configured replay duration before the full source dataset was exhausted, which is expected in finite replay mode.
