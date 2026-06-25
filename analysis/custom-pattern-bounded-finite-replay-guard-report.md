# Custom-Pattern Bounded Finite Replay Guard Report

## Scope

- File changed: [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)
- Approach validated: approximation only
- Pattern validated: `low_variability` only
- No other benchmark families were changed

## Change

Added a small env guard in [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js):

- if `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS` is set
- and `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY` is not explicitly set
- then set `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1`

Preserved behavior:

- if the caller explicitly sets `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY`, that value wins unchanged
- the paper benchmark wrapper behavior is unchanged

## Validation

### Syntax

Passed:

```bash
node --check experiments/pattern-analysis/run-custom-patterns-comparison.js
```

### Bounded approximation rerun

Command used:

```bash
CUSTOM_PATTERN_SELECTED_PATTERNS=low_variability \
CUSTOM_PATTERN_SELECTED_APPROACHES=approximation \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=150 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability -i 1 --pattern-test-timeout 240000
```

Result:

- run completed successfully
- publisher exited cleanly with `code=0`
- approximation orchestrator exited cleanly with `code=0`
- extractor succeeded

Key artifact paths:

- [publisher.log](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/publisher.log)
- [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_latency_log.csv)
- [approximation_results.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_results.csv)
- [benchmark_window_cap_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/benchmark_window_cap_summary.json)
- [mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic.ndjson)
- [mqtt_traffic_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic_summary.json)

## Verification against requested checks

### 1. Verify reusable_result traffic appears

Pass in raw MQTT trace.

From [mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic.ndjson):

- `raw_input_stream`: `1204`
- `reusable_result`: `8`
- `superquery_result`: `1`
- `control`: `1`

Note:

- [mqtt_traffic_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic_summary.json) still reports `reusable_result_published_bytes: 0`.
- The raw NDJSON trace clearly shows reusable-result messages, so the summary artifact appears to classify these bytes under `reuse_layer_*` instead of `reusable_result_*`.
- That summary quirk was not patched here because it is outside this scoped guard change.

### 2. Verify approximation_latency_log.csv has completed_window_approximation rows

Pass.

From [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_latency_log.csv):

- window `1`
- `approximation_status=completed_window_approximation`
- `result_value=-23.00835540094843`

### 3. Verify extract-pattern-results.js extracts those rows

Pass.

From [approximation_results.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_results.csv):

- extracted `1` comparable result
- window `1`
- `window_start=1716454104620`
- `window_end=1716454224620`
- `result_value=-23.00835540094843`

The runner’s extraction phase also completed successfully during the rerun.

## Outcome

The finite replay guard fixes the bounded custom-pattern approximation invocation shape that previously failed when only `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS` was supplied.

Specifically, after the guard:

- bounded direct invocations now enter finite replay mode automatically
- the publisher runs for the intended bounded duration
- approximation receives reusable-result traffic
- completed-window approximation rows are emitted
- result extraction succeeds

## Files changed

- [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)
