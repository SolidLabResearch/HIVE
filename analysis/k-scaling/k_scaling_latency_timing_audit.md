# K-scaling latency timing audit

## Bottom line

The `~156s` latency in the current K-scaling summary is not a raw-data bug. It comes from two facts:

1. The benchmark query is `RANGE 120000 STEP 60000`, not a 60-second window.
2. Each latency CSV contains more than one emitted window per run, so the current summarizer averages window 1 and window 2 together.

That means the current table is measuring **registered-to-result latency averaged across logged windows**, not **first-result latency**.

If you only look at the first emitted window in each run, the latency is about `126-128s`, not `156s`.

## Actual window configuration

The K-scaling benchmark runner states the fixed query shape directly:

- [experiments/k-scaling/run-k-scaling-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/run-k-scaling-comparison.js#L7)

That runner says the benchmark holds the query shape fixed at `AVG, RANGE 120s STEP 60s`.

The chunked orchestrator shows the two distinct window layers:

- Main output query uses `RANGE 120000 STEP 60000`
- Chunk subqueries use `RANGE 60000 STEP 30000`

References:

- [src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts#L146)
- [src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts#L148)
- [src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts#L192)
- [src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts#L194)

## What the raw CSV columns mean

The fetching latency writer records:

- `query_registered_at`
- `first_data_received_at`
- `expected_window_close`
- `last_obs_received_at`
- `result_emitted_at`
- `delay_past_expected_close_ms = result_emitted_at - expected_window_close`
- `delay_past_data_start_ms = result_emitted_at - first_data_received_at`
- `delay_past_last_obs_ms = result_emitted_at - last_obs_received_at`

Reference:

- [src/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator.ts#L31)
- [src/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator.ts#L187)

The chunked latency writer records the same top-level anchors plus chunk-specific readiness fields:

- `query_registered_at`
- `first_data_received_at`
- `expected_window_close`
- `last_chunk_received_at`
- `interval_trigger_at`
- `result_emitted_at`
- `delay_past_expected_close_ms`
- `delay_past_data_start_ms`
- `interval_wait_ms`
- `computation_ms`
- `required_chunk_intervals`
- `last_required_chunk_received_at`
- `semantic_ready_at`
- `window_close_to_ready_ms`
- `ready_to_emit_ms`

Reference:

- [src/services/operators/chunked/ChunkedDiagnosticsWriter.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/chunked/ChunkedDiagnosticsWriter.ts#L10)
- [src/services/operators/chunked/ChunkedDiagnosticsWriter.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/chunked/ChunkedDiagnosticsWriter.ts#L67)

## Timing decomposition

The raw CSVs contain two rows in the first run, corresponding to window 1 and window 2.

### Fetching, K=1

Source:

- [analysis/k-scaling/raw/fetching/K1/low_variability/iteration1/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/analysis/k-scaling/raw/fetching/K1/low_variability/iteration1/fetching_latency_log.csv#L1)

| window | query_registered_at | expected_window_close | result_emitted_at | expected_window_close - query_registered_at | result_emitted_at - expected_window_close | result_emitted_at - query_registered_at |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1781553354604 | 1781553474604 | 1781553482382 | 120000 | 7778 | 127778 |
| 2 | 1781553354604 | 1781553534604 | 1781553542778 | 180000 | 8174 | 188174 |

### Fetching, K=32

Source:

- [analysis/k-scaling/raw/fetching/K32/low_variability/iteration1/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/analysis/k-scaling/raw/fetching/K32/low_variability/iteration1/fetching_latency_log.csv#L1)

| window | query_registered_at | expected_window_close | result_emitted_at | expected_window_close - query_registered_at | result_emitted_at - expected_window_close | result_emitted_at - query_registered_at |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1781554643684 | 1781554763684 | 1781554800657 | 120000 | 36973 | 156973 |
| 2 | 1781554643684 | 1781554823684 | 1781554861598 | 180000 | 37914 | 217914 |

### Chunked, K=1

Source:

- [analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_latency_log_consumer_1.csv](/Users/kushbisen/Code/streaming-query-hive/analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_latency_log_consumer_1.csv#L1)

| window | query_registered_at | expected_window_close | result_emitted_at | expected_window_close - query_registered_at | result_emitted_at - expected_window_close | result_emitted_at - query_registered_at |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1781554901935 | 1781555021935 | 1781555027629 | 120000 | 5694 | 125694 |
| 2 | 1781554901935 | 1781555081935 | 1781555088121 | 180000 | 6186 | 186186 |

The same pattern appears in the chunked parent-partial log:

- [analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_parent_partial_latency_log_consumer_1.csv](/Users/kushbisen/Code/streaming-query-hive/analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_parent_partial_latency_log_consumer_1.csv#L1)

That file shows:

- `parent_range_ms = 120000`
- `covered_duration_ms = 60000`
- `chunks_used = 2`

### Chunked, K=32

Source:

- [analysis/k-scaling/raw/chunked/K32/low_variability/iteration1/chunked_latency_log_consumer_1.csv](/Users/kushbisen/Code/streaming-query-hive/analysis/k-scaling/raw/chunked/K32/low_variability/iteration1/chunked_latency_log_consumer_1.csv#L1)

| window | query_registered_at | expected_window_close | result_emitted_at | expected_window_close - query_registered_at | result_emitted_at - expected_window_close | result_emitted_at - query_registered_at |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1781556191357 | 1781556311357 | 1781556317040 | 120000 | 5683 | 125683 |
| 2 | 1781556191357 | 1781556371357 | 1781556377529 | 180000 | 6172 | 186172 |

## Why the mean is around 156s

For each run, the current summarizer averages the row-level registered-to-result latency across the emitted rows in that file.

For K=1 fetching, that is:

- `(127778 + 188174) / 2 = 157976 ms`

For K=1 chunked, that is:

- `(125694 + 186186) / 2 = 155940 ms`

The same pattern holds for K=32:

- Fetching: `(156973 + 217914) / 2 = 187443.5 ms`
- Chunked: `(125683 + 186172) / 2 = 155927.5 ms`

So the `~156s` value is the average of two emitted windows:

- window 1 is about `120s + 6s`
- window 2 is about `180s + 6s`

That averages to about `150s + 6s = 156s`

## Is the current latency table correct?

Yes, numerically.

No, if the intended label is “first-result latency”.

The current table is best described as:

- **registered-to-result latency averaged across emitted windows in the logged run**

It should not be called:

- `registration-to-first-result latency`

because it includes window 2, not only the first emitted result.

It also should not be called:

- `post-window delay`

because the value in the table includes the full window lifetime from registration to emission, not just the delay after `expected_window_close`.

## What should the presentation call it?

Recommended label:

- **mean registered-to-result latency across emitted windows**

If the goal is the first visible result after the benchmark starts, then the cleaner metric is:

- **first-result latency**, computed from the earliest emitted window only

If the goal is to compare how long the system takes after a window is already eligible to close, then use:

- **post-window delay** or **delay past expected close**

## Is fetching comparable to chunked?

Under the current definition:

- yes, because both use `result_emitted_at - query_registered_at`

But that comparison is only about end-to-end emission timing.

It is not a clean comparison of readiness mechanics, because chunked exposes extra fields that separate:

- time until the window is semantically ready
- time from readiness to actual emission

Fetching does not expose an equivalent readiness breakdown in the same raw file.

So the current end-to-end table is fair as a coarse comparison, but it is not the best way to isolate the chunking benefit.

## Recommendation

If the goal is to compare approach overhead fairly:

1. Keep the current end-to-end table, but relabel it as window-level registered-to-result latency.
2. Add a second table or footnote using:
   - fetching: `delay_past_expected_close_ms`
   - chunked: `window_close_to_ready_ms` and `ready_to_emit_ms`
3. If you want “first result” latency, change the summarizer to select the earliest emitted window per run instead of averaging all emitted windows.

## Summary

- The benchmark is `RANGE 120s STEP 60s`, not a 60-second window.
- The first emitted window is about `126-128s` after registration.
- The current `~156s` summary is the average of the first two emitted windows in each run.
- The current calculation is internally consistent, but the label should make the averaging explicit.
