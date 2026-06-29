# Fetching produced zero accepted final comparable windows

## Technical Summary

Fetching did not fail to start, and this is not a normalizer bug. The run registered the query, processed MQTT input, and generated 166 RStream candidate rows, but every candidate was suppressed before final emission. The diagnostics show 0 accepted rows and 166 suppressed rows. The exact failure mode is the completeness gate inside fetching: first the window must be fully settled in time span, then the settled event count must meet the expected threshold. In this run, the settled windows never reached that threshold, so `output.csv` stayed header-only and the normalizer had no final rows to accept.

The evidence points to a runtime/configuration mismatch rather than a schema mismatch:

- fetching emitted candidate rows, not final comparable payload rows
- the candidate rows were repeatedly marked `incomplete_span` and then `incomplete_count`
- the run did receive the finite replay complete control signal, but the replay flush still did not produce any accepted rows because the settled windows remained below the expected event count
- approximation and chunked both wrote final comparable rows in the same final-output schema, which confirms the downstream comparator is looking in the right place

## Evidence

### Exact commands used

```bash
python3 /Users/kushbisen/.codex/plugins/cache/openai-curated-remote/data-analytics/0.1.43-7f4d2c05f012/skills/user-context/scripts/data_analytics_preflight.py \
  --workflow build-report \
  --request-mode ordinary_workflow

tail -n 80 /Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log

wc -l \
  /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/output.csv \
  /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv \
  /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv \
  /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv

head -n 5 /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv
head -n 5 /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/output.csv

head -n 5 /Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/output.csv
head -n 5 /Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/output.csv

head -n 10 /Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson
head -n 10 /Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.ndjson
```

### Artifact paths

- Fetching console log: [/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log](/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log)
- Fetching raw log: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_log.csv)
- Fetching diagnostics: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_window_diagnostics.csv)
- Fetching latency: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv)
- Fetching output: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/output.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/output.csv)
- Fetching MQTT traffic: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv)
- Fetching MQTT NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson)
- Fetching run summary: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json)
- Approximation output: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/output.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/output.csv)
- Approximation latency: [/Users/kushbisen/Code/streaming-query-hive/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/approximation_latency_log.csv)
- Chunked output: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/output.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/output.csv)
- Chunked latency: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv)

### What the run summary says

Fetching run summary:

- `finalComparableWindowCount`: 0
- `provisionalWindowCandidateCount`: 41
- `emittedWindows`: 0
- `latency`: null
- `comparisonStatus`: `incomplete`

The run summary also records:

- `Fetching diagnostics emitted only suppressed candidate rows in this run.`
- `Unique provisional window starts observed: 41.`
- `No accepted/finalized fetching rows were available for final-output comparison.`

### The raw counts

Fetching artifact sizes:

- `output.csv`: 1 line total, header only
- `fetching_latency_log.csv`: 1 line total, header only
- `fetching_window_diagnostics.csv`: 167 lines total, 166 data rows
- `mqtt_traffic.csv`: 50,625 lines total, 50,624 data rows

Fetching diagnostics row counts:

- accepted rows: 0
- suppressed rows: 166
- completeness statuses: 164 `incomplete_count`, 2 `incomplete_span`
- reasons: 164 `event_count_below_expected_threshold_23760`, 2 `waiting_for_all_streams_to_progress_past_window_end`

Fetching MQTT traffic row counts:

- `raw_input_stream`: 50,623
- `control`: 1
- `superquery_result`: 0

### The console evidence

The console log shows all the right upstream activity, but no accepted final row:

```text
LOG: 1782134797119 - Finite replay complete signal received: {"topic":"one-pattern-three-approach-300s/fetching/__benchmark_control__","source":"benchmark_publisher"}
LOG: 1782134797120 - Fetching candidate row seen: {"logicalWindowKey":"1756122905256:1756123025256","windowStart":1756122905256,"windowEnd":1756123025256,"eventCount":1802,"expectedEventCount":24000,"completenessStatus":"incomplete_count","reason":"event_count_below_expected_threshold_23760"}
LOG: 1782134797126 - Delayed because incomplete: {"logicalWindowKey":"1756125305256:1756125425256","eventCount":2400,"expectedEventCount":24000,"completenessStatus":"incomplete_count","reason":"event_count_below_expected_threshold_23760"}
Timeout reached, killing processes...
LOG: 1782134916286 - Process terminated, cleaning up...
```

Two facts matter here:

- the finite replay complete signal did arrive, so the run was not blocked because the benchmark never signaled completion
- the settled counts still remained below the acceptance threshold, so the finalization branch never executed

### Schema comparison

Fetching’s diagnostics schema is provisional and separate from the final comparable output schema:

- fetching diagnostics header: `benchmark_event_time_anchor,window_number,window_start,window_end,event_count,expected_event_count,sum,avg,first_event_timestamp,last_event_timestamp,completeness_status,accepted_or_suppressed,reason,result_value`
- fetching latency header: `window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,delay_past_expected_close_ms,delay_past_data_start_ms,delay_past_last_obs_ms,result_value`
- approximation final-output header: `approach,window_number,query_registered_at,first_data_received_at,expected_window_close,last_observed_at,result_emitted_at,registration_to_result_ms,data_start_to_result_ms,post_window_delay_ms,result_value,warmup,aggregation_type,result_topic`
- chunked final-output header: same final-output columns as approximation

The important part is that fetching’s `output.csv` already uses the final-output schema, but it has zero data rows. That means the comparator is looking in the right place; there simply are no accepted fetching rows to compare.

## Diagnosis

### Exact reason accepted final rows = 0

Fetching never emitted an accepted final row because every candidate failed the internal completeness test:

1. The first gate requires the logical window span to be complete. The first two fetching candidates failed this gate with `incomplete_span`.
2. After that, the settled window count still fell far below the expected threshold. The run summary and diagnostics show `expected_event_count = 24000`, threshold `23760`, and observed settled counts around `2400`.
3. Because `settledCompleteness.isSettled` never became true, the accepted/finalized branch at [/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts#L965) never executed.

That is why `output.csv` remained header-only and the normalizer accepted zero final rows.

### Why this is not a normalizer bug

The normalizer is not rejecting valid final rows. There are no valid final rows to normalize.

Evidence:

- `fetching_window_diagnostics.csv` contains only suppressed candidate rows
- `fetching_latency_log.csv` is header-only
- [/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log](/Users/kushbisen/Code/streaming-query-hive/analysis/benchmark-logs/corrected-fetching-console.log) has zero `Accepted/finalized:` lines
- the accepted branch in [/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts#L965) only runs after `settledCompleteness.isSettled` succeeds

If the goal is to surface fetching candidates without changing algorithm semantics, the normalizer would need a separate fallback to the diagnostics CSV. That would still not create final comparable rows, so it would not solve the benchmark-comparison problem.

## Smallest instrumentation note

No instrumentation patch is needed to explain this diagnosis. If you still want fetching to emit final comparable rows when it does finalize in a future run, the smallest non-semantic instrumentation is already present in the accepted branch: write the same final-output columns used by approximation and chunked after `settledCompleteness.isSettled` becomes true.

If the practical goal is to make fetching finalize under this benchmark, the valid fix is a config change, not an instrumentation change. The current benchmark needs either:

- a longer replay horizon so the settled count can reach the acceptance threshold before teardown, or
- a less strict completeness threshold for this data density, or
- a different data density / `K` setting that makes the 120s windows reach the expected count

## Recommendation

Treat this run as an incomplete fetching benchmark, not as a failed normalization pass. Do not compute accuracy against fetching until the runtime emits at least one accepted final comparable row.
