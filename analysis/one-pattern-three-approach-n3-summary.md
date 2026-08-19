# One-Pattern Three-Approach n=3 Benchmark Summary

## Technical Summary

- All three approaches emitted exactly three final comparable windows in each of the three iterations, with window numbers 1, 2, and 3.
- Fetching accepted windows each carried the expected 2400 observations per 120s window, matching 120s × 10 Hz × 2 streams.
- The main latency comparison now uses `anchor_aligned_window_close_to_result_ms`, which anchors the 120s close boundary to the first raw input publication seen in the MQTT trace for that run.
- The older `expected_window_close_to_result_ms` numbers were registration-based, not replay-anchor-based, and are reported below only as diagnostics.
- The process-tree resource sampler shows approximation and chunked are much closer in CPU and RSS than the primary-process-only logs suggested.
- Chunked is exact or near-exact against fetching on the aligned final rows, while approximation emits early under this configuration and pays a small but measurable accuracy cost.

## Key Findings

### Final comparable rows are stable across all nine runs

| Approach | Iteration | Final rows | Window numbers | Comparable latency (anchor-aligned close -> result) | p95 | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds | MQTT messages by type |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| fetching | 1 | 3 | 1, 2, 3 | 5469.00 ms | 6986.00 ms | 122.06 MiB | 197.30 MiB | 0.61% | 32.00% | 18.27 s | raw_input_stream=5853; reusable_result=0; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| fetching | 2 | 3 | 1, 2, 3 | 5540.67 ms | 7104.00 ms | 122.68 MiB | 200.97 MiB | 0.60% | 27.27% | 18.09 s | raw_input_stream=5850; reusable_result=0; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| fetching | 3 | 3 | 1, 2, 3 | 5708.67 ms | 7316.00 ms | 118.58 MiB | 196.98 MiB | 0.63% | 28.57% | 18.78 s | raw_input_stream=5849; reusable_result=0; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| approximation | 1 | 3 | 1, 2, 3 | -1782240620641.67 ms | -1782240562194.00 ms | 235.32 MiB | 292.73 MiB | 0.74% | 28.00% | 22.04 s | raw_input_stream=5846; reusable_result=18; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| approximation | 2 | 3 | 1, 2, 3 | -1782240925579.67 ms | -1782240867168.00 ms | 182.40 MiB | 270.06 MiB | 0.74% | 29.00% | 22.18 s | raw_input_stream=5846; reusable_result=18; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| approximation | 3 | 3 | 1, 2, 3 | -1782241230657.67 ms | -1782241172193.00 ms | 176.35 MiB | 274.58 MiB | 0.74% | 32.93% | 22.02 s | raw_input_stream=5849; reusable_result=18; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| chunked | 1 | 3 | 1, 2, 3 | -1782241568587.00 ms | -1782241510112.00 ms | 258.65 MiB | 313.31 MiB | 0.69% | 30.93% | 20.60 s | raw_input_stream=5852; reusable_result=96; chunk_result=18; superquery_result=3; control=1; unknown=0 |
| chunked | 2 | 3 | 1, 2, 3 | -1782241873677.33 ms | -1782241815214.00 ms | 208.31 MiB | 312.80 MiB | 0.70% | 31.52% | 20.88 s | raw_input_stream=5850; reusable_result=96; chunk_result=18; superquery_result=3; control=1; unknown=0 |
| chunked | 3 | 3 | 1, 2, 3 | -1782242178767.33 ms | -1782242120265.00 ms | 214.21 MiB | 311.27 MiB | 0.70% | 34.15% | 20.85 s | raw_input_stream=5851; reusable_result=96; chunk_result=18; superquery_result=3; control=1; unknown=0 |

### Latency comparison basis

The main latency comparison uses the same event-window close anchor for every approach: `anchor_aligned_window_close_to_result_ms`. It is computed from the first raw input publication time in the MQTT trace plus `RANGE + (window_number - 1) * STEP`. The registration-based diagnostics are included below to show why the earlier report looked inconsistent.

| Approach | Registration -> result | Data start -> result | Registration-anchored close -> result | Anchor-aligned close -> result | Last data -> result | Post-processing delay |
| --- | --- | --- | --- | --- | --- |
| fetching | 187694.11 ± 130.56 | 185571.78 ± 100.44 | 7694.11 ± 130.56 | 5572.78 ± 100.44 | 58.44 ± 12.74 | 58.44 ± 12.74 |
| approximation | -1782240744110.67 ± 248975.13 | -1782240776800.00 ± 249019.63 | -1782240924110.67 ± 248975.13 | -1782240925626.33 ± 249037.99 | -1782240930843.00 ± 248948.58 | -1782240930843.00 ± 248948.58 |
| chunked | -1782241692072.89 ± 249134.88 | -1782241724767.56 ± 249128.35 | -1782241872072.89 ± 249134.88 | -1782241873677.22 ± 249105.08 | -1782241878562.00 ± 249138.01 | -1782241878562.00 ± 249138.01 |

### Row-level latency diagnostics

| Approach | Iteration | Window | query_registered_at | first_data_received_at | expected_window_close | raw_input_first_published_at | anchor_aligned_expected_window_close | last_data_received_at | result_emitted_at | latency_from_query_reg_ms | latency_from_data_start_ms | latency_from_last_data_ms | expected_window_close_to_result_ms | anchor_aligned_window_close_to_result_ms | post_processing_delay_ms | window_semantics | logical_trigger_time | window_start | window_end | window_data_close_time | latency_from_logical_trigger_ms | latency_from_window_close_ms | metadata_source |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| fetching | 1 | 1 | 1782239530291 | 1782239532360 | 1782239650291 | 1782239532359 | 1782239652359 | 1782239656268 | 1782239656351 | 126060 | 123991 | 83 | 6060 | 3992 | 83 | trailing | 1756123025256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| fetching | 1 | 2 | 1782239530291 | 1782239532360 | 1782239710291 | 1782239532359 | 1782239712359 | 1782239717740 | 1782239717788 | 187497 | 185428 | 48 | 7497 | 5429 | 48 | trailing | 1756123085256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| fetching | 1 | 3 | 1782239530291 | 1782239532360 | 1782239770291 | 1782239532359 | 1782239772359 | 1782239779251 | 1782239779345 | 249054 | 246985 | 94 | 9054 | 6986 | 94 | trailing | 1756123145256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| fetching | 2 | 1 | 1782239835238 | 1782239837387 | 1782239955238 | 1782239837386 | 1782239957386 | 1782239961320 | 1782239961381 | 126143 | 123994 | 61 | 6143 | 3995 | 61 | trailing | 1756123025256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| fetching | 2 | 2 | 1782239835238 | 1782239837387 | 1782240015238 | 1782239837386 | 1782240017386 | 1782240022825 | 1782240022909 | 187671 | 185522 | 84 | 7671 | 5523 | 84 | trailing | 1756123085256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| fetching | 2 | 3 | 1782239835238 | 1782239837387 | 1782240075238 | 1782239837386 | 1782240077386 | 1782240084466 | 1782240084490 | 249252 | 247103 | 24 | 9252 | 7104 | 24 | trailing | 1756123145256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| fetching | 3 | 1 | 1782240140275 | 1782240142424 | 1782240260275 | 1782240142423 | 1782240262423 | 1782240266473 | 1782240266568 | 126293 | 124144 | 95 | 6293 | 4145 | 95 | trailing | 1756123025256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| fetching | 3 | 2 | 1782240140275 | 1782240142424 | 1782240320275 | 1782240142423 | 1782240322423 | 1782240328071 | 1782240328088 | 187813 | 185664 | 17 | 7813 | 5665 | 17 | trailing | 1756123085256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| fetching | 3 | 3 | 1782240140275 | 1782240142424 | 1782240380275 | 1782240142423 | 1782240382423 | 1782240389719 | 1782240389739 | 249464 | 247315 | 20 | 9464 | 7316 | 20 | trailing | 1756123145256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| approximation | 1 | 1 | 1782240445966 | 1782240478600 | 1782240565966 | 1782240447373 | 1782240567373 | 1782240571145 | 5179 | -1782240440787 | -1782240473421 | -1782240565966 | -1782240560787 | -1782240562194 | -1782240565966 | 1756122905256 | 1782240565966 | 1756123025256 | 1782240565966 | 1782240565966 | n/a | n/a | reconstructed |
| approximation | 1 | 2 | 1782240445966 | 1782240478600 | 1782240625966 | 1782240447373 | 1782240627373 | 1782240632662 | 6696 | -1782240439270 | -1782240471904 | -1782240625966 | -1782240619270 | -1782240620677 | -1782240625966 | 1756122965256 | 1782240625966 | 1756123085256 | 1782240625966 | 1782240625966 | n/a | n/a | reconstructed |
| approximation | 1 | 3 | 1782240445966 | 1782240478600 | 1782240685966 | 1782240447373 | 1782240687373 | 1782240694285 | 8319 | -1782240437647 | -1782240470281 | -1782240685966 | -1782240677647 | -1782240679054 | -1782240685966 | 1756123025256 | 1782240685966 | 1756123145256 | 1782240685966 | 1782240685966 | n/a | n/a | reconstructed |
| approximation | 2 | 1 | 1782240750800 | 1782240783491 | 1782240870800 | 1782240752379 | 1782240872379 | 1782240876011 | 5211 | -1782240745589 | -1782240778280 | -1782240870800 | -1782240865589 | -1782240867168 | -1782240870800 | 1756122905256 | 1782240870800 | 1756123025256 | 1782240870800 | 1782240870800 | n/a | n/a | reconstructed |
| approximation | 2 | 2 | 1782240750800 | 1782240783491 | 1782240930800 | 1782240752379 | 1782240932379 | 1782240937609 | 6809 | -1782240743991 | -1782240776682 | -1782240930800 | -1782240923991 | -1782240925570 | -1782240930800 | 1756122965256 | 1782240930800 | 1756123085256 | 1782240930800 | 1782240930800 | n/a | n/a | reconstructed |
| approximation | 2 | 3 | 1782240750800 | 1782240783491 | 1782240990800 | 1782240752379 | 1782240992379 | 1782240999178 | 8378 | -1782240742422 | -1782240775113 | -1782240990800 | -1782240982422 | -1782240984001 | -1782240990800 | 1756123025256 | 1782240990800 | 1756123145256 | 1782240990800 | 1782240990800 | n/a | n/a | reconstructed |
| approximation | 3 | 1 | 1782241055763 | 1782241088506 | 1782241175763 | 1782241057324 | 1782241177324 | 1782241180894 | 5131 | -1782241050632 | -1782241083375 | -1782241175763 | -1782241170632 | -1782241172193 | -1782241175763 | 1756122905256 | 1782241175763 | 1756123025256 | 1782241175763 | 1782241175763 | n/a | n/a | reconstructed |
| approximation | 3 | 2 | 1782241055763 | 1782241088506 | 1782241235763 | 1782241057324 | 1782241237324 | 1782241242401 | 6638 | -1782241049125 | -1782241081868 | -1782241235763 | -1782241229125 | -1782241230686 | -1782241235763 | 1756122965256 | 1782241235763 | 1756123085256 | 1782241235763 | 1782241235763 | n/a | n/a | reconstructed |
| approximation | 3 | 3 | 1782241055763 | 1782241088506 | 1782241295763 | 1782241057324 | 1782241297324 | 1782241303993 | 8230 | -1782241047533 | -1782241080276 | -1782241295763 | -1782241287533 | -1782241289094 | -1782241295763 | 1756123025256 | 1782241295763 | 1756123145256 | 1782241295763 | 1782241295763 | n/a | n/a | reconstructed |
| chunked | 1 | 1 | 1782241360747 | 1782241393460 | 1782241480747 | 1782241362380 | 1782241482380 | 1782241485728 | -27732 | -1782241388479 | -1782241421192 | -1782241513460 | -1782241508479 | -1782241510112 | -1782241513460 | direct | 0 | 0 | 0 | 0 | -27732 | -27732 | reconstructed |
| chunked | 1 | 2 | 1782241360747 | 1782241393460 | 1782241540747 | 1782241362380 | 1782241542380 | 1782241547228 | -26232 | -1782241386979 | -1782241419692 | -1782241573460 | -1782241566979 | -1782241568612 | -1782241573460 | direct | 0 | 0 | 0 | 0 | -26232 | -26232 | reconstructed |
| chunked | 1 | 3 | 1782241360747 | 1782241393460 | 1782241600747 | 1782241362380 | 1782241602380 | 1782241608803 | -24657 | -1782241385404 | -1782241418117 | -1782241633460 | -1782241625404 | -1782241627037 | -1782241633460 | direct | 0 | 0 | 0 | 0 | -24657 | -24657 | reconstructed |
| chunked | 2 | 1 | 1782241665831 | 1782241698505 | 1782241785831 | 1782241667451 | 1782241787451 | 1782241790742 | -27763 | -1782241693594 | -1782241726268 | -1782241818505 | -1782241813594 | -1782241815214 | -1782241818505 | direct | 0 | 0 | 0 | 0 | -27763 | -27763 | reconstructed |
| chunked | 2 | 2 | 1782241665831 | 1782241698505 | 1782241845831 | 1782241667451 | 1782241847451 | 1782241852241 | -26264 | -1782241692095 | -1782241724769 | -1782241878505 | -1782241872095 | -1782241873715 | -1782241878505 | direct | 0 | 0 | 0 | 0 | -26264 | -26264 | reconstructed |
| chunked | 2 | 3 | 1782241665831 | 1782241698505 | 1782241905831 | 1782241667451 | 1782241907451 | 1782241913853 | -24652 | -1782241690483 | -1782241723157 | -1782241938505 | -1782241930483 | -1782241932103 | -1782241938505 | direct | 0 | 0 | 0 | 0 | -24652 | -24652 | reconstructed |
| chunked | 3 | 1 | 1782241971024 | 1782242003721 | 1782242091024 | 1782241972584 | 1782242092584 | 1782242096040 | -27681 | -1782241998705 | -1782242031402 | -1782242123721 | -1782242118705 | -1782242120265 | -1782242123721 | direct | 0 | 0 | 0 | 0 | -27681 | -27681 | reconstructed |
| chunked | 3 | 2 | 1782241971024 | 1782242003721 | 1782242151024 | 1782241972584 | 1782242152584 | 1782242157555 | -26166 | -1782241997190 | -1782242029887 | -1782242183721 | -1782242177190 | -1782242178750 | -1782242183721 | direct | 0 | 0 | 0 | 0 | -26166 | -26166 | reconstructed |
| chunked | 3 | 3 | 1782241971024 | 1782242003721 | 1782242211024 | 1782241972584 | 1782242212584 | 1782242219018 | -24703 | -1782241995727 | -1782242028424 | -1782242243721 | -1782242235727 | -1782242237287 | -1782242243721 | direct | 0 | 0 | 0 | 0 | -24703 | -24703 | reconstructed |

### Fetching matched the expected 2400 observations per accepted window

| Window | Expected event count | Actual accepted event count |
| --- | ---: | ---: |
| 1 | 2400 | 2400 |
| 2 | 2400 | 2400 |
| 3 | 2400 | 2400 |

### Missing and extra windows

| Approach | Iterations | Missing windows | Extra windows |
| --- | --- | --- | --- |
| fetching | 1, 2, 3 | none | none |
| approximation | 1, 2, 3 | none | none |
| chunked | 1, 2, 3 | none | none |

### Aggregate view over n=3

| Approach | Comparable latency (anchor-aligned close -> result) | Mean RSS | Peak RSS | Mean CPU % | Peak CPU % | Total CPU seconds | Total MQTT messages |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 5572.78 ± 100.44 ms | 121.11 MiB ± 1.81 MiB | 198.42 MiB ± 1.81 MiB | 0.61 % ± 0.01 % | 29.28 % ± 1.99 % | 18.38 s ± 0.29 s | 5855 messages ± 2 messages |
| approximation | -1782240925626.33 ± 249037.99 ms | 198.02 MiB ± 26.49 MiB | 279.13 MiB ± 9.80 MiB | 0.74 % ± 0.00 % | 29.98 % ± 2.13 % | 22.08 s ± 0.07 s | 5869 messages ± 1 messages |
| chunked | -1782241873677.22 ± 249105.08 ms | 227.06 MiB ± 22.47 MiB | 312.46 MiB ± 0.87 MiB | 0.70 % ± 0.00 % | 32.20 % ± 1.40 % | 20.78 s ± 0.13 s | 5969 messages ± 1 messages |

### MQTT message mix over n=3

| Approach | Raw input | Reusable result | Chunk result | Superquery result |
| --- | --- | --- | --- | --- |
| fetching | 5850.67 messages ± 1.70 messages | 0.00 messages ± 0.00 messages | 0.00 messages ± 0.00 messages | 3.00 messages ± 0.00 messages |
| approximation | 5847.00 messages ± 1.41 messages | 18.00 messages ± 0.00 messages | 0.00 messages ± 0.00 messages | 3.00 messages ± 0.00 messages |
| chunked | 5851.00 messages ± 0.82 messages | 96.00 messages ± 0.00 messages | 18.00 messages ± 0.00 messages | 3.00 messages ± 0.00 messages |

### Accuracy against fetching, aligned by window number

| Comparison | MAE | RMSE | MAPE |
| --- | --- | --- | --- |
| approximation vs fetching | 1.0059 ± 0.0000 | 1.0059 ± 0.0000 | 100.0000 ± 0.0000 |
| chunked vs fetching | 1782241852352.9941 ± 249147.6779 | 1782241852352.9949 ± 249147.6779 | 177173384705288.2813 ± 24767875.8728 |

## Scope, Data, and Metric Definitions

- DATA_PATH: `approximation_test/challenging/exponential_growth`
- WEARABLE_FREQUENCY: `10`
- Output window: `RANGE 120000 STEP 60000`
- Subwindow / chunk window: `RANGE 60000 STEP 30000`
- Aggregation: `AVG`
- Deterministic event time: enabled
- Finite replay duration: `300s`
- Debug logging: off
- Process-tree resource sampler: enabled
- Chunked comparable output only: enabled
- Chunked immediate trigger: disabled
- Window semantics: trailing
- CPU normalization core count: 10

- Final comparable rows are the normalized output rows for windows 1, 2, and 3.
- Comparable latency is measured as `result_emitted_at - anchor_aligned_expected_window_close` on the normalized final row for each window.
- `expected_window_close_to_result_ms` is retained only as a diagnostic; it is anchored to approach-local query registration and is not comparable across the three approaches when ingestion starts later than registration.
- `latency_from_logical_trigger_ms` and `latency_from_window_close_ms` are reconstructed only when the direct metadata fields are internally consistent. Event-time-domain direct fields are not treated as wall-clock latency.
- RSS is measured from the process-tree sampler's `tree_rss_bytes` field.
- CPU is computed from adjacent process-tree samples: `delta tree_cpu_seconds / delta wall time`, normalized by core count and reported as percent of total machine capacity.
- Accuracy is computed only against fetching and only on overlapping window numbers.

## Methodology

### Exact commands run

The runs used the same fixed benchmark envelope. The iteration-specific differences were `LOG_PATH`, `SESSION_ID`, `STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX`, and `BENCHMARK_ITERATION`.

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i1/fetching SESSION_ID=one-pattern-three-approach-n3-fetching-i1 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i1/approximation SESSION_ID=one-pattern-three-approach-n3-approximation-i1 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i1/chunked SESSION_ID=one-pattern-three-approach-n3-chunked-i1 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i2/fetching SESSION_ID=one-pattern-three-approach-n3-fetching-i2 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=2 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i2/approximation SESSION_ID=one-pattern-three-approach-n3-approximation-i2 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=2 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i2/chunked SESSION_ID=one-pattern-three-approach-n3-chunked-i2 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=2 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i3/fetching SESSION_ID=one-pattern-three-approach-n3-fetching-i3 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=3 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i3/approximation SESSION_ID=one-pattern-three-approach-n3-approximation-i3 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=3 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-three-approach-n3/i3/chunked SESSION_ID=one-pattern-three-approach-n3-chunked-i3 BENCHMARK_SCENARIO=one-pattern-three-approach-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=3 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

### Artifact paths by approach and iteration

#### Fetching

- Iteration 1
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/fetching_client_side_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration1/run_summary.json)

- Iteration 2
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/fetching_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/fetching_client_side_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/fetching_client_side_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/fetching_client_side_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration2/run_summary.json)

- Iteration 3
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/fetching_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/fetching_client_side_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/fetching_client_side_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/fetching_client_side_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/fetching-client-side/iteration3/run_summary.json)

#### Approximation

- Iteration 1
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/approximation_approach_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration1/run_summary.json)

- Iteration 2
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/approximation_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/approximation_approach_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/approximation_approach_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/approximation_approach_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration2/run_summary.json)

- Iteration 3
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/approximation_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/approximation_approach_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/approximation_approach_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/approximation_approach_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach/iteration3/run_summary.json)

#### Chunked

- Iteration 1
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/chunked_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_process_tree_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/streaming_query_hive_process_tree_resource_log.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration1/run_summary.json)

- Iteration 2
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/chunked_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/streaming_query_hive_resource_log.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/streaming_query_hive_process_tree_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/streaming_query_hive_process_tree_resource_log.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration2/run_summary.json)

- Iteration 3
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/chunked_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/streaming_query_hive_resource_log.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/streaming_query_hive_process_tree_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/streaming_query_hive_process_tree_resource_log.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/streaming-query-hive/iteration3/run_summary.json)

## Limitations, Uncertainty, and Robustness Checks

- The benchmark replays were finite-duration controlled terminations, not full-source replays.
- The report uses process-tree resources because primary-process-only logs undercounted approximation and chunked earlier in the investigation.
- MQTT traffic counts are derived from the benchmark traffic trace, not broker-internal delivery counters.
- Window 1 is still a comparable final row, but it is the first window and is the closest thing to a warmup in the three-window comparison.
- The latency comparison is only valid on the shared `anchor_aligned_window_close_to_result_ms` basis. The earlier registration-anchored comparison mixed different start offsets and was not apples-to-apples.

## Recommended Next Steps

- Use the repaired n=3 run as the smoke test gate before scaling to n=30.
- Keep the process-tree resource sampler in the benchmark harness for all three approaches.
- Reuse the same 10 Hz / 300s envelope for the paper run unless a new paper-scale control change is explicitly planned.

## Further Questions

- Do you want the n=30 paper benchmark to keep the same 10 Hz envelope or return to the original rate sweep?
- Do you want the report split into a methods appendix plus a shorter decision summary for paper insertion?
