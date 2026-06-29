# One-Pattern Centered-Window n=3 Benchmark Summary

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
| fetching | 1 | 3 | 1, 2, 3 | 6007.00 ms | 7732.00 ms | 104.95 MiB | 191.50 MiB | 0.62% | 27.37% | 18.42 s | raw_input_stream=5841; reusable_result=0; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| fetching | 2 | 3 | 1, 2, 3 | 5671.33 ms | 7191.00 ms | 108.88 MiB | 198.23 MiB | 0.62% | 26.19% | 18.61 s | raw_input_stream=5851; reusable_result=0; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| fetching | 3 | 3 | 1, 2, 3 | 5591.00 ms | 7185.00 ms | 116.30 MiB | 192.30 MiB | 0.62% | 29.59% | 18.63 s | raw_input_stream=5848; reusable_result=0; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| approximation | 1 | 3 | 1, 2, 3 | -56236.00 ms | -54598.00 ms | 158.88 MiB | 269.17 MiB | 0.76% | 32.22% | 22.30 s | raw_input_stream=5845; reusable_result=108; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| approximation | 2 | 3 | 1, 2, 3 | -56387.33 ms | -54795.00 ms | 167.72 MiB | 269.16 MiB | 0.75% | 28.72% | 22.28 s | raw_input_stream=5846; reusable_result=108; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| approximation | 3 | 3 | 1, 2, 3 | -56351.33 ms | -54714.00 ms | 167.29 MiB | 268.88 MiB | 0.75% | 30.99% | 22.31 s | raw_input_stream=5845; reusable_result=108; chunk_result=0; superquery_result=3; control=1; unknown=0 |
| chunked | 1 | 3 | 1, 2, 3 | 58478.00 ms | 58479.00 ms | 128.34 MiB | 298.91 MiB | 0.72% | 31.03% | 21.22 s | raw_input_stream=5838; reusable_result=96; chunk_result=18; superquery_result=3; control=1; unknown=0 |
| chunked | 2 | 3 | 1, 2, 3 | 58415.67 ms | 58418.00 ms | 229.78 MiB | 312.33 MiB | 0.69% | 31.25% | 20.39 s | raw_input_stream=5853; reusable_result=96; chunk_result=18; superquery_result=3; control=1; unknown=0 |
| chunked | 3 | 3 | 1, 2, 3 | 58496.00 ms | 58497.00 ms | 199.16 MiB | 309.83 MiB | 0.72% | 32.22% | 21.19 s | raw_input_stream=5851; reusable_result=96; chunk_result=18; superquery_result=3; control=1; unknown=0 |

### Latency comparison basis

The main latency comparison uses the same event-window close anchor for every approach: `anchor_aligned_window_close_to_result_ms`. It is computed from the first raw input publication time in the MQTT trace plus `RANGE + (window_number - 1) * STEP`. The registration-based diagnostics are included below to show why the earlier report looked inconsistent.

| Approach | Registration -> result | Data start -> result | Registration-anchored close -> result | Anchor-aligned close -> result | Last data -> result | Post-processing delay |
| --- | --- | --- | --- | --- | --- |
| fetching | 187716.44 ± 318.30 | 185755.11 ± 179.72 | 7716.44 ± 318.30 | 5756.44 ± 180.18 | 45.67 ± 4.53 | 45.67 ± 4.53 |
| approximation | 125221.44 ± 36.38 | 92408.78 ± 129.34 | -54778.56 ± 36.38 | -56324.89 ± 64.55 | 0.22 ± 0.16 | 0.22 ± 0.16 |
| chunked | 240007.56 ± 1.13 | 207338.89 ± 43.31 | 60007.56 ± 1.13 | 58463.22 ± 34.42 | 22627.33 ± 296.19 | 22627.33 ± 296.19 |

### Row-level latency diagnostics

| Approach | Iteration | Window | query_registered_at | first_data_received_at | expected_window_close | raw_input_first_published_at | anchor_aligned_expected_window_close | last_data_received_at | result_emitted_at | latency_from_query_reg_ms | latency_from_data_start_ms | latency_from_last_data_ms | expected_window_close_to_result_ms | anchor_aligned_window_close_to_result_ms | post_processing_delay_ms | window_semantics | logical_trigger_time | window_start | window_end | window_data_close_time | latency_from_logical_trigger_ms | latency_from_window_close_ms | metadata_source |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| fetching | 1 | 1 | 1782220507617 | 1782220509719 | 1782220627617 | 1782220509717 | 1782220629717 | 1782220633901 | 1782220633973 | 126356 | 124254 | 72 | 6356 | 4256 | 72 | centered | 1756122965256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| fetching | 1 | 2 | 1782220507617 | 1782220509719 | 1782220687617 | 1782220509717 | 1782220689717 | 1782220695718 | 1782220695750 | 188133 | 186031 | 32 | 8133 | 6033 | 32 | centered | 1756123025256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| fetching | 1 | 3 | 1782220507617 | 1782220509719 | 1782220747617 | 1782220509717 | 1782220749717 | 1782220757423 | 1782220757449 | 249832 | 247730 | 26 | 9832 | 7732 | 26 | centered | 1756123085256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| fetching | 2 | 1 | 1782221423600 | 1782221425257 | 1782221543600 | 1782221425256 | 1782221545256 | 1782221549422 | 1782221549467 | 125867 | 124210 | 45 | 5867 | 4211 | 45 | centered | 1756122965256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| fetching | 2 | 2 | 1782221423600 | 1782221425257 | 1782221603600 | 1782221425256 | 1782221605256 | 1782221610829 | 1782221610868 | 187268 | 185611 | 39 | 7268 | 5612 | 39 | centered | 1756123025256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| fetching | 2 | 3 | 1782221423600 | 1782221425257 | 1782221663600 | 1782221425256 | 1782221665256 | 1782221672375 | 1782221672447 | 248847 | 247190 | 72 | 8847 | 7191 | 72 | centered | 1756123085256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| fetching | 3 | 1 | 1782222338338 | 1782222340463 | 1782222458338 | 1782222340462 | 1782222460462 | 1782222464406 | 1782222464485 | 126147 | 124022 | 79 | 6147 | 4023 | 79 | centered | 1756122965256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| fetching | 3 | 2 | 1782222338338 | 1782222340463 | 1782222518338 | 1782222340462 | 1782222520462 | 1782222526023 | 1782222526027 | 187689 | 185564 | 4 | 7689 | 5565 | 4 | centered | 1756123025256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| fetching | 3 | 3 | 1782222338338 | 1782222340463 | 1782222578338 | 1782222340462 | 1782222580462 | 1782222587605 | 1782222587647 | 249309 | 247184 | 42 | 9309 | 7185 | 42 | centered | 1756123085256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| approximation | 1 | 1 | 1782220813407 | 1782220846191 | 1782220933407 | 1782220814912 | 1782220934912 | 1782220877099 | 1782220877099 | 63692 | 30908 | 0 | -56308 | -57813 | 0 | centered | 1782220873407 | 1782220813407 | 1782220933407 | 1782220933407 | 3692 | -56308 | reconstructed |
| approximation | 1 | 2 | 1782220813407 | 1782220846191 | 1782220993407 | 1782220814912 | 1782220994912 | 1782220938615 | 1782220938615 | 125208 | 92424 | 0 | -54792 | -56297 | 0 | centered | 1782220933407 | 1782220873407 | 1782220993407 | 1782220993407 | 5208 | -54792 | reconstructed |
| approximation | 1 | 3 | 1782220813407 | 1782220846191 | 1782221053407 | 1782220814912 | 1782221054912 | 1782221000314 | 1782221000314 | 186907 | 154123 | 0 | -53093 | -54598 | 0 | centered | 1782220993407 | 1782220933407 | 1782221053407 | 1782221053407 | 6907 | -53093 | reconstructed |
| approximation | 2 | 1 | 1782221728640 | 1782221761594 | 1782221848640 | 1782221730208 | 1782221850208 | 1782221792265 | 1782221792266 | 63626 | 30672 | 1 | -56374 | -57942 | 1 | centered | 1782221788640 | 1782221728640 | 1782221848640 | 1782221848640 | 3626 | -56374 | reconstructed |
| approximation | 2 | 2 | 1782221728640 | 1782221761594 | 1782221908640 | 1782221730208 | 1782221910208 | 1782221853783 | 1782221853783 | 125143 | 92189 | 0 | -54857 | -56425 | 0 | centered | 1782221848640 | 1782221788640 | 1782221908640 | 1782221908640 | 5143 | -54857 | reconstructed |
| approximation | 2 | 3 | 1782221728640 | 1782221761594 | 1782221968640 | 1782221730208 | 1782221970208 | 1782221915413 | 1782221915413 | 186773 | 153819 | 0 | -53227 | -54795 | 0 | centered | 1782221908640 | 1782221848640 | 1782221968640 | 1782221968640 | 6773 | -53227 | reconstructed |
| approximation | 3 | 1 | 1782222643789 | 1782222676489 | 1782222763789 | 1782222645355 | 1782222765355 | 1782222707419 | 1782222707419 | 63630 | 30930 | 0 | -56370 | -57936 | 0 | centered | 1782222703789 | 1782222643789 | 1782222763789 | 1782222763789 | 3630 | -56370 | reconstructed |
| approximation | 3 | 2 | 1782222643789 | 1782222676489 | 1782222823789 | 1782222645355 | 1782222825355 | 1782222768951 | 1782222768951 | 125162 | 92462 | 0 | -54838 | -56404 | 0 | centered | 1782222763789 | 1782222703789 | 1782222823789 | 1782222823789 | 5162 | -54838 | reconstructed |
| approximation | 3 | 3 | 1782222643789 | 1782222676489 | 1782222883789 | 1782222645355 | 1782222885355 | 1782222830640 | 1782222830641 | 186852 | 154152 | 1 | -53148 | -54714 | 1 | centered | 1782222823789 | 1782222763789 | 1782222883789 | 1782222883789 | 6852 | -53148 | reconstructed |
| chunked | 1 | 1 | 1782221118505 | 1782221151231 | 1782221238505 | 1782221120035 | 1782221240035 | 1782221274791 | 1782221298512 | 180007 | 147281 | 23721 | 60007 | 58477 | 23721 | centered | 1756122965256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| chunked | 1 | 2 | 1782221118505 | 1782221151231 | 1782221298505 | 1782221120035 | 1782221300035 | 1782221336302 | 1782221358513 | 240008 | 207282 | 22211 | 60008 | 58478 | 22211 | centered | 1756123025256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| chunked | 1 | 3 | 1782221118505 | 1782221151231 | 1782221358505 | 1782221120035 | 1782221360035 | 1782221397819 | 1782221418514 | 300009 | 267283 | 20695 | 60009 | 58479 | 20695 | centered | 1756123085256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| chunked | 2 | 1 | 1782222033620 | 1782222066281 | 1782222153620 | 1782222035213 | 1782222155213 | 1782222189256 | 1782222213627 | 180007 | 147346 | 24371 | 60007 | 58414 | 24371 | centered | 1756122965256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| chunked | 2 | 2 | 1782222033620 | 1782222066281 | 1782222213620 | 1782222035213 | 1782222215213 | 1782222250743 | 1782222273628 | 240008 | 207347 | 22885 | 60008 | 58415 | 22885 | centered | 1756123025256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| chunked | 2 | 3 | 1782222033620 | 1782222066281 | 1782222273620 | 1782222035213 | 1782222275213 | 1782222312322 | 1782222333631 | 300011 | 267350 | 21309 | 60011 | 58418 | 21309 | centered | 1756123085256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |
| chunked | 3 | 1 | 1782222948862 | 1782222981481 | 1782223068862 | 1782222950372 | 1782223070372 | 1782223104528 | 1782223128867 | 180005 | 147386 | 24339 | 60005 | 58495 | 24339 | centered | 1756122965256 | 1756122905256 | 1756123025256 | 1756123025256 | n/a | n/a | reconstructed |
| chunked | 3 | 2 | 1782222948862 | 1782222981481 | 1782223128862 | 1782222950372 | 1782223130372 | 1782223166030 | 1782223188868 | 240006 | 207387 | 22838 | 60006 | 58496 | 22838 | centered | 1756123025256 | 1756122965256 | 1756123085256 | 1756123085256 | n/a | n/a | reconstructed |
| chunked | 3 | 3 | 1782222948862 | 1782222981481 | 1782223188862 | 1782222950372 | 1782223190372 | 1782223227592 | 1782223248869 | 300007 | 267388 | 21277 | 60007 | 58497 | 21277 | centered | 1756123085256 | 1756123025256 | 1756123145256 | 1756123145256 | n/a | n/a | reconstructed |

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
| fetching | 5756.44 ± 180.18 ms | 110.04 MiB ± 4.71 MiB | 194.01 MiB ± 3.00 MiB | 0.62 % ± 0.00 % | 27.72 % ± 1.41 % | 18.55 s ± 0.09 s | 5851 messages ± 4 messages |
| approximation | -56324.89 ± 64.55 ms | 164.63 MiB ± 4.07 MiB | 269.07 MiB ± 0.14 MiB | 0.76 % ± 0.00 % | 30.64 % ± 1.45 % | 22.30 s ± 0.01 s | 5957 messages ± 0 messages |
| chunked | 58463.22 ± 34.42 ms | 185.76 MiB ± 42.48 MiB | 307.02 MiB ± 5.83 MiB | 0.71 % ± 0.02 % | 31.50 % ± 0.52 % | 20.93 s ± 0.38 s | 5965 messages ± 7 messages |

### MQTT message mix over n=3

| Approach | Raw input | Reusable result | Chunk result | Superquery result |
| --- | --- | --- | --- | --- |
| fetching | 5846.67 messages ± 4.19 messages | 0.00 messages ± 0.00 messages | 0.00 messages ± 0.00 messages | 3.00 messages ± 0.00 messages |
| approximation | 5845.33 messages ± 0.47 messages | 108.00 messages ± 0.00 messages | 0.00 messages ± 0.00 messages | 3.00 messages ± 0.00 messages |
| chunked | 5847.33 messages ± 6.65 messages | 96.00 messages ± 0.00 messages | 18.00 messages ± 0.00 messages | 3.00 messages ± 0.00 messages |

### Accuracy against fetching, aligned by window number

| Comparison | MAE | RMSE | MAPE |
| --- | --- | --- | --- |
| approximation vs fetching | 0.0022 ± 0.0000 | 0.0022 ± 0.0000 | 0.2200 ± 0.0000 |
| chunked vs fetching | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 | 0.0000 ± 0.0000 |

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
- Window semantics: centered
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
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i1/fetching SESSION_ID=one-pattern-centered-n3-fetching-i1 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i1/approximation SESSION_ID=one-pattern-centered-n3-approximation-i1 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i1/chunked SESSION_ID=one-pattern-centered-n3-chunked-i1 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=1 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i2/fetching SESSION_ID=one-pattern-centered-n3-fetching-i2 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=2 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i2/approximation SESSION_ID=one-pattern-centered-n3-approximation-i2 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=2 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i2/chunked SESSION_ID=one-pattern-centered-n3-chunked-i2 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=2 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i3/fetching SESSION_ID=one-pattern-centered-n3-fetching-i3 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=fetching BENCHMARK_ITERATION=3 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3 node scripts/analysis-js/experiment-evaluation-fetching-client-side.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i3/approximation SESSION_ID=one-pattern-centered-n3-approximation-i3 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=approximation BENCHMARK_ITERATION=3 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3 node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

```bash
DATA_PATH=approximation_test/challenging/exponential_growth WEARABLE_FREQUENCY=10 STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_START_TIME=1756122905256 STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX=one-pattern-centered-n3/i3/chunked SESSION_ID=one-pattern-centered-n3-chunked-i3 BENCHMARK_SCENARIO=one-pattern-centered-n3 BENCHMARK_SCALE=challenging_exponential_growth BENCHMARK_APPROACH=chunked BENCHMARK_ITERATION=3 OUTPUT_WINDOW_RANGE=120000 OUTPUT_WINDOW_STEP=60000 SUB_WINDOW_RANGE=60000 SUB_WINDOW_STEP=30000 AGGREGATION_FUNCTION=AVG AGGREGATION_FUNC=AVG STREAMING_QUERY_HIVE_DEBUG_CHUNKS=0 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1 STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=300 STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS=420000 STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1 STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0 RSP_WINDOW_SEMANTICS=centered LOG_PATH=/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3 node scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
```

### Artifact paths by approach and iteration

#### Fetching

- Iteration 1
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/fetching_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/fetching_client_side_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/fetching_client_side_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/fetching_client_side_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration1/run_summary.json)

- Iteration 2
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/fetching_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/fetching_client_side_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/fetching_client_side_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/fetching_client_side_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration2/run_summary.json)

- Iteration 3
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/fetching_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/fetching_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/fetching_client_side_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/fetching_client_side_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/fetching_client_side_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/fetching_client_side_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/fetching/iteration3/run_summary.json)

#### Approximation

- Iteration 1
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/approximation_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/approximation_approach_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/approximation_approach_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/approximation_approach_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration1/run_summary.json)

- Iteration 2
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/approximation_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/approximation_approach_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/approximation_approach_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/approximation_approach_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration2/run_summary.json)

- Iteration 3
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/approximation_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/approximation_approach_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/approximation_approach_resource_usage.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/approximation_approach_process_tree_resource_usage.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/approximation_approach_process_tree_resource_usage.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/approximation/iteration3/run_summary.json)

#### Chunked

- Iteration 1
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/chunked_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/streaming_query_hive_resource_log.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/streaming_query_hive_process_tree_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/streaming_query_hive_process_tree_resource_log.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration1/run_summary.json)

- Iteration 2
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/chunked_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/streaming_query_hive_resource_log.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/streaming_query_hive_process_tree_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/streaming_query_hive_process_tree_resource_log.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration2/run_summary.json)

- Iteration 3
- Normalized final rows: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/normalized_final_rows.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/normalized_final_rows.csv)
- Latency CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/chunked_latency_log.csv)
- Primary resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/streaming_query_hive_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/streaming_query_hive_resource_log.csv)
- Process-tree resource CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/streaming_query_hive_process_tree_resource_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/streaming_query_hive_process_tree_resource_log.csv)
- MQTT traffic CSV: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/mqtt_traffic.csv](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/mqtt_traffic.csv)
- MQTT traffic NDJSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/mqtt_traffic.ndjson)
- Run summary JSON: [/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/centered-window-n3/chunked/iteration3/run_summary.json)

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
