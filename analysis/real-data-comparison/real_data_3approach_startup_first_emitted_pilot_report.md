# Real-Data 3-Approach First-Emitted Startup Summary

Input root:
`/Users/kushbisen/Code/streaming-query-hive/results/paper-benchmarks/real-data-3approach-startup-first-emitted-pilot-2026-06-30/real-data/raw`

Selected approaches exact:
`yes`

Startup metric:
First usable non-warmup emitted result row per iteration, using the best comparable startup latency field available for that row

Window policy:
- target windows per iteration: 5
- target windows are an upper bound / flush allowance, not a startup completion requirement
- warmup rows are skipped only when the row is explicitly marked `warmup=true`
- the first usable row does not need to be window 1
- first usable rows may be windows 1..5
- final-window completeness and stop reason are diagnostic only for startup-first-emitted
- accuracy is aligned against `fetching` by iteration and first emitted window number where possible

## Completeness

| Approach | Iterations found | Startup-valid first-emitted rows | Missing startup-valid rows | All diagnostic stop reasons hit target | All diagnostic final windows hit upper bound | All run summaries completed |
| --- | --- | --- | --- | --- | --- | --- |
| `fetching` | 3 | 3 | 0 | no | no | yes |
| `approximation` | 3 | 0 | 3 | yes | yes | yes |
| `chunked` | 3 | 3 | 0 | yes | yes | yes |

## Startup Latency Summary

| Approach | Startup-valid first-emitted rows | Missing startup-valid rows | First emitted window mean | First emitted window min | First emitted window max | Startup latency mean ms | Startup latency std | Latency sources | Latency domains |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `fetching` | 3 | 0 | 1.00 | 1.00 | 1.00 | 144354.67 | 143719.27 | anchorAlignedWindowCloseToResultMs:3 | missing:3 |
| `approximation` | 0 | 3 | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| `chunked` | 3 | 0 | 1.00 | 1.00 | 1.00 | 996690.00 | 141855.50 | anchorAlignedWindowCloseToResultMs:3 | wall_clock_mapped:3 |

## Per-Iteration First-Emitted Rows

| Approach | Iteration | Startup-valid | Warmup rows skipped | First emitted window | Startup latency ms | Latency source | Latency domain | Result value | Stop reason diagnostic | Final windows diagnostic | Accuracy comparable | Accuracy alignment | Accuracy note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `fetching` | 1 | yes | 0 | 1 | 0.00 | `anchorAlignedWindowCloseToResultMs` | `missing` | -12.379903 | `other` | `1,2,3 / 5` | baseline | `baseline` | `baseline_fetching` |
| `fetching` | 2 | yes | 0 | 1 | 145634.00 | `anchorAlignedWindowCloseToResultMs` | `missing` | -12.375020 | `target_window_count_reached` | `1,2,3,4,5 / 5` | baseline | `baseline` | `baseline_fetching` |
| `fetching` | 3 | yes | 0 | 1 | 287430.00 | `anchorAlignedWindowCloseToResultMs` | `missing` | -12.375020 | `target_window_count_reached` | `1,2,3,4,5 / 5` | baseline | `baseline` | `baseline_fetching` |
| `approximation` | 1 | no | 0 | 1 | n/a | `domain_mismatch` | `domain_mismatch` | -12.370800 | `target_window_count_reached` | `1,2,3,4,5 / 5` | yes | `matched_first_emitted_window` | `same_first_emitted_window` |
| `approximation` | 2 | no | 0 | 1 | n/a | `domain_mismatch` | `domain_mismatch` | -12.370800 | `target_window_count_reached` | `1,2,3,4,5 / 5` | yes | `matched_first_emitted_window` | `same_first_emitted_window` |
| `approximation` | 3 | no | 0 | 1 | n/a | `domain_mismatch` | `domain_mismatch` | -12.370800 | `target_window_count_reached` | `1,2,3,4,5 / 5` | yes | `matched_first_emitted_window` | `same_first_emitted_window` |
| `chunked` | 1 | yes | 0 | 1 | 854829.00 | `anchorAlignedWindowCloseToResultMs` | `wall_clock_mapped` | -12.375020 | `target_window_count_reached` | `1,2,3,4,5 / 5` | yes | `matched_first_emitted_window` | `same_first_emitted_window` |
| `chunked` | 2 | yes | 0 | 1 | 996701.00 | `anchorAlignedWindowCloseToResultMs` | `wall_clock_mapped` | -12.375020 | `target_window_count_reached` | `1,2,3,4,5 / 5` | yes | `matched_first_emitted_window` | `same_first_emitted_window` |
| `chunked` | 3 | yes | 0 | 1 | 1138540.00 | `anchorAlignedWindowCloseToResultMs` | `wall_clock_mapped` | -12.375020 | `target_window_count_reached` | `1,2,3,4,5 / 5` | yes | `matched_first_emitted_window` | `same_first_emitted_window` |

## Accuracy

| Approach vs fetching | Comparable iterations | Not comparable | Same first-emitted window | Window-aligned to fetching | MAE | RMSE | MAPE | Max abs error | Chunked exact |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `approximation` | 3 | 0 | 3 | 0 | 0.005848 | 0.005848 | 0.047248 | 0.009104 | n/a |
| `chunked` | 3 | 0 | 3 | 0 | 0.001628 | 0.001628 | 0.013149 | 0.004883 | no |

## Resources

| Approach | CPU seconds mean | CPU seconds std | Mean RSS MiB mean | Mean RSS MiB std | Peak RSS MiB mean | Peak RSS MiB std |
| --- | --- | --- | --- | --- | --- | --- |
| `fetching` | 3.910 | 0.521 | 170.46 | 13.65 | 237.75 | 0.28 |
| `approximation` | 4.333 | 0.145 | 222.99 | 14.31 | 323.52 | 5.56 |
| `chunked` | 3.283 | 0.105 | 222.07 | 19.79 | 334.03 | 26.82 |

## Warnings

- fetching/iteration1: diagnostic stop reason is other; startup-first-emitted only requires a usable first row
- fetching/iteration1: diagnostic final windows 1,2,3 did not reach the target-window upper bound 5

## Errors

- approximation/iteration1: first usable row has invalid comparable startup latency (latency_domain_mismatch)
- approximation/iteration2: first usable row has invalid comparable startup latency (latency_domain_mismatch)
- approximation/iteration3: first usable row has invalid comparable startup latency (latency_domain_mismatch)

