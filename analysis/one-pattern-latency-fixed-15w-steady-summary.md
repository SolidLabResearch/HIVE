# One-Pattern Latency-Fixed 15-Window Steady-State Summary

## Result

- Fetching, approximation, and chunked each exited cleanly and finalized windows `1..15` exactly once.
- No approach emitted window `16`.
- Approximation stayed on `branch=structured` only; legacy fallback counters stayed at zero.
- Chunked matched fetching exactly to recorded precision.
- Approximation runtime and chunked runtime stayed domain-safe by marking `latency_domain_status=domain_mismatch`; the comparable close-to-result numbers below are report-side wall-clock mappings from the first raw MQTT publish timestamp.

## Validation

| Approach | Windows | Duplicates | Cap summary | Domain status | Notes |
| --- | --- | --- | --- | --- | --- |
| fetching | `1..15` | none | `target=15 emitted=15 stopReason=target_window_count_reached` | `direct` | deterministic cadence bypass active |
| approximation | `1..15` | none | `target=15 emitted=15 stopReason=target_window_count_reached` | `domain_mismatch` | `branch=structured` only, counters `legacy=0 adapted_legacy=0 suppressed=0 structured=64` |
| chunked | `1..15` | none | `target=15 emitted=15 stopReason=target_window_count_reached` | `domain_mismatch` | `ready_to_emit_ms` stayed near zero |

## Window Table

| Window | Phase | Fetching close->result ms | Approx close->result ms | Chunked close->result ms | Fetching value | Approx value | Chunked value |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | warmup | 4280 | 3446 | 3388 | 1.002853491667 | 1.002504116667 | 1.002853491667 |
| 2 | warmup | 5865 | 4933 | 4856 | 1.005838936667 | 1.005415022500 | 1.005838936667 |
| 3 | steady | 7478 | 6473 | 6349 | 1.009119850000 | 1.008696943095 | 1.009119850000 |
| 4 | steady | 9148 | 8087 | 7821 | 1.012401377083 | 1.011847520595 | 1.012401377083 |
| 5 | steady | 10681 | 9571 | 9311 | 1.015658735833 | 1.015176326786 | 1.015658735833 |
| 6 | steady | 12334 | 11191 | 10920 | 1.018476496667 | 1.018212339167 | 1.018476496667 |
| 7 | steady | 13924 | 12773 | 12799 | 1.021284795417 | 1.020998177024 | 1.021284795417 |
| 8 | steady | 15538 | 14339 | 14359 | 1.024685659167 | 1.023993959048 | 1.024685659167 |
| 9 | steady | 17188 | 16068 | 15963 | 1.027476063333 | 1.027143091786 | 1.027476063333 |
| 10 | steady | 18769 | 17477 | 17473 | 1.030544782500 | 1.030245289167 | 1.030544782500 |
| 11 | steady | 20364 | 19058 | 19011 | 1.034150590833 | 1.033560065595 | 1.034150590833 |
| 12 | steady | 21937 | 20626 | 20577 | 1.036912753750 | 1.036473671786 | 1.036912753750 |
| 13 | steady | 23610 | 22213 | 22068 | 1.039579590417 | 1.039210941786 | 1.039579590417 |
| 14 | end-edge | 25184 | 23931 | 23610 | 1.042520794167 | 1.042056329643 | 1.042520794167 |
| 15 | end-edge | 26843 | 25564 | 25192 | 1.045562687083 | 1.045162351071 | 1.045562687083 |

## Steady-State Slice

Windows `3..13` only.

| Approach | Close->result mean ± std ms | RSS mean ± std MiB | CPU mean ± std % | MQTT messages | MQTT published MiB | MQTT est. delivery MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 15542.82 ± 5336.79 | 122.72 ± 25.85 | 4.90 ± 20.81 | 12156 | 8.25 | 8.25 |
| approximation | 14352.36 ± 5221.95 | 161.61 ± 28.12 | 7.68 ± 25.54 | 12178 | 8.35 | 8.35 |
| chunked | 14241.00 ± 5277.29 | 163.82 ± 22.93 | 6.91 ± 23.13 | 12428 | 8.33 | 16.54 |

| Comparison vs fetching | MAE | RMSE | MAPE |
| --- | ---: | ---: | ---: |
| approximation | 0.000430215379 | 0.000449917694 | 0.042000% |
| chunked | 0.000000000000 | 0.000000000000 | 0.000000000000% |

Chunked `ready_to_emit_ms` over windows `3..13`: `2.55 ± 0.52 ms`.

## Comparison To Cold n=3

The cold-start batch in [analysis/one-pattern-latency-fixed-cold-n3-summary.md](/Users/kushbisen/Code/streaming-query-hive/analysis/one-pattern-latency-fixed-cold-n3-summary.md) showed much lower mean close-to-result latency over windows `1..3`:

- fetching: about `5.47-5.71s`
- approximation: about `5.11-5.32s`
- chunked: about `4.83-4.95s`

In this 15-window single-run steady-state validation, the comparable wall-clock-mapped close-to-result latency rises to about `14.24-15.54s` over windows `3..13`. The longer replay accumulates additional wall-clock drift, but correctness remains intact: exact window set, no duplicates, no legacy approximation fallback, and chunked exactness vs fetching.
