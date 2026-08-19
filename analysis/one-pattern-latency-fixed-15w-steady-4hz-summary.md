# One-Pattern Latency-Fixed 15-Window Steady-State 4Hz Summary

## Result

- Fetching, approximation, and chunked each exited cleanly and finalized windows `1..15` exactly once.
- No approach emitted window `16`.
- Fetching accepted complete windows at `expected_event_count=960` for every finalized 120s window.
- Approximation stayed on the structured completed-window path only; `branch=legacy` never appeared.
- Chunked matched fetching exactly to floating-point precision.
- Approximation and chunked runtime diagnostics remained domain-safe by leaving runtime close-to-result blank and marking `latency_domain_status=domain_mismatch`; the comparable close-to-result numbers below are report-side wall-clock mappings from the first raw MQTT publish timestamp.

## Validation

| Approach | Windows | Duplicates | Cap summary | Domain status | Notes |
| --- | --- | --- | --- | --- | --- |
| fetching | `1..15` | none | `target=15 emitted=15 stopReason=target_window_count_reached` | `direct` | deterministic cadence bypass active |
| approximation | `1..15` | none | `target=15 emitted=15 stopReason=target_window_count_reached` | `domain_mismatch` | `branch=structured` only, branch log count `64`, legacy count `0` |
| chunked | `1..15` | none | `target=15 emitted=15 stopReason=target_window_count_reached` | `domain_mismatch` | `ready_to_emit_ms` stayed near zero |

## Window Table

| Window | Phase | Fetching close->result ms | Approx close->result ms | Chunked close->result ms | Fetching value | Approx value | Chunked value |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | warmup | 386 | 259 | 234 | 1.002233646875 | 1.002023854167 | 1.002233646875 |
| 2 | warmup | 386 | 275 | 224 | 1.005297553125 | 1.004826813095 | 1.005297553125 |
| 3 | steady | 385 | 258 | 230 | 1.009359177083 | 1.008750599405 | 1.009359177083 |
| 4 | steady | 386 | 259 | 222 | 1.012957041667 | 1.012335293155 | 1.012957041667 |
| 5 | steady | 850 | 254 | 227 | 1.016118594792 | 1.015697413393 | 1.016118594792 |
| 6 | steady | 387 | 254 | 229 | 1.018343143750 | 1.018068700298 | 1.018343143750 |
| 7 | steady | 446 | 259 | 225 | 1.020564110417 | 1.020380604762 | 1.020564110417 |
| 8 | steady | 420 | 278 | 231 | 1.024343384375 | 1.023714807738 | 1.024343384375 |
| 9 | steady | 429 | 330 | 236 | 1.027559460417 | 1.027258959226 | 1.027559460417 |
| 10 | steady | 382 | 260 | 220 | 1.030244196875 | 1.029797132738 | 1.030244196875 |
| 11 | steady | 385 | 253 | 233 | 1.034014608333 | 1.033366213095 | 1.034014608333 |
| 12 | steady | 397 | 257 | 230 | 1.037109934375 | 1.036596084524 | 1.037109934375 |
| 13 | steady | 437 | 253 | 223 | 1.039469009375 | 1.039106482738 | 1.039469009375 |
| 14 | end-edge | 410 | 269 | 231 | 1.042468332292 | 1.042292057143 | 1.042468332292 |
| 15 | end-edge | 448 | 257 | 223 | 1.045014426042 | 1.044601348214 | 1.045014426042 |

## Steady-State Slice

Windows `3..13` only.

| Approach | Close->result mean ± std ms | Slope ms/window | Replay drift mean ± std ms | Data-available->result mean ± std ms | RSS mean ± std MiB | CPU mean ± std % | MQTT raw publish lag mean / p95 / max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | `445.82 ± 138.79` | `-10.16` | `345.91 ± 151.94` | `99.91 ± 30.09` | `103.46 ± 26.84` | `4.33 ± 19.01` | `2.55 / 4 / 36` |
| approximation | `265.00 ± 22.05` | `+0.43` | `264.82 ± 22.42` | `0.18 ± 0.40` | `161.99 ± 35.19` | `4.69 ± 19.91` | `2.22 / 3 / 91` |
| chunked | `227.82 ± 4.64` | `+0.07` | `225.73 ± 4.76` | `2.09 ± 0.30` | `158.16 ± 18.85` | `0.51 ± 305.30` | `2.25 / 3 / 118` |

| Comparison vs fetching | MAE | RMSE | MAPE |
| --- | ---: | ---: | ---: |
| approximation | 0.000455488217 | 0.000480954054 | 0.044471% |
| chunked | 0.000000000000 | 0.000000000000 | 0.000000000000% |

Chunked `ready_to_emit_ms` over windows `3..13`: `2.09 ± 0.30 ms`.

## Interpretation

- The 4Hz steady-state series is effectively flat. The 10Hz run had a strong monotone drift; the 4Hz run does not.
- Approximation remains slightly faster than fetching on comparable close-to-result latency, typically by `~0.10-0.19s`.
- Chunked remains exact and is the fastest of the three on comparable close-to-result latency, typically by `~0.15-0.23s` vs fetching.
- Window `5` is the only visible fetching outlier (`850 ms`), but the broader steady-state slice still stays in the sub-second range.
