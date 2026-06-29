# One-Pattern Latency-Fixed 15-Window Steady-State 4Hz Compact Summary

## Result

- Fetching, approximation, and chunked each emitted windows `1..15` exactly once.
- No approach emitted window `16`.
- Approximation remained on the structured branch only with legacy fallback `0`.
- Approximation values were identical to the non-compact approximation rerun for all `15` windows.
- Chunked remained exact vs fetching to floating-point precision.
- Compact reusable-result payload mode reduced approximation reusable-result payload bytes by `62.72%`.

## Acceptance

| Check | Status | Notes |
| --- | --- | --- |
| all approaches emit windows `1..15` exactly once | pass | all three cap summaries ended at window `15` |
| no window `16` | pass | no run emitted beyond the cap |
| approximation `branch=structured` only | pass | `structured_messages_seen=64` |
| legacy fallback `0` | pass | `legacy_messages_seen=0`, `adapted_legacy_messages_seen=0`, `suppressed_missing_window_metadata=0` |
| approximation values identical or numerically equivalent to non-compact mode | pass | compact vs patched non-compact approximation: MAE `0`, RMSE `0`, MAPE `0%` |
| chunked exact vs fetching | pass | chunked vs fetching MAPE `4.92e-14%` |
| no mixed-domain latency | not met | approximation and chunked runtime CSVs still report `latency_domain_status=domain_mismatch`; comparable close-to-result latency below is mapped report-side from first raw input publish |

## Validation

| Approach | Windows | Cap summary | Domain status in runtime logs |
| --- | --- | --- | --- |
| fetching | `1..15` | `target=15 emitted=15 stopReason=target_window_count_reached` | `direct` |
| approximation | `1..15` | `target=15 emitted=15 stopReason=target_window_count_reached` | `domain_mismatch` |
| chunked | `1..15` | `target=15 emitted=15 stopReason=target_window_count_reached` | `domain_mismatch` |

Approximation branch counters:

- `legacy_messages_seen=0`
- `structured_messages_seen=64`
- `adapted_legacy_messages_seen=0`
- `suppressed_missing_window_metadata=0`

## Steady-State Slice

Windows `3..13` only.

| Approach | Close-to-result mean ± std ms | Data-available-to-result mean ± std ms | CPU-seconds total | CPU-seconds / steady-state window | Mean RSS MiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | `852.09 ± 1180.04` | `96.82 ± 64.29` | `33.43` | `3.04` | `190.04` | `285.63` |
| approximation | `270.82 ± 7.41` | `0.18 ± 0.39` | `18.27` | `1.66` | `287.46` | `351.13` |
| chunked | `237.36 ± 3.44` | `2.27 ± 0.45` | `15.35` | `1.40` | `249.71` | `335.48` |

Notes:

- Fetching had two large close-to-result spikes inside the run, especially window `6` (`4544 ms` comparable close-to-result), which inflated the steady-state mean and standard deviation.
- Approximation and chunked remained tightly clustered after report-side wall-clock mapping.

## Accuracy

| Comparison vs fetching | MAE | RMSE | MAPE | Max abs error |
| --- | ---: | ---: | ---: | ---: |
| approximation | `0.000418683740` | `0.000448092267` | `0.040892080562%` | `0.000648395238` |
| chunked | `0.000000000000` | `0.000000000000` | `0.000000000000%` | `0.000000000000` |

Compact approximation vs patched non-compact approximation:

- MAE `0`
- RMSE `0`
- MAPE `0%`

## MQTT Traffic

| Approach | Message type | Count | Payload bytes | Published bytes |
| --- | --- | ---: | ---: | ---: |
| fetching | `raw_input_stream` | 7683 | 5,016,112 | 5,530,874 |
| fetching | `superquery_result` | 15 | 2,674 | 3,019 |
| approximation | `raw_input_stream` | 7683 | 5,016,112 | 5,607,704 |
| approximation | `reusable_result` | 64 | 24,977 | 27,537 |
| approximation | `superquery_result` | 15 | 21,131 | 21,431 |
| chunked | `raw_input_stream` | 7683 | 5,016,112 | 5,561,606 |
| chunked | `chunk_result` | 64 | 146,210 | 152,674 |
| chunked | `reusable_result` | 372 | 2,997 | 17,877 |
| chunked | `superquery_result` | 15 | 2,600 | 2,690 |

Reusable-result payload reduction for approximation:

- non-compact baseline: `66,992` payload bytes across `64` reusable-result messages
- compact run: `24,977` payload bytes across `64` reusable-result messages
- reduction: `42,015` bytes (`62.72%`)

## Interpretation

- Compact reusable-result payload mode preserved approximation semantics exactly while materially reducing approximation-side reusable-result traffic.
- In this full three-approach rerun, approximation remained much cheaper than fetching on active CPU and much lower on comparable close-to-result latency.
- Chunked remained the fastest and exact approach on this workload.
- The remaining open issue is latency-domain hygiene, not correctness or compact-payload semantics: approximation and chunked still emit runtime latency rows with `domain_mismatch`, so comparable close-to-result latency still has to be reconstructed report-side.
