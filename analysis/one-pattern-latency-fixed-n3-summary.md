# One-Pattern Latency-Fixed n=3 Validation Summary

## Result

- 9/9 runs exited cleanly.
- Fetching, approximation, and chunked each emitted windows `1,2,3` exactly once in every iteration.
- Approximation stayed on `branch=structured` only.
- No legacy fallback was used.
- No mixed-domain latency values were written in the raw runtime CSVs.
- Chunked values matched fetching exactly at recorded precision.
- Approximation MAPE is reported and remains small.
- Process-tree RSS/CPU is reported for every approach.

## Benchmark Envelope

- Scenario: `one-pattern-latency-fixed-n3`
- Approaches: `fetching`, `approximation`, `chunked`
- `WEARABLE_FREQUENCY=10`
- Finite replay duration: `300s`
- `OUTPUT_WINDOW_RANGE=120000`
- `OUTPUT_WINDOW_STEP=60000`
- `SUB_WINDOW_RANGE=60000`
- `SUB_WINDOW_STEP=30000`
- Aggregation: `AVG`
- Deterministic event time: enabled
- Process-tree sampler: enabled
- Approximation completed-window mode: enabled
- Approximation early-trigger mode: disabled
- Chunked comparable output only: enabled
- Chunked semantic-ready/immediate trigger: enabled
- Chunked cadence-only: disabled

## Per-Run Validation

| Approach | Iteration | Final rows | Windows | Mean close-to-result ms | P95 ms | Mean RSS MiB | Peak RSS MiB | Mean CPU % | Peak CPU % | Total CPU s |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 1 | 3 | 1, 2, 3 | 5469.00 | 6986.00 | 122.06 | 197.30 | 0.61 | 32.00 | 18.27 |
| fetching | 2 | 3 | 1, 2, 3 | 5540.67 | 7104.00 | 122.68 | 200.97 | 0.60 | 27.27 | 18.09 |
| fetching | 3 | 3 | 1, 2, 3 | 5708.67 | 7316.00 | 118.58 | 196.98 | 0.63 | 28.57 | 18.78 |
| approximation | 1 | 3 | 1, 2, 3 | 5324.33 | 6912.00 | 235.32 | 292.73 | 0.74 | 28.00 | 22.04 |
| approximation | 2 | 3 | 1, 2, 3 | 5220.33 | 6799.00 | 182.40 | 270.06 | 0.74 | 29.00 | 22.18 |
| approximation | 3 | 3 | 1, 2, 3 | 5105.33 | 6669.00 | 176.35 | 274.58 | 0.74 | 32.93 | 22.02 |
| chunked | 1 | 3 | 1, 2, 3 | 4873.00 | 6423.00 | 258.65 | 313.31 | 0.69 | 30.93 | 20.60 |
| chunked | 2 | 3 | 1, 2, 3 | 4827.67 | 6402.00 | 208.31 | 312.80 | 0.70 | 31.52 | 20.88 |
| chunked | 3 | 3 | 1, 2, 3 | 4953.67 | 6434.00 | 214.21 | 311.27 | 0.70 | 34.15 | 20.85 |

## Branch And Domain Checks

Approximation runtime log:

```text
Approximation mode configuration: {"completedWindowMode":true,"earlyTriggerMode":false}
Approximation branch decision: topic=chunked/43eccb2e15dda5f97f3f09904b2f5dfc branch=structured window_start=1756122905256 window_end=1756122965256
Approximation branch decision: topic=chunked/bf5f09395e17db1b5fc2904cf4673f69 branch=structured window_start=1756122905256 window_end=1756122965256
Approximation message counters: {"legacy_messages_seen":0,"structured_messages_seen":18,"adapted_legacy_messages_seen":0,"suppressed_missing_window_metadata":0}
```

Chunked runtime diagnostics:

- raw runtime rows were marked `latency_domain_status=domain_mismatch`
- no mixed event-time / wall-clock subtraction was written
- `ready_to_emit_ms` remained near zero

## Accuracy

| Comparison | MAE | RMSE | MAPE |
| --- | ---: | ---: | ---: |
| approximation vs fetching | 0.0004 | 0.0004 | 0.03963063795355186 |
| chunked vs fetching | 0.0000 | 0.0000 | ~0 |

## Final Comparison

| Metric | Value |
| --- | ---: |
| fetching close-to-result | 5572.78 ± 100.44 ms |
| approximation close-to-result | 5216.67 ± 89.44 ms |
| chunked close-to-result | 4884.78 ± 52.11 ms |
| approximation MAPE | 0.03963063795355186% |
| chunked MAPE | ~0% |

## Conclusion

The n=3 latency-fixed benchmark passed end-to-end with clean exits, stable windows, plausible close-to-result latency, exact chunked results, and structured-only approximation branching.
