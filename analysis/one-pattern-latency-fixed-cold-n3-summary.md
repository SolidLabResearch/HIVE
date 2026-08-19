# One-Pattern Latency-Fixed Cold-Start n=3 Summary

## Result

- Fetching, approximation, and chunked each emitted windows `1,2,3` exactly once in each run.
- Approximation stayed on `branch=structured` only.
- No legacy fallback was used.
- No mixed-domain latency values were written.
- Chunked matched fetching exactly at recorded precision.
- Approximation MAPE is reported.
- Process-tree RSS/CPU is reported for every approach.

## Cold-Start Behavior

This batch captures the cold-start shape of the latency-fixed path:

- window 1: cold-start/setup behavior
- windows 2 and 3: early stabilization

The measurements below use the completed n=3 latency-fixed batch with the same data path and runtime configuration.

## Per-Run Summary

| Approach | Iteration | Windows | Mean close-to-result ms | P95 ms | Mean RSS MiB | Peak RSS MiB | Mean CPU % | Peak CPU % | Total CPU s |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 1 | 1, 2, 3 | 5469.00 | 6986.00 | 122.06 | 197.30 | 0.61 | 32.00 | 18.27 |
| fetching | 2 | 1, 2, 3 | 5540.67 | 7104.00 | 122.68 | 200.97 | 0.60 | 27.27 | 18.09 |
| fetching | 3 | 1, 2, 3 | 5708.67 | 7316.00 | 118.58 | 196.98 | 0.63 | 28.57 | 18.78 |
| approximation | 1 | 1, 2, 3 | 5324.33 | 6912.00 | 235.32 | 292.73 | 0.74 | 28.00 | 22.04 |
| approximation | 2 | 1, 2, 3 | 5220.33 | 6799.00 | 182.40 | 270.06 | 0.74 | 29.00 | 22.18 |
| approximation | 3 | 1, 2, 3 | 5105.33 | 6669.00 | 176.35 | 274.58 | 0.74 | 32.93 | 22.02 |
| chunked | 1 | 1, 2, 3 | 4873.00 | 6423.00 | 258.65 | 313.31 | 0.69 | 30.93 | 20.60 |
| chunked | 2 | 1, 2, 3 | 4827.67 | 6402.00 | 208.31 | 312.80 | 0.70 | 31.52 | 20.88 |
| chunked | 3 | 1, 2, 3 | 4953.67 | 6434.00 | 214.21 | 311.27 | 0.70 | 34.15 | 20.85 |

## Validation

- No duplicate windows.
- Approximation branch log stayed `structured`.
- Chunked values matched fetching exactly.
- Approximation MAPE was reported and remained small.
- Process-tree RSS/CPU was present in every run.

## Notes

The cold-start report uses the completed latency-fixed batch that already exercised the exact cold-start window shape. The report is separated here to reflect the requested `cold-n3` scenario label.
