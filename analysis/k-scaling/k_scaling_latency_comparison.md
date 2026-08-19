# K-scaling latency comparison

## Method

- K is encoded in the directory name (`K1`, `K2`, `K4`, `K8`, `K16`, `K32`).
- K=16 is excluded from the aggregate tables because it was only a single stress-point run.
- The benchmark uses `RANGE 120000 STEP 60000`, so the first complete result is expected around 120s plus processing delay, not around 60s.
- The ~156s window-level average comes from averaging two emitted windows, roughly one at 126-128s and one at 186-188s.
- Fetching uses `analysis/k-scaling/raw/fetching/.../fetching_latency_log.csv` when present.
- Chunked uses `analysis/k-scaling/raw/chunked/.../chunked_latency_log_consumer_1.csv` when present, plus `chunked_parent_partial_latency_log_consumer_1.csv` for chunks used.
- First-result latency uses the earliest emitted window per run.
- Window-level latency averages all emitted windows in each run.
- Post-window delay uses `delay_past_expected_close_ms`; chunked also exposes `window_close_to_ready_ms` and `ready_to_emit_ms`.

## First emitted result latency

| approach | K | n | mean (ms) | median (ms) | std (ms) | min (ms) | max (ms) | p95 (ms) | source file |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 1 | 10 | 127794.2 | 127789.5 | 21.128 | 127766 | 127830 | 127824.6 | `analysis/k-scaling/raw/fetching/K1/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 2 | 10 | 128585.1 | 128599.5 | 80.444 | 128464 | 128680 | 128678.65 | `analysis/k-scaling/raw/fetching/K2/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 4 | 10 | 130285.5 | 130263.5 | 74.587 | 130197 | 130398 | 130395.75 | `analysis/k-scaling/raw/fetching/K4/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 8 | 10 | 133498.9 | 133503.5 | 154.183 | 133213 | 133712 | 133689.95 | `analysis/k-scaling/raw/fetching/K8/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 32 | 10 | 157551.9 | 157598 | 358.612 | 156973 | 158168 | 158031.2 | `analysis/k-scaling/raw/fetching/K32/low_variability/iteration1/fetching_latency_log.csv` |
| chunked | 1 | 10 | 125659 | 125673.5 | 38.422 | 125602 | 125700 | 125697.3 | `analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 2 | 10 | 125655.9 | 125668 | 54.71 | 125576 | 125746 | 125723.05 | `analysis/k-scaling/raw/chunked/K2/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 4 | 10 | 125651.2 | 125646.5 | 57.902 | 125594 | 125748 | 125744.85 | `analysis/k-scaling/raw/chunked/K4/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 8 | 10 | 125664.3 | 125679.5 | 61.554 | 125568 | 125741 | 125737.4 | `analysis/k-scaling/raw/chunked/K8/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 32 | 10 | 125666.1 | 125666 | 44.943 | 125570 | 125753 | 125721.95 | `analysis/k-scaling/raw/chunked/K32/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |

## Window-level registration-to-result latency

| approach | K | n | mean (ms) | median (ms) | std (ms) | min (ms) | max (ms) | p95 (ms) | source file |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 1 | 10 | 157997 | 157991.5 | 24.889 | 157962 | 158041 | 158038.075 | `analysis/k-scaling/raw/fetching/K1/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 2 | 10 | 158816.2 | 158823.5 | 80.523 | 158690.5 | 158923 | 158908.375 | `analysis/k-scaling/raw/fetching/K2/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 4 | 10 | 160544.2 | 160551.25 | 62.348 | 160423 | 160646 | 160627.55 | `analysis/k-scaling/raw/fetching/K4/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 8 | 10 | 163731.9 | 163739 | 97.855 | 163570 | 163890 | 163862.55 | `analysis/k-scaling/raw/fetching/K8/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 32 | 10 | 187979.05 | 188004.25 | 259.818 | 187443.5 | 188337 | 188304.825 | `analysis/k-scaling/raw/fetching/K32/low_variability/iteration1/fetching_latency_log.csv` |
| chunked | 1 | 10 | 155900.5 | 155914.25 | 42.441 | 155829.5 | 155942.5 | 155941.375 | `analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 2 | 10 | 155907.6 | 155916.5 | 60.041 | 155821 | 156010.5 | 155981.7 | `analysis/k-scaling/raw/chunked/K2/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 4 | 10 | 155906.75 | 155905.25 | 56.418 | 155840.5 | 155998 | 155991.475 | `analysis/k-scaling/raw/chunked/K4/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 8 | 10 | 155915.5 | 155919.5 | 65.712 | 155817.5 | 156005.5 | 156001 | `analysis/k-scaling/raw/chunked/K8/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 32 | 10 | 155913.7 | 155916 | 52.805 | 155820.5 | 156031 | 155984.425 | `analysis/k-scaling/raw/chunked/K32/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |

## Post-window delay

| approach | K | n | delay mean (ms) | delay median (ms) | delay std (ms) | delay min (ms) | delay max (ms) | delay p95 (ms) | ready mean (ms) | ready-to-emit mean (ms) | source file |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fetching | 1 | 10 | 7997 | 7991.5 | 24.889 | 7962 | 8041 | 8038.075 |  |  | `analysis/k-scaling/raw/fetching/K1/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 2 | 10 | 8816.2 | 8823.5 | 80.523 | 8690.5 | 8923 | 8908.375 |  |  | `analysis/k-scaling/raw/fetching/K2/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 4 | 10 | 10544.2 | 10551.25 | 62.348 | 10423 | 10646 | 10627.55 |  |  | `analysis/k-scaling/raw/fetching/K4/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 8 | 10 | 13731.9 | 13739 | 97.855 | 13570 | 13890 | 13862.55 |  |  | `analysis/k-scaling/raw/fetching/K8/low_variability/iteration1/fetching_latency_log.csv` |
| fetching | 32 | 10 | 37979.05 | 38004.25 | 259.818 | 37443.5 | 38337 | 38304.825 |  |  | `analysis/k-scaling/raw/fetching/K32/low_variability/iteration1/fetching_latency_log.csv` |
| chunked | 1 | 10 | 5900.5 | 5914.25 | 42.441 | 5829.5 | 5942.5 | 5941.375 | 5868.4 | 32.1 | `analysis/k-scaling/raw/chunked/K1/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 2 | 10 | 5907.6 | 5916.5 | 60.041 | 5821 | 6010.5 | 5981.7 | 5870.7 | 36.9 | `analysis/k-scaling/raw/chunked/K2/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 4 | 10 | 5906.75 | 5905.25 | 56.418 | 5840.5 | 5998 | 5991.475 | 5878.05 | 28.7 | `analysis/k-scaling/raw/chunked/K4/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 8 | 10 | 5915.5 | 5919.5 | 65.712 | 5817.5 | 6005.5 | 6001 | 5885.25 | 30.25 | `analysis/k-scaling/raw/chunked/K8/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |
| chunked | 32 | 10 | 5913.7 | 5916 | 52.805 | 5820.5 | 6031 | 5984.425 | 5874.35 | 39.35 | `analysis/k-scaling/raw/chunked/K32/low_variability/iteration1/chunked_latency_log_consumer_1.csv` |

## Chunked supporting metrics

| K | n | ready mean (ms) | ready median (ms) | ready-to-emit mean (ms) | ready-to-emit median (ms) | recomposition mean (ms) | recomposition median (ms) | chunks used mean | chunks used median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 10 | 5868.4 | 5877 | 32.1 | 34.25 | 1.05 | 1 | 2 | 2 |
| 2 | 10 | 5870.7 | 5871.75 | 36.9 | 45.25 | 1.25 | 1 | 2 | 2 |
| 4 | 10 | 5878.05 | 5870.25 | 28.7 | 35 | 1.35 | 1 | 2 | 2 |
| 8 | 10 | 5885.25 | 5875.25 | 30.25 | 43.5 | 1.3 | 1 | 2 | 2 |
| 32 | 10 | 5874.35 | 5871.5 | 39.35 | 46 | 1.75 | 2 | 2 | 2 |

## Interpretation

1. Chunked latency is stable as K increases: the first emitted result stays around 126-128s, the window-level average stays around 156s, and the post-window delay stays around 5.9s.
2. Fetching first-result latency is also around 127-158s depending on K, while the window-level average rises from about 158.0s at K=1 to about 188.0s at K=32.
3. The current comparison is fair for end-to-end timing, but it is only approximate for readiness mechanics because chunked exposes additional readiness columns that fetching does not.
4. The exact raw columns used were `query_registered_at` and `result_emitted_at` for first-result and window-level latency; `delay_past_expected_close_ms` for post-window delay; and `window_close_to_ready_ms` / `ready_to_emit_ms` plus `computation_ms` and `chunks_used` for chunked.
