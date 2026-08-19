# K-scaling first emitted result latency

This table uses the earliest emitted row in each run, so it reflects the first complete result after `RANGE 120000 STEP 60000` rather than the mean of all emitted windows.

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

Because the benchmark uses `RANGE 120000 STEP 60000`, the first complete result is expected around 120s plus processing delay, not around 60s.
The current ~156s window-level average comes from averaging two emitted windows, roughly one at 126-128s and one at 186-188s.
