# K-scaling variability analysis

This document contains standard-deviation comparison tables detailing metric variability across K for chunked latency, resource usage, and MQTT traffic.

## Chunked latency variability

| K | n | readiness mean (ms) | readiness std (ms) | final delay mean (ms) | final delay std (ms) | recomposition mean (ms) | recomposition std (ms) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 10 | 5.868e+03 | 33.50 | 5.900e+03 | 42.44 | 1.05 | 0.55 |
| 2 | 10 | 5.871e+03 | 48.28 | 5.908e+03 | 60.04 | 1.25 | 0.75 |
| 4 | 10 | 5.878e+03 | 38.96 | 5.907e+03 | 56.42 | 1.35 | 0.85 |
| 8 | 10 | 5.885e+03 | 50.74 | 5.916e+03 | 65.71 | 1.30 | 1.11 |
| 32 | 10 | 5.874e+03 | 42.84 | 5.914e+03 | 52.80 | 1.75 | 0.82 |

## Resource variability by approach

| approach | K | n | CPU mean (%) | CPU std (%) | peak RSS mean (MB) | peak RSS std (MB) | total RSS mean (MB) | total RSS std (MB) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| chunked | 1 | 10 | 11.23 | 0.25 | 154.59 | 3.71 | 363.85 | 6.67 |
| chunked | 2 | 10 | 11.21 | 0.27 | 155.99 | 3.15 | 367.95 | 5.79 |
| chunked | 4 | 10 | 11.19 | 0.18 | 155.06 | 1.22 | 364.54 | 7.58 |
| chunked | 8 | 10 | 11.09 | 0.23 | 155.08 | 3.03 | 365.88 | 5.99 |
| chunked | 32 | 10 | 11.37 | 0.22 | 157.66 | 3.68 | 370.14 | 4.37 |
| fetching | 1 | 10 | 11.10 | 0.08 | 242.52 | 1.72 | 352.81 | 2.71 |
| fetching | 2 | 10 | 12.55 | 0.34 | 305.78 | 8.17 | 412.05 | 7.46 |
| fetching | 4 | 10 | 16.84 | 0.24 | 485.99 | 2.38 | 594.87 | 4.26 |
| fetching | 8 | 10 | 22.80 | 0.59 | 1.130e+03 | 332.57 | 1.235e+03 | 335.78 |
| fetching | 32 | 10 | 50.42 | 0.31 | 2.963e+03 | 12.04 | 3.071e+03 | 10.12 |

## MQTT variability by approach

| approach | K | n | delivery mean (KB) | delivery std (KB) | published mean (KB) | published std (KB) |
| --- | --- | --- | --- | --- | --- | --- |
| chunked | 1 | 10 | 1.274e+03 | 0.53 | 1.274e+03 | 0.53 |
| chunked | 2 | 10 | 1.274e+03 | 0.51 | 1.274e+03 | 0.51 |
| chunked | 4 | 10 | 1.274e+03 | 0.61 | 1.274e+03 | 0.61 |
| chunked | 8 | 10 | 1.274e+03 | 0.57 | 1.274e+03 | 0.57 |
| chunked | 32 | 10 | 1.274e+03 | 0.41 | 1.274e+03 | 0.41 |
| fetching | 1 | 10 | 1.239e+03 | 0.41 | 1.239e+03 | 0.41 |
| fetching | 2 | 10 | 1.240e+03 | 3.706e-03 | 1.240e+03 | 3.706e-03 |
| fetching | 4 | 10 | 1.242e+03 | 6.176e-03 | 1.242e+03 | 6.176e-03 |
| fetching | 8 | 10 | 1.247e+03 | 0.01 | 1.247e+03 | 0.01 |
| fetching | 32 | 10 | 1.275e+03 | 0.59 | 1.275e+03 | 0.59 |
