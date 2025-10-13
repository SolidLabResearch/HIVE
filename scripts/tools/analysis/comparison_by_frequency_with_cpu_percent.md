# Comparative Results — per frequency (with CPU % of 10-core M4)

Assumptions:
- CPU percent is estimated as described below. For `independent` we computed percent from CPU delta across resource CSVs per iteration. For other approaches we used a heuristic: percent = (avgCPUTotal / assumed_duration_ms) / cores * 100, with assumed_duration_ms = 68134 ms and cores = 10.
- Treat streaming-query-hive raw CPU numbers with caution; units may differ.

---

## Frequency: 4Hz


| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|
| fetching-client-side | 62,415.33 | 3,439.37 | 3,080.67 | 0.50% | 103.59 | 40.51 |  |
| streaming-query-hive | N/A | 1,428,119.32 | 1,604.67 | 209.60% | 120.81 | 41.07 |  |
| approximation-approach | 62,108.67 | 3,370.17 | 1,489.67 | 0.49% | 114.43 | 43.53 |  |
| independent | 65,123.67 | 6,676.88 | 2,003.00 | 0.59% | 219.28 | 173.94 |  |

---

## Frequency: 8Hz


| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|
| fetching-client-side | 62,475.00 | 4,302.53 | 1,493.00 | 0.63% | 114.90 | 44.76 |  |
| streaming-query-hive | N/A | 2,438,471.30 | 1,501.33 | 357.89% | 123.34 | 50.78 |  |
| approximation-approach | 62,043.67 | 4,928.53 | 1,488.33 | 0.72% | 120.82 | 47.64 |  |
| independent | 65,388.67 | 29,454.22 | 1,978.00 | 1.59% | 281.81 | 232.65 |  |

---

## Frequency: 16Hz


| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|
| fetching-client-side | 62,627.33 | 6,882.84 | 1,491.67 | 1.01% | 124.36 | 49.02 |  |
| streaming-query-hive | N/A | 3,940,525.30 | 1,495.33 | 578.35% | 134.76 | 61.13 |  |
| approximation-approach | 62,205.67 | 7,402.76 | 1,489.67 | 1.09% | 126.65 | 53.05 |  |
| independent | 66,406.25 | 23,084.21 | 3,233.00 | 2.40% | 262.10 | 218.41 |  |

---

## Frequency: 32Hz


| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|
| fetching-client-side | 63,092.67 | 10,429.64 | 1,498.67 | 1.53% | 123.68 | 60.68 |  |
| streaming-query-hive | N/A | 6,342,353.22 | 1,490.00 | 930.86% | 132.11 | 74.37 |  |
| approximation-approach | 62,422.00 | 10,775.99 | 1,495.67 | 1.58% | 135.46 | 59.84 |  |
| independent | 66,825.33 | 157,473.50 | 62.00 | 3.23% | 622.63 | 2461.06 | UNRELIABLE (low sample count) |

---

## Frequency: 64Hz


| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|
| fetching-client-side | 63,924.67 | 12,421.93 | 1,490.33 | 1.82% | 137.30 | 81.26 |  |
| streaming-query-hive | N/A | 450,938.51 | 1,047.33 | 66.18% | 69.99 | 30.80 |  |
| approximation-approach | 62,815.33 | 13,230.99 | 1,491.00 | 1.94% | 146.28 | 72.41 |  |
| independent | N/A | 225,255.63 | 6.00 | 3.50% | 1098.90 | 3715.95 | UNRELIABLE (low sample count) |

---

## Frequency: 128Hz


| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU % (10-core) | Avg RSS (MB) | Avg Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|
| fetching-client-side | 66,133.33 | 13,903.17 | 1,481.33 | 2.04% | 149.72 | 101.03 |  |
| streaming-query-hive | N/A | N/A | N/A | N/A | N/A | N/A | No data |
| approximation-approach | 63,963.67 | 14,526.94 | 1,491.33 | 2.13% | 138.21 | 95.38 |  |
| independent | N/A | N/A | N/A | N/A | N/A | N/A | No data |
