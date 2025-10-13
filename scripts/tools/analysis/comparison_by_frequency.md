# Comparative Results — per frequency

Source: `tools/analysis/final-comprehensive-analysis.updated.json`

Notes:
- CPU-per-sample-per-core = avgCPUTotal / samples / 10 (assumes a 10-core M4 machine). This is a heuristic to make the `avgCPUTotal` values easier to compare across approaches. The underlying `avgCPUTotal` unit is not fully documented in the data and may be cumulative; treat CPU numbers as relative.
- Entries marked `N/A` or `null` indicate missing data in the JSON.
- Where sample counts are very small (e.g., < 100), the resource metrics are flagged as unreliable.

---

## Frequency: 4 Hz

| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU per-sample-per-core | Avg RSS (MB) | Avg Heap (MB) | Max RSS (MB) | Max Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fetching-client-side | 62,415.33 | 3,439.37 | 3,080.67 | 0.112 | 103.59 | 40.51 | 195.64 | 108.74 | Stable samples |
| streaming-query-hive | N/A | 1,428,119.32 | 1,604.67 | 89.000 | 120.81 | 41.07 | 182.75 | 82.65 | Missing latency; raw CPU very large (likely different unit) |
| approximation-approach | 62,108.67 | 3,370.17 | 1,489.67 | 0.226 | 114.43 | 43.53 | 156.70 | 68.59 | Stable |
| independent | 65,123.67 | 6,676.88 | 2,003 | 0.333 | 219.28 | 173.94 | 277.72 | 225.88 | Independent uses more memory at 4Hz |

**Interpretation:** latencies cluster ~62–65s. `streaming-query-hive` has CPU reported with dramatically larger raw numbers (probably cumulative); independent approach uses higher memory than the others at 4Hz.

---

## Frequency: 8 Hz

| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU per-sample-per-core | Avg RSS (MB) | Avg Heap (MB) | Max RSS (MB) | Max Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fetching-client-side | 62,475.00 | 4,302.53 | 1,493 | 0.288 | 114.90 | 44.76 | 203.61 | 120.67 | Stable |
| streaming-query-hive | N/A | 2,438,471.30 | 1,501.33 | 162.362 | 123.34 | 50.78 | 180.52 | 95.15 | Missing latency; very large CPU |
| approximation-approach | 62,043.67 | 4,928.53 | 1,488.33 | 0.331 | 120.82 | 47.64 | 176.33 | 88.65 | Best latency here |
| independent | 65,388.67 | 29,454.22 | 1,978 | 1.489 | 281.81 | 232.65 | 414.92 | 394.73 | Independent: notably higher CPU & memory at 8Hz |

**Interpretation:** approximation gives slightly lower latency; independent shows much higher resource usage at 8Hz (but sample counts are adequate).

---

## Frequency: 16 Hz

| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU per-sample-per-core | Avg RSS (MB) | Avg Heap (MB) | Max RSS (MB) | Max Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fetching-client-side | 62,627.33 | 6,882.84 | 1,491.67 | 0.461 | 124.36 | 49.02 | 226.89 | 137.40 | Stable |
| streaming-query-hive | N/A | 3,940,525.30 | 1,495.33 | 263.644 | 134.76 | 61.13 | 198.17 | 124.91 | Missing latency; raw CPU huge |
| approximation-approach | 62,205.67 | 7,402.76 | 1,491.67 | 0.497 | 126.65 | 53.05 | 191.31 | 104.92 | Similar to fetching |
| independent | 66,406.25 | 23,084.21 | 3,233 | 0.714 | 262.10 | 218.41 | 1,431.72 | 1,563.00 | Independent: higher memory & CPU, latency has more variance (4 latency entries) |

**Interpretation:** approximation & fetching similar latency. Independent shows larger memory footprint and higher CPU but also more latency variance.

---

## Frequency: 32 Hz

> WARNING: Independent resource sample count is very small (62 samples). Treat independent metrics at 32Hz as unreliable.

| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU per-sample-per-core | Avg RSS (MB) | Avg Heap (MB) | Max RSS (MB) | Max Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fetching-client-side | 63,092.67 | 10,429.64 | 1,498.67 | 0.696 | 123.68 | 60.68 | 355.23 | 266.99 | Stable |
| streaming-query-hive | N/A | 6,342,353.22 | 1,490 | 425.692 | 132.11 | 74.37 | 264.16 | 189.82 | Missing latency; raw CPU enormous |
| approximation-approach | 62,422.00 | 10,775.99 | 1,495.67 | 0.721 | 135.46 | 59.84 | 213.22 | 130.39 | Best latency here |
| independent | 66,825.33 | 157,473.50 | 62 | 254.054 | 622.63 | 2,461.06 | 2,061.89 | 3,520.96 | UNRELIABLE: only 62 samples — treat numbers cautiously |

**Interpretation:** fetching/approximation remain low-latency; independent shows large CPU & memory but sample count is too small to trust.

---

## Frequency: 64 Hz

> WARNING: Independent resource sample count is extremely small (6 samples). Metrics are unreliable.

| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU per-sample-per-core | Avg RSS (MB) | Avg Heap (MB) | Max RSS (MB) | Max Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fetching-client-side | 63,924.67 | 12,421.93 | 1,490.33 | 0.834 | 137.30 | 81.26 | 495.52 | 444.38 | Stable |
| streaming-query-hive | N/A | 450,938.51 | 1,047.33 | 43.068 | 69.99 | 30.80 | 133.69 | 34.20 | Missing latency; raw CPU lower than in other frequencies (inconsistent) |
| approximation-approach | 62,815.33 | 13,230.99 | 1,491.00 | 0.888 | 146.28 | 72.41 | 260.72 | 190.54 | Best latency here |
| independent | N/A | 225,255.63 | 6 | 3,754.260 | 1,098.90 | 3,715.95 | 2,237.02 | 3,824.64 | UNRELIABLE — only 6 samples and no latency recorded |

**Interpretation:** approximation/fetching perform similarly. Independent data at 64Hz is insufficient for reliable comparison.

---

## Frequency: 128 Hz

| Approach | Avg Latency (ms) | Avg CPU (raw) | Samples | CPU per-sample-per-core | Avg RSS (MB) | Avg Heap (MB) | Max RSS (MB) | Max Heap (MB) | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| fetching-client-side | 66,133.33 | 13,903.17 | 1,481.33 | 0.939 | 149.72 | 101.03 | 717.58 | 697.32 | Stable |
| streaming-query-hive | N/A | (null) | — | — | — | — | — | — | No data in JSON |
| approximation-approach | 63,963.67 | 14,526.94 | 1,491.33 | 0.974 | 138.21 | 95.38 | 360.31 | 295.27 | Best latency here |
| independent | N/A | (null) | — | — | — | — | — | — | No independent data for 128Hz |

**Interpretation:** approximation gives the lowest latency; independent/streaming missing.

---

# Overall summary

- Latency: `approximation-approach` and `fetching-client-side` are consistently close; `approximation` often has the smallest average latency. `independent` where available shows slightly larger latency (and larger variance).
- CPU: raw `avgCPUTotal` for `streaming-query-hive` is orders of magnitude larger than other approaches at several frequencies — likely due to different units or cumulative accumulation. I used CPU-per-sample-per-core = avgCPUTotal / samples / 10 to offer a relative comparison; results should be interpreted cautiously until `avgCPUTotal` units are confirmed.
- Memory: `independent` reports much higher RSS/heap at several frequencies (8–32Hz), indicating higher memory usage in the independent processing implementation. However, several of these independent measurements have very low sample counts at high frequencies and are thus unreliable.

# Recommendations

1. Confirm the unit/definition of `avgCPUTotal` in the JSON sources. If it is cumulative CPU-time, convert to CPU% over the runtime window for fair comparison.
2. Re-run independent experiments for 32Hz and 64Hz to collect more resource samples (the current sample counts are too low for reliable conclusions).
3. If you want this report committed, I can save it into the repo at `tools/analysis/comparison_by_frequency.md` (already done). I can also generate a condensed PDF or add plots inline.

---

Generated from `tools/analysis/final-comprehensive-analysis.updated.json` and independent logs under `tools/logs/independent-stream-processing/`.
