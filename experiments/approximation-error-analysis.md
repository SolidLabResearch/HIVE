# Why the Approximation Approach Produces Large Errors Under Asymmetric Sensor Rates

## 1. Background

The Approximation approach decomposes a multi-sensor output query into per-sensor sub-queries, collects sub-window results from an RSP engine, and reconstructs the output query result by merging overlapping sub-windows and combining cross-sensor values. This document explains why the approach produces errors of 323–863% when sensors publish at different rates (e.g., smartphone at 4 Hz, wearable at 1 Hz), while performing adequately (0.05–2.16% error) at equal rates.

## 2. The Output Query

The system evaluates a SPARQL-based streaming query over two accelerometer streams:

```sparql
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW wearableX ON STREAM wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW smartphoneX ON STREAM smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    { WINDOW wearableX { ?s1 hasValue ?value } }
    UNION
    { WINDOW smartphoneX { ?s2 hasValue ?value } }
}
```

This query computes `AVG(?value)` over the **union** of all observations from both sensors within a 120-second sliding window. The correct result is a **count-weighted average**: every individual observation contributes equally, regardless of which sensor produced it.

## 3. How the Approximation Computes Its Result

The Approximation approach processes results in two stages:

### Stage 1: Per-Sensor Sub-Window Aggregation

Each sensor has its own sub-query with a smaller window (RANGE 60s, STEP 30s). The RSP engine produces sub-window AVGs — for example:
- `wearableX` sub-window result: AVG of all wearable readings in that 60s window
- `smartphoneX` sub-window result: AVG of all smartphone readings in that 60s window

These sub-window results are merged into the output window's timeframe using duration-weighted overlap in `mergeMultipleSlidingWindowResults()` (line 821 of `RateBasedApproximationApproachOperator.ts`):

```
per_sensor_avg = (sub_window_1_avg × overlap_duration_1 + sub_window_2_avg × overlap_duration_2) / total_overlap_duration
```

This stage is mathematically reasonable — it correctly reconstructs a temporal average for each individual sensor.

### Stage 2: Cross-Sensor Unification (The Flaw)

The per-sensor results are stored in `globalLatestValues` — a map that holds the most recent sub-window result from each sensor (line 512):

```typescript
globalLatestValues.set(topic, { value: value, timestamp: now });
```

When the output window triggers, the code retrieves all values from `globalLatestValues` and combines them using a **simple arithmetic mean** (lines 748–751):

```typescript
case "AVG":
default:
    unifiedResult =
        allAvailableValues.reduce((sum, val) => sum + val, 0) /
        allAvailableValues.length;
    break;
```

With two sensors, this computes:

```
approximation_result = (per_sensor_avg_phone + per_sensor_avg_wearable) / 2
```

**Each sensor is weighted equally (50/50), regardless of how many observations it contributed.**

## 4. What the Correct Result Should Be

The Fetching approach processes all raw observations directly. The RSP engine evaluates the UNION query over the complete set of observations, producing:

```
correct_result = (sum of ALL observations) / (total count of ALL observations)
```

This is equivalent to a **count-weighted** combination of per-sensor averages:

```
correct_result = (n_phone × avg_phone + n_wearable × avg_wearable) / (n_phone + n_wearable)
```

where `n_phone` and `n_wearable` are the number of observations from each sensor in the window.

## 5. Why the Error is Small at Equal Rates (4 Hz / 4 Hz)

When both sensors publish at 4 Hz, over a 120-second window:
- `n_phone = 4 × 120 = 480`
- `n_wearable = 4 × 120 = 480`

The correct formula becomes:

```
correct = (480 × avg_phone + 480 × avg_wearable) / 960 = (avg_phone + avg_wearable) / 2
```

This is **identical** to what the Approximation computes. Equal counts make equal weighting correct. Even with different sensor means (phone ≈ +1.0, wrist ≈ −8.0), the Approximation's 50/50 weighting matches the true 50/50 count ratio. The small residual errors (0.05–2.16%) come from the temporal sub-window overlap merging, not from the cross-sensor step.

## 6. Why the Error Explodes at Asymmetric Rates (4 Hz / 1 Hz)

When the smartphone publishes at 4 Hz and the wearable at 1 Hz, over a 120-second window:
- `n_phone = 4 × 120 = 480`
- `n_wearable = 1 × 120 = 120`

The correct count-weighted formula:

```
correct = (480 × avg_phone + 120 × avg_wearable) / 600
        = 0.80 × avg_phone + 0.20 × avg_wearable
```

But the Approximation still computes:

```
approximation = 0.50 × avg_phone + 0.50 × avg_wearable
```

The Approximation gives the wearable **2.5× more weight than it should** (50% instead of 20%).

### Numerical Example: Normal Walking

With realistic sensor data (based on WISDM dataset and normative wrist accelerometer literature):
- Phone (pocket): `avg_phone ≈ +1.01 m/s²` (near-horizontal x-axis, small gravity component)
- Wrist (wearable): `avg_wearable ≈ −8.0 m/s²` (arm hangs down, large gravity projection on x-axis)

**Correct (Fetching):**
```
correct = 0.80 × (+1.01) + 0.20 × (−8.0) = 0.808 − 1.60 = −0.79
```

**Approximation:**
```
approx = 0.50 × (+1.01) + 0.50 × (−8.0) = 0.505 − 4.0 = −3.50
```

**Relative error:**
```
|−3.50 − (−0.79)| / |−0.79| = 2.71 / 0.79 = 343%
```

This matches the measured result of **334.17%** for the normal_walking pattern.

### Why the Error Percentage is So Large

The error percentage is amplified by a mathematical interaction between the sensor mean difference and the rate ratio:

```
error = |(μ_w − μ_s) × (f_s − f_w)| / |2 × (f_s × μ_s + f_w × μ_w)|
```

where:
- `μ_s`, `μ_w` = sensor means (phone, wearable)
- `f_s`, `f_w` = sensor frequencies (4 Hz, 1 Hz)

When `f_s >> f_w`, the correct result is dominated by the phone's mean (`μ_s ≈ +1.0`), making the correct value close to zero. Meanwhile, the Approximation result stays near the midpoint of the two sensor means (`≈ −3.5`). A large absolute difference divided by a near-zero correct value creates a very high relative error.

## 7. Measured Results Across Activity Patterns

All patterns use realistic sensor characteristics derived from accelerometer research literature.

### Baseline: Both Sensors at 4 Hz

| Pattern | Fetching | Approximation | Chunked | Approx Error |
|---------|----------|---------------|---------|-------------|
| sitting_resting | −2.0027 | −2.0055 | −2.0027 | 0.14% |
| normal_walking | −3.5008 | −3.4949 | −3.5008 | 0.17% |
| running | −2.5075 | −2.4534 | −2.5075 | 2.16% |
| walk_to_run | −3.4440 | −3.5145 | −3.4440 | 2.05% |
| walk_with_fall | −3.5008 | −3.5027 | −3.5008 | 0.05% |

At equal rates, all approaches produce similar results.

### Asymmetric: Phone 4 Hz, Wearable 1 Hz

| Pattern | Fetching | Approximation | Chunked | Approx Error | Chunked Error |
|---------|----------|---------------|---------|-------------|---------------|
| sitting_resting | −0.2085 | −2.0081 | −0.2085 | 862.90% | 0.00% |
| normal_walking | −0.8056 | −3.4975 | −0.8056 | 334.17% | 0.00% |
| running | −0.3782 | −2.4370 | −0.3782 | 544.37% | 0.00% |
| walk_to_run | −0.7885 | −3.4535 | −0.7737 | 338.00% | 1.88% |
| walk_with_fall | −0.8127 | −3.4410 | −0.8127 | 323.40% | 0.00% |

Note: The Approximation values (column 3) are nearly identical to the baseline values — the approach produces essentially the same output regardless of rate ratio, because it always weights both sensors equally. Meanwhile, the correct values (column 2) shift dramatically toward the phone's mean as the phone contributes 80% of the data.

### Real-World DAHCC Dataset

| Config | Fetching | Approximation | Chunked | Approx Error |
|--------|----------|---------------|---------|-------------|
| 4 Hz / 4 Hz | −10.7514 | −10.7891 | −10.7514 | 0.35% |
| 4 Hz / 2 Hz | −6.7442 | −10.6683 | −6.7442 | 58.18% |
| 4 Hz / 1 Hz | −3.4354 | −10.6766 | −3.4354 | 210.78% |

## 8. Why Chunked Remains Accurate

The Chunked approach's sub-queries carry **both the aggregate value and the observation count**:

```sparql
SELECT (AVG(?value) AS ?avgWearableX) (COUNT(?value) AS ?countWearableX) ...
```

Its reconstruction formula uses `SUM(avg × count) / SUM(count)`, which is mathematically equivalent to the true count-weighted average:

```
chunked_result = (avg_phone × count_phone + avg_wearable × count_wearable) / (count_phone + count_wearable)
```

This produces the correct result regardless of rate ratios, because the count information preserves the relative contribution of each sensor.

## 9. Root Cause Summary

| Aspect | Approximation | Chunked |
|--------|--------------|---------|
| Cross-sensor weighting | Equal (50/50) | Count-weighted |
| Count information | Discarded at sub-query level | Preserved via COUNT in sub-queries |
| Rate sensitivity | Accuracy degrades with rate asymmetry | Rate-independent |
| Configuration sensitivity | Accuracy depends on window alignment | Configuration-independent |

The fundamental issue is that the Approximation approach **discards observation count information** when it reduces each sensor's data to a single sub-window AVG. Without counts, it cannot reconstruct the correct count-weighted average and defaults to equal weighting — an assumption that only holds when all sensors publish at the same rate.

## 10. Relevance to Real-World Deployments

This limitation is significant for practical IoT and wearable computing scenarios:

1. **BLE wearables commonly operate at 1–2 Hz** due to power and bandwidth constraints, while smartphones sample at 4–10 Hz. Asymmetric rates are the norm, not the exception.

2. **Sensors at different body positions measure different absolute values.** A phone in a pocket and a watch on a wrist experience different gravity projections and dynamic accelerations. The WISDM dataset shows x-axis ranges of [−1, 0] for sitting (phone) vs [−36, −9] for the same activity (wrist), confirming that mean differences of 5–27 m/s² are typical.

3. **The Approximation's error is predictable and systematic** — it always over-represents the slower sensor. This makes the error worse precisely when the sensors measure the most different values (high activity like running), which is often the most important data to capture accurately.

## References

- WISDM Activity Recognition Dataset. Fordham University. https://www.cis.fordham.edu/wisdm/dataset.php
- Small, S. R., et al. (2021). "Normative wrist-worn accelerometer values for self-paced walking and running: a walk in the park." *Journal of Sports Sciences*, 39(sup1), 34–41. https://doi.org/10.1080/02640414.2021.1976491
- Reiss, A., & Stricker, D. (2012). "Introducing a new benchmarked dataset for activity monitoring." PAMAP2 Physical Activity Monitoring. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/231/pamap2+physical+activity+monitoring
- Ellis, K., et al. (2016). "Hip and Wrist Accelerometer Algorithms for Free-Living Behavior Classification." *Medicine & Science in Sports & Exercise*, 48(5), 933–940. https://doi.org/10.1249/MSS.0000000000000840
- Noor, M. H. M., et al. (2022). "Analyzing the Effectiveness and Contribution of Each Axis of Tri-Axial Accelerometer Sensor for Accurate Activity Recognition." *Sensors*, 20(8), 2413. https://doi.org/10.3390/s20082413
