# Streaming Query Approaches: Comprehensive Frequency Comparison

**Date:** February 4, 2026  
**Experiment Duration:** 180 seconds per approach per frequency  
**Window Configuration:** RANGE 120s, SLIDE 60s

## Executive Summary

This report presents a comprehensive comparison of three streaming query approaches across five different data frequencies (0.1, 0.5, 1.0, 1.5, 2.0 Hz). The comparison evaluates:

- **Accuracy**: Mean Absolute Percentage Error (MAPE) compared to the Fetching baseline
- **Resource Usage**: CPU and memory consumption
- **Latency**: Query processing and result emission times

### Approaches Tested

1. **Fetching (Client-Side)** - Baseline approach that fetches all data to the client for processing
2. **Approximation** - Rate-based estimation using sub-queries for incremental updates
3. **Chunked** - Incremental aggregation using GCD-based window chunking

---

## Results Summary

### Accuracy Comparison

| Frequency | Approach | Result Value | MAPE (%) | Absolute Error |
|-----------|----------|--------------|----------|----------------|
| 0.1 Hz | Fetching      | 51.09698     | 0.0000   | 0.000000       |
| 0.1 Hz | Approximation | 50.06942     | 2.0110   | 1.027558       |
| 0.1 Hz | Chunked       | 50.07163     | 2.0067   | 1.025347       |
| 0.5 Hz | Fetching      | 50.05904     | 0.0000   | 0.000000       |
| 0.5 Hz | Approximation | 50.09867     | 0.0792   | 0.039630       |
| 0.5 Hz | Chunked       | 50.08682     | 0.0555   | 0.027779       |
| 1.0 Hz | Fetching      | 50.02524     | 0.0000   | 0.000000       |
| 1.0 Hz | Approximation | 50.07060     | 0.0907   | 0.045361       |
| 1.0 Hz | Chunked       | 50.03821     | 0.0259   | 0.012963       |
| 1.5 Hz | Fetching      | 50.15735     | 0.0000   | 0.000000       |
| 1.5 Hz | Approximation | 49.91331     | 0.4866   | 0.244043       |
| 1.5 Hz | Chunked       | 50.03752     | 0.2389   | 0.119826       |
| 2.0 Hz | Fetching      | 49.99723     | 0.0000   | 0.000000       |
| 2.0 Hz | Approximation | 49.96808     | 0.0583   | 0.029150       |
| 2.0 Hz | Chunked       | 50.01531     | 0.0362   | 0.018083       |


### Resource Usage Comparison

| Frequency | Approach | Avg CPU User | Avg CPU Sys | Avg Memory (MB) | Peak Memory (MB) |
|-----------|----------|--------------|-------------|-----------------|------------------|
| 0.1 Hz | Fetching      | 4194.66      | 1490.85     | 47.55           | 109.50           |
| 0.1 Hz | Approximation | 3426.65      | 1019.14     | 48.17           | 73.74            |
| 0.1 Hz | Chunked       | 3440.92      | 1017.96     | 48.51           | 73.79            |
| 0.5 Hz | Fetching      | 3601.56      | 1210.23     | 45.96           | 109.50           |
| 0.5 Hz | Approximation | 3414.62      | 963.38      | 48.35           | 74.67            |
| 0.5 Hz | Chunked       | 3405.49      | 1003.23     | 48.35           | 73.79            |
| 1.0 Hz | Fetching      | 3393.79      | 1106.89     | 45.44           | 109.50           |
| 1.0 Hz | Approximation | 3481.10      | 955.37      | 47.62           | 75.17            |
| 1.0 Hz | Chunked       | 3485.04      | 985.47      | 47.87           | 76.57            |
| 1.5 Hz | Fetching      | 3272.76      | 1049.10     | 44.78           | 109.50           |
| 1.5 Hz | Approximation | 3443.94      | 947.54      | 47.75           | 75.17            |
| 1.5 Hz | Chunked       | 3490.21      | 989.35      | 48.10           | 76.57            |
| 2.0 Hz | Fetching      | 3203.21      | 1010.09     | 44.43           | 109.50           |
| 2.0 Hz | Approximation | 3390.23      | 925.61      | 47.94           | 75.17            |
| 2.0 Hz | Chunked       | 3482.44      | 977.24      | 48.18           | 76.57            |


---

## Detailed Analysis by Frequency

### 0.1 Hz - Very Low Frequency

**Characteristics:**
- Samples per cycle: 40 (at 4Hz sampling)
- Nyquist ratio: 0.05x
- Expected behavior: Excellent sampling, minimal aliasing

**Results:**

| Metric | Fetching | Approximation | Chunked |
|--------|----------|---------------|---------|
| Result Value | 51.09698 | 50.06942 | 50.07163 |
| MAPE (%) | 0.0 | 2.0110 | 2.0067 |
| Avg CPU User | 4194.66 | 3426.65 | 3440.92 |
| Avg Memory (MB) | 47.55 | 48.17 | 48.51 |
| Peak Memory (MB) | 109.50 | 73.74 | 73.79 |

---

### 0.5 Hz - Low-Medium Frequency

**Characteristics:**
- Samples per cycle: 8 (at 4Hz sampling)
- Nyquist ratio: 0.25x
- Expected behavior: Good sampling, low aliasing risk

**Results:**

| Metric | Fetching | Approximation | Chunked |
|--------|----------|---------------|---------|
| Result Value | 50.05904 | 50.09867 | 50.08682 |
| MAPE (%) | 0.0 | 0.0792 | 0.0555 |
| Avg CPU User | 3601.56 | 3414.62 | 3405.49 |
| Avg Memory (MB) | 45.96 | 48.35 | 48.35 |
| Peak Memory (MB) | 109.50 | 74.67 | 73.79 |

---

### 1.0 Hz - Medium Frequency

**Characteristics:**
- Samples per cycle: 4 (at 4Hz sampling)
- Nyquist ratio: 0.5x
- Expected behavior: Adequate sampling, some aliasing risk

**Results:**

| Metric | Fetching | Approximation | Chunked |
|--------|----------|---------------|---------|
| Result Value | 50.02524 | 50.07060 | 50.03821 |
| MAPE (%) | 0.0 | 0.0907 | 0.0259 |
| Avg CPU User | 3393.79 | 3481.10 | 3485.04 |
| Avg Memory (MB) | 45.44 | 47.62 | 47.87 |
| Peak Memory (MB) | 109.50 | 75.17 | 76.57 |

---

### 1.5 Hz - High Frequency

**Characteristics:**
- Samples per cycle: 2.67 (at 4Hz sampling)
- Nyquist ratio: 0.75x
- Expected behavior: Approaching Nyquist limit, increased aliasing

**Results:**

| Metric | Fetching | Approximation | Chunked |
|--------|----------|---------------|---------|
| Result Value | 50.15735 | 49.91331 | 50.03752 |
| MAPE (%) | 0.0 | 0.4866 | 0.2389 |
| Avg CPU User | 3272.76 | 3443.94 | 3490.21 |
| Avg Memory (MB) | 44.78 | 47.75 | 48.10 |
| Peak Memory (MB) | 109.50 | 75.17 | 76.57 |

---

### 2.0 Hz - Near Nyquist Limit

**Characteristics:**
- Samples per cycle: 2.0 (at 4Hz sampling)
- Nyquist ratio: 1.0x (at Nyquist limit)
- Expected behavior: Critical sampling, high aliasing risk

**Results:**

| Metric | Fetching | Approximation | Chunked |
|--------|----------|---------------|---------|
| Result Value | 49.99723 | 49.96808 | 50.01531 |
| MAPE (%) | 0.0 | 0.0583 | 0.0362 |
| Avg CPU User | 3203.21 | 3390.23 | 3482.44 |
| Avg Memory (MB) | 44.43 | 47.94 | 48.18 |
| Peak Memory (MB) | 109.50 | 75.17 | 76.57 |

---

## Key Findings

### Accuracy

*To be filled after analysis...*

1. Overall accuracy across all frequencies
2. Impact of frequency on approximation error
3. Comparison between Approximation and Chunked approaches

### Performance

*To be filled after analysis...*

1. CPU usage patterns across approaches
2. Memory consumption differences
3. Scalability observations

### Latency

*To be filled after analysis...*

1. First-event latency comparison
2. Window processing times
3. Real-time capabilities

---

## Recommendations

*To be filled after analysis...*

1. **Best approach for accuracy:**
2. **Best approach for resource efficiency:**
3. **Best approach for real-time constraints:**
4. **Frequency-specific recommendations:**

---

## Methodology

### Experimental Setup

- **Query Window:** RANGE 120s, SLIDE 60s
- **Data Pattern:** Complex oscillation with harmonics
- **Sensor Streams:** 2 streams (smartphone, wearable)
- **Sampling Rate:** ~4 Hz
- **Test Duration:** 180 seconds per test
- **Repetitions:** Single run per configuration

### Metrics Collected

1. **Accuracy Metrics:**
   - Query result values
   - MAPE (Mean Absolute Percentage Error)
   - Absolute error

2. **Resource Metrics:**
   - CPU user time (average)
   - CPU system time (average)
   - Memory usage (average and peak)

3. **Latency Metrics:**
   - Query registration to first result
   - Expected window close to result emission
   - Last observation to result emission

### Data Files

All experimental data is stored in `frequency_comparison_results/`:
- Individual latency logs per approach per frequency
- Resource usage logs per approach per frequency
- Consolidated summary CSV

---

## Appendix

### RSP-QL Query

```sparql
PREFIX : <https://dahcc.idlab.ugent.be/Protego/_participant1/>
PREFIX saref: <https://saref.etsi.org/core/>

SELECT (AVG(?value) AS ?avg)
FROM NAMED WINDOW :window ON STREAM :stream [RANGE 120s STEP 60s]
WHERE {
    WINDOW :window {
        ?obs saref:hasValue ?value .
    }
}
```

### Chunk Configuration (Chunked Approach)

- Main query: RANGE 120s, STEP 60s
- Sub-queries: RANGE 30s, STEP 30s (GCD-based rewriting)
- Aggregation: 4 chunks per window (2 streams × 2 time intervals)

---

**Experiment Status:** Running (started at timestamp recorded in logs)  
**Expected Completion:** ~45 minutes from start  
**Results Location:** `frequency_comparison_results/summary.csv`
