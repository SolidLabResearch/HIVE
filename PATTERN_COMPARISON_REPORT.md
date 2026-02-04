# Pattern Comparison Report: Streaming Query Approaches

**Date**: February 4, 2026  
**Experiment**: Pattern-Based Accuracy and Performance Comparison  
**Duration**: 120 seconds per test, 4Hz sampling (480 observations each)

## Executive Summary

This report presents a comprehensive comparison of three streaming query processing approaches across five distinct data patterns. Each pattern tests different characteristics of the streaming data to evaluate robustness, accuracy, and resource efficiency.

### Tested Approaches
1. **Fetching (Client-Side)** - Baseline approach with client-side aggregation
2. **Approximation** - Rate-based sub-query approach  
3. **Chunked** - Incremental aggregation with GCD-based sub-queries

### Data Patterns Tested
1. **Low Variability** - Gaussian noise (μ=-23.0, σ=0.25)
2. **Step Pattern** - Step change from -23.0 to -15.0 at t=60s
3. **Spike Pattern** - Brief spike from -23.0 to -5.0 for 1.25s
4. **Low Frequency Oscillation** - Sinusoidal (μ=-23.0, A=5.0, f=0.05Hz)
5. **High Frequency Oscillation** - Sinusoidal (μ=-23.0, A=3.0, f=0.5Hz)

## Overall Results Summary

### Accuracy Comparison

| Pattern | Approach | Result Value | MAPE vs Fetching (%) | Absolute Error |
|---------|----------|--------------|----------------------|----------------|
| low_variability | Fetching | -22.987 | 0.00 | 0.000 |
| low_variability | Approximation | -22.993 | -0.03 | 0.006 |
| step_pattern | Fetching | -17.684 | 0.00 | 0.000 |
| step_pattern | Approximation | -17.032 | -3.69 | 0.652 |
| spike_pattern | Fetching | -22.930 | 0.00 | 0.000 |
| spike_pattern | Approximation | -22.741 | -0.82 | 0.189 |
| low_freq_oscillation | Fetching | -23.002 | 0.00 | 0.000 |
| low_freq_oscillation | Approximation | -23.003 | -0.01 | 0.002 |
| high_freq_oscillation | Fetching | -23.000 | 0.00 | 0.000 |
| high_freq_oscillation | Approximation | -22.989 | -0.05 | 0.011 |
| low_variability | Chunked | -22.987 | 0.00 | 0.000 |
| step_pattern | Chunked | -19.441 | 9.94 | 1.758 |
| spike_pattern | Chunked | -22.870 | 0.26 | 0.060 |
| low_freq_oscillation | Chunked | -22.886 | 0.50 | 0.116 |
| high_freq_oscillation | Chunked | -22.994 | 0.03 | 0.006 |





### Resource Usage Comparison

| Pattern | Approach | Avg CPU User | Avg CPU System | Avg Memory (MB) | Peak Memory (MB) |
|---------|----------|--------------|----------------|-----------------|------------------|
| low_variability | Fetching | 3936.61 | 1149.97 | 45.23 | 45.58 |
| low_variability | Approximation | 4523.14 | 1208.91 | 47.43 | 42.32 |
| step_pattern | Fetching | 4003.88 | 1162.75 | 45.35 | 41.23 |
| step_pattern | Approximation | 4200.70 | 1146.08 | 48.66 | 41.98 |
| spike_pattern | Fetching | 3944.82 | 1091.13 | 44.23 | 39.06 |
| spike_pattern | Approximation | 4548.93 | 1167.90 | 47.12 | 42.16 |
| low_freq_oscillation | Fetching | 3912.92 | 1097.71 | 45.52 | 42.74 |
| low_freq_oscillation | Approximation | 4261.94 | 1160.81 | 48.69 | 42.14 |
| high_freq_oscillation | Fetching | 4023.16 | 1192.15 | 45.50 | 45.43 |
| high_freq_oscillation | Approximation | 4398.86 | 1216.76 | 48.36 | 42.26 |
| low_variability | Chunked | 4507.48 | 1261.40 | 48.23 | 42.15 |
| step_pattern | Chunked | 4587.61 | 1227.81 | 48.28 | 42.27 |
| spike_pattern | Chunked | 4383.96 | 1168.35 | 48.64 | 42.31 |
| low_freq_oscillation | Chunked | 4763.17 | 1256.17 | 47.30 | 41.95 |
| high_freq_oscillation | Chunked | 4476.21 | 1213.59 | 48.45 | 42.32 |





---

## Detailed Analysis by Pattern

### Low Variability

**Pattern Characteristics:**
- Type: Gaussian noise
- Mean (μ): -23.0
- Standard Deviation (σ): 0.25
- Expected Behavior: Stable mean with minimal variance

**Results:**

| Approach | Windows | Result Value | MAPE (%) | Absolute Error | CPU User | CPU System | Memory (MB) | Peak Mem (MB) |
|----------|---------|--------------|----------|----------------|----------|------------|-------------|---------------|
| Fetching | 3 | -22.987 | 0.00 | 0.000 | 3936.6 | 1150.0 | 45.2 | 45.6 |
| Approximation | 2 | -22.993 | -0.03 | 0.006 | 4523.1 | 1208.9 | 47.4 | 42.3 |
| Chunked | 2 | -22.987 | 0.00 | 0.000 | 4507.5 | 1261.4 | 48.2 | 42.1 |





**Analysis:**
- This pattern tests the approaches' ability to handle low-variance, steady-state data
- Expected: All approaches should produce highly accurate results due to data stability

---

### Step Pattern

**Pattern Characteristics:**
- Type: Step function
- Initial Value (v₁): -23.0
- Final Value (v₂): -15.0
- Step Time (t_step): 60s
- Expected Behavior: Abrupt transition halfway through observation period

**Results:**

| Approach | Windows | Result Value | MAPE (%) | Absolute Error | CPU User | CPU System | Memory (MB) | Peak Mem (MB) |
|----------|---------|--------------|----------|----------------|----------|------------|-------------|---------------|
| Fetching | 2 | -17.684 | 0.00 | 0.000 | 4003.9 | 1162.8 | 45.4 | 41.2 |
| Approximation | 2 | -17.032 | -3.69 | 0.652 | 4200.7 | 1146.1 | 48.7 | 42.0 |
| Chunked | 2 | -19.441 | 9.94 | 1.758 | 4587.6 | 1227.8 | 48.3 | 42.3 |




**Analysis:**
- This pattern tests handling of abrupt regime changes
- The 120s window with 60s step creates a 50-50 split between low and high values
- Expected mean: (-23.0 + -15.0) / 2 = -19.0

---

### Spike Pattern

**Pattern Characteristics:**
- Type: Transient spike
- Base Value (v_base): -23.0
- Spike Value (v_spike): -5.0
- Spike Duration (Δt): 1.25s
- Spike Position: Center of observation window (t=60s)
- Expected Behavior: Brief high-magnitude deviation

**Results:**

| Approach | Windows | Result Value | MAPE (%) | Absolute Error | CPU User | CPU System | Memory (MB) | Peak Mem (MB) |
|----------|---------|--------------|----------|----------------|----------|------------|-------------|---------------|
| Fetching | 2 | -22.930 | 0.00 | 0.000 | 3944.8 | 1091.1 | 44.2 | 39.1 |
| Approximation | 2 | -22.741 | -0.82 | 0.189 | 4548.9 | 1167.9 | 47.1 | 42.2 |
| Chunked | 2 | -22.870 | 0.26 | 0.060 | 4384.0 | 1168.3 | 48.6 | 42.3 |




**Analysis:**
- This pattern tests handling of transient anomalies
- Spike duration: 1.25s out of 120s total (1.04% of window)
- Approximation approaches may smooth out or miss brief spikes
- Tests temporal resolution and anomaly preservation

---

### Low Freq Oscillation

**Pattern Characteristics:**
- Type: Sinusoidal oscillation
- Mean (μ): -23.0
- Amplitude (A): 5.0
- Frequency (f): 0.05 Hz
- Sampling Rate: 4 Hz (80 samples per cycle)
- Expected Behavior: Slow, smooth oscillation with 6 complete cycles in 120s

**Results:**

| Approach | Windows | Result Value | MAPE (%) | Absolute Error | CPU User | CPU System | Memory (MB) | Peak Mem (MB) |
|----------|---------|--------------|----------|----------------|----------|------------|-------------|---------------|
| Fetching | 2 | -23.002 | 0.00 | 0.000 | 3912.9 | 1097.7 | 45.5 | 42.7 |
| Approximation | 2 | -23.003 | -0.01 | 0.002 | 4261.9 | 1160.8 | 48.7 | 42.1 |
| Chunked | 2 | -22.886 | 0.50 | 0.116 | 4763.2 | 1256.2 | 47.3 | 42.0 |




**Analysis:**
- Tests ability to capture slow periodic variations
- 80 samples per cycle provides excellent temporal resolution
- All approaches should accurately capture this pattern
- Expected mean over full cycles: μ = -23.0

---

### High Freq Oscillation

**Pattern Characteristics:**
- Type: Sinusoidal oscillation
- Mean (μ): -23.0
- Amplitude (A): 3.0
- Frequency (f): 0.5 Hz
- Sampling Rate: 4 Hz (8 samples per cycle)
- Expected Behavior: Rapid oscillation with 60 complete cycles in 120s

**Results:**

| Approach | Windows | Result Value | MAPE (%) | Absolute Error | CPU User | CPU System | Memory (MB) | Peak Mem (MB) |
|----------|---------|--------------|----------|----------------|----------|------------|-------------|---------------|
| Fetching | 2 | -23.000 | 0.00 | 0.000 | 4023.2 | 1192.2 | 45.5 | 45.4 |
| Approximation | 2 | -22.989 | -0.05 | 0.011 | 4398.9 | 1216.8 | 48.4 | 42.3 |
| Chunked | 2 | -22.994 | 0.03 | 0.006 | 4476.2 | 1213.6 | 48.5 | 42.3 |




**Analysis:**
- Tests handling of higher frequency periodic patterns
- 8 samples per cycle is adequate but less margin than low-frequency case
- Frequency is below Nyquist limit (2 Hz), so no aliasing expected
- Tests aggregation accuracy with rapid fluctuations

---

## Cross-Pattern Insights

### Accuracy Trends
- **TBD after experiment completion**

### Resource Efficiency
- **TBD after experiment completion**

### Best Use Cases
- **Low Variability**: All approaches suitable, choose based on resource constraints
- **Step Pattern**: Tests boundary handling and regime change detection
- **Spike Pattern**: Highlights temporal resolution differences
- **Low Freq Oscillation**: Ideal for all approaches, well-sampled
- **High Freq Oscillation**: Tests aggregation fidelity under rapid changes

---

## Methodology

### Query Configuration
- **Window Size**: RANGE 120s (rolling window)
- **Slide Interval**: STEP 60s (50% overlap)
- **Chunked Sub-queries**: RANGE 30s, STEP 30s (GCD-based decomposition)

### Data Configuration
- **Duration**: 120 seconds
- **Sampling Rate**: 4 Hz (250ms intervals)
- **Data Points**: 480 observations per stream
- **Streams**: Two correlated sensors (smartphoneX, wearableX)

### Metrics Collected
1. **Accuracy**: MAPE (Mean Absolute Percentage Error) vs Fetching baseline
2. **Latency**: Time from query registration to result emission
3. **CPU Usage**: User and system CPU time (average)
4. **Memory**: Average and peak memory consumption (MB)

### RSP-QL Query
```sparql
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>

REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?averageValue)
FROM NAMED WINDOW :w1 ON STREAM <mqtt://localhost:1883/smartphoneX> [RANGE PT120S STEP PT60S]
FROM NAMED WINDOW :w2 ON STREAM <mqtt://localhost:1883/wearableX> [RANGE PT120S STEP PT60S]
WHERE {
  WINDOW :w1 { ?obs saref:relatesToProperty dahccsensors:smartphoneX ; saref:hasValue ?v1 . }
  WINDOW :w2 { ?obs saref:relatesToProperty dahccsensors:wearableX ; saref:hasValue ?v2 . }
  BIND((?v1 + ?v2) / 2 AS ?v)
}
```

---

## Appendix

### Pattern Parameters Summary

| Pattern | Key Parameters | Description |
|---------|---------------|-------------|
| Low Variability | μ=-23.0, σ=0.25 | Gaussian noise, minimal variation |
| Step Pattern | v₁=-23.0, v₂=-15.0, t_step=60s | Abrupt regime change at midpoint |
| Spike Pattern | v_base=-23.0, v_spike=-5.0, Δt=1.25s | Brief transient anomaly |
| Low Freq Osc. | μ=-23.0, A=5.0, f=0.05Hz | Slow periodic variation (6 cycles) |
| High Freq Osc. | μ=-23.0, A=3.0, f=0.5Hz | Rapid periodic variation (60 cycles) |

### Experiment Environment
- **OS**: macOS
- **Node.js**: v14+
- **MQTT Broker**: Mosquitto (localhost:1883)
- **Experiment Duration**: ~45 minutes (15 tests × 180s each)

---

*Report generated automatically from experimental data. For questions or additional analysis, see experiment logs in `pattern_comparison_results/`*
