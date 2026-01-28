# Pattern Analysis Experiment Plan

## Overview

This document describes the comprehensive pattern analysis experiment comparing three streaming query approaches across multiple data patterns with 35 iterations per test for statistical significance.

## Approaches Being Tested

1. **Fetching (Client-Side)** - Baseline/ground truth approach
2. **Approximation (Rate-Based)** - Time-weighted sub-query aggregation
3. **Chunked (Chunk Aggregator)** - AVG + COUNT weighted aggregation

## Stream Patterns

### 1. Exponential Growth Patterns
Tests system behavior with exponentially increasing values.

| Pattern Type | Rate (λ) | Formula |
|--------------|----------|---------|
| Exponential Growth | 0.001 | `v(t) = v₀ × e^(0.001t)` |
| Exponential Growth | 0.01  | `v(t) = v₀ × e^(0.01t)` |
| Exponential Growth | 0.1   | `v(t) = v₀ × e^(0.1t)` |
| Exponential Growth | 1     | `v(t) = v₀ × e^(1t)` |
| Exponential Growth | 10    | `v(t) = v₀ × e^(10t)` |
| Exponential Growth | 100   | `v(t) = v₀ × e^(100t)` |

### 2. Exponential Decay Patterns
Tests system behavior with exponentially decreasing values.

| Pattern Type | Rate (λ) | Formula |
|--------------|----------|---------|
| Exponential Decay | 0.001 | `v(t) = v₀ × e^(-0.001t)` |
| Exponential Decay | 0.01  | `v(t) = v₀ × e^(-0.01t)` |
| Exponential Decay | 0.1   | `v(t) = v₀ × e^(-0.1t)` |
| Exponential Decay | 1     | `v(t) = v₀ × e^(-1t)` |
| Exponential Decay | 10    | `v(t) = v₀ × e^(-10t)` |
| Exponential Decay | 100   | `v(t) = v₀ × e^(-100t)` |

### 3. Noisy Datasets
Tests robustness to measurement noise and variability.

| Pattern Type | Noise Level (σ) | Description |
|--------------|-----------------|-------------|
| Noise | 0.1 | Low noise: `v(t) = v₀ + N(0, 0.1)` |
| Noise | 0.2 | Light noise: `v(t) = v₀ + N(0, 0.2)` |
| Noise | 0.5 | Moderate noise: `v(t) = v₀ + N(0, 0.5)` |
| Noise | 1.0 | High noise: `v(t) = v₀ + N(0, 1.0)` |
| Noise | 2.0 | Very high noise: `v(t) = v₀ + N(0, 2.0)` |

## Window Configuration

### Main Query Window
- **RANGE**: 120,000 ms (120 seconds)
- **STEP**: 60,000 ms (60 seconds)

### Sub-Query Window (Approximation & Chunked)
- **RANGE**: 60,000 ms (60 seconds)
- **STEP**: 30,000 ms (30 seconds)

### Sampling Configuration
- **Sampling Rate**: ~4 Hz (250 ms interval)
- **Nyquist Frequency**: ≈ 2.0 Hz

## Iteration Strategy

- **Iterations per Test**: 35
- **Total Pattern-Approach Combinations**: 17 patterns × 3 approaches = 51
- **Total Test Runs**: 51 × 35 = **1,785 tests**

### Rationale for 35 Iterations
- Provides sufficient sample size for statistical analysis (n=35 > 30)
- Enables calculation of mean ± standard deviation for all metrics
- Allows detection of statistical significance in differences between approaches
- Accounts for system variability and random effects

## Metrics Collected

### Accuracy Metrics (per window)
- **MAPE** (Mean Absolute Percentage Error): `(1/n)Σ|actual - predicted|/|actual| × 100%`
- **MAE** (Mean Absolute Error): `(1/n)Σ|actual - predicted|`
- **RMSE** (Root Mean Square Error): `√((1/n)Σ(actual - predicted)²)`

### Latency Metrics
- **Query Registration Time**: Timestamp when query is registered
- **First Result Time**: Timestamp of first window result
- **First-Event Latency**: Time from registration to first result

### Resource Usage Metrics
- **Heap Used (MB)**: Memory consumption over time
- **CPU Usage**: Process CPU utilization
- **External Memory (MB)**: Additional memory allocations

## Directory Structure

```
logs/pattern-comparison/
├── fetching/
│   ├── exponential_growth_rate_0.001/
│   │   ├── iteration1/
│   │   ├── iteration2/
│   │   ├── ...
│   │   └── iteration35/
│   ├── exponential_growth_rate_0.01/
│   │   └── iteration1...35/
│   └── ...
├── approximation/
│   └── (same structure)
├── chunked/
│   └── (same structure)
├── pattern_accuracy_comparison.csv
├── pattern_analysis_summary.json
└── pattern_comparison_summary.json
```

### Per-Iteration Files
Each iteration directory contains:
- `{approach}_orchestrator.log` - Main orchestrator logs
- `publisher.log` - Data publisher logs
- `{approach}_results.csv` - Window-by-window results
- `{approach}_metadata.json` - Test metadata
- `{approach}_latency_log.csv` - Latency measurements
- `{approach}_resource_usage.csv` - Resource usage over time

## Running the Experiments

### Run All Patterns with 35 Iterations
```bash
node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 35
```

### Run Specific Pattern with 35 Iterations
```bash
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100 --iterations 35
```

### Run with Custom Iteration Count
```bash
node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 10
```

## Expected Runtime

- **Per Test**: ~2-4 minutes (including setup, execution, teardown)
- **Per Pattern-Approach-Iteration**: ~3 minutes average
- **All Tests (1,785)**: ~3 × 1,785 = **~89 hours (~3.7 days)**

### Recommended Execution Strategy
1. Run on remote server with adequate resources
2. Use `nohup` or `screen` for background execution
3. Monitor progress via log files
4. Run overnight/over weekend

## Analysis Pipeline

### 1. Per-Iteration Extraction
```bash
node experiments/pattern-analysis/extract-pattern-results.js <approach> <pattern> <logdir>
```

### 2. Aggregate Analysis
```bash
node analysis/accuracy/pattern-accuracy-comparison.js
```

### 3. Statistical Analysis
Will compute for each pattern-approach combination:
- Mean ± Standard Deviation for MAPE, MAE, RMSE
- Mean ± Standard Deviation for latency
- Mean ± Standard Deviation for resource usage
- Confidence intervals (95%)
- Statistical significance tests (t-tests between approaches)

## Expected Outcomes

### Hypotheses

1. **Low-Rate Exponential (λ ≤ 0.1)**
   - All approaches should perform similarly
   - MAPE < 5% for all approaches
   - Time-weighted approximation effective

2. **High-Rate Exponential (λ ≥ 10)**
   - Approximation expected to show significant error
   - MAPE > 20% for approximation
   - Chunked approach more robust than approximation
   - Possible numeric overflow/instability

3. **Noisy Data**
   - Approximation may smooth out noise (good or bad depending on use case)
   - Chunked approach should be more representative
   - Higher noise → higher variance across iterations

4. **Resource Usage**
   - Fetching: Highest memory (stores all data)
   - Approximation: Lowest memory (only stores sub-windows)
   - Chunked: Medium memory (stores aggregates)

5. **Latency**
   - First-event latency ~60s for all (STEP = 60s)
   - Approximation/Chunked may have slight overhead

## Deliverables

1. **Raw Data**: All iteration logs and CSVs
2. **Aggregated Metrics**: Mean ± SD for all metrics
3. **Comparison Tables**: Side-by-side approach comparison
4. **Plots**:
   - MAPE vs Rate (exponential patterns)
   - MAPE vs Noise Level
   - Memory usage vs Pattern
   - Latency distribution (box plots)
5. **Statistical Report**: Significance tests, confidence intervals
6. **Summary Report**: Key findings and recommendations

## LaTeX Table Format (for Paper)

```latex
\begin{table}[h]
\centering
\caption{Experimental Stream Patterns}
\begin{tabular}{c|l}
\hline
\textbf{Stream Pattern} & \textbf{Parameters} \\
\hline
Exponential Growth & $\lambda \in \{0.001, 0.01, 0.1, 1, 10, 100\}$ \\
\hline
Exponential Decay & $\lambda \in \{0.001, 0.01, 0.1, 1, 10, 100\}$ \\
\hline
Noisy Data & $\sigma \in \{0.1, 0.2, 0.5, 1.0, 2.0\}$ \\
\hline
\multicolumn{2}{c}{\textit{35 iterations per pattern-approach combination}} \\
\hline
\end{tabular}
\end{table}
```

## Notes

- All tests use pre-generated data in `src/streamer/data/`
- Data generation scripts: `src/streamer/data/generate-*.js`
- Ensure sufficient disk space: ~50GB for all logs
- Monitor system resources during execution
- Consider running subset first to validate pipeline