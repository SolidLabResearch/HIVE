# Rate Comparison Experiments - 3-Way Approach Comparison

This directory contains experiments to compare three different approaches for streaming query processing across various data rates.

## Overview

The experiments test how each approach handles exponential growth and decay patterns at different rates, measuring:
- **Window Close Latency**: Time taken to close and process windows
- **Accuracy**: Correctness of results (using Fetching Client Side as baseline)

## Approaches

1. **Fetching Client Side** (Baseline for Accuracy)
   - Fetches all data client-side for processing
   - Most accurate but potentially slower
   
2. **Approximation**
   - Uses approximation techniques to reduce computation
   - Trades accuracy for performance
   
3. **Chunked**
   - Processes data in chunks
   - Balances performance and accuracy

## Test Configuration

- **Rates**: 0.001, 0.01, 0.1, 1, 10, 100
- **Patterns**: 
  - `exponential_growth`: Data grows exponentially
  - `exponential_decay`: Data decays exponentially
- **Metrics**:
  - Average/Min/Max window close latency (ms)
  - Accuracy percentage vs baseline
  - Mean Absolute Error (MAE)
  - Mean Absolute Percentage Error (MAPE)

## Quick Start

### Run Full 3-Way Comparison

```bash
cd experiments/rate-comparison
node run-all-3-approaches-comparison.js
```

This will:
1. Run all three approaches for all rate/pattern combinations
2. Analyze results and calculate metrics
3. Generate comparison reports

**Note**: This runs 36 experiments (3 approaches × 2 patterns × 6 rates) and may take 30-60 minutes.

### Run Individual Approaches

#### Fetching Client Side
```bash
node experiment-rate-comparison-fetching.js                           # All rates
node experiment-rate-comparison-fetching.js rate 0.1                 # Specific rate
node experiment-rate-comparison-fetching.js test exponential_growth 1 # Specific test
```

#### Approximation
```bash
node experiment-rate-comparison-approximation.js                           # All rates
node experiment-rate-comparison-approximation.js rate 0.1                 # Specific rate
node experiment-rate-comparison-approximation.js test exponential_growth 1 # Specific test
```

#### Chunked
```bash
node experiment-rate-comparison-chunked.js                           # All rates
node experiment-rate-comparison-chunked.js rate 0.1                 # Specific rate
node experiment-rate-comparison-chunked.js test exponential_growth 1 # Specific test
```

### Analyze Existing Results

If you've already run the experiments and just want to regenerate the comparison report:

```bash
node run-all-3-approaches-comparison.js analyze-only
```

## Output

### Directory Structure

```
logs/
├── rate-comparison-fetching/
│   ├── exponential_growth_rate_0.001/
│   │   └── iteration1/
│   │       ├── fetching_client_side_log.csv
│   │       ├── fetching_client_side_resource_usage.csv
│   │       └── replayer-log.csv
│   └── ...
├── rate-comparison-approximation/
│   └── ...
├── rate-comparison-chunked/
│   └── ...
└── rate_comparison_3way/
    ├── three_way_comparison_results.csv
    └── three_way_comparison_results.json
```

### Reports

1. **CSV Report** (`logs/rate_comparison_3way/three_way_comparison_results.csv`)
   - Structured data for further analysis
   - Columns: Rate, Pattern, Approach, Latencies, Accuracy, MAE, MAPE

2. **JSON Report** (`logs/rate_comparison_3way/three_way_comparison_results.json`)
   - Complete results with metadata
   - Includes all raw data for custom analysis

3. **Console Output**
   - Detailed comparison tables
   - Summary statistics by approach, rate, and pattern
   - Average metrics across all tests

## Understanding Results

### Window Close Latency

Lower is better. This measures how quickly the system can close and process a window.

### Accuracy Metrics

- **Match Rate %**: Percentage of results that exactly match the baseline (within tolerance)
- **MAE (Mean Absolute Error)**: Average absolute difference from baseline values
- **MAPE (Mean Absolute Percentage Error)**: Average percentage difference from baseline

For Accuracy, Fetching Client Side is used as the baseline (100% accurate).

### Example Output

```
Rate: 0.1 | Pattern: exponential_growth
────────────────────────────────────────────────────────────────────────────────

| Approach              | Avg Latency | Min Latency | Max Latency | Windows | Accuracy | MAE      | MAPE     |
|----------------------|-------------|-------------|-------------|---------|----------|----------|----------|
| Fetching Client Side | 15.42 ms    | 12.30 ms    | 18.50 ms    | 10      | 100.0%   | 0.000000 | 0.00%    |
| Approximation        | 8.23 ms     | 6.10 ms     | 10.40 ms    | 10      | 94.5%    | 0.002341 | 2.34%    |
| Chunked              | 12.15 ms    | 10.20 ms    | 14.80 ms    | 10      | 98.2%    | 0.000891 | 0.89%    |
```

## Interpreting Trade-offs

- **Low latency + high accuracy**: Ideal performance
- **Low latency + low accuracy**: Fast but unreliable
- **High latency + high accuracy**: Reliable but slow
- **High latency + low accuracy**: Poor performance

Look for approaches that maintain high accuracy while minimizing latency across different rates.

## Prerequisites

1. Build the project: `npm run build`
2. Ensure test data exists in `src/streamer/data/rate_comparison/`
3. All orchestrators must be compiled in `dist/approaches/`

## Troubleshooting

### "Data file not found" error
Generate test data first:
```bash
# Check data generation scripts in analysis/data-generation/
```

### Experiments hang or timeout
- Default timeout is 3 minutes per test
- Check system resources (memory, CPU)
- Review logs in the respective approach directories

### Missing results in comparison
- Ensure all three approach experiments completed successfully
- Check individual approach summary files for errors
- Run `analyze-only` to see which results are available

## Next Steps

After running comparisons:

1. Analyze CSV/JSON reports for trends
2. Generate visualizations (see `analysis/visualization/`)
3. Compare with frequency-based experiments
4. Identify optimal approach for your use case