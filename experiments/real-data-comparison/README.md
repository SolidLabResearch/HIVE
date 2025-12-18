# Real Data 3-Way Comparison Experiment

This experiment compares three streaming query approaches using real acceleration data from smartphone and wearable sensors.

## Overview

Unlike synthetic data experiments, this uses actual sensor data to provide realistic performance benchmarks:
- **Data Source**: `smartphone.acceleration.x` and `wearable.acceleration.x`
- **Approaches**: Fetching Client Side, Approximation, Chunked
- **Metrics**: Window Close Latency, Accuracy (using Fetching as baseline)

## Data

The experiment streams real acceleration data from:
- `src/streamer/data/smartphone.acceleration.x/data.nt`
- `src/streamer/data/wearable.acceleration.x/data.nt`

These files contain actual sensor observations with timestamps, providing realistic streaming patterns including natural variations, noise, and timing irregularities.

## How It Works

### Streaming Process

1. Uses `StreamToMQTT.ts` to replay data streams
2. Publishes to MQTT topics: `smartphoneX` and `wearableX`
3. Each approach processes the streams with its own strategy
4. Metrics are collected from logs after completion

### Approaches

1. **Fetching Client Side** (Baseline)
   - Fetches complete data for accurate aggregation
   - Highest accuracy, potentially higher latency
   - Used as ground truth for accuracy comparison

2. **Approximation**
   - Uses approximation techniques to reduce computation
   - Aims for lower latency with acceptable accuracy trade-off

3. **Chunked**
   - Processes data in chunks for balanced performance
   - Balances latency and accuracy

### Metrics Collected

- **Window Close Latency**: Time to close and process each window (ms)
  - Average, Min, Max across all windows
- **Accuracy**: Comparison against baseline
  - Match Rate: % of results matching baseline (within tolerance)
  - MAE (Mean Absolute Error): Average absolute difference
  - MAPE (Mean Absolute Percentage Error): Average percentage difference

## Prerequisites

Before running the experiment:

1. **Build the project**:
   ```bash
   npm run build
   ```

2. **Start MQTT broker** (mosquitto):
   ```bash
   brew services start mosquitto
   # or
   mosquitto -v
   ```

3. **Start MongoDB**:
   ```bash
   brew services start mongodb-community
   # or
   mongod --config /usr/local/etc/mongod.conf
   ```

4. **Verify data files exist**:
   ```bash
   ls -lh src/streamer/data/smartphone.acceleration.x/data.nt
   ls -lh src/streamer/data/wearable.acceleration.x/data.nt
   ```

## Running the Experiment

### Full Comparison

```bash
cd experiments/real-data-comparison
node run-real-data-3-approaches.js
```

This will:
1. Prompt for confirmation
2. Run all 3 approaches × 3 iterations each (9 total tests)
3. Collect logs and metrics
4. Generate comparison reports

**Expected Duration**: 15-30 minutes depending on data size

### Analyze Existing Results

If you've already run the experiments:

```bash
node run-real-data-3-approaches.js analyze-only
```

## Output

### Directory Structure

```
logs/real_data_comparison/
├── fetching/
│   ├── iteration1/
│   │   ├── fetching_client_side_log.csv
│   │   ├── fetching_client_side_resource_usage.csv
│   │   └── replayer-log.csv
│   ├── iteration2/
│   └── iteration3/
├── approximation/
│   ├── iteration1/
│   ├── iteration2/
│   └── iteration3/
├── chunked/
│   ├── iteration1/
│   ├── iteration2/
│   └── iteration3/
├── real_data_comparison_results.csv
└── real_data_comparison_results.json
```

### Reports

1. **Console Output**
   - Real-time progress updates
   - Detailed comparison table
   - Summary statistics

2. **CSV Report** (`real_data_comparison_results.csv`)
   - Structured data for analysis
   - Columns: Approach, Iterations, Latencies, Accuracy, MAE, MAPE

3. **JSON Report** (`real_data_comparison_results.json`)
   - Complete results with metadata
   - Raw data for custom analysis
   - Timestamp and configuration info

## Example Output

```
================================================================================
REAL DATA 3-WAY COMPARISON REPORT
================================================================================
Data Source: smartphone.acceleration.x & wearable.acceleration.x
Baseline for Accuracy: Fetching Client Side Approach

| Approach              | Iterations | Avg Latency | Min Latency | Max Latency | Windows | Accuracy | MAE      | MAPE     |
|----------------------|------------|-------------|-------------|-------------|---------|----------|----------|----------|
| Fetching Client Side | 3          | 15.42 ms    | 12.30 ms    | 18.50 ms    | 30      | 100.0%   | 0.000000 | 0.00%    |
| Approximation        | 3          | 8.23 ms     | 6.10 ms     | 10.40 ms    | 30      | 94.5%    | 0.002341 | 2.34%    |
| Chunked              | 3          | 12.15 ms    | 10.20 ms    | 14.80 ms    | 30      | 98.2%    | 0.000891 | 0.89%    |

================================================================================

📈 Performance Comparison:
────────────────────────────────────────────────────────────────────────────────
Fetching Client Side     : Avg Latency = 15.42 ms 
Approximation            : Avg Latency = 8.23 ms (-46.6% vs baseline)
Chunked                  : Avg Latency = 12.15 ms (-21.2% vs baseline)

📊 Key Findings:
────────────────────────────────────────────────────────────────────────────────
🏆 Fastest approach: Approximation (8.23 ms avg)
```

## Interpreting Results

### Latency Analysis

- **Lower is better**: Faster window processing
- Compare against baseline (Fetching) to see performance gains/losses
- Consider min/max to understand consistency

### Accuracy Analysis

- **Higher is better**: Closer to ground truth
- 100% = perfect match with baseline
- MAPE < 5% = generally acceptable approximation
- MAE depends on data magnitude

### Trade-offs

Look for the approach that best fits your requirements:
- **Need accuracy**: Use Fetching or Chunked
- **Need speed**: Use Approximation if accuracy is acceptable
- **Balance**: Chunked often provides good middle ground

## Troubleshooting

### "MQTT connection failed"
```bash
# Check if mosquitto is running
ps aux | grep mosquitto

# Restart if needed
brew services restart mosquitto
```

### "MongoDB connection refused"
```bash
# Check if MongoDB is running
ps aux | grep mongod

# Restart if needed
brew services restart mongodb-community
```

### "Data file not found"
Ensure data files exist:
```bash
ls -lh src/streamer/data/smartphone.acceleration.x/data.nt
ls -lh src/streamer/data/wearable.acceleration.x/data.nt
```

### Experiment hangs
- Check system resources (memory, CPU)
- Increase timeout in script if needed
- Check logs in individual iteration directories

### No window close latency in results
The logs may not contain latency metrics. Check:
- Log file format in each approach
- Whether window close events are logged
- Parser logic in the script

## Configuration

To modify the experiment:

### Change Iterations
Edit `ITERATIONS` constant in the script:
```javascript
const ITERATIONS = 5; // Run 5 times per approach
```

### Change Timeout
Edit `TIMEOUT_MS` constant:
```javascript
const TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
```

### Use Different Data
Modify the `DATA_PATH` environment variable to point to other data:
```javascript
DATA_PATH: 'custom_data/my_sensors'
```

## Next Steps

After running the comparison:

1. Analyze CSV/JSON reports
2. Generate visualizations (see `analysis/visualization/`)
3. Compare with synthetic data experiments (rate-comparison)
4. Determine optimal approach for your use case
5. Run additional iterations for statistical significance

## Related Experiments

- **Rate Comparison**: Tests with synthetic exponential data at different rates
- **Frequency Comparison**: Tests with different data frequencies
- **Pattern Analysis**: Tests with specific data patterns