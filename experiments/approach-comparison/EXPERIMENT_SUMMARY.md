# Approach Comparison Experiment

## Overview

This experiment compares the three streaming query processing approaches implemented in the Streaming Query Hive project:

1. **Client-Side Processing** (Ground Truth) - Fetches all data and processes client-side using RSP-JS
2. **Chunked Query Approach** - Pre-computes chunks from sub-queries and reuses them
3. **Approximation Approach** - Approximates results using sub-query outputs

## Latency Definition

**Critical: Latency is measured as the time from window close to result availability.**

```
Latency = result_available_time - window_close_time
```

Where:
- `window_close_time` = `query_registered_time + (window_number * window_slide)`
- `result_available_time` = timestamp when the result is received via MQTT

This definition matches the August 2024 benchmark methodology.

## August 2024 Benchmark Results (Reference)

| Approach | Latency (ms) | CPU % | Memory (MB) | Accuracy |
|----------|--------------|-------|-------------|----------|
| Chunked Query | 414 +/- 12.3 | 0.21 | 45.68 +/- 2.3 | 100% |
| Approximation | 359 +/- 31.2 | 0.20 | 53.92 +/- 1.2 | 89.5% |
| Client-Side Processing | 2543 +/- 213.3 | 0.20 | 66.05 +/- 4.2 | 100% (GT) |

## Running the Experiment

### Prerequisites

1. MQTT broker running on `localhost:1883`
   ```bash
   # macOS
   brew services start mosquitto
   
   # Linux
   sudo systemctl start mosquitto
   
   # Or run directly
   mosquitto -v
   ```

2. Node.js and npm installed

3. Project dependencies installed
   ```bash
   npm install
   ```

### Using the Benchmark Script (Recommended)

The standalone benchmark script runs each approach independently with correct latency measurement:

```bash
# Run all approaches sequentially
npx ts-node experiments/approach-comparison/benchmark.ts all

# Run a specific approach
npx ts-node experiments/approach-comparison/benchmark.ts client-side
npx ts-node experiments/approach-comparison/benchmark.ts chunked
npx ts-node experiments/approach-comparison/benchmark.ts approximation
```

### Using the Shell Script

```bash
cd experiments/approach-comparison
./run-experiment.sh

# Options:
./run-experiment.sh --frequency 4      # Data frequency in Hz
./run-experiment.sh --warmup 5000      # Warmup period in ms
./run-experiment.sh --skip-mqtt-check  # Skip MQTT broker check
./run-experiment.sh --compare          # Show August benchmark comparison
```

### Using the Full Experiment

```bash
npx ts-node experiments/approach-comparison/ApproachComparisonExperiment.ts [output_dir]
```

## Configuration

Default configuration matches the August 2024 benchmark:

| Parameter | Value | Description |
|-----------|-------|-------------|
| Data Frequency | 4 Hz | Events per second per stream |
| Window Width | 120000 ms | Main query window width (2 minutes) |
| Window Slide | 60000 ms | Main query window slide (1 minute) |
| Sub-query Window Width | 60000 ms | Sub-query window width (1 minute) |
| Sub-query Window Slide | 30000 ms | Sub-query window slide (30 seconds) |
| Warmup Period | 5000 ms | Ignored initial period |

## Output Files

Results are saved to `experiments/approach-comparison/results/`:

- `summary_<timestamp>.csv` - Aggregate latency and resource statistics
- `<approach>_results_<timestamp>.csv` - Per-window results for each approach
- `accuracy_<timestamp>.csv` - Accuracy comparison vs ground truth
- `benchmark_<timestamp>.json` - Full benchmark results in JSON format

## How Each Approach Works

### Client-Side Processing (Ground Truth)

1. Subscribes to raw data streams via MQTT
2. Feeds data into RSP-JS engine with the main query
3. RSP-JS processes windows and emits results
4. Results published to `client_operation_output` topic

### Chunked Query Approach

1. Sub-queries are registered and process raw streams
2. Sub-query results are published as chunks to MQTT
3. The operator subscribes to chunk topics
4. Chunks are aggregated to answer the main query
5. Results published to `output` topic

### Approximation Approach

1. Sub-queries process raw streams
2. Sub-query aggregation results are published
3. The operator combines sub-query results
4. Approximates the main query result using latest values
5. Results published to `approximation/output` topic

## Troubleshooting

### No results collected

- Check that MQTT broker is running: `nc -z localhost 1883`
- Check that the HTTP query server is accessible: `curl http://localhost:3001/queries`
- Verify data files exist: `ls src/streamer/data/wearable.acceleration.x/data.nt`

### Results don't match August benchmark

- Ensure you're using the same data files
- Verify data frequency is 4 Hz
- Check that approaches run separately (not concurrently)
- Verify latency calculation: `result_time - window_close_time` (not `result_time - last_data_arrival`)

### High error in approximation

- This is expected - approximation trades accuracy for speed
- August benchmark showed 89.5% accuracy for approximation
- Check if sub-queries are correctly registered before main query runs

## Files

- `ApproachComparisonExperiment.ts` - Full experiment runner
- `benchmark.ts` - Standalone benchmark script
- `run-experiment.sh` - Shell script wrapper
- `EXPERIMENT_SUMMARY.md` - This documentation