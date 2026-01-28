# Quick Start: Real Data Comparison

Run a 3-way comparison of streaming approaches using real sensor data in 5 minutes.

## Prerequisites (One-Time Setup)

```bash
# 1. Build the project
npm run build

# 2. Start required services
brew services start mosquitto


# 3. Verify data exists
ls -lh src/streamer/data/smartphone.acceleration.x/data.nt
ls -lh src/streamer/data/wearable.acceleration.x/data.nt
```

## Run Comparison

```bash
cd experiments/real-data-comparison
node run-real-data-3-approaches.js
```

Press Enter when prompted. The experiment will:
- Run 3 approaches × 3 iterations = 9 tests
- Take ~15-30 minutes
- Generate reports in `logs/real_data_comparison/`

## View Results

```bash
# View CSV results
cat ../../logs/real_data_comparison/real_data_comparison_results.csv

# View JSON results
cat ../../logs/real_data_comparison/real_data_comparison_results.json
```

## What Gets Measured

- **Window Close Latency**: How fast each approach processes windows (ms)
- **Accuracy**: How close results are to baseline (Fetching = 100%)
- **MAE/MAPE**: Error metrics vs baseline

## Understanding Output

```
| Approach              | Avg Latency | Accuracy | MAPE    |
|----------------------|-------------|----------|---------|
| Fetching Client Side | 15.42 ms    | 100.0%   | 0.00%   | ← Baseline (accurate)
| Approximation        | 8.23 ms     | 94.5%    | 2.34%   | ← Fastest
| Chunked              | 12.15 ms    | 98.2%    | 0.89%   | ← Balanced
```

**Key Takeaways:**
- Lower latency = faster processing
- Higher accuracy = more reliable
- Best approach depends on your priority (speed vs accuracy)

## Troubleshooting

**Services not running?**
```bash
brew services list
brew services restart mosquitto

```

**Need help?** See full README.md in this directory.

## Next Steps

1. Compare with rate-comparison experiments (`experiments/rate-comparison/`)
2. Generate visualizations (`analysis/visualization/`)
3. Tweak ITERATIONS constant in script for more data