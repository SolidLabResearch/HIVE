# Quick Start: Three-Approach Frequency Comparison

This guide will help you quickly run and compare all three streaming query approaches (Fetching, Approximation, and Chunked) for frequency-based accuracy and latency analysis.

---

## Prerequisites

1. **Build the project:**
   ```bash
   npm run build
   ```

2. **Start MQTT broker:**
   ```bash
   mosquitto -c /opt/homebrew/etc/mosquitto/mosquitto.conf
   ```

3. **Verify test data exists:**
   ```bash
   ls src/streamer/data/frequency_comparison/
   ```
   You should see directories for different frequencies (e.g., `complex_oscillation_freq_0.1`)

---

## Quick Test: Single Frequency (All Three Approaches)

Run all three approaches for a single frequency (recommended for first run):

```bash
node experiments/frequency-comparison/run-all-approaches-comparison.js 0.1
```

**This will:**
1. ✅ Run Fetching approach at 0.1 Hz (~3 minutes)
2. ✅ Run Approximation approach at 0.1 Hz (~3 minutes)
3. ✅ Run Chunked approach at 0.1 Hz (~3 minutes)
4. ✅ Extract results from logs for all approaches
5. ✅ Generate accuracy and latency comparison tables

**Total time:** ~15-20 minutes

---

## Full Test: All Frequencies (All Three Approaches)

Run complete comparison across all frequencies:

```bash
node experiments/frequency-comparison/run-all-approaches-comparison.js
```

**This will test:**
- Frequencies: 0.1, 0.5, 1.0, 1.5, 2.0 Hz
- Approaches: Fetching, Approximation, Chunked
- Total tests: 15 (3 approaches × 5 frequencies)

**Total time:** ~60-75 minutes

---

## Expected Output

### Console Output

You'll see:
```
█████████████████████████████████████████████████████████████████████████████████
FREQUENCY COMPARISON - ALL APPROACHES: 0.1 Hz
█████████████████████████████████████████████████████████████████████████████████
...
RUNNING: FETCHING APPROACH - 0.1 Hz
...
✓ fetching completed successfully

RUNNING: APPROXIMATION APPROACH - 0.1 Hz
...
✓ approximation completed successfully

RUNNING: CHUNKED APPROACH - 0.1 Hz
...
✓ chunked completed successfully

EXTRACTING RESULTS FROM LOGS
...
📊 Extracting results for fetching at 0.1 Hz...
✓ Extraction completed for fetching

RUNNING ACCURACY & LATENCY COMPARISON ANALYSIS
...
═════════════════════════════════════════════════════════════════════════════════
ACCURACY COMPARISON TABLE
═════════════════════════════════════════════════════════════════════════════════
Frequency | Approach      | MAPE (%)   | MAE        | RMSE       | Windows
─────────────────────────────────────────────────────────────────────────────────
0.1       | Fetching      | Baseline   | Baseline   | Baseline   | 1
          | Approximation | 0.0129     | 0.006447   | 0.006447   | 1
          | Chunked       | 0.0085     | 0.004252   | 0.004252   | 1
...

═════════════════════════════════════════════════════════════════════════════════
LATENCY COMPARISON TABLE
═════════════════════════════════════════════════════════════════════════════════
Frequency | Approach      | First-Event Latency | Difference vs Fetching
─────────────────────────────────────────────────────────────────────────────────
0.1       | Fetching      | 61.75s              | Baseline
          | Approximation | 61.61s              | -0.14s
          | Chunked       | 61.50s              | -0.25s
```

### Result Files

After running, check these files:

```bash
# Accuracy comparison
cat logs/accuracy_comparison_all_approaches.csv

# Latency comparison
cat logs/latency_comparison_all_approaches.csv

# Full JSON summary
cat logs/comparison_summary_all_approaches.json
```

### Per-Approach Logs

Detailed logs for each approach:

```
logs/
├── frequency-comparison-fetching/
│   └── complex_oscillation_freq_0.1/
│       └── iteration1/
│           ├── fetching_results.csv          # Query results
│           ├── fetching_metadata.json        # Latency info
│           └── fetching_client_side_log.csv  # Full logs
├── frequency-comparison-approximation/
│   └── complex_oscillation_freq_0.1/
│       └── iteration1/
│           ├── approximation_results.csv
│           ├── approximation_metadata.json
│           └── approximation_approach_log.csv
└── frequency-comparison-chunked/
    └── complex_oscillation_freq_0.1/
        └── iteration1/
            ├── chunked_results.csv
            ├── chunked_metadata.json
            └── streaming_query_chunk_aggregator_log.csv
```

---

## Manual Steps (If Needed)

If you need to run approaches individually or re-extract results:

### 1. Run Individual Approach

```bash
# Fetching only
node experiments/frequency-comparison/experiment-frequency-fetching-with-capture.js 0.1

# Approximation only
node experiments/frequency-comparison/experiment-frequency-approximation-with-capture.js 0.1

# Chunked only
node experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js 0.1
```

### 2. Extract Results from Logs

```bash
node experiments/frequency-comparison/extract-results-from-logs.js fetching 0.1
node experiments/frequency-comparison/extract-results-from-logs.js approximation 0.1
node experiments/frequency-comparison/extract-results-from-logs.js chunked 0.1
```

### 3. Run Analysis

```bash
node analysis/accuracy/accuracy-comparison-all-approaches.js
```

---

## Understanding the Results

### First-Event Latency

All three approaches should show **similar latency (~60-62 seconds)** because:
- Window configuration: `RANGE 120000 STEP 60000`
- First result emitted at STEP boundary (60 seconds)
- Query semantics are identical across approaches

**Key Insight:** First-event latency is governed by window configuration, not processing approach.

### Accuracy (MAPE)

- **Fetching:** 0% (baseline/ground truth)
- **Approximation:** < 0.1% at low frequencies, may increase near Nyquist (2.0 Hz)
- **Chunked:** < 0.1% at low frequencies, similar to approximation

**Key Insight:** Both approximation and chunked maintain high accuracy with minimal error.

### Why Use Approximation or Chunked?

Since accuracy and latency are similar, the advantages are:
- **Lower resource usage** (CPU, memory)
- **Reduced network overhead** (transferring aggregates vs raw data)
- **Better scalability** (handling more concurrent queries)
- **Incremental processing** (can emit results early)

---

## Interpreting Specific Metrics

### MAPE (Mean Absolute Percentage Error)
- **< 0.1%**: Excellent accuracy
- **0.1% - 1%**: Good accuracy for real-time systems
- **> 1%**: May need investigation

### MAE (Mean Absolute Error)
- Absolute difference between approach and baseline
- Lower is better
- Scale depends on your data values

### RMSE (Root Mean Square Error)
- Penalizes larger errors more heavily
- Good for detecting outliers
- Lower is better

### First-Event Latency
- Time from query registration to first result
- Expected: ~60 seconds (matching STEP parameter)
- Variance of ±2 seconds is normal

---

## Troubleshooting

### Problem: No results captured

**Solution:** Extract from logs manually
```bash
node experiments/frequency-comparison/extract-results-from-logs.js <approach> <frequency>
```

### Problem: Process hangs or times out

**Checks:**
1. Is MQTT broker running? `ps aux | grep mosquitto`
2. Are data files present? `ls src/streamer/data/frequency_comparison/`
3. Check process logs in `logs/` directories

**Solution:** Kill hanging processes and restart
```bash
pkill -f "StreamingQuery.*Orchestrator"
pkill -f "publish.js"
```

### Problem: Negative latency values in logs

**This is normal!** Negative latency (e.g., `-58388ms`) means:
- Result emitted BEFORE expected window close time
- Shows early emission capability
- Expected close = registration + RANGE (120s)
- Result emitted at STEP (60s)
- Difference = -60s (approximately)

### Problem: Analysis script fails

**Check:** Do all three metadata files exist?
```bash
ls logs/frequency-comparison-*/complex_oscillation_freq_0.1/iteration1/*_metadata.json
```

**Solution:** Re-extract missing results
```bash
node experiments/frequency-comparison/extract-results-from-logs.js <missing_approach> 0.1
```

---

## Next Steps

After running the quick test:

1. **Review results:** Check CSV files in `logs/` directory
2. **Compare approaches:** Look at accuracy and latency tables
3. **Run full test:** Test all frequencies with `run-all-approaches-comparison.js`
4. **Read detailed docs:** See `THREE_APPROACH_COMPARISON.md` for in-depth explanation

---

## Quick Commands Reference

```bash
# Quick test (single frequency, all approaches)
node experiments/frequency-comparison/run-all-approaches-comparison.js 0.1

# Full test (all frequencies, all approaches)
node experiments/frequency-comparison/run-all-approaches-comparison.js

# Extract results manually
node experiments/frequency-comparison/extract-results-from-logs.js <approach> <frequency>

# Run analysis only
node analysis/accuracy/accuracy-comparison-all-approaches.js

# Clean up old logs (be careful!)
rm -rf logs/frequency-comparison-*
```

---

## Expected Timeline

**Single Frequency Test (0.1 Hz):**
- Fetching: ~3 minutes
- Approximation: ~3 minutes
- Chunked: ~3 minutes
- Extraction & Analysis: ~1 minute
- **Total: ~15-20 minutes**

**Full Test (All Frequencies):**
- 5 frequencies × 3 approaches × ~3 minutes = ~45 minutes
- Extraction & Analysis: ~5 minutes
- Overhead & delays: ~10 minutes
- **Total: ~60-75 minutes**

---

## Questions?

- **What query is being run?** See `THREE_APPROACH_COMPARISON.md` - Section "Query Configuration"
- **How is latency calculated?** See `FIRST_EVENT_LATENCY.md`
- **What's the difference between approaches?** See `THREE_APPROACH_COMPARISON.md` - Section "Approaches Tested"
- **Why similar latency?** Window STEP parameter (60s) controls result emission timing

---

## Success Indicators

You'll know the test succeeded when you see:

✅ All three approaches complete without errors
✅ Metadata JSON files created for each approach
✅ Results CSV files contain data
✅ Analysis produces comparison tables
✅ MAPE values < 1% for all approaches at low frequencies
✅ First-event latency ~60-62 seconds for all approaches

**Happy testing!** 🚀