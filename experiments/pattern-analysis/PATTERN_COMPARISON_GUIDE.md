# Pattern Comparison Experiment Guide

Complete guide for testing all three streaming query approaches (Fetching, Approximation, Chunked) across different data patterns to identify accuracy boundaries.

---

## Overview

This experiment tests where the **approximation approach breaks down** by comparing accuracy across:

1. **Exponential Growth/Decay** - Tests rapid changes (rates: 0.001 to 100)
2. **Noisy Datasets** - Tests robustness to noise (levels: 0.1 to 2.0)

### Why This Matters

The approximation approach works well on smooth frequency data because it uses **time-weighted averaging of sub-query results**. This experiment reveals where this breaks:

- ✅ **Should work**: Slow exponential changes, low noise
- ⚠️ **Might degrade**: Fast exponential changes, moderate noise
- ❌ **Expected to fail**: Very rapid changes (rate 100), high noise (2.0)

---

## Quick Start

### Prerequisites

```bash
# 1. Build the project
npm run build

# 2. Start MQTT broker
mosquitto -c /opt/homebrew/etc/mosquitto/mosquitto.conf

# 3. Verify test data exists
ls src/streamer/data/rate_comparison/
ls src/streamer/data/noisy_datasets/
```

### Run Single Pattern (Recommended First Test)

Test all three approaches on a specific pattern:

```bash
# Test slow exponential growth (should be accurate)
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 1

# Test fast exponential growth (expect accuracy degradation)
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100

# Test noisy data
node experiments/pattern-analysis/run-all-patterns-comparison.js noise 0.5
```

**Time per pattern**: ~15-20 minutes (3 approaches × ~5 min each)

### Run All Patterns (Full Suite)

```bash
node experiments/pattern-analysis/run-all-patterns-comparison.js
```

**Warning**: This runs 51 tests total:
- 12 exponential patterns (growth + decay) × 3 approaches = 36 tests
- 5 noisy patterns × 3 approaches = 15 tests
- **Total time**: ~4-5 hours

---

## Understanding the Results

### Console Output

```
═══════════════════════════════════════════════════════════════════════════════
TESTING: APPROXIMATION - exponential_growth_rate_100
Data: rate_comparison/exponential_growth_rate_100
═══════════════════════════════════════════════════════════════════════════════
...
✓ Test completed in 180.3s

📊 Extracting results for approximation - exponential_growth_rate_100...
✓ Extraction completed

Analyzing: exponential_growth_rate_100
────────────────────────────────────────────────────────────────────────────────
  Approximation:
    Latency: 61.52s (Δ -0.23s)
    MAPE: 15.4328%      ← High error on fast changes!
    MAE: 7.652341
    RMSE: 8.234567
    Memory: 145.2 MB avg, 187.3 MB max
```

### Accuracy Interpretation

| MAPE | Status | Meaning |
|------|--------|---------|
| < 1% | ✓ Good | Approximation is highly accurate |
| 1-5% | ⚠ Fair | Acceptable for most use cases |
| ≥ 5% | ✗ Poor | Significant accuracy loss |

### Expected Results

**Exponential Growth/Decay:**
```
Rate    | Expected MAPE | Reason
--------|---------------|---------------------------------------
0.001   | < 0.1%        | Very slow change, smooth
0.01    | < 0.5%        | Slow change
0.1     | < 1%          | Moderate change
1       | 1-3%          | Faster change
10      | 5-10%         | Rapid change, sub-windows lag behind
100     | > 10%         | Very rapid change, major lag
```

**Noisy Data:**
```
Noise   | Expected MAPE | Reason
--------|---------------|---------------------------------------
0.1     | < 0.5%        | Low noise, averaging helps
0.2     | < 1%          | Moderate noise
0.5     | 1-3%          | Higher noise
1.0     | 3-5%          | High noise, time-weighting less accurate
2.0     | > 5%          | Very high noise
```

---

## Output Files

After running experiments:

```
logs/pattern-comparison/
├── fetching/
│   ├── exponential_growth_rate_1/
│   │   └── iteration1/
│   │       ├── fetching_results.csv           # Query results
│   │       ├── fetching_metadata.json         # Latency & stats
│   │       ├── fetching_resource_usage.csv    # CPU/memory
│   │       └── fetching_client_side_log.csv
│   ├── exponential_growth_rate_100/
│   ├── noise_0.5/
│   └── ...
├── approximation/
│   ├── exponential_growth_rate_1/
│   ├── exponential_growth_rate_100/
│   ├── noise_0.5/
│   └── ...
├── chunked/
│   └── ...
├── pattern_accuracy_comparison.csv            # Main results CSV
├── pattern_analysis_summary.json              # Complete JSON summary
└── pattern_comparison_summary.json            # Execution summary
```

### Key Result Files

**pattern_accuracy_comparison.csv**:
```csv
pattern_type,pattern_value,approach,mape_percent,mae,rmse,avg_value,memory_mb
exponential_growth,1,fetching,0.0,0.0,0.0,50.123456,123.45
exponential_growth,1,approximation,0.8234,0.412345,0.523456,50.535801,98.32
exponential_growth,1,chunked,0.6123,0.305678,0.401234,50.417889,110.21
exponential_growth,100,fetching,0.0,0.0,0.0,75.234567,125.67
exponential_growth,100,approximation,15.4328,11.652341,13.234567,86.886908,102.45
exponential_growth,100,chunked,12.3456,9.234567,10.567890,84.567890,115.89
```

---

## Analysis Reports

The analysis script generates comprehensive reports:

```bash
# View accuracy comparison
cat logs/pattern-comparison/pattern_accuracy_comparison.csv

# View full analysis
node analysis/accuracy/pattern-accuracy-comparison.js
```

### Report Sections

**1. Accuracy Comparison Table**
- Shows MAPE, MAE, RMSE for each pattern
- Memory usage per approach
- Easy to spot where approximation degrades

**2. Approximation Breakdown**
- Groups patterns by type
- Shows accuracy distribution (Good/Fair/Poor)
- Identifies threshold where approximation fails

**3. Key Findings**
- Average MAPE across all patterns
- Percentage of patterns with good/fair/poor accuracy
- Clear identification of failure modes

---

## Understanding Why Approximation Fails

### The Mechanism

Approximation uses **time-weighted averaging**:

```
Main query: RANGE 120s, STEP 60s
Sub-queries: RANGE 60s, STEP 30s

For target window [0-120s]:
- Sub-window 1 [0-60s]:   AVG = 10.0
- Sub-window 2 [30-90s]:  AVG = 15.0
- Sub-window 3 [60-120s]: AVG = 25.0

Approximation = (10.0*60 + 15.0*60 + 25.0*60) / 180 = 16.67
```

### When It Works

**Smooth, slow-changing data:**
- Sub-window averages are representative
- Time-weighting accurately reflects distribution
- Result ≈ true average

### When It Fails

**1. Rapid Exponential Growth (rate 100):**
```
Time    | Value | Sub-Window | Problem
0-60s   | 1-10  | AVG = 5    | ✓ OK
60-120s | 10-100| AVG = 55   | ⚠ Growing fast
120-180s| 100-1000| AVG = 550| ✗ Exploding

Approximation uses old sub-window data → lags behind actual growth
```

**2. High Noise (level 2.0):**
```
Sub-windows capture different noise samples
Time-weighting doesn't account for noise distribution
Result may not represent true average
```

**3. Bursty Data:**
```
If data arrives in bursts, sub-windows may have:
- Empty periods (no data)
- Dense periods (all data)
Time-weighting breaks down
```

---

## Comparison: Chunked vs Approximation

Both use sub-queries, but differently:

| Aspect | Approximation | Chunked |
|--------|---------------|---------|
| Sub-query aggregation | AVG only | AVG + COUNT |
| Combination method | Time-weighted average | Weighted by count |
| Accuracy on fast changes | Degrades | More robust |
| Memory usage | Lower | Moderate |

**Expected**: Chunked should outperform approximation on rapid changes because it weights by actual data points (COUNT), not just time.

---

## Practical Recommendations

### When to Use Approximation

✅ **Good fit:**
- Slowly changing data (rate < 1)
- Low noise (< 0.5)
- Smooth patterns (sine waves, stable values)
- Resource-constrained environments

❌ **Poor fit:**
- Rapid changes (rate > 10)
- High noise (> 1.0)
- Bursty data arrival
- Critical accuracy requirements

### When to Use Chunked

✅ **Good fit:**
- Moderate to fast changes (rate < 100)
- Need better accuracy than approximation
- Have moderate resources available

### When to Use Fetching

✅ **Always accurate** (baseline)
❌ Highest resource usage

---

## Troubleshooting

### No Results Captured

```bash
# Re-extract from logs
node experiments/pattern-analysis/extract-pattern-results.js \
  approximation \
  exponential_growth_rate_100 \
  ./logs/pattern-comparison/approximation/exponential_growth_rate_100/iteration1
```

### Process Hangs

```bash
# Kill stuck processes
pkill -f "StreamingQuery.*Orchestrator"
pkill -f "publish.js"

# Check MQTT broker
ps aux | grep mosquitto
```

### Missing Data Files

```bash
# Check if data exists
ls src/streamer/data/rate_comparison/exponential_growth_rate_100/

# If missing, may need to regenerate
# (check data generation scripts in src/streamer/)
```

### Analysis Shows "Data not available"

This means the experiment didn't run or extraction failed:

1. Check if log directory exists
2. Check if log files contain data
3. Re-run the specific pattern test
4. Re-run extraction script

---

## Advanced Usage

### Run Specific Subset

```bash
# Only exponential patterns
for rate in 0.001 0.01 0.1 1 10 100; do
  node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth $rate
done

# Only noisy patterns
for noise in 0.1 0.2 0.5 1.0 2.0; do
  node experiments/pattern-analysis/run-all-patterns-comparison.js noise $noise
done
```

### Custom Analysis

Edit `analysis/accuracy/pattern-accuracy-comparison.js` to:
- Add custom metrics
- Change accuracy thresholds
- Generate plots (requires plotting library)
- Export to different formats

---

## Expected Timeline

**Single Pattern Test:**
- Fetching: ~5 minutes
- Approximation: ~5 minutes
- Chunked: ~5 minutes
- Extraction & Analysis: ~1 minute
- **Total: ~20 minutes**

**Full Suite (51 patterns):**
- Tests: ~4-5 hours
- Analysis: ~5 minutes
- **Total: ~5 hours**

**Recommended approach**: Start with a few key patterns to verify, then run full suite overnight.

---

## Key Questions This Experiment Answers

1. **At what rate does approximation accuracy degrade?**
   - Look at exponential patterns: threshold is likely between rate 1 and 10

2. **How does noise affect approximation?**
   - Look at noisy patterns: compare MAPE across noise levels

3. **Is chunked more robust than approximation?**
   - Compare MAPE for same patterns: chunked should have lower error

4. **What's the memory trade-off?**
   - Compare memory usage: fetching > chunked > approximation

5. **Where should we NOT use approximation?**
   - Any pattern with MAPE > 5% is a bad fit

---

## Sample Commands

```bash
# Quick test: Single slow pattern (should be accurate)
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 0.1

# Quick test: Single fast pattern (expect degradation)
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100

# Quick test: Noisy data
node experiments/pattern-analysis/run-all-patterns-comparison.js noise 1.0

# Full suite (overnight run)
nohup node experiments/pattern-analysis/run-all-patterns-comparison.js > pattern-experiment.log 2>&1 &

# Check progress
tail -f pattern-experiment.log

# After completion, view results
node analysis/accuracy/pattern-accuracy-comparison.js
```

---

## Success Indicators

You'll know experiments succeeded when:

✅ All three approaches complete for each pattern
✅ Metadata JSON files exist with latency data
✅ Results CSV files contain query outputs
✅ Analysis script generates comparison tables
✅ Clear accuracy degradation visible at high rates/noise
✅ Memory usage data captured for all approaches

---

## Further Reading

- Time-weighted averaging: `src/services/operators/RateBasedApproximationApproachOperator.ts` (line 954)
- Chunked aggregation: `src/services/operators/StreamingQueryChunkAggregatorOperator.ts`
- Window configuration: All orchestrators use RANGE 120s, STEP 60s
- Sub-queries: RANGE 60s, STEP 30s

**Happy experimenting!** 🚀