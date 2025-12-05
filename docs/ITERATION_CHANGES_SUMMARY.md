# Iteration Changes Summary

## Overview

The experiment configuration has been updated to support 35 iterations with warmup and cooldown periods, and each iteration's results are now saved in separate folders.

## Changes Made

### 1. Configuration Updates

**File**: `scripts/benchmarks/frequency-experiment-config.json`

**Changes**:
```json
{
  "experiment": {
    "iterations": 35,              // Increased from 5 to 35
    "warmupIterations": 3,         // NEW: First 3 iterations excluded
    "cooldownIterations": 2        // NEW: Last 2 iterations excluded
  },
  "saveIterationsSeparately": true // NEW: Save each iteration in folder
}
```

### 2. Iteration Breakdown

**Total Iterations**: 35 per experiment combination

**Iteration Types**:
- **Warmup** (iterations 1-3): System warmup, excluded from analysis
- **Valid** (iterations 4-33): Used for performance analysis (30 iterations)
- **Cooldown** (iterations 34-35): System cooldown, excluded from analysis

**Why Warmup/Cooldown?**
- Warmup iterations allow the system to reach steady state
- Removes JIT compilation effects, cache warming, etc.
- Cooldown iterations capture any end-of-run effects
- Only valid iterations provide accurate performance metrics

### 3. Output Structure

**New Directory Structure**:
```
results/frequency-experiments/
├── detailed-results-[timestamp].json
├── results-all-[timestamp].csv              # All 1,260 experiments
├── results-valid-[timestamp].csv            # Only 1,080 valid experiments
└── [approach]/
    └── [frequency]/
        └── [deviceType]/
            ├── iteration_1/
            │   └── result.json
            ├── iteration_2/
            │   └── result.json
            ...
            └── iteration_35/
                └── result.json
```

**Per-Iteration Folders**:
- Each iteration saved in: `[approach]/[frequency]/[deviceType]/iteration_[N]/result.json`
- Contains complete metrics, results, and metadata for that iteration
- Allows detailed per-iteration analysis

### 4. Total Experiments

**Previous Configuration**:
- Iterations: 5
- Total experiments: 3 approaches × 6 frequencies × 2 devices × 5 iterations = 180

**New Configuration**:
- Total iterations: 35
- Warmup: 3 (excluded)
- Valid: 30 (analyzed)
- Cooldown: 2 (excluded)
- Total experiments: 3 × 6 × 2 × 35 = 1,260
- Valid experiments: 3 × 6 × 2 × 30 = 1,080

### 5. CSV Output

**Two CSV Files Generated**:

1. **results-all-[timestamp].csv**
   - Contains all 1,260 experiments
   - Includes warmup and cooldown iterations
   - New columns: `is_warmup`, `is_cooldown`

2. **results-valid-[timestamp].csv**
   - Contains only 1,080 valid experiments
   - Warmup and cooldown iterations filtered out
   - Ready for analysis without additional filtering

**CSV Columns**:
```csv
approach,frequency,deviceType,iteration,is_warmup,is_cooldown,timestamp,latency_ms,memory_mb,accuracy_percent,throughput_obs_sec,observations_processed,execution_time_ms,error
```

### 6. Code Changes

**File**: `scripts/benchmarks/run-frequency-experiment.ts`

**Key Updates**:
- Added `warmupIterations` and `cooldownIterations` to config interface
- Added `isWarmup` and `isCooldown` to experiment results
- Added `saveIterationsSeparately` option
- Created `saveIterationResult()` method for per-iteration storage
- Modified `saveResults()` to generate both all-results and valid-results CSV
- Updated summary generation with warmup/cooldown statistics
- Added iteration type logging during execution

## How to Use

### Run Experiments

```bash
# Run all experiments with new configuration
npm run experiment:run
```

**Console Output**:
```
Starting Frequency-Based Streaming Query Experiments
======================================================================
Experiment: Frequency-Based Streaming Query Comparison
Total Iterations: 35
Warmup Iterations: 3 (excluded from analysis)
Valid Iterations: 30 (used for analysis)
Cooldown Iterations: 2 (excluded from analysis)
...
  chunked-query-approach:
    4Hz smartphone:
      Iteration 1 [WARMUP]: [OK] (125.50ms, 45.20MB)
      Iteration 2 [WARMUP]: [OK] (128.30ms, 46.10MB)
      Iteration 3 [WARMUP]: [OK] (127.80ms, 45.80MB)
      Iteration 4 [VALID]: [OK] (126.20ms, 45.50MB)
      ...
      Iteration 33 [VALID]: [OK] (127.50ms, 45.90MB)
      Iteration 34 [COOLDOWN]: [OK] (129.10ms, 46.20MB)
      Iteration 35 [COOLDOWN]: [OK] (130.50ms, 46.80MB)
```

### Access Results

**Individual Iteration**:
```bash
# View specific iteration result
cat results/frequency-experiments/chunked-query-approach/4Hz/smartphone/iteration_10/result.json | jq

# View all iterations for a specific combination
ls results/frequency-experiments/chunked-query-approach/4Hz/smartphone/
```

**Valid Results Only** (recommended for analysis):
```bash
cat results/frequency-experiments/results-valid-*.csv
```

**All Results** (including warmup/cooldown):
```bash
cat results/frequency-experiments/results-all-*.csv
```

### Analyze Results

**Python Example**:
```python
import pandas as pd

# Load only valid iterations
df = pd.read_csv('results/frequency-experiments/results-valid-2025-12-05T20-30-00-000Z.csv')

# Calculate mean and std for each approach
stats = df.groupby('approach')['latency_ms'].agg(['mean', 'std', 'min', 'max'])
print(stats)

# With 30 iterations, you can calculate confidence intervals
from scipy import stats as sp_stats
confidence_level = 0.95
for approach in df['approach'].unique():
    data = df[df['approach'] == approach]['latency_ms']
    ci = sp_stats.t.interval(confidence_level, len(data)-1, 
                              loc=data.mean(), scale=data.std()/np.sqrt(len(data)))
    print(f"{approach}: {data.mean():.2f}ms ± {ci[1]-data.mean():.2f}ms (95% CI)")
```

**Filter by Iteration Type**:
```bash
# All results file contains iteration type flags
awk -F',' 'NR==1 || ($5=="false" && $6=="false")' results-all-*.csv > valid-only.csv
```

## Benefits

### Statistical Robustness
- **30 valid iterations** provide strong statistical power
- Can calculate confidence intervals with high accuracy
- Detect smaller performance differences between approaches
- More reliable mean and variance estimates

### System Stability
- **Warmup iterations** ensure steady-state performance
- Remove initialization overhead from measurements
- Eliminate JIT compilation effects
- Cache warming effects excluded

### Detailed Analysis
- **Per-iteration folders** enable deep-dive analysis
- Identify outlier iterations easily
- Debug specific iteration failures
- Track performance trends across iterations

### Clean Data
- **Separate CSV files** for all vs valid results
- No manual filtering needed for analysis
- Clear distinction between warmup/valid/cooldown
- Analysis scripts can use valid-results directly

## Migration from Previous Configuration

**Before** (5 iterations):
- All iterations used for analysis
- 180 total experiments
- Single CSV file
- No per-iteration storage

**After** (35 iterations):
- Only iterations 4-33 used for analysis
- 1,260 total experiments (1,080 valid)
- Two CSV files (all + valid)
- Per-iteration folders with complete data

**No Breaking Changes**:
- Existing analysis scripts work with valid-results CSV
- File format remains compatible
- Only new columns added (is_warmup, is_cooldown)

## Summary

- Total iterations increased from 5 to 35
- First 3 iterations excluded as warmup
- Last 2 iterations excluded as cooldown
- 30 valid iterations used for analysis
- Each iteration saved in separate folder
- Two CSV outputs: all results and valid results only
- 1,080 valid experiments provide robust statistical analysis
- Clear labeling of iteration types in all outputs

For detailed information, see `docs/EXPERIMENT_DATA_STORAGE.md`.