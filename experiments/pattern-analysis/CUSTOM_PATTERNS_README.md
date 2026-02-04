# Custom Stream Pattern Experiments

## Overview

This experiment suite tests three streaming query approaches across 5 custom stream patterns with configurable iterations (default: 35) for statistical analysis.

## The 5 Custom Patterns

| Pattern | Parameters | Description |
|---------|-----------|-------------|
| **Low Variability** | μ=-23.0, σ=0.25 | Gaussian noise around mean |
| **Step Pattern** | v₁=-23.0, v₂=-15.0, t_step=60s | Step change at 60 seconds |
| **Spike Pattern** | v_base=-23.0, v_spike=-5.0, Δt=1.25s | Brief spike at center (60s) |
| **Low Freq. Oscillation** | μ=-23.0, A=5.0, f=0.05Hz | Slow sinusoidal oscillation |
| **High Freq. Oscillation** | μ=-23.0, A=3.0, f=0.5Hz | Fast sinusoidal oscillation |

## Quick Start

### Step 1: Generate Pattern Data

```bash
# Generate all 5 custom patterns
node scripts/generate-custom-patterns.js
```

This creates data in `src/streamer/data/custom_patterns/` with:
- 120 seconds duration
- 4 Hz sampling rate (250ms interval)
- 480 data points per pattern
- Both smartphone and wearable sensor data
- RDF N-Triples format + CSV for inspection

### Step 2: Run Experiments

```bash
# Run all 5 patterns with 35 iterations (default)
# Total: 525 tests (5 patterns × 3 approaches × 35 iterations)
# Runtime: ~26 hours
node experiments/pattern-analysis/run-custom-patterns-comparison.js

# Run with fewer iterations for testing
node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 3

# Run specific pattern with 35 iterations
node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability
node experiments/pattern-analysis/run-custom-patterns-comparison.js step_pattern
node experiments/pattern-analysis/run-custom-patterns-comparison.js spike_pattern
node experiments/pattern-analysis/run-custom-patterns-comparison.js low_freq_oscillation
node experiments/pattern-analysis/run-custom-patterns-comparison.js high_freq_oscillation

# Run specific pattern with custom iterations
node experiments/pattern-analysis/run-custom-patterns-comparison.js spike_pattern -i 10
```

## Experiment Configuration

### Test Matrix

- **Patterns**: 5
- **Approaches**: 3 (fetching, approximation, chunked)
- **Iterations**: 35 (default, configurable)
- **Total Tests**: 525

### Window Configuration

```
Main Query:
  RANGE: 120,000 ms (120 seconds)
  STEP: 60,000 ms (60 seconds)

Sub-Query (Approximation & Chunked):
  RANGE: 60,000 ms (60 seconds)
  STEP: 30,000 ms (30 seconds)

Sampling:
  Rate: 4 Hz (250 ms interval)
  Nyquist Frequency: 2.0 Hz
```

### Runtime Estimates

| Configuration | Tests | Estimated Time |
|--------------|-------|----------------|
| 1 pattern, 1 iteration | 3 | ~10 minutes |
| 1 pattern, 35 iterations | 105 | ~5-6 hours |
| All 5 patterns, 1 iteration | 15 | ~45 minutes |
| All 5 patterns, 35 iterations | 525 | ~26 hours |

## Output Structure

```
logs/custom-pattern-comparison/
├── fetching/
│   ├── low_variability/
│   │   ├── iteration1/
│   │   │   ├── fetching_orchestrator.log
│   │   │   ├── publisher.log
│   │   │   ├── fetching_results.csv
│   │   │   ├── fetching_metadata.json
│   │   │   ├── fetching_latency_log.csv
│   │   │   └── fetching_resource_usage.csv
│   │   ├── iteration2/
│   │   └── ...iteration35/
│   ├── step_pattern/
│   ├── spike_pattern/
│   ├── low_freq_oscillation/
│   └── high_freq_oscillation/
├── approximation/
│   └── (same structure)
├── chunked/
│   └── (same structure)
└── custom_pattern_comparison_summary.json
```

## Metrics Collected (Per Iteration)

### Accuracy Metrics
- **MAPE** (Mean Absolute Percentage Error)
- **MAE** (Mean Absolute Error)
- **RMSE** (Root Mean Square Error)

### Performance Metrics
- **First-Event Latency**: Time from query registration to first result
- **Memory Usage**: Heap used (MB), average and maximum
- **CPU Usage**: System and user CPU time

### Output Windows
- Each test produces 2 output windows (STEP=60s, DURATION=120s)
- Compared against fetching (ground truth) approach

## Expected Patterns of Behavior

### Low Variability
- **Expected**: All approaches perform similarly
- **MAPE**: < 1% for all
- **Reason**: Low variance, stable mean

### Step Pattern
- **Expected**: Approximation may lag at step transition
- **MAPE**: Approximation 5-10%, Chunked < 5%
- **Reason**: Time-weighted average smooths the step

### Spike Pattern
- **Expected**: Approximation misses spike detail
- **MAPE**: Approximation > 15%, Chunked < 10%
- **Reason**: Spike occurs within sub-window, gets averaged out

### Low Freq. Oscillation (0.05 Hz)
- **Expected**: All approaches accurate
- **MAPE**: < 2% for all
- **Reason**: Changes slower than window size
- **Note**: Well below Nyquist frequency (2.0 Hz)

### High Freq. Oscillation (0.5 Hz)
- **Expected**: Approximation smooths oscillation slightly
- **MAPE**: Approximation 3-8%, Chunked 2-5%
- **Reason**: Multiple cycles within window, time-weighting averages
- **Note**: Still below Nyquist, adequately sampled

## Analysis (After Running Experiments)

The current analysis script needs to be adapted for multiple iterations. You'll need to:

1. **Aggregate across iterations** to compute mean ± std for:
   - MAPE, MAE, RMSE
   - Latency
   - Memory usage

2. **Statistical significance tests** between approaches

3. **Generate outputs**:
   - CSV with mean ± std columns
   - JSON with full statistical summaries
   - Confidence intervals (95%)

See `ANALYSIS_TODO.md` for implementation details.

## LaTeX Table for Paper

```latex
\begin{table}[h]
\centering
\caption{Experimental Stream Patterns (n=35 iterations per pattern-approach)}
\label{tab:stream-patterns}
\begin{tabular}{c|l}
\hline
\textbf{Stream Pattern} & \textbf{Parameters} \\
\hline
Low Variability & $\mu=-23.0$, $\sigma=0.25$ \\
\hline
Step Pattern & $v_1=-23.0$, $v_2=-15.0$, $t_{step}=60$s \\
\hline
Spike Pattern & $v_{base}=-23.0$, $v_{spike}=-5.0$, $\Delta t=1.25$s \\
\hline
Low Freq. Oscillation & $\mu=-23.0$, $A=5.0$, $f=0.05$Hz \\
\hline
High Freq. Oscillation & $\mu=-23.0$, $A=3.0$, $f=0.5$Hz \\ 
\hline
\end{tabular}
\end{table}
```

## Running on Remote Server

```bash
# SSH to server
ssh your-server

# Clone/sync code
cd streaming-query-hive

# Generate data (only once)
node scripts/generate-custom-patterns.js

# Run experiments in background
nohup node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 35 > experiment.log 2>&1 &

# Monitor progress
tail -f experiment.log

# Or use screen/tmux
screen -S patterns
node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 35
# Ctrl+A, D to detach
# screen -r patterns to reattach
```

## Verification

After experiments complete:

```bash
# Count completed iterations (should be 15 for all 5 patterns × 3 approaches)
find logs/custom-pattern-comparison -name "iteration35" | wc -l

# Check summary
cat logs/custom-pattern-comparison/custom_pattern_comparison_summary.json

# List all pattern directories
ls logs/custom-pattern-comparison/fetching/
# Should show:
# low_variability
# step_pattern
# spike_pattern
# low_freq_oscillation
# high_freq_oscillation
```

## Troubleshooting

### Data not found error
```bash
# Make sure data is generated
node scripts/generate-custom-patterns.js

# Verify data exists
ls src/streamer/data/custom_patterns/*/smartphone.acceleration.x/data.nt
```

### Port conflicts
```bash
# Kill any running orchestrators
pkill -f "StreamingQuery"
pkill -f "publish.js"
```

### Out of memory
```bash
# Increase Node.js heap
NODE_OPTIONS="--max-old-space-size=8192" node experiments/pattern-analysis/run-custom-patterns-comparison.js -i 35
```

### Resume partial run
The runner doesn't auto-resume. To continue:

```bash
# Check which patterns are complete
ls logs/custom-pattern-comparison/fetching/*/iteration35

# Run missing patterns individually
node experiments/pattern-analysis/run-custom-patterns-comparison.js spike_pattern -i 35
```

## Data Inspection

Each pattern has a CSV file for easy inspection:

```bash
# View generated data
head -20 src/streamer/data/custom_patterns/low_variability/data.csv
head -20 src/streamer/data/custom_patterns/spike_pattern/data.csv

# Check metadata
cat src/streamer/data/custom_patterns/step_pattern/metadata.json
```

## Next Steps

1. ✅ Generate custom pattern data
2. ✅ Run experiments with 35 iterations
3. ⏳ Create multi-iteration analysis script (see `ANALYSIS_TODO.md`)
4. ⏳ Generate plots (MAPE by pattern, memory usage, etc.)
5. ⏳ Compute statistical significance between approaches
6. ⏳ Create LaTeX tables for paper

## Notes

- Each pattern is exactly 120 seconds (matches RANGE of main query)
- Sampling at 4 Hz provides good temporal resolution
- 35 iterations provides sufficient data for mean ± std reporting
- All patterns use acceleration.x values around -23.0 (typical smartphone accelerometer range)
- Step and spike patterns designed to test response to abrupt changes
- Oscillation patterns test temporal aggregation at different frequencies