# Quick Start Guide - 35 Iteration Pattern Experiments

## What Changed

The pattern analysis runner now supports multiple iterations per test:

- **Default**: 35 iterations per pattern-approach combination
- **Configurable**: Use `--iterations N` or `-i N` to set custom count
- **Per-iteration logging**: Each iteration gets its own directory
- **Statistical analysis**: Enables mean ± std calculation across iterations

## Commands

### Run ALL Patterns with 35 Iterations (Default)

```bash
node experiments/pattern-analysis/run-all-patterns-comparison.js
```

This will run:
- 17 patterns (12 exponential + 5 noisy)
- 3 approaches (fetching, approximation, chunked)
- 35 iterations each
- **Total: 1,785 tests (~89 hours)**

### Run Specific Pattern with 35 Iterations

```bash
# Exponential growth, rate 100, 35 iterations
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100

# Exponential decay, rate 0.01, 35 iterations
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_decay 0.01

# Noisy data, noise level 0.5, 35 iterations
node experiments/pattern-analysis/run-all-patterns-comparison.js noise 0.5
```

### Run with Custom Iteration Count

```bash
# All patterns, 10 iterations each (for testing)
node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 10

# Specific pattern, 50 iterations
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100 --iterations 50

# Single run (no statistical analysis)
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100 --iterations 1
```

### Short Flag

```bash
# -i is shorthand for --iterations
node experiments/pattern-analysis/run-all-patterns-comparison.js -i 35
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100 -i 35
```

## Recommended Workflow

### 1. Test the Pipeline (Quick)

Run a small test first to validate everything works:

```bash
# Run 1 pattern with 3 iterations (~9 tests, ~30 minutes)
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 0.1 -i 3
```

Check the output:
```bash
ls -la logs/pattern-comparison/fetching/exponential_growth_rate_0.1/
# Should see: iteration1, iteration2, iteration3
```

### 2. Run Full Experiment (Long)

On your remote server:

```bash
# Start in background with nohup
nohup node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 35 > experiment.log 2>&1 &

# Or use screen/tmux
screen -S pattern-experiment
node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 35
# Ctrl+A, D to detach
```

Monitor progress:
```bash
tail -f experiment.log
```

### 3. Run Analysis

After all tests complete:

```bash
node analysis/accuracy/pattern-accuracy-comparison.js
```

This will generate:
- `logs/pattern-comparison/pattern_accuracy_comparison.csv`
- `logs/pattern-comparison/pattern_analysis_summary.json`

## Output Structure

```
logs/pattern-comparison/
├── fetching/
│   └── exponential_growth_rate_100/
│       ├── iteration1/
│       │   ├── fetching_orchestrator.log
│       │   ├── publisher.log
│       │   ├── fetching_results.csv
│       │   ├── fetching_metadata.json
│       │   ├── fetching_latency_log.csv
│       │   └── fetching_resource_usage.csv
│       ├── iteration2/
│       │   └── (same files)
│       └── ...
│       └── iteration35/
├── approximation/
│   └── (same structure)
└── chunked/
    └── (same structure)
```

## Expected Runtime

| Configuration | Tests | Estimated Time |
|--------------|-------|----------------|
| 1 pattern, 1 iteration, all approaches | 3 | ~10 minutes |
| 1 pattern, 35 iterations, all approaches | 105 | ~5-6 hours |
| All patterns, 1 iteration, all approaches | 51 | ~3 hours |
| All patterns, 35 iterations, all approaches | 1,785 | ~89 hours (~3.7 days) |

## Pattern List

### Exponential Patterns (12)
- `exponential_growth` with rates: 0.001, 0.01, 0.1, 1, 10, 100
- `exponential_decay` with rates: 0.001, 0.01, 0.1, 1, 10, 100

### Noisy Patterns (5)
- `noise` with levels: 0.1, 0.2, 0.5, 1.0, 2.0

## Progress Tracking

The runner outputs progress like:

```
████████████████████████████████████████████████████████████████████████████████
COMPREHENSIVE PATTERN COMPARISON - ALL APPROACHES
████████████████████████████████████████████████████████████████████████████████
Exponential patterns: 12
Noisy patterns: 5
Approaches: fetching, approximation, chunked
Iterations per test: 35
Total tests: 1785
████████████████████████████████████████████████████████████████████████████████

████████████████████████████████████████████████████████████████████████████████
PATTERN SET: EXPONENTIAL
Total patterns: 12
Iterations per pattern-approach: 35
Total tests: 1260
████████████████████████████████████████████████████████████████████████████████

────────────────────────────────────────────────────────────────────────────────
Pattern: exponential_growth_rate_0.001
────────────────────────────────────────────────────────────────────────────────

  Approach: FETCHING

================================================================================
TESTING: FETCHING - exponential_growth_rate_0.001 - Iteration 1/35
Data: rate_comparison/exponential_growth_rate_0.001
================================================================================
...
```

## Disk Space Requirements

- Per iteration: ~5-10 MB
- Per pattern-approach (35 iterations): ~175-350 MB
- All tests: ~9-18 GB (raw logs)
- With analysis outputs: ~20 GB total

Check available space:
```bash
df -h .
```

## Stopping/Resuming

### Stop Experiment
```bash
# If running in foreground
Ctrl+C

# If running in background
pkill -f "run-all-patterns-comparison"
```

### Resume Strategy
The runner doesn't have built-in resume. To continue:

1. Check which patterns completed:
```bash
ls -d logs/pattern-comparison/fetching/*/iteration35 | wc -l
```

2. Run remaining patterns individually:
```bash
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 10 -i 35
node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 100 -i 35
# etc.
```

## Troubleshooting

### "Data not found" error
```bash
# Check data exists
ls src/streamer/data/rate_comparison/
ls src/streamer/data/noisy_datasets/
```

### Out of memory
```bash
# Increase Node.js heap size
NODE_OPTIONS="--max-old-space-size=8192" node experiments/pattern-analysis/run-all-patterns-comparison.js -i 35
```

### Port already in use
```bash
# Kill existing processes
pkill -f "StreamingQueryFetchingClientSideApproachOrchestrator"
pkill -f "StreamingQueryApproximationApproachOrchestrator"
pkill -f "StreamingQueryChunkedApproachOrchestrator"
```

## Next Steps After Completion

1. Verify all iterations completed:
```bash
find logs/pattern-comparison -name "iteration35" | wc -l
# Should be 51 (17 patterns × 3 approaches)
```

2. Run aggregated analysis:
```bash
node analysis/accuracy/pattern-accuracy-comparison.js
```

3. Create statistical summaries and plots (you'll need to write these scripts based on the CSV outputs)

4. Generate LaTeX tables for paper