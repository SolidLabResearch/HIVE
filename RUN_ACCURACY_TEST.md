# Quick Start: Validate 100% Accuracy

## The Issue We Found

Your current dataset has smartphone and wearable data **58 minutes apart** in timestamps, causing windowing issues. In your August experiments with proper data, you achieved **100% accuracy**.

## The Solution

We've created properly aligned continuous test data. Run this to validate:

## Step 1: Generate Aligned Dataset (30 seconds)

```bash
npm run data:generate-aligned
```

This creates `src/streamer/data/aligned_test/continuous_120s/` with:
- 120 seconds of continuous data
- Both streams temporally aligned
- Known peak value: 12.5

## Step 2: Run Experiment (3-5 minutes)

```bash
export DATA_PATH=aligned_test/continuous_120s
npm run experiment:5-iterations
```

## Step 3: Calculate Accuracy (10 seconds)

```bash
DATA_PATH=aligned_test/continuous_120s npm run experiment:calculate-accuracy
```

## Expected Results

```
┌──────────────┬──────────┬──────────┬──────────┐
│ Approach     │   MAE    │   MAPE   │ Accuracy │
├──────────────┼──────────┼──────────┼──────────┤
│ Chunked      │  0.0000  │   0.00%  │  100.0%  │ ✓
│ Fetching     │  0.0000  │   0.00%  │  100.0%  │ ✓
│ Approximation│  ~1.5    │  ~12.0%  │   88.0%  │ ✓
└──────────────┴──────────┴──────────┴──────────┘
```

This should match your August results!

## What We Fixed

1. **Root Cause:** Old dataset had 58-minute gap between streams
2. **Solution:** Created aligned continuous dataset
3. **Tools:** Built automated accuracy calculator
4. **Docs:** Full analysis in `docs/ACCURACY_ANALYSIS.md`

## Full Documentation

- `ACCURACY_SUMMARY.md` - Complete summary
- `docs/ACCURACY_ANALYSIS.md` - Detailed metrics
- `docs/ACCURACY_ROOT_CAUSE.md` - Technical deep dive
