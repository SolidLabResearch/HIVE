# Quick Start Guide - Streaming Query Hive

## System is Fixed and Working! ✅

The system now produces results consistently. Two critical fixes were applied:
1. Orchestrators stay alive during experiments
2. Flush events trigger final window emissions

## Quick Run

```bash
# 1. Build the project
npm run build

# 2. Run a quick test (5 iterations)
npm run experiment:5-iterations
```

**Expected Results**:
- Approximation: ~1 result per run
- Chunked: ~4-8 results per run  
- Fetching: ~4 results per run

## What Was Fixed

### Problem
Experiments returned 0 results because:
- Orchestrators exited after 2 seconds (before windows could produce results)
- Windows never emitted final results (no watermark advancement)

### Solution
- Orchestrators now stay alive for full experiment duration
- Flush events with future timestamps trigger window emissions after data stops

## Available Commands

### Experiments
```bash
npm run experiment:5-iterations                      # Quick test (5 runs)
npm run experiment:35-iterations                     # Full test (35 runs)
npm run experiment:patterns-approx-vs-fetching-5     # Pattern comparison (5 runs)
npm run experiment:patterns-approx-vs-fetching       # Pattern comparison (full)
```

### Testing & Verification
```bash
npm run test:orchestrator-lifecycle    # Verify orchestrators stay alive
npm run experiment:test-mqtt           # Test MQTT infrastructure
```

### Maintenance
```bash
npm run clean-logs                     # Clean logs before new experiments
npm run build                          # Rebuild after code changes
```

## Understanding Results

### Result Counts
- **Approximation**: 1 result (final aggregated value)
- **Chunked**: 4-8 results (multiple window emissions)
- **Fetching**: 4 results (window-based emissions)

### Success Rates
Current performance after fixes:
- Approximation: 100% (5/5 runs)
- Fetching: 100% (5/5 runs)
- Chunked: 80% (4/5 runs) - occasional timing issues

### Expected Logs
You'll see these messages (they're NORMAL):
```
[APPROXIMATION ERROR] Could not fetch queries from HTTP server
[CHUNKED ERROR] Could not fetch queries from HTTP server
```
These approaches use locally-provided queries, not the HTTP server. The fallback works correctly.

## Experiment Timeline

For a typical run:
- **0-10s**: Orchestrators initialize, connect to MQTT
- **10-90s**: Data publishes (481 observations at 4 Hz)
- **90s**: Flush events sent (triggers window emissions)
- **90-110s**: Results collected
- **110s**: Orchestrators terminated, summary generated

## Output Files

After running experiments, check:
```
results/
├── approximation_results.csv       # Approximation results
├── chunked_query_results.csv       # Chunked results  
├── fetching_client_side_results.csv # Fetching results

logs/
├── approximation/orchestrator.csv  # Approximation logs
├── fetching/orchestrator.csv       # Fetching logs
├── CSPARQLWindow.log               # Window processing logs
├── R2ROperator.log                 # Query execution logs

multi-run-results-*.json            # Experiment summary (JSON)
```

## Troubleshooting

### No Results
```bash
# 1. Clean and rebuild
npm run clean-logs
npm run build

# 2. Check MQTT is running
pgrep -fl mosquitto

# 3. If MQTT isn't running, start it
brew services start mosquitto   # macOS
```

### Orchestrators Exit Early
```bash
# Run lifecycle test
npm run test:orchestrator-lifecycle

# Should see: "[PASS] Orchestrator stayed alive for 30s"
```

### Port Conflicts
If you see `EADDRINUSE` errors:
```bash
# Kill processes on conflicting ports
lsof -ti :8081 | xargs kill -9  # Approximation HTTP
lsof -ti :8082 | xargs kill -9  # Chunked HTTP
lsof -ti :8083 | xargs kill -9  # Fetching HTTP
lsof -ti :9091 | xargs kill -9  # Approximation health
lsof -ti :9092 | xargs kill -9  # Fetching health
```

### Orphaned Processes
```bash
# Kill all experiment-related processes
pkill -f "ApproximationApproachOrchestrator"
pkill -f "ChunkedQueryApproachOrchestrator"
pkill -f "FetchingClientSideApproachOrchestrator"
pkill -f "experiment-publisher"
```

## Query Configuration

### Subquery Windows
```
RANGE: 60000ms (60 seconds)
STEP:  30000ms (30 seconds)
Results: Emit at 30s and 60s
```

### Main Query Windows
```
RANGE: 120000ms (120 seconds)
STEP:  60000ms (60 seconds)
Results: Emit at 60s and 120s
```

## Documentation

Detailed docs:
- `FIX_SUMMARY.md` - What was fixed and why
- `docs/ORCHESTRATOR_LIFECYCLE_FIX.md` - Technical deep dive
- `docs/EXPERIMENTS.md` - Full experiment guide
- `README.md` - Project overview

## What to Expect

### Normal Run Output
```
======================================================================
MULTI-RUN VERIFICATION: 5 ITERATIONS (QUICK TEST)
======================================================================

[INFO] Starting run 1/5...
  [RUN 1] Launching orchestrators...
  [RUN 1] All orchestrators launched
  [RUN 1] Waiting 10s for initialization...
  [RUN 1] Publishing test data...
  [Run #1] Data replay completed
  [Run #1] Sending flush events...
  [Run #1] Flush events sent
  [RUN 1] Waiting 20s for final results...
  
  Run 1 Results (157.8s):
    [OK] Approximation: 1 results
    [OK] Chunked:       8 results
    [OK] Fetching:      4 results

======================================================================
FINAL MULTI-RUN SUMMARY
======================================================================

Approximation Approach:
  Success rate: 100% (5/5)
  Avg results per run: 1.0

Chunked Query Approach:
  Success rate: 80% (4/5)
  Avg results per run: 4.4

Fetching Client Side:
  Success rate: 100% (5/5)
  Avg results per run: 4.0
```

## Performance

Typical experiment timing:
- 5 iterations: ~14 minutes
- 35 iterations: ~90 minutes

## System Requirements

- Node.js (working installation confirmed)
- TypeScript (via npx)
- Mosquitto MQTT broker (running on port 1883)
- ~2GB RAM for orchestrators + publishers
- Ports: 1883 (MQTT), 8081-8083 (HTTP), 9091-9094 (health checks)

---

**Status**: System is operational and producing results ✅

For issues or questions, check:
1. `FIX_SUMMARY.md` for recent changes
2. `docs/EXPERIMENTS.md` for detailed experiment info
3. Log files in `logs/` directory