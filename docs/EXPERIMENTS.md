# Experiment Guide

This document describes all available experiments in the Streaming Query Hive project.

## Quick Reference

```bash
# Quick validation (5 iterations)
npm run experiment:5-iterations                    # Multi-approach test (5 runs)
npm run experiment:patterns-approx-vs-fetching-5   # Pattern-based test (5 runs)

# Comprehensive validation (35 iterations)
npm run experiment:35-iterations                   # Multi-approach test (35 runs)
npm run experiment:patterns-approx-vs-fetching     # Pattern-based test (35 runs)

# Real-world frequency experiments
npm run experiment:run-realworld                   # Real MQTT-based experiments

# Setup and analysis
npm run experiment:setup                           # Generate test data
npm run experiment:test-mqtt                       # Test MQTT infrastructure
npm run experiment:analyze                         # Analyze results
```

---

## Experiment Types

### 1. Multi-Run Verification Tests

Tests all 3 streaming query approaches with pre-recorded data to verify stability.

**Approaches tested:**
- Approximation Approach
- Chunked Query Approach
- Fetching Client Side (ground truth)

#### Quick Test (5 iterations)
```bash
npm run experiment:5-iterations
```
- **Duration:** ~15-20 minutes
- **Purpose:** Quick validation, bug checking, development testing
- **Iterations:** 5 runs per approach
- **Output:** Summary statistics, pass/fail for each run

#### Comprehensive Test (35 iterations)
```bash
npm run experiment:35-iterations
```
- **Duration:** ~1.5-2 hours
- **Purpose:** Statistical significance, stability verification, publication-ready results
- **Iterations:** 35 runs per approach
- **Output:** Detailed statistics, CSV results, confidence intervals

**When to use:**
- 5 iterations: Development, debugging, quick validation
- 35 iterations: Final validation, research papers, production readiness

---

### 2. Pattern-Based Approximation Tests

Tests approximation approach against ground truth across different data patterns.

**Approaches tested:**
- Approximation Approach
- Fetching Client Side (ground truth)

**Patterns tested:**
- Constant values
- Linear increase/decrease
- Sinusoidal waves
- Random noise
- Step functions
- Spike patterns
- Combined patterns

#### Quick Pattern Test (5 iterations)
```bash
npm run experiment:patterns-approx-vs-fetching-5
```
- **Duration:** ~30-40 minutes
- **Purpose:** Quick pattern validation
- **Iterations:** 5 runs per pattern (7 patterns = 35 total runs)
- **Output:** Per-pattern accuracy, average values, pass/fail

#### Comprehensive Pattern Test (35 iterations)
```bash
npm run experiment:patterns-approx-vs-fetching
```
- **Duration:** ~4-5 hours
- **Purpose:** Statistical pattern analysis, accuracy verification
- **Iterations:** 35 runs per pattern (7 patterns = 245 total runs)
- **Output:** Detailed pattern analysis, statistical comparisons, CSV results

**When to use:**
- 5 iterations: Testing pattern handling, development
- 35 iterations: Research validation, accuracy studies

---

### 3. Real-World Frequency Experiments

Tests all approaches with real sensor data at different frequencies.

```bash
npm run experiment:run-realworld
```

**Features:**
- Real MQTT broker integration
- Multiple frequencies: 4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz
- Device types: smartphone, wearable, combined
- 2 minutes of pre-recorded sensor data per test

**Approaches tested:**
- Fetching Client Side (ground truth generation)
- Streaming Query Hive (chunked approach)
- Chunked Query Approach
- Approximation Approach

**Duration:** ~3-4 hours (depends on number of approaches × frequencies × iterations)

**Metrics collected:**
- Latency
- Memory usage
- Throughput
- Accuracy (vs ground truth)
- Observations processed

**Output:**
- JSON results file
- CSV data for analysis
- Ground truth comparison
- Performance metrics

---

## Experiment Setup

### Prerequisites

1. **MQTT Broker Running:**
   ```bash
   # Check if mosquitto is running
   pgrep -fl mosquitto
   
   # If not running, start it
   brew services start mosquitto  # macOS
   ```

2. **Project Built:**
   ```bash
   npm run build
   ```

3. **Data Generated (for frequency experiments):**
   ```bash
   npm run experiment:setup
   ```

4. **Test MQTT Infrastructure:**
   ```bash
   npm run experiment:test-mqtt
   ```

---

## Understanding Results

### Multi-Run Verification Output

```
Run 1 Results (95.2s):
  [OK] Approximation: 2 results
  [OK] Chunked:       2 results
  [OK] Fetching:      2 results
```

**Interpretation:**
- `[OK]`: Approach produced results (passed)
- `[X]`: Approach failed (0 results or error)
- Result count: Number of window results emitted

**Expected results per run:** 2
- Window STEP: 60 seconds
- First result at ~60s
- Second result at ~120s

### Pattern Test Output

```
Pattern: constant_values (90.5s):
  [OK] Approximation: 2 results (avg: 10.00)
  [OK] Fetching:      2 results (avg: 10.00)
```

**Interpretation:**
- `avg`: Average value across all results
- Compare approximation vs fetching averages to assess accuracy

### Real-World Experiment Output

```
Iteration 1: [OK] (1.31ms, 24.5MB, 2 results)
```

**Metrics:**
- Latency: Time per query result
- Memory: Peak memory usage
- Results: Number of results collected

---

## Troubleshooting

### No Results Collected

**Symptoms:** Experiment runs but gets 0 results

**Common causes:**
1. Orchestrator not starting (check `[APPROACH]` logs)
2. Publishers not sending data (check `[PUBLISHER]` logs)
3. Wrong MQTT topics (check topic configuration)
4. Experiment duration too short (needs 60s+ for first result)

**Solutions:**
```bash
# Check MQTT broker
pgrep -fl mosquitto

# Rebuild project
npm run build

# Check for port conflicts
lsof -ti :9092 | xargs kill -9
lsof -ti :9093 | xargs kill -9
lsof -ti :9094 | xargs kill -9
```

### Port Already In Use

**Error:** `EADDRINUSE: address already in use :::9092`

**Solution:**
```bash
# Kill processes on health check ports
lsof -ti :9091 | xargs kill -9
lsof -ti :9092 | xargs kill -9
lsof -ti :9093 | xargs kill -9
lsof -ti :9094 | xargs kill -9
```

### Approach Process Crashes

**Check logs for:**
- Missing dependencies
- TypeScript compilation errors
- MQTT connection failures

**Solution:**
```bash
# Rebuild
npm run build

# Test individual orchestrator
node dist/src/approaches/FetchingClientSideApproachOrchestrator.js
```

---

## Choosing the Right Experiment

| Goal | Recommended Experiment |
|------|----------------------|
| Quick validation during development | `experiment:5-iterations` |
| Verify all approaches work | `experiment:5-iterations` |
| Test pattern handling | `experiment:patterns-approx-vs-fetching-5` |
| Statistical significance | `experiment:35-iterations` |
| Research paper results | `experiment:35-iterations` + `experiment:patterns-approx-vs-fetching` |
| Performance benchmarking | `experiment:run-realworld` |
| Frequency analysis | `experiment:run-realworld` |
| Production readiness | All 35-iteration experiments |

---

## Expected Durations

| Experiment | 5 Iterations | 35 Iterations |
|------------|-------------|---------------|
| Multi-run verification | 15-20 min | 1.5-2 hours |
| Pattern-based (7 patterns) | 30-40 min | 4-5 hours |
| Real-world frequency | N/A | 3-4 hours |

**Note:** Times assume 130-second experiment duration per run (120s data + 10s buffer)

---

## Output Files

### Multi-Run Verification
- `results/multi-run-verification-{timestamp}.json` - Detailed results
- Console output with summary statistics

### Pattern-Based Tests
- `results/pattern-test-{timestamp}.json` - Detailed results
- `results/pattern-test-{timestamp}.csv` - CSV for analysis

### Real-World Experiments
- `results/frequency-experiments/detailed-{timestamp}.json` - All data
- `results/frequency-experiments/summary-{timestamp}.csv` - CSV summary
- `replayer-log.csv` - Publisher statistics

### Resource Logs (per approach)
- `approximation_approach_resource_usage.csv`
- `chunked_query_approach_resource_log.csv`
- `fetching_client_side_resource_usage.csv`

---

## Tips for Success

1. **Always build first:** `npm run build`
2. **Start with 5 iterations** to validate everything works
3. **Check MQTT broker** is running before starting
4. **Monitor logs** for `[APPROACH]` and `[PUBLISHER]` messages
5. **Kill orphaned processes** if experiments are interrupted
6. **Use 35 iterations** only for final validation
7. **Save results files** before running new experiments

---

## Support

For issues or questions:
- Check troubleshooting section above
- Review logs for error messages
- Verify MQTT broker is running
- Ensure project is built (`npm run build`)
- Check that data files exist in `src/streamer/data/`

Contact: [Kush](mailto:kushbisen@proton.me)