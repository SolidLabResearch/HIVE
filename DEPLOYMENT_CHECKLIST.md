# Deployment Checklist for Cloud Benchmarking

## Pre-Deployment Verification ✓

All systems verified and operational as of December 5, 2025.

- [x] MQTT broker connection tested
- [x] SPARQL endpoint verified
- [x] All 3 approaches tested successfully
- [x] Data files accessible (480 observations @ 4Hz)
- [x] Performance metrics collection working

## Test Results Summary

```
Approach                  | Time (ms) | Memory (MB) | Throughput (obs/s)
--------------------------|-----------|-------------|-------------------
fetching-client-side      | 17        | 2.25        | 28,235
chunked-query-approach    | 15        | 3.11        | 32,000
approximation-approach    | 2         | 0.78        | 240,000
```

All tests PASSED ✓

## Cloud Machine Setup

### 1. System Requirements

```bash
# Minimum specs recommended
- CPU: 4+ cores
- RAM: 8+ GB
- Disk: 20+ GB free space
- Node.js: v20.x
- Network: Stable connection for MQTT
```

### 2. Installation Steps

```bash
# Clone repository
git clone <your-repo-url>
cd streaming-query-hive

# Install dependencies
npm install

# Build project
npm run build

# Verify build
npm run test
```

### 3. MQTT Broker Setup

```bash
# Ensure MQTT broker is running
# Option 1: Local broker
mosquitto -c /path/to/mosquitto.conf

# Option 2: Use existing broker
# Update config if broker is on different host/port
```

### 4. Verify Test Run

```bash
# Run single iteration test
npm run experiment:test

# Expected output: "ALL TESTS PASSED"
# Check: results/frequency-experiments-test/test-results-*.json
```

## Full Deployment

### Experiment Configuration

**Current Settings:**
- **Iterations:** 35 (3 warmup + 30 valid + 2 cooldown)
- **Frequencies:** 4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz (6 total)
- **Devices:** smartphone, wearable (2 total)
- **Approaches:** 3 (fetching-client-side, chunked-query-approach, approximation-approach)

**Total Experiments:** 1,260
**Estimated Runtime:** ~22 hours (conservative)

### Execution Options

#### Option A: Single Run (All Approaches)

```bash
# Start in screen/tmux session for stability
screen -S benchmark

# Run full experiment suite
npm run experiment:run

# Detach: Ctrl+A, D
# Reattach: screen -r benchmark
```

#### Option B: Separate Runs (Per Approach)

More reliable for long runs, easier to monitor:

```bash
# 1. Edit config for first approach
# File: scripts/benchmarks/frequency-experiment-config.json
# Change: "approaches": ["fetching-client-side"]
npm run experiment:run

# 2. Run second approach
# Change: "approaches": ["chunked-query-approach"]
npm run experiment:run

# 3. Run third approach
# Change: "approaches": ["approximation-approach"]
npm run experiment:run
```

### Monitoring

```bash
# Watch experiment progress
tail -f results/frequency-experiments/detailed-results-*.json

# Check resource usage
htop

# Monitor disk space
df -h

# Check log files
ls -lh *.csv  # Resource logs at project root
```

## During Execution

### Expected Behavior

1. HTTP server starts on port 8080
2. MQTT connections established
3. Progress updates printed to console
4. CSV resource logs written to project root
5. Iteration results saved to results/ directory

### Warning Signs

- **Memory leak:** Memory usage continuously growing
- **Disk full:** Less than 5GB free space
- **Connection errors:** MQTT/SPARQL failures
- **Process hang:** No output for >5 minutes

### Troubleshooting

```bash
# If process hangs
# Check MQTT broker
netstat -an | grep 1883

# Check HTTP server
netstat -an | grep 8080

# Review logs
tail -100 test-output.log

# Restart experiment from last checkpoint
# (Note: Current version doesn't support resume - manual restart needed)
```

## Post-Execution

### Result Verification

```bash
# Check results directory
ls -lh results/frequency-experiments/

# Expected files:
# - detailed-results-[timestamp].json
# - results-all-[timestamp].csv
# - results-valid-[timestamp].csv
# - [approach]/iteration_N/result.json (for each iteration)

# Verify record count
wc -l results/frequency-experiments/results-valid-*.csv
# Expected: 1261 lines (1 header + 1260 experiments)
```

### Data Analysis

```bash
# Run analysis script
npm run experiment:analyze

# Backup results
tar -czf benchmark-results-$(date +%Y%m%d).tar.gz results/
```

### Archive and Transfer

```bash
# Create compressed archive
tar -czf streaming-benchmark-complete.tar.gz \
  results/ \
  *.csv \
  test-output.log \
  TEST_RUN_SUMMARY.md

# Transfer from cloud machine
# On local machine:
scp user@cloud-machine:~/streaming-query-hive/streaming-benchmark-complete.tar.gz .
```

## Emergency Procedures

### Stop Experiment

```bash
# Graceful stop
Ctrl+C

# Force stop if needed
pkill -f "run-frequency-experiment"

# Clean up background processes
ps aux | grep "ts-node"
kill <PID>
```

### Resume (Manual)

```bash
# 1. Count completed iterations
ls results/frequency-experiments/*/iteration_* | wc -l

# 2. Calculate remaining
# Total needed: 1260
# Completed: <count from above>

# 3. Edit config to run remaining only
# Adjust "iterations" in config file

# 4. Restart
npm run experiment:run
```

## Quick Commands Reference

```bash
# Test single iteration
npm run experiment:test

# Estimate runtime
npm run experiment:estimate

# Run full benchmark
npm run experiment:run

# Analyze results
npm run experiment:analyze

# Check diagnostics
npm run build
```

## Contact & Support

- Test results: `results/frequency-experiments-test/`
- Full documentation: `docs/`
- Configuration: `scripts/benchmarks/frequency-experiment-config.json`

## Final Checklist Before Starting

- [ ] Cloud machine has stable network
- [ ] MQTT broker is running
- [ ] Port 8080 is available
- [ ] Sufficient disk space (20+ GB)
- [ ] Running in screen/tmux session
- [ ] Monitoring plan in place
- [ ] Backup strategy defined
- [ ] Test run completed successfully

**When all items checked, execute:**

```bash
npm run experiment:run
```

**Estimated completion:** 22 hours from start

Good luck with the benchmarking run!