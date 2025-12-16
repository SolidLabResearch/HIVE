# Quick Start Guide

Get started with Streaming Query Hive experiments in minutes.

## Prerequisites

- Node.js v20+
- MQTT Broker (mosquitto)
- Git

## Installation

```bash
# Clone and install
git clone <repository-url>
cd streaming-query-hive
npm install
```

## Essential Commands

### 1. Setup & Build

```bash
npm run build                    # Build TypeScript project
npm run experiment:setup         # Generate test data
npm run experiment:test-mqtt     # Test MQTT broker
npm run clean-logs              # Clean all logs/results
```

### 2. Quick Validation (5 iterations)

```bash
# Multi-approach test (~15-20 min)
npm run experiment:5-iterations

# Pattern-based test (~30-40 min)
npm run experiment:patterns-approx-vs-fetching-5
```

### 3. Comprehensive Tests (35 iterations)

```bash
# Multi-approach test (~1.5-2 hours)
npm run experiment:35-iterations

# Pattern-based test (~4-5 hours)
npm run experiment:patterns-approx-vs-fetching

# Real-world frequency test (~3-4 hours)
npm run experiment:run-realworld
```

### 4. Analysis

```bash
npm run experiment:analyze       # Analyze results
```

## Typical Workflow

### For Development/Testing

```bash
# 1. Clean slate
npm run clean-logs

# 2. Build
npm run build

# 3. Quick test (5 iterations)
npm run experiment:5-iterations

# 4. Check results
cat results/multi-run-verification-*.json
```

### For Research/Production

```bash
# 1. Clean slate
npm run clean-logs

# 2. Build
npm run build

# 3. Comprehensive test (35 iterations)
npm run experiment:35-iterations

# 4. Pattern validation
npm run experiment:patterns-approx-vs-fetching

# 5. Analyze
npm run experiment:analyze
```

## Troubleshooting

### MQTT Broker Not Running

```bash
# macOS
brew services start mosquitto

# Linux
sudo systemctl start mosquitto

# Check if running
pgrep -fl mosquitto
```

### Port Already In Use

```bash
# Kill processes on health ports
lsof -ti :9091 | xargs kill -9
lsof -ti :9092 | xargs kill -9
lsof -ti :9093 | xargs kill -9
lsof -ti :9094 | xargs kill -9
```

### No Results Collected

```bash
# 1. Clean logs
npm run clean-logs

# 2. Rebuild
npm run build

# 3. Test MQTT
npm run experiment:test-mqtt

# 4. Try again
npm run experiment:5-iterations
```

### Orphaned Processes

```bash
# Kill Node processes
pkill -f "FetchingClientSide"
pkill -f "ChunkedQuery"
pkill -f "Approximation"
pkill -f "experiment-publisher"
```

## Quick Reference

| Command | Duration | Purpose |
|---------|----------|---------|
| `clean-logs` | <1 min | Remove all logs |
| `build` | 1-2 min | Compile TypeScript |
| `experiment:test-mqtt` | <1 min | Verify MQTT setup |
| `experiment:5-iterations` | 15-20 min | Quick validation |
| `experiment:35-iterations` | 1.5-2 hrs | Statistical validation |
| `experiment:patterns-approx-vs-fetching-5` | 30-40 min | Quick pattern test |
| `experiment:patterns-approx-vs-fetching` | 4-5 hrs | Full pattern test |
| `experiment:run-realworld` | 3-4 hrs | Real-world scenarios |

## Understanding Results

### Success Indicators

```
Run 1 Results (95.2s):
  [OK] Approximation: 2 results    ← Success!
  [OK] Chunked:       2 results    ← Success!
  [OK] Fetching:      2 results    ← Success!
```

### Expected Result Counts

- **Per run**: 2 results (60s and 120s window steps)
- **5 iterations**: 10 total results per approach
- **35 iterations**: 70 total results per approach

### Output Files

```
results/
├── multi-run-verification-<timestamp>.json
├── pattern-test-<timestamp>.json
└── frequency-experiments/
    ├── detailed-<timestamp>.json
    └── summary-<timestamp>.csv

logs/
├── approximation/
├── chunked/
└── fetching/

*.csv (project root)
├── approximation_approach_log.csv
├── approximation_approach_resource_usage.csv
├── chunked_query_approach_log.csv
├── chunked_query_approach_resource_log.csv
├── fetching_client_side_resource_usage.csv
└── replayer-log.csv
```

## Best Practices

1. ✅ Always run `npm run clean-logs` before new experiments
2. ✅ Start with 5-iteration tests for validation
3. ✅ Monitor logs for `[APPROACH]` and `[PUBLISHER]` messages
4. ✅ Save result files before running `clean-logs`
5. ✅ Use 35 iterations only for final validation
6. ✅ Check MQTT broker is running: `pgrep -fl mosquitto`
7. ✅ Build after code changes: `npm run build`

## Common Workflows

### Daily Development

```bash
npm run clean-logs && npm run build && npm run experiment:5-iterations
```

### Weekly Validation

```bash
npm run clean-logs && \
npm run build && \
npm run experiment:5-iterations && \
npm run experiment:patterns-approx-vs-fetching-5
```

### Pre-Publication

```bash
npm run clean-logs && \
npm run build && \
npm run experiment:35-iterations && \
npm run experiment:patterns-approx-vs-fetching && \
npm run experiment:analyze
```

## Getting Help

- 📖 Full documentation: `docs/EXPERIMENTS.md`
- 🔧 Scripts guide: `scripts/README.md`
- 🏗️ Architecture: `docs/ARCHITECTURE.md`
- 📧 Contact: kushbisen@proton.me
- 🐛 Issues: https://github.com/SolidLabResearch/streaming-query-hive/issues

## Next Steps

1. Read full experiment guide: `docs/EXPERIMENTS.md`
2. Understand approaches: `docs/APPROACH_COMPARISON.md`
3. Review architecture: `docs/ARCHITECTURE.md`
4. Run your first experiment: `npm run experiment:5-iterations`

---

**Remember**: Start small (5 iterations), validate, then scale to 35 iterations for final results!