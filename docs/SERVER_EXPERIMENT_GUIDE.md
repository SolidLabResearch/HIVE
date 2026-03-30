# Server Experiment Guide — 4-Way Approach Comparison

## Approaches Compared

| Role | Approach |
|---|---|
| Baseline 1 | Fetching Client Side (Local-Only) |
| Baseline 2 | Naive Distributed |
| Proposed | Approximation |
| Proposed | Chunked Query Reuse |

---

## Prerequisites

```bash
# 1. Install Node.js (v20 recommended)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# 2. Install MQTT broker
sudo apt-get install -y mosquitto mosquitto-clients

# 3. Start MQTT broker
mosquitto -d

# 4. Clone and build the project
git clone <repo-url> streaming-query-hive
cd streaming-query-hive
npm install
npm run build

# 5. Verify data files exist
ls src/streamer/data/smartphone.acceleration.x/data.nt
ls src/streamer/data/wearable.acceleration.x/data.nt
ls src/streamer/data/custom_patterns/
```

---

## Running the Experiments

### Phase 1a — Real Data (all 4 approaches)

```bash
node experiments/real-data-comparison/run-real-data-4-approaches.js --iterations 35
```

- Runs: 4 approaches × 35 iterations = 140 tests
- Duration: ~3 min per test → ~7 hours total
- Logs: `experiments/real-data-comparison/logs/{approach}/iteration{N}/`

### Phase 1b — Custom Patterns (all 4 approaches × 5 patterns)

```bash
node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 35
```

- Runs: 4 approaches × 5 patterns × 35 iterations = 700 tests
- Duration: ~3 min per test → ~35 hours total
- Logs: `logs/custom-pattern-comparison/{approach}/{pattern}/iteration{N}/`

> **Quick smoke test** (1 iteration, ~30 min):
> ```bash
> node experiments/real-data-comparison/run-real-data-4-approaches.js --iterations 1
> node experiments/pattern-analysis/run-custom-patterns-comparison.js --iterations 1
> ```

---

## Generating the Report

After the experiments complete, run:

```bash
node experiments/generate-phase1-report.js
```

This reads all log files and prints three tables:
1. **Accuracy** — avg result value per dataset per approach, % error vs Fetching
2. **Latency** — avg, std dev, min, max across all windows
3. **Resources** — avg CPU%, avg memory MB ± std

With JSON output:
```bash
node experiments/generate-phase1-report.js --json
# writes: experiments/phase1-report.json
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `ECONNREFUSED` on MQTT | `mosquitto -d` to start the broker |
| Approximation shows N/A | Stale process on port 8080 — the runner cleans this automatically |
| Build errors | `npm run build` — check TypeScript errors |
| Data not found | Run `node scripts/generate-custom-patterns.js` for custom patterns |
| Processes left over after crash | `pkill -f "dist/approaches"; pkill -f "dist/services/BeeWorker"; lsof -ti:8080 \| xargs kill -9` |
