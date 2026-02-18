#!/bin/bash
#
# Run all benchmark experiments sequentially.
#
# Usage:
#   ./experiments/run-all-experiments.sh              # 5 iterations (default)
#   ./experiments/run-all-experiments.sh 35            # 35 iterations
#   ./experiments/run-all-experiments.sh 5 --skip-to 3 # Resume from experiment 3
#
# Estimated time per experiment (5 iterations): ~18 minutes
# Total for all 12 experiments (5 iterations):  ~3.5 hours
#
# Estimated time per experiment (35 iterations): ~2 hours
# Total for all 12 experiments (35 iterations):  ~24 hours

set -e

ITERATIONS=${1:-5}
SKIP_TO=0

# Parse --skip-to flag
for i in "$@"; do
  if [[ "$prev" == "--skip-to" ]]; then
    SKIP_TO=$i
  fi
  prev=$i
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCHMARK="node $SCRIPT_DIR/unified-benchmark.js"

cd "$PROJECT_ROOT"

EXPERIMENT_NUM=0
TOTAL_EXPERIMENTS=12
START_TIME=$(date +%s)

run_experiment() {
  EXPERIMENT_NUM=$((EXPERIMENT_NUM + 1))
  local description="$1"
  shift
  local args="$@"

  if [ $EXPERIMENT_NUM -lt $SKIP_TO ]; then
    echo "[$EXPERIMENT_NUM/$TOTAL_EXPERIMENTS] SKIPPING: $description"
    return
  fi

  echo ""
  echo "=================================================================="
  echo "[$EXPERIMENT_NUM/$TOTAL_EXPERIMENTS] $description"
  echo "  Command: $BENCHMARK --iterations $ITERATIONS $args"
  echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=================================================================="
  echo ""

  $BENCHMARK --iterations $ITERATIONS $args

  local elapsed=$(( $(date +%s) - START_TIME ))
  local elapsed_min=$((elapsed / 60))
  echo ""
  echo "  Completed: $(date '+%Y-%m-%d %H:%M:%S') (${elapsed_min}m elapsed total)"
  echo ""

  # Brief pause between experiments to let MQTT settle
  sleep 5
}

echo "========================================================================"
echo " STREAMING QUERY HIVE - FULL EXPERIMENT SUITE"
echo " Iterations per experiment: $ITERATIONS"
echo " Total experiments: $TOTAL_EXPERIMENTS"
echo " Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================================"

# ── Experiment 1: Baseline (real data, default settings) ──────────────
run_experiment "Baseline: Real data, AVG, default sub-windows"

# ── Experiment 2a-b: Sub-Window Variation (chunked reusability) ───────
run_experiment "Sub-window: finer (30s range, 15s step)" \
  --sub-window-range 30000 --sub-window-step 15000

run_experiment "Sub-window: coarser (120s range, 60s step)" \
  --sub-window-range 120000 --sub-window-step 60000

# ── Experiment 3a-d: Aggregation Functions ────────────────────────────
run_experiment "Aggregation: SUM" \
  --aggregation SUM

run_experiment "Aggregation: COUNT" \
  --aggregation COUNT

run_experiment "Aggregation: MIN" \
  --aggregation MIN

run_experiment "Aggregation: MAX" \
  --aggregation MAX

# ── Experiment 4a-d: Data Patterns (accuracy under stress) ───────────
run_experiment "Pattern: spike_pattern (sudden jumps)" \
  --pattern spike_pattern

run_experiment "Pattern: high_freq_oscillation (rapid changes)" \
  --pattern high_freq_oscillation

run_experiment "Pattern: noise_2.0 (high noise)" \
  --pattern noise_2.0

run_experiment "Pattern: low_variability (stable baseline)" \
  --pattern low_variability

# ── Experiment 5a-b: Publish Rate (scalability) ──────────────────────
run_experiment "Publish rate: 1 Hz wearable (low throughput)" \
  --wearable-freq 1

run_experiment "Publish rate: 16 Hz wearable (high throughput)" \
  --wearable-freq 16

# ── Done ──────────────────────────────────────────────────────────────
TOTAL_ELAPSED=$(( $(date +%s) - START_TIME ))
TOTAL_MIN=$((TOTAL_ELAPSED / 60))
TOTAL_HOURS=$((TOTAL_MIN / 60))
REMAINING_MIN=$((TOTAL_MIN % 60))

echo ""
echo "========================================================================"
echo " ALL EXPERIMENTS COMPLETE"
echo " Total time: ${TOTAL_HOURS}h ${REMAINING_MIN}m"
echo " Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo " Now run the analysis:"
echo "   node experiments/analyze-results.js"
echo "========================================================================"
