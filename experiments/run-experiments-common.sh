#!/bin/bash

# Shared helpers for batched experiment runners.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCHMARK="node $SCRIPT_DIR/unified-benchmark.js"

parse_iterations_and_skip() {
  ITERATIONS=5
  SKIP_TO=0

  if [[ -n "$1" && "$1" =~ ^[0-9]+$ ]]; then
    ITERATIONS=$1
    shift
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-to)
        SKIP_TO=${2:-0}
        shift 2
        ;;
      *)
        shift
        ;;
    esac
  done
}

init_suite() {
  local suite_name="$1"
  local total_experiments="$2"

  EXPERIMENT_NUM=0
  TOTAL_EXPERIMENTS=$total_experiments
  START_TIME=$(date +%s)

  cd "$PROJECT_ROOT" || exit 1

  echo "========================================================================"
  echo " $suite_name"
  echo " Iterations per experiment: $ITERATIONS"
  echo " Total experiments in this batch: $TOTAL_EXPERIMENTS"
  echo " Started: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "========================================================================"
}

run_experiment() {
  EXPERIMENT_NUM=$((EXPERIMENT_NUM + 1))
  local description="$1"
  shift
  local args="$*"

  if [ "$EXPERIMENT_NUM" -lt "$SKIP_TO" ]; then
    echo "[$EXPERIMENT_NUM/$TOTAL_EXPERIMENTS] SKIPPING: $description"
    return
  fi

  echo ""
  echo "=================================================================="
  echo "[$EXPERIMENT_NUM/$TOTAL_EXPERIMENTS] $description"
  echo "  Command: $BENCHMARK --iterations $ITERATIONS $args"
  echo "  Class file logging: disabled (set LOG_DISABLE_FILE_OUTPUT=0 to re-enable)"
  echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=================================================================="
  echo ""

  LOG_DISABLE_FILE_OUTPUT="${LOG_DISABLE_FILE_OUTPUT:-1}" $BENCHMARK --iterations "$ITERATIONS" "$@"
  local exit_code=$?

  local elapsed=$(( $(date +%s) - START_TIME ))
  local elapsed_min=$((elapsed / 60))
  echo ""
  if [ "$exit_code" -ne 0 ]; then
    echo "  WARNING: Experiment exited with code $exit_code"
  fi
  echo "  Completed: $(date '+%Y-%m-%d %H:%M:%S') (${elapsed_min}m elapsed in this batch)"
  echo ""

  sleep 5
}

finish_suite() {
  local total_elapsed=$(( $(date +%s) - START_TIME ))
  local total_min=$((total_elapsed / 60))
  local total_hours=$((total_min / 60))
  local remaining_min=$((total_min % 60))

  echo ""
  echo "========================================================================"
  echo " BATCH COMPLETE"
  echo " Total time: ${total_hours}h ${remaining_min}m"
  echo " Finished: $(date '+%Y-%m-%d %H:%M:%S')"
  echo ""
  echo " You can export/archive data now before running the next batch."
  echo "========================================================================"
}
