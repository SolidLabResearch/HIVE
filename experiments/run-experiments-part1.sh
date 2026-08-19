#!/bin/bash
# Batch 1/6: baseline + sub-window variation

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 1/6 (BASELINE + SUB-WINDOWS)" 3

run_experiment "Baseline: Real data, AVG, default sub-windows"
run_experiment "Sub-window: finer (30s range, 15s step)" \
  --sub-window-range 30000 --sub-window-step 15000
run_experiment "Sub-window: coarser (120s range, 60s step)" \
  --sub-window-range 120000 --sub-window-step 60000

finish_suite
