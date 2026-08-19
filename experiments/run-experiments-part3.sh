#!/bin/bash
# Batch 3/6: aggregation (MIN, MAX)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 3/6 (AGGREGATION: MIN, MAX)" 2

run_experiment "Aggregation: MIN" --aggregation MIN
run_experiment "Aggregation: MAX" --aggregation MAX

finish_suite
