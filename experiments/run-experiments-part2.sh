#!/bin/bash
# Batch 2/6: aggregation (SUM, COUNT)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 2/6 (AGGREGATION: SUM, COUNT)" 2

run_experiment "Aggregation: SUM" --aggregation SUM
run_experiment "Aggregation: COUNT" --aggregation COUNT

finish_suite
