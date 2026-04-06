#!/bin/bash
# Batch 5/6: pattern stress tests (noise, low variability)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 5/6 (PATTERNS: NOISE, LOW-VARIABILITY)" 2

run_experiment "Pattern: noise_2.0 (high noise)" --pattern noise_2.0
run_experiment "Pattern: low_variability (stable baseline)" --pattern low_variability

finish_suite
