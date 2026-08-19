#!/bin/bash
# Batch 4/6: pattern stress tests (spike, high frequency oscillation)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 4/6 (PATTERNS: SPIKE, HIGH-FREQ)" 2

run_experiment "Pattern: spike_pattern (sudden jumps)" --pattern spike_pattern
run_experiment "Pattern: high_freq_oscillation (rapid changes)" --pattern high_freq_oscillation

finish_suite
