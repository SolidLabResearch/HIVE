#!/bin/bash
# Batch 6/6: publish-rate scalability (1Hz only)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 6/6 (PUBLISH RATE: 1HZ ONLY)" 1

run_experiment "Publish rate: 1 Hz wearable (low throughput)" --wearable-freq 1

finish_suite
