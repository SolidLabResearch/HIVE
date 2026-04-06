#!/bin/bash
# Batch 6/6: publish-rate scalability

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run-experiments-common.sh"

parse_iterations_and_skip "$@"
init_suite "STREAMING QUERY HIVE - BATCH 6/6 (PUBLISH RATE)" 2

run_experiment "Publish rate: 1 Hz wearable (low throughput)" --wearable-freq 1
run_experiment "Publish rate: 16 Hz wearable (high throughput)" --wearable-freq 16

finish_suite
