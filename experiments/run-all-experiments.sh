#!/bin/bash
#
# Run all benchmark experiments in 6 smaller batches.
#
# Usage:
#   ./experiments/run-all-experiments.sh                # 5 iterations (default)
#   ./experiments/run-all-experiments.sh 35             # 35 iterations
#   ./experiments/run-all-experiments.sh 5 --from-part 3 # resume from batch 3
#
# Tip:
#   You can run one batch at a time (recommended for disk usage), e.g.:
#   ./experiments/run-experiments-part1.sh 5
#

ITERATIONS=${1:-5}
FROM_PART=1

# Parse --from-part flag
prev=""
for i in "$@"; do
  if [[ "$prev" == "--from-part" ]]; then
    FROM_PART=$i
  fi
  prev=$i
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_TIME=$(date +%s)

run_part() {
  local part_num="$1"
  local script="$SCRIPT_DIR/run-experiments-part${part_num}.sh"

  if [ "$part_num" -lt "$FROM_PART" ]; then
    echo "[Batch $part_num/6] SKIPPING"
    return
  fi

  echo ""
  echo "========================================================================"
  echo " RUNNING BATCH $part_num/6"
  echo " Script: $script"
  echo "========================================================================"
  "$script" "$ITERATIONS"

  local exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    echo "WARNING: Batch $part_num exited with code $exit_code"
  fi

  echo ""
  echo "Batch $part_num complete. Export/archive logs before continuing if needed."
}

for part in 1 2 3 4 5 6; do
  run_part "$part"
done

TOTAL_ELAPSED=$(( $(date +%s) - START_TIME ))
TOTAL_MIN=$((TOTAL_ELAPSED / 60))
TOTAL_HOURS=$((TOTAL_MIN / 60))
REMAINING_MIN=$((TOTAL_MIN % 60))

echo ""
echo "========================================================================"
echo " ALL 6 BATCHES COMPLETE"
echo " Total time: ${TOTAL_HOURS}h ${REMAINING_MIN}m"
echo " Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo " Now run the analysis:"
echo "   node experiments/analyze-results.js"
echo "========================================================================"
