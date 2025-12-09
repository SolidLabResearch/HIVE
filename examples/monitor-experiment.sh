#!/bin/bash

# Monitor the pattern accuracy experiment progress

LOG_FILE="pattern_experiment_results.log"

echo "=== Pattern Accuracy Experiment Monitor ==="
echo "Monitoring: $LOG_FILE"
echo ""

# Function to get current pattern
get_current_pattern() {
    grep "--- Running Pattern:" "$LOG_FILE" | tail -1 | sed 's/.*Pattern: //' | sed 's/ ---//'
}

# Function to get completed patterns
get_completed_patterns() {
    grep -c "--- Finished Pattern:" "$LOG_FILE"
}

# Function to get result counts
get_result_counts() {
    local approx=$(grep -c "Result (Approx):" "$LOG_FILE")
    local truth=$(grep -c "Result (Ground Truth):" "$LOG_FILE")
    echo "Approx: $approx | Ground Truth: $truth"
}

# Function to check if experiment is complete
is_complete() {
    grep -q "--- Experiment Summary ---" "$LOG_FILE"
}

# Main monitoring loop
while true; do
    clear
    echo "=== Pattern Accuracy Experiment Monitor ==="
    echo "Time: $(date '+%H:%M:%S')"
    echo ""

    if [ ! -f "$LOG_FILE" ]; then
        echo "Log file not found. Experiment not started yet."
        sleep 5
        continue
    fi

    # Check if complete
    if is_complete; then
        echo "✓ EXPERIMENT COMPLETE!"
        echo ""
        echo "=== Final Results ==="
        grep -A 20 "--- Experiment Summary ---" "$LOG_FILE"
        echo ""
        break
    fi

    # Show progress
    COMPLETED=$(get_completed_patterns)
    CURRENT=$(get_current_pattern)
    RESULTS=$(get_result_counts)

    echo "Progress: $COMPLETED/7 patterns completed"
    echo ""

    if [ -n "$CURRENT" ]; then
        echo "Current Pattern: $CURRENT"
        echo ""
    fi

    echo "Results Collected:"
    echo "  $RESULTS"
    echo ""

    # Show recent activity
    echo "Recent Activity (last 5 lines):"
    echo "--------------------------------"
    grep -E "Running Pattern|Finished Pattern|Starting data|Result \(|Data generation complete|Waiting for processing|Publishers started" "$LOG_FILE" | tail -5
    echo ""

    # Show estimated time remaining
    if [ "$COMPLETED" -gt 0 ]; then
        PATTERNS_REMAINING=$((7 - COMPLETED))
        TIME_PER_PATTERN=150  # ~2.5 minutes per pattern in seconds
        ESTIMATED_REMAINING=$((PATTERNS_REMAINING * TIME_PER_PATTERN))
        MINUTES=$((ESTIMATED_REMAINING / 60))
        SECONDS=$((ESTIMATED_REMAINING % 60))
        echo "Estimated time remaining: ${MINUTES}m ${SECONDS}s"
    fi

    echo ""
    echo "Press Ctrl+C to stop monitoring (experiment will continue)"

    sleep 10
done
