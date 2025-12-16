#!/bin/bash

################################################################################
# Cleanup Logs Script
#
# Removes all log files, CSV outputs, and experimental results for a fresh start.
# This script cleans up:
# - CSV log files (orchestrator logs, resource usage)
# - Results directories
# - Unified log directories
# - Publisher logs
# - Temporary experiment files
################################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo -e "${BLUE}================================${NC}"
echo -e "${BLUE}Streaming Query Hive - Log Cleanup${NC}"
echo -e "${BLUE}================================${NC}"
echo ""

# Function to remove files/directories with confirmation
remove_items() {
    local description=$1
    shift
    local items=("$@")
    local found=0

    echo -e "${YELLOW}Checking ${description}...${NC}"

    for item in "${items[@]}"; do
        if [ -e "$item" ]; then
            found=1
            echo -e "  ${RED}✗${NC} Removing: $item"
            rm -rf "$item"
        fi
    done

    if [ $found -eq 0 ]; then
        echo -e "  ${GREEN}✓${NC} No files found"
    fi
    echo ""
}

# 1. Clean CSV logs in project root
echo -e "${BLUE}[1/8] CSV Log Files${NC}"
remove_items "CSV log files" \
    "approximation_approach_log.csv" \
    "chunked_query_approach_log.csv" \
    "streaming_query_chunk_aggregator_log.csv" \
    "naive_approximation_approach_log.csv" \
    "fetching_client_side_log.csv"

# 2. Clean resource usage logs
echo -e "${BLUE}[2/8] Resource Usage Logs${NC}"
remove_items "resource usage logs" \
    "approximation_approach_resource_usage.csv" \
    "chunked_query_approach_resource_log.csv" \
    "fetching_client_side_resource_usage.csv"

# 3. Clean replayer logs
echo -e "${BLUE}[3/8] Publisher/Replayer Logs${NC}"
remove_items "publisher logs" \
    "replayer-log.csv" \
    "publisher-log.csv" \
    "streamer-log.csv"

# 4. Clean results directories
echo -e "${BLUE}[4/8] Results Directories${NC}"
remove_items "results directories" \
    "results/chunked_query_results.csv" \
    "results/approximation_results.csv" \
    "results/fetching_client_side_results.csv" \
    "results/multi-run-verification-*.json" \
    "results/pattern-test-*.json" \
    "results/pattern-test-*.csv" \
    "results/frequency-experiments/"

# 5. Clean unified log directories
echo -e "${BLUE}[5/8] Unified Log Directories${NC}"
remove_items "unified logs" \
    "logs/approximation/" \
    "logs/fetching/" \
    "logs/chunked/"

# 6. Clean experiment-specific logs
echo -e "${BLUE}[6/8] Experiment Logs${NC}"
remove_items "experiment logs" \
    "experiment-*.log" \
    "experiment-*.json" \
    "benchmark-*.json" \
    "test-*.log"

# 7. Clean temporary files
echo -e "${BLUE}[7/8] Temporary Files${NC}"
remove_items "temporary files" \
    "*.tmp" \
    "*.temp" \
    ".experiment-state" \
    "pid-*.txt"

# 8. Clean any orphaned result files
echo -e "${BLUE}[8/8] Orphaned Result Files${NC}"
remove_items "orphaned result files" \
    "output-*.csv" \
    "metrics-*.json" \
    "summary-*.txt"

# Summary
echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Cleanup Complete!${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
echo -e "All log files and experimental results have been removed."
echo -e "You can now run experiments with a clean slate."
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo -e "  1. Build the project: ${YELLOW}npm run build${NC}"
echo -e "  2. Test MQTT: ${YELLOW}npm run experiment:test-mqtt${NC}"
echo -e "  3. Run experiment: ${YELLOW}npm run experiment:5-iterations${NC}"
echo ""
