#!/bin/bash

# Approach Comparison Experiment Runner
# This script runs the experiment comparing the three approaches:
# 1. Client-Side Processing (Ground Truth)
# 2. Chunked Query Approach
# 3. Approximation Approach
#
# Latency Definition: result_available_time - window_close_time
# This matches the August 2024 benchmark methodology.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "============================================================"
echo "  Approach Comparison Experiment"
echo "  Latency = result_time - window_close_time"
echo "============================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default configuration
FREQUENCY=4
WARMUP_MS=5000
ITERATIONS=1

# Check if MQTT broker is running
check_mqtt() {
    echo -e "${YELLOW}Checking MQTT broker...${NC}"

    # Try mosquitto_pub first
    if command -v mosquitto_pub &> /dev/null; then
        if mosquitto_pub -h localhost -p 1883 -t "test" -m "test" -q 0 2>/dev/null; then
            echo -e "${GREEN}MQTT broker is running${NC}"
            return 0
        fi
    fi

    # Alternative check using netcat
    if command -v nc &> /dev/null; then
        if nc -z localhost 1883 2>/dev/null; then
            echo -e "${GREEN}MQTT broker is running (port 1883 open)${NC}"
            return 0
        fi
    fi

    # Alternative check using lsof
    if command -v lsof &> /dev/null; then
        if lsof -i :1883 &> /dev/null; then
            echo -e "${GREEN}MQTT broker is running (port 1883 in use)${NC}"
            return 0
        fi
    fi

    echo -e "${RED}MQTT broker is not running on localhost:1883${NC}"
    echo "Please start the MQTT broker before running the experiment."
    echo "  - On macOS: brew services start mosquitto"
    echo "  - On Linux: sudo systemctl start mosquitto"
    echo "  - Or run: mosquitto -v"
    return 1
}

# Check if HTTP server is running for query registration
check_http_server() {
    echo -e "${YELLOW}Checking HTTP query server...${NC}"

    if command -v curl &> /dev/null; then
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:3001/queries 2>/dev/null | grep -q "200\|404"; then
            echo -e "${GREEN}HTTP query server is running${NC}"
            return 0
        fi
    fi

    echo -e "${YELLOW}HTTP query server might not be running on localhost:3001${NC}"
    echo "Some approaches may start their own server. Continuing..."
    return 0
}

# Create results directory with timestamp
setup_results_dir() {
    local results_dir="$SCRIPT_DIR/results"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local run_dir="$results_dir/run_$timestamp"

    mkdir -p "$run_dir"
    echo "$run_dir"
}

# Clean up any leftover processes from previous runs
cleanup_processes() {
    echo -e "${YELLOW}Cleaning up any leftover processes...${NC}"

    # Kill any existing approach processes
    pkill -f "StreamingQueryFetchingClientSideApproachOrchestrator" 2>/dev/null || true
    pkill -f "StreamingQueryChunkedApproachOrchestrator" 2>/dev/null || true
    pkill -f "StreamingQueryApproximationApproachOrchestrator" 2>/dev/null || true
    pkill -f "StreamToMQTT" 2>/dev/null || true

    # Wait for processes to terminate
    sleep 2

    echo -e "${GREEN}Cleanup complete${NC}"
}

# Run the experiment
run_experiment() {
    local results_dir="$1"

    echo ""
    echo -e "${CYAN}Running experiment with configuration:${NC}"
    echo "  Data Frequency: ${FREQUENCY} Hz"
    echo "  Warmup Period: ${WARMUP_MS} ms"
    echo "  Results Directory: $results_dir"
    echo ""

    cd "$PROJECT_ROOT"

    # Run with ts-node
    npx ts-node experiments/approach-comparison/ApproachComparisonExperiment.ts \
        "$results_dir" \
        --frequency "$FREQUENCY" \
        --warmup "$WARMUP_MS" \
        --iterations "$ITERATIONS"
}

# Display results summary
display_summary() {
    local results_dir="$1"

    echo ""
    echo -e "${CYAN}============================================================${NC}"
    echo -e "${CYAN}  Results Summary${NC}"
    echo -e "${CYAN}============================================================${NC}"
    echo ""

    # Find the summary file
    local summary_file=$(ls -t "$results_dir"/summary_*.csv 2>/dev/null | head -1)

    if [ -n "$summary_file" ]; then
        echo -e "${YELLOW}Latency and Resource Usage:${NC}"
        echo ""

        if command -v python3 &> /dev/null; then
            python3 << EOF
import csv

with open("$summary_file", 'r') as f:
    reader = csv.DictReader(f)

    print(f"{'Approach':<25} {'Latency (ms)':<20} {'CPU %':<10} {'Memory (MB)':<20}")
    print("-" * 75)

    for row in reader:
        approach = row['approach']
        latency = f"{float(row['avg_latency_ms']):.0f} +/- {float(row['std_dev_ms']):.1f}"
        cpu = f"{float(row['avg_cpu_percent']):.2f}"
        memory = f"{float(row['avg_memory_mb']):.2f} +/- {float(row['peak_memory_mb']) - float(row['avg_memory_mb']):.1f}"

        print(f"{approach:<25} {latency:<20} {cpu:<10} {memory:<20}")
EOF
        else
            cat "$summary_file"
        fi
    fi

    # Find the accuracy file
    local accuracy_file=$(ls -t "$results_dir"/accuracy_*.csv 2>/dev/null | head -1)

    if [ -n "$accuracy_file" ]; then
        echo ""
        echo -e "${YELLOW}Accuracy vs Ground Truth:${NC}"
        echo ""

        if command -v python3 &> /dev/null; then
            python3 << EOF
import csv
from collections import defaultdict

accuracy_data = defaultdict(list)

with open("$accuracy_file", 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        approach = row['approach']
        pct_error = float(row['percentage_error'])
        accuracy_data[approach].append(pct_error)

print(f"{'Approach':<25} {'Windows':<10} {'Avg Error %':<15} {'Max Error %':<15}")
print("-" * 65)

for approach, errors in sorted(accuracy_data.items()):
    if errors:
        avg_error = sum(errors) / len(errors)
        max_error = max(errors)
        accuracy = 100 - avg_error if avg_error < 100 else 0

        print(f"{approach:<25} {len(errors):<10} {avg_error:<15.2f} {max_error:<15.2f}")
EOF
        else
            cat "$accuracy_file"
        fi
    fi

    echo ""
    echo -e "${GREEN}Detailed results saved to: $results_dir${NC}"
}

# Compare with August benchmark
compare_with_benchmark() {
    echo ""
    echo -e "${CYAN}============================================================${NC}"
    echo -e "${CYAN}  August 2024 Benchmark Comparison${NC}"
    echo -e "${CYAN}============================================================${NC}"
    echo ""
    echo "August Benchmark Results:"
    echo ""
    echo "| Approach              | Latency (ms)      | CPU %  | Memory (MB)      | Accuracy |"
    echo "|-----------------------|-------------------|--------|------------------|----------|"
    echo "| Chunked Query         | 414 +/- 12.3      | 0.21   | 45.68 +/- 2.3    | 100%     |"
    echo "| Approximation         | 359 +/- 31.2      | 0.20   | 53.92 +/- 1.2    | 89.5%    |"
    echo "| Client Side Processing| 2543 +/- 213.3    | 0.20   | 66.05 +/- 4.2    | 100% (GT)|"
    echo ""
}

# Print help
print_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --frequency <hz>     Data publish frequency in Hz (default: 4)"
    echo "  --warmup <ms>        Warmup period in milliseconds (default: 5000)"
    echo "  --iterations <n>     Number of iterations (default: 1)"
    echo "  --skip-mqtt-check    Skip MQTT broker check"
    echo "  --skip-cleanup       Skip cleanup of leftover processes"
    echo "  --compare            Show comparison with August benchmark"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --frequency 4 --warmup 5000"
    echo ""
}

# Main execution
main() {
    local skip_mqtt_check=false
    local skip_cleanup=false
    local show_comparison=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --frequency)
                FREQUENCY="$2"
                shift 2
                ;;
            --warmup)
                WARMUP_MS="$2"
                shift 2
                ;;
            --iterations)
                ITERATIONS="$2"
                shift 2
                ;;
            --skip-mqtt-check)
                skip_mqtt_check=true
                shift
                ;;
            --skip-cleanup)
                skip_cleanup=true
                shift
                ;;
            --compare)
                show_comparison=true
                shift
                ;;
            -h|--help)
                print_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                print_help
                exit 1
                ;;
        esac
    done

    # Show August benchmark for comparison
    if [ "$show_comparison" = true ]; then
        compare_with_benchmark
    fi

    # Cleanup unless skipped
    if [ "$skip_cleanup" = false ]; then
        cleanup_processes
    fi

    # Check MQTT unless skipped
    if [ "$skip_mqtt_check" = false ]; then
        check_mqtt || exit 1
    fi

    # Check HTTP server (optional)
    check_http_server

    # Setup results directory
    results_dir=$(setup_results_dir)

    # Run experiment
    run_experiment "$results_dir"

    # Display summary
    display_summary "$results_dir"

    # Show comparison with August benchmark
    compare_with_benchmark

    echo ""
    echo -e "${GREEN}Experiment completed successfully!${NC}"
}

main "$@"
