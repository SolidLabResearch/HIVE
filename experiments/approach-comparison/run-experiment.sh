#!/bin/bash

# Approach Comparison Experiment Runner
# This script compiles and runs the experiment comparing the three approaches:
# 1. Fetching Client-Side (Ground Truth)
# 2. Approximation Approach
# 3. Chunked Query Approach

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "============================================================"
echo "  Approach Comparison Experiment"
echo "============================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if MQTT broker is running
check_mqtt() {
    echo -e "${YELLOW}Checking MQTT broker...${NC}"
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

    echo -e "${RED}MQTT broker is not running on localhost:1883${NC}"
    echo "Please start the MQTT broker before running the experiment."
    echo "  - On macOS: brew services start mosquitto"
    echo "  - On Linux: sudo systemctl start mosquitto"
    echo "  - Or run: mosquitto -v"
    return 1
}

# Compile TypeScript
compile_ts() {
    echo -e "${YELLOW}Compiling TypeScript...${NC}"
    cd "$PROJECT_ROOT"

    if [ ! -f "tsconfig.json" ]; then
        echo -e "${RED}tsconfig.json not found in project root${NC}"
        exit 1
    fi

    npm run build 2>/dev/null || npx tsc
    echo -e "${GREEN}Compilation complete${NC}"
}

# Create results directory
setup_results_dir() {
    local results_dir="$SCRIPT_DIR/results"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local run_dir="$results_dir/run_$timestamp"

    mkdir -p "$run_dir"
    echo "$run_dir"
}

# Run the experiment
run_experiment() {
    local results_dir="$1"

    echo ""
    echo -e "${YELLOW}Running experiment...${NC}"
    echo "Results will be saved to: $results_dir"
    echo ""

    cd "$PROJECT_ROOT"

    # Run with ts-node for development, or compiled JS for production
    if command -v ts-node &> /dev/null; then
        npx ts-node experiments/approach-comparison/ApproachComparisonExperiment.ts "$results_dir"
    else
        node dist/experiments/approach-comparison/ApproachComparisonExperiment.js "$results_dir"
    fi
}

# Generate analysis report
generate_report() {
    local results_dir="$1"

    echo ""
    echo -e "${YELLOW}Generating analysis report...${NC}"

    # Find the latest CSV files
    local latency_file=$(ls -t "$results_dir"/latency_results_*.csv 2>/dev/null | head -1)
    local accuracy_file=$(ls -t "$results_dir"/accuracy_results_*.csv 2>/dev/null | head -1)

    if [ -z "$latency_file" ] || [ -z "$accuracy_file" ]; then
        echo -e "${RED}Result files not found${NC}"
        return 1
    fi

    echo ""
    echo "============================================================"
    echo "  Latency Results Summary"
    echo "============================================================"
    echo ""

    if command -v python3 &> /dev/null; then
        python3 << EOF
import csv
import sys
from collections import defaultdict

latency_file = "$latency_file"
accuracy_file = "$accuracy_file"

# Parse latency results
approaches = defaultdict(list)
with open(latency_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        approach = row['approach']
        latency = float(row['first_event_latency_ms'])
        approaches[approach].append(latency)

print("First Event Latency (ms):")
print("-" * 60)
print(f"{'Approach':<25} {'Count':>8} {'Avg':>10} {'Min':>10} {'Max':>10}")
print("-" * 60)

for approach, latencies in sorted(approaches.items()):
    if latencies:
        avg = sum(latencies) / len(latencies)
        print(f"{approach:<25} {len(latencies):>8} {avg:>10.2f} {min(latencies):>10.2f} {max(latencies):>10.2f}")

print("")
print("============================================================")
print("  Accuracy Results Summary")
print("============================================================")
print("")

# Parse accuracy results
accuracy_data = defaultdict(list)
with open(accuracy_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        approach = row['approach']
        pct_error = float(row['percentage_error'])
        accuracy_data[approach].append(pct_error)

print("Accuracy vs Ground Truth (Percentage Error):")
print("-" * 60)
print(f"{'Approach':<25} {'Count':>8} {'Avg %':>10} {'Min %':>10} {'Max %':>10}")
print("-" * 60)

for approach, errors in sorted(accuracy_data.items()):
    if errors:
        avg = sum(errors) / len(errors)
        print(f"{approach:<25} {len(errors):>8} {avg:>10.2f} {min(errors):>10.2f} {max(errors):>10.2f}")

EOF
    else
        echo "Python3 not found. Raw data available in:"
        echo "  - $latency_file"
        echo "  - $accuracy_file"
    fi

    echo ""
    echo -e "${GREEN}Results saved to: $results_dir${NC}"
}

# Main execution
main() {
    local skip_mqtt_check=false
    local skip_compile=false
    local duration=""

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-mqtt-check)
                skip_mqtt_check=true
                shift
                ;;
            --skip-compile)
                skip_compile=true
                shift
                ;;
            --duration)
                duration="$2"
                shift 2
                ;;
            -h|--help)
                echo "Usage: $0 [options]"
                echo ""
                echo "Options:"
                echo "  --skip-mqtt-check    Skip MQTT broker check"
                echo "  --skip-compile       Skip TypeScript compilation"
                echo "  --duration <ms>      Set experiment duration in milliseconds"
                echo "  -h, --help           Show this help message"
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
        esac
    done

    # Check MQTT unless skipped
    if [ "$skip_mqtt_check" = false ]; then
        check_mqtt || exit 1
    fi

    # Compile unless skipped
    if [ "$skip_compile" = false ]; then
        compile_ts
    fi

    # Setup results directory
    results_dir=$(setup_results_dir)

    # Run experiment
    run_experiment "$results_dir"

    # Generate report
    generate_report "$results_dir"

    echo ""
    echo -e "${GREEN}Experiment completed successfully!${NC}"
}

main "$@"
