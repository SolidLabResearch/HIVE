#!/bin/bash

# Kill any existing processes
pkill -f "node.*dist.*Orchestrator" 2>/dev/null || true
pkill -f "python.*ground_truth" 2>/dev/null || true
sleep 2

# Clean logs
rm -f orchestrator.log publisher.log streaming_query_hive_resource_log.csv

# Set data path for step pattern
export DATA_PATH="pattern_comparison/step_pattern"

# Start publisher (ground truth script) in background
echo "Starting publisher..."
python3 scripts/ground_truth.py > publisher.log 2>&1 &
PUBLISHER_PID=$!
echo "Publisher started (PID: $PUBLISHER_PID)"

# Wait for publisher to initialize and start producing data
sleep 5

# Start chunked orchestrator in background
echo "Starting chunked orchestrator..."
node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > orchestrator.log 2>&1 &
ORCHESTRATOR_PID=$!
echo "Orchestrator started (PID: $ORCHESTRATOR_PID)"

# Wait for test duration (run for 150s to get at least 2 windows)
echo "Running test for 150 seconds..."
sleep 150

# Stop processes
echo "Stopping processes..."
kill $PUBLISHER_PID $ORCHESTRATOR_PID 2>/dev/null || true
sleep 2

# Show Window 2 results
echo ""
echo "=== Window 2 Debug Info ==="
grep "!!!!! Computing Window 2" orchestrator.log
grep "!!!!! Window 2 time range" orchestrator.log  
grep "!!!!! Window 2 boundaries" orchestrator.log | head -2
echo ""
echo "Showing first chunk details for Window 2:"
grep "!!!!! Window chunks for" orchestrator.log | grep -A 1 "Window 2" | head -3

echo ""
echo "=== Final Results ===" 
node verify_counts_silent.js | grep "Window 2" || echo "No Window 2 results found"
