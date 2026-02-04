#!/bin/bash

# Kill any existing processes
pkill -f "node.*dist.*orchestrator" || true
pkill -f "node.*dist.*publisher" || true
sleep 2

# Clean logs
rm -f orchestrator.log publisher.log

# Start publisher in background (step_pattern for 120s)
node dist/examples/stream-to-mqtt-publisher.js step_pattern 120 > publisher.log 2>&1 &
PUBLISHER_PID=$!
echo "Publisher started (PID: $PUBLISHER_PID)"

# Wait for publisher to initialize
sleep 5

# Start orchestrator in background
node dist/examples/intelligent-orchestrator-demo.js > orchestrator.log 2>&1 &
ORCHESTRATOR_PID=$!
echo "Orchestrator started (PID: $ORCHESTRATOR_PID)"

# Wait for test duration (run for 150s to get at least 2 windows)
echo "Running test for 150 seconds..."
sleep 150

# Stop processes
kill $PUBLISHER_PID $ORCHESTRATOR_PID 2>/dev/null || true
sleep 2

# Show Window 2 results
echo ""
echo "=== Window 2 Debug Info ==="
grep "!!!!! Computing Window 2" orchestrator.log
grep "!!!!! Window 2 time range" orchestrator.log
grep "!!!!! Window 2 boundaries" orchestrator.log | head -2
grep "!!!!! Window chunks for" orchestrator.log | grep "Window 2" | head -1

echo ""
echo "=== Final Results ===" 
node verify_counts_silent.js | grep "Window 2"
