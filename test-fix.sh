#!/bin/bash

echo "Testing Fixed Chunked Approach with Step Pattern"
echo "================================================="

# Configuration
PATTERN="step_pattern"
export DATA_PATH="pattern_comparison/${PATTERN}"

# Clean up old results
rm -f chunked_latency_log.csv streaming_query_hive_resource_log.csv
pkill -f "StreamingQueryChunkedApproachOrchestrator" 2>/dev/null
pkill -f "publish.js" 2>/dev/null
sleep 2

echo "Starting orchestrator..."
node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js &
ORCH_PID=$!
sleep 8

echo "Starting publisher..."
node dist/streamer/src/publish.js &
PUB_PID=$!

echo "Waiting for results (max 180 seconds)..."
COUNTER=0
while [ $COUNTER -lt 180 ]; do
    if [ -f chunked_latency_log.csv ]; then
        LINES=$(wc -l < chunked_latency_log.csv)
        if [ $LINES -ge 3 ]; then
            echo "✓ Got $((LINES-1)) windows"
            break
        fi
    fi
    sleep 5
    COUNTER=$((COUNTER+5))
    echo "  Waited ${COUNTER}s..."
done

echo "Stopping processes..."
kill $ORCH_PID $PUB_PID 2>/dev/null
wait $ORCH_PID $PUB_PID 2>/dev/null

echo ""
echo "Results:"
echo "--------"
if [ -f chunked_latency_log.csv ]; then
    echo "Window results:"
    tail -n +2 chunked_latency_log.csv | awk -F',' '{print "Window " $1 ": " $NF}'
    
    echo ""
    echo "Comparing with baseline (Fetching):"
    echo "  Fetching Window 1: -22.999"
    echo "  Fetching Window 2: -17.684"
    echo ""
    echo "  Chunked Window 1: $(tail -n +2 chunked_latency_log.csv | head -1 | awk -F',' '{print $NF}')"
    echo "  Chunked Window 2: $(tail -n +2 chunked_latency_log.csv | tail -1 | awk -F',' '{print $NF}')"
else
    echo "✗ No results generated"
fi

echo ""
echo "Done!"
