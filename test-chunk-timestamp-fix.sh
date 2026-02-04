#!/bin/bash

echo "=========================================="
echo "Testing Chunk Timestamp Fix"
echo "=========================================="
echo ""

# Clean up
pkill -f "Streaming|Orchestrator|publish" 2>/dev/null
sleep 3

# Clear logs
rm -f chunked_latency_log.csv streaming_query_chunk_aggregator_log.csv

# Set environment
export DATA_PATH="pattern_comparison/step_pattern"

echo "Starting Chunked approach for step_pattern..."
node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > orchestrator.log 2>&1 &
ORCH_PID=$!

sleep 10

echo "Starting publisher..."
node dist/streamer/src/publish.js > publisher.log 2>&1 &
PUB_PID=$!

echo "Waiting 200 seconds for 2+ windows..."
sleep 200

echo ""
echo "Stopping processes..."
kill $ORCH_PID $PUB_PID 2>/dev/null
wait 2>/dev/null

echo ""
echo "=========================================="
echo "Results:"
echo "=========================================="

if [ -f chunked_latency_log.csv ]; then
    echo ""
    echo "Chunked Results:"
    cat chunked_latency_log.csv
else
    echo "No chunked results file"
fi

echo ""
echo "=========================================="
echo "Checking Chunk Timestamps in Logs:"
echo "=========================================="

if [ -f streaming_query_chunk_aggregator_log.csv ]; then
    echo ""
    echo "Recent chunk data (should now have hasTimestamp in chunks):"
    grep "Window chunks for chunked" streaming_query_chunk_aggregator_log.csv | tail -3
else
    echo "No aggregator log file"
fi

echo ""
echo "Complete!"
