#!/bin/bash

echo "=== Testing Chunked vs Fetching Alignment ==="
echo ""

# Kill any existing processes
pkill -f "python.*ground_truth" 2>/dev/null
pkill -f "node.*Orchestrator" 2>/dev/null
sleep 2

# Clean up old results
rm -f chunked_latency_log.csv streaming_query_chunk_aggregator_log.csv
rm -f fetching_client_side_log.csv
rm -f publisher.log orchestrator_chunked.log orchestrator_fetching.log

# Set data path
export DATA_PATH="pattern_comparison/step_pattern"

echo "Step 1: Running Chunked Approach..."
python3 scripts/ground_truth.py > publisher.log 2>&1 &
PUB_PID=$!
sleep 3

node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > orchestrator_chunked.log 2>&1 &
CHUNKED_PID=$!

echo "Running for 140 seconds to get Window 1..."
sleep 140

kill $PUB_PID $CHUNKED_PID 2>/dev/null
sleep 2

# Check if we got results
if [ ! -f chunked_latency_log.csv ]; then
    echo "ERROR: Chunked test failed - no results"
    cat publisher.log | tail -20
    exit 1
fi

CHUNKED_W1=$(grep "^1," chunked_latency_log.csv | tail -1 | cut -d',' -f12)

if [ -z "$CHUNKED_W1" ]; then
    echo "ERROR: No Window 1 result from Chunked"
    exit 1
fi

echo "✓ Chunked Window 1: $CHUNKED_W1"
echo ""

echo "Step 2: Running Fetching Approach..."
python3 scripts/ground_truth.py > publisher.log 2>&1 &
PUB_PID=$!
sleep 3

node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > orchestrator_fetching.log 2>&1 &
FETCHING_PID=$!

echo "Running for 140 seconds to get Window 1..."
sleep 140

kill $PUB_PID $FETCHING_PID 2>/dev/null
sleep 2

# Extract Fetching Window 1 result
FETCHING_W1=$(grep "RStream result generated" fetching_client_side_log.csv | head -1 | grep -o '"message":"RStream result generated: [^"]*"' | grep -o ': [0-9.-]*' | cut -d' ' -f2)

if [ -z "$FETCHING_W1" ]; then
    echo "ERROR: No Window 1 result from Fetching"
    exit 1
fi

echo "✓ Fetching Window 1: $FETCHING_W1"
echo ""

echo "=== COMPARISON ==="
echo "Chunked:  $CHUNKED_W1"
echo "Fetching: $FETCHING_W1"
echo ""

# Calculate difference using Python
python3 << EOF
chunked = float('$CHUNKED_W1')
fetching = float('$FETCHING_W1')
diff = abs(chunked - fetching)
pct = (diff / abs(fetching)) * 100

print(f"Difference: {diff:.6f}")
print(f"Error: {pct:.2f}%")
print("")

if pct < 1.0:
    print("✅ EXCELLENT: Results match within 1%")
elif pct < 5.0:
    print("✓ GOOD: Results match within 5%")
elif pct < 10.0:
    print("⚠ FAIR: Results differ by {pct:.1f}%")
else:
    print(f"❌ POOR: Results differ by {pct:.1f}%")
EOF
