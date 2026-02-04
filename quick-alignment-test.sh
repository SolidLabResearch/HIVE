#!/bin/bash

echo "=== Quick Chunked vs Fetching Alignment Test ==="
echo "Pattern: step_pattern (switches from -23 to -15 at t=60s)"
echo "Query: RANGE 120s, STEP 60s"
echo "Expected Window 1: ~-19 (average of -23 and -15)"
echo ""

# Cleanup
pkill -9 -f "node dist/approaches" 2>/dev/null
pkill -9 -f "python.*replayer" 2>/dev/null
sleep 2

# Clean result files
rm -f chunked_latency_log.csv fetching_client_side_log.csv streaming_query_chunk_aggregator_log.csv fetching_client_side_resource_usage.csv

# Set data path
export DATA_PATH="custom_patterns/step_pattern"

echo "Step 1: Testing CHUNKED approach..."
# Start data publisher
python3 src/streamer/publisher/replayer.py > /dev/null 2>&1 &
PUBLISHER_PID=$!
sleep 2

# Start chunked orchestrator
node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
CHUNKED_PID=$!

# Run for 140 seconds to get Window 1
sleep 140

# Stop processes
kill $PUBLISHER_PID $CHUNKED_PID 2>/dev/null
pkill -9 -f "python.*replayer" 2>/dev/null
sleep 2

# Extract Window 1 result
if [ -f chunked_latency_log.csv ]; then
    CHUNKED_W1=$(grep "^1," chunked_latency_log.csv | head -1 | cut -d',' -f12)
    if [ ! -z "$CHUNKED_W1" ]; then
        echo "✓ Chunked Window 1: $CHUNKED_W1"
    else
        echo "✗ Chunked produced no Window 1 result"
        exit 1
    fi
else
    echo "✗ Chunked test failed - no log file"
    exit 1
fi

echo ""
echo "Step 2: Testing FETCHING approach..."

# Clean for fetching test
rm -f fetching_client_side_log.csv

# Start publisher again
python3 src/streamer/publisher/replayer.py > /dev/null 2>&1 &
PUBLISHER_PID=$!
sleep 2

# Start fetching orchestrator
node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
FETCHING_PID=$!

# Run for 140 seconds to get Window 1
sleep 140

# Stop processes
kill $PUBLISHER_PID $FETCHING_PID 2>/dev/null
pkill -9 -f "python.*replayer" 2>/dev/null
sleep 2

# Extract Window 1 result
if [ -f fetching_client_side_log.csv ]; then
    # Extract first RStream result
    FETCHING_W1=$(grep "RStream result generated:" fetching_client_side_log.csv | head -1 | sed 's/.*generated: \([0-9.-]*\).*/\1/')
    if [ ! -z "$FETCHING_W1" ]; then
        echo "✓ Fetching Window 1: $FETCHING_W1"
    else
        echo "✗ Fetching produced no Window 1 result"
        exit 1
    fi
else
    echo "✗ Fetching test failed - no log file"
    exit 1
fi

echo ""
echo "=== COMPARISON ==="
echo "Chunked:  $CHUNKED_W1"
echo "Fetching: $FETCHING_W1"
echo "Expected: -19.0"
echo ""

# Calculate alignment
python3 << EOF
import sys

try:
    chunked = float('$CHUNKED_W1')
    fetching = float('$FETCHING_W1')
    expected = -19.0
    
    diff = abs(chunked - fetching)
    pct_error = (diff / abs(fetching)) * 100
    
    chunked_error = abs(chunked - expected) / abs(expected) * 100
    fetching_error = abs(fetching - expected) / abs(expected) * 100
    
    print(f"Chunked vs Fetching:")
    print(f"  Difference: {diff:.6f}")
    print(f"  Error: {pct_error:.2f}%")
    print(f"")
    print(f"vs Expected (-19.0):")
    print(f"  Chunked error: {chunked_error:.2f}%")
    print(f"  Fetching error: {fetching_error:.2f}%")
    print(f"")
    
    if pct_error < 0.1:
        print("✅ PERFECT: Chunked and Fetching match within 0.1%!")
        sys.exit(0)
    elif pct_error < 1.0:
        print("✅ EXCELLENT: Results match within 1%")
        sys.exit(0)
    elif pct_error < 5.0:
        print("✓ GOOD: Results match within 5%")
        sys.exit(0)
    else:
        print(f"❌ POOR: Results differ by {pct_error:.1f}%")
        print(f"   The fix did NOT resolve the alignment issue")
        sys.exit(1)
except Exception as e:
    print(f"Error in comparison: {e}")
    sys.exit(1)
EOF
