#!/bin/bash

echo "=========================================="
echo "VERIFICATION TEST: Step Pattern"
echo "Testing all 3 approaches with fixed Chunked"
echo "=========================================="
echo ""

PATTERN="step_pattern"
export DATA_PATH="pattern_comparison/${PATTERN}"

# Clean up
pkill -f "Streaming\|publish\|Orchestrator" 2>/dev/null
sleep 3

# Clean result files
rm -f fetching_client_side_log.csv approximation_latency_log.csv chunked_latency_log.csv
rm -f streaming_query_hive_resource_log.csv

echo "Pattern: ${PATTERN}"
echo "Data: ${DATA_PATH}"
echo ""

# ========== FETCHING APPROACH ==========
echo "1. Testing FETCHING (baseline)..."
echo "   Starting orchestrator..."
node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js &
FETCH_PID=$!
sleep 8

echo "   Starting publisher..."
node dist/streamer/src/publish.js &
PUB_PID=$!

echo "   Waiting for results (max 150s)..."
COUNTER=0
while [ $COUNTER -lt 150 ]; do
    if [ -f fetching_client_side_log.csv ]; then
        LINES=$(wc -l < fetching_client_side_log.csv)
        if [ $LINES -ge 3 ]; then
            echo "   ✓ Got results after ${COUNTER}s"
            break
        fi
    fi
    sleep 10
    COUNTER=$((COUNTER+10))
done

kill $FETCH_PID $PUB_PID 2>/dev/null
wait 2>/dev/null
sleep 3

if [ -f fetching_client_side_log.csv ] && [ $(wc -l < fetching_client_side_log.csv) -gt 1 ]; then
    FETCH_W1=$(tail -n +2 fetching_client_side_log.csv | head -1 | awk -F',' '{print $NF}')
    FETCH_W2=$(tail -n +2 fetching_client_side_log.csv | tail -1 | awk -F',' '{print $NF}')
    echo "   Window 1: ${FETCH_W1}"
    echo "   Window 2: ${FETCH_W2}"
else
    echo "   ✗ Failed to get results"
    FETCH_W1="N/A"
    FETCH_W2="N/A"
fi
echo ""

# ========== APPROXIMATION APPROACH ==========
echo "2. Testing APPROXIMATION..."
rm -f approximation_latency_log.csv

echo "   Starting orchestrator..."
node dist/approaches/StreamingQueryApproximationApproachOrchestrator.js &
APPROX_PID=$!
sleep 8

echo "   Starting publisher..."
node dist/streamer/src/publish.js &
PUB_PID=$!

echo "   Waiting for results (max 150s)..."
COUNTER=0
while [ $COUNTER -lt 150 ]; do
    if [ -f approximation_latency_log.csv ]; then
        LINES=$(wc -l < approximation_latency_log.csv)
        if [ $LINES -ge 3 ]; then
            echo "   ✓ Got results after ${COUNTER}s"
            break
        fi
    fi
    sleep 10
    COUNTER=$((COUNTER+10))
done

kill $APPROX_PID $PUB_PID 2>/dev/null
wait 2>/dev/null
sleep 3

if [ -f approximation_latency_log.csv ] && [ $(wc -l < approximation_latency_log.csv) -gt 1 ]; then
    APPROX_W1=$(tail -n +2 approximation_latency_log.csv | head -1 | awk -F',' '{print $NF}')
    APPROX_W2=$(tail -n +2 approximation_latency_log.csv | tail -1 | awk -F',' '{print $NF}')
    echo "   Window 1: ${APPROX_W1}"
    echo "   Window 2: ${APPROX_W2}"
else
    echo "   ✗ Failed to get results"
    APPROX_W1="N/A"
    APPROX_W2="N/A"
fi
echo ""

# ========== CHUNKED APPROACH (FIXED) ==========
echo "3. Testing CHUNKED (with fix)..."
rm -f chunked_latency_log.csv

echo "   Starting orchestrator..."
node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js &
CHUNK_PID=$!
sleep 8

echo "   Starting publisher..."
node dist/streamer/src/publish.js &
PUB_PID=$!

echo "   Waiting for results (max 150s)..."
COUNTER=0
while [ $COUNTER -lt 150 ]; do
    if [ -f chunked_latency_log.csv ]; then
        LINES=$(wc -l < chunked_latency_log.csv)
        if [ $LINES -ge 3 ]; then
            echo "   ✓ Got results after ${COUNTER}s"
            break
        fi
    fi
    sleep 10
    COUNTER=$((COUNTER+10))
done

kill $CHUNK_PID $PUB_PID 2>/dev/null
wait 2>/dev/null
sleep 3

if [ -f chunked_latency_log.csv ] && [ $(wc -l < chunked_latency_log.csv) -gt 1 ]; then
    CHUNK_W1=$(tail -n +2 chunked_latency_log.csv | head -1 | awk -F',' '{print $NF}')
    CHUNK_W2=$(tail -n +2 chunked_latency_log.csv | tail -1 | awk -F',' '{print $NF}')
    echo "   Window 1: ${CHUNK_W1}"
    echo "   Window 2: ${CHUNK_W2}"
else
    echo "   ✗ Failed to get results"
    CHUNK_W1="N/A"
    CHUNK_W2="N/A"
fi
echo ""

# ========== RESULTS COMPARISON ==========
echo "=========================================="
echo "RESULTS COMPARISON"
echo "=========================================="
echo ""
printf "%-15s | %-15s | %-15s\n" "Approach" "Window 1" "Window 2"
echo "----------------+-----------------+-----------------"
printf "%-15s | %-15s | %-15s\n" "Fetching" "$FETCH_W1" "$FETCH_W2"
printf "%-15s | %-15s | %-15s\n" "Approximation" "$APPROX_W1" "$APPROX_W2"
printf "%-15s | %-15s | %-15s\n" "Chunked (fixed)" "$CHUNK_W1" "$CHUNK_W2"
echo ""

# Calculate errors if we have valid results
if [ "$FETCH_W2" != "N/A" ] && [ "$CHUNK_W2" != "N/A" ]; then
    echo "Window 2 Analysis (where bug was):"
    echo "  Fetching (baseline): $FETCH_W2"
    echo "  Chunked (fixed):     $CHUNK_W2"
    
    # Calculate difference
    DIFF=$(echo "$CHUNK_W2 - $FETCH_W2" | bc -l)
    ABS_DIFF=$(echo "$DIFF" | sed 's/-//')
    MAPE=$(echo "scale=4; ($ABS_DIFF / ($FETCH_W2 * -1)) * 100" | bc -l)
    
    echo "  Difference:          $DIFF"
    echo "  Absolute Difference: $ABS_DIFF"
    echo "  MAPE:                ${MAPE}%"
    echo ""
    
    # Compare to old buggy result
    OLD_CHUNK_W2="-19.441"
    OLD_DIFF=$(echo "$OLD_CHUNK_W2 - $FETCH_W2" | bc -l)
    OLD_ABS=$(echo "$OLD_DIFF" | sed 's/-//')
    OLD_MAPE=$(echo "scale=2; ($OLD_ABS / ($FETCH_W2 * -1)) * 100" | bc -l)
    
    echo "Previous (buggy) Chunked result:"
    echo "  Value:               $OLD_CHUNK_W2"
    echo "  MAPE:                ${OLD_MAPE}%"
    echo ""
    
    IMPROVEMENT=$(echo "$OLD_MAPE - $MAPE" | bc -l)
    echo "Improvement: ${IMPROVEMENT}% reduction in MAPE"
    echo ""
    
    # Determine if fix worked
    if [ $(echo "$MAPE < 1.0" | bc -l) -eq 1 ]; then
        echo "✓✓✓ FIX VERIFIED! MAPE < 1% ✓✓✓"
    elif [ $(echo "$MAPE < 5.0" | bc -l) -eq 1 ]; then
        echo "✓ Fix shows improvement (MAPE < 5%)"
    else
        echo "⚠ MAPE still high, may need further investigation"
    fi
fi

echo ""
echo "=========================================="
echo "Test completed!"
echo "=========================================="
