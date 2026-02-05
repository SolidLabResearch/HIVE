#!/bin/bash

# Simple Pattern Comparison - Based on validated test approach
# Tests all 5 patterns with Chunked and Fetching approaches only

echo "=========================================="
echo "SIMPLE PATTERN COMPARISON"
echo "Testing Chunked vs Fetching (First Window)"
echo "=========================================="
echo ""

PATTERNS=("step_pattern" "spike_pattern" "low_freq_oscillation" "high_freq_oscillation" "low_variability")

mkdir -p pattern_comparison_results

# Initialize results file
RESULTS_FILE="pattern_comparison_results/simple_comparison_results.csv"
echo "Pattern,Chunked_Result,Fetching_Result,Absolute_Diff,Percent_Error" > "$RESULTS_FILE"

for PATTERN in "${PATTERNS[@]}"; do
    echo "=========================================="
    echo "Testing: $PATTERN"
    echo "=========================================="
    
    # Clean up
    pkill -f "StreamingQuery" 2>/dev/null
    pkill -f "publish" 2>/dev/null
    sleep 3
    
    # Test Chunked
    echo "Running Chunked..."
    rm -f chunked_latency_log.csv
    export DATA_PATH="custom_patterns/$PATTERN"
    
    timeout 100 node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 3
    
    timeout 100 node dist/streamer/src/publish.js > /dev/null 2>&1
    
    sleep 3
    kill $ORCH_PID 2>/dev/null
    
    CHUNKED_RESULT="N/A"
    if [ -f "chunked_latency_log.csv" ]; then
        CHUNKED_RESULT=$(tail -n +2 chunked_latency_log.csv | head -1 | awk -F',' '{print $NF}')
        echo "  Chunked Window 1: $CHUNKED_RESULT"
        cp chunked_latency_log.csv "pattern_comparison_results/chunked_${PATTERN}.csv"
    else
        echo "  Chunked: Failed (no log)"
    fi
    
    sleep 3
    
    # Test Fetching
    echo "Running Fetching..."
    rm -f fetching_client_side_log.csv
    export DATA_PATH="custom_patterns/$PATTERN"
    
    timeout 100 node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 3
    
    timeout 100 node dist/streamer/src/publish.js > /dev/null 2>&1
    
    sleep 3
    kill $ORCH_PID 2>/dev/null
    
    FETCHING_RESULT="N/A"
    if [ -f "fetching_client_side_log.csv" ]; then
        # Extract result from "RStream result generated: X" line
        FETCHING_RESULT=$(grep "RStream result generated" fetching_client_side_log.csv | head -1 | awk -F': ' '{print $2}' | awk '{print $1}')
        echo "  Fetching Window 1: $FETCHING_RESULT"
        cp fetching_client_side_log.csv "pattern_comparison_results/fetching_${PATTERN}.csv"
    else
        echo "  Fetching: Failed (no log)"
    fi
    
    # Calculate difference
    if [ "$CHUNKED_RESULT" != "N/A" ] && [ "$FETCHING_RESULT" != "N/A" ]; then
        python3 << EOF
chunked = float($CHUNKED_RESULT)
fetching = float($FETCHING_RESULT)
diff = abs(chunked - fetching)
pct = (diff / abs(fetching)) * 100 if fetching != 0 else 0

print(f"  Absolute Difference: {diff}")
print(f"  Percent Error: {pct:.10f}%")

# Save to file
with open("$RESULTS_FILE", "a") as f:
    f.write(f"$PATTERN,{chunked},{fetching},{diff},{pct:.10f}\n")
EOF
    else
        echo "  Cannot calculate - missing data"
        echo "$PATTERN,$CHUNKED_RESULT,$FETCHING_RESULT,N/A,N/A" >> "$RESULTS_FILE"
    fi
    
    echo ""
    
    # Cleanup before next test
    pkill -f "StreamingQuery" 2>/dev/null
    pkill -f "publish" 2>/dev/null
    sleep 3
done

echo "=========================================="
echo "RESULTS SUMMARY"
echo "=========================================="
echo ""
column -t -s',' "$RESULTS_FILE"
echo ""
echo "Detailed results saved to: pattern_comparison_results/"
