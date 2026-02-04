#!/bin/bash

# Comprehensive comparison of Chunked vs Fetching approaches
# Focuses on first window accuracy and performance metrics

PATTERNS=("step_pattern" "spike_pattern" "low_freq_oscillation" "high_freq_oscillation" "low_variability")
RESULTS_FILE="first_window_comparison_results.csv"

# Initialize results file
echo "Pattern,Approach,Window1_Result,Latency_ms,CPU_Percent,Memory_MB,Test_Duration_s,Status" > $RESULTS_FILE

echo "======================================"
echo "FIRST WINDOW COMPARISON TEST"
echo "======================================"
echo ""

for PATTERN in "${PATTERNS[@]}"; do
    echo "--------------------------------------"
    echo "Testing Pattern: $PATTERN"
    echo "--------------------------------------"
    
    # Check if pattern exists
    if [ ! -d "src/streamer/data/custom_patterns/$PATTERN" ]; then
        echo "⚠️  Pattern $PATTERN not found, skipping..."
        echo "$PATTERN,CHUNKED,N/A,N/A,N/A,N/A,N/A,PATTERN_NOT_FOUND" >> $RESULTS_FILE
        echo "$PATTERN,FETCHING,N/A,N/A,N/A,N/A,N/A,PATTERN_NOT_FOUND" >> $RESULTS_FILE
        continue
    fi
    
    # ============================================
    # TEST 1: CHUNKED APPROACH
    # ============================================
    echo ""
    echo "🔹 Testing CHUNKED approach..."
    
    # Clean up previous runs
    pkill -9 -f "StreamingQueryChunkedApproachOrchestrator" 2>/dev/null
    pkill -9 -f "publish.js" 2>/dev/null
    sleep 2
    
    rm -f chunked_latency_log.csv streaming_query_chunk_aggregator_log.csv
    
    # Set environment and start orchestrator
    export DATA_PATH="custom_patterns/$PATTERN"
    START_TIME=$(date +%s)
    
    # Start orchestrator in background and capture PID
    node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
    ORCHESTRATOR_PID=$!
    
    sleep 3
    
    # Start publisher
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUBLISHER_PID=$!
    
    # Monitor resources while waiting
    CPU_SAMPLES=""
    MEM_SAMPLES=""
    
    for i in {1..50}; do
        sleep 1
        if ps -p $ORCHESTRATOR_PID > /dev/null 2>&1; then
            CPU=$(ps -p $ORCHESTRATOR_PID -o %cpu= | awk '{print $1}')
            MEM=$(ps -p $ORCHESTRATOR_PID -o rss= | awk '{print $1/1024}')
            CPU_SAMPLES="$CPU_SAMPLES $CPU"
            MEM_SAMPLES="$MEM_SAMPLES $MEM"
        fi
        
        # Check if we have first window result
        if [ -f "chunked_latency_log.csv" ]; then
            WINDOW_COUNT=$(grep -c "Window closed" chunked_latency_log.csv 2>/dev/null || echo "0")
            if [ "$WINDOW_COUNT" -ge 1 ]; then
                break
            fi
        fi
    done
    
    END_TIME=$(date +%s)
    TEST_DURATION=$((END_TIME - START_TIME))
    
    # Kill processes
    kill $ORCHESTRATOR_PID 2>/dev/null
    kill $PUBLISHER_PID 2>/dev/null
    pkill -9 -f "StreamingQueryChunkedApproachOrchestrator" 2>/dev/null
    pkill -9 -f "publish.js" 2>/dev/null
    
    # Extract first window result and latency
    if [ -f "chunked_latency_log.csv" ]; then
        WINDOW1_RESULT=$(awk -F',' 'NR==2 {print $4}' chunked_latency_log.csv)
        LATENCY=$(awk -F',' 'NR==2 {print $5}' chunked_latency_log.csv)
        
        # Calculate average CPU and memory
        AVG_CPU=$(echo "$CPU_SAMPLES" | awk '{sum=0; for(i=1; i<=NF; i++) sum+=$i; print sum/NF}')
        AVG_MEM=$(echo "$MEM_SAMPLES" | awk '{sum=0; for(i=1; i<=NF; i++) sum+=$i; print sum/NF}')
        
        echo "  ✓ Window 1 Result: $WINDOW1_RESULT"
        echo "  ✓ Latency: ${LATENCY}ms"
        echo "  ✓ Avg CPU: ${AVG_CPU}%"
        echo "  ✓ Avg Memory: ${AVG_MEM}MB"
        
        echo "$PATTERN,CHUNKED,$WINDOW1_RESULT,$LATENCY,$AVG_CPU,$AVG_MEM,$TEST_DURATION,SUCCESS" >> $RESULTS_FILE
    else
        echo "  ✗ Failed to get results"
        echo "$PATTERN,CHUNKED,N/A,N/A,N/A,N/A,$TEST_DURATION,FAILED" >> $RESULTS_FILE
    fi
    
    sleep 3
    
    # ============================================
    # TEST 2: FETCHING APPROACH
    # ============================================
    echo ""
    echo "🔹 Testing FETCHING approach..."
    
    # Clean up previous runs
    pkill -9 -f "StreamingQueryFetchingClientSideApproachOrchestrator" 2>/dev/null
    pkill -9 -f "publish.js" 2>/dev/null
    sleep 2
    
    rm -f fetching_client_side_log.csv
    
    # Set environment and start orchestrator
    export DATA_PATH="custom_patterns/$PATTERN"
    START_TIME=$(date +%s)
    
    # Start orchestrator in background and capture PID
    node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
    ORCHESTRATOR_PID=$!
    
    sleep 3
    
    # Start publisher
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUBLISHER_PID=$!
    
    # Monitor resources while waiting
    CPU_SAMPLES=""
    MEM_SAMPLES=""
    
    for i in {1..50}; do
        sleep 1
        if ps -p $ORCHESTRATOR_PID > /dev/null 2>&1; then
            CPU=$(ps -p $ORCHESTRATOR_PID -o %cpu= | awk '{print $1}')
            MEM=$(ps -p $ORCHESTRATOR_PID -o rss= | awk '{print $1/1024}')
            CPU_SAMPLES="$CPU_SAMPLES $CPU"
            MEM_SAMPLES="$MEM_SAMPLES $MEM"
        fi
        
        # Check if we have first window result
        if [ -f "fetching_client_side_log.csv" ]; then
            LINE_COUNT=$(wc -l < fetching_client_side_log.csv 2>/dev/null || echo "0")
            if [ "$LINE_COUNT" -ge 2 ]; then
                break
            fi
        fi
    done
    
    END_TIME=$(date +%s)
    TEST_DURATION=$((END_TIME - START_TIME))
    
    # Kill processes
    kill $ORCHESTRATOR_PID 2>/dev/null
    kill $PUBLISHER_PID 2>/dev/null
    pkill -9 -f "StreamingQueryFetchingClientSideApproachOrchestrator" 2>/dev/null
    pkill -9 -f "publish.js" 2>/dev/null
    
    # Extract first window result and latency
    if [ -f "fetching_client_side_log.csv" ]; then
        WINDOW1_RESULT=$(awk -F',' 'NR==2 {print $4}' fetching_client_side_log.csv)
        LATENCY=$(awk -F',' 'NR==2 {print $5}' fetching_client_side_log.csv)
        
        # Calculate average CPU and memory
        AVG_CPU=$(echo "$CPU_SAMPLES" | awk '{sum=0; for(i=1; i<=NF; i++) sum+=$i; print sum/NF}')
        AVG_MEM=$(echo "$MEM_SAMPLES" | awk '{sum=0; for(i=1; i<=NF; i++) sum+=$i; print sum/NF}')
        
        echo "  ✓ Window 1 Result: $WINDOW1_RESULT"
        echo "  ✓ Latency: ${LATENCY}ms"
        echo "  ✓ Avg CPU: ${AVG_CPU}%"
        echo "  ✓ Avg Memory: ${AVG_MEM}MB"
        
        echo "$PATTERN,FETCHING,$WINDOW1_RESULT,$LATENCY,$AVG_CPU,$AVG_MEM,$TEST_DURATION,SUCCESS" >> $RESULTS_FILE
    else
        echo "  ✗ Failed to get results"
        echo "$PATTERN,FETCHING,N/A,N/A,N/A,N/A,$TEST_DURATION,FAILED" >> $RESULTS_FILE
    fi
    
    sleep 3
    
done

echo ""
echo "======================================"
echo "COMPARISON COMPLETE"
echo "======================================"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""
echo "Summary:"
cat $RESULTS_FILE

# Create analysis script
cat > analyze_first_window_comparison.js << 'EOF'
const fs = require('fs');

// Read results
const results = fs.readFileSync('first_window_comparison_results.csv', 'utf8')
    .split('\n')
    .filter(line => line.trim() && !line.startsWith('Pattern,'))
    .map(line => {
        const [pattern, approach, window1Result, latency, cpu, memory, duration, status] = line.split(',');
        return {
            pattern,
            approach,
            window1Result: parseFloat(window1Result),
            latency: parseFloat(latency),
            cpu: parseFloat(cpu),
            memory: parseFloat(memory),
            duration: parseFloat(duration),
            status
        };
    });

// Expected values for each pattern (for first window [0-120s])
const expectedValues = {
    'step_pattern': -23.0,  // All -23 for first 120s
    'spike_pattern': null,  // Need to calculate from data
    'low_freq_oscillation': null,
    'high_freq_oscillation': null,
    'low_variability': null
};

console.log('\n========================================');
console.log('FIRST WINDOW COMPARISON ANALYSIS');
console.log('========================================\n');

// Group by pattern
const patterns = [...new Set(results.map(r => r.pattern))];

patterns.forEach(pattern => {
    const patternResults = results.filter(r => r.pattern === pattern && r.status === 'SUCCESS');
    
    if (patternResults.length === 0) {
        console.log(`${pattern}: No successful results\n`);
        return;
    }
    
    console.log(`Pattern: ${pattern}`);
    console.log('─'.repeat(60));
    
    const chunked = patternResults.find(r => r.approach === 'CHUNKED');
    const fetching = patternResults.find(r => r.approach === 'FETCHING');
    
    if (chunked && fetching) {
        // Accuracy comparison
        const accuracyDiff = Math.abs(chunked.window1Result - fetching.window1Result);
        const accuracyPct = chunked.window1Result !== 0 
            ? (accuracyDiff / Math.abs(chunked.window1Result)) * 100 
            : 0;
        
        console.log('Window 1 Results:');
        console.log(`  Chunked:  ${chunked.window1Result.toFixed(4)}`);
        console.log(`  Fetching: ${fetching.window1Result.toFixed(4)}`);
        console.log(`  Difference: ${accuracyDiff.toFixed(6)} (${accuracyPct.toFixed(2)}%)`);
        console.log(`  Alignment: ${accuracyPct < 0.01 ? '✅ PERFECT' : accuracyPct < 1 ? '⚠️ GOOD' : '❌ POOR'}`);
        
        if (expectedValues[pattern] !== null) {
            const expectedError = Math.abs(chunked.window1Result - expectedValues[pattern]);
            console.log(`  Expected: ${expectedValues[pattern]}`);
            console.log(`  Error: ${expectedError.toFixed(4)}`);
        }
        
        console.log('');
        
        // Latency comparison
        const latencyDiff = chunked.latency - fetching.latency;
        const latencyPct = (latencyDiff / fetching.latency) * 100;
        
        console.log('Latency (ms):');
        console.log(`  Chunked:  ${chunked.latency.toFixed(2)}ms`);
        console.log(`  Fetching: ${fetching.latency.toFixed(2)}ms`);
        console.log(`  Difference: ${latencyDiff.toFixed(2)}ms (${latencyPct > 0 ? '+' : ''}${latencyPct.toFixed(1)}%)`);
        console.log(`  Winner: ${latencyDiff < 0 ? '🏆 Chunked faster' : '🏆 Fetching faster'}`);
        console.log('');
        
        // CPU comparison
        const cpuDiff = chunked.cpu - fetching.cpu;
        const cpuPct = (cpuDiff / fetching.cpu) * 100;
        
        console.log('CPU Usage (%):');
        console.log(`  Chunked:  ${chunked.cpu.toFixed(2)}%`);
        console.log(`  Fetching: ${fetching.cpu.toFixed(2)}%`);
        console.log(`  Difference: ${cpuDiff > 0 ? '+' : ''}${cpuDiff.toFixed(2)}% (${cpuPct > 0 ? '+' : ''}${cpuPct.toFixed(1)}%)`);
        console.log(`  Winner: ${cpuDiff < 0 ? '🏆 Chunked lower' : '🏆 Fetching lower'}`);
        console.log('');
        
        // Memory comparison
        const memDiff = chunked.memory - fetching.memory;
        const memPct = (memDiff / fetching.memory) * 100;
        
        console.log('Memory Usage (MB):');
        console.log(`  Chunked:  ${chunked.memory.toFixed(2)}MB`);
        console.log(`  Fetching: ${fetching.memory.toFixed(2)}MB`);
        console.log(`  Difference: ${memDiff > 0 ? '+' : ''}${memDiff.toFixed(2)}MB (${memPct > 0 ? '+' : ''}${memPct.toFixed(1)}%)`);
        console.log(`  Winner: ${memDiff < 0 ? '🏆 Chunked lower' : '🏆 Fetching lower'}`);
        console.log('');
        
    } else if (chunked) {
        console.log('Only Chunked results available:');
        console.log(`  Window 1: ${chunked.window1Result.toFixed(4)}`);
        console.log(`  Latency: ${chunked.latency.toFixed(2)}ms`);
        console.log(`  CPU: ${chunked.cpu.toFixed(2)}%`);
        console.log(`  Memory: ${chunked.memory.toFixed(2)}MB`);
        console.log('');
    } else if (fetching) {
        console.log('Only Fetching results available:');
        console.log(`  Window 1: ${fetching.window1Result.toFixed(4)}`);
        console.log(`  Latency: ${fetching.latency.toFixed(2)}ms`);
        console.log(`  CPU: ${fetching.cpu.toFixed(2)}%`);
        console.log(`  Memory: ${fetching.memory.toFixed(2)}MB`);
        console.log('');
    }
    
    console.log('');
});

// Overall summary
console.log('========================================');
console.log('OVERALL SUMMARY');
console.log('========================================\n');

const successfulTests = results.filter(r => r.status === 'SUCCESS');
const chunkedResults = successfulTests.filter(r => r.approach === 'CHUNKED');
const fetchingResults = successfulTests.filter(r => r.approach === 'FETCHING');

if (chunkedResults.length > 0 && fetchingResults.length > 0) {
    // Calculate averages
    const avgChunkedLatency = chunkedResults.reduce((sum, r) => sum + r.latency, 0) / chunkedResults.length;
    const avgFetchingLatency = fetchingResults.reduce((sum, r) => sum + r.latency, 0) / fetchingResults.length;
    
    const avgChunkedCPU = chunkedResults.reduce((sum, r) => sum + r.cpu, 0) / chunkedResults.length;
    const avgFetchingCPU = fetchingResults.reduce((sum, r) => sum + r.cpu, 0) / fetchingResults.length;
    
    const avgChunkedMem = chunkedResults.reduce((sum, r) => sum + r.memory, 0) / chunkedResults.length;
    const avgFetchingMem = fetchingResults.reduce((sum, r) => sum + r.memory, 0) / fetchingResults.length;
    
    console.log('Average Performance Across All Patterns:');
    console.log('─'.repeat(60));
    console.log(`Latency:  Chunked ${avgChunkedLatency.toFixed(2)}ms vs Fetching ${avgFetchingLatency.toFixed(2)}ms`);
    console.log(`CPU:      Chunked ${avgChunkedCPU.toFixed(2)}% vs Fetching ${avgFetchingCPU.toFixed(2)}%`);
    console.log(`Memory:   Chunked ${avgChunkedMem.toFixed(2)}MB vs Fetching ${avgFetchingMem.toFixed(2)}MB`);
}

console.log('\nTest completed successfully!');
EOF

echo ""
echo "Running analysis..."
node analyze_first_window_comparison.js
