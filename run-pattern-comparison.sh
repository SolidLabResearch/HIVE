#!/bin/bash

# Simple pattern comparison focusing on first window only

PATTERNS=("step_pattern" "spike_pattern" "low_freq_oscillation" "high_freq_oscillation" "low_variability")
RESULTS_DIR="pattern_comparison_results"
mkdir -p $RESULTS_DIR

echo "======================================"
echo "PATTERN COMPARISON - FIRST WINDOW"
echo "======================================"
echo ""

for PATTERN in "${PATTERNS[@]}"; do
    echo "========== $PATTERN =========="
    
    if [ ! -d "src/streamer/data/custom_patterns/$PATTERN" ]; then
        echo "Pattern not found, skipping..."
        continue
    fi
    
    # Clean up
    pkill -9 -f "StreamingQuery" 2>/dev/null
    pkill -9 -f "publish" 2>/dev/null
    sleep 2
    
    # Test Chunked
    echo "Testing Chunked..."
    rm -f chunked_latency_log.csv streaming_query_chunk_aggregator_log.csv
    export DATA_PATH="custom_patterns/$PATTERN"
    
    node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 3
    
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUB_PID=$!
    
    # Wait for completion (max 80 seconds)
    for i in {1..80}; do
        sleep 1
        if [ ! -d "/proc/$PUB_PID" ] 2>/dev/null; then
            if ! ps -p $PUB_PID > /dev/null 2>&1; then
                break
            fi
        fi
    done
    
    sleep 2
    kill $ORCH_PID 2>/dev/null
    kill $PUB_PID 2>/dev/null
    
    # Save results
    if [ -f "chunked_latency_log.csv" ]; then
        cp chunked_latency_log.csv "$RESULTS_DIR/${PATTERN}_chunked.csv"
        echo "✓ Chunked completed"
    else
        echo "✗ Chunked failed"
    fi
    
    sleep 3
    
    # Test Fetching
    echo "Testing Fetching..."
    rm -f fetching_client_side_log.csv
    export DATA_PATH="custom_patterns/$PATTERN"
    
    node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
    ORCH_PID=$!
    sleep 3
    
    node dist/streamer/src/publish.js > /dev/null 2>&1 &
    PUB_PID=$!
    
    # Wait for completion
    for i in {1..80}; do
        sleep 1
        if ! ps -p $PUB_PID > /dev/null 2>&1; then
            break
        fi
    done
    
    sleep 2
    kill $ORCH_PID 2>/dev/null
    kill $PUB_PID 2>/dev/null
    
    # Save results
    if [ -f "fetching_client_side_log.csv" ]; then
        cp fetching_client_side_log.csv "$RESULTS_DIR/${PATTERN}_fetching.csv"
        echo "✓ Fetching completed"
    else
        echo "✗ Fetching failed"
    fi
    
    sleep 3
    echo ""
done

echo "======================================"
echo "Tests complete! Analyzing results..."
echo "======================================"

# Create analysis script
node << 'EOF'
const fs = require('fs');
const path = require('path');

const resultsDir = 'pattern_comparison_results';
const patterns = ['step_pattern', 'spike_pattern', 'low_freq_oscillation', 'high_freq_oscillation', 'low_variability'];

console.log('\n========================================');
console.log('FIRST WINDOW ANALYSIS');
console.log('========================================\n');

const summaryData = [];

patterns.forEach(pattern => {
    const chunkedFile = path.join(resultsDir, `${pattern}_chunked.csv`);
    const fetchingFile = path.join(resultsDir, `${pattern}_fetching.csv`);
    
    console.log(`Pattern: ${pattern}`);
    console.log('─'.repeat(70));
    
    let chunkedResult = null;
    let chunkedLatency = null;
    let fetchingResult = null;
    let fetchingLatency = null;
    let fetchingQueryReg = null;
    let fetchingResultTime = null;
    
    // Read Chunked results (first window only)
    if (fs.existsSync(chunkedFile)) {
        try {
            const content = fs.readFileSync(chunkedFile, 'utf8');
            const lines = content.split('\n').filter(l => l.trim());
            if (lines.length >= 2) {
                // Parse CSV: window_number,query_registered_at,...,result_value
                const parts = lines[1].split(',');
                const windowNum = parseInt(parts[0]);
                const queryReg = parseInt(parts[1]);
                const resultEmitted = parseInt(parts[6]);
                chunkedResult = parseFloat(parts[11]);
                chunkedLatency = resultEmitted - queryReg;
                console.log(`Chunked   - Window ${windowNum}: ${chunkedResult.toFixed(4)}, Latency: ${chunkedLatency.toFixed(2)}ms`);
            }
        } catch (e) {
            console.log(`Chunked   - Error reading results: ${e.message}`);
        }
    } else {
        console.log('Chunked   - No results file');
    }
    
    // Read Fetching results (first window only)
    if (fs.existsSync(fetchingFile)) {
        try {
            const content = fs.readFileSync(fetchingFile, 'utf8');
            const lines = content.split('\n').filter(l => l.trim());
            
            // Parse log format
            lines.forEach(line => {
                if (line.includes('fetching_query_registered')) {
                    const parts = line.split(',');
                    fetchingQueryReg = parseInt(parts[0]);
                } else if (line.includes('RStream result generated:') && fetchingResult === null) {
                    const parts = line.split(',');
                    fetchingResultTime = parseInt(parts[0]);
                    const match = line.match(/result generated: ([-\d.]+)/);
                    if (match) {
                        fetchingResult = parseFloat(match[1]);
                    }
                }
            });
            
            if (fetchingResult !== null && fetchingQueryReg !== null && fetchingResultTime !== null) {
                fetchingLatency = fetchingResultTime - fetchingQueryReg;
                console.log(`Fetching  - Window 1: ${fetchingResult.toFixed(4)}, Latency: ${fetchingLatency.toFixed(2)}ms`);
            } else {
                console.log('Fetching  - Could not parse results');
            }
        } catch (e) {
            console.log(`Fetching  - Error reading results: ${e.message}`);
        }
    } else {
        console.log('Fetching  - No results file');
    }
    
    // Compare
    if (chunkedResult !== null && fetchingResult !== null) {
        const accuracyDiff = Math.abs(chunkedResult - fetchingResult);
        const accuracyPct = chunkedResult !== 0 ? (accuracyDiff / Math.abs(chunkedResult)) * 100 : 0;
        
        const latencyDiff = chunkedLatency - fetchingLatency;
        const latencyPct = (latencyDiff / fetchingLatency) * 100;
        
        console.log('');
        console.log(`Accuracy Difference: ${accuracyDiff.toFixed(6)} (${accuracyPct.toFixed(4)}%)`);
        console.log(`Alignment: ${accuracyPct < 0.01 ? '✅ PERFECT (< 0.01%)' : accuracyPct < 1 ? '⚠️  GOOD (< 1%)' : '❌ POOR (>= 1%)'}`);
        console.log('');
        console.log(`Latency Difference: ${latencyDiff > 0 ? '+' : ''}${latencyDiff.toFixed(2)}ms (${latencyPct > 0 ? '+' : ''}${latencyPct.toFixed(1)}%)`);
        console.log(`Latency Winner: ${latencyDiff < 0 ? '🏆 Chunked (faster)' : '🏆 Fetching (faster)'}`);
        
        summaryData.push({
            pattern,
            chunkedResult,
            fetchingResult,
            accuracyDiff,
            accuracyPct,
            chunkedLatency,
            fetchingLatency,
            latencyDiff,
            latencyPct
        });
    }
    
    console.log('\n');
});

// Overall summary
if (summaryData.length > 0) {
    console.log('========================================');
    console.log('OVERALL SUMMARY');
    console.log('========================================\n');
    
    const avgAccuracyPct = summaryData.reduce((sum, d) => sum + d.accuracyPct, 0) / summaryData.length;
    const avgLatencyDiff = summaryData.reduce((sum, d) => sum + d.latencyDiff, 0) / summaryData.length;
    const avgLatencyPct = summaryData.reduce((sum, d) => sum + d.latencyPct, 0) / summaryData.length;
    
    const avgChunkedLatency = summaryData.reduce((sum, d) => sum + d.chunkedLatency, 0) / summaryData.length;
    const avgFetchingLatency = summaryData.reduce((sum, d) => sum + d.fetchingLatency, 0) / summaryData.length;
    
    console.log(`Patterns tested: ${summaryData.length}/${patterns.length}`);
    console.log('');
    console.log('Average Accuracy:');
    console.log(`  Difference: ${avgAccuracyPct.toFixed(4)}%`);
    console.log(`  Status: ${avgAccuracyPct < 0.01 ? '✅ PERFECT' : avgAccuracyPct < 1 ? '⚠️ GOOD' : '❌ NEEDS WORK'}`);
    console.log('');
    console.log('Average Latency:');
    console.log(`  Chunked:  ${avgChunkedLatency.toFixed(2)}ms`);
    console.log(`  Fetching: ${avgFetchingLatency.toFixed(2)}ms`);
    console.log(`  Difference: ${avgLatencyDiff > 0 ? '+' : ''}${avgLatencyDiff.toFixed(2)}ms (${avgLatencyPct > 0 ? '+' : ''}${avgLatencyPct.toFixed(1)}%)`);
    console.log(`  Winner: ${avgLatencyDiff < 0 ? '🏆 Chunked faster on average' : '🏆 Fetching faster on average'}`);
    
    // Create CSV summary
    const csvLines = ['Pattern,Chunked_Window1,Fetching_Window1,Accuracy_Diff_%,Chunked_Latency_ms,Fetching_Latency_ms,Latency_Diff_ms,Latency_Diff_%'];
    summaryData.forEach(d => {
        csvLines.push(`${d.pattern},${d.chunkedResult},${d.fetchingResult},${d.accuracyPct.toFixed(4)},${d.chunkedLatency},${d.fetchingLatency},${d.latencyDiff.toFixed(2)},${d.latencyPct.toFixed(2)}`);
    });
    
    fs.writeFileSync('pattern_comparison_summary.csv', csvLines.join('\n'));
    console.log('\n✓ Summary saved to pattern_comparison_summary.csv');
}

EOF
