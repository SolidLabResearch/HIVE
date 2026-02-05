#!/bin/bash

# Quick test for spike_pattern only with all 3 approaches

PATTERN="spike_pattern"
RESULTS_DIR="spike_pattern_results"
mkdir -p $RESULTS_DIR

echo "======================================"
echo "SPIKE PATTERN - ALL APPROACHES TEST"
echo "======================================"
echo ""

# Clean up
pkill -9 -f "StreamingQuery" 2>/dev/null
pkill -9 -f "publish" 2>/dev/null
sleep 2

# Test Fetching
echo "[1/3] Testing Fetching..."
rm -f fetching_client_side_log.csv fetching_client_side_resource_usage.csv
export DATA_PATH="custom_patterns/$PATTERN"

gtimeout 100 node dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js > /dev/null 2>&1 &
ORCH_PID=$!
sleep 3

gtimeout 100 node dist/streamer/src/publish.js > /dev/null 2>&1
sleep 3
kill $ORCH_PID 2>/dev/null
sleep 1

if [ -f "fetching_client_side_log.csv" ]; then
    cp fetching_client_side_log.csv "$RESULTS_DIR/fetching.csv"
    [ -f "fetching_client_side_resource_usage.csv" ] && cp fetching_client_side_resource_usage.csv "$RESULTS_DIR/fetching_resources.csv"
    echo "  ✓ Fetching completed"
else
    echo "  ✗ Fetching failed"
fi

sleep 3

# Test Approximation
echo "[2/3] Testing Approximation..."
rm -f approximation_latency_log.csv streaming_query_hive_resource_log.csv
export DATA_PATH="custom_patterns/$PATTERN"

gtimeout 100 node dist/approaches/StreamingQueryApproximationApproachOrchestrator.js > /dev/null 2>&1 &
ORCH_PID=$!
sleep 3

gtimeout 100 node dist/streamer/src/publish.js > /dev/null 2>&1
sleep 3
kill $ORCH_PID 2>/dev/null
sleep 1

if [ -f "approximation_latency_log.csv" ]; then
    cp approximation_latency_log.csv "$RESULTS_DIR/approximation.csv"
    [ -f "streaming_query_hive_resource_log.csv" ] && cp streaming_query_hive_resource_log.csv "$RESULTS_DIR/approximation_resources.csv"
    echo "  ✓ Approximation completed"
else
    echo "  ✗ Approximation failed"
fi

sleep 3

# Test Chunked
echo "[3/3] Testing Chunked..."
rm -f chunked_latency_log.csv streaming_query_hive_resource_log.csv
export DATA_PATH="custom_patterns/$PATTERN"

gtimeout 100 node dist/approaches/StreamingQueryChunkedApproachOrchestrator.js > /dev/null 2>&1 &
ORCH_PID=$!
sleep 3

gtimeout 100 node dist/streamer/src/publish.js > /dev/null 2>&1
sleep 3
kill $ORCH_PID 2>/dev/null
sleep 1

if [ -f "chunked_latency_log.csv" ]; then
    cp chunked_latency_log.csv "$RESULTS_DIR/chunked.csv"
    [ -f "streaming_query_hive_resource_log.csv" ] && cp streaming_query_hive_resource_log.csv "$RESULTS_DIR/chunked_resources.csv"
    echo "  ✓ Chunked completed"
else
    echo "  ✗ Chunked failed"
fi

echo ""
echo "======================================"
echo "Analyzing results..."
echo "======================================"

# Analysis
node << 'EOF'
const fs = require('fs');
const path = require('path');

const resultsDir = 'spike_pattern_results';

console.log('\n========================================');
console.log('SPIKE PATTERN - RESULTS');
console.log('========================================\n');

const results = {
    fetching: { result: null, latency: null },
    approximation: { result: null, latency: null },
    chunked: { result: null, latency: null }
};

// Parse Fetching
const fetchingFile = path.join(resultsDir, 'fetching.csv');
if (fs.existsSync(fetchingFile)) {
    const content = fs.readFileSync(fetchingFile, 'utf8');
    const lines = content.split('\n');
    
    let queryReg = null, resultTime = null;
    lines.forEach(line => {
        if (line.includes('fetching_query_registered')) {
            queryReg = parseInt(line.split(',')[0]);
        } else if (line.includes('RStream result generated:') && results.fetching.result === null) {
            resultTime = parseInt(line.split(',')[0]);
            const match = line.match(/result generated: ([-\d.]+)/);
            if (match) {
                results.fetching.result = parseFloat(match[1]);
            }
        }
    });
    
    if (queryReg && resultTime) {
        results.fetching.latency = resultTime - queryReg;
    }
}

// Parse Approximation
const approxFile = path.join(resultsDir, 'approximation.csv');
if (fs.existsSync(approxFile)) {
    const content = fs.readFileSync(approxFile, 'utf8');
    const lines = content.split('\n').filter(l => l.trim());
    if (lines.length >= 2) {
        const parts = lines[1].split(',');
        results.approximation.result = parseFloat(parts[parts.length - 1]);
        results.approximation.latency = parseInt(parts[parts.length - 2]);
    }
}

// Parse Chunked
const chunkedFile = path.join(resultsDir, 'chunked.csv');
if (fs.existsSync(chunkedFile)) {
    const content = fs.readFileSync(chunkedFile, 'utf8');
    const lines = content.split('\n').filter(l => l.trim());
    if (lines.length >= 2) {
        const parts = lines[1].split(',');
        results.chunked.result = parseFloat(parts[parts.length - 1]);
        results.chunked.latency = parseInt(parts[parts.length - 4]);
    }
}

// Print results
console.log('Approach          | Result        | Latency (ms) | Error %');
console.log('-'.repeat(70));

const baseline = results.fetching.result;
console.log(`Fetching          | ${results.fetching.result?.toFixed(4) || 'N/A'} | ${results.fetching.latency || 'N/A'} | 0.0000 (baseline)`);

if (results.approximation.result && baseline) {
    const error = Math.abs((results.approximation.result - baseline) / baseline) * 100;
    console.log(`Approximation     | ${results.approximation.result.toFixed(4)} | ${results.approximation.latency} | ${error.toFixed(4)}`);
}

if (results.chunked.result && baseline) {
    const error = Math.abs((results.chunked.result - baseline) / baseline) * 100;
    console.log(`Chunked (FIXED)   | ${results.chunked.result.toFixed(4)} | ${results.chunked.latency} | ${error.toFixed(4)}`);
    
    console.log('\n========================================');
    console.log('KEY FINDING');
    console.log('========================================');
    if (error < 0.01) {
        console.log(`✅ EXCELLENT! Chunked error: ${error.toFixed(6)}% (< 0.01%)`);
        console.log('✅ Overlap adjustment fix is VALIDATED!');
    } else if (error < 1) {
        console.log(`⚠️  GOOD: Chunked error: ${error.toFixed(4)}% (< 1%)`);
    } else {
        console.log(`❌ NEEDS WORK: Chunked error: ${error.toFixed(4)}%`);
    }
}

EOF

echo ""
echo "Results saved to spike_pattern_results/"
