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
    
    // Read Chunked results
    if (fs.existsSync(chunkedFile)) {
        try {
            const content = fs.readFileSync(chunkedFile, 'utf8');
            const lines = content.split('\n').filter(l => l.trim());
            if (lines.length >= 2) {
                const parts = lines[1].split(',');
                const windowNum = parseInt(parts[0]);
                const queryReg = parseInt(parts[1]);
                const resultEmitted = parseInt(parts[6]);
                chunkedResult = parseFloat(parts[11]);
                chunkedLatency = resultEmitted - queryReg;
                console.log(`Chunked   - Window ${windowNum}: ${chunkedResult.toFixed(4)}, Latency: ${chunkedLatency.toFixed(2)}ms`);
            }
        } catch (e) {
            console.log(`Chunked   - Error: ${e.message}`);
        }
    }
    
    // Read Fetching results
    if (fs.existsSync(fetchingFile)) {
        try {
            const content = fs.readFileSync(fetchingFile, 'utf8');
            const lines = content.split('\n').filter(l => l.trim());
            
            lines.forEach(line => {
                if (line.includes('fetching_query_registered')) {
                    fetchingQueryReg = parseInt(line.split(',')[0]);
                } else if (line.includes('RStream result generated:') && fetchingResult === null) {
                    fetchingResultTime = parseInt(line.split(',')[0]);
                    const match = line.match(/result generated: ([-\d.]+)/);
                    if (match) {
                        fetchingResult = parseFloat(match[1]);
                    }
                }
            });
            
            if (fetchingResult !== null && fetchingQueryReg !== null) {
                fetchingLatency = fetchingResultTime - fetchingQueryReg;
                console.log(`Fetching  - Window 1: ${fetchingResult.toFixed(4)}, Latency: ${fetchingLatency.toFixed(2)}ms`);
            }
        } catch (e) {
            console.log(`Fetching  - Error: ${e.message}`);
        }
    }
    
    // Compare
    if (chunkedResult !== null && fetchingResult !== null) {
        const accuracyDiff = Math.abs(chunkedResult - fetchingResult);
        const accuracyPct = chunkedResult !== 0 ? (accuracyDiff / Math.abs(chunkedResult)) * 100 : 0;
        const latencyDiff = chunkedLatency - fetchingLatency;
        const latencyPct = (latencyDiff / fetchingLatency) * 100;
        
        console.log('');
        console.log(`Accuracy Diff: ${accuracyDiff.toFixed(6)} (${accuracyPct.toFixed(4)}%)`);
        console.log(`Alignment: ${accuracyPct < 0.01 ? '✅ PERFECT' : accuracyPct < 1 ? '⚠️  GOOD' : '❌ POOR'}`);
        console.log(`Latency Diff: ${latencyDiff > 0 ? '+' : ''}${latencyDiff.toFixed(2)}ms (${latencyPct > 0 ? '+' : ''}${latencyPct.toFixed(1)}%)`);
        console.log(`Winner: ${latencyDiff < 0 ? '🏆 Chunked faster' : '🏆 Fetching faster'}`);
        
        summaryData.push({ pattern, chunkedResult, fetchingResult, accuracyDiff, accuracyPct, chunkedLatency, fetchingLatency, latencyDiff, latencyPct });
    }
    
    console.log('\n');
});

// Summary
if (summaryData.length > 0) {
    console.log('========================================');
    console.log('OVERALL SUMMARY');
    console.log('========================================\n');
    
    const avgAccuracyPct = summaryData.reduce((s, d) => s + d.accuracyPct, 0) / summaryData.length;
    const avgChunkedLatency = summaryData.reduce((s, d) => s + d.chunkedLatency, 0) / summaryData.length;
    const avgFetchingLatency = summaryData.reduce((s, d) => s + d.fetchingLatency, 0) / summaryData.length;
    const avgLatencyDiff = avgChunkedLatency - avgFetchingLatency;
    const avgLatencyPct = (avgLatencyDiff / avgFetchingLatency) * 100;
    
    console.log(`Patterns tested: ${summaryData.length}/${patterns.length}`);
    console.log('');
    console.log('Accuracy:');
    console.log(`  Avg difference: ${avgAccuracyPct.toFixed(4)}%`);
    console.log(`  Status: ${avgAccuracyPct < 0.01 ? '✅ PERFECT ALIGNMENT' : '⚠️ NEEDS INVESTIGATION'}`);
    console.log('');
    console.log('Latency:');
    console.log(`  Chunked:  ${avgChunkedLatency.toFixed(2)}ms`);
    console.log(`  Fetching: ${avgFetchingLatency.toFixed(2)}ms`);
    console.log(`  Difference: ${avgLatencyDiff > 0 ? '+' : ''}${avgLatencyDiff.toFixed(2)}ms (${avgLatencyPct > 0 ? '+' : ''}${avgLatencyPct.toFixed(1)}%)`);
    console.log(`  Winner: ${avgLatencyDiff < 0 ? '🏆 Chunked is faster' : '🏆 Fetching is faster'}`);
    
    // CSV export
    const csv = ['Pattern,Chunked_Result,Fetching_Result,Accuracy_Diff_%,Chunked_Latency_ms,Fetching_Latency_ms,Latency_Diff_ms,Latency_Diff_%'];
    summaryData.forEach(d => csv.push(`${d.pattern},${d.chunkedResult},${d.fetchingResult},${d.accuracyPct.toFixed(4)},${d.chunkedLatency},${d.fetchingLatency},${d.latencyDiff.toFixed(2)},${d.latencyPct.toFixed(2)}`));
    fs.writeFileSync('pattern_comparison_summary.csv', csv.join('\n'));
    console.log('\n✓ Summary saved to: pattern_comparison_summary.csv');
}
