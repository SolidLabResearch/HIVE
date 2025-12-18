const fs = require('fs');
const path = require('path');

function extractFirstApproximationResult(logContent) {
    const lines = logContent.split('\n');
    
    // Look for the first "Successfully published unified cross-sensor average" line
    for (const line of lines) {
        if (line.includes('Successfully published unified cross-sensor average:')) {
            const match = line.match(/Successfully published unified cross-sensor average:\s*([\d.]+)/);
            if (match) {
                return parseFloat(match[1]);
            }
        }
    }
    return null;
}

function extractFetchingResult(logContent) {
    const lines = logContent.split('\n');
    
    // Look for the first "Successfully published result" line
    for (const line of lines) {
        if (line.includes('Successfully published result:')) {
            const match = line.match(/Successfully published result:\s*([\d.]+)/);
            if (match) {
                return parseFloat(match[1]);
            }
        }
    }
    return null;
}

const patterns = [
    'challenging_chaotic_oscillation',
    'challenging_exponential_decay', 
    'challenging_exponential_growth',
    'challenging_high_frequency_oscillation',
    'challenging_high_variance_random',
    'challenging_logarithmic',
    'challenging_sine_wave',
    'challenging_spike_pattern',
    'challenging_step_function',
    'favorable_constant_value',
    'favorable_gentle_sine',
    'favorable_gradual_trend',
    'favorable_linear_decreasing',
    'favorable_linear_increasing',
    'favorable_low_variance_random',
    'favorable_smooth_polynomial'
];

const results = [];

console.log('Analyzing first approximation results vs fetching client side results...\n');

for (const pattern of patterns) {
    console.log(`Processing ${pattern}...`);
    
    // Extract first approximation result from iteration1
    let firstApproxResult = null;
    const approxPath = `logs/approximation-patterns/${pattern}/iteration1`;
    
    if (fs.existsSync(approxPath)) {
        const files = fs.readdirSync(approxPath);
        const logFile = files.find(f => f.includes('_log.csv'));
        
        if (logFile) {
            const logContent = fs.readFileSync(path.join(approxPath, logFile), 'utf8');
            firstApproxResult = extractFirstApproximationResult(logContent);
        }
    }
    
    // Extract fetching client side result from iteration1
    let fetchingResult = null;
    const fetchingPath = `logs/fetching-client-side-patterns/${pattern}/iteration1`;
    
    if (fs.existsSync(fetchingPath)) {
        const files = fs.readdirSync(fetchingPath);
        const logFile = files.find(f => f.includes('_log.csv'));
        
        if (logFile) {
            const logContent = fs.readFileSync(path.join(fetchingPath, logFile), 'utf8');
            fetchingResult = extractFetchingResult(logContent);
        }
    }
    
    if (firstApproxResult !== null && fetchingResult !== null) {
        const diff = Math.abs((firstApproxResult - fetchingResult) / fetchingResult * 100);
        const patternType = pattern.startsWith('challenging') ? 'Challenging' : 'Favorable';
        
        results.push({
            pattern,
            type: patternType,
            firstApprox: firstApproxResult,
            fetching: fetchingResult,
            diff: diff
        });
        
        console.log(`  First approximation: ${firstApproxResult.toFixed(4)}`);
        console.log(`  Fetching client side: ${fetchingResult.toFixed(4)}`);
        console.log(`  Difference: ${diff.toFixed(2)}%`);
    } else {
        console.log(`  Missing data - Approx: ${firstApproxResult}, Fetching: ${fetchingResult}`);
    }
    console.log();
}

// Generate comparison table
console.log('='.repeat(100));
console.log('           FIRST APPROXIMATION RESULT vs FETCHING CLIENT SIDE COMPARISON');
console.log('='.repeat(100));
console.log();
console.log('┌─' + '─'.repeat(35) + '┬─' + '─'.repeat(15) + '┬─' + '─'.repeat(15) + '┬─' + '─'.repeat(10) + '┬─' + '─'.repeat(12) + '┐');
console.log('│ Pattern                            │ First Approx    │ Fetching Client │ Accuracy   │ Pattern Type │');
console.log('│                                    │ Result          │ Result          │ Diff (%)   │              │');
console.log('├─' + '─'.repeat(35) + '┼─' + '─'.repeat(15) + '┼─' + '─'.repeat(15) + '┼─' + '─'.repeat(10) + '┼─' + '─'.repeat(12) + '┤');

results.forEach(result => {
    const pattern = result.pattern.replace('_', ' ').substring(0, 33).padEnd(34);
    const approx = result.firstApprox.toFixed(4).padEnd(15);
    const fetching = result.fetching.toFixed(4).padEnd(15);
    const diff = `${result.diff.toFixed(2)}%`.padEnd(10);
    const type = result.type.padEnd(12);
    
    console.log(`│ ${pattern} │ ${approx} │ ${fetching} │ ${diff} │ ${type} │`);
});

console.log('└─' + '─'.repeat(35) + '┴─' + '─'.repeat(15) + '┴─' + '─'.repeat(15) + '┴─' + '─'.repeat(10) + '┴─' + '─'.repeat(12) + '┘');

// Calculate summary statistics
const challenging = results.filter(r => r.type === 'Challenging');
const favorable = results.filter(r => r.type === 'Favorable');

console.log('\n' + '='.repeat(100));
console.log('                               FIRST RESULT ANALYSIS SUMMARY');
console.log('='.repeat(100));

if (challenging.length > 0) {
    const challengingAvgDiff = challenging.reduce((sum, r) => sum + r.diff, 0) / challenging.length;
    console.log('\nCHALLENGING PATTERNS (First approximation result):');
    console.log(`   • Average accuracy difference: ${challengingAvgDiff.toFixed(2)}%`);
    console.log(`   • Range: ${Math.min(...challenging.map(r => r.diff)).toFixed(2)}% - ${Math.max(...challenging.map(r => r.diff)).toFixed(2)}%`);
    console.log(`   • Patterns with data: ${challenging.length}/9`);
}

if (favorable.length > 0) {
    const favorableAvgDiff = favorable.reduce((sum, r) => sum + r.diff, 0) / favorable.length;
    console.log('\nFAVORABLE PATTERNS (First approximation result):');
    console.log(`   • Average accuracy difference: ${favorableAvgDiff.toFixed(2)}%`);
    console.log(`   • Range: ${Math.min(...favorable.map(r => r.diff)).toFixed(2)}% - ${Math.max(...favorable.map(r => r.diff)).toFixed(2)}%`);
    console.log(`   • Patterns with data: ${favorable.length}/7`);
}

console.log('\nKEY OBSERVATIONS (First Result Analysis):');
console.log('   • Comparing initial approximation accuracy vs exact computation');
console.log('   • Eliminates variability from multiple approximation iterations');
console.log('   • Shows immediate approximation performance');

if (results.length > 0) {
    const bestResult = results.reduce((best, current) => current.diff < best.diff ? current : best);
    const worstResult = results.reduce((worst, current) => current.diff > worst.diff ? current : worst);
    
    console.log(`   • Best first approximation: ${bestResult.pattern} (${bestResult.diff.toFixed(2)}%)`);
    console.log(`   • Worst first approximation: ${worstResult.pattern} (${worstResult.diff.toFixed(2)}%)`);
}

console.log('\n='.repeat(100));

// Export results for further analysis
fs.writeFileSync('first-result-comparison.json', JSON.stringify(results, null, 2));
console.log('\nResults exported to first-result-comparison.json');
