const fs = require('fs');
const path = require('path');

function extractFirstApproximationResult(logContent) {
    const lines = logContent.split('\n');
    
    // Look for the first "Final aggregation results" line containing unifiedAverage or unifiedResult
    for (const line of lines) {
        if (line.includes('Final aggregation results')) {
            // Handle escaped JSON in CSV - look for \"unifiedResult\": or \"unifiedAverage\":
            let match = line.match(/\\"unifiedResult\\":([\d.]+)/);
            if (match) {
                return parseFloat(match[1]);
            }
            // Fallback to unifiedAverage for backward compatibility
            match = line.match(/\\"unifiedAverage\\":([\d.]+)/);
            if (match) {
                return parseFloat(match[1]);
            }
        }
    }
    
    // Fallback: look for "Successfully published unified cross-sensor" line
    for (const line of lines) {
        if (line.includes('Successfully published unified cross-sensor')) {
            const match = line.match(/Successfully published unified cross-sensor [\w\s]+:\s*([\d.]+)/);
            if (match) {
                return parseFloat(match[1]);
            }
        }
    }
    return null;
}

function extractAllApproximationResults(logContent) {
    const lines = logContent.split('\n');
    const results = [];
    
    for (const line of lines) {
        // Look for lines containing "Final aggregation results" which contain unifiedResult or unifiedAverage
        if (line.includes('Final aggregation results')) {
            // Handle escaped JSON in CSV - look for \"unifiedResult\": first, then \"unifiedAverage\":
            let match = line.match(/\\"unifiedResult\\":([\d.]+)/);
            if (match) {
                results.push(parseFloat(match[1]));
                continue;
            }
            // Fallback to unifiedAverage for backward compatibility
            match = line.match(/\\"unifiedAverage\\":([\d.]+)/);
            if (match) {
                results.push(parseFloat(match[1]));
            }
        }
    }
    
    return results;
}

function extractAllFetchingResults(logContent) {
    const lines = logContent.split('\n');
    const results = [];
    
    // Look for ALL "Successfully published result" lines
    for (const line of lines) {
        if (line.includes('Successfully published result:')) {
            const match = line.match(/Successfully published result:\s*([\d.]+)/);
            if (match) {
                results.push(parseFloat(match[1]));
            }
        }
    }
    
    return results;
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

console.log('='.repeat(80));
console.log('              FIRST APPROXIMATION vs FETCHING CLIENT SIDE COMPARISON');
console.log('='.repeat(80));
console.log();

for (const pattern of patterns) {
    // Extract ALL approximation results from iteration1
    let firstApproxResult = null;
    let allApproxResults = [];
    const approxPath = `logs/approximation-patterns/${pattern}/iteration1/approximation_approach_log.csv`;
    
    if (fs.existsSync(approxPath)) {
        const logContent = fs.readFileSync(approxPath, 'utf8');
        firstApproxResult = extractFirstApproximationResult(logContent);
        allApproxResults = extractAllApproximationResults(logContent);
    }
    
    // Extract ALL fetching client side results from iteration1
    let firstFetchingResult = null;
    let allFetchingResults = [];
    const fetchingPath = `logs/fetching-client-side-patterns/${pattern}/iteration1/fetching_client_side_log.csv`;
    
    if (fs.existsSync(fetchingPath)) {
        const logContent = fs.readFileSync(fetchingPath, 'utf8');
        firstFetchingResult = extractFetchingResult(logContent);
        allFetchingResults = extractAllFetchingResults(logContent);
    }
    
    // Calculate accuracy metrics for all available results
    let averageAccuracy = null;
    let allAccuracies = [];
    
    if (allApproxResults.length > 0 && allFetchingResults.length > 0) {
        // Compare each approximation result with corresponding fetching result
        const minLength = Math.min(allApproxResults.length, allFetchingResults.length);
        
        for (let i = 0; i < minLength; i++) {
            const accuracy = Math.abs((allApproxResults[i] - allFetchingResults[i]) / allFetchingResults[i] * 100);
            allAccuracies.push(accuracy);
        }
        
        if (allAccuracies.length > 0) {
            averageAccuracy = allAccuracies.reduce((sum, acc) => sum + acc, 0) / allAccuracies.length;
        }
    }
    
    // Use first results for backward compatibility
    const firstResultAccuracy = (firstApproxResult !== null && firstFetchingResult !== null) ? 
        Math.abs((firstApproxResult - firstFetchingResult) / firstFetchingResult * 100) : null;
    
    const patternType = pattern.startsWith('challenging') ? 'Challenging' : 'Favorable';
    
    results.push({
        pattern,
        type: patternType,
        firstApprox: firstApproxResult,
        firstFetching: firstFetchingResult,
        firstResultAccuracy: firstResultAccuracy,
        allApproxResults: allApproxResults,
        allFetchingResults: allFetchingResults,
        allAccuracies: allAccuracies,
        averageAccuracy: averageAccuracy,
        approxResultCount: allApproxResults.length,
        fetchingResultCount: allFetchingResults.length
    });
}

// Generate detailed table showing ALL results
console.log('┌─' + '─'.repeat(25) + '┬─' + '─'.repeat(15) + '┬─' + '─'.repeat(15) + '┬─' + '─'.repeat(12) + '┬─' + '─'.repeat(12) + '┬─' + '─'.repeat(8) + '┬─' + '─'.repeat(8) + '┬─' + '─'.repeat(12) + '┐');
console.log('│ Pattern                  │ First Approx    │ First Fetching  │ First Acc    │ Avg Accuracy │ Approx   │ Fetching │ Pattern Type │');
console.log('│                          │ Result          │ Result          │ Diff (%)     │ Diff (%)     │ Count    │ Count    │              │');
console.log('├─' + '─'.repeat(25) + '┼─' + '─'.repeat(15) + '┼─' + '─'.repeat(15) + '┼─' + '─'.repeat(12) + '┼─' + '─'.repeat(12) + '┼─' + '─'.repeat(8) + '┼─' + '─'.repeat(8) + '┼─' + '─'.repeat(12) + '┤');

results.forEach(result => {
    const pattern = result.pattern.replace('_', ' ').substring(0, 23).padEnd(24);
    const firstApprox = result.firstApprox ? result.firstApprox.toFixed(4).padEnd(15) : 'N/A'.padEnd(15);
    const firstFetching = result.firstFetching ? result.firstFetching.toFixed(4).padEnd(15) : 'N/A'.padEnd(15);
    const firstAcc = result.firstResultAccuracy !== null ? `${result.firstResultAccuracy.toFixed(2)}%`.padEnd(12) : 'N/A'.padEnd(12);
    const avgAcc = result.averageAccuracy !== null ? `${result.averageAccuracy.toFixed(2)}%`.padEnd(12) : 'N/A'.padEnd(12);
    const approxCount = result.approxResultCount.toString().padEnd(8);
    const fetchingCount = result.fetchingResultCount.toString().padEnd(8);
    const type = result.type.padEnd(12);
    
    console.log(`│ ${pattern} │ ${firstApprox} │ ${firstFetching} │ ${firstAcc} │ ${avgAcc} │ ${approxCount} │ ${fetchingCount} │ ${type} │`);
});

console.log('└─' + '─'.repeat(25) + '┴─' + '─'.repeat(15) + '┴─' + '─'.repeat(15) + '┴─' + '─'.repeat(12) + '┴─' + '─'.repeat(12) + '┴─' + '─'.repeat(8) + '┴─' + '─'.repeat(8) + '┴─' + '─'.repeat(12) + '┘');

// Show detailed arrays for each pattern
console.log('\n' + '='.repeat(80));
console.log('                     DETAILED RESULTS ARRAYS');
console.log('='.repeat(80));

results.forEach(result => {
    console.log(`\n${result.pattern.replace('_', ' ').toUpperCase()} (${result.type}):`);
    console.log(`   Approximation Results (${result.allApproxResults.length}): [${result.allApproxResults.map(r => r.toFixed(4)).join(', ')}]`);
    console.log(`   Fetching Results (${result.allFetchingResults.length}):     [${result.allFetchingResults.map(r => r.toFixed(4)).join(', ')}]`);
    
    if (result.allAccuracies.length > 0) {
        console.log(`   Accuracy Differences (%):           [${result.allAccuracies.map(a => a.toFixed(2)).join(', ')}]`);
        console.log(`   Average Accuracy Difference:        ${result.averageAccuracy.toFixed(2)}%`);
        console.log(`   Best Accuracy:                      ${Math.min(...result.allAccuracies).toFixed(2)}%`);
        console.log(`   Worst Accuracy:                     ${Math.max(...result.allAccuracies).toFixed(2)}%`);
    } else {
        console.log(`    Cannot calculate accuracy - missing data`);
    }
});

console.log('\n' + '='.repeat(80));

// Generate CSV format for Google Sheets with ALL results
console.log('                    CSV FORMAT FOR GOOGLE SHEETS');
console.log('='.repeat(80));
console.log('Copy the following CSV data and paste it into Google Sheets:\n');

console.log('Pattern,First Approximation,First Fetching,First Accuracy Diff (%),First Accuracy %,Average Accuracy Diff (%),Average Accuracy %,Approx Count,Fetching Count,Pattern Type,All Approx Results,All Fetching Results,All Accuracy Diffs');
results.forEach(result => {
    const pattern = result.pattern.replace('_', ' ');
    const firstApprox = result.firstApprox ? result.firstApprox.toFixed(4) : 'N/A';
    const firstFetching = result.firstFetching ? result.firstFetching.toFixed(4) : 'N/A';
    const firstAccDiff = result.firstResultAccuracy !== null ? result.firstResultAccuracy.toFixed(2) : 'N/A';
    const firstAccuracy = result.firstResultAccuracy !== null ? (100 - result.firstResultAccuracy).toFixed(2) : 'N/A';
    const avgAccDiff = result.averageAccuracy !== null ? result.averageAccuracy.toFixed(2) : 'N/A';
    const avgAccuracy = result.averageAccuracy !== null ? (100 - result.averageAccuracy).toFixed(2) : 'N/A';
    const approxCount = result.approxResultCount;
    const fetchingCount = result.fetchingResultCount;
    const type = result.type;
    
    // Create arrays as pipe-separated strings for CSV compatibility
    const allApproxStr = result.allApproxResults.length > 0 ? result.allApproxResults.map(r => r.toFixed(4)).join('|') : 'N/A';
    const allFetchingStr = result.allFetchingResults.length > 0 ? result.allFetchingResults.map(r => r.toFixed(4)).join('|') : 'N/A';
    const allAccStr = result.allAccuracies.length > 0 ? result.allAccuracies.map(a => a.toFixed(2)).join('|') : 'N/A';
    
    console.log(`${pattern},${firstApprox},${firstFetching},${firstAccDiff},${firstAccuracy},${avgAccDiff},${avgAccuracy},${approxCount},${fetchingCount},${type},"${allApproxStr}","${allFetchingStr}","${allAccStr}"`);
});

console.log('\n' + '='.repeat(80));

// Calculate summary statistics for patterns with complete data
const validResults = results.filter(r => r.firstResultAccuracy !== null);
const validAvgResults = results.filter(r => r.averageAccuracy !== null);
const challenging = validResults.filter(r => r.type === 'Challenging');
const favorable = validResults.filter(r => r.type === 'Favorable');
const challengingAvg = validAvgResults.filter(r => r.type === 'Challenging');
const favorableAvg = validAvgResults.filter(r => r.type === 'Favorable');

console.log('\n' + '='.repeat(80));
console.log('                               COMPARISON SUMMARY');
console.log('='.repeat(80));

console.log(`\nTOTAL PATTERNS WITH COMPLETE DATA:`);
console.log(`   • First result comparisons: ${validResults.length}/16`);
console.log(`   • Average result comparisons: ${validAvgResults.length}/16`);

// Summary for first results
if (challenging.length > 0) {
    const challengingAvgDiff = challenging.reduce((sum, r) => sum + r.firstResultAccuracy, 0) / challenging.length;
    const challengingMin = Math.min(...challenging.map(r => r.firstResultAccuracy));
    const challengingMax = Math.max(...challenging.map(r => r.firstResultAccuracy));
    
    console.log(`\n🔴 CHALLENGING PATTERNS - FIRST RESULTS (${challenging.length}/9 with data):`);
    console.log(`   • Average difference: ${challengingAvgDiff.toFixed(3)}%`);
    console.log(`   • Range: ${challengingMin.toFixed(3)}% - ${challengingMax.toFixed(3)}%`);
}

if (favorable.length > 0) {
    const favorableAvgDiff = favorable.reduce((sum, r) => sum + r.firstResultAccuracy, 0) / favorable.length;
    const favorableMin = Math.min(...favorable.map(r => r.firstResultAccuracy));
    const favorableMax = Math.max(...favorable.map(r => r.firstResultAccuracy));
    
    console.log(`\n🟢 FAVORABLE PATTERNS - FIRST RESULTS (${favorable.length}/7 with data):`);
    console.log(`   • Average difference: ${favorableAvgDiff.toFixed(3)}%`);
    console.log(`   • Range: ${favorableMin.toFixed(3)}% - ${favorableMax.toFixed(3)}%`);
}

// Summary for average results across all windows
if (challengingAvg.length > 0) {
    const challengingAvgDiff = challengingAvg.reduce((sum, r) => sum + r.averageAccuracy, 0) / challengingAvg.length;
    const challengingMin = Math.min(...challengingAvg.map(r => r.averageAccuracy));
    const challengingMax = Math.max(...challengingAvg.map(r => r.averageAccuracy));
    
    console.log(`\n🔴 CHALLENGING PATTERNS - AVERAGE ACROSS ALL WINDOWS (${challengingAvg.length}/9 with data):`);
    console.log(`   • Average difference: ${challengingAvgDiff.toFixed(3)}%`);
    console.log(`   • Range: ${challengingMin.toFixed(3)}% - ${challengingMax.toFixed(3)}%`);
}

if (favorableAvg.length > 0) {
    const favorableAvgDiff = favorableAvg.reduce((sum, r) => sum + r.averageAccuracy, 0) / favorableAvg.length;
    const favorableMin = Math.min(...favorableAvg.map(r => r.averageAccuracy));
    const favorableMax = Math.max(...favorableAvg.map(r => r.averageAccuracy));
    
    console.log(`\n🟢 FAVORABLE PATTERNS - AVERAGE ACROSS ALL WINDOWS (${favorableAvg.length}/7 with data):`);
    console.log(`   • Average difference: ${favorableAvgDiff.toFixed(3)}%`);
    console.log(`   • Range: ${favorableMin.toFixed(3)}% - ${favorableMax.toFixed(3)}%`);
}

if (validResults.length > 0) {
    const overallAvg = validResults.reduce((sum, r) => sum + r.firstResultAccuracy, 0) / validResults.length;
    const overallMin = Math.min(...validResults.map(r => r.firstResultAccuracy));
    const overallMax = Math.max(...validResults.map(r => r.firstResultAccuracy));
    
    console.log(`\nOVERALL PERFORMANCE - FIRST RESULTS:`);
    console.log(`   • Average difference: ${overallAvg.toFixed(3)}%`);
    console.log(`   • Range: ${overallMin.toFixed(3)}% - ${overallMax.toFixed(3)}%`);
}

if (validAvgResults.length > 0) {
    const overallAvgAll = validAvgResults.reduce((sum, r) => sum + r.averageAccuracy, 0) / validAvgResults.length;
    const overallMinAll = Math.min(...validAvgResults.map(r => r.averageAccuracy));
    const overallMaxAll = Math.max(...validAvgResults.map(r => r.averageAccuracy));
    
    console.log(`\nOVERALL PERFORMANCE - AVERAGE ACROSS ALL WINDOWS:`);
    console.log(`   • Average difference: ${overallAvgAll.toFixed(3)}%`);
    console.log(`   • Range: ${overallMinAll.toFixed(3)}% - ${overallMaxAll.toFixed(3)}%`);
}

// Result count analysis
console.log(`\nRESULT COUNT ANALYSIS:`);
const approxCounts = results.map(r => r.approxResultCount);
const fetchingCounts = results.map(r => r.fetchingResultCount);

console.log(`   • Approximation results per pattern: ${Math.min(...approxCounts)} - ${Math.max(...approxCounts)} (avg: ${(approxCounts.reduce((a,b) => a+b, 0) / approxCounts.length).toFixed(1)})`);
console.log(`   • Fetching results per pattern: ${Math.min(...fetchingCounts)} - ${Math.max(...fetchingCounts)} (avg: ${(fetchingCounts.reduce((a,b) => a+b, 0) / fetchingCounts.length).toFixed(1)})`);

// Missing data analysis
const missingApprox = results.filter(r => r.firstApprox === null);
const missingFetching = results.filter(r => r.firstFetching === null);

if (missingApprox.length > 0 || missingFetching.length > 0) {
    console.log(`\n MISSING DATA:`);
    if (missingApprox.length > 0) {
        console.log(`   • Missing approximation data: ${missingApprox.map(r => r.pattern).join(', ')}`);
    }
    if (missingFetching.length > 0) {
        console.log(`   • Missing fetching client side data: ${missingFetching.map(r => r.pattern).join(', ')}`);
    }
}

// Best and worst performers
if (validResults.length > 0) {
    const bestFirstResult = validResults.reduce((best, current) => current.firstResultAccuracy < best.firstResultAccuracy ? current : best);
    const worstFirstResult = validResults.reduce((worst, current) => current.firstResultAccuracy > worst.firstResultAccuracy ? current : worst);
    
    console.log(`\n🏆 PERFORMANCE HIGHLIGHTS - FIRST RESULTS:`);
    console.log(`   • Best accuracy: ${bestFirstResult.pattern} (${bestFirstResult.firstResultAccuracy.toFixed(3)}% difference)`);
    console.log(`   • Worst accuracy: ${worstFirstResult.pattern} (${worstFirstResult.firstResultAccuracy.toFixed(3)}% difference)`);
}

if (validAvgResults.length > 0) {
    const bestAvgResult = validAvgResults.reduce((best, current) => current.averageAccuracy < best.averageAccuracy ? current : best);
    const worstAvgResult = validAvgResults.reduce((worst, current) => current.averageAccuracy > worst.averageAccuracy ? current : worst);
    
    console.log(`\n🏆 PERFORMANCE HIGHLIGHTS - AVERAGE ACROSS ALL WINDOWS:`);
    console.log(`   • Best average accuracy: ${bestAvgResult.pattern} (${bestAvgResult.averageAccuracy.toFixed(3)}% difference)`);
    console.log(`   • Worst average accuracy: ${worstAvgResult.pattern} (${worstAvgResult.averageAccuracy.toFixed(3)}% difference)`);
}

console.log('\n' + '='.repeat(80));

// Export detailed results
const exportData = {
    summary: {
        totalPatterns: validResults.length,
        challengingPatterns: challenging.length,
        favorablePatterns: favorable.length,
        overallStats: validResults.length > 0 ? {
            averageDifference: validResults.reduce((sum, r) => sum + r.diff, 0) / validResults.length,
            minDifference: Math.min(...validResults.map(r => r.diff)),
            maxDifference: Math.max(...validResults.map(r => r.diff)),
            perfectAccuracy: validResults.filter(r => r.diff < 0.01).length
        } : null
    },
    detailedResults: results,
    missingData: {
        missingApproximation: missingApprox.map(r => r.pattern),
        missingFetching: missingFetching.map(r => r.pattern)
    }
};

fs.writeFileSync('first-vs-fetching-comparison.json', JSON.stringify(exportData, null, 2));
console.log('Detailed results exported to first-vs-fetching-comparison.json');

// Export to CSV file for easy import to Google Sheets - Enhanced with all results
const csvContent = [
    'Pattern,First Approximation,First Fetching,First Accuracy Diff (%),First Accuracy %,Average Accuracy Diff (%),Average Accuracy %,Approx Count,Fetching Count,Pattern Type,All Approx Results,All Fetching Results,All Accuracy Diffs'
];

results.forEach(result => {
    const pattern = result.pattern.replace('_', ' ');
    const firstApprox = result.firstApprox ? result.firstApprox.toFixed(4) : 'N/A';
    const firstFetching = result.firstFetching ? result.firstFetching.toFixed(4) : 'N/A';
    const firstAccDiff = result.firstResultAccuracy !== null ? result.firstResultAccuracy.toFixed(2) : 'N/A';
    const firstAccuracy = result.firstResultAccuracy !== null ? (100 - result.firstResultAccuracy).toFixed(2) : 'N/A';
    const avgAccDiff = result.averageAccuracy !== null ? result.averageAccuracy.toFixed(2) : 'N/A';
    const avgAccuracy = result.averageAccuracy !== null ? (100 - result.averageAccuracy).toFixed(2) : 'N/A';
    const approxCount = result.approxResultCount;
    const fetchingCount = result.fetchingResultCount;
    const type = result.type;
    
    // Create arrays as pipe-separated strings for CSV compatibility
    const allApproxStr = result.allApproxResults.length > 0 ? result.allApproxResults.map(r => r.toFixed(4)).join('|') : 'N/A';
    const allFetchingStr = result.allFetchingResults.length > 0 ? result.allFetchingResults.map(r => r.toFixed(4)).join('|') : 'N/A';
    const allAccStr = result.allAccuracies.length > 0 ? result.allAccuracies.map(a => a.toFixed(2)).join('|') : 'N/A';
    
    csvContent.push(`${pattern},${firstApprox},${firstFetching},${firstAccDiff},${firstAccuracy},${avgAccDiff},${avgAccuracy},${approxCount},${fetchingCount},${type},"${allApproxStr}","${allFetchingStr}","${allAccStr}"`);
});

fs.writeFileSync('comparison-results-detailed.csv', csvContent.join('\n'));
console.log('Enhanced CSV results exported to comparison-results-detailed.csv for Google Sheets import');

// Also create a simplified CSV for backward compatibility
const simpleCsvContent = [
    'Pattern,First Approximation,Fetching Client Side Result,Accuracy Difference (%),Accuracy Percentage (%),Results Count,Pattern Type'
];

results.forEach(result => {
    const pattern = result.pattern.replace('_', ' ');
    const approx = result.firstApprox ? result.firstApprox.toFixed(4) : 'N/A';
    const fetching = result.firstFetching ? result.firstFetching.toFixed(4) : 'N/A';
    const diff = result.firstResultAccuracy !== null ? result.firstResultAccuracy.toFixed(2) : 'N/A';
    const accuracy = result.firstResultAccuracy !== null ? (100 - result.firstResultAccuracy).toFixed(2) : 'N/A';
    const count = result.approxResultCount;
    const type = result.type;
    
    simpleCsvContent.push(`${pattern},${approx},${fetching},${diff},${accuracy},${count},${type}`);
});

fs.writeFileSync('comparison-results.csv', simpleCsvContent.join('\n'));
console.log('Simple CSV results exported to comparison-results.csv for backward compatibility');
