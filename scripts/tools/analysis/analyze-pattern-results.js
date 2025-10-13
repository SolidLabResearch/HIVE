const fs = require('fs');
const path = require('path');

console.log('Analyzing experiment results for pattern comparison...\n');

const APPROXIMATION_DIR = 'logs/approximation-patterns';
const FETCHING_DIR = 'logs/fetching-client-side-patterns';

// Function to extract unified averages from approximation approach logs
function extractApproximationResults(logPath) {
    if (!fs.existsSync(logPath)) return [];
    
    const content = fs.readFileSync(logPath, 'utf8');
    const lines = content.split('\n');
    const results = [];
    
    for (const line of lines) {
        if (line.includes('Successfully published unified cross-sensor average:')) {
            const match = line.match(/unified cross-sensor average: ([-\d.]+)/);
            if (match) {
                const timestamp = line.split(',')[0];
                results.push({
                    timestamp: parseInt(timestamp),
                    average: parseFloat(match[1])
                });
            }
        }
    }
    
    return results;
}

// Function to extract results from fetching client side logs
function extractFetchingResults(logPath) {
    if (!fs.existsSync(logPath)) return [];
    
    const content = fs.readFileSync(logPath, 'utf8');
    const lines = content.split('\n');
    const results = [];
    
    for (const line of lines) {
        if (line.includes('Processing valid result:')) {
            const match = line.match(/Processing valid result: ([-\d.]+)/);
            if (match) {
                const timestamp = line.split(',')[0];
                results.push({
                    timestamp: parseInt(timestamp),
                    average: parseFloat(match[1])
                });
            }
        }
    }
    
    return results;
}

// Function to calculate statistics
function calculateStats(values) {
    if (values.length === 0) return { count: 0, mean: null, std: null, min: null, max: null };
    
    const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
    const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
    const std = Math.sqrt(variance);
    const min = Math.min(...values);
    const max = Math.max(...values);
    
    return { count: values.length, mean, std, min, max };
}

// Get all pattern directories
const approximationPatterns = fs.readdirSync(APPROXIMATION_DIR).filter(dir => 
    fs.statSync(path.join(APPROXIMATION_DIR, dir)).isDirectory()
);

const fetchingPatterns = fs.readdirSync(FETCHING_DIR).filter(dir => 
    fs.statSync(path.join(FETCHING_DIR, dir)).isDirectory()
);

const allPatterns = [...new Set([...approximationPatterns, ...fetchingPatterns])].sort();

console.log('='.repeat(120));
console.log('PATTERN COMPARISON ANALYSIS');
console.log('='.repeat(120));

const results = [];

for (const pattern of allPatterns) {
    console.log(`\\nAnalyzing pattern: ${pattern}`);
    console.log('-'.repeat(80));
    
    const approximationPath = path.join(APPROXIMATION_DIR, pattern, 'iteration1', 'approximation_approach_log.csv');
    const fetchingPath = path.join(FETCHING_DIR, pattern, 'iteration1', 'fetching_client_side_log.csv');
    
    const approxResults = extractApproximationResults(approximationPath);
    const fetchingResults = extractFetchingResults(fetchingPath);
    
    const approxValues = approxResults.map(r => r.average);
    const fetchingValues = fetchingResults.map(r => r.average);
    
    const approxStats = calculateStats(approxValues);
    const fetchingStats = calculateStats(fetchingValues);
    
    console.log(`Approximation Approach:`);
    console.log(`  Results found: ${approxStats.count}`);
    if (approxStats.count > 0) {
        console.log(`  Mean: ${approxStats.mean.toFixed(6)}`);
        console.log(`  Std Dev: ${approxStats.std.toFixed(6)}`);
        console.log(`  Range: [${approxStats.min.toFixed(6)}, ${approxStats.max.toFixed(6)}]`);
    }
    
    console.log(`\\nFetching Client Side:`);
    console.log(`  Results found: ${fetchingStats.count}`);
    if (fetchingStats.count > 0) {
        console.log(`  Mean: ${fetchingStats.mean.toFixed(6)}`);
        console.log(`  Std Dev: ${fetchingStats.std.toFixed(6)}`);
        console.log(`  Range: [${fetchingStats.min.toFixed(6)}, ${fetchingStats.max.toFixed(6)}]`);
    }
    
    // Calculate accuracy difference
    let accuracyDiff = null;
    let relativeDiff = null;
    if (approxStats.count > 0 && fetchingStats.count > 0) {
        accuracyDiff = Math.abs(approxStats.mean - fetchingStats.mean);
        relativeDiff = (accuracyDiff / Math.abs(fetchingStats.mean)) * 100;
        console.log(`\\nComparison:`);
        console.log(`  Absolute difference: ${accuracyDiff.toFixed(6)}`);
        console.log(`  Relative difference: ${relativeDiff.toFixed(2)}%`);
    }
    
    results.push({
        pattern,
        approximation: approxStats,
        fetching: fetchingStats,
        accuracyDiff,
        relativeDiff
    });
}

console.log('\\n' + '='.repeat(120));
console.log('SUMMARY TABLE');
console.log('='.repeat(120));

console.log('| Pattern | Approximation Avg | Fetching Avg | Abs Diff | Rel Diff (%) | Approx Count | Fetch Count |');
console.log('|---------|------------------|--------------|----------|--------------|--------------|-------------|');

for (const result of results) {
    const pattern = result.pattern.padEnd(20);
    const approxAvg = result.approximation.count > 0 ? result.approximation.mean.toFixed(4) : 'N/A';
    const fetchAvg = result.fetching.count > 0 ? result.fetching.mean.toFixed(4) : 'N/A';
    const absDiff = result.accuracyDiff !== null ? result.accuracyDiff.toFixed(4) : 'N/A';
    const relDiff = result.relativeDiff !== null ? result.relativeDiff.toFixed(2) : 'N/A';
    const approxCount = result.approximation.count;
    const fetchCount = result.fetching.count;
    
    console.log(`| ${pattern} | ${approxAvg.padStart(16)} | ${fetchAvg.padStart(12)} | ${absDiff.padStart(8)} | ${relDiff.padStart(12)} | ${approxCount.toString().padStart(12)} | ${fetchCount.toString().padStart(11)} |`);
}

console.log('\\n' + '='.repeat(120));
console.log('PATTERN CATEGORIZATION');
console.log('='.repeat(120));

const challenging = results.filter(r => r.pattern.startsWith('challenging_'));
const favorable = results.filter(r => r.pattern.startsWith('favorable_'));

console.log('\\nCHALLENGING PATTERNS (Expected lower approximation accuracy):');
challenging.forEach(r => {
    if (r.relativeDiff !== null) {
        console.log(`  ${r.pattern}: ${r.relativeDiff.toFixed(2)}% difference`);
    } else {
        console.log(`  ${r.pattern}: No comparison available`);
    }
});

console.log('\\nFAVORABLE PATTERNS (Expected higher approximation accuracy):');
favorable.forEach(r => {
    if (r.relativeDiff !== null) {
        console.log(`  ${r.pattern}: ${r.relativeDiff.toFixed(2)}% difference`);
    } else {
        console.log(`  ${r.pattern}: No comparison available`);
    }
});

// Calculate average differences
const challengingDiffs = challenging.filter(r => r.relativeDiff !== null).map(r => r.relativeDiff);
const favorableDiffs = favorable.filter(r => r.relativeDiff !== null).map(r => r.relativeDiff);

if (challengingDiffs.length > 0) {
    const avgChallengingDiff = challengingDiffs.reduce((sum, diff) => sum + diff, 0) / challengingDiffs.length;
    console.log(`\\nAverage relative difference for challenging patterns: ${avgChallengingDiff.toFixed(2)}%`);
}

if (favorableDiffs.length > 0) {
    const avgFavorableDiff = favorableDiffs.reduce((sum, diff) => sum + diff, 0) / favorableDiffs.length;
    console.log(`Average relative difference for favorable patterns: ${avgFavorableDiff.toFixed(2)}%`);
}

console.log('\\nAnalysis complete!');
