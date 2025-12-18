const fs = require('fs');

// Read the detailed CSV data
const csvContent = fs.readFileSync('comparison-results-detailed.csv', 'utf8');
const lines = csvContent.split('\n');
const headers = lines[0].split(',');

// Parse the data
const results = [];
for (let i = 1; i < lines.length; i++) {
    if (lines[i].trim()) {
        const values = lines[i].split(',');
        if (values.length >= 10) {
            const pattern = values[0];
            const allApproxResults = values[10].replace(/"/g, '').split('|').map(r => parseFloat(r));
            const allFetchingResults = values[11].replace(/"/g, '').split('|').map(r => parseFloat(r));
            const avgAccuracy = parseFloat(values[6]);
            
            results.push({
                pattern,
                approxResults: allApproxResults.filter(r => !isNaN(r)),
                fetchingResults: allFetchingResults.filter(r => !isNaN(r)),
                accuracy: isNaN(avgAccuracy) ? 0 : avgAccuracy
            });
        }
    }
}

// Calculate overall statistics
const allApproxResults = [];
const allFetchingResults = [];
const allAccuracies = [];

results.forEach(result => {
    allApproxResults.push(...result.approxResults);
    allFetchingResults.push(...result.fetchingResults);
    if (result.accuracy > 0) {
        allAccuracies.push(result.accuracy);
    }
});

// Calculate overall accuracy
const overallAccuracy = allAccuracies.length > 0 ? 
    allAccuracies.reduce((sum, acc) => sum + acc, 0) / allAccuracies.length : 0;

// Format results for display
const formatResults = (resultsArray) => {
    if (resultsArray.length === 0) return 'N/A';
    if (resultsArray.length <= 4) {
        return resultsArray.map(r => r.toFixed(3)).join(', ');
    } else {
        // Show first few and last few if many results
        const first3 = resultsArray.slice(0, 3).map(r => r.toFixed(3));
        const last1 = resultsArray.slice(-1).map(r => r.toFixed(3));
        return `${first3.join(', ')}, ..., ${last1[0]}`;
    }
};

// Create the comparison table
console.log('\n' + '='.repeat(80));
console.log('                    SIMPLE COMPARISON TABLE');
console.log('='.repeat(80));
console.log();

console.log('| **Approaches** | **Results** | **Accuracy** |');
console.log('| --- | --- | --- |');
console.log(`| Approximation Approach | ${formatResults(allApproxResults)} | ${overallAccuracy.toFixed(1)}% |`);
console.log(`| Fetching Client Side (Ground Truth) | ${formatResults(allFetchingResults)} | 100% (ground truth) |`);

console.log();
console.log('**Note:** Results shown are aggregated values from all patterns and windows.');
console.log(`Total approximation results: ${allApproxResults.length}`);
console.log(`Total fetching results: ${allFetchingResults.length}`);
console.log(`Patterns analyzed: ${results.length}`);

// Create detailed table for each pattern
console.log('\n' + '='.repeat(80));
console.log('                    DETAILED BY PATTERN');
console.log('='.repeat(80));
console.log();

results.forEach(result => {
    if (result.approxResults.length > 0 && result.fetchingResults.length > 0) {
        const patternName = result.pattern.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase());
        
        console.log(`### ${patternName}`);
        console.log('| **Approaches** | **Results** | **Accuracy** |');
        console.log('| --- | --- | --- |');
        console.log(`| Approximation Approach | ${formatResults(result.approxResults)} | ${result.accuracy.toFixed(1)}% |`);
        console.log(`| Fetching Client Side (Ground Truth) | ${formatResults(result.fetchingResults)} | 100% (ground truth) |`);
        console.log();
    }
});

// Create markdown file
const markdownContent = `# Streaming Query Approaches Comparison

## Overall Results Summary

| **Approaches** | **Results** | **Accuracy** |
| --- | --- | --- |
| Approximation Approach | ${formatResults(allApproxResults)} | ${overallAccuracy.toFixed(1)}% |
| Fetching Client Side (Ground Truth) | ${formatResults(allFetchingResults)} | 100% (ground truth) |

**Note:** Results shown are aggregated values from all patterns and windows.
- Total approximation results: ${allApproxResults.length}
- Total fetching results: ${allFetchingResults.length}
- Patterns analyzed: ${results.length}

## Detailed Results by Pattern

${results.map(result => {
    if (result.approxResults.length > 0 && result.fetchingResults.length > 0) {
        const patternName = result.pattern.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase());
        return `### ${patternName}

| **Approaches** | **Results** | **Accuracy** |
| --- | --- | --- |
| Approximation Approach | ${formatResults(result.approxResults)} | ${result.accuracy.toFixed(1)}% |
| Fetching Client Side (Ground Truth) | ${formatResults(result.fetchingResults)} | 100% (ground truth) |
`;
    }
    return '';
}).filter(content => content).join('\n')}

## Summary Statistics

- **Best Accuracy Pattern:** ${results.reduce((best, current) => 
    current.accuracy > best.accuracy ? current : best, results[0])?.pattern.replace('_', ' ')} (${results.reduce((best, current) => 
    current.accuracy > best.accuracy ? current : best, results[0])?.accuracy.toFixed(1)}%)
- **Worst Accuracy Pattern:** ${results.reduce((worst, current) => 
    current.accuracy < worst.accuracy && current.accuracy > 0 ? current : worst, results.find(r => r.accuracy > 0))?.pattern.replace('_', ' ')} (${results.reduce((worst, current) => 
    current.accuracy < worst.accuracy && current.accuracy > 0 ? current : worst, results.find(r => r.accuracy > 0))?.accuracy.toFixed(1)}%)
- **Overall Accuracy:** ${overallAccuracy.toFixed(1)}%

*This comparison includes enhanced support for MAX aggregation across sensors in addition to the original AVG functionality.*
`;

fs.writeFileSync('comparison_table.md', markdownContent);
console.log('\n' + '='.repeat(80));
console.log('Markdown table saved to: comparison_table.md');
console.log('='.repeat(80));
