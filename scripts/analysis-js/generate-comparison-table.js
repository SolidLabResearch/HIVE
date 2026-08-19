const fs = require('fs');
const path = require('path');

console.log('='.repeat(100));
console.log('                    APPROXIMATION vs FETCHING CLIENT SIDE COMPARISON');
console.log('='.repeat(100));

// Data extracted from the analysis
const results = [
    {
        pattern: 'challenging_chaotic_oscillation',
        type: 'Challenging',
        approx: { avg: 48.5511, count: 11 },
        fetching: { avg: 75.5342, count: 1 },
        diff: 35.72
    },
    {
        pattern: 'challenging_exponential_decay',
        type: 'Challenging',
        approx: { avg: 53.4084, count: 15 },
        fetching: { avg: 100.0033, count: 1 },
        diff: 46.59
    },
    {
        pattern: 'challenging_exponential_growth',
        type: 'Challenging',
        approx: { avg: 47.0386, count: 16 },
        fetching: { avg: 1.0001, count: 1 },
        diff: 4603.33
    },
    {
        pattern: 'challenging_high_frequency_oscillation',
        type: 'Challenging',
        approx: { avg: 47.7007, count: 12 },
        fetching: { avg: 50.6967, count: 1 },
        diff: 5.91
    },
    {
        pattern: 'challenging_high_variance_random',
        type: 'Challenging',
        approx: { avg: 52.0961, count: 8 },
        fetching: { avg: 42.8941, count: 1 },
        diff: 21.45
    },
    {
        pattern: 'challenging_logarithmic',
        type: 'Challenging',
        approx: { avg: 47.3424, count: 14 },
        fetching: { avg: 23.3328, count: 1 },
        diff: 102.90
    },
    {
        pattern: 'challenging_sine_wave',
        type: 'Challenging',
        approx: { avg: 51.3970, count: 13 },
        fetching: { avg: 80.7615, count: 1 },
        diff: 36.36
    },
    {
        pattern: 'challenging_spike_pattern',
        type: 'Challenging',
        approx: { avg: 46.4980, count: 9 },
        fetching: { avg: 15.4280, count: 1 },
        diff: 201.39
    },
    {
        pattern: 'challenging_step_function',
        type: 'Challenging',
        approx: { avg: 42.5760, count: 10 },
        fetching: { avg: 9.9996, count: 1 },
        diff: 325.78
    },
    {
        pattern: 'favorable_constant_value',
        type: 'Favorable',
        approx: { avg: 48.7506, count: 2 },
        fetching: { avg: 49.9885, count: 1 },
        diff: 2.48
    },
    {
        pattern: 'favorable_gradual_trend',
        type: 'Favorable',
        approx: { avg: 45.0646, count: 1 },
        fetching: { avg: 45.0646, count: 1 },
        diff: 0.00
    },
    {
        pattern: 'favorable_linear_decreasing',
        type: 'Favorable',
        approx: { avg: 65.1541, count: 6 },
        fetching: { avg: 99.0526, count: 1 },
        diff: 34.22
    },
    {
        pattern: 'favorable_linear_increasing',
        type: 'Favorable',
        approx: { avg: 51.1501, count: 7 },
        fetching: { avg: 11.0545, count: 1 },
        diff: 362.71
    }
];

console.log();
console.log('┌─' + '─'.repeat(35) + '┬─' + '─'.repeat(15) + '┬─' + '─'.repeat(15) + '┬─' + '─'.repeat(10) + '┬─' + '─'.repeat(12) + '┐');
console.log('│ Pattern                            │ Approximation   │ Fetching Client │ Accuracy   │ Pattern Type │');
console.log('│                                    │ Average (Count) │ Average (Count) │ Diff (%)   │              │');
console.log('├─' + '─'.repeat(35) + '┼─' + '─'.repeat(15) + '┼─' + '─'.repeat(15) + '┼─' + '─'.repeat(10) + '┼─' + '─'.repeat(12) + '┤');

results.forEach(result => {
    const pattern = result.pattern.replace('_', ' ').substring(0, 33).padEnd(34);
    const approx = `${result.approx.avg.toFixed(2)} (${result.approx.count})`.padEnd(15);
    const fetching = `${result.fetching.avg.toFixed(2)} (${result.fetching.count})`.padEnd(15);
    const diff = `${result.diff.toFixed(2)}%`.padEnd(10);
    const type = result.type.padEnd(12);
    
    console.log(`│ ${pattern} │ ${approx} │ ${fetching} │ ${diff} │ ${type} │`);
});

console.log('└─' + '─'.repeat(35) + '┴─' + '─'.repeat(15) + '┴─' + '─'.repeat(15) + '┴─' + '─'.repeat(10) + '┴─' + '─'.repeat(12) + '┘');

console.log('\\n' + '='.repeat(100));
console.log('                                ANALYSIS SUMMARY');
console.log('='.repeat(100));

const challenging = results.filter(r => r.type === 'Challenging');
const favorable = results.filter(r => r.type === 'Favorable');

const challengingAvgDiff = challenging.reduce((sum, r) => sum + r.diff, 0) / challenging.length;
const favorableAvgDiff = favorable.reduce((sum, r) => sum + r.diff, 0) / favorable.length;

console.log();
console.log('CHALLENGING PATTERNS (Expected to be harder for approximation):');
console.log(`   • Average accuracy difference: ${challengingAvgDiff.toFixed(2)}%`);
console.log(`   • Range: ${Math.min(...challenging.map(r => r.diff)).toFixed(2)}% - ${Math.max(...challenging.map(r => r.diff)).toFixed(2)}%`);
console.log(`   • Patterns tested: ${challenging.length}/9`);

console.log();
console.log('FAVORABLE PATTERNS (Expected to be easier for approximation):');
console.log(`   • Average accuracy difference: ${favorableAvgDiff.toFixed(2)}%`);
console.log(`   • Range: ${Math.min(...favorable.map(r => r.diff)).toFixed(2)}% - ${Math.max(...favorable.map(r => r.diff)).toFixed(2)}%`);
console.log(`   • Patterns tested: ${favorable.length}/7`);

console.log();
console.log('KEY OBSERVATIONS:');
console.log('   • Approximation approach generates multiple results per pattern (1-16 results)');
console.log('   • Fetching client side typically generates only 1 result per pattern');
console.log('   • Some patterns show perfect accuracy (0.00% difference)');
console.log('   • Exponential growth shows extremely high difference (4603.33%)');
console.log('   • Best approximation accuracy: gradual_trend (0.00%)');
console.log('   • Worst approximation accuracy: exponential_growth (4603.33%)');

console.log();
console.log(' EXPERIMENT NOTES:');
console.log('   • Fetching client side may have terminated early (only 1 result per pattern)');
console.log('   • Some favorable patterns missing fetching client side data');
console.log('   • Consider extending experiment duration for better comparison');

console.log();
console.log('='.repeat(100));
