#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

/**
 * Targeted test script for exponential patterns only
 * Compares Approximation Approach vs Fetching Client Side for exponential data
 */

const patterns = [
    'exponential_growth',
    'exponential_decay', 
    'extreme_exponential_growth',
    'extreme_exponential_decay'
];

const approaches = {
    'approximation': 'experiment-evaluation-approximation-patterns.js',
    'fetching': 'experiment-evaluation-fetching-client-side-patterns.js'
};

console.log('🧪 Testing Exponential Patterns: Approximation vs Fetching Client Side');
console.log('=' * 60);

async function runTest(approach, approachName, pattern) {
    console.log(`\nRunning ${approachName} approach for ${pattern}...`);
    
    try {
        const startTime = Date.now();
        const command = `node ${approach} ${pattern}`;
        console.log(`Command: ${command}`);
        
        const result = execSync(command, { 
            cwd: '/Users/kushbisen/Code/streaming-query-hive',
            encoding: 'utf8',
            timeout: 60000  // 60 second timeout
        });
        
        const duration = Date.now() - startTime;
        console.log(`${approachName} completed for ${pattern} in ${duration}ms`);
        return { success: true, duration, pattern, approach: approachName };
        
    } catch (error) {
        console.log(`${approachName} failed for ${pattern}: ${error.message}`);
        return { success: false, error: error.message, pattern, approach: approachName };
    }
}

async function runAllTests() {
    const results = [];
    
    for (const pattern of patterns) {
        console.log(`\n🔬 Testing Pattern: ${pattern}`);
        console.log('-'.repeat(40));
        
        // Test both approaches for this pattern
        const approximationResult = await runTest(
            approaches.approximation, 
            'Approximation', 
            pattern
        );
        results.push(approximationResult);
        
        // Small delay between tests
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        const fetchingResult = await runTest(
            approaches.fetching, 
            'Fetching Client Side', 
            pattern
        );
        results.push(fetchingResult);
        
        // Delay between patterns
        await new Promise(resolve => setTimeout(resolve, 3000));
    }
    
    return results;
}

function generateSummary(results) {
    console.log('\nTEST SUMMARY');
    console.log('=' * 50);
    
    const byPattern = {};
    results.forEach(result => {
        if (!byPattern[result.pattern]) {
            byPattern[result.pattern] = {};
        }
        byPattern[result.pattern][result.approach] = result;
    });
    
    for (const pattern of patterns) {
        console.log(`\n🧮 ${pattern}:`);
        const patternResults = byPattern[pattern] || {};
        
        const approx = patternResults['Approximation'];
        const fetching = patternResults['Fetching Client Side'];
        
        if (approx) {
            console.log(`  Approximation: ${approx.success ? 'Success' : 'Failed'} ${approx.duration ? `(${approx.duration}ms)` : ''}`);
        }
        
        if (fetching) {
            console.log(`  Fetching Client Side: ${fetching.success ? 'Success' : 'Failed'} ${fetching.duration ? `(${fetching.duration}ms)` : ''}`);
        }
        
        if (approx?.success && fetching?.success) {
            const speedDiff = approx.duration - fetching.duration;
            if (speedDiff > 0) {
                console.log(`  Fetching was ${speedDiff}ms faster`);
            } else {
                console.log(`  Approximation was ${Math.abs(speedDiff)}ms faster`);
            }
        }
    }
    
    // Generate logs analysis
    console.log('\n📁 Log files for analysis:');
    patterns.forEach(pattern => {
        console.log(`\nPattern: ${pattern}`);
        console.log(`  Approximation logs: logs/approximation-patterns/challenging_${pattern}/`);
        console.log(`  Fetching logs: logs/fetching-client-side-patterns/challenging_${pattern}/`);
    });
    
    console.log('\nNext steps:');
    console.log('1. Run compare-first-vs-fetching.js to analyze accuracy differences');
    console.log('2. Check log files for value progressions and timing details');
    console.log('3. Look for patterns where extreme exponentials break the "plateau effect"');
}

// Main execution
runAllTests()
    .then(results => {
        generateSummary(results);
        console.log('\nExponential pattern testing complete!');
    })
    .catch(error => {
        console.error('💥 Test execution failed:', error);
        process.exit(1);
    });
