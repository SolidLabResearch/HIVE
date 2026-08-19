#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

/**
 * Simple data pattern analyzer for exponential patterns
 * Analyzes the generated data to understand the rate of change
 */

function analyzePattern(filePath, patternName) {
    try {
        const data = fs.readFileSync(filePath, 'utf8');
        const lines = data.split('\n').filter(line => line.trim());
        
        console.log(`\nAnalyzing ${patternName}:`);
        console.log(`   Data file: ${filePath}`);
        console.log(`   Total lines: ${lines.length}`);
        
        const values = [];
        const timestamps = [];
        
        lines.forEach(line => {
            // Extract value from N-Triples format
            const valueMatch = line.match(/"([^"]+)"\^\^<http:\/\/www\.w3\.org\/2001\/XMLSchema#float>/);
            const timestampMatch = line.match(/"([^"]+)"\^\^<http:\/\/www\.w3\.org\/2001\/XMLSchema#dateTime>/);
            
            if (valueMatch && timestampMatch) {
                values.push(parseFloat(valueMatch[1]));
                timestamps.push(new Date(timestampMatch[1]));
            }
        });
        
        if (values.length > 0) {
            const startValue = values[0];
            const endValue = values[values.length - 1];
            const minValue = Math.min(...values);
            const maxValue = Math.max(...values);
            const change = endValue - startValue;
            const percentChange = (change / startValue) * 100;
            
            const startTime = timestamps[0];
            const endTime = timestamps[timestamps.length - 1];
            const durationMs = endTime - startTime;
            const durationS = durationMs / 1000;
            
            console.log(`   Value Analysis:`);
            console.log(`      Start: ${startValue.toFixed(6)}`);
            console.log(`      End: ${endValue.toFixed(6)}`);
            console.log(`      Min: ${minValue.toFixed(6)}`);
            console.log(`      Max: ${maxValue.toFixed(6)}`);
            console.log(`      Change: ${change.toFixed(6)} (${percentChange.toFixed(2)}%)`);
            
            console.log(`   ⏱️  Time Analysis:`);
            console.log(`      Duration: ${durationMs}ms (${durationS.toFixed(2)}s)`);
            console.log(`      Data points: ${values.length}`);
            console.log(`      Avg interval: ${(durationMs / (values.length - 1)).toFixed(2)}ms`);
            
            // Calculate rate of change over different windows
            const windowSizes = [10, 50, 100];
            console.log(`   Rate of Change Analysis:`);
            
            windowSizes.forEach(windowSize => {
                if (values.length >= windowSize) {
                    const windowStart = values[0];
                    const windowEnd = values[windowSize - 1];
                    const windowChange = windowEnd - windowStart;
                    const windowPercent = (windowChange / windowStart) * 100;
                    
                    console.log(`      First ${windowSize} points: ${windowChange.toFixed(6)} (${windowPercent.toFixed(2)}%)`);
                }
            });
            
            // Show sample values
            console.log(`   Sample Values:`);
            const sampleIndices = [0, Math.floor(values.length * 0.25), Math.floor(values.length * 0.5), 
                                   Math.floor(values.length * 0.75), values.length - 1];
            sampleIndices.forEach((idx, i) => {
                const positions = ['Start', '25%', '50%', '75%', 'End'];
                console.log(`      ${positions[i]}: ${values[idx].toFixed(6)}`);
            });
            
            return {
                pattern: patternName,
                startValue,
                endValue,
                change,
                percentChange,
                duration: durationS,
                dataPoints: values.length,
                values: values.slice(0, 10) // First 10 values for comparison
            };
        }
    } catch (error) {
        console.error(`Error analyzing ${patternName}: ${error.message}`);
        return null;
    }
}

function main() {
    console.log('EXPONENTIAL PATTERN DATA ANALYSIS');
    console.log('=' * 50);
    
    const basePath = '/Users/kushbisen/Code/streaming-query-hive/src/streamer/data/approximation_test/challenging';
    const patterns = [
        'exponential_growth',
        'exponential_decay',
        'extreme_exponential_growth', 
        'extreme_exponential_decay'
    ];
    
    const results = [];
    
    patterns.forEach(pattern => {
        const smartphonePath = path.join(basePath, pattern, 'smartphone.acceleration.x', 'data.nt');
        const result = analyzePattern(smartphonePath, pattern);
        if (result) results.push(result);
    });
    
    // Summary comparison
    console.log('\nSUMMARY COMPARISON');
    console.log('=' * 50);
    
    results.forEach(result => {
        console.log(`${result.pattern}:`);
        console.log(`  Change: ${result.change.toFixed(6)} (${result.percentChange.toFixed(2)}%)`);
        console.log(`  Duration: ${result.duration.toFixed(2)}s`);
        console.log(`  Rate: ${(result.change / result.duration).toFixed(6)} units/second`);
    });
    
    console.log('\nExpected Impact on Approximation Accuracy:');
    results.forEach(result => {
        let impact = 'Unknown';
        if (Math.abs(result.percentChange) < 5) {
            impact = '🟢 Low impact (plateau effect likely)';
        } else if (Math.abs(result.percentChange) < 20) {
            impact = '🟡 Medium impact (some accuracy loss)';
        } else {
            impact = '🔴 High impact (significant accuracy loss expected)';
        }
        
        console.log(`  ${result.pattern}: ${impact}`);
    });
}

main();
