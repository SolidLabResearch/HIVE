#!/usr/bin/env node

/**
 * Extract and compare results from approximation vs fetching client side approaches
 * - Approximation: Extract "unifiedAverage" values from JSON format
 * - Fetching: Extract "Successfully published result" values from CSV logs
 */

const fs = require('fs');
const path = require('path');

const RATES = [0.001, 0.01, 0.1, 1, 10, 100];
const PATTERNS = ['exponential_growth', 'exponential_decay'];

function extractApproximationResults(logDir) {
    const results = [];
    
    for (const pattern of PATTERNS) {
        for (const rate of RATES) {
            const dirPath = path.join(logDir, `${pattern}_rate_${rate}`, 'iteration1');
            const logFile = path.join(dirPath, 'approximation_approach_log.csv');
            
            if (fs.existsSync(logFile)) {
                try {
                    const content = fs.readFileSync(logFile, 'utf8');
                    const lines = content.split('\n');
                    const values = [];
                    
                    // Look for Final aggregation results containing unifiedResult or unifiedAverage (same as compare-first-vs-fetching.js)
                    for (const line of lines) {
                        if (line.includes('Final aggregation results')) {
                            // Handle escaped JSON in CSV - look for \"unifiedResult\": first, then \"unifiedAverage\":
                            let match = line.match(/\\"unifiedResult\\":([\d.]+)/);
                            if (match) {
                                values.push(parseFloat(match[1]));
                                continue;
                            }
                            // Fallback to unifiedAverage for backward compatibility
                            match = line.match(/\\"unifiedAverage\\":([\d.]+)/);
                            if (match) {
                                values.push(parseFloat(match[1]));
                            }
                        }
                    }
                    
                    // Fallback: look for "Successfully published unified cross-sensor" line
                    if (values.length === 0) {
                        for (const line of lines) {
                            if (line.includes('Successfully published unified cross-sensor')) {
                                const match = line.match(/Successfully published unified cross-sensor [\w\s]+:\s*([\d.]+)/);
                                if (match) {
                                    values.push(parseFloat(match[1]));
                                }
                            }
                        }
                    }
                    
                    if (values.length > 0) {
                        
                        results.push({
                            approach: 'approximation',
                            pattern,
                            rate,
                            values,
                            lastValue: values[values.length - 1] || null,
                            count: values.length
                        });
                        
                        console.log(`Approximation ${pattern} rate ${rate}: Found ${values.length} results, last = ${values[values.length - 1]}`);
                    } else {
                        console.log(` No Final aggregation results or unified cross-sensor results found in ${logFile}`);
                    }
                } catch (error) {
                    console.error(`Error reading ${logFile}:`, error.message);
                }
            } else {
                console.log(` File not found: ${logFile}`);
            }
        }
    }
    
    return results;
}

function extractFetchingResults(logDir) {
    const results = [];
    
    for (const pattern of PATTERNS) {
        for (const rate of RATES) {
            const dirPath = path.join(logDir, `${pattern}_rate_${rate}`, 'iteration1');
            const logFile = path.join(dirPath, 'fetching_client_side_log.csv');
            
            if (fs.existsSync(logFile)) {
                try {
                    const content = fs.readFileSync(logFile, 'utf8');
                    
                    // Look for "Successfully published result" values
                    const resultMatches = content.match(/"Successfully published result: ([0-9.]+)"/g);
                    
                    if (resultMatches) {
                        const values = resultMatches.map(match => {
                            const value = match.match(/([0-9.]+)/)[1];
                            return parseFloat(value);
                        });
                        
                        results.push({
                            approach: 'fetching',
                            pattern,
                            rate,
                            values,
                            lastValue: values[values.length - 1] || null,
                            count: values.length
                        });
                        
                        console.log(`Fetching ${pattern} rate ${rate}: Found ${values.length} results, last = ${values[values.length - 1]}`);
                    } else {
                        console.log(` No published results found in ${logFile}`);
                    }
                } catch (error) {
                    console.error(`Error reading ${logFile}:`, error.message);
                }
            } else {
                console.log(` File not found: ${logFile}`);
            }
        }
    }
    
    return results;
}

function calculateExpectedValue(pattern, rate, timepoint = 120) {
    // Calculate expected value at end of 120-second experiment
    // Base value is 1.0, timepoint normalized to [0,1] range
    const normalizedTime = timepoint / 120.0;
    
    if (pattern === 'exponential_growth') {
        return Math.exp(rate * normalizedTime);
    } else if (pattern === 'exponential_decay') {
        return Math.exp(-rate * normalizedTime);
    }
    
    return null;
}

function compareResults(approximationResults, fetchingResults) {
    console.log('\nACCURACY COMPARISON ANALYSIS');
    console.log('='.repeat(80));
    
    const comparison = [];
    
    for (const pattern of PATTERNS) {
        console.log(`\n ${pattern.toUpperCase()} PATTERN:`);
        console.log('-'.repeat(50));
        
        for (const rate of RATES) {
            const approxResult = approximationResults.find(r => r.pattern === pattern && r.rate === rate);
            const fetchingResult = fetchingResults.find(r => r.pattern === pattern && r.rate === rate);
            const expectedValue = calculateExpectedValue(pattern, rate);
            
            console.log(`\nRate ${rate}:`);
            console.log(`   Expected value: ${expectedValue ? expectedValue.toFixed(6) : 'N/A'}`);
            
            if (approxResult) {
                const approxError = expectedValue ? Math.abs(approxResult.lastValue - expectedValue) / expectedValue * 100 : null;
                console.log(`   Approximation: ${approxResult.lastValue?.toFixed(6) || 'N/A'} (${approxResult.count} results)`);
                console.log(`   Approx Error: ${approxError ? approxError.toFixed(2) + '%' : 'N/A'}`);
            } else {
                console.log(`   Approximation: No results found`);
            }
            
            if (fetchingResult) {
                const fetchingError = expectedValue ? Math.abs(fetchingResult.lastValue - expectedValue) / expectedValue * 100 : null;
                console.log(`   Fetching: ${fetchingResult.lastValue?.toExponential(3) || 'N/A'} (${fetchingResult.count} results)`);
                console.log(`   Fetching Error: ${fetchingError ? fetchingError.toFixed(2) + '%' : 'N/A'}`);
            } else {
                console.log(`   Fetching: No results found`);
            }
            
            if (approxResult && fetchingResult && expectedValue) {
                const approxError = Math.abs(approxResult.lastValue - expectedValue) / expectedValue * 100;
                const fetchingError = Math.abs(fetchingResult.lastValue - expectedValue) / expectedValue * 100;
                const winner = approxError < fetchingError ? 'Approximation' : 'Fetching';
                
                console.log(`    More Accurate: ${winner}`);
                
                comparison.push({
                    pattern,
                    rate,
                    expected: expectedValue,
                    approximation: approxResult.lastValue,
                    fetching: fetchingResult.lastValue,
                    approxError,
                    fetchingError,
                    winner
                });
            }
        }
    }
    
    return comparison;
}

function generateSummary(comparison) {
    console.log('\nSUMMARY ANALYSIS');
    console.log('='.repeat(80));
    
    const approxWins = comparison.filter(c => c.winner === 'Approximation').length;
    const fetchingWins = comparison.filter(c => c.winner === 'Fetching').length;
    
    console.log(`\nOverall Accuracy Comparison:`);
    console.log(`   Approximation wins: ${approxWins}/${comparison.length} tests`);
    console.log(`   Fetching wins: ${fetchingWins}/${comparison.length} tests`);
    
    // Group by rate
    const rateAnalysis = {};
    for (const result of comparison) {
        if (!rateAnalysis[result.rate]) {
            rateAnalysis[result.rate] = { approx: 0, fetching: 0 };
        }
        if (result.winner === 'Approximation') rateAnalysis[result.rate].approx++;
        else rateAnalysis[result.rate].fetching++;
    }
    
    console.log(`\nRate-based Analysis:`);
    for (const rate of RATES) {
        if (rateAnalysis[rate]) {
            const total = rateAnalysis[rate].approx + rateAnalysis[rate].fetching;
            console.log(`   Rate ${rate}: Approx ${rateAnalysis[rate].approx}/${total}, Fetching ${rateAnalysis[rate].fetching}/${total}`);
        }
    }
    
    // Group by pattern
    const patternAnalysis = {};
    for (const result of comparison) {
        if (!patternAnalysis[result.pattern]) {
            patternAnalysis[result.pattern] = { approx: 0, fetching: 0 };
        }
        if (result.winner === 'Approximation') patternAnalysis[result.pattern].approx++;
        else patternAnalysis[result.pattern].fetching++;
    }
    
    console.log(`\nPattern-based Analysis:`);
    for (const pattern of PATTERNS) {
        if (patternAnalysis[pattern]) {
            const total = patternAnalysis[pattern].approx + patternAnalysis[pattern].fetching;
            console.log(`   ${pattern}: Approx ${patternAnalysis[pattern].approx}/${total}, Fetching ${patternAnalysis[pattern].fetching}/${total}`);
        }
    }
}

function main() {
    console.log('EXTRACTING RESULTS FROM EXPERIMENTS');
    console.log('='.repeat(80));
    
    const approximationLogDir = './logs/rate-comparison-approximation';
    const fetchingLogDir = './logs/rate-comparison-fetching';
    
    console.log('\nExtracting Approximation Results...');
    const approximationResults = extractApproximationResults(approximationLogDir);
    
    console.log('\nExtracting Fetching Results...');
    const fetchingResults = extractFetchingResults(fetchingLogDir);
    
    console.log('\nRunning Comparison Analysis...');
    const comparison = compareResults(approximationResults, fetchingResults);
    
    if (comparison.length > 0) {
        generateSummary(comparison);
        
        // Save detailed results
        const detailedResults = {
            timestamp: new Date().toISOString(),
            comparison,
            approximationResults,
            fetchingResults
        };
        
        fs.writeFileSync('./logs/accuracy_comparison_results.json', JSON.stringify(detailedResults, null, 2));
        console.log('\nDetailed results saved to: ./logs/accuracy_comparison_results.json');
    } else {
        console.log('\nNo comparable results found. Please ensure both approximation and fetching experiments have been run.');
    }
}

main();
