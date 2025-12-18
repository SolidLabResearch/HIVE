#!/usr/bin/env node

/**
 * Accuracy Analysis: Approximation vs Ground Truth (Fetching Client Side)
 * Shows approximation accuracy as percentages relative to ground truth
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
                    
                    // Look for Final aggregation results containing unifiedResult or unifiedAverage
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
                            pattern,
                            rate,
                            values,
                            finalValue: values[values.length - 1],
                            count: values.length
                        });
                    }
                } catch (error) {
                    console.error(`Error reading approximation file: ${error.message}`);
                }
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
                    const lines = content.split('\n');
                    const values = [];
                    
                    // Look for "Successfully published result" values
                    for (const line of lines) {
                        if (line.includes('Successfully published result:')) {
                            const match = line.match(/Successfully published result:\s*([\d.]+)/);
                            if (match) {
                                values.push(parseFloat(match[1]));
                            }
                        }
                    }
                    
                    if (values.length > 0) {
                        results.push({
                            pattern,
                            rate,
                            values,
                            finalValue: values[values.length - 1],
                            count: values.length
                        });
                    }
                } catch (error) {
                    console.error(`Error reading fetching file: ${error.message}`);
                }
            }
        }
    }
    
    return results;
}

function calculateAccuracy(approximationValue, groundTruthValue) {
    if (groundTruthValue === 0) {
        return approximationValue === 0 ? 100 : 0;
    }
    
    const errorPercent = Math.abs((approximationValue - groundTruthValue) / groundTruthValue) * 100;
    const accuracy = Math.max(0, 100 - errorPercent);
    return accuracy;
}

function main() {
    console.log('APPROXIMATION ACCURACY ANALYSIS');
    console.log('Ground Truth: Fetching Client Side Approach');
    console.log('='.repeat(80));
    
    const approximationLogDir = './logs/rate-comparison-approximation';
    const fetchingLogDir = './logs/rate-comparison-fetching';
    
    console.log('\nExtracting Results...');
    const approximationResults = extractApproximationResults(approximationLogDir);
    const fetchingResults = extractFetchingResults(fetchingLogDir);
    
    console.log(`Found ${approximationResults.length} approximation results`);
    console.log(`Found ${fetchingResults.length} fetching (ground truth) results`);
    
    const accuracyAnalysis = [];
    
    console.log('\nACCURACY COMPARISON BY PATTERN AND RATE');
    console.log('='.repeat(80));
    
    for (const pattern of PATTERNS) {
        console.log(`\n🔬 ${pattern.toUpperCase().replace('_', ' ')} PATTERN:`);
        console.log('-'.repeat(60));
        console.log('Rate      | Ground Truth    | Approximation   | Accuracy');
        console.log('-'.repeat(60));
        
        for (const rate of RATES) {
            const approxResult = approximationResults.find(r => r.pattern === pattern && r.rate === rate);
            const fetchingResult = fetchingResults.find(r => r.pattern === pattern && r.rate === rate);
            
            if (approxResult && fetchingResult) {
                const accuracy = calculateAccuracy(approxResult.finalValue, fetchingResult.finalValue);
                const errorPercent = Math.abs((approxResult.finalValue - fetchingResult.finalValue) / fetchingResult.finalValue) * 100;
                
                accuracyAnalysis.push({
                    pattern,
                    rate,
                    groundTruth: fetchingResult.finalValue,
                    approximation: approxResult.finalValue,
                    accuracy: accuracy,
                    errorPercent: errorPercent,
                    approxCount: approxResult.count,
                    fetchingCount: fetchingResult.count
                });
                
                // Format numbers for display
                const groundTruthStr = fetchingResult.finalValue < 1000 ? 
                    fetchingResult.finalValue.toFixed(6) : 
                    fetchingResult.finalValue.toExponential(3);
                
                const approximationStr = approxResult.finalValue < 1000 ? 
                    approxResult.finalValue.toFixed(6) : 
                    approxResult.finalValue.toExponential(3);
                
                const rateStr = rate.toString().padEnd(8);
                const gtStr = groundTruthStr.padEnd(15);
                const appStr = approximationStr.padEnd(15);
                const accStr = `${accuracy.toFixed(2)}%`.padEnd(8);
                
                console.log(`${rateStr} | ${gtStr} | ${appStr} | ${accStr}`);
                
            } else {
                const rateStr = rate.toString().padEnd(8);
                const missingData = !approxResult ? 'Missing Approx' : 'Missing Ground Truth';
                console.log(`${rateStr} | ${missingData.padEnd(47)} | N/A`);
            }
        }
    }
    
    // Summary Statistics
    console.log('\nSUMMARY STATISTICS');
    console.log('='.repeat(80));
    
    if (accuracyAnalysis.length > 0) {
        // Overall statistics
        const overallAccuracy = accuracyAnalysis.reduce((sum, r) => sum + r.accuracy, 0) / accuracyAnalysis.length;
        const overallError = accuracyAnalysis.reduce((sum, r) => sum + r.errorPercent, 0) / accuracyAnalysis.length;
        
        console.log(`\nOVERALL PERFORMANCE:`);
        console.log(`   • Average Accuracy: ${overallAccuracy.toFixed(2)}%`);
        console.log(`   • Average Error: ${overallError.toFixed(2)}%`);
        console.log(`   • Tests Completed: ${accuracyAnalysis.length}/${RATES.length * PATTERNS.length}`);
        
        // Pattern-based analysis
        for (const pattern of PATTERNS) {
            const patternResults = accuracyAnalysis.filter(r => r.pattern === pattern);
            if (patternResults.length > 0) {
                const avgAccuracy = patternResults.reduce((sum, r) => sum + r.accuracy, 0) / patternResults.length;
                const avgError = patternResults.reduce((sum, r) => sum + r.errorPercent, 0) / patternResults.length;
                const bestAccuracy = Math.max(...patternResults.map(r => r.accuracy));
                const worstAccuracy = Math.min(...patternResults.map(r => r.accuracy));
                
                console.log(`\n${pattern.toUpperCase().replace('_', ' ')} PATTERN:`);
                console.log(`   • Average Accuracy: ${avgAccuracy.toFixed(2)}%`);
                console.log(`   • Average Error: ${avgError.toFixed(2)}%`);
                console.log(`   • Best Accuracy: ${bestAccuracy.toFixed(2)}%`);
                console.log(`   • Worst Accuracy: ${worstAccuracy.toFixed(2)}%`);
                console.log(`   • Tests: ${patternResults.length}/${RATES.length}`);
            }
        }
        
        // Rate-based analysis
        console.log(`\nACCURACY BY RATE:`);
        for (const rate of RATES) {
            const rateResults = accuracyAnalysis.filter(r => r.rate === rate);
            if (rateResults.length > 0) {
                const avgAccuracy = rateResults.reduce((sum, r) => sum + r.accuracy, 0) / rateResults.length;
                console.log(`   • Rate ${rate}: ${avgAccuracy.toFixed(2)}% accuracy (${rateResults.length}/${PATTERNS.length} patterns)`);
            }
        }
        
        // Best and worst performers
        const bestResult = accuracyAnalysis.reduce((best, current) => 
            current.accuracy > best.accuracy ? current : best);
        const worstResult = accuracyAnalysis.reduce((worst, current) => 
            current.accuracy < worst.accuracy ? current : worst);
        
        console.log(`\n🏆 PERFORMANCE HIGHLIGHTS:`);
        console.log(`   • Best: ${bestResult.pattern} rate ${bestResult.rate} (${bestResult.accuracy.toFixed(2)}% accuracy)`);
        console.log(`   • Worst: ${worstResult.pattern} rate ${worstResult.rate} (${worstResult.accuracy.toFixed(2)}% accuracy)`);
        
        // Accuracy categories
        const excellent = accuracyAnalysis.filter(r => r.accuracy >= 95).length;
        const good = accuracyAnalysis.filter(r => r.accuracy >= 90 && r.accuracy < 95).length;
        const acceptable = accuracyAnalysis.filter(r => r.accuracy >= 80 && r.accuracy < 90).length;
        const poor = accuracyAnalysis.filter(r => r.accuracy < 80).length;
        
        console.log(`\nACCURACY DISTRIBUTION:`);
        console.log(`   • Excellent (≥95%): ${excellent} tests`);
        console.log(`   • Good (90-95%): ${good} tests`);
        console.log(`   • Acceptable (80-90%): ${acceptable} tests`);
        console.log(`   • Poor (<80%): ${poor} tests`);
        
    } else {
        console.log(`\nNo comparable results found.`);
        console.log(`   Please ensure both approximation and fetching experiments have been completed.`);
    }
    
    // Generate CSV export
    console.log('\nEXPORTING RESULTS...');
    
    const csvContent = [
        'Pattern,Rate,Ground Truth (Fetching),Approximation,Accuracy (%),Error (%),Approx Results Count,Fetching Results Count'
    ];
    
    accuracyAnalysis.forEach(result => {
        csvContent.push([
            result.pattern.replace('_', ' '),
            result.rate,
            result.groundTruth,
            result.approximation,
            result.accuracy.toFixed(2),
            result.errorPercent.toFixed(2),
            result.approxCount,
            result.fetchingCount
        ].join(','));
    });
    
    fs.writeFileSync('./logs/approximation_accuracy_analysis.csv', csvContent.join('\n'));
    
    // Export detailed JSON
    const exportData = {
        timestamp: new Date().toISOString(),
        summary: {
            totalTests: accuracyAnalysis.length,
            overallAccuracy: accuracyAnalysis.length > 0 ? 
                accuracyAnalysis.reduce((sum, r) => sum + r.accuracy, 0) / accuracyAnalysis.length : 0,
            overallError: accuracyAnalysis.length > 0 ? 
                accuracyAnalysis.reduce((sum, r) => sum + r.errorPercent, 0) / accuracyAnalysis.length : 0
        },
        detailedResults: accuracyAnalysis,
        patternSummary: PATTERNS.map(pattern => {
            const patternResults = accuracyAnalysis.filter(r => r.pattern === pattern);
            return {
                pattern,
                count: patternResults.length,
                averageAccuracy: patternResults.length > 0 ? 
                    patternResults.reduce((sum, r) => sum + r.accuracy, 0) / patternResults.length : 0
            };
        })
    };
    
    fs.writeFileSync('./logs/approximation_accuracy_analysis.json', JSON.stringify(exportData, null, 2));
    
    console.log(`CSV exported to: ./logs/approximation_accuracy_analysis.csv`);
    console.log(`JSON exported to: ./logs/approximation_accuracy_analysis.json`);
    
    console.log('\n' + '='.repeat(80));
    console.log('ANALYSIS COMPLETE');
    console.log('='.repeat(80));
}

main();
