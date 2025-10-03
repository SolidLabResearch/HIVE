#!/usr/bin/env node

/**
 * 4Hz Frequency Accuracy Analysis: Approximation vs Ground Truth (Fetching Client Side)
 * Shows approximation accuracy as percentages relative to ground truth for different noise patterns at 4Hz
 */

const fs = require('fs');
const path = require('path');

const NOISE_LEVELS = [0.1, 0.2, 0.5, 1.0, 2.0];
const FREQUENCY = '4Hz';

function extractApproximationResults(logDir) {
    const results = [];

    for (const noise of NOISE_LEVELS) {
        const dirPath = path.join(logDir, 'approximation-approach', '4Hz', `noise_${noise}`);
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
                        noise,
                        values,
                        finalValue: values[values.length - 1],
                        count: values.length
                    });
                }
            } catch (error) {
                console.error(`Error reading approximation file for noise ${noise}: ${error.message}`);
            }
        }
    }

    return results;
}

function extractFetchingResults(logDir) {
    const results = [];

    for (const noise of NOISE_LEVELS) {
        const dirPath = path.join(logDir, 'fetching-client-side', '4Hz', `noise_${noise}`);
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
                        noise,
                        values,
                        finalValue: values[values.length - 1],
                        count: values.length
                    });
                }
            } catch (error) {
                console.error(`Error reading fetching file for noise ${noise}: ${error.message}`);
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
    console.log('4Hz FREQUENCY ACCURACY ANALYSIS');
    console.log('Ground Truth: Fetching Client Side Approach');
    console.log('Frequency: 4Hz');
    console.log('='.repeat(80));

    const logDir = './experiments/results/frequency-experiments';

    console.log('\nExtracting Results...');
    const approximationResults = extractApproximationResults(logDir);
    const fetchingResults = extractFetchingResults(logDir);

    console.log(`Found ${approximationResults.length} approximation results`);
    console.log(`Found ${fetchingResults.length} fetching (ground truth) results`);

    const accuracyAnalysis = [];

    console.log('\nACCURACY COMPARISON BY NOISE LEVEL (4Hz)');
    console.log('='.repeat(80));
    console.log('Noise     | Ground Truth    | Approximation   | Accuracy');
    console.log('-'.repeat(60));

    for (const noise of NOISE_LEVELS) {
        const approxResult = approximationResults.find(r => r.noise === noise);
        const fetchingResult = fetchingResults.find(r => r.noise === noise);

        if (approxResult && fetchingResult) {
            const accuracy = calculateAccuracy(approxResult.finalValue, fetchingResult.finalValue);
            const errorPercent = Math.abs((approxResult.finalValue - fetchingResult.finalValue) / fetchingResult.finalValue) * 100;

            accuracyAnalysis.push({
                noise,
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

            const noiseStr = noise.toString().padEnd(8);
            const gtStr = groundTruthStr.padEnd(15);
            const appStr = approximationStr.padEnd(15);
            const accStr = `${accuracy.toFixed(2)}%`.padEnd(8);

            console.log(`${noiseStr} | ${gtStr} | ${appStr} | ${accStr}`);

        } else {
            const noiseStr = noise.toString().padEnd(8);
            const missingData = !approxResult ? 'Missing Approx' : 'Missing Ground Truth';
            console.log(`${noiseStr} | ${missingData.padEnd(47)} | N/A`);
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
        console.log(`   • Tests Completed: ${accuracyAnalysis.length}/${NOISE_LEVELS.length}`);

        // Best and worst performers
        const bestResult = accuracyAnalysis.reduce((best, current) =>
            current.accuracy > best.accuracy ? current : best);
        const worstResult = accuracyAnalysis.reduce((worst, current) =>
            current.accuracy < worst.accuracy ? current : worst);

        console.log(`\n PERFORMANCE HIGHLIGHTS:`);
        console.log(`   • Best: Noise ${bestResult.noise} (${bestResult.accuracy.toFixed(2)}% accuracy)`);
        console.log(`   • Worst: Noise ${worstResult.noise} (${worstResult.accuracy.toFixed(2)}% accuracy)`);

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

        // Noise level analysis
        console.log(`\nACCURACY BY NOISE LEVEL:`);
        for (const noise of NOISE_LEVELS) {
            const noiseResults = accuracyAnalysis.filter(r => r.noise === noise);
            if (noiseResults.length > 0) {
                const accuracy = noiseResults[0].accuracy;
                console.log(`   • Noise ${noise}: ${accuracy.toFixed(2)}% accuracy`);
            }
        }

    } else {
        console.log(`\nNo comparable results found.`);
        console.log(`   Please ensure both approximation and fetching experiments have been completed for 4Hz.`);
    }

    // Generate CSV export
    console.log('\nEXPORTING RESULTS...');

    const csvContent = [
        'Noise Level,Ground Truth (Fetching),Approximation,Accuracy (%),Error (%),Approx Results Count,Fetching Results Count'
    ];

    accuracyAnalysis.forEach(result => {
        csvContent.push([
            result.noise,
            result.groundTruth,
            result.approximation,
            result.accuracy.toFixed(2),
            result.errorPercent.toFixed(2),
            result.approxCount,
            result.fetchingCount
        ].join(','));
    });

    fs.writeFileSync('./experiments/results/4hz_accuracy_analysis.csv', csvContent.join('\n'));

    // Export detailed JSON
    const exportData = {
        timestamp: new Date().toISOString(),
        frequency: FREQUENCY,
        summary: {
            totalTests: accuracyAnalysis.length,
            overallAccuracy: accuracyAnalysis.length > 0 ?
                accuracyAnalysis.reduce((sum, r) => sum + r.accuracy, 0) / accuracyAnalysis.length : 0,
            overallError: accuracyAnalysis.length > 0 ?
                accuracyAnalysis.reduce((sum, r) => sum + r.errorPercent, 0) / accuracyAnalysis.length : 0
        },
        detailedResults: accuracyAnalysis,
        noiseLevelSummary: NOISE_LEVELS.map(noise => {
            const noiseResults = accuracyAnalysis.filter(r => r.noise === noise);
            return {
                noise,
                completed: noiseResults.length > 0,
                accuracy: noiseResults.length > 0 ? noiseResults[0].accuracy : null
            };
        })
    };

    fs.writeFileSync('./experiments/results/4hz_accuracy_analysis.json', JSON.stringify(exportData, null, 2));

    console.log(`CSV exported to: ./experiments/results/4hz_accuracy_analysis.csv`);
    console.log(`JSON exported to: ./experiments/results/4hz_accuracy_analysis.json`);

    console.log('\n' + '='.repeat(80));
    console.log('4Hz ACCURACY ANALYSIS COMPLETE');
    console.log('='.repeat(80));
}

main();
