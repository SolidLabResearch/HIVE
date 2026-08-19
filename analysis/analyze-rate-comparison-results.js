#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

/**
 * Analyze accuracy results from rate comparison experiments
 */

class RateComparisonAccuracyAnalyzer {
    constructor() {
        this.logDir = './logs/rate_comparison';
        this.rates = [0.001, 0.01, 0.1, 1, 10, 100];
        this.patterns = ['exponential_growth', 'exponential_decay'];
        this.approaches = ['approximation', 'fetching_client_side'];
    }

    parseLogFile(filePath) {
        if (!fs.existsSync(filePath)) {
            return null;
        }

        const content = fs.readFileSync(filePath, 'utf8');
        const results = {
            accuracy: null,
            windowResults: [],
            executionTime: null,
            memoryUsage: null
        };

        // Extract accuracy percentage
        const accuracyMatch = content.match(/Final Accuracy: ([\d.]+)%/);
        if (accuracyMatch) {
            results.accuracy = parseFloat(accuracyMatch[1]);
        }

        // Extract window-by-window results
        const windowMatches = content.matchAll(/Window \d+.*?Accuracy: ([\d.]+)%/g);
        for (const match of windowMatches) {
            results.windowResults.push(parseFloat(match[1]));
        }

        // Extract execution time
        const timeMatch = content.match(/Total execution time: ([\d.]+)/);
        if (timeMatch) {
            results.executionTime = parseFloat(timeMatch[1]);
        }

        // Extract memory usage
        const memoryMatch = content.match(/Peak memory usage: ([\d.]+)/);
        if (memoryMatch) {
            results.memoryUsage = parseFloat(memoryMatch[1]);
        }

        return results;
    }

    analyzeAllResults() {
        console.log('Analyzing Rate Comparison Results');
        console.log('=' .repeat(60));

        const analysisResults = {};

        // Initialize structure
        this.rates.forEach(rate => {
            analysisResults[rate] = {};
            this.patterns.forEach(pattern => {
                analysisResults[rate][pattern] = {};
            });
        });

        // Parse all log files
        for (const rate of this.rates) {
            for (const pattern of this.patterns) {
                for (const approach of this.approaches) {
                    const logFile = path.join(this.logDir, `${approach}_${pattern}_rate_${rate}.log`);
                    const results = this.parseLogFile(logFile);
                    
                    if (results && results.accuracy !== null) {
                        analysisResults[rate][pattern][approach] = results;
                    }
                }
            }
        }

        return analysisResults;
    }

    generateAccuracyComparison(analysisResults) {
        console.log('\nACCURACY COMPARISON TABLE');
        console.log('=' .repeat(100));

        // Header
        let header = 'Rate'.padEnd(8) + '| Pattern'.padEnd(20) + '| Approximation'.padEnd(15) + '| Fetching'.padEnd(12) + '| Difference'.padEnd(12) + '| Notes';
        console.log(header);
        console.log('-'.repeat(100));

        for (const rate of this.rates) {
            for (const pattern of this.patterns) {
                const approxData = analysisResults[rate][pattern]['approximation'];
                const fetchingData = analysisResults[rate][pattern]['fetching_client_side'];
                
                let approxAccuracy = 'N/A';
                let fetchingAccuracy = 'N/A';
                let difference = 'N/A';
                let notes = '';

                if (approxData && approxData.accuracy !== null) {
                    approxAccuracy = `${approxData.accuracy.toFixed(2)}%`;
                }
                
                if (fetchingData && fetchingData.accuracy !== null) {
                    fetchingAccuracy = `${fetchingData.accuracy.toFixed(2)}%`;
                }

                if (approxData && fetchingData && 
                    approxData.accuracy !== null && fetchingData.accuracy !== null) {
                    const diff = fetchingData.accuracy - approxData.accuracy;
                    difference = `${diff >= 0 ? '+' : ''}${diff.toFixed(2)}%`;
                    
                    if (Math.abs(diff) < 1) {
                        notes = 'Similar';
                    } else if (diff > 0) {
                        notes = 'Fetching better';
                    } else {
                        notes = 'Approximation better';
                    }
                }

                const row = `${rate.toString().padEnd(7)}| ${pattern.padEnd(19)}| ${approxAccuracy.padEnd(14)}| ${fetchingAccuracy.padEnd(11)}| ${difference.padEnd(11)}| ${notes}`;
                console.log(row);
            }
        }
    }

    generateRateTrendAnalysis(analysisResults) {
        console.log('\nRATE TREND ANALYSIS');
        console.log('=' .repeat(80));

        for (const pattern of this.patterns) {
            console.log(`\n${pattern.toUpperCase()} PATTERN:`);
            console.log('-'.repeat(50));

            const trendData = [];
            
            for (const rate of this.rates) {
                const approxData = analysisResults[rate][pattern]['approximation'];
                const fetchingData = analysisResults[rate][pattern]['fetching_client_side'];
                
                if (approxData && fetchingData && 
                    approxData.accuracy !== null && fetchingData.accuracy !== null) {
                    trendData.push({
                        rate,
                        approxAccuracy: approxData.accuracy,
                        fetchingAccuracy: fetchingData.accuracy,
                        difference: fetchingData.accuracy - approxData.accuracy
                    });
                }
            }

            if (trendData.length > 0) {
                console.log('Rate    | Approximation | Fetching | Difference | Assessment');
                console.log('--------|---------------|----------|------------|------------');
                
                trendData.forEach(data => {
                    let assessment = '';
                    if (data.approxAccuracy > 95) assessment = 'Excellent approx';
                    else if (data.approxAccuracy > 85) assessment = 'Good approx';
                    else if (data.approxAccuracy > 70) assessment = 'Fair approx';
                    else assessment = 'Poor approx';

                    console.log(
                        `${data.rate.toString().padEnd(7)} | ` +
                        `${data.approxAccuracy.toFixed(2)}%`.padEnd(13) + ' | ' +
                        `${data.fetchingAccuracy.toFixed(2)}%`.padEnd(8) + ' | ' +
                        `${data.difference >= 0 ? '+' : ''}${data.difference.toFixed(2)}%`.padEnd(10) + ' | ' +
                        assessment
                    );
                });

                // Analyze trend
                const approxTrend = this.calculateTrend(trendData.map(d => d.approxAccuracy));
                const fetchingTrend = this.calculateTrend(trendData.map(d => d.fetchingAccuracy));
                
                console.log(`\nApproximation trend: ${approxTrend}`);
                console.log(`Fetching trend: ${fetchingTrend}`);
            } else {
                console.log('No complete data available for trend analysis');
            }
        }
    }

    calculateTrend(values) {
        if (values.length < 2) return 'Insufficient data';
        
        const n = values.length;
        const sumX = (n * (n - 1)) / 2; // Sum of indices 0,1,2...n-1
        const sumY = values.reduce((a, b) => a + b, 0);
        const sumXY = values.reduce((sum, y, x) => sum + x * y, 0);
        const sumX2 = values.reduce((sum, _, x) => sum + x * x, 0);
        
        const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        
        if (slope > 1) return 'Strong positive trend';
        if (slope > 0.1) return 'Positive trend';
        if (slope > -0.1) return 'Stable';
        if (slope > -1) return 'Negative trend';
        return 'Strong negative trend';
    }

    generateCSVReport(analysisResults) {
        const csvPath = path.join(this.logDir, 'rate_comparison_results.csv');
        const csvRows = [];
        
        // Header
        csvRows.push('Rate,Pattern,Approach,Accuracy,ExecutionTime,MemoryUsage');
        
        // Data rows
        for (const rate of this.rates) {
            for (const pattern of this.patterns) {
                for (const approach of this.approaches) {
                    const data = analysisResults[rate][pattern][approach];
                    if (data && data.accuracy !== null) {
                        csvRows.push(
                            `${rate},${pattern},${approach},${data.accuracy},${data.executionTime || ''},${data.memoryUsage || ''}`
                        );
                    }
                }
            }
        }
        
        fs.writeFileSync(csvPath, csvRows.join('\n'));
        console.log(`\nCSV report generated: ${csvPath}`);
    }

    async run() {
        const analysisResults = this.analyzeAllResults();
        
        this.generateAccuracyComparison(analysisResults);
        this.generateRateTrendAnalysis(analysisResults);
        this.generateCSVReport(analysisResults);
        
        console.log('\nAnalysis complete!');
        console.log('\nKey Insights:');
        console.log('1. Compare accuracy degradation as rate increases');
        console.log('2. Identify the rate threshold where approximation becomes unreliable');
        console.log('3. Check if extreme rates break the plateau effect discovered earlier');
        console.log('4. Look for patterns where fetching significantly outperforms approximation');
    }
}

// Command line interface
async function main() {
    const analyzer = new RateComparisonAccuracyAnalyzer();
    await analyzer.run();
}

if (require.main === module) {
    main().catch(error => {
        console.error('💥 Analysis failed:', error);
        process.exit(1);
    });
}

module.exports = RateComparisonAccuracyAnalyzer;
