#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

/**
 * Extract unifiedAverage values from approximation and fetching client side logs
 * for accuracy comparison across different exponential rates
 */

class UnifiedAverageExtractor {
    constructor() {
        this.approxDir = './logs/rate-comparison-approximation';
        this.fetchingDir = './logs/rate-comparison-fetching';
        this.rates = [0.001, 0.01, 0.1, 1, 10, 100];
        this.patterns = ['exponential_growth', 'exponential_decay'];
        this.results = {
            approximation: {},
            fetching: {}
        };
    }

    extractUnifiedAverages(filePath) {
        if (!fs.existsSync(filePath)) {
            console.log(` File not found: ${filePath}`);
            return [];
        }

        try {
            const content = fs.readFileSync(filePath, 'utf8');
            const lines = content.split('\n');
            const unifiedAverages = [];

            for (const line of lines) {
                // Look for unifiedAverage in the line
                const match = line.match(/"unifiedAverage":([0-9.]+)/);
                if (match) {
                    const value = parseFloat(match[1]);
                    unifiedAverages.push(value);
                }
            }

            return unifiedAverages;
        } catch (error) {
            console.error(`Error reading file ${filePath}:`, error.message);
            return [];
        }
    }

    calculateStatistics(values) {
        if (values.length === 0) return null;

        const sorted = [...values].sort((a, b) => a - b);
        const sum = values.reduce((a, b) => a + b, 0);
        const mean = sum / values.length;
        
        const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
        const stdDev = Math.sqrt(variance);

        return {
            count: values.length,
            min: sorted[0],
            max: sorted[sorted.length - 1],
            mean: mean,
            median: sorted.length % 2 === 0 
                ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
                : sorted[Math.floor(sorted.length / 2)],
            stdDev: stdDev,
            values: values
        };
    }

    extractAllResults() {
        console.log('Extracting unifiedAverage values from logs...\n');

        for (const pattern of this.patterns) {
            for (const rate of this.rates) {
                console.log(`Processing ${pattern} with rate ${rate}`);

                // Approximation results
                const approxLogPath = path.join(
                    this.approxDir, 
                    `${pattern}_rate_${rate}`, 
                    'iteration1', 
                    'approximation_approach_log.csv'
                );
                
                const approxValues = this.extractUnifiedAverages(approxLogPath);
                console.log(`   Approximation: ${approxValues.length} values found`);

                // Fetching client side results
                const fetchingLogPath = path.join(
                    this.fetchingDir, 
                    `${pattern}_rate_${rate}`, 
                    'iteration1', 
                    'fetching_client_side_log.csv'
                );
                
                const fetchingValues = this.extractUnifiedAverages(fetchingLogPath);
                console.log(`   Fetching: ${fetchingValues.length} values found`);

                // Store results
                const key = `${pattern}_${rate}`;
                this.results.approximation[key] = this.calculateStatistics(approxValues);
                this.results.fetching[key] = this.calculateStatistics(fetchingValues);
            }
        }
    }

    compareAccuracy() {
        console.log('\nACCURACY COMPARISON RESULTS\n');
        console.log('=' .repeat(80));

        for (const pattern of this.patterns) {
            console.log(`\n ${pattern.toUpperCase()} PATTERN\n`);
            console.log('Rate    | Approximation Avg  | Fetching Avg      | Difference    | Rel Error %');
            console.log('-'.repeat(75));

            for (const rate of this.rates) {
                const key = `${pattern}_${rate}`;
                const approxStats = this.results.approximation[key];
                const fetchingStats = this.results.fetching[key];

                if (approxStats && fetchingStats && approxStats.count > 0 && fetchingStats.count > 0) {
                    const approxMean = approxStats.mean;
                    const fetchingMean = fetchingStats.mean;
                    const difference = Math.abs(approxMean - fetchingMean);
                    const relativeError = (difference / Math.max(approxMean, fetchingMean)) * 100;

                    console.log(
                        `${rate.toString().padEnd(7)} | ` +
                        `${approxMean.toFixed(6).padEnd(18)} | ` +
                        `${fetchingMean.toFixed(6).padEnd(17)} | ` +
                        `${difference.toFixed(6).padEnd(13)} | ` +
                        `${relativeError.toFixed(3)}%`
                    );
                } else {
                    console.log(`${rate.toString().padEnd(7)} | Missing data for comparison`);
                }
            }
        }
    }

    generateDetailedReport() {
        console.log('\n\nDETAILED STATISTICS REPORT\n');
        console.log('=' .repeat(80));

        for (const pattern of this.patterns) {
            console.log(`\n ${pattern.toUpperCase()} - DETAILED ANALYSIS\n`);

            for (const rate of this.rates) {
                const key = `${pattern}_${rate}`;
                const approxStats = this.results.approximation[key];
                const fetchingStats = this.results.fetching[key];

                console.log(`\nRate ${rate}:`);
                console.log('─'.repeat(40));

                if (approxStats && approxStats.count > 0) {
                    console.log(`  Approximation Approach:`);
                    console.log(`    Count: ${approxStats.count}`);
                    console.log(`    Mean:  ${approxStats.mean.toFixed(6)}`);
                    console.log(`    Std:   ${approxStats.stdDev.toFixed(6)}`);
                    console.log(`    Range: ${approxStats.min.toFixed(6)} - ${approxStats.max.toFixed(6)}`);
                } else {
                    console.log(`  Approximation Approach: No data available`);
                }

                if (fetchingStats && fetchingStats.count > 0) {
                    console.log(`  Fetching Client Side:`);
                    console.log(`    Count: ${fetchingStats.count}`);
                    console.log(`    Mean:  ${fetchingStats.mean.toFixed(6)}`);
                    console.log(`    Std:   ${fetchingStats.stdDev.toFixed(6)}`);
                    console.log(`    Range: ${fetchingStats.min.toFixed(6)} - ${fetchingStats.max.toFixed(6)}`);
                } else {
                    console.log(`  Fetching Client Side: No data available`);
                }
            }
        }
    }

    saveResults() {
        const outputFile = './rate_comparison_accuracy_results.json';
        const reportData = {
            timestamp: new Date().toISOString(),
            rates: this.rates,
            patterns: this.patterns,
            results: this.results,
            summary: this.generateSummaryStats()
        };

        fs.writeFileSync(outputFile, JSON.stringify(reportData, null, 2));
        console.log(`\nResults saved to: ${outputFile}`);
    }

    generateSummaryStats() {
        const summary = {
            approximation: {},
            fetching: {},
            comparison: {}
        };

        for (const pattern of this.patterns) {
            summary.approximation[pattern] = {};
            summary.fetching[pattern] = {};
            summary.comparison[pattern] = {};

            for (const rate of this.rates) {
                const key = `${pattern}_${rate}`;
                const approxStats = this.results.approximation[key];
                const fetchingStats = this.results.fetching[key];

                if (approxStats && approxStats.count > 0) {
                    summary.approximation[pattern][rate] = {
                        mean: approxStats.mean,
                        stdDev: approxStats.stdDev,
                        count: approxStats.count
                    };
                }

                if (fetchingStats && fetchingStats.count > 0) {
                    summary.fetching[pattern][rate] = {
                        mean: fetchingStats.mean,
                        stdDev: fetchingStats.stdDev,
                        count: fetchingStats.count
                    };
                }

                if (approxStats && fetchingStats && approxStats.count > 0 && fetchingStats.count > 0) {
                    const difference = Math.abs(approxStats.mean - fetchingStats.mean);
                    const relativeError = (difference / Math.max(approxStats.mean, fetchingStats.mean)) * 100;
                    
                    summary.comparison[pattern][rate] = {
                        absoluteDifference: difference,
                        relativeErrorPercent: relativeError,
                        approximationMean: approxStats.mean,
                        fetchingMean: fetchingStats.mean
                    };
                }
            }
        }

        return summary;
    }

    run() {
        console.log('Starting Unified Average Extraction and Accuracy Analysis\n');
        
        this.extractAllResults();
        this.compareAccuracy();
        this.generateDetailedReport();
        this.saveResults();
        
        console.log('\nAnalysis complete!');
    }
}

// Run the analysis
const extractor = new UnifiedAverageExtractor();
extractor.run();
