#!/usr/bin/env node

/**
 * Master Runner for All Three Approaches - Frequency Comparison
 *
 * This script runs the frequency comparison experiment for all four approaches:
 * 1. Fetching (client-side, Local-Only baseline)
 * 2. Approximation (rate-based)
 * 3. Chunked (aggregation)
 * 4. Naive Distributed (subqueries + super-query simultaneously, no result reuse)
 *
 * It then extracts results from logs and performs accuracy + latency comparison.
 *
 * Usage:
 *   node run-all-approaches-comparison.js [frequency]
 *
 * Examples:
 *   node run-all-approaches-comparison.js           # Run all frequencies for all approaches
 *   node run-all-approaches-comparison.js 0.1       # Run single frequency for all approaches
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { attachReplayMetadata } = require('../utils/benchmarkResultMetadata');

class AllApproachesComparisonRunner {
    constructor() {
        this.approaches = [
            {
                name: 'fetching',
                script: 'experiments/frequency-comparison/experiment-frequency-fetching-with-capture.js',
                logDir: './logs/frequency-comparison-fetching'
            },
            {
                name: 'approximation',
                script: 'experiments/frequency-comparison/experiment-frequency-approximation-with-capture.js',
                logDir: './logs/frequency-comparison-approximation'
            },
            {
                name: 'chunked',
                script: 'experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js',
                logDir: './logs/frequency-comparison-chunked'
            },
            {
                name: 'naive-distributed',
                script: 'experiments/frequency-comparison/experiment-frequency-naive-distributed-with-capture.js',
                logDir: './logs/frequency-comparison-naive-distributed'
            }
        ];

        this.frequencies = [0.1, 0.5, 1.0, 1.5, 2.0];
        this.oscillationType = 'complex_oscillation';
    }

    async runApproachForFrequency(approach, frequency) {
        console.log('\n' + '='.repeat(80));
        console.log(`RUNNING: ${approach.name.toUpperCase()} APPROACH - ${frequency} Hz`);
        console.log('='.repeat(80));

        return new Promise((resolve, reject) => {
            const proc = spawn('node', [approach.script, frequency.toString()], {
                stdio: 'inherit'
            });

            const timeout = setTimeout(() => {
                console.log(`⏰ Timeout for ${approach.name} at ${frequency} Hz`);
                proc.kill();
                reject(new Error('Timeout'));
            }, 240000); // 4 minutes timeout per approach

            proc.on('close', (code) => {
                clearTimeout(timeout);

                if (code === 0) {
                    console.log(`✓ ${approach.name} completed successfully`);
                    resolve({ approach: approach.name, frequency, success: true });
                } else {
                    console.log(`✗ ${approach.name} failed with code ${code}`);
                    resolve({ approach: approach.name, frequency, success: false, exitCode: code });
                }
            });

            proc.on('error', (err) => {
                clearTimeout(timeout);
                console.error(`✗ ${approach.name} error:`, err.message);
                reject(err);
            });
        });
    }

    async extractResultsForApproach(approach, frequency) {
        console.log(`\n📊 Extracting results for ${approach.name} at ${frequency} Hz...`);

        return new Promise((resolve, reject) => {
            const proc = spawn('node', [
                'experiments/frequency-comparison/extract-results-from-logs.js',
                approach.name,
                frequency.toString()
            ], { stdio: 'inherit' });

            proc.on('close', (code) => {
                if (code === 0) {
                    console.log(`✓ Extraction completed for ${approach.name}`);
                    resolve(true);
                } else {
                    console.log(`✗ Extraction failed for ${approach.name}`);
                    resolve(false);
                }
            });

            proc.on('error', (err) => {
                console.error(`✗ Extraction error for ${approach.name}:`, err.message);
                reject(err);
            });
        });
    }

    async runComparisonAnalysis() {
        console.log('\n' + '='.repeat(80));
        console.log('RUNNING ACCURACY & LATENCY COMPARISON ANALYSIS');
        console.log('='.repeat(80));

        return new Promise((resolve, reject) => {
            const proc = spawn('node', [
                'analysis/accuracy/accuracy-comparison-all-approaches.js'
            ], { stdio: 'inherit' });

            proc.on('close', (code) => {
                if (code === 0) {
                    console.log('✓ Analysis completed successfully');
                    resolve(true);
                } else {
                    console.log('✗ Analysis failed');
                    resolve(false);
                }
            });

            proc.on('error', (err) => {
                console.error('✗ Analysis error:', err.message);
                reject(err);
            });
        });
    }

    async runSingleFrequency(frequency) {
        console.log('\n' + '█'.repeat(80));
        console.log(`FREQUENCY COMPARISON - ALL APPROACHES: ${frequency} Hz`);
        console.log('█'.repeat(80));
        console.log(`Oscillation type: ${this.oscillationType}`);
        console.log(`Sampling rate: ~4 Hz (250ms intervals)`);
        console.log(`Nyquist frequency: 2.0 Hz`);
        console.log(`Nyquist ratio: ${(frequency / 2.0).toFixed(2)}x`);
        console.log('█'.repeat(80));

        const results = [];

        // Run each approach
        for (const approach of this.approaches) {
            try {
                const result = await this.runApproachForFrequency(approach, frequency);
                results.push(result);

                // Wait between approaches
                console.log('\n⏸️  Waiting 5 seconds before next approach...');
                await new Promise(resolve => setTimeout(resolve, 5000));
            } catch (error) {
                console.error(`Failed to run ${approach.name}:`, error.message);
                results.push({
                    approach: approach.name,
                    frequency,
                    success: false,
                    error: error.message
                });
            }
        }

        // Extract results for all approaches
        console.log('\n' + '='.repeat(80));
        console.log('EXTRACTING RESULTS FROM LOGS');
        console.log('='.repeat(80));

        for (const approach of this.approaches) {
            try {
                await this.extractResultsForApproach(approach, frequency);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
                console.error(`Failed to extract results for ${approach.name}:`, error.message);
            }
        }

        // Run comparison analysis
        try {
            await this.runComparisonAnalysis();
        } catch (error) {
            console.error('Failed to run comparison analysis:', error.message);
        }

        return results;
    }

    async runAllFrequencies() {
        console.log('\n' + '█'.repeat(80));
        console.log('COMPREHENSIVE FREQUENCY COMPARISON - ALL APPROACHES');
        console.log('█'.repeat(80));
        console.log(`Frequencies: ${this.frequencies.join(', ')} Hz`);
        console.log(`Approaches: ${this.approaches.map(a => a.name).join(', ')}`);
        console.log(`Total tests: ${this.frequencies.length * this.approaches.length}`);
        console.log('█'.repeat(80));

        const allResults = [];

        for (const frequency of this.frequencies) {
            try {
                const results = await this.runSingleFrequency(frequency);
                allResults.push(...results);

                // Wait between frequencies
                console.log('\n⏸️  Waiting 10 seconds before next frequency...');
                await new Promise(resolve => setTimeout(resolve, 10000));
            } catch (error) {
                console.error(`Failed to run tests for ${frequency} Hz:`, error.message);
            }
        }

        this.generateFinalReport(allResults);
        return allResults;
    }

    generateFinalReport(results) {
        console.log('\n' + '█'.repeat(80));
        console.log('FINAL REPORT - ALL APPROACHES COMPARISON');
        console.log('█'.repeat(80));

        const successful = results.filter(r => r.success);
        const failed = results.filter(r => !r.success);

        console.log(`\nTotal Tests: ${results.length}`);
        console.log(`Successful: ${successful.length}`);
        console.log(`Failed: ${failed.length}`);

        // Group by approach
        const byApproach = {};
        this.approaches.forEach(approach => {
            byApproach[approach.name] = results.filter(r => r.approach === approach.name);
        });

        console.log('\n' + '─'.repeat(80));
        console.log('Results by Approach:');
        console.log('─'.repeat(80));

        Object.entries(byApproach).forEach(([approachName, approachResults]) => {
            const successCount = approachResults.filter(r => r.success).length;
            const totalCount = approachResults.length;
            console.log(`\n${approachName.toUpperCase()}: ${successCount}/${totalCount} successful`);

            approachResults.forEach(result => {
                const status = result.success ? '✓' : '✗';
                console.log(`  ${status} ${result.frequency} Hz`);
            });
        });

        if (failed.length > 0) {
            console.log('\n' + '─'.repeat(80));
            console.log('Failed Tests:');
            console.log('─'.repeat(80));
            failed.forEach(result => {
                console.log(`  ✗ ${result.approach} at ${result.frequency} Hz`);
                if (result.error) {
                    console.log(`    Error: ${result.error}`);
                }
            });
        }

        // Save summary
        const summaryPath = './logs/frequency-comparison-all-approaches-summary.json';
        const summary = attachReplayMetadata({
            timestamp: new Date().toISOString(),
            frequencies: this.frequencies,
            approaches: this.approaches.map(a => a.name),
            results: results,
            summary: {
                total: results.length,
                successful: successful.length,
                failed: failed.length
            }
        });

        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        console.log(`\n📄 Summary saved to: ${summaryPath}`);

        console.log('\n' + '█'.repeat(80));
        console.log('For detailed accuracy and latency analysis, check:');
        console.log('  - logs/accuracy_comparison_all_approaches.csv');
        console.log('  - logs/latency_comparison_all_approaches.csv');
        console.log('█'.repeat(80));
    }
}

// Main execution
async function main() {
    const args = process.argv.slice(2);
    const runner = new AllApproachesComparisonRunner();

    try {
        if (args.length === 0) {
            // Run all frequencies for all approaches
            await runner.runAllFrequencies();
        } else if (args.length === 1) {
            // Run single frequency for all approaches
            const frequency = parseFloat(args[0]);
            if (isNaN(frequency)) {
                console.error('Error: Invalid frequency');
                process.exit(1);
            }
            await runner.runSingleFrequency(frequency);
        } else {
            console.log('Usage:');
            console.log('  node run-all-approaches-comparison.js           # Run all frequencies');
            console.log('  node run-all-approaches-comparison.js 0.1       # Run single frequency');
            process.exit(1);
        }

        console.log('\n✓ All experiments completed!');
    } catch (error) {
        console.error('\n✗ Experiment failed:', error.message);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = AllApproachesComparisonRunner;
