#!/usr/bin/env node

/**
 * Master Runner for Frequency Comparison Experiments with Results Capture
 *
 * This script orchestrates the complete frequency comparison experiment workflow:
 * 1. Runs fetching client-side approach (baseline) for all frequencies
 * 2. Runs approximation approach for all frequencies
 * 3. Captures results to CSV files for both approaches
 * 4. Enables accuracy comparison using fetching as ground truth
 *
 * Usage:
 *   node run-frequency-comparison-with-capture.js
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const FREQUENCIES = [0.1, 0.5, 1.0, 1.5, 2.0];
const OSCILLATION_TYPE = 'complex_oscillation';

class FrequencyComparisonRunner {
    constructor() {
        this.results = {
            fetching: [],
            approximation: []
        };
    }

    async runCommand(command, args, label) {
        return new Promise((resolve, reject) => {
            console.log(`\n${'='.repeat(80)}`);
            console.log(`Running: ${label}`);
            console.log(`Command: ${command} ${args.join(' ')}`);
            console.log('='.repeat(80));

            const child = spawn(command, args, {
                stdio: 'inherit',
                shell: true
            });

            child.on('close', (code) => {
                if (code === 0) {
                    console.log(`\n✅ Completed: ${label}`);
                    resolve({ success: true, code });
                } else {
                    console.error(`\n❌ Failed: ${label} (exit code: ${code})`);
                    resolve({ success: false, code });
                }
            });

            child.on('error', (error) => {
                console.error(`\n❌ Error running ${label}:`, error.message);
                reject(error);
            });
        });
    }

    async runFetchingExperiments() {
        console.log('\n' + '█'.repeat(80));
        console.log('PHASE 1: FETCHING CLIENT-SIDE APPROACH (BASELINE)');
        console.log('█'.repeat(80));

        for (let i = 0; i < FREQUENCIES.length; i++) {
            const frequency = FREQUENCIES[i];
            console.log(`\n[${i + 1}/${FREQUENCIES.length}] Testing Fetching Approach at ${frequency} Hz`);

            const result = await this.runCommand(
                'node',
                [
                    'experiments/frequency-comparison/experiment-frequency-fetching-with-capture.js',
                    OSCILLATION_TYPE,
                    frequency.toString(),
                    '1'
                ],
                `Fetching ${frequency} Hz`
            );

            this.results.fetching.push({
                frequency,
                success: result.success,
                code: result.code
            });

            // Brief pause between experiments
            if (i < FREQUENCIES.length - 1) {
                console.log('\nPausing for 5 seconds before next experiment...');
                await new Promise(resolve => setTimeout(resolve, 5000));
            }
        }

        const successfulFetching = this.results.fetching.filter(r => r.success).length;
        console.log(`\n✅ Fetching Phase Complete: ${successfulFetching}/${FREQUENCIES.length} experiments successful`);
    }

    async runApproximationExperiments() {
        console.log('\n' + '█'.repeat(80));
        console.log('PHASE 2: APPROXIMATION APPROACH');
        console.log('█'.repeat(80));

        for (let i = 0; i < FREQUENCIES.length; i++) {
            const frequency = FREQUENCIES[i];
            console.log(`\n[${i + 1}/${FREQUENCIES.length}] Testing Approximation Approach at ${frequency} Hz`);

            const result = await this.runCommand(
                'node',
                [
                    'experiments/frequency-comparison/experiment-frequency-approximation-with-capture.js',
                    OSCILLATION_TYPE,
                    frequency.toString(),
                    '1'
                ],
                `Approximation ${frequency} Hz`
            );

            this.results.approximation.push({
                frequency,
                success: result.success,
                code: result.code
            });

            // Brief pause between experiments
            if (i < FREQUENCIES.length - 1) {
                console.log('\nPausing for 5 seconds before next experiment...');
                await new Promise(resolve => setTimeout(resolve, 5000));
            }
        }

        const successfulApproximation = this.results.approximation.filter(r => r.success).length;
        console.log(`\n✅ Approximation Phase Complete: ${successfulApproximation}/${FREQUENCIES.length} experiments successful`);
    }

    printSummary() {
        console.log('\n' + '█'.repeat(80));
        console.log('EXPERIMENT SUMMARY');
        console.log('█'.repeat(80));

        const fetchingSuccess = this.results.fetching.filter(r => r.success).length;
        const approximationSuccess = this.results.approximation.filter(r => r.success).length;
        const totalSuccess = fetchingSuccess + approximationSuccess;
        const totalExperiments = FREQUENCIES.length * 2;

        console.log('\n📊 Overall Results:');
        console.log(`   Total Experiments: ${totalExperiments}`);
        console.log(`   Successful: ${totalSuccess}`);
        console.log(`   Failed: ${totalExperiments - totalSuccess}`);

        console.log('\n📈 Fetching Approach (Baseline):');
        console.log(`   Successful: ${fetchingSuccess}/${FREQUENCIES.length}`);
        this.results.fetching.forEach(r => {
            const status = r.success ? '✅' : '❌';
            console.log(`   ${status} ${r.frequency} Hz`);
        });

        console.log('\n📉 Approximation Approach:');
        console.log(`   Successful: ${approximationSuccess}/${FREQUENCIES.length}`);
        this.results.approximation.forEach(r => {
            const status = r.success ? '✅' : '❌';
            console.log(`   ${status} ${r.frequency} Hz`);
        });

        console.log('\n📁 Output Directories:');
        console.log('   Fetching: experiments/frequency-comparison/logs/fetching/');
        console.log('   Approximation: experiments/frequency-comparison/logs/approximation/');

        console.log('\n🔍 Each test directory contains:');
        console.log('   - <approach>_results.csv : Query results captured from MQTT');
        console.log('   - <approach>_metadata.json : Test metadata and timing info');
        console.log('   - <approach>_log.csv : System logs');
        console.log('   - replayer-log.csv : Data replay timing');

        console.log('\n📊 Next Steps:');
        console.log('   1. Run analysis script to compare accuracy:');
        console.log('      node analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js');
        console.log('');
        console.log('   2. The analysis will calculate:');
        console.log('      - MAPE (Mean Absolute Percentage Error)');
        console.log('      - MAE (Mean Absolute Error)');
        console.log('      - RMSE (Root Mean Square Error)');
        console.log('      - Correlation coefficient');
        console.log('      - First event latency comparison');
        console.log('');
        console.log('   3. Results will show accuracy degradation at high frequencies');
        console.log('      (near Nyquist limit at 2.0 Hz with 4 Hz sampling rate)');
    }

    async run() {
        console.log('╔' + '═'.repeat(78) + '╗');
        console.log('║' + ' '.repeat(78) + '║');
        console.log('║' + '  FREQUENCY COMPARISON EXPERIMENT WITH RESULTS CAPTURE'.padEnd(78) + '║');
        console.log('║' + ' '.repeat(78) + '║');
        console.log('╚' + '═'.repeat(78) + '╝');

        console.log('\n📋 Experiment Configuration:');
        console.log(`   Oscillation Type: ${OSCILLATION_TYPE}`);
        console.log(`   Frequencies: ${FREQUENCIES.join(', ')} Hz`);
        console.log(`   Approaches: Fetching (baseline), Approximation`);
        console.log(`   Total Experiments: ${FREQUENCIES.length * 2}`);
        console.log(`   Estimated Duration: ~${FREQUENCIES.length * 2 * 3.5} minutes`);

        const startTime = Date.now();

        try {
            // Phase 1: Run fetching experiments (baseline)
            await this.runFetchingExperiments();

            // Phase 2: Run approximation experiments
            await this.runApproximationExperiments();

            const endTime = Date.now();
            const durationMinutes = ((endTime - startTime) / 1000 / 60).toFixed(2);

            console.log(`\n⏱️  Total Duration: ${durationMinutes} minutes`);

            // Print summary
            this.printSummary();

            // Check if all experiments succeeded
            const allSuccessful =
                this.results.fetching.every(r => r.success) &&
                this.results.approximation.every(r => r.success);

            if (allSuccessful) {
                console.log('\n🎉 All experiments completed successfully!\n');
                process.exit(0);
            } else {
                console.log('\n⚠️  Some experiments failed. Check logs for details.\n');
                process.exit(1);
            }

        } catch (error) {
            console.error('\n💥 Fatal error:', error);
            process.exit(1);
        }
    }
}

// Main execution
async function main() {
    const runner = new FrequencyComparisonRunner();
    await runner.run();
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n\n⚠️  Experiment interrupted by user');
    console.log('Partial results may be available in logs/ directory');
    process.exit(130);
});

process.on('SIGTERM', () => {
    console.log('\n\n⚠️  Experiment terminated');
    console.log('Partial results may be available in logs/ directory');
    process.exit(143);
});

main().catch(error => {
    console.error('💥 Unhandled error:', error);
    process.exit(1);
});
