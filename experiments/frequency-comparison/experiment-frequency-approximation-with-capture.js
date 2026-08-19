#!/usr/bin/env node

/**
 * Frequency Comparison Experiment - Approximation Approach with Results Capture
 *
 * This script runs the approximation approach while simultaneously capturing
 * results to CSV files for analysis. It launches both the approach orchestrator
 * and the results capture utility.
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { createBenchmarkReplayRunEnv } = require('../utils/benchmarkReplayEnv');

class FrequencyComparisonApproximationWithCapture {
    constructor() {
        this.logDir = 'experiments/frequency-comparison/logs/approximation';
        this.dataDir = './src/streamer/data/frequency_comparison';
        this.replayEnv = createBenchmarkReplayRunEnv(process.env);

        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    async runSingleTest(oscillationType, frequency, iteration = 1) {
        const formattedFreq = parseFloat(frequency) % 1 === 0 ?
            parseFloat(frequency).toFixed(1) : frequency.toString();
        const datasetName = `${oscillationType}_freq_${formattedFreq}`;

        console.log(`\nTesting ${oscillationType} at ${frequency} Hz (Iteration ${iteration})...`);
        console.log(`   Dataset: ${datasetName}`);
        console.log(`   Nyquist ratio: ${(frequency / 2.0).toFixed(2)}x`);

        const env = this.replayEnv.withBenchmarkReplayEnv({
            ...process.env,
            DATA_PATH: `frequency_comparison/${datasetName}`
        });

        return new Promise((resolve, reject) => {
            const testLogDir = path.join(this.logDir, datasetName, `iteration${iteration}`);
            if (!fs.existsSync(testLogDir)) {
                fs.mkdirSync(testLogDir, { recursive: true });
            }

            // Start results capture first
            console.log('Starting results capture...');
            const captureProcess = spawn('node', [
                'experiments/frequency-comparison/capture-results.js',
                'approximation',
                frequency.toString(),
                testLogDir
            ], {
                stdio: ['inherit', 'pipe', 'pipe'],
                cwd: process.cwd()
            });

            let captureStdout = '';
            let captureStderr = '';

            captureProcess.stdout.on('data', (data) => {
                const output = data.toString();
                captureStdout += output;
                process.stdout.write(`[CAPTURE] ${output}`);
            });

            captureProcess.stderr.on('data', (data) => {
                const output = data.toString();
                captureStderr += output;
                process.stderr.write(`[CAPTURE] ${output}`);
            });

            // Wait a bit for capture to initialize
            setTimeout(() => {
                console.log('Starting approximation approach orchestrator...');

                const approach = spawn('node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'], {
                    stdio: ['inherit', 'pipe', 'pipe'],
                    cwd: process.cwd(),
                    env: env
                });

                let approachStdout = '';
                let approachStderr = '';

                approach.stdout.on('data', (data) => {
                    const output = data.toString();
                    approachStdout += output;
                    process.stdout.write(`[APPROACH] ${output}`);
                });

                approach.stderr.on('data', (data) => {
                    const output = data.toString();
                    approachStderr += output;
                    process.stderr.write(`[APPROACH] ${output}`);
                });

                // Start the data publisher after a short delay
                setTimeout(() => {
                    console.log('Starting data publisher...');

                    const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
                        stdio: ['inherit', 'pipe', 'pipe'],
                        cwd: process.cwd(),
                        env: env
                    });

                    let publisherStdout = '';
                    let publisherStderr = '';

                    publisher.stdout.on('data', (data) => {
                        const output = data.toString();
                        publisherStdout += output;
                        process.stdout.write(`[PUBLISHER] ${output}`);
                    });

                    publisher.stderr.on('data', (data) => {
                        const output = data.toString();
                        publisherStderr += output;
                        process.stderr.write(`[PUBLISHER] ${output}`);
                    });

                    // Set up timeout to kill all processes
                    const timeout = setTimeout(() => {
                        console.log('⏰ Timeout reached, killing all processes...');
                        publisher.kill();
                        approach.kill();
                        captureProcess.kill('SIGINT'); // Send SIGINT to allow graceful capture shutdown
                    }, 3 * 60 * 1000); // 3 minutes

                    // Wait for publisher to finish (it replays all data)
                    publisher.on('close', (code) => {
                        clearTimeout(timeout);

                        console.log('Publisher finished, stopping approach and capture...');

                        // Give capture a moment to receive final results
                        setTimeout(() => {
                            approach.kill();
                            captureProcess.kill('SIGINT');
                        }, 2000);

                        // Wait for everything to finish
                        setTimeout(() => {
                            // Write logs to files
                            const approximationLogFile = path.join(testLogDir, 'approximation_approach_log.csv');
                            const captureLogFile = path.join(testLogDir, 'capture_log.txt');
                            const replayerLogFile = path.join(testLogDir, 'replayer-log.csv');

                            fs.writeFileSync(approximationLogFile, approachStdout);
                            fs.writeFileSync(captureLogFile, captureStdout);

                            if (fs.existsSync('replayer-log.csv')) {
                                fs.renameSync('replayer-log.csv', replayerLogFile);
                            }

                            console.log(`\nCompleted ${oscillationType} ${frequency} Hz`);
                            console.log(`Results saved to: ${testLogDir}`);

                            resolve({
                                success: true,
                                oscillationType,
                                frequency,
                                datasetName,
                                logPath: testLogDir
                            });
                        }, 3000);
                    });

                    publisher.on('error', (error) => {
                        clearTimeout(timeout);
                        approach.kill();
                        captureProcess.kill();
                        console.error(`Publisher error: ${error.message}`);
                        resolve({
                            success: false,
                            oscillationType,
                            frequency,
                            error: error.message
                        });
                    });
                }, 2000); // 2 second delay before starting publisher

            }, 1000); // 1 second delay after approach starts

        });
    }

    async runTests(oscillationType, frequencies) {
        console.log(`\nRunning Approximation Approach Tests with Results Capture`);
        console.log(`Oscillation Type: ${oscillationType}`);
        console.log(`Frequencies: ${frequencies.join(', ')} Hz`);
        console.log('='.repeat(80));

        const results = [];

        for (const frequency of frequencies) {
            const result = await this.runSingleTest(oscillationType, frequency);
            results.push(result);

            // Brief pause between tests
            await new Promise(resolve => setTimeout(resolve, 5000));
        }

        console.log('\n' + '='.repeat(80));
        console.log('All Tests Completed');
        console.log('='.repeat(80));

        const successful = results.filter(r => r.success).length;
        console.log(`\nResults: ${successful}/${results.length} tests completed successfully`);

        return results;
    }
}

// Main execution
async function main() {
    const args = process.argv.slice(2);

    if (args.length < 2) {
        console.error('Usage: node experiment-frequency-approximation-with-capture.js <oscillation_type> <frequency> [iteration]');
        console.error('Example: node experiment-frequency-approximation-with-capture.js complex_oscillation 0.1 1');
        process.exit(1);
    }

    const oscillationType = args[0];
    const frequency = parseFloat(args[1]);
    const iteration = args[2] ? parseInt(args[2], 10) : 1;

    if (isNaN(frequency)) {
        console.error('Error: Frequency must be a number');
        process.exit(1);
    }

    const experiment = new FrequencyComparisonApproximationWithCapture();

    try {
        await experiment.runSingleTest(oscillationType, frequency, iteration);
        console.log('\nExperiment completed successfully!');
        process.exit(0);
    } catch (error) {
        console.error('\nExperiment failed:', error);
        process.exit(1);
    }
}

// Handle script interruption
process.on('SIGINT', () => {
    console.log('\n⚠️ Experiment interrupted by user');
    process.exit(0);
});

if (require.main === module) {
    main();
}

module.exports = FrequencyComparisonApproximationWithCapture;
