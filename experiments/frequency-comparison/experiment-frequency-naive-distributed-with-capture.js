#!/usr/bin/env node

/**
 * Frequency Comparison Experiment - Naive Distributed Approach with Results Capture
 *
 * Runs the Naive Distributed baseline: subqueries AND the super-query execute
 * simultaneously against raw MQTT streams with no result reuse. Results are
 * captured to CSV files for comparison with the other three approaches.
 *
 * Usage:
 *   node experiment-frequency-naive-distributed-with-capture.js <oscillation_type> <frequency> [iteration]
 *   node experiment-frequency-naive-distributed-with-capture.js complex_oscillation 1.0
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { createBenchmarkReplayRunEnv } = require('../utils/benchmarkReplayEnv');

class FrequencyComparisonNaiveDistributedWithCapture {
    constructor() {
        this.logDir = './logs/frequency-comparison-naive-distributed';
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
        console.log(`   Approach: Naive Distributed (subqueries + super-query, no reuse)`);
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

            // Start results capture first (subscribes to naive_distributed/output MQTT topic)
            console.log('Starting results capture...');
            const captureProcess = spawn('node', [
                'experiments/frequency-comparison/capture-results.js',
                'naive-distributed',
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

            // Wait for capture to initialise before starting the orchestrator
            setTimeout(() => {
                console.log('Starting Naive Distributed orchestrator...');

                const approach = spawn('node', [
                    'dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js'
                ], {
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

                // Start the data publisher after the orchestrator has initialised
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

                    // Hard timeout – kill everything if the publisher doesn't finish in time
                    const timeout = setTimeout(() => {
                        console.log('⏰ Timeout reached, killing all processes...');
                        publisher.kill();
                        approach.kill();
                        captureProcess.kill('SIGINT');
                    }, 3 * 60 * 1000); // 3 minutes

                    // Normal path: publisher finishes replaying all data
                    publisher.on('close', (code) => {
                        clearTimeout(timeout);

                        console.log('Publisher finished, stopping approach and capture...');

                        // Give capture a moment to receive any in-flight results
                        setTimeout(() => {
                            approach.kill();
                            captureProcess.kill('SIGINT');
                        }, 2000);

                        // After processes have stopped, write log files
                        setTimeout(() => {
                            const naiveLogFile = path.join(testLogDir, 'naive_distributed_approach_log.csv');
                            const captureLogFile = path.join(testLogDir, 'capture_log.txt');
                            const replayerLogFile = path.join(testLogDir, 'replayer-log.csv');

                            // approachStdout contains LOG: and DEBUG: lines – this is what
                            // extract-results-from-logs.js will parse.
                            fs.writeFileSync(naiveLogFile, approachStdout);
                            fs.writeFileSync(captureLogFile, captureStdout);

                            if (fs.existsSync('replayer-log.csv')) {
                                fs.renameSync('replayer-log.csv', replayerLogFile);
                            }

                            // Move resource usage log if present
                            if (fs.existsSync('naive_distributed_approach_resource_usage.csv')) {
                                fs.renameSync(
                                    'naive_distributed_approach_resource_usage.csv',
                                    path.join(testLogDir, 'naive_distributed_approach_resource_usage.csv')
                                );
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

                }, 2000); // 2 s delay before starting publisher

            }, 1000); // 1 s delay after starting capture
        });
    }

    async runTests(oscillationType, frequencies) {
        console.log(`\nRunning Naive Distributed Approach Tests with Results Capture`);
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
        console.error('Usage: node experiment-frequency-naive-distributed-with-capture.js <oscillation_type> <frequency> [iteration]');
        console.error('Example: node experiment-frequency-naive-distributed-with-capture.js complex_oscillation 1.0 1');
        process.exit(1);
    }

    const oscillationType = args[0];
    const frequency = parseFloat(args[1]);
    const iteration = args[2] ? parseInt(args[2], 10) : 1;

    if (isNaN(frequency)) {
        console.error('Error: Frequency must be a number');
        process.exit(1);
    }

    const experiment = new FrequencyComparisonNaiveDistributedWithCapture();

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
    console.log('\n⚠️  Experiment interrupted by user');
    process.exit(0);
});

if (require.main === module) {
    main();
}

module.exports = FrequencyComparisonNaiveDistributedWithCapture;
