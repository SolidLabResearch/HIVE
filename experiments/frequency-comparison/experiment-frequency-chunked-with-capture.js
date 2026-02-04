#!/usr/bin/env node

/**
 * Frequency Comparison Experiment - Chunked Approach with Results Capture
 *
 * This script runs the chunked approach while simultaneously capturing
 * results to CSV files for analysis. It launches both the approach orchestrator
 * and the results capture utility.
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

class FrequencyComparisonChunkedWithCapture {
    constructor() {
        this.logDir = './logs/frequency-comparison-chunked';
        this.dataDir = './src/streamer/data/frequency_comparison';

        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    async runSingleTest(oscillationType, frequency) {
        const formattedFreq = parseFloat(frequency) % 1 === 0 ?
            parseFloat(frequency).toFixed(1) : frequency.toString();
        const datasetName = `${oscillationType}_freq_${formattedFreq}`;

        console.log(`\nTesting ${oscillationType} at ${frequency} Hz...`);
        console.log(`   Dataset: ${datasetName}`);
        console.log(`   Nyquist ratio: ${(frequency / 2.0).toFixed(2)}x`);

        const env = {
            ...process.env,
            DATA_PATH: `frequency_comparison/${datasetName}`
        };

        return new Promise((resolve, reject) => {
            const testLogDir = path.join(this.logDir, datasetName, 'iteration1');
            if (!fs.existsSync(testLogDir)) {
                fs.mkdirSync(testLogDir, { recursive: true });
            }

            // Start results capture first
            console.log('Starting results capture...');
            const captureProcess = spawn('node', [
                'experiments/frequency-comparison/capture-results.js',
                'chunked',
                frequency.toString(),
                oscillationType
            ], { stdio: 'inherit' });

            // Wait a moment for capture to initialize
            setTimeout(() => {
                console.log('Starting chunked orchestrator...');
                const orchestratorProcess = spawn('node', [
                    'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'
                ], { env, stdio: 'pipe' });

                // Capture orchestrator logs
                const orchestratorLogPath = path.join(testLogDir, 'chunked_orchestrator_log.txt');
                const orchestratorLogStream = fs.createWriteStream(orchestratorLogPath);

                orchestratorProcess.stdout.pipe(orchestratorLogStream);
                orchestratorProcess.stderr.pipe(orchestratorLogStream);

                // Wait a moment for orchestrator to initialize
                setTimeout(() => {
                    console.log('Starting data publisher...');
                    const publisherProcess = spawn('node', [
                        'dist/streamer/src/publish.js'
                    ], { env, stdio: 'pipe' });

                    // Capture publisher logs
                    const publisherLogPath = path.join(testLogDir, 'publisher_log.txt');
                    const publisherLogStream = fs.createWriteStream(publisherLogPath);

                    publisherProcess.stdout.pipe(publisherLogStream);
                    publisherProcess.stderr.pipe(publisherLogStream);

                    const timeout = setTimeout(() => {
                        console.log('Test timeout reached, terminating...');
                        publisherProcess.kill();
                        orchestratorProcess.kill();
                        captureProcess.kill();
                        reject(new Error('Test timeout'));
                    }, 180000); // 3 minutes timeout

                    publisherProcess.on('close', (code) => {
                        clearTimeout(timeout);

                        // Give a moment for final results to be captured
                        setTimeout(() => {
                            orchestratorProcess.kill();
                            captureProcess.kill();

                            // Move log files to test directory
                            this.moveLogFiles(testLogDir);

                            console.log(`   Test completed with code ${code}`);
                            resolve({ frequency, oscillationType, exitCode: code });
                        }, 2000);
                    });

                    publisherProcess.on('error', (err) => {
                        clearTimeout(timeout);
                        orchestratorProcess.kill();
                        captureProcess.kill();
                        reject(err);
                    });

                }, 2000); // Wait 2s for orchestrator to start

            }, 1000); // Wait 1s for capture to start

        });
    }

    moveLogFiles(testLogDir) {
        const logFiles = [
            'streaming_query_chunk_aggregator_log.csv',
            'streaming_query_hive_resource_log.csv',
            'chunked_latency_log.csv',
            'replayer-log.csv'
        ];

        logFiles.forEach(logFile => {
            const srcPath = path.join('.', logFile);
            const destPath = path.join(testLogDir, logFile);

            if (fs.existsSync(srcPath)) {
                fs.copyFileSync(srcPath, destPath);
                fs.unlinkSync(srcPath);
                console.log(`   Moved ${logFile} to ${testLogDir}`);
            }
        });
    }

    async runFrequencyTest(frequency) {
        const oscillationType = 'complex_oscillation';

        try {
            const result = await this.runSingleTest(oscillationType, frequency);
            console.log(`✓ Completed test for ${frequency} Hz`);
            return result;
        } catch (error) {
            console.error(`✗ Test failed for ${frequency} Hz:`, error.message);
            throw error;
        }
    }

    async runAllFrequencies() {
        const frequencies = [0.1, 0.5, 1.0, 1.5, 2.0];
        const results = [];

        console.log('='.repeat(80));
        console.log('FREQUENCY COMPARISON - CHUNKED APPROACH');
        console.log('='.repeat(80));
        console.log(`Testing frequencies: ${frequencies.join(', ')} Hz`);
        console.log(`Sampling rate: ~4 Hz (250ms intervals)`);
        console.log(`Nyquist frequency: 2.0 Hz`);
        console.log('='.repeat(80));

        for (const freq of frequencies) {
            try {
                const result = await this.runFrequencyTest(freq);
                results.push(result);

                // Wait between tests
                await new Promise(resolve => setTimeout(resolve, 3000));
            } catch (error) {
                console.error(`Failed to run test for ${freq} Hz`);
                results.push({ frequency: freq, error: error.message });
            }
        }

        console.log('\n' + '='.repeat(80));
        console.log('ALL TESTS COMPLETED');
        console.log('='.repeat(80));
        console.log(`Successful: ${results.filter(r => !r.error).length}/${results.length}`);
        console.log(`Results saved to: ${this.logDir}`);

        return results;
    }
}

// Main execution
async function main() {
    const args = process.argv.slice(2);
    const runner = new FrequencyComparisonChunkedWithCapture();

    try {
        if (args.length === 0) {
            // Run all frequencies
            await runner.runAllFrequencies();
        } else if (args.length === 1) {
            // Run single frequency
            const frequency = parseFloat(args[0]);
            await runner.runFrequencyTest(frequency);
        } else {
            console.log('Usage:');
            console.log('  node experiment-frequency-chunked-with-capture.js           # Run all frequencies');
            console.log('  node experiment-frequency-chunked-with-capture.js 0.1       # Run single frequency');
            process.exit(1);
        }
    } catch (error) {
        console.error('Experiment failed:', error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = FrequencyComparisonChunkedWithCapture;
