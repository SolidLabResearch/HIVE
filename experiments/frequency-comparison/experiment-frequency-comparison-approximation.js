#!/usr/bin/env node

/**
 * Frequency Comparison Experiment - Approximation Approach
 * Tests approximation accuracy across different oscillation frequencies
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const FREQUENCIES = [0.1, 0.5, 1.0, 1.5, 2.0];
const OSCILLATION_TYPES = ['simple_oscillation', 'variable_amplitude', 'complex_oscillation'];

class FrequencyComparisonApproximationExperiment {
    constructor() {
        this.frequencies = [0.1, 0.5, 1.0, 1.5, 2.0];
        this.logDir = './logs/frequency-comparison-approximation';
        this.dataDir = './src/streamer/data/frequency_comparison';
        
        // Ensure log directory exists
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    async runSingleTest(oscillationType, frequency, frequencyStr = null) {
        // Use frequencyStr for path naming if provided, otherwise use frequency
        const freqForPath = frequencyStr || frequency;
        const datasetName = `${oscillationType}_freq_${freqForPath}`;
        
        console.log(`\nTesting ${oscillationType} at ${frequency} Hz...`);
        console.log(`   Dataset: ${datasetName}`);
        console.log(`   Nyquist ratio: ${(frequency / 2.0).toFixed(2)}x`);
        
        // Set DATA_PATH environment variable
        const env = { 
            ...process.env, 
            DATA_PATH: `frequency_comparison/${datasetName}`
        };
        
        return new Promise((resolve, reject) => {
            const logFile = path.join(this.logDir, `${datasetName}.log`);
            const resourceFile = path.join(this.logDir, `${datasetName}_resource_usage.csv`);
            
            // Create individual log directory
            const testLogDir = path.join(this.logDir, datasetName, 'iteration1');
            if (!fs.existsSync(testLogDir)) {
                fs.mkdirSync(testLogDir, { recursive: true });
            }
            
            // Start the approach process
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
                process.stdout.write(output);
            });

            approach.stderr.on('data', (data) => {
                const output = data.toString();
                approachStderr += output;
                process.stderr.write(output);
            });

            // Start the publisher process (data replayer) after a short delay
            setTimeout(() => {
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
                    process.stdout.write(output);
                });

                publisher.stderr.on('data', (data) => {
                    const output = data.toString();
                    publisherStderr += output;
                    process.stderr.write(output);
                });

                // Set up a timeout to kill both processes
                const timeout = setTimeout(() => {
                    console.log('⏰ Timeout reached, killing processes...');
                    approach.kill();
                    publisher.kill();
                }, 3 * 60 * 1000); // 3 minutes timeout

                // Wait for publisher to finish
                publisher.on('close', (code) => {
                    clearTimeout(timeout);
                    approach.kill();
                    
                    // Write logs to individual test directory
                    const approxLogFile = path.join(testLogDir, 'approximation_approach_log.csv');
                    const approxResourceFile = path.join(testLogDir, 'approximation_approach_resource_usage.csv');
                    const replayerLogFile = path.join(testLogDir, 'replayer-log.csv');
                    
                    fs.writeFileSync(approxLogFile, approachStdout);
                    fs.writeFileSync(approxResourceFile, approachStderr);
                    
                    // Copy replayer log if it exists
                    if (fs.existsSync('replayer-log.csv')) {
                        fs.renameSync('replayer-log.csv', replayerLogFile);
                    }
                    
                    if (code === 0) {
                        console.log(`Completed ${oscillationType} ${frequency} Hz`);
                        resolve({ 
                            success: true, 
                            oscillationType, 
                            frequency, 
                            datasetName,
                            logPath: testLogDir
                        });
                    } else {
                        console.error(`Failed ${oscillationType} ${frequency} Hz (exit code: ${code})`);
                        resolve({ 
                            success: false, 
                            oscillationType, 
                            frequency, 
                            datasetName, 
                            error: `Exit code: ${code}`,
                            logPath: testLogDir
                        });
                    }
                });

                publisher.on('error', (error) => {
                    clearTimeout(timeout);
                    approach.kill();
                    console.error(`Publisher error in ${oscillationType} ${frequency} Hz:`, error.message);
                    resolve({ 
                        success: false, 
                        oscillationType, 
                        frequency, 
                        datasetName, 
                        error: error.message,
                        logPath: testLogDir
                    });
                });
            }, 2000); // 2 second delay before starting publisher

            approach.on('error', (error) => {
                console.error(`Approach error in ${oscillationType} ${frequency} Hz:`, error.message);
                resolve({ 
                    success: false, 
                    oscillationType, 
                    frequency, 
                    datasetName, 
                    error: error.message,
                    logPath: testLogDir
                });
            });
        });
    }

    async runAllTests() {
        console.log('FREQUENCY COMPARISON EXPERIMENT - APPROXIMATION APPROACH');
        console.log('='.repeat(80));
        console.log(`Testing ${FREQUENCIES.length} frequencies across ${OSCILLATION_TYPES.length} oscillation types`);
        
        const results = [];
        let testCount = 0;
        
        for (const oscillationType of OSCILLATION_TYPES) {
            console.log(`\n🔬 Testing ${oscillationType.replace('_', ' ').toUpperCase()} oscillations:`);
            
            for (const frequency of FREQUENCIES) {
                testCount++;
                const result = await this.runSingleTest(oscillationType, frequency);
                results.push(result);
                
                // Brief pause between tests
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }
        
        // Generate summary
        const summary = {
            config: {
                frequencies: FREQUENCIES,
                oscillationTypes: OSCILLATION_TYPES,
                totalTests: testCount
            },
            results: results,
            summary: {
                total: results.length,
                successful: results.filter(r => r.success).length,
                failed: results.filter(r => !r.success).length,
                timestamp: new Date().toISOString()
            }
        };
        
        // Save summary
        const summaryPath = path.join(this.logDir, 'frequency_approximation_summary.json');
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        
        console.log('\nFREQUENCY EXPERIMENT SUMMARY:');
        console.log(`   Total tests: ${summary.summary.total}`);
        console.log(`   Successful: ${summary.summary.successful}`);
        console.log(`   Failed: ${summary.summary.failed}`);
        console.log(`   Success rate: ${(summary.summary.successful / summary.summary.total * 100).toFixed(1)}%`);
        
        // Show results by frequency
        console.log('\nRESULTS BY FREQUENCY:');
        for (const freq of FREQUENCIES) {
            const freqResults = results.filter(r => r.frequency === freq);
            const freqSuccess = freqResults.filter(r => r.success).length;
            console.log(`   ${freq} Hz: ${freqSuccess}/${freqResults.length} successful`);
        }
        
        // Show results by oscillation type
        console.log('\nRESULTS BY OSCILLATION TYPE:');
        for (const type of OSCILLATION_TYPES) {
            const typeResults = results.filter(r => r.oscillationType === type);
            const typeSuccess = typeResults.filter(r => r.success).length;
            console.log(`   ${type}: ${typeSuccess}/${typeResults.length} successful`);
        }
        
        console.log(`\nSummary saved to: ${summaryPath}`);
        console.log('Frequency comparison experiment complete!');
        
        return summary;
    }
}

// Handle command line arguments
if (process.argv.length > 2) {
    const command = process.argv[2];
    
    if (command === 'test' && process.argv.length >= 5) {
        const oscillationType = process.argv[3];
        const frequencyStr = process.argv[4]; // Keep as string to preserve format
        const frequency = parseFloat(frequencyStr); // Parse for calculations
        
        const experiment = new FrequencyComparisonApproximationExperiment();
        // Pass both the string format and numeric value
        experiment.runSingleTest(oscillationType, frequency, frequencyStr)
            .then(result => {
                console.log(`\nTest completed: ${result.success ? 'SUCCESS' : 'FAILED'}`);
                process.exit(result.success ? 0 : 1);
            })
            .catch(error => {
                console.error('Test failed:', error);
                process.exit(1);
            });
    } else {
        console.log('Usage:');
        console.log('  node experiment-frequency-comparison-approximation.js                    # Run all tests');
        console.log('  node experiment-frequency-comparison-approximation.js test simple_oscillation 1.0  # Run specific test');
        process.exit(1);
    }
} else {
    // Run all tests
    const experiment = new FrequencyComparisonApproximationExperiment();
    experiment.runAllTests()
        .then(() => process.exit(0))
        .catch(error => {
            console.error('Experiment failed:', error);
            process.exit(1);
        });
}
