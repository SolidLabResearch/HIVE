#!/usr/bin/env node

/**
 * Complex Oscillation Frequency Comparison Experiment
 * Tests both approximation and fetching client side approaches 
 * specifically for complex oscillation patterns across different frequencies
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const FREQUENCIES = [0.1, 0.5, 1.0]; // Only frequencies that support complex oscillation
const OSCILLATION_TYPE = 'complex_oscillation';

class ComplexOscillationComparisonExperiment {
    constructor() {
        this.frequencies = FREQUENCIES;
        this.oscillationType = OSCILLATION_TYPE;
        this.logDir = './logs/complex-oscillation-comparison';
        this.dataDir = './src/streamer/data/frequency_comparison';
        
        // Ensure log directory exists
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }

    async runSingleTest(approach, frequency) {
        const datasetName = `${this.oscillationType}_freq_${frequency}`;
        const approachName = approach === 'approximation' ? 'Approximation' : 'Fetching Client Side';
        
        console.log(`\nTesting ${approachName} for Complex Oscillation at ${frequency} Hz...`);
        console.log(`   Dataset: ${datasetName}`);
        console.log(`   Nyquist ratio: ${(frequency / 2.0).toFixed(2)}x`);
        
        // Set DATA_PATH environment variable
        const env = { 
            ...process.env, 
            DATA_PATH: `frequency_comparison/${datasetName}`
        };
        
        return new Promise((resolve, reject) => {
            // Create individual log directory
            const testLogDir = path.join(this.logDir, `${approach}_${datasetName}`, 'iteration1');
            if (!fs.existsSync(testLogDir)) {
                fs.mkdirSync(testLogDir, { recursive: true });
            }
            
            // Select the appropriate orchestrator
            const orchestratorScript = approach === 'approximation' 
                ? 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'
                : 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js';
            
            // Start the approach process
            const approachProcess = spawn('node', [orchestratorScript], {
                stdio: ['inherit', 'pipe', 'pipe'],
                cwd: process.cwd(),
                env: env
            });

            let approachStdout = '';
            let approachStderr = '';

            approachProcess.stdout.on('data', (data) => {
                const output = data.toString();
                approachStdout += output;
                process.stdout.write(output);
            });

            approachProcess.stderr.on('data', (data) => {
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
                    approachProcess.kill();
                    publisher.kill();
                }, 3 * 60 * 1000); // 3 minutes timeout

                // Wait for publisher to finish
                publisher.on('close', (code) => {
                    clearTimeout(timeout);
                    approachProcess.kill();
                    
                    // Write logs to individual test directory
                    const logFileName = approach === 'approximation' 
                        ? 'approximation_approach_log.csv' 
                        : 'fetching_client_side_log.csv';
                    const resourceFileName = approach === 'approximation'
                        ? 'approximation_approach_resource_usage.csv'
                        : 'fetching_client_side_resource_usage.csv';
                    
                    const logFile = path.join(testLogDir, logFileName);
                    const resourceFile = path.join(testLogDir, resourceFileName);
                    const replayerLogFile = path.join(testLogDir, 'replayer-log.csv');
                    
                    fs.writeFileSync(logFile, approachStdout);
                    fs.writeFileSync(resourceFile, approachStderr);
                    
                    // Copy replayer log if it exists
                    if (fs.existsSync('replayer-log.csv')) {
                        fs.renameSync('replayer-log.csv', replayerLogFile);
                    }
                    
                    if (code === 0) {
                        console.log(`Completed ${approachName} for ${frequency} Hz`);
                        resolve({ 
                            success: true, 
                            approach,
                            frequency, 
                            datasetName,
                            logPath: testLogDir
                        });
                    } else {
                        console.error(`Failed ${approachName} for ${frequency} Hz (exit code: ${code})`);
                        resolve({ 
                            success: false, 
                            approach,
                            frequency, 
                            datasetName, 
                            error: `Exit code: ${code}`,
                            logPath: testLogDir
                        });
                    }
                });

                publisher.on('error', (error) => {
                    clearTimeout(timeout);
                    approachProcess.kill();
                    console.error(`Publisher error in ${approachName} for ${frequency} Hz:`, error.message);
                    resolve({ 
                        success: false, 
                        approach,
                        frequency, 
                        datasetName, 
                        error: error.message,
                        logPath: testLogDir
                    });
                });
            }, 2000); // 2 second delay before starting publisher

            approachProcess.on('error', (error) => {
                console.error(`Approach error in ${approachName} for ${frequency} Hz:`, error.message);
                resolve({ 
                    success: false, 
                    approach,
                    frequency, 
                    datasetName, 
                    error: error.message,
                    logPath: testLogDir
                });
            });
        });
    }

    async runAllTests() {
        console.log('COMPLEX OSCILLATION FREQUENCY COMPARISON EXPERIMENT');
        console.log('='.repeat(70));
        console.log(`Testing Complex Oscillation across ${FREQUENCIES.length} frequencies`);
        console.log(`Approaches: Approximation vs Fetching Client Side`);
        console.log(`Frequencies: ${FREQUENCIES.join(', ')} Hz`);
        
        const results = [];
        let testCount = 0;
        
        for (const frequency of FREQUENCIES) {
            console.log(`\n🔬 Testing frequency: ${frequency} Hz`);
            
            // Test approximation approach
            testCount++;
            console.log(`   [${testCount}/6] Running Approximation Approach...`);
            const approxResult = await this.runSingleTest('approximation', frequency);
            results.push(approxResult);
            
            // Brief pause between approaches
            await new Promise(resolve => setTimeout(resolve, 3000));
            
            // Test fetching client side approach
            testCount++;
            console.log(`   [${testCount}/6] Running Fetching Client Side Approach...`);
            const fetchingResult = await this.runSingleTest('fetching', frequency);
            results.push(fetchingResult);
            
            // Brief pause between frequencies
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
        
        // Generate summary
        const summary = {
            config: {
                oscillationType: this.oscillationType,
                frequencies: FREQUENCIES,
                approaches: ['approximation', 'fetching'],
                totalTests: testCount
            },
            results: results,
            summary: {
                successful: results.filter(r => r.success).length,
                failed: results.filter(r => !r.success).length,
                successRate: `${((results.filter(r => r.success).length / results.length) * 100).toFixed(1)}%`
            }
        };
        
        // Write summary to file
        const summaryPath = path.join(this.logDir, 'experiment_summary.json');
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        
        // Print final summary
        console.log('\nEXPERIMENT SUMMARY');
        console.log('='.repeat(70));
        console.log(`Total tests: ${summary.config.totalTests}`);
        console.log(`Successful: ${summary.summary.successful}`);
        console.log(`Failed: ${summary.summary.failed}`);
        console.log(`Success rate: ${summary.summary.successRate}`);
        
        console.log('\n📁 Results by frequency:');
        for (const freq of FREQUENCIES) {
            const freqResults = results.filter(r => r.frequency === freq);
            const approxResult = freqResults.find(r => r.approach === 'approximation');
            const fetchingResult = freqResults.find(r => r.approach === 'fetching');
            
            console.log(`  ${freq} Hz:`);
            console.log(`    Approximation: ${approxResult?.success ? '' : ''}`);
            console.log(`    Fetching:      ${fetchingResult?.success ? '' : ''}`);
        }
        
        console.log(`\nSummary saved to: ${summaryPath}`);
        console.log(`📂 Detailed logs in: ${this.logDir}`);
        
        return summary;
    }
}

// Main execution
async function main() {
    const experiment = new ComplexOscillationComparisonExperiment();
    
    try {
        await experiment.runAllTests();
        console.log('\nComplex oscillation comparison experiment completed successfully!');
    } catch (error) {
        console.error('\n💥 Experiment failed:', error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = ComplexOscillationComparisonExperiment;
