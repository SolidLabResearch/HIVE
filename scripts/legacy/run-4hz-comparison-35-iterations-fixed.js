#!/usr/bin/env node

const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const ITERATIONS = 35;
const FREQUENCY = '4Hz';

// Create timestamped experiment directory
const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
const experimentDir = `experiment-4hz-35iter-${timestamp}`;
const experimentLogsDir = path.join(__dirname, 'logs', experimentDir);

console.log('Running 4Hz Comparison Experiment (35 Iterations Each)');
console.log('='.repeat(60));
console.log(`Target Frequency: ${FREQUENCY}`);
console.log(`Iterations per approach: ${ITERATIONS}`);
console.log(`Experiment Directory: ${experimentDir}`);
console.log(`Approaches: Independent Stream Processing, Approximation, Streaming Query Hive, Fetching Client Side`);
console.log('');

// Ensure experiment directory exists
if (!fs.existsSync(experimentLogsDir)) {
    fs.mkdirSync(experimentLogsDir, { recursive: true });
}

function killLingering() {
    try {
        execSync('pkill -f StreamingQueryChunkedApproachOrchestrator', { stdio: 'ignore' });
        execSync('pkill -f StreamingQueryApproximationApproachOrchestrator', { stdio: 'ignore' });
        execSync('pkill -f StreamingQueryFetchingClientSideApproachOrchestrator', { stdio: 'ignore' });
        execSync('pkill -f IndependentStreamProcessingApproach', { stdio: 'ignore' });
        execSync('pkill -f publish.js', { stdio: 'ignore' });
    } catch (e) {
        // Ignore errors - processes might not exist
    }
}

async function runIndependentStreamProcessing() {
    const approachDir = path.join(experimentLogsDir, 'independent-stream-processing');
    console.log('\nRunning Independent Stream Processing approach...');
    console.log(`   Logs will be saved to: experiments/logs/${experimentDir}/independent-stream-processing/`);

    // Create approach-specific directory
    if (!fs.existsSync(approachDir)) {
        fs.mkdirSync(approachDir, { recursive: true });
    }

    // Set environment variables
    const env = { ...process.env };
    env.CUSTOM_LOG_DIR = approachDir;

    try {
        killLingering();

        console.log(`   Running ${ITERATIONS} iterations internally...`);

        // This approach handles iterations internally
        const approach = spawn('npx', ['ts-node', 'experiments/experiment-evaluation-independent-stream-processing.ts', '--frequency', FREQUENCY, '--iterations', ITERATIONS.toString()], {
            stdio: 'inherit',
            cwd: path.join(__dirname, '..'),
            env: env
        });

        // Wait for completion or timeout (10 minutes for 35 iterations)
        const timeout = 10 * 60 * 1000; 
        await new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                approach.kill('SIGTERM');
                reject(new Error('Timeout'));
            }, timeout);

            approach.on('exit', (code) => {
                clearTimeout(timeoutId);
                console.log(`   Independent Stream Processing completed with exit code: ${code}`);
                resolve();
            });

            approach.on('error', (error) => {
                clearTimeout(timeoutId);
                reject(error);
            });
        });

    } catch (error) {
        console.log(`   Independent Stream Processing failed: ${error.message}`);
    }

    console.log('Independent Stream Processing approach completed');
}

async function runSingleApproachIteration(name, command, approachKey, iteration) {
    const approachDir = path.join(experimentLogsDir, approachKey);
    const iterationDir = path.join(approachDir, `iteration-${iteration}`);
    
    // Create iteration-specific directory
    if (!fs.existsSync(iterationDir)) {
        fs.mkdirSync(iterationDir, { recursive: true });
    }

    // Set environment variables for this iteration
    const env = { ...process.env };
    env.CUSTOM_LOG_DIR = iterationDir;

    try {
        killLingering();

        // Start the approach
        console.log(`   Starting ${name} iteration ${iteration}...`);
        const approach = spawn(command[0], command.slice(1), {
            stdio: 'pipe', // Capture output to reduce noise
            cwd: path.join(__dirname, '..'),
            env: env
        });

        // Wait a bit for startup
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Start the publisher
        console.log(`   Starting data publisher for iteration ${iteration}...`);
        const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
            stdio: 'pipe', // Capture output
            cwd: path.join(__dirname, '..')
        });

        // Wait for completion or timeout (5 minutes per iteration)
        const timeout = 5 * 60 * 1000;
        await new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                approach.kill('SIGTERM');
                publisher.kill('SIGTERM');
                reject(new Error('Timeout'));
            }, timeout);

            let publisherFinished = false;

            publisher.on('exit', (code) => {
                publisherFinished = true;
                console.log(`   Publisher finished for iteration ${iteration}, waiting for approach to process...`);
                // Wait additional 45 seconds for processing after publisher finishes
                setTimeout(() => {
                    clearTimeout(timeoutId);
                    approach.kill('SIGTERM');
                    resolve();
                }, 45000);
            });

            approach.on('exit', (code) => {
                clearTimeout(timeoutId);
                if (!publisherFinished) {
                    publisher.kill('SIGTERM');
                }
                resolve();
            });

            // Handle errors
            approach.on('error', (error) => {
                clearTimeout(timeoutId);
                publisher.kill('SIGTERM');
                reject(error);
            });

            publisher.on('error', (error) => {
                clearTimeout(timeoutId);
                approach.kill('SIGTERM');
                reject(error);
            });
        });

    } catch (error) {
        console.log(`   Iteration ${iteration} failed: ${error.message}`);
    }

    // Check if logs were generated
    const files = fs.readdirSync(iterationDir);
    const logFiles = files.filter(f => f.endsWith('.csv') || f.endsWith('.log'));
    console.log(`   Iteration ${iteration} completed. Generated ${logFiles.length} log files.`);
}

async function runMultipleIterationsApproach(name, command, approachKey) {
    const approachDir = path.join(experimentLogsDir, approachKey);
    console.log(`\nRunning ${name} approach...`);
    console.log(`   Logs will be saved to: experiments/logs/${experimentDir}/${approachKey}/`);

    // Create approach-specific directory
    if (!fs.existsSync(approachDir)) {
        fs.mkdirSync(approachDir, { recursive: true });
    }

    // Run iterations sequentially
    for (let i = 1; i <= ITERATIONS; i++) {
        console.log(`   Running iteration ${i}/${ITERATIONS}...`);
        await runSingleApproachIteration(name, command, approachKey, i);
        
        // Wait between iterations to ensure clean state
        if (i < ITERATIONS) {
            console.log(`   Waiting 3 seconds before next iteration...`);
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }

    // Count total log files
    let totalLogFiles = 0;
    for (let i = 1; i <= ITERATIONS; i++) {
        const iterationDir = path.join(approachDir, `iteration-${i}`);
        if (fs.existsSync(iterationDir)) {
            const files = fs.readdirSync(iterationDir);
            const logFiles = files.filter(f => f.endsWith('.csv') || f.endsWith('.log'));
            totalLogFiles += logFiles.length;
        }
    }

    console.log(`${name} approach completed. Total log files generated: ${totalLogFiles}`);
}

async function main() {
    try {
        // Ensure dist directory exists
        if (!fs.existsSync(path.join(__dirname, '../dist'))) {
            console.log('Building project...');
            execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });
        }

        // Kill any existing processes
        killLingering();

        // Run Independent Stream Processing (handles iterations internally)
        await runIndependentStreamProcessing();

        // Run Approximation Approach (35 external iterations)
        await runMultipleIterationsApproach(
            'Approximation Approach',
            ['node', 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'],
            'approximation-approach'
        );

        // Run Streaming Query Hive (35 external iterations)
        await runMultipleIterationsApproach(
            'Streaming Query Hive',
            ['node', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'],
            'streaming-query-hive'
        );

        // Run Fetching Client Side (35 external iterations)
        await runMultipleIterationsApproach(
            'Fetching Client Side',
            ['node', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js'],
            'fetching-client-side'
        );

        console.log('\nAll experiments completed!');
        console.log(`Results are saved in: experiments/logs/${experimentDir}/`);
        
        // Show final summary
        console.log('\nFinal Summary:');
        ['independent-stream-processing', 'approximation-approach', 'streaming-query-hive', 'fetching-client-side'].forEach(approach => {
            const approachDir = path.join(experimentLogsDir, approach);
            if (fs.existsSync(approachDir)) {
                const subdirs = fs.readdirSync(approachDir, { withFileTypes: true })
                    .filter(dirent => dirent.isDirectory())
                    .map(dirent => dirent.name);
                
                let totalFiles = 0;
                if (approach === 'independent-stream-processing') {
                    // Count files recursively for independent stream processing
                    function countFiles(dir) {
                        let count = 0;
                        const items = fs.readdirSync(dir, { withFileTypes: true });
                        for (const item of items) {
                            if (item.isDirectory()) {
                                count += countFiles(path.join(dir, item.name));
                            } else if (item.name.endsWith('.csv') || item.name.endsWith('.log')) {
                                count++;
                            }
                        }
                        return count;
                    }
                    totalFiles = countFiles(approachDir);
                } else {
                    // Count files in iteration directories
                    subdirs.forEach(subdir => {
                        const subPath = path.join(approachDir, subdir);
                        const files = fs.readdirSync(subPath);
                        totalFiles += files.filter(f => f.endsWith('.csv') || f.endsWith('.log')).length;
                    });
                }
                
                console.log(`   ${approach}: ${subdirs.length} subdirectories, ${totalFiles} log files`);
            } else {
                console.log(`   ${approach}: No directory created`);
            }
        });
        
        console.log('\nUse analysis scripts to process the results');

    } catch (error) {
        console.error('Experiment failed:', error);
        process.exit(1);
    }
}

main();
