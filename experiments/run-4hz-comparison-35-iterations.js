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

console.log('🚀 Running 4Hz Comparison Experiment (35 Iterations Each)');
console.log('=' .repeat(60));
console.log(`Target Frequency: ${FREQUENCY}`);
console.log(`Iterations per approach: ${ITERATIONS}`);
console.log(`Experiment Directory: ${experimentDir}`);
console.log(`Approaches: Independent Stream Processing, Approximation, Streaming Query Hive, Fetching Client Side`);
console.log('');

async function runApproach(name, command, approachKey) {
    const approachDir = path.join(experimentLogsDir, approachKey);
    console.log(`\n🎯 Running ${name} approach...`);
    console.log(`   Logs will be saved to: experiments/logs/${experimentDir}/${approachKey}/`);

    // Create approach-specific directory
    if (!fs.existsSync(approachDir)) {
        fs.mkdirSync(approachDir, { recursive: true });
    }

    // Set environment variable for custom log directory (for Independent Stream Processing)
    const env = { ...process.env };
    if (approachKey === 'independent-stream-processing') {
        env.CUSTOM_LOG_DIR = approachDir;
    }

    for (let i = 1; i <= ITERATIONS; i++) {
        console.log(`   Iteration ${i}/${ITERATIONS}...`);

        try {
            // Kill any lingering processes
            try {
                execSync('pkill -f StreamingQueryHiveApproachOrchestrator.js');
                execSync('pkill -f StreamingQueryApproximationApproachOrchestrator.js');
                execSync('pkill -f IndependentStreamProcessingApproach');
                execSync('pkill -f publish.js');
            } catch (e) { }

            // Start the approach
            const approach = spawn(command[0], command[1], {
                stdio: 'inherit',
                cwd: path.join(__dirname, '..'),
                env: env
            });

            // Wait a bit for startup
            await new Promise(resolve => setTimeout(resolve, 2000));

            // Start the publisher
            const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
                stdio: 'inherit',
                cwd: path.join(__dirname, '..')
            });

            // Wait for completion or timeout
            const timeout = 5 * 60 * 1000; // 5 minutes timeout
            const startTime = Date.now();

            await new Promise((resolve, reject) => {
                const timeoutId = setTimeout(() => {
                    approach.kill();
                    publisher.kill();
                    reject(new Error('Timeout'));
                }, timeout);

                publisher.on('exit', (code) => {
                    clearTimeout(timeoutId);
                    approach.kill();
                    resolve();
                });

                approach.on('exit', (code) => {
                    clearTimeout(timeoutId);
                    publisher.kill();
                    resolve();
                });
            });

            console.log(`   ✅ Iteration ${i} completed`);

        } catch (error) {
            console.log(`   ❌ Iteration ${i} failed: ${error.message}`);
        }
    }

    console.log(`✅ ${name} approach completed`);
}

async function main() {
    try {
        // Ensure dist directory exists
        if (!fs.existsSync(path.join(__dirname, '../dist'))) {
            console.log('📦 Building project...');
            execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });
        }

        // Run Independent Stream Processing (TypeScript)
        await runApproach(
            'Independent Stream Processing',
            ['npx', ['ts-node', 'experiments/experiment-evaluation-independent-stream-processing.ts', '--frequency', FREQUENCY, '--iterations', ITERATIONS.toString()]],
            'independent-stream-processing'
        );

        // Run Approximation Approach
        await runApproach(
            'Approximation Approach',
            ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']],
            'approximation-approach'
        );

        // Run Streaming Query Hive
        await runApproach(
            'Streaming Query Hive',
            ['node', ['dist/approaches/StreamingQueryHiveApproachOrchestrator.js']],
            'streaming-query-hive'
        );

        // Run Fetching Client Side
        await runApproach(
            'Fetching Client Side',
            ['node', ['dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js']],
            'fetching-client-side'
        );

        console.log('\n🎉 All experiments completed!');
        console.log(`📊 Results are saved in: experiments/logs/${experimentDir}/`);
        console.log('📁 Directory structure:');
        console.log(`   ├── ${experimentDir}/`);
        console.log('   │   ├── independent-stream-processing/');
        console.log('   │   ├── approximation-approach/');
        console.log('   │   ├── streaming-query-hive/');
        console.log('   │   └── fetching-client-side/');
        console.log('📈 Use analysis scripts to process the results');

    } catch (error) {
        console.error('💥 Experiment failed:', error);
        process.exit(1);
    }
}

main();
