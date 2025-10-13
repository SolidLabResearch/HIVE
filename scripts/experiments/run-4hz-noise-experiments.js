#!/usr/bin/env node

/**
 * 4Hz Noise Pattern Accuracy Experiment Runner
 * Runs approximation vs fetching client side approaches for 4Hz frequency
 * across different noise levels (0.1, 0.2, 0.5, 1.0, 2.0)
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const NOISE_LEVELS = [0.1, 0.2, 0.5, 1.0, 2.0];
const FREQUENCY = '4Hz';
const ITERATIONS = 1; // Single iteration per noise level

// Create results directory
const resultsDir = path.join(__dirname, 'results', 'frequency-experiments');
if (!fs.existsSync(resultsDir)) {
    fs.mkdirSync(resultsDir, { recursive: true });
}

async function runApproachForNoiseLevel(approach, noiseLevel, iteration = 1) {
    return new Promise((resolve, reject) => {
        console.log(`\nRunning ${approach} approach for noise ${noiseLevel} (iteration ${iteration})...`);

        const approachDir = path.join(resultsDir, approach, FREQUENCY, `noise_${noiseLevel}`, `iteration${iteration}`);
        if (!fs.existsSync(approachDir)) {
            fs.mkdirSync(approachDir, { recursive: true });
        }

        // Set environment variables for custom log directory
        const env = { ...process.env };
        if (approach === 'approximation-approach') {
            env.CUSTOM_LOG_DIR = approachDir;
        }

        // Determine the script and command based on approach
        let scriptPath, args;
        if (approach === 'approximation-approach') {
            scriptPath = 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js';
            args = [];
        } else if (approach === 'fetching-client-side') {
            scriptPath = 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js';
            args = [];
        } else {
            reject(new Error(`Unknown approach: ${approach}`));
            return;
        }

        const approachProcess = spawn('node', [scriptPath, ...args], {
            stdio: 'inherit',
            cwd: path.join(__dirname, '..'),
            env: env
        });

        // Start the publisher with noisy data
        const publisherDataPath = `src/streamer/data/noisy_datasets/noise_${noiseLevel}`;
        const publisher = spawn('node', ['dist/streamer/src/publish.js', '--data-path', publisherDataPath], {
            stdio: 'inherit',
            cwd: path.join(__dirname, '..')
        });

        // Wait for completion or timeout
        const timeout = 5 * 60 * 1000; // 5 minutes timeout
        const startTime = Date.now();

        let publisherFinished = false;

        publisher.on('exit', (code) => {
            publisherFinished = true;
            console.log(`Publisher finished for ${approach} noise ${noiseLevel}, waiting for processing...`);
            // Wait additional 30 seconds for processing after publisher finishes
            setTimeout(() => {
                approachProcess.kill();
                resolve();
            }, 30000);
        });

        approachProcess.on('exit', (code) => {
            if (!publisherFinished) {
                publisher.kill();
            }
            resolve();
        });

        // Timeout handling
        const timeoutId = setTimeout(() => {
            console.log(`Timeout reached for ${approach} noise ${noiseLevel}`);
            approachProcess.kill();
            publisher.kill();
            resolve();
        }, timeout);
    });
}

async function runAllExperiments() {
    console.log('🚀 4Hz Noise Pattern Accuracy Experiments');
    console.log('=' .repeat(60));
    console.log(`Frequency: ${FREQUENCY}`);
    console.log(`Noise Levels: ${NOISE_LEVELS.join(', ')}`);
    console.log(`Approaches: approximation-approach, fetching-client-side`);
    console.log(`Iterations per combination: ${ITERATIONS}`);
    console.log('');

    const approaches = ['approximation-approach', 'fetching-client-side'];

    for (const approach of approaches) {
        console.log(`\n📊 Running ${approach} experiments...`);

        for (const noise of NOISE_LEVELS) {
            for (let i = 1; i <= ITERATIONS; i++) {
                try {
                    await runApproachForNoiseLevel(approach, noise, i);
                    console.log(`✅ Completed ${approach} noise ${noise} iteration ${i}`);
                } catch (error) {
                    console.error(`❌ Failed ${approach} noise ${noise} iteration ${i}: ${error.message}`);
                }
            }
        }

        console.log(`✅ Completed all ${approach} experiments`);
    }

    console.log('\n🎉 All experiments completed!');
    console.log(`Results saved to: ${resultsDir}`);
    console.log('\nNext: Run accuracy analysis with:');
    console.log('node tools/analysis/accuracy/4hz-accuracy-analysis.js');
}

runAllExperiments().catch(console.error);
