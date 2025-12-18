#!/usr/bin/env node

/**
 * Run fetching client side experiments for exponential patterns across different rates
 */

const { spawn } = require('child_process');

const RATES = [0.001, 0.01, 0.1, 1, 10, 100];
const PATTERNS = ['exponential_growth', 'exponential_decay'];

async function runExperiment(pattern, rate) {
    return new Promise((resolve, reject) => {
        console.log(`\n🧪 Running fetching client side: ${pattern} with rate ${rate}`);
        
        const child = spawn('node', ['experiment-rate-comparison-fetching.js', 'test', pattern, rate.toString()], {
            stdio: 'inherit',
            cwd: process.cwd()
        });
        
        child.on('close', (code) => {
            if (code === 0) {
                console.log(`Completed: ${pattern} rate ${rate}`);
                resolve();
            } else {
                console.error(`Failed: ${pattern} rate ${rate} (exit code: ${code})`);
                reject(new Error(`Experiment failed with exit code ${code}`));
            }
        });
        
        child.on('error', (error) => {
            console.error(`Error: ${error.message}`);
            reject(error);
        });
    });
}

async function runAllExperiments() {
    console.log('Starting Fetching Client Side Rate Comparison Experiments');
    console.log(`Testing ${RATES.length} rates × ${PATTERNS.length} patterns = ${RATES.length * PATTERNS.length} total experiments`);
    
    let completed = 0;
    const total = RATES.length * PATTERNS.length;
    
    for (const pattern of PATTERNS) {
        console.log(`\nTesting ${pattern} pattern`);
        
        for (const rate of RATES) {
            try {
                completed++;
                console.log(`\n[${completed}/${total}] Testing ${pattern} at rate ${rate}`);
                
                await runExperiment(pattern, rate);
                
                // Brief pause between experiments
                await new Promise(resolve => setTimeout(resolve, 2000));
                
            } catch (error) {
                console.error(`Failed experiment: ${pattern} ${rate}`);
                console.error(error.message);
                // Continue with other experiments
            }
        }
    }
    
    console.log('\nFetching client side experiments completed!');
    console.log(`\nResults summary:`);
    console.log(`- Patterns: ${PATTERNS.join(', ')}`);
    console.log(`- Rates: ${RATES.join(', ')}`);
    console.log(`- Total experiments: ${total}`);
}

// Handle interruption gracefully
process.on('SIGINT', () => {
    console.log('\nExperiments interrupted');
    process.exit(0);
});

runAllExperiments().catch(error => {
    console.error('💥 Fatal error:', error);
    process.exit(1);
});
