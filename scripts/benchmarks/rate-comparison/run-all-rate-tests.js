#!/usr/bin/env node

/**
 * Simple sequential experiment runner for rate comparison
 */

const { spawn } = require('child_process');

const RATES = [0.001, 0.01, 0.1, 1, 10, 100];
const PATTERNS = ['exponential_growth', 'exponential_decay'];

async function runSingleTest(approach, pattern, rate) {
    return new Promise((resolve, reject) => {
        console.log(`\n Running ${approach} - ${pattern} - rate ${rate}`);
        
        const scriptName = approach === 'approximation' ? 
            'experiment-rate-comparison-approximation.js' : 
            'experiment-rate-comparison-fetching.js';
        
        const child = spawn('node', [scriptName, 'test', pattern, rate.toString()], {
            stdio: 'inherit'
        });
        
        child.on('close', (code) => {
            if (code === 0) {
                console.log(`Completed: ${approach} - ${pattern} - rate ${rate}`);
                resolve();
            } else {
                console.error(`Failed: ${approach} - ${pattern} - rate ${rate}`);
                reject(new Error(`Exit code: ${code}`));
            }
        });
    });
}

async function runAllTests() {
    console.log('Starting Rate Comparison Experiments with Corrected Data');
    console.log('='.repeat(70));
    
    let completed = 0;
    const total = RATES.length * PATTERNS.length * 2; // 2 approaches
    
    for (const approach of ['approximation', 'fetching']) {
        console.log(`\nRunning ${approach.toUpperCase()} experiments...`);
        
        for (const pattern of PATTERNS) {
            console.log(`\n Pattern: ${pattern}`);
            
            for (const rate of RATES) {
                try {
                    await runSingleTest(approach, pattern, rate);
                    completed++;
                    console.log(`Progress: ${completed}/${total} tests completed`);
                    
                    // Brief pause between tests
                    await new Promise(resolve => setTimeout(resolve, 2000));
                    
                } catch (error) {
                    console.error(`Failed: ${error.message}`);
                    // Continue with next test
                }
            }
        }
    }
    
    console.log('\nAll experiments completed!');
    console.log(`\nReady for analysis. Run: node approximation-accuracy-analysis.js`);
}

runAllTests().catch(console.error);
