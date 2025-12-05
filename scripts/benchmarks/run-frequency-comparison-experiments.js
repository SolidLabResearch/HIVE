#!/usr/bin/env node

/**
 * Frequency Comparison Experiment Runner
 * Tests complex oscillation patterns across different frequencies
 * with both approximation and fetching client side approaches
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const FREQUENCIES = [0.1, 0.5, 1.0, 1.5, 2.0];
const PATTERNS = ['complex_oscillation']; // Focus on complex oscillation only
const APPROACHES = ['approximation', 'fetching'];

async function runCommand(command, args) {
    return new Promise((resolve, reject) => {
        console.log(`\nRunning: ${command} ${args.join(' ')}`);
        
        const child = spawn(command, args, {
            stdio: 'inherit',
            shell: true
        });
        
        child.on('close', (code) => {
            if (code === 0) {
                console.log(`Completed: ${command} ${args.join(' ')}`);
                resolve();
            } else {
                console.error(`Failed: ${command} ${args.join(' ')} (exit code: ${code})`);
                reject(new Error(`Command failed with exit code ${code}`));
            }
        });
        
        child.on('error', (error) => {
            console.error(`Error running command: ${error.message}`);
            reject(error);
        });
    });
}

async function runFrequencyExperiments() {
    console.log('Starting Frequency Comparison Experiments');
    console.log('Focus: Complex Oscillation Patterns at Different Frequencies');
    console.log(`Testing ${FREQUENCIES.length} frequencies × ${APPROACHES.length} approaches = ${FREQUENCIES.length * APPROACHES.length} total experiments`);
    
    let completedCount = 0;
    const totalCount = FREQUENCIES.length * APPROACHES.length;
    
    for (const approach of APPROACHES) {
        console.log(`\n Testing ${approach.toUpperCase()} approach`);
        
        for (const frequency of FREQUENCIES) {
            try {
                completedCount++;
                console.log(`\n[${completedCount}/${totalCount}] Testing ${approach} with complex oscillation at ${frequency} Hz`);
                
                // Create appropriate experiment script name
                const scriptName = approach === 'approximation' ? 
                    'experiment-frequency-comparison-approximation.js' : 
                    'experiment-frequency-comparison-fetching.js';
                
                const datasetName = `complex_oscillation_freq_${frequency}`;
                
                await runCommand('node', [scriptName, 'test', datasetName, frequency.toString()]);
                
                // Brief pause between experiments
                await new Promise(resolve => setTimeout(resolve, 2000));
                
            } catch (error) {
                console.error(`Failed experiment: ${approach} complex oscillation ${frequency} Hz`);
                console.error(error.message);
                
                // Continue with other experiments despite failures
                continue;
            }
        }
    }
    
    console.log('\nAll frequency comparison experiments completed!');
    console.log('\nSummary:');
    console.log(`- Frequencies tested: ${FREQUENCIES.join(' Hz, ')} Hz`);
    console.log(`- Pattern: Complex Oscillation (with harmonics)`);
    console.log(`- Approaches tested: ${APPROACHES.join(', ')}`);
    console.log(`- Total experiments: ${totalCount}`);
    
    console.log('\nFrequency Analysis:');
    console.log('- 0.1 Hz: Low frequency, well-sampled (40 samples/cycle)');
    console.log('- 0.5 Hz: Medium-low frequency, well-sampled (8 samples/cycle)');
    console.log('- 1.0 Hz: Medium frequency, adequately sampled (4 samples/cycle)');
    console.log('- 1.5 Hz: High frequency, medium aliasing risk (2.7 samples/cycle)');
    console.log('- 2.0 Hz: Near Nyquist, high aliasing risk (2.0 samples/cycle)');
    
    console.log('\nNext steps:');
    console.log('1. Check logs/frequency-comparison-* directories for detailed results');
    console.log('2. Run: node analyze-frequency-comparison-results.js');
    console.log('3. Generate frequency vs accuracy plots');
}

// Handle script interruption gracefully
process.on('SIGINT', () => {
    console.log('\n Frequency experiments interrupted by user');
    console.log('Partial results may be available in logs/ directory');
    process.exit(0);
});

// Run experiments
runFrequencyExperiments().catch(error => {
    console.error(' Fatal error in frequency experiment runner:', error);
    process.exit(1);
});
