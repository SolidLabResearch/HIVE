#!/usr/bin/env node
/**
 * Quick Test for Comprehensive Frequency Experiment
 * 
 * Runs a single iteration of each approach with 4Hz frequency only
 * to validate the experiment framework before running the full experiment.
 */

const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Quick test configuration - only 4Hz, 1 iteration
const QUICK_CONFIG = {
    RUNS: 1,
    FREQUENCIES: ['4Hz'], // Only test 4Hz for quick validation
    DEVICES: ['smartphone'], // Only test smartphone for quick validation
    APPROACHES: [
        {
            name: 'fetching-client-side',
            cmd: ['node', ['dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js']],
            isGroundTruth: true
        }
        // Add other approaches one by one as they're validated
    ],
    TIMEOUT_MS: 2 * 60 * 1000, // 2 minutes for quick test
    BASE_LOGS_DIR: 'logs/quick-test',
    DATA_BASE_PATH: 'src/streamer/data/frequency_variants/2mins'
};

// Import the main experiment functions
const experimentModule = require('./comprehensive-frequency-experiment.js');

async function quickTest() {
    console.log('🧪 QUICK FREQUENCY EXPERIMENT TEST');
    console.log('=' * 40);
    console.log('Testing configuration:');
    console.log(`  Approaches: ${QUICK_CONFIG.APPROACHES.map(a => a.name).join(', ')}`);
    console.log(`  Frequencies: ${QUICK_CONFIG.FREQUENCIES.join(', ')}`);
    console.log(`  Devices: ${QUICK_CONFIG.DEVICES.join(', ')}`);
    console.log(`  Iterations: ${QUICK_CONFIG.RUNS}`);
    console.log(`  Timeout: ${QUICK_CONFIG.TIMEOUT_MS / 1000}s`);
    console.log('=' * 40);
    
    // Ensure quick test logs directory exists
    if (!fs.existsSync(QUICK_CONFIG.BASE_LOGS_DIR)) {
        fs.mkdirSync(QUICK_CONFIG.BASE_LOGS_DIR, { recursive: true });
    }
    
    // Override the main experiment configuration
    const originalConfig = { ...experimentModule.CONFIG };
    Object.assign(experimentModule.CONFIG, QUICK_CONFIG);
    
    try {
        console.log('\\n🚀 Starting quick test...');
        
        // Run single experiment
        const approach = QUICK_CONFIG.APPROACHES[0];
        const frequency = QUICK_CONFIG.FREQUENCIES[0];
        const device = QUICK_CONFIG.DEVICES[0];
        const iteration = 1;
        
        console.log(`\\nTesting: ${approach.name} with ${device} ${frequency}...`);
        
        const result = await experimentModule.runSingleExperiment(approach, frequency, device, iteration);
        
        if (result.success) {
            console.log('\\n✅ Quick test PASSED!');
            console.log(`   Duration: ${(result.duration / 1000).toFixed(1)}s`);
            console.log(`   Logs: ${result.iterationDir}`);
            
            // Check if log files were created
            const expectedLogs = [
                'approach_output.log',
                'publisher_output.log',
                'metadata.json'
            ];
            
            const foundLogs = expectedLogs.filter(logFile => 
                fs.existsSync(path.join(result.iterationDir, logFile))
            );
            
            console.log(`   Log files created: ${foundLogs.length}/${expectedLogs.length}`);
            foundLogs.forEach(log => console.log(`     ✓ ${log}`));
            
            console.log('\\n🎉 Framework is ready for full experiment!');
            console.log('\\nTo run the full experiment:');
            console.log('  $ node tools/experiments/comprehensive-frequency-experiment.js');
            
        } else {
            console.log('\\n❌ Quick test FAILED!');
            console.log(`   Error: ${result.error}`);
            console.log(`   Duration: ${(result.duration / 1000).toFixed(1)}s`);
            
            console.log('\\n🔍 Troubleshooting suggestions:');
            console.log('  1. Check if TypeScript is compiled: npm run build');
            console.log('  2. Verify frequency data exists in src/streamer/data/frequency_variants/2mins/');
            console.log('  3. Ensure no other instances are running');
            console.log('  4. Check if MQTT broker is available');
        }
        
    } catch (error) {
        console.error('\\n💥 Quick test crashed:', error.message);
        console.error(error.stack);
    } finally {
        // Restore original configuration
        Object.assign(experimentModule.CONFIG, originalConfig);
        
        // Clean up any lingering processes
        try {
            execSync('pkill -f StreamingQuery', { stdio: 'ignore' });
            execSync('pkill -f publish.js', { stdio: 'ignore' });
        } catch (e) {
            // Ignore cleanup errors
        }
    }
}

// Run quick test if called directly
if (require.main === module) {
    quickTest().catch(console.error);
}

module.exports = { quickTest, QUICK_CONFIG };
