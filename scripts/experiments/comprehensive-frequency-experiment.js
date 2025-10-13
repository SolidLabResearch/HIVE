#!/usr/bin/env node
/**
 * Comprehensive Streaming Query Approaches Experiment Framework
 * 
 * Tests all approaches across different frequencies:
 * - Fetching Client Side (Ground Truth)
 * - Query Chunk Approach 
 * - Approximation Approach
 * - Independent Full Processing (Baseline)
 * 
 * Frequencies: 4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz
 * Devices: smartphone, wearable
 * Iterations: 5 per combination
 * 
 * Metrics: Latency, Memory Usage, Accuracy
 */

const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Configuration
const CONFIG = {
    RUNS: 5, // Number of iterations per approach-frequency combination
    FREQUENCIES: ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz'],
    DEVICES: ['smartphone', 'wearable'],
    APPROACHES: [
        {
            name: 'fetching-client-side',
            cmd: ['node', ['dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js']],
            isGroundTruth: true
        },
        {
            name: 'query-chunk',
            cmd: ['node', ['dist/approaches/StreamingQueryChunkedApproachOrchestrator.js']],
            isGroundTruth: false
        },
        {
            name: 'approximation',
            cmd: ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']],
            isGroundTruth: false
        },
        {
            name: 'independent-full-processing',
            cmd: ['node', ['dist/examples/independent-full-processing-demo.js']],
            isGroundTruth: false
        }
    ],
    TIMEOUT_MS: 5 * 60 * 1000, // 5 minutes per run
    BASE_LOGS_DIR: 'logs/comprehensive-experiment',
    DATA_BASE_PATH: 'src/streamer/data/frequency_variants/2mins'
};

// Ensure logs directory exists
if (!fs.existsSync(CONFIG.BASE_LOGS_DIR)) {
    fs.mkdirSync(CONFIG.BASE_LOGS_DIR, { recursive: true });
}

/**
 * Kill any lingering processes from previous runs
 */
function killLingeringProcesses() {
    const processesToKill = [
        'StreamingQueryFetchingClientSideApproachOrchestrator.js',
        'StreamingQueryChunkedApproachOrchestrator.js', 
        'StreamingQueryApproximationApproachOrchestrator.js',
        'independent-full-processing-demo.js',
        'publish.js'
    ];
    
    processesToKill.forEach(proc => {
        try {
            execSync(`pkill -f ${proc}`, { stdio: 'ignore' });
        } catch (e) {
            // Process not found, ignore
        }
    });
    
    // Give processes time to clean up
    setTimeout(() => {}, 1000);
}

/**
 * Setup data source for specific frequency and device
 */
function setupDataSource(frequency, device) {
    const sourcePath = path.join(CONFIG.DATA_BASE_PATH, device, frequency, 'data.nt');
    const publisherDataPath = 'src/streamer/data';
    
    console.log(`  Setting up data source: ${device} ${frequency}`);
    console.log(`    Source: ${sourcePath}`);
    
    // Create device-specific directories in publisher data path
    const deviceDirs = {
        smartphone: `${publisherDataPath}/smartphone.acceleration.x`,
        wearable: `${publisherDataPath}/wearable.acceleration.x`
    };
    
    // Ensure directories exist
    Object.values(deviceDirs).forEach(dir => {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
    });
    
    // Copy frequency-specific data to the expected locations
    const targetPath = path.join(deviceDirs[device], 'data.nt');
    
    if (fs.existsSync(sourcePath)) {
        fs.copyFileSync(sourcePath, targetPath);
        console.log(`    Copied to: ${targetPath}`);
        
        // Verify file size
        const stats = fs.statSync(targetPath);
        console.log(`    File size: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);
        
        // Count observations
        const content = fs.readFileSync(targetPath, 'utf8');
        const lineCount = content.split('\\n').filter(line => line.trim()).length;
        console.log(`    Observations: ${lineCount}`);
        
        return true;
    } else {
        console.error(`    ERROR: Source file not found: ${sourcePath}`);
        return false;
    }
}

/**
 * Run a single experiment iteration
 */
async function runSingleExperiment(approach, frequency, device, iteration) {
    const runId = `${approach.name}_${device}_${frequency}_iter${iteration}`;
    console.log(`\\n--- Starting ${runId} ---`);
    
    // Setup data source for this frequency/device combination
    const dataSetupSuccess = setupDataSource(frequency, device);
    if (!dataSetupSuccess) {
        console.error(`Failed to setup data for ${device} ${frequency}, skipping...`);
        return { success: false, error: 'Data setup failed' };
    }
    
    // Kill any lingering processes
    killLingeringProcesses();
    
    // Create iteration-specific log directory
    const iterationDir = path.join(
        CONFIG.BASE_LOGS_DIR, 
        approach.name, 
        `${device}_${frequency}`, 
        `iteration${iteration}`
    );
    
    if (!fs.existsSync(iterationDir)) {
        fs.mkdirSync(iterationDir, { recursive: true });
    }
    
    const startTime = Date.now();
    
    try {
        // Start the approach
        console.log(`  Starting approach: ${approach.cmd.join(' ')}`);
        const approachProcess = spawn(approach.cmd[0], approach.cmd[1], { 
            stdio: 'pipe',
            env: { ...process.env, EXPERIMENT_RUN_ID: runId }
        });
        
        // Log approach output
        const approachLogPath = path.join(iterationDir, 'approach_output.log');
        const approachLogStream = fs.createWriteStream(approachLogPath);
        approachProcess.stdout.pipe(approachLogStream);
        approachProcess.stderr.pipe(approachLogStream);
        
        // Wait for approach to initialize
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        // Start data publisher
        console.log(`  Starting publisher for ${device} ${frequency} data...`);
        const publisherProcess = spawn('node', ['dist/streamer/src/publish.js'], { 
            stdio: 'pipe',
            env: { ...process.env, FREQUENCY: frequency, DEVICE: device }
        });
        
        // Log publisher output
        const publisherLogPath = path.join(iterationDir, 'publisher_output.log');
        const publisherLogStream = fs.createWriteStream(publisherLogPath);
        publisherProcess.stdout.pipe(publisherLogStream);
        publisherProcess.stderr.pipe(publisherLogStream);
        
        // Setup timeout
        const timeout = setTimeout(() => {
            console.log(`  Timeout reached for ${runId}, terminating...`);
            approachProcess.kill('SIGTERM');
            publisherProcess.kill('SIGTERM');
            killLingeringProcesses();
        }, CONFIG.TIMEOUT_MS);
        
        // Wait for processes to complete
        await Promise.all([
            new Promise((resolve) => {
                publisherProcess.on('exit', (code) => {
                    console.log(`  Publisher exited with code: ${code}`);
                    resolve();
                });
            }),
            new Promise((resolve) => {
                approachProcess.on('exit', (code) => {
                    console.log(`  Approach exited with code: ${code}`);
                    resolve();
                });
            })
        ]);
        
        clearTimeout(timeout);
        
        // Clean up any remaining processes
        killLingeringProcesses();
        
        const endTime = Date.now();
        const duration = endTime - startTime;
        
        // Move log files to iteration directory
        const logFiles = [
            'fetching_client_side_log.csv',
            'fetching_client_side_resource_usage.csv',
            'approximation_approach_log.csv',
            'streaming_query_chunk_aggregator_log.csv',
            'independent_processing_log.csv',
            'replayer-log.csv'
        ];
        
        logFiles.forEach(logFile => {
            if (fs.existsSync(logFile)) {
                const targetPath = path.join(iterationDir, logFile);
                fs.renameSync(logFile, targetPath);
                console.log(`  Moved log: ${logFile} -> ${targetPath}`);
            }
        });
        
        // Create run metadata
        const metadata = {
            runId,
            approach: approach.name,
            frequency,
            device,
            iteration,
            startTime,
            endTime,
            duration,
            isGroundTruth: approach.isGroundTruth,
            timestamp: new Date().toISOString()
        };
        
        fs.writeFileSync(
            path.join(iterationDir, 'metadata.json'), 
            JSON.stringify(metadata, null, 2)
        );
        
        console.log(`  ✅ Completed ${runId} in ${(duration/1000).toFixed(1)}s`);
        
        return { 
            success: true, 
            duration, 
            iterationDir,
            metadata 
        };
        
    } catch (error) {
        console.error(`  ❌ Error in ${runId}:`, error.message);
        killLingeringProcesses();
        
        return { 
            success: false, 
            error: error.message,
            duration: Date.now() - startTime
        };
    }
}

/**
 * Run all experiments for a specific approach across all frequencies
 */
async function runApproachExperiments(approach) {
    console.log(`\\n🚀 Starting experiments for approach: ${approach.name.toUpperCase()}`);
    console.log(`   ${approach.isGroundTruth ? '(Ground Truth)' : '(Test Approach)'}`);
    
    const results = [];
    
    for (const frequency of CONFIG.FREQUENCIES) {
        for (const device of CONFIG.DEVICES) {
            console.log(`\\n📊 Testing ${approach.name} with ${device} ${frequency}...`);
            
            for (let iteration = 1; iteration <= CONFIG.RUNS; iteration++) {
                const result = await runSingleExperiment(approach, frequency, device, iteration);
                results.push({
                    approach: approach.name,
                    frequency,
                    device,
                    iteration,
                    ...result
                });
                
                // Brief pause between iterations
                if (iteration < CONFIG.RUNS) {
                    console.log(`  Pausing before next iteration...`);
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
        }
    }
    
    return results;
}

/**
 * Generate experiment summary report
 */
function generateSummaryReport(allResults) {
    const summaryPath = path.join(CONFIG.BASE_LOGS_DIR, 'experiment_summary.json');
    const csvSummaryPath = path.join(CONFIG.BASE_LOGS_DIR, 'experiment_summary.csv');
    
    // JSON Summary
    const summary = {
        experimentConfig: CONFIG,
        totalRuns: allResults.length,
        successfulRuns: allResults.filter(r => r.success).length,
        failedRuns: allResults.filter(r => !r.success).length,
        approaches: CONFIG.APPROACHES.map(a => a.name),
        frequencies: CONFIG.FREQUENCIES,
        devices: CONFIG.DEVICES,
        results: allResults,
        timestamp: new Date().toISOString()
    };
    
    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
    
    // CSV Summary
    const csvHeaders = [
        'approach', 'frequency', 'device', 'iteration', 
        'success', 'duration_ms', 'timestamp', 'is_ground_truth'
    ];
    
    const csvRows = allResults.map(result => [
        result.approach,
        result.frequency,
        result.device,
        result.iteration,
        result.success,
        result.duration || 0,
        result.metadata?.timestamp || '',
        CONFIG.APPROACHES.find(a => a.name === result.approach)?.isGroundTruth || false
    ]);
    
    const csvContent = [
        csvHeaders.join(','),
        ...csvRows.map(row => row.join(','))
    ].join('\\n');
    
    fs.writeFileSync(csvSummaryPath, csvContent);
    
    console.log(`\\n📋 Summary report generated:`);
    console.log(`   JSON: ${summaryPath}`);
    console.log(`   CSV: ${csvSummaryPath}`);
    
    return summary;
}

/**
 * Main experiment execution
 */
async function main() {
    console.log('🧪 COMPREHENSIVE STREAMING QUERY APPROACHES EXPERIMENT');
    console.log('=' * 60);
    console.log(`Approaches: ${CONFIG.APPROACHES.map(a => a.name).join(', ')}`);
    console.log(`Frequencies: ${CONFIG.FREQUENCIES.join(', ')}`);
    console.log(`Devices: ${CONFIG.DEVICES.join(', ')}`);
    console.log(`Iterations per combination: ${CONFIG.RUNS}`);
    console.log(`Total expected runs: ${CONFIG.APPROACHES.length * CONFIG.FREQUENCIES.length * CONFIG.DEVICES.length * CONFIG.RUNS}`);
    console.log(`Logs directory: ${CONFIG.BASE_LOGS_DIR}`);
    console.log('=' * 60);
    
    const startTime = Date.now();
    const allResults = [];
    
    try {
        // Run experiments for each approach
        for (const approach of CONFIG.APPROACHES) {
            const approachResults = await runApproachExperiments(approach);
            allResults.push(...approachResults);
            
            console.log(`\\n✅ Completed all experiments for ${approach.name}`);
            console.log(`   Successful: ${approachResults.filter(r => r.success).length}/${approachResults.length}`);
        }
        
        const endTime = Date.now();
        const totalDuration = endTime - startTime;
        
        console.log(`\\n🎉 ALL EXPERIMENTS COMPLETED!`);
        console.log(`   Total duration: ${(totalDuration / 1000 / 60).toFixed(1)} minutes`);
        console.log(`   Total runs: ${allResults.length}`);
        console.log(`   Successful: ${allResults.filter(r => r.success).length}`);
        console.log(`   Failed: ${allResults.filter(r => !r.success).length}`);
        
        // Generate summary report
        const summary = generateSummaryReport(allResults);
        
        console.log(`\\n📊 Next Steps:`);
        console.log(`   1. Analyze results in: ${CONFIG.BASE_LOGS_DIR}`);
        console.log(`   2. Compare metrics: latency, memory, accuracy`);
        console.log(`   3. Ground truth data: fetching-client-side approach`);
        console.log(`   4. Run analysis scripts for visualization`);
        
    } catch (error) {
        console.error(`\\n❌ Experiment failed:`, error);
        killLingeringProcesses();
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\\n🛑 Experiment interrupted, cleaning up...');
    killLingeringProcesses();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\\n🛑 Experiment terminated, cleaning up...');
    killLingeringProcesses();
    process.exit(0);
});

// Run the experiment
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { main, runSingleExperiment, CONFIG };
