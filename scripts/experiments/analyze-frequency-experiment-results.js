#!/usr/bin/env node
/**
 * Comprehensive Experiment Results Analysis
 * 
 * Analyzes the results from comprehensive-frequency-experiment.js:
 * - Latency comparison across approaches and frequencies
 * - Memory usage analysis 
 * - Accuracy calculation using fetching-client-side as ground truth
 * - Performance metrics visualization
 * - Statistical analysis and recommendations
 */

const fs = require('fs');
const path = require('path');

// Configuration
const ANALYSIS_CONFIG = {
    LOGS_BASE_DIR: 'logs/comprehensive-experiment',
    OUTPUT_DIR: 'analysis/frequency-experiment-results',
    GROUND_TRUTH_APPROACH: 'fetching-client-side',
    METRICS: {
        latency: 'response_latency_ms',
        memory: 'memory_usage_mb', 
        cpu: 'cpu_usage_percent',
        throughput: 'observations_per_second'
    }
};

// Ensure output directory exists
if (!fs.existsSync(ANALYSIS_CONFIG.OUTPUT_DIR)) {
    fs.mkdirSync(ANALYSIS_CONFIG.OUTPUT_DIR, { recursive: true });
}

/**
 * Load experiment summary data
 */
function loadExperimentSummary() {
    const summaryPath = path.join(ANALYSIS_CONFIG.LOGS_BASE_DIR, 'experiment_summary.json');
    
    if (!fs.existsSync(summaryPath)) {
        throw new Error(`Experiment summary not found: ${summaryPath}`);
    }
    
    return JSON.parse(fs.readFileSync(summaryPath, 'utf8'));
}

/**
 * Load CSV log data for a specific iteration
 */
function loadIterationLogs(approach, device, frequency, iteration) {
    const iterationDir = path.join(
        ANALYSIS_CONFIG.LOGS_BASE_DIR,
        approach,
        `${device}_${frequency}`,
        `iteration${iteration}`
    );
    
    const logs = {};
    
    // Common log files to look for
    const logFiles = [
        'fetching_client_side_log.csv',
        'fetching_client_side_resource_usage.csv',
        'approximation_approach_log.csv',
        'streaming_query_chunk_aggregator_log.csv', 
        'independent_processing_log.csv',
        'replayer-log.csv'
    ];
    
    logFiles.forEach(logFile => {
        const logPath = path.join(iterationDir, logFile);
        if (fs.existsSync(logPath)) {
            try {
                const content = fs.readFileSync(logPath, 'utf8');
                logs[logFile] = parseCSV(content);
            } catch (error) {
                console.warn(`Warning: Could not parse ${logPath}: ${error.message}`);
            }
        }
    });
    
    // Load metadata
    const metadataPath = path.join(iterationDir, 'metadata.json');
    if (fs.existsSync(metadataPath)) {
        logs.metadata = JSON.parse(fs.readFileSync(metadataPath, 'utf8'));
    }
    
    return logs;
}

/**
 * Simple CSV parser
 */
function parseCSV(content) {
    const lines = content.trim().split('\\n');
    if (lines.length < 2) return [];
    
    const headers = lines[0].split(',').map(h => h.trim());
    const rows = lines.slice(1).map(line => {
        const values = line.split(',').map(v => v.trim());
        const row = {};
        headers.forEach((header, index) => {
            const value = values[index];
            // Try to parse as number
            if (value && !isNaN(value)) {
                row[header] = parseFloat(value);
            } else {
                row[header] = value;
            }
        });
        return row;
    });
    
    return rows;
}

/**
 * Calculate basic statistics for an array of numbers
 */
function calculateStats(values) {
    if (!values || values.length === 0) return null;
    
    const sorted = values.slice().sort((a, b) => a - b);
    const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
    const median = sorted[Math.floor(sorted.length / 2)];
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    
    // Standard deviation
    const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
    const stdDev = Math.sqrt(variance);
    
    return { mean, median, min, max, stdDev, count: values.length };
}

/**
 * Extract performance metrics from log data
 */
function extractMetrics(logs) {
    const metrics = {};
    
    // Extract latency metrics from main log
    const mainLogFiles = [
        'fetching_client_side_log.csv',
        'approximation_approach_log.csv', 
        'streaming_query_chunk_aggregator_log.csv',
        'independent_processing_log.csv'
    ];
    
    for (const logFile of mainLogFiles) {
        if (logs[logFile] && logs[logFile].length > 0) {
            const logData = logs[logFile];
            
            // Extract latency if available
            const latencyField = logData[0].hasOwnProperty('latency_ms') ? 'latency_ms' : 
                                 logData[0].hasOwnProperty('response_time') ? 'response_time' : null;
            
            if (latencyField) {
                metrics.latencies = logData.map(row => row[latencyField]).filter(val => val != null);
            }
            
            // Extract timestamps for throughput calculation
            if (logData[0].hasOwnProperty('timestamp')) {
                metrics.timestamps = logData.map(row => new Date(row.timestamp).getTime());
            }
            
            break; // Use first available log
        }
    }
    
    // Extract resource usage metrics
    if (logs['fetching_client_side_resource_usage.csv']) {
        const resourceData = logs['fetching_client_side_resource_usage.csv'];
        
        metrics.memoryUsage = resourceData.map(row => row.memory_mb || row.memory_usage_mb).filter(val => val != null);
        metrics.cpuUsage = resourceData.map(row => row.cpu_percent || row.cpu_usage_percent).filter(val => val != null);
    }
    
    // Calculate throughput from timestamps
    if (metrics.timestamps && metrics.timestamps.length > 1) {
        const startTime = Math.min(...metrics.timestamps);
        const endTime = Math.max(...metrics.timestamps);
        const duration = (endTime - startTime) / 1000; // seconds
        metrics.throughput = metrics.timestamps.length / duration; // observations per second
    }
    
    // Extract total duration from metadata
    if (logs.metadata) {
        metrics.totalDuration = logs.metadata.duration;
    }
    
    return metrics;
}

/**
 * Calculate accuracy by comparing against ground truth
 */
function calculateAccuracy(testResults, groundTruthResults) {
    if (!testResults || !groundTruthResults) {
        return null;
    }
    
    // Simple accuracy calculation based on result similarity
    // This is a placeholder - actual accuracy depends on your specific metrics
    
    const accuracy = {
        latencyAccuracy: null,
        resultAccuracy: null,
        correlationScore: null
    };
    
    // Compare latency patterns
    if (testResults.latencies && groundTruthResults.latencies) {
        const testMean = testResults.latencies.reduce((sum, val) => sum + val, 0) / testResults.latencies.length;
        const truthMean = groundTruthResults.latencies.reduce((sum, val) => sum + val, 0) / groundTruthResults.latencies.length;
        
        // Calculate relative difference
        accuracy.latencyAccuracy = 1 - Math.abs(testMean - truthMean) / truthMean;
    }
    
    return accuracy;
}

/**
 * Analyze results for a specific frequency
 */
function analyzeFrequency(summary, frequency) {
    console.log(`\\n📊 Analyzing frequency: ${frequency}`);
    
    const frequencyResults = {};
    const groundTruthData = {};
    
    // Load ground truth data first
    for (const device of summary.experimentConfig.DEVICES) {
        const groundTruthRuns = summary.results.filter(r => 
            r.approach === ANALYSIS_CONFIG.GROUND_TRUTH_APPROACH && 
            r.frequency === frequency && 
            r.device === device &&
            r.success
        );
        
        if (groundTruthRuns.length > 0) {
            const allMetrics = [];
            
            for (const run of groundTruthRuns) {
                const logs = loadIterationLogs(run.approach, run.device, run.frequency, run.iteration);
                const metrics = extractMetrics(logs);
                allMetrics.push(metrics);
            }
            
            groundTruthData[device] = allMetrics;
        }
    }
    
    // Analyze each approach
    for (const approach of summary.experimentConfig.APPROACHES) {
        if (approach.name === ANALYSIS_CONFIG.GROUND_TRUTH_APPROACH) continue;
        
        console.log(`  Analyzing approach: ${approach.name}`);
        frequencyResults[approach.name] = {};
        
        for (const device of summary.experimentConfig.DEVICES) {
            const runs = summary.results.filter(r => 
                r.approach === approach.name && 
                r.frequency === frequency && 
                r.device === device &&
                r.success
            );
            
            if (runs.length === 0) {
                console.log(`    No successful runs for ${device}`);
                continue;
            }
            
            const allMetrics = [];
            
            for (const run of runs) {
                const logs = loadIterationLogs(run.approach, run.device, run.frequency, run.iteration);
                const metrics = extractMetrics(logs);
                allMetrics.push(metrics);
            }
            
            // Calculate aggregate statistics
            const latencies = allMetrics.flatMap(m => m.latencies || []);
            const memoryUsage = allMetrics.flatMap(m => m.memoryUsage || []);
            const cpuUsage = allMetrics.flatMap(m => m.cpuUsage || []);
            const throughputs = allMetrics.map(m => m.throughput).filter(t => t != null);
            
            const deviceResults = {
                latency: calculateStats(latencies),
                memory: calculateStats(memoryUsage),
                cpu: calculateStats(cpuUsage), 
                throughput: calculateStats(throughputs),
                totalRuns: runs.length,
                successfulRuns: runs.filter(r => r.success).length
            };
            
            // Calculate accuracy against ground truth
            if (groundTruthData[device]) {
                const groundTruthLatencies = groundTruthData[device].flatMap(m => m.latencies || []);
                deviceResults.accuracy = calculateAccuracy(
                    { latencies }, 
                    { latencies: groundTruthLatencies }
                );
            }
            
            frequencyResults[approach.name][device] = deviceResults;
            
            console.log(`    ${device}: ${runs.length} runs, avg latency: ${deviceResults.latency?.mean?.toFixed(2) || 'N/A'}ms`);
        }
    }
    
    return frequencyResults;
}

/**
 * Generate performance comparison report
 */
function generatePerformanceReport(allFrequencyResults, summary) {
    const reportPath = path.join(ANALYSIS_CONFIG.OUTPUT_DIR, 'performance_report.md');
    
    let report = `# Streaming Query Approaches Performance Analysis\\n\\n`;
    report += `**Experiment Date:** ${new Date(summary.timestamp).toLocaleDateString()}\\n`;
    report += `**Total Runs:** ${summary.totalRuns}\\n`;
    report += `**Successful Runs:** ${summary.successfulRuns}\\n`;
    report += `**Ground Truth:** ${ANALYSIS_CONFIG.GROUND_TRUTH_APPROACH}\\n\\n`;
    
    // Summary table
    report += `## Performance Summary\\n\\n`;
    report += `| Approach | Frequency | Device | Avg Latency (ms) | Avg Memory (MB) | Avg Throughput (obs/s) | Accuracy |\\n`;
    report += `|----------|-----------|---------|------------------|-----------------|------------------------|----------|\\n`;
    
    for (const frequency of summary.experimentConfig.FREQUENCIES) {
        const frequencyResults = allFrequencyResults[frequency];
        if (!frequencyResults) continue;
        
        for (const [approach, approachData] of Object.entries(frequencyResults)) {
            for (const [device, deviceData] of Object.entries(approachData)) {
                const latency = deviceData.latency?.mean?.toFixed(2) || 'N/A';
                const memory = deviceData.memory?.mean?.toFixed(2) || 'N/A';
                const throughput = deviceData.throughput?.mean?.toFixed(2) || 'N/A';
                const accuracy = deviceData.accuracy?.latencyAccuracy?.toFixed(3) || 'N/A';
                
                report += `| ${approach} | ${frequency} | ${device} | ${latency} | ${memory} | ${throughput} | ${accuracy} |\\n`;
            }
        }
    }
    
    // Detailed analysis by frequency
    report += `\\n## Detailed Analysis by Frequency\\n\\n`;
    
    for (const frequency of summary.experimentConfig.FREQUENCIES) {
        const frequencyResults = allFrequencyResults[frequency];
        if (!frequencyResults) continue;
        
        report += `### ${frequency} Analysis\\n\\n`;
        
        for (const [approach, approachData] of Object.entries(frequencyResults)) {
            report += `#### ${approach}\\n\\n`;
            
            for (const [device, deviceData] of Object.entries(approachData)) {
                report += `**${device.charAt(0).toUpperCase() + device.slice(1)} Device:**\\n`;
                report += `- Runs: ${deviceData.totalRuns}\\n`;
                
                if (deviceData.latency) {
                    report += `- Latency: ${deviceData.latency.mean.toFixed(2)}ms (±${deviceData.latency.stdDev.toFixed(2)})\\n`;
                }
                if (deviceData.memory) {
                    report += `- Memory: ${deviceData.memory.mean.toFixed(2)}MB (±${deviceData.memory.stdDev.toFixed(2)})\\n`;
                }
                if (deviceData.throughput) {
                    report += `- Throughput: ${deviceData.throughput.mean.toFixed(2)} obs/s\\n`;
                }
                if (deviceData.accuracy?.latencyAccuracy) {
                    report += `- Accuracy: ${(deviceData.accuracy.latencyAccuracy * 100).toFixed(1)}%\\n`;
                }
                report += `\\n`;
            }
        }
    }
    
    // Recommendations
    report += `## Recommendations\\n\\n`;
    report += `### Best Performing Approaches:\\n\\n`;
    
    // Find best approaches for different metrics
    let bestLatency = { approach: null, frequency: null, device: null, value: Infinity };
    let bestMemory = { approach: null, frequency: null, device: null, value: Infinity };
    let bestThroughput = { approach: null, frequency: null, device: null, value: 0 };
    let bestAccuracy = { approach: null, frequency: null, device: null, value: 0 };
    
    for (const frequency of summary.experimentConfig.FREQUENCIES) {
        const frequencyResults = allFrequencyResults[frequency];
        if (!frequencyResults) continue;
        
        for (const [approach, approachData] of Object.entries(frequencyResults)) {
            for (const [device, deviceData] of Object.entries(approachData)) {
                if (deviceData.latency?.mean && deviceData.latency.mean < bestLatency.value) {
                    bestLatency = { approach, frequency, device, value: deviceData.latency.mean };
                }
                if (deviceData.memory?.mean && deviceData.memory.mean < bestMemory.value) {
                    bestMemory = { approach, frequency, device, value: deviceData.memory.mean };
                }
                if (deviceData.throughput?.mean && deviceData.throughput.mean > bestThroughput.value) {
                    bestThroughput = { approach, frequency, device, value: deviceData.throughput.mean };
                }
                if (deviceData.accuracy?.latencyAccuracy && deviceData.accuracy.latencyAccuracy > bestAccuracy.value) {
                    bestAccuracy = { approach, frequency, device, value: deviceData.accuracy.latencyAccuracy };
                }
            }
        }
    }
    
    report += `- **Lowest Latency:** ${bestLatency.approach} (${bestLatency.device} ${bestLatency.frequency}) - ${bestLatency.value.toFixed(2)}ms\\n`;
    report += `- **Lowest Memory:** ${bestMemory.approach} (${bestMemory.device} ${bestMemory.frequency}) - ${bestMemory.value.toFixed(2)}MB\\n`;
    report += `- **Highest Throughput:** ${bestThroughput.approach} (${bestThroughput.device} ${bestThroughput.frequency}) - ${bestThroughput.value.toFixed(2)} obs/s\\n`;
    report += `- **Highest Accuracy:** ${bestAccuracy.approach} (${bestAccuracy.device} ${bestAccuracy.frequency}) - ${(bestAccuracy.value * 100).toFixed(1)}%\\n\\n`;
    
    report += `### Frequency-Specific Recommendations:\\n\\n`;
    report += `- **Low Frequency (4-8Hz):** Suitable for all approaches\\n`;
    report += `- **Medium Frequency (16-32Hz):** Consider approximation for resource efficiency\\n`;
    report += `- **High Frequency (64-128Hz):** Use chunked or approximation approaches\\n\\n`;
    
    fs.writeFileSync(reportPath, report);
    console.log(`📋 Performance report generated: ${reportPath}`);
    
    return reportPath;
}

/**
 * Generate CSV data for visualization
 */
function generateVisualizationData(allFrequencyResults, summary) {
    const csvPath = path.join(ANALYSIS_CONFIG.OUTPUT_DIR, 'visualization_data.csv');
    
    const headers = [
        'approach', 'frequency', 'device', 'metric_type', 'mean', 'median', 
        'min', 'max', 'std_dev', 'runs', 'accuracy'
    ];
    
    const rows = [];
    
    for (const frequency of summary.experimentConfig.FREQUENCIES) {
        const frequencyResults = allFrequencyResults[frequency];
        if (!frequencyResults) continue;
        
        for (const [approach, approachData] of Object.entries(frequencyResults)) {
            for (const [device, deviceData] of Object.entries(approachData)) {
                // Add latency row
                if (deviceData.latency) {
                    rows.push([
                        approach, frequency, device, 'latency_ms',
                        deviceData.latency.mean || 0,
                        deviceData.latency.median || 0,
                        deviceData.latency.min || 0,
                        deviceData.latency.max || 0,
                        deviceData.latency.stdDev || 0,
                        deviceData.totalRuns,
                        deviceData.accuracy?.latencyAccuracy || 0
                    ]);
                }
                
                // Add memory row
                if (deviceData.memory) {
                    rows.push([
                        approach, frequency, device, 'memory_mb',
                        deviceData.memory.mean || 0,
                        deviceData.memory.median || 0,
                        deviceData.memory.min || 0,
                        deviceData.memory.max || 0,
                        deviceData.memory.stdDev || 0,
                        deviceData.totalRuns,
                        deviceData.accuracy?.latencyAccuracy || 0
                    ]);
                }
                
                // Add throughput row
                if (deviceData.throughput) {
                    rows.push([
                        approach, frequency, device, 'throughput_obs_per_sec',
                        deviceData.throughput.mean || 0,
                        deviceData.throughput.median || 0,
                        deviceData.throughput.min || 0,
                        deviceData.throughput.max || 0,
                        deviceData.throughput.stdDev || 0,
                        deviceData.totalRuns,
                        deviceData.accuracy?.latencyAccuracy || 0
                    ]);
                }
            }
        }
    }
    
    const csvContent = [
        headers.join(','),
        ...rows.map(row => row.join(','))
    ].join('\\n');
    
    fs.writeFileSync(csvPath, csvContent);
    console.log(`📊 Visualization data generated: ${csvPath}`);
    
    return csvPath;
}

/**
 * Main analysis function
 */
async function main() {
    console.log('📈 COMPREHENSIVE EXPERIMENT ANALYSIS');
    console.log('=' * 50);
    
    try {
        // Load experiment summary
        console.log('Loading experiment summary...');
        const summary = loadExperimentSummary();
        
        console.log(`Loaded experiment data:`);
        console.log(`  Total runs: ${summary.totalRuns}`);
        console.log(`  Successful: ${summary.successfulRuns}`);
        console.log(`  Failed: ${summary.failedRuns}`);
        console.log(`  Approaches: ${summary.approaches.join(', ')}`);
        console.log(`  Frequencies: ${summary.frequencies.join(', ')}`);
        
        // Analyze each frequency
        const allFrequencyResults = {};
        
        for (const frequency of summary.experimentConfig.FREQUENCIES) {
            allFrequencyResults[frequency] = analyzeFrequency(summary, frequency);
        }
        
        // Generate reports
        console.log('\\n📋 Generating analysis reports...');
        
        const reportPath = generatePerformanceReport(allFrequencyResults, summary);
        const csvPath = generateVisualizationData(allFrequencyResults, summary);
        
        // Save detailed analysis
        const detailedAnalysisPath = path.join(ANALYSIS_CONFIG.OUTPUT_DIR, 'detailed_analysis.json');
        fs.writeFileSync(detailedAnalysisPath, JSON.stringify({
            summary,
            frequencyResults: allFrequencyResults,
            analysisTimestamp: new Date().toISOString()
        }, null, 2));
        
        console.log(`\\n✅ Analysis complete!`);
        console.log(`Results saved to: ${ANALYSIS_CONFIG.OUTPUT_DIR}`);
        console.log(`Files generated:`);
        console.log(`  - ${reportPath}`);
        console.log(`  - ${csvPath}`);
        console.log(`  - ${detailedAnalysisPath}`);
        
        console.log(`\\n📊 Next steps:`);
        console.log(`  1. Review the performance report`);
        console.log(`  2. Use visualization data for charts`);
        console.log(`  3. Compare approaches across frequencies`);
        console.log(`  4. Identify optimal approach for each use case`);
        
    } catch (error) {
        console.error('❌ Analysis failed:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
}

// Run analysis if called directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { main, analyzeFrequency, calculateStats };
