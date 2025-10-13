#!/usr/bin/env node

/**
 * Comprehensive Query Performance Analysis
 * 
 * This script provides a comprehensive analysis of all three approaches,
 * calculating event latencies, memory usage, and CPU usage with detailed
 * summaries and comparisons.
 */

const fs = require('fs');
const path = require('path');

console.log('🔍 COMPREHENSIVE QUERY PERFORMANCE ANALYSIS');
console.log('=' .repeat(80));

const LOGS_BASE_DIR = '/Users/kushbisen/Code/streaming-query-hive/tools/experiments/logs';
const APPROACHES = ['fetching-client-side', 'streaming-query-hive', 'approximation-approach'];
const FREQUENCIES = ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz'];

/**
 * Extract query registration and first result timestamps
 */
function extractLatencyMetrics(logPath, approach) {
    if (!fs.existsSync(logPath)) return null;
    
    const content = fs.readFileSync(logPath, 'utf8');
    const lines = content.split('\n');
    
    let registrationTime = null;
    let firstResultTime = null;
    
    const registrationPatterns = {
        'fetching-client-side': ['fetching_query_registered'],
        'streaming-query-hive': ['Registering main streaming query hive query', 'Starting streaming query hive execution'],
        'approximation-approach': ['Registered query:', 'Sub-queries added:']
    };
    
    const resultPatterns = {
        'fetching-client-side': ['RStream result generated:', 'Processing valid result:'],
        'streaming-query-hive': ['Successfully published result:', 'Published aggregated result:'],
        'approximation-approach': ['Successfully published unified cross-sensor average:']
    };
    
    const regPatterns = registrationPatterns[approach] || [];
    const resPatterns = resultPatterns[approach] || [];
    
    for (const line of lines) {
        if (line.includes('timestamp,message')) continue;
        
        const parts = line.split(',');
        if (parts.length < 2) continue;
        
        const timestamp = parseInt(parts[0]);
        const message = parts[1] ? parts[1].replace(/"/g, '') : '';
        
        if (isNaN(timestamp)) continue;
        
        // Look for registration
        if (!registrationTime) {
            for (const pattern of regPatterns) {
                if (message.includes(pattern)) {
                    registrationTime = timestamp;
                    break;
                }
            }
        }
        
        // Look for first result
        if (!firstResultTime && registrationTime) {
            for (const pattern of resPatterns) {
                if (message.includes(pattern)) {
                    firstResultTime = timestamp;
                    break;
                }
            }
        }
        
        if (registrationTime && firstResultTime) break;
    }
    
    return {
        registrationTime,
        firstResultTime,
        latencyMs: (registrationTime && firstResultTime) ? firstResultTime - registrationTime : null
    };
}

/**
 * Extract resource usage statistics
 */
function extractResourceUsage(resourcePath) {
    if (!fs.existsSync(resourcePath)) return null;
    
    const content = fs.readFileSync(resourcePath, 'utf8');
    const lines = content.split('\n');
    
    let cpuUser = [];
    let cpuSystem = [];
    let memoryRSS = [];
    let memoryHeap = [];
    
    for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(',');
        if (parts.length < 8) continue;
        
        try {
            cpuUser.push(parseFloat(parts[1]));
            cpuSystem.push(parseFloat(parts[2]));
            memoryRSS.push(parseInt(parts[3]) / (1024 * 1024)); // Convert to MB
            memoryHeap.push(parseFloat(parts[6])); // heapUsedMB
        } catch (e) {
            // Skip invalid lines
        }
    }
    
    if (cpuUser.length === 0) return null;
    
    return {
        avgCPUUser: cpuUser.reduce((a, b) => a + b, 0) / cpuUser.length,
        avgCPUSystem: cpuSystem.reduce((a, b) => a + b, 0) / cpuSystem.length,
        avgCPUTotal: (cpuUser.reduce((a, b) => a + b, 0) + cpuSystem.reduce((a, b) => a + b, 0)) / cpuUser.length,
        maxCPUUser: Math.max(...cpuUser),
        maxCPUSystem: Math.max(...cpuSystem),
        avgMemoryRSS: memoryRSS.reduce((a, b) => a + b, 0) / memoryRSS.length,
        avgMemoryHeap: memoryHeap.reduce((a, b) => a + b, 0) / memoryHeap.length,
        maxMemoryRSS: Math.max(...memoryRSS),
        maxMemoryHeap: Math.max(...memoryHeap),
        samples: cpuUser.length
    };
}

/**
 * Get file paths for approach and frequency
 */
function getFilePaths(approach, frequency, iteration = 'iteration1') {
    const basePath = path.join(LOGS_BASE_DIR, approach, `${frequency}_combined`, iteration);
    
    const logFiles = {
        'fetching-client-side': 'fetching_client_side_log.csv',
        'streaming-query-hive': 'streaming_query_hive_log.csv',
        'approximation-approach': 'approximation_approach_log.csv'
    };
    
    const resourceFiles = {
        'fetching-client-side': 'fetching_client_side_resource_usage.csv',
        'streaming-query-hive': 'streaming_query_hive_resource_usage.csv',
        'approximation-approach': 'approximation_approach_resource_usage.csv'
    };
    
    // Also check chunk aggregator for streaming-query-hive
    const alternativeLogFiles = {
        'streaming-query-hive': 'streaming_query_chunk_aggregator_log.csv'
    };
    
    const mainLog = path.join(basePath, logFiles[approach]);
    const alternativeLog = alternativeLogFiles[approach] ? path.join(basePath, alternativeLogFiles[approach]) : null;
    const resourceLog = path.join(basePath, resourceFiles[approach]);
    
    return { mainLog, alternativeLog, resourceLog };
}

/**
 * Analyze single approach-frequency combination
 */
function analyzeConfiguration(approach, frequency) {
    const results = [];
    
    // Check all iterations
    for (let iteration = 1; iteration <= 3; iteration++) {
        const iterationName = `iteration${iteration}`;
        const paths = getFilePaths(approach, frequency, iterationName);
        
        // Try main log first, then alternative
        let logPath = paths.mainLog;
        if (!fs.existsSync(logPath) && paths.alternativeLog) {
            logPath = paths.alternativeLog;
        }
        
        if (!fs.existsSync(logPath)) {
            continue;
        }
        
        const latencyMetrics = extractLatencyMetrics(logPath, approach);
        const resourceUsage = extractResourceUsage(paths.resourceLog);
        
        if (latencyMetrics || resourceUsage) {
            results.push({
                iteration,
                latency: latencyMetrics,
                resources: resourceUsage
            });
        }
    }
    
    return results;
}

/**
 * Calculate statistics from multiple iterations
 */
function calculateStats(results) {
    if (results.length === 0) return null;
    
    // Latency stats
    const validLatencies = results.filter(r => r.latency && r.latency.latencyMs !== null).map(r => r.latency.latencyMs);
    const latencyStats = validLatencies.length > 0 ? {
        avg: validLatencies.reduce((a, b) => a + b, 0) / validLatencies.length,
        min: Math.min(...validLatencies),
        max: Math.max(...validLatencies),
        count: validLatencies.length
    } : null;
    
    // Resource stats
    const validResources = results.filter(r => r.resources).map(r => r.resources);
    const resourceStats = validResources.length > 0 ? {
        avgCPUTotal: validResources.reduce((sum, r) => sum + r.avgCPUTotal, 0) / validResources.length,
        avgMemoryRSS: validResources.reduce((sum, r) => sum + r.avgMemoryRSS, 0) / validResources.length,
        avgMemoryHeap: validResources.reduce((sum, r) => sum + r.avgMemoryHeap, 0) / validResources.length,
        maxCPUTotal: Math.max(...validResources.map(r => r.avgCPUTotal)),
        maxMemoryRSS: Math.max(...validResources.map(r => r.maxMemoryRSS)),
        maxMemoryHeap: Math.max(...validResources.map(r => r.maxMemoryHeap)),
        samples: validResources.reduce((sum, r) => sum + r.samples, 0) / validResources.length
    } : null;
    
    return {
        iterations: results.length,
        latency: latencyStats,
        resources: resourceStats,
        hasLatencyData: validLatencies.length > 0,
        hasResourceData: validResources.length > 0
    };
}

/**
 * Main analysis
 */
function runMainAnalysis() {
    const analysis = {};
    
    console.log('📊 Analyzing all approaches and frequencies...\n');
    
    for (const approach of APPROACHES) {
        console.log(`🔬 ${approach.toUpperCase().replace(/-/g, ' ')}:`);
        analysis[approach] = {};
        
        for (const frequency of FREQUENCIES) {
            process.stdout.write(`  ${frequency}... `);
            
            const results = analyzeConfiguration(approach, frequency);
            const stats = calculateStats(results);
            
            analysis[approach][frequency] = stats;
            
            if (stats) {
                if (stats.hasLatencyData && stats.hasResourceData) {
                    console.log(`✅ (${stats.latency.avg.toFixed(0)}ms, ${stats.resources.avgCPUTotal.toFixed(1)}% CPU, ${stats.resources.avgMemoryRSS.toFixed(1)}MB)`);
                } else if (stats.hasLatencyData) {
                    console.log(`⚠️  (${stats.latency.avg.toFixed(0)}ms, no resource data)`);
                } else if (stats.hasResourceData) {
                    console.log(`⚠️  (no latency data, ${stats.resources.avgCPUTotal.toFixed(1)}% CPU)`);
                } else {
                    console.log(`❌ (no valid data)`);
                }
            } else {
                console.log(`❌ (no data found)`);
            }
        }
        console.log();
    }
    
    return analysis;
}

/**
 * Generate comprehensive summary table
 */
function generateSummaryTable(analysis) {
    console.log('📋 COMPREHENSIVE PERFORMANCE SUMMARY');
    console.log('=' .repeat(120));
    console.log();
    
    // Header
    console.log('┌─────────────────────────┬─────────┬─────────────┬──────────┬─────────────┬─────────────┬─────────────┐');
    console.log('│ Approach                │ Freq    │ Avg Latency │ CPU Avg  │ Memory RSS  │ Memory Heap │ Status      │');
    console.log('│                         │         │ (ms)        │ (%)      │ (MB)        │ (MB)        │             │');
    console.log('├─────────────────────────┼─────────┼─────────────┼──────────┼─────────────┼─────────────┼─────────────┤');
    
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        
        for (const frequency of FREQUENCIES) {
            const data = analysis[approach][frequency];
            
            const approachName = approach.replace(/-/g, ' ').substring(0, 23).padEnd(23);
            const freq = frequency.padEnd(7);
            
            let latency = 'N/A'.padStart(11);
            let cpu = 'N/A'.padStart(8);
            let memRSS = 'N/A'.padStart(11);
            let memHeap = 'N/A'.padStart(11);
            let status = 'No Data'.padEnd(11);
            
            if (data) {
                if (data.latency) {
                    latency = data.latency.avg.toFixed(0).padStart(11);
                }
                
                if (data.resources) {
                    cpu = data.resources.avgCPUTotal.toFixed(1).padStart(8);
                    memRSS = data.resources.avgMemoryRSS.toFixed(1).padStart(11);
                    memHeap = data.resources.avgMemoryHeap.toFixed(1).padStart(11);
                }
                
                if (data.hasLatencyData && data.hasResourceData) {
                    status = '✅ Complete'.padEnd(11);
                } else if (data.hasLatencyData) {
                    status = '⚠️  Partial'.padEnd(11);
                } else if (data.hasResourceData) {
                    status = '⚠️  Partial'.padEnd(11);
                } else {
                    status = '❌ Failed'.padEnd(11);
                }
            }
            
            console.log(`│ ${approachName} │ ${freq} │ ${latency} │ ${cpu} │ ${memRSS} │ ${memHeap} │ ${status} │`);
        }
    }
    
    console.log('└─────────────────────────┴─────────┴─────────────┴──────────┴─────────────┴─────────────┴─────────────┘');
}

/**
 * Generate approach comparison
 */
function generateApproachComparison(analysis) {
    console.log('\n📊 APPROACH COMPARISON ANALYSIS');
    console.log('=' .repeat(80));
    
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        
        console.log(`\n🔬 ${approach.toUpperCase().replace(/-/g, ' ')}:`);
        console.log('-' .repeat(50));
        
        const validConfigs = [];
        let totalLatency = 0;
        let latencyCount = 0;
        let totalCPU = 0;
        let cpuCount = 0;
        let totalMemory = 0;
        let memoryCount = 0;
        
        for (const frequency of FREQUENCIES) {
            const data = analysis[approach][frequency];
            if (!data) continue;
            
            if (data.hasLatencyData) {
                totalLatency += data.latency.avg;
                latencyCount++;
            }
            
            if (data.hasResourceData) {
                totalCPU += data.resources.avgCPUTotal;
                cpuCount++;
                totalMemory += data.resources.avgMemoryRSS;
                memoryCount++;
            }
            
            if (data.hasLatencyData || data.hasResourceData) {
                validConfigs.push(frequency);
            }
        }
        
        console.log(`   Frequencies with data: ${validConfigs.join(', ')}`);
        
        if (latencyCount > 0) {
            console.log(`   Average latency across all frequencies: ${(totalLatency / latencyCount).toFixed(1)}ms`);
        } else {
            console.log(`   Average latency: No data available`);
        }
        
        if (cpuCount > 0) {
            console.log(`   Average CPU usage: ${(totalCPU / cpuCount).toFixed(1)}%`);
        } else {
            console.log(`   Average CPU usage: No data available`);
        }
        
        if (memoryCount > 0) {
            console.log(`   Average memory usage: ${(totalMemory / memoryCount).toFixed(1)}MB`);
        } else {
            console.log(`   Average memory usage: No data available`);
        }
    }
}

/**
 * Generate performance rankings
 */
function generatePerformanceRankings(analysis) {
    console.log('\n🏆 PERFORMANCE RANKINGS');
    console.log('=' .repeat(80));
    
    const validConfigurations = [];
    
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        
        for (const frequency of FREQUENCIES) {
            const data = analysis[approach][frequency];
            if (!data || !data.hasLatencyData || !data.hasResourceData) continue;
            
            validConfigurations.push({
                approach,
                frequency,
                latency: data.latency.avg,
                cpu: data.resources.avgCPUTotal,
                memory: data.resources.avgMemoryRSS
            });
        }
    }
    
    if (validConfigurations.length === 0) {
        console.log('❌ No complete configurations found for ranking');
        return;
    }
    
    // Sort by latency (lower is better)
    const byLatency = [...validConfigurations].sort((a, b) => a.latency - b.latency);
    
    console.log('\n🚀 BEST LATENCY PERFORMANCE:');
    byLatency.slice(0, 5).forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        console.log(`   ${index + 1}. ${name}: ${config.latency.toFixed(0)}ms (CPU: ${config.cpu.toFixed(1)}%, Memory: ${config.memory.toFixed(1)}MB)`);
    });
    
    console.log('\n🐌 WORST LATENCY PERFORMANCE:');
    byLatency.slice(-3).reverse().forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        console.log(`   ${index + 1}. ${name}: ${config.latency.toFixed(0)}ms (CPU: ${config.cpu.toFixed(1)}%, Memory: ${config.memory.toFixed(1)}MB)`);
    });
    
    // Sort by CPU (lower is better)
    const byCPU = [...validConfigurations].sort((a, b) => a.cpu - b.cpu);
    
    console.log('\n💻 BEST CPU EFFICIENCY:');
    byCPU.slice(0, 3).forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        console.log(`   ${index + 1}. ${name}: ${config.cpu.toFixed(1)}% (Latency: ${config.latency.toFixed(0)}ms, Memory: ${config.memory.toFixed(1)}MB)`);
    });
    
    // Sort by memory (lower is better)
    const byMemory = [...validConfigurations].sort((a, b) => a.memory - b.memory);
    
    console.log('\n🧠 BEST MEMORY EFFICIENCY:');
    byMemory.slice(0, 3).forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        console.log(`   ${index + 1}. ${name}: ${config.memory.toFixed(1)}MB (Latency: ${config.latency.toFixed(0)}ms, CPU: ${config.cpu.toFixed(1)}%)`);
    });
}

/**
 * Generate frequency impact analysis
 */
function generateFrequencyImpactAnalysis(analysis) {
    console.log('\n📈 FREQUENCY IMPACT ANALYSIS');
    console.log('=' .repeat(80));
    
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        
        const validData = [];
        for (const frequency of FREQUENCIES) {
            const data = analysis[approach][frequency];
            if (data && data.hasLatencyData && data.hasResourceData) {
                validData.push({
                    frequency,
                    freqNum: parseInt(frequency.replace('Hz', '')),
                    latency: data.latency.avg,
                    cpu: data.resources.avgCPUTotal,
                    memory: data.resources.avgMemoryRSS
                });
            }
        }
        
        if (validData.length < 2) continue;
        
        console.log(`\n🔬 ${approach.toUpperCase().replace(/-/g, ' ')} - Frequency Impact:`);
        
        // Sort by frequency
        validData.sort((a, b) => a.freqNum - b.freqNum);
        
        const lowestFreq = validData[0];
        const highestFreq = validData[validData.length - 1];
        
        const latencyIncrease = ((highestFreq.latency - lowestFreq.latency) / lowestFreq.latency * 100);
        const cpuIncrease = ((highestFreq.cpu - lowestFreq.cpu) / lowestFreq.cpu * 100);
        const memoryIncrease = ((highestFreq.memory - lowestFreq.memory) / lowestFreq.memory * 100);
        
        console.log(`   ${lowestFreq.frequency} → ${highestFreq.frequency}:`);
        console.log(`   • Latency: ${lowestFreq.latency.toFixed(0)}ms → ${highestFreq.latency.toFixed(0)}ms (${latencyIncrease > 0 ? '+' : ''}${latencyIncrease.toFixed(1)}%)`);
        console.log(`   • CPU: ${lowestFreq.cpu.toFixed(1)}% → ${highestFreq.cpu.toFixed(1)}% (${cpuIncrease > 0 ? '+' : ''}${cpuIncrease.toFixed(1)}%)`);
        console.log(`   • Memory: ${lowestFreq.memory.toFixed(1)}MB → ${highestFreq.memory.toFixed(1)}MB (${memoryIncrease > 0 ? '+' : ''}${memoryIncrease.toFixed(1)}%)`);
    }
}

// Run the main analysis
const analysis = runMainAnalysis();

// Generate all reports
generateSummaryTable(analysis);
generateApproachComparison(analysis);
generatePerformanceRankings(analysis);
generateFrequencyImpactAnalysis(analysis);

// Export results
const outputFile = 'comprehensive-performance-analysis.json';
fs.writeFileSync(outputFile, JSON.stringify(analysis, null, 2));

console.log('\n' + '=' .repeat(80));
console.log(`✅ Analysis complete! Detailed results exported to: ${outputFile}`);
console.log('=' .repeat(80));
