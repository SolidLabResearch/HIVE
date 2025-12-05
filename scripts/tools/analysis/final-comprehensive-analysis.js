#!/usr/bin/env node

/**
 * FINAL Comprehensive Query Performance Analysis
 * 
 * This script provides the definitive analysis of event latencies, memory usage, 
 * and CPU usage for all three streaming query approaches across all frequencies.
 */

const fs = require('fs');
const path = require('path');

console.log(' FINAL COMPREHENSIVE QUERY PERFORMANCE ANALYSIS');
console.log('=' .repeat(90));

const LOGS_BASE_DIR = '/Users/kushbisen/Code/streaming-query-hive/tools/experiments/logs';
const APPROACHES = ['fetching-client-side', 'streaming-query-hive', 'approximation-approach'];
const FREQUENCIES = ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz'];

/**
 * Extract query registration and first result timestamps with corrected patterns
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
        'streaming-query-hive': ['Successfully published result:', 'Published aggregated result:', 'Final aggregation results:'],
        'approximation-approach': ['Successfully published unified cross-sensor']
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
    
    const mainLog = path.join(basePath, logFiles[approach]);
    const resourceLog = path.join(basePath, resourceFiles[approach]);
    
    return { mainLog, resourceLog };
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
        
        if (!fs.existsSync(paths.mainLog)) {
            continue;
        }
        
        const latencyMetrics = extractLatencyMetrics(paths.mainLog, approach);
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
    
    console.log(' Analyzing all approaches and frequencies...\n');
    
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
                    const latency = (stats.latency.avg / 1000).toFixed(1); // Convert to seconds
                    const cpu = stats.resources.avgCPUTotal.toFixed(1);
                    const memory = stats.resources.avgMemoryRSS.toFixed(1);
                    console.log(`[OK] (${latency}s, ${cpu}% CPU, ${memory}MB)`);
                } else if (stats.hasLatencyData) {
                    const latency = (stats.latency.avg / 1000).toFixed(1);
                    console.log(`[WARNING]  (${latency}s, no resource data)`);
                } else if (stats.hasResourceData) {
                    const cpu = stats.resources.avgCPUTotal.toFixed(1);
                    console.log(`[WARNING]  (no latency data, ${cpu}% CPU)`);
                } else {
                    console.log(`[FAIL] (no valid data)`);
                }
            } else {
                console.log(`[FAIL] (no data found)`);
            }
        }
        console.log();
    }
    
    return analysis;
}

/**
 * Generate final summary table
 */
function generateFinalSummaryTable(analysis) {
    console.log(' FINAL PERFORMANCE SUMMARY TABLE');
    console.log('=' .repeat(130));
    console.log();
    
    // Header
    console.log('┌─────────────────────────┬─────────┬─────────────┬──────────┬─────────────┬─────────────┬─────────────┐');
    console.log('│ Approach                │ Freq    │ Latency     │ CPU Avg  │ Memory RSS  │ Memory Heap │ Status      │');
    console.log('│                         │         │ (seconds)   │ (%)      │ (MB)        │ (MB)        │             │');
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
                    latency = (data.latency.avg / 1000).toFixed(1).padStart(11);
                }
                
                if (data.resources) {
                    cpu = data.resources.avgCPUTotal.toFixed(1).padStart(8);
                    memRSS = data.resources.avgMemoryRSS.toFixed(1).padStart(11);
                    memHeap = data.resources.avgMemoryHeap.toFixed(1).padStart(11);
                }
                
                if (data.hasLatencyData && data.hasResourceData) {
                    status = '[OK] Complete'.padEnd(11);
                } else if (data.hasLatencyData) {
                    status = '[WARNING]  Latency'.padEnd(11);
                } else if (data.hasResourceData) {
                    status = '[WARNING]  Resource'.padEnd(11);
                } else {
                    status = '[FAIL] Failed'.padEnd(11);
                }
            }
            
            console.log(`│ ${approachName} │ ${freq} │ ${latency} │ ${cpu} │ ${memRSS} │ ${memHeap} │ ${status} │`);
        }
    }
    
    console.log('└─────────────────────────┴─────────┴─────────────┴──────────┴─────────────┴─────────────┴─────────────┘');
}

/**
 * Generate key insights
 */
function generateKeyInsights(analysis) {
    console.log('\n KEY INSIGHTS & FINDINGS');
    console.log('=' .repeat(90));
    
    // Find best and worst performers
    const validConfigurations = [];
    
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        
        for (const frequency of FREQUENCIES) {
            const data = analysis[approach][frequency];
            if (!data || !data.hasLatencyData || !data.hasResourceData) continue;
            
            validConfigurations.push({
                approach,
                frequency,
                latency: data.latency.avg / 1000, // Convert to seconds
                cpu: data.resources.avgCPUTotal,
                memory: data.resources.avgMemoryRSS
            });
        }
    }
    
    if (validConfigurations.length > 0) {
        // Sort by latency (best performance)
        const byLatency = [...validConfigurations].sort((a, b) => a.latency - b.latency);
        
        console.log('\n🏆 BEST OVERALL PERFORMANCE:');
        const best = byLatency[0];
        console.log(`   ${best.approach.replace(/-/g, ' ')} @ ${best.frequency}: ${best.latency.toFixed(1)}s latency`);
        console.log(`   ├─ CPU Usage: ${best.cpu.toFixed(1)}%`);
        console.log(`   └─ Memory Usage: ${best.memory.toFixed(1)}MB`);
        
        console.log('\n🐌 WORST OVERALL PERFORMANCE:');
        const worst = byLatency[byLatency.length - 1];
        console.log(`   ${worst.approach.replace(/-/g, ' ')} @ ${worst.frequency}: ${worst.latency.toFixed(1)}s latency`);
        console.log(`   ├─ CPU Usage: ${worst.cpu.toFixed(1)}%`);
        console.log(`   └─ Memory Usage: ${worst.memory.toFixed(1)}MB`);
        
        // Performance improvement calculation
        const improvement = ((worst.latency - best.latency) / worst.latency * 100);
        console.log(`\n📈 PERFORMANCE DELTA: ${improvement.toFixed(1)}% improvement from worst to best`);
    }
    
    // Approach-specific insights
    console.log('\n🔬 APPROACH-SPECIFIC INSIGHTS:');
    
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        
        const approachConfigs = validConfigurations.filter(c => c.approach === approach);
        if (approachConfigs.length === 0) continue;
        
        const avgLatency = approachConfigs.reduce((sum, c) => sum + c.latency, 0) / approachConfigs.length;
        const avgCPU = approachConfigs.reduce((sum, c) => sum + c.cpu, 0) / approachConfigs.length;
        const avgMemory = approachConfigs.reduce((sum, c) => sum + c.memory, 0) / approachConfigs.length;
        
        console.log(`\n    ${approach.replace(/-/g, ' ').toUpperCase()}:`);
        console.log(`      • Average Latency: ${avgLatency.toFixed(1)}s`);
        console.log(`      • Average CPU: ${avgCPU.toFixed(1)}%`);
        console.log(`      • Average Memory: ${avgMemory.toFixed(1)}MB`);
        console.log(`      • Data Points: ${approachConfigs.length}/${FREQUENCIES.length} frequencies`);
    }
    
    // Frequency impact analysis
    console.log('\n📈 FREQUENCY IMPACT ANALYSIS:');
    
    for (const approach of APPROACHES) {
        const approachConfigs = validConfigurations.filter(c => c.approach === approach);
        if (approachConfigs.length < 2) continue;
        
        // Sort by frequency
        approachConfigs.sort((a, b) => parseInt(a.frequency) - parseInt(b.frequency));
        
        const lowest = approachConfigs[0];
        const highest = approachConfigs[approachConfigs.length - 1];
        
        const latencyIncrease = ((highest.latency - lowest.latency) / lowest.latency * 100);
        const cpuIncrease = ((highest.cpu - lowest.cpu) / lowest.cpu * 100);
        const memoryIncrease = ((highest.memory - lowest.memory) / lowest.memory * 100);
        
        console.log(`\n   ⚡ ${approach.replace(/-/g, ' ').toUpperCase()} (${lowest.frequency} → ${highest.frequency}):`);
        console.log(`      • Latency Impact: ${latencyIncrease > 0 ? '+' : ''}${latencyIncrease.toFixed(1)}%`);
        console.log(`      • CPU Impact: ${cpuIncrease > 0 ? '+' : ''}${cpuIncrease.toFixed(1)}%`);
        console.log(`      • Memory Impact: ${memoryIncrease > 0 ? '+' : ''}${memoryIncrease.toFixed(1)}%`);
    }
}

/**
 * Generate recommendations
 */
function generateRecommendations(analysis) {
    console.log('\n💡 RECOMMENDATIONS');
    console.log('=' .repeat(90));
    
    const validConfigs = [];
    for (const approach of APPROACHES) {
        if (!analysis[approach]) continue;
        for (const frequency of FREQUENCIES) {
            const data = analysis[approach][frequency];
            if (data && data.hasLatencyData && data.hasResourceData) {
                validConfigs.push({
                    approach,
                    frequency,
                    latency: data.latency.avg / 1000,
                    cpu: data.resources.avgCPUTotal,
                    memory: data.resources.avgMemoryRSS
                });
            }
        }
    }
    
    if (validConfigs.length === 0) {
        console.log('[FAIL] Insufficient data for recommendations');
        return;
    }
    
    // Sort by latency performance
    validConfigs.sort((a, b) => a.latency - b.latency);
    
    console.log('\n FOR MINIMUM LATENCY:');
    const fastestConfigs = validConfigs.slice(0, 3);
    fastestConfigs.forEach((config, index) => {
        console.log(`   ${index + 1}. Use ${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`);
        console.log(`      └─ Expected: ${config.latency.toFixed(1)}s latency, ${config.cpu.toFixed(1)}% CPU, ${config.memory.toFixed(1)}MB`);
    });
    
    // Sort by CPU efficiency
    const byCPU = [...validConfigs].sort((a, b) => a.cpu - b.cpu);
    console.log('\n🔋 FOR CPU EFFICIENCY:');
    const efficientConfigs = byCPU.slice(0, 3);
    efficientConfigs.forEach((config, index) => {
        console.log(`   ${index + 1}. Use ${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`);
        console.log(`      └─ Expected: ${config.cpu.toFixed(1)}% CPU, ${config.latency.toFixed(1)}s latency, ${config.memory.toFixed(1)}MB`);
    });
    
    // Sort by memory efficiency
    const byMemory = [...validConfigs].sort((a, b) => a.memory - b.memory);
    console.log('\n🧠 FOR MEMORY EFFICIENCY:');
    const memoryConfigs = byMemory.slice(0, 3);
    memoryConfigs.forEach((config, index) => {
        console.log(`   ${index + 1}. Use ${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`);
        console.log(`      └─ Expected: ${config.memory.toFixed(1)}MB memory, ${config.latency.toFixed(1)}s latency, ${config.cpu.toFixed(1)}% CPU`);
    });
    
    console.log('\n[WARNING]  IMPORTANT NOTES:');
    console.log('   • streaming-query-hive approach appears to have execution issues');
    console.log('   • Consider investigating MQTT connectivity problems for streaming-query-hive');
    console.log('   • All measurements are for first result latency (query registration → first output)');
    console.log('   • Higher frequencies generally increase resource usage and latency');
}

// Run the final analysis
const analysis = runMainAnalysis();

// Generate all reports
generateFinalSummaryTable(analysis);
generateKeyInsights(analysis);
generateRecommendations(analysis);

// Export final results
const outputFile = 'final-comprehensive-analysis.json';
fs.writeFileSync(outputFile, JSON.stringify(analysis, null, 2));

console.log('\n' + '=' .repeat(90));
console.log(`[OK] ANALYSIS COMPLETE! Full results exported to: ${outputFile}`);
console.log('=' .repeat(90));
