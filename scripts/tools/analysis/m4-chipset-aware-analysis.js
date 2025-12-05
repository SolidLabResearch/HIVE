#!/usr/bin/env node

/**
 * Apple M4 Chipset-Aware Performance Analysis
 * 
 * This script re-analyzes the performance data with proper consideration
 * for Apple M4 chipset architecture and multi-core CPU reporting.
 */

const fs = require('fs');
const path = require('path');

console.log('🍎 APPLE M4 CHIPSET-AWARE PERFORMANCE ANALYSIS');
console.log('=' .repeat(90));

// Apple M4 Chipset Specifications
const M4_SPECS = {
    performanceCores: 4,      // High-performance cores
    efficiencyCores: 6,       // Efficiency cores  
    totalCores: 10,           // Total CPU cores
    totalThreads: 10,         // M4 doesn't use hyperthreading like Intel
    baseFrequency: 3.2,       // Base frequency in GHz
    maxFrequency: 4.4,        // Max boost frequency in GHz
    architecture: 'ARM64',
    process: '3nm',
    gpu: 'M4 GPU (10-core)',
    unifiedMemory: true
};

console.log(' Apple M4 Chipset Specifications:');
console.log(`   • Performance Cores: ${M4_SPECS.performanceCores}`);
console.log(`   • Efficiency Cores: ${M4_SPECS.efficiencyCores}`);
console.log(`   • Total Cores: ${M4_SPECS.totalCores}`);
console.log(`   • Base/Max Frequency: ${M4_SPECS.baseFrequency}GHz / ${M4_SPECS.maxFrequency}GHz`);
console.log(`   • Architecture: ${M4_SPECS.architecture}`);
console.log(`   • Process: ${M4_SPECS.process}`);
console.log('');

const LOGS_BASE_DIR = '/Users/kushbisen/Code/streaming-query-hive/tools/experiments/logs';
const APPROACHES = ['fetching-client-side', 'streaming-query-hive', 'approximation-approach'];
const FREQUENCIES = ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz'];

/**
 * Normalize CPU percentage for M4 chipset
 */
function normalizeCPUForM4(rawCPUPercent) {
    // Node.js reports CPU as cumulative percentage across all cores
    // For M4 with 10 cores, 100% usage would be reported as 1000%
    // So we normalize by dividing by total cores
    const normalizedPercent = rawCPUPercent / M4_SPECS.totalCores;
    
    return {
        raw: rawCPUPercent,
        normalized: normalizedPercent,
        coreUtilization: normalizedPercent / 100, // As a fraction of total capacity
        equivalentCoresUsed: rawCPUPercent / 100  // How many cores worth of work
    };
}

/**
 * Calculate performance efficiency metrics
 */
function calculateEfficiencyMetrics(cpuData, memoryMB, latencySeconds) {
    const cpu = normalizeCPUForM4(cpuData);
    
    return {
        cpuEfficiency: latencySeconds / cpu.normalized, // seconds per CPU%
        memoryEfficiency: latencySeconds / memoryMB,    // seconds per MB
        throughputPerCore: 1 / (latencySeconds * cpu.equivalentCoresUsed), // operations per core-second
        powerEfficiency: 1 / (cpu.equivalentCoresUsed * memoryMB), // relative power efficiency
        coreUtilization: cpu.coreUtilization
    };
}

/**
 * Load and reanalyze the existing results with M4 awareness
 */
function loadAndReanalyzeResults() {
    const resultsFile = path.join(__dirname, 'final-comprehensive-analysis.json');
    
    if (!fs.existsSync(resultsFile)) {
        console.log('[FAIL] Previous analysis results not found. Please run final-comprehensive-analysis.js first.');
        return null;
    }
    
    const rawResults = JSON.parse(fs.readFileSync(resultsFile, 'utf8'));
    
    // Reanalyze with M4-aware CPU metrics
    const m4AwareResults = {};
    
    for (const approach of APPROACHES) {
        if (!rawResults[approach]) continue;
        
        m4AwareResults[approach] = {};
        
        for (const frequency of FREQUENCIES) {
            const data = rawResults[approach][frequency];
            if (!data || !data.hasLatencyData || !data.hasResourceData) {
                m4AwareResults[approach][frequency] = data;
                continue;
            }
            
            const latencySeconds = data.latency.avg / 1000;
            const rawCPU = data.resources.avgCPUTotal;
            const memoryMB = data.resources.avgMemoryRSS;
            
            const cpuMetrics = normalizeCPUForM4(rawCPU);
            const efficiency = calculateEfficiencyMetrics(rawCPU, memoryMB, latencySeconds);
            
            m4AwareResults[approach][frequency] = {
                ...data,
                m4Metrics: {
                    cpu: cpuMetrics,
                    efficiency: efficiency,
                    latencySeconds: latencySeconds,
                    memoryMB: memoryMB
                }
            };
        }
    }
    
    return m4AwareResults;
}

/**
 * Generate M4-aware performance table
 */
function generateM4AwareTable(results) {
    console.log(' M4 CHIPSET-AWARE PERFORMANCE ANALYSIS');
    console.log('=' .repeat(130));
    console.log();
    
    console.log('┌─────────────────────────┬─────────┬─────────────┬────────────┬─────────────┬─────────────┬─────────────┐');
    console.log('│ Approach                │ Freq    │ Latency     │ CPU Usage  │ Cores Used  │ Memory      │ Efficiency  │');
    console.log('│                         │         │ (seconds)   │ (%)        │ (equivalent)│ (MB)        │ Score       │');
    console.log('├─────────────────────────┼─────────┼─────────────┼────────────┼─────────────┼─────────────┼─────────────┤');
    
    for (const approach of APPROACHES) {
        if (!results[approach]) continue;
        
        for (const frequency of FREQUENCIES) {
            const data = results[approach][frequency];
            
            const approachName = approach.replace(/-/g, ' ').substring(0, 23).padEnd(23);
            const freq = frequency.padEnd(7);
            
            if (!data || !data.m4Metrics) {
                const latency = 'N/A'.padStart(11);
                const cpu = 'N/A'.padStart(10);
                const cores = 'N/A'.padStart(11);
                const memory = 'N/A'.padStart(11);
                const efficiency = 'N/A'.padStart(11);
                
                console.log(`│ ${approachName} │ ${freq} │ ${latency} │ ${cpu} │ ${cores} │ ${memory} │ ${efficiency} │`);
                continue;
            }
            
            const metrics = data.m4Metrics;
            const latency = metrics.latencySeconds.toFixed(1).padStart(11);
            const cpu = metrics.cpu.normalized.toFixed(1).padStart(10);
            const cores = metrics.cpu.equivalentCoresUsed.toFixed(1).padStart(11);
            const memory = metrics.memoryMB.toFixed(1).padStart(11);
            const efficiency = metrics.efficiency.powerEfficiency.toFixed(4).padStart(11);
            
            console.log(`│ ${approachName} │ ${freq} │ ${latency} │ ${cpu} │ ${cores} │ ${memory} │ ${efficiency} │`);
        }
    }
    
    console.log('└─────────────────────────┴─────────┴─────────────┴────────────┴─────────────┴─────────────┴─────────────┘');
    
    console.log('\n📝 Notes:');
    console.log('   • CPU Usage: Normalized percentage (0-100% represents full M4 utilization)');
    console.log('   • Cores Used: Equivalent number of M4 cores working at 100%');
    console.log('   • Efficiency Score: Higher is better (operations per core-MB)');
}

/**
 * Generate M4-specific insights
 */
function generateM4Insights(results) {
    console.log('\n M4 CHIPSET-SPECIFIC INSIGHTS');
    console.log('=' .repeat(90));
    
    const validConfigs = [];
    
    for (const approach of APPROACHES) {
        if (!results[approach]) continue;
        
        for (const frequency of FREQUENCIES) {
            const data = results[approach][frequency];
            if (data && data.m4Metrics) {
                validConfigs.push({
                    approach,
                    frequency,
                    ...data.m4Metrics
                });
            }
        }
    }
    
    if (validConfigs.length === 0) {
        console.log('[FAIL] No valid M4 metrics found');
        return;
    }
    
    // Analyze CPU utilization patterns
    console.log('\n🔋 CPU CORE UTILIZATION ANALYSIS:');
    
    for (const approach of APPROACHES) {
        const approachConfigs = validConfigs.filter(c => c.approach === approach);
        if (approachConfigs.length === 0) continue;
        
        console.log(`\n ${approach.replace(/-/g, ' ').toUpperCase()}:`);
        
        approachConfigs.forEach(config => {
            const utilizationPercent = (config.cpu.coreUtilization * 100).toFixed(1);
            const coresUsed = config.cpu.equivalentCoresUsed.toFixed(1);
            
            console.log(`   ${config.frequency}: ${utilizationPercent}% utilization (${coresUsed} cores)`);
            
            // Provide context for utilization level
            if (config.cpu.coreUtilization < 0.3) {
                console.log(`       └─ 🟢 Light load - good headroom for scaling`);
            } else if (config.cpu.coreUtilization < 0.7) {
                console.log(`       └─ 🟡 Moderate load - consider optimization`);
            } else {
                console.log(`       └─ 🔴 Heavy load - approaching M4 limits`);
            }
        });
    }
    
    // Find most efficient configurations
    console.log('\n🏆 MOST EFFICIENT CONFIGURATIONS (M4-optimized):');
    
    const byEfficiency = [...validConfigs].sort((a, b) => b.efficiency.powerEfficiency - a.efficiency.powerEfficiency);
    
    byEfficiency.slice(0, 5).forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        const cores = config.cpu.equivalentCoresUsed.toFixed(1);
        const utilization = (config.cpu.coreUtilization * 100).toFixed(1);
        
        console.log(`   ${index + 1}. ${name}`);
        console.log(`      └─ ${cores} cores (${utilization}% utilization), ${config.efficiency.powerEfficiency.toFixed(4)} efficiency`);
    });
    
    // Analyze frequency scaling on M4
    console.log('\n📈 M4 FREQUENCY SCALING ANALYSIS:');
    
    for (const approach of APPROACHES) {
        const approachConfigs = validConfigs.filter(c => c.approach === approach);
        if (approachConfigs.length < 2) continue;
        
        approachConfigs.sort((a, b) => parseInt(a.frequency) - parseInt(b.frequency));
        
        const lowest = approachConfigs[0];
        const highest = approachConfigs[approachConfigs.length - 1];
        
        const coreScaling = ((highest.cpu.equivalentCoresUsed - lowest.cpu.equivalentCoresUsed) / lowest.cpu.equivalentCoresUsed * 100);
        const efficiencyChange = ((highest.efficiency.powerEfficiency - lowest.efficiency.powerEfficiency) / lowest.efficiency.powerEfficiency * 100);
        
        console.log(`\n⚡ ${approach.replace(/-/g, ' ').toUpperCase()} (${lowest.frequency} → ${highest.frequency}):`);
        console.log(`   • Core usage scaling: ${coreScaling > 0 ? '+' : ''}${coreScaling.toFixed(1)}%`);
        console.log(`   • Efficiency change: ${efficiencyChange > 0 ? '+' : ''}${efficiencyChange.toFixed(1)}%`);
        
        // M4-specific recommendations
        if (highest.cpu.coreUtilization > 0.8) {
            console.log(`   [WARNING]  High M4 utilization at ${highest.frequency} - consider workload distribution`);
        }
        
        if (coreScaling > 300) {
            console.log(`    Significant core scaling - may benefit from M4 performance core optimization`);
        }
    }
}

/**
 * Generate M4-optimized recommendations
 */
function generateM4Recommendations(results) {
    console.log('\n💡 M4 CHIPSET-OPTIMIZED RECOMMENDATIONS');
    console.log('=' .repeat(90));
    
    const validConfigs = [];
    
    for (const approach of APPROACHES) {
        if (!results[approach]) continue;
        
        for (const frequency of FREQUENCIES) {
            const data = results[approach][frequency];
            if (data && data.m4Metrics) {
                validConfigs.push({
                    approach,
                    frequency,
                    ...data.m4Metrics
                });
            }
        }
    }
    
    if (validConfigs.length === 0) {
        console.log('[FAIL] Insufficient data for M4-specific recommendations');
        return;
    }
    
    console.log('\n FOR M4 PERFORMANCE OPTIMIZATION:');
    
    // Sort by efficiency while considering M4 utilization
    const m4Optimized = [...validConfigs].sort((a, b) => {
        // Prefer configurations with good efficiency and reasonable utilization
        const efficiencyA = a.efficiency.powerEfficiency;
        const efficiencyB = b.efficiency.powerEfficiency;
        const utilizationPenaltyA = a.cpu.coreUtilization > 0.8 ? 0.5 : 1.0;
        const utilizationPenaltyB = b.cpu.coreUtilization > 0.8 ? 0.5 : 1.0;
        
        return (efficiencyB * utilizationPenaltyB) - (efficiencyA * utilizationPenaltyA);
    });
    
    m4Optimized.slice(0, 3).forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        const cores = config.cpu.equivalentCoresUsed.toFixed(1);
        const utilization = (config.cpu.coreUtilization * 100).toFixed(1);
        const latency = config.latencySeconds.toFixed(1);
        
        console.log(`   ${index + 1}. ${name}`);
        console.log(`      ├─ Latency: ${latency}s`);
        console.log(`      ├─ M4 Utilization: ${utilization}% (${cores} equivalent cores)`);
        console.log(`      ├─ Memory: ${config.memoryMB.toFixed(1)}MB`);
        console.log(`      └─ Efficiency Score: ${config.efficiency.powerEfficiency.toFixed(4)}`);
    });
    
    console.log('\n🔋 FOR M4 POWER EFFICIENCY:');
    
    // Find configurations with best performance per core
    const byPerformancePerCore = [...validConfigs].sort((a, b) => b.efficiency.throughputPerCore - a.efficiency.throughputPerCore);
    
    byPerformancePerCore.slice(0, 3).forEach((config, index) => {
        const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
        const throughput = config.efficiency.throughputPerCore.toFixed(6);
        
        console.log(`   ${index + 1}. ${name}: ${throughput} ops/core-second`);
    });
    
    console.log('\n⚙️  M4-SPECIFIC TUNING SUGGESTIONS:');
    console.log('   • Consider using M4 performance cores for latency-critical tasks');
    console.log('   • Efficiency cores may be suitable for background processing');
    console.log('   • Unified memory architecture reduces memory bottlenecks');
    console.log('   • High core usage (>80%) suggests potential for parallel optimization');
    console.log('   • Consider ARM64-specific optimizations for better M4 utilization');
    
    console.log('\n SCALING RECOMMENDATIONS FOR M4:');
    
    const highUtilizationConfigs = validConfigs.filter(c => c.cpu.coreUtilization > 0.7);
    if (highUtilizationConfigs.length > 0) {
        console.log('   [WARNING]  High utilization detected in:');
        highUtilizationConfigs.forEach(config => {
            const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
            const utilization = (config.cpu.coreUtilization * 100).toFixed(1);
            console.log(`      • ${name}: ${utilization}% M4 utilization`);
        });
        console.log('    Consider: workload distribution, async processing, or algorithm optimization');
    }
    
    const lowUtilizationConfigs = validConfigs.filter(c => c.cpu.coreUtilization < 0.3);
    if (lowUtilizationConfigs.length > 0) {
        console.log('\n   [OK] Underutilized M4 capacity in:');
        lowUtilizationConfigs.forEach(config => {
            const name = `${config.approach.replace(/-/g, ' ')} @ ${config.frequency}`;
            const utilization = (config.cpu.coreUtilization * 100).toFixed(1);
            console.log(`      • ${name}: ${utilization}% M4 utilization`);
        });
        console.log('    Opportunity: increase throughput or run concurrent workloads');
    }
}

// Run M4-aware analysis
const results = loadAndReanalyzeResults();

if (results) {
    generateM4AwareTable(results);
    generateM4Insights(results);
    generateM4Recommendations(results);
    
    // Export M4-aware results
    const outputFile = 'm4-chipset-aware-analysis.json';
    fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));
    
    console.log('\n' + '=' .repeat(90));
    console.log(`[OK] M4 CHIPSET-AWARE ANALYSIS COMPLETE!`);
    console.log(` Results exported to: ${outputFile}`);
    console.log('=' .repeat(90));
}
