const fs = require('fs');
const path = require('path');

/**
 * Resource Usage Comparison Analysis
 * Compares approximation approach vs streaming query hive (chunked) approach
 */

function parseCSV(filePath) {
    try {
        const data = fs.readFileSync(filePath, 'utf8');
        const lines = data.trim().split('\n');
        const headers = lines[0].split(',');
        
        const parsed = lines.slice(1).map(line => {
            const values = line.split(',');
            const row = {};
            headers.forEach((header, index) => {
                const value = values[index];
                if (header === 'timestamp') {
                    row[header] = parseInt(value);
                } else {
                    row[header] = parseFloat(value);
                }
            });
            return row;
        });
        
        return parsed;
    } catch (error) {
        console.error(`Error reading ${filePath}:`, error.message);
        return [];
    }
}

function calculateStats(data, field) {
    if (!data || data.length === 0) return null;
    
    const values = data.map(row => row[field]).filter(val => !isNaN(val));
    if (values.length === 0) return null;
    
    values.sort((a, b) => a - b);
    
    return {
        min: Math.min(...values),
        max: Math.max(...values),
        avg: values.reduce((sum, val) => sum + val, 0) / values.length,
        median: values[Math.floor(values.length / 2)],
        p95: values[Math.floor(values.length * 0.95)],
        p99: values[Math.floor(values.length * 0.99)],
        samples: values.length
    };
}

function analyzeTrends(data, field) {
    if (!data || data.length < 2) return null;
    
    const values = data.map(row => row[field]).filter(val => !isNaN(val));
    if (values.length < 2) return null;
    
    const start = values.slice(0, Math.min(10, Math.floor(values.length * 0.1))).reduce((sum, val) => sum + val, 0) / Math.min(10, Math.floor(values.length * 0.1));
    const end = values.slice(-Math.min(10, Math.floor(values.length * 0.1))).reduce((sum, val) => sum + val, 0) / Math.min(10, Math.floor(values.length * 0.1));
    
    return {
        startAvg: start,
        endAvg: end,
        trend: ((end - start) / start * 100).toFixed(2) + '%',
        growthRate: end > start ? 'Increasing' : 'Decreasing'
    };
}

function formatBytes(bytes) {
    return (bytes / 1024 / 1024).toFixed(2) + ' MB';
}

function formatPercent(value) {
    return value.toFixed(2) + '%';
}

function compareApproaches() {
    console.log('🔍 RESOURCE USAGE COMPARISON ANALYSIS');
    console.log('=====================================\n');
    
    // Load data
    const approxPath = 'logs/approximation-approach/iteration1/approximation_approach_resource_usage.csv';
    const chunkedPath = 'logs/streaming-query-hive/iteration1/streaming_query_hive_resource_log.csv';
    
    const approxData = parseCSV(approxPath);
    const chunkedData = parseCSV(chunkedPath);
    
    if (approxData.length === 0) {
        console.error('❌ Could not load approximation approach data');
        return;
    }
    
    if (chunkedData.length === 0) {
        console.error('❌ Could not load chunked approach data');
        return;
    }
    
    console.log(`📊 Data Summary:`);
    console.log(`   Approximation Approach: ${approxData.length} samples`);
    console.log(`   Chunked Approach: ${chunkedData.length} samples\n`);
    
    // Memory Usage Analysis
    console.log('🧠 MEMORY USAGE COMPARISON');
    console.log('==========================');
    
    const approxMemory = calculateStats(approxData, 'heapUsedMB');
    const chunkedMemory = calculateStats(chunkedData, 'heapUsedMB');
    
    if (approxMemory && chunkedMemory) {
        console.log('\n📈 Heap Memory Usage (MB):');
        console.log('                    Approximation    Chunked       Difference');
        console.log('                    -------------    -------       ----------');
        console.log(`Average:            ${approxMemory.avg.toFixed(2).padStart(8)}      ${chunkedMemory.avg.toFixed(2).padStart(8)}    ${((approxMemory.avg - chunkedMemory.avg) / chunkedMemory.avg * 100).toFixed(1)}%`);
        console.log(`Peak:               ${approxMemory.max.toFixed(2).padStart(8)}      ${chunkedMemory.max.toFixed(2).padStart(8)}    ${((approxMemory.max - chunkedMemory.max) / chunkedMemory.max * 100).toFixed(1)}%`);
        console.log(`Minimum:            ${approxMemory.min.toFixed(2).padStart(8)}      ${chunkedMemory.min.toFixed(2).padStart(8)}    ${((approxMemory.min - chunkedMemory.min) / chunkedMemory.min * 100).toFixed(1)}%`);
        console.log(`95th Percentile:    ${approxMemory.p95.toFixed(2).padStart(8)}      ${chunkedMemory.p95.toFixed(2).padStart(8)}    ${((approxMemory.p95 - chunkedMemory.p95) / chunkedMemory.p95 * 100).toFixed(1)}%`);
        
        const memoryImprovement = ((chunkedMemory.avg - approxMemory.avg) / chunkedMemory.avg * 100);
        if (memoryImprovement > 0) {
            console.log(`\n✅ Memory Optimization: ${memoryImprovement.toFixed(1)}% reduction in average memory usage`);
        } else {
            console.log(`\n⚠️  Memory Usage: ${Math.abs(memoryImprovement).toFixed(1)}% increase vs chunked approach`);
        }
    }
    
    // RSS Memory Analysis
    const approxRSS = calculateStats(approxData, 'rss');
    const chunkedRSS = calculateStats(chunkedData, 'rss');
    
    if (approxRSS && chunkedRSS) {
        console.log('\n💾 Resident Set Size (RSS):');
        console.log('                    Approximation    Chunked       Difference');
        console.log('                    -------------    -------       ----------');
        console.log(`Average:            ${formatBytes(approxRSS.avg).padStart(8)}      ${formatBytes(chunkedRSS.avg).padStart(8)}    ${((approxRSS.avg - chunkedRSS.avg) / chunkedRSS.avg * 100).toFixed(1)}%`);
        console.log(`Peak:               ${formatBytes(approxRSS.max).padStart(8)}      ${formatBytes(chunkedRSS.max).padStart(8)}    ${((approxRSS.max - chunkedRSS.max) / chunkedRSS.max * 100).toFixed(1)}%`);
    }
    
    // CPU Usage Analysis
    console.log('\n⚡ CPU USAGE COMPARISON');
    console.log('=======================');
    
    const approxCPUUser = calculateStats(approxData, 'cpu_user');
    const chunkedCPUUser = calculateStats(chunkedData, 'cpu_user');
    
    if (approxCPUUser && chunkedCPUUser) {
        console.log('\n🔥 CPU User Time (ms):');
        console.log('                    Approximation    Chunked       Difference');
        console.log('                    -------------    -------       ----------');
        console.log(`Average:            ${approxCPUUser.avg.toFixed(2).padStart(8)}      ${chunkedCPUUser.avg.toFixed(2).padStart(8)}    ${((approxCPUUser.avg - chunkedCPUUser.avg) / chunkedCPUUser.avg * 100).toFixed(1)}%`);
        console.log(`Peak:               ${approxCPUUser.max.toFixed(2).padStart(8)}      ${chunkedCPUUser.max.toFixed(2).padStart(8)}    ${((approxCPUUser.max - chunkedCPUUser.max) / chunkedCPUUser.max * 100).toFixed(1)}%`);
    }
    
    // Trend Analysis
    console.log('\n📊 TREND ANALYSIS');
    console.log('==================');
    
    const approxMemTrend = analyzeTrends(approxData, 'heapUsedMB');
    const chunkedMemTrend = analyzeTrends(chunkedData, 'heapUsedMB');
    
    if (approxMemTrend && chunkedMemTrend) {
        console.log('\n📈 Memory Usage Trends:');
        console.log(`Approximation: ${approxMemTrend.startAvg.toFixed(1)}MB → ${approxMemTrend.endAvg.toFixed(1)}MB (${approxMemTrend.trend})`);
        console.log(`Chunked:       ${chunkedMemTrend.startAvg.toFixed(1)}MB → ${chunkedMemTrend.endAvg.toFixed(1)}MB (${chunkedMemTrend.trend})`);
        
        if (approxMemTrend.growthRate === 'Increasing' && chunkedMemTrend.growthRate === 'Decreasing') {
            console.log('⚠️  Approximation shows memory growth, chunked approach is more stable');
        } else if (approxMemTrend.growthRate === 'Decreasing' && chunkedMemTrend.growthRate === 'Increasing') {
            console.log('✅ Approximation approach shows better memory stability');
        }
    }
    
    // Performance Summary
    console.log('\n🎯 PERFORMANCE SUMMARY');
    console.log('======================');
    
    if (approxMemory && chunkedMemory) {
        const avgDiff = ((approxMemory.avg - chunkedMemory.avg) / chunkedMemory.avg * 100);
        const peakDiff = ((approxMemory.max - chunkedMemory.max) / chunkedMemory.max * 100);
        
        console.log(`📊 Memory Performance:`);
        if (avgDiff < 0) {
            console.log(`   ✅ Approximation uses ${Math.abs(avgDiff).toFixed(1)}% less memory on average`);
        } else {
            console.log(`   ⚠️  Approximation uses ${avgDiff.toFixed(1)}% more memory on average`);
        }
        
        if (peakDiff < 0) {
            console.log(`   ✅ Peak memory usage ${Math.abs(peakDiff).toFixed(1)}% lower`);
        } else {
            console.log(`   ⚠️  Peak memory usage ${peakDiff.toFixed(1)}% higher`);
        }
    }
    
    // Recommendations
    console.log('\n💡 OPTIMIZATION RECOMMENDATIONS');
    console.log('================================');
    
    if (approxMemory && chunkedMemory && approxMemory.avg > chunkedMemory.avg) {
        const difference = approxMemory.avg - chunkedMemory.avg;
        console.log(`🔧 The approximation approach still uses ${difference.toFixed(1)}MB more memory on average`);
        console.log(`   Consider further optimizations:`);
        console.log(`   • Reduce window overlap further (currently 25%)`);
        console.log(`   • Implement connection pooling`);
        console.log(`   • Use the OptimizedOrchestrator implementation`);
        console.log(`   • Enable periodic garbage collection more frequently`);
    } else if (approxMemory && chunkedMemory) {
        console.log(`✅ Memory optimizations were successful!`);
        console.log(`   The approximation approach is now competitive with the chunked approach`);
    }
    
    // Duration Analysis
    const approxDuration = (approxData[approxData.length - 1]?.timestamp - approxData[0]?.timestamp) / 1000;
    const chunkedDuration = (chunkedData[chunkedData.length - 1]?.timestamp - chunkedData[0]?.timestamp) / 1000;
    
    if (approxDuration && chunkedDuration) {
        console.log(`\n⏱️  Execution Duration:`);
        console.log(`   Approximation: ${approxDuration.toFixed(1)} seconds`);
        console.log(`   Chunked:       ${chunkedDuration.toFixed(1)} seconds`);
    }
}

// Run the analysis
compareApproaches();
