const fs = require('fs');

console.log('================================================================================');
console.log('SPIKE PATTERN - COMPREHENSIVE RESULTS');
console.log('================================================================================\n');

const results = {};

// Parse Fetching
const fetchingLog = fs.readFileSync('fetching_client_side_log.csv', 'utf8').split('\n');
let fetchingQueryReg = null, fetchingResultTime = null, fetchingResult = null;
fetchingLog.forEach(line => {
    if (line.includes('fetching_query_registered')) {
        fetchingQueryReg = parseInt(line.split(',')[0]);
    } else if (line.includes('RStream result generated:') && fetchingResult === null) {
        fetchingResultTime = parseInt(line.split(',')[0]);
        const match = line.match(/result generated: ([-\d.]+)/);
        if (match) fetchingResult = parseFloat(match[1]);
    }
});

const fetchingResources = fs.readFileSync('fetching_client_side_resource_usage.csv', 'utf8').split('\n').slice(1).filter(l => l.trim());
let fcpu_u = 0, fcpu_s = 0, fmem = 0, fcount = 0;
fetchingResources.forEach(line => {
    const parts = line.split(',');
    if (parts.length >= 4) {
        fcpu_u += parseFloat(parts[1]);
        fcpu_s += parseFloat(parts[2]);
        fmem += parseFloat(parts[3]) / (1024 * 1024);
        fcount++;
    }
});

results.fetching = {
    result: fetchingResult,
    latency: fetchingResultTime - fetchingQueryReg,
    cpu_user: fcpu_u / fcount,
    cpu_sys: fcpu_s / fcount,
    memory: fmem / fcount
};

// Parse Approximation
const approxLog = fs.readFileSync('approximation_latency_log.csv', 'utf8').split('\n');
if (approxLog.length >= 2) {
    const parts = approxLog[1].split(',');
    results.approximation = {
        result: parseFloat(parts[parts.length - 1]),
        latency: parseInt(parts[6])
    };
}

// Parse Chunked
const chunkedLog = fs.readFileSync('chunked_latency_log.csv', 'utf8').split('\n');
if (chunkedLog.length >= 2) {
    const parts = chunkedLog[1].split(',');
    results.chunked = {
        result: parseFloat(parts[parts.length - 1]),
        latency: parseInt(parts[7])
    };
}

// Print results
const baseline = results.fetching.result;

console.log('Approach          | Result            | Latency (ms) | Error %       | CPU User % | CPU Sys %  | Memory MB');
console.log('-'.repeat(115));

console.log(`Fetching          | ${results.fetching.result.toFixed(6).padEnd(17)} | ${String(results.fetching.latency).padEnd(12)} | 0.0000 (baseline) | ${results.fetching.cpu_user.toFixed(2).padEnd(10)} | ${results.fetching.cpu_sys.toFixed(2).padEnd(10)} | ${results.fetching.memory.toFixed(2)}`);

if (results.approximation) {
    const approxError = Math.abs((results.approximation.result - baseline) / baseline) * 100;
    console.log(`Approximation     | ${results.approximation.result.toFixed(6).padEnd(17)} | ${String(results.approximation.latency).padEnd(12)} | ${approxError.toFixed(4).padEnd(13)} | N/A        | N/A        | N/A`);
}

if (results.chunked) {
    const chunkedError = Math.abs((results.chunked.result - baseline) / baseline) * 100;
    console.log(`Chunked (FIXED)   | ${results.chunked.result.toFixed(6).padEnd(17)} | ${String(results.chunked.latency).padEnd(12)} | ${chunkedError.toFixed(4).padEnd(13)} | N/A        | N/A        | N/A`);
}

console.log('\n================================================================================');
console.log('KEY FINDINGS');
console.log('================================================================================');

if (results.chunked) {
    const chunkedError = Math.abs((results.chunked.result - baseline) / baseline) * 100;
    if (chunkedError < 0.5) {
        console.log(`✅ EXCELLENT! Chunked error: ${chunkedError.toFixed(6)}% (< 0.5%)`);
        console.log('✅ Overlap adjustment fix is WORKING!');
    } else if (chunkedError < 1) {
        console.log(`✅ GOOD: Chunked error: ${chunkedError.toFixed(4)}% (< 1%)`);
    } else {
        console.log(`⚠️  Chunked error: ${chunkedError.toFixed(4)}%`);
    }
}

console.log('\nNote: Negative latency values indicate results emitted before expected window close time.');
console.log('      This is expected for Approximation and Chunked approaches due to their streaming nature.');
