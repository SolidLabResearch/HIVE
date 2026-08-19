#!/usr/bin/env node
/**
 * Parse already-collected logs and print the 4-approach comparison table.
 * Usage: node experiments/frequency-comparison/analyze-comparison-table.js [logBaseDir]
 *
 * Default logBaseDir: logs/comparison-table-run/complex_oscillation_freq_1.0
 */

const fs   = require('fs');
const os   = require('os');
const path = require('path');

const NUM_CORES = os.cpus().length;
const BASE = process.argv[2] || 'logs/comparison-table-run/complex_oscillation_freq_1.0';

const APPROACHES = [
    {
        name:         'Fetching (Local-Only)',
        dir:          'fetching',
        latencyFile:  'fetching_latency_log.csv',
        resourceFile: 'fetching_client_side_resource_usage.csv',
    },
    {
        name:         'Approximation',
        dir:          'approximation',
        latencyFile:  'approximation_latency_log.csv',
        resourceFile: 'approximation_approach_resource_usage.csv',
    },
    {
        name:         'Chunked',
        dir:          'chunked',
        latencyFile:  'chunked_latency_log.csv',
        resourceFile: 'streaming_query_hive_resource_log.csv',
    },
    {
        name:         'Naive Distributed',
        dir:          'naive-distributed',
        latencyFile:  'naive_distributed_latency_log.csv',
        resourceFile: 'naive_distributed_approach_resource_usage.csv',
    },
];

function mean(arr) { return arr.reduce((a, b) => a + b, 0) / arr.length; }
function stddev(arr) {
    if (arr.length < 2) return 0;
    const m = mean(arr);
    return Math.sqrt(arr.reduce((s, x) => s + (x - m) ** 2, 0) / arr.length);
}

/**
 * Latency = result_emitted_at − query_registered_at  (end-to-end, ~60 s for window 1)
 *
 * Strategy: rows are grouped by query_registered_at (each unique value = one run).
 * We take the MOST RECENT run and return only its FIRST window (lowest
 * result_emitted_at within that run). This gives the "first event latency" the
 * user cares about — how long until the result fires after the STEP boundary.
 *
 * Why not just average all rows?
 *   - Log files are appended across runs, so multiple runs accumulate.
 *   - Later windows in the same run (window 2 = ~120 s, window 3 = ~180 s)
 *     would inflate the average.
 *   - UNION-based queries (Naive Distributed) fire one binding per stream per
 *     step, producing 2 rows per window close, not 1.
 */
function parseLatency(file) {
    if (!fs.existsSync(file)) { console.warn(`  ⚠ Missing: ${file}`); return null; }
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
    if (lines.length < 2) return null;
    const hdr = lines[0].split(',');
    const regIdx  = hdr.indexOf('query_registered_at');
    const emitIdx = hdr.indexOf('result_emitted_at');
    const valIdx  = hdr.indexOf('result_value');
    if (regIdx < 0 || emitIdx < 0) {
        console.warn(`  ⚠ Required columns missing in ${path.basename(file)}`);
        console.warn(`    Available: ${hdr.join(', ')}`);
        return null;
    }

    // Parse all rows
    const rows = lines.slice(1)
        .map(l => {
            const p        = l.split(',');
            const regAt    = parseFloat(p[regIdx]);
            const emitAt   = parseFloat(p[emitIdx]);
            const val      = valIdx >= 0 ? parseFloat(p[valIdx]) : NaN;
            return { regAt, emitAt, lat: emitAt - regAt, val };
        })
        .filter(r => !isNaN(r.val) && !isNaN(r.lat));

    if (rows.length === 0) return null;

    // Find the most recent run (highest query_registered_at)
    const latestReg = Math.max(...rows.map(r => r.regAt));
    const latestRun = rows.filter(r => r.regAt === latestReg);

    // Take the first window result of that run (smallest result_emitted_at)
    const firstWindow = latestRun.reduce((best, r) => r.emitAt < best.emitAt ? r : best);

    console.log(`  (${rows.length} total row(s) across all runs; using most recent run's first window)`);
    if (rows.length > latestRun.length) {
        const nOlderRuns = new Set(rows.filter(r => r.regAt !== latestReg).map(r => r.regAt)).size;
        console.log(`  ⚠ Discarded ${rows.length - latestRun.length} row(s) from ${nOlderRuns} older run(s)`);
    }
    if (latestRun.length > 1) {
        console.log(`  ⚠ Latest run has ${latestRun.length} row(s); using earliest-emitted one (window 1 only)`);
    }

    return [firstWindow];
}

function parseResource(file) {
    if (!fs.existsSync(file)) { console.warn(`  ⚠ Missing: ${file}`); return null; }
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
    if (lines.length < 2) return null;
    const hdr = lines[0].split(',');
    const ti = hdr.indexOf('timestamp'), ci = hdr.indexOf('cpu_user'), hi = hdr.indexOf('heapUsedMB');
    if (ti < 0 || ci < 0 || hi < 0) return null;
    const cpus = [], heaps = [];
    for (let i = 1; i < lines.length; i++) {
        const p = lines[i].split(',');
        heaps.push(parseFloat(p[hi]));
        if (i >= 2) {
            const pp  = lines[i - 1].split(',');
            const dt  = parseFloat(p[ti]) - parseFloat(pp[ti]);
            const dcu = parseFloat(p[ci]) - parseFloat(pp[ci]);
            if (dt > 0) cpus.push((dcu / (dt * NUM_CORES)) * 100);
        }
    }
    return {
        avgCPU: cpus.length  ? mean(cpus)         : NaN,
        maxMem: heaps.length ? Math.max(...heaps) : NaN,
    };
}

const results = [];
for (const a of APPROACHES) {
    console.log(`\n── ${a.name} ──`);
    const lp = path.join(BASE, a.dir, a.latencyFile);
    const rp = path.join(BASE, a.dir, a.resourceFile);

    const windows   = parseLatency(lp);
    const resources = parseResource(rp);

    let avgLat = NaN, sdLat = NaN, avgVal = NaN;
    if (windows && windows.length > 0) {
        avgLat = windows[0].lat;
        sdLat  = 0;
        avgVal = windows[0].val;
        console.log(`  First-window latency: ${avgLat.toFixed(0)} ms  (${(avgLat/1000).toFixed(1)} s),  value=${avgVal.toFixed(4)}`);
    }

    results.push({ name: a.name, avgLat, sdLat, avgCPU: resources?.avgCPU ?? NaN, maxMem: resources?.maxMem ?? NaN, avgVal });
}

// ─── Print table ─────────────────────────────────────────────────────────────
const fmt = (v, d = 2) => isNaN(v) ? 'N/A' : v.toFixed(d);
const fetchVal = results.find(r => r.name.includes('Fetching'))?.avgVal ?? null;

console.log('\n' + '═'.repeat(120));
console.log(` COMPARISON TABLE — complex_oscillation @ 1.0 Hz  (1 iteration | ${NUM_CORES} CPU cores)`);
    console.log(` Latency = result_emitted_at − query_registered_at  (first window only, most recent run)`);
    console.log(` Expected: ~60 000 ms  (STEP = 60 s)`);
const pad = (s, n) => String(s).padEnd(n);
const COL = [24, 18, 14, 13, 14, 10, 21];
const headers = ['Approach', 'Avg Latency (ms)', 'Std Dev (ms)', 'Avg CPU (%)', 'Max Mem (MB)', 'Value', 'Error vs Fetching'];

console.log('| ' + headers.map((h, i) => pad(h, COL[i])).join(' | ') + ' |');
console.log('| ' + COL.map(n => '-'.repeat(n)).join(' | ') + ' |');

for (const r of results) {
    const err = fetchVal && !isNaN(r.avgVal) ? Math.abs((r.avgVal - fetchVal) / fetchVal) * 100 : 0;
    const row = [
        r.name,
        fmt(r.avgLat, 1),
        fmt(r.sdLat, 1),
        fmt(r.avgCPU, 2),
        fmt(r.maxMem, 1),
        fmt(r.avgVal, 3),
        fmt(err, 2) + (isNaN(err) ? '' : '%'),
    ];
    console.log('| ' + row.map((v, i) => pad(v, COL[i])).join(' | ') + ' |');
}
console.log('');

// Write markdown file
const mdLines = [
    '# 4-Approach Comparison — complex_oscillation @ 1.0 Hz',
    `> 1 iteration per approach | ${NUM_CORES} CPU cores | ${new Date().toISOString()}`,
    '>',
    '> **Latency** = `result_emitted_at − query_registered_at` (end-to-end: from query registration until the window result is emitted)',
    '> Expected ~60 000 ms for the first window given STEP = 60 s.',
    '',
    '| ' + headers.map((h, i) => pad(h, COL[i])).join(' | ') + ' |',
    '| ' + COL.map(n => '-'.repeat(n)).join(' | ') + ' |',
];
for (const r of results) {
    const err = fetchVal && !isNaN(r.avgVal) ? Math.abs((r.avgVal - fetchVal) / fetchVal) * 100 : 0;
    const row = [r.name, fmt(r.avgLat, 1), fmt(r.sdLat, 1), fmt(r.avgCPU, 2), fmt(r.maxMem, 1), fmt(r.avgVal, 3), fmt(err, 2) + (isNaN(err) ? '' : '%')];
    mdLines.push('| ' + row.map((v, i) => pad(v, COL[i])).join(' | ') + ' |');
}
mdLines.push('');

const outPath = path.join(BASE, 'comparison_table.md');
fs.writeFileSync(outPath, mdLines.join('\n'));
console.log(`📄 Saved to: ${outPath}`);
