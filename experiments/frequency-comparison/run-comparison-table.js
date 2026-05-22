#!/usr/bin/env node
/**
 * One-shot script: run all 4 approaches at 1.0 Hz (1 iteration each) then
 * print a Markdown comparison table.
 *
 * Metrics computed:
 *   Avg Latency (ms)  — mean of latency_from_last_obs_ms per window
 *   Std Dev (ms)      — std-dev of latency_from_last_obs_ms per window
 *   Avg CPU (%)       — average per-interval CPU% from resource logs
 *   Max Mem (MB)      — peak heapUsedMB from resource logs
 *   Value             — mean result value across windows
 *   Error vs Fetching — |value - fetching_value| / |fetching_value| × 100
 *
 * Usage:
 *   node experiments/frequency-comparison/run-comparison-table.js [frequency]
 *   node experiments/frequency-comparison/run-comparison-table.js 1.0
 */

const { spawn, execSync } = require('child_process');
const fs   = require('fs');
const path = require('path');
const os   = require('os');
const { createBenchmarkReplayRunEnv } = require('../utils/benchmarkReplayEnv');

// ─── Configuration ────────────────────────────────────────────────────────────

const FREQUENCY        = parseFloat(process.argv[2] || '1.0');
const OSCILLATION_TYPE = 'complex_oscillation';
const DATA_PATH        = `frequency_comparison/${OSCILLATION_TYPE}_freq_${FREQUENCY % 1 === 0 ? FREQUENCY.toFixed(1) : FREQUENCY}`;
const TIMEOUT_MS       = 3 * 60 * 1000; // 3 minutes per approach
const NUM_CORES        = os.cpus().length;
const replayEnv = createBenchmarkReplayRunEnv(process.env);

const OUT_DIR = path.join('logs', 'comparison-table-run',
    `${OSCILLATION_TYPE}_freq_${FREQUENCY % 1 === 0 ? FREQUENCY.toFixed(1) : FREQUENCY}`);

const APPROACHES = [
    {
        name:         'Fetching (Local-Only)',
        key:          'fetching',
        orchestrator: 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js',
        logFiles: [
            'fetching_client_side_log.csv',
            'fetching_client_side_resource_usage.csv',
            'fetching_latency_log.csv',
            'replayer-log.csv',
        ],
        latencyFile:   'fetching_latency_log.csv',
        latencyColumn: 'latency_from_last_obs_ms',  // result_emitted_at - last_obs_received_at
        resourceFile:  'fetching_client_side_resource_usage.csv',
    },
    {
        name:         'Approximation',
        key:          'approximation',
        orchestrator: 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js',
        logFiles: [
            'approximation_approach_log.csv',
            'approximation_approach_resource_usage.csv',
            'approximation_latency_log.csv',
            'replayer-log.csv',
        ],
        latencyFile:   'approximation_latency_log.csv',
        latencyColumn: 'latency_from_last_data_ms', // result_emitted_at - last_data_received_at
        resourceFile:  'approximation_approach_resource_usage.csv',
    },
    {
        name:         'Chunked',
        key:          'chunked',
        orchestrator: 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js',
        logFiles: [
            'streaming_query_chunk_aggregator_log.csv',
            'streaming_query_hive_resource_log.csv',
            'chunked_latency_log.csv',
            'replayer-log.csv',
        ],
        latencyFile:   'chunked_latency_log.csv',
        latencyColumn: 'computation_ms',            // result_emitted_at - interval_trigger_at (pure processing time)
        resourceFile:  'streaming_query_hive_resource_log.csv',
    },
    {
        name:         'Naive Distributed',
        key:          'naive-distributed',
        orchestrator: 'dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js',
        logFiles: [
            'naive_distributed_approach_log.csv',
            'naive_distributed_approach_resource_usage.csv',
            'naive_distributed_latency_log.csv',
            'replayer-log.csv',
        ],
        latencyFile:   'naive_distributed_latency_log.csv',
        latencyColumn: 'latency_from_last_obs_ms',  // result_emitted_at - last_obs_received_at
        resourceFile:  'naive_distributed_approach_resource_usage.csv',
    },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function killLingeringProcesses() {
    const targets = [
        'StreamingQueryFetchingClientSideApproachOrchestrator',
        'StreamingQueryApproximationApproachOrchestrator',
        'StreamingQueryChunkedApproachOrchestrator',
        'StreamingQueryNaiveDistributedApproachOrchestrator',
        'dist/streamer/src/publish.js',
    ];
    for (const t of targets) {
        try { execSync(`pkill -f "${t}" 2>/dev/null`); } catch (_) {}
    }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Run one approach ─────────────────────────────────────────────────────────

// ─── Run all approaches concurrently with one shared publisher ────────────────
//
// All 4 orchestrators subscribe to the same MQTT broker simultaneously. A
// single publisher replays the data once so every approach sees identical
// events with identical wall-clock timestamps. This makes the window slice
// identical for all approaches, giving a genuine "Error vs Fetching" accuracy
// comparison instead of a sequential-benchmark artifact.

async function runAllConcurrent() {
    console.log('\n' + '═'.repeat(70));
    console.log('  STARTING ALL 4 APPROACHES CONCURRENTLY');
    console.log('═'.repeat(70));

    killLingeringProcesses();
    await sleep(1000);

    // Delete any pre-existing log files so append-mode does not accumulate
    for (const approach of APPROACHES) {
        for (const file of approach.logFiles) {
            if (fs.existsSync(file)) { try { fs.unlinkSync(file); } catch (_) {} }
        }
        const approachDir = path.join(OUT_DIR, approach.key);
        if (!fs.existsSync(approachDir)) fs.mkdirSync(approachDir, { recursive: true });
    }

    const env = replayEnv.withBenchmarkReplayEnv({ ...process.env, DATA_PATH });

    // Start all orchestrators simultaneously
    const orchProcs = APPROACHES.map(approach => {
        console.log(`  Spawning: ${approach.name}`);
        const orch = spawn('node', [approach.orchestrator], {
            stdio: ['ignore', 'pipe', 'pipe'],
            env,
        });
        let orchOut = '';
        orch.stdout.on('data', d => {
            orchOut += d;
            process.stdout.write(`[${approach.key.toUpperCase()}] ` + d);
        });
        orch.stderr.on('data', d => process.stderr.write(`[${approach.key.toUpperCase()}-ERR] ` + d));
        return { approach, orch, getOut: () => orchOut };
    });

    console.log('\n  Waiting 3s for all orchestrators to initialise...');
    await sleep(3000);

    // Start a single shared publisher
    console.log('  Starting publisher (shared across all approaches)...');
    const pub = spawn('node', ['dist/streamer/src/publish.js'], {
        stdio: ['ignore', 'pipe', 'pipe'],
        env,
    });
    pub.stdout.on('data', d => process.stdout.write('[PUB] ' + d));
    pub.stderr.on('data', d => process.stderr.write('[PUB-ERR] ' + d));

    // Wait for publisher to complete or timeout
    await new Promise((resolve) => {
        const timer = setTimeout(() => {
            console.log('\n  Timeout — killing publisher');
            pub.kill();
            resolve();
        }, TIMEOUT_MS);
        pub.on('close', () => { clearTimeout(timer); resolve(); });
        pub.on('error', () => { clearTimeout(timer); resolve(); });
    });

    console.log('\n  Publisher done — waiting 5s for all approaches to emit final windows...');
    await sleep(5000);

    // Kill all orchestrators
    for (const { orch } of orchProcs) { try { orch.kill(); } catch (_) {} }
    await sleep(1000);

    // Collect generated files into per-approach output directories
    for (const { approach, getOut } of orchProcs) {
        const approachDir = path.join(OUT_DIR, approach.key);
        for (const file of approach.logFiles) {
            if (fs.existsSync(file)) {
                try {
                    fs.copyFileSync(file, path.join(approachDir, file));
                    fs.unlinkSync(file);
                } catch (_) {}
            }
        }
        fs.writeFileSync(path.join(approachDir, `${approach.key}_stdout.txt`), getOut());
        console.log(`  \u2713 ${approach.name} logs saved`);
    }
}


function parseLatencyCSV(filePath, latencyColumn) {
    if (!fs.existsSync(filePath)) return null;

    const lines = fs.readFileSync(filePath, 'utf8').trim().split('\n');
    if (lines.length < 2) return null;

    const header = lines[0].split(',');
    const latencyIdx = header.indexOf(latencyColumn);
    const valueIdx   = header.indexOf('result_value');

    if (latencyIdx < 0) {
        console.warn(`  ⚠ Column "${latencyColumn}" not found in ${filePath}`);
        console.warn(`    Available columns: ${header.join(', ')}`);
        return null;
    }

    const windows = [];
    for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(',');
        if (parts.length < Math.max(latencyIdx, valueIdx) + 1) continue;

        const latency = parseFloat(parts[latencyIdx]);
        const value   = valueIdx >= 0 ? parseFloat(parts[valueIdx]) : NaN;
        if (!isNaN(value)) windows.push({ latency: isNaN(latency) ? 0 : latency, value });
    }
    return windows.length > 0 ? windows : null;
}

// ─── Parse resource CSV ───────────────────────────────────────────────────────

function parseResourceCSV(filePath) {
    if (!fs.existsSync(filePath)) return null;

    const lines = fs.readFileSync(filePath, 'utf8').trim().split('\n');
    if (lines.length < 2) return null;

    const cpuPercents = [];
    const heapMBs     = [];

    const header  = lines[0].split(',');
    const tsIdx   = header.indexOf('timestamp');
    const cpuIdx  = header.indexOf('cpu_user');
    const heapIdx = header.indexOf('heapUsedMB');

    if (tsIdx < 0 || cpuIdx < 0 || heapIdx < 0) return null;

    for (let i = 1; i < lines.length; i++) {
        const curr = lines[i].split(',');
        if (curr.length < Math.max(tsIdx, cpuIdx, heapIdx) + 1) continue;

        heapMBs.push(parseFloat(curr[heapIdx]));

        if (i >= 2) {
            const prev       = lines[i - 1].split(',');
            const deltaTime  = parseFloat(curr[tsIdx]) - parseFloat(prev[tsIdx]);
            const deltaCpuMs = parseFloat(curr[cpuIdx]) - parseFloat(prev[cpuIdx]);
            if (deltaTime > 0) {
                // cpu_user stored as microseconds / 1000 = milliseconds
                // cpuPercent = deltaCpuMs / (deltaTime_ms * numCores) * 100
                cpuPercents.push((deltaCpuMs / (deltaTime * NUM_CORES)) * 100);
            }
        }
    }

    const avgCPU = cpuPercents.length > 0
        ? cpuPercents.reduce((a, b) => a + b, 0) / cpuPercents.length
        : NaN;
    const maxMem = heapMBs.length > 0 ? Math.max(...heapMBs) : NaN;

    return { avgCPU, maxMem };
}

// ─── Statistics helpers ───────────────────────────────────────────────────────

function mean(arr) { return arr.reduce((a, b) => a + b, 0) / arr.length; }

function stddev(arr) {
    if (arr.length < 2) return 0;
    const m = mean(arr);
    return Math.sqrt(arr.reduce((s, x) => s + (x - m) ** 2, 0) / arr.length);
}

// ─── Generate Markdown table ──────────────────────────────────────────────────

function generateTable(metrics) {
    const fetchingValue = metrics.find(m => m.key === 'fetching')?.avgValue ?? null;

    const rows = metrics.map(m => {
        const error = fetchingValue !== null && fetchingValue !== 0
            ? Math.abs((m.avgValue - fetchingValue) / fetchingValue) * 100
            : 0;
        return {
            name:       m.name,
            avgLatency: m.avgLatency,
            stdDev:     m.stdDev,
            avgCPU:     m.avgCPU,
            maxMem:     m.maxMem,
            value:      m.avgValue,
            error,
        };
    });

    const pad = (s, n) => String(s).padEnd(n);
    const fmt = (v, decimals = 2) => isNaN(v) ? 'N/A' : v.toFixed(decimals);

    const COL = [22, 22, 13, 15, 16, 14, 23];
    const headers = ['Approach', 'Avg Latency (ms)', 'Std Dev (ms)', 'Avg CPU (%)', 'Max Mem (MB)', 'Value', 'Error vs Fetching'];
    const sep = COL.map(n => '-'.repeat(n));

    const fmtRow = r => [
        r.name,
        fmt(r.avgLatency, 1),
        fmt(r.stdDev, 1),
        fmt(r.avgCPU, 2),
        fmt(r.maxMem, 1),
        fmt(r.value, 3),
        fmt(r.error, 2) + (isNaN(r.error) ? '' : '%'),
    ].map((v, i) => pad(v, COL[i])).join(' | ');

    const line = '| ' + headers.map((h, i) => pad(h, COL[i])).join(' | ') + ' |';
    const sepLine = '| ' + sep.map((s, i) => pad(s, COL[i])).join(' | ') + ' |';

    console.log('\n' + '═'.repeat(130));
    console.log(' COMPARISON TABLE — ' + OSCILLATION_TYPE + ' @ ' + FREQUENCY + ' Hz (1 iteration, ' + NUM_CORES + ' CPU cores detected)');
    console.log(' Latency = processing time from last data received to result emitted');
    console.log('   Fetching/Naive: latency_from_last_obs_ms | Approximation: latency_from_last_data_ms | Chunked: computation_ms');
    console.log('═'.repeat(130));
    console.log('\n' + line);
    console.log(sepLine);
    rows.forEach(r => console.log('| ' + fmtRow(r) + ' |'));
    console.log('');

    // Also write to a file
    const tableFile = path.join(OUT_DIR, 'comparison_table.md');
    const md = [
        `# Comparison Table — ${OSCILLATION_TYPE} @ ${FREQUENCY} Hz`,
        `> 1 iteration per approach | ${NUM_CORES} CPU cores | ${new Date().toISOString()}`,
        `> **Latency** = time from receiving last stream observation to emitting the window result (\`latency_from_last_obs_ms\`)`,
        '',
        '| ' + headers.map((h, i) => pad(h, COL[i])).join(' | ') + ' |',
        '| ' + sep.map((s, i) => pad(s, COL[i])).join(' | ') + ' |',
        ...rows.map(r => '| ' + fmtRow(r) + ' |'),
        '',
    ].join('\n');
    fs.writeFileSync(tableFile, md);
    console.log(`📄 Table saved to: ${tableFile}\n`);
}

// ─── Entry point ─────────────────────────────────────────────────────────────

(async () => {
    if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

    console.log('█'.repeat(70));
    console.log(`  4-APPROACH COMPARISON — ${OSCILLATION_TYPE} @ ${FREQUENCY} Hz`);
    console.log('█'.repeat(70));
    console.log(`  Output: ${OUT_DIR}`);
    console.log(`  CPU cores (for normalisation): ${NUM_CORES}`);
    console.log('█'.repeat(70));

    // Run all approaches simultaneously on the same MQTT publisher stream
    await runAllConcurrent();
    await sleep(3000); // let MQTT settle and log flush to disk

    const allMetrics = [];

    for (const approach of APPROACHES) {
        const approachDir  = path.join(OUT_DIR, approach.key);
        const latencyPath  = path.join(approachDir, approach.latencyFile);
        const resourcePath = path.join(approachDir, approach.resourceFile);

        const windows   = parseLatencyCSV(latencyPath, approach.latencyColumn);
        const resources = parseResourceCSV(resourcePath);

        let avgLatency = NaN, stdDev = NaN, avgValue = NaN;

        if (windows && windows.length > 0) {
            const latencies = windows.map(w => w.latency);
            const values    = windows.map(w => w.value);
            avgLatency = mean(latencies);
            stdDev     = stddev(latencies);
            avgValue   = mean(values);
            console.log(`  ${approach.name}: ${windows.length} window(s) parsed`);
            windows.forEach((w, i) =>
                console.log(`    Window ${i + 1}: value=${w.value.toFixed(4)}, latency=${w.latency}ms`)
            );
        } else {
            console.warn(`  No latency data parsed for ${approach.name} — file: ${latencyPath}`);
        }

        allMetrics.push({
            name:       approach.name,
            key:        approach.key,
            avgLatency,
            stdDev,
            avgCPU:     resources?.avgCPU  ?? NaN,
            maxMem:     resources?.maxMem  ?? NaN,
            avgValue,
        });
    }


    generateTable(allMetrics);

    // Mathematical accuracy verification for the Chunked approach
    console.log('\n' + '─'.repeat(70));
    console.log('  ACCURACY VERIFICATION: verify-chunked-accuracy.js');
    console.log('─'.repeat(70));
    await new Promise((resolve) => {
        const v = spawn('node', ['experiments/frequency-comparison/verify-chunked-accuracy.js'], {
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        v.stdout.on('data', d => process.stdout.write(d));
        v.stderr.on('data', d => process.stderr.write(d));
        v.on('close', resolve);
        v.on('error', resolve);
    });

    console.log('✓ Done.\n');
})().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
