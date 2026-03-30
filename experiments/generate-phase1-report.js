#!/usr/bin/env node
/**
 * Phase 1 Report Generator
 *
 * Reads log files from both real-data and pattern experiments and generates
 * the three Phase 1 tables:
 *   1. Accuracy table  (avg result value per dataset per approach, % error vs fetching)
 *   2. Latency table   (avg, std dev, min, max, success rate)
 *   3. Resource table  (avg CPU%, avg memory MB ± std)
 *
 * Usage:
 *   node experiments/generate-phase1-report.js
 *   node experiments/generate-phase1-report.js --json   # also writes report.json
 */

const fs   = require('fs');
const path = require('path');

// ─── Configuration ────────────────────────────────────────────────────────────

const APPROACHES = [
  { key: 'fetching',          label: 'Fetching Client Side' },
  { key: 'naive_distributed', label: 'Naive Distributed'    },
  { key: 'approximation',     label: 'Approximation'        },
  { key: 'chunked',           label: 'Chunked'              },
];

// All datasets: real data first, then the 5 custom patterns
const DATASETS = [
  { key: 'real_data',            label: 'Real Data',             logBase: 'experiments/real-data-comparison/logs' },
  { key: 'low_variability',      label: 'low_variability',       logBase: 'logs/custom-pattern-comparison' },
  { key: 'step_pattern',         label: 'step_pattern',          logBase: 'logs/custom-pattern-comparison' },
  { key: 'spike_pattern',        label: 'spike_pattern',         logBase: 'logs/custom-pattern-comparison' },
  { key: 'low_freq_oscillation', label: 'low_freq_oscillation',  logBase: 'logs/custom-pattern-comparison' },
  { key: 'high_freq_oscillation','label': 'high_freq_oscillation', logBase: 'logs/custom-pattern-comparison' },
];

// Latency log column names per approach
const LATENCY_LOG = {
  fetching:          'fetching_latency_log.csv',
  naive_distributed: 'naive_distributed_latency_log.csv',
  approximation:     'approximation_latency_log.csv',
  chunked:           'chunked_latency_log.csv',
};

// Resource log per approach
const RESOURCE_LOG = {
  fetching:          'fetching_client_side_resource_usage.csv',
  naive_distributed: 'naive_distributed_approach_resource_usage.csv',
  approximation:     'approximation_approach_resource_usage.csv',
  chunked:           'streaming_query_hive_resource_log.csv',
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseCSV(filePath) {
  if (!fs.existsSync(filePath)) return null;
  try {
    const lines = fs.readFileSync(filePath, 'utf8').trim().split('\n');
    if (lines.length < 2) return null;
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1)
      .filter(l => l.trim())
      .map(line => {
        // handle quoted fields
        const cols = [];
        let cur = '', inQ = false;
        for (const ch of line) {
          if (ch === '"') { inQ = !inQ; }
          else if (ch === ',' && !inQ) { cols.push(cur); cur = ''; }
          else { cur += ch; }
        }
        cols.push(cur);
        const row = {};
        headers.forEach((h, i) => row[h] = (cols[i] || '').trim());
        return row;
      });
  } catch { return null; }
}

function stdDev(arr) {
  if (arr.length < 2) return 0;
  const m = arr.reduce((a, b) => a + b, 0) / arr.length;
  return Math.sqrt(arr.reduce((s, v) => s + (v - m) ** 2, 0) / arr.length);
}

// Find all iteration directories for a given approach + dataset combination
function findIterationDirs(approachKey, dataset) {
  let searchDir;
  if (dataset.key === 'real_data') {
    searchDir = path.join(dataset.logBase, approachKey);
  } else {
    searchDir = path.join(dataset.logBase, approachKey, dataset.key);
  }
  if (!fs.existsSync(searchDir)) return [];
  return fs.readdirSync(searchDir)
    .filter(d => d.startsWith('iteration'))
    .sort()
    .map(d => path.join(searchDir, d));
}

// ─── Latency extraction ───────────────────────────────────────────────────────

function extractLatencyFromDir(dir, approachKey) {
  const logFile = path.join(dir, LATENCY_LOG[approachKey]);
  const rows = parseCSV(logFile);
  if (!rows || rows.length === 0) return [];

  return rows.map(r => {
    // Try common column names
    const lat = parseFloat(
      r.latency_from_last_obs_ms  ||
      r.latency_from_last_data_ms ||
      r.computation_ms            ||
      r.latency_ms                ||
      '0'
    );
    const val = parseFloat(r.result_value || '0');
    return { latency: isNaN(lat) ? null : lat, value: isNaN(val) ? null : val };
  }).filter(r => r.latency !== null && isFinite(r.latency));
}

// ─── Resource extraction ──────────────────────────────────────────────────────

function extractResourcesFromDir(dir, approachKey) {
  const logFile = path.join(dir, RESOURCE_LOG[approachKey]);
  const rows = parseCSV(logFile);
  if (!rows || rows.length < 2) return null;

  // cpu_user/cpu_system are cumulative ms from process.cpuUsage().
  // CPU% = (delta_cpu_ms) / (delta_wall_ms) * 100
  const first = rows[0], last = rows[rows.length - 1];
  const wallMs = parseFloat(last.timestamp) - parseFloat(first.timestamp);
  let avgCpu = null;
  if (wallMs > 0) {
    const deltaCpu = (parseFloat(last.cpu_user || '0') - parseFloat(first.cpu_user || '0'))
                   + (parseFloat(last.cpu_system || '0') - parseFloat(first.cpu_system || '0'));
    avgCpu = (deltaCpu / wallMs) * 100;
  }

  // heapUsedMB column is already in MB
  const memVals = rows
    .map(r => parseFloat(r.heapUsedMB || '0'))
    .filter(v => !isNaN(v) && v > 0);

  if (memVals.length === 0) return null;

  return {
    avgCpu:   avgCpu,
    avgMemMB: memVals.reduce((a, b) => a + b, 0) / memVals.length,
    stdMemMB: stdDev(memVals),
  };
}

// ─── Per-approach, per-dataset aggregation ────────────────────────────────────

function aggregateApproachDataset(approachKey, dataset) {
  const dirs = findIterationDirs(approachKey, dataset);
  if (dirs.length === 0) return null;

  const allLatencies = [];
  const allValues    = [];
  const cpuArr = [], memArr = [];
  let successCount = 0;

  for (const dir of dirs) {
    const rows = extractLatencyFromDir(dir, approachKey);
    if (rows.length > 0) {
      successCount++;
      rows.forEach(r => {
        allLatencies.push(r.latency);
        if (r.value !== null) allValues.push(r.value);
      });
    }

    const res = extractResourcesFromDir(dir, approachKey);
    if (res) {
      cpuArr.push(res.avgCpu);
      memArr.push(res.avgMemMB);
    }
  }

  if (allValues.length === 0 && allLatencies.length === 0) return null;

  const avgValue   = allValues.length   ? allValues.reduce((a, b) => a + b, 0) / allValues.length : null;
  const avgLatency = allLatencies.length ? allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length : null;

  return {
    approach:     approachKey,
    dataset:      dataset.key,
    iterations:   dirs.length,
    successCount,
    avgValue,
    avgLatency,
    stdLatency:   stdDev(allLatencies),
    minLatency:   allLatencies.length ? Math.min(...allLatencies) : null,
    maxLatency:   allLatencies.length ? Math.max(...allLatencies) : null,
    windowCount:  allLatencies.length,
    avgCpu:       cpuArr.length ? cpuArr.reduce((a, b) => a + b, 0) / cpuArr.length : null,
    avgMemMB:     memArr.length ? memArr.reduce((a, b) => a + b, 0) / memArr.length : null,
    stdMemMB:     stdDev(memArr),
  };
}

// ─── Table printing ───────────────────────────────────────────────────────────

function fmt(v, decimals = 4) {
  if (v === null || v === undefined || isNaN(v)) return 'N/A';
  return v.toFixed(decimals);
}

function pct(val, base) {
  if (val === null || base === null || base === 0) return 'N/A';
  return (Math.abs(val - base) / Math.abs(base) * 100).toFixed(2) + '%';
}

function printAccuracyTable(data, fetchingKey = 'fetching') {
  const approachKeys = APPROACHES.map(a => a.key);
  const nonFetching  = approachKeys.filter(k => k !== fetchingKey);

  // Header
  const header = ['Dataset', ...APPROACHES.map(a => a.label),
    ...nonFetching.map(k => `${APPROACHES.find(a=>a.key===k).label} Error`)];

  console.log('\n### Accuracy — Average Result Value per Dataset\n');
  console.log('| ' + header.join(' | ') + ' |');
  console.log('| ' + header.map(() => '---').join(' | ') + ' |');

  for (const dataset of DATASETS) {
    const row = [dataset.label];
    const baseline = data[fetchingKey]?.[dataset.key]?.avgValue ?? null;

    for (const ap of APPROACHES) {
      const v = data[ap.key]?.[dataset.key]?.avgValue ?? null;
      row.push(fmt(v, 4));
    }
    for (const k of nonFetching) {
      const v = data[k]?.[dataset.key]?.avgValue ?? null;
      row.push(pct(v, baseline));
    }
    console.log('| ' + row.join(' | ') + ' |');
  }
}

function printLatencyTable(data) {
  console.log('\n### Latency per Approach (across all datasets and iterations)\n');
  console.log('| Approach | Avg | Std Dev | Min | Max | Windows |');
  console.log('|---|---|---|---|---|---|');

  for (const ap of APPROACHES) {
    // Aggregate across all datasets
    const lats = [], mins = [], maxs = [];
    let windows = 0;

    for (const ds of DATASETS) {
      const d = data[ap.key]?.[ds.key];
      if (!d || d.avgLatency === null) continue;
      lats.push(d.avgLatency);
      if (d.minLatency !== null) mins.push(d.minLatency);
      if (d.maxLatency !== null) maxs.push(d.maxLatency);
      windows += d.windowCount || 0;
    }

    if (lats.length === 0) {
      console.log(`| ${ap.label} | N/A | N/A | N/A | N/A | 0 |`);
      continue;
    }

    const avg = lats.reduce((a, b) => a + b, 0) / lats.length;
    const std = stdDev(lats);
    const mn  = Math.min(...mins);
    const mx  = Math.max(...maxs);

    console.log(`| ${ap.label} | ${avg.toFixed(2)}ms | ${std.toFixed(2)}ms | ${mn.toFixed(0)}ms | ${mx.toFixed(0)}ms | ${windows} |`);
  }
}

function printResourceTable(data) {
  console.log('\n### Resource Usage per Approach (across all datasets and iterations)\n');
  console.log('| Approach | Avg CPU% | Avg Memory (MB) |');
  console.log('|---|---|---|');

  for (const ap of APPROACHES) {
    const cpus = [], mems = [], memStds = [];

    for (const ds of DATASETS) {
      const d = data[ap.key]?.[ds.key];
      if (!d || d.avgCpu === null) continue;
      cpus.push(d.avgCpu);
      if (d.avgMemMB !== null) mems.push(d.avgMemMB);
      if (d.stdMemMB !== null) memStds.push(d.stdMemMB);
    }

    if (cpus.length === 0) {
      console.log(`| ${ap.label} | N/A | N/A |`);
      continue;
    }

    const avgCpu = cpus.reduce((a, b) => a + b, 0) / cpus.length;
    const avgMem = mems.length ? mems.reduce((a, b) => a + b, 0) / mems.length : null;
    const avgStd = memStds.length ? memStds.reduce((a, b) => a + b, 0) / memStds.length : null;

    const memStr = avgMem !== null
      ? `${avgMem.toFixed(2)}MB${avgStd ? ` ±${avgStd.toFixed(2)}MB` : ''}`
      : 'N/A';

    console.log(`| ${ap.label} | ${avgCpu.toFixed(1)}% | ${memStr} |`);
  }
}

function printDatasetLatencyTable(data) {
  console.log('\n### Latency per Approach per Dataset\n');
  const cols = ['Dataset', ...APPROACHES.map(a => `${a.label} (ms)`)];
  console.log('| ' + cols.join(' | ') + ' |');
  console.log('| ' + cols.map(() => '---').join(' | ') + ' |');

  for (const ds of DATASETS) {
    const row = [ds.label];
    for (const ap of APPROACHES) {
      const d = data[ap.key]?.[ds.key];
      row.push(d?.avgLatency != null ? d.avgLatency.toFixed(2) : 'N/A');
    }
    console.log('| ' + row.join(' | ') + ' |');
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const writeJson = process.argv.includes('--json');

  console.log('='.repeat(80));
  console.log('PHASE 1 REPORT — 4-WAY APPROACH COMPARISON');
  console.log('Approaches: Fetching (baseline) | Naive Distributed | Approximation | Chunked');
  console.log('='.repeat(80));

  // Build data[approachKey][datasetKey] = aggregated stats
  const data = {};
  for (const ap of APPROACHES) {
    data[ap.key] = {};
    for (const ds of DATASETS) {
      const agg = aggregateApproachDataset(ap.key, ds);
      if (agg) {
        data[ap.key][ds.key] = agg;
        console.log(`  ✓ ${ap.label} / ${ds.label}: ${agg.successCount}/${agg.iterations} iters, ${agg.windowCount} windows`);
      } else {
        console.log(`  ✗ ${ap.label} / ${ds.label}: no data`);
      }
    }
  }

  printAccuracyTable(data);
  printLatencyTable(data);
  printResourceTable(data);
  printDatasetLatencyTable(data);

  if (writeJson) {
    const out = path.join('experiments', 'phase1-report.json');
    fs.writeFileSync(out, JSON.stringify(data, null, 2));
    console.log(`\nJSON written to ${out}`);
  }

  console.log('\n' + '='.repeat(80));
}

main().catch(e => { console.error(e); process.exit(1); });
