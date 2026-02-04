#!/usr/bin/env node

/**
 * Debug script to examine chunk timestamps and filtering for step pattern
 */

const fs = require('fs');
const path = require('path');

console.log('='.repeat(80));
console.log('CHUNKED APPROACH TIMESTAMP DEBUG ANALYSIS');
console.log('='.repeat(80));
console.log('');

// Read the chunked latency log
const chunkedLog = fs.readFileSync('pattern_comparison_results/chunked_step_pattern.csv', 'utf-8');
const chunkedLines = chunkedLog.trim().split('\n');
const chunkedHeaders = chunkedLines[0].split(',');

console.log('Chunked Results for step_pattern:');
console.log('-'.repeat(80));

for (let i = 1; i < chunkedLines.length; i++) {
  const values = chunkedLines[i].split(',');
  const row = {};
  chunkedHeaders.forEach((header, idx) => {
    row[header.trim()] = values[idx];
  });
  
  console.log(`\nWindow ${row.window_number}:`);
  console.log(`  Query registered at: ${row.query_registered_at}`);
  console.log(`  First data received: ${row.first_data_received_at}`);
  console.log(`  Expected window close: ${row.expected_window_close}`);
  console.log(`  Last chunk received: ${row.last_chunk_received_at}`);
  console.log(`  Interval trigger: ${row.interval_trigger_at}`);
  console.log(`  Result emitted: ${row.result_emitted_at}`);
  console.log(`  Result value: ${row.result_value}`);
  
  // Calculate what the window should cover
  const queryReg = parseInt(row.query_registered_at);
  const windowClose = parseInt(row.expected_window_close);
  const windowStart = windowClose - 120000; // RANGE = 120s
  
  console.log(`\n  Window should cover data from:`);
  console.log(`    Start: ${windowStart} (${new Date(windowStart).toISOString()})`);
  console.log(`    End: ${windowClose} (${new Date(windowClose).toISOString()})`);
}

console.log('\n' + '='.repeat(80));
console.log('EXPECTED BEHAVIOR:');
console.log('='.repeat(80));
console.log('');
console.log('Step Pattern: data switches from -23 to -15 at t=60s into stream');
console.log('');
console.log('Window 1 (first 120s): Should see mostly -23 values');
console.log('  - First 60s: -23 (low regime)');
console.log('  - Last 60s: -15 (high regime)');
console.log('  - Expected average: ~-19.0');
console.log('');
console.log('Window 2 (next 60s of data, overlapping): ');
console.log('  - RANGE=120s, STEP=60s');
console.log('  - Window covers: [t+60s to t+180s] of stream data');
console.log('  - First 0s: transition point (at t=60s)');
console.log('  - Last 120s: all -15 (high regime)');
console.log('  - Expected average: ~-15.0 to -17.0 (depending on exact timing)');
console.log('');
console.log('Actual Results:');
console.log('  Fetching Window 2: -17.684 (BASELINE - CORRECT)');
console.log('  Chunked Window 2:  -19.577 (10.70% MAPE - WRONG)');
console.log('');
console.log('The -19.577 value suggests Chunked is including too much -23 data');
console.log('from the first regime, indicating chunks from wrong time period');
console.log('are being included in Window 2.');
console.log('');
console.log('='.repeat(80));
console.log('HYPOTHESIS:');
console.log('='.repeat(80));
console.log('');
console.log('The hasTimestamp in chunks may NOT accurately represent the data');
console.log('time range. It might be:');
console.log('  1. The query registration time (wrong)');
console.log('  2. The chunk emission time (wrong)');
console.log('  3. The chunk window close time (correct, but maybe not precise)');
console.log('');
console.log('To fix: Need to verify chunks are being filtered correctly by');
console.log('examining actual chunk timestamps from aggregator logs.');
console.log('');
