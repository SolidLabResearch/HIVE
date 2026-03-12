#!/usr/bin/env node
'use strict';

// Analyze why Fetching vs Chunked show different values in the comparison table.
// Root cause: sequential benchmark → different wall-clock start times →
//             publisher replaces timestamps with current time → each approach
//             captures a slightly different number of observations in its window.

const fetching_first_data = 1773233319743;
const fetching_result     = 1773233380086;
const chunked_first_data  = 1773233700064;
const chunked_result      = 1773233759536;
const naiveDist_first_data= 1773233890128;
const naiveDist_result    = 1773233950520;

const delay_ms = 250; // 1000 / 4 Hz

console.log('=== Window observation counts per stream ===');
[
  ['Fetching',         fetching_first_data,  fetching_result],
  ['Chunked',          chunked_first_data,   chunked_result],
  ['Naive Distributed',naiveDist_first_data, naiveDist_result],
].forEach(([name, t_start, t_end]) => {
  const span_ms = t_end - t_start;
  const obs = Math.round(span_ms / delay_ms);
  console.log(`  ${name.padEnd(20)}: ${span_ms} ms  →  ${obs} obs/stream  (${2*obs} total)`);
});

const fetching_obs = Math.round((fetching_result - fetching_first_data) / delay_ms);
const chunked_obs  = Math.round((chunked_result  - chunked_first_data)  / delay_ms);
console.log(`\n  Delta Fetching vs Chunked: ${fetching_obs - chunked_obs} obs/stream`);
console.log('\nConclusion: Different observation counts → different window-slice averages.');
console.log('This is a sequential-benchmark artifact, NOT a Chunked accuracy error.');
console.log('verify-chunked-accuracy.js proves Chunked is mathematically 100% correct');
console.log('for the same input data as Fetching.');
