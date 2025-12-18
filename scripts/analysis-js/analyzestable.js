const fs = require('fs');
const csvParse = require('csv-parse/sync');

const content = fs.readFileSync('streaming_query_hive_latency_all_results.csv', 'utf8');
const records = csvParse.parse(content, { columns: true, skip_empty_lines: true });

const STEP = 30000;
const TOLERANCE = 5000; // 2 seconds tolerance

let currentIteration = null;
let prevTs = null;
let stable = true;
let stableIterations = [];
let unstableIterations = [];

for (const row of records) {
  if (row.iteration !== currentIteration) {
    if (currentIteration && stable) stableIterations.push(currentIteration);
    else if (currentIteration) unstableIterations.push(currentIteration);
    currentIteration = row.iteration;
    prevTs = row.result_ts;
    stable = true;
    continue;
  }
  const diff = row.result_ts - prevTs;
  if (Math.abs(diff - STEP) > TOLERANCE) stable = false;
  prevTs = row.result_ts;
}
// Add the last iteration
if (currentIteration && stable) stableIterations.push(currentIteration);
else if (currentIteration) unstableIterations.push(currentIteration);

console.log('Stable iterations:', stableIterations);
console.log('Unstable iterations:', unstableIterations);