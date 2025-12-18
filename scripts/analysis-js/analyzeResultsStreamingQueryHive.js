const fs = require('fs');
const path = require('path');
const csvParse = require('csv-parse/sync');

const LOGS_DIR = 'logs/streaming-query-hive';
const OUT_CSV = 'streaming_query_hive_latency_summary.csv';
const ITERATIONS = 1; // or detect dynamically
const NUM_CORES = 10; // Set this to the number of logical CPU cores on your system

const summaryRows = [['iteration', 'registered_query_ts', 'first_result_ts', 'latency_ms', 'result_value', 'avg_cpu_percent', 'avg_heapUsedMB']];

for (let i = 1; i <= ITERATIONS; i++) {
  const logPath = path.join(LOGS_DIR, `iteration${i}`, 'streaming_query_chunk_aggregator_log.csv');
  const resourcePath = path.join(LOGS_DIR, `iteration${i}`, 'streaming_query_hive_resource_log.csv');
  let avgCpuPercent = '';
  let avgHeapUsedMB = '';

  // Calculate average cpu percent and average heapUsedMB if file exists
  if (fs.existsSync(resourcePath)) {
    const resourceContent = fs.readFileSync(resourcePath, 'utf8');
    const resourceRecords = csvParse.parse(resourceContent, { columns: true, skip_empty_lines: true });
    const cpuPercents = [];
    const heapUsedMBs = [];
    for (let j = 1; j < resourceRecords.length; j++) {
      const prev = resourceRecords[j - 1];
      const curr = resourceRecords[j];
      const deltaCpuUser = Number(curr.cpu_user) - Number(prev.cpu_user);
      const deltaTime = Number(curr.timestamp) - Number(prev.timestamp);
      if (deltaTime > 0) {
        const cpuPercent = ((deltaCpuUser) / (deltaTime * NUM_CORES)) * 100;
        cpuPercents.push(cpuPercent);
      }
      heapUsedMBs.push(Number(curr.heapUsedMB));
    }
    if (cpuPercents.length > 0) {
      avgCpuPercent = (cpuPercents.reduce((a, b) => a + b, 0) / cpuPercents.length).toFixed(2);
    }
    if (heapUsedMBs.length > 0) {
      avgHeapUsedMB = (heapUsedMBs.reduce((a, b) => a + b, 0) / heapUsedMBs.length).toFixed(2);
    }
  }

  if (!fs.existsSync(logPath)) {
    console.warn(`Missing log for iteration ${i}`);
    summaryRows.push([i, '', '', '', '', avgCpuPercent, avgHeapUsedMB]);
    continue;
  }
  
  let content = fs.readFileSync(logPath, 'utf8');
  let records;
  try {
    // Remove repeated header lines
    content = content
      .split('\n')
      .filter((line, idx) => line.trim() !== 'timestamp,message' || idx === 0)
      .join('\n');
    // Replace escaped quotes with double quotes for CSV compliance
    content = content.replace(/\\"/g, '""');
    records = csvParse.parse(content, { 
      columns: true, 
      skip_empty_lines: true,
      relax_quotes: true,
      skip_records_with_error: true
    });
  } catch (err) {
    console.warn(`Error parsing CSV for iteration ${i}: ${err.message}`);
    summaryRows.push([i, '', '', '', '', avgCpuPercent, avgHeapUsedMB]);
    continue;
  }

  let registeredTs = null;
  let resultTs = null;
  let resultValue = '';

  for (const row of records) {
    if (!registeredTs && row.message && row.message.includes('Registered Query')) {
      registeredTs = Number(row.timestamp);
    }
    if (registeredTs && !resultTs && row.message && row.message.includes('calculated result')) {
      resultTs = Number(row.timestamp);
      // Try to extract the value from the message (look for hasValue with a numeric value)
      const valueMatch = row.message.match(/hasValue[^"]*"([+-]?\d*\.?\d+)"/);
      if (valueMatch) {
        resultValue = valueMatch[1];
      } else {
        // Fallback: try to find any number in quotes
        const fallbackMatch = row.message.match(/"([+-]?\d*\.?\d+)"/);
        resultValue = fallbackMatch ? fallbackMatch[1] : '';
      }
      break; // Only the first result after registration
    }
  }

  if (registeredTs && resultTs) {
    summaryRows.push([i, registeredTs, resultTs, resultTs - registeredTs, resultValue, avgCpuPercent, avgHeapUsedMB]);
  } else {
    summaryRows.push([i, registeredTs || '', resultTs || '', '', '', avgCpuPercent, avgHeapUsedMB]);
  }
}

fs.writeFileSync(OUT_CSV, summaryRows.map(r => r.join(',')).join('\n'));
console.log(`Latency and resource usage summary written to ${OUT_CSV}`);