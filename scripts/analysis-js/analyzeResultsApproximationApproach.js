const fs = require('fs');
const path = require('path');
const csvParse = require('csv-parse/sync');

const LOGS_DIR = '/Users/kushbisen/Code/streaming-query-hive/logs/approximation-approach';
const OUT_CSV = 'approximation_approach_latency_summary.csv';
const ITERATIONS = 35; // or detect dynamically
const NUM_CORES = 10; // Set this to the number of logical CPU cores on your system

let summaryRows = [['iteration', 'registered_query_ts', 'first_result_ts', 'latency_ms', 'result_value', 'avg_cpu_percent', 'avg_heapUsedMB']];

for (let i = 1; i <= ITERATIONS; i++) {
  const logPath = path.join(LOGS_DIR, `iteration${i}`, 'approximation_approach_log.csv');
  const resourcePath = path.join(LOGS_DIR, `iteration${i}`, 'approximation_approach_resource_usage.csv');
  let avgCpuPercent = '';
  let avgHeapUsedMB = '';

  // Calculate average cpu percent and average heapUsedMB if file exists
  if (fs.existsSync(resourcePath)) {
    const resourceContent = fs.readFileSync(resourcePath, 'utf8');
    const resourceRecords = csvParse.parse(resourceContent, { columns: true, skip_empty_lines: true });
    let cpuPercents = [];
    let heapUsedMBs = [];
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

  console.log(`\nProcessing iteration ${i} with ${records.length} records`);
  
  // First, let's see what columns are available
  if (records.length > 0) {
    console.log(`Available columns: ${Object.keys(records[0]).join(', ')}`);
  }
  
  for (const row of records) {
    // Debug: log all messages to see what we're working with
    if (row.message) {
      if (row.message.includes('Registered query') || row.message.includes('unifiedAverage') || row.message.includes('average') || row.message.includes('result') || row.message.includes('value')) {
        console.log(`Iteration ${i}: Message: ${row.message}`);
      }
    }
    
    if (!registeredTs && row.message && row.message.includes('Registered query')) {
      registeredTs = Number(row.timestamp);
      console.log(`Iteration ${i}: Found registration at ${registeredTs}`);
    }
    
    // Look for result messages with multiple patterns
    if (registeredTs && !resultTs && row.message) {
      const message = row.message;
      if (message.includes('unifiedAverage') || 
          message.includes('result') || 
          message.includes('average') ||
          message.includes('value') ||
          message.includes('approximation')) {
        
        resultTs = Number(row.timestamp);
        console.log(`Iteration ${i}: Found potential result at ${resultTs}, message: ${message}`);
        
        // Try multiple patterns to extract the value
        let valueMatch = message.match(/unifiedAverage[^"]*"([+-]?\d*\.?\d+)"/);
        if (!valueMatch) {
          valueMatch = message.match(/hasValue[^"]*"([+-]?\d*\.?\d+)"/);
        }
        if (!valueMatch) {
          valueMatch = message.match(/result[^"]*"([+-]?\d*\.?\d+)"/);
        }
        if (!valueMatch) {
          valueMatch = message.match(/average[^"]*"([+-]?\d*\.?\d+)"/);
        }
        if (!valueMatch) {
          valueMatch = message.match(/value[^"]*"([+-]?\d*\.?\d+)"/);
        }
        if (!valueMatch) {
          // Look for any numeric value in quotes
          valueMatch = message.match(/"([+-]?\d*\.?\d+)"/);
        }
        if (!valueMatch) {
          // Look for numeric values without quotes
          valueMatch = message.match(/([+-]?\d*\.?\d+)/);
        }
        
        if (valueMatch) {
          resultValue = valueMatch[1];
          console.log(`Iteration ${i}: Extracted value: ${resultValue}`);
        } else {
          console.log(`Iteration ${i}: Could not extract value from message: ${message}`);
        }
        break; // Only the first result after registration
      }
    }
  }

  console.log(`Iteration ${i}: registeredTs=${registeredTs}, resultTs=${resultTs}, resultValue=${resultValue}`);

  if (registeredTs && resultTs) {
    summaryRows.push([i, registeredTs, resultTs, resultTs - registeredTs, resultValue, avgCpuPercent, avgHeapUsedMB]);
  } else {
    summaryRows.push([i, registeredTs || '', resultTs || '', '', '', avgCpuPercent, avgHeapUsedMB]);
  }
}

fs.writeFileSync(OUT_CSV, summaryRows.map(r => r.join(',')).join('\n'));
console.log(`Latency and resource usage summary written to ${OUT_CSV}`);
