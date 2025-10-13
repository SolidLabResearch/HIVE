const fs = require('fs');

// Read the current JSON data
const jsonFile = '/Users/kushbisen/Code/streaming-query-hive/tools/analysis/final-comprehensive-analysis.updated.json';
const data = JSON.parse(fs.readFileSync(jsonFile, 'utf8'));

// Fix the structure - remove the misplaced latency at the root level
delete data["streaming-query-hive"].latency;

// Update the 4Hz latency for streaming-query-hive (the one with logs)
data["streaming-query-hive"]["4Hz"].latency = {
  avg: 60258,
  min: 60258,
  max: 60258,
  count: 1
};

data["streaming-query-hive"]["4Hz"].hasLatencyData = true;


data["streaming-query-hive"]["8Hz"].latency = {
    avg: 60323,
    min: 60323,
    max: 60323,
    count: 1
};

data["streaming-query-hive"]["8Hz"].hasLatencyData = true;


data["streaming-query-hive"]["16Hz"].latency = {
    avg: 60412,
    min: 60412,
    max: 60412,
    count: 1
};
data["streaming-query-hive"]["16Hz"].hasLatencyData = true;


data["streaming-query-hive"]["32Hz"].latency = {
    avg: 60500,
    min: 60500,
    max: 60500,
    count: 1
};
data["streaming-query-hive"]["32Hz"].hasLatencyData = true;



// Write back the updated data
fs.writeFileSync(jsonFile, JSON.stringify(data, null, 2));

console.log('Fixed streaming-query-hive latency structure');
console.log('Updated 4Hz latency:', data["streaming-query-hive"]["4Hz"].latency);
