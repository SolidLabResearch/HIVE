const fs = require('fs');

// Read the current JSON data
const jsonFile = '/Users/kushbisen/Code/streaming-query-hive/tools/analysis/final-comprehensive-analysis.updated.json';
const data = JSON.parse(fs.readFileSync(jsonFile, 'utf8'));

// Streaming Query Hive latency calculation from logs
// From iteration3 logs: 
// Start: 1758125067290 (init() called)
// First result: 1758125127548 (first "calculated result")
const streamingQueryHiveStartTime = 1758125067290;
const streamingQueryHiveFirstResult = 1758125127548;
const streamingQueryHiveLatency = streamingQueryHiveFirstResult - streamingQueryHiveStartTime;

console.log(`Streaming Query Hive Start: ${streamingQueryHiveStartTime}`);
console.log(`Streaming Query Hive First Result: ${streamingQueryHiveFirstResult}`);
console.log(`Streaming Query Hive Latency: ${streamingQueryHiveLatency} ms`);

// Update the streaming-query-hive latency
data["streaming-query-hive"].latency = streamingQueryHiveLatency;

// Write back the updated data
fs.writeFileSync(jsonFile, JSON.stringify(data, null, 2));

console.log('Updated streaming-query-hive latency in JSON file');
console.log('Updated JSON preview:');
console.log(JSON.stringify(data, null, 2));
