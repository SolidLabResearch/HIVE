// Test timestamp extraction from chunk
const chunkSample = '"<https://rsp.js/aggregation_event/89302616-a8d1-4a99-8b97-1f7efac51d88> <https://saref.etsi.org/core/hasTimestamp> \\"1749592410235\\"^^<http://www.w3.org/2001/XMLSchema#long> .\\n    <https://rsp.js/aggregation_event/89302616-a8d1-4a99-8b97-1f7efac51d88> <https://saref.etsi.org/core/hasValue> \\"-22.666666666666668\\"^^<http://www.w3.org/2001/XMLSchema#float> ."';

function extractDataTimestampFromChunk(chunkData) {
    try {
        // Remove quotes if the data is JSON-stringified
        let cleanData = chunkData;
        if (cleanData.startsWith('"') && cleanData.endsWith('"')) {
            cleanData = JSON.parse(cleanData);
        }
        
        // Extract timestamp using regex
        // Format: <...> <https://saref.etsi.org/core/hasTimestamp> "TIMESTAMP"^^<...>
        const timestampMatch = cleanData.match(/hasTimestamp>\s*"(\d+)"/);
        
        if (timestampMatch && timestampMatch[1]) {
            const timestamp = parseInt(timestampMatch[1], 10);
            console.log(`✓ Extracted data timestamp from chunk: ${timestamp}`);
            return timestamp;
        } else {
            console.log(`✗ Could not extract timestamp from chunk`);
            return Date.now();
        }
    } catch (error) {
        console.log(`✗ Error extracting timestamp from chunk: ${error}`);
        return Date.now();
    }
}

console.log("Testing timestamp extraction...");
console.log("Sample chunk:", chunkSample.substring(0, 100) + "...");
console.log("");
const result = extractDataTimestampFromChunk(chunkSample);
console.log("");
console.log(`Result: ${result}`);
console.log(`Expected: 1749592410235`);
console.log(`Match: ${result === 1749592410235 ? "✓ YES" : "✗ NO"}`);
