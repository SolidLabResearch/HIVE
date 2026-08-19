const { spawn } = require('child_process');
const fs = require('fs');

async function runOrchestrator(name, scriptPath, durationMs) {
    return new Promise((resolve) => {
        console.log(`\n--- Running ${name} (${durationMs}ms) ---`);
        const env = { ...process.env, DATA_PATH: '' };
        
        const orchestrator = spawn('node', [scriptPath], {
            stdio: ['ignore', 'pipe', 'pipe'],
            env
        });
        
        // Capture output to find counts
        let output = '';
        orchestrator.stdout.on('data', (data) => {
            const str = data.toString();
            output += str;
            // Provide live feedback on counts
            if (str.includes('count:')) console.log(`[${name}] Found count log: ${str.trim()}`);
            if (str.includes('Processing valid result')) console.log(`[${name}] RESULT: ${str.trim()}`);
        });

        // Start Publisher
        setTimeout(() => {
             console.log(`[${name}] Starting Publisher...`);
             const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
                stdio: ['ignore', 'pipe', 'pipe'],
                env
             });
             
             setTimeout(() => {
                 console.log(`[${name}] Stopping...`);
                 publisher.kill();
                 orchestrator.kill();
                 resolve(output);
             }, durationMs);
        }, 2000);
    });
}

async function main() {
    // Run Chunked first (known to work with fix)
    console.log("Starting Verification...");
    
    // FETCHING
    // const fetchingOutput = await runOrchestrator('Fetching', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js', 90000);
    // Note: Fetching window is 120s, so we need > 120s run time effectively? 
    // Wait, the previous chunked run worked in ~45s? 
    // Chunked window logic might be faster or use different range?
    // Let's check: Chunked [RANGE 30000 STEP 30000].
    // Fetching [RANGE 120000 STEP 60000].
    // Ah! Fetching needs 120s of data to close the first window. Chunked produces results every 30s.
    // If we want to compare, we must run Fetching for > 120s.
    
    const fetchingOutput = await runOrchestrator('Fetching', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js', 140000);
    
    // CHUNKED
    const chunkedOutput = await runOrchestrator('Chunked', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js', 140000);
    
    console.log("\n--- Analysis ---");
    
    // Extract Fetching Count
    const fetchMatch = fetchingOutput.match(/Processing valid result: .* with count: (\d+)/);
    if (fetchMatch) {
        console.log(`Fetching Count from logs: ${fetchMatch[1]}`);
    } else {
        console.log("Fetching: No valid result log found.");
    }
    
    // Extract Chunked Counts (sum them)
    // Chunked logs: "Merged Binding: ... count... value: 120" or similar from debug logs
    // Or check the Aggregator output if it logs inputs.
    // The verify script captures stdout, so we rely on console.logs.
    // Our aggregator logs "Executing the R2R Operator with results: ... hasCount ..."
    
    const chunkMatches = [...chunkedOutput.matchAll(/hasCount> \\"(\d+)\\"/g)];
    let totalChunkCount = 0;
    if (chunkMatches.length > 0) {
        // This might match duplicates if logs are verbose.
        // Better to extract the FINAL result line?
        // Let's check for "Generated Output Query Event".
        // But that doesn't show the sum count unless we changed the aggregator query to return it?
        // We changed aggregator query to return `SUM(?val * ?cnt) / SUM(?cnt)`. It returns the avg.
        // We need to see the INPUT counts.
        console.log(`Found ${chunkMatches.length} count occurrences in logic.`);
        // Assuming the aggregator log "Executing the R2R Operator..." lists all inputs.
        // We can parse that line.
        
        // Find the line "Executing the R2R Operator with results:"
        const execLines = chunkedOutput.match(/Executing the R2R Operator with results: .*/g);
        if (execLines) {
            execLines.forEach((line, idx) => {
                 const counts = [...line.matchAll(/hasCount> \\"(\d+)\\"/g)].map(m => parseInt(m[1]));
                 const sum = counts.reduce((a, b) => a + b, 0);
                 console.log(`Chunked Aggregation ${idx+1} Input Sum: ${sum} (from ${counts.length} chunks)`);
            });
        }
    } else {
        console.log("Chunked: No count logs found.");
    }
}

main();
