const { spawn } = require('child_process');
const fs = require('fs');

async function runOrchestrator(name, scriptPath, durationMs) {
    return new Promise((resolve) => {
        console.log(`\n--- Running ${name} (${durationMs}ms) ---`);
        const env = { ...process.env, DATA_PATH: '' };
        
        // Use 'pipe' for stdout to capture data, 'ignore' for stderr to suppress warnings if possible
        const orchestrator = spawn('node', [scriptPath], {
            stdio: ['ignore', 'pipe', 'ignore'], 
            env
        });
        
        let capturedCounts = [];
        let r2rCounts = [];

        orchestrator.stdout.on('data', (data) => {
            const str = data.toString();
            // Fetching logs
            if (name === 'Fetching' && str.includes('Processing valid result')) {
                const match = str.match(/count: (\d+)/);
                if (match) {
                     console.log(`[${name}] Captured Result Count: ${match[1]}`);
                     capturedCounts.push(parseInt(match[1]));
                }
            }
            // Chunked logs (Aggregator)
            if (name === 'Chunked' && str.includes('Executing the R2R Operator')) {
                 const counts = [...str.matchAll(/hasCount> \\"(\d+)\\"/g)].map(m => parseInt(m[1]));
                 const sum = counts.reduce((a, b) => a + b, 0);
                 console.log(`[${name}] Chunk Aggregation Input Sum: ${sum}`);
                 r2rCounts.push(sum);
            }
        });

        setTimeout(() => {
             console.log(`[${name}] Starting Publisher...`);
             const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
                stdio: ['ignore', 'ignore', 'ignore'], // SILENCE THE PUBLISHER
                env
             });
             
             setTimeout(() => {
                 console.log(`[${name}] Stopping...`);
                 publisher.kill();
                 orchestrator.kill();
                 resolve({ capturedCounts, r2rCounts });
             }, durationMs);
        }, 5000); // Give orchestrator 5s to settle
    });
}

async function main() {
    console.log("Starting Silent Verification...");
    
    // FETCHING (180s to be safe)
    const fetchResults = await runOrchestrator('Fetching', 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js', 180000);
    
    // CHUNKED (180s to be safe)
    const chunkResults = await runOrchestrator('Chunked', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js', 180000);
    
    const analysis = `
--- FINAL ANALYSIS ---
Fetching Counts: ${fetchResults.capturedCounts.join(', ')}
Chunked Aggregation Sums: ${chunkResults.r2rCounts.join(', ')}
    `;
    console.log(analysis);
    fs.writeFileSync('verify_output.txt', analysis);
}


main();
