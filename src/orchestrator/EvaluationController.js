const { spawn, execSync, } = require('child_process');
const fs = require('fs');
const path = require('path');

const RUNS = 1;
const TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes
const LOGS_DIR = 'logs/multi-approach';

const APPROACHES = [
    {
        name: 'chunked-query-approach',
        cmd: ['node', ['dist/approaches/StreamingQueryChunkedApproachOrchestrator.js']],
        logs: ['streaming_query_hive_resource_log.csv', 'streaming_query_chunk_aggregator_log.csv'],
    },
    {
        name: 'approximation-approach',
        cmd: ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']],
        logs: ['streaming_query_approximation_resource_log.csv', 'streaming_query_approximation_log.csv'],

    },
    {
        name: 'fetching-client-side-approach',
        cmd: ['node', ['dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js']],
        logs: ['streaming_query_clientfetch_resource_log.csv', 'streaming_query_clientfetch_log.csv'],
    }
];

const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const REPLAYER_LOG = 'replayer-log.csv';

function killLingeringProcesses() {
    for (const approach of APPROACHES) {
        try {
            execSync(`pkill -f ${path.basename(approach.cmd[1][0])}`);
        } catch (e) {

        }
    }
    try {
        execSync('pkill -f publish.js');
    } catch (e) { }
}

async function runOnce(iteration) {
    console.log(`--- Multi-Approach Run ${iteration} ---`);
    killLingeringProcesses(); // Ensure no lingering processes before starting

    const processes = [];

    for (const approach of APPROACHES) {
        console.log(`Starting approach: ${approach.name}`);
        const process = spawn(approach.cmd[0], approach.cmd[1], { stdio: 'inherit' });
        processes.push({ name: approach.name, process });
    }

    await new Promise(res => setTimeout(res, 5000)); // To ensure the subscriber is ready

    console.log('Starting the publisher...');
    const publisher = spawn(PUBLISH_CMD[0], PUBLISH_CMD[1], { stdio: 'inherit' });

    const timeout = setTimeout(() => {
        console.log('Timeout reached, killing processes...');
        publisher.kill();
        for (const p of processes) {
            p.process.kill();
        }
        killLingeringProcesses(); // Extra cleanup on timeout
    }, TIMEOUT_MS);

    await Promise.race([
        publisher,
        ...processes.map(p => p.process)
    ]);

    await new Promise(resolve => publisher.on('exit', resolve));
    for (const p of processes) {
        await new Promise(resolve => p.process.on('exit', resolve));
    }

    clearTimeout(timeout);
    killLingeringProcesses();

    const iterationDirectory = path.join(LOGS_DIR, `iteration${iteration}`);

    if (!fs.existsSync(iterationDirectory)) {
        fs.mkdirSync(iterationDirectory, { recursive: true });
    }

    for (const approach of APPROACHES) {
        for (const logFile of approach.logs) {
            if (fs.existsSync(logFile)) {
                const newName = path.join(iterationDirectory, `${approach.name}_${logFile}`);
                fs.renameSync(logFile, newName);
            }
        }
    }

    if (fs.existsSync(REPLAYER_LOG)) {
        fs.renameSync(REPLAYER_LOG, path.join(iterationDirectory, REPLAYER_LOG));
    }
}

(async () => {
    for (let i = 1; i <= RUNS; i++) {
        await runOnce(i);
        await new Promise(res => setTimeout(res, 2000)); // Wait between runs
    }
    console.log('All Multi-Approach runs complete.');
})();

