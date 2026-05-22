const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const SCRIPT_PATH = path.resolve(__dirname, 'analyzeResultsStreamingQueryHive.js');
const TEMP_ROOTS = [];

function makeTempRoot(prefix) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
    TEMP_ROOTS.push(root);
    return root;
}

function makeSingleIterationFixture(prefix, label) {
    const root = makeTempRoot(prefix);
    const dataPath = path.join(root, 'data');
    const chunkedCsvPath = path.join(root, `${label}-chunked.csv`);
    const stdoutLogPath = path.join(root, `${label}-stdout.log`);
    const metadataPath = path.join(root, `${label}-metadata.json`);

    fs.mkdirSync(dataPath, { recursive: true });
    fs.writeFileSync(
        chunkedCsvPath,
        'timestamp,message\n1,Registered Query\n2,calculated result "42"\n',
    );
    fs.writeFileSync(stdoutLogPath, 'Registered Query\ncalculated result "42"\n');
    fs.writeFileSync(metadataPath, JSON.stringify({ replayMetadata: { label } }, null, 2));

    return {
        root,
        dataPath,
        chunkedCsvPath,
        stdoutLogPath,
        metadataPath,
    };
}

function runAnalyzer(args, envOverrides, cwd) {
    return spawnSync('node', [SCRIPT_PATH, ...args], {
        cwd,
        encoding: 'utf8',
        env: {
            ...process.env,
            ...envOverrides,
        },
    });
}

afterEach(() => {
    while (TEMP_ROOTS.length > 0) {
        const root = TEMP_ROOTS.pop();
        fs.rmSync(root, { recursive: true, force: true });
    }
});

describe('analyzeResultsStreamingQueryHive CLI contract', () => {
    test('--help prints usage and exits successfully', () => {
        const result = runAnalyzer(['--help'], {}, makeTempRoot('sqh-analyzer-help-'));

        expect(result.status).toBe(0);
        expect(result.stderr).toContain('Usage: node scripts/analysis-js/analyzeResultsStreamingQueryHive.js');
    });

    test('single-iteration mode rejects missing required inputs', () => {
        const result = runAnalyzer(['--iteration', '1'], {}, makeTempRoot('sqh-analyzer-missing-'));

        expect(result.status).toBe(1);
        expect(result.stderr).toContain('Missing required analyzer inputs: --frequency, --chunked-csv, --stdout-log, --metadata');
        expect(result.stderr).toContain('Usage: node scripts/analysis-js/analyzeResultsStreamingQueryHive.js');
    });

    test('CLI args override environment variables', () => {
        const cliFixture = makeSingleIterationFixture('sqh-analyzer-cli-', 'cli');
        const envFixture = makeSingleIterationFixture('sqh-analyzer-env-', 'env');

        const result = runAnalyzer(
            [
                '--data-path', cliFixture.dataPath,
                '--iteration', '1',
                '--chunked-csv', cliFixture.chunkedCsvPath,
                '--stdout-log', cliFixture.stdoutLogPath,
                '--metadata', cliFixture.metadataPath,
                '--frequency', '7.5',
                '--debug-chunks', 'true',
            ],
            {
                DATA_PATH: envFixture.dataPath,
                WEARABLE_FREQUENCY: '1.5',
                STREAMING_QUERY_HIVE_ITERATION: '1',
                STREAMING_QUERY_HIVE_CHUNKED_CSV: envFixture.chunkedCsvPath,
                STREAMING_QUERY_HIVE_STDOUT_LOG: envFixture.stdoutLogPath,
                STREAMING_QUERY_HIVE_METADATA: envFixture.metadataPath,
                STREAMING_QUERY_HIVE_DEBUG_CHUNKS: '0',
            },
            cliFixture.root,
        );

        expect(result.status).toBe(0);
        expect(result.stdout).toContain('[DEBUG_CHUNKS] resolved analyzer inputs');
        expect(result.stdout).toContain(`"dataPath": "${cliFixture.dataPath}"`);
        expect(result.stdout).toContain('"frequency": 7.5');
        expect(result.stdout).toContain(`"chunkedCsvPath": "${cliFixture.chunkedCsvPath}"`);
        expect(result.stdout).toContain(`"stdoutLogPath": "${cliFixture.stdoutLogPath}"`);
        expect(result.stdout).toContain(`"metadataPath": "${cliFixture.metadataPath}"`);
        expect(result.stdout).not.toContain(envFixture.dataPath);
        expect(result.stdout).not.toContain('"frequency": 1.5');
    });

    test('environment variables still work when CLI args are absent', () => {
        const envFixture = makeSingleIterationFixture('sqh-analyzer-env-fallback-', 'env-fallback');

        const result = runAnalyzer(
            [],
            {
                DATA_PATH: envFixture.dataPath,
                WEARABLE_FREQUENCY: '4.25',
                STREAMING_QUERY_HIVE_ITERATION: '1',
                STREAMING_QUERY_HIVE_CHUNKED_CSV: envFixture.chunkedCsvPath,
                STREAMING_QUERY_HIVE_STDOUT_LOG: envFixture.stdoutLogPath,
                STREAMING_QUERY_HIVE_METADATA: envFixture.metadataPath,
                STREAMING_QUERY_HIVE_DEBUG_CHUNKS: '1',
            },
            envFixture.root,
        );

        expect(result.status).toBe(0);
        expect(result.stdout).toContain('[DEBUG_CHUNKS] resolved analyzer inputs');
        expect(result.stdout).toContain(`"dataPath": "${envFixture.dataPath}"`);
        expect(result.stdout).toContain('"frequency": 4.25');
        expect(result.stdout).toContain(`"chunkedCsvPath": "${envFixture.chunkedCsvPath}"`);
        expect(result.stdout).toContain(`"stdoutLogPath": "${envFixture.stdoutLogPath}"`);
        expect(result.stdout).toContain(`"metadataPath": "${envFixture.metadataPath}"`);
    });

    test('--debug-chunks true enables debug mode and prints resolved inputs', () => {
        const fixture = makeSingleIterationFixture('sqh-analyzer-debug-true-', 'debug-true');

        const result = runAnalyzer(
            [
                '--data-path', fixture.dataPath,
                '--iteration', '1',
                '--chunked-csv', fixture.chunkedCsvPath,
                '--stdout-log', fixture.stdoutLogPath,
                '--metadata', fixture.metadataPath,
                '--frequency', '6.5',
                '--debug-chunks', 'true',
            ],
            {},
            fixture.root,
        );

        expect(result.status).toBe(0);
        expect(result.stdout).toContain('[DEBUG_CHUNKS] resolved analyzer inputs');
        expect(result.stdout).toContain(`"dataPath": "${fixture.dataPath}"`);
        expect(result.stdout).toContain('"frequency": 6.5');
    });

    test('--debug-chunks false disables debug mode even if the env var is true', () => {
        const fixture = makeSingleIterationFixture('sqh-analyzer-debug-false-', 'debug-false');

        const result = runAnalyzer(
            [
                '--data-path', fixture.dataPath,
                '--iteration', '1',
                '--chunked-csv', fixture.chunkedCsvPath,
                '--stdout-log', fixture.stdoutLogPath,
                '--metadata', fixture.metadataPath,
                '--frequency', '8.5',
                '--debug-chunks', 'false',
            ],
            {
                STREAMING_QUERY_HIVE_DEBUG_CHUNKS: '1',
            },
            fixture.root,
        );

        expect(result.status).toBe(0);
        expect(result.stdout).not.toContain('[DEBUG_CHUNKS] resolved analyzer inputs');
    });
});