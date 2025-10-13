const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

console.log('Script starting...');

const RUNS_PER_PATTERN = 1;
const LOGS_DIR = 'logs/fetching-client-side-patterns';
const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const LOG_FILES = [
  'fetching_client_side_log.csv',
  'fetching_client_side_resource_usage.csv',
  'replayer-log.csv'
];
const TIMEOUT_MS = 150 * 1000; // Increased to 150 seconds to allow full 120s data stream + processing

console.log('Loading configuration...');
const configPath = 'src/streamer/data/approximation_test/experiment_config.json';
let experimentConfig = {};
if (fs.existsSync(configPath)) {
  experimentConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  console.log('Configuration loaded successfully');
} else {
  console.error('Configuration file not found:', configPath);
  process.exit(1);
}

if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });

async function runOnceWithPattern(patternName, dataPath, iter) {
  console.log(`--- Testing ${patternName} - Run ${iter} ---`);
  
  // Clean up any existing log files before starting
  console.log('Cleaning up existing log files...');
  for (const file of LOG_FILES) {
    if (fs.existsSync(file)) {
      fs.unlinkSync(file);
      console.log(`Removed existing ${file}`);
    }
  }
  
  const env = { ...process.env, DATA_PATH: dataPath };
  
  const approach = spawn(APPROACH_CMD[0], APPROACH_CMD[1], { 
    stdio: 'inherit',
    env: env
  });

  await new Promise(res => setTimeout(res, 2000));
  const publisher = spawn(PUBLISH_CMD[0], PUBLISH_CMD[1], { 
    stdio: 'inherit',
    env: env
  });

  let timeoutReached = false;
  const timeout = setTimeout(() => {
    console.log(`Timeout reached (${TIMEOUT_MS/1000}s), killing processes...`);
    timeoutReached = true;
    try { approach.kill('SIGKILL'); } catch(e) {}
    try { publisher.kill('SIGKILL'); } catch(e) {}
  }, TIMEOUT_MS);

  let publisherDone = false;
  let approachDone = false;

  await new Promise((resolve) => {
    publisher.on('exit', (code) => {
      console.log(`Publisher exited with code ${code}`);
      publisherDone = true;
      if (approachDone || timeoutReached) {
        clearTimeout(timeout);
        resolve();
      }
    });

    approach.on('exit', (code) => {
      console.log(`Approach exited with code ${code}`);
      approachDone = true;
      if (publisherDone || timeoutReached) {
        clearTimeout(timeout);
        resolve();
      }
    });

    setTimeout(() => {
      console.log('Forcing completion...');
      clearTimeout(timeout);
      resolve();
    }, TIMEOUT_MS + 5000);
  });

  const iterDir = path.join(LOGS_DIR, patternName, `iteration${iter}`);
  if (!fs.existsSync(iterDir)) fs.mkdirSync(iterDir, { recursive: true });

  for (const file of LOG_FILES) {
    if (fs.existsSync(file)) {
      const newName = path.join(iterDir, file);
      fs.renameSync(file, newName);
      console.log(`Moved ${file} to ${newName}`);
    }
  }
}

async function testPattern(patternName, dataPath) {
  console.log(`\n=== Testing Pattern: ${patternName} ===`);
  console.log(`Data path: ${dataPath}`);
  
  for (let i = 1; i <= RUNS_PER_PATTERN; i++) {
    await runOnceWithPattern(patternName, dataPath, i);
    
    // Small delay between runs to ensure complete cleanup
    if (i < RUNS_PER_PATTERN) {
      console.log('Waiting 5 seconds before next run...');
      await new Promise(res => setTimeout(res, 5000));
    }
  }
}

async function main() {
  console.log('Starting fetching client side pattern accuracy tests...\n');

  // Test challenging patterns (expected to have lower accuracy with approximation)
  
  for (const pattern of experimentConfig.challenging_patterns || []) {
    await testPattern(`challenging_${pattern}`, `approximation_test/challenging/${pattern}`);
    console.log('Waiting 3 seconds before next pattern...');
    await new Promise(res => setTimeout(res, 3000));
  }

  // Test favorable patterns (expected to have higher accuracy with approximation)
  
  for (const pattern of experimentConfig.favorable_patterns || []) {
    await testPattern(`favorable_${pattern}`, `approximation_test/favorable/${pattern}`);
    console.log('Waiting 3 seconds before next pattern...');
    await new Promise(res => setTimeout(res, 3000));
  }
  
  console.log(`Results saved in: ${LOGS_DIR}`);
  console.log('\\nTo analyze results, check the CSV files in each iteration folder.');
}

main().catch(console.error);
