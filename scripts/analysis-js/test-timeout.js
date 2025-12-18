const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const LOGS_DIR = 'logs/approximation-patterns';
const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const LOG_FILES = [
  'approximation_approach_log.csv',
  'approximation_approach_resource_usage.csv',
  'replayer-log.csv'
];
const TIMEOUT_MS = 30 * 1000; // 30 seconds for quick test

if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });

async function testSinglePattern() {
  console.log('Testing single pattern with timeout...');
  
  const env = { ...process.env, DATA_PATH: 'approximation_test/challenging/exponential_growth' };

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
    console.log(`\nTimeout reached (${TIMEOUT_MS/1000}s), killing processes...`);
    timeoutReached = true;
    approach.kill('SIGKILL');
    publisher.kill('SIGKILL');
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

  console.log('Test completed!');
}

testSinglePattern().catch(console.error);
