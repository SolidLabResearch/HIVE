const { spawn } = require('child_process');
const fs = require('fs');

console.log('Testing single approximation pattern...');

const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const TIMEOUT_MS = 90 * 1000; // 90 seconds to allow windows to close

async function testSinglePattern() {
  console.log('Starting single pattern test...');
  
  const env = { 
    ...process.env, 
    DATA_PATH: 'approximation_test/challenging/exponential_growth' 
  };
  
  console.log('Starting approach...');
  const approach = spawn(APPROACH_CMD[0], APPROACH_CMD[1], { 
    stdio: 'inherit',
    env: env
  });

  await new Promise(res => setTimeout(res, 2000));
  
  console.log('Starting publisher...');
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

  console.log('Test completed. Checking log files...');
  
  if (fs.existsSync('approximation_approach_log.csv')) {
    console.log('\n=== LOG CONTENT ===');
    const content = fs.readFileSync('approximation_approach_log.csv', 'utf8');
    console.log(content);
    console.log('===================\n');
    console.log(`Total lines: ${content.split('\n').length - 1}`);
  } else {
    console.log('No log file found!');
  }
}

testSinglePattern().catch(console.error);
