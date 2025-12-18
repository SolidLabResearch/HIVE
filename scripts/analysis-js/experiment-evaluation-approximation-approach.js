const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const RUNS = 1;
const LOGS_DIR = 'logs/approximation-approach';
const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const LOG_FILES = [
  'approximation_approach_log.csv',
  'approximation_approach_resource_usage.csv',
  'replayer-log.csv'
];
const TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes

// Check if we should run pattern tests
const args = process.argv.slice(2);
const runPatternTests = args.includes('--patterns');
const testPattern = args.find(arg => arg.startsWith('--pattern='))?.split('=')[1];

if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });

async function runOnce(iter, patternName = null, dataPath = null) {
  const runLabel = patternName ? `${patternName} - Run ${iter}` : `Run ${iter}`;
  console.log(`--- ${runLabel} ---`);

  // Set environment variable if using custom data path
  const env = dataPath ? { ...process.env, DATA_PATH: dataPath } : process.env;

  // Start the approach process
  const approach = spawn(APPROACH_CMD[0], APPROACH_CMD[1], { 
    stdio: 'inherit',
    env: env
  });

  // Start the publisher process after a short delay
  await new Promise(res => setTimeout(res, 2000));
  const publisher = spawn(PUBLISH_CMD[0], PUBLISH_CMD[1], { 
    stdio: 'inherit',
    env: env
  });

  // Set up a timeout to kill both processes after TIMEOUT_MS
  const timeout = setTimeout(() => {
    console.log('Timeout reached, killing processes...');
    approach.kill();
    publisher.kill();
  }, TIMEOUT_MS);

  // Wait for publisher to finish or timeout
  await new Promise((resolve) => {
    publisher.on('exit', () => resolve());
  });

  // Wait for approach to finish or timeout
  await new Promise((resolve) => {
    approach.on('exit', () => resolve());
  });

  clearTimeout(timeout);

  // Move/rename log files into a subfolder for this iteration
  const iterDir = patternName 
    ? path.join(LOGS_DIR, 'patterns', patternName, `iteration${iter}`)
    : path.join(LOGS_DIR, `iteration${iter}`);
  
  if (!fs.existsSync(iterDir)) fs.mkdirSync(iterDir, { recursive: true });

  for (const file of LOG_FILES) {
    if (fs.existsSync(file)) {
      const newName = path.join(iterDir, file);
      fs.renameSync(file, newName);
    }
  }
}

async function runPatternTest(patternName, dataPath, runs = 5) {
  console.log(`\n=== Testing Pattern: ${patternName} ===`);
  console.log(`Data path: ${dataPath}`);
  
  for (let i = 1; i <= runs; i++) {
    await runOnce(i, patternName, dataPath);
    await new Promise(res => setTimeout(res, 1000));
  }
}

(async () => {
  if (runPatternTests || testPattern) {
    // Load experiment configuration for pattern tests
    const configPath = 'src/streamer/data/approximation_test/experiment_config.json';
    let experimentConfig = {};
    
    try {
      if (fs.existsSync(configPath)) {
        const configContent = fs.readFileSync(configPath, 'utf8').trim();
        if (configContent) {
          experimentConfig = JSON.parse(configContent);
        } else {
          throw new Error('Configuration file is empty');
        }
      } else {
        throw new Error('Configuration file not found');
      }
    } catch (error) {
      console.error(`Error loading configuration: ${error.message}`);
      console.error('Please run the data generation script first: python3 src/streamer/src/generate_approximation_test_data.py');
      process.exit(1);
    }

    if (testPattern) {
      // Test specific pattern
      const patternKey = testPattern.startsWith('challenging_') || testPattern.startsWith('favorable_') 
        ? testPattern 
        : `challenging_${testPattern}`;
      
      if (experimentConfig.data_paths[patternKey]) {
        await runPatternTest(patternKey, experimentConfig.data_paths[patternKey], 10);
      } else {
        console.error(`Pattern not found: ${testPattern}`);
        console.error('Available patterns:', Object.keys(experimentConfig.data_paths));
        process.exit(1);
      }
    } else {
      // Test all patterns
      console.log('Starting approximation pattern accuracy tests...');
      
      // Test challenging patterns
      console.log('\nTESTING CHALLENGING PATTERNS (Expected: Lower Accuracy) ');
      for (const pattern of experimentConfig.challenging_patterns || []) {
        const patternKey = `challenging_${pattern}`;
        if (experimentConfig.data_paths[patternKey]) {
          await runPatternTest(patternKey, experimentConfig.data_paths[patternKey], 5);
        }
      }
      
      // Test favorable patterns
      console.log('\nTESTING FAVORABLE PATTERNS (Expected: Higher Accuracy) ');
      for (const pattern of experimentConfig.favorable_patterns || []) {
        const patternKey = `favorable_${pattern}`;
        if (experimentConfig.data_paths[patternKey]) {
          await runPatternTest(patternKey, experimentConfig.data_paths[patternKey], 5);
        }
      }
      
      console.log('\nAll pattern tests complete!');
      console.log(`Results saved in: ${LOGS_DIR}/patterns`);
    }
  } else {
    // Original functionality - run standard tests
    console.log('Running standard approximation approach tests...');
    for (let i = 1; i <= RUNS; i++) {
      await runOnce(i);
      await new Promise(res => setTimeout(res, 2000));
    }
    console.log('All runs complete.');
  }
})();