const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const RUNS = 35;
const LOGS_DIR = process.env.CUSTOM_LOG_DIR || 'logs/streaming-query-hive';
const APPROACH_CMD = ['node', 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'];
const LOG_FILES = [
  'streaming_query_hive_log.csv',
  'streaming_query_hive_resource_usage.csv',
  'replayer-log.csv'
];
const TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

// Check if we should run frequency tests
const args = process.argv.slice(2);
const runFrequencyTests = args.includes('--frequencies');
const testFrequency = args.find(arg => arg.startsWith('--frequency='))?.split('=')[1];

if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });

function killLingeringProcesses() {
  try {
    execSync('pkill -f StreamingQueryHiveApproachOrchestrator.js');
  } catch (e) { }
  try {
    execSync('pkill -f experiment-publisher.js');
  } catch (e) { }
  try {
    execSync('pkill -f publish.js');
  } catch (e) { }
}

async function runOnce(iter, frequency = null) {
  const runLabel = frequency ? `${frequency} - Run ${iter}` : `Run ${iter}`;
  console.log(`--- ${runLabel} ---`);

  killLingeringProcesses(); // Ensure no lingering processes before starting

  const approach = spawn(APPROACH_CMD[0], APPROACH_CMD.slice(1), { stdio: 'inherit' });

  if (frequency) {
    // Frequency testing with experiment publishers
    await new Promise(res => setTimeout(res, 2000));
    
    const publishers = [];
    
    // Start smartphone publisher
    console.log(`Starting smartphone publisher for ${frequency}...`);
    const smartphoneCmd = ['node', 'dist/streamer/src/experiment-publisher.js', 
      `src/streamer/data/frequency_variants/2mins/smartphone/${frequency}/data.nt`, 
      'smartphoneX', 
      frequency.replace('Hz', '')];
    
    const smartphonePublisher = spawn(smartphoneCmd[0], smartphoneCmd.slice(1), { stdio: 'inherit' });
    publishers.push(smartphonePublisher);
    
    // Start wearable publisher
    console.log(`Starting wearable publisher for ${frequency}...`);
    const wearableCmd = ['node', 'dist/streamer/src/experiment-publisher.js', 
      `src/streamer/data/frequency_variants/2mins/wearable/${frequency}/data.nt`, 
      'wearableX', 
      frequency.replace('Hz', '')];
    
    const wearablePublisher = spawn(wearableCmd[0], wearableCmd.slice(1), { stdio: 'inherit' });
    publishers.push(wearablePublisher);

    const timeout = setTimeout(() => {
      console.log('Timeout reached, killing processes...');
      approach.kill();
      publishers.forEach(p => p.kill());
      killLingeringProcesses();
    }, TIMEOUT_MS);

    // Wait for publishers to finish
    await Promise.all(publishers.map(publisher => 
      new Promise((resolve) => {
        publisher.on('exit', () => resolve());
      })
    ));
    
    // Wait a bit more for approach to process final results
    await new Promise(res => setTimeout(res, 5000));
    approach.kill();
    
    await new Promise((resolve) => {
      approach.on('exit', () => resolve());
    });

    clearTimeout(timeout);
  } else {
    // Original testing with publish.js
    await new Promise(res => setTimeout(res, 2000));
    const publisher = spawn('node', ['dist/streamer/src/publish.js'], { stdio: 'inherit' });

    const timeout = setTimeout(() => {
      console.log('Timeout reached, killing processes...');
      approach.kill();
      publisher.kill();
      killLingeringProcesses();
    }, TIMEOUT_MS);

    await new Promise((resolve) => { publisher.on('exit', () => resolve()); });
    await new Promise((resolve) => { approach.on('exit', () => resolve()); });
    clearTimeout(timeout);
  }

  // Move/rename log files into a subfolder for this iteration
  const iterDir = frequency 
    ? path.join(LOGS_DIR, 'frequencies', frequency, `iteration${iter}`)
    : path.join(LOGS_DIR, `iteration${iter}`);
  
  if (!fs.existsSync(iterDir)) fs.mkdirSync(iterDir, { recursive: true });

  for (const file of LOG_FILES) {
    if (fs.existsSync(file)) {
      const newName = path.join(iterDir, file);
      fs.renameSync(file, newName);
    }
  }
  
  console.log(`Results saved to: ${iterDir}`);
}

async function runFrequencyTest(frequency, runs = 5) {
  console.log(`\n=== Testing Frequency: ${frequency} ===`);
  
  for (let i = 1; i <= runs; i++) {
    await runOnce(i, frequency);
    await new Promise(res => setTimeout(res, 2000));
  }
}

(async () => {
  if (runFrequencyTests || testFrequency) {
    const frequencies = ['4Hz', '8Hz', '16Hz', '32Hz', '64Hz', '128Hz'];
    
    if (testFrequency) {
      // Test specific frequency
      if (frequencies.includes(testFrequency)) {
        await runFrequencyTest(testFrequency, 10);
      } else {
        console.error(`Frequency not found: ${testFrequency}`);
        console.error('Available frequencies:', frequencies);
        process.exit(1);
      }
    } else {
      // Test all frequencies
      console.log('Starting streaming query hive frequency tests...');
      
      for (const freq of frequencies) {
        await runFrequencyTest(freq, 3);
      }
      
      console.log('\nAll frequency tests complete!');
      console.log(`Results saved in: ${LOGS_DIR}/frequencies`);
    }
  } else {
    // Original functionality - run standard test
    console.log('Running standard streaming query hive test...');
    for (let i = 1; i <= RUNS; i++) {
      await runOnce(i);
      await new Promise(res => setTimeout(res, 2000));
    }
    console.log('All runs complete.');
  }
})();
