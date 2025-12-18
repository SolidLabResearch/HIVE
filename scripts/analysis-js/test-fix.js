const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

// Test just one pattern to validate the fix
const testPattern = 'challenging_exponential_growth';
const TIMEOUT_MS = 150 * 1000; // 150 seconds timeout

async function testSinglePattern() {
  console.log(`Testing pattern: ${testPattern}`);
  console.log('======================================');
  
  // Clean up any existing logs
  const logDir = `./logs/approximation-patterns/${testPattern}/iteration1`;
  if (fs.existsSync(`${logDir}/approximation_approach_log.csv`)) {
    fs.unlinkSync(`${logDir}/approximation_approach_log.csv`);
    console.log('Cleaned up existing approximation log');
  }
  if (fs.existsSync(`${logDir}/replayer-log.csv`)) {
    fs.unlinkSync(`${logDir}/replayer-log.csv`);
    console.log('Cleaned up existing replayer log');
  }

  // Start approximation approach
  console.log('Starting approximation approach...');
  const approachProcess = spawn('node', ['src/approaches/StreamingQueryApproximationApproachOrchestrator.ts'], {
    env: { ...process.env, TS_NODE_PROJECT: './tsconfig.json' },
    stdio: 'pipe'
  });

  // Start data publisher after delay
  await new Promise(res => setTimeout(res, 2000));
  console.log('Starting data publisher...');
  
  const publisherProcess = spawn('python3', [
    'src/streamer/data/approximation_test/start_streaming.py',
    `src/streamer/data/approximation_test/${testPattern.startsWith('challenging_') ? 'challenging' : 'favorable'}/${testPattern.replace('challenging_', '').replace('favorable_', '')}`
  ], { stdio: 'pipe' });

  // Set up timeout
  let timeoutReached = false;
  const timeout = setTimeout(() => {
    console.log(`Timeout reached (${TIMEOUT_MS/1000}s), killing processes...`);
    timeoutReached = true;
    approachProcess.kill('SIGTERM');
    publisherProcess.kill('SIGTERM');
  }, TIMEOUT_MS);

  // Wait for completion
  await Promise.race([
    new Promise(resolve => {
      approachProcess.on('exit', () => {
        console.log('Approximation approach completed');
        resolve();
      });
    }),
    new Promise(resolve => {
      publisherProcess.on('exit', () => {
        console.log('Publisher completed');
        resolve();
      });
    }),
    new Promise(resolve => setTimeout(resolve, TIMEOUT_MS))
  ]);

  clearTimeout(timeout);
  
  // Force cleanup
  try {
    approachProcess.kill('SIGTERM');
    publisherProcess.kill('SIGTERM');
  } catch (e) {}

  await new Promise(res => setTimeout(res, 2000));

  // Analyze results
  console.log('\\nAnalyzing results...');
  if (fs.existsSync(`${logDir}/approximation_approach_log.csv`)) {
    const logContent = fs.readFileSync(`${logDir}/approximation_approach_log.csv`, 'utf8');
    const successfulResults = logContent.split('\\n').filter(line => 
      line.includes('Successfully published unified cross-sensor average')
    ).length;
    
    console.log(`Results generated: ${successfulResults}`);
    console.log(`Expected results: 2`);
    
    if (successfulResults <= 3) {
      console.log('SUCCESS: Result count is reasonable (≤3)');
    } else {
      console.log('ISSUE: Still generating too many results');
    }
    
    // Extract timing info
    const timestamps = [];
    logContent.split('\\n').forEach(line => {
      if (line.includes('Successfully published unified cross-sensor average')) {
        const timestamp = parseInt(line.split(',')[0]);
        if (!isNaN(timestamp)) timestamps.push(timestamp);
      }
    });
    
    if (timestamps.length > 0) {
      const totalDuration = timestamps[timestamps.length - 1] - timestamps[0];
      console.log(`Total duration: ${totalDuration}ms (${(totalDuration/1000).toFixed(1)}s)`);
    }
  } else {
    console.log('No log file found');
  }
}

testSinglePattern().catch(console.error);
