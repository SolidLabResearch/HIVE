const { spawn } = require('child_process');
const fs = require('fs');

console.log('Testing 2-minute dataset behavior...');

// Test just one pattern to verify timing
const testPattern = 'challenging_exponential_growth';
const dataPath = 'approximation_test/challenging/exponential_growth';

console.log(`Testing pattern: ${testPattern}`);
console.log(`Data path: ${dataPath}`);

const startTime = Date.now();

// Set environment and start the test
const env = { ...process.env, DATA_PATH: dataPath };

const approach = spawn('node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js'], {
  env: env,
  stdio: 'pipe'
});

// Wait 2 seconds for approach to start, then start publisher
setTimeout(() => {
  const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
    env: env,
    stdio: 'pipe'
  });

  let publisherFinished = false;
  let approximationResults = [];
  
  // Monitor publisher output
  publisher.stdout.on('data', (data) => {
    const output = data.toString();
    if (output.includes('All observations published')) {
      const endTime = Date.now();
      const duration = (endTime - startTime) / 1000;
      console.log(`\nPublisher finished! Duration: ${duration.toFixed(1)} seconds`);
      console.log('This should be approximately 2 minutes (120 seconds) for 480 observations at 4Hz');
      publisherFinished = true;
    }
  });

  // Monitor approximation results  
  approach.stdout.on('data', (data) => {
    const output = data.toString();
    if (output.includes('Successfully published unified cross-sensor average')) {
      const match = output.match(/Successfully published unified cross-sensor average:\s*([\d.]+)/);
      if (match) {
        approximationResults.push({
          result: parseFloat(match[1]),
          timestamp: Date.now()
        });
        console.log(`Approximation result ${approximationResults.length}: ${match[1]}`);
      }
    }
  });

  // Clean up after publisher finishes
  publisher.on('exit', () => {
    setTimeout(() => {
      try { approach.kill('SIGTERM'); } catch(e) {}
      
      console.log(`\nSummary:`);
      console.log(`Total approximation results: ${approximationResults.length}`);
      console.log(`Expected: Consistent results since all patterns now have same duration`);
      console.log(`First result: ${approximationResults[0]?.result || 'N/A'}`);
      console.log(`Last result: ${approximationResults[approximationResults.length-1]?.result || 'N/A'}`);
      
      process.exit(0);
    }, 5000);
  });

}, 2000);

// Safety timeout
setTimeout(() => {
  console.log('\nTimeout reached, cleaning up...');
  try { approach.kill('SIGTERM'); } catch(e) {}
  process.exit(1);
}, 150000); // 2.5 minutes max
