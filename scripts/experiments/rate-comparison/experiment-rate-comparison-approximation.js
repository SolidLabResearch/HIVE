#!/usr/bin/env node

/**
 * Experiment evaluation script specifically for exponential rate comparison
 * Tests only exponential growth/decay patterns with different rates using approximation approach
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const LOGS_DIR = 'logs/rate-comparison-approximation';
const DATA_BASE_PATH = 'src/streamer/data/rate_comparison';
const APPROACH_CMD = ['node', ['dist/approaches/StreamingQueryApproximationApproachOrchestrator.js']];
const PUBLISH_CMD = ['node', ['dist/streamer/src/publish.js']];
const LOG_FILES = [
  'approximation_approach_log.csv',
  'approximation_approach_resource_usage.csv',
  'replayer-log.csv'
];
const TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes

// Rate comparison test configuration
const RATE_TEST_CONFIG = {
  rates: [0.001, 0.01, 0.1, 1, 10, 100],
  patterns: ['exponential_growth', 'exponential_decay'],
  iterations: 1
};

if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR, { recursive: true });
}

async function runSingleTest(rate, pattern, iteration = 1) {
  const datasetName = `${pattern}_rate_${rate}`;
  const dataPath = path.join(DATA_BASE_PATH, datasetName);
  const runLabel = `${pattern} rate ${rate} - iteration ${iteration}`;
  
  console.log(`\n Testing Approximation: ${runLabel}`);
  console.log(` Data path: ${dataPath}`);
  
  // Check if data exists
  const smartphoneDataPath = path.join(dataPath, 'smartphone.acceleration.x', 'data.nt');
  if (!fs.existsSync(smartphoneDataPath)) {
    throw new Error(`Data file not found: ${smartphoneDataPath}`);
  }

  const logDir = path.join(LOGS_DIR, `${pattern}_rate_${rate}`, `iteration${iteration}`);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }

  // Set environment variables - use relative path from src/streamer/data/
  const env = { 
    ...process.env, 
    DATA_PATH: `rate_comparison/${datasetName}`,
    LOG_PATH: logDir
  };

  const startTime = Date.now();

  try {
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

    // Set up a timeout to kill both processes
    const timeout = setTimeout(() => {
      console.log(' Timeout reached, killing processes...');
      approach.kill();
      publisher.kill();
    }, TIMEOUT_MS);

    // Wait for publisher to finish or timeout
    await new Promise((resolve, reject) => {
      publisher.on('close', (code) => {
        clearTimeout(timeout);
        approach.kill();
        
        if (code === 0) {
          console.log(`${runLabel} completed successfully`);
        } else {
          console.log(`${runLabel} finished with exit code ${code}`);
        }
        
        resolve(code);
      });

      publisher.on('error', (err) => {
        clearTimeout(timeout);
        approach.kill();
        reject(err);
      });
    });

    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;

    // Copy log files to organized location
    for (const logFile of LOG_FILES) {
      const srcPath = path.join('.', logFile);
      const destPath = path.join(logDir, logFile);
      
      if (fs.existsSync(srcPath)) {
        fs.copyFileSync(srcPath, destPath);
        fs.unlinkSync(srcPath); // Clean up
      }
    }

    console.log(`  ${runLabel} completed in ${duration.toFixed(1)}s`);
    
    return {
      rate,
      pattern,
      iteration,
      duration,
      success: true,
      logPath: logDir
    };

  } catch (error) {
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(` ${runLabel} failed: ${error.message}`);
    
    return {
      rate,
      pattern,
      iteration,
      duration,
      success: false,
      error: error.message,
      logPath: logDir
    };
  }
}

async function runAllRateTests() {
  console.log('Starting Exponential Rate Comparison - Approximation Approach');
  console.log(`Testing rates: ${RATE_TEST_CONFIG.rates.join(', ')}`);
  console.log(`Testing patterns: ${RATE_TEST_CONFIG.patterns.join(', ')}`);
  console.log(` Results will be logged to: ${LOGS_DIR}`);

  const results = [];
  const totalTests = RATE_TEST_CONFIG.rates.length * RATE_TEST_CONFIG.patterns.length * RATE_TEST_CONFIG.iterations;
  let completedTests = 0;

  for (const rate of RATE_TEST_CONFIG.rates) {
    for (const pattern of RATE_TEST_CONFIG.patterns) {
      for (let iteration = 1; iteration <= RATE_TEST_CONFIG.iterations; iteration++) {
        try {
          const result = await runSingleTest(rate, pattern, iteration);
          results.push(result);
          completedTests++;
          console.log(`Progress: ${completedTests}/${totalTests} tests completed`);
          
          // Small delay between tests
          await new Promise(resolve => setTimeout(resolve, 2000));
          
        } catch (error) {
          console.error(` Test failed: ${pattern} rate ${rate} iteration ${iteration}`, error.message);
          results.push({
            rate,
            pattern,
            iteration,
            success: false,
            error: error.message
          });
          completedTests++;
        }
      }
    }
  }

  generateSummaryReport(results);
  return results;
}

async function runSpecificRate(rate) {
  if (!RATE_TEST_CONFIG.rates.includes(rate)) {
    throw new Error(`Invalid rate: ${rate}. Valid rates: ${RATE_TEST_CONFIG.rates.join(', ')}`);
  }

  console.log(`Testing Approximation Approach for rate: ${rate}`);
  
  const results = [];
  for (const pattern of RATE_TEST_CONFIG.patterns) {
    for (let iteration = 1; iteration <= RATE_TEST_CONFIG.iterations; iteration++) {
      try {
        const result = await runSingleTest(rate, pattern, iteration);
        results.push(result);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        console.error(` Test failed: ${pattern} rate ${rate} iteration ${iteration}`, error.message);
        results.push({
          rate,
          pattern,
          iteration,
          success: false,
          error: error.message
        });
      }
    }
  }

  generateSummaryReport(results);
  return results;
}

async function runSpecificTest(pattern, rate) {
  if (!RATE_TEST_CONFIG.patterns.includes(pattern)) {
    throw new Error(`Invalid pattern: ${pattern}. Valid patterns: ${RATE_TEST_CONFIG.patterns.join(', ')}`);
  }

  console.log(`Testing Approximation Approach: ${pattern} with rate ${rate}`);
  
  try {
    const result = await runSingleTest(rate, pattern, 1);
    generateSummaryReport([result]);
    return result;
  } catch (error) {
    console.error(` Test failed: ${pattern} rate ${rate}`, error.message);
    const result = {
      rate,
      pattern,
      iteration: 1,
      success: false,
      error: error.message
    };
    generateSummaryReport([result]);
    return result;
  }
}

function generateSummaryReport(results) {
  console.log('\n' + '='.repeat(80));
  console.log('APPROXIMATION APPROACH - RATE COMPARISON SUMMARY');
  console.log('='.repeat(80));

  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);

  console.log(`Total Tests: ${results.length}`);
  console.log(`Successful: ${successful.length}`);
  console.log(`Failed: ${failed.length}`);

  if (successful.length > 0) {
    console.log('\nSuccessful Tests:');
    console.log('Rate    | Pattern           | Duration');
    console.log('--------|-------------------|----------');
    
    successful.forEach(result => {
      console.log(
        `${result.rate.toString().padEnd(7)} | ` +
        `${result.pattern.padEnd(17)} | ` +
        `${result.duration.toFixed(1)}s`
      );
    });
  }

  if (failed.length > 0) {
    console.log('\nFailed Tests:');
    failed.forEach(result => {
      console.log(`  ${result.pattern} rate ${result.rate}: ${result.error}`);
    });
  }

  // Save detailed results
  const reportPath = path.join(LOGS_DIR, 'approximation_rate_comparison_summary.json');
  fs.writeFileSync(reportPath, JSON.stringify({
    config: RATE_TEST_CONFIG,
    results: results,
    summary: {
      total: results.length,
      successful: successful.length,
      failed: failed.length,
      timestamp: new Date().toISOString()
    }
  }, null, 2));

  console.log(`\nDetailed report saved to: ${reportPath}`);
  console.log('='.repeat(80));
}

// Command line interface
async function main() {
  const args = process.argv.slice(2);
  
  try {
    if (args.length === 0) {
      // Run all rate tests
      await runAllRateTests();
    } else if (args[0] === 'rate' && args[1]) {
      // Run tests for specific rate
      const rate = parseFloat(args[1]);
      await runSpecificRate(rate);
    } else if (args[0] === 'test' && args[1] && args[2]) {
      // Run specific pattern and rate
      const pattern = args[1];
      const rate = parseFloat(args[2]);
      await runSpecificTest(pattern, rate);
    } else {
      console.log('Usage:');
      console.log('  node experiment-rate-comparison-approximation.js                           # Run all rate tests');
      console.log('  node experiment-rate-comparison-approximation.js rate 0.1                 # Run tests for specific rate');
      console.log('  node experiment-rate-comparison-approximation.js test exponential_growth 1 # Run specific test');
      process.exit(1);
    }
  } catch (error) {
    console.error(' Experiment failed:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}
