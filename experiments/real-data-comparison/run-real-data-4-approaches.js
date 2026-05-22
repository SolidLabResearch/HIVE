#!/usr/bin/env node

/**
 * Real Data 4-Way Comparison Experiment
 * Uses actual smartphone.acceleration.x and wearable.acceleration.x data
 * Compares: Fetching Client Side, Naive Distributed, Approximation, and Chunked approaches
 * Metrics: Window Close Latency, Accuracy (using Fetching as baseline)
 */

const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { createBenchmarkReplayRunEnv } = require('../utils/benchmarkReplayEnv');

const APPROACHES = [
  {
    name: 'fetching',
    label: 'Fetching Client Side',
    orchestrator: 'dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js',
    logFiles: {
      main: 'fetching_client_side_log.csv',
      resource: 'fetching_client_side_resource_usage.csv',
      latency: 'fetching_latency_log.csv',
      replayer: 'replayer-log.csv'
    }
  },
  {
    name: 'approximation',
    label: 'Approximation',
    orchestrator: 'dist/approaches/StreamingQueryApproximationApproachOrchestrator.js',
    logFiles: {
      main: 'approximation_approach_log.csv',
      resource: 'approximation_approach_resource_usage.csv',
      latency: 'approximation_latency_log.csv',
      replayer: 'replayer-log.csv'
    }
  },
  {
    name: 'chunked',
    label: 'Chunked',
    orchestrator: 'dist/approaches/StreamingQueryChunkedApproachOrchestrator.js',
    logFiles: {
      main: 'streaming_query_chunk_aggregator_log.csv',
      resource: 'streaming_query_hive_resource_log.csv',
      latency: 'chunked_latency_log.csv',
      replayer: 'replayer-log.csv'
    }
  },
  {
    name: 'naive_distributed',
    label: 'Naive Distributed',
    orchestrator: 'dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js',
    logFiles: {
      main: 'naive_distributed_approach_log.csv',
      resource: 'naive_distributed_approach_resource_usage.csv',
      latency: 'naive_distributed_latency_log.csv',
      replayer: 'replayer-log.csv'
    }
  }
];

const args = process.argv.slice(2);
let iterArg = 3; // Default
const iterIndex = args.findIndex(a => a === '--iterations' || a === '-i');
if (iterIndex !== -1 && args[iterIndex + 1]) {
  iterArg = parseInt(args[iterIndex + 1], 10);
}

const ITERATIONS = iterArg; // Number of times to run each approach for statistical significance
const TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes
const LOGS_DIR = 'experiments/real-data-comparison/logs';

class RealDataComparisonRunner {
  constructor() {
    this.results = [];
    this.replayEnv = createBenchmarkReplayRunEnv(process.env);

    if (!fs.existsSync(LOGS_DIR)) {
      fs.mkdirSync(LOGS_DIR, { recursive: true });
    }
  }

  cleanupStaleProcesses() {
    try {
      execSync('pkill -f "dist/approaches" 2>/dev/null || true', { stdio: 'ignore' });
    } catch (_) {}
    try {
      execSync('pkill -f "dist/services/BeeWorker" 2>/dev/null || true', { stdio: 'ignore' });
    } catch (_) {}
    try {
      execSync('lsof -ti:8080 | xargs kill -9 2>/dev/null || true', { stdio: 'ignore' });
    } catch (_) {}
    // Give OS time to release resources
    return new Promise(resolve => setTimeout(resolve, 1500));
  }

  async runSingleTest(approach, iteration) {
    await this.cleanupStaleProcesses();
    return new Promise((resolve, reject) => {
      const runLabel = `${approach.label} - Iteration ${iteration}`;
      console.log(`\n${'='.repeat(80)}`);
      console.log(`🧪 Running: ${runLabel}`);
      console.log('='.repeat(80));

      const logDir = path.join(LOGS_DIR, approach.name, `iteration${iteration}`);
      if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir, { recursive: true });
      }

      // Set environment to use the base smartphone/wearable data
      // The DATA_PATH should be empty or just the base directory name
      const env = this.replayEnv.withBenchmarkReplayEnv({
        ...process.env,
        DATA_PATH: '', // Uses default which points to the base data directories
        LOG_PATH: logDir
      });

      const startTime = Date.now();

      try {
        // Start the approach process
        console.log(`Starting ${approach.label} orchestrator...`);
        const orchestrator = spawn('node', [approach.orchestrator], {
          stdio: 'inherit',
          env: env
        });

        // Start publisher after a short delay
        setTimeout(() => {
          console.log('Starting data publisher...');
          const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
            stdio: 'inherit',
            env: env
          });

          // Set up timeout
          const timeout = setTimeout(() => {
            console.log('⏰ Timeout reached, killing processes...');
            orchestrator.kill();
            publisher.kill();
          }, TIMEOUT_MS);

          // Wait for publisher to finish
          publisher.on('close', (code) => {
            clearTimeout(timeout);
            orchestrator.kill();

            const endTime = Date.now();
            const duration = (endTime - startTime) / 1000;

            console.log(`\n${code === 0 ? '✅' : '⚠️'} ${runLabel} completed in ${duration.toFixed(1)}s`);

            // Copy log files
            this.copyLogFiles(approach, logDir);

            resolve({
              approach: approach.name,
              iteration,
              duration,
              success: code === 0,
              logDir
            });
          });

          publisher.on('error', (err) => {
            clearTimeout(timeout);
            orchestrator.kill();
            reject(err);
          });
        }, 2000);

        orchestrator.on('error', (err) => {
          console.error(`💥 Failed to start ${approach.label}:`, err);
          reject(err);
        });

      } catch (error) {
        const endTime = Date.now();
        const duration = (endTime - startTime) / 1000;

        console.error(`💥 ${runLabel} failed:`, error.message);

        resolve({
          approach: approach.name,
          iteration,
          duration,
          success: false,
          error: error.message,
          logDir
        });
      }
    });
  }

  copyLogFiles(approach, logDir) {
    for (const logFile of Object.values(approach.logFiles)) {
      const srcPath = path.join('.', logFile);
      const destPath = path.join(logDir, logFile);

      if (fs.existsSync(srcPath)) {
        fs.copyFileSync(srcPath, destPath);
        fs.unlinkSync(srcPath); // Clean up
        console.log(`📄 Copied ${logFile} to ${logDir}`);
      }
    }
  }

  async runAllTests() {
    console.log('🚀 Starting Real Data 4-Way Comparison');
    console.log(`Approaches: ${APPROACHES.map(a => a.label).join(', ')}`);
    console.log(`Iterations per approach: ${ITERATIONS}`);
    console.log(`Data: smartphone.acceleration.x & wearable.acceleration.x\n`);

    const totalTests = APPROACHES.length * ITERATIONS;
    let completedTests = 0;

    for (const approach of APPROACHES) {
      for (let iteration = 1; iteration <= ITERATIONS; iteration++) {
        try {
          const result = await this.runSingleTest(approach, iteration);
          this.results.push(result);
          completedTests++;

          console.log(`\n📊 Progress: ${completedTests}/${totalTests} tests completed\n`);

          // Small delay between tests
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (error) {
          console.error(`💥 Test failed: ${approach.label} iteration ${iteration}`, error);
          this.results.push({
            approach: approach.name,
            iteration,
            success: false,
            error: error.message
          });
          completedTests++;
        }
      }
    }

    console.log('\n✅ All experiments completed!\n');
  }

  parseReplayerLog(logPath) {
    if (!fs.existsSync(logPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(logPath, 'utf8');
      const lines = content.trim().split('\n');

      if (lines.length < 2) {
        return null;
      }

      // Parse CSV manually or use the last line with summary
      const records = parse(content, { columns: true, skip_empty_lines: true });

      // Look for summary line with intended/successful/failed
      const summaryRecord = records.find(r => r.intended && r.successful && r.failed);

      if (summaryRecord) {
        return {
          intended: parseInt(summaryRecord.intended),
          successful: parseInt(summaryRecord.successful),
          failed: parseInt(summaryRecord.failed),
          successRate: (parseInt(summaryRecord.successful) / parseInt(summaryRecord.intended)) * 100
        };
      }

      return null;
    } catch (error) {
      console.error(`Error parsing replayer log ${logPath}:`, error.message);
      return null;
    }
  }

  parseLatencyLog(logPath) {
    if (!fs.existsSync(logPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(logPath, 'utf8');
      const records = parse(content, { columns: true, skip_empty_lines: true });

      const results = records.map(record => {
        // Handle different column names across approaches
        const latency = parseInt(
          record.latency_from_last_obs_ms || 
          record.latency_from_last_data_ms || 
          record.computation_ms || 
          "0"
        );

        return {
          timestamp: parseInt(record.result_emitted_at),
          latency: latency,
          value: parseFloat(record.result_value)
        };
      });

      return results;
    } catch (error) {
      console.error(`Error parsing latency log ${logPath}:`, error.message);
      return null;
    }
  }

  parseMainLog(logPath) {
    if (!fs.existsSync(logPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(logPath, 'utf8');
      const records = parse(content, { 
        columns: true, 
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true
      });

      // Extract window close events and result values
      const windowCloseEvents = [];
      const resultValues = [];

      records.forEach(record => {
        // Debug: log first 5 records
        if (records.indexOf(record) < 5) {
          // console.log(`DEBUG Record: ${JSON.stringify(record)}`);
        }

        // Look for window close latency in message
        if (record.message && record.message.includes('Window closed')) {
          const latencyMatch = record.message.match(/latency:\s*([\d.]+)\s*ms/);
          if (latencyMatch) {
            windowCloseEvents.push({
              timestamp: parseInt(record.timestamp),
              latency: parseFloat(latencyMatch[1])
            });
          }
        }

        // Extract result values
        let val = null;
        if (record.message) {
          // Fetching approach pattern: "RStream result generated: -10.767... (count: 478) at timestamp: ..."
          if (record.message.includes('RStream result generated:')) {
            const fetchingMatch = record.message.match(/RStream result generated:\s*([-0-9.]+)/);
            if (fetchingMatch) {
              val = parseFloat(fetchingMatch[1]);
              // console.log(`DEBUG matched fetching: ${val}`);
            }
          }

          // Approximation approach pattern: "Successfully published unified cross-sensor avg: -10.760... (from 2 topics)"
          if (val === null && record.message.includes('unified cross-sensor')) {
            const approxMatch = record.message.match(/unified cross-sensor (?:avg|max):\s*([-0-9.]+)/);
            if (approxMatch) {
              val = parseFloat(approxMatch[1]);
              // console.log(`DEBUG matched approx: ${val}`);
            }
          }

          // Chunked approach pattern: "calculated result <...> <...> "-10.767..."^^<...>"
          if (val === null && record.message.includes('calculated result')) {
            const chunkedMatch = record.message.match(/calculated result.*hasValue>\s*\\?"?([-0-9.]+)\\?"?/);
            if (chunkedMatch) {
              val = parseFloat(chunkedMatch[1]);
              // console.log(`DEBUG matched chunked: ${val}`);
            }
          }

          // Naive Distributed approach pattern: "Successfully published unified cross-sensor avgValue: -10.767..."
          if (val === null && record.message.includes('unified cross-sensor avgValue')) {
            const naiveMatch = record.message.match(/unified cross-sensor avgValue:\s*([-0-9.]+)/);
            if (naiveMatch) {
              val = parseFloat(naiveMatch[1]);
            }
          }

          // Legacy patterns
          if (val === null && (
            record.message.includes('avgWearableX') ||
            record.message.includes('avgSmartphoneX') ||
            record.message.includes('Result:')
          )) {
            const valueMatch = record.message.match(/([-0-9.]+\.?\d*)/);
            if (valueMatch) val = parseFloat(valueMatch[1]);
          }
        }

        if (val !== null && !isNaN(val)) {
          console.log(`FOUND RESULT: ${val} at ${record.timestamp}`);
          resultValues.push({
            timestamp: parseInt(record.timestamp),
            value: val
          });
        }
      });

      const latencies = windowCloseEvents.map(e => e.latency);

      return {
        recordCount: records.length,
        windowCloseCount: windowCloseEvents.length,
        avgLatency: latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : null,
        minLatency: latencies.length > 0 ? Math.min(...latencies) : null,
        maxLatency: latencies.length > 0 ? Math.max(...latencies) : null,
        latencies: latencies,
        resultValues: resultValues.map(r => r.value),
        resultCount: resultValues.length
      };
    } catch (error) {
      console.error(`Error parsing main log ${logPath}:`, error.message);
      return null;
    }
  }

  calculateAccuracy(baselineValues, comparisonValues) {
    if (!baselineValues || !comparisonValues ||
        baselineValues.length === 0 || comparisonValues.length === 0) {
      return null;
    }

    const minLength = Math.min(baselineValues.length, comparisonValues.length);
    let matches = 0;
    let totalAbsError = 0;
    let totalRelError = 0;

    for (let i = 0; i < minLength; i++) {
      const baseline = baselineValues[i];
      const comparison = comparisonValues[i];

      const tolerance = 0.001;
      if (Math.abs(baseline - comparison) < tolerance) {
        matches++;
      }

      totalAbsError += Math.abs(baseline - comparison);
      if (baseline !== 0) {
        totalRelError += Math.abs((baseline - comparison) / baseline);
      }
    }

    return {
      matchRate: (matches / minLength) * 100,
      mae: totalAbsError / minLength,
      mape: (totalRelError / minLength) * 100,
      comparedCount: minLength
    };
  }

  parseLatencyLog(logPath) {
    if (!fs.existsSync(logPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(logPath, 'utf8');
      const records = parse(content, { columns: true, skip_empty_lines: true });

      const results = records.map(record => {
        // Try different latency column names
        const latency = parseInt(record.latency_from_last_obs_ms || 
                                 record.latency_from_last_data_ms || 
                                 record.computation_ms || 
                                 "0");
        
        return {
          timestamp: parseInt(record.result_emitted_at),
          latency: latency,
          value: parseFloat(record.result_value)
        };
      });

      return results;
    } catch (error) {
      console.error(`Error parsing latency log ${logPath}:`, error.message);
      return null;
    }
  }

  analyzeResults() {
    console.log('\n🔍 Analyzing Results...\n');

    const analysis = {
      byApproach: {}
    };

    // Collect metrics for each approach
    for (const approach of APPROACHES) {
      // Include timed-out runs too — log files are written even when the publisher is killed
      const approachResults = this.results.filter(r => r.approach === approach.name && r.logDir);

      if (approachResults.length === 0) {
        console.log(`⚠️  No successful results for ${approach.label}`);
        continue;
      }

      const latencies = [];
      const allResultValues = [];

      approachResults.forEach(result => {
        const mainLogPath = path.join(result.logDir, approach.logFiles.main);
        const latencyLogPath = path.join(result.logDir, approach.logFiles.latency);
        
        // Prefer latency log for more accurate data
        const latencyData = this.parseLatencyLog(latencyLogPath);
        if (latencyData && latencyData.length > 0) {
          latencyData.forEach(entry => {
            latencies.push(entry.latency);
          });
          allResultValues.push(latencyData.map(e => e.value));
        } else {
          // Fallback to main log parsing
          const mainData = this.parseMainLog(mainLogPath);
          if (mainData) {
            if (mainData.latencies.length > 0) {
              latencies.push(...mainData.latencies);
            }
            if (mainData.resultValues.length > 0) {
              allResultValues.push(mainData.resultValues);
            }
          }
        }
      });

      analysis.byApproach[approach.name] = {
        label: approach.label,
        iterations: approachResults.length,
        avgLatency: latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : null,
        minLatency: latencies.length > 0 ? Math.min(...latencies) : null,
        maxLatency: latencies.length > 0 ? Math.max(...latencies) : null,
        windowCloseCount: latencies.length,
        resultValues: allResultValues.length > 0 ? allResultValues[0] : [] // Use first iteration for comparison
      };
    }

    return analysis;
  }

  generateReport(analysis) {
    console.log('\n' + '='.repeat(100));
    console.log('REAL DATA 4-WAY COMPARISON REPORT');
    console.log('='.repeat(100));
    console.log('Data Source: smartphone.acceleration.x & wearable.acceleration.x');
    console.log('Baseline for Accuracy: Fetching Client Side Approach\n');

    const fetchingResults = analysis.byApproach.fetching?.resultValues || [];

    console.log('| Approach              | Iterations | Avg Latency | Min Latency | Max Latency | Windows | Accuracy | MAE      | MAPE     |');
    console.log('|----------------------|------------|-------------|-------------|-------------|---------|----------|----------|----------|');

    const csvRows = [];
    csvRows.push('Approach,Iterations,Avg_Latency_ms,Min_Latency_ms,Max_Latency_ms,Window_Count,Accuracy_%,MAE,MAPE_%');

    for (const approach of APPROACHES) {
      const data = analysis.byApproach[approach.name];

      if (!data) {
        console.log(`| ${approach.label.padEnd(20)} | No Data    |             |             |             |         |          |          |          |`);
        continue;
      }

      const avgLat = data.avgLatency ? data.avgLatency.toFixed(2) : 'N/A';
      const minLat = data.minLatency ? data.minLatency.toFixed(2) : 'N/A';
      const maxLat = data.maxLatency ? data.maxLatency.toFixed(2) : 'N/A';
      const winCount = data.windowCloseCount || 'N/A';
      const iterations = data.iterations || 0;

      let accuracy = 'N/A';
      let mae = 'N/A';
      let mape = 'N/A';

      if (approach.name === 'fetching') {
        accuracy = '100.0% (baseline)';
        mae = '0.000000';
        mape = '0.00%';
      } else if (data.resultValues && fetchingResults.length > 0) {
        const acc = this.calculateAccuracy(fetchingResults, data.resultValues);
        if (acc) {
          accuracy = `${acc.matchRate.toFixed(1)}%`;
          mae = acc.mae.toFixed(6);
          mape = `${acc.mape.toFixed(2)}%`;
        }
      }

      const label = approach.label.padEnd(20);
      const iterStr = iterations.toString().padEnd(10);
      const avgLatStr = (avgLat + ' ms').padEnd(11);
      const minLatStr = (minLat + ' ms').padEnd(11);
      const maxLatStr = (maxLat + ' ms').padEnd(11);
      const winStr = winCount.toString().padEnd(7);
      const accStr = accuracy.padEnd(8);
      const maeStr = mae.padEnd(8);
      const mapeStr = mape.padEnd(8);

      console.log(`| ${label} | ${iterStr} | ${avgLatStr} | ${minLatStr} | ${maxLatStr} | ${winStr} | ${accStr} | ${maeStr} | ${mapeStr} |`);

      csvRows.push(
        `${approach.label},${iterations},${avgLat},${minLat},${maxLat},${winCount},` +
        `${accuracy.replace('%', '').replace(' (baseline)', '')},${mae},${mape.replace('%', '')}`
      );
    }

    console.log('\n' + '='.repeat(100));

    // Save CSV
    const csvPath = path.join(LOGS_DIR, 'real_data_comparison_results.csv');
    fs.writeFileSync(csvPath, csvRows.join('\n'));
    console.log(`\n📊 CSV report saved to: ${csvPath}`);

    // Save JSON
    const jsonPath = path.join(LOGS_DIR, 'real_data_comparison_results.json');
    fs.writeFileSync(jsonPath, JSON.stringify({
      timestamp: new Date().toISOString(),
      dataSource: 'smartphone.acceleration.x & wearable.acceleration.x',
      baseline: 'Fetching Client Side',
      approaches: APPROACHES.map(a => a.label),
      iterations: ITERATIONS,
      analysis: analysis,
      rawResults: this.results
    }, null, 2));
    console.log(`📊 JSON report saved to: ${jsonPath}`);
  }

  generateSummary(analysis) {
    console.log('\n' + '='.repeat(100));
    console.log('SUMMARY STATISTICS');
    console.log('='.repeat(100));

    console.log('\n📈 Performance Comparison:');
    console.log('─'.repeat(80));

    const fetchingData = analysis.byApproach.fetching;

    for (const approach of APPROACHES) {
      const data = analysis.byApproach[approach.name];

      if (!data) continue;

      const avgLat = data.avgLatency ? data.avgLatency.toFixed(2) : 'N/A';

      let latencyComparison = '';
      if (fetchingData && data.avgLatency && fetchingData.avgLatency) {
        const diff = data.avgLatency - fetchingData.avgLatency;
        const pct = ((diff / fetchingData.avgLatency) * 100).toFixed(1);
        latencyComparison = diff > 0
          ? `(+${pct}% vs baseline)`
          : `(${pct}% vs baseline)`;
      }

      console.log(`${approach.label.padEnd(25)}: Avg Latency = ${avgLat} ms ${latencyComparison}`);
    }

    console.log('\n📊 Key Findings:');
    console.log('─'.repeat(80));

    // Find fastest approach
    const latencies = APPROACHES
      .map(a => ({ name: a.label, latency: analysis.byApproach[a.name]?.avgLatency }))
      .filter(a => a.latency !== null);

    if (latencies.length > 0) {
      const fastest = latencies.reduce((min, curr) =>
        curr.latency < min.latency ? curr : min
      );
      console.log(`🏆 Fastest approach: ${fastest.name} (${fastest.latency.toFixed(2)} ms avg)`);
    }

    console.log('\n' + '='.repeat(100));
  }

  async run() {
    const startTime = Date.now();

    console.log('📋 Prerequisites Check:');

    console.log('  ✓ Ensure MQTT broker is running (mosquitto)');
    console.log('  ✓ Data files exist in src/streamer/data/');
    console.log('  ✓ Project is built (npm run build)\n');

    await this.runAllTests();
    const analysis = this.analyzeResults();
    this.generateReport(analysis);
    this.generateSummary(analysis);

    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`\n⏱️  Total execution time: ${duration} minutes`);
    console.log('\n🎉 Real data comparison complete!\n');
  }
}

// Command line interface
async function main() {
  const args = process.argv.slice(2);

  if (args.length > 0 && args[0] === 'analyze-only') {
    console.log('📊 Running analysis only (skipping experiments)...\n');
    const runner = new RealDataComparisonRunner();
    
    // Discover existing results from the file system
    for (const approach of APPROACHES) {
      const approachDir = path.join(LOGS_DIR, approach.name);
      if (fs.existsSync(approachDir)) {
        const iterations = fs.readdirSync(approachDir).filter(d => d.startsWith('iteration'));
        for (const iter of iterations) {
          const iterNum = parseInt(iter.replace('iteration', ''));
          const logDir = path.join(approachDir, iter);
          
          // Check if at least the main log exists
          if (fs.existsSync(path.join(logDir, approach.logFiles.main))) {
            runner.results.push({
              approach: approach.name,
              iteration: iterNum,
              success: true, // Assume success if logs exist
              logDir: logDir
            });
          }
        }
      }
    }
    
    console.log(`Discovered ${runner.results.length} iterations to analyze.`);
    
    // Load existing results from logs
    const analysis = runner.analyzeResults();
    runner.generateReport(analysis);
    runner.generateSummary(analysis);
  } else {
    const runner = new RealDataComparisonRunner();
    await runner.run();
  }
}

if (require.main === module) {
  main().catch(error => {
    console.error('💥 Comparison runner failed:', error);
    process.exit(1);
  });
}

module.exports = RealDataComparisonRunner;
