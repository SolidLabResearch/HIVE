#!/usr/bin/env node

/**
 * Unified Comparison Script: Run all 3 approaches and compare results
 * Approaches: Approximation, Fetching Client Side, Chunked
 * Metrics: Window Close Latency, Accuracy (using Fetching as baseline)
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');

const APPROACHES = [
  {
    name: 'fetching',
    label: 'Fetching Client Side',
    script: 'experiment-rate-comparison-fetching.js',
    logDir: 'logs/rate-comparison-fetching',
    logFiles: {
      main: 'fetching_client_side_log.csv',
      resource: 'fetching_client_side_resource_usage.csv',
      replayer: 'replayer-log.csv'
    }
  },
  {
    name: 'approximation',
    label: 'Approximation',
    script: 'experiment-rate-comparison-approximation.js',
    logDir: 'logs/rate-comparison-approximation',
    logFiles: {
      main: 'approximation_approach_log.csv',
      resource: 'approximation_approach_resource_usage.csv',
      replayer: 'replayer-log.csv'
    }
  },
  {
    name: 'chunked',
    label: 'Chunked',
    script: 'experiment-rate-comparison-chunked.js',
    logDir: 'logs/rate-comparison-chunked',
    logFiles: {
      main: 'streaming_query_chunk_aggregator_log.csv',
      resource: 'streaming_query_hive_resource_log.csv',
      replayer: 'replayer-log.csv'
    }
  }
];

const RATES = [0.001, 0.01, 0.1, 1, 10, 100];
const PATTERNS = ['exponential_growth', 'exponential_decay'];
const COMPARISON_DIR = 'logs/rate_comparison_3way';

class ThreeWayComparisonRunner {
  constructor() {
    this.results = {};
    this.comparisonResults = [];

    if (!fs.existsSync(COMPARISON_DIR)) {
      fs.mkdirSync(COMPARISON_DIR, { recursive: true });
    }
  }

  async runApproachForRate(approach, pattern, rate) {
    return new Promise((resolve, reject) => {
      console.log(`\n${'='.repeat(80)}`);
      console.log(`Running ${approach.label} - ${pattern} rate ${rate}`);
      console.log('='.repeat(80));

      const scriptPath = path.join(__dirname, approach.script);
      const args = ['test', pattern, rate.toString()];

      const child = spawn('node', [scriptPath, ...args], {
        stdio: 'inherit',
        cwd: process.cwd()
      });

      child.on('close', (code) => {
        if (code === 0) {
          console.log(`✅ ${approach.label} completed successfully`);
          resolve({ success: true, approach: approach.name, pattern, rate });
        } else {
          console.log(`⚠️  ${approach.label} finished with code ${code}`);
          resolve({ success: false, approach: approach.name, pattern, rate, code });
        }
      });

      child.on('error', (error) => {
        console.error(`💥 Failed to run ${approach.label}:`, error.message);
        reject(error);
      });
    });
  }

  async runAllApproaches() {
    console.log('🚀 Starting 3-Way Approach Comparison');
    console.log(`Testing rates: ${RATES.join(', ')}`);
    console.log(`Testing patterns: ${PATTERNS.join(', ')}`);
    console.log(`Approaches: ${APPROACHES.map(a => a.label).join(', ')}\n`);

    const totalTests = APPROACHES.length * PATTERNS.length * RATES.length;
    let completedTests = 0;

    for (const rate of RATES) {
      for (const pattern of PATTERNS) {
        for (const approach of APPROACHES) {
          try {
            const result = await this.runApproachForRate(approach, pattern, rate);
            completedTests++;
            console.log(`\n📊 Progress: ${completedTests}/${totalTests} tests completed\n`);

            // Small delay between tests
            await new Promise(resolve => setTimeout(resolve, 2000));
          } catch (error) {
            console.error(`💥 Test failed: ${approach.label} ${pattern} rate ${rate}`, error);
            completedTests++;
          }
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
      const records = parse(content, { columns: true, skip_empty_lines: true });

      const windowCloseEvents = records.filter(r =>
        r.message && r.message.includes('Window closed')
      );

      if (windowCloseEvents.length === 0) {
        return null;
      }

      const latencies = windowCloseEvents.map(event => {
        const match = event.message.match(/latency:\s*([\d.]+)\s*ms/);
        return match ? parseFloat(match[1]) : null;
      }).filter(l => l !== null);

      if (latencies.length === 0) {
        return null;
      }

      return {
        count: latencies.length,
        avgLatency: latencies.reduce((a, b) => a + b, 0) / latencies.length,
        minLatency: Math.min(...latencies),
        maxLatency: Math.max(...latencies),
        latencies: latencies
      };
    } catch (error) {
      console.error(`Error parsing replayer log ${logPath}:`, error.message);
      return null;
    }
  }

  parseMainLog(logPath, approachName) {
    if (!fs.existsSync(logPath)) {
      return null;
    }

    try {
      const content = fs.readFileSync(logPath, 'utf8');
      const records = parse(content, { columns: true, skip_empty_lines: true });

      // Extract result values from the log
      const resultRecords = records.filter(r =>
        r.message && (
          r.message.includes('Result:') ||
          r.message.includes('Window result') ||
          r.message.includes('avgWearableX') ||
          r.message.includes('avgSmartphoneX')
        )
      );

      const values = [];
      resultRecords.forEach(record => {
        const valueMatch = record.message.match(/(?:avgWearableX|avgSmartphoneX|value).*?(\d+\.?\d*)/);
        if (valueMatch) {
          values.push(parseFloat(valueMatch[1]));
        }
      });

      return {
        recordCount: records.length,
        resultCount: values.length,
        values: values
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

      // Check if values match within tolerance
      const tolerance = 0.001;
      if (Math.abs(baseline - comparison) < tolerance) {
        matches++;
      }

      // Calculate errors
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

  analyzeResults() {
    console.log('\n🔍 Analyzing Results...\n');

    for (const rate of RATES) {
      for (const pattern of PATTERNS) {
        const comparisonData = {
          rate,
          pattern,
          approaches: {}
        };

        // Collect data from all approaches
        for (const approach of APPROACHES) {
          const logDir = path.join(approach.logDir, `${pattern}_rate_${rate}`, 'iteration1');
          const replayerLogPath = path.join(logDir, approach.logFiles.replayer);
          const mainLogPath = path.join(logDir, approach.logFiles.main);

          const replayerData = this.parseReplayerLog(replayerLogPath);
          const mainData = this.parseMainLog(mainLogPath, approach.name);

          comparisonData.approaches[approach.name] = {
            label: approach.label,
            latency: replayerData,
            results: mainData
          };
        }

        this.comparisonResults.push(comparisonData);
      }
    }
  }

  generateComparisonReport() {
    console.log('\n' + '='.repeat(100));
    console.log('3-WAY COMPARISON REPORT: WINDOW CLOSE LATENCY & ACCURACY');
    console.log('='.repeat(100));
    console.log('Baseline for Accuracy: Fetching Client Side Approach\n');

    const csvRows = [];
    csvRows.push('Rate,Pattern,Approach,Avg_Latency_ms,Min_Latency_ms,Max_Latency_ms,Window_Count,Accuracy_%,MAE,MAPE_%');

    for (const comparison of this.comparisonResults) {
      console.log(`\n${'─'.repeat(100)}`);
      console.log(`Rate: ${comparison.rate} | Pattern: ${comparison.pattern}`);
      console.log('─'.repeat(100));

      const fetchingResults = comparison.approaches.fetching?.results?.values || [];

      // Display header
      console.log('\n| Approach              | Avg Latency | Min Latency | Max Latency | Windows | Accuracy | MAE      | MAPE     |');
      console.log('|----------------------|-------------|-------------|-------------|---------|----------|----------|----------|');

      for (const approach of APPROACHES) {
        const data = comparison.approaches[approach.name];
        const latency = data?.latency;
        const results = data?.results;

        const avgLat = latency?.avgLatency ? latency.avgLatency.toFixed(2) : 'N/A';
        const minLat = latency?.minLatency ? latency.minLatency.toFixed(2) : 'N/A';
        const maxLat = latency?.maxLatency ? latency.maxLatency.toFixed(2) : 'N/A';
        const winCount = latency?.count || 'N/A';

        let accuracy = 'N/A';
        let mae = 'N/A';
        let mape = 'N/A';

        if (approach.name === 'fetching') {
          accuracy = '100.0% (baseline)';
          mae = '0.000000';
          mape = '0.00%';
        } else if (results?.values && fetchingResults.length > 0) {
          const acc = this.calculateAccuracy(fetchingResults, results.values);
          if (acc) {
            accuracy = `${acc.matchRate.toFixed(1)}%`;
            mae = acc.mae.toFixed(6);
            mape = `${acc.mape.toFixed(2)}%`;
          }
        }

        const label = approach.label.padEnd(20);
        const avgLatStr = (avgLat + ' ms').padEnd(11);
        const minLatStr = (minLat + ' ms').padEnd(11);
        const maxLatStr = (maxLat + ' ms').padEnd(11);
        const winStr = winCount.toString().padEnd(7);
        const accStr = accuracy.padEnd(8);
        const maeStr = mae.padEnd(8);
        const mapeStr = mape.padEnd(8);

        console.log(`| ${label} | ${avgLatStr} | ${minLatStr} | ${maxLatStr} | ${winStr} | ${accStr} | ${maeStr} | ${mapeStr} |`);

        // CSV row
        csvRows.push(
          `${comparison.rate},${comparison.pattern},${approach.label},` +
          `${avgLat},${minLat},${maxLat},${winCount},` +
          `${accuracy.replace('%', '').replace(' (baseline)', '')},${mae},${mape.replace('%', '')}`
        );
      }
    }

    console.log('\n' + '='.repeat(100));

    // Save CSV report
    const csvPath = path.join(COMPARISON_DIR, 'three_way_comparison_results.csv');
    fs.writeFileSync(csvPath, csvRows.join('\n'));
    console.log(`\n📊 Detailed CSV report saved to: ${csvPath}`);

    // Save JSON report
    const jsonPath = path.join(COMPARISON_DIR, 'three_way_comparison_results.json');
    fs.writeFileSync(jsonPath, JSON.stringify({
      timestamp: new Date().toISOString(),
      baseline: 'Fetching Client Side',
      approaches: APPROACHES.map(a => a.label),
      rates: RATES,
      patterns: PATTERNS,
      results: this.comparisonResults
    }, null, 2));
    console.log(`📊 JSON report saved to: ${jsonPath}`);
  }

  generateSummary() {
    console.log('\n' + '='.repeat(100));
    console.log('SUMMARY STATISTICS');
    console.log('='.repeat(100));

    const stats = {
      byApproach: {},
      byRate: {},
      byPattern: {}
    };

    // Initialize stats
    APPROACHES.forEach(a => {
      stats.byApproach[a.name] = { totalLatency: 0, count: 0, accuracies: [] };
    });
    RATES.forEach(r => {
      stats.byRate[r] = { totalLatency: 0, count: 0 };
    });
    PATTERNS.forEach(p => {
      stats.byPattern[p] = { totalLatency: 0, count: 0 };
    });

    // Collect stats
    for (const comparison of this.comparisonResults) {
      const fetchingResults = comparison.approaches.fetching?.results?.values || [];

      for (const approach of APPROACHES) {
        const data = comparison.approaches[approach.name];
        const latency = data?.latency;
        const results = data?.results;

        if (latency?.avgLatency) {
          stats.byApproach[approach.name].totalLatency += latency.avgLatency;
          stats.byApproach[approach.name].count++;
          stats.byRate[comparison.rate].totalLatency += latency.avgLatency;
          stats.byRate[comparison.rate].count++;
          stats.byPattern[comparison.pattern].totalLatency += latency.avgLatency;
          stats.byPattern[comparison.pattern].count++;

          if (approach.name !== 'fetching' && results?.values && fetchingResults.length > 0) {
            const acc = this.calculateAccuracy(fetchingResults, results.values);
            if (acc) {
              stats.byApproach[approach.name].accuracies.push(acc.matchRate);
            }
          }
        }
      }
    }

    // Display stats by approach
    console.log('\n📈 Average Metrics by Approach:');
    console.log('─'.repeat(80));
    for (const approach of APPROACHES) {
      const s = stats.byApproach[approach.name];
      const avgLat = s.count > 0 ? (s.totalLatency / s.count).toFixed(2) : 'N/A';
      const avgAcc = s.accuracies.length > 0
        ? (s.accuracies.reduce((a, b) => a + b, 0) / s.accuracies.length).toFixed(1)
        : (approach.name === 'fetching' ? '100.0' : 'N/A');

      console.log(`${approach.label.padEnd(25)}: Avg Latency = ${avgLat} ms, Avg Accuracy = ${avgAcc}%`);
    }

    // Display stats by rate
    console.log('\n📈 Average Latency by Rate:');
    console.log('─'.repeat(80));
    for (const rate of RATES) {
      const s = stats.byRate[rate];
      const avgLat = s.count > 0 ? (s.totalLatency / s.count).toFixed(2) : 'N/A';
      console.log(`Rate ${rate.toString().padEnd(6)}: ${avgLat} ms`);
    }

    // Display stats by pattern
    console.log('\n📈 Average Latency by Pattern:');
    console.log('─'.repeat(80));
    for (const pattern of PATTERNS) {
      const s = stats.byPattern[pattern];
      const avgLat = s.count > 0 ? (s.totalLatency / s.count).toFixed(2) : 'N/A';
      console.log(`${pattern.padEnd(20)}: ${avgLat} ms`);
    }

    console.log('\n' + '='.repeat(100));
  }

  async run() {
    const startTime = Date.now();

    // Run all experiments
    await this.runAllApproaches();

    // Analyze and generate reports
    this.analyzeResults();
    this.generateComparisonReport();
    this.generateSummary();

    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`\n⏱️  Total execution time: ${duration} minutes`);
    console.log('\n🎉 3-Way comparison complete!\n');
  }
}

// Command line interface
async function main() {
  const args = process.argv.slice(2);

  if (args.length > 0 && args[0] === 'analyze-only') {
    console.log('📊 Running analysis only (skipping experiments)...\n');
    const runner = new ThreeWayComparisonRunner();
    runner.analyzeResults();
    runner.generateComparisonReport();
    runner.generateSummary();
  } else {
    console.log('Usage:');
    console.log('  node run-all-3-approaches-comparison.js              # Run all experiments and analyze');
    console.log('  node run-all-3-approaches-comparison.js analyze-only # Only analyze existing results');
    console.log('');
    console.log('Starting full 3-way comparison...\n');

    const runner = new ThreeWayComparisonRunner();
    await runner.run();
  }
}

if (require.main === module) {
  main().catch(error => {
    console.error('💥 Comparison runner failed:', error);
    process.exit(1);
  });
}

module.exports = ThreeWayComparisonRunner;
