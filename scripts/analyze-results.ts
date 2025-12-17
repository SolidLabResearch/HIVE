import * as fs from 'fs';
import * as path from 'path';

interface ResultRow {
  queryRegisteredTimestamp: number;
  resultTimestamp: number;
  result: number;
}

interface WindowData {
  queryRegisteredTimestamp: number;
  results: Array<{ timestamp: number; value: number }>;
}

interface ApproachStats {
  approach: string;
  runNumber: number;
  lastWindowTimestamp: number;
  lastWindowLatency: number;
  lastWindowResult: number;
  groundTruthResult: number;
  absoluteError: number;
  relativeError: number;
}

function parseCSV(filePath: string): ResultRow[] {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.trim().split('\n');

  // Skip header
  const rows: ResultRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    const [queryRegisteredTimestamp, resultTimestamp, result] = lines[i].split(',');
    rows.push({
      queryRegisteredTimestamp: parseInt(queryRegisteredTimestamp),
      resultTimestamp: parseInt(resultTimestamp),
      result: parseFloat(result)
    });
  }

  return rows;
}

function groupByWindow(rows: ResultRow[]): WindowData[] {
  const windows = new Map<number, WindowData>();

  for (const row of rows) {
    if (!windows.has(row.queryRegisteredTimestamp)) {
      windows.set(row.queryRegisteredTimestamp, {
        queryRegisteredTimestamp: row.queryRegisteredTimestamp,
        results: []
      });
    }
    windows.get(row.queryRegisteredTimestamp)!.results.push({
      timestamp: row.resultTimestamp,
      value: row.result
    });
  }

  return Array.from(windows.values()).sort((a, b) =>
    a.queryRegisteredTimestamp - b.queryRegisteredTimestamp
  );
}

function getLastResult(window: WindowData): { timestamp: number; value: number } {
  // Sort by timestamp and get the last one (most recent)
  const sorted = window.results.sort((a, b) => a.timestamp - b.timestamp);
  return sorted[sorted.length - 1];
}

function analyzeResults() {
  const resultsDir = path.join(__dirname, '..', 'results');

  const approximationPath = path.join(resultsDir, 'approximation_results.csv');
  const chunkedPath = path.join(resultsDir, 'chunked_query_results.csv');
  const fetchingPath = path.join(resultsDir, 'fetching_client_side_results.csv');

  console.log('='.repeat(80));
  console.log('ANALYSIS: Latency and Accuracy for Largest Window (Last Window)');
  console.log('='.repeat(80));
  console.log();

  // Parse all results
  const approximationRows = parseCSV(approximationPath);
  const chunkedRows = parseCSV(chunkedPath);
  const fetchingRows = parseCSV(fetchingPath);

  // Group by window
  const approximationWindows = groupByWindow(approximationRows);
  const chunkedWindows = groupByWindow(chunkedRows);
  const fetchingWindows = groupByWindow(fetchingRows);

  console.log(`Total windows found:`);
  console.log(`  Approximation: ${approximationWindows.length}`);
  console.log(`  Chunked Query: ${chunkedWindows.length}`);
  console.log(`  Fetching Client Side: ${fetchingWindows.length}`);
  console.log();

  const allStats: ApproachStats[] = [];

  // Analyze each run (window)
  const numRuns = Math.max(
    approximationWindows.length,
    chunkedWindows.length,
    fetchingWindows.length
  );

  for (let i = 0; i < numRuns; i++) {
    const runNumber = i + 1;
    console.log(`${'='.repeat(80)}`);
    console.log(`RUN ${runNumber}`);
    console.log(`${'='.repeat(80)}`);

    // Get ground truth (Fetching Client Side)
    let groundTruth: number | null = null;
    let groundTruthTimestamp: number | null = null;
    let groundTruthLatency: number | null = null;

    if (i < fetchingWindows.length) {
      const fetchingWindow = fetchingWindows[i];
      const lastResult = getLastResult(fetchingWindow);
      groundTruth = lastResult.value;
      groundTruthTimestamp = fetchingWindow.queryRegisteredTimestamp;
      groundTruthLatency = lastResult.timestamp - fetchingWindow.queryRegisteredTimestamp;

      console.log(`\nGROUND TRUTH (Fetching Client Side):`);
      console.log(`  Query Registered: ${new Date(groundTruthTimestamp).toISOString()}`);
      console.log(`  Last Result Time: ${new Date(lastResult.timestamp).toISOString()}`);
      console.log(`  Latency: ${groundTruthLatency} ms`);
      console.log(`  Result Value: ${groundTruth}`);
    }

    // Approximation Approach
    if (i < approximationWindows.length) {
      const approxWindow = approximationWindows[i];
      const lastResult = getLastResult(approxWindow);
      const latency = lastResult.timestamp - approxWindow.queryRegisteredTimestamp;
      const absoluteError = groundTruth !== null ? Math.abs(lastResult.value - groundTruth) : 0;
      const relativeError = groundTruth !== null && groundTruth !== 0
        ? (absoluteError / Math.abs(groundTruth)) * 100
        : 0;

      console.log(`\nAPPROXIMATION APPROACH:`);
      console.log(`  Query Registered: ${new Date(approxWindow.queryRegisteredTimestamp).toISOString()}`);
      console.log(`  Last Result Time: ${new Date(lastResult.timestamp).toISOString()}`);
      console.log(`  Latency: ${latency} ms`);
      console.log(`  Result Value: ${lastResult.value}`);
      if (groundTruth !== null) {
        console.log(`  Absolute Error: ${absoluteError.toFixed(6)}`);
        console.log(`  Relative Error: ${relativeError.toFixed(2)}%`);
      }

      allStats.push({
        approach: 'Approximation',
        runNumber,
        lastWindowTimestamp: approxWindow.queryRegisteredTimestamp,
        lastWindowLatency: latency,
        lastWindowResult: lastResult.value,
        groundTruthResult: groundTruth || 0,
        absoluteError,
        relativeError
      });
    }

    // Chunked Query Approach
    if (i < chunkedWindows.length) {
      const chunkedWindow = chunkedWindows[i];
      const lastResult = getLastResult(chunkedWindow);
      const latency = lastResult.timestamp - chunkedWindow.queryRegisteredTimestamp;
      const absoluteError = groundTruth !== null ? Math.abs(lastResult.value - groundTruth) : 0;
      const relativeError = groundTruth !== null && groundTruth !== 0
        ? (absoluteError / Math.abs(groundTruth)) * 100
        : 0;

      console.log(`\nCHUNKED QUERY APPROACH:`);
      console.log(`  Query Registered: ${new Date(chunkedWindow.queryRegisteredTimestamp).toISOString()}`);
      console.log(`  Last Result Time: ${new Date(lastResult.timestamp).toISOString()}`);
      console.log(`  Latency: ${latency} ms`);
      console.log(`  Result Value: ${lastResult.value}`);
      if (groundTruth !== null) {
        console.log(`  Absolute Error: ${absoluteError.toFixed(6)}`);
        console.log(`  Relative Error: ${relativeError.toFixed(2)}%`);
      }

      allStats.push({
        approach: 'Chunked',
        runNumber,
        lastWindowTimestamp: chunkedWindow.queryRegisteredTimestamp,
        lastWindowLatency: latency,
        lastWindowResult: lastResult.value,
        groundTruthResult: groundTruth || 0,
        absoluteError,
        relativeError
      });
    }

    console.log();
  }

  // Summary statistics
  console.log(`${'='.repeat(80)}`);
  console.log('SUMMARY STATISTICS (Across All Runs)');
  console.log(`${'='.repeat(80)}`);
  console.log();

  const approaches = ['Approximation', 'Chunked', 'Fetching Client Side'];

  for (const approach of approaches) {
    const stats = allStats.filter(s => s.approach === approach);

    if (stats.length === 0 && approach === 'Fetching Client Side') {
      // Handle fetching separately since it's ground truth
      const fetchingLatencies = fetchingWindows.map((w, idx) => {
        const lastResult = getLastResult(w);
        return lastResult.timestamp - w.queryRegisteredTimestamp;
      });

      const avgLatency = fetchingLatencies.reduce((a, b) => a + b, 0) / fetchingLatencies.length;
      const minLatency = Math.min(...fetchingLatencies);
      const maxLatency = Math.max(...fetchingLatencies);

      console.log(`${approach.toUpperCase()}:`);
      console.log(`  Average Latency: ${avgLatency.toFixed(2)} ms`);
      console.log(`  Min Latency: ${minLatency} ms`);
      console.log(`  Max Latency: ${maxLatency} ms`);
      console.log(`  Accuracy: 100% (Ground Truth)`);
      console.log();
      continue;
    }

    if (stats.length === 0) continue;

    const avgLatency = stats.reduce((sum, s) => sum + s.lastWindowLatency, 0) / stats.length;
    const minLatency = Math.min(...stats.map(s => s.lastWindowLatency));
    const maxLatency = Math.max(...stats.map(s => s.lastWindowLatency));
    const avgAbsError = stats.reduce((sum, s) => sum + s.absoluteError, 0) / stats.length;
    const avgRelError = stats.reduce((sum, s) => sum + s.relativeError, 0) / stats.length;

    console.log(`${approach.toUpperCase()}:`);
    console.log(`  Average Latency: ${avgLatency.toFixed(2)} ms`);
    console.log(`  Min Latency: ${minLatency} ms`);
    console.log(`  Max Latency: ${maxLatency} ms`);
    console.log(`  Average Absolute Error: ${avgAbsError.toFixed(6)}`);
    console.log(`  Average Relative Error: ${avgRelError.toFixed(2)}%`);
    console.log();
  }

  // Write detailed CSV
  const csvPath = path.join(resultsDir, 'analysis_summary.csv');
  const csvHeader = 'Approach,Run,WindowTimestamp,Latency(ms),Result,GroundTruth,AbsError,RelError(%)\n';
  const csvRows = allStats.map(s =>
    `${s.approach},${s.runNumber},${s.lastWindowTimestamp},${s.lastWindowLatency},${s.lastWindowResult},${s.groundTruthResult},${s.absoluteError.toFixed(6)},${s.relativeError.toFixed(2)}`
  ).join('\n');

  fs.writeFileSync(csvPath, csvHeader + csvRows);
  console.log(`Detailed analysis saved to: ${csvPath}`);
}

analyzeResults();
