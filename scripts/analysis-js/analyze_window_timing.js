const fs = require('fs');

function analyzeWindowTiming() {
  console.log('WINDOW TIMING ANALYSIS\n');
  
  // Analyze approximation approach timing
  console.log('=== APPROXIMATION APPROACH TIMING ===');
  const approxContent = fs.readFileSync('logs/approximation-patterns/challenging_chaotic_oscillation/iteration1/approximation_approach_log.csv', 'utf8');
  
  const approxResults = [];
  const approxLines = approxContent.split('\n').filter(line => line.includes('Final aggregation results'));
  
  approxLines.forEach((line, index) => {
    const timestampMatch = line.match(/^(\d+),/);
    const windowMatch = line.match(/"window":\{"start":(\d+),"end":(\d+)\}/);
    
    if (timestampMatch && windowMatch) {
      const eventTime = parseInt(timestampMatch[1]);
      const windowStart = parseInt(windowMatch[1]);
      const windowEnd = parseInt(windowMatch[2]);
      const windowDuration = windowEnd - windowStart;
      
      approxResults.push({
        index: index + 1,
        eventTime,
        windowStart,
        windowEnd,
        windowDuration
      });
    }
  });
  
  console.log('Approximation Results:');
  console.log('| # | Event Time | Window Start | Window End | Duration (ms) | Duration (s) |');
  console.log('|---|------------|--------------|------------|---------------|--------------|');
  
  approxResults.forEach(result => {
    const durationSeconds = (result.windowDuration / 1000).toFixed(1);
    console.log(`| ${result.index} | ${result.eventTime} | ${result.windowStart} | ${result.windowEnd} | ${result.windowDuration} | ${durationSeconds}s |`);
  });
  
  // Calculate intervals between results
  console.log('\nApproximation Intervals:');
  console.log('| # | Time Between Results (ms) | Time Between Results (s) |');
  console.log('|---|---------------------------|--------------------------|');
  
  for (let i = 1; i < approxResults.length; i++) {
    const interval = approxResults[i].eventTime - approxResults[i-1].eventTime;
    const intervalSeconds = (interval / 1000).toFixed(1);
    console.log(`| ${i+1} | ${interval} | ${intervalSeconds}s |`);
  }
  
  // Analyze fetching client side timing
  console.log('\n=== FETCHING CLIENT SIDE TIMING ===');
  const fetchingContent = fs.readFileSync('logs/fetching-client-side-patterns/challenging_chaotic_oscillation/iteration1/fetching_client_side_log.csv', 'utf8');
  
  const fetchingResults = [];
  const fetchingLines = fetchingContent.split('\n').filter(line => line.includes('RStream result generated'));
  
  fetchingLines.forEach((line, index) => {
    const timestampMatch = line.match(/^(\d+),/);
    const resultMatch = line.match(/RStream result generated: ([\d.]+) at timestamp: (\d+)/);
    
    if (timestampMatch && resultMatch) {
      const eventTime = parseInt(timestampMatch[1]);
      const resultValue = parseFloat(resultMatch[1]);
      const resultTimestamp = parseInt(resultMatch[2]);
      
      fetchingResults.push({
        index: index + 1,
        eventTime,
        resultValue,
        resultTimestamp
      });
    }
  });
  
  console.log('Fetching Client Side Results:');
  console.log('| # | Event Time | Result Value | Result Timestamp |');
  console.log('|---|------------|--------------|------------------|');
  
  fetchingResults.forEach(result => {
    console.log(`| ${result.index} | ${result.eventTime} | ${result.resultValue.toFixed(6)} | ${result.resultTimestamp} |`);
  });
  
  // Calculate intervals
  if (fetchingResults.length > 1) {
    console.log('\nFetching Client Side Intervals:');
    console.log('| # | Time Between Results (ms) | Time Between Results (s) |');
    console.log('|---|---------------------------|--------------------------|');
    
    for (let i = 1; i < fetchingResults.length; i++) {
      const interval = fetchingResults[i].eventTime - fetchingResults[i-1].eventTime;
      const intervalSeconds = (interval / 1000).toFixed(1);
      console.log(`| ${i+1} | ${interval} | ${intervalSeconds}s |`);
    }
  }
  
  console.log('\n=== ANALYSIS ===');
  console.log(`Approximation approach: ${approxResults.length} results`);
  console.log(`Fetching client side: ${fetchingResults.length} results`);
  
  if (approxResults.length > 0) {
    const avgApproxInterval = approxResults.slice(1).reduce((sum, result, i) => {
      return sum + (result.eventTime - approxResults[i].eventTime);
    }, 0) / (approxResults.length - 1);
    console.log(`⏱️  Average approximation interval: ${(avgApproxInterval / 1000).toFixed(1)}s`);
  }
  
  if (fetchingResults.length > 1) {
    const avgFetchingInterval = fetchingResults.slice(1).reduce((sum, result, i) => {
      return sum + (result.eventTime - fetchingResults[i].eventTime);
    }, 0) / (fetchingResults.length - 1);
    console.log(`⏱️  Average fetching interval: ${(avgFetchingInterval / 1000).toFixed(1)}s`);
  }
  
  console.log('\n=== EXPECTED vs ACTUAL ===');
  console.log('Expected: 2 results for 2-minute data stream');
  console.log('Window config: RANGE 120000ms (120s), STEP 60000ms (60s)');
  console.log('Data duration: 2 minutes (120s)');
  console.log('Expected windows: [0-120s], [60-180s] = 2 overlapping windows');
  console.log(`Approximation actual: ${approxResults.length} results`);
  console.log(`Fetching actual: ${fetchingResults.length} results`);
}

analyzeWindowTiming();
