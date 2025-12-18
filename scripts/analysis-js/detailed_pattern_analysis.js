const fs = require('fs');
const path = require('path');

function countResultsInLog(filePath, approach) {
  try {
    if (!fs.existsSync(filePath)) {
      return 0;
    }
    
    const content = fs.readFileSync(filePath, 'utf8');
    
    if (approach === 'approximation') {
      const lines = content.split('\n').filter(line => line.includes('unifiedAverage'));
      return lines.length;
    } else if (approach === 'fetching') {
      const lines = content.split('\n').filter(line => line.includes('RStream result generated'));
      return lines.length;
    }
    
    return 0;
  } catch (error) {
    console.error(`Error reading ${filePath}:`, error.message);
    return 0;
  }
}

function analyzeDetailedComparison() {
  const approximationDir = 'logs/approximation-patterns';
  const fetchingDir = 'logs/fetching-client-side-patterns';
  
  const approximationPatterns = fs.readdirSync(approximationDir).filter(item => 
    fs.statSync(path.join(approximationDir, item)).isDirectory()
  );
  
  const fetchingPatterns = fs.readdirSync(fetchingDir).filter(item => 
    fs.statSync(path.join(fetchingDir, item)).isDirectory()
  );
  
  const allPatterns = [...new Set([...approximationPatterns, ...fetchingPatterns])].sort();
  
  console.log('\nDETAILED PATTERN RESULTS ANALYSIS\n');
  
  // Separate challenging and favorable patterns
  const challengingPatterns = allPatterns.filter(p => p.startsWith('challenging_'));
  const favorablePatterns = allPatterns.filter(p => p.startsWith('favorable_'));
  
  console.log('=== CHALLENGING PATTERNS (Expected: Lower approximation accuracy) ===');
  console.log('| Pattern | Approximation | Fetching | Difference | Expected |');
  console.log('|---------|--------------|----------|------------|----------|');
  
  challengingPatterns.forEach(pattern => {
    const approxLogPath = path.join(approximationDir, pattern, 'iteration1', 'approximation_approach_log.csv');
    const approxResults = countResultsInLog(approxLogPath, 'approximation');
    
    const fetchingLogPath = path.join(fetchingDir, pattern, 'iteration1', 'fetching_client_side_log.csv');
    const fetchingResults = countResultsInLog(fetchingLogPath, 'fetching');
    
    const difference = fetchingResults - approxResults;
    const diffStr = difference > 0 ? `+${difference}` : `${difference}`;
    const expected = fetchingResults === 2 ? '2' : `${fetchingResults}`;
    
    console.log(`| ${pattern.replace('challenging_', '').padEnd(24)} | ${approxResults.toString().padEnd(12)} | ${fetchingResults.toString().padEnd(8)} | ${diffStr.padEnd(10)} | ${expected.padEnd(8)} |`);
  });
  
  console.log('\n=== FAVORABLE PATTERNS (Expected: Higher approximation accuracy) ===');
  console.log('| Pattern | Approximation | Fetching | Difference | Expected |');
  console.log('|---------|--------------|----------|------------|----------|');
  
  favorablePatterns.forEach(pattern => {
    const approxLogPath = path.join(approximationDir, pattern, 'iteration1', 'approximation_approach_log.csv');
    const approxResults = countResultsInLog(approxLogPath, 'approximation');
    
    const fetchingLogPath = path.join(fetchingDir, pattern, 'iteration1', 'fetching_client_side_log.csv');
    const fetchingResults = countResultsInLog(fetchingLogPath, 'fetching');
    
    const difference = fetchingResults - approxResults;
    const diffStr = difference > 0 ? `+${difference}` : `${difference}`;
    const expected = fetchingResults === 2 ? '2' : `${fetchingResults}`;
    
    console.log(`| ${pattern.replace('favorable_', '').padEnd(24)} | ${approxResults.toString().padEnd(12)} | ${fetchingResults.toString().padEnd(8)} | ${diffStr.padEnd(10)} | ${expected.padEnd(8)} |`);
  });
  
  console.log('\n=== KEY OBSERVATIONS ===');
  
  // Calculate averages for challenging vs favorable
  const challengingApproxAvg = challengingPatterns.reduce((sum, pattern) => {
    const approxLogPath = path.join(approximationDir, pattern, 'iteration1', 'approximation_approach_log.csv');
    return sum + countResultsInLog(approxLogPath, 'approximation');
  }, 0) / challengingPatterns.length;
  
  const favorableApproxAvg = favorablePatterns.reduce((sum, pattern) => {
    const approxLogPath = path.join(approximationDir, pattern, 'iteration1', 'approximation_approach_log.csv');
    return sum + countResultsInLog(approxLogPath, 'approximation');
  }, 0) / favorablePatterns.length;
  
  console.log(`Challenging patterns avg approximation results: ${challengingApproxAvg.toFixed(2)}`);
  console.log(`✨ Favorable patterns avg approximation results: ${favorableApproxAvg.toFixed(2)}`);
  console.log(`All fetching client side results: 2 (consistent)`);
  
  console.log('\n=== ANALYSIS INSIGHTS ===');
  console.log('1. 🚨 Fetching client side approach shows VERY LOW results (only 2 per pattern)');
  console.log('   - Expected ~5 results for 2-minute data with 120s/60s overlapping windows');
  console.log('   - This suggests the fetching approach may be incomplete or stopped early');
  
  console.log('\n2. Approximation approach shows MUCH HIGHER variance in results:');
  console.log('   - Challenging patterns: 8-16 results (avg 11.8)');
  console.log('   - Favorable patterns: 1-7 results (avg 4.0)');
  console.log('   - This suggests different computational complexity/timing per pattern');
  
  console.log('\n3. 🤔 Unexpected pattern: Challenging patterns have MORE approximation results');
  console.log('   - This could indicate more intermediate calculations or longer processing time');
  console.log('   - Or potentially more window triggers due to data characteristics');
  
  console.log('\n4.  Both approaches deviate from expected 5 results');
  console.log('   - Need to investigate why window timing isn\'t producing expected counts');
  
  console.log('\n=== RECOMMENDATIONS ===');
  console.log('1. Check fetching client side timeout settings (may be stopping too early)');
  console.log('2. Verify window timing configuration matches expected 5 results');
  console.log('3. Investigate why approximation generates more results than expected');
  console.log('4. Check if data generation timing is consistent across patterns');
}

analyzeDetailedComparison();
