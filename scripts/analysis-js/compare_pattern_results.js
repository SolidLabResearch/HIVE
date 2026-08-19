const fs = require('fs');
const path = require('path');

function countResultsInLog(filePath, approach) {
  try {
    if (!fs.existsSync(filePath)) {
      console.log(`File not found: ${filePath}`);
      return 0;
    }
    
    const content = fs.readFileSync(filePath, 'utf8');
    
    if (approach === 'approximation') {
      // Count lines that contain "unifiedAverage" - these are the actual results
      const lines = content.split('\n').filter(line => line.includes('unifiedAverage'));
      return lines.length;
    } else if (approach === 'fetching') {
      // Count lines that contain "RStream result generated" - these are the actual results
      const lines = content.split('\n').filter(line => line.includes('RStream result generated'));
      return lines.length;
    }
    
    return 0;
  } catch (error) {
    console.error(`Error reading ${filePath}:`, error.message);
    return 0;
  }
}

function analyzePatternsComparison() {
  const approximationDir = 'logs/approximation-patterns';
  const fetchingDir = 'logs/fetching-client-side-patterns';
  
  // Get all pattern directories
  const approximationPatterns = fs.readdirSync(approximationDir).filter(item => 
    fs.statSync(path.join(approximationDir, item)).isDirectory()
  );
  
  const fetchingPatterns = fs.readdirSync(fetchingDir).filter(item => 
    fs.statSync(path.join(fetchingDir, item)).isDirectory()
  );
  
  // Combine and deduplicate pattern names
  const allPatterns = [...new Set([...approximationPatterns, ...fetchingPatterns])].sort();
  
  console.log('\n=== PATTERN RESULTS COMPARISON ===\n');
  console.log('| Pattern Name | Approximation Results | Fetching Client Side Results | Difference |');
  console.log('|--------------|----------------------|------------------------------|-----------|');
  
  const results = [];
  
  for (const pattern of allPatterns) {
    // Count approximation results
    const approxLogPath = path.join(approximationDir, pattern, 'iteration1', 'approximation_approach_log.csv');
    const approxResults = countResultsInLog(approxLogPath, 'approximation');
    
    // Count fetching client side results  
    const fetchingLogPath = path.join(fetchingDir, pattern, 'iteration1', 'fetching_client_side_log.csv');
    const fetchingResults = countResultsInLog(fetchingLogPath, 'fetching');
    
    const difference = fetchingResults - approxResults;
    const diffStr = difference > 0 ? `+${difference}` : `${difference}`;
    
    results.push({
      pattern,
      approx: approxResults,
      fetching: fetchingResults,
      diff: difference
    });
    
    console.log(`| ${pattern.padEnd(20)} | ${approxResults.toString().padEnd(20)} | ${fetchingResults.toString().padEnd(28)} | ${diffStr.padEnd(9)} |`);
  }
  
  // Summary statistics
  console.log('\n=== SUMMARY STATISTICS ===');
  const totalApprox = results.reduce((sum, r) => sum + r.approx, 0);
  const totalFetching = results.reduce((sum, r) => sum + r.fetching, 0);
  const avgApprox = totalApprox / results.length;
  const avgFetching = totalFetching / results.length;
  
  console.log(`Total Approximation Results: ${totalApprox}`);
  console.log(`Total Fetching Client Side Results: ${totalFetching}`);
  console.log(`Average Approximation Results per Pattern: ${avgApprox.toFixed(2)}`);
  console.log(`Average Fetching Client Side Results per Pattern: ${avgFetching.toFixed(2)}`);
  
  // Check for consistency
  const inconsistentPatterns = results.filter(r => r.approx !== r.fetching);
  if (inconsistentPatterns.length > 0) {
    console.log(`\n Found ${inconsistentPatterns.length} patterns with different result counts:`);
    inconsistentPatterns.forEach(p => {
      console.log(`   ${p.pattern}: Approximation=${p.approx}, Fetching=${p.fetching} (diff: ${p.diff})`);
    });
  } else {
    console.log('\nAll patterns have consistent result counts between approaches!');
  }
  
  // Check if results match expected count (5 for 2-minute data with overlapping windows)
  const expectedResults = 5;
  const unexpectedCounts = results.filter(r => r.approx !== expectedResults || r.fetching !== expectedResults);
  
  if (unexpectedCounts.length > 0) {
    console.log(`\nPatterns not matching expected count of ${expectedResults}:`);
    unexpectedCounts.forEach(p => {
      console.log(`   ${p.pattern}: Approximation=${p.approx}, Fetching=${p.fetching}`);
    });
  } else {
    console.log(`\nAll patterns have the expected ${expectedResults} results!`);
  }
}

analyzePatternsComparison();
