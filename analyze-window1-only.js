// Analyze only Window 1 results
const fs = require('fs');

// Read chunked results
const chunkedData = fs.readFileSync('chunked_latency_log.csv', 'utf8');
const lines = chunkedData.split('\n').filter(line => line.trim());

console.log('\n=== WINDOW 1 ANALYSIS (Chunked Approach) ===\n');

// Parse Window 1 results only
const window1Results = [];
for (let i = 1; i < lines.length; i++) {
  const parts = lines[i].split(',');
  const windowNum = parseInt(parts[0]);
  if (windowNum === 1) {
    const queryReg = parseInt(parts[1]);
    const expectedClose = parseInt(parts[3]);
    const result = parseFloat(parts[11]);
    
    const expectedWindowStart = queryReg;
    const expectedWindowEnd = expectedClose;
    const duration = expectedWindowEnd - expectedWindowStart;
    
    window1Results.push({
      queryReg,
      expectedClose,
      expectedStart: expectedWindowStart,
      duration,
      result
    });
    
    console.log(`Query Registered: ${queryReg}`);
    console.log(`Expected Window 1: [${expectedWindowStart}, ${expectedWindowEnd}] (${duration}ms = ${duration/1000}s)`);
    console.log(`Result: ${result}`);
    console.log(`---`);
  }
}

console.log('\n=== EXPECTED VALUES FOR STEP PATTERN ===\n');
console.log('Step pattern: -23 for first 60s, then switches to -15');
console.log('');
console.log('For RANGE=120s, STEP=60s:');
console.log('  Window 1 [t+0, t+120s]: Should see ~60s of -23 AND ~60s of -15');
console.log('  Expected average: (-23 + -15) / 2 = -19');
console.log('');
console.log('Actual Window 1 results:');
window1Results.forEach((w, i) => {
  const error = w.result - (-19);
  const errorPct = Math.abs(error / -19 * 100).toFixed(2);
  console.log(`  Test ${i+1}: ${w.result.toFixed(3)} (error: ${error.toFixed(3)}, ${errorPct}% MAPE)`);
});

console.log('\n=== DIAGNOSIS ===\n');
const avgResult = window1Results.reduce((sum, w) => sum + w.result, 0) / window1Results.length;
console.log(`Average Window 1 result: ${avgResult.toFixed(3)}`);
console.log(`Expected: -19.000`);
console.log(`Difference: ${(avgResult - (-19)).toFixed(3)}`);

if (avgResult < -19.5) {
  console.log('\n❌ Window 1 is TOO NEGATIVE (closer to -23)');
  console.log('   This suggests the window is including TOO MUCH early data');
  console.log('   OR not including enough late data (after the regime switch)');
} else if (avgResult > -18.5) {
  console.log('\n❌ Window 1 is TOO POSITIVE (closer to -15)');  
  console.log('   This suggests the window is including TOO MUCH late data');
  console.log('   OR not including enough early data');
} else {
  console.log('\n✅ Window 1 is CORRECT (within acceptable range)');
}
