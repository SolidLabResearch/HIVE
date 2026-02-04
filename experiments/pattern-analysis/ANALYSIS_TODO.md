# Analysis Updates for Multi-Iteration Support

## Current State

The pattern analysis runner now supports 35 iterations per test, but the analysis script (`analysis/accuracy/pattern-accuracy-comparison.js`) currently only reads from `iteration1` directories.

## What Needs to Be Updated

### 1. Analysis Script Updates

The file `analysis/accuracy/pattern-accuracy-comparison.js` needs modifications to:

#### A. Read All Iterations

Current behavior:
```javascript
readMetadata(approach, pattern) {
  const metadataPath = path.join(
    this.baseLogDir,
    approach,
    pattern,
    'iteration1',  // <-- Hardcoded
    `${approach}_metadata.json`
  );
  // ...
}
```

Needed behavior:
```javascript
readAllIterations(approach, pattern) {
  const patternDir = path.join(this.baseLogDir, approach, pattern);
  const iterations = fs.readdirSync(patternDir)
    .filter(dir => dir.startsWith('iteration'))
    .map(dir => parseInt(dir.replace('iteration', '')))
    .sort((a, b) => a - b);
  
  const allData = [];
  for (const iter of iterations) {
    const iterDir = path.join(patternDir, `iteration${iter}`);
    const data = this.readIterationData(approach, pattern, iter);
    if (data) allData.push(data);
  }
  
  return allData;
}
```

#### B. Calculate Statistics Across Iterations

For each pattern-approach combination, compute:

```javascript
{
  mape: {
    values: [12.3, 11.8, 12.5, ...],  // 35 values
    mean: 12.2,
    std: 0.35,
    min: 11.5,
    max: 13.1,
    median: 12.3,
    ci95: [11.85, 12.55]  // 95% confidence interval
  },
  mae: { /* same structure */ },
  rmse: { /* same structure */ },
  latency: { /* same structure */ },
  memory: { /* same structure */ }
}
```

#### C. Statistical Functions to Add

```javascript
class Statistics {
  static mean(values) {
    return values.reduce((a, b) => a + b, 0) / values.length;
  }
  
  static std(values) {
    const avg = this.mean(values);
    const squareDiffs = values.map(v => Math.pow(v - avg, 2));
    const avgSquareDiff = this.mean(squareDiffs);
    return Math.sqrt(avgSquareDiff);
  }
  
  static median(values) {
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 0 
      ? (sorted[mid - 1] + sorted[mid]) / 2 
      : sorted[mid];
  }
  
  static confidenceInterval95(values) {
    const avg = this.mean(values);
    const sd = this.std(values);
    const n = values.length;
    const margin = 1.96 * (sd / Math.sqrt(n));  // Z-score for 95% CI
    return [avg - margin, avg + margin];
  }
  
  static min(values) {
    return Math.min(...values);
  }
  
  static max(values) {
    return Math.max(...values);
  }
}
```

#### D. Updated Output Format

**CSV Output** (`pattern_accuracy_comparison.csv`):
```csv
Pattern,Type,Value,Approach,MAPE_Mean,MAPE_Std,MAPE_Min,MAPE_Max,MAE_Mean,MAE_Std,RMSE_Mean,RMSE_Std,Latency_Mean,Latency_Std,Memory_Mean,Memory_Std,Iterations
exponential_growth_rate_0.001,exponential_growth,0.001,approximation,0.123,0.015,0.105,0.145,0.0012,0.0002,0.0015,0.0003,61.5,0.3,45.2,1.2,35
exponential_growth_rate_0.001,exponential_growth,0.001,chunked,0.118,0.012,0.098,0.138,0.0011,0.0001,0.0014,0.0002,61.8,0.4,52.3,1.5,35
...
```

**JSON Output** (`pattern_analysis_summary.json`):
```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "totalPatterns": 17,
  "iterations": 35,
  "comparisons": [
    {
      "pattern": "exponential_growth_rate_0.001",
      "type": "exponential_growth",
      "value": 0.001,
      "fetching": {
        "latency": { "mean": 61.2, "std": 0.2 },
        "memory": { "mean": 120.5, "std": 2.3 },
        "iterations": 35
      },
      "approximation": {
        "accuracy": {
          "mape": { "mean": 0.123, "std": 0.015, "ci95": [0.118, 0.128] },
          "mae": { "mean": 0.0012, "std": 0.0002 },
          "rmse": { "mean": 0.0015, "std": 0.0003 }
        },
        "latency": { "mean": 61.5, "std": 0.3 },
        "memory": { "mean": 45.2, "std": 1.2 },
        "iterations": 35
      },
      "chunked": {
        "accuracy": {
          "mape": { "mean": 0.118, "std": 0.012, "ci95": [0.114, 0.122] },
          "mae": { "mean": 0.0011, "std": 0.0001 },
          "rmse": { "mean": 0.0014, "std": 0.0002 }
        },
        "latency": { "mean": 61.8, "std": 0.4 },
        "memory": { "mean": 52.3, "std": 1.5 },
        "iterations": 35
      }
    }
  ]
}
```

### 2. Create New Multi-Iteration Analysis Script

Recommended: Create a new script `analysis/accuracy/pattern-accuracy-multi-iteration.js` that:

1. Reads all iterations for each pattern-approach
2. Computes statistical summaries
3. Performs significance tests
4. Generates enhanced outputs

### 3. Statistical Significance Testing

Add t-tests to compare approaches:

```javascript
// Compare approximation vs chunked for a given pattern
function tTest(values1, values2) {
  const mean1 = Statistics.mean(values1);
  const mean2 = Statistics.mean(values2);
  const var1 = Math.pow(Statistics.std(values1), 2);
  const var2 = Math.pow(Statistics.std(values2), 2);
  const n1 = values1.length;
  const n2 = values2.length;
  
  // Welch's t-test (unequal variances)
  const t = (mean1 - mean2) / Math.sqrt(var1/n1 + var2/n2);
  const df = Math.pow(var1/n1 + var2/n2, 2) / 
             (Math.pow(var1/n1, 2)/(n1-1) + Math.pow(var2/n2, 2)/(n2-1));
  
  return { t, df, significant: Math.abs(t) > 2.0 };  // Simplified
}
```

Output:
```
Pattern: exponential_growth_rate_100
  Approximation vs Chunked:
    MAPE: Approximation worse by 25.3% (p < 0.001) ✗✗✗
    
Pattern: exponential_growth_rate_0.001
  Approximation vs Chunked:
    MAPE: No significant difference (p = 0.23) ✓
```

### 4. Visualization Scripts (Optional)

Create plotting scripts using the statistical data:

**`analysis/plots/plot-mape-by-rate.js`**
- X-axis: Exponential rate
- Y-axis: MAPE (mean)
- Error bars: ±1 std
- Lines: One per approach

**`analysis/plots/plot-mape-by-noise.js`**
- X-axis: Noise level
- Y-axis: MAPE (mean)
- Error bars: ±1 std
- Lines: One per approach

**`analysis/plots/plot-memory-comparison.js`**
- Box plots showing memory distribution for each approach

## Quick Implementation Plan

### Option 1: Modify Existing Script

1. Update `readMetadata`, `readResults`, `readResourceUsage` to accept `iterationNum`
2. Add `readAllIterations(approach, pattern)` method
3. Add `Statistics` helper class
4. Update `analyzePattern` to aggregate across iterations
5. Update output format to include mean/std

### Option 2: Create New Script (Recommended)

1. Create `analysis/accuracy/pattern-accuracy-multi-iteration.js`
2. Keep existing script for backward compatibility
3. Implement full statistical analysis
4. Generate enhanced outputs

## Example Usage After Update

```bash
# Run experiments (1,785 tests)
node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 35

# Run multi-iteration analysis
node analysis/accuracy/pattern-accuracy-multi-iteration.js

# View aggregated results
cat logs/pattern-comparison/pattern_accuracy_comparison_aggregated.csv
cat logs/pattern-comparison/pattern_analysis_summary_stats.json
```

## Expected Output Benefits

With 35 iterations, you can now:

1. **Report with confidence**: "Approximation MAPE = 12.3% ± 0.4% (n=35)"
2. **Detect significance**: "Chunked significantly outperforms approximation (p < 0.001)"
3. **Show reliability**: Error bars on plots demonstrate consistency
4. **Handle outliers**: Median provides robust central tendency
5. **Publication-ready**: Mean ± std is standard for academic papers

## LaTeX Table Example (With Statistics)

```latex
\begin{table}[h]
\centering
\caption{Accuracy Comparison (Mean ± SD, n=35)}
\begin{tabular}{l|c|c|c}
\hline
\textbf{Pattern} & \textbf{Approx MAPE (\%)} & \textbf{Chunked MAPE (\%)} & \textbf{p-value} \\
\hline
Exp. Growth λ=0.001 & $0.12 \pm 0.02$ & $0.11 \pm 0.01$ & 0.23 \\
Exp. Growth λ=0.01  & $0.45 \pm 0.05$ & $0.42 \pm 0.04$ & 0.12 \\
Exp. Growth λ=0.1   & $1.23 \pm 0.15$ & $1.05 \pm 0.12$ & 0.003** \\
Exp. Growth λ=1     & $5.67 \pm 0.82$ & $3.21 \pm 0.45$ & <0.001*** \\
Exp. Growth λ=10    & $25.3 \pm 3.4$  & $12.8 \pm 1.9$  & <0.001*** \\
Exp. Growth λ=100   & $49.8 \pm 5.2$  & $28.5 \pm 3.1$  & <0.001*** \\
\hline
\multicolumn{4}{l}{\textit{* p<0.05, ** p<0.01, *** p<0.001}} \\
\end{tabular}
\end{table}
```

## Priority

**High Priority**: Update analysis script to handle multiple iterations
**Medium Priority**: Add statistical significance testing
**Low Priority**: Create visualization scripts

You can start with Option 2 (new script) to avoid breaking existing analysis while developing the enhanced version.