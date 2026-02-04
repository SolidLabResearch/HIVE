# Pattern Comparison Report: Chunked vs Fetching Approaches
**First Window Analysis**  
*Date: February 4, 2026*

---

## Executive Summary

Comprehensive comparison of **Chunked** vs **Fetching** approaches across 5 different stream patterns, focusing on **first window results only** to assess:
- ✅ **Accuracy alignment** between approaches
- ⚡ **Latency performance**
- 📊 **Pattern-specific behavior**

### Key Findings

| Metric | Chunked | Fetching | Difference |
|--------|---------|----------|------------|
| **Avg Latency** | 60,375.60ms | 63,097.40ms | -2,721.80ms (-4.3%) 🏆 |
| **Avg Accuracy Diff** | - | - | 2.45% ⚠️ |
| **Perfect Alignment** | 1/5 patterns | - | step_pattern only |

**Result:** Chunked approach is consistently **4.3% faster** but shows accuracy misalignment on 4/5 patterns.

---

## Detailed Results by Pattern

### 1. step_pattern ✅ PERFECT
- **Chunked:**  -23.0000 | Latency: 60,390ms
- **Fetching:** -23.0000 | Latency: 63,267ms
- **Accuracy Diff:** 0.0000% ✅ **PERFECT ALIGNMENT**
- **Latency Winner:** 🏆 Chunked (2,877ms faster, -4.5%)

**Analysis:** Both approaches produce identical results. The regime switch occurs at exactly t=120s (observation 240), meaning Window 1 [0-120s] contains only -23 values. This validates that both approaches correctly compute the window boundaries.

---

### 2. spike_pattern ❌ POOR
- **Chunked:**  -20.9444 | Latency: 60,359ms
- **Fetching:** -22.9247 | Latency: 63,070ms
- **Accuracy Diff:** 1.98 | **9.45%** ❌ **POOR ALIGNMENT**
- **Latency Winner:** 🏆 Chunked (2,711ms faster, -4.3%)

**Analysis:** Significant discrepancy (9.45%). This pattern contains sudden spikes that may expose timing issues in chunk selection or window boundary interpretation between approaches.

---

### 3. low_freq_oscillation ❌ POOR
- **Chunked:**  -22.5195 | Latency: 60,358ms
- **Fetching:** -22.9984 | Latency: 63,064ms
- **Accuracy Diff:** 0.48 | **2.13%** ❌ **POOR ALIGNMENT**
- **Latency Winner:** 🏆 Chunked (2,706ms faster, -4.3%)

**Analysis:** 2.13% difference suggests Chunked may be missing some data points or including/excluding different boundary values compared to Fetching.

---

### 4. high_freq_oscillation ⚠️ GOOD
- **Chunked:**  -23.1322 | Latency: 60,364ms
- **Fetching:** -22.9911 | Latency: 63,090ms
- **Accuracy Diff:** 0.14 | **0.61%** ⚠️ **GOOD ALIGNMENT**
- **Latency Winner:** 🏆 Chunked (2,726ms faster, -4.3%)

**Analysis:** Close alignment (0.61%). Minor differences likely due to subtle timestamp boundary handling during high-frequency data arrival.

---

### 5. low_variability ⚠️ GOOD
- **Chunked:**  -22.9916 | Latency: 60,407ms
- **Fetching:** -23.0107 | Latency: 62,996ms
- **Accuracy Diff:** 0.02 | **0.08%** ⚠️ **GOOD ALIGNMENT**
- **Latency Winner:** 🏆 Chunked (2,589ms faster, -4.1%)

**Analysis:** Excellent alignment (0.08%). Low variability reduces edge cases, resulting in near-identical results.

---

## Performance Analysis

### Latency Comparison

```
┌─────────────────────────┬──────────────┬──────────────┬─────────────┐
│ Pattern                 │ Chunked (ms) │ Fetching(ms) │ Diff (ms)   │
├─────────────────────────┼──────────────┼──────────────┼─────────────┤
│ step_pattern            │   60,390     │   63,267     │  -2,877 🏆  │
│ spike_pattern           │   60,359     │   63,070     │  -2,711 🏆  │
│ low_freq_oscillation    │   60,358     │   63,064     │  -2,706 🏆  │
│ high_freq_oscillation   │   60,364     │   63,090     │  -2,726 🏆  │
│ low_variability         │   60,407     │   62,996     │  -2,589 🏆  │
├─────────────────────────┼──────────────┼──────────────┼─────────────┤
│ **AVERAGE**             │  **60,376**  │  **63,097**  │**-2,722 🏆**│
└─────────────────────────┴──────────────┴──────────────┴─────────────┘
```

**Chunked wins on latency across ALL patterns** by an average of 2.7 seconds (4.3% faster).

### Accuracy Comparison

```
┌─────────────────────────┬──────────────┬───────────────┬────────────┐
│ Pattern                 │ Difference   │ Diff %        │ Status     │
├─────────────────────────┼──────────────┼───────────────┼────────────┤
│ step_pattern            │   0.000      │   0.00%       │ ✅ PERFECT │
│ low_variability         │   0.019      │   0.08%       │ ⚠️  GOOD   │
│ high_freq_oscillation   │   0.141      │   0.61%       │ ⚠️  GOOD   │
│ low_freq_oscillation    │   0.479      │   2.13%       │ ❌ POOR    │
│ spike_pattern           │   1.980      │   9.45%       │ ❌ POOR    │
├─────────────────────────┼──────────────┼───────────────┼────────────┤
│ **AVERAGE**             │  **0.524**   │  **2.45%**    │ ⚠️         │
└─────────────────────────┴──────────────┴───────────────┴────────────┘
```

**Accuracy Status:** 1 perfect, 2 good, 2 poor alignments.

---

## Root Cause Analysis

### Why Perfect Alignment on step_pattern?

The `step_pattern` achieves perfect alignment because:
1. Data regime switches **exactly** at t=120s (observation 240)
2. Window 1 [0-120s] contains **only** -23 values (no mixing)
3. No ambiguity in timestamp boundaries
4. Both approaches see identical data points

### Why Misalignment on Other Patterns?

Possible causes for 2-9% discrepancies:

1. **Chunk Boundary Timing**
   - Sub-query windows may close at slightly different times than expected
   - Chunks may include/exclude boundary data points differently than Fetching

2. **Data Point Inclusion/Exclusion**
   - Fetching queries the RSP engine directly for window [0, 120s]
   - Chunked aggregates chunks that may have slightly different timestamp ranges
   - Edge case: Data arriving exactly at t=120,000ms may be handled differently

3. **Pattern-Specific Behavior**
   - **spike_pattern**: Large sudden changes amplify small timing differences
   - **oscillation patterns**: Averaging over partial periods creates sensitivity
   - **low_variability**: Consistent values reduce impact of boundary issues

---

## Investigation Required

### Critical Questions

1. **Chunk Timestamp Verification**
   - Are chunk timestamps truly representing the exact RSP window close times?
   - Check: `RSPQueryProcess.ts` line ~150 (using `object.timestamp`)

2. **Window Boundary Alignment**
   - Does Window 1 for Chunked close at exactly `queryReg + 120000ms`?
   - Does Fetching use the same window boundary?
   - Check: `StreamingQueryChunkAggregatorOperator.ts` line ~352

3. **Data Point Counting**
   - How many data points does each approach see for Window 1?
   - Are they the same count?
   - spike_pattern: Fetching saw 478 points for Window 1

### Recommended Tests

```bash
# 1. Add detailed logging for chunk selection
# Log: chunk timestamp, windowStart, windowClose, data points included

# 2. Compare data point counts
# Chunked: Count data points in all chunks for Window 1
# Fetching: Count from RStream result

# 3. Test with synchronous data
# Use step_pattern (which works) to validate approach
# Then test with patterns that have gradual transitions
```

---

## Memory & CPU Analysis

**Note:** Memory and CPU metrics were attempted but monitoring script had issues. Current results only capture latency.

**TODO:** Enhance monitoring to capture:
- Peak memory usage (RSS)
- Average CPU percentage
- Process execution time

---

## Conclusions

### ✅ Achievements

1. **Latency Performance:** Chunked is consistently **4.3% faster** (2.7 seconds average improvement)
2. **step_pattern Validation:** Perfect alignment proves approaches CAN match when data/timing is clean
3. **Systematic Testing:** Successfully tested across 5 diverse patterns

### ⚠️ Outstanding Issues

1. **Accuracy Misalignment:** 2.45% average difference requires investigation
2. **Pattern Sensitivity:** spike_pattern shows 9.45% error (unacceptable)
3. **Root Cause Unknown:** Need to determine if issue is:
   - Chunk boundary timing
   - Data point inclusion/exclusion
   - Window interpretation differences

### 🎯 Recommendations

1. **Immediate:** Investigate spike_pattern and low_freq_oscillation misalignment
2. **Add Logging:** Enhanced chunk selection logging to track data point inclusion
3. **Data Count Validation:** Compare exact data point counts between approaches
4. **Boundary Testing:** Create test with known boundary conditions to isolate issue

---

## Files Generated

- `pattern_comparison_summary.csv` - Machine-readable results
- `pattern_comparison_results/` - Individual test logs for each pattern
  - `{pattern}_chunked.csv` - Chunked approach detailed logs
  - `{pattern}_fetching.csv` - Fetching approach detailed logs

---

*Report Generated: February 4, 2026*
