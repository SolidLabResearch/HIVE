# Streaming Query Approach Accuracy Comparison Report

**Date:** December 16, 2025  
**Experiment:** 5-Iteration Quick Test  
**Data:** Pre-recorded smartphone and wearable acceleration data (481 observations per stream, 4Hz replay)

---

## Executive Summary

This report compares three approaches for streaming query processing:
1. **Client-Side Fetching** (Ground Truth)
2. **Approximation Approach**
3. **Chunked Query Approach**

**Key Findings:**
- Both streaming approaches (Approximation and Chunked Query) are missing one result value (0.7044244) that appears in the ground truth
- Approximation approach has minimal latency overhead (+0.71s) but produces error values (-9.0) in some iterations
- Chunked Query approach has significant latency overhead (+29.78s) but produces consistent results once operational
- Both approaches achieve 66.7% accuracy when comparing unique result values against ground truth

---

## Ground Truth Analysis

**Approach:** Client-Side Fetching (Baseline)

The client-side fetching approach retrieves all streaming data and processes it locally, serving as the ground truth for accuracy comparison.

### Results Summary
- **Total Iterations:** 5
- **Total Results:** 40 (8 per iteration)
- **Unique Values:** [3.9639792, 0.9412997, 0.7044244]
- **Average Latency:** 80.61s
- **Consistency:** 100% - all iterations produced identical unique values

### Per-Iteration Breakdown
| Iteration | Results | Unique Values | Latency |
|-----------|---------|---------------|---------|
| 1 | 8 | 3 | 80.50s |
| 2 | 8 | 3 | 80.67s |
| 3 | 8 | 3 | 80.64s |
| 4 | 8 | 3 | 80.65s |
| 5 | 8 | 3 | 80.59s |

**Standard Deviation:** 0.06s (extremely stable)

---

## Approximation Approach

### Results Summary
- **Total Results:** 10 (2 per iteration)
- **Unique Values:** [3.9639792, 0.9412997, -9.0000000]
- **Average Latency:** 81.32s (+0.71s vs Ground Truth)
- **Accuracy:** 66.7% (2 out of 3 unique values match)

### Issues Identified
1. **Missing Value:** The value 0.7044244 never appears in approximation results
2. **Error Value:** Iteration 3 produced -9.0 instead of 3.9639792, indicating an error condition
3. **Incomplete Results:** Only 2 results per iteration vs 8 in ground truth

### Per-Iteration Analysis
| Iteration | Unique Values | Missing from GT | Extra Values | Latency |
|-----------|---------------|-----------------|--------------|---------|
| 1 | [3.9639792, 0.9412997] | [0.7044244] | None | 81.19s |
| 2 | [3.9639792, 0.9412997] | [0.7044244] | None | 81.69s |
| 3 | [0.9412997, -9.0000000] | [3.9639792, 0.7044244] | [-9.0] | 81.20s |
| 4 | [3.9639792, 0.9412997] | [0.7044244] | None | 81.31s |
| 5 | [3.9639792, 0.9412997] | [0.7044244] | None | 81.23s |

### Performance
- **Latency Overhead:** +0.71s (0.9% increase)
- **Latency Std Dev:** 0.19s (stable)

### Critical Issues
- **Iteration 3 Failure:** Produced -9.0 placeholder/error value
- **Systematic Missing Value:** 0.7044244 is consistently absent across all iterations

---

## Chunked Query Approach

### Results Summary
- **Total Results:** 46 (variable per iteration: 16, 12, 9, 6, 3)
- **Unique Values:** [3.9639792, 0.9412997, 0.0000000]
- **Average Latency:** 110.39s (+29.78s vs Ground Truth)
- **Accuracy:** 66.7% (2 out of 3 unique values match)

### Issues Identified
1. **Missing Value:** The value 0.7044244 never appears in chunked query results
2. **Extra Zero Value:** Iteration 1 produced 0.0 (likely initialization or empty window)
3. **Variable Result Count:** Different iterations produce different numbers of intermediate results
4. **High Latency:** 37% slower than ground truth due to two-stage processing

### Per-Iteration Analysis
| Iteration | Results | Unique Values | Missing from GT | Extra Values | Latency |
|-----------|---------|---------------|-----------------|--------------|---------|
| 1 | 16 | [3.9639792, 0.9412997, 0.0] | [0.7044244] | [0.0] | 62.38s |
| 2 | 12 | [3.9639792, 0.9412997] | [0.7044244] | None | 122.34s |
| 3 | 9 | [3.9639792, 0.9412997] | [0.7044244] | None | 122.41s |
| 4 | 6 | [3.9639792, 0.9412997] | [0.7044244] | None | 122.43s |
| 5 | 3 | [3.9639792, 0.9412997] | [0.7044244] | None | 122.41s |

### Performance
- **Latency Overhead:** +29.78s (37% increase)
- **Latency Std Dev:** 24.01s (highly variable, especially iteration 1)

### Critical Issues
- **Iteration 1 Anomaly:** Much faster (62.38s) but produced spurious 0.0 value
- **Systematic Missing Value:** 0.7044244 is consistently absent across all iterations
- **Decreasing Result Count:** Later iterations produce fewer intermediate results (16→12→9→6→3)

---

## Comparative Analysis

### Accuracy Comparison

| Approach | Matches with GT | Missing Values | Extra Values | Accuracy |
|----------|-----------------|----------------|--------------|----------|
| Ground Truth | 3/3 | - | - | 100.0% |
| Approximation | 2/3 | [0.7044244] | [-9.0] | 66.7% |
| Chunked Query | 2/3 | [0.7044244] | [0.0] | 66.7% |

**Common Issue:** Both streaming approaches miss the value 0.7044244

### Latency Comparison

| Approach | Avg Latency | Std Dev | Overhead vs GT |
|----------|-------------|---------|----------------|
| Ground Truth | 80.61s | 0.06s | - |
| Approximation | 81.32s | 0.19s | +0.71s (0.9%) |
| Chunked Query | 110.39s | 24.01s | +29.78s (37.0%) |

**Winner:** Approximation approach has minimal latency overhead and better stability

### Result Count Comparison

| Approach | Total Results | Results per Iteration | Consistency |
|----------|---------------|----------------------|-------------|
| Ground Truth | 40 | 8 | Perfect |
| Approximation | 10 | 2 | Perfect |
| Chunked Query | 46 | 16→12→9→6→3 | Poor |

---

## Root Cause Analysis

### Missing Value: 0.7044244

[Inference] This value appears consistently in the ground truth but is absent in both streaming approaches. Possible causes:

1. **Window Alignment Issue:** The value may fall on window boundaries and is excluded by the windowing logic
2. **Aggregation Logic:** The value may be a local minimum/intermediate value that doesn't survive the MAX aggregation in chunked/approximate processing
3. **Subquery Coverage:** The subquery windows may not cover the time range where this value occurs

**Recommendation:** Investigate the timestamp of observations with value 0.7044244 in the original data and verify window coverage.

### Approximation -9.0 Error

The -9.0 value in iteration 3 appears to be an error placeholder or sentinel value, suggesting:

1. **Approximation Operator Failure:** The approximation algorithm failed to compute a valid result
2. **Insufficient Data:** The approximation may require a minimum dataset size not met in that window
3. **Error Handling:** The operator is using -9.0 as an error signal rather than skipping the result

**Recommendation:** Review the approximation operator code for error handling and the conditions under which -9.0 is emitted.

### Chunked Query 0.0 Value

The 0.0 value in chunked query iteration 1 suggests:

1. **Empty Window:** An initial window may have no data and returns 0 as default
2. **Aggregation Initialization:** The MAX aggregator may emit 0 before receiving any subquery results
3. **Race Condition:** The aggregator may publish before all subquery results arrive

**Recommendation:** Add logging to track which subquery/window produces 0.0 and verify data coverage.

### Chunked Query Latency Variance

The high variance (24.01s) and the anomalous fast first iteration (62.38s) suggest:

1. **Warm-up Effect:** The first iteration may benefit from incomplete processing
2. **Stale State:** The first iteration may be reusing state from a previous run
3. **Process Initialization:** Subsequent iterations may have different initialization overhead

**Recommendation:** Add proper cleanup between iterations and verify process state reset.

---

## Recommendations

### Immediate Actions

1. **Fix Approximation Error Handling**
   - Investigate why -9.0 is produced in iteration 3
   - Replace error sentinel values with proper error handling (skip or retry)
   - Add logging for approximation failures

2. **Investigate Missing 0.7044244 Value**
   - Analyze the timestamp distribution of this value in the source data
   - Verify that subquery/approximation windows cover the relevant time range
   - Check if this value is being filtered by aggregation logic

3. **Fix Chunked Query Zero Value**
   - Determine source of 0.0 in first iteration
   - Add initialization guards to prevent publishing default/uninitialized values
   - Implement readiness checks before aggregation begins

### Medium-Term Improvements

1. **Add Window Coverage Validation**
   - Log which observations fall into which windows
   - Verify that window union covers all data points
   - Add assertions that each observation is processed by at least one window

2. **Improve Latency Stability**
   - Investigate chunked query latency variance
   - Add per-window and per-subquery timing instrumentation
   - Identify and eliminate race conditions in first iteration

3. **Add Resource Monitoring**
   - Instrument CPU and memory usage during experiments
   - Compare resource consumption across approaches
   - Complete the resource usage table in benchmarking reports

### Long-Term Enhancements

1. **Automated Accuracy Testing**
   - Create unit tests that validate window configuration produces correct aggregations
   - Add property-based tests: assert MAX(chunks) == MAX(all) for various window configs
   - Implement continuous accuracy regression testing

2. **Enhanced Observability**
   - Add distributed tracing across subqueries and aggregators
   - Log provenance: track which source observation contributed to each result
   - Implement result replay and debugging tools

3. **Configuration Optimization**
   - Experiment with different RANGE/STEP window parameters
   - Profile optimal subquery window sizes for different data rates
   - Add auto-tuning based on data characteristics

---

## Conclusion

The 5-iteration experiment reveals that both streaming approaches have accuracy and latency trade-offs compared to the ground truth client-side fetching approach:

- **Approximation Approach:** Low latency overhead but produces error values and misses one result
- **Chunked Query Approach:** High latency overhead with variable performance but more stable once operational

Both approaches suffer from the same missing value issue (0.7044244), suggesting a systematic problem with window coverage or aggregation logic that affects both implementations.

The next steps should focus on:
1. Understanding why 0.7044244 is missing from both streaming approaches
2. Fixing the approximation error handling (-9.0 sentinel values)
3. Stabilizing chunked query latency and eliminating spurious 0.0 values

Once these issues are resolved, a full 35-iteration benchmark should be conducted to validate the fixes and collect comprehensive resource usage metrics.