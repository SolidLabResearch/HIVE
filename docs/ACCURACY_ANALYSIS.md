# Accuracy Analysis: Approximation vs Chunked vs Fetching Approaches

## Executive Summary

This document presents the accuracy analysis comparing three streaming query approaches (Approximation, Chunked, and Fetching) against the calculated ground truth. The analysis evaluates how accurately each approach computes `MAX(?value)` across two sensor streams (wearableX and smartphoneX).

**Key Findings:**
- **Approximation**: No results produced (0% coverage)
- **Chunked**: 6.38% mean error, 60 results
- **Fetching**: 6.38% mean error, 20 results
- Both Chunked and Fetching produce identical error rates but differ in result frequency

---

## Query Configuration

```sparql
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW ... ON STREAM wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW ... ON STREAM smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    { WINDOW wearableX { ?s1 saref:hasValue ?value } }
    UNION
    { WINDOW smartphoneX { ?s2 saref:hasValue ?value } }
}
```

**Parameters:**
- Window Range: 120 seconds (120,000 ms)
- Window Step: 60 seconds (60,000 ms)
- Aggregation: MAX(?value) across both streams
- Replay Frequency: 4 Hz
- Data Source: `noisy_datasets/noise_0.5`
- Total Observations: 962 (481 wearable + 481 smartphone)
- Total Replay Time: ~240 seconds

---

## Ground Truth Calculation

### Expected Maximum Value
**Ground Truth MAX = 4.234195**

This is the maximum value across all 962 observations from both sensor streams.

### Window-Level Ground Truth

The data replay (at 4 Hz) produces 5 overlapping windows:

| Window | Time Range | MAX Value | Observations |
|--------|------------|-----------|--------------|
| 1 | 0.0s - 120.0s | 4.234195 | 480 |
| 2 | 60.0s - 180.0s | 1.926378 | 480 |
| 3 | 120.0s - 240.0s | 1.219225 | 480 |
| 4 | 180.0s - 300.0s | -17.775304 | 242 |
| 5 | 240.0s - 360.0s | -31.392803 | 2 |

**Note:** The global maximum (4.234195) appears only in Window 1, demonstrating the importance of capturing early data accurately.

---

## Results by Approach

### 1. Approximation Approach

**Status:** Failed to produce results

| Metric | Value |
|--------|-------|
| Total Results | 0 |
| Coverage | 0.0% |
| MAE | N/A |
| RMSE | N/A |
| MAPE | N/A |

**Analysis:**
- The Approximation approach did not produce any valid results in the analyzed run
- This appears to be due to the approach using `MAX` subqueries which return placeholder values (-9)
- All -9 values were filtered out during analysis
- **Conclusion:** Approximation approach requires debugging or different data

---

### 2. Chunked Query Approach

**Status:** Active, producing results with small error

| Metric | Value |
|--------|-------|
| Total Results | 60 |
| Unique Values | 1 |
| Computed Value | 3.9639792 |
| Expected Value | 4.234195 |
| **MAE** | **0.270216** |
| **RMSE** | **0.270216** |
| **MAPE** | **6.38%** |
| Max Error | 0.270216 |
| Min Error | 0.270216 |
| Accuracy (within 0.01) | 0.0% |

**Analysis:**
- Produces 60 results (highest frequency)
- All results return the same value: 3.9639792
- Consistent 6.38% error from ground truth
- Error magnitude: 0.270216
- **Hypothesis:** May be computing MAX correctly but missing the peak value from Window 1, or there's a systematic bias in the chunked aggregation

**Strengths:**
- High result frequency (60 results)
- Consistent, predictable output
- Zero variance in computed values

**Weaknesses:**
- Does not capture the true maximum (4.234195)
- 6.38% error is non-negligible for precision applications

---

### 3. Fetching Client-Side Approach

**Status:** Active, producing results with small error

| Metric | Value |
|--------|-------|
| Total Results | 20 |
| Unique Values | 1 |
| Computed Value | 3.9639792 |
| Expected Value | 4.234195 |
| **MAE** | **0.270216** |
| **RMSE** | **0.270216** |
| **MAPE** | **6.38%** |
| Max Error | 0.270216 |
| Min Error | 0.270216 |
| Accuracy (within 0.01) | 0.0% |

**Analysis:**
- Produces 20 results (moderate frequency)
- All results return the same value: 3.9639792 (identical to Chunked)
- Identical error profile to Chunked approach
- **Hypothesis:** Both approaches may share the same underlying issue or use similar subquery results

**Strengths:**
- Moderate result frequency (3x less than Chunked)
- Consistent output
- Same accuracy as Chunked with fewer messages (potentially more efficient)

**Weaknesses:**
- Same 6.38% error as Chunked
- Does not capture the true maximum

---

## Comparative Analysis

### Accuracy Ranking

| Rank | Approach | MAE | MAPE | Result Count |
|------|----------|-----|------|--------------|
| 1 (tie) | Chunked | 0.270216 | 6.38% | 60 |
| 1 (tie) | Fetching | 0.270216 | 6.38% | 60 |
| 3 | Approximation | N/A | N/A | 0 |

### Result Frequency

| Approach | Results | Results per Second | Efficiency |
|----------|---------|-------------------|------------|
| Chunked | 60 | ~0.25 | High frequency |
| Fetching | 20 | ~0.08 | Moderate frequency |
| Approximation | 0 | 0 | No results |

---

## Root Cause Analysis

### Confirmed Root Cause

**Investigation of chunked aggregator logs reveals the exact cause:**

The wearable stream subquery returns **-9.0** (error placeholder) instead of the correct MAX value **-8.695410**. The smartphone stream subquery returns **3.9639792** instead of **4.234195**.

**Log Evidence:**
```
Received message on topic chunked/9a89a03b69e815a39572aa78ff1248f7:
  hasValue> "-9.0"  ← Wearable subquery (WRONG - should be -8.695410)

Received message on topic chunked/135041d6769a9eb448ce3d7632c43b8b:
  hasValue> "3.9639792"  ← Smartphone subquery (WRONG - should be 4.234195)
```

**Final Aggregation:**
```
MAX(-9.0, 3.9639792) = 3.9639792
```

### Data Analysis

**Smartphone Stream:**
- Actual MAX: **4.234195** (this is the global maximum)
- Subquery returns: **3.9639792**
- Error: 0.270216 (6.38%)

**Wearable Stream:**
- Actual MAX: **-8.695410** (all values are negative)
- Subquery returns: **-9.0** (error placeholder)
- This is a complete failure - no valid data processed

### Why Both Approaches Have Identical Errors

Both Chunked and Fetching approaches use the **same subquery infrastructure**:
- Same BeeWorker processes compute subquery results
- Same HTTP registry or MQTT topics distribute results
- Both inherit the same subquery failures

**The aggregation logic is correct** - the problem is upstream in the subquery processing.

### Two Independent Failures

1. **Wearable Subquery Failure (Critical)**
   - Returns -9.0 (placeholder) instead of -8.695410
   - Indicates window has no data or query processing failed
   - Likely due to timing: wearable data arrives too late for first window

2. **Smartphone Subquery Peak Miss (Critical)**
   - Returns 3.9639792 instead of 4.234195
   - Missing the peak value from early in the stream
   - Likely due to initialization: first observations arrive before subscription is ready

---

## Recommendations

### Critical Fix: Peak Value Capture (Highest Priority)

The smartphone stream contains the global maximum (4.234195). Fixing peak capture resolves the 6.38% error.

**Action Items:**
1. **Add warm-up period before data replay**
   ```typescript
   // Wait for all subqueries to be ready before publishing
   await sleep(10000); // 10 second warm-up
   ```

2. **Implement explicit readiness checks**
   - Each BeeWorker logs "READY" when subscribed
   - Experiment runner waits for all "READY" signals
   - Only then start data publishers

3. **Log first observations**
   - Add logging in subquery BeeWorker for first 50 observations
   - Verify observation with value 4.234195 is captured
   - Alert if peak values are missing

4. **Increase initialization wait**
   - Current INIT_WAIT: 20s
   - Recommended: 30s for safety margin

### Important Fix: Wearable Subquery Error (High Priority)

The wearable subquery returns -9.0 (error placeholder) instead of valid data.

**Action Items:**
1. **Debug wearable subquery BeeWorker**
   - Add logging to verify observations arrive
   - Check window boundaries and timing
   - Ensure no early timeout or failure condition

2. **Monitor MQTT topics**
   ```bash
   mosquitto_sub -h localhost -t wearableX -v
   ```

3. **Validate window alignment**
   - Check if wearable data arrives within window bounds
   - Verify both streams are temporally synchronized during replay

### Validation Approach**
   - Investigate why Approximation returns only -9 values
   - Check if the MAX approximation logic is working correctly
   - Consider alternative approximation strategies

### Long-Term Improvements

1. **Add Warm-Up Period**
   - Implement a 5-10 second warm-up before starting actual data replay
   - Ensures all operators are fully initialized and subscribed

2. **Implement End-to-End Validation**
   - Add assertions to verify peak values are captured
   - Log first/last observation timestamps per window
   - Alert if computed MAX is suspiciously lower than expected

3. **Create Synthetic Test Cases**
   - Design data with known peak values at different time positions
   - Test early-peak, mid-peak, and late-peak scenarios
   - Validate each approach captures peaks correctly

4. **Enhance Monitoring**
   - Track per-window MAX values separately
   - Log observation counts per window
   - Detect and alert on missing windows

---

## Accuracy vs Performance Trade-off

### Current State

| Approach | Accuracy | Result Frequency | Verdict |
|----------|----------|------------------|---------|
| Approximation | Unknown (0 results) | N/A | Needs fixing |
| Chunked | 93.62% (6.38% error) | High (60 results) | Good, needs peak capture fix |
| Fetching | 93.62% (6.38% error) | Moderate (20 results) | Good, more efficient than Chunked |

### Interpretation

- **Chunked vs Fetching:** Both have identical accuracy but Chunked produces 3x more results
  - If low latency/high frequency is critical: Choose Chunked
  - If efficiency/lower overhead is critical: Choose Fetching
- **Approximation:** Requires investigation before evaluation

---

## Conclusion

Both the Chunked and Fetching approaches achieve **~94% accuracy** with a mean absolute percentage error of **6.38%**. The error is systematic (all results return 3.9639792 instead of 4.234195), suggesting a consistent issue with capturing the peak value from the first window.

**Next Steps:**
1. Investigate first-window data capture (likely root cause)
2. Debug Approximation approach to enable comparison
3. Implement recommended monitoring and validation
4. Re-run accuracy analysis after fixes

**Expected Outcome After Fixes:**
- Target: <1% MAPE across all approaches
- All approaches should capture the true maximum: 4.234195
- Approximation should produce valid results for comparison

---

## Appendix: Running the Accuracy Analysis

### Prerequisites
```bash
# Ensure results exist from previous experiment runs
npm run experiment:5-iterations

# Results should be in:
# - results/approximation_results.csv
# - results/chunked_query_results.csv
# - results/fetching_client_side_results.csv
```

### Run Analysis
```bash
npm run experiment:calculate-accuracy
```

### Output Files
- Console report with detailed metrics table
- `results/accuracy-report-<timestamp>.json` with full details

### Interpreting Results
- **MAE (Mean Absolute Error):** Average magnitude of error (lower is better)
- **RMSE (Root Mean Squared Error):** Penalizes large errors more (lower is better)
- **MAPE (Mean Absolute Percentage Error):** Error as percentage (lower is better)
- **Accuracy:** Percentage of results within tolerance (higher is better)

---

---

## Additional Resources

- **[Root Cause Deep Dive](ACCURACY_ROOT_CAUSE.md)** - Detailed technical analysis with log evidence
- **[Known Issues](KNOWN_ISSUES.md)** - Current limitations and workarounds
- **[Quick Start](../QUICK_START.md)** - Running experiments and analysis

---

**Document Version:** 1.1  
**Last Updated:** 2025-12-16  
**Root Cause:** Identified - Subquery peak capture failure  
**Data Version:** noisy_datasets/noise_0.5  
**Author:** Accuracy Analysis Script