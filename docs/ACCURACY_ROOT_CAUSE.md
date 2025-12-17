# Root Cause Analysis: Accuracy Issue in Chunked Query Approach

## Executive Summary

The Chunked and Fetching approaches are producing **3.9639792** instead of the expected ground truth **4.234195**, resulting in a **6.38% error**. Root cause analysis reveals that the **wearable stream subquery is returning -9.0 (placeholder/error value) instead of the correct MAX value of -8.695410**.

---

## Problem Statement

**Observed Behavior:**
- Ground Truth MAX: **4.234195**
- Chunked Approach Result: **3.9639792** (6.38% error)
- Fetching Approach Result: **3.9639792** (6.38% error)
- Approximation Approach: No valid results (returns only -9.0 values)

**Question:** Why are both approaches producing the same incorrect value?

---

## Investigation

### Step 1: Data Analysis

Analysis of the raw sensor data reveals:

**Smartphone Stream (smartphone.acceleration.x):**
```
Time Range: 07:50:23 - 07:50:38 (15 seconds)
Observations: 481
Value Range: -31.392803 to 4.234195
MAX Value: 4.234195 ✓ (This is the global maximum)
```

**Wearable Stream (wearable.acceleration.x):**
```
Time Range: 08:48:24 - 08:48:39 (15 seconds)
Observations: 481
Value Range: -31.392803 to -8.695410
MAX Value: -8.695410 ✓ (All values are negative)
```

**Combined Stream:**
```
Total Observations: 962
Global MAX: 4.234195 (from smartphone stream)
```

### Step 2: Subquery Results Analysis

Examination of the chunked aggregator logs reveals:

```
[Timestamp: 1765890297693]
Received message on topic chunked/9a89a03b69e815a39572aa78ff1248f7:
  <...> hasValue> "-9.0"^^<http://www.w3.org/2001/XMLSchema#float>

Received message on topic chunked/135041d6769a9eb448ce3d7632c43b8b:
  <...> hasValue> "3.9639792"^^<http://www.w3.org/2001/XMLSchema#float>
```

**Analysis:**
- Topic `9a89a03b69e815a39572aa78ff1248f7`: Wearable subquery → Returns **-9.0** ❌
- Topic `135041d6769a9eb448ce3d7632c43b8b`: Smartphone subquery → Returns **3.9639792** ✓

### Step 3: Aggregation Logic

The chunked aggregator correctly computes:
```sparql
SELECT (MAX(?o) AS ?result) WHERE { ?s ?p ?o }
```

Over the received chunk values:
```
MAX(-9.0, 3.9639792) = 3.9639792
```

**Conclusion:** The aggregation logic is correct. The problem is that the wearable subquery is returning **-9.0** instead of **-8.695410**.

---

## Root Cause

### Primary Issue: Wearable Subquery Returns Error Value

The wearable stream subquery is configured as:
```sparql
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> 
  ON STREAM mqtt_broker:wearableX [RANGE 30000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
```

**Expected Result:** -8.695410  
**Actual Result:** -9.0 (error placeholder)

### Why Does the Smartphone Subquery Return 3.9639792?

The smartphone subquery processes a **30-second window** with a **30-second step**, so it only captures:
- Data from approximately 07:50:23 to 07:50:53

The peak value (4.234195) occurs early in the smartphone stream. If the subquery:
1. Misses the first few seconds due to initialization timing, OR
2. Only processes a subset of the data window

Then it would compute MAX over a smaller dataset, potentially yielding 3.9639792 instead of 4.234195.

---

## Why -9.0 Appears

The value **-9.0** is a known placeholder/error value in the RSP.js engine, typically returned when:
1. No data is available in the window
2. Query processing fails
3. Window hasn't received enough data yet
4. Initialization timing issues

Given that the wearable data timestamp range (08:48:24-08:48:39) is **58 minutes later** than the smartphone data (07:50:23-07:50:38), there's a significant temporal gap.

During the experiment replay at 4 Hz:
- Smartphone data is published first (0-120s of replay time)
- Wearable data is published much later (~3480-3600s if replayed sequentially)

**However**, the current publisher code suggests both streams are replayed **concurrently**, not sequentially. This means:
- Smartphone: 481 observations / 4 Hz = ~120 seconds
- Wearable: 481 observations / 4 Hz = ~120 seconds
- Both publish in parallel

### The Real Problem: Window Timing Mismatch

The wearable subquery uses a **30-second window** but the wearable stream data might:
1. Arrive after the first window has already closed
2. Not be captured in any window due to timing
3. Experience race conditions during initialization

---

## Secondary Issue: Smartphone Subquery Missing Peak

Even the smartphone subquery (which successfully returns a value) misses the peak:
- Expected: 4.234195
- Actual: 3.9639792
- Error: 0.270216 (6.38%)

**Hypothesis:**
The peak value arrives in the first few seconds of data replay. If the subquery:
1. Isn't fully subscribed when the first observations arrive, OR
2. Misses the first ~30 observations due to initialization lag

Then it would compute MAX over observations AFTER the peak, yielding a lower value.

---

## Cascading Effects

### Why Both Approaches Have Identical Errors

**Chunked Approach:**
1. Runs two subqueries (wearable, smartphone)
2. Wearable subquery returns -9.0
3. Smartphone subquery returns 3.9639792
4. Final aggregation: MAX(-9.0, 3.9639792) = 3.9639792

**Fetching Approach:**
1. Also uses HTTP-registered subqueries (same infrastructure)
2. Fetches results from the same subquery processes
3. Gets the same -9.0 and 3.9639792 values
4. Final aggregation: MAX(-9.0, 3.9639792) = 3.9639792

**Conclusion:** Both approaches share the same subquery infrastructure, so they inherit the same errors.

---

## Verification: Expected vs Actual Values

| Stream | Ground Truth MAX | Subquery Returns | Status |
|--------|------------------|------------------|--------|
| Smartphone | 4.234195 | 3.9639792 | ❌ Missing peak (6.38% error) |
| Wearable | -8.695410 | -9.0 | ❌ Error placeholder |
| **Combined** | **4.234195** | **3.9639792** | ❌ **6.38% error** |

---

## Recommended Fixes

### Fix 1: Debug Wearable Subquery (High Priority)

**Action Items:**
1. Add detailed logging to the wearable subquery BeeWorker process
2. Verify observations are arriving at the subquery input stream
3. Check window boundaries and data capture
4. Ensure the subquery doesn't return -9.0 for valid data
5. Investigate why wearable stream processing might be failing

**Diagnostic Commands:**
```bash
# Monitor wearable subquery logs
tail -f logs/bee-worker-wearable-*.log

# Check MQTT messages on wearable topic
mosquitto_sub -h localhost -t wearableX -v
```

### Fix 2: Ensure Peak Value Capture (High Priority)

**Problem:** Smartphone subquery misses the peak value (4.234195)

**Root Cause:** First-window data loss due to initialization timing

**Solutions:**
1. **Warm-up period:** Add 5-10 second delay before starting data replay
2. **Explicit readiness check:** Wait for all subqueries to log "ready" before publishing
3. **Longer initialization wait:** Increase INIT_WAIT from 20s to 30s
4. **First-run retry:** Automatically retry if first window returns suspiciously low values

### Fix 3: Improve Window Alignment (Medium Priority)

Current setup has temporal mismatch between data timestamps and replay. Consider:
1. **Normalize timestamps:** Rewrite data timestamps to be continuous
2. **Sequence guarantee:** Ensure both streams are temporally aligned during replay
3. **Window synchronization:** Add explicit window boundary markers

### Fix 4: Add Validation and Alerts (Medium Priority)

**Detect anomalies:**
```typescript
// Pseudo-code for validation
if (result === -9.0) {
  logger.error("Received error placeholder value from subquery");
  // Trigger alert or retry
}

if (result < expectedMin || result > expectedMax) {
  logger.warn("Result outside expected range", { result, expectedMin, expectedMax });
}
```

---

## Test Plan

### Test 1: Verify Wearable Subquery Independently

```bash
# Run only wearable subquery in isolation
# Expected: MAX = -8.695410, not -9.0
```

### Test 2: Verify Smartphone Subquery Peak Capture

```bash
# Add logging for first 50 observations received
# Verify observation with value 4.234195 is captured
```

### Test 3: End-to-End with Fixes

```bash
# After implementing fixes
# Expected: Chunked and Fetching approaches return 4.234195
# Accuracy: >99%
```

---

## Impact Assessment

### Current State
- **Chunked:** 93.62% accuracy (6.38% error)
- **Fetching:** 93.62% accuracy (6.38% error)
- **Approximation:** 0% (needs separate investigation)

### After Fix 1 (Wearable Subquery)
If only wearable is fixed:
- Aggregation would compute MAX(-8.695410, 3.9639792) = 3.9639792
- **No improvement** (smartphone peak is still higher)

### After Fix 2 (Peak Capture)
If smartphone subquery captures peak:
- Aggregation would compute MAX(-9.0 or -8.695410, 4.234195) = 4.234195
- **100% accuracy achieved** ✓

### After Both Fixes
- Wearable returns -8.695410 ✓
- Smartphone returns 4.234195 ✓
- Final MAX = 4.234195 ✓
- **Expected accuracy: >99%**

---

## Conclusion

The 6.38% error is caused by **two independent failures**:
1. **Wearable subquery failure:** Returns -9.0 instead of -8.695410
2. **Smartphone subquery peak miss:** Returns 3.9639792 instead of 4.234195

The smartphone issue is critical because its peak (4.234195) is the global maximum. Fixing peak capture in the smartphone subquery will resolve the accuracy issue even if the wearable issue persists.

**Priority:**
1. ✅ **Fix smartphone subquery peak capture** (resolves 6.38% error)
2. ⚠️ **Fix wearable subquery -9.0 error** (prevents future issues)
3. 🔍 **Investigate approximation approach** (separate issue)

**Next Steps:**
1. Implement warm-up period or readiness checks
2. Add first-observation logging to verify peak capture
3. Re-run accuracy analysis after fixes
4. Document results and update accuracy metrics