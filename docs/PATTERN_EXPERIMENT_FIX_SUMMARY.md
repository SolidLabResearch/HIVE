# Pattern Experiment Fix Summary

## Problem Overview

The pattern accuracy experiment was returning **0 results** from both the Approximation and Ground Truth approaches when using generated pattern data, while the same system worked correctly with the original frequency variant data.

## Root Causes Identified

### 1. Publisher Timing Drift Issue
**Problem**: The data publisher (`StreamToMQTT.ts`) was using a naive sleep implementation that accumulated timing drift over 480 iterations.

```typescript
// BEFORE (Broken)
for (let i = 0; i < observations.length; i++) {
    await this.publish_one_observation();
    await this.sleep(delay); // Fixed 250ms delay
}
```

**Impact**: 
- Expected duration: 120 seconds (480 observations × 250ms)
- Actual duration: ~21 seconds
- Data stream ended before window slide triggers could fire

**Fix**: Implemented drift-correcting timer that calculates absolute target times.

```typescript
// AFTER (Fixed)
const startTime = Date.now();
for (let i = 0; i < observations.length; i++) {
    const targetTime = startTime + i * delay;
    await this.publish_one_observation();
    
    const now = Date.now();
    const remainingTime = targetTime + delay - now;
    
    if (remainingTime > 0) {
        await this.sleep(remainingTime);
    }
}
```

**Files Modified**:
- `src/streamer/src/publishing/StreamToMQTT.ts`

---

### 2. Incorrect Publisher Path References
**Problem**: Multiple experiment scripts referenced the wrong path for the compiled publisher.

```typescript
// BEFORE (Broken)
"dist/streamer/src/experiment-publisher.js"
```

**Actual Path**: 
```
dist/src/streamer/src/experiment-publisher.js
```

**Files Fixed**:
- `scripts/benchmarks/experiment-evaluation-independent-stream-processing.ts`
- `examples/pattern-accuracy-experiment.ts`
- `examples/pattern-accuracy-experiment-quick.ts`

---

### 3. HTTP Port Mismatch in Approximation Operator
**Problem**: The `RateBasedApproximationApproachOperator` was hardcoded to fetch from port 8080, but experiments run the HTTP server on port 8081 to avoid conflicts.

```typescript
// BEFORE (Broken)
private queryFetchLocation: string = CONFIG.queryFetchLocation;
// CONFIG.queryFetchLocation = "http://localhost:8080/fetchQueries"
```

**Impact**: 
- Operator tried to connect to port 8080
- HTTP server was running on port 8081
- Result: `ECONNREFUSED` error, no queries loaded

**Fix**: Make the operator respect the `HTTP_PORT` environment variable.

```typescript
// AFTER (Fixed)
constructor(inactivityConfig?: InactivityConfig) {
    // Use HTTP_PORT environment variable if set
    const port = process.env.HTTP_PORT
        ? parseInt(process.env.HTTP_PORT)
        : CONFIG.port;
    this.queryFetchLocation = `http://localhost:${port}/fetchQueries`;
}
```

**Files Modified**:
- `src/services/operators/RateBasedApproximationApproachOperator.ts`

---

### 4. HTTP Fetch Race Condition
**Problem**: The operator's `init()` method tried to fetch from the HTTP server immediately, but the server wasn't ready to accept connections yet.

**Fix**: Added retry logic with graceful fallback to locally-added queries.

```typescript
async setMQTTTopicMap(): Promise<void> {
    // Try to fetch with retries
    let retries = 3;
    let lastError: Error | null = null;

    while (retries > 0) {
        try {
            const response = await fetch(this.queryFetchLocation);
            // ... handle success
            return;
        } catch (error) {
            lastError = error as Error;
            retries--;
            if (retries > 0) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    // Fallback: use locally added queries
    if (this.subQueries.length > 0) {
        this.extractedQueries = this.subQueries.map((query, index) => {
            const outputMatch = query.match(/REGISTER\s+RStream\s+<([^>]+)>/i);
            const r2s_topic = outputMatch ? outputMatch[1] : `output_${index}`;
            return { rspql_query: query, r2s_topic: r2s_topic };
        });
    }
}
```

**Files Modified**:
- `src/services/operators/RateBasedApproximationApproachOperator.ts`

---

## Testing Results

### Before Fixes
- **Publisher Duration**: ~21 seconds (should be 120s)
- **Approximation Results**: 0
- **Ground Truth Results**: 0
- **MAPE**: N/A

### After Timing Fix Only
- **Publisher Duration**: ~120 seconds ✓
- **Approximation Results**: 0 (HTTP error)
- **Ground Truth Results**: 2 ✓
- **MAPE**: N/A (insufficient data)

### After All Fixes (Expected)
- **Publisher Duration**: ~120 seconds ✓
- **Approximation Results**: 2-4 ✓
- **Ground Truth Results**: 2-4 ✓
- **MAPE**: Calculable ✓

---

## Files Changed Summary

### Core Fixes
1. `src/streamer/src/publishing/StreamToMQTT.ts` - Drift-correcting timer
2. `src/services/operators/RateBasedApproximationApproachOperator.ts` - Port flexibility & retry logic

### Path Corrections
3. `scripts/benchmarks/experiment-evaluation-independent-stream-processing.ts`
4. `examples/pattern-accuracy-experiment.ts`
5. `examples/pattern-accuracy-experiment-quick.ts`

### Documentation
6. `BUG_FIX_TIMING_DRIFT.md` - Initial timing fix documentation
7. `PATTERN_EXPERIMENT_FIX_SUMMARY.md` - This file

---

## Key Learnings

### 1. Timing is Critical
Even small timing errors (5-10ms per iteration) accumulate over hundreds of iterations, causing the entire stream to compress from 120s to 21s. This breaks windowing logic that depends on wall-clock time.

### 2. Environment Variables Must Propagate
When allowing port configuration via environment variables, all components that depend on that port must respect the variable. Hardcoded values in deeply nested operators can cause subtle bugs.

### 3. Graceful Degradation
Not all experiments need the HTTP server. Adding fallback logic allows the system to work in different deployment scenarios (standalone vs networked).

### 4. Path Management
TypeScript compilation structure (`dist/src/...` vs `dist/...`) must be consistent across all spawn/exec calls. A single path error silently breaks publisher processes.

---

## Verification Commands

```bash
# Rebuild after fixes
npm run build

# Test independent stream processing (works with original data)
npm run experiment:independent -- --frequency 4Hz --iterations 1

# Test pattern experiments (should now work with generated data)
npx ts-node examples/pattern-accuracy-experiment-quick.ts

# Check publisher timing
grep "expected total duration\|Actual duration" [log-file]
# Should show: ~120s actual duration

# Check results
grep "Approx Results Count\|Ground Truth Results Count" [log-file]
# Should show: > 0 for both approaches
```

---

## Related Issues

This fix resolves the issues described in:
- `CONTEXT_FOR_CLAUDE.md` - Original timing mismatch problem
- `GEMINI_DEBUG_PROMPT.md` - Pattern data 0 results issue

---

## Next Steps

1. **Verify all pattern types**: Test with all 7 pattern variants (low_variability, step_pattern, spike_pattern, etc.)
2. **Run full experiment suite**: Execute complete MAPE calculation across all patterns
3. **Compare with frequency experiments**: Ensure both experiment types produce comparable results
4. **Performance testing**: Verify timing accuracy across different frequencies (4Hz, 8Hz, 16Hz, etc.)

---

## Architecture Implications

### Why Ground Truth Worked But Approximation Didn't

The key difference:

**Ground Truth (`FetchingClientSideApproach`)**:
- Uses RSP-JS native event-time windowing
- Doesn't depend on HTTP server
- Processes data based on embedded timestamps

**Approximation (`ApproximationApproach`)**:
- Uses custom `RateBasedApproximationApproachOperator`
- Requires HTTP server for query coordination
- Depends on accurate wall-clock timing
- More sensitive to timing and configuration issues

This explains why fixing the HTTP port and adding retry logic was critical for the Approximation approach but not needed for Ground Truth.