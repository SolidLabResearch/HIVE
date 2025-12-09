# Bug Fix: Publisher Timing Drift Issue

## Problem Summary

The streaming RDF query benchmark was producing **0 results** for the Approximation Approach despite the Ground Truth approach working correctly. The root cause was a timing mismatch between the data publisher and the aggregation operator.

## Root Cause Analysis

### 1. Publisher Speed Issue
- **Expected behavior**: Publisher configured for 4Hz (250ms per message) should take 120 seconds to stream 480 observations
- **Actual behavior**: Publisher finished in ~21 seconds
- **Cause**: The `replay_streams()` method used a naive sleep implementation that accumulated drift

### 2. Operator Timing Logic
- The `RateBasedApproximationApproachOperator` uses wall-clock time (`Date.now()`) for window triggers
- Aggregation triggers based on slide intervals (30s or 60s)
- Has inactivity detection that stops processing when no data arrives

### 3. The Critical Conflict
- All 480 messages arrived in 21 seconds (instead of 120s)
- The operator's first slide trigger was set for 30s or 60s
- After 21s, the data stream ended
- The operator detected "inactivity" and stopped before the first slide could trigger
- **Result**: Zero aggregation results produced

## The Fix

### Before (Naive Implementation)

```typescript
const delay = 1000 / this.frequency; // 250ms for 4Hz

for (let i = 0; i < observations.length; i++) {
    await this.publish_one_observation();
    await this.sleep(delay); // Simple fixed delay
}
```

**Problem**: This approach doesn't account for:
- Execution time of `publish_one_observation()`
- Cumulative drift over 480 iterations
- Async publish operations

### After (Drift-Correcting Timer)

```typescript
const delay = 1000 / this.frequency;
const startTime = Date.now();

for (let i = 0; i < observations.length; i++) {
    const targetTime = startTime + i * delay;
    
    await this.publish_one_observation();
    
    // Calculate remaining time until next target
    const now = Date.now();
    const remainingTime = targetTime + delay - now;
    
    if (remainingTime > 0) {
        await this.sleep(remainingTime);
    } else if (i % 50 === 0) {
        console.log(`Warning: Publisher running behind by ${-remainingTime}ms`);
    }
}
```

**Solution**: This approach:
- Calculates absolute target times based on start time
- Adjusts sleep duration to compensate for execution time
- Prevents drift accumulation across iterations
- Logs warnings if system can't keep up with target rate

## Implementation Details

### File Modified
`src/streamer/src/publishing/StreamToMQTT.ts`

### Key Changes
1. Added `startTime` tracking before the publish loop
2. Calculate `targetTime` for each observation: `startTime + i * delay`
3. Calculate `remainingTime` dynamically: `targetTime + delay - Date.now()`
4. Only sleep for remaining time (not fixed delay)
5. Added drift warning logs every 50 messages
6. Added actual duration logging at completion

### Additional Logging
- Expected total duration calculation
- Actual duration measurement
- Drift warnings when running behind schedule

## Expected Outcomes

### Before Fix
- Publisher duration: ~21 seconds
- Approximation results: 0
- Ground Truth results: ~4

### After Fix
- Publisher duration: ~120 seconds (for 480 observations at 4Hz)
- Approximation results: Should match Ground Truth (~4 results)
- Both approaches should produce comparable results

## Testing

To verify the fix:

```bash
# Rebuild the project
npm run build

# Run experiment with 4Hz frequency
npm run experiment:independent -- --frequency 4Hz --iterations 1

# Check results
# - Publisher logs should show ~120s duration
# - Approximation approach should produce results
# - Count should be > 0
```

## Technical Notes

### Why Wall-Clock Time?
The operator uses wall-clock time instead of event time because:
- Legacy `rsp-js` codebase architecture
- Refactoring to event time would require major changes
- Wall-clock approach works if publisher timing is accurate

### Drift Tolerance
The drift-correcting timer:
- Compensates for execution overhead automatically
- Handles minor system delays
- Will log warnings if system is too slow to maintain rate
- Never "rushes" to catch up (only sleeps for positive remainingTime)

### Alternative Considered: Operator-Side Fix
Instead of fixing publisher timing, we could have:
- Made operator flush on stream end detection
- Used event timestamps from RDF data

**Decision**: Fix publisher timing because:
- More robust across different query patterns
- Aligns with real-world streaming scenarios
- Simpler implementation
- Avoids major refactor of operator logic

## Related Files
- `src/streamer/src/publishing/StreamToMQTT.ts` (fixed)
- `src/services/operators/RateBasedApproximationApproachOperator.ts` (uses wall-clock time)
- `src/streamer/src/experiment-publisher.ts` (calls StreamToMQTT)

## References
- CONTEXT_FOR_CLAUDE.md - Original problem description
- DEPLOYMENT_CHECKLIST.md - Experiment configuration