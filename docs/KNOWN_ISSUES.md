# Known Issues

## Chunked Approach - First Run Failure (FIXED)

### Issue
The Chunked Query Approach was failing on the first run of multi-run experiments (80% success rate instead of 100%).

### Symptoms
- First run (Run 1): 0 results
- Subsequent runs (Runs 2-5): Normal results (4-8 results per run)
- Other approaches (Approximation, Fetching) work fine on Run 1

### Root Cause
**Asynchronous MQTT Connection Not Awaited**

The `handleAggregation()` method in `StreamingQueryChunkAggregatorOperator` was calling `mqtt.connect()` but returning immediately without waiting for the connection to be established. The MQTT subscription happened in the async `connect` callback, which could fire after data publishing had already started.

```typescript
// BEFORE (problematic)
const rsp_client = mqtt.connect(this.mqttBroker);
rsp_client.on("connect", () => {
  // Subscribe to topics...
});
// Method returns here, connection may not be ready!
```

### Timeline Evidence
```
T+0s:    Orchestrators launch
T+10s:   Data publishing starts
T+10-15s: Chunked operator MQTT connection still establishing
T+120s:  Data publishing completes
Result:  Operator missed all data → 0 results
```

### Fix Applied
Modified `handleAggregation()` in `StreamingQueryChunkAggregatorOperator.ts` to wait for MQTT connection before returning:

```typescript
// AFTER (fixed)
await new Promise<void>((resolve, reject) => {
  const timeout = setTimeout(() => {
    reject(new Error("MQTT connection timeout after 10 seconds"));
  }, 10000);

  rsp_client.on("connect", () => {
    clearTimeout(timeout);
    // Subscribe to topics...
    resolve(); // Only resolve after connection is ready
  });
});
console.log("[CHUNKED] Connected to MQTT broker");
```

**Files Modified:**
- `src/services/operators/StreamingQueryChunkAggregatorOperator.ts`: Made `handleAggregation()` wait for MQTT connection
- `scripts/run-5-iterations.ts`: Increased `INIT_WAIT_S` from 10 to 15 (additional safety margin)
- `scripts/run-35-iterations.ts`: Increased `INIT_WAIT_S` from 10 to 15 (additional safety margin)

### Expected Impact
- ✅ Chunked success rate should now be 100% (5/5 runs)
- Proper synchronization ensures operator is ready before data arrives
- Initialization wait increase adds 5 seconds per run as safety margin

### Why This Affects Chunked More Than Others

**Approximation Approach:**
- Direct operator creation (1 step)
- Faster MQTT subscription

**Fetching Approach:**
- Direct client creation (1 step)
- Immediate MQTT subscription

**Chunked Approach:**
- Orchestrator → HTTP server → RSPAgents → HTTP registration → BeeWorker → HTTP fetch → Operator → MQTT subscription
- **Longest initialization chain**

### Alternative Solutions Considered

1. **Asynchronous MQTT connection handling (IMPLEMENTED)**
   - Make `handleAggregation()` wait for connection before returning
   - Clean, proper async/await pattern
   - ✅ **Current solution**

2. **Longer wait time (ALSO IMPLEMENTED)**
   - Increased initialization wait from 10s to 15s
   - Additional safety margin
   - ✅ **Belt-and-suspenders approach**

3. **Health check endpoint polling (NOT NEEDED)**
   - Would poll orchestrator health until "ready" status
   - More complex, not necessary with proper async handling
   - ❌ Not implemented

### Verification
Run experiments and check Chunked success rate:
```bash
npm run build
npm run experiment:5-iterations
```

Expected:
```
Chunked Query Approach:
  Success rate: 100% (5/5)  ← FIXED
  Avg results per run: 4-9
```

Look for this log message confirming proper initialization:
```
[CHUNKED] Connected to MQTT broker
```
This now appears AFTER the connection is fully established.

### Future Improvements

1. **Add readiness probe**
   - Orchestrators expose `/ready` endpoint
   - Experiment waits for all orchestrators to be ready
   - More robust than fixed wait time

2. **Improve MQTT subscription confirmation**
   - Operators log when fully subscribed
   - Experiment script monitors logs for "ready" signals

3. **Reduce initialization complexity**
   - Simplify Chunked approach initialization chain
   - Make HTTP registration synchronous

## HTTP Fetch Warnings (Not an Issue)

### Symptoms
```
[APPROXIMATION ERROR] Could not fetch queries from HTTP server
[CHUNKED ERROR] Could not fetch queries from HTTP server
```

### Explanation
These are **expected and harmless** warnings. The operators try to fetch queries from the HTTP server for backward compatibility, then fall back to using locally-provided queries (which works correctly).

### Why It Happens
- Orchestrator passes queries via environment variables to BeeWorker
- Operator first tries HTTP fetch (legacy behavior)
- HTTP fetch fails (no queries registered yet)
- Operator falls back to `SUB_QUERIES` env var (current design)
- Everything works as intended

### Action Required
**None** - This is normal operation. The fallback mechanism works correctly.

---

**Last Updated:** December 16, 2024  
**Status:** ✅ FIXED - Proper async MQTT connection handling implemented  
**Success Rate:** Expected 100% (was 80% before fix)