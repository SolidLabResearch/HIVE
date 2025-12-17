# Orchestrator Lifecycle Fix

## Problem Summary

The orchestrators were exiting immediately after spawning BeeWorker child processes, resulting in 0 results being collected from all approaches during experiments.

## Root Cause

The `runExperiment()` method in all three orchestrators was completing and returning immediately after calling `runRegisteredQuery()`, which only spawned a BeeWorker child process but didn't wait for results. The sequence was:

1. Orchestrator's `runExperiment()` sets up queries
2. `runRegisteredQuery()` spawns BeeWorker child process (non-blocking)
3. `runExperiment()` returns immediately
4. In standalone mode, the setTimeout fires but the orchestrator has already completed setup
5. The parent orchestrator process exits
6. BeeWorker child process is orphaned/killed before it can process any data
7. No results are produced

### Timeline Evidence

From logs before fix:
```
1765884040034 - "Approximation Approach Orchestrator initialized"
1765884040035 - "Starting experiment"
1765884042069 - "Subqueries registered"
1765884042069 - "Query registered"
1765884042070 - "Experiment completed"  ← Only 2 seconds elapsed!
```

The orchestrator was "completing" in 2 seconds, but query windows require 60-120 seconds to produce results (RANGE 120000ms, STEP 60000ms).

## Solution

Modified all three orchestrators to keep the process alive indefinitely after spawning BeeWorker:

### Before (ApproximationApproachOrchestrator.ts)
```typescript
public async runExperiment(): Promise<any> {
  // ... setup code ...
  
  // Run the registered query
  const result = await this.orchestrator.runRegisteredQuery();
  
  console.log(`[ApproximationApproach] Experiment completed`);
  this.unifiedLogger.info("Experiment completed", { result });
  
  return result;  // ← Returns immediately
}
```

### After
```typescript
public async runExperiment(): Promise<any> {
  // ... setup code ...
  
  // Run the registered query (spawns BeeWorker child process)
  this.orchestrator.runRegisteredQuery();
  
  console.log(
    `[ApproximationApproach] Query execution started, keeping process alive...`,
  );
  this.unifiedLogger.info("Query execution started");
  
  // Keep the process alive indefinitely
  return new Promise(() => {
    // This promise never resolves, keeping the process alive
    // The experiment runner script will kill this process when appropriate
  });
}
```

### Standalone Runner Fix

Also updated the standalone execution functions to not await the experiment:

```typescript
async function runStandaloneApproximationApproach() {
  const orchestrator = new ApproximationApproachOrchestrator();

  try {
    // Start the experiment (this will keep running)
    orchestrator.runExperiment().catch((error) => {
      console.error("Error in orchestrator:", error);
      orchestrator.cleanup();
      process.exit(1);
    });

    // Set a timeout to exit after processing window completes
    const experimentDuration = 150000; // 2.5 minutes
    setTimeout(() => {
      console.log("Approximation approach processing completed, exiting...");
      orchestrator.cleanup();
      process.exit(0);
    }, experimentDuration);
  } catch (error) {
    // ... error handling ...
  }
}
```

## Files Modified

1. **src/approaches/ApproximationApproachOrchestrator.ts**
   - Modified `runExperiment()` to return a never-resolving Promise
   - Updated standalone runner to use fire-and-forget pattern with timeout
   - Increased timeout from 120s to 150s (2.5 minutes)

2. **src/approaches/ChunkedQueryApproachOrchestrator.ts**
   - Same changes as ApproximationApproachOrchestrator

3. **src/approaches/FetchingClientSideApproachOrchestrator.ts**
   - Same changes as ApproximationApproachOrchestrator

4. **scripts/test-orchestrator-lifecycle.ts** (NEW)
   - Created test script to verify orchestrators stay alive
   - Runs orchestrator for 30 seconds and verifies it doesn't exit prematurely

5. **package.json**
   - Added `test:orchestrator-lifecycle` script

## Why This Works

### In Experiment Runner Context
The experiment runner script (`run-5-iterations.ts`, etc.) spawns orchestrators as separate processes and explicitly kills them after the experiment duration. Now:

1. Orchestrator spawns and enters `runExperiment()`
2. BeeWorker child process is spawned
3. `runExperiment()` returns a never-resolving Promise, keeping the Node.js event loop active
4. Orchestrator stays alive processing data
5. Experiment runner kills the orchestrator after configured duration
6. BeeWorker has time to process windows and produce results

### In Standalone Context
When running orchestrators directly:

1. Orchestrator spawns and enters `runExperiment()`
2. BeeWorker child process is spawned
3. `runExperiment()` returns a never-resolving Promise
4. The setTimeout in standalone runner keeps the process alive for 150 seconds
5. BeeWorker has time to process data
6. Process exits gracefully after timeout

## Testing

Run the lifecycle test to verify orchestrators stay alive:
```bash
npm run test:orchestrator-lifecycle
```

Expected output:
```
[STATUS] Orchestrator still running... (5s elapsed)
[STATUS] Orchestrator still running... (10s elapsed)
...
[PASS] Orchestrator stayed alive for 30s
```

## Impact on Experiments

With this fix:
- Orchestrators stay alive for the full experiment duration
- BeeWorker processes have time to initialize and process data
- Query windows can complete their full cycles (60s and 120s)
- Results are properly collected and published to MQTT
- Experiment runner can collect results before terminating orchestrators

## HTTP Fetch Errors (Unrelated)

The HTTP fetch errors you were seeing (`Could not fetch queries from HTTP server at http://localhost:8080/fetchQueries`) are **expected behavior** and **not related to the 0 results issue**. 

The chunked and approximation approaches:
1. Try to fetch queries from HTTP server (compatibility with older design)
2. Fall back to using locally provided subqueries (the current design)
3. This fallback works correctly - the operator receives queries via the `SUB_QUERIES` environment variable

The fetching approach is the only one that truly relies on the HTTP query registry.

## Verification Steps

After rebuilding:
```bash
npm run build
npm run experiment:5-iterations
```

You should now see:
- Orchestrators staying alive during data replay
- Results being collected (non-zero counts)
- Orchestrators exiting gracefully after experiment completes