# Decision Record: K-Scaling Process Cleanup and Run Validation

Status: Proposed

## Context
During K-scaling benchmark runs, process execution must not leave orphan processes (such as orchestrators, publishers, or BeeWorker instances) alive. Node process cleanup must execute asynchronously and handle process signals (SIGINT, SIGTERM) and fatal exceptions (uncaughtException, unhandledRejection) robustly, without relying on Node's exit event for asynchronous execution. Additionally, success and failure classification must verify clean completion and fail if processes were terminated via SIGKILL or if expected result files and counts are missing.

## Decision
1. Implement process group management for child processes spawned by the runner, and kill the whole group using negative PIDs in a dedicated [terminateProcessTree](file:///Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/run-k-scaling-comparison.js) helper.
2. If `SIGKILL` was required to terminate a process group after the grace period, mark the run as failed.
3. Update [IStreamQueryOperator](file:///Users/kushbisen/Code/streaming-query-hive/src/util/Interfaces.ts) and [BeeWorker](file:///Users/kushbisen/Code/streaming-query-hive/src/services/BeeWorker.ts) lifecycles to support async cleanup (returning `Promise<void>`).
4. Update orchestrator entrypoints ([StreamingQueryFetchingKScalingOrchestrator.ts](file:///Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingKScalingOrchestrator.ts) and [StreamingQueryChunkedKScalingOrchestrator.ts](file:///Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts)) to catch SIGINT, SIGTERM, uncaughtException, and unhandledRejection to run the async shutdown sequence before calling `process.exit()`.
5. Incorporate stale process checks (using `pgrep`) and fallback pkill commands, failing the run if stale processes cannot be cleared.
6. Write run status and error info into `resource_summary.json` for validation and post-extraction.

## Alternatives Considered

### Alternative 1: Rely on Node process exit event for cleanup
Perform the cleanup of MQTT clients, servers, and workers in the exit event.
- Pros: Simple handler setup.
- Cons: Node does not execute asynchronous tasks (such as event loops, socket writes, or child process signal handlers) during the exit event loop tick, leading to silent connection leaks and orphans.

### Alternative 2: Manual child process tracking and manual SIGKILL fallback (Selected)
Register robust handlers on SIGINT/SIGTERM/exceptions in the orchestrators to trigger async cleanup, and use detached process groups with SIGTERM/SIGKILL signals in the runner.
- Pros: Guaranteed cleanup of the process tree, correct detection of failures, and no orphan processes left alive.
- Cons: Increases orchestrator complexity slightly.

## Consequences
- No orphan processes will remain after a benchmark run completes or fails.
- Failed runs will be correctly detected and flagged in the summary artifacts rather than reporting false completion.
- Stale process state will not leak into subsequent runs.
