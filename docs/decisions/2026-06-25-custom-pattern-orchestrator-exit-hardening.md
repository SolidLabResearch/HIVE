# Decision Record: Custom Pattern Orchestrator Exit Hardening

Status: Proposed

## Context
The custom-pattern benchmark pipeline previously did not immediately detect when the approach orchestrator process exited early due to errors or invalid states. This led to data publishers continuing to publish data until timeouts occurred, causing wasted CPU resources and slow error feedback. We need to validate the orchestrator exit state immediately and terminate all dependent publisher processes upon failure.

## Decision
Modify the close event handler of the approach orchestrator process in [run-custom-patterns-comparison.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js) to:
1. Retrieve the effective target window count from the run environment.
2. Check for successful exit code (0) and absence of kill signals.
3. Validate that [benchmark_window_cap_summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/benchmark_window_cap_summary.json) exists and is parseable.
4. Verify the summary has stoppedAfterTargetWindows set to true and emittedFinalWindowCount greater than or equal to the effective target windows.
5. If validation fails, trigger immediate process cleanup to kill the publisher process tree, fail the benchmark case, and record the exact exit code, signal, and validation failure details.

## Alternatives Considered

### Alternative 1: Periodic Log Polling
Implement a periodic polling timer to inspect the orchestrator output files and check for progress.
- Pros: Decoupled from process event listeners.
- Cons: Introduces polling latency, adds resource overhead, and does not immediately clean up the publisher on sudden crash exits.

### Alternative 2: Event-Driven validation on process close (Selected)
Utilize the close event handler of the orchestrator child process to validate the execution state and trigger cleanups immediately.
- Pros: Instantly responds to process termination, terminates the publisher process tree with zero latency, and provides a clear failure reason including exit codes and signals.
- Cons: Requires handling asynchronous process termination synchronization.

## Consequences
- Invalid orchestrator exits immediately stop the corresponding publisher, freeing resources.
- Hardened benchmark runs fail quickly, allowing faster debugging and retry progression.
- Exact failure details including exit code, signal, and summary diagnostics are persisted in failure logs.
