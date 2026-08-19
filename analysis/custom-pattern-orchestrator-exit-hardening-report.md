# Custom Pattern Orchestrator Exit Hardening Report

Status: Verified

## Context
The custom-pattern benchmark runner previously failed to immediately detect when an approach orchestrator exited early. Data publishers continued to publish events, resulting in wasted CPU cycles and delayed error feedback. This report documents the implementation and validation of the exit-hardening logic in [run-custom-patterns-comparison.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js).

## Implementation Details
The runner's child process close event handler was hardened to perform immediate verification:
1. Retrieve the effective target window count from the run environment instead of using a hardcoded value.
2. Verify that the approach orchestrator process exited with code 0 and signal null.
3. Check that [benchmark_window_cap_summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/benchmark_window_cap_summary.json) was successfully created and is parseable.
4. Verify that stoppedAfterTargetWindows: true is recorded and that emittedFinalWindowCount is greater than or equal to the target window count.
5. If any validation step fails, immediately terminate the publisher process tree and finalize the case as failed.

## Happy Path Validation
A smoke run was executed with target window count: 5 and iterations: 1 for both fetching and approximation approaches.

- Command: PAPER_BENCHMARK_SMOKE=1 STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability --iterations 1
- Fetching approach: Completed 5 windows cleanly in 363.0 seconds. The close handler detected the clean exit and immediately terminated the publisher cleanly.
- Approximation approach: Completed 5 windows cleanly in 542.4 seconds. The close handler detected the clean exit and finalized the run.
- Final summary: Verified that the results for both approaches were correctly extracted and written to [custom_pattern_comparison_summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/custom_pattern_comparison_summary.json).

## Negative Path Validation
To verify early exit detection and immediate publisher cleanup, a crash simulation was executed.

- Simulation Method: Configured a temporary simulator orchestrator that starts the run, allows the publisher to spawn after 2 seconds, and then exits with code 5 after 4 seconds of execution.
- Command: TEST_SIMULATE_ORCHESTRATOR_CRASH=1 PAPER_BENCHMARK_SMOKE=1 STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 CUSTOM_PATTERN_SELECTED_APPROACHES=fetching node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability --iterations 1
- Execution Metrics:
  - Orchestrator spawned: pid 78057
  - Publisher spawned: pid 78102 (2 seconds later)
  - Orchestrator exited: code 5, signal null (4.05 seconds elapsed)
- Hardened Close Handler Action:
  - Detected invalid early exit: code 5, signal null (missing benchmark_window_cap_summary.json)
  - Triggered immediate cleanup: sent SIGTERM to publisher process tree (pid 78102)
  - Publisher closed cleanly under SIGTERM within milliseconds
  - Case finalized as failed with reason: Approach process exited early and invalidly: exit code 5, signal null
  - Total test run duration: 4.1 seconds (instead of running to the 11-minute timeout)

## Conclusion
The orchestrator exit-hardening validation is complete. The custom-pattern benchmark pipeline is now robust against early exits and ready for full 35-window paper runs.
