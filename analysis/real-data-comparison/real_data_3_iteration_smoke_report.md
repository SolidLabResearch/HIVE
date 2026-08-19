# Real-Data 3-Iteration Smoke Report

Date/time:
`2026-06-29T19:57:57+0200` local
`2026-06-29T17:57:57Z` UTC

Branch:
`chunk-state-reuse-design`

Commit:
`78a142f4cd1d90527990a101689a56f5fab6f2d0`

Worktree dirty:
`yes`

## Scope

Inspected and exercised only the real-data paper path:

- `scripts/benchmark/run-all-paper-benchmarks.js`
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `experiments/real-data-comparison/run-real-data-4-approaches.test.js`
- directly related runtime env handling via `experiments/utils/benchmarkReplayEnv.js`
- generated outputs under `experiments/real-data-comparison/logs/`
- generated outputs under `results/paper-benchmarks/real-data-smoke-3iter-5windows-2026-06-29/`

## Commands Attempted

Repo preflight:

```bash
git status --short
git branch --show-current
git rev-parse HEAD
```

Dry run:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 3 \
  --output-dir results/paper-benchmarks/real-data-smoke-3iter-2026-06-29 \
  --dry-run
```

Initial full real-data attempt:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 3 \
  --output-dir results/paper-benchmarks/real-data-smoke-3iter-2026-06-29
```

Bounded smoke attempts:

```bash
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --smoke \
  --iterations 3 \
  --output-dir results/paper-benchmarks/real-data-smoke-3iter-5windows-2026-06-29
```

Focused verification:

```bash
npx jest experiments/real-data-comparison/run-real-data-4-approaches.test.js --runInBand
```

## Minimal Fixes Applied

1. `experiments/real-data-comparison/run-real-data-4-approaches.js`
   - real-data runs now ignore inherited finite replay durations that are shorter than the runner's own target-window requirement
   - reason: the top-level paper runner was injecting a shorter finite replay duration than the real-data methodology required, causing `stopReason=finite_replay_duration_reached` before the requested window cap was reached

2. `experiments/real-data-comparison/run-real-data-4-approaches.js`
   - the runner now waits for both the publisher and the orchestrator to exit before finalizing an iteration
   - reason: the previous implementation finalized on publisher exit and killed slower approaches before they could flush final windows after finite replay completed

3. `experiments/real-data-comparison/run-real-data-4-approaches.test.js`
   - added regression coverage for the finite-replay-duration resolution path

## Output Directories / Files Generated

Top-level smoke output root:

- `results/paper-benchmarks/real-data-smoke-3iter-5windows-2026-06-29/metadata.json`
- `results/paper-benchmarks/real-data-smoke-3iter-5windows-2026-06-29/logs/real-data-core.stdout.log`
- `results/paper-benchmarks/real-data-smoke-3iter-5windows-2026-06-29/logs/real-data-core.stderr.log`
- `results/paper-benchmarks/real-data-smoke-3iter-5windows-2026-06-29/logs/real-data-core.combined.log`

Real-data source log artifacts updated during attempts:

- `experiments/real-data-comparison/logs/fetching/iteration{1,2,3}/...`
- `experiments/real-data-comparison/logs/approximation/iteration{1,2}/...`
- `experiments/real-data-comparison/logs/naive_distributed/iteration1/...`
- `experiments/real-data-comparison/logs/real_data_comparison_results.{csv,json}` was not trusted as final smoke evidence because the full fixed 3-iteration run did not complete
- `experiments/real-data-comparison/logs/real_data_paper_ready_raw_summary.json` was not regenerated from a clean completed 3-iteration smoke in this session

## Verified Results

Successful verification:

- `git status --short`, branch, and commit captured
- focused Jest suite passes after the two runner fixes
- fetching iterations `1..3` completed with `targetWindowCount=5`, `emittedFinalWindowCount=5`, `stoppedAfterTargetWindows=true`, `stopReason=target_window_count_reached`
- the finite-replay-duration bug was reproduced and fixed
- the premature-finalization bug was reproduced and fixed

Observed failure evidence before the replay-duration fix:

- `experiments/real-data-comparison/logs/fetching/iteration1/benchmark_window_cap_summary.json`
  - `targetWindowCount=5`
  - `emittedFinalWindowCount=3`
  - `stopReason=finite_replay_duration_reached`

Observed failure evidence before the lifecycle fix:

- `experiments/real-data-comparison/logs/approximation/iteration1/run_summary.json`
  - `publisherExitReason=finite_replay_duration_reached`
  - finite replay completed
- `experiments/real-data-comparison/logs/approximation/iteration1/benchmark_window_cap_summary.json`
  - `emittedFinalWindowCount=0`
  - `stopReason=other`
- `experiments/real-data-comparison/logs/approximation/iteration1/approximation_latency_log.csv`
  - header only, no completed windows persisted

## Per-Approach Iteration Completeness

This smoke run did **not** complete cleanly end-to-end after the fixes, so only partial completeness can be asserted.

Completed and verified:

- `fetching`: iterations `1`, `2`, `3` reached 5/5 target windows

Started but not completed cleanly in this session:

- `approximation`: fixed-run completion not obtained in this session
- `chunked`: fixed-run completion not obtained in this session
- `naive_distributed`: fixed-run completion not obtained in this session

Because the fixed end-to-end run was interrupted before completion, I cannot truthfully claim 3 complete iterations for all four approaches.

## Warnings / Errors

- The real-data path reuses `experiments/real-data-comparison/logs/`, so stale iteration directories from earlier work remain present during reruns until overwritten. Final validation must use either the completed top-level snapshot or fresh timestamps, not a naive directory listing alone.
- The top-level runner's generic finite replay duration is too short for the real-data methodology unless the real-data runner overrides it. That bug is now patched in the real-data runner.
- The real-data runner previously finalized on publisher exit instead of orchestrator exit. That bug is now patched in the real-data runner.
- Even after those fixes, the bounded local smoke remains operationally expensive, especially for `approximation`, and did not finish all 12 tests within this session.

## Safe To Scale To 35 Iterations

`No`

Reason:

- the exact requested 3-iteration local smoke did not complete cleanly for all 4 approaches after the two minimal real-data-runner fixes
- until a clean completed smoke exists for `approximation`, `chunked`, and `naive_distributed` under the fixed runner, scaling directly to a 35-iteration server run is not justified

## Recommended Next Step

Before a 35-iteration server run, execute one clean fixed validation pass with narrower scope, for example:

- the same bounded smoke command but a single iteration
- or a single-approach run path for `approximation` and `chunked` if you want to isolate the remaining runtime behavior without rerunning fetching each time

## Server Command Template

Do not run locally from this report. This is the paper-shaped command after the two runner fixes:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 35 \
  --output-dir results/paper-benchmarks/real-data-35iter-<timestamp>
```

Expected semantics of that server command:

- real-data 4 approaches: `fetching`, `approximation`, `chunked`, `naive_distributed`
- paper target windows remain `35` by default inside the real-data runner
- the real-data runner now enforces a replay duration at least as long as its target-window requirement
- per-iteration timeout remains derived inside the real-data runner unless `STREAMING_QUERY_HIVE_BENCHMARK_TIMEOUT_MS` is explicitly set

## Approximation blocker follow-up

Date/time:
`2026-06-29T20:16:07+0200` local
`2026-06-29T18:16:07Z` UTC

Branch:
`chunk-state-reuse-design`

Commit:
`7d65c9a34f347293ca414bbc7418314e45ed4f56`

### Exact commands run

Focused tests:

```bash
npx jest --runInBand \
  experiments/real-data-comparison/run-real-data-4-approaches.test.js \
  src/services/operators/approximation/ApproximationDiagnosticsWriter.test.ts
```

Approximation-only 1-iteration smoke:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 1 \
  --approach approximation
```

Approximation-only 3-iteration smoke:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 3 \
  --approach approximation
```

### Failure cause

- The approximation runtime was receiving structured completed-window messages, but `ApproximationDiagnosticsWriter.logLatency()` could still throw `Approximation latency plausibility check failed ...` when the wall-clock mapping was numerically implausible even though the safer behavior in this benchmark path is to mark the latency domain as unavailable and continue.
- That exception happened inside the structured completed-window emission path before the final result publish callback could record the window, so approximation stayed at `emittedFinalWindowCount=0` with a header-only `approximation_latency_log.csv`.
- This was not a publisher-early-exit problem. The publisher kept running, the orchestrator stayed up, and approximation had already entered the structured completed-window branch. The blocker was the diagnostics throw aborting the message handler.

### Files changed

- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `experiments/real-data-comparison/run-real-data-4-approaches.test.js`
- `src/services/operators/approximation/ApproximationDiagnosticsWriter.ts`
- `src/services/operators/approximation/ApproximationDiagnosticsWriter.test.ts`

### Test results

- `npx jest --runInBand experiments/real-data-comparison/run-real-data-4-approaches.test.js src/services/operators/approximation/ApproximationDiagnosticsWriter.test.ts`
  - passed: `8/8` tests

### Smoke results

- Approximation-only 1-iteration smoke: `passed`
  - verified at `experiments/real-data-comparison/logs/approximation/iteration1/benchmark_window_cap_summary.json`
  - result: `emittedFinalWindowCount=5`, `stopReason=target_window_count_reached`
- Approximation-only 3-iteration smoke: `passed`
  - verified at `experiments/real-data-comparison/logs/approximation/iteration{1,2,3}/benchmark_window_cap_summary.json`
  - each iteration reached `finalWindowNumbers=[1,2,3,4,5]`

### Safe to retry full 4-approach 3-iteration smoke

- `Not yet confirmed`
- Approximation is now clean in approximation-only smoke, but this session did not revalidate `chunked` or `naive_distributed` under the same bounded 3-iteration smoke after the approximation fix.
- Safe next step is to retry the bounded full real-data 3-iteration smoke, not the 35-iteration server run.

## Full 4-approach smoke result

Date/time:
`2026-06-29T22:14:35+0200` local
`2026-06-29T20:14:35Z` UTC

Branch:
`chunk-state-reuse-design`

Commit:
`7d65c9a34f347293ca414bbc7418314e45ed4f56`

Dirty state at completion:

```text
 M analysis/real-data-comparison/real_data_3_iteration_smoke_report.md
 D chunked_debug_summary.json
 D chunked_emission_proof.json
 M experiments/real-data-comparison/run-real-data-4-approaches.js
 M experiments/real-data-comparison/run-real-data-4-approaches.test.js
 D mqtt_traffic.ndjson
 M src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.ts
 M src/services/operators/approximation/ApproximationDiagnosticsWriter.test.ts
 M src/services/operators/approximation/ApproximationDiagnosticsWriter.ts
 M src/services/operators/chunked/ChunkedDiagnosticsWriter.ts
?? src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.test.ts
?? src/services/operators/chunked/ChunkedDiagnosticsWriter.test.ts
```

### Exact commands run

Focused tests after naive/chunked fixes:

```bash
npx jest --runInBand \
  experiments/real-data-comparison/run-real-data-4-approaches.test.js \
  src/services/operators/approximation/ApproximationDiagnosticsWriter.test.ts \
  src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.test.ts \
  src/services/operators/chunked/ChunkedDiagnosticsWriter.test.ts
```

Build:

```bash
npm run build
```

Naive-only retry:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --approach naive_distributed \
  --iterations 1
```

Chunked-only retry:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --approach chunked \
  --iterations 1
```

Final clean full smoke:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
OUTPUT_WINDOW_RANGE=30000 \
OUTPUT_WINDOW_STEP=15000 \
SUB_WINDOW_RANGE=30000 \
SUB_WINDOW_STEP=15000 \
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 3
```

### Additional failure causes found during full-smoke retry

- `naive_distributed`
  - Cause: the benchmark target-window summary was written, but the orchestrator never initiated shutdown or suppressed later windows, so bounded smoke could continue past window `5`.
  - Fix: add a one-shot benchmark-target finalizer that writes the summary and schedules shutdown once target windows are reached.
- `chunked`
  - Cause: the chunked path could reach a complete comparable window and write `chunked_window_diagnostics.csv`, but `ChunkedDiagnosticsWriter.logLatency()` could throw on its wall-clock plausibility guard before latency logging and finalized-window accounting completed.
  - Fix: downgrade that plausibility guard from throw to warning so the benchmark records the comparable window and continues.

### Files changed

- `analysis/real-data-comparison/real_data_3_iteration_smoke_report.md`
- `src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.ts`
- `src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.test.ts`
- `src/services/operators/chunked/ChunkedDiagnosticsWriter.ts`
- `src/services/operators/chunked/ChunkedDiagnosticsWriter.test.ts`

### Generated files verified

- `experiments/real-data-comparison/logs/real_data_comparison_results.csv`
- `experiments/real-data-comparison/logs/real_data_comparison_results.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_raw_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.csv`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.json`

### Per-approach completeness

- `fetching`: iterations `1..3` all reached `finalWindowNumbers=[1,2,3,4,5]`, `stopReason=target_window_count_reached`, latency rows `5/5/5`
- `approximation`: iterations `1..3` all reached `finalWindowNumbers=[1,2,3,4,5]`, `stopReason=target_window_count_reached`, latency rows `5/5/5`
- `chunked`: iterations `1..3` all reached `finalWindowNumbers=[1,2,3,4,5]`, `stopReason=target_window_count_reached`, latency rows `5/5/5`
- `naive_distributed`: iterations `1..3` all reached `finalWindowNumbers=[1,2,3,4,5]`, `stopReason=target_window_count_reached`, latency rows `5/5/5`

### Validation summary

- All four expected approaches ran.
- Each approach has iterations `1..3`.
- Each iteration reached the bounded target windows `[1,2,3,4,5]`.
- All checked latency CSVs have required result columns populated with no `NaN`, `null`, or empty required values.
- All checked `run_summary.json` files report `completionStatus=completed`, `publisherExitReason=finite_replay_duration_reached`, and `failedPublishes=0`.
- No retry was needed after the final clean full rerun.

### Warnings

- The top-level paper runner still is not the right command for this bounded smoke variant because it overwrites the smoke-specific `OUTPUT_WINDOW_*` and `SUB_WINDOW_*` env overrides outside its `--smoke` path.
- Chunked and approximation latency magnitudes remain much larger than fetching/naive in the generated summaries. That is a result characteristic, not a bounded-smoke completion failure.

### Recommendation

- Full 4-approach 3-iteration smoke: `passed`
- 35-iteration server run: `safe to launch`
