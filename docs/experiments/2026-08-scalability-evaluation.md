# 2026-08 Scalability Evaluation

This document defines the paper-facing scalability evaluation for Streaming Query Hive as of 2026-08-03. It replaces older ad hoc smoke matrices and must be treated as the authoritative contract for local 3-iteration smokes and final 35-iteration server runs.

## Shared Methodology

- Local smoke: 3 iterations per configuration, sequential by approach, no overlapping benchmark configurations.
- Final server run: 35 iterations per configuration, retain iterations 4-33, analyze 30 runs.
- Latency:
  - `queryToFirstResultMs = resultEmittedAt - queryRegisteredAt`
  - `registrationAnchoredWindowCloseAt = queryRegisteredAt + outputWindowRangeMs + ((windowNumber - 1) * outputWindowStepMs)`
  - `postWindowCloseLatencyMs = resultEmittedAt - registrationAnchoredWindowCloseAt`
  - For window 1, validate `queryToFirstResultMs - outputWindowRangeMs ~= postWindowCloseLatencyMs`.
- Resource scope:
  - summed live process-tree RSS in MiB
  - process-tree CPU
  - orchestrator as root
  - publisher excluded consistently
  - MQTT broker excluded consistently
  - same sampling interval across approaches
  - record peak process count, process topology, worker count, query executions, and delivery subscribers
- Correctness:
  - one shared deterministic replay and event-time anchor per experiment/configuration/iteration
  - compare only matching relative replay slice, event-time window, aggregation, target set, and iteration
  - classify results as `EXACT`, `NUMERICALLY EQUIVALENT`, `NON-EXACT`, or `NOT COMPARABLE`
  - predefined numerical-equivalence tolerance for chunk-state reconstruction: absolute error `<= 1e-3`
  - exact-final reuse in S1 must remain `EXACT`; approximation is reported via MAE, max absolute error, and percentage error where valid
- Reproducibility:
  - capture branch, commit, `git status`, environment, Node/npm versions, OS, input checksums, RSP-JS revision, timestamps, failures, checkpoints, raw output, and analysis output
  - a checkpoint is valid only when execution reports success and required artifacts validate

## S1: Equivalent-Query Concurrency

- Research question: How does final-result reuse scale as the number of concurrently registered equivalent consumer queries increases?
- Independent variable: `K = 1, 2, 4, 8, 32`
- Fixed variables:
  - same input streams
  - same aggregation
  - same window range
  - same window step
  - same final query result
- Approaches:
  - Fetching
  - Streaming Query Hive exact-final-result reuse
  - Approximation as a separate reference, not as proof of exact-final reuse
- Hypothesis: final-result reuse keeps CPU, latency, and process-tree RSS approximately stable as `K` increases, while Fetching grows with `K`.
- Metrics:
  - query executions
  - final-result subscribers registered
  - exact deliveries out of `K`
  - query-to-first-result latency
  - post-window-close latency
  - process-tree CPU and RSS
  - process count and worker topology
- Correctness criteria:
  - Fetching must execute `K` queries
  - exact-final reuse must execute one shared query and deliver to `K` real subscribers
  - all `K` deliveries must be recorded; no symlinked or copied output files count as evidence
  - all delivered values must be exact
- Local smoke matrix:
  - runner: `experiments/k-scaling/run-k-scaling-3approach-local-smoke.js`
  - default: 3 iterations across `K = 1,2,4,8,32`
- Server matrix:
  - runner: `experiments/k-scaling/run-k-scaling-3approach-paper.js`
  - 35 iterations across `K = 1,2,4,8,32`

## S2: Same Data, Different Windows

- Research question: How does chunk-state reuse scale when consumers query the same data and aggregation using different compatible window ranges?
- Independent variable: `superquery range = 120, 180, 240, 300, 360, 420 seconds`
- Fixed variables:
  - window step `= 60 seconds`
  - base chunk range `= 60 seconds`
  - base chunk step `= 30 seconds`
  - aggregation `= AVG`
  - same input streams
  - same deterministic replay
- Approaches:
  - Fetching
  - Approximation
  - Chunked
- Hypothesis: the fixed coordination cost of Chunked is amortized as the requested window grows, leading to lower memory and eventually lower CPU than Fetching.
- Metrics:
  - first complete target window latency
  - post-window-close latency
  - process-tree CPU and RSS
  - chunks consumed per emitted result
  - expected chunks per result
  - MAE, max absolute error, and percentage error for Approximation
- Correctness criteria:
  - same event-time anchor across approaches for a configuration/iteration
  - identical target window boundaries for comparisons
  - Chunked must be numerically equivalent within `1e-3`
  - Approximation is allowed to be non-exact and must report error metrics explicitly
- Local smoke matrix:
  - runner: `experiments/window-parameter-sensitivity/run-window-parameter-local-smoke.js`
  - command focus: `--experiment superquery-range-scaling --ranges 120,180,240,300,360,420`
- Server matrix:
  - runner: `experiments/window-parameter-sensitivity/run-window-parameter-paper.js`
  - command focus: `--experiment superquery-range-scaling --ranges 120,180,240,300,360,420`

## S3: Different Targets, Same Window

- Research question: How does chunk-state reuse scale as the number of distinct targets in a superquery increases while the window configuration remains fixed?
- Independent variable: `T = 2, 4, 6, 8 distinct targets`
- Fixed variables:
  - output range `= 120 seconds`
  - output step `= 60 seconds`
  - base chunk range `= 60 seconds`
  - base chunk step `= 30 seconds`
  - aggregation `= AVG`
  - input rate and replay duration
- Approaches:
  - Fetching
  - Approximation
  - Chunked
- Hypothesis: Chunked has fixed overhead at small `T`, but reuse becomes beneficial as superquery width increases, eventually reducing both CPU and memory relative to Fetching.
- Metrics:
  - exact distinct target count processed
  - query executions
  - process-tree CPU and RSS
  - first complete target window latency
  - Chunked exactness / numerical equivalence
  - Approximation MAE, max absolute error, and percentage error
- Correctness criteria:
  - targets must be operationally distinct by identifier, binding, topic, and selected value/property
  - do not implement scaling as repeated copies of the same target
  - current repo supports real-target `T=2`; larger `T` values use distinct synthetic targets until more real targets are wired
- Local smoke matrix:
  - runner: `experiments/window-parameter-sensitivity/run-window-parameter-local-smoke.js`
  - command focus: `--experiment query-target-scaling --target-source synthetic --target-counts 2,4,6,8`
- Server matrix:
  - runner: `experiments/window-parameter-sensitivity/run-window-parameter-paper.js`
  - command focus: `--experiment query-target-scaling --target-source synthetic --target-counts 2,4,6,8`

## Current Implementation Mapping

- S1 implementation path:
  - runner: `experiments/k-scaling/run-k-scaling-3approach-local-smoke.js`
  - analyzer: `experiments/k-scaling/analyze-k-scaling-3approach-local-smoke.js`
- S2 and S3 implementation path:
  - runner: `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
  - extractor: `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`
  - standard wrappers:
    - `experiments/window-parameter-sensitivity/run-window-parameter-local-smoke.js`
    - `experiments/window-parameter-sensitivity/run-window-parameter-paper.js`
