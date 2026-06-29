# Streaming Query Hive Paper Methodology Review

Scope: methodology only. This review uses the current repository state as evidence and does not propose code changes or benchmark execution changes.

## Executive View

The strongest paper methodology is a repeated-measures, window-matched evaluation with three layers:

1. Core steady-state evaluation on real data and custom patterns.
2. Separate cold-start / first-result studies, not mixed into steady-state claims.
3. Sensitivity studies for frequency, aggregation, sub-window settings, and scaling.

The paper should treat these as distinct experimental questions. The main claims should be supported by steady-state windows only, with latency and accuracy matched by actual window identity and resource usage measured with process-tree CPU-seconds.

## 1. Scientific Questions

The paper is really trying to answer these questions:

| Question | Why it matters |
| --- | --- |
| Can Streaming Query Hive preserve result correctness relative to fetching? | This is the core semantic claim. |
| Can it reduce compute and communication cost relative to fetching? | This is the efficiency claim, now best measured with CPU-seconds and MQTT traffic. |
| Does approximation provide an acceptable latency/accuracy tradeoff? | This is the main approximation claim. |
| Does chunked remain semantically comparable while changing execution structure? | This is the main chunked claim after the latency-domain fix. |
| Do the results hold across real data, synthetic patterns, and parameter sweeps? | This is the robustness claim. |
| How sensitive are the conclusions to frequency, aggregation, sub-window size, and query scaling? | This is the external-validity / sensitivity claim. |

The paper should not collapse all of these into a single average. They are different questions.

## 2. Metrics Actually Needed

The metrics that actually answer the questions are:

| Metric | Needed for | Primary? | Notes |
| --- | --- | --- | --- |
| Comparable window latency, anchored to the same semantic close point for all approaches | Latency comparison | Yes | Use the corrected latency-domain-safe form, not mixed event-time / wall-clock subtraction. |
| MAE / MAPE / RMSE against fetching | Accuracy comparison | Yes | Compute per matched window, then aggregate across steady-state windows and iterations. |
| Exact-match or near-exact rate | Accuracy comparison | Yes | Useful as an interpretable companion to MAE/MAPE/RMSE. |
| Matched window count, baseline-only count, approach-only count | Completeness / missingness | Yes | Needed so missing or extra windows are not silently ignored. |
| Process-tree CPU-seconds | Resource efficiency | Yes | This is the right replacement for `%CPU`. |
| Peak RSS / mean RSS | Resource efficiency | Yes | Memory matters as a secondary resource metric. |
| MQTT traffic bytes / message counts | Communication cost | Yes | Especially important for reuse/chunked claims. |
| Finalized window count / emitted result count | Normalization and completeness | Yes | Needed for per-window normalization and completeness checks. |
| Failure / timeout / termination reason | Reproducibility and validity | Yes | Needed to distinguish true behavior from aborted runs. |
| Time to first result / startup delay | Cold-start analysis | Only for a separate study | Do not mix this into steady-state latency. |
| `%CPU` | Diagnostic only | No | Keep only as a legacy debug field if needed. |
| Iteration1-only averages | Legacy only | No | Too brittle and not aligned with the current methodology. |

## 3. Metrics That Are Obsolete or Potentially Misleading

These should not be used as primary paper metrics:

| Metric / practice | Why it is misleading |
| --- | --- |
| `%CPU` as the main resource metric | It is cadence-sensitive and can undercount child workers or overstate teardown effects. |
| Launcher-only CPU usage | It ignores descendant workers and is not comparable across approaches. |
| Event-time close minus wall-clock result time without domain alignment | This mixes time domains and can create absurd latency values. |
| Averages over all 35 runs without trimming warmup/cooldown | This mixes startup, steady-state, and teardown behavior. |
| `iteration1`-only analysis | It is not statistically stable and can miss drift across repeated runs. |
| First fetching iteration as the only accuracy baseline | It is not as robust as a window-matched steady-state baseline over the common slice. |
| Raw result counts without completeness checks | Missing or extra windows can look like accuracy gains or losses when they are actually missing-data problems. |
| Wall-clock runtime alone | It is confounded by startup, shutdown, and scheduling noise. |

## 4. Cold-Start vs Steady-State

Yes, they should be separate experiments.

Reasoning:

- Cold-start answers a startup question: how quickly does the system become ready and emit its first valid results?
- Steady-state answers the paper’s core throughput / efficiency / accuracy question under sustained replay.
- Mixing them hides the very differences reviewers care about: initialization cost, child-process spin-up, and first-window convergence.

Recommended separation:

- Cold-start study: report time-to-first-result, readiness, and initial resource footprint.
- Steady-state study: analyze only the settled slice of the run, using windows 4..33.

## 5. Correct Experimental Unit

The experimental unit should be defined differently for each metric family.

| Metric family | Correct experimental unit | Why |
| --- | --- | --- |
| Latency | Matched output window within a run | Latency is a per-window phenomenon; the same window must be compared across approaches using the same semantic bounds. |
| Resource usage | Benchmark run / approach-case / iteration | CPU-seconds and RSS are cumulative over the run, so the run is the natural unit. Per-window normalization is a derived quantity, not the base unit. |
| Accuracy | Matched output window within a run | Accuracy is also per-window; the unit must be the window, matched by actual bounds or window identity. |

For paper reporting, the safest framing is:

- Window-level observations for latency and accuracy.
- Run-level observations for resource use.
- Repeated measures across runs and patterns for statistical summaries.

## 6. Strongest IJSEKE Methodology

The strongest methodology for an IJSEKE reviewer is:

1. Use a fixed, documented replay protocol for the main paper.
2. Evaluate the same approaches on the same inputs and the same window schedule.
3. Treat steady-state windows 4..33 as the main analysis slice.
4. Use 4 Hz as the default replay rate for the main evaluation.
5. Compare fetching, approximation, chunked, and naive distributed under identical conditions.
6. Measure latency with the corrected semantic anchor, not with mixed-domain arithmetic.
7. Measure resource use with process-tree CPU-seconds, RSS, and MQTT traffic.
8. Measure accuracy window-by-window against fetching, and report completeness / missingness explicitly.
9. Keep cold-start separate so startup effects do not contaminate steady-state claims.
10. Present sensitivity studies as secondary evidence, not as the core result.

This is stronger than a simple mean-over-runs evaluation because it is:

- window-matched,
- repeatable,
- semantically aligned,
- resource-complete, and
- explicit about missingness.

## 7. Existing Outputs That Already Support This Methodology

These existing outputs already support most of the methodology:

| Output / script | What it supports | Methodology coverage |
| --- | --- | --- |
| [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) | Orchestrates real-data and custom-pattern paper runs, preserving `summary.raw-35.json` and `summary.trimmed-4-33.json` / `summary.trimmed-4-33.csv` | Core steady-state paper matrix |
| [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) | Window-matched accuracy, completeness counts, and the raw/trimmed summary files used by the paper runner | Accuracy and completeness |
| [`scripts/analysis-js/process-tree-resource-sampler.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/process-tree-resource-sampler.js) and [`experiments/utils/processTreeMetrics.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/processTreeMetrics.js) | Canonical process-tree CPU-seconds and RSS | Resource usage |
| [`scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js) | Corrected latency-domain-safe comparison, CPU-seconds, MQTT traffic, completeness, per-window diagnostics | Latency methodology validation |
| [`scripts/benchmark/validate-process-tree-cpu.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/validate-process-tree-cpu.js) | Explicit validation that CPU-seconds is the correct replacement for `%CPU` | Resource methodology validation |
| [`scripts/benchmark/run-scalability-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-scalability-benchmarks.js) | Frequency / scale / approach matrix with latency, accuracy, MQTT traffic | Sensitivity and scalability |
| [`experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js) | CPU-seconds, latency distributions, resource summaries | Parameter sensitivity |
| [`experiments/k-scaling/extract-k-scaling-results.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/extract-k-scaling-results.js) | CPU-seconds, latency, accuracy, MQTT traffic, reuse counters | Scaling and reuse claims |
| [`analysis/latency-runtime-fix-report.md`](/Users/kushbisen/Code/streaming-query-hive/analysis/latency-runtime-fix-report.md) | Documents the latency-domain fix and why mixed-domain values are invalid | Latency correctness rationale |

## 8. Outputs That Cannot Support This Methodology

These outputs are insufficient for the final paper methodology on their own:

| Output family | Why it cannot support the methodology |
| --- | --- |
| Legacy `iteration1` analysis scripts in `analysis/accuracy/` | They are too narrow and do not provide the repeated-measures, trimmed steady-state view needed for the paper. |
| `%CPU`-centric resource summaries | They are not process-tree complete and are sensitive to process boundaries and teardown. |
| Latency outputs based on raw event-time subtraction | They can mix time domains and produce invalid values. |
| Single-run debug helpers in `scripts/analysis-js/` | They are useful for diagnostics but not for the final methodology. |
| Real-data summary outputs that only preserve one baseline iteration | They do not expose the same robust window-matched comparison that the steady-state paper method needs. |
| Outputs without completeness tracking | They cannot distinguish “slower” from “missing”. |

## 9. Final Benchmark Matrix Before Submission

The final submission should have a two-level matrix:

### Core paper matrix

| Workload | Approaches | Protocol | Primary metrics |
| --- | --- | --- | --- |
| Real data | fetching, approximation, chunked, naive distributed | 35 output windows per case, analyze windows 4..33, 4 Hz default replay | Latency, accuracy, CPU-seconds, RSS, MQTT traffic, completeness |
| Custom patterns: low variability | fetching, approximation, chunked, naive distributed | Same as above | Same as above |
| Custom patterns: step | fetching, approximation, chunked, naive distributed | Same as above | Same as above |
| Custom patterns: spike | fetching, approximation, chunked, naive distributed | Same as above | Same as above |
| Custom patterns: low-frequency oscillation | fetching, approximation, chunked, naive distributed | Same as above | Same as above |
| Custom patterns: high-frequency oscillation | fetching, approximation, chunked, naive distributed | Same as above | Same as above |

### Separate cold-start matrix

| Workload | Approaches | Protocol | Primary metrics |
| --- | --- | --- | --- |
| Cold-start / first-result fixture | fetching, approximation, chunked | Separate short-run protocol, not mixed with steady-state | Time-to-first-result, readiness, startup CPU-seconds, startup MQTT traffic |

### Sensitivity / appendix matrix

| Axis | Values |
| --- | --- |
| Frequency | 0.1, 0.5, 1.0, 1.5, 2.0 Hz |
| Aggregation | `AVG` for the core paper; any additional supported aggregations only if a sensitivity appendix is explicitly included |
| Sub-window range / step | 60000 / 30000 as the paper default, plus targeted variants if the appendix needs them |
| Scaling | Same-query-different-windows and K-scaling style studies |

## Bottom Line

For the paper, the methodology should be:

- steady-state first,
- window-matched,
- process-tree based for resources,
- latency-domain safe,
- completeness-aware,
- and statistically repeated.

That is the strongest defensible methodology currently supported by the repository’s existing outputs.
