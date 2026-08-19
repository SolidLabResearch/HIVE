# Tier 1 / Tier 2 Paper Experiment Readiness Audit

Date: 2026-06-25
Repo: `streaming-query-hive`
Scope: static audit only; no benchmark reruns; no code changes

## Goal

Audit whether the semantic-ready chunked full-window fix and the paper-ready methodology are wired into every Tier 1 and Tier 2 paper experiment family, not just real-data.

Families checked:

1. real-data core
2. custom patterns
3. sub-window / window-parameter sensitivity
4. aggregation variations
5. frequency variations
6. K-scaling / scalability

## Executive Summary

- `experiments/real-data-comparison/run-real-data-4-approaches.js` is the only Tier 1/Tier 2 family runner clearly wired to the current paper-ready method end to end.
- `experiments/pattern-analysis/run-custom-patterns-comparison.js` uses modern replay/process-tree/MQTT plumbing, but it still inherits chunked comparable-output defaults from `experiments/utils/benchmarkReplayEnv.js` because it does not override the chunked mode locally.
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js` and `experiments/window-parameter-sensitivity/common.js` are partly modernized:
  - they already use process-tree CPU-seconds,
  - their extractor already reports matched-window MAE/MAPE/RMSE and ready-to-emit metrics,
  - but they do not yet wire the paper-ready 35-window / trim-4..33 methodology,
  - and they do not explicitly disable chunked comparable-output mode.
- `experiments/k-scaling/run-k-scaling-comparison.js` is only partially aligned:
  - it uses process-tree CPU-seconds collection,
  - but it does not explicitly opt into semantic-ready chunked mode or the paper-ready window/trim/payload method.
- `scripts/benchmark/run-scalability-benchmarks.js` is not paper-ready:
  - it explicitly forces `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1`,
  - it computes latency as `result_emitted_at - expected_window_close`,
  - it uses old-style resource CSV CPU% rather than the process-tree sampler summary,
  - and it has no 35-window / trim-4..33 paper method.
- Frequency experiments are still on older standalone scripts and are not wired to the current paper-ready method.
- Legacy batch wrappers in `experiments/run-experiments-part*.sh` route through `experiments/unified-benchmark.js`, which is also legacy and should not be treated as the paper pipeline.

## Shared Patch Surface

### Canonical paper-ready pieces already present

- Real-data runner:
  - `experiments/real-data-comparison/run-real-data-4-approaches.js`
- Shared accuracy comparison:
  - `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- Shared process-tree CPU-seconds sampler:
  - `experiments/utils/processTreeMetrics.js`

### Main shared risk

- `experiments/utils/benchmarkReplayEnv.js` still defaults chunked to:
  - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1`
  - `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
  - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
- Any family that only calls `withBenchmarkReplayEnv(...)` and does not locally override `CHUNKED_COMPARABLE_OUTPUT_ONLY` is still on the stale comparable-output path.

## Family Matrix

| Family | Runner script actually used | Completed-window approximation | Compact reusable payload | Chunked semantic-ready immediate mode | Full external-window completeness before emit | 35 output windows and trim 4..33 | Latency domain-safe | CPU uses process-tree CPU-seconds | MQTT bytes/counts reported | Accuracy uses matched windows with MAE/MAPE/RMSE | Completeness reports missing/extra windows | Status | Exact patch points |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Real-data core | `experiments/real-data-comparison/run-real-data-4-approaches.js` | Yes | Yes | Yes | Yes, by current real-data semantic-ready path intent | Yes | Yes | Yes | Yes | Yes | Yes | Ready | none for this audit |
| Custom patterns | `experiments/pattern-analysis/run-custom-patterns-comparison.js` plus `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Yes via shared replay helper default | No explicit local wiring | No explicit local override; inherits comparable-output default | Not confirmed on final semantic-ready full-window path | Runner has 35 iterations, but paper trim is only in outer analysis flow, not in the runner | Partly; outer paper summary logic is modern, runner-local extraction path is stale | Yes | Yes | Yes in shared post-analysis | Yes in shared post-analysis | Partial | `experiments/pattern-analysis/run-custom-patterns-comparison.js`, optionally `experiments/utils/benchmarkReplayEnv.js` if changed centrally |
| Sub-window / window-parameter sensitivity | `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js` plus extractor | No explicit paper-mode override; only generic defaults | No | No explicit local override; likely inherits comparable-output default | Not confirmed; current scenario env only forces immediate trigger | No | Partly; extractor uses `delay_past_expected_close_ms` and `ready_to_emit_ms`, but not real-data style domain normalization | Yes | No full MQTT bytes/counts; only profile counter `mqtt_clients_created` | Yes | No explicit missing/extra reporting; only matched count | Partial | `experiments/window-parameter-sensitivity/common.js`, `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`, extractor if paper summary fields are needed |
| Aggregation variations | Legacy shell wrappers `experiments/run-experiments-part2.sh` and `experiments/run-experiments-part3.sh` route to `experiments/unified-benchmark.js`; modern sensitivity harness can accept `--aggregation` but is not the current paper entrypoint | Legacy path: No | Legacy path: No | Legacy path: No | Legacy path: No | Legacy path: No | Legacy path: No | Legacy path: No | Legacy path: only ad hoc MQTT summary | Legacy path: baseline comparison exists but not paper-ready | Legacy path: No | Not ready | Replace legacy runner choice with `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js --aggregation ...`; do not use `unified-benchmark.js` |
| Frequency variations | `experiments/frequency-comparison/run-frequency-comparison-experiments.js`, `run-all-approaches-comparison.js`, `run-comparison-table.js` | No | No | No explicit paper-ready chunked override found | No | No | No; uses mixed latency columns across approaches | Mixed; one report reconstructs CPU-seconds from legacy tree CSV, but the family is still legacy overall | No consistent paper-ready MQTT summary | Not consistently; some scripts report error metrics, but not on the current matched-window paper path | No | Not ready | New family-level patch should target the actual four-approach frequency runner, not the old comparison-table scripts |
| K-scaling | `experiments/k-scaling/run-k-scaling-comparison.js` | No explicit local override | No | Only `CHUNKED_USE_IMMEDIATE_TRIGGER`; no explicit disable for comparable-output mode | Not confirmed | No | No evidence of domain-safe normalization in current audit | Yes for collection | No clear paper-ready MQTT summary in inspected path | Not confirmed in inspected path | Not confirmed | Partial / not paper-ready | `experiments/k-scaling/run-k-scaling-comparison.js` |
| Scalability (`same_query_different_windows`) | `scripts/benchmark/run-scalability-benchmarks.js` | No explicit approximation completed-window override | No | No; explicitly forces comparable-output mode | No | No | No; latency normalized as `result_emitted_at - expected_window_close` only | No; uses `computeResourceMetrics` from legacy resource CSV, not process-tree summary | Yes | Yes for MAE/MAPE/RMSE | No explicit missing/extra summary | Not ready | `scripts/benchmark/run-scalability-benchmarks.js` |

## Family Notes

### 1. Real-data core

Runner:

- `experiments/real-data-comparison/run-real-data-4-approaches.js`

Current wiring:

- sets `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- uses default replay `4 Hz`
- sets `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
- sets `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
- sets:
  - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
  - `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
  - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
- normalizes latency rows with domain awareness
- reports:
  - CPU-seconds from process-tree logs
  - MQTT bytes and counts
  - matched-window completeness
  - missing/extra windows
  - MAE/MAPE/RMSE
- trims analysis to windows `4..33`

Assessment:

- This is the reference implementation for the paper-ready methodology.

### 2. Custom patterns

Runner:

- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- post-analysis for paper summaries:
  - `analysis/accuracy/accuracy-comparison-custom-patterns.js`

What is already modern:

- uses `createBenchmarkReplayRunEnv(...)`
- uses process-tree sampler from `experiments/utils/processTreeMetrics.js`
- finalizes MQTT artifacts
- shared post-analysis already computes:
  - matched windows
  - missing/extra counts
  - MAE/MAPE/RMSE

Gaps:

- runner-local env build does not explicitly set:
  - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
  - `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
  - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
  - `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
  - `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- because `benchmarkReplayEnv` defaults comparable-output to `1`, chunked patterns likely still run on the stale mode unless the environment overrides it externally
- the runner’s own extraction path (`extract-pattern-results.js`) is older than the current paper summary path

Assessment:

- The analysis side is reusable.
- The run configuration is not yet guaranteed paper-ready.

### 3. Sub-window / window-parameter sensitivity

Runner:

- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
- shared scenario builder:
  - `experiments/window-parameter-sensitivity/common.js`
- extractor:
  - `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`

What is already modern:

- uses process-tree CPU-seconds sampler
- extractor computes:
  - `mean_window_adjusted_latency_ms`
  - `mean_ready_to_emit_ms`
  - `mean_computation_ms`
  - `mae`
  - `rmse`
  - `mape`
  - `matched_window_count`
- extractor validates chunked exactness tolerance and detects fallback/original-agent leakage

Gaps:

- scenario env in `common.js` only guarantees:
  - finite replay
  - deterministic timestamp domain
  - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=true`
- it does not explicitly set:
  - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
  - `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
  - `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
  - `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
  - `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- extractor does not currently report paper-style missing/extra window counts
- no built-in trim `4..33`
- no MQTT bytes/counts summary in the extractor output

Assessment:

- Good base harness.
- Not yet wired to the real-data paper method.

### 4. Aggregation variations

Observed runner paths:

- legacy batch entrypoints:
  - `experiments/run-experiments-part2.sh`
  - `experiments/run-experiments-part3.sh`
- both route to:
  - `experiments/unified-benchmark.js`

Modern alternative already in repo:

- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js` accepts `--aggregation`
- `experiments/window-parameter-sensitivity/common.js` threads `AGGREGATION_FUNCTION`

Assessment:

- The currently documented batch aggregation path is legacy and not paper-ready.
- The minimal paper-ready direction is to reuse the window-parameter-sensitivity harness for aggregation variants instead of reviving `unified-benchmark.js`.

### 5. Frequency variations

Observed scripts:

- `experiments/frequency-comparison/run-frequency-comparison-experiments.js`
- `experiments/frequency-comparison/run-all-approaches-comparison.js`
- `experiments/frequency-comparison/run-comparison-table.js`
- multiple per-approach `experiment-frequency-*.js` scripts

Why this family is not paper-ready:

- no evidence of:
  - target windows `1..35`
  - trim `4..33`
  - compact reusable payload
  - completed-window approximation mode
  - semantic-ready chunked full-window completeness
- `run-comparison-table.js` mixes latency domains:
  - fetching/naive use `latency_from_last_obs_ms`
  - approximation uses `latency_from_last_data_ms`
  - chunked uses `computation_ms`
- CPU-seconds are reconstructed from legacy tree CSV only in one report helper, not as a consistent family method
- MQTT reporting is ad hoc or absent

Assessment:

- This family is still on older experimental infrastructure and should not be considered wired to the paper-ready methodology.

### 6. K-scaling

Runner:

- `experiments/k-scaling/run-k-scaling-comparison.js`

What is already modern:

- uses process-tree CPU-seconds sampler
- records `tree_cpu_seconds` in resource logs
- uses `createBenchmarkReplayRunEnv(...)`

Gaps:

- only explicitly sets `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER`
- does not explicitly disable comparable-output mode
- does not explicitly enable compact payload or completed-window approximation mode
- no evidence in the inspected path of:
  - 35 target windows
  - trim `4..33`
  - domain-safe latency normalization
  - paper-style completeness summary
  - MQTT traffic summary rollup

Assessment:

- Instrumentation base is good.
- Paper-ready methodology is not fully wired.

### 7. Scalability (`same_query_different_windows`)

Runner:

- `scripts/benchmark/run-scalability-benchmarks.js`

What it already does:

- computes matched-window MAE/RMSE/MAPE against fetching baseline
- finalizes MQTT traffic artifacts

Why it is not paper-ready:

- `createEnv()` explicitly sets:
  - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1`
  - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
- that means chunked is still on comparable-output mode, not semantic-ready immediate mode
- latency is normalized to `window_end_latency_ms = result_emitted_at - expected_window_close`
- resource metrics come from `computeResourceMetrics(resourceCsvPath)` using old per-process CPU deltas / CPU%, not the process-tree CPU-seconds summary
- no paper window cap / trim configuration
- no explicit missing/extra completeness summary

Assessment:

- This is the clearest non-real-data script still pinned to the obsolete chunked mode.

## Which scripts are legacy or obsolete for paper work

These should not be treated as the current paper pipeline:

- `experiments/unified-benchmark.js`
  - old benchmark umbrella
  - CPU% oriented
  - not paper-ready for chunked semantic mode
- `experiments/run-experiments-part1.sh` through `experiments/run-experiments-part6.sh`
  - wrappers around `unified-benchmark.js`
- `experiments/analyze-results.js`
  - legacy post-analysis for unified benchmark outputs
- `experiments/frequency-comparison/run-frequency-comparison-experiments.js`
  - old two-approach path
- `experiments/frequency-comparison/run-comparison-table.js`
  - mixed latency domains
  - not a paper-ready methodology script

## Dependency / Flow Diagram

```text
Tier 1 / Tier 2 family
  -> family runner
     -> experiments/utils/benchmarkReplayEnv.js
        -> injects finite replay / timestamp-domain defaults
        -> currently defaults chunked comparable-output-only = 1
     -> orchestrator(s) in dist/approaches/*
     -> publisher dist/streamer/src/publish.js
     -> logs + latency csv + window diagnostics + mqtt artifacts
     -> experiments/utils/processTreeMetrics.js
        -> process-tree CPU-seconds / RSS
     -> family analysis / extractor
        -> matched-window accuracy
        -> completeness
        -> latency summary
        -> resource summary

Current family mapping

real-data
  -> experiments/real-data-comparison/run-real-data-4-approaches.js
  -> analysis/accuracy/accuracy-comparison-custom-patterns.js
  -> scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js helpers

custom patterns
  -> experiments/pattern-analysis/run-custom-patterns-comparison.js
  -> analysis/accuracy/accuracy-comparison-custom-patterns.js

sub-window / aggregation / target-scaling sensitivity
  -> experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js
  -> experiments/window-parameter-sensitivity/common.js
  -> experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js

k-scaling
  -> experiments/k-scaling/run-k-scaling-comparison.js

scalability same_query_different_windows
  -> scripts/benchmark/run-scalability-benchmarks.js

frequency
  -> experiments/frequency-comparison/* legacy standalone scripts

legacy batch wrappers
  -> experiments/run-experiments-part*.sh
  -> experiments/unified-benchmark.js
  -> experiments/analyze-results.js
```

## Minimal Patch Plan Ranked By Risk

### Low risk

1. Custom patterns: mirror real-data runner env flags
   - patch `experiments/pattern-analysis/run-custom-patterns-comparison.js`
   - explicitly set:
     - `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
     - `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
     - `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
     - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
     - `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
     - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
   - keep outer analysis on shared matched-window comparison path

2. Window-parameter sensitivity family: wire paper env explicitly
   - patch `experiments/window-parameter-sensitivity/common.js`
   - add the same explicit paper env flags there
   - this covers:
     - sub-window sensitivity
     - aggregation variations if routed through this harness
     - target-scaling variant of that harness

3. Window-parameter sensitivity extractor: add paper completeness fields
   - patch `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`
   - add:
     - target-window expectation
     - missing-window count
     - extra-window count
     - optional trimmed-window stats for `4..33`

### Medium risk

4. K-scaling: align env and summaries with paper method
   - patch `experiments/k-scaling/run-k-scaling-comparison.js`
   - explicit paper env flags
   - add target window cap `35`
   - add trim-aware summary path
   - confirm accuracy/completeness path

5. Scalability same-query-different-windows: switch chunked out of comparable-output mode
   - patch `scripts/benchmark/run-scalability-benchmarks.js`
   - change chunked env to:
     - `CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
     - `CHUNKED_CADENCE_ONLY=0`
     - `CHUNKED_USE_IMMEDIATE_TRIGGER=1`
   - replace legacy resource CPU summary with process-tree CPU-seconds source
   - add completeness reporting

### Higher risk

6. Frequency family: consolidate onto a modern four-approach runner
   - current scripts are fragmented and methodology-inconsistent
   - do not patch `run-comparison-table.js` incrementally unless the paper explicitly depends on it
   - better patch target is the actual four-approach execution path, likely `run-all-approaches-comparison.js` or a new wrapper around modern shared helpers

### Cross-cutting decision

7. Decide whether to patch `experiments/utils/benchmarkReplayEnv.js`
   - changing the default comparable-output flag there would fix multiple families at once
   - risk: broad blast radius across non-paper scripts
   - safer approach:
     - keep shared default unchanged for now
     - explicitly override per paper-family runner

## Recommended Minimal Sequence

1. Custom patterns runner
2. Window-parameter-sensitivity shared scenario env
3. Window-parameter-sensitivity extractor completeness/trim fields
4. K-scaling runner
5. Scalability runner
6. Frequency family consolidation
7. Only after that, consider whether `benchmarkReplayEnv.js` should change globally

## Bottom Line

- Real-data is already wired.
- Custom patterns and window-parameter sensitivity are the nearest to ready; both mainly need explicit paper env wiring and paper completeness/window-cap reporting.
- K-scaling has the right CPU collection foundation but not the full paper method.
- `scripts/benchmark/run-scalability-benchmarks.js` and the frequency family are still visibly on older methodology.
- Legacy batch shell scripts and `unified-benchmark.js` should not be used as evidence that aggregation/frequency/scalability are already paper-ready.
