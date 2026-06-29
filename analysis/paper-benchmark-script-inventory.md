# Paper Benchmark Script Inventory

Date: 2026-06-24

Scope:
- `scripts/benchmark/`
- `scripts/analysis-js/`
- `experiments/`
- `analysis/accuracy/`
- existing summary generators
- process-tree/resource samplers
- custom-pattern, real-data, frequency, aggregation, sub-window, and naive-distributed runners

Method:
- Static inspection only
- No full benchmark runs
- Lightweight shell inspection only

## Executive Summary

There is already a usable paper-benchmark spine in place.

The current paper-ready path is:
1. `scripts/benchmark/run-all-paper-benchmarks.js`
2. `experiments/real-data-comparison/run-real-data-4-approaches.js`
3. `experiments/pattern-analysis/run-custom-patterns-comparison.js`
4. `analysis/accuracy/accuracy-comparison-custom-patterns.js`
5. `experiments/utils/processTreeMetrics.js`
6. `scripts/analysis-js/process-tree-resource-sampler.js`
7. `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`

The older frequency and early pattern-analysis families still exist, but most of them are not paper-ready. They still assume:
- single iteration or `iteration1`
- positional window matching instead of benchmark-window matching
- older resource files
- CPU% or heap-only summaries instead of CPU-seconds
- older latency derivation or log parsing

The minimal adaptation should be a narrow patch on the already-current paper path, not a rewrite.

## Direct Answers

### 1. Which scripts already run the paper evaluation suites?

Primary coordinators already in place:
- `scripts/benchmark/run-all-paper-benchmarks.js`
  - top-level paper suite runner
  - supports suites: `real-data`, `patterns`, `latency`, `resources`, `accuracy`, `naive-distributed`
  - snapshots outputs into `results/paper-benchmarks/...`
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
  - real-data paper runner for fetching, approximation, chunked, naive distributed
  - emits raw and trimmed paper-ready summaries
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
  - custom-pattern benchmark runner with retries, per-attempt directories, resource summaries, MQTT summaries
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
  - paper-style custom-pattern accuracy aggregation against fetching baseline

Secondary but not top-level paper coordinators:
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
  - focused sensitivity study infrastructure, not the main paper suite
- `scripts/benchmark/run-scalability-benchmarks.js`
  - separate scalability family, adjacent but not the main paper suite

Older/non-paper coordinators:
- `experiments/frequency-comparison/run-all-approaches-comparison.js`
- `experiments/pattern-analysis/run-all-patterns-comparison.js`

### 2. Which scripts already support fetching, approximation, chunked, and naive distributed?

Already supports all four:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `scripts/benchmark/run-all-paper-benchmarks.js`
- `scripts/benchmark/run-scalability-benchmarks.js`
- `experiments/frequency-comparison/run-all-approaches-comparison.js`
- `analysis/accuracy/accuracy-comparison-all-approaches.js`

Only partial subsets:
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
  - required: fetching, approximation, chunked
  - optional: naive distributed
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
  - defaults to fetching + chunked
- `analysis/accuracy/pattern-accuracy-comparison.js`
  - fetching, approximation, chunked only

### 3. Which scripts already support custom patterns, real data, aggregation variations, frequency variations, and sub-window variations?

Custom patterns:
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- `scripts/benchmark/run-all-paper-benchmarks.js`
- older: `experiments/pattern-analysis/run-all-patterns-comparison.js`

Real data:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `scripts/benchmark/run-all-paper-benchmarks.js`

Aggregation variations:
- `scripts/benchmark/run-all-paper-benchmarks.js` via `--aggregation`
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js` via `--aggregation`
- `experiments/window-parameter-sensitivity/common.js`

Frequency variations:
- `scripts/benchmark/run-all-paper-benchmarks.js` via `--frequency`
- `experiments/frequency-comparison/run-all-approaches-comparison.js`
- individual frequency experiment scripts under `experiments/frequency-comparison/`

Sub-window variations:
- `scripts/benchmark/run-all-paper-benchmarks.js` via `--sub-window-range`, `--sub-window-step`
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
- `experiments/window-parameter-sensitivity/common.js`

Gaps:
- No dedicated top-level paper runner specifically for aggregation sweeps
- No dedicated top-level paper runner specifically for frequency sweeps
- The repo treats ASF/sub-window settings as parameters, not a single named paper suite

### 4. Which scripts already support finite replay and target-window caps?

Finite replay already supported:
- `experiments/utils/benchmarkReplayEnv.js`
- `scripts/benchmark/run-all-paper-benchmarks.js`
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`

Target-window caps explicitly supported:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
  - sets `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS`
- operator support exists in runtime, referenced by the benchmark path
  - chunked/approximation operator support is already present in the runtime codebase

Target-window cap gaps:
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
  - finite replay yes, but no explicit paper target-window cap orchestration in this script
- older frequency and older pattern runners
  - finite replay is mixed, target-window caps are not part of their orchestration model

### 5. Which scripts already calculate latency, accuracy, memory, CPU-seconds, MQTT traffic, and window completeness?

Most complete current coverage:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
  - latency
  - accuracy vs fetching
  - memory
  - CPU-seconds
  - MQTT traffic
  - window completeness

Custom-pattern paper analysis:
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
  - accuracy
  - execution completeness
  - iteration completeness
  - per-pattern/per-approach aggregate stats
  - does not itself compute process-tree CPU-seconds; it consumes extracted artifacts

Custom-pattern runner:
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
  - resource summaries
  - MQTT traffic summaries
  - attempt/extraction bookkeeping
  - provides inputs needed for paper analysis

Helper/normalizer:
- `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`
  - latency normalization
  - comparable window reconstruction
  - MQTT summary integration

Sensitivity pipeline:
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`
- `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`
  - latency/resource/accuracy-style extraction for that experiment family

Older coverage, weaker methodology:
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
- `analysis/accuracy/pattern-accuracy-comparison.js`

### 6. Which scripts still use outdated CPU% or iteration-based analysis?

Outdated CPU% / heap-only / older resource logic:
- `analysis/accuracy/pattern-accuracy-comparison.js`
  - reads older resource CSVs, reports heap averages, not process-tree CPU-seconds
- `analysis/accuracy/pattern-accuracy-multi-iteration.js`
  - still built around old resource file names and heap metrics
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
  - no process-tree CPU-seconds path
- `experiments/frequency-comparison/extract-results-from-logs.js`
  - older extraction, not CPU-seconds aware
- many `scripts/analysis-js/analyzeResults*.js`
  - older per-approach resource CSV assumptions

Outdated iteration model:
- `analysis/accuracy/pattern-accuracy-comparison.js`
  - fixed to `iteration1`
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
  - fixed to `iteration1`
- `experiments/frequency-comparison/run-all-approaches-comparison.js`
  - one-iteration workflow
- `experiments/pattern-analysis/run-all-patterns-comparison.js`
  - older family, sequential extraction, separate from the newer custom-pattern runner

Mixed state:
- `scripts/benchmark/extract-custom-pattern-validation.js`
  - understands newer `resource_summary.json` and retries, but is a validation/export helper, not the canonical paper analysis step

### 7. Which scripts already use the corrected process-tree CPU-seconds logic?

Authoritative corrected logic:
- `experiments/utils/processTreeMetrics.js`
  - parses `ps` CPU time
  - accumulates monotonic process-tree CPU-seconds
  - guards against negative deltas / PID reuse

Consumers already using it:
- `scripts/analysis-js/process-tree-resource-sampler.js`
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`

Paper summaries already consuming CPU-seconds outputs:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
  - reads process-tree resource CSVs and summarizes `tree_cpu_seconds`

### 8. Which scripts already use the corrected latency-domain-safe logic?

Current corrected latency-domain-safe helper:
- `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`
  - `buildWindowMetadataFromRow`
  - `attachComparableTiming`
  - reconstructs safe comparable window timing when raw metadata is incomplete or suspicious

Consumers already using that logic:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
  - imports and uses `attachComparableTiming` and `buildWindowMetadataFromRow`

Older scripts not using the corrected helper:
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
- `analysis/accuracy/pattern-accuracy-comparison.js`
- `experiments/frequency-comparison/extract-results-from-logs.js`
- much of the older `scripts/analysis-js/analyzeResults*.js` family

### 9. Which scripts are duplicated or obsolete?

Likely duplicated/obsolete relative to the paper path:
- `experiments/pattern-analysis/run-all-patterns-comparison.js`
  - superseded by `run-custom-patterns-comparison.js` for paper benchmarking
- `analysis/accuracy/pattern-accuracy-comparison.js`
  - superseded by `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
  - useful as legacy frequency summary, not paper-authoritative
- `experiments/frequency-comparison/extract-results-from-logs.js`
  - old parser-based extraction, weaker than newer benchmark metadata / latency logs
- many `scripts/analysis-js/analyzeResults*.js`
  - pre-paper analysis family, valuable for archaeology but not the canonical path

Still useful but not canonical:
- `scripts/benchmark/extract-custom-pattern-validation.js`
- `scripts/benchmark/extract-custom-pattern-latency.js`
- `scripts/benchmark/validate-mqtt-traffic-summary.js`
- `scripts/benchmark/validate-process-tree-cpu.js`
- `scripts/benchmark/verify-mqtt-traffic-counts.js`

### 10. Minimal patch plan to adapt the existing infrastructure

Target requirements:
- 35 output windows per benchmark case
- analyze windows 4..33
- 4Hz default replay
- CPU-seconds instead of CPU%
- compact reusable-result payloads
- completed-window approximation mode
- semantic-ready chunked mode

Minimal plan:

1. Keep `scripts/benchmark/run-all-paper-benchmarks.js` as the top-level entrypoint.
2. Keep `experiments/real-data-comparison/run-real-data-4-approaches.js` as the real-data paper runner.
3. Keep `experiments/pattern-analysis/run-custom-patterns-comparison.js` as the custom-pattern runner.
4. Keep `analysis/accuracy/accuracy-comparison-custom-patterns.js` as the custom-pattern analyzer.
5. Do not adapt older frequency/pattern analysis scripts for paper output.
6. Standardize one benchmark envelope across the current paper path:
   - `iterations=35`
   - trimmed analysis windows `4..33`
   - default `WEARABLE_FREQUENCY=4`
   - `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
   - `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
   - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1`
   - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0`
   - finite replay duration derived from target windows
7. Continue using process-tree CSVs and `tree_cpu_seconds` as the only CPU metric in paper summaries.
8. Continue using `buildWindowMetadataFromRow` + `attachComparableTiming` as the only comparable latency normalization path.
9. If a script still emits legacy summaries, keep them only as sidecars, not as paper outputs.

Remaining likely code work, ranked by risk:
- Low risk:
  - make custom-pattern runner explicitly align with the same 35-window / 4..33 paper envelope as the real-data runner
  - ensure top-level paper metadata always records the paper envelope explicitly
- Medium risk:
  - remove any remaining paper summary columns or docs that still foreground CPU% instead of CPU-seconds
  - ensure custom-pattern extraction preserves compact reusable-result payload assumptions
- Higher risk:
  - naive distributed correctness in any path that still emits per-stream partial windows or non-comparable latency rows
  - harmonizing old frequency-family outputs with the current paper methodology, if needed at all

## Script Table

| Script | Purpose | Current Status | Reusable Parts | Obsolete Parts | Required Changes |
| --- | --- | --- | --- | --- | --- |
| `scripts/benchmark/run-all-paper-benchmarks.js` | Top-level paper suite runner and artifact snapshotter | Current primary coordinator | suite selection, output layout, trimmed iteration selection, metadata, pattern analysis orchestration | none major; warns ASF is only parameterized, not a named suite | keep as entrypoint; ensure all paper defaults remain aligned to 35 windows / 4Hz / AVG |
| `experiments/real-data-comparison/run-real-data-4-approaches.js` | Real-data 4-approach paper runner and summarizer | Current primary real-data runner | four-approach support, finite replay env, target-window cap env, trimmed 4..33 summaries, CPU-seconds, MQTT traffic, completeness, accuracy | legacy sidecar summary block is older methodology | narrow patches only; keep as canonical real-data path |
| `experiments/pattern-analysis/run-custom-patterns-comparison.js` | Custom-pattern 4-approach benchmark runner with retries and artifact capture | Current primary custom-pattern runner | retries, attempt dirs, process-tree resource sampling, MQTT summary, extraction pipeline | older comments still say “three approaches”; mixed legacy artifact cleanup paths | align explicitly to the same paper envelope and target-window assumptions as real-data |
| `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Paper-style custom-pattern accuracy aggregation | Current primary custom-pattern analyzer | iteration-aware comparison, fetching baseline matching, execution completeness, optional naive support | none major | use as canonical pattern analysis; only patch if trimmed-window policy must be enforced here |
| `experiments/utils/processTreeMetrics.js` | Corrected process-tree CPU-seconds accounting | Current authoritative metric core | monotonic CPU-seconds, PID-reuse guard, per-PID summaries | none | no conceptual change needed |
| `scripts/analysis-js/process-tree-resource-sampler.js` | Writes process-tree resource CSV from authoritative metric core | Current helper | process-tree CSV logging, per-PID sidecar | returns empty summary on stop, but CSV is authoritative anyway | no paper-method change needed |
| `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js` | One-pattern benchmark normalizer and report generator | Current authoritative latency-normalization helper | `buildWindowMetadataFromRow`, `attachComparableTiming`, latency-domain-safe reconstruction | scenario itself is a one-pattern artifact, not the paper runner | reuse helper functions only; avoid extending the old report format |
| `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js` | Sub-window/range/chunk-size/target-count sensitivity runner | Current specialized experiment runner | aggregation parameterization, sub-window parameterization, process-tree CPU-seconds | not the paper suite; defaults only fetching+chunked | keep separate; adapt only if paper needs a dedicated sensitivity appendix |
| `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js` | Extracts sensitivity experiment summaries | Current specialized extractor | experiment-specific CSV/JSON aggregation | not part of main paper suite | none unless appendix needs new columns |
| `scripts/benchmark/run-scalability-benchmarks.js` | Same-query/different-windows scalability benchmark runner | Current but separate experiment family | four-approach orchestration, replay env, result capture | not part of paper suite; separate scenario model | none for current paper path |
| `scripts/benchmark/analyze-scalability-results.py` | Scalability report generator | Current but separate experiment family | post-processing and visualization | not paper-core | none for current paper path |
| `experiments/frequency-comparison/run-all-approaches-comparison.js` | Older frequency sweep coordinator | Legacy / non-paper | simple all-four-approach sweep coverage | single-iteration model, older extraction pipeline | do not extend for paper output |
| `experiments/frequency-comparison/extract-results-from-logs.js` | Older result extractor from raw logs | Legacy / fragile | useful for old log archaeology | positional parsing, older latency assumptions, no paper-safe normalization | obsolete for paper benchmarking |
| `analysis/accuracy/accuracy-comparison-all-approaches.js` | Older frequency comparison analysis | Legacy / non-paper | rough cross-approach comparison | single-iteration, positional window matching, not CPU-seconds-centric | do not use for paper outputs |
| `experiments/pattern-analysis/run-all-patterns-comparison.js` | Older pattern benchmark runner | Superseded | broad synthetic coverage idea | older 3-approach model, older orchestration, older extraction flow | obsolete for paper path |
| `analysis/accuracy/pattern-accuracy-comparison.js` | Older pattern summary analysis | Superseded | pattern taxonomy and narrative ideas | `iteration1`-only, heap-oriented resource analysis, old file conventions | obsolete for paper path |
| `scripts/benchmark/extract-custom-pattern-validation.js` | Validation/export helper over custom-pattern outputs | Useful support tool | attempt-dir awareness, resource summary consumption, validation exports | not canonical paper analysis | optional helper only |

## Dependency / Flow Diagram

```text
scripts/benchmark/run-all-paper-benchmarks.js
  -> creates benchmark env defaults
  -> runs:
     -> experiments/real-data-comparison/run-real-data-4-approaches.js
        -> experiments/utils/benchmarkReplayEnv.js
        -> scripts/analysis-js/process-tree-resource-sampler.js
           -> experiments/utils/processTreeMetrics.js
        -> analysis/accuracy/accuracy-comparison-custom-patterns.js (compareResults helper only)
        -> scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js
           -> buildWindowMetadataFromRow
           -> attachComparableTiming
        -> dist/util/mqttTraffic
        -> outputs:
           -> experiments/real-data-comparison/logs/... per-iteration artifacts
           -> real_data_paper_ready_raw_summary.json
           -> real_data_paper_ready_trimmed-4-33_summary.json/csv

  -> runs:
     -> experiments/pattern-analysis/run-custom-patterns-comparison.js
        -> experiments/utils/benchmarkReplayEnv.js
        -> experiments/utils/processTreeMetrics.js
        -> dist/util/mqttTraffic
        -> outputs:
           -> logs/custom-pattern-comparison/... per-attempt artifacts
           -> custom_pattern_comparison_summary.json

  -> then runs:
     -> analysis/accuracy/accuracy-comparison-custom-patterns.js
        -> reads custom-pattern raw artifacts
        -> compares against fetching baseline
        -> writes:
           -> accuracy/patterns/custom-pattern-accuracy/summary.json
           -> accuracy/patterns/custom-pattern-accuracy/summary.csv

  -> snapshots/mirrors artifacts into:
     -> results/paper-benchmarks/<timestamp>/...
        -> real-data/
        -> patterns/
        -> latency/
        -> resources/
        -> accuracy/
        -> naive-distributed/
        -> summary.json
        -> metadata.json
```

## Notes By Topic

### Result-summary generators

Current and relevant:
- `scripts/benchmark/run-all-paper-benchmarks.js`
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`
- `scripts/benchmark/analyze-scalability-results.py`

Older / not canonical:
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
- `analysis/accuracy/pattern-accuracy-comparison.js`
- many `scripts/analysis-js/analyzeResults*.js`

### Process-tree / resource samplers

Authoritative:
- `experiments/utils/processTreeMetrics.js`
- `scripts/analysis-js/process-tree-resource-sampler.js`

Embedded copies of the same logic shape:
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`

Observation:
- The metric core is already centralized in `experiments/utils/processTreeMetrics.js`.
- The remaining cleanup is mostly about ensuring paper summaries consume CPU-seconds everywhere and stop foregrounding CPU%.

### Custom-pattern runners

Current:
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`

Superseded:
- `experiments/pattern-analysis/run-all-patterns-comparison.js`

### Real-data runners

Current:
- `experiments/real-data-comparison/run-real-data-4-approaches.js`

Older adjacent utilities:
- `run-automated.js`
- `run-comparison-with-latency.js`
- `run-full-comparison-with-resources.js`
- `run-sequential-comparison.js`
- `run-single-test.js`

These look like evolutionary predecessors or one-off helpers, not the canonical paper runner.

### Frequency runners

Current but non-paper:
- `experiments/frequency-comparison/run-all-approaches-comparison.js`

This family remains useful for dedicated frequency studies, but it should not be promoted into the paper core without first replacing its older extraction/analysis assumptions.

## Minimal Implementation Plan Ranked By Risk

### Low Risk

1. Standardize paper defaults in the existing coordinators only.
   - Keep `scripts/benchmark/run-all-paper-benchmarks.js` as the single paper CLI.
   - Keep `experiments/real-data-comparison/run-real-data-4-approaches.js` and `experiments/pattern-analysis/run-custom-patterns-comparison.js` as the only benchmark executors.

2. Ensure both paper executors share the same benchmark envelope.
   - 35 output windows
   - analyze windows 4..33
   - 4Hz replay
   - AVG aggregation
   - finite replay

3. Keep CPU-seconds and latency normalization on the existing current helpers.
   - CPU: `experiments/utils/processTreeMetrics.js`
   - Latency: `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`

### Medium Risk

4. Make the custom-pattern path emit and preserve the same paper-oriented window metadata as the real-data path.

5. Remove ambiguity in summary outputs.
   - legacy summaries can stay
   - paper summaries should clearly be the only authoritative outputs for paper tables

6. Ensure compact reusable-result payload mode is explicitly set everywhere in the current paper path, not only in real-data.

### Higher Risk

7. Naive distributed correctness.
   - Any path still treating subquery partials as final windows will contaminate completeness, latency, and accuracy.
   - This is the highest-risk area because it affects methodology, not just formatting.

8. Backporting paper methodology into older frequency and legacy pattern-analysis scripts.
   - Avoid unless required.
   - The better move is to leave those scripts as legacy experiment families.

## Recommendation

Do not reimplement the benchmarking stack.

Reuse:
- `scripts/benchmark/run-all-paper-benchmarks.js`
- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- `experiments/utils/processTreeMetrics.js`
- `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`

Treat these as legacy/non-authoritative unless explicitly needed:
- `experiments/frequency-comparison/*`
- `experiments/pattern-analysis/run-all-patterns-comparison.js`
- `analysis/accuracy/pattern-accuracy-comparison.js`
- `analysis/accuracy/accuracy-comparison-all-approaches.js`
- most older `scripts/analysis-js/analyzeResults*.js`
