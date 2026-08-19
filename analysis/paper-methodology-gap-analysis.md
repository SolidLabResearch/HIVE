# Streaming Query Hive Paper Methodology Gap Analysis

Scope: audit only. No code changes, no file edits outside this report, no benchmark runs.

This report answers two questions:

1. What does the current repository already support for the final paper methodology?
2. What is the minimum set of benchmark and analysis changes still required?

## Legend

- `Ready? = Yes` means the current pipeline already produces the metric in a paper-usable form.
- `Ready? = Partial` means the metric exists, but the current output is not yet in the final methodology shape.
- `Ready? = No` means the metric is missing or only available in a legacy form that should not be used for the final paper.
- `Ready? = N/A` means the metric is not a natural output of that experiment shape.

## 1. Paper Workflow Inventory

The scripts below are the ones currently in or adjacent to the paper workflow.

| Script | Role | Status | Notes |
| --- | --- | --- | --- |
| [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) | Top-level paper orchestrator | Current | Central entrypoint for real data, patterns, latency/resources/accuracy views, and naive-distributed snapshots. |
| [`experiments/utils/benchmarkReplayEnv.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/benchmarkReplayEnv.js) | Shared replay/env helper | Current | Centralizes finite replay, chunked comparable output, and completed-window approximation defaults. |
| [`scripts/analysis-js/process-tree-resource-sampler.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/process-tree-resource-sampler.js) | Resource sampler | Current | Writes process-tree CPU-seconds and RSS-friendly samples. |
| [`experiments/utils/processTreeMetrics.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/processTreeMetrics.js) | Canonical process-tree CPU-seconds logic | Current | The preferred CPU accounting implementation. |
| [`experiments/real-data-comparison/run-real-data-4-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js) | Real-data benchmark runner | Current but legacy-shaped | Produces latency, MAE/MAPE, and MQTT traffic; resource accounting is still legacy. |
| [`experiments/pattern-analysis/run-custom-patterns-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js) | Custom-pattern benchmark runner | Current | Uses process-tree resource sampling and writes per-case result/latency/resource artifacts. |
| [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) | Custom-pattern accuracy/completeness analyzer | Current and preferred | Window-matched comparisons, completeness counts, MAE/MAPE/RMSE, raw and trimmed summaries. |
| [`scripts/benchmark/run-scalability-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-scalability-benchmarks.js) | Scalability benchmark runner | Current but legacy-shaped | Strong latency/accuracy/MQTT coverage, but CPU accounting is still `%CPU`-style. |
| [`scripts/benchmark/analyze-scalability-results.py`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/analyze-scalability-results.py) | Scalability analyzer | Current | Produces summary tables and plots from scalability outputs. |
| [`experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js) | Window sensitivity runner | Current | Uses process-tree CPU-seconds and emits latency/resource summaries. |
| [`experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js) | Window sensitivity extractor | Current | Emits CPU-seconds, latency stats, completeness-like counts, and MAE/MAPE/RMSE. |
| [`experiments/k-scaling/run-k-scaling-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/run-k-scaling-comparison.js) | K-scaling runner | Current | Uses process-tree CPU-seconds and captures latency, accuracy, and MQTT traffic. |
| [`experiments/k-scaling/extract-k-scaling-results.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/extract-k-scaling-results.js) | K-scaling extractor | Current | Emits CPU-seconds, RSS, latency, MAE/MAPE/RMSE, and reuse counters. |
| [`experiments/frequency-comparison/run-all-approaches-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/run-all-approaches-comparison.js) | Frequency master runner | Legacy | Still used for frequency sweeps, but analysis is iteration1-shaped. |
| [`experiments/frequency-comparison/run-frequency-comparison-with-capture.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/run-frequency-comparison-with-capture.js) | Frequency capture orchestrator | Legacy | Produces captured results for older frequency studies. |
| [`experiments/frequency-comparison/extract-results-from-logs.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/extract-results-from-logs.js) | Frequency log extractor | Legacy | Older capture/extract path. |
| [`analysis/accuracy/accuracy-comparison-all-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-all-approaches.js) | Legacy frequency accuracy analyzer | Legacy | Iteration1-oriented and not aligned to the final paper methodology. |
| [`scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js) | Corrected latency/resource one-pattern summary | Current and highly reusable | Latency-domain-safe, CPU-seconds, RSS, MQTT traffic, and completeness diagnostics. |
| [`scripts/analysis-js/analyzeResultsStreamingQueryHive.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/analyzeResultsStreamingQueryHive.js) | Chunked analyzer | Current | Contains the corrected latency-domain-safe reconstruction path. |
| [`scripts/benchmark/validate-process-tree-cpu.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/validate-process-tree-cpu.js) | CPU-seconds validation report | Current | Confirms the process-tree CPU-seconds interpretation. |

## 2. Metric Coverage Matrix

### Real Data

| Metric | Existing Source | Ready? | Missing? | Notes |
| --- | --- | --- | --- | --- |
| Latency | `experiments/real-data-comparison/run-real-data-4-approaches.js`, `scripts/benchmark/run-all-paper-benchmarks.js` | Partial | Yes | Latency exists, but the current report is mean/min/max oriented and not a final window-matched paper summary. |
| CPU-seconds | `scripts/analysis-js/process-tree-resource-sampler.js`, `experiments/utils/processTreeMetrics.js` | No | Yes | The real-data runner still consumes legacy resource logs instead of process-tree CPU-seconds. |
| RSS | `experiments/real-data-comparison/run-real-data-4-approaches.js`, raw `resource_usage.csv` copies | Partial | Yes | Raw resource files exist, but there is no paper-ready aggregated RSS summary. |
| MQTT traffic | `finalizeMqttTrafficArtifacts` inside `experiments/real-data-comparison/run-real-data-4-approaches.js` | Yes | No | This part is already present. |
| Completeness metrics | None in the current real-data report path | No | Yes | No explicit matched / baseline-only / approach-only accounting. |
| MAE | `experiments/real-data-comparison/run-real-data-4-approaches.js` | Partial | Yes | MAE exists, but baseline handling is first-iteration-based, not a trimmed steady-state slice. |
| MAPE | `experiments/real-data-comparison/run-real-data-4-approaches.js` | Partial | Yes | Same limitation as MAE. |
| RMSE | None in the current real-data report path | No | Yes | Not currently produced. |
| Trimmed windows 4..33 | None in the current real-data report path | No | Yes | This is the major steady-state gap for real data. |

### Synthetic Patterns

| Metric | Existing Source | Ready? | Missing? | Notes |
| --- | --- | --- | --- | --- |
| Latency | `experiments/pattern-analysis/run-custom-patterns-comparison.js`, `scripts/benchmark/run-all-paper-benchmarks.js` | Partial | Yes | Raw latency logs exist; the general multi-pattern pipeline does not yet surface a latency-domain-safe paper summary. |
| CPU-seconds | `scripts/analysis-js/process-tree-resource-sampler.js`, `experiments/pattern-analysis/run-custom-patterns-comparison.js` | Yes | No | Current pattern runner already uses process-tree CPU-seconds. |
| RSS | `scripts/analysis-js/process-tree-resource-sampler.js`, `experiments/pattern-analysis/run-custom-patterns-comparison.js` | Yes | No | Current pattern runner already captures RSS. |
| MQTT traffic | `experiments/pattern-analysis/run-custom-patterns-comparison.js` | Yes | No | Finalized MQTT traffic artifacts are already produced. |
| Completeness metrics | `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Yes | No | Matched windows, baseline-only windows, and approach-only windows are already reported. |
| MAE | `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Yes | No | Already supported. |
| MAPE | `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Yes | No | Already supported. |
| RMSE | `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Yes | No | Already supported. |
| Trimmed windows 4..33 | `scripts/benchmark/run-all-paper-benchmarks.js`, `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Yes | No | This is already the intended paper path for the 35-run pattern study. |

### Aggregation Variations

| Metric | Existing Source | Ready? | Missing? | Notes |
| --- | --- | --- | --- | --- |
| Latency | `scripts/benchmark/run-all-paper-benchmarks.js`, `experiments/k-scaling/run-k-scaling-comparison.js` | Partial | Yes | Aggregation is parameterized, but there is no dedicated paper-ready aggregation summary table. |
| CPU-seconds | `experiments/k-scaling/run-k-scaling-comparison.js`, `scripts/benchmark/run-all-paper-benchmarks.js` | Partial | Yes | Some aggregation-capable paths already use CPU-seconds, but the paper workflow does not yet standardize them across all aggregation variants. |
| RSS | `experiments/k-scaling/extract-k-scaling-results.js` | Partial | Yes | RSS exists in some aggregation-capable outputs, but not as a consistent paper view. |
| MQTT traffic | `experiments/k-scaling/extract-k-scaling-results.js` | Partial | Yes | MQTT counters and traffic fields exist in some outputs. |
| Completeness metrics | `experiments/k-scaling/extract-k-scaling-results.js` | Partial | Yes | Reuse-path and emitted-result counts exist, but not a uniform completeness schema. |
| MAE | `experiments/k-scaling/extract-k-scaling-results.js` | Partial | Yes | Available in k-scaling, not standardized as a paper aggregation summary. |
| MAPE | `experiments/k-scaling/extract-k-scaling-results.js` | Partial | Yes | Same limitation. |
| RMSE | `experiments/k-scaling/extract-k-scaling-results.js` | Partial | Yes | Same limitation. |
| Trimmed windows 4..33 | `scripts/benchmark/run-all-paper-benchmarks.js` | Partial | Yes | Only applies when aggregation runs through the core 35-run paper orchestrator. |

### Frequency Variations

| Metric | Existing Source | Ready? | Missing? | Notes |
| --- | --- | --- | --- | --- |
| Latency | `experiments/frequency-comparison/extract-results-from-logs.js`, `analysis/accuracy/accuracy-comparison-all-approaches.js` | Partial | Yes | Latency exists, but the path is legacy and not latency-domain-safe. |
| CPU-seconds | `experiments/frequency-comparison/run-comparison-table.js` | Partial | Yes | CPU-seconds can be derived in a legacy helper, but the main frequency workflow still centers older outputs. |
| RSS | `experiments/frequency-comparison/run-comparison-table.js` | Partial | Yes | Memory is present in the comparison table path, not in the main paper path. |
| MQTT traffic | `experiments/frequency-comparison/run-frequency-comparison-with-capture.js` and related capture scripts | Partial | Yes | Capture exists, but it is not standardized in the final paper summary. |
| Completeness metrics | None in the current frequency paper path | No | Yes | No explicit matched / missing / extra window accounting. |
| MAE | `analysis/accuracy/accuracy-comparison-all-approaches.js` | Yes | No | Already available for the legacy frequency workflow. |
| MAPE | `analysis/accuracy/accuracy-comparison-all-approaches.js` | Yes | No | Already available for the legacy frequency workflow. |
| RMSE | `analysis/accuracy/accuracy-comparison-all-approaches.js` | Yes | No | Already available for the legacy frequency workflow. |
| Trimmed windows 4..33 | Not applicable to the current frequency workflow shape | N/A | N/A | The current frequency studies are not 35-window paper runs. |

### Window / Sub-window Sensitivity

| Metric | Existing Source | Ready? | Missing? | Notes |
| --- | --- | --- | --- | --- |
| Latency | `experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js` | Yes | No | Strong latency distribution support already exists. |
| CPU-seconds | `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`, `extract-window-parameter-sensitivity-results.js` | Yes | No | Process-tree CPU-seconds are already present. |
| RSS | `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`, `extract-window-parameter-sensitivity-results.js` | Yes | No | RSS is already present. |
| MQTT traffic | `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js` | No | Yes | The current output tracks `mqtt_clients_created`, but not the traffic bytes needed for the paper methodology. |
| Completeness metrics | `extract-window-parameter-sensitivity-results.js` | Yes | No | Matched-window counts and emitted-window counts are already present. |
| MAE | `extract-window-parameter-sensitivity-results.js` | Yes | No | Already present. |
| MAPE | `extract-window-parameter-sensitivity-results.js` | Yes | No | Already present. |
| RMSE | `extract-window-parameter-sensitivity-results.js` | Yes | No | Already present. |
| Trimmed windows 4..33 | Not applicable to the current sensitivity workflow shape | N/A | N/A | These studies are not the 35-window core benchmark. |

### Scalability Studies

| Metric | Existing Source | Ready? | Missing? | Notes |
| --- | --- | --- | --- | --- |
| Latency | `scripts/benchmark/run-scalability-benchmarks.js`, `scripts/benchmark/analyze-scalability-results.py` | Yes | No | Latency is already computed and summarized. |
| CPU-seconds | `scripts/benchmark/run-scalability-benchmarks.js` | No | Yes | The current summary uses `%CPU`-style resource metrics, not process-tree CPU-seconds. |
| RSS | `scripts/benchmark/run-scalability-benchmarks.js` | Yes | No | Mean and peak memory are already summarized. |
| MQTT traffic | `scripts/benchmark/run-scalability-benchmarks.js` | Yes | No | MQTT traffic summaries are already emitted and validated. |
| Completeness metrics | `scripts/benchmark/run-scalability-benchmarks.js` | Partial | Yes | Matched-window counts and exact-rate exist, but missing / extra windows are not fully explicit in the final table. |
| MAE | `scripts/benchmark/run-scalability-benchmarks.js`, `scripts/benchmark/analyze-scalability-results.py` | Yes | No | Already present. |
| MAPE | `scripts/benchmark/run-scalability-benchmarks.js`, `scripts/benchmark/analyze-scalability-results.py` | Yes | No | Already present. |
| RMSE | `scripts/benchmark/run-scalability-benchmarks.js`, `scripts/benchmark/analyze-scalability-results.py` | Yes | No | Already present. |
| Trimmed windows 4..33 | Not applicable to the current scalability workflow shape | N/A | N/A | Scalability is a distinct study design, not the 35-window paper core run. |

## 3. Obsolete, Duplicate, or Legacy Paths

These should not be used as the primary paper methodology path.

### Legacy analysis scripts

- [`analysis/accuracy/approximation-accuracy-analysis.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/approximation-accuracy-analysis.js)
- [`analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js)
- [`analysis/accuracy/accuracy-comparison-all-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-all-approaches.js)
- [`analysis/accuracy/pattern-accuracy-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/pattern-accuracy-comparison.js)
- [`analysis/accuracy/pattern-accuracy-multi-iteration.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/pattern-accuracy-multi-iteration.js)

### Legacy helper / debug paths

- [`scripts/analysis-js/analyze_window_timing.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/analyze_window_timing.js)
- [`scripts/analysis-js/experiment-evaluation-all.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/experiment-evaluation-all.js)
- [`scripts/analysis-js/compare-first-vs-fetching.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/compare-first-vs-fetching.js)
- [`scripts/analysis-js/compare-accuracy-results.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/compare-accuracy-results.js)
- [`scripts/analysis-js/extract-and-compare-results.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/extract-and-compare-results.js)

### Older real-data / pattern variants

- [`experiments/real-data-comparison/run-full-comparison-with-resources.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-full-comparison-with-resources.js)
- [`experiments/real-data-comparison/run-automated.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-automated.js)
- [`experiments/real-data-comparison/run-sequential-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-sequential-comparison.js)
- [`experiments/real-data-comparison/run-single-test.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-single-test.js)
- [`experiments/real-data-comparison/run-comparison-with-latency.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-comparison-with-latency.js)
- [`experiments/real-data-comparison/run-first-result-latency.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-first-result-latency.js)
- [`experiments/pattern-analysis/run-all-patterns-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-all-patterns-comparison.js)

### Summaries that should no longer be the paper source of truth

- `real_data_comparison_results.csv` / `real_data_comparison_results.json`
- `analysis/accuracy/accuracy-comparison-all-approaches.js` outputs for the paper story
- `analysis/accuracy/pattern-accuracy-comparison.js` outputs
- `analysis/accuracy/pattern-accuracy-multi-iteration.js` outputs
- `analysis/accuracy/approximation-accuracy-analysis.js` outputs
- `analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js` outputs

## 4. Missing Capabilities and Exact Patch Targets

This section lists only the capabilities that are still missing or only partially supported for the final methodology.

| Missing capability | Files to patch | Difficulty | Why it is necessary |
| --- | --- | --- | --- |
| Real-data steady-state methodology: process-tree CPU-seconds, explicit completeness counts, RMSE, and a trimmed 4..33 paper summary | [`experiments/real-data-comparison/run-real-data-4-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js), [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) | Medium | The current real-data report uses first-iteration baseline alignment and legacy resource logs; it does not satisfy the final steady-state methodology. |
| Pattern-suite latency-domain-safe paper summary | [`experiments/pattern-analysis/run-custom-patterns-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js), [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) | Medium | The raw latency logs exist, but the general multi-pattern paper summary does not yet expose a latency-domain-safe comparison. |
| Window / sub-window sensitivity MQTT traffic bytes | [`experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js), [`experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js) | Small | Current outputs capture latency, CPU-seconds, RSS, and matching counts, but not traffic bytes. |
| Scalability CPU-seconds and explicit missing / extra window completeness | [`scripts/benchmark/run-scalability-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-scalability-benchmarks.js), [`scripts/benchmark/analyze-scalability-results.py`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/analyze-scalability-results.py) | Medium | The current scalability path still uses `%CPU`-style accounting and only partial completeness indicators. |
| Frequency-sweep modernization if frequency variation stays in the submission | [`experiments/frequency-comparison/run-all-approaches-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/run-all-approaches-comparison.js), [`analysis/accuracy/accuracy-comparison-all-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-all-approaches.js), [`experiments/frequency-comparison/run-comparison-table.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/run-comparison-table.js) | Medium | The current frequency path is still iteration1-shaped and should not be the final paper source without alignment to CPU-seconds and completeness-aware comparison. |

## 5. Minimal Patch Plan

The smallest defensible patch plan is:

1. Keep [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) as the single paper orchestrator and keep the 35-run / trim-4..33 policy there.
2. Patch the real-data runner to emit the same final methodology shape as the pattern pipeline: process-tree CPU-seconds, RSS, explicit completeness, and RMSE.
3. Patch the scalability runner/analyzer to replace `%CPU` with process-tree CPU-seconds and to emit explicit missing / extra window counts.
4. Patch the window-sensitivity extractor only where MQTT traffic bytes are missing, because the rest of that pipeline is already close to the target methodology.
5. Decide whether frequency sweeps stay in the paper. If they do, modernize the legacy frequency runner/analyzer path; if they do not, keep them out of the main paper matrix.
6. Keep the current custom-pattern accuracy pipeline and the corrected one-pattern latency-reporting path; they already provide the cleanest reusable pieces.

## 6. Bottom Line

The repository is much closer than it first appears:

- Patterns are mostly ready.
- Window sensitivity is mostly ready except for MQTT traffic bytes.
- Scalability is structurally strong but still needs CPU-seconds and explicit completeness.
- Real data is the biggest gap because the current report is still legacy-shaped.
- Frequency studies are legacy and should either be modernized or kept out of the main paper claim set.

The minimum high-value work is therefore to patch the real-data, scalability, and window-sensitivity paths first, then decide how much of the legacy frequency path should remain in the final paper.
