# Phase 1 Reuse and Consolidation Plan

Source material used:
- `analysis/paper-methodology-gap-analysis.md`
- `analysis/paper-final-methodology-review.md`
- `analysis/process-tree-cpu-validation-4hz.md`
- `analysis/latency-runtime-fix-report.md`

## Verdict

Phase 1 already exists, but it is distributed across shared helpers, samplers, analyzers, and a single top-level paper orchestrator.

The correct next step is consolidation, not reimplementation:

- reuse the existing helpers for CPU, latency, MQTT, completeness, and accuracy;
- export or adapt the existing helpers where consumers still duplicate logic;
- keep the final paper path limited to the agreed benchmark scope.

## Scope

### Included

- Tier 1: real data, synthetic patterns, window/sub-window sensitivity, K-scaling/scalability
- Tier 2: frequency variations, aggregation variations

### Excluded

- HiveScoutBee routing
- stream signature validation
- routing sensitivity

## Reuse Map

| Capability | Canonical Existing File | Reuse Status | Duplicate / Legacy Files To Avoid | Patch Needed? |
| --- | --- | --- | --- | --- |
| Process-tree CPU-seconds helper | [`experiments/utils/processTreeMetrics.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/processTreeMetrics.js) (`ProcessTreeTracker`, `collectTreeMetrics`, `summarizeResourceSamples`, `computeCpuSecondsFromLegacyTreeRows`) | Canonical and paper-ready | Local tree accounting in [`experiments/pattern-analysis/run-custom-patterns-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js), [`experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js), [`experiments/k-scaling/run-k-scaling-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/run-k-scaling-comparison.js) | No for the helper; yes for callers still bypassing it |
| Process-tree sampler | [`scripts/analysis-js/process-tree-resource-sampler.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/process-tree-resource-sampler.js) (`startProcessTreeResourceLogging`) | Canonical and paper-ready | Ad hoc resource logging in legacy summaries and runner-local code paths | No for the sampler; callers should reuse it |
| Latency-domain-safe close-to-result mapping | [`scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js) (`attachComparableTiming`, `validateWindows`, `accuracyMetrics`) and [`scripts/analysis-js/analyzeResultsStreamingQueryHive.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/analyzeResultsStreamingQueryHive.js) | Canonical for paper-safe latency comparisons | [`analysis/accuracy/accuracy-comparison-all-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-all-approaches.js), [`analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js), [`analysis/accuracy/pattern-accuracy-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/pattern-accuracy-comparison.js), [`analysis/accuracy/pattern-accuracy-multi-iteration.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/pattern-accuracy-multi-iteration.js), [`scripts/analysis-js/analyze_window_timing.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/analyze_window_timing.js) | Yes in any consumer still doing raw event-time minus wall-clock subtraction |
| Completeness / matched-window accounting | [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) (`compareResults`, `buildPatternSummary`) | Canonical and paper-ready for patterns | Legacy iteration1 analyzers in `analysis/accuracy/` | No for patterns; adapt if other tiers need the same schema |
| MQTT traffic summaries | [`src/util/mqttTraffic.ts`](/Users/kushbisen/Code/streaming-query-hive/src/util/mqttTraffic.ts) (`measureMqttPublish`, `recordPublishedMqttMessage`, `finalizeMqttTrafficArtifacts`) | Canonical and paper-ready | Manual traffic counting or ad hoc capture/report logic in legacy frequency and summary scripts | No; reuse the existing helper chain |
| MAE / MAPE / RMSE computation | [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) | Canonical and paper-ready for the main paper path | [`analysis/accuracy/approximation-accuracy-analysis.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/approximation-accuracy-analysis.js), [`analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js), [`analysis/accuracy/accuracy-comparison-all-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-all-approaches.js), [`analysis/accuracy/pattern-accuracy-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/pattern-accuracy-comparison.js), [`analysis/accuracy/pattern-accuracy-multi-iteration.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/pattern-accuracy-multi-iteration.js) | Yes for real-data/frequency/scalability consumers that still need the same completeness-aware aggregation |
| Trimmed window selection 4..33 | [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) (`buildTrimmedIterationSelection`) | Canonical and paper-ready | Manual warmup/cooldown dropping in old reports and one-off analyzers | No for the selector; yes only if a consumer ignores the existing trim pass |
| Shared replay defaults for completed-window approximation and chunked comparable output | [`experiments/utils/benchmarkReplayEnv.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/benchmarkReplayEnv.js) (`createBenchmarkReplayRunEnv`) | Canonical and paper-ready | Hardcoded per-script env blocks that diverge from the shared defaults | No; reuse the helper in all paper-facing runners |

## Do-Not-Reimplement

Reuse these existing files and functions instead of copying the logic into new benchmark code:

| File | Reuse This | Why |
| --- | --- | --- |
| [`experiments/utils/processTreeMetrics.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/processTreeMetrics.js) | `ProcessTreeTracker`, `collectTreeMetrics`, `summarizeResourceSamples`, `computeCpuSecondsFromLegacyTreeRows` | This is the canonical CPU-seconds implementation. |
| [`scripts/analysis-js/process-tree-resource-sampler.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/process-tree-resource-sampler.js) | `startProcessTreeResourceLogging` | This is the shared sampler writer for resource CSVs. |
| [`src/util/mqttTraffic.ts`](/Users/kushbisen/Code/streaming-query-hive/src/util/mqttTraffic.ts) | `measureMqttPublish`, `recordPublishedMqttMessage`, `finalizeMqttTrafficArtifacts` | This is the canonical MQTT traffic accounting path. |
| [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) | `compareResults`, `buildPatternSummary`, `buildSummary` | This is the canonical completeness-aware accuracy comparator for the paper path. |
| [`scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js) | `attachComparableTiming`, `validateWindows`, `accuracyMetrics` | This is the canonical latency-domain-safe comparison path. |
| [`scripts/analysis-js/analyzeResultsStreamingQueryHive.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/analyzeResultsStreamingQueryHive.js) | Latency reconstruction and safe metadata handling | Reuse the existing safe chunked analyzer logic. |
| [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) | `buildTrimmedIterationSelection` and the existing trimmed summary outputs | This already encodes the 35-run, trim-4..33 methodology. |
| [`experiments/utils/benchmarkReplayEnv.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/utils/benchmarkReplayEnv.js) | `createBenchmarkReplayRunEnv` | This already centralizes the replay defaults used by the paper workflow. |

## Minimal Patch Plan

The minimum plan is to patch only the consumers that still miss the final methodology. No new benchmark runners are needed if these existing paths are extended.

| Area | Missing Piece | Existing Piece To Reuse | Files To Patch | Action Type | Difficulty | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Real data | Process-tree CPU-seconds, explicit completeness, RMSE, and a trimmed steady-state paper view | `experiments/utils/processTreeMetrics.js`, `analysis/accuracy/accuracy-comparison-custom-patterns.js`, `scripts/benchmark/run-all-paper-benchmarks.js` | [`experiments/real-data-comparison/run-real-data-4-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js), [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) | Call existing helper + adapt existing analyzer | Medium | Keep the runner, but make its paper output match the final steady-state methodology. |
| Synthetic patterns | A general paper-safe latency summary for the multi-pattern path | `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`, `scripts/analysis-js/analyzeResultsStreamingQueryHive.js`, `analysis/accuracy/accuracy-comparison-custom-patterns.js` | [`experiments/pattern-analysis/run-custom-patterns-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js), [`analysis/accuracy/accuracy-comparison-custom-patterns.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) | Export existing helper + adapt existing analyzer | Medium | Do not rebuild latency logic; reuse the corrected anchor-aware mapping. |
| Window / sub-window sensitivity | MQTT traffic bytes in the sensitivity outputs | `src/util/mqttTraffic.ts` | [`experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js), [`experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js) | Call existing helper + adapt existing analyzer | Small | The rest of the sensitivity pipeline is already close to the target methodology. |
| Scalability | CPU-seconds plus explicit missing / extra window completeness | `experiments/utils/processTreeMetrics.js`, `analysis/accuracy/accuracy-comparison-custom-patterns.js` for completeness style | [`scripts/benchmark/run-scalability-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-scalability-benchmarks.js), [`scripts/benchmark/analyze-scalability-results.py`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/analyze-scalability-results.py) | Call existing helper + adapt existing analyzer / reporting labels | Medium | Replace `%CPU` as the primary resource metric and surface missing/extra windows explicitly. |
| Frequency | Keep the sweep only if Tier 2 remains in the submission | `analysis/accuracy/accuracy-comparison-custom-patterns.js`, `experiments/utils/processTreeMetrics.js`, `src/util/mqttTraffic.ts` | [`experiments/frequency-comparison/run-all-approaches-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/run-all-approaches-comparison.js), [`analysis/accuracy/accuracy-comparison-all-approaches.js`](/Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-all-approaches.js), [`experiments/frequency-comparison/run-comparison-table.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/frequency-comparison/run-comparison-table.js) | Adapt existing analyzer | Medium | The existing frequency path is legacy-shaped; keep it only if the paper includes Tier 2 frequency claims. |
| Aggregation | Mostly already available through existing env plumbing; only reporting consistency may remain | `scripts/benchmark/run-all-paper-benchmarks.js`, `experiments/k-scaling/run-k-scaling-comparison.js` | [`scripts/benchmark/run-all-paper-benchmarks.js`](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js), [`experiments/k-scaling/run-k-scaling-comparison.js`](/Users/kushbisen/Code/streaming-query-hive/experiments/k-scaling/run-k-scaling-comparison.js) | Only change reporting labels / metadata if needed | Small | No new runner is needed; keep `AVG` as the core paper default unless an appendix requires more. |

## Consolidation Order

1. Keep the existing top-level paper orchestrator and trim-4..33 policy intact.
2. Reuse the shared process-tree, MQTT, and completeness helpers everywhere a consumer still duplicates logic.
3. Patch the real-data path first, because it still carries the most legacy analysis.
4. Patch the scalability path next, because it still relies on `%CPU`.
5. Patch the remaining Tier 2 frequency path only if it stays in the submission.
6. Leave HiveScoutBee routing, stream signature validation, and routing sensitivity out of the paper benchmark path.

## Bottom Line

Phase 1 already exists as reusable infrastructure.

The minimum viable consolidation is to wire the final paper paths through the shared helpers above, adapt the legacy analyzers that still compute metrics in old ways, and avoid creating any new runner unless a missing capability cannot be expressed by extending the current ones.
