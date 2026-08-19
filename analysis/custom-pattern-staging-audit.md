# Custom-Pattern Staging Audit

Audit of files staged for commit and remaining unstaged changes.

## Staged Files

The following files are staged and verified as custom-pattern benchmark work:

* analysis/accuracy/accuracy-comparison-custom-patterns.js
* analysis/custom-pattern-all-patterns-paper-smoke-validation.md
* analysis/custom-pattern-approximation-bounded-smoke-audit.md
* analysis/custom-pattern-bounded-finite-replay-guard-report.md
* analysis/custom-pattern-commit-readiness.md
* analysis/custom-pattern-final-commit-audit.md
* analysis/custom-pattern-final-go-no-go.md
* analysis/custom-pattern-full-run-prereq-check.md
* analysis/custom-pattern-orchestrator-exit-hardening-report.md
* analysis/custom-pattern-paper-method-alignment-report.md
* analysis/custom-pattern-production-readiness-audit.md
* docs/decisions/2026-06-25-custom-pattern-orchestrator-exit-hardening.md
* docs/decisions/2026-06-25-custom-pattern-paper-methodology-alignment.md
* experiments/pattern-analysis/extract-pattern-results.js
* experiments/pattern-analysis/extract-pattern-results.test.js
* experiments/pattern-analysis/run-custom-patterns-comparison.js

## Unstaged Files

The following files remain unstaged:

* .gitignore
* chunked_debug_summary.json
* experiments/frequency-comparison/run-comparison-table.js
* experiments/generate-phase1-report.js
* experiments/k-scaling/run-k-scaling-comparison.js
* experiments/real-data-comparison/run-real-data-4-approaches.js
* experiments/utils/benchmarkReplayEnv.js
* experiments/window-parameter-sensitivity/README.md
* experiments/window-parameter-sensitivity/common.js
* experiments/window-parameter-sensitivity/common.test.js
* experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js
* experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js
* experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js
* experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js
* scripts/analysis-js/experiment-evaluation-approximation-approach.js
* scripts/analysis-js/experiment-evaluation-fetching-client-side.js
* scripts/analysis-js/experiment-evaluation-streaming-query-hive.js
* src/agent/RSPAgent.test.ts
* src/agent/RSPAgent.ts
* src/approaches/ScalabilitySameQueryDifferentWindowsApproximationOrchestrator.ts
* src/approaches/ScalabilitySameQueryDifferentWindowsChunkedOrchestrator.ts
* src/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator.ts
* src/approaches/ScalabilitySameQueryDifferentWindowsNaiveDistributedOrchestrator.ts
* src/approaches/StreamingQueryApproximationApproachOrchestrator.ts
* src/approaches/StreamingQueryChunkedApproachOrchestrator.ts
* src/approaches/StreamingQueryChunkedKScalingOrchestrator.ts
* src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts
* src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.ts
* src/orchestrator/Orchestrator.ts
* src/rsp/RSPQueryProcess.ts
* src/services/operators/RateBasedApproximationApproachOperator.test.ts
* src/services/operators/RateBasedApproximationApproachOperator.ts
* src/services/operators/StreamingQueryChunkAggregatorOperator.test.ts
* src/services/operators/StreamingQueryChunkAggregatorOperator.ts
* src/services/operators/approximation/ApproximationDiagnosticsWriter.ts
* src/services/operators/approximation/ApproximationWindowBuffer.ts
* src/services/operators/approximation/RateBasedApproximationMath.ts
* src/services/operators/chunked/ChunkedDiagnosticsWriter.ts
* src/services/operators/chunked/WindowRecomposer.ts
* src/services/operators/chunked/types.ts
* src/streamer/data/custom_patterns/high_freq_oscillation/metadata.json
* src/streamer/data/custom_patterns/low_freq_oscillation/metadata.json
* src/streamer/data/custom_patterns/low_variability/data.csv
* src/streamer/data/custom_patterns/low_variability/metadata.json
* src/streamer/data/custom_patterns/low_variability/smartphone.acceleration.x/data.nt
* src/streamer/data/custom_patterns/low_variability/wearable.acceleration.x/data.nt
* src/streamer/data/custom_patterns/spike_pattern/metadata.json
* src/streamer/data/custom_patterns/step_pattern/metadata.json
* src/streamer/src/publish.ts
* src/streamer/src/publishing/StreamToMQTT.ts
* src/util/chunkTypes.ts
* src/util/mqttTraffic.ts
* src/util/profiling.ts
* src/util/runtimeConfig.ts
* Untracked files (excluding staged new files):
    * analysis/_artifact_backups/
    * analysis/approximation-cpu-optimization-report.md
    * analysis/approximation-cpu-overhead-audit.md
    * analysis/approximation-cpu-stage-attribution.md
    * analysis/approximation-vs-chunked-cpu-boundary-audit.md
    * analysis/compact-full-4hz-metrics.json
    * analysis/compact-reusable-result-payload-audit.md
    * analysis/fetching-zero-final-comparable-diagnosis.md
    * analysis/k-scaling/k_scaling_first_result_latency_table.md
    * analysis/k-scaling/k_scaling_latency_comparison.md
    * analysis/k-scaling/k_scaling_latency_timing_audit.md
    * analysis/latency-runtime-fix-report.md
    * analysis/latency-semantics-bug-audit.md
    * analysis/one-pattern-15w-fetching-blocker-diagnosis.md
    * analysis/one-pattern-15w-fetching-cadence-filter-fix.md
    * analysis/one-pattern-centered-window-n3-summary.md
    * analysis/one-pattern-centered-window-smoke-data.json
    * analysis/one-pattern-centered-window-smoke.md
    * analysis/one-pattern-latency-fixed-15w-4hz-vs-10hz-drift.md
    * analysis/one-pattern-latency-fixed-15w-steady-4hz-compact-summary.md
    * analysis/one-pattern-latency-fixed-15w-steady-4hz-summary.md
    * analysis/one-pattern-latency-fixed-15w-steady-summary.md
    * analysis/one-pattern-latency-fixed-15w-trend-analysis.md
    * analysis/one-pattern-latency-fixed-cold-n3-summary.md
    * analysis/one-pattern-latency-fixed-n3-summary.md
    * analysis/one-pattern-three-approach-comparison-corrected.md
    * analysis/one-pattern-three-approach-comparison.md
    * analysis/one-pattern-three-approach-cpu-diagnosis.md
    * analysis/one-pattern-three-approach-n3-summary.md
    * analysis/paper-benchmark-script-inventory.md
    * analysis/paper-final-methodology-review.md
    * analysis/paper-methodology-gap-analysis.md
    * analysis/phase1-reuse-and-consolidation-plan.md
    * analysis/process-tree-cpu-validation-4hz.md
    * analysis/real-data-paper-methodology-consolidation.md
    * analysis/real-data-paper-ready-smoke-validation.md
    * analysis/real-data-semantic-ready-chunked-exactness-audit.md
    * analysis/replay-dataset-duration-audit.md
    * analysis/replay-drift-accumulation-audit.md
    * analysis/replay-scheduler-fix-report.md
    * analysis/tier1-low-risk-paper-wiring-report.md
    * analysis/tier1-low-risk-smoke-validation.md
    * analysis/tier1-smoke-blocker-fix-report.md
    * analysis/tier1-tier2-paper-experiment-readiness-audit.md
    * chunked_emission_proof.json
    * experiments/real-data-comparison/run-real-data-4-approaches.test.js
    * experiments/utils/processTreeMetrics.js
    * experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js
    * mqtt_traffic.ndjson
    * run_summary.json
    * scripts/__pycache__/
    * scripts/analysis-js/generate-one-pattern-centered-window-n3-summary.js
    * scripts/analysis-js/generate-one-pattern-three-approach-corrected-report.js
    * scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js
    * scripts/analysis-js/process-tree-resource-sampler.js
    * scripts/analysis-js/run-one-pattern-centered-window-n3.js
    * scripts/analysis-js/run-one-pattern-latency-fixed-steady.js
    * scripts/analysis-js/run-one-pattern-three-approach-n3.js
    * scripts/benchmark/validate-process-tree-cpu.js
    * scripts/summarize-k-scaling-latency.py
    * src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.test.ts
    * src/services/operators/approximation/ApproximationDiagnosticsWriter.test.ts
    * src/util/queryTargets.ts

## Files Requiring Later Review

* src/streamer/data/custom_patterns/ (contains metadata/data changes that might need synchronization with custom-pattern execution runs)
* src/streamer/src/publish.ts & src/streamer/src/publishing/StreamToMQTT.ts (custom pattern streaming changes)

## Files That Should Never Be Committed

* chunked_debug_summary.json
* chunked_emission_proof.json
* mqtt_traffic.ndjson
* run_summary.json
* scripts/__pycache__/
* analysis/_artifact_backups/
