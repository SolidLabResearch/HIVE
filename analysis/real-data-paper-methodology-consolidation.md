# Real-Data Paper Methodology Consolidation

## Scope

This change set patches only the real-data workflow. It does not modify the pattern, frequency, scalability, or window-sensitivity pipelines.

## Files Changed

| File | Change |
| --- | --- |
| `experiments/real-data-comparison/run-real-data-4-approaches.js` | Reworked the real-data runner to emit paper-ready summaries while preserving the legacy CSV/JSON outputs. |
| `experiments/real-data-comparison/run-real-data-4-approaches.test.js` | Added a focused synthetic test for trimmed-window analysis, completeness, latency, CPU-seconds, and MQTT counts. |
| `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Exported the existing `compareResults` helper for reuse. |
| `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js` | Exported the existing latency-domain-safe helpers and made the script safe to import. |

## Helpers Reused

| Helper | Reuse |
| --- | --- |
| `experiments/utils/processTreeMetrics.js` | Reused indirectly through the existing process-tree sampler; no new CPU accounting logic was added. |
| `scripts/analysis-js/process-tree-resource-sampler.js` | Used to capture process-tree CPU-seconds and RSS for each orchestrator run. |
| `src/util/mqttTraffic.ts` via `dist/util/mqttTraffic` | Reused through `finalizeMqttTrafficArtifacts`; no new MQTT byte accounting logic was added. |
| `analysis/accuracy/accuracy-comparison-custom-patterns.js` | Reused `compareResults` for completeness-aware `MAE`, `MAPE`, `RMSE`, matched windows, fetching-only windows, and approach-only windows. |
| `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js` | Reused `buildWindowMetadataFromRow` and `attachComparableTiming` for latency-domain-safe close-to-result analysis. |
| `experiments/utils/benchmarkReplayEnv.js` | Reused shared finite-replay, completed-window approximation, and chunked comparable-output defaults. |

## Methodology Applied

The patched real-data runner now supports:

- 35 output windows per run via `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- trimmed analysis over windows `4..33`
- approaches `fetching`, `approximation`, `chunked`, `naive_distributed`
- latency-domain-safe close-to-result latency
- process-tree CPU-seconds
- CPU-seconds per emitted window
- mean and peak RSS
- MQTT traffic bytes
- MQTT message counts from finalized MQTT traffic CSVs
- completeness-aware comparisons against fetching:
  - matched windows
  - fetching-only windows
  - approach-only windows
  - missing windows
  - extra windows
- `MAE`, `MAPE`, `RMSE` against fetching
- preserved raw outputs plus new paper-ready trimmed summaries

## Old Metrics Replaced

| Legacy behavior | Paper-ready replacement |
| --- | --- |
| Ad hoc / legacy latency parsing with fallback to message text | Latency normalization through `buildWindowMetadataFromRow` + `attachComparableTiming` |
| First-iteration-only accuracy comparison | Per-iteration completeness-aware comparison against fetching |
| CPU% style resource interpretation | Process-tree CPU-seconds and CPU-seconds per emitted window |
| Implicit or absent window completeness | Explicit matched / fetching-only / approach-only / missing / extra window reporting |
| Weak MQTT message counting | Message counts derived from finalized MQTT traffic CSV rows |

Legacy outputs are still written:

- `experiments/real-data-comparison/logs/real_data_comparison_results.csv`
- `experiments/real-data-comparison/logs/real_data_comparison_results.json`

## New Output Artifacts

The real-data runner now writes:

- `experiments/real-data-comparison/logs/real_data_paper_ready_raw_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.csv`

The raw summary keeps per-approach per-iteration detail. The trimmed summary keeps the paper-facing aggregates for windows `4..33`.

## Why `scripts/benchmark/run-all-paper-benchmarks.js` Was Not Patched

The top-level paper runner already:

- invokes `experiments/real-data-comparison/run-real-data-4-approaches.js`
- uses the paper replay defaults (`4 Hz`, `35` iterations by default)
- snapshots the full real-data log tree into the benchmark results directory

Because the real-data runner now self-applies the target-window and trimmed-summary methodology, the top-level runner did not need additional changes for this phase.

## Smoke Commands Run

```bash
node experiments/real-data-comparison/run-real-data-4-approaches.js analyze-only
npx jest experiments/real-data-comparison/run-real-data-4-approaches.test.js --runInBand
```

## Smoke Result

- `analyze-only` completed successfully and wrote the new paper-ready summary artifacts under `experiments/real-data-comparison/logs/`
- the focused Jest test passed and verified:
  - trimming to windows `4..33`
  - latency-domain-safe close-to-result aggregation
  - completeness-aware comparison against fetching
  - CPU-seconds and CPU-seconds-per-window
  - MQTT byte summaries and message counts

## Remaining Limitations

- The `analyze-only` smoke used the existing local real-data logs, which currently contain only a legacy-sized run. As a result, the generated trimmed summary for those historical logs contains many null aggregate fields because windows `4..33` are not all present.
- No live broker-backed end-to-end 35-window real-data benchmark was run in this phase.
- The new paper-ready summary is limited to the real-data workflow by design; other benchmark families remain unchanged in this phase.
