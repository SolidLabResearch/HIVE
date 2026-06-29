# Tier 1 Low-Risk Smoke Validation

Date: 2026-06-25
Scope:

1. custom-pattern runner
2. window-parameter-sensitivity runner/extractor

No full 35-window benchmarks were run.

## Summary

- The low-risk paper-mode env wiring is active in both target families.
- Two smoke-exposed runner bugs were found and patched:
  - custom-pattern resource sampler wrote nonexistent per-sample mean fields
  - window-parameter-sensitivity runner exported a removed symbol
- Custom-pattern bounded smokes produced real fetching and chunked artifacts, including:
  - benchmark window-cap summaries with `targetWindowCount=35`
  - process-tree CPU-seconds summaries
  - MQTT traffic summaries
  - exact chunked-vs-fetching values for the emitted windows
- Chunked custom-pattern smoke showed full external-window recomposition:
  - each final window required four 30s chunk intervals for the 120s external window
  - final outputs were `metadata_source=reconstructed`
- Approximation custom-pattern smoke showed the new paper-mode status:
  - `approximation_status=completed_window_approximation`
  - no fallback counters were recorded
  - but the legacy pattern extractor still failed on the bounded approximation run even though the latency CSV had 2 completed-window rows
- Window-parameter-sensitivity smoke confirmed:
  - runtime metadata carries `target_window_count=35`, `trimmed_window_start=4`, `trimmed_window_end=33`
  - extractor outputs now include expected/matched/missing/extra fields for both raw and trimmed windows
  - existing `cpu_seconds`, `mae`, `mape`, and `rmse` fields remain present
- The bounded window-parameter sensitivity smoke still ended with runner status `FAILED` because the orchestrator exited before publisher completion. Despite that, the extractor output CSVs were produced and the new completeness fields were present.

## Bugs Found During Smoke

### 1. Custom-pattern runner resource sampler bug

Symptom:

- bounded custom-pattern smoke crashed during cleanup

Root cause:

- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `AttemptResourceSampler.writeArtifacts()` expected `sample.meanCpuPct` and `sample.meanRssMb`
- `collectTreeMetrics(...)` does not emit those fields

Patch:

- aligned the custom-pattern sampler CSV shape with the already-correct K-scaling implementation
- removed writes of nonexistent per-sample mean fields

### 2. Window-parameter-sensitivity stale export bug

Symptom:

- runner crashed immediately with:
  - `ReferenceError: collectTreeStatsFromPs is not defined`

Root cause:

- stale export in `experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js`

Patch:

- removed the dead export reference only

## Validation Commands

### Static checks

```bash
node --check experiments/pattern-analysis/run-custom-patterns-comparison.js
node --check experiments/window-parameter-sensitivity/common.js
node --check experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js
node --check experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js
```

### Focused unit tests

```bash
npx jest experiments/window-parameter-sensitivity/common.test.js experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js --runInBand
```

### Custom-pattern bounded smokes

Fetching:

```bash
CUSTOM_PATTERN_SELECTED_APPROACHES=fetching,naive_distributed,approximation,chunked \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=150 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability -i 1 --pattern-test-timeout 240000
```

Approximation:

```bash
CUSTOM_PATTERN_SELECTED_APPROACHES=approximation \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=150 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability -i 1 --pattern-test-timeout 240000
```

Chunked:

```bash
CUSTOM_PATTERN_SELECTED_APPROACHES=chunked \
STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=150 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js low_variability -i 1 --pattern-test-timeout 240000
```

### Window-parameter sensitivity smoke

Rejected too-short probe:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js \
  --experiment superquery-range-scaling \
  --iterations 1 \
  --ranges 120 \
  --patterns low_variability \
  --approaches fetching,chunked \
  --replay-duration-seconds 150 \
  --timeout-ms 240000 \
  --skip-build \
  --log-root logs/window-parameter-sensitivity-smoke \
  --result-root results/window-parameter-sensitivity-smoke
```

Accepted minimal comparable probe:

```bash
node experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js \
  --experiment superquery-range-scaling \
  --iterations 1 \
  --ranges 120 \
  --patterns low_variability \
  --approaches fetching,chunked \
  --replay-duration-seconds 180 \
  --timeout-ms 240000 \
  --skip-build \
  --log-root logs/window-parameter-sensitivity-smoke \
  --result-root results/window-parameter-sensitivity-smoke
```

## Custom-Pattern Findings

### Env wiring

Verified by source-level env block in:

- [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)

Explicit flags present:

- `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35`
- `STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1`
- `STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1`
- `STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0`
- `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`

Runtime corroboration:

- `benchmark_window_cap_summary.json` written for fetching, approximation, and chunked contains:
  - `targetWindowCount: 35`
  - `emittedFinalWindowCount: 2`
  - `finalWindowNumbers: [1, 2]`

Artifact examples:

- [benchmark_window_cap_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/benchmark_window_cap_summary.json)
- [benchmark_window_cap_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/benchmark_window_cap_summary.json)
- [benchmark_window_cap_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/benchmark_window_cap_summary.json)

### Fetching smoke

Artifacts produced:

- [fetching_results.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/fetching_results.csv)
- [resource_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/resource_summary.json)
- [mqtt_traffic_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/mqtt_traffic_summary.json)

Observed:

- 2 finalized windows emitted
- process-tree CPU-seconds present:
  - `cpuSeconds: 10.37`
- MQTT artifact present with bytes/counts
- first 2 result values:
  - window 1: `-23.011740634095613`
  - window 2: `-23.011956228690238`

### Approximation smoke

Artifacts produced:

- [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_latency_log.csv)
- [resource_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/resource_summary.json)
- [hive_profile_summary.aggregate.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/hive_profile_summary.aggregate.json)

Observed:

- latency CSV contains 2 rows
- each row is tagged:
  - `approximation_status=completed_window_approximation`
  - `metadata_source=reconstructed`
- process-tree CPU-seconds present:
  - `cpuSeconds: 11.61`
- profile counters show no legacy fallback:
  - `fallback_original_agent_rsps_started: 0`
  - `compatible_queries_detected: 0`
  - `emitted_results: 0` in the profile aggregate, so the profile summary itself is not yet useful for approximation completeness in this bounded run

Issue:

- the legacy custom-pattern extraction script reported `Extracted 0 results` even though the bounded latency CSV contains 2 completed-window rows
- this looks like an existing extractor limitation in the bounded approximation path, not evidence that the new paper env wiring failed

### Chunked smoke

Artifacts produced:

- [chunked_results.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_results.csv)
- [chunked_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_latency_log.csv)
- [chunked_window_diagnostics.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_window_diagnostics.csv)
- [resource_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/resource_summary.json)
- [mqtt_traffic_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/mqtt_traffic_summary.json)

Observed:

- 2 finalized windows emitted
- process-tree CPU-seconds present:
  - `cpuSeconds: 10.54`
- MQTT artifact present with:
  - chunk traffic
  - reusable-result traffic
  - superquery-result traffic

Exactness vs fetching:

- fetching and chunked values match to floating precision for both emitted windows
- examples:
  - window 1:
    - fetching: `-23.011740634095613`
    - chunked: `-23.011740634095634`
  - window 2:
    - fetching: `-23.011956228690238`
    - chunked: `-23.011956228690227`

Semantic-ready full-window evidence:

- `chunked_latency_log.csv` shows:
  - `trigger_type=immediate`
  - `emission_reason=immediate_ready_check`
  - `metadata_source=reconstructed`
- `required_chunk_intervals` for each external output window contains 4 chunk intervals, not a single internal chunk
- example for window 1:
  - `1716454104620-1716454134620`
  - `1716454134620-1716454164620`
  - `1716454164620-1716454194620`
  - `1716454194620-1716454224620`
- `chunked_window_diagnostics.csv` confirms external window 1 was recomposed from four internal chunk groups covering the full 120s external window

Conclusion:

- chunked is emitting recomposed external-window results, not single internal chunks

### Custom-pattern overall assessment

- Env wiring: passed
- process-tree CPU-seconds artifact generation: passed
- MQTT artifact generation: passed
- chunked semantic-ready full-window recomposition: passed
- chunked exactness against fetching for emitted windows: passed
- approximation completed-window mode flagging: passed
- approximation extractor success under bounded smoke: failed, likely legacy extractor issue

## Window-Parameter Sensitivity Findings

### Metadata propagation

Verified in runtime metadata:

- [run_metadata.json](/Users/kushbisen/Code/streaming-query-hive/logs/window-parameter-sensitivity-smoke/superquery-range-scaling/chunked/low_variability/range-120s/iteration1/run_metadata.json)

Observed:

- `target_window_count: 35`
- `trimmed_window_start: 4`
- `trimmed_window_end: 33`

This confirms the shared env metadata patch is flowing into real run artifacts.

### Extractor field preservation and completeness fields

Verified in:

- [superquery_range_scaling_per_run.csv](/Users/kushbisen/Code/streaming-query-hive/results/window-parameter-sensitivity-smoke/superquery-range-scaling/superquery_range_scaling_per_run.csv)
- [superquery_range_scaling_aggregate.csv](/Users/kushbisen/Code/streaming-query-hive/results/window-parameter-sensitivity-smoke/superquery-range-scaling/superquery_range_scaling_aggregate.csv)

Per-run fields now present:

- `expected_window_count`
- `actual_window_count`
- `missing_window_count`
- `extra_window_count`
- `matched_window_count`
- `trimmed_expected_window_count`
- `trimmed_matched_window_count`
- `trimmed_missing_window_count`
- `trimmed_extra_window_count`
- `trimmed_actual_window_count`

Existing fields preserved:

- `cpu_seconds`
- `mae`
- `mape`
- `rmse`

Observed per-run values in the smoke:

- `expected_window_count=35`
- `actual_window_count=1`
- `missing_window_count=34`
- `extra_window_count=0`
- `matched_window_count=1`
- `trimmed_expected_window_count=30`
- `trimmed_matched_window_count=0`
- `trimmed_missing_window_count=30`
- `trimmed_extra_window_count=1`
- `trimmed_actual_window_count=1`

### Sensitivity smoke runtime status

The minimal accepted `180s` sensitivity smoke still ended with:

- `validity_reason=Orchestrator exited unexpectedly before publisher completion`

for both fetching and chunked.

However:

- run metadata was written
- extractor output CSVs were written
- the new completeness fields were present
- `cpu_seconds` fields were present

So for this task, the extractor wiring was validated even though the bounded run itself did not end cleanly.

## Final Assessment

### Custom patterns

- explicit paper env wiring: validated
- target window cap propagation: validated
- chunked semantic-ready immediate full-window recomposition: validated
- process-tree CPU-seconds artifacts: validated
- MQTT artifacts: validated
- accuracy outputs for fetching/chunked emitted windows: validated
- approximation structured-only completed-window mode: validated
- approximation bounded-smoke extraction: still problematic

### Window-parameter sensitivity

- shared metadata fields `35 / 4 / 33`: validated
- extractor raw completeness fields: validated
- extractor trimmed completeness fields: validated
- preserved `cpu_seconds`, `mae`, `mape`, `rmse`: validated
- bounded runner stability: not fully validated due early orchestrator completion status

## Changed Files During This Smoke Turn

Bug fixes only, both directly exposed by the smoke:

- [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)
  - fixed resource sampler CSV writing bug
- [run-window-parameter-sensitivity.js](/Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js)
  - removed stale `collectTreeStatsFromPs` export

## Recommended Next Step

1. Audit the bounded custom-pattern approximation extractor path so it can consume `completed_window_approximation` rows without reporting zero results.
2. Audit why the bounded window-parameter-sensitivity runner marks both approaches as failed even though artifacts and extracted rows are present.
