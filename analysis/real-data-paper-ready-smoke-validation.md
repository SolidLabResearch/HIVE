# Real-Data Paper-Ready Smoke Validation

## Scope

Validated only the real-data workflow with:

- approaches: `fetching`, `approximation`, `chunked`, `naive_distributed`
- target windows: `35`
- analyzed windows: `4..33`
- replay frequency: `4 Hz`
- aggregation: `AVG`
- compact reusable payload: enabled
- completed-window approximation mode: enabled
- semantic-ready chunked mode: enabled
- process-tree CPU-seconds: enabled

## Commands Run

Fresh smoke execution:

- anchor-fixed fresh smoke launched in two isolated waves with a temporary Node wrapper around `experiments/real-data-comparison/run-real-data-4-approaches.js`
- wave 1: `fetching`, `approximation`, `naive_distributed`
- wave 2: `chunked`

Post-run analysis:

```bash
node experiments/real-data-comparison/run-real-data-4-approaches.js analyze-only
```

Controller evidence:

- `experiments/real-data-comparison/logs/controller/fetching-anchorfix-2026-06-24T19-06-50-564Z.log`
- `experiments/real-data-comparison/logs/controller/approximation-anchorfix-2026-06-24T19-06-50-564Z.log`
- `experiments/real-data-comparison/logs/controller/naive_distributed-anchorfix-2026-06-24T19-06-50-564Z.log`
- `experiments/real-data-comparison/logs/controller/chunked-anchorfix-2026-06-24T19-06-50-564Z.log`

## Artifact Paths

Top-level summaries:

- `experiments/real-data-comparison/logs/real_data_paper_ready_raw_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.csv`

Per-approach benchmark caps:

- `experiments/real-data-comparison/logs/fetching/iteration1/benchmark_window_cap_summary.json`
- `experiments/real-data-comparison/logs/approximation/iteration1/benchmark_window_cap_summary.json`
- `experiments/real-data-comparison/logs/chunked/iteration1/benchmark_window_cap_summary.json`
- `experiments/real-data-comparison/logs/naive_distributed/iteration1/benchmark_window_cap_summary.json`

Per-approach latency artifacts:

- `experiments/real-data-comparison/logs/fetching/iteration1/fetching_latency_log.csv`
- `experiments/real-data-comparison/logs/approximation/iteration1/approximation_latency_log.csv`
- `experiments/real-data-comparison/logs/chunked/iteration1/chunked_latency_log.csv`
- `experiments/real-data-comparison/logs/naive_distributed/iteration1/naive_distributed_latency_log.csv`

Per-approach process-tree resource artifacts:

- `experiments/real-data-comparison/logs/fetching/iteration1/fetching_client_side_process_tree_resource_usage.csv`
- `experiments/real-data-comparison/logs/approximation/iteration1/approximation_approach_process_tree_resource_usage.csv`
- `experiments/real-data-comparison/logs/chunked/iteration1/streaming_query_hive_process_tree_resource_log.csv`
- `experiments/real-data-comparison/logs/naive_distributed/iteration1/naive_distributed_approach_process_tree_resource_usage.csv`

Per-approach MQTT summaries:

- `experiments/real-data-comparison/logs/fetching/iteration1/mqtt_traffic_summary.json`
- `experiments/real-data-comparison/logs/approximation/iteration1/mqtt_traffic_summary.json`
- `experiments/real-data-comparison/logs/chunked/iteration1/mqtt_traffic_summary.json`
- `experiments/real-data-comparison/logs/naive_distributed/iteration1/mqtt_traffic_summary.json`

## Emitted Window Counts Per Approach

Verified from both `benchmark_window_cap_summary.json` and latency CSVs.

| Approach | Final windows | Count | Extra final window 36 | Result |
| --- | --- | ---: | --- | --- |
| fetching | `1..35` | `35` | no | pass |
| approximation | `1..35` | `35` | no | pass |
| chunked | `1..35` | `35` | no | pass |
| naive distributed | `1..35` | `35` | no | pass |

Notes:

- all four cap summaries report `stoppedAfterTargetWindows=true`
- all four cap summaries report `stopReason="target_window_count_reached"`
- all four latency CSVs contain exactly `35` data rows, with unique window numbers `1..35`
- fetching did log an internal post-cap candidate for window `36`, but it was not finalized and does not appear in any final latency artifact or cap summary

## Trimmed Window Range Confirmation

Confirmed from:

- `real_data_paper_ready_raw_summary.json`
- `real_data_paper_ready_trimmed-4-33_summary.json`
- `real_data_paper_ready_trimmed-4-33_summary.csv`

Observed:

- methodology label: `analyzedWindows = "4..33"`
- raw summary: `rawWindowCountMean = 35`, `trimmedWindowCountMean = 30`
- trimmed summary / CSV: all approaches report `matched_windows_mean = 30`

This proves the analysis kept all `35` emitted windows but only analyzed windows `4..33`.

## Latency Summary

Trimmed paper-ready latency means:

| Approach | Mean latency ms | Domain handling | Notes |
| --- | ---: | --- | --- |
| fetching | `25.7333` | direct window-close / trigger metadata | plausible |
| approximation | `2489.3667` | `wall_clock_mapped` | plausible |
| chunked | `61037.1333` | `wall_clock_mapped` | large but expected for comparable cadence mode |
| naive distributed | `0.3` | direct window-close / trigger metadata | plausible |

Detailed observations:

- fetching latency rows use `metadata_source=direct`
- approximation latency rows use `latency_domain_status=wall_clock_mapped`
- chunked latency rows use `latency_domain_status=wall_clock_mapped`
- naive distributed latency rows use `metadata_source=direct`
- no negative trimmed close-to-result latencies were reported in the paper-ready summaries

Chunked caveat:

- chunked comparable mode intentionally emits on the interval cadence rather than immediate chunk completion
- the observed chunked mean of about `61 s` is therefore a mode artifact, not a calculation bug
- the per-window values remained bounded at about `60.99 s .. 61.08 s`

## CPU-Seconds / RSS Summary

Trimmed paper-ready resource summary:

| Approach | CPU-seconds | CPU-seconds / window | Mean RSS MiB | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| fetching | `25.91` | `0.7403` | `182.7297` | `244.9688` |
| approximation | `30.45` | `0.87` | `139.1199` | `293.125` |
| chunked | `30.86` | `0.8817` | `211.7790` | `317.8906` |
| naive distributed | `40.15` | `1.1471` | `196.5384` | `244.1563` |

This confirms:

- CPU metric is CPU-seconds, not CPU%
- CPU-seconds per emitted window is present
- mean RSS and peak RSS are present for all approaches
- values are sourced from process-tree resource logs

## MQTT Summary

Trimmed paper-ready MQTT summary:

| Approach | Published bytes | Estimated delivery bytes | Message count |
| --- | ---: | ---: | ---: |
| fetching | `11,421,391` | `11,421,391` | `16,354` |
| approximation | `11,600,936` | `11,600,936` | `16,491` |
| chunked | `11,729,251` | `23,089,398` | `17,307` |
| naive distributed | `11,577,644` | `23,140,436` | `16,356` |

Mode-specific detail:

- approximation published `57,851` reusable-result bytes
- chunked published `38,724` reusable-result bytes and `285,481` chunk-result bytes
- fetching published only raw input plus superquery output

This is consistent with compact reusable-result payloads being active.

## Accuracy / Completeness Summary

Trimmed paper-ready summary against fetching:

| Approach | Matched windows | Missing | Extra | MAE | MAPE | RMSE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | `30` | `0` | `0` | `0` | `0` | `0` |
| approximation | `30` | `0` | `0` | `0.0022936525` | `0.0184534731` | `0.0034914685` |
| chunked | `30` | `0` | `0` | `2.6882200169590456e-14` | `2.1635692132354555e-13` | `2.6931975549283662e-14` |
| naive distributed | `30` | `0` | `0` | `0.0148524072` | `0.1195077841` | `0.0175900225` |

Conclusions:

- completeness fields are present for all approaches
- MAE / MAPE / RMSE are present for all approaches
- approximation matched all trimmed windows with no fallback artifact indicators
- chunked is effectively exact against fetching for the trimmed window set

Approximation fallback check:

- searched the fresh approximation logs for fallback markers
- found no `legacy fallback`, `legacy_fallback`, or generic fallback markers in the fresh run artifacts

## Chunked Latency Mode Resolution

Root cause:

- `experiments/real-data-comparison/run-real-data-4-approaches.js` was still forcing chunked comparable-cadence behavior for real-data via:
  - `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1`
  - `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0`
- this overrode the runtime defaults that otherwise favor semantic-ready immediate emission
- isolated rerun follow-up also exposed a second issue: when chunked was rerun alone against older fetching artifacts, cross-run benchmark anchors could invalidate exactness comparisons even when topic scoping was fixed

Config before:

- `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=1`
- `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY` unset
- `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=0`

Config after:

- `STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
- `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
- comparable output window matching and `STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35` stayed intact

Artifact paths:

- old comparable-cadence trimmed summary:
  - `experiments/real-data-comparison/logs/chunked-pre-semantic-fix-20260625-005126/real_data_paper_ready_trimmed-4-33_summary.json`
- semantic-ready chunked artifact set used for latency validation:
  - `experiments/real-data-comparison/logs/chunked/iteration1/chunked_latency_log.csv`
  - `experiments/real-data-comparison/logs/chunked/iteration1/benchmark_window_cap_summary.json`
  - `experiments/real-data-comparison/logs/chunked/iteration1/chunked_emission_proof.json`
- same-anchor fetching probe used to test exactness against the semantic-ready chunked run:
  - `experiments/real-data-comparison/logs/fetching/iteration1/fetching_latency_log.csv`
  - `experiments/real-data-comparison/logs/fetching/iteration1/benchmark_window_cap_summary.json`
  - `experiments/real-data-comparison/logs/fetching/iteration1.pre-semantic-align-20260625-015045/`

Old chunked latency mean:

- trimmed mean close-to-result latency: `61037.1333 ms`

New chunked latency mean:

- semantic-ready raw mean close-to-result latency across windows `1..35`: `3960.6571 ms`
- semantic-ready trimmed mean close-to-result latency across windows `4..33`: `3962.3667 ms`
- `ready_to_emit_ms` stayed near zero:
  - mean `0.8857 ms`
  - min `0 ms`
  - max `2 ms`

Window-cap verification:

- semantic-ready chunked emitted windows `1..35` exactly once
- no finalized window `36`
- cap summary reports:
  - `emittedFinalWindowCount=35`
  - `stoppedAfterTargetWindows=true`
  - `stopReason="target_window_count_reached"`
- trimmed analysis target remains `4..33`

Exactness result:

- semantic-ready chunked exactness regression is now resolved
- the backed-up broken semantic-ready artifact showed only one required 30s chunk interval per finalized 120s output window:
  - example broken row: `1782341589957-1782341619957`
- the fixed artifact now records four required 30s chunk intervals for each 120s output window:
  - example fixed row: `1782375324304-1782375354304|1782375354304-1782375384304|1782375384304-1782375414304|1782375414304-1782375444304`
- exactness was revalidated against the full fetching baseline restored from:
  - `experiments/real-data-comparison/logs/fetching/iteration1.pre-semantic-align-20260625-015045/`
- regenerated trimmed summary now reports:
  - `MAE = 2.6882200169590456e-14`
  - `MAPE = 2.1635692132354555e-13`
  - `RMSE = 2.6931975549283662e-14`
- completeness remains exact:
  - matched trimmed windows: `30`
  - fetching-only windows: `0`
  - approach-only windows: `0`

Final readiness decision:

- latency-mode resolution itself is successful: chunked is no longer stuck in comparable-cadence mode and now shows plausible semantic-ready latency
- semantic-ready chunked exactness is restored against fetching for the real-data benchmark
- real-data is paper-ready again for the intended semantic-ready chunked methodology under the current `35`-window / `4..33` trimmed analysis method

## Bugs Fixed During Validation

Only bugs exposed by validation were patched.

1. `src/streamer/src/publishing/StreamToMQTT.ts`

- fixed replay initialization so the publisher does not reinitialize the dataset twice
- fixed replay loop duration handling to avoid `TimeoutOverflowWarning` and invalid long sleeps

2. `experiments/utils/benchmarkReplayEnv.js`

- defaulted `STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR` to the benchmark start time when unset
- this fixed fetching window alignment and removed the bad latency / cross-approach drift caused by `null` anchors

3. `src/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.ts`

- minimal TypeScript nullability fix required to build the orchestrator cleanly for the smoke run

4. `analysis/accuracy/accuracy-comparison-custom-patterns.js`

- comparison now prefers matching by `windowNumber` when present

5. `scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js`

- timing attachment now prefers direct wall-clock-mapped latency when present
- fallback reconstruction is bounded and non-negative

6. `src/services/operators/StreamingQueryChunkAggregatorOperator.ts`

- semantic-ready chunked emission now buffers complete 30s chunk groups and recomposes the full external 120s window before immediate emission
- recomposed metadata now uses the external output window boundaries and trigger time instead of leaking internal chunk-window metadata

7. `src/services/operators/StreamingQueryChunkAggregatorOperator.test.ts`

- logical exactness coverage now reflects full 120s external-window semantics
- focused checks cover semantic-ready exactness, full required chunk intervals, and non-mixed latency fields

## Dependency / Flow Summary

Text flow for this validation:

```text
real-data publisher(s)
  -> approach orchestrator
  -> per-approach latency/resource/MQTT artifacts
  -> benchmark_window_cap_summary.json
  -> experiments/real-data-comparison/run-real-data-4-approaches.js analyze-only
  -> compareResults() + attachComparableTiming()
  -> real_data_paper_ready_raw_summary.json
  -> real_data_paper_ready_trimmed-4-33_summary.json
  -> real_data_paper_ready_trimmed-4-33_summary.csv
```

## Readiness Verdict

The original smoke-ready verdict is superseded by the chunked latency-mode resolution check.

Current status:

- latency semantics are corrected for real-data chunked runs
- window-cap behavior remains correct
- trimmed analysis still targets windows `4..33`
- CPU-seconds, RSS, MQTT, completeness, and latency fields remain present
- semantic-ready chunked exactness against fetching is restored

Current decision:

- real-data is ready for final paper runs under semantic-ready chunked mode
- the real-data chunked exactness investigation is complete
