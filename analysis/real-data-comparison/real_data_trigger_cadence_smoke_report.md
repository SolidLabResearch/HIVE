# Real-Data Trigger Cadence Smoke Report

- Branch: `chunk-state-reuse-design`
- Commit: `690ea8fdc49115bc1aa56742a461386766027514`
- Dirty state: yes
- Smoke wrapper output dir: `/tmp/real-data-3approach-trigger-cadence-smoke`
- Real-data runner log root used by the run: `/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/logs`

## Command

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
OUTPUT_WINDOW_RANGE=120000 \
OUTPUT_WINDOW_STEP=60000 \
SUB_WINDOW_RANGE=60000 \
SUB_WINDOW_STEP=30000 \
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 1 \
  --approaches fetching,approximation,chunked \
  --target-windows 5 \
  --output-dir /tmp/real-data-3approach-trigger-cadence-smoke
```

## Paper Window Configuration

| Variable | Value |
| --- | ---: |
| `OUTPUT_WINDOW_RANGE` | `120000` |
| `OUTPUT_WINDOW_STEP` | `60000` |
| `SUB_WINDOW_RANGE` | `60000` |
| `SUB_WINDOW_STEP` | `30000` |

## Scope Validation

- Code inspection confirmed fallback comparable trigger mapping is `firstRawInputAt + (windowNumber * OUTPUT_WINDOW_STEP)` in [scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js:633).
- The older `OUTPUT_WINDOW_RANGE + (windowNumber - 1) * OUTPUT_WINDOW_STEP` fallback is not used there.
- The real-data runner imports that shared helper in [experiments/real-data-comparison/run-real-data-4-approaches.js](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js:10).
- The runner records `outputWindowRangeMs`, `outputWindowStepMs`, and `firstOutputTriggerOffsetMs` in its paper-analysis metadata in [experiments/real-data-comparison/run-real-data-4-approaches.js](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js:833).
- Cadence verification exists for both raw rows and trimmed `4..33` rows in [experiments/real-data-comparison/run-real-data-4-approaches.js](/Users/kushbisen/Code/streaming-query-hive/experiments/real-data-comparison/run-real-data-4-approaches.js:794).

## Completeness

| Approach | Started | Fresh summary for this smoke | Finalized windows seen | Usable result rows trusted for this smoke | Status |
| --- | --- | --- | --- | --- | --- |
| fetching | yes | yes | `1,2,3,4,5` | 5 | completed |
| approximation | yes | yes | `1,2,3,4` | not trustworthy | incomplete |
| chunked | no | no | none | 0 | not started |
| naive_distributed | no | no | none | 0 | not requested |

## Latency Domain And Cadence

| Approach | Current smoke evidence | Close-to-result status | Trigger cadence status | Notes |
| --- | --- | --- | --- | --- |
| fetching | fresh `fetching_latency_log.csv` and fresh target summary | nonnegative comparable latency on windows `1..5` via direct `latency_from_window_close_ms=0` | passes `n * 60000` for windows `1..5` | wrapper log shows accepted/finalized windows at `1..5` and target stop at window `5` |
| approximation | fresh target summary only; sibling latency and MQTT summaries are stale | not trustworthy for current smoke | not fully verifiable end-to-end | fresh summary says only windows `1..4`, but `approximation_latency_log.csv` still contains an older 5-window trace |
| chunked | no fresh current-smoke artifacts | unavailable | unavailable | chunked never started before the smoke was stopped |

## Accuracy Vs Fetching

| Comparison | MAE | RMSE | MAPE | Max abs error | Matched windows | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| approximation vs fetching | n/a | n/a | n/a | n/a | n/a | not computable from trustworthy current-smoke artifacts |
| chunked vs fetching | n/a | n/a | n/a | n/a | n/a | not computable because chunked did not start |

## Resources

| Approach | CPU seconds | Mean RSS MiB | Peak RSS MiB | Notes |
| --- | ---: | ---: | ---: | --- |
| fetching | 9.29 | 148.85 | 238.25 | computed from fresh process-tree samples for the completed 5-window smoke |
| approximation | 7.83 | 187.48 | 324.97 | process-tree samples are fresh, but the approach did not complete the requested 5-window smoke |
| chunked | n/a | n/a | n/a | no fresh current-smoke samples |

## Warnings And Errors

- The smoke did not finish all requested approaches. `chunked` never started before the run was stopped.
- The current approximation smoke left mixed-age artifacts in `experiments/real-data-comparison/logs/approximation/iteration1`.
- Fresh file: `benchmark_window_cap_summary.json` at `2026-06-30 18:04:31`.
- Stale sibling files: `approximation_latency_log.csv`, `approximation_approach_log.csv`, and `mqtt_traffic_summary.json` at `2026-06-30 17:00:29`.
- Because of that mismatch, current-smoke approximation latency and accuracy cannot be trusted end-to-end.
- The wrapper `--output-dir` captured only wrapper metadata/stdout/stderr. The real-data runner still wrote approach artifacts into the shared repo log root, which increases stale-artifact risk.
- Fetching validated the corrected trigger semantics, but its accepted windows were still marked `incomplete_span` or `incomplete_count` in diagnostics, so this startup smoke is intentionally using first-emitted startup semantics rather than full steady-state completeness.

## Recommendation

`not safe yet`

### Exact blocker to fix next

The real-data smoke path is still not trustworthy for a server benchmark because the current approximation run can leave stale per-iteration latency/MQTT artifacts in place while writing a fresh benchmark summary, and the three-approach smoke did not reach chunked at all. Until the runner either isolates each smoke into a fresh log root or clears per-iteration files before each run, approximation and chunked accuracy/latency cannot be validated end-to-end from smoke artifacts.
