# Custom-Pattern Approximation Bounded Smoke Audit

## Scope

- Approach: approximation only
- Pattern: `low_variability` only
- Mode: bounded smoke only

No code was changed in this audit.

## Artifacts inspected

- [approximation_approach_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_approach_log.csv)
- [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_latency_log.csv)
- [publisher.log](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/publisher.log)
- [mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic.ndjson)
- [mqtt_traffic_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic_summary.json)
- [benchmark_window_cap_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/benchmark_window_cap_summary.json)
- [attempt_metadata.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/attempt_metadata.json)
- Runner code: [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)
- Publisher code: [publish.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publish.ts)
- Replay code: [StreamToMQTT.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publishing/StreamToMQTT.ts)
- Working comparison artifact:
  - [run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1/run_summary.json)
  - [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1/approximation_latency_log.csv)

## Root cause

The bounded custom-pattern smoke was run without enabling finite replay mode.

Evidence:

- The publisher log begins with:
  - `finiteReplayMode=false`
- The custom-pattern runner does not set `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1` itself in [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js).
- The paper benchmark wrapper does set that flag for the custom-pattern family in [run-all-paper-benchmarks.js](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js).

Because finite replay was off:

- `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=150` had no effect on publisher control flow.
- The publisher never emitted a finite-replay completion control message.
- The approximation orchestrator never received a finite-replay completion signal.
- Target-window cap logic never had a chance to stop on emitted final windows because no final windows were ever emitted.

The bounded smoke artifact also shows the publisher was terminated after only a few seconds, long before approximation could emit a completed 120s output window.

## Key observations

### Replay lifetime vs required window closures

From [attempt_metadata.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/attempt_metadata.json):

- Output window range: `120000 ms`
- Output window step: `60000 ms`
- Subwindow range: `60000 ms`
- Subwindow step: `30000 ms`
- Benchmark anchor: `1716454104620`

Required event-time closures:

- First subquery full-window close: `1716454164620` (`anchor + 60000`)
- First output full-window close: `1716454224620` (`anchor + 120000`)

Actual bounded smoke replay observed:

- Publisher emitted only `8` raw input messages total
- MQTT topics seen:
  - `4` on `.../smartphoneX`
  - `4` on `.../wearableX`
- Publisher log activity spans roughly:
  - first: `2026-06-25T10:20:32.871Z`
  - last: `2026-06-25T10:20:33.713Z`

That is nowhere near enough replayed event-time to close either:

- the first 60s subquery window
- or the first 120s output window

## Answers

### 1. Does approximation receive structured reusable_result messages during the bounded run?

No.

Evidence:

- [approximation_approach_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_approach_log.csv) shows:
  - `structured_messages_seen: 0`
  - `legacy_messages_seen: 0`
- [mqtt_traffic_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic_summary.json) shows:
  - `reusable_result_published_bytes: 0`
  - `subquery_result_published_bytes: 0`
- [mqtt_traffic.ndjson](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/mqtt_traffic.ndjson) contains only `raw_input_stream` messages.

### 2. Are subquery windows closing before the bounded replay ends?

No.

Evidence:

- First full subquery close requires `+60s` of event-time.
- The bounded artifact shows only the first `4` observations per stream were published.
- No subquery result MQTT traffic exists.

### 3. Is the replay duration too short for approximation’s completed-window mode?

For this failing bounded artifact, the operative issue is not the configured duration value. The operative issue is that finite replay mode was off, so the configured finite replay duration was ignored.

More precisely:

- The command supplied `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS=150`.
- But [publish.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publish.ts) only uses that duration when `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1`.
- The actual run shows `finiteReplayMode=false`.

So the relevant failure is:

- not "150s is too short"
- but "finite replay duration was never activated"

### 4. Is the approximation operator waiting for a later subwindow/full-window closure than fetching/chunked?

Approximation completed-window mode is waiting for full semantic window completeness, which is expected.

For this audit, there is no evidence of an approximation-specific over-wait beyond intended semantics.

Evidence:

- [approximation_approach_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_approach_log.csv) confirms:
  - `completedWindowMode=true`
  - `earlyTriggerMode=false`
- A working bounded approximation run exists at [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1/approximation_latency_log.csv), which emits completed windows 1..15 with `completed_window_approximation`.

So the bounded smoke failure is upstream of approximation readiness. The operator never receives enough subquery state to attempt a completed-window emission.

### 5. Is the runner terminating approximation too early because publisher completion/cap logic is wrong?

Indirectly, yes for the smoke invocation shape, but not because of approximation cap logic itself.

Observed behavior:

- The runner finalizes the attempt when the publisher process closes.
- In this failing artifact, the publisher closes after only a few seconds and the runner then immediately tears down approximation.

But the main underlying reason is:

- the publisher was not running in finite replay mode
- the bounded smoke invocation therefore did not use the paper-style bounded replay control path

Evidence:

- [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js) sets paper-mode approximation flags and target windows, but does not itself set `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1`.
- [run-all-paper-benchmarks.js](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js) does set `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1` for the custom-pattern family.
- [benchmark_window_cap_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/benchmark_window_cap_summary.json) shows:
  - `emittedFinalWindowCount: 0`
  - `stoppedAfterTargetWindows: false`

So cap logic did not fire because there were no final windows to count.

### 6. Is this only a bounded-smoke artifact, or would it affect the full 35-window run?

This is a bounded-smoke artifact for the manual invocation shape that omitted finite replay enablement.

Evidence:

- The working bounded approximation artifact at [run_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1/run_summary.json) shows:
  - `targetWindowCount: 15`
  - `emittedFinalWindowCount: 15`
  - `stoppedAfterTargetWindows: true`
- Its corresponding [approximation_latency_log.csv](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact-full/approximation/iteration1/approximation_latency_log.csv) contains completed rows for windows `1..15`.
- The paper benchmark wrapper already injects `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1` for custom patterns in [run-all-paper-benchmarks.js](/Users/kushbisen/Code/streaming-query-hive/scripts/benchmark/run-all-paper-benchmarks.js).

## Decision

Bounded-smoke-only caveat.

The evidence does not show a paper-methodology defect in approximation completed-window mode itself. It shows the manual bounded custom-pattern smoke was not launched with finite replay enabled, so it never exercised the same control path used by the paper-ready custom-pattern benchmark wrapper.

## Fix decision

No code patch applied in this audit.

Reason:

- The root cause is concrete and configuration-level.
- The existing paper benchmark wrapper already sets the needed finite replay flag for custom patterns.
- The failing artifact does not justify changing approximation logic.

## Minimal patch point if a safety guard is still desired later

If you want the custom-pattern runner to be self-sufficient outside the paper wrapper, the minimal patch point is:

- [run-custom-patterns-comparison.js](/Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)

Specifically:

- set `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY=1` whenever `STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS` is provided for bounded benchmark-style runs

That would make direct runner invocations consistent with the paper wrapper, but I did not apply it here.
