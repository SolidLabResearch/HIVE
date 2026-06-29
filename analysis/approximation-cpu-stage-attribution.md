# Approximation CPU Stage Attribution

Profiled run roots:

- approximation: `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-profiled/approximation/iteration1`
- chunked: `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-profiled/chunked/iteration1`

## Verdict

The approximation approach CPU is still dominated by the process that runs the RSP subqueries and publishes reusable results, not by the approximation operator math.

In the clean profiled 4Hz approximation rerun:

- process-tree CPU over the active interval: `25.92s`
- root PID CPU: `26.17s`
- child bee-worker PID CPU: `0.66s`

So about `97.5%` of the measured approximation CPU is in the root process, while only about `2.5%` is in the child worker that performs structured reusable-result consumption and final aggregation.

## Process split

Approximation process-tree summary:

| PID | PPID | Role inference | CPU-s | Share of tree CPU |
| --- | ---: | --- | ---: | ---: |
| `47922` | `47883` | root approximation process running subqueries / RSP / reusable-result production | `26.17` | `97.5%` |
| `47993` | `47922` | approximation bee worker / aggregation operator | `0.66` | `2.5%` |

This directly answers the main concern:

- the measured CPU is mostly not approximation math
- it is mostly upstream subquery execution, MQTT ingest, RSP execution, and reusable-result production

## Approximation stage timings

Source:

- [approximation_cpu_attribution_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-profiled/approximation/iteration1/approximation_cpu_attribution_summary.json)

These timings are from the bee worker process only.

| Stage | Count | Total ms | Mean ms | p95 ms | Max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| `approximation.structured_json_parse_ms` | 64 | 5.607 | 0.0876 | 0.2178 | 1.5678 |
| `approximation.structured_branch_decision_ms` | 64 | 5.402 | 0.0844 | 0.2853 | 0.8551 |
| `approximation.buffer_update_ms` | 64 | 0.954 | 0.0149 | 0.0603 | 0.1493 |
| `approximation.completed_window_readiness_check_ms` | 64 | 10.749 | 0.1679 | 0.9000 | 2.7960 |
| `approximation.aggregation_math_ms` | 30 | 1.825 | 0.0608 | 0.2485 | 0.3099 |
| `approximation.final_payload_json_stringify_ms` | 15 | 0.803 | 0.0535 | 0.1290 | 0.1290 |
| `approximation.final_mqtt_publish_total_ms` | 15 | 16.224 | 1.0816 | 6.8784 | 6.8784 |
| `approximation.diagnostics_write_ms` | 15 | 0.376 | 0.0251 | 0.0593 | 0.0593 |
| `approximation.mqtt_message_callback_total_ms` | 64 | 27.783 | 0.4341 | 1.4952 | 3.5982 |

Important note:

- `approximation.mqtt_message_callback_total_ms` is inclusive of parse, branch, buffer, and readiness work, so do not sum it with those sub-stages.

## What the numbers mean

### 1. Approximation math is negligible

`approximation.aggregation_math_ms = 1.825ms` total for the whole run.

That is:

- about `0.12ms` per finalized output window
- effectively zero compared with `25.92s` active process-tree CPU

Conclusion:

- the approximation algorithm itself is not the CPU problem

### 2. JSON work is also small

Approximation worker JSON costs:

- reusable-result parse: `5.607ms`
- final result stringify: `0.803ms`

Combined: `6.410ms`

Conclusion:

- JSON in the approximation worker is not materially driving the benchmark CPU

### 3. Diagnostics/log writes in the worker are tiny after the earlier patch

- diagnostics CSV writes: `0.376ms` total

Conclusion:

- after gating verbose console output, diagnostics are no longer a meaningful CPU contributor in the worker

### 4. Final-result MQTT publish is visible but still small

- final output publish total: `16.224ms` across `15` windows

Conclusion:

- output MQTT publish is measurable but still tiny relative to whole-run CPU

### 5. The remaining CPU is upstream

Because the worker process uses only about `0.66s` total CPU while the root process uses about `26.17s`, the remaining cost is overwhelmingly in:

- RSP subquery execution
- MQTT stream ingestion for raw inputs
- Turtle parsing / stream-to-RSP conversion
- RSPAgent reusable-result production
- root-process logging and wrapper work

That is the real explanation for why approximation still appears “high” even though it only emits `64` structured reusable results and `15` final outputs.

## Comparison against chunked

Chunked worker stage summary:

- [chunked_cpu_attribution_summary.json](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-profiled/chunked/iteration1/chunked_cpu_attribution_summary.json)

| Stage | Count | Total ms |
| --- | ---: | ---: |
| `chunked.structured_json_parse_ms` | 64 | 1.167 |
| `chunked.aggregation_recomposition_ms` | 15 | 0.067 |
| `chunked.final_payload_json_stringify_ms` | 15 | 0.052 |
| `chunked.final_mqtt_publish_total_ms` | 15 | 3.342 |
| `chunked.diagnostics_write_ms` | 31 | 0.842 |
| `chunked.mqtt_message_callback_total_ms` | 64 | 145.748 |

Comparison takeaways:

- chunked aggregation math is also negligible
- chunked’s worker callback path is heavier than approximation’s worker callback path
- despite that, chunked’s whole-approach CPU can still be lower because the dominant cost is not in the final aggregation worker but in the upstream process topology

## Message counts

Approximation profiled rerun:

- `raw_input_stream`: `7684`
- `reusable_result`: `64`
- `superquery_result`: `15`

Structured-only checks:

- windows `1..15` emitted exactly once
- `branch=structured` only
- `legacy_messages_seen=0`
- `adapted_legacy_messages_seen=0`
- `suppressed_missing_window_metadata=0`

## Resource sampler overhead

The process-tree sampler is not included in the measured approximation process tree.

Reason:

- the sampler runs in the benchmark harness process, outside the sampled root PID plus descendants
- the sampled tree includes only the approach root process and its child processes

So sampler overhead is not the explanation for approximation CPU.

## What is explained vs unexplained

Explained directly by process split:

- about `97.5%` of approximation CPU is in the root process, not the approximation worker

Explained directly inside the worker:

- the measured approximation operator stages total only a few tens of milliseconds
- approximation math itself is `1.825ms`

Remaining unaccounted within the root process:

- we do not yet have a preserved per-stage summary from the root approximation process for this clean rerun
- however, the per-PID attribution is already decisive: the root process is where the CPU is going

This is enough to explain well over `80%` of the active CPU path:

- the dominant bucket is “RSP subquery execution / RSPAgent reusable-result production and related upstream work”

## Safe optimization candidates

The next safe optimization target is upstream payload production, not approximation math.

Best candidates:

- reduce structured reusable-result payload size in benchmark mode
- remove `raw_bindings` from reusable-result payloads unless debug mode explicitly needs them
- avoid repeated producer-side serialization of fields that are not consumed by the approximation worker
- add preserved stage summaries for the root approximation process if a future audit needs a finer split inside the `97.5%` bucket

Lower-value candidates:

- approximation math tuning
- worker-side diagnostics changes
- worker-side JSON parse micro-optimizations

Those are too small to move the benchmark materially.

## Bottom line

Approximation CPU mostly comes from maintaining and running the subqueries and producing reusable results, not from approximating those results.

The approximation operator itself is cheap:

- parse/branch/buffer/readiness work is small
- aggregation math is negligible
- diagnostics are negligible

If more CPU reduction is needed, the next optimization should focus on the root process reusable-result production path, especially producer payload construction and benchmark-only metadata volume.
