# Approximation vs Chunked CPU Boundary Audit

Run root:

- approximation: `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-vs-chunked-cpu-boundary-audit/approximation/iteration1`
- chunked: `/Users/kushbisen/Code/streaming-query-hive/logs/approximation-vs-chunked-cpu-boundary-audit/chunked/iteration1`

Benchmark envelope:

- dataset: `approximation_test/challenging/exponential_growth`
- replay rate: `4 Hz`
- output window target: `15`
- output window config: `RANGE 120000 STEP 60000`
- subwindow config: `RANGE 60000 STEP 30000`
- aggregation: `AVG`
- approximation branch constraints: compact structured-only reusable results, `legacy fallback = 0`
- chunked branch constraints: exact chunk recomposition, `comparable_output_only = 1`, `immediate_trigger = 1`

All final runs emitted windows `1..15` exactly once and stopped via `target_window_count_reached`.

## Verdict

The measured CPU difference is a pipeline-boundary and producer-path difference, not evidence that approximation math is more expensive than chunked recomposition.

Do not compare approximation root CPU to chunked root CPU as if they cover equivalent work:

- approximation root includes the active reusable-result producer path (`RSPAgent` + subquery RSP execution + reusable-result publish)
- chunked root does not run the equivalent producer path for this benchmark shape; it only registers compatible chunk reuse and hands off work to the bee-worker subtree
- chunked’s heavy producer/recomposition work is in the bee worker and its shared chunk producers, not in the root orchestrator

## Final Conclusion

The measured CPU difference is a pipeline-boundary and producer-path difference, not evidence that approximation math is more expensive than chunked recomposition.

## Process Trees

### Approximation

Measured tree root:

- root PID `20395`, command `node`
- child PID `20468`, command `/opt/homebrew/Cellar/node/25.9.0_2/bin/node`

Per-PID CPU from the finished process-tree summary:

| PID | Role | CPU-s | Active wall interval |
| --- | --- | ---: | ---: |
| `20395` | approximation root orchestrator + `RSPAgent` producers | `31.46` | `963.3s` |
| `20468` | approximation bee worker / approximation operator | `0.79` | `962.5s` |

Total active CPU from non-negative process-tree deltas:

- approximation total active CPU-s: `32.25`

Overlap and inclusion:

- the process-tree sampler is outside the measured tree
- benchmark publisher/replayer are outside the measured tree
- the measured tree starts at the orchestrator root and includes its bee-worker child
- equivalent upstream producer work is inside the approximation root tree

### Chunked

Measured tree root:

- root PID `2908`, command `node`
- child PID `3027`, command `/opt/homebrew/Cellar/node/25.9.0_2/bin/node`

Per-PID CPU from the finished process-tree summary:

| PID | Role | CPU-s | Active wall interval |
| --- | --- | ---: | ---: |
| `2908` | chunked root orchestrator | `2.25` | `962.8s` |
| `3027` | chunked bee worker + shared chunk producers + recomposition | `17.24` | `962.1s` |

Total active CPU from non-negative process-tree deltas:

- chunked total active CPU-s: `19.49`

Overlap and inclusion:

- the process-tree sampler is outside the measured tree
- benchmark publisher/replayer are outside the measured tree
- the measured tree starts at the chunked orchestrator root and includes its bee-worker child
- equivalent chunk production and recomposition work is concentrated in the chunked bee-worker subtree, not in the root

## Comparable Work Boundary

The roots are not performing comparable work.

### Approximation root

Included in the measured root:

- two live `RSPAgent` subquery producers
- raw MQTT stream subscription and input parsing for those subqueries
- RSP callback execution
- structured reusable-result payload construction
- JSON serialization of reusable results
- MQTT publish of reusable results

Evidence:

- root profile counters show `rsp_engines_created=2`
- root profile counters show `mqtt_messages_received=7683`
- root profile counters show `mqtt_messages_published=64`
- root profile counters show `rsp_agent_rstream_callbacks=64`
- root profile counters show `rsp_agent_binding_rows=384`

### Chunked root

Included in the measured root:

- compatible chunk-reuse detection
- query registration only
- bee-worker handoff

Not included in the chunked root:

- no `RSPAgent` producer callbacks
- no raw-subquery `RSPAgent` stream processing
- no reusable-result production loop

Evidence:

- root profile counters show `compatible_queries_detected=2`
- root profile counters show `original_agent_rsps_skipped=2`
- root profile counters show `rsp_engines_created=0`
- root profile counters show `mqtt_messages_received=0`
- root profile counters show `mqtt_messages_published=0`

This is the main fairness issue: approximation root CPU includes active producer work that chunked root explicitly skips.

## Pipeline Architecture

### Approximation path

Where subqueries run:

- in the approximation root process via two `RSPAgent` instances created by `Orchestrator.addSubQuery()`

Where `RSPAgent` runs:

- in the approximation root process

Where reusable-result payloads are built:

- in the approximation root process inside `RSPAgent.subscribeRStream()`

Where approximation aggregation runs:

- in the approximation bee worker child (`ApproximationApproachOperator`)

Which process publishes final `superquery_result`:

- the approximation bee worker child

Net effect:

- approximation upstream producer cost is root-heavy
- approximation math/final aggregation cost is child-light

### Chunked path

Where subqueries run:

- rewritten chunk subqueries run in shared `RSPQueryProcess` instances created by the chunked bee worker

Where `RSPAgent` runs:

- for this benchmark shape, the chunked root skips the original `RSPAgent` path through compatible chunk reuse

Where reusable-result payloads are built:

- not by the chunked root for the original subqueries
- instead, the bee worker derives original reusable outputs from chunk results when needed

Where `chunk_result` payloads are built:

- in shared chunk producer processes started by the chunked bee worker (`rsp_query_processes_started=2`)

Where chunk recomposition runs:

- in the chunked bee worker child

Which process publishes final `superquery_result`:

- the chunked bee worker child

Net effect:

- chunked root is orchestration-only
- chunked bee-worker subtree carries both producer-side and recomposition-side work

## Required Distinction

### 1. Approximation operator cost

This is the approximation bee-worker child only, not the approximation root.

Finished worker stage summary:

| Stage | Count | Total ms |
| --- | ---: | ---: |
| `approximation.structured_json_parse_ms` | `64` | `4.61` |
| `approximation.structured_branch_decision_ms` | `64` | `13.62` |
| `approximation.buffer_update_ms` | `64` | `4.59` |
| `approximation.completed_window_readiness_check_ms` | `64` | `12.18` |
| `approximation.aggregation_math_ms` | `30` | `2.52` |
| `approximation.final_payload_json_stringify_ms` | `15` | `0.81` |
| `approximation.final_mqtt_publish_total_ms` | `15` | `9.29` |
| `approximation.diagnostics_write_ms` | `15` | `0.42` |

Key point:

- approximation math itself is only `2.52ms` total
- the whole approximation operator child used only `0.79 CPU-s`

This is not the source of the `32.25 CPU-s` tree total.

### 2. Approximation producer / `RSPAgent` cost

This is the approximation root process.

Finished root stage summary:

| Stage | Count | Total ms |
| --- | ---: | ---: |
| `orchestrator.rsp_agent_setup_ms` | `2` | `60.53` |
| `orchestrator.rsp_agent_stream_process_start_ms` | `2` | `3.51` |
| `rsp_agent.binding_extraction_ms` | `64` | `3.23` |
| `rsp_agent.reusable_payload_build_ms` | `64` | `2.12` |
| `rsp_agent.reusable_json_stringify_ms` | `64` | `0.41` |
| `rsp_agent.reusable_mqtt_publish_total_ms` | `64` | `2.41` |
| `rsp_agent.rstream_callback_total_ms` | `64` | `9.02` |

Finished root counters:

| Counter | Value |
| --- | ---: |
| `mqtt_messages_received` | `7683` |
| `mqtt_messages_published` | `64` |
| `rsp_engines_created` | `2` |
| `rsp_agent_rstream_callbacks` | `64` |
| `rsp_agent_binding_rows` | `384` |
| `rsp_agent_json_serializations` | `64` |
| `rsp_agent_output_construction_calls` | `64` |
| `rsp_agent_reusable_payload_bytes` | `24977` |

Interpretation:

- the visible reusable-result callback path is cheap in wall-time totals
- the approximation root still consumed `31.46 CPU-s`
- therefore most root CPU is upstream raw-input/RSP execution work around those callbacks, not approximation math

### 3. Chunked root orchestration cost

The patched chunked rerun finished cleanly, but it still did not emit a dedicated `chunked_root_cpu_attribution_summary.json`.

Reason:

- the flush path was fixed
- however the chunked root has counters but no meaningful stage-timed blocks analogous to approximation’s `RSPAgent` callback stages, so no dedicated root stage file was produced in the finished rerun

What the finished patched rerun does prove:

- root CPU is only `2.25 CPU-s`
- root counters are only:
  - `compatible_queries_detected=2`
  - `original_agent_rsps_skipped=2`

So chunked root orchestration cost is small and does not include equivalent producer work.

### 4. Chunked bee-worker / shared chunk producer cost

This is where the chunked pipeline spends its CPU.

Finished worker process-tree CPU:

- chunked bee worker subtree CPU-s: `17.24`

Finished worker counters:

| Counter | Value |
| --- | ---: |
| `rsp_query_processes_started` | `2` |
| `shared_chunk_producers_created` | `2` |
| `mqtt_messages_received` | `7746` |
| `mqtt_messages_published` | `451` |
| `chunk_state_messages_published` | `64` |
| `buffered_chunk_results` | `64` |
| `chunk_groups_completed` | `32` |
| `comparable_windows_emitted` | `15` |
| `reconstructed_superquery_results` | `15` |
| `original_agent_outputs_derived_from_chunks` | `62` |
| `rsp_engines_created` | `2` |

Finished worker stage summary:

| Stage | Count | Total ms |
| --- | ---: | ---: |
| `chunked.structured_json_parse_ms` | `64` | `1.77` |
| `chunked.aggregation_recomposition_ms` | `15` | `0.03` |
| `chunked.final_payload_json_stringify_ms` | `15` | `0.05` |
| `chunked.final_mqtt_publish_total_ms` | `15` | `3.38` |
| `chunked.diagnostics_write_ms` | `31` | `0.93` |
| `chunked.mqtt_message_callback_total_ms` | `64` | `149.31` |

Key point:

- chunk recomposition itself is only `0.03ms` total
- chunked cost is dominated by the bee-worker/shared-producer pipeline, not by recomposition math

## Equivalent RSP/Subquery Work Inclusion

### Is approximation root doing `RSPAgent` work that chunked does in a child process?

Yes.

- approximation root directly runs the original subquery `RSPAgent` producer path
- chunked root skips that path
- chunked bee worker instead starts shared chunk producers and handles derived outputs there

### Is chunked RSP work outside the measured tree?

No.

- chunked shared chunk producers are inside the bee-worker subtree
- they are included in the measured process tree

### Are worker processes spawned differently?

Yes.

- approximation: root orchestrator creates `RSPAgent`s, then forks one bee worker for approximation aggregation
- chunked: root orchestrator forks one bee worker, and that bee worker creates two shared `RSPQueryProcess` producers internally

### Are publisher/replayer/broker excluded consistently?

Yes.

- benchmark publisher/replayer are outside both measured trees
- the process-tree sampler is outside both measured trees
- broker is not inside either measured tree

### Are benchmark wrappers included inconsistently?

No material inconsistency was found in the wrapper boundary itself.

The inconsistency is inside the application pipeline:

- approximation root includes producer execution
- chunked root does not

## RSPAgent / Output Path Comparison

### Approximation structured reusable-result path

- `64` `RSPAgent` callbacks
- `384` binding rows extracted
- `64` reusable-result JSON serializations
- `24977` bytes of reusable-result payload content at the root profile counter level
- MQTT traffic summary: `64` reusable-result publishes, `27537` published bytes
- final output publishes: `15`, `21341` published bytes
- console/log writes in the producer path were minimal in the final run

### Chunked reusable-result / chunk-result path

- `64` `chunk_result` publishes, `149986` published bytes
- `372` derived `reusable_result` publishes, `17877` published bytes
- `15` final `superquery_result` publishes, `2690` published bytes
- `62` `original_agent_outputs_derived_from_chunks`
- `64` structured chunk callbacks into the chunked bee worker

Important asymmetry:

- approximation produces a small number of richer reusable-result messages directly from `RSPAgent`
- chunked produces chunk results plus many lightweight derived reusable-result publications inside the bee-worker pipeline

So even the producer message path is not structurally equivalent.

## Output Construction and Message Counts

### Approximation

| Metric | Value |
| --- | ---: |
| raw input messages in tree | `7683` |
| reusable-result messages | `64` |
| final `superquery_result` messages | `15` |
| output construction calls | `64` reusable-result builds in root, `15` final result builds in worker |

### Chunked

| Metric | Value |
| --- | ---: |
| raw input messages in tree | `7682` |
| `chunk_result` messages | `64` |
| derived `reusable_result` messages | `372` |
| final `superquery_result` messages | `15` |
| output construction calls | `64` chunk builds, `62` derived original outputs, `15` final recomposed outputs |

## Root vs Child CPU Summary

| Approach | Tree CPU-s | Root CPU-s | Child/Subtree CPU-s | Root work type |
| --- | ---: | ---: | ---: | --- |
| approximation | `32.25` | `31.46` | `0.79` | producer-heavy `RSPAgent` root |
| chunked | `19.49` | `2.25` | `17.24` | lightweight orchestration root |

This is the decisive comparison.

The roots do not represent equivalent work:

- approximation root is almost the whole pipeline cost
- chunked root is almost none of the pipeline cost

## Which Explanation Is True?

### A. approximation genuinely performs more upstream producer work

True.

Evidence:

- approximation root has `31.46 CPU-s`
- approximation root runs two live `RSPAgent` producers
- approximation root ingests `7683` raw messages and emits `64` reusable results

### B. chunked excludes some equivalent work from the measured tree

False in the strict sense of process-tree inclusion, but true if interpreted as root-boundary comparison.

Clarified statement:

- chunked does not exclude the work from the measured tree
- chunked does exclude that work from the root process boundary because the heavy work lives in the bee-worker subtree instead

### C. chunked has a cheaper reusable-result producer path

True.

Evidence:

- chunked root skips original `RSPAgent` reusable-result production
- chunked worker derives original reusable outputs from chunk results
- chunked derived reusable results are lightweight in published bytes (`17877` across `372` messages)

### D. approximation still has avoidable logging / serialization / diagnostic overhead

Not the primary explanation.

Evidence:

- approximation operator math is `2.52ms`
- approximation root reusable-result stringify is `0.41ms`
- approximation root reusable-result callback total is only `9.02ms`

There may still be small residual producer overhead, but it does not explain the bulk gap.

### E. CPU difference is within run-to-run noise

False.

Evidence:

- approximation tree CPU `32.25s`
- chunked tree CPU `19.49s`
- the structural process split explains the direction and magnitude

## Answer to the Original Concern

Approximation using more active CPU than chunked is not evidence that approximation final logic is more expensive than chunked recomposition.

The finished reruns show:

- approximation operator math is tiny
- chunked recomposition math is even tinier
- the benchmark CPU gap comes from where upstream producer work lives and how producer outputs are generated

The fair interpretation is:

- approximation root CPU mostly measures subquery execution and `RSPAgent` reusable-result production
- chunked root CPU mostly measures orchestration only
- chunked bee-worker subtree measures the shared chunk producer path plus recomposition

So the measured CPU difference is a pipeline-boundary and producer-path difference, not evidence that approximation math is more expensive than chunked recomposition.
