# Chunked Resource Cause Analysis

## Scope

This document explains why the current Chunked approach uses substantially more CPU and memory than Fetching in the HIVE / Streaming Query Hive benchmark runs, without changing semantics, windows, timeout, or approximation behavior.

Primary evidence sources:

- Code inspection of:
  - `src/approaches/StreamingQueryChunkedApproachOrchestrator.ts`
  - `src/services/operators/StreamingQueryChunkAggregatorOperator.ts`
  - `src/rsp/RSPQueryProcess.ts`
  - `src/agent/RSPAgent.ts`
  - `src/services/HiveQueryBee.ts`
  - `src/services/BeeWorker.ts`
  - `experiments/pattern-analysis/run-custom-patterns-comparison.js`
  - `src/util/mqttTraffic.ts`
  - `src/util/profiling.ts`
- Existing benchmark artifacts under `logs/custom-pattern-comparison/...`
- One gated trace run with `HIVE_RESOURCE_TRACE=1` and `HIVE_PROFILE=1` for:
  - `fetching / low_variability / iteration1`
  - `chunked / low_variability / iteration1`

## Architecture Diagram

Fetching:

```text
publisher process
  -> MQTT raw topic wearableX
  -> MQTT raw topic smartphoneX

fetching orchestrator process
  -> 1 RSPEngine for final query
  -> 2 MQTT subscribers to raw topics
  -> 2 control-topic subscriptions in finite replay mode
  -> publishes final result topic
```

Approximation:

```text
publisher process
  -> MQTT raw topic wearableX
  -> MQTT raw topic smartphoneX

approximation orchestrator process
  -> 2 RSPAgent instances
  -> each RSPAgent subscribes to one raw topic
  -> each RSPAgent publishes reusable subquery results

BeeWorker child process
  -> ApproximationApproachOperator
  -> subscribes to reusable subquery result topics
  -> publishes final result topic
```

Chunked:

```text
publisher process
  -> MQTT raw topic wearableX
  -> MQTT raw topic smartphoneX

chunked orchestrator process
  -> 2 RSPAgent instances
  -> each RSPAgent subscribes to one raw topic
  -> each RSPAgent publishes reusable subquery results
  -> starts BeeWorker child

BeeWorker child process
  -> StreamingQueryChunkAggregatorOperator
  -> rewrites both subqueries to chunk size
  -> starts 2 RSPQueryProcess instances in-process
  -> each RSPQueryProcess subscribes to one raw topic again
  -> each RSPQueryProcess publishes chunk-result topic
  -> aggregator subscribes to both chunk-result topics
  -> reconstructs comparable windows
  -> publishes final result topic
```

## 1. Process Tree Attribution

### Process table

| approach | process role | process count | expected lifetime | included in CPU/RSS sampler? | reason it exists |
|---|---:|---:|---|---|---|
| Fetching | orchestrator process | 1 | full benchmark lifetime until watchdog cleanup | yes | runs final RSPEngine and final result publisher |
| Fetching | publisher/replayer process | 1 | full replay, then remains until timeout kill in current benchmark | yes | replays benchmark RDF events |
| Fetching | BeeWorker child process | 0 | n/a | n/a | not used |
| Fetching | `RSPQueryProcess` OS child process | 0 | n/a | n/a | not used |
| Fetching | MQTT broker (`mosquitto`) | 1 external | outside benchmark tree | no | message broker |
| Approximation | orchestrator process | 1 | full benchmark lifetime | yes | starts `RSPAgent` subqueries and HTTP server |
| Approximation | BeeWorker child process | 1 | full benchmark lifetime | yes | runs `ApproximationApproachOperator` |
| Approximation | publisher/replayer process | 1 | full replay, then remains until timeout kill in current benchmark | yes | replays benchmark RDF events |
| Approximation | `RSPQueryProcess` OS child process | 0 | n/a | n/a | not used |
| Approximation | MQTT broker (`mosquitto`) | 1 external | outside benchmark tree | no | message broker |
| Chunked | orchestrator process | 1 | full benchmark lifetime | yes | starts `RSPAgent` subqueries and HTTP server |
| Chunked | BeeWorker child process | 1 | full benchmark lifetime | yes | runs `StreamingQueryChunkAggregatorOperator` |
| Chunked | publisher/replayer process | 1 | full replay, then remains until timeout kill in current benchmark | yes | replays benchmark RDF events |
| Chunked | `RSPQueryProcess` OS child process | 0 | n/a | n/a | they are in-process objects inside BeeWorker, not OS processes |
| Chunked | MQTT broker (`mosquitto`) | 1 external | outside benchmark tree | no | message broker |

### Important clarification

`RSPAgent` and `RSPQueryProcess` are not separate PIDs. They are extra in-process `RSPEngine` + MQTT-client layers living inside existing Node processes.

That means the resource sampler correctly counts them, but their cost is merged into the orchestrator PID or BeeWorker PID.

## 2. CPU Attribution

### Existing benchmark totals

Existing 3-window validation artifacts show:

- Fetching low variability: about `16.3-16.9 CPU-s`, peak RSS about `285-290 MB`
- Chunked low variability: about `29.4-31.2 CPU-s`, peak RSS about `455-463 MB`
- Fetching spike: about `16.0-16.8 CPU-s`, peak RSS about `283-286 MB`
- Chunked spike: about `27.0-28.0 CPU-s`, peak RSS about `432-437 MB`

`rdf_parse_time_ms`, `r2r_execution_time_ms`, `structured_recomposition_time_ms`, and `diagnostics_write_time_ms` are near zero in current Chunked profile artifacts, so final recomposition is not the dominant CPU cost anymore.

### Per-PID traced CPU attribution

Trace run: `logs/custom-pattern-comparison/chunked/low_variability/iteration1/resource_per_pid_summary.json`

| pid | role | cpu_seconds | mean_cpu_pct | peak_cpu_pct | wall_time_s |
|---:|---|---:|---:|---:|---:|
| 29397 | chunked orchestrator process | 12.48 | 5.16 | 123.0 | 241.98 |
| 29448 | chunked BeeWorker process | 9.77 | 4.05 | 89.6 | 240.98 |
| 29537 | publisher | 3.22 | 1.34 | 27.0 | 239.44 |

Trace run: `logs/custom-pattern-comparison/fetching/low_variability/iteration1/resource_per_pid_summary.json`

| pid | role | cpu_seconds | mean_cpu_pct | peak_cpu_pct | wall_time_s |
|---:|---|---:|---:|---:|---:|
| 18902 | fetching orchestrator process | 10.15 | 4.20 | 146.4 | 241.79 |
| 19015 | publisher | 3.11 | 1.30 | 36.5 | 239.27 |

### CPU conclusions

Chunked CPU is mainly coming from:

1. Duplicated raw-stream processing in two layers.
   - The orchestrator starts two `RSPAgent` subqueries on raw streams.
   - The BeeWorker starts two rewritten `RSPQueryProcess` subqueries on the same raw streams again.
   - This means raw MQTT payloads are parsed and inserted into `RSPEngine` twice for the Chunked pipeline, before final aggregation work even begins.

2. Extra always-on RSPEngines.
   - Fetching keeps one final-query `RSPEngine`.
   - Chunked keeps four subquery engines alive at once:
     - 2 `RSPAgent` engines in orchestrator
     - 2 rewritten `RSPQueryProcess` engines in BeeWorker
   - The final aggregator itself also remains active in the BeeWorker.

3. Extra MQTT message handling.
   - Chunked processes raw messages in the original `RSPAgent` layer.
   - Chunked processes the same raw messages again in rewritten chunk-query layer.
   - Chunked additionally serializes, publishes, receives, parses, and buffers chunk-result JSON messages.

4. Idle CPU from long-lived extra processes during the watchdog tail.
   - All approach trees stay alive until timeout in the current benchmark.
   - Chunked has one extra long-lived BeeWorker process and more active timers/clients/engines during the idle tail.

### CPU that is not the main culprit

- Final structured recomposition
- RDF fallback parsing for final recomposition
- R2R fallback execution
- Diagnostics writing
- One-time query rewriting / chunk planning

All of those are currently negligible relative to total CPU.

## 3. Memory Attribution

### Traced stage snapshots for Chunked BeeWorker PID

Trace file: `logs/custom-pattern-comparison/chunked/low_variability/iteration1/resource_trace_chunked_bee_worker_29448.ndjson`

| stage | rss_mb | heap_used_mb | external_mb | array_buffers_mb | notes |
|---|---:|---:|---:|---:|---|
| startup | 109.188 | 39.595 | 2.433 | 0.134 | BeeWorker constructed |
| after result publisher for first rewritten subquery | 137.688 | 46.328 | 3.942 | 0.698 | first `RSPQueryProcess` layer allocated |
| after result publisher for second rewritten subquery | 139.719 | 46.580 | 3.942 | 0.698 | second `RSPQueryProcess` layer allocated |
| after subquery streams started | 140.625 | 46.625 | 3.942 | 0.698 | both rewritten engines active |
| after MQTT subscriptions ready | 150.672 | 46.789 | 3.942 | 0.698 | aggregator subscribed to chunk-result topics |
| after first chunk received | 168.016 | 57.584 | 3.940 | 0.697 | chunk buffering and stream state populated |
| after first comparable window emitted | 139.688 | 57.287 | 4.012 | 0.769 | result emitted, engines still alive |
| before cleanup | 110.031 | 54.958 | 3.982 | 0.740 | timeout reached, processes still alive |

Trace file: `logs/custom-pattern-comparison/chunked/low_variability/iteration1/resource_trace_chunked_orchestrator_29397.ndjson`

| stage | rss_mb | heap_used_mb | external_mb | array_buffers_mb | notes |
|---|---:|---:|---:|---:|---|
| startup | 97.734 | 35.522 | 2.396 | 0.133 | orchestrator boot |
| after registered query started | 116.266 | 44.413 | 3.800 | 0.680 | two `RSPAgent` subqueries + BeeWorker fork path started |
| process_exit | 98.250 | 42.975 | 3.992 | 0.739 | timeout cleanup |

### Memory conclusions

Chunked RSS comes mainly from:

1. Base Node process overhead of an extra long-lived BeeWorker PID.
   - Fetching tree peaks at 2 PIDs.
   - Chunked tree peaks at 3 PIDs.
   - The extra BeeWorker alone contributes a large constant RSS floor.

2. Duplicated RSPEngine state.
   - Orchestrator holds 2 `RSPAgent` engines.
   - BeeWorker holds 2 rewritten `RSPQueryProcess` engines.
   - Each engine has its own MQTT client state, internal stream/window state, parsed query objects, and event-store allocations.

3. Raw-message parsing and in-memory RDF/event structures in two layers.
   - Both `RSPAgent` and `RSPQueryProcess` call `turtleStringToStore(...)` and feed quads into an `RSPEngine`.
   - This doubles the amount of transient RDF parsing/storage work for raw inputs.

4. Chunked operator buffers and summaries.
   - These are not the largest source, but they do add memory on top of the duplicated engine layers.
   - The first chunk-received snapshot shows the BeeWorker jumping from about `150.7 MB` to `168.0 MB` once chunk state becomes populated.

### Memory that is not the main culprit

- Final RDF fallback store used in `executeR2ROperator(...)`
- Diagnostics CSV buffers
- `completedChunkGroups` and `chunksByWindow` at this small 3-window benchmark size
- Large retained raw RDF payloads in steady state

Reason: current code compacts structured partials and drops `rdfPayload` for recomposable aggregations before buffering them, so retained buffered payload strings are not dominating current RSS.

## 4. Message-Volume Attribution

### Profile counters

Existing profile artifacts:

- Fetching low variability iteration3:
  - `mqtt_messages_received = 2372`
  - `mqtt_messages_published = 3`
- Chunked low variability iteration3:
  - `mqtt_messages_received = 2388`
  - `mqtt_messages_published = 21`
  - `buffered_chunk_results = 18`
  - `chunk_groups_completed = 9`
  - `comparable_windows_emitted = 3`

Interpretation:

- The Chunked aggregator BeeWorker received:
  - 18 chunk-result messages
  - plus about 2370 raw input messages in its two rewritten subquery engines
- The Chunked orchestrator process also had two `RSPAgent` instances consuming raw inputs separately.
- Therefore total raw-input handling in the full Chunked tree is substantially larger than the BeeWorker-only `mqtt_messages_received` counter suggests.

### MQTT traffic artifacts

Chunked trace run `mqtt_traffic_summary.json` shows:

- `raw_input_subscriber_count = 2`
- `reusable_result_published_bytes > 0`
- `chunk_result_published_bytes > 0`

This confirms:

1. Chunked does not avoid raw input traffic at `K=1`.
2. Chunked adds reusable-result traffic from `RSPAgent`.
3. Chunked adds chunk-result traffic from rewritten `RSPQueryProcess`.

### Message-volume conclusion

Chunked is expensive because it receives approximately the same raw message volume as Fetching at least once, then receives it again in an additional raw-subquery layer, and then adds intermediate reusable-result and chunk-result messages on top.

It is not a case where Chunked avoids raw messages and only pays intermediate-message overhead.

## 5. RSP Process Attribution

### How many `RSPQueryProcess` instances Chunked starts

Current Chunked BeeWorker starts:

- 2 rewritten `RSPQueryProcess` instances
  - one for `wearableX`
  - one for `smartphoneX`

These are created in `StreamingQueryChunkAggregatorOperator.initializeSubQueryProcesses()`.

### What each runs

Each instance runs:

- a rewritten version of one input subquery
- on the same raw topic as the original subquery
- publishing to a session-scoped topic like:
  - `chunked/<sessionId>/<rewrittenQueryHash>`

### Unnecessary duplication

These rewritten `RSPQueryProcess` instances are not duplicated relative to Chunked semantics.

The duplication problem is elsewhere:

- the orchestrator already started 2 original `RSPAgent` subqueries before BeeWorker even begins
- BeeWorker then starts 2 rewritten `RSPQueryProcess` subqueries on the same raw streams

So the unnecessary duplication is:

- original `RSPAgent` layer
- plus rewritten `RSPQueryProcess` layer

for the same raw streams in the same benchmark run.

### Lifetime

Both layers remain alive until the watchdog timeout.

They do not shut down after the expected comparable windows are emitted.

## 6. Buffer Lifecycle

| structure | what it stores | approximate max size during 3-window run | eviction policy | could retain memory longer than necessary? | optimization candidate? |
|---|---|---|---|---|---|
| `chunksByWindow` | `Map<chunkGroupId, Map<subqueryId, PartialChunkResult>>` for incomplete chunk groups | small, roughly up to 2 partials per chunk group before completion | delete on completion | low at this benchmark size | yes |
| `completedChunkGroups` | completed chunk-group summaries keyed by chunk id | small, about 9 groups in current runs | evicted after comparable windows advance | low to moderate | yes |
| `orderedCompletedChunkGroups` | ordered array view of completed chunk groups | small, same order of magnitude as above | spliced after comparable windows advance | low to moderate | yes |
| `readyChunkGroupIds` | queue of chunk groups ready to process | very small | drained in `processChunks` | low | yes |
| `readyChunkGroupSet` | duplicate protection for ready queue | very small | cleared as queue drains | low | yes |
| diagnostics streams | CSV strings written to files | tiny in-memory buffering | stream backpressure only | low | no priority |
| internal chunk summaries | compact summaries with count/sum/avg/value | small | retained while needed for overlapping windows | low | yes |
| retained raw RDF strings | mostly removed for exact recomposable aggregations via `compactStructuredPartial(...)` | low in current exact path | omitted before buffer insertion | low | not primary |

### Buffer conclusion

Current buffer structures are not the primary explanation for the large CPU/RSS gap.

They are optimization candidates, but they are secondary behind duplicated streaming-engine lifetime and duplicated raw-stream ingestion.

## 7. Timeout / Lifetime Effect

The benchmark trees currently live for about `302s` in the existing validation runs and `242s` in the traced runs with the `240000 ms` timeout.

Observed behavior:

- comparable outputs finish much earlier
- publisher is killed by watchdog
- orchestrator trees remain alive until watchdog cleanup
- no approach currently stops immediately after the expected comparable windows are emitted

### Effect on Chunked

This inflates Chunked more than Fetching because Chunked keeps:

- one extra long-lived BeeWorker PID
- extra always-on MQTT clients
- 2 original `RSPAgent` engines
- 2 rewritten `RSPQueryProcess` engines
- aggregator timers/subscriptions

Therefore idle tail time is a real contributor to measured CPU and RSS.

### Important fairness note

This is still a real measurement under the current harness. It is not hidden or excluded. But it measures end-to-end benchmark lifetime rather than useful-work-only lifetime.

A better stop condition after the expected comparable windows would likely reduce measured cost for all approaches, and especially for Chunked, without changing output semantics. That is a measurement-policy question, not a correctness question.

## 8. Fairness Check

### Fair parts

- Same publisher/replayer process is included for Fetching and Chunked.
- Same broker is excluded for both because sampler only roots at approach PID and publisher PID.
- Child process inclusion is consistent:
  - Fetching has no BeeWorker child
  - Chunked includes BeeWorker child because it is under the approach PID
- Same wall-time watchdog policy is applied.
- Same timeout policy is applied.

### Current flaws / caveats

1. Timeout tail dominates both approaches after useful outputs are done.
   - This is not asymmetric in harness code, but it amplifies approaches with more long-lived processes.

2. Current message-traffic summaries use warmup cutoffs based on first superquery result.
   - Because Chunked’s first result arrives later, the “steady-state” window differs by approach.
   - For strict cross-approach traffic comparisons, raw counters and full-PID traces are safer than only the steady-state MQTT summary.

3. Existing `HIVE_PROFILE` counters are per process, not whole tree.
   - For Chunked, BeeWorker profile summaries do not capture orchestrator-side `RSPAgent` raw-message work.
   - That is why process-tree sampling and code inspection were needed.

## Top 5 Root Causes of Chunked Overhead

1. Duplicate raw-stream ingestion.
   - Evidence: `RSPAgent` consumes raw streams in orchestrator and `RSPQueryProcess` consumes the same raw streams in BeeWorker.

2. Duplicate subquery engines.
   - Evidence: 2 `RSPAgent` engines plus 2 rewritten `RSPQueryProcess` engines stay alive simultaneously.

3. Extra long-lived BeeWorker PID.
   - Evidence: Chunked tree has 3 sampled processes vs 2 for Fetching.

4. Extra intermediate MQTT traffic.
   - Evidence: reusable-result messages from `RSPAgent` and chunk-result messages from rewritten processes are both present; Fetching publishes only final results.

5. Timeout-tail idle lifetime.
   - Evidence: processes remain alive until watchdog after outputs are already emitted.

## Top 5 Safest Optimization Candidates

1. Remove the unused original `RSPAgent` layer from Chunked when explicit subqueries are already provided.
   - Safest because Chunked already uses rewritten-topic subscriptions, not the original `RSPAgent` outputs, for final aggregation.

2. Stop Chunked processes once all expected comparable windows are emitted.
   - Safe if benchmark policy allows it and semantics remain identical.

3. Avoid creating per-result publish clients where a shared client is enough.
   - Mostly relevant outside Chunked, but safe and semantics-preserving.

4. Reduce duplicate control/logging/subscription overhead in benchmark mode.
   - Safe if gated to benchmark mode only.

5. Tighten buffer eviction after comparable-window advancement.
   - Safe but lower impact than removing duplicated engines.

## Top 5 Risky Optimizations to Avoid

1. Changing window range/step/chunk parameters.
2. Making Chunked approximate.
3. Emitting fewer comparable windows.
4. Excluding BeeWorker or publisher from resource accounting.
5. Reintroducing semantic shortcuts in final recomposition.

## Final Diagnosis

### Why Chunked CPU is high

Chunked CPU is high mainly because it is doing too much continuous stream-processing work before final recomposition:

- two original `RSPAgent` subqueries on raw streams
- two rewritten `RSPQueryProcess` subqueries on the same raw streams
- one chunk aggregator consuming chunk-result topics
- extra MQTT serialization/deserialization for intermediate results
- extra idle CPU from a larger long-lived process tree during timeout tail

### Why Chunked memory is high

Chunked memory is high mainly because it keeps more always-on runtime state:

- one extra BeeWorker PID
- four subquery `RSPEngine` instances instead of one final-query engine
- more MQTT clients
- more transient RDF/store allocations from duplicated raw parsing
- chunk-state buffers on top of that

### Costs inherent to Chunked at `K=1`

These costs are inherent even in a correct exact Chunked implementation:

- query rewrite / chunk planning
- rewritten subquery execution
- chunk-result publication and aggregation
- chunk buffering until comparable windows are complete

These are real architectural costs, but they are currently small compared with the duplicated-engine overhead.

### Costs that are implementation overhead

These costs are not inherent and currently dominate:

- starting original `RSPAgent` subqueries and rewritten `RSPQueryProcess` subqueries together
- publishing reusable subquery results that Chunked does not use for final aggregation
- keeping extra engines and clients alive for the whole timeout tail

## Optimization Priority

If only one optimization is attempted first, it should be:

1. Eliminate the unused original `RSPAgent` subquery layer from Chunked when the BeeWorker already has explicit subqueries and is going to start rewritten chunk-query processes anyway.

That target directly addresses the largest observed CPU, RSS, and message-volume overhead without changing output semantics.
