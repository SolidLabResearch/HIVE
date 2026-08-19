# Approximation CPU Overhead Audit

Run roots:

- legacy validated run: `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz`
- patched rerun: `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-patched`

## Answer

The original `Approximation = 44.19 CPU-s` vs `Fetching = 40.56 CPU-s` result was suspicious for two reasons:

1. the benchmark was still relying on a legacy tree-CPU reconstruction path, and
2. approximation was doing avoidable per-message work in normal benchmark mode.

After switching the sampler to emit monotone process-tree totals and rerunning 4Hz with the same benchmark shape, approximation drops to `20.52` active CPU-seconds, slightly below fetching at `20.92`.

That means the earlier gap was not genuine algorithm cost.

## 1. Per-PID CPU attribution

### Fetching

| PID | PPID | Command | CPU-s | Mean RSS | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: |
| `21892` | `21854` | `node` | 21.34 | 219.15 MiB | 305.25 MiB |

### Approximation

| PID | PPID | Command | CPU-s | Mean RSS | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: |
| `95017` | `95002` | `node` | 20.89 | 161.30 MiB | 228.08 MiB |
| `95120` | `95017` | `/opt/homebrew/Cellar/node/25.9.0_2/bin/node` | 0.56 | 49.50 MiB | 131.83 MiB |

### Chunked

| PID | PPID | Command | CPU-s | Mean RSS | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: |
| `46807` | `46736` | `/opt/homebrew/Cellar/node/25.9.0_2/bin/node` | 15.13 | 144.76 MiB | 219.39 MiB |
| `46736` | `46698` | `node` | 2.04 | 49.58 MiB | 158.11 MiB |

## 2. Process-tree coverage

| Approach | Coverage rule | Observed live PIDs | Extra worker? |
| --- | --- | --- | --- |
| fetching | root PID plus descendants | one PID | no |
| approximation | root PID plus descendants | root + one child | no unique extra tree shape |
| chunked | root PID plus descendants | root + one child | same tree shape as approximation |

Approximation does not include replay or broker CPU accidentally. The publisher runs as a separate process launched by the benchmark harness and is outside the sampled tree.

## 3. Active-interval recomputation

Definition used for all three reruns:

- start: first `raw_input_stream` publish
- end: last `superquery_result` publish for window `15`
- exclude: teardown tail and post-result shutdown

| Approach | Legacy reconstructed CPU-s | Patched rerun active CPU-s | CPU-s/window |
| --- | ---: | ---: | ---: |
| fetching | 40.09 | 20.92 | 1.39 |
| approximation | 43.29 | 20.52 | 1.37 |
| chunked | 32.88 | 16.29 | 1.09 |

The legacy numbers were directionally useful but too large for publication and, in approximation’s case, amplified benchmark-only overhead that is now removed.

## 4. MQTT message counts and payload sizes

### Fetching

| Message type | Count | Avg payload bytes | Total payload bytes |
| --- | ---: | ---: | ---: |
| raw_input_stream | 7683 | 652.88 | 5,016,112 |
| superquery_result | 15 | 178.27 | 2,674 |

### Approximation

| Message type | Count | Avg payload bytes | Total payload bytes |
| --- | ---: | ---: | ---: |
| raw_input_stream | 7683 | 652.88 | 5,016,112 |
| reusable_result | 64 | 1,046.75 | 66,992 |
| superquery_result | 15 | 1,395.73 | 20,936 |

### Chunked

| Message type | Count | Avg payload bytes | Total payload bytes |
| --- | ---: | ---: | ---: |
| raw_input_stream | 7683 | 652.88 | 5,016,112 |
| reusable_result | 372 | 8.06 | 2,997 |
| chunk_result | 64 | 2,206.53 | 141,218 |
| superquery_result | 15 | 173.33 | 2,600 |

Findings:

- Approximation does not emit more reusable-result messages than chunked if we compare the semantically similar structured subquery outputs: both emit `64` result-bearing messages.
- Chunked emits much more internal traffic overall (`372 reusable_result` plus `64 chunk_result`), so approximation’s old CPU lead was not caused by higher MQTT volume.
- Approximation reusable-result payloads are larger than strictly necessary because they duplicate value fields and carry `raw_bindings` plus repeated window metadata.

## 5. Approximation hot-path inspection

### RSPAgent structured reusable-result publication

Before the patch, each reusable result did all of the following on the hot path:

- reparsed the query to rediscover `range`, `step`, aggregation type, and source topic
- serialized the structured payload with `JSON.stringify`
- unconditionally `console.log`ed the full structured payload string

The unconditional payload logging was the clearest benchmark-only waste. It serialized and wrote ~64 large log lines during the run, each around the same size as the MQTT payload itself.

### JSON serialization / deserialization

- `JSON.stringify` on publish and `JSON.parse` on receive are still present.
- They are necessary for the current schema, but they were not the dominant problem by themselves.
- No evidence showed replay, broker, or publisher CPU entering the approximation tree.

### MQTT callbacks

- Approximation subscribes only to the two reusable-result topics in the approach process.
- There is no extra approximation-only callback fanout beyond that.

### RateBasedApproximationApproachOperator structured path

- The operator receives `64` structured reusable results and emits `15` finals.
- It remains on `branch=structured` only.
- `legacy_messages_seen=0`, `adapted_legacy_messages_seen=0`, `suppressed_missing_window_metadata=0`.

### ApproximationWindowBuffer / RateBasedApproximationMath

- No evidence of pathological rescanning in this benchmark.
- There are only two topics and a very small active sliding-window set per topic.
- The buffer cleanup cursor already prevents unbounded prefix rescans.
- This path is not the main reason approximation was previously above fetching.

### Diagnostics / logging

Before the patch, normal benchmark mode still emitted:

- full structured reusable-result payload logs from `RSPAgent`
- verbose per-window latency console blocks from `ApproximationDiagnosticsWriter`

That work was unnecessary for validated benchmark runs and directly inflated approximation CPU.

## 6. Stale rows, teardown tails, and sample contamination

- Approximation total CPU in the patched rerun: `21.45s`
- Approximation active-interval CPU: `20.52s`
- Teardown tail excluded: `0.93s`

Equivalent exclusions:

- fetching: `0.42s`
- chunked: `0.88s`

So teardown exists, but it is not large enough to explain the original `44.19 > 40.56` inversion.

## 7. What changed

The safe patch did not change algorithm semantics, topics, values, or payload schema. It only removed avoidable benchmark overhead:

- cache parsed query metadata in `RSPAgent` instead of reparsing on every reusable result
- gate full structured reusable-result console logging behind approximation debug mode
- gate verbose approximation latency console logging behind `STREAMING_QUERY_HIVE_VERBOSE_LATENCY_LOGS=1`
- extend the process-tree sampler to write per-PID sidecar summaries for reruns

## 8. Acceptance check

- windows `1..15` emitted exactly once: yes
- `branch=structured` only: yes
- legacy fallback: `0`
- approximation vs fetching error on rerun: MAE `0.0004187`, MAPE `0.040892%`, max abs error `0.0006484`
- replay/publisher/broker counted in approximation CPU: no
- CPU-s/window decreased: yes, from `2.886` active legacy to `1.368` active rerun

Residual issue:

- latency-domain columns in the reruns still show `domain_mismatch`; that is a separate benchmark-latency concern and was not changed by this CPU audit patch.

## Conclusion

Approximation CPU was not genuinely higher than fetching for the validated 4Hz workload. The earlier result came from a mix of legacy CPU accounting and approximation-only benchmark logging/parsing overhead. After removing that overhead, approximation is slightly below fetching on the same active-interval process-tree metric.
