# Compact Reusable-Result Payload Audit

## Scope

Goal: audit whether approximation reusable-result payloads are oversized, reduce producer-side overhead safely, and keep approximation semantics unchanged.

Patch files:

- `/Users/kushbisen/Code/streaming-query-hive/src/agent/RSPAgent.ts`
- `/Users/kushbisen/Code/streaming-query-hive/src/util/runtimeConfig.ts`
- `/Users/kushbisen/Code/streaming-query-hive/src/agent/RSPAgent.test.ts`
- `/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.ts`
- `/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.test.ts`

The payload compaction is opt-in behind:

```bash
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1
```

Default behavior is unchanged.

## Field Audit

Current producer: `RSPAgent.buildReusableResultPayload()`

### Field classification

| Field | Classification | Reason |
| --- | --- | --- |
| `message_format` | required for approximation semantics | used to identify structured reusable-result messages |
| `source_query_id` | required for correctness/traceability | stable producer identity; retained |
| `source_topic` | useful metadata, retained | consumer can use topic fallback, but source topic is cheap and still useful |
| `reusable_result_topic` | redundant alias | duplicates MQTT envelope topic and was removed in compact mode |
| `aggregationType` | required for approximation semantics | used by approximation operator |
| `value` | required for approximation semantics | primary reusable result value |
| `resultValue` | redundant alias | duplicates `value`; removed in compact mode |
| `count` | retained for compatibility | not required by current approximation path, but cheap and potentially used by other consumers |
| `sum` | retained for compatibility | same rationale |
| `avg` | retained for compatibility | same rationale |
| `min` | retained for compatibility | same rationale |
| `max` | retained for compatibility | same rationale |
| `raw_bindings` | debug-only / producer-internal | large, string-heavy, not needed by approximation correctness; removed in compact mode |
| `window_start` | required for approximation semantics | used to identify completed windows |
| `window_end` | required for approximation semantics | used to identify completed windows |
| `window_data_close_time` | required for correctness/diagnostics | retained |
| `logical_trigger_time` | diagnostics metadata, retained | cheap and useful; retained |
| `timestamp_from` | redundant alias | duplicates `window_start`; removed in compact mode |
| `timestamp_to` | redundant alias | duplicates `window_end`; removed in compact mode |
| `result_emitted_at` | diagnostics only | removed in compact mode |
| nested `window` object | redundant structured alias | duplicates flat window metadata; removed in compact mode |

## Compact Schema

Compact mode now publishes:

```json
{
  "message_format": "structured_reusable_result",
  "source_query_id": "<hash>",
  "source_topic": "<topic>",
  "aggregationType": "AVG",
  "value": 1.23,
  "count": 240,
  "sum": 295.2,
  "avg": 1.23,
  "min": 0.31,
  "max": 1.92,
  "window_start": 1756123685256,
  "window_end": 1756123745256,
  "window_data_close_time": 1756123745256,
  "logical_trigger_time": 1756123715256
}
```

Removed only in compact mode:

- `raw_bindings`
- `resultValue`
- `reusable_result_topic`
- `timestamp_from`
- `timestamp_to`
- `result_emitted_at`
- nested `window`

## Size Findings

Baseline full-payload run:

- `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-patched/approximation/iteration1`

Compact rerun:

- `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-compact/approximation/iteration1`

### Reusable-result payload distribution

| Mode | Count | Total bytes | Mean | p50 | p95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| full payload | 64 | 66,992 | 1,046.75 | 1,051 | 1,065 | 1,065 |
| compact payload | 64 | 24,977 | 390.27 | 394 | 396 | 396 |

Delta:

- total reusable-result payload bytes: `-42,015` bytes
- mean reusable-result payload size: `-656.48` bytes
- relative reduction: `62.72%`

### Approximate byte drivers

Using a representative full payload sample, the largest removable contributors were:

| Field removed | Approx bytes freed |
| --- | ---: |
| nested `window` | 271 |
| `raw_bindings` | 150 |
| `reusable_result_topic` | 67 |
| `resultValue` | 33 |
| `timestamp_from` | 31 |
| `timestamp_to` | 29 |
| `result_emitted_at` | 34 |

Representative sample estimate:

- full payload: `1,017` bytes
- compact payload: `402` bytes
- estimated reduction: `615` bytes

That estimate is directionally consistent with the measured benchmark reduction of about `656` bytes/message.

## CPU Impact

Active-interval CPU was recomputed from:

- first raw input publish
- through result emission for window `15`
- using monotone `tree_cpu_seconds` from the process-tree sampler

### Approximation active CPU

| Mode | Active CPU-seconds |
| --- | ---: |
| patched full payload | 20.51 |
| compact payload | 18.34 |

Delta:

- active CPU-seconds: `-2.17s`
- relative reduction: `10.58%`

Per-PID compact rerun summary:

- root PID `15130` (`node`): `18.67s`
- child PID `15230` (`node` worker): `0.55s`

This is consistent with the earlier attribution finding: most approximation CPU remains in the root producer process, and reducing reusable-result serialization/payload construction pressure lowers that root-process cost.

## Correctness Validation

Validation commands run:

```bash
npm run build
npx tsc --noEmit
npx jest src/agent/RSPAgent.test.ts src/services/operators/RateBasedApproximationApproachOperator.test.ts --runInBand
```

All passed.

Approximation compact rerun command shape:

```bash
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1
WEARABLE_FREQUENCY=4
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=15
node scripts/analysis-js/experiment-evaluation-approximation-approach.js
```

### Benchmark acceptance checks

- windows `1..15` emitted exactly once: yes
- `branch=structured` only: yes
- final counters: `structured_messages_seen=64`, `legacy_messages_seen=0`, `adapted_legacy_messages_seen=0`, `suppressed_missing_window_metadata=0`
- legacy fallback: `0`
- compact vs patched approximation result values: identical for all `15` windows
- compact vs fetching MAPE: `0.040892080562013794%`
- compact vs patched approximation MAPE: `0%`
- no mixed-domain behavior change: unchanged; latency CSV still reports the same pre-existing `domain_mismatch` status as the patched baseline

## Conclusion

Approximation reusable-result payloads were materially oversized for benchmark mode. The largest unnecessary costs were the nested `window` object, `raw_bindings`, duplicate `resultValue`, duplicate timestamp aliases, and repeated topic aliasing.

The opt-in compact payload mode is safe for the current approximation path:

- it preserves result values exactly
- it preserves window numbering exactly
- it preserves structured-branch handling
- it reduces reusable-result payload bytes by `62.72%`
- it reduces approximation active CPU by about `2.17s` on the validated 4Hz 15-window rerun

The remaining approximation CPU is still mostly producer-side root-process work, but the payload audit confirms that oversized reusable-result payloads were a real and safely reducible part of that cost.
