# Approximation CPU Optimization Report

## Patch scope

Files changed:

- `src/agent/RSPAgent.ts`
- `src/services/operators/approximation/ApproximationDiagnosticsWriter.ts`
- `scripts/analysis-js/process-tree-resource-sampler.js`

No algorithm semantics changed.

## Changes

### 1. Cache reusable-result metadata in `RSPAgent`

`RSPAgent` now computes these once at construction time instead of per reusable result:

- parsed query
- aggregation type
- source topic
- subquery window `range`
- subquery window `step`

This removes repeated query parsing and repeated stream-name inspection from the hot path.

### 2. Stop unconditional structured payload console logging

Before:

- every structured reusable-result payload was serialized and logged to stdout in normal benchmark mode

After:

- that log only appears when approximation debug mode is enabled

### 3. Stop verbose per-window latency console logging in normal mode

Before:

- every approximation final result emitted a multi-line console latency block

After:

- CSV latency artifacts are still written
- console latency detail is only emitted with `STREAMING_QUERY_HIVE_VERBOSE_LATENCY_LOGS=1`

### 4. Add per-PID sampler sidecar output

The process-tree sampler now writes a `*_per_pid_summary.json` sidecar so reruns include:

- PID
- PPID
- command
- CPU-seconds
- mean RSS
- peak RSS

## Validation

Commands run:

```bash
npm run build
npx tsc --noEmit
npx jest src/services/operators/RateBasedApproximationApproachOperator.test.ts --runInBand
npx jest src/agent/RSPAgent.test.ts --runInBand
```

All passed.

## Rerun results

### Approximation-only rerun

Patched 4Hz approximation rerun root:

- `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-patched/approximation/iteration1`

Results:

- windows `1..15` emitted exactly once
- `branch=structured` only
- `legacy_messages_seen=0`
- active CPU-seconds: `20.52`
- CPU-s/window: `1.368`
- approximation vs fetching MAPE in rerun: `0.040892%`

### Full three-approach rerun

| Approach | Active CPU-s | CPU-s/window |
| --- | ---: | ---: |
| fetching | 20.92 | 1.395 |
| approximation | 20.52 | 1.368 |
| chunked | 16.29 | 1.086 |

## Outcome

The optimization removed the approximation-vs-fetching inversion seen in the earlier 4Hz benchmark. Approximation is now slightly below fetching on the same active-interval process-tree metric, while preserving output windows and structured-only behavior.

## Remaining caveat

The rerun still reports `domain_mismatch` in latency-domain fields. That issue is unrelated to CPU attribution and was not changed here.
