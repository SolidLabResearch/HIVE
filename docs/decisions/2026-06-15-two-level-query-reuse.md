# Two-Level Query Reuse Routing

Date: 2026-06-15

## Decision

RSP-QL query registration now supports two reuse levels for the K-scaling chunked path:

1. Level 1: exact final-result reuse
2. Level 2: chunk-state reuse for non-identical but compatible queries

The routing order is:

```text
Incoming query
  -> normalize/canonicalize
  -> check exact final-result registry
      -> if hit: return existing final result topic
  -> else check chunk-state/window-compatible reuse
      -> if hit: create reconstruction path from chunk states
  -> else fresh execution
```

## Rationale

The previous chunked K-scaling path already shared chunk producers, but exact duplicate superqueries still created one reconstruction path per consumer. That makes CPU and RSS grow with K even when all registered queries are semantically the same.

Exact duplicates should not pay the reconstruction cost repeatedly. If the final result already exists, the correct reuse target is the final result topic itself, not the chunk-state stream behind it.

## Level 1: Exact Final-Result Reuse

Phase 1 exact reuse uses conservative canonicalization:

- strip `REGISTER RStream <...> AS`
- remove comments
- normalize whitespace
- normalize simple prefix formatting
- preserve SELECT, aggregation, stream IRIs, RANGE, STEP, and WHERE content

This is intentionally not full SPARQL algebra isomorphism. It is a benchmark-focused first pass.

If two queries normalize to the same canonical form:

- they share one canonical query hash
- they share one final result topic
- no new RSP engine is started
- no new chunk-state reconstruction path is created

For exact-final K-scaling mode, the shared result topic is:

```text
hive/results/final/<canonicalQueryHash>
```

## Level 2: Chunk-State Reuse

If exact reuse misses, the existing chunk-state reuse path remains in place:

- shared chunk producers stay constant
- a reconstruction path may still be created for the requested window semantics
- existing chunk-state counters and process cleanup behavior remain unchanged

This is still the correct path for non-identical but compatible queries, especially same-query-different-window scenarios.

## Validation Counters

The profile summary now includes:

- `exact_final_result_reuse_hits`
- `final_result_topics_created`
- `final_result_topics_reused`
- `final_result_subscribers_registered`
- `chunk_reuse_paths_created`
- `chunk_reuse_paths_skipped_due_to_exact_hit`
- `reconstruction_paths_created`
- `reconstruction_paths_skipped`
- `fresh_executions_started`
- `canonical_query_hashes_seen`

Expected exact-final shape for duplicate-query K-scaling:

- `final_result_topics_created ~= 1`
- `exact_final_result_reuse_hits ~= K - 1`
- `final_result_subscribers_registered ~= K`
- reconstruction paths should not grow with K
- RSP engines should not grow with K

Expected chunk-state-only shape:

- shared chunk producers remain constant
- chunk-state messages remain roughly constant for the fixed-shape benchmark
- reconstruction paths can still scale with K

## Experiment Modes

Two benchmark modes are now distinguished through:

- `K_SCALING_REUSE_MODE=chunk-state`
- `K_SCALING_REUSE_MODE=exact-final`

Exact final-result reuse is additionally gated by:

- `HIVE_ENABLE_EXACT_FINAL_RESULT_REUSE=true`

This keeps the previous chunk-state benchmark semantics as the default and makes the exact-final experiment explicit.
