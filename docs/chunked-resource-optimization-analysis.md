# Chunked Resource Optimization Analysis

## Algorithmically necessary work

Chunked has to do this work to stay exactly equivalent to Fetching for decomposable aggregations:

- Parse the output query and derive a chunk plan that is semantically aligned with the output window.
- Run the rewritten subqueries over MQTT-backed source streams.
- Collect one partial result per subquery for each logical chunk window.
- Recompose output windows from the exact set of chunk groups that cover that output window.
- Publish the final result through the existing MQTT/RDF-facing interface.
- Keep correctness diagnostics strong enough to prove exactness against Fetching.

For `AVG`, `SUM`, `COUNT`, `MIN`, and `MAX`, exact recomposition is arithmetic on structured chunk partials. RDF/R2R is not algorithmically necessary on the hot path when the chunk payload already contains the exact numeric partials.

## Avoidable implementation overhead

- Parsing RDF chunk payloads into an `N3.Store` before checking whether structured exact recomposition already suffices.
- Creating a fresh MQTT publisher client per final emission instead of reusing one lifecycle-owned client.
- Writing large debug summaries and full payload logs on the per-message path.
- Synchronous `appendFileSync` diagnostics writes in the hot path.
- Repeated `Array.from(...).sort(...)`, repeated slicing, and repeated flattening of completed chunk groups.
- Retaining full per-partial RDF payloads after structured numeric recomposition is already guaranteed.
- Recomputing per-chunk diagnostic summaries instead of caching compact summaries once per completed chunk group.

## Likely CPU hotspots

- JSON parsing plus repeated chunk normalization on every MQTT message.
- RDF parsing and `N3.Store` construction in `executeR2ROperator()` fallback.
- Re-sorting completed chunk groups for every emission attempt.
- Rebuilding `internalChunks` and flattening comparable windows multiple times.
- Excess logging and synchronous file writes on chunk arrival and partial-diagnostic emission.
- Repeated query parsing and rewriting on cold paths without surfaced cache accounting.

## Likely memory retention points

- `chunksByWindow` while waiting for all subqueries in a chunk group.
- `completedChunkGroups` when full partial objects, including `rdfPayload`, are retained longer than needed.
- `internalChunks` arrays materialized repeatedly for diagnostics and result payloads.
- Debug summary rewrites and retained JSON strings.
- MQTT client objects if publisher/subscriber lifecycle cleanup is incomplete.

## Safe optimizations

- Prefer structured exact recomposition before RDF parsing for supported decomposable aggregations.
- Reuse one MQTT publisher client per operator lifecycle and close it in cleanup.
- Gate full payload logs behind a debug env var and keep benchmark-required summary artifacts.
- Replace synchronous hot-path writes with append streams or deferred summary flushes.
- Keep a compact ordered completed-group buffer instead of re-sorting the whole map.
- Cache per-group summaries and evict groups once they are no longer needed by any future output window.
- Drop `rdfPayload` from retained structured chunk state once the aggregation can be recomposed exactly from numeric fields.
- Surface query parse/rewrite cache hits and misses in `hive_profile_summary.json`.
- Ensure timers and MQTT clients are owned and closed explicitly.

## Risky optimizations

- Sharing chunk producers across independent Chunked query runs without a registry above the current operator process boundary.
- Evicting completed groups without proving the maximum overlap required by future output windows.
- Collapsing diagnostics too aggressively if benchmark payloads still need chunk IDs or internal chunk metadata.
- Changing message timing or cadence in a way that affects emitted result counts or output-window semantics.
- Removing RDF fallback entirely; it should remain available for non-structured or malformed fallback cases.

## Current K-scaling blocker

The current architecture does not genuinely reuse chunk producers across K target reconstructions. `StreamingQueryChunkAggregatorOperator.initializeSubQueryProcesses()` creates fresh `RSPQueryProcess` instances per operator instance, and the topics are session-scoped (`chunked/${sessionId}/${hash}`). That prevents cross-query reuse even when the logical rewritten subquery and chunk plan are identical.

To make K=2/4/8 reuse real, the chunk producer lifecycle needs a registry above the per-query operator instance, keyed by source stream set, aggregation, normalized rewritten subquery body, chunk range, chunk step, and result topic/session policy. The registry would need reference counting, shared topic publication, and deterministic cleanup once the last dependent target query is gone.
