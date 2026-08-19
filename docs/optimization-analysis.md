# Optimization Analysis

## Chunked

Current overhead came from several places:

- Recomputing chunk plans and query rewrites on every registration.
- Parsing the same RSP-QL queries repeatedly.
- Starting a fresh MQTT publisher for each emitted result.
- Repeatedly scanning chunk buffers to find complete groups.
- Rebuilding RDF/N3 structures for intermediate chunk data before final emission.
- Very verbose logging in hot paths.

Some overhead is algorithmically necessary:

- Chunked still has to wait for all subquery contributions that belong to the same logical chunk group.
- Exact reconstruction for AVG, SUM, COUNT, MIN, and MAX still has to combine the per-chunk partials.

The avoidable implementation overhead is:

- Re-parsing identical queries.
- Rewriting the same logical subquery more than once.
- Creating duplicate MQTT clients and connect listeners.
- Full-buffer scans when only newly completed chunk groups need processing.
- JSON parsing and RDF serialization for internal data that is already structured.

Safe optimizations applied:

- Cache parsed queries and rewritten chunk plans.
- Deduplicate identical rewritten subqueries before starting child RSP processes.
- Reuse a shared MQTT publisher client.
- Compute exact supported chunked results directly from structured partials instead of round-tripping through RDF/N3.
- Reduce logging and profiling noise in hot loops.
- Add cleanup hooks for tracked clients and log streams.

Risky optimizations that should not be done:

- Dropping subquery contributions to reduce CPU.
- Changing window boundaries, trigger cadence, or publication counts.
- Approximating Chunked for the supported exact aggregations.
- Evicting incomplete chunk groups too aggressively without a proof that they can no longer complete.

## Approximation

Current overhead came from:

- Re-parsing the same query text for topic/window extraction.
- Repeated filtering and shifting of per-topic buffers.
- Rebuilding large debug objects on every message.
- Repeated MQTT client creation for publishing.

What is algorithmically necessary:

- Approximation still has to inspect overlapping windows and combine per-topic values.
- It must continue to publish the same result stream for the same inputs.

Safe optimizations applied:

- Cache parsed queries used for topic/window extraction.
- Reuse MQTT clients where possible.
- Replace multi-pass aggregation helpers with single-pass reductions where the math is unchanged.
- Reduce debug-object construction in the hot loop.

Risky optimizations to avoid:

- Changing the overlap logic or weights.
- Skipping publication or reducing emitted results.
- Changing the supported approximation math unless the implementation is clearly buggy and the change is covered by tests.
