# Decision Record: MIN and MAX Chunk-State-Primary Reuse

Status: Proposed

## Context
Following the generalization of the exact chunk-state-primary reuse path from AVG to SUM and COUNT, we need to add support for MIN and MAX aggregation functions. To support paper-level exactness validation, we must also add MIN and MAX support to the client-side Fetching baseline to compare actual results.

## Decision
Extend the chunk-state-primary reuse class to support MIN and MAX aggregation functions.

Supported exact chunk-state-primary reuse list:
- AVG: state `sum,count` (uses sum / count)
- SUM: state `sum`
- COUNT: state `count`
- MIN: state `min` (uses min of chunk minima)
- MAX: state `max` (uses max of chunk maxima)

We implement strict limitations:
- Reject `MIN(DISTINCT ?x)` and `MAX(DISTINCT ?x)`.
- Reject queries containing `GROUP BY`, `OPTIONAL`, `FILTER`, `UNION`, `HAVING`, `ORDER BY`.
- Reject ambiguous projection aliases or missing aliases.
- Exact chunk coverage of the output window is required.
- Only decomposable aggregations are supported.

In the Fetching baseline, we compute the window minimum or maximum of observed values without emitting fake `0` for empty windows.

## Alternatives Considered

### Alternative 1: Treat MIN/MAX as Non-Decomposable
Force MIN and MAX to run through the fallback raw RSP engine path instead of reusing chunk state.
- Pros: Simple, avoids modifying the state extraction schema.
- Cons: Fails to exploit the chunk state for MIN/MAX, which are mathematically decomposable (associative and commutative) over non-empty sets.

### Alternative 2: Decomposable Min/Max Chunk State (Selected)
Generalize the chunk state schema to include `min` and `max` properties and recompose them by taking the minimum/maximum over chunks. Empty chunks are ignored, and if all chunks are empty, no result is emitted.
- Pros: Exact chunk-state reuse is achieved, avoiding raw engine execution.
- Cons: Increases the size of chunk state diagnostics slightly.

## Consequences
- Five major aggregation functions (AVG, SUM, COUNT, MIN, MAX) are now fully supported in the exact chunk-state-primary path.
- Exactness comparison between Chunked and Fetching baseline is fully validated.
- Robust handling of empty windows prevents fake `0` values from being published.
