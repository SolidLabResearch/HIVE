# Decision Record: Chunk-State Reuse Extension Design

Status: Proposed

## Context
The primary chunk-state reuse path has been successfully validated for the AVG aggregation function. In order to expand the system capability, we need to extend this path to support other decomposable aggregation functions: SUM, COUNT, MIN, and MAX.

## Decision
We propose to generalize the compatibility detection and recomposition utilities in the chunk-state reuse module to support SUM, COUNT, MIN, and MAX, using a unified chunk-state format.

## Alternatives Considered

### Alternative 1: Function-Specific Checkers
Implement distinct detection and recomposition functions for each aggregation type (e.g. detectCompatibleSumChunkReuse, detectCompatibleMinChunkReuse).
- Pros: Simple to write for each function independently.
- Cons: High code duplication. Harder to maintain as more aggregation functions are added.

### Alternative 2: Generalized Aggregation Recomposer (Selected)
Generalize detectCompatibleAvgChunkReuse into detectCompatibleChunkReuse. This returns a generic chunk reuse specification specifying the target aggregation type, required chunk state signature, and recomposition mapping.
- Pros: Highly extensible. Unified code path for all aggregation functions. Easy to add future functions.
- Cons: Requires refactoring of the existing AVG-specific detection signature.

## Consequences
- Unified signature for compatibility checks.
- Code reuse across all aggregation functions.
- Ensures all correctness conditions (same source stream, same filters, compatible event time) are checked uniformly.
