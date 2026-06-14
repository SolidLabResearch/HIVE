# Decision Record: Generalizing Chunk-State-Primary Reuse for SUM and COUNT

Status: Proposed

## Context
The primary chunk-state reuse pathway was previously hardcoded for the AVG aggregation function. To enable exact chunk-state reuse for SUM and COUNT while preserving existing AVG capabilities, we need to generalize the query compatibility checks, state representation, and projection derivation logic.

## Decision
Refactor `CompatibleAvgChunkReuseSpec` to `CompatibleChunkReuseSpec` and detect compatibility dynamically using a generalized parser.

We define strict conditions under which SUM and COUNT queries are considered compatible:
- SUM requires a value variable (e.g. `saref:hasValue ?value`).
- COUNT is supported for `COUNT(?value)` or `COUNT(*)`.
- Reject `COUNT(DISTINCT ?x)` since simple sum of counts is incorrect when duplicates or non-disjoint sets exist.
- Reject GROUP BY, OPTIONAL, and queries containing unsupported shapes.
- Preserve exact projection serialization orders so that existing topic consumers do not break.

## Alternatives Considered

### Alternative 1: Monolithic Type-Specific Paths
Create separate logic blocks for AVG, SUM, and COUNT detection and handling.
- Pros: Simple to implement within isolated helper functions.
- Cons: Introduces significant code duplication and increases maintenance surface area.

### Alternative 2: Generalized Decomposable Model (Selected)
Refactor and unify the logic under a generic structure (`CompatibleChunkReuseSpec`) parameterized by `aggregationFunction` and `aggregationStateSignature`.
- Pros: Extremely clean, highly maintainable, and allows easy extension to other decomposable aggregations.
- Cons: Requires refactoring existing AVG-specific interfaces.

## Consequences
- Unified detection of exact chunk-state reuse for AVG, SUM, and COUNT.
- Safer validation preventing incorrect exact reuse for unsupported structures (like COUNT DISTINCT or GROUP BY).
- Unaltered topic serialization layout maintaining backward compatibility.
