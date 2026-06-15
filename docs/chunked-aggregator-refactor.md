# Chunked Aggregator Refactor

The chunk-state-reuse flow now reads as seven algorithmic steps:

1. Receive a chunk partial from MQTT and ignore legacy or unstructured payloads.
2. Normalize the structured payload into a compact partial that preserves the fields needed for exact recomposition.
3. Assign the partial to its logical chunk group using `queryId + window.start + window.end`.
4. Check coverage against the expected subquery set and track duplicates separately from the first accepted chunk.
5. Buffer complete chunk groups in start-time order so contiguous groups can be reused safely.
6. Recompose a comparable parent window from contiguous chunk groups using aggregation-specific exact rules.
7. Emit the final result together with coverage/proof metadata and latency/diagnostic records.
