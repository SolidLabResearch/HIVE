# Topic: Per-Producer Watermark Tracking in Chunk Aggregator

## Context

In the chunked query execution model (specifically N=2 Scenario), chunks are received asynchronously from multiple subquery processes (producers) corresponding to different sensors or streams (e.g. `thing1` and `thing2`). Currently, the chunk aggregator does not track watermark progression independently per subquery producer. This led to cross-producer watermark comparison, resulting in out-of-order/stall warnings and incorrect final reconstruction watermark calculations.

We need to implement a mechanism to track watermarks independently per producer, handle duplicate and regressed watermarks correctly, and derive a correct reconstruction watermark for the outer query.

## Decision

We will:
1. Maintain `latestWatermarkByProducer` as a `Map<string, number>` property inside the `StreamingQueryChunkAggregatorOperator` class.
2. Key this map by the stable `subqueryId` derived from the chunk metadata.
3. Apply producer-local update semantics:
   - Accept watermark advancement if candidate > previous.
   - Accept duplicate/idempotent watermark (candidate = previous) without treating it as a regression or counting chunk contributions twice.
   - Reject/ignore regressions (candidate < previous).
4. Derive the overall reconstruction watermark as the minimum of the latest watermarks of all expected producers. This watermark is only derived once every expected producer has reported at least one watermark.
5. Track unique accepted contributions using the key format `<producerId>:<chunkStart>:<chunkEnd>` to prevent double-counting of duplicate chunk contributions.
6. Provide structured debug/experiment tracing of watermark updates under a debug flag.

## Alternatives Considered

- **Global Watermark Tracking**: Maintaining a single watermark for the entire reconstruction worker. This was rejected because watermarks from different producers (`thing1` and `thing2`) fluctuate independently, leading to incorrect regressions and stall warnings.
- **Arrival-Order Index Tracking**: Tracking watermarks based on message arrival sequence. This was rejected because network jitter and process scheduling can alter the order of chunk arrivals, which does not reflect event-time order.
