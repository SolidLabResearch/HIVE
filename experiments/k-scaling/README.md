# K-Scaling / Reuse-Density Benchmark

This experiment measures how resource consumption and window-adjusted latency scale as the number of compatible consumers ($K$) increases. It compares two evaluation approaches:
1. **Fetching**: Each superquery evaluates/fetches required stream data directly (meaning evaluation work scales linearly with $K$).
2. **Chunked**: Compatible queries are evaluated through shared chunk states, where window results are reconstructed from completed chunk coverage.

## Experimental Parameters

- **Aggregation Function**: AVG
- **Window Range (RANGE)**: 120s
- **Window Step (STEP)**: 60s
- **Data Pattern**: `low_variability` (deterministic custom pattern)
- **Scaling Range**: $K \in \{1, 2, 4, 8, (16)\}$
- **Duration**: 5 minutes (300 seconds) per iteration

## Purpose and Key Metrics

The benchmark isolates reuse density by holding the query shape fixed and varying only the number of compatible consumers. This evaluates whether the fixed overhead of chunk-state materialization can be amortized across multiple consumers.

- **For Fetching**: Work must scale directly with $K$. Direct evaluation CPU and RSS increments should scale linearly because each consumer evaluates the full logic independently.
- **For Chunked**: The chunk-state producers created must remain constant (only 1 set of producers created for $K=1$, shared by all $K$ consumers). Total chunk-state messages published should remain constant across different values of $K$.
- **Validation**: Derived metrics such as `cpu_seconds_delta_from_previous_k` and `peak_rss_mb_delta_from_previous_k` are computed to compare incremental costs.

> [!NOTE]
> For this fixed-shape K-scaling experiment, `chunk_state_messages_published` is expected to remain constant as $K$ increases because the same chunks are produced once and shared. This is not a universal HIVE rule; under mismatched-window diversity or other complex patterns, chunk state message counts may vary depending on the chunking plan.
