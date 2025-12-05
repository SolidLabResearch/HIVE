# Streaming Query Approach Comparison

## Overview

This document explains the differences and relationships between the various streaming query processing approaches in this project.

## Approach Summary

| Approach Name | Identifier | Implementation | Purpose |
|---------------|------------|----------------|---------|
| **Fetching Client Side** | `fetching-client-side` | `StreamingQueryFetchingClientSideApproachOrchestrator.ts` | Ground truth baseline - fetches all data client-side |
| **Streaming Query Hive** | `streaming-query-hive` | `StreamingQueryHiveApproachOrchestrator.ts` | **ALIAS** for Chunked Approach |
| **Chunked Approach** | `chunked-approach` | `StreamingQueryChunkedApproachOrchestrator.ts` | Chunk-based aggregation (main implementation) |
| **Approximation Approach** | `approximation-approach` | `StreamingQueryApproximationApproachOrchestrator.ts` | Approximation-based processing |
| **Independent Stream Processing** | `independent-stream-processing` | `IndependentStreamProcessingApproach.ts` | Independent processor pattern |

## Key Finding: Hive = Chunked

**Important**: `streaming-query-hive` and `chunked-approach` are **THE SAME IMPLEMENTATION**.

### Why Two Names?

The "Streaming Query Hive" approach was the original name for what is now called the "Chunked Approach". Both refer to the same underlying implementation that uses chunk-based aggregation with the `StreamingQueryChunkAggregatorOperator`.

### Implementation Details

```typescript
// StreamingQueryHiveApproachOrchestrator.ts
// This is just an alias that extends the chunked approach
export class StreamingQueryHiveApproachOrchestrator 
  extends StreamingQueryChunkedApproachOrchestrator {
  
  public getName(): string {
    return "streaming-query-hive";  // Returns different identifier
  }
}
```

### Technical Architecture

Both use:
- **Operator**: `StreamingQueryChunkAggregatorOperator`
- **Logger**: `streaming_query_chunk_aggregator_log.csv`
- **Resource Log**: `streaming_query_hive_resource_log.csv`
- **Same SPARQL queries** (with fixed prefixes)
- **Same processing logic**

The only difference is the `getName()` return value for experiment identification.

## Detailed Approach Breakdown

### 1. Fetching Client Side Approach

**Purpose**: Ground truth baseline for comparison

**How it works**:
- Connects directly to MQTT broker
- Fetches ALL streaming data on the client side
- Processes everything locally without distribution
- Uses full RSP-QL engine client-side
- Most accurate but least scalable

**Use case**: 
- Generating ground truth for accuracy comparisons
- Baseline performance metrics

**Queries**:
```sparql
# Single query processing both streams
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> [RANGE 120000 STEP 60000]
WHERE {
    { WINDOW ... wearableX ... }
    UNION
    { WINDOW ... smartphoneX ... }
}
```

### 2. Streaming Query Hive / Chunked Approach

**Purpose**: Distributed chunk-based processing

**How it works**:
- Splits query into subqueries (one per stream)
- Each subquery processes data in chunks
- Uses `StreamingQueryChunkAggregatorOperator` to combine results
- Aggregates partial results into final output
- More scalable than client-side fetching

**Architecture**:
```
Stream 1 (wearableX)  → SubQuery 1 → Chunk Processor → 
                                                         → Aggregator → Final Result
Stream 2 (smartphoneX) → SubQuery 2 → Chunk Processor → 
```

**Queries**:

*SubQuery 1* (processes wearableX):
```sparql
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
```

*SubQuery 2* (processes smartphoneX):
```sparql
SELECT (MAX(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
```

*Main Query* (aggregates subquery results):
```sparql
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> [RANGE 120000 STEP 60000]
WHERE {
    { WINDOW ... wearableX ... }
    UNION
    { WINDOW ... smartphoneX ... }
}
```

**Use case**:
- Production streaming query processing
- Scalable multi-stream aggregation
- Main approach for the "Hive" concept

### 3. Approximation Approach

**Purpose**: Fast approximate results with reduced accuracy

**How it works**:
- Uses `ApproximationApproachOperator`
- Similar structure to chunked approach (subqueries + main query)
- Trades accuracy for speed
- May use sampling or estimation techniques

**Architecture**:
```
Stream 1 → SubQuery 1 → Approximation → 
                                         → Approximate Aggregator → Approximate Result
Stream 2 → SubQuery 2 → Approximation → 
```

**Use case**:
- Real-time dashboards where approximate is acceptable
- High-frequency data where exact values aren't critical
- Situations requiring lower latency over accuracy

### 4. Independent Stream Processing Approach

**Purpose**: Fully independent processor pattern

**How it works**:
- Creates separate independent processors for each subquery
- Each processor independently fetches, processes, and publishes
- Similar to client-side fetching but distributed
- Each processor acts like `FetchingAllDataClientSide`

**Architecture**:
```
Processor 1 (SubQuery) → MQTT Subscribe → Process → MQTT Publish →
                                                                      → Final Aggregation
Processor 2 (SubQuery) → MQTT Subscribe → Process → MQTT Publish →

Processor 3 (SuperQuery) → MQTT Subscribe → Process → MQTT Publish
```

**Use case**:
- Testing independent worker patterns
- Comparing orchestration strategies
- Evaluating communication overhead

## Performance Characteristics

### Latency (Expected)

1. **Fetching Client Side**: Highest latency (all processing centralized)
2. **Chunked/Hive**: Medium latency (distributed processing with aggregation)
3. **Approximation**: Lowest latency (trades accuracy for speed)
4. **Independent**: Medium-high latency (MQTT communication overhead)

### Accuracy (Expected)

1. **Fetching Client Side**: 100% (ground truth)
2. **Chunked/Hive**: ~99-100% (exact computation, distributed)
3. **Independent**: ~99-100% (exact computation, distributed differently)
4. **Approximation**: 90-98% (approximate computation)

### Scalability (Expected)

1. **Approximation**: Highest (lightweight processing)
2. **Chunked/Hive**: High (chunk-based distribution)
3. **Independent**: Medium (independent processors, more MQTT traffic)
4. **Fetching Client Side**: Lowest (centralized, no distribution)

### Memory Usage (Expected)

1. **Fetching Client Side**: Highest (stores all data client-side)
2. **Independent**: Medium-high (multiple independent processors)
3. **Chunked/Hive**: Medium (chunk-based, partial results)
4. **Approximation**: Lowest (minimal state for approximation)

## When to Use Each Approach

### Use Fetching Client Side When:
- You need absolute ground truth
- Generating baseline metrics
- Data volume is manageable
- Accuracy is critical and scalability is not

### Use Chunked/Hive When:
- You need production-ready distributed processing
- Exact results are required
- Multiple streams need aggregation
- Moderate scalability is needed

### Use Approximation When:
- Real-time dashboards require fast updates
- Approximate values are acceptable
- High data volumes need processing
- Latency is more critical than precision

### Use Independent When:
- Testing worker communication patterns
- Evaluating orchestration strategies
- Comparing against other approaches
- Research and experimentation

## Experiment Configuration

In `frequency-experiment-config.json`:

```json
{
  "approaches": [
    "fetching-client-side",    // Ground truth
    "streaming-query-hive",    // Same as chunked-approach
    "chunked-approach",        // Main implementation
    "approximation-approach"   // Fast approximate
  ]
}
```

### Recommendation: Remove Duplicate

Since `streaming-query-hive` and `chunked-approach` are identical, you may want to remove one from the config to avoid duplicate experiments:

**Option 1** (Keep descriptive name):
```json
{
  "approaches": [
    "fetching-client-side",
    "chunked-approach",
    "approximation-approach"
  ]
}
```

**Option 2** (Keep project name):
```json
{
  "approaches": [
    "fetching-client-side",
    "streaming-query-hive",
    "approximation-approach"
  ]
}
```

Both will produce identical results for the chunked/hive approach since they use the same implementation.

## Running Comparisons

### Compare All Approaches
```bash
npm run experiment:run
```

### Compare Specific Approaches
Edit `scripts/benchmarks/frequency-experiment-config.json` to include only desired approaches.

### Standalone Testing
```bash
# Test chunked/hive approach
npx ts-node src/approaches/StreamingQueryChunkedApproachOrchestrator.ts
# OR (identical)
npx ts-node src/approaches/StreamingQueryHiveApproachOrchestrator.ts

# Test approximation
npx ts-node src/approaches/StreamingQueryApproximationApproachOrchestrator.ts

# Test fetching client side
npx ts-node src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts
```

## Metrics for Comparison

When experiments run, these metrics are collected for each approach:

- **Latency**: Time to process and return results (ms)
- **Memory Usage**: Peak heap usage (MB)
- **Accuracy**: Deviation from ground truth (%)
- **Throughput**: Observations processed per second
- **CPU Usage**: User and system CPU time
- **Execution Time**: Total time to complete

## Summary

**Key Takeaway**: "Streaming Query Hive" and "Chunked Approach" are the **same thing** - two names for one implementation. This maintains backward compatibility with older configurations while using the more descriptive "chunked approach" name going forward.

The four distinct implementations are:
1. Fetching Client Side (ground truth)
2. Chunked/Hive (distributed chunked processing)
3. Approximation (fast approximate processing)
4. Independent Stream Processing (independent processor pattern)

Choose based on your requirements for accuracy, latency, scalability, and resource usage.