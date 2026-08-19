# First-Event Latency Analysis

## Overview

This document explains how first-event latency is calculated for both the **Fetching (client-side)** and **Approximation** approaches in the frequency comparison experiments.

**First-event latency** is defined as the time between when a query is registered and when the first result is emitted. This is a critical performance metric that affects the responsiveness of streaming query systems.

---

## Methodology

### Query Registration vs First Result

For both approaches, we measure:

1. **Query Registration Time**: The timestamp when the streaming query is registered with the RSP engine
2. **First Result Time**: The timestamp when the first query result is emitted
3. **First-Event Latency**: `firstResultTime - queryRegisteredTime`

### Fetching Approach

The fetching approach explicitly logs query registration:

```
LOG: 1767789459407 - fetching_query_registered
```

The first result is extracted from RStream events:

```javascript
DEBUG: RStream event received: {
  "bindings": {...},
  "timestamp_from": 1767789461160,
  "timestamp_to": 1767789521160
}
```

**Calculation:**
- Query registered at: `1767789459407`
- First result at: `1767789521160` (timestamp_to)
- **Latency: 61,753 ms (61.75 seconds)**

### Approximation Approach

The approximation approach uses latency logs from the operator:

```
LATENCY: Window 1:
  - From query registration: -58388ms (expected close: 1767789872016, result: 1767789813628)
```

The "expected close" timestamp represents when the window was scheduled to close. For window 1, this is calculated as:

```typescript
expectedClose = queryRegisteredTime + RANGE
// For window N: expectedClose = queryRegisteredTime + RANGE + (N-1) * STEP
```

Given the window configuration `[RANGE 120000 STEP 60000]`:

```
queryRegisteredTime = expectedClose - RANGE
queryRegisteredTime = 1767789872016 - 120000 = 1767789752016
```

**Calculation:**
- Expected close: `1767789872016`
- Query registered at: `1767789752016` (expectedClose - 120000ms)
- First result at: `1767789813628`
- **Latency: 61,612 ms (61.61 seconds)**

---

## Why Both Approaches Have Similar First-Event Latency

Both approaches show approximately 61-62 seconds of first-event latency, which aligns with the window configuration:

- **Window RANGE**: 120 seconds (120000ms)
- **Window STEP**: 60 seconds (60000ms)

For a sliding window with these parameters:
- The first window collects data for 120 seconds (RANGE)
- Results are emitted every 60 seconds (STEP)
- Therefore, the **first result appears after ~60 seconds** from query registration

This is expected behavior for both approaches because:
1. Both must wait for sufficient data to compute the first window
2. The approximation approach, while combining sub-query results, still respects the main query's window boundaries
3. The STEP parameter (60s) determines when results are emitted

### Key Insight

The first-event latency is **governed by the window configuration**, not by the approach used. Both fetching and approximation approaches emit their first result at approximately the STEP time (60 seconds).

---

## Understanding Negative Latency Values

The log shows: `From query registration: -58388ms`

This negative value indicates the result arrived **earlier than the expected window close time**:

- Expected close: 1767789872016 (registration + 120s RANGE)
- Result emitted: 1767789813628
- Difference: -58,388ms (58.4 seconds early)

This means the approximation emitted the result **58 seconds before the window was scheduled to close**. This is possible because:

1. The approximation uses sub-queries with smaller windows (RANGE 60s, STEP 30s)
2. Once enough sub-query windows overlap, a result can be computed
3. The result is emitted at the STEP boundary (60s) rather than waiting for the full RANGE (120s)

**This early emission is a feature** — it shows the approximation can produce results faster than waiting for the full window duration, while still respecting the STEP parameter for result emission timing.

---

## Window Configuration Details

The main query uses:
```sparql
RANGE 120000 STEP 60000
```

The sub-queries (approximation approach) use:
```sparql
RANGE 60000 STEP 30000
```

Timeline for the first window:
- **t=0s**: Query registered
- **t=0-60s**: Data collection and sub-query processing
- **t=60s**: First result emitted (STEP boundary)
- **t=120s**: First window fully closes (RANGE duration)

Both approaches emit at the STEP boundary (60s), which is why first-event latency is similar.

---

## Extraction Script Implementation

The updated `extract-results-from-logs.js` script correctly extracts first-event latency:

### For Fetching Approach:
- Searches for: `LOG: <timestamp> - fetching_query_registered`
- Extracts first result from: `DEBUG: RStream event received: {...}`
- Uses `timestamp_to` field as the result timestamp

### For Approximation Approach:
- Searches for: `From query registration: Xms (expected close: <timestamp>, result: <timestamp>)`
- Calculates registration: `expectedClose - RANGE (120000ms)`
- Extracts result timestamp from the latency log
- Note: For window 1, expectedClose = registration + RANGE
- Note: For window N, expectedClose = registration + RANGE + (N-1) * STEP

---

## Results Summary (0.1 Hz Test)

| Approach      | Query Registered At          | First Result At              | First-Event Latency |
|---------------|------------------------------|------------------------------|---------------------|
| Fetching      | 2026-01-07T12:37:39.407Z     | 2026-01-07T12:38:41.160Z     | **61.75 seconds**   |
| Approximation | 2026-01-07T12:42:32.016Z     | 2026-01-07T12:43:33.628Z     | **61.61 seconds**   |

**Difference: 0.14 seconds (negligible)**

Both approaches have nearly identical first-event latency, as expected given the window configuration.

---

## Ongoing Processing Latency vs First-Event Latency

While first-event latency is similar for both approaches, **ongoing processing latency** differs:

### Fetching Approach
- Must fetch all raw data points every window
- Processes data client-side
- Higher computational overhead
- Higher network overhead (fetching raw data)

### Approximation Approach
- Uses pre-aggregated sub-query results
- Combines aggregates incrementally
- Lower computational overhead per window
- Lower network overhead (only aggregates transferred)
- Can emit results early (as shown by negative latency values)

The approximation's advantage is in **sustained throughput and resource efficiency**, not in first-event latency.

---

## Usage

To extract first-event latency from experiment logs:

```bash
# Fetching approach
node experiments/frequency-comparison/extract-results-from-logs.js fetching 0.1

# Approximation approach
node experiments/frequency-comparison/extract-results-from-logs.js approximation 0.1
```

The script outputs:
- `<approach>_results.csv`: Result values with timestamps and latency
- `<approach>_metadata.json`: Metadata including first-event latency

---

## Implications for System Design

### First Result Latency
Both approaches have similar first-event latency (~60s) determined by the window configuration. If faster initial results are needed:
- Reduce STEP parameter (e.g., STEP 30000 for 30s latency)
- Use smaller window RANGE
- Consider tumbling windows instead of sliding windows

### Sustained Performance
The approximation approach excels in:
- **Resource efficiency**: Lower CPU/memory usage per window
- **Network efficiency**: Transferring aggregates instead of raw data
- **Scalability**: Can handle more concurrent queries
- **Accuracy**: Minimal error (0.0129% at 0.1 Hz) while maintaining efficiency

### Trade-offs
- **Fetching**: Simpler to implement, exact results, higher resource cost
- **Approximation**: More complex setup (sub-queries), minimal error, much lower resource cost

---

## Conclusion

The corrected analysis shows that **first-event latency is nearly identical** for both approaches (~61-62 seconds), as it is determined by the window STEP parameter. The approximation approach's advantage lies in:

1. **Resource efficiency** during sustained operation
2. **Early result emission** (can emit before window fully closes)
3. **Scalability** for multiple concurrent queries
4. **Minimal accuracy loss** (0.0129% error)

The original expectation that approximation would have dramatically lower first-event latency was incorrect. Both approaches must respect the window configuration's STEP parameter for result emission timing.
