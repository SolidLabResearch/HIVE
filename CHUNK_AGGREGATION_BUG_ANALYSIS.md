# 🐛 ROOT CAUSE FOUND: Chunk Aggregation Bug

## The Problem

The Chunked approach is producing different results than Fetching because of **incorrect weighted average calculation**.

### What's Happening

1. **Sub-queries produce chunks** with format:
   ```turtle
   <event> hasValue "-23.027" .
   <event> hasCount "119" .
   <event> hasTimestamp "1770224322368" .
   ```

2. **Aggregator combines chunks** using weighted average:
   ```sparql
   SELECT ((SUM(?val * ?cnt) / SUM(?cnt)) AS ?result)
   WHERE {
     ?s saref:hasValue ?val .
     ?s saref:hasCount ?cnt .
   }
   ```

3. **The Bug**: Chunks are selected by **timestamp window overlap**, but their `hasCount` values represent the number of data points in the **sub-query window** (e.g., 60s chunk), NOT the number of points that should contribute to the target window!

### Why It Fails

#### Example for Window 1 [0-120s]:

**Fetching Approach:**
- Queries RSP directly: "Give me AVG of data in [0, 120000ms]"
- RSP counts all data points in that exact range
- Returns: `-23.0` (for step_pattern with 240 uniform points)

**Chunked Approach (BUGGY):**
- Selects chunks whose timestamps fall in [0, 120000ms]
- Each chunk has:
  - `hasValue`: Average of ~119-120 points from a 60s sub-window
  - `hasCount`: 112, 116, 119, or 120 (varies!)
- Combines with weighted average: `SUM(value * count) / SUM(count)`

**The Issue:**
- A chunk with timestamp T=60000ms has `hasCount=119` 
  - This means "I averaged 119 points" in the sub-query window
  - But for Window 1 [0, 120s], we might only want SOME of those 119 points!
  - If the chunk covers [0, 60s] and Window 1 needs [0, 120s], using all 119 points is CORRECT
  - But if another chunk covers [60, 120s] and has different values, the weighting gets complex

### Evidence from Logs

From `streaming_query_chunk_aggregator_log.csv`:

```
Chunk 1: hasValue="-22.99859186607143", hasCount="112"
Chunk 2: hasValue="-22.99372093103448", hasCount="116"  
Chunk 3: hasValue="-22.995263386554623", hasCount="119"
Chunk 4: hasValue="-23.0277611092437", hasCount="119"
Chunk 5: hasValue="-22.974861666666666", hasCount="120"
... (many more)
```

The aggregator does:
```
result = (v1*112 + v2*116 + v3*119 + v4*119 + v5*120 + ...) / (112+116+119+119+120+...)
```

But this is **WRONG** because:
1. Not all chunks should contribute to Window 1
2. Chunks may only partially overlap the window
3. The count values don't represent "how much of this chunk belongs to Window 1"

### Why step_pattern Works

**step_pattern has uniform value (-23) for first 120s:**
- All chunks have value ≈ -23
- Weighted average of -23 with ANY weights = -23
- Formula: `(-23*a + -23*b + -23*c) / (a+b+c) = -23`
- So even though the weighting is wrong, result is correct!

### Why Other Patterns Fail

**spike_pattern, oscillation patterns have VARYING values:**
- Different chunks have different average values
- Example:
  - Chunk A: value=-20, count=119
  - Chunk B: value=-25, count=116
- Wrong weighting produces wrong result
- The 9.45% error on spike_pattern is because sharp value changes amplify the weighting error

## The Correct Solution

### Option 1: Don't Use Weighted Average (WRONG)
Just average the chunk values without weights:
```sparql
SELECT (AVG(?val) AS ?result)
WHERE { ?s saref:hasValue ?val . }
```

**Problem:** This assumes all chunks represent equal amounts of data, which isn't true!

### Option 2: Recalculate Weights Based on Overlap (COMPLEX)
For each chunk, calculate how much it overlaps with the target window:
```typescript
for each chunk:
  overlapStart = max(chunk.dataWindowStart, targetWindowStart)
  overlapEnd = min(chunk.dataWindowEnd, targetWindowEnd)
  overlapDuration = overlapEnd - overlapStart
  
  // Adjust the count proportionally
  adjustedCount = (overlapDuration / chunkDuration) * chunk.count
  
  weightedSum += chunk.value * adjustedCount
  totalCount += adjustedCount
```

**Problem:** Very complex, and chunks don't provide enough info about data distribution

### Option 3: Use Raw Data Points (CORRECT BUT DEFEATS PURPOSE)
Don't aggregate at sub-query level - pass raw data points:
```
- Sub-queries output individual data points (not pre-aggregated)
- Aggregator combines raw points
- Calculate average from combined raw data
```

**Problem:** This defeats the purpose of chunking! We'd be transmitting and storing all individual data points.

### Option 4: Align Chunk Boundaries with Target Windows (IDEAL)
Ensure sub-query windows align perfectly with target window boundaries:
```
If target window is RANGE=120s, STEP=60s:
- Window 1: [0, 120s]
- Window 2: [60s, 180s]

Then sub-queries should produce chunks covering:
- Chunk for [0, 60s]
- Chunk for [60s, 120s]
- Chunk for [120s, 180s]
- etc.

When computing Window 1 [0, 120s]:
- Use chunks [0-60s] and [60s-120s]
- These cover EXACTLY the window
- No partial overlaps!
```

**This is the correct solution!** The GCD-based chunking should already do this, but there's a mismatch somewhere.

## Next Steps

1. **Verify chunk boundaries** - Check if sub-query windows align with target windows
2. **Fix chunk selection logic** - Ensure only chunks that FULLY cover parts of the window are included
3. **Validate the GCD calculation** - Make sure chunkGCD = 60000ms for RANGE=120s, STEP=60s
4. **Test with aligned boundaries** - Verify that when boundaries align perfectly, results match

## Hypothesis

The issue is likely that **sub-query windows don't align perfectly** with the target window boundaries, causing:
- Partial overlaps
- Incorrect assumption that a chunk's count applies fully to the target window
- Weighted average using counts that don't match the actual data distribution in the target window

**The fix:** Ensure sub-queries produce chunks that align with target window boundaries, OR adjust the weighting to account for partial overlaps.
