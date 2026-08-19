# Bug Report: Chunk Window Alignment — First Window Incorrect Result

**Component:** `StreamingQueryChunkAggregatorOperator`
**File:** `src/services/operators/StreamingQueryChunkAggregatorOperator.ts`
**Severity:** High (produces wrong result for window 1 in every run)
**Discovered:** 2026-03-30

---

## Summary

The Chunked Query Reuse approach produced a ~9% error on its first window result
compared to the Fetching (Local-Only) baseline, despite being mathematically sound
and showing 0% error in all subsequent windows. The root cause was that the chunk
collection logic used wall-clock arrival time to determine window boundaries, which
broke when the data replayer ran slightly faster than real-time.

---

## Background

The Chunked approach works as follows:

1. Sub-queries are **rewritten** from sliding windows (RANGE 60 s / STEP 30 s) to
   **tumbling windows** (RANGE 30 s / STEP 30 s = chunkGCD). This ensures every
   event appears in exactly one chunk — no double-counting.
2. Each 30 s tumbling window produces a chunk carrying `saref:hasValue` (AVG) and
   `saref:hasCount` (COUNT) as RDF triples.
3. The aggregator collects chunks from both sub-queries and computes:

   ```
   SUM(avg_i × count_i) / SUM(count_i)
   ```

   This is a weighted average over non-overlapping chunks — mathematically identical
   to computing AVG over all raw events in the window. The algorithm is **sound**.

4. The super-query window (RANGE 120 s / STEP 60 s) fires every 60 s (one STEP).
   At each firing, the aggregator collects the chunks that arrived in the last
   `outputQueryWidth = 120 s` of **wall-clock time**.

---

## The Bug

### Trigger: replayer runs slightly faster than 1×

The data replayer sends sensor events at approximately (but not exactly) 4 Hz
real-time. In practice it runs at roughly 1.003×, meaning 60 s of data is replayed
in ~59.8 s wall-clock time.

### Chunk arrival timeline (wall-clock, relative to first chunk `T₀`)

| Wall-clock offset | Event |
|---|---|
| T₀ + 0 ms | Chunk 1 arrives (data 0–30 s) — per sensor |
| T₀ + ~30,000 ms | Chunk 2 arrives (data 30–60 s) — per sensor |
| T₀ + ~59,849 ms | **Chunk 3 arrives (data 60–90 s)** — per sensor |
| T₀ + ~60,001 ms | Interval trigger fires (first window) |

Because the replayer is ~0.3% faster than real-time, **chunk 3 (data 60–90 s)
arrives 151 ms before the 60 s trigger** fires.

### Effect on window collection

The first window trigger uses:

```typescript
const windowStart = now - outputQueryWidth;  // now − 120 s
```

At trigger time `T₀ + 60,001 ms`:

```
windowStart = T₀ + 60,001 − 120,000 = T₀ − 59,999 ms
```

This is well before `T₀`, so **all chunks are within the window** — including chunk 3
which covers data 60–90 s. The aggregator therefore computes a weighted average over
90 s of data instead of 60 s.

### Measured impact

| Approach | Window 1 result | Error |
|---|---|---|
| Fetching (baseline) | −10.7671 | 0% |
| Chunked (buggy) | −11.7357 | **~9%** |

The fetching approach uses RSP-JS which fires based on **data timestamps** and
correctly covers [0, 60 s] of data for window 1. The chunked approach included an
extra 30 s of data, producing a different average.

### Why previous 35-iteration experiments showed 0% error

The prior experiments ran long enough that the reported result was from a **later
steady-state window** (window 2 or beyond), not window 1. From window 2 onwards,
`lastProcessedTime` provides a stable anchor and the timing drift is small enough
that the results align. The comparison script also averaged over multiple windows,
diluting the first-window anomaly.

---

## Root Cause Analysis

The core issue: **wall-clock chunk arrival time is used as a proxy for data-time
window membership**. This assumption breaks when replay speed ≠ 1×.

Three fixes were attempted:

### Attempt 1 — Align interval to first data arrival

Restart the `setInterval` when the first chunk arrives so it fires at exactly
`firstDataTime + outputQuerySlide`. This reduced the trigger offset but did **not**
fix the problem: at trigger time `T₀ + 60,001 ms`, chunk 3 had already arrived at
`T₀ + 59,849 ms` (within the window boundary), so it was still included.

### Attempt 2 — Canonical window boundaries

Anchor window start/end to `firstDataReceivedTime` mathematically:

```typescript
canonicalWindowEnd   = firstDataReceivedTime + (windowsSoFar + 1) * outputQuerySlide
canonicalWindowStart = canonicalWindowEnd - outputQueryWidth
```

Filter: `chunk.timestamp >= canonicalWindowStart && chunk.timestamp < canonicalWindowEnd`

This also failed: chunk 3 arrived at `T₀ + 59,849 ms` which is strictly less than
`canonicalWindowEnd = T₀ + 60,000 ms`, so it passed the filter and was still included.

Both approaches failed because they still relied on wall-clock chunk arrival times
as a proxy for the data-time range a chunk covers. A chunk arriving at 59.8 s
wall-clock time can carry data from 60–90 s if the replayer is even slightly faster
than 1×.

---

## The Fix

### Approach: count-based chunk selection

Data is **always replayed in chronological order**. Therefore, the chunks that arrive
earliest always cover the earliest data period, regardless of replay speed. The
**n oldest chunks per topic** (sorted by arrival time) are guaranteed to correspond
to the first n × chunkGCD seconds of data.

For window 1: take the oldest `STEP / chunkGCD = 2` chunks per topic.
For subsequent windows: use `lastProcessedTime` as anchor and take the oldest
`RANGE / chunkGCD = 4` chunks per topic that arrived after the window start.

```typescript
const chunksPerStep       = Math.ceil(outputQuerySlide / this.chunkGCD); // 2
const chunksPerFullWindow = Math.ceil(outputQueryWidth  / this.chunkGCD); // 4

const sorted = [...chunks].sort((a, b) => a.timestamp - b.timestamp);

let windowChunks;
if (this.lastProcessedTime === 0) {
    // First window: oldest chunksPerStep chunks per topic
    // These are always the [0, STEP] data period regardless of replay speed.
    windowChunks = sorted.slice(0, chunksPerStep);
} else {
    // Subsequent windows: oldest chunksPerFullWindow chunks after window start
    const inWindow = sorted.filter(c => c.timestamp >= canonicalWindowStart);
    windowChunks = inWindow.slice(0, chunksPerFullWindow);
}
```

### Result after fix

| Approach | Window 1 result | Error |
|---|---|---|
| Fetching (baseline) | −10.7671 | 0% |
| **Chunked (fixed)** | **−10.7671** | **0%** ✓ |

---

## Why This Fix Is Robust

- **Replay speed independent.** No matter how fast or slow the replayer runs, the
  oldest 2 chunks per topic will always be the [0–30 s] and [30–60 s] data windows.
- **No data-time parsing required.** The fix works without needing to embed or parse
  data-time timestamps in chunk payloads.
- **Preserves algorithm correctness.** The weighted average over exactly 2 non-
  overlapping tumbling chunks per topic is mathematically identical to AVG over all
  raw events in [0, 60 s] — matching the fetching baseline exactly.

---

## Files Changed

| File | Change |
|---|---|
| `src/services/operators/StreamingQueryChunkAggregatorOperator.ts` | Replace time-based chunk filter with count-based selection per topic; also align `setInterval` to first data arrival |

