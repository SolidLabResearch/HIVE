# HIVE Latency Semantics Bug Audit

## 1. Executive Summary

The observed values are a mix of real scheduling behavior and reporting bugs.

- `fetching ≈ +7.5s` was mostly real, but the reported baseline was wrong. After anchoring the close time to the first raw input publication instead of query registration, the comparable close-to-result latency is about `+5.39s` trailing and `+5.76s` centered.
- `approximation ≈ -55s` is real early availability, but it is mislabeled if presented as exact window-close latency. The operator emits on a wall-clock trigger using `Date.now()` and trigger count, not on the same event-time window close as fetching/chunked.
- `chunked ≈ +60s` is mostly real scheduling behavior in the current configuration: semantic readiness happens about `+3s` to `+7s` after the true event-window close, then cadence-only interval emission adds another `~51s` to `~55s`. The reported `+60s` baseline was also inflated slightly by registration-vs-replay-start skew.

Confirmed bugs:

1. `expected_window_close` in all three latency CSVs is computed from `queryRegisteredTime + RANGE + (N-1)*STEP`, not from the replay anchor actually used by the deterministic publisher.
2. The RSP engine’s `result_emitted_at` metadata is event-time watermark, not wall-clock publish time, but downstream code treated it as wall-clock latency metadata.
3. Chunked centered metadata promoted internal chunk metadata to external output-window metadata, producing the bogus midpoint `window_start + 15000` and `window_data_close_time = window_start + 30000`.
4. Approximation window numbering is trigger-order based, not aligned to the same event-time intervals as fetching/chunked.
5. Approximation’s latency log opens in append mode, so reused log directories can contain stale rows from older runs.

## 2. Direct Answers

1. If `STEP=60s` and `RANGE=120s`, exact-output latency should mean:
   `wall_clock(result emitted) - wall_clock(mapped close of the event-time window [start,end))`

2. Expected emission semantics in the current code:
   - Fetching: after the exact 120s window is complete and both streams have progressed past the window end.
   - Approximation: every output slide on a wall-clock trigger, without waiting for full 120s coverage.
   - Chunked: after all required chunks exist, but with `comparableOutputCadenceOnly=1` and `useImmediateTrigger=0`, emission waits for the next output cadence tick.

3. For the final comparable rows in this benchmark, `window 1` is:
   `[1756122905256, 1756123025256)`

4. Window numbering alignment:
   - Fetching: yes, exact final rows are aligned to the benchmark event-time intervals.
   - Chunked: yes, final comparable rows are aligned to the same intervals.
   - Approximation: no, `window_number` is trigger ordinal, not a shared event-time interval id.

5. `expected_window_close` is not truly the same anchor for all approaches today. It is registration-based everywhere, and registration happens at different times in different pipelines.

6. `result_emitted_at` is not recorded at comparable points in all metadata paths:
   - Top-level latency CSVs use wall-clock `Date.now()`.
   - RSP direct metadata uses event-time watermark.
   - Approximation reconstructed metadata uses wall clock.

## 3. Timeline: Trailing n=3 Iteration 1

Final comparable windows for fetching and chunked share the same event-time intervals:

| Approach | Window | Event-time window | Query registered | First raw input | First subquery/chunk data | Registration-anchored close | Anchor-aligned close | Semantic ready | Result emitted | Close -> result (reported) | Close -> result (anchor-aligned) |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 1 | `[1756122905256,1756123025256)` | 1782202738769 | 1782202740782 | 1782202740783 | 1782202858769 | 1782202860782 | n/a | 1782202864646 | 5877 ms | 3864 ms |
| fetching | 2 | `[1756122965256,1756123085256)` | 1782202738769 | 1782202740782 | 1782202740783 | 1782202918769 | 1782202920782 | n/a | 1782202926103 | 7334 ms | 5321 ms |
| fetching | 3 | `[1756123025256,1756123145256)` | 1782202738769 | 1782202740782 | 1782202740783 | 1782202978769 | 1782202980782 | n/a | 1782202987585 | 8816 ms | 6803 ms |
| approximation | 1 | not a shared event-time interval | 1782203208747 | 1782203210469 | 1782203241569 | 1782203328747 | 1782203330469 | n/a | 1782203272424 | -56323 ms | -58045 ms |
| approximation | 2 | not a shared event-time interval | 1782203208747 | 1782203210469 | 1782203241569 | 1782203388747 | 1782203390469 | n/a | 1782203333874 | -54873 ms | -56595 ms |
| approximation | 3 | not a shared event-time interval | 1782203208747 | 1782203210469 | 1782203241569 | 1782203448747 | 1782203450469 | n/a | 1782203395316 | -53431 ms | -55153 ms |
| chunked | 1 | `[1756122905256,1756123025256)` | 1782203550667 | 1782203552287 | 1782203583378 | 1782203670667 | 1782203672287 | 1782203675439 | 1782203730675 | 60008 ms | 58388 ms |
| chunked | 2 | `[1756122965256,1756123085256)` | 1782203550667 | 1782203552287 | 1782203583378 | 1782203730667 | 1782203732287 | 1782203736871 | 1782203790672 | 60005 ms | 58385 ms |
| chunked | 3 | `[1756123025256,1756123145256)` | 1782203550667 | 1782203552287 | 1782203583378 | 1782203790667 | 1782203792287 | 1782203798302 | 1782203850674 | 60007 ms | 58387 ms |

Notes:

- Chunked `ready_to_emit_ms` is about `55.2s`, `53.8s`, `52.4s` for windows `1..3`.
- Fetching last-data-to-result is only `25ms`, `15ms`, `27ms`; most of its close-to-result time is waiting for the last after-close observations to arrive.

## 4. Timeline: Centered n=3 Iteration 1

Centered mode changes the logical trigger timestamp, not the underlying final 120s event-time interval.

| Approach | Window | Event-time window | Logical trigger | Query registered | First raw input | Registration-anchored close | Anchor-aligned close | Semantic ready | Result emitted | Close -> result (reported) | Close -> result (anchor-aligned) |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 1 | `[1756122905256,1756123025256)` | 1756122965256 | 1782220507617 | 1782220509717 | 1782220627617 | 1782220629717 | n/a | 1782220633973 | 6356 ms | 4256 ms |
| fetching | 2 | `[1756122965256,1756123085256)` | 1756123025256 | 1782220507617 | 1782220509717 | 1782220687617 | 1782220689717 | n/a | 1782220695750 | 8133 ms | 6033 ms |
| fetching | 3 | `[1756123025256,1756123145256)` | 1756123085256 | 1782220507617 | 1782220509717 | 1782220747617 | 1782220749717 | n/a | 1782220757449 | 9832 ms | 7732 ms |
| approximation | 1 | not a shared event-time interval | 1782220873407 | 1782220813407 | 1782220814912 | 1782220933407 | 1782220934912 | n/a | 1782220877099 | -56308 ms | -57813 ms |
| approximation | 2 | not a shared event-time interval | 1782220933407 | 1782220813407 | 1782220814912 | 1782220993407 | 1782220994912 | n/a | 1782220938615 | -54792 ms | -56297 ms |
| approximation | 3 | not a shared event-time interval | 1782220993407 | 1782220813407 | 1782220814912 | 1782221053407 | 1782221054912 | n/a | 1782221000314 | -53093 ms | -54598 ms |
| chunked | 1 | `[1756122905256,1756123025256)` | 1756122965256 | 1782221118505 | 1782221120035 | 1782221238505 | 1782221240035 | 1782221243976 | 1782221298512 | 60007 ms | 58477 ms |
| chunked | 2 | `[1756122965256,1756123085256)` | 1756123025256 | 1782221118505 | 1782221120035 | 1782221298505 | 1782221300035 | 1782221305513 | 1782221358513 | 60008 ms | 58478 ms |
| chunked | 3 | `[1756123025256,1756123145256)` | 1756123085256 | 1782221118505 | 1782221120035 | 1782221358505 | 1782221360035 | 1782221367056 | 1782221418514 | 60009 ms | 58479 ms |

Notes:

- The centered chunked report had previously shown `logical_trigger_time = window_start + 15000`. That was a metadata bug. The corrected report reconstructs the proper midpoint `window_start + 60000`.
- Chunked `window_close_to_ready_ms` is `3941ms`, `5478ms`, `7021ms`; the remaining delay is cadence waiting.

## 5. Event-Time vs Wall-Clock Mapping Diagnosis

### Deterministic publisher

The publisher rewrites event timestamps to:

- `benchmarkStartTime + originalDatasetOffset`
- code: [StreamToMQTT.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publishing/StreamToMQTT.ts:722)

That means the RDF timestamps inside the replay are event-time values anchored to `STREAMING_QUERY_HIVE_BENCHMARK_START_TIME`, not wall-clock publish time.

### Current bug in expected close

All three approaches compute `expected_window_close` from registration time:

- fetching: [StreamingQueryFetchingClientSideApproachOrchestrator.ts](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts:352)
- approximation: [ApproximationDiagnosticsWriter.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/approximation/ApproximationDiagnosticsWriter.ts:43)
- chunked: [StreamingQueryChunkAggregatorOperator.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/StreamingQueryChunkAggregatorOperator.ts:306)

This is not the same as:

- the publisher’s benchmark event-time anchor
- the first raw input wall-clock publication
- the first subquery/chunk arrival

So the reported close baseline is skewed by pipeline startup differences.

### Current bug in direct metadata

The RSP engine emits:

- `result_emitted_at = watermark`
- `window_data_close_time = window.close`
- code: [s2r.ts](/Users/kushbisen/Code/streaming-query-hive/node_modules/rsp-js/src/operators/s2r.ts:279)

Those are event-time-domain values. Downstream fetching/chunked metadata treated them as wall-clock emission metadata, which made `latency_from_window_close_ms` and `latency_from_logical_trigger_ms` invalid.

## 6. Window-Number Alignment Diagnosis

### Fetching

Fetching’s final rows are explicitly settled against the benchmark anchor and exact event counts. Its accepted windows `1..3` are:

- `[1756122905256,1756123025256)`
- `[1756122965256,1756123085256)`
- `[1756123025256,1756123145256)`

Evidence:

- `fetching_window_diagnostics.csv`
- exact `2400` events per accepted window

### Chunked

Chunked’s final comparable rows use the same external window intervals and match fetching’s result values exactly in the shown runs. The emission proof shows the four required 30s chunks per subquery for each 120s output window.

### Approximation

Approximation does not assign the same `window_number` to the same event-time interval.

Reasons:

- it uses `Date.now()` for `windowEnd` and `windowStartGlobal = now - outputQueryWidth`
- code: [RateBasedApproximationApproachOperator.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.ts:520), [RateBasedApproximationApproachOperator.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.ts:604)
- it triggers on `Date.now() - lastTriggerTime >= outputQuerySlide`
- code: [RateBasedApproximationApproachOperator.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.ts:560)
- it increments `windowCount` on each publish trigger
- code: [RateBasedApproximationApproachOperator.ts](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.ts:828)

So approximation `window_number=1` means “first periodic approximation publish”, not “the same closed event-time window as fetching/chunked window 1”.

## 7. Per-Approach Diagnosis

### Fetching

Why `+7.5s` looked suspicious:

- the report used registration-based close time
- fetching query registration leads raw replay by about `2s`
- the replay itself runs slightly slower than ideal `10Hz`
- accepted windows wait until both streams progress past the window end

What is real:

- exact-output close-to-result is about `+4s` to `+8s`
- last-data-to-result is tiny (`~15ms` to `~80ms`)
- the dominant cost is waiting for the last after-close observations to arrive, not post-processing

### Approximation

Why `-55s` happens:

- the operator is intentionally early under current semantics
- it emits on a wall-clock cadence before a full 120s event-time window can close
- it can use `globalLatestValues` and partial window buffers without waiting for all required subwindows

This is not an exact “window-close latency” metric. It is an early approximate availability metric.

### Chunked

Why `+60s` happens:

- required chunks for window 1 are complete only `~4s` to `~7s` after the true close
- with `comparableOutputCadenceOnly=1` and `useImmediateTrigger=0`, emission waits until the next 60s cadence tick
- the next tick is roughly one full `STEP` later

This is real scheduling behavior, not recomposition cost. Recomposition itself is `1ms` to `3ms` in the shown logs.

If `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`, the same data suggests emission would move from about `+58.5s` to about `+4s` to `+7s` after close, plus a tiny compute/publish overhead.

## 8. Centered Metadata Diagnosis

What centered should mean here:

- same 120s event-time window interval
- `logical_trigger_time = window_start + RANGE/2`

What was wrong:

- chunked used internal chunk metadata as if it were external output-window metadata
- for `RANGE=120000`, the report showed `window_start + 15000` instead of `window_start + 60000`
- `window_data_close_time` was also pulled from the first 30s internal chunk instead of the 120s external window end

Why fetching/chunked were `direct` earlier but approximation was `reconstructed`:

- fetching/chunked inherited RSP metadata from the exact/chunk paths
- approximation never had direct event-time window metadata for the final comparable output, so it reconstructed from its own trigger schedule

The patched report now reconstructs chunked centered metadata from the external output window and no longer trusts inconsistent direct latency fields.

## 9. Confirmed Bugs

1. Registration-based `expected_window_close` is mislabeled as a comparable close anchor.
2. RSP direct `result_emitted_at` is event time, not wall clock.
3. Fetching/chunked direct `latency_from_logical_trigger_ms` and `latency_from_window_close_ms` therefore mixed domains.
4. Chunked centered direct metadata used internal chunk midpoint/close instead of external output-window midpoint/close.
5. Approximation uses wall-clock windows and trigger count, so its `window_number` is not aligned to fetching/chunked.
6. Approximation latency logs can accumulate stale rows because the writer appends.

## 10. Misleading but Not Algorithmically Buggy

- Chunked `+60s` under `comparableOutputCadenceOnly=1` and `useImmediateTrigger=0`
- Approximation negative close latency, if explicitly labeled as early approximate availability rather than exact close latency
- Fetching `registration_to_result_ms`, because it depends on pre-replay startup and not just query execution

## 11. Reporting Patch Applied

Patched file:

- [generate-one-pattern-three-approach-n3-summary.js](/Users/kushbisen/Code/streaming-query-hive/scripts/analysis-js/generate-one-pattern-three-approach-n3-summary.js)

What changed:

- comparable latency now uses `anchor_aligned_window_close_to_result_ms`
- the anchor is `first raw input publish + RANGE + (window_number - 1) * STEP`
- chunked centered logical trigger and close metadata are reconstructed from the external 120s window
- inconsistent direct latency metadata is no longer treated as valid wall-clock latency
- stale prepended MQTT rows are ignored by choosing the first raw-input timestamp at or after the current run’s `query_registered_at`

Smoke test run after the patch:

- summary generators rerun successfully for trailing and centered reports
- assertion checks passed for:
  - chunked centered midpoint reconstruction
  - chunked centered external close reconstruction
  - trailing fetching run-scoped raw-input anchoring

## 12. Before / After Latency Table

These are report-level means, not single-window values.

| Scenario | Approach | Before: registration-anchored close -> result | After: anchor-aligned close -> result |
| --- | --- | ---: | ---: |
| trailing n=3 | fetching | +7.45s | +5.39s |
| trailing n=3 | approximation | -54.94s | -56.53s |
| trailing n=3 | chunked | +60.01s | +58.48s |
| centered n=3 | fetching | +7.72s | +5.76s |
| centered n=3 | approximation | -54.78s | -56.32s |
| centered n=3 | chunked | +60.01s | +58.46s |

Interpretation:

- fetching moved down because registration led replay start by about `2s`
- approximation became slightly more negative because the true raw-input anchor is slightly later than query registration
- chunked moved down by about `1.5s`, but the dominant `~58.5s` remains a real cadence wait

## 13. Recommended Final Metrics

### Exact output

- Fetching: `anchor_aligned_window_close_to_result_ms`
- Chunked exact comparable mode: `anchor_aligned_window_close_to_result_ms`

### Early approximate output

- Approximation: `anchor_aligned_window_close_to_result_ms`, explicitly labeled as negative “lead time before exact close”
- Optional companion metric: `result_emitted_at - raw_input_first_published_at`

### Scheduling overhead

- Fetching: `last_data_to_result_ms`
- Chunked: split into
  - `window_close_to_ready_ms`
  - `ready_to_emit_ms`
- Approximation: trigger-to-publish delay is effectively near zero in the shown runs; the dominant issue is semantics, not overhead

### Paper figure

- Main cross-approach figure:
  `anchor_aligned_window_close_to_result_ms`
- Chunked decomposition figure:
  `window_close_to_ready_ms` and `ready_to_emit_ms`
- Approximation should be shown as an early-output tradeoff figure, not as an exact closed-window latency figure

## 14. Recommended Next Runtime Patches

Not applied in this pass:

1. Rename or split RSP metadata fields into explicit event-time vs wall-clock domains.
2. Replace registration-based `expected_window_close` in runtime logs with a replay-anchor-based wall-clock close.
3. Make chunked output metadata direct at the external output-window level, not inherited from the first internal chunk.
4. Make approximation publish metadata carry an explicit wall-clock trigger interval and an explicit “not exact / early” status.
5. Open approximation latency logs in truncate mode for clean runs.
