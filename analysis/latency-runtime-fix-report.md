# Latency Runtime Fix Report

## Status
The approximation runtime path still uses the structured completed-window branch, and the smoke validation is now complete across fetching, approximation, and chunked.

- `approximation` still emits windows `1,2,3`.
- `approximation` branch selection remains `branch=structured`.
- no legacy fallback is used.
- runtime no longer writes a mixed-domain `anchor_aligned_window_close_to_result_ms`.
- the comparable approximation close-to-result numbers are now derived from the MQTT replay wall-clock anchor, exactly like the fetching/chunked comparison reports.
- `chunked` emits windows `1,2,3` with exact values and no duplicates.
- `chunked` runtime diagnostics no longer write the old mixed-domain `2611458xxxx` comparable latency values.
- `chunked` now marks the raw runtime rows as `latency_domain_status=domain_mismatch` when no valid wall-clock close anchor is available in runtime.

## What Was Wrong
The bad value `26105071228 ms` came from subtracting:

- event-time window close: `1756123025256`
- wall-clock result emission: `1782236914575`

That subtraction spans different time domains, so the result looked like about `302` days even though the real emission delay was only a few seconds.

Concretely, the old structured approximation path recorded:

- `window_end` / `window_data_close_time`: event-time
- `result_emitted_at`: wall-clock
- `anchor_aligned_window_close_to_result_ms = result_emitted_at - window_end`

That was invalid.

## Runtime Fix
`/Users/kushbisen/Code/streaming-query-hive/src/services/operators/approximation/ApproximationDiagnosticsWriter.ts` now separates the domains:

- `event_time_window_close` keeps the event-time close from reusable-result metadata.
- `wall_clock_window_close` is only written if the runtime has a valid wall-clock replay anchor in the same domain.
- `wall_clock_close_to_result_ms` is only written when that wall-clock close is known.
- if the runtime anchor is missing or mismatched, the row is marked with `latency_domain_status=domain_mismatch` and the close-to-result field is left blank.

It also enforces a plausibility check:

- fail if `abs(wall_clock_close_to_result_ms) > 120000`

`/Users/kushbisen/Code/streaming-query-hive/src/services/operators/RateBasedApproximationApproachOperator.ts` no longer publishes:

- `anchorAlignedWindowClose`
- `anchorAlignedWindowCloseToResultMs`

from event-time metadata. It publishes `eventTimeWindowClose` instead.

## Before / After
### Before
Approximation structured rows mixed domains:

| window | event-time close | wall-clock result | bogus close-to-result |
| --- | ---: | ---: | ---: |
| 1 | 1756123025256 | 1782236914575 | 26105071228 |
| 2 | 1756123085256 | 1782236976117 | 26105090861 |
| 3 | 1756123145256 | 1782237037658 | 26105192402 |

### After
Runtime now writes the row as event-time only unless a valid wall-clock anchor exists:

```csv
1,1782236789582,1782236822084,1782236909582,1782236909582,1756123025256,,1782236914575,1782236914575,4993,-27509,0,,domain_mismatch,completed_window_approximation,trailing,1756122965256,1756122905256,1756123025256,1756123025256,,,reconstructed,1.0025041166666666
```

Key change:

- `event_time_window_close=1756123025256`
- `wall_clock_window_close=` blank
- `wall_clock_close_to_result_ms=` blank
- `latency_domain_status=domain_mismatch`

So the runtime no longer emits a fake latency.

## Corrected Approximation Latency Table
For the smoke run, the report-side comparable latency uses the same wall-clock mapping as fetching/chunked:

- approximation first raw input publication from MQTT trace:
  - `1782236791119`
- mapped wall-clock close for window `N`:
  - `first_raw_input_published_at + RANGE + (N - 1) * STEP`

With `RANGE=120000` and `STEP=60000`:

| window | mapped wall-clock close | result_emitted_at | corrected close-to-result |
| --- | ---: | ---: | ---: |
| 1 | 1782236911119 | 1782236914575 | 3456 |
| 2 | 1782236971119 | 1782236976117 | 4998 |
| 3 | 1782237031119 | 1782237037658 | 6539 |

These values are plausible and in the same operational range as fetching.

## Fetching Comparison
From `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-smoke/fetching/iteration1/fetching_latency_log.csv`:

| window | expected close | result_emitted_at | fetching close-to-result |
| --- | ---: | ---: | ---: |
| 1 | 1782236586690 | 1782236592761 | 6071 |
| 2 | 1782236646690 | 1782236654188 | 7498 |
| 3 | 1782236706690 | 1782236716001 | 9311 |

Approximation is now in the same latency domain and the same order of magnitude as fetching.

## Branch Log
From `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-smoke/approximation/iteration1/approximation_approach_log.csv`:

```text
Approximation mode configuration: {"completedWindowMode":true,"earlyTriggerMode":false}
Approximation branch decision: topic=chunked/bf5f09395e17db1b5fc2904cf4673f69 branch=structured window_start=1756122875256 window_end=1756122935256
Approximation branch decision: topic=chunked/43eccb2e15dda5f97f3f09904b2f5dfc branch=structured window_start=1756122875256 window_end=1756122935256
Approximation message counters: {"legacy_messages_seen":0,"structured_messages_seen":18,"adapted_legacy_messages_seen":0,"suppressed_missing_window_metadata":0}
```

## Chunked Runtime Fix
From `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-smoke/chunked/iteration1/chunked_latency_log.csv` and `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-smoke/chunked/iteration1/mqtt_traffic.ndjson`:

- chunked first raw input publication from MQTT trace:
  - `1782238291775`
- corrected mapped wall-clock close for window `N`:
  - `first_raw_input_published_at + RANGE + (N - 1) * STEP`
- unique emitted windows:
  - `1,2,3`
- duplicate windows:
  - none

Corrected wall-clock close-to-result:

| window | mapped wall-clock close | result_emitted_at | corrected close-to-result | ready_to_emit_ms | chunked value |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1782238411775 | 1782238415128 | 3353 | 2 | 1.0028534916666667 |
| 2 | 1782238471775 | 1782238476635 | 4860 | 2 | 1.0058389366666667 |
| 3 | 1782238531775 | 1782238538184 | 6409 | 2 | 1.00911985 |

Raw runtime rows are now domain-safe:

| window | wall_clock_window_close | wall_clock_close_to_result_ms | anchor_aligned_window_close_to_result_ms | latency_domain_status |
| --- | --- | --- | --- | --- |
| 1 | blank | blank | blank | `domain_mismatch` |
| 2 | blank | blank | blank | `domain_mismatch` |
| 3 | blank | blank | blank | `domain_mismatch` |

Example raw runtime row after the fix:

```csv
1,1782238289803,1782238322855,1782238409803,1782238409803,1756122905256,1756123025256,1756123025256,,,1782238415126,1782238415128,1782238415128,5325,-27727,2,0,1.0028534916666667,1756122905256-1756122935256|1756122935256-1756122965256|1756122965256-1756122995256|1756122995256-1756123025256,1782238415126,1782238415126,,2,,,domain_mismatch,immediate,immediate_ready_check,trailing,1756122935256,1756122905256,1756123025256,1756123025256,,,direct
```

Meaning:

- `event_time_window_start/end/close` are preserved
- `wall_clock_window_close` is blank
- `wall_clock_close_to_result_ms` is blank
- `window_close_to_ready_ms` is blank
- `ready_to_emit_ms` remains valid and near zero
- no mixed-domain close subtraction is written

## Smoke Results
Completed reruns in this validation pass:

- fetching
- approximation
- chunked

Result values:

| approach | window 1 | window 2 | window 3 |
| --- | ---: | ---: | ---: |
| fetching result | 1.0028534916666654 | 1.0058389366666671 | 1.00911985 |
| approximation result | 1.0025041166666666 | 1.0054150224999998 | 1.008696943095238 |
| chunked result | 1.0028534916666667 | 1.0058389366666667 | 1.00911985 |

## Final Comparison Table
| window | fetching close-to-result ms | approximation close-to-result ms | chunked close-to-result ms | approximation MAPE % | chunked MAPE % |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 6071 | 3456 | 3353 | 0.03963063795355186 | 0.000000000000059 |
| 2 | 7498 | 4998 | 4860 | 0.03963063795355186 | 0.000000000000059 |
| 3 | 9311 | 6539 | 6409 | 0.03963063795355186 | 0.000000000000059 |

## Validation
- `approximation` still uses `branch=structured`: pass
- no legacy fallback: pass
- approximation emits windows `1,2,3`: pass
- approximation close-to-result is not negative: pass
- approximation close-to-result is not enormous: pass
- approximation close-to-result is in the expected `0-15000 ms` range: pass
- runtime no longer writes a mixed-domain close-to-result metric: pass
- chunked emits windows `1,2,3`: pass
- chunked values match fetching to recorded precision: pass
- chunked `ready_to_emit_ms` remains near zero: pass
- chunked corrected wall-clock close-to-result is plausible and in the expected `4-10s` neighborhood: pass
- chunked has no duplicate windows: pass
- raw chunked CSV no longer contains `2611458xxxx`-style mixed-domain latency: pass
- chunked runtime explicitly marks missing wall-clock close anchors as `domain_mismatch`: pass

## Tests
- `npx jest src/services/operators/StreamingQueryChunkAggregatorOperator.test.ts --runInBand`
- `npx tsc --noEmit`

Added regression coverage for:

- chunked diagnostics leave close-based wall-clock fields blank for `domain_mismatch`
- chunked diagnostics write plausible `wall_clock_close_to_result_ms` only when the anchor is valid
- semantic-ready emission still reports `ready_to_emit_ms` near zero

## Acceptance Summary
Accepted on emitted outputs and report-side comparable latency:
Accepted on emitted outputs, raw chunked runtime diagnostics, and report-side comparable latency:

- fetching close-to-result is plausible
- approximation close-to-result is plausible
- chunked close-to-result is plausible when mapped from the chunked MQTT wall-clock anchor
- approximation MAPE is `0.03963063795355186%`
- chunked MAPE is effectively `0%`
- raw chunked CSV no longer writes mixed-domain `2611458xxxx` close-to-result values
- raw chunked CSV explicitly reports `domain_mismatch` when runtime cannot derive a wall-clock close anchor

## n=3 Validation

The latency-fixed n=3 benchmark completed cleanly across fetching, approximation, and chunked. The corrected summary is written to `/Users/kushbisen/Code/streaming-query-hive/analysis/one-pattern-latency-fixed-n3-summary.md`.

| Metric | Value |
| --- | ---: |
| fetching close-to-result | 5572.78 ± 100.44 ms |
| approximation close-to-result | 5216.67 ± 89.44 ms |
| chunked close-to-result | 4884.78 ± 52.11 ms |
| approximation MAPE | 0.03963063795355186% |
| chunked MAPE | ~0% |

Validation checks:

- all 9 runs exited cleanly
- windows `1,2,3` were emitted exactly once by every approach
- approximation stayed on `branch=structured`
- no legacy fallback was used
- no mixed-domain latency values were emitted in the raw runtime CSVs
- chunked matched fetching exactly at recorded precision
- process-tree RSS/CPU was reported for all approaches
