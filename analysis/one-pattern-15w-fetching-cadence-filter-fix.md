# One-Pattern 15W Fetching Cadence Filter Fix

## What Changed

The fetching client-side orchestrator now bypasses the wall-clock cadence filter in deterministic benchmark mode, so finalized aligned windows are no longer truncated once wall-clock drift exceeds the old `5000 ms` tolerance.

Change made in:

- [`src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts`](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts)

Key behavior:

- deterministic benchmark mode skips `isWithinExpectedWindowTiming(...)`
- completeness and duplicate suppression still gate finalization
- diagnostics now record whether the timing filter was enabled or bypassed

Tests added:

- deterministic mode accepts a complete aligned window even when the cadence filter would reject it
- legacy/live mode still applies the cadence filter
- incomplete windows remain suppressed
- duplicates remain suppressed

## Validation Evidence

I reran the fetching benchmark on:

- `one-pattern-latency-fixed-15w-steady`
- `DATA_PATH=approximation_test/challenging/exponential_growth`
- `WEARABLE_FREQUENCY=10`
- `OUTPUT_WINDOW_RANGE=120000`
- `OUTPUT_WINDOW_STEP=60000`
- deterministic event time enabled
- finite replay duration `2000s`

Observed in `logs/one-pattern-latency-fixed-15w-steady/fetching/iteration2`:

- windows `1..15` were finalized exactly once each
- `timing_filter_enabled=false`
- `timing_filter_bypassed_reason=STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1`
- `filtered_due_to_timing_count=0`
- no incomplete window was accepted
- no duplicate window was accepted

The relevant accepted rows show the expected progression:

- window 4
- window 5
- window 6
- ...
- window 15

## Caveat

Because the replay duration is longer than the 15-window horizon, the replay can continue into a tail window beyond the benchmark target. In the validation run, window `16` appeared after window `15`.

That tail is outside the requested `1..15` benchmark slice, but if you want the process to stop exactly at 15 in future runs, add a benchmark window cap or a domain-max guard before the next long smoke.

## Outcome

The fetching 15-window blocker caused by the wall-clock cadence filter is fixed.

The steady-state benchmark can now use the intended aligned windows for downstream approximation and chunked comparisons.
