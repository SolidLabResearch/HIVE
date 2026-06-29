# One-Pattern 15W Fetching Blocker Diagnosis

## Scope

This diagnoses why the fetching run for `one-pattern-latency-fixed-15w-steady` finalized only windows `1..4` even though the current dataset audit reported enough event-time span for 15 complete windows.

Run inspected:

- `logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1`

Relevant config:

- `DATA_PATH=src/streamer/data/approximation_test/challenging/exponential_growth`
- `WEARABLE_FREQUENCY=10`
- `OUTPUT_WINDOW_RANGE=120000`
- `OUTPUT_WINDOW_STEP=60000`
- `AVG`
- deterministic event time enabled
- finite replay enabled

## Short Answer

The run did **not** stop at window 4 because the dataset was too short.

The dataset contains the full `2400` events for each of windows `1..15`, and the replay published far past the close of window 15 for both streams. The actual blocker is the fetching client's wall-clock timing filter in [`src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts`](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts), specifically `isWithinExpectedWindowTiming(...)`.

After window 4, result-arrival drift exceeded the hard `5000 ms` tolerance. Starting at window 5, the client continued to see aligned logical windows, but it dropped them as:

- `Filtered out result due to timing`

So windows `5..15` were:

- considered by the fetching runtime,
- not suppressed for lack of source data,
- not lost by normalization,
- not blocked by finite replay length,
- but rejected before emission by the wall-clock timing gate.

## Artifacts Inspected

- [`fetching_window_diagnostics.csv`](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1/fetching_window_diagnostics.csv)
- [`fetching_latency_log.csv`](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1/fetching_latency_log.csv)
- [`fetching_client_side_log.csv`](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1/fetching_client_side_log.csv)
- [`run_summary.json`](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1/run_summary.json)
- [`replayer-log.csv`](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1/replayer-log.csv)
- [`mqtt_traffic.ndjson`](/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady/fetching/iteration1/mqtt_traffic.ndjson)

`normalized_final_rows.csv` is not present in this fetching iteration directory, so there is no downstream normalized artifact that could have dropped windows after emission.

## What The Fetching Diagnostics Actually Show

`fetching_window_diagnostics.csv` contains only 10 data rows:

- 4 accepted rows: windows `1..4`
- 4 suppressed duplicates: windows `1..4`
- 2 initial suppressed rows for window 1 while waiting for all streams to progress

Reason counts:

- `accepted / finalized_settled_window`: `4`
- `suppressed / duplicate_after_finalization`: `4`
- `suppressed / waiting_for_all_streams_to_progress_past_window_end`: `2`

So the diagnostics CSV confirms:

- exact last finalized window: `4`
- exact first missing finalized window: `5`

## Candidate Windows Seen By The Runtime

The runtime log tells a different story than the final diagnostics CSV.

From `fetching_client_side_log.csv`:

- candidate rows seen: `118`
- unique logical windows seen: `31`
- accepted/finalized windows: `4`
- filtered due to timing: `108`
- finite replay complete signals seen: `2`

Candidate status counts before final acceptance:

- `incomplete_span / observed_timestamp_span_shorter_than_window_range`: `2`
- `incomplete_count / event_count_below_expected_threshold_2376`: `116`

Important point:

- windows beyond 4 were **definitely considered**
- they were **not never-seen windows**
- they were then dropped by the timing filter before emission

## Exact First Missing Window

The first missing output window is window 5:

- `window_start=1756123145256`
- `window_end=1756123265256`
- event-time interval: `2025-08-25T11:59:05.256Z` to `2025-08-25T12:01:05.256Z`

Log excerpt for window 5:

```text
1782246735557,"RStream result generated: 1.0160224011111112 at timestamp: 1782246735556"
1782246735557,"Fetching candidate row seen: {"logicalWindowKey":"1756123145256:1756123265256","windowStart":1756123145256,"windowEnd":1756123265256,"eventCount":1800,"expectedEventCount":2400,"completenessStatus":"incomplete_count","reason":"event_count_below_expected_threshold_2376"}"
1782246735559,"Updated candidate for same window: {"logicalWindowKey":"1756123145256:1756123265256","previousEventCount":null,"nextEventCount":1800}"
1782246735560,"Filtered out result due to timing: 1.0160224011111112"
1782246735581,"RStream result generated: 1.016349071071627 at timestamp: 1782246735581"
1782246735581,"Fetching candidate row seen: {"logicalWindowKey":"1756123145256:1756123265256","windowStart":1756123145256,"windowEnd":1756123265256,"eventCount":1801,"expectedEventCount":2400,"completenessStatus":"incomplete_count","reason":"event_count_below_expected_threshold_2376"}"
1782246735581,"Updated candidate for same window: {"logicalWindowKey":"1756123145256:1756123265256","previousEventCount":1800,"nextEventCount":1801}"
1782246735581,"Filtered out result due to timing: 1.016349071071627"
```

After window 4, there are no later `Accepted/finalized` lines. There are only repeated `Filtered out result due to timing` lines.

## Raw Data Availability For Windows 1..15

The source data is sufficient for every requested window.

Anchor:

- first event timestamp: `1756122905256`
- first event ISO: `2025-08-25T11:55:05.256Z`

Per-window raw counts from the actual `wearable.acceleration.x` and `smartphone.acceleration.x` files:

| Window | Start | End | Wearable | Smartphone | Total |
|---|---:|---:|---:|---:|---:|
| 1 | 1756122905256 | 1756123025256 | 1200 | 1200 | 2400 |
| 2 | 1756122965256 | 1756123085256 | 1200 | 1200 | 2400 |
| 3 | 1756123025256 | 1756123145256 | 1200 | 1200 | 2400 |
| 4 | 1756123085256 | 1756123205256 | 1200 | 1200 | 2400 |
| 5 | 1756123145256 | 1756123265256 | 1200 | 1200 | 2400 |
| 6 | 1756123205256 | 1756123325256 | 1200 | 1200 | 2400 |
| 7 | 1756123265256 | 1756123385256 | 1200 | 1200 | 2400 |
| 8 | 1756123325256 | 1756123445256 | 1200 | 1200 | 2400 |
| 9 | 1756123385256 | 1756123505256 | 1200 | 1200 | 2400 |
| 10 | 1756123445256 | 1756123565256 | 1200 | 1200 | 2400 |
| 11 | 1756123505256 | 1756123625256 | 1200 | 1200 | 2400 |
| 12 | 1756123565256 | 1756123685256 | 1200 | 1200 | 2400 |
| 13 | 1756123625256 | 1756123745256 | 1200 | 1200 | 2400 |
| 14 | 1756123685256 | 1756123805256 | 1200 | 1200 | 2400 |
| 15 | 1756123745256 | 1756123865256 | 1200 | 1200 | 2400 |

This rules out:

- insufficient event-time span for 15 windows
- missing per-window event counts in the source files

## Replay Coverage And Finite Replay Duration

The replay was also long enough.

From `run_summary.json`:

- `completionStatus=completed`
- `publisherExitReason=finite_replay_duration_reached`
- `finiteReplayDurationSeconds=2000`
- `replayLoopCount=1`

From `mqtt_traffic.ndjson`:

- smartphone topic messages: `19462`
- wearable topic messages: `19463`
- per-topic wall-clock publish span: about `2,000,000 ms`

From `replayer-log.csv`:

- smartphone published observations: `19462`
- smartphone first/last event timestamps: `2025-08-25T11:55:05.256Z` to `2025-08-25T12:27:31.256Z`
- wearable published observations: `19463`
- wearable first/last event timestamps: `2025-08-25T11:55:05.256Z` to `2025-08-25T12:27:31.356Z`

This also answers the `publishedObservations=19463` ambiguity:

- it is **not** the total across both streams
- the per-stream replay log shows about `19.46k` publishes for each stream

### Window 15 close check

Window 15 close is:

- `anchor + RANGE + 14*STEP = 1756123865256`
- ISO: `2025-08-25T12:11:05.256Z`

Both replayed streams progressed well beyond:

- window 15 end
- window 15 end + one event

Because the replay reached about:

- smartphone last event timestamp: `2025-08-25T12:27:31.256Z`
- wearable last event timestamp: `2025-08-25T12:27:31.356Z`

So this run was **not** blocked by finite replay termination before window 15 could close.

## Why The Fetching Runtime Still Stopped At 4

The filtering logic is here:

- [`StreamingQueryFetchingClientSideApproachOrchestrator.ts`](/Users/kushbisen/Code/streaming-query-hive/src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts)

Relevant behavior:

1. `isWithinExpectedWindowTiming(...)` accepts the first valid result and anchors subsequent emissions to a wall-clock cadence.
2. It requires:
   - result timestamp deviation from the expected cadence to stay within `tolerance = 5000`
   - enough interval since the last valid result
3. Once the result drift exceeds `5000 ms`, the client logs:
   - `Filtered out result due to timing`
4. It does this **before** emitting the finalized output row.

Observed drift relative to the first accepted result:

| Window | Result wall-clock | Drift from expected 60s cadence |
|---|---:|---:|
| 1 | 1782246489571 | 0 ms |
| 2 | 1782246550803 | 1232 ms |
| 3 | 1782246612054 | 2483 ms |
| 4 | 1782246673778 | 4207 ms |
| 5 | 1782246735560 | 5989 ms |

Window 5 is the first one past the `5000 ms` tolerance, and that is exactly where accepted output stops.

After that point, windows continue to be seen but are dropped by timing.

## Interpreting The `eventCount=1800/1801` Candidate Rows

The late candidate rows show `eventCount=1800` or `1801`, which looks inconsistent with the raw data.

That number comes from the incoming RStream binding that the client logs as `Fetching candidate row seen`. It is **not** the same thing as the final raw-data completeness available in the local observation store.

Evidence:

- windows `1..4` are accepted with final `event_count=2400`
- window `5` and later show `Filtered out result due to timing`
- there are no later `Delayed because incomplete` lines after window 4

That means the decisive blocker for windows `5..15` is **not** the count-completeness gate. The decisive blocker is the timing filter.

## Classification Of Windows 5..15

Requested classification:

- windows `5..15` were **considered**
- they were **not accepted**
- they were **not emitted**
- therefore they were **not dropped by normalization**
- they were **not missing because of finite replay completion**
- they were **blocked by the fetching runtime's timing filter**

## Root Cause

Root cause:

- the fetching runtime still applies a wall-clock cadence gate (`isWithinExpectedWindowTiming`) to finalized windows
- result-arrival drift accumulated past the hard `5000 ms` tolerance by window 5
- the runtime then silently discarded otherwise valid later windows as `Filtered out result due to timing`

This is why a dataset with enough span for 15 windows still finalized only windows `1..4`.

## Conclusions

- exact last finalized window: `4`
- exact first missing finalized window: `5`
- longer data is **not** required for the 15-window target
- simply extending finite replay duration does **not** fix this run
- shutdown-edge exclusion should **not** be the explanation for windows `14..15`
- the fetching blocker is a runtime timing gate, not dataset length and not replay length

## Operational Recommendation Before Running Approximation Or Chunked

Do not use this fetching run as the baseline for the 15-window steady-state benchmark.

Fetching must first be made to finalize aligned windows `1..15` without dropping them due to the wall-clock timing filter; otherwise approximation and chunked comparisons will be measured against a truncated baseline.
