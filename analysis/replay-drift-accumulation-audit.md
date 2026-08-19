# Replay Drift Accumulation Audit

## Root Cause

The original replay path in [StreamToMQTT.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publishing/StreamToMQTT.ts) used cumulative pacing:

- publish one observation
- wait for the MQTT publish callback
- `sleep(1000 / frequency)`
- repeat

That design accumulates callback cost, serialization cost, event-loop jitter, and timer overshoot on every observation. In the previous 10Hz run, that showed up as about `+1.57s` to `+1.61s` of additional close-to-result latency per output window.

There was a second mismatch as well: `WEARABLE_FREQUENCY=4` only slowed wall-clock replay. It did not reduce the 10Hz source event density, so the first 4Hz attempt still produced `2400` events per complete 120s window instead of the expected `960`.

## Fix

The replay path was patched in [StreamToMQTT.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publishing/StreamToMQTT.ts) and [mqttTraffic.ts](/Users/kushbisen/Code/streaming-query-hive/src/util/mqttTraffic.ts):

- added source-density-aware sampling so a 10Hz dataset replayed at 4Hz actually yields `960` events per 120s two-stream window
- replaced cumulative fixed sleeps with absolute-time scheduling:
  `targetPublishTime = replayStartWallClock + eventOffset`
- if late, publish immediately and record lag instead of compounding the delay
- recorded replay diagnostics per raw MQTT publish:
  `targetPublishTime`, `actualPublishTime`, `publishLagMs`, `effectiveReplaySpeed`, `eventTimeSpanMs`, `wallClockPublishSpanMs`

## Validation

The patched 4Hz fetching-only smoke and the full 4Hz three-approach run both validate the fix.

### Expected event count

- Fetching finalized every complete window at `expected_event_count=960`.
- That matches `120s * 4Hz * 2 streams = 960`.

### Raw publish lag

| Approach | Count | Mean ms | P95 ms | Max ms |
| --- | ---: | ---: | ---: | ---: |
| fetching | 7685 | 2.55 | 4 | 36 |
| approximation | 7684 | 2.22 | 3 | 91 |
| chunked | 7683 | 2.25 | 3 | 118 |

### Steady-state close-to-result slope

| Approach | Old 10Hz slope ms/window | Patched 4Hz slope ms/window |
| --- | ---: | ---: |
| fetching | `+1609.08` | `-10.16` |
| approximation | `+1574.40` | `+0.43` |
| chunked | `+1590.81` | `+0.07` |

That is the decisive result: the replay-drift trend is gone.

## Interpretation

- The original long-run drift came from replay scheduling, not from fetching, approximation, or chunked semantics.
- After the scheduler change, all three approaches stay in a plausible close-to-result range for the 15-window 4Hz run.
- Approximation and chunked still rely on report-side wall-clock mapping for comparable close-to-result metrics, but the replay input side is no longer the dominant source of drift.
