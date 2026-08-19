# Replay Scheduler Fix Report

## Scope

Relevant code changes:

- [StreamToMQTT.ts](/Users/kushbisen/Code/streaming-query-hive/src/streamer/src/publishing/StreamToMQTT.ts)
- [mqttTraffic.ts](/Users/kushbisen/Code/streaming-query-hive/src/util/mqttTraffic.ts)

## What Changed

1. Replay pacing now uses absolute event offsets instead of cumulative fixed sleeps.
2. Replay at lower frequencies now samples the source observations by event-time bucket, rather than merely slowing down publication of the full 10Hz source.
3. Raw MQTT traffic logs now capture scheduler diagnostics per publish.

## Before

- `WEARABLE_FREQUENCY=4` still replayed all 10Hz source observations.
- The first 4Hz validation therefore produced `2400` events per complete window instead of `960`.
- The old 10Hz steady-state run accumulated about `+1.6s/window` of close-to-result drift.

## After

- The patched 4Hz fetching run finalized windows `1..15` with `expected_event_count=960`.
- The full 4Hz run finalized windows `1..15` exactly once for fetching, approximation, and chunked.
- The full 4Hz steady-state close-to-result slopes over windows `3..13` are essentially flat:
  - fetching: `-10.16 ms/window`
  - approximation: `+0.43 ms/window`
  - chunked: `+0.07 ms/window`

## Scheduler Diagnostics

Raw publish lag from `mqtt_traffic.ndjson`:

| Approach | Mean ms | P95 ms | Max ms |
| --- | ---: | ---: | ---: |
| fetching | 2.55 | 4 | 36 |
| approximation | 2.22 | 3 | 91 |
| chunked | 2.25 | 3 | 118 |

These numbers are consistent with a healthy absolute-time scheduler. The remaining end-to-end differences come from approach runtime behavior, not from replay drift accumulation.

## Benchmark Outcome

- fetching comparable close-to-result stayed around `0.38-0.45s` in steady state, with one `0.85s` outlier at window `5`
- approximation stayed around `0.25-0.33s`
- chunked stayed around `0.22-0.24s`
- chunked remained exact vs fetching
- approximation MAPE over windows `3..13` was `0.044471%`
