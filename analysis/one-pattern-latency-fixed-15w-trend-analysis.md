# One-Pattern Latency-Fixed 15-Window Trend Analysis

## Result

The 15-window close-to-result latency is not stationary for any approach. It increases almost linearly over time and is best explained by replay wall-clock drift rather than random variance, computation spikes, or semantic-ready delay.

Over the steady-state slice `windows 3..13`:

- fetching slope: `+1609.08 ms/window`
- approximation slope: `+1574.40 ms/window`
- chunked slope: `+1590.81 ms/window`

The high standard deviation in the steady-state summary is therefore mostly trend-driven. Each approach has `R^2 ~= 0.9996-0.99997` for latency vs window number on `windows 3..13`, which is far too linear to describe as noise.

## Latency By Window

| Window | Phase | Fetching ms | Approximation ms | Chunked ms |
| --- | --- | ---: | ---: | ---: |
| 1 | warmup | 4280 | 3446 | 3388 |
| 2 | warmup | 5865 | 4933 | 4856 |
| 3 | steady | 7478 | 6473 | 6349 |
| 4 | steady | 9148 | 8087 | 7821 |
| 5 | steady | 10681 | 9571 | 9311 |
| 6 | steady | 12334 | 11191 | 10920 |
| 7 | steady | 13924 | 12773 | 12799 |
| 8 | steady | 15538 | 14339 | 14359 |
| 9 | steady | 17188 | 16068 | 15963 |
| 10 | steady | 18769 | 17477 | 17473 |
| 11 | steady | 20364 | 19058 | 19011 |
| 12 | steady | 21937 | 20626 | 20577 |
| 13 | steady | 23610 | 22213 | 22068 |
| 14 | end-edge | 25184 | 23931 | 23610 |
| 15 | end-edge | 26843 | 25564 | 25192 |

## Trend Classification

- fetching: increasing over time, almost perfectly linear
- approximation: increasing over time, almost perfectly linear
- chunked: increasing over time, almost perfectly linear
- sawtooth/cadence-like behavior: not dominant
- replay drift: yes, this is the main pattern

The per-window increments are very regular:

- fetching delta mean over `3..13`: `+1613.20 ms/window`, delta std `46.78 ms`
- approximation delta mean over `3..13`: `+1574.00 ms/window`, delta std `83.96 ms`
- chunked delta mean over `3..13`: `+1571.90 ms/window`, delta std `117.90 ms`

That is a clear monotone drift, not a stationary series with random scatter.

## Slopes

| Approach | Slice | Slope ms/window | R² |
| --- | --- | ---: | ---: |
| fetching | `1..15` | 1610.19 | 0.999988 |
| fetching | `3..13` | 1609.08 | 0.999975 |
| approximation | `1..15` | 1578.56 | 0.999893 |
| approximation | `3..13` | 1574.40 | 0.999903 |
| chunked | `1..15` | 1571.93 | 0.999719 |
| chunked | `3..13` | 1590.81 | 0.999555 |

## Per-Window Deltas

| Window | Approx - Fetching ms | Chunked - Fetching ms | Chunked - Approx ms |
| --- | ---: | ---: | ---: |
| 1 | -834 | -892 | -58 |
| 2 | -932 | -1009 | -77 |
| 3 | -1005 | -1129 | -124 |
| 4 | -1061 | -1327 | -266 |
| 5 | -1110 | -1370 | -260 |
| 6 | -1143 | -1414 | -271 |
| 7 | -1151 | -1125 | 26 |
| 8 | -1199 | -1179 | 20 |
| 9 | -1120 | -1225 | -105 |
| 10 | -1292 | -1296 | -4 |
| 11 | -1306 | -1353 | -47 |
| 12 | -1311 | -1360 | -49 |
| 13 | -1397 | -1542 | -145 |
| 14 | -1253 | -1574 | -321 |
| 15 | -1279 | -1651 | -372 |

Interpretation:

- Approximation is consistently faster than fetching by about `1.0-1.4s`.
- Chunked is consistently faster than fetching by about `1.1-1.7s`.
- Chunked is usually slightly faster than approximation, though the gap is small around windows `7, 8, 10`.

## Why The Std Dev Is High

The steady-state summary reported:

- fetching: `15542.82 ± 5336.79 ms`
- approximation: `14352.36 ± 5221.95 ms`
- chunked: `14241.00 ± 5277.29 ms`

Those `~5.2-5.3s` standard deviations are not evidence of unstable jitter. They are mostly the spread caused by a strong upward trend across `windows 3..13`.

The line fits show this directly:

- correlation of window number vs latency is `0.99978-0.99999`
- per-window delta std is only `~47-118 ms`, much smaller than the total latency std

So the variance is dominated by deterministic drift over time, not by noisy per-window fluctuations.

## Correlation Checks

### Replay Wall-Clock Drift

This is the strongest explanation.

- Each approach replays about `985-987s` of raw-input wall-clock span.
- The close-to-result metric rises by about `1.57-1.61s` every output window.
- The runtime fields `delay_past_expected_close_ms` and close-to-result move together almost perfectly where available.

Correlations on `windows 3..13`:

| Approach | corr(window, latency) | corr(delay_past_expected_close, latency) |
| --- | ---: | ---: |
| fetching | 0.999987 | 1.000000 |
| approximation | 0.999952 | n/a in runtime csv |
| chunked | 0.999777 | 1.000000 |

### last_data_to_result

This does not explain the trend.

- approximation `latency_from_last_data_ms` stays at `0-1 ms`
- chunked semantic-ready delay stays near zero via `ready_to_emit_ms`
- fetching `delay_past_last_obs_ms` in the original runtime csv was also tiny

Approximation correlation:

- `corr(last_data_to_result, latency) = -0.225`

That is negligible compared with the near-perfect latency trend.

### MQTT Input Timing

Yes, indirectly.

The trend is consistent with replay drift in the input publication schedule rather than downstream compute delay:

- raw-input spans are nearly identical across approaches
- latency rises with window number even though last-data-to-result stays minimal
- approximation and chunked remain faster than fetching by a mostly stable offset

### Process-Tree CPU/RSS

There is no evidence that rising CPU or RSS causes the latency climb.

Correlations over `windows 3..13`:

| Approach | corr(mean RSS, latency) | corr(mean CPU, latency) |
| --- | ---: | ---: |
| fetching | -0.243 | -0.941 |
| approximation | 0.056 | -0.716 |
| chunked | -0.714 | -0.887 |

These are not causal evidence of compute pressure. If anything, CPU usage trends slightly downward while latency rises, which points away from compute bottlenecks and toward replay scheduling drift.

## Interpretation

The steady-state latency series is best described as:

- monotone increasing
- approximately linear
- replay-drift dominated
- not cadence-sawtooth dominated
- not computation dominated

Approximation and chunked are both consistently faster than fetching on the same windows, but all three inherit the same upward replay-time drift shape.
