# 4Hz vs 10Hz Replay Drift Comparison

## Result

Lowering replay pressure from the old 10Hz run to the patched 4Hz run removed the long-run latency drift almost entirely.

Over steady-state windows `3..13`:

| Approach | 10Hz slope ms/window | 4Hz slope ms/window | Change |
| --- | ---: | ---: | ---: |
| fetching | `+1609.08` | `-10.16` | `-1619.24` |
| approximation | `+1574.40` | `+0.43` | `-1573.97` |
| chunked | `+1590.81` | `+0.07` | `-1590.74` |

The old 10Hz drift was replay-scheduler dominated. After the scheduler fix, replay lag stays in the low single-digit milliseconds and the close-to-result trend is effectively stationary.

## Window-by-Window Close-To-Result

| Window | Fetching 4Hz ms | Approx 4Hz ms | Chunked 4Hz ms |
| --- | ---: | ---: | ---: |
| 1 | 386 | 259 | 234 |
| 2 | 386 | 275 | 224 |
| 3 | 385 | 258 | 230 |
| 4 | 386 | 259 | 222 |
| 5 | 850 | 254 | 227 |
| 6 | 387 | 254 | 229 |
| 7 | 446 | 259 | 225 |
| 8 | 420 | 278 | 231 |
| 9 | 429 | 330 | 236 |
| 10 | 382 | 260 | 220 |
| 11 | 385 | 253 | 233 |
| 12 | 397 | 257 | 230 |
| 13 | 437 | 253 | 223 |
| 14 | 410 | 269 | 231 |
| 15 | 448 | 257 | 223 |

## Interpretation

- `4Hz`: roughly stationary
- `10Hz`: increasing over time, almost perfectly linear
- `4Hz` replay drift accumulation: no meaningful slope remains
- `10Hz` replay drift accumulation: dominant effect

At 4Hz, the replay-side drift estimate stays small:

| Approach | Replay drift mean ± std ms over windows `3..13` |
| --- | ---: |
| fetching | `345.91 ± 151.94` |
| approximation | `264.82 ± 22.42` |
| chunked | `225.73 ± 4.76` |

That is qualitatively different from the old 10Hz run, where close-to-result latency climbed by about `1.57-1.61s` every output window.

## Per-Window Deltas At 4Hz

| Window | Approx - Fetching ms | Chunked - Fetching ms | Chunked - Approx ms |
| --- | ---: | ---: | ---: |
| 1 | -127 | -152 | -25 |
| 2 | -111 | -162 | -51 |
| 3 | -127 | -155 | -28 |
| 4 | -127 | -164 | -37 |
| 5 | -596 | -623 | -27 |
| 6 | -133 | -158 | -25 |
| 7 | -187 | -221 | -34 |
| 8 | -142 | -189 | -47 |
| 9 | -99 | -193 | -94 |
| 10 | -122 | -162 | -40 |
| 11 | -132 | -152 | -20 |
| 12 | -140 | -167 | -27 |
| 13 | -184 | -214 | -30 |
| 14 | -141 | -179 | -38 |
| 15 | -191 | -225 | -34 |

The remaining differences are small fixed offsets, not a growing trend.
