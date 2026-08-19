# Replay Dataset Duration Audit

## Conclusion

All current datasets under `src/streamer/data/approximation_test/challenging` are about `999.9s` long per stream at `10 Hz` with `10,000` observations each. They are sufficient for the 3-window validation and just sufficient for a 15-window run, but they do not support a 35-window long run.

## Window Support Thresholds

- `OUTPUT_WINDOW_RANGE=120000`
- `OUTPUT_WINDOW_STEP=60000`
- 3-window validation requires at least `240s` of event-time span, which is satisfied.
- 15-window steady-state requires at least `960s` of event-time span, preferably `1080s` with buffer.
- 35-window long run requires at least `2160s` of event-time span, preferably `2400s` with buffer.

## Per-Stream Findings

| Pattern | Stream | Observations | First timestamp (ms) | Last timestamp (ms) | Span (s) | Inferred frequency (Hz) | Max complete windows | 3-window | 15-window | 35-window |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| chaotic_oscillation | wearable.acceleration.x | 10000 | 1756122905719 | 1756123905619 | 999.9 | 10.000 | 15 | yes | yes | no |
| chaotic_oscillation | smartphone.acceleration.x | 10000 | 1756122905719 | 1756123905619 | 999.9 | 10.000 | 15 | yes | yes | no |
| exponential_decay | wearable.acceleration.x | 10000 | 1756122905337 | 1756123905237 | 999.9 | 10.000 | 15 | yes | yes | no |
| exponential_decay | smartphone.acceleration.x | 10000 | 1756122905337 | 1756123905237 | 999.9 | 10.000 | 15 | yes | yes | no |
| exponential_growth | wearable.acceleration.x | 10000 | 1756122905256 | 1756123905156 | 999.9 | 10.000 | 15 | yes | yes | no |
| exponential_growth | smartphone.acceleration.x | 10000 | 1756122905256 | 1756123905156 | 999.9 | 10.000 | 15 | yes | yes | no |
| extreme_exponential_decay | wearable.acceleration.x | 10000 | 1756122905462 | 1756123905362 | 999.9 | 10.000 | 15 | yes | yes | no |
| extreme_exponential_decay | smartphone.acceleration.x | 10000 | 1756122905462 | 1756123905362 | 999.9 | 10.000 | 15 | yes | yes | no |
| extreme_exponential_growth | wearable.acceleration.x | 10000 | 1756122905402 | 1756123905302 | 999.9 | 10.000 | 15 | yes | yes | no |
| extreme_exponential_growth | smartphone.acceleration.x | 10000 | 1756122905402 | 1756123905302 | 999.9 | 10.000 | 15 | yes | yes | no |
| high_frequency_oscillation | wearable.acceleration.x | 10000 | 1756122905655 | 1756123905555 | 999.9 | 10.000 | 15 | yes | yes | no |
| high_frequency_oscillation | smartphone.acceleration.x | 10000 | 1756122905655 | 1756123905555 | 999.9 | 10.000 | 15 | yes | yes | no |
| high_variance_random | wearable.acceleration.x | 10000 | 1756122905948 | 1756123905848 | 999.9 | 10.000 | 15 | yes | yes | no |
| high_variance_random | smartphone.acceleration.x | 10000 | 1756122905948 | 1756123905848 | 999.9 | 10.000 | 15 | yes | yes | no |
| logarithmic | wearable.acceleration.x | 10000 | 1756122905531 | 1756123905431 | 999.9 | 10.000 | 15 | yes | yes | no |
| logarithmic | smartphone.acceleration.x | 10000 | 1756122905531 | 1756123905431 | 999.9 | 10.000 | 15 | yes | yes | no |
| sine_wave | wearable.acceleration.x | 10000 | 1756122905593 | 1756123905493 | 999.9 | 10.000 | 15 | yes | yes | no |
| sine_wave | smartphone.acceleration.x | 10000 | 1756122905593 | 1756123905493 | 999.9 | 10.000 | 15 | yes | yes | no |
| spike_pattern | wearable.acceleration.x | 10000 | 1756122905872 | 1756123905772 | 999.9 | 10.000 | 15 | yes | yes | no |
| spike_pattern | smartphone.acceleration.x | 10000 | 1756122905872 | 1756123905772 | 999.9 | 10.000 | 15 | yes | yes | no |
| step_function | wearable.acceleration.x | 10000 | 1756122905784 | 1756123905684 | 999.9 | 10.000 | 15 | yes | yes | no |
| step_function | smartphone.acceleration.x | 10000 | 1756122905784 | 1756123905684 | 999.9 | 10.000 | 15 | yes | yes | no |

## Pattern-Level Support

| Pattern | Max complete windows | 3-window | 15-window | 35-window |
| --- | ---: | --- | --- | --- |
| chaotic_oscillation | 15 | yes | yes | no |
| exponential_decay | 15 | yes | yes | no |
| exponential_growth | 15 | yes | yes | no |
| extreme_exponential_decay | 15 | yes | yes | no |
| extreme_exponential_growth | 15 | yes | yes | no |
| high_frequency_oscillation | 15 | yes | yes | no |
| high_variance_random | 15 | yes | yes | no |
| logarithmic | 15 | yes | yes | no |
| sine_wave | 15 | yes | yes | no |
| spike_pattern | 15 | yes | yes | no |
| step_function | 15 | yes | yes | no |

## Smallest Safe Fix

The safest fix is to generate longer synthetic data in a new path instead of looping timestamps. Recommended target:

- `src/streamer/data/approximation_test/challenging_long/<pattern>/wearable.acceleration.x/data.nt`
- `src/streamer/data/approximation_test/challenging_long/<pattern>/smartphone.acceleration.x/data.nt`

Recommended duration:

- at least `2400s` of event-time span
- `10 Hz` frequency
- monotonically increasing timestamps per stream
- preserved RDF structure, sensor properties, and stream names

If only the benchmarked patterns are needed, generate the long replay for `exponential_growth` and `sine_wave` first.

## Validation Notes

- Timestamps in the current challenging datasets are strictly increasing per stream.
- Inferred frequency is consistently `10.000 Hz`.
- The current datasets do not need timestamp looping; they just need a longer replay horizon for 35-window steady-state coverage.
