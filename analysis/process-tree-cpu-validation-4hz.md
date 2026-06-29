# Process-Tree CPU Validation for 4Hz Steady-State Run

Validated rerun root: `/Users/kushbisen/Code/streaming-query-hive/logs/one-pattern-latency-fixed-15w-steady-4hz-patched`

## Verdict

- The old `40.56 / 44.19 / 33.83` CPU-second table mixed a legacy tree-CPU methodology with approximation-specific avoidable overhead.
- With a monotone process-tree sampler and an active-interval cut (`first raw_input_stream publish` to `last superquery_result`), approximation is not higher than fetching in the rerun.
- Replay, publisher, and broker are not included in the approach CPU totals below. The sampled tree is the approach root PID plus descendants only.

## Active-Interval Metric

- Start: first `raw_input_stream` MQTT publish in the run log.
- End: last emitted `superquery_result` for window `15`.
- Excluded: teardown tail after the final result and any shutdown-only samples.

## Patched 4Hz Comparison

| Approach | Active CPU-seconds | CPU-s/window | CPU/final result | CPU/raw message | Mean RSS | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fetching | 20.92 | 1.39 | 1.39 | 0.002723 | 219.15 MiB | 305.25 MiB |
| approximation | 20.52 | 1.37 | 1.37 | 0.002671 | 210.77 MiB | 297.72 MiB |
| chunked | 16.29 | 1.09 | 1.09 | 0.002120 | 194.22 MiB | 297.50 MiB |

## Process Coverage

| Approach | Root PID | Observed live PIDs | Coverage |
| --- | ---: | --- | --- |
| fetching | `21892` | `21892` | root only |
| approximation | `95017` | `95017`, `95120` | root plus one child |
| chunked | `46736` | `46736`, `46807` | root plus one child |

## Per-PID Attribution

### Fetching

| PID | PPID | Command | CPU-s | Mean RSS | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: |
| `21892` | `21854` | `node` | 21.34 | 219.15 MiB | 305.25 MiB |

### Approximation

| PID | PPID | Command | CPU-s | Mean RSS | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: |
| `95017` | `95002` | `node` | 20.89 | 161.30 MiB | 228.08 MiB |
| `95120` | `95017` | `/opt/homebrew/Cellar/node/25.9.0_2/bin/node` | 0.56 | 49.50 MiB | 131.83 MiB |

### Chunked

| PID | PPID | Command | CPU-s | Mean RSS | Peak RSS |
| --- | ---: | --- | ---: | ---: | ---: |
| `46807` | `46736` | `/opt/homebrew/Cellar/node/25.9.0_2/bin/node` | 15.13 | 144.76 MiB | 219.39 MiB |
| `46736` | `46698` | `node` | 2.04 | 49.58 MiB | 158.11 MiB |

## Implications

- Approximation does not have extra replay, publisher, or broker CPU accidentally counted.
- Approximation and chunked both have the same tree boundary shape in this rerun: root plus one child.
- The patched rerun removes the earlier inversion: approximation `20.52s` is slightly below fetching `20.92s`.

## Notes

- The approximation rerun still logs `branch=structured` only and `legacy_messages_seen=0`.
- Latency-domain fields in these reruns still show `domain_mismatch`; that is a separate benchmark-latency issue, not part of process-tree CPU accounting.
