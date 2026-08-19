# Real-Data 3-Approach 35-Window Steady-State Plan

Date:
`2026-06-29`

Mode:
steady-state latency/resource/accuracy

Decision:
- approaches: `fetching`, `approximation`, `chunked`
- excluded: `naive_distributed`
- iterations per approach: `1`
- target windows per iteration: `35`
- main steady-state window range: `4..33`
- dropped warm-up windows: `1,2,3`
- dropped shutdown windows: `34,35`

## Local Validation Performed

- Focused Jest suites only:
  - `experiments/real-data-comparison/run-real-data-4-approaches.test.js`
  - `scripts/benchmark/run-all-paper-benchmarks.test.js`
  - `analysis/real-data-comparison/generate-real-data-3approach-metrics.test.js`
- Top-level wrapper dry-run only:
  - `node scripts/benchmark/run-all-paper-benchmarks.js --suite real-data --iterations 1 --approaches fetching,approximation,chunked --target-windows 35 --dry-run`
- No local live benchmark smoke was run in this turn.

Verified dry-run command line:

```bash
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 1 \
  --approaches fetching,approximation,chunked \
  --target-windows 35
```

## Server Command

Use the top-level wrapper on the server so the run has a concrete archived output directory:

```bash
STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1 \
STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0 \
STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0 \
STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1 \
STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1 \
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite real-data \
  --iterations 1 \
  --approaches fetching,approximation,chunked \
  --target-windows 35 \
  --output-dir results/paper-benchmarks/real-data-3approach-steady-35w-2026-06-29
```

## Post-Run Analysis Command

```bash
node analysis/real-data-comparison/generate-real-data-3approach-metrics.js \
  --mode steady-state \
  --input-root results/paper-benchmarks/real-data-3approach-steady-35w-2026-06-29 \
  --output analysis/real-data-comparison/real_data_3approach_35window_steady_state_report.md
```

## Required Validation After the Server Run

- Exactly these approach directories are present in the real-data snapshot:
  - `fetching`
  - `approximation`
  - `chunked`
- `naive_distributed` does not appear in the real-data snapshot.
- Each selected approach has exactly one iteration directory.
- Each selected approach reaches windows `1..35`.
- Main latency table must use only windows `4..33`.
- Warm-up latency for windows `1..3` must be reported separately.
- Shutdown latency for windows `34..35` must be reported separately.
- Accuracy must align by window number against `fetching`.
- Chunked exactness against `fetching` must be stated explicitly.
- CPU metric must be process-tree CPU-seconds.
- RSS metrics must be mean RSS MiB and peak RSS MiB.

## Methodology Notes

- This run is only for steady-state latency/resource/accuracy.
- Do not mix its latency mean with the startup-cost experiment.
- The single-run resource values are not across-run variability statistics.
