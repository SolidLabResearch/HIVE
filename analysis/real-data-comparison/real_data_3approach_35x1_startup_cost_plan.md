# Real-Data 3-Approach 35x1 Startup-Cost Plan

Date:
`2026-06-29`

Mode:
startup-cost

Decision:
- approaches: `fetching`, `approximation`, `chunked`
- excluded: `naive_distributed`
- iterations per approach: `35`
- target windows per iteration: `1`
- startup metric is first-window aggregate startup cost across independent iterations

## Local Validation Performed

- Focused Jest suites only:
  - `experiments/real-data-comparison/run-real-data-4-approaches.test.js`
  - `scripts/benchmark/run-all-paper-benchmarks.test.js`
  - `analysis/real-data-comparison/generate-real-data-3approach-metrics.test.js`
- Top-level wrapper dry-run only:
  - `node scripts/benchmark/run-all-paper-benchmarks.js --suite real-data --iterations 35 --approaches fetching,approximation,chunked --target-windows 1 --dry-run`
- No local live benchmark smoke was run in this turn.

Verified dry-run command line:

```bash
node experiments/real-data-comparison/run-real-data-4-approaches.js \
  --iterations 35 \
  --approaches fetching,approximation,chunked \
  --target-windows 1
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
  --iterations 35 \
  --approaches fetching,approximation,chunked \
  --target-windows 1 \
  --output-dir results/paper-benchmarks/real-data-3approach-startup-35x1-2026-06-29
```

## Post-Run Analysis Command

```bash
node analysis/real-data-comparison/generate-real-data-3approach-metrics.js \
  --mode startup-cost \
  --input-root results/paper-benchmarks/real-data-3approach-startup-35x1-2026-06-29 \
  --output analysis/real-data-comparison/real_data_3approach_35x1_startup_cost_report.md
```

## Required Validation After the Server Run

- Exactly these approach directories are present in the real-data snapshot:
  - `fetching`
  - `approximation`
  - `chunked`
- `naive_distributed` does not appear in the real-data snapshot.
- Each selected approach has exactly `35` iteration directories.
- Each selected approach reaches window `1` only in every iteration.
- Startup latency summary must use first-window results across independent iterations.
- Accuracy should align first-window results against `fetching` by iteration when available.
- CPU metric must be process-tree CPU-seconds with mean and standard deviation across the `35` independent iterations.
- RSS metrics must report mean RSS MiB mean plus standard deviation, and peak RSS MiB mean plus standard deviation.

## Methodology Notes

- This run is only for startup cost.
- The helper treats `registrationToResultMs` as the aggregate startup cost.
- No separate containment-check timing field is currently extracted, so any containment/reuse planning cost remains part of the aggregate first-window startup metric.
- Do not merge this experiment’s startup latency mean with the steady-state 35-window experiment.
