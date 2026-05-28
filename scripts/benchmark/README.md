# Paper Benchmark Runner

Use the top-level runner to execute the paper evaluation from one command while reusing the existing benchmark drivers already present in the repository.

## Run All Benchmarks

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --suite all
```

This orchestrates the two existing core benchmark jobs used by the paper workflow:

- `experiments/real-data-comparison/run-real-data-4-approaches.js`
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`

The additional suite names (`latency`, `resources`, `accuracy`, `naive-distributed`) are exposed as paper-oriented views over those same shared benchmark runs rather than as separate reimplementations.

## Run A Single Suite

Real-world DAHCC data:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --suite real-data
```

Synthetic stream patterns:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --suite patterns
```

The patterns suite runs the existing custom-pattern benchmark driver and, by default, follows it with the dedicated accuracy aggregation step that produces the paper-ready per-pattern summaries.

For a smoke run that validates the full custom-pattern pipeline quickly, use:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --suite patterns --smoke --iterations 1 --output-dir results/paper-benchmarks/smoke-patterns
```

Latency-only paper view:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --suite latency
```

## Dry Run

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --dry-run --suite all
```

Dry-run mode prints the commands that would be executed and still performs lightweight preflight validation. It does not launch any benchmark processes.

To skip the post-benchmark accuracy aggregation step:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js --suite patterns --skip-analysis
```

## Default Configuration

The runner defaults to the paper settings described in the request:

- MQTT broker: `mqtt://localhost:1883`
- Sources: `smartphone.accel.x` and `wearable.accel.x`
- Output window: `120000 ms`
- Output slide: `60000 ms`
- Sub-window range: `60000 ms`
- Sub-window step: `30000 ms`
- Replay frequency: `4 Hz`
- Aggregation: `AVG`
- Timeout metadata: `300000 ms`
- Custom-pattern per-test timeout: `240000 ms` by default (`CUSTOM_PATTERN_TEST_TIMEOUT_MS` / `--pattern-test-timeout`)
- Iterations: `35`
- Analysis trimming metadata: drop first `3`, drop last `2`

You can override these through CLI flags:

```bash
node scripts/benchmark/run-all-paper-benchmarks.js \
  --suite all \
  --iterations 35 \
  --drop-warmup 3 \
  --drop-cooldown 2 \
  --broker mqtt://localhost:1883 \
  --window-width 120000 \
  --window-slide 60000 \
  --sub-window-range 60000 \
  --sub-window-step 30000 \
  --frequency 4 \
  --aggregation AVG \
  --timeout 300000 \
  --pattern-test-timeout 240000 \
  --output-dir results/paper-benchmarks/<timestamp>
```

## Output Structure

The default output root is:

```text
results/paper-benchmarks/<timestamp>/
```

The runner writes:

- `metadata.json`: environment, CLI config, git hash, preflight checks, failures
- `summary.json`: suite/job status and runtime summary
- `real-data/raw/`: snapshot of the existing real-data benchmark logs
- `patterns/raw/`: snapshot of the existing custom-pattern benchmark logs
- `patterns/low-variability/`
- `patterns/step/`
- `patterns/spike/`
- `patterns/low-frequency-oscillation/`
- `patterns/high-frequency-oscillation/`
- `latency/`: latency log extracts copied from the real-data benchmark outputs
- `resources/`: resource log extracts copied from the real-data benchmark outputs
- `accuracy/`: copied result and metadata files from the pattern benchmark outputs
- `accuracy/patterns/custom-pattern-accuracy/summary.json`: per-pattern accuracy summary against the fetching baseline
- `accuracy/patterns/custom-pattern-accuracy/summary.csv`: paper-friendly CSV export of the same custom-pattern accuracy summary
- `naive-distributed/`: copied naive baseline artifacts from real-data and pattern runs
- `logs/`: stdout, stderr, and combined command logs for each launched benchmark command

## Paper Claim Support Matrix

All paths below are relative to `results/paper-benchmarks/<timestamp>/` unless noted otherwise.

| Paper claim | Current repo support | Script(s) | Output path(s) | Notes / limitations |
| --- | --- | --- | --- | --- |
| Real-world DAHCC accelerometer evaluation | Supported | `scripts/benchmark/run-all-paper-benchmarks.js --suite real-data`<br>`experiments/real-data-comparison/run-real-data-4-approaches.js` | `real-data/raw/`<br>`real-data/raw/real_data_comparison_results.csv`<br>`real-data/raw/real_data_comparison_results.json` | Uses `src/streamer/data/smartphone.acceleration.x/data.nt` and `src/streamer/data/wearable.acceleration.x/data.nt`. The unified runner snapshots the underlying experiment logs rather than re-computing metrics itself. |
| Local-only / client-side baseline | Supported | `scripts/benchmark/run-all-paper-benchmarks.js --suite real-data`<br>`scripts/benchmark/run-all-paper-benchmarks.js --suite patterns`<br>`experiments/real-data-comparison/run-real-data-4-approaches.js`<br>`experiments/pattern-analysis/run-custom-patterns-comparison.js` | `real-data/raw/fetching/iteration*/`<br>`latency/real-data/fetching/iteration*/fetching_latency_log.csv`<br>`resources/real-data/fetching/iteration*/fetching_client_side_resource_usage.csv`<br>`patterns/*/fetching/iteration*/`<br>`accuracy/patterns/*/fetching/iteration*/fetching_results.csv` | The baseline is the `fetching` approach. It is present in both real-data and synthetic-pattern runs, but not exposed as a standalone suite. |
| Naive distributed baseline | Supported | `scripts/benchmark/run-all-paper-benchmarks.js --suite naive-distributed`<br>`experiments/real-data-comparison/run-real-data-4-approaches.js`<br>`experiments/pattern-analysis/run-custom-patterns-comparison.js` | `naive-distributed/real-data/naive_distributed/iteration*/`<br>`naive-distributed/patterns/*/naive_distributed/iteration*/`<br>`real-data/raw/naive_distributed/iteration*/`<br>`patterns/raw/naive_distributed/*/iteration*/` | The suite is a paper-oriented snapshot view over the shared real-data and pattern jobs. It does not run a separate benchmark implementation. |
| Latency comparison | Supported | `scripts/benchmark/run-all-paper-benchmarks.js --suite latency`<br>`experiments/real-data-comparison/run-real-data-4-approaches.js` | `latency/real-data/*/iteration*/*_latency_log.csv`<br>`real-data/raw/real_data_comparison_results.csv`<br>`real-data/raw/real_data_comparison_results.json` | The `latency` suite is only a filtered view over the real-data job outputs. There is no dedicated latency-only experiment script in the unified runner path. |
| CPU / memory resource comparison | Partially supported | `scripts/benchmark/run-all-paper-benchmarks.js --suite resources`<br>`experiments/real-data-comparison/run-real-data-4-approaches.js` | `resources/real-data/fetching/iteration*/fetching_client_side_resource_usage.csv`<br>`resources/real-data/approximation/iteration*/approximation_approach_resource_usage.csv`<br>`resources/real-data/chunked/iteration*/streaming_query_hive_resource_log.csv`<br>`resources/real-data/naive_distributed/iteration*/naive_distributed_approach_resource_usage.csv` | Raw per-iteration resource logs are preserved, but the unified runner does not produce a paper-ready aggregated CPU / memory summary table. |
| Accuracy against client-side ground truth | Partially supported | `experiments/real-data-comparison/run-real-data-4-approaches.js`<br>`scripts/benchmark/run-all-paper-benchmarks.js --suite real-data` | `real-data/raw/real_data_comparison_results.csv`<br>`real-data/raw/real_data_comparison_results.json` | Real-data accuracy is computed against `fetching` inside the underlying experiment and is preserved in `real-data/raw/`. The unified runner does not expose a dedicated real-data `accuracy/` output, and the underlying script compares against the first collected fetching iteration rather than an explicitly trimmed aggregate. |
| Synthetic stream-pattern accuracy comparison | Supported | `scripts/benchmark/run-all-paper-benchmarks.js --suite patterns`<br>`analysis/accuracy/accuracy-comparison-custom-patterns.js`<br>`experiments/pattern-analysis/run-custom-patterns-comparison.js` | `accuracy/patterns/custom-pattern-accuracy/summary.json`<br>`accuracy/patterns/custom-pattern-accuracy/summary.csv`<br>`accuracy/patterns/*/<approach>/iteration*/` | The aggregation compares extracted per-window result CSVs against the `fetching` baseline and reports MAE, RMSE, MAPE, matched-window counts, and missing/unmatched counts for the five paper patterns. |
| 35 iterations with first 3 and last 2 dropped in analysis | Partially supported | `scripts/benchmark/run-all-paper-benchmarks.js` | `metadata.json`<br>`summary.json` | Defaults are `--iterations 35 --drop-warmup 3 --drop-cooldown 2`, and those values are recorded in metadata. The runner and current aggregation scripts do not apply the trimming themselves; the full iteration count is still executed and preserved. |
| ASF represented through sub-window range / step | Partially supported | `scripts/benchmark/run-all-paper-benchmarks.js`<br>`experiments/real-data-comparison/run-real-data-4-approaches.js`<br>`experiments/pattern-analysis/run-custom-patterns-comparison.js` | `metadata.json` | The runner passes `SUB_WINDOW_RANGE` and `SUB_WINDOW_STEP` into the benchmark environment and records them in metadata. This is only a proxy for ASF; there is no dedicated ASF sweep or ASF-specific analysis output in the repo. |
| Limitations around naive-distributed custom-pattern accuracy outputs | Supported | `analysis/accuracy/accuracy-comparison-custom-patterns.js`<br>`scripts/benchmark/run-all-paper-benchmarks.js --suite patterns`<br>`experiments/pattern-analysis/run-custom-patterns-comparison.js` | `accuracy/patterns/custom-pattern-accuracy/summary.json`<br>`naive-distributed/patterns/*/naive_distributed/iteration*/`<br>`patterns/raw/naive_distributed/*/iteration*/` | `naive_distributed` is treated as an optional approach by the aggregation script. It is included in `summary.json` under `optionalApproaches` when present, but it is not emitted in `summary.csv` and is not counted as a required comparison for pattern-level completeness. |

## Limitations

- The existing repository hardcodes `mqtt://localhost:1883` in the orchestrators and publisher. The runner accepts `--broker` and records it in metadata, but non-default brokers are not fully supported without broader refactoring.
- Warm-up and cool-down dropping are recorded in `metadata.json` for downstream analysis. The existing benchmark scripts still execute the full iteration count.
- ASF is not a first-class benchmark axis in the current repo. Use `--sub-window-range` and `--sub-window-step` as the approximation-granularity proxy unless a dedicated ASF sweep is implemented later.
- `experiments/pattern-analysis/run-custom-patterns-comparison.js` explicitly notes that a dedicated aggregated custom-pattern analysis script is still needed. The unified runner preserves and snapshots the raw pattern outputs instead of replacing that missing analysis layer.
- The unified runner now invokes `analysis/accuracy/accuracy-comparison-custom-patterns.js` after the patterns benchmark unless `--skip-analysis` is set.
