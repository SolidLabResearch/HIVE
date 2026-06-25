# Custom-Pattern Full Run Prerequisite Check

## Scope

Cleanliness and configuration audit of the custom-pattern benchmark family prior to initiating the full 35-window paper evaluation.

## 1. Git Diff Summary

Modified files in repository:

- .gitignore: Commented out multi-line shell command on lines 129-134 to prevent ripgrep parsing failures.
- analysis/accuracy/accuracy-comparison-custom-patterns.js: Integrated dual raw and trimmed analysis (using windows 4..33) and compatibility outputs.
- experiments/pattern-analysis/run-custom-patterns-comparison.js: Integrated finite replay defaults, auto-derived replay duration calculation, and automated analysis trigger upon benchmark completion.
- src/streamer/data/custom_patterns/: Regenerated low_variability dataset files and metadata due to random number generation seed differences on run-through.

Other modified files (unrelated to custom-pattern pipeline):
- various real-data, window parameter sensitivity, and chunk aggregators.

## 2. Unrelated Edits and Dataset Confirmation

The changes under src/streamer/data/custom_patterns/ are due to running the script scripts/generate-custom-patterns.js. Since the script uses Math.random() for noise, the low_variability values were updated.

Verdict on committing generated datasets:
- Generated data files should remain uncommitted to prevent git history noise.
- They are safe to revert, as the benchmark runner will operate correctly on any generated dataset with the matching pattern directory.

## 3. Final Benchmark Command for Full Paper Run

To execute the full paper evaluation:

- all five patterns (low_variability, spike_pattern, step_pattern, low_freq_oscillation, high_freq_oscillation)
- fetching, approximation, and chunked approaches
- 4 Hz replay frequency
- 35 target windows (implies 36 minutes of event time per run)
- 1 iteration

Command:

```bash
CUSTOM_PATTERN_ITERATIONS=1 \
CUSTOM_PATTERN_SELECTED_APPROACHES=fetching,approximation,chunked \
CUSTOM_PATTERN_SELECTED_PATTERNS=low_variability,spike_pattern,step_pattern,low_freq_oscillation,high_freq_oscillation \
WEARABLE_FREQUENCY=4 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=35 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js \
  --iterations 1 \
  --pattern-test-timeout 2400000
```

Note: Per-test timeout is increased to 2400000 ms (40 minutes) to accommodate the 36-minute (2160 seconds) execution time of a single approach run.

## 4. Expected Runtime and Output Paths

Replay duration calculation:
- duration = range + (target_windows - 1) * step
- duration = 120s + (35 - 1) * 60s = 2160s (36 minutes) per test.

Total runtime:
- 15 cases (5 patterns x 3 approaches x 1 iteration)
- 15 x 36 minutes = 540 minutes (9.0 hours).

Output paths:
- Top-level accuracy summaries: logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/
- Raw summaries: summary.raw.json, summary.raw.csv
- Trimmed summaries (analyzing windows 4..33): summary.trimmed-4-33.json, summary.trimmed-4-33.csv
- Per-run iteration directories: logs/custom-pattern-comparison/<approach>/<pattern>/iteration1/

## 5. Readiness Verdict

Safe to run full custom-pattern benchmark: yes (after reverting dataset file changes to keep clean git history).

Files safe to commit:
- [.gitignore](file:///Users/kushbisen/Code/streaming-query-hive/.gitignore)
- [accuracy-comparison-custom-patterns.js](file:///Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js)
- [run-custom-patterns-comparison.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js)
- [custom-pattern-all-patterns-paper-smoke-validation.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/custom-pattern-all-patterns-paper-smoke-validation.md)
- [custom-pattern-full-run-prereq-check.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/custom-pattern-full-run-prereq-check.md)

Files to revert or keep uncommitted:
- src/streamer/data/custom_patterns/ (generated datasets)
- Any other unrelated benchmark suite files modified locally.
