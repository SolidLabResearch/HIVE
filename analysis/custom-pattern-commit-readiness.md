# Custom-Pattern Commit Readiness Audit

Status: Completed

## 1. Git Status Summary
The repository contains modifications in multiple files. The subset belonging to the custom-pattern benchmark pipeline alignment and exit-hardening includes:
- [run-custom-patterns-comparison.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/run-custom-patterns-comparison.js) (hardened orchestrator close handler and runtime env matching)
- [accuracy-comparison-custom-patterns.js](file:///Users/kushbisen/Code/streaming-query-hive/analysis/accuracy/accuracy-comparison-custom-patterns.js) (methodology alignment for accuracy reporting)
- [extract-pattern-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/pattern-analysis/extract-pattern-results.js) (result extraction logic)
- [2026-06-25-custom-pattern-orchestrator-exit-hardening.md](file:///Users/kushbisen/Code/streaming-query-hive/docs/decisions/2026-06-25-custom-pattern-orchestrator-exit-hardening.md) (decision record)
- [2026-06-25-custom-pattern-paper-methodology-alignment.md](file:///Users/kushbisen/Code/streaming-query-hive/docs/decisions/2026-06-25-custom-pattern-paper-methodology-alignment.md) (previous decision record for alignment)
- [custom-pattern-orchestrator-exit-hardening-report.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/custom-pattern-orchestrator-exit-hardening-report.md) (verification report)
- [custom-pattern-final-go-no-go.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/custom-pattern-final-go-no-go.md) (go/no-go report)

## 2. Commit vs. Revert Classifications

### Files to Commit
The following files are essential to preserve the custom-pattern alignment and exit-hardening features:
- `experiments/pattern-analysis/run-custom-patterns-comparison.js`
- `analysis/accuracy/accuracy-comparison-custom-patterns.js`
- `experiments/pattern-analysis/extract-pattern-results.js`
- `docs/decisions/2026-06-25-custom-pattern-orchestrator-exit-hardening.md`
- `docs/decisions/2026-06-25-custom-pattern-paper-methodology-alignment.md`
- `analysis/custom-pattern-orchestrator-exit-hardening-report.md`
- `analysis/custom-pattern-final-go-no-go.md`
- `analysis/custom-pattern-commit-readiness.md`
- `src/streamer/data/custom_patterns/` (all generated dataset files, to guarantee deterministic run inputs)

### Files to Revert or Keep Uncommitted
The following files are outside the custom-pattern benchmark pipeline or are temporary/local runner files:
- `.gitignore` (keep uncommitted unless global repository updates are requested)
- `chunked_debug_summary.json` (local debug artifact)
- `run_summary.json` (local execution summary)
- `mqtt_traffic.ndjson` (local test traffic dump)
- `analysis/_artifact_backups/` (internal backups)
- Antigravity-local `task.md` and `walkthrough.md` (located in the app data directory, not inside the repository)
- Any unrelated scripts or source files belonging to K-scaling, scalability, frequency, or real-data families (e.g. `experiments/real-data-comparison/run-real-data-4-approaches.js`, sensitivity scripts, or core operator tests not related to custom-patterns)

## 3. Simulator Cleanup Verification
Verified that `experiments/pattern-analysis/simulate-orchestrator-crash.js` was deleted successfully. No temporary simulator files remain in the repository.

## 4. Suggested Commit Message
```
feat(pattern-analysis): harden custom-pattern runner and align methodology

- Update custom-pattern runner close event listener to check for valid orchestrator exits.
- Terminate publisher process tree immediately on invalid exit and fail the benchmark case.
- Check benchmark_window_cap_summary.json against target windows from run environment.
- Align accuracy calculations and commit static custom pattern datasets to ensure reproducibility.
```

## 5. Commit Safety Assessment
It is safe to commit the specified custom-pattern benchmark pipeline changes once unrelated files are unstaged/reverted.
