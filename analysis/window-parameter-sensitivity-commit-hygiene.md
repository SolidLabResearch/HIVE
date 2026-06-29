# Commit-Hygiene Audit: Window-Parameter Sensitivity

This document provides a strict commit-hygiene review of the window-parameter-sensitivity modifications before staging.

## 1. Git Status and Diff Summary

The current modifications to the experiments/window-parameter-sensitivity/ folder are summarized as follows:
- 7 modified files in working directory
- 2 untracked files related to this family (test and audit report)
- 0 cached (staged) files

## 2. File Classifications

Each file in the window-parameter-sensitivity family has been audited and classified:

### Core Implementation
- [experiments/window-parameter-sensitivity/common.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.js)
  Class: CORE IMPLEMENTATION
  Reasoning: Formulates scenario configurations, default options, and scaling parameters.
- [experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js)
  Class: CORE IMPLEMENTATION
  Reasoning: Orchestrates process execution, handles cleanup checks, and classifies bounded runs.

### Test
- [experiments/window-parameter-sensitivity/common.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.test.js)
  Class: TEST
  Reasoning: Exercises scenario generation logic.
- [experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js)
  Class: TEST
  Reasoning: Tests correctness metric and error parsing helpers.
- [experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js)
  Class: TEST
  Reasoning: Validates exit status classification and bounded success detection.

### Documentation
- [experiments/window-parameter-sensitivity/README.md](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/README.md)
  Class: DOCUMENTATION
  Reasoning: Explains experiments 2, 3, and 4, listing options and smoke test invocation commands.
- [analysis/window-parameter-sensitivity-paper-readiness.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/window-parameter-sensitivity-paper-readiness.md)
  Class: DOCUMENTATION
  Reasoning: Holds the official paper readiness audit.
- [analysis/window-parameter-sensitivity-commit-hygiene.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/window-parameter-sensitivity-commit-hygiene.md)
  Class: DOCUMENTATION
  Reasoning: Pre-stage commit hygiene report.

### Plot/Reporting
- [experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js)
  Class: PLOT/REPORTING
  Reasoning: Processes logs and generates the per-run, profile, and aggregate results CSVs.
- [experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js)
  Class: PLOT/REPORTING
  Reasoning: Script for graphing extracted CSV outputs.

## 3. Minimal Commit Set Recommendation

To maintain strict hygiene, the following files should be staged and committed together:
1. [experiments/window-parameter-sensitivity/common.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.js)
2. [experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.js)
3. [experiments/window-parameter-sensitivity/common.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/common.test.js)
4. [experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.test.js)
5. [experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/run-window-parameter-sensitivity.test.js)
6. [experiments/window-parameter-sensitivity/README.md](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/README.md)
7. [experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js)
8. [experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js](file:///Users/kushbisen/Code/streaming-query-hive/experiments/window-parameter-sensitivity/plot-window-parameter-sensitivity-results.js)
9. [analysis/window-parameter-sensitivity-paper-readiness.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/window-parameter-sensitivity-paper-readiness.md)
10. [analysis/window-parameter-sensitivity-commit-hygiene.md](file:///Users/kushbisen/Code/streaming-query-hive/analysis/window-parameter-sensitivity-commit-hygiene.md)

All other modified files (such as files under `src/` or unrelated experiment folders) must be left out of this commit to avoid mixing concerns.
