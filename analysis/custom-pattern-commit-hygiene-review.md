# Custom-Pattern Commit Hygiene Review

This document evaluates the repository hygiene of the current staged custom-pattern commit set, classifies each file, assesses information duplication, and defines the recommended minimal high-signal commit set.

## File Classifications

The staged files are categorized below:

| File Path | Classification | Recommendation |
| --- | --- | --- |
| `experiments/pattern-analysis/run-custom-patterns-comparison.js` | CORE IMPLEMENTATION | Keep Staged |
| `experiments/pattern-analysis/extract-pattern-results.js` | CORE IMPLEMENTATION | Keep Staged |
| `experiments/pattern-analysis/extract-pattern-results.test.js` | TEST | Keep Staged |
| `analysis/accuracy/accuracy-comparison-custom-patterns.js` | CORE IMPLEMENTATION | Keep Staged |
| `docs/decisions/2026-06-25-custom-pattern-orchestrator-exit-hardening.md` | DECISION RECORD | Keep Staged |
| `docs/decisions/2026-06-25-custom-pattern-paper-methodology-alignment.md` | DECISION RECORD | Keep Staged |
| `analysis/custom-pattern-all-patterns-paper-smoke-validation.md` | FINAL REPORT | Keep Staged |
| `analysis/custom-pattern-orchestrator-exit-hardening-report.md` | FINAL REPORT | Keep Staged |
| `analysis/custom-pattern-paper-method-alignment-report.md` | FINAL REPORT | Keep Staged |
| `analysis/custom-pattern-final-go-no-go.md` | FINAL REPORT | Keep Staged |
| `analysis/custom-pattern-bounded-finite-replay-guard-report.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-commit-readiness.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-final-commit-audit.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-full-run-prereq-check.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-production-readiness-audit.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-approximation-bounded-smoke-audit.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-staging-audit.md` | INTERMEDIATE AUDIT | Unstage |
| `analysis/custom-pattern-commit-hygiene-review.md` | INTERMEDIATE AUDIT | Unstage |

## Duplicate-Report Analysis

The intermediate audits duplicate verification outcomes and configuration discussions that are formalized elsewhere:
* `analysis/custom-pattern-production-readiness-audit.md` lists blockers that were resolved and verified in `analysis/custom-pattern-orchestrator-exit-hardening-report.md`.
* `analysis/custom-pattern-approximation-bounded-smoke-audit.md` and `analysis/custom-pattern-bounded-finite-replay-guard-report.md` explain the finite replay guard, which is formalized in the decision record `docs/decisions/2026-06-25-custom-pattern-paper-methodology-alignment.md` and verified in `analysis/custom-pattern-paper-method-alignment-report.md`.
* `analysis/custom-pattern-commit-readiness.md`, `analysis/custom-pattern-final-commit-audit.md`, `analysis/custom-pattern-staging-audit.md`, and `analysis/custom-pattern-commit-hygiene-review.md` are metadata checklists checking staging readiness and do not contain unique benchmark methodology parameters.

To ensure repository hygiene, these intermediate audits should be unstaged.

## Recommended Final Staged Set

The minimal high-signal set comprises the four code/test files, two decision records, and four final reports:

1. `experiments/pattern-analysis/run-custom-patterns-comparison.js`
2. `experiments/pattern-analysis/extract-pattern-results.js`
3. `experiments/pattern-analysis/extract-pattern-results.test.js`
4. `analysis/accuracy/accuracy-comparison-custom-patterns.js`
5. `docs/decisions/2026-06-25-custom-pattern-orchestrator-exit-hardening.md`
6. `docs/decisions/2026-06-25-custom-pattern-paper-methodology-alignment.md`
7. `analysis/custom-pattern-all-patterns-paper-smoke-validation.md`
8. `analysis/custom-pattern-orchestrator-exit-hardening-report.md`
9. `analysis/custom-pattern-paper-method-alignment-report.md`
10. `analysis/custom-pattern-final-go-no-go.md`
