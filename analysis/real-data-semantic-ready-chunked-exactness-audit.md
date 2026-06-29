# Real-Data Semantic-Ready Chunked Exactness Audit

## Scope

This audit and fix was limited to the real-data semantic-ready chunked path.

Not changed:

- fetching behavior
- approximation behavior
- naive-distributed behavior
- benchmark methodology
- target windows `1..35`
- trimmed analysis windows `4..33`

Kept enabled:

- `STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1`
- `STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0`
- semantic-ready immediate emission

## Root Cause

The semantic-ready branch in [`src/services/operators/StreamingQueryChunkAggregatorOperator.ts`](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/StreamingQueryChunkAggregatorOperator.ts) bypassed external-window recomposition.

What happened before:

- each complete internal `30s` chunk group was emitted immediately as if it were a final output window
- `buildWindowMetadata()` then used that internal chunk window as the finalized output-window metadata
- `required_chunk_intervals` therefore recorded only one `30s` interval
- latency looked good because emission happened right after one internal chunk completed
- exactness regressed because the final query is `RANGE=120000 STEP=60000`, so each final output window must cover four contiguous `30s` chunk groups

What the operator needed to do instead:

- buffer complete internal chunk groups
- recompose the external `120s` output window from four contiguous internal groups
- emit immediately after the fourth required group makes the external window semantically complete

## Files Changed

1. [`src/services/operators/StreamingQueryChunkAggregatorOperator.ts`](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/StreamingQueryChunkAggregatorOperator.ts)

- semantic-ready mode now buffers complete chunk groups and uses the same external-window recomposition path as cadence mode
- immediate emission now waits for full external coverage
- finalized metadata now uses:
  - external `windowStart`
  - external `windowEnd`
  - external `windowDataCloseTime`
  - external `logicalTriggerTime`
- finalized chunked rows now record the full `required_chunk_intervals` list

2. [`src/services/operators/StreamingQueryChunkAggregatorOperator.test.ts`](/Users/kushbisen/Code/streaming-query-hive/src/services/operators/StreamingQueryChunkAggregatorOperator.test.ts)

- updated the exactness model to require full `120s` external-window coverage from window `1`
- increased synthetic chunk coverage length for `35` output windows under `RANGE=120000 STEP=60000`

## Validation Commands

```bash
npm test -- --runInBand StreamingQueryChunkAggregatorOperator.test.ts
npm run build
node -e 'const { RealDataComparisonRunner, APPROACHES } = require("./experiments/real-data-comparison/run-real-data-4-approaches.js"); const runner = new RealDataComparisonRunner({ iterations: 1 }); const approach = APPROACHES.find((entry) => entry.name === "chunked"); runner.runSingleTest(approach, 1).then((result) => { console.log(JSON.stringify(result, null, 2)); process.exit(result.success ? 0 : 1); }).catch((error) => { console.error(error); process.exit(1); });'
node experiments/real-data-comparison/run-real-data-4-approaches.js analyze-only --iterations 1
```

Comparison baseline used for exactness:

- restored fetching artifact:
  - `experiments/real-data-comparison/logs/fetching/iteration1.pre-semantic-align-20260625-015045/`

Broken semantic-ready reference artifact:

- `analysis/_artifact_backups/chunked-iteration1-before-semantic-ready-exactness-fix/`

## Artifact Paths

Current fixed chunked artifacts:

- `experiments/real-data-comparison/logs/chunked/iteration1/chunked_latency_log.csv`
- `experiments/real-data-comparison/logs/chunked/iteration1/chunked_emission_proof.json`
- `experiments/real-data-comparison/logs/chunked/iteration1/chunked_window_diagnostics.csv`
- `experiments/real-data-comparison/logs/chunked/iteration1/benchmark_window_cap_summary.json`

Regenerated paper-ready summaries:

- `experiments/real-data-comparison/logs/real_data_paper_ready_raw_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.json`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.csv`

## Before / After

### Broken Semantic-Ready Chunked

Source:

- `analysis/_artifact_backups/chunked-iteration1-before-semantic-ready-exactness-fix/chunked_latency_log.csv`

Observed:

- trimmed close-to-result latency mean: `3862.6666666666665 ms`
- trimmed `ready_to_emit_ms` mean: `0.9 ms`
- trimmed MAE vs fetching: `0.028424092598613036`
- trimmed MAPE vs fetching: `0.22879271044260058`
- trimmed RMSE vs fetching: `0.03726876617653983`
- required chunk intervals example: `1782341589957-1782341619957`

Interpretation:

- latency was low because emission happened after one internal chunk
- exactness failed because the finalized output row used partial external coverage

### Fixed Semantic-Ready Chunked

Sources:

- `experiments/real-data-comparison/logs/chunked/iteration1/chunked_latency_log.csv`
- `experiments/real-data-comparison/logs/real_data_paper_ready_trimmed-4-33_summary.json`

Observed:

- raw close-to-result latency mean: `3960.657142857143 ms`
- trimmed close-to-result latency mean: `3962.366666666667 ms`
- raw `ready_to_emit_ms` mean: `0.9714285714285714 ms`
- trimmed `ready_to_emit_ms` mean: `0.9 ms`
- trimmed MAE vs fetching: `2.6882200169590456e-14`
- trimmed MAPE vs fetching: `2.1635692132354555e-13`
- trimmed RMSE vs fetching: `2.6931975549283662e-14`
- required chunk intervals example: `1782375324304-1782375354304|1782375354304-1782375384304|1782375384304-1782375414304|1782375414304-1782375444304`

Interpretation:

- semantic-ready latency stayed low
- emission now waits for full semantic completeness of the external `120s` window
- exactness against fetching returned to floating-point noise only

## Acceptance Check

1. Chunked emits windows `1..35` exactly once

- pass
- `benchmark_window_cap_summary.json` reports `emittedFinalWindowCount=35`
- latency CSV has `35` data rows

2. No window `36`

- pass
- no finalized `36` in the latency CSV

3. Trimmed windows remain `4..33`

- pass
- confirmed in `real_data_paper_ready_trimmed-4-33_summary.json`

4. `ready_to_emit_ms` remains near zero after full semantic completeness

- pass
- raw mean `0.9714285714285714 ms`
- trimmed mean `0.9 ms`

5. Chunked close-to-result latency is plausible and far below `61s`

- pass
- trimmed mean `3962.366666666667 ms`

6. Chunked exactness vs fetching returns to effectively zero

- pass
- MAE `2.6882200169590456e-14`
- MAPE `2.1635692132354555e-13`
- RMSE `2.6931975549283662e-14`

7. `required_chunk_intervals` shows full `120s` coverage

- pass
- each finalized row now records four contiguous `30s` intervals

8. No mixed-domain latency fields were reintroduced

- pass
- chunked rows use `latency_domain_status=wall_clock_mapped`
- no mixed-domain fallback fields were needed

## Additional Evidence

Emission proof:

- `chunked_emission_proof.json` contains `35` finalized windows
- first and last proof entries each require four chunk IDs per subquery
- no missing chunks
- no duplicate chunks consumed for finalized windows

Window diagnostics:

- `chunked_window_diagnostics.csv` shows each external window composed from four contiguous internal chunk groups

## Remaining Caveats

1. This fix resolves the real-data semantic-ready chunked exactness regression only.
2. The fetching artifact used for final comparison had to be restored from:
   - `experiments/real-data-comparison/logs/fetching/iteration1.pre-semantic-align-20260625-015045/`
3. The broader benchmark families were intentionally not patched or rerun here.

## Final Decision

The real-data semantic-ready chunked exactness regression is fixed.

Current real-data paper-ready status:

- semantic-ready chunked stays low-latency
- semantic-ready chunked emits only after full external-window completeness
- semantic-ready chunked is exact against fetching for trimmed windows `4..33`
- real-data paper-ready summaries are regenerated and consistent with the final method
