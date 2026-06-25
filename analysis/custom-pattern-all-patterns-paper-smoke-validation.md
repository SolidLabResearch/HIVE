# Custom-Pattern Paper-Method Validation Report

## Scope

Validated the custom-pattern benchmark family with:

- patterns: low_variability, spike_pattern, step_pattern, low_freq_oscillation, high_freq_oscillation
- approaches: fetching, approximation, chunked
- iteration: 1
- target windows: 5
- replay frequency: 4 Hz
- aggregation: AVG
- compact reusable payload: enabled (1)
- completed-window approximation mode: enabled (1)
- semantic-ready chunked mode: enabled (1)
- process-tree CPU-seconds: enabled (1)
- MQTT traffic measurement: enabled (1)

## Commands Run

To generate data and run validation:

```bash
node scripts/generate-custom-patterns.js

CUSTOM_PATTERN_ITERATIONS=1 \
CUSTOM_PATTERN_SELECTED_APPROACHES=fetching,approximation,chunked \
CUSTOM_PATTERN_SELECTED_PATTERNS=low_variability,spike_pattern,step_pattern,low_freq_oscillation,high_freq_oscillation \
WEARABLE_FREQUENCY=4 \
STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5 \
node experiments/pattern-analysis/run-custom-patterns-comparison.js \
  --iterations 1 \
  --pattern-test-timeout 300000
```

## Artifact Paths

Top-level summaries:

- [summary.raw.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.raw.json)
- [summary.raw.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.raw.csv)
- [summary.trimmed-4-33.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.trimmed-4-33.json)
- [summary.trimmed-4-33.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.trimmed-4-33.csv)
- [summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.json)
- [summary.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/analysis/custom-pattern-accuracy/summary.csv)

Per-pattern iteration outputs example (low_variability):

- [fetching_results.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/fetching/low_variability/iteration1/fetching_results.csv)
- [approximation_results.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/approximation/low_variability/iteration1/approximation_results.csv)
- [chunked_results.csv](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_results.csv)
- [chunked_emission_proof.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/chunked_emission_proof.json)
- [resource_summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/resource_summary.json)
- [mqtt_traffic_summary.json](file:///Users/kushbisen/Code/streaming-query-hive/logs/custom-pattern-comparison/chunked/low_variability/iteration1/mqtt_traffic_summary.json)

## Validation Results Per Pattern

Below are the detailed evaluations against the 11 validation criteria.

### 1. Low Variability

- fetching emits windows 1..5: pass (5 results verified)
- approximation emits completed_window_approximation rows 1..5: pass (5 results verified)
- chunked emits windows 1..5: pass (5 results verified)
- chunked required_chunk_intervals cover full 120s: pass (four 30s intervals cover 120s)
- approximation extractor produces nonzero results: pass (MAE = 0.001171)
- summary.raw.json includes windows 1..5: pass (matched_windows = 5)
- summary.trimmed-4-33.json includes windows 4..5: pass (matched_windows = 2)
- chunked MAE/MAPE/RMSE vs fetching are effectively zero: pass (MAE = 3.979e-14, RMSE = 4.000e-14, MAPE = 1.728e-13)
- approximation MAE/MAPE/RMSE are reported: pass (MAE = 0.001171, RMSE = 0.001341, MAPE = 0.005086)
- CPU-seconds summaries exist: pass (resource_summary.json exists with cpuSeconds = 12.73s)
- MQTT summaries exist: pass (mqtt_traffic_summary.json exists with published_application_bytes = 2426260)

### 2. Step Pattern

- fetching emits windows 1..5: pass (5 results verified)
- approximation emits completed_window_approximation rows 1..5: pass (5 results verified)
- chunked emits windows 1..5: pass (5 results verified)
- chunked required_chunk_intervals cover full 120s: pass (four 30s intervals cover 120s)
- approximation extractor produces nonzero results: pass (MAE = 0.124037)
- summary.raw.json includes windows 1..5: pass (matched_windows = 5)
- summary.trimmed-4-33.json includes windows 4..5: pass (matched_windows = 2)
- chunked MAE/MAPE/RMSE vs fetching are effectively zero: pass (all error metrics are exactly 0.000000)
- approximation MAE/MAPE/RMSE are reported: pass (MAE = 0.124037, RMSE = 0.256322, MAPE = 0.652570)
- CPU-seconds summaries exist: pass (resource_summary.json exists)
- MQTT summaries exist: pass (mqtt_traffic_summary.json exists)

### 3. Spike Pattern

- fetching emits windows 1..5: pass (5 results verified)
- approximation emits completed_window_approximation rows 1..5: pass (5 results verified)
- chunked emits windows 1..5: pass (5 results verified)
- chunked required_chunk_intervals cover full 120s: pass (four 30s intervals cover 120s)
- approximation extractor produces nonzero results: pass (MAE = 0.033243)
- summary.raw.json includes windows 1..5: pass (matched_windows = 5)
- summary.trimmed-4-33.json includes windows 4..5: pass (matched_windows = 2)
- chunked MAE/MAPE/RMSE vs fetching are effectively zero: pass (all error metrics are exactly 0.000000)
- approximation MAE/MAPE/RMSE are reported: pass (MAE = 0.033243, RMSE = 0.034112, MAPE = 0.145844)
- CPU-seconds summaries exist: pass (resource_summary.json exists)
- MQTT summaries exist: pass (mqtt_traffic_summary.json exists)

### 4. Low Freq. Oscillation

- fetching emits windows 1..5: pass (5 results verified)
- approximation emits completed_window_approximation rows 1..5: pass (5 results verified)
- chunked emits windows 1..5: pass (5 results verified)
- chunked required_chunk_intervals cover full 120s: pass (four 30s intervals cover 120s)
- approximation extractor produces nonzero results: pass (MAE = 0.030719)
- summary.raw.json includes windows 1..5: pass (matched_windows = 5)
- summary.trimmed-4-33.json includes windows 4..5: pass (matched_windows = 2)
- chunked MAE/MAPE/RMSE vs fetching are effectively zero: pass (all error metrics are exactly 0.000000)
- approximation MAE/MAPE/RMSE are reported: pass (MAE = 0.030719, RMSE = 0.067755, MAPE = 0.133562)
- CPU-seconds summaries exist: pass (resource_summary.json exists)
- MQTT summaries exist: pass (mqtt_traffic_summary.json exists)

### 5. High Freq. Oscillation

- fetching emits windows 1..5: pass (5 results verified)
- approximation emits completed_window_approximation rows 1..5: pass (5 results verified)
- chunked emits windows 1..5: pass (5 results verified)
- chunked required_chunk_intervals cover full 120s: pass (four 30s intervals cover 120s)
- approximation extractor produces nonzero results: pass (MAE = 0.000788)
- summary.raw.json includes windows 1..5: pass (matched_windows = 5)
- summary.trimmed-4-33.json includes windows 4..5: pass (matched_windows = 2)
- chunked MAE/MAPE/RMSE vs fetching are effectively zero: pass (all error metrics are exactly 0.000000)
- approximation MAE/MAPE/RMSE are reported: pass (MAE = 0.000788, RMSE = 0.001096, MAPE = 0.003425)
- CPU-seconds summaries exist: pass (resource_summary.json exists)
- MQTT summaries exist: pass (mqtt_traffic_summary.json exists)

## Validation Verdict

All five custom patterns pass the required criteria. There are no blocking, smoke-only, or cosmetic failures.

| Pattern | Verdict | Failure Classification | Notes |
| --- | --- | --- | --- |
| low_variability | pass | none | all 11 criteria met successfully |
| step_pattern | pass | none | all 11 criteria met successfully |
| spike_pattern | pass | none | all 11 criteria met successfully |
| low_freq_oscillation | pass | none | all 11 criteria met successfully |
| high_freq_oscillation | pass | none | all 11 criteria met successfully |

All custom patterns have passed alignment with the paper methodology. The pipeline is cleared to run full 35-window evaluations.
