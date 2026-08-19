# Frequency Comparison Experiments with Results Capture

## Overview

This directory contains experiments that compare the **Fetching Client-Side Approach** (baseline) with the **Approximation Approach** across different signal frequencies to evaluate:

1. **First Event Latency** - Time from data replay start to first query result
2. **Accuracy** - Using fetching approach as ground truth to measure approximation error
3. **Frequency Impact** - How signal frequency affects approximation accuracy (especially near Nyquist limit)

## Architecture

The experiment infrastructure consists of:

### 1. Results Capture Utility (`capture-results.js`)
- Subscribes to MQTT result topics
- Captures query results in real-time to CSV files
- Records timing metadata (first event latency, window numbers)
- Runs alongside the approach orchestrators

### 2. Modified Experiment Runners
- **`experiment-frequency-fetching-with-capture.js`** - Runs fetching approach with results capture
- **`experiment-frequency-approximation-with-capture.js`** - Runs approximation approach with results capture
- Each launches 3 processes:
  1. Results capture (subscribes to MQTT)
  2. Approach orchestrator (runs queries)
  3. Data publisher (replays sensor data)

### 3. Master Runner (`run-frequency-comparison-with-capture.js`)
- Orchestrates complete experiment workflow
- Runs fetching (baseline) for all frequencies first
- Then runs approximation for all frequencies
- Provides comprehensive summary and next steps

## Quick Start

### Run All Experiments

```bash
# Clean old logs (optional)
rm -rf logs/frequency-comparison-*

# Run complete experiment suite (both approaches, all frequencies)
node experiments/frequency-comparison/run-frequency-comparison-with-capture.js
```

This will test 5 frequencies (0.1, 0.5, 1.0, 1.5, 2.0 Hz) with both approaches.
Expected duration: ~35-40 minutes total.

### Run Single Experiment

```bash
# Fetching approach at 0.1 Hz
node experiments/frequency-comparison/experiment-frequency-fetching-with-capture.js complex_oscillation 0.1

# Approximation approach at 0.1 Hz
node experiments/frequency-comparison/experiment-frequency-approximation-with-capture.js complex_oscillation 0.1
```

## Output Structure

After running experiments, results are organized as:

```
logs/
├── frequency-comparison-fetching/
│   ├── complex_oscillation_freq_0.1/
│   │   └── iteration1/
│   │       ├── fetching_results.csv         ← Query results (timestamp, window_number, result_value, latency)
│   │       ├── fetching_metadata.json       ← Test metadata (first event latency, topic info)
│   │       ├── fetching_client_side_log.csv ← System logs
│   │       └── replayer-log.csv             ← Data replay timing
│   ├── complex_oscillation_freq_0.5/
│   ├── complex_oscillation_freq_1.0/
│   ├── complex_oscillation_freq_1.5/
│   └── complex_oscillation_freq_2.0/
│
└── frequency-comparison-approximation/
    ├── complex_oscillation_freq_0.1/
    │   └── iteration1/
    │       ├── approximation_results.csv      ← Query results
    │       ├── approximation_metadata.json    ← Test metadata
    │       ├── approximation_approach_log.csv ← System logs
    │       └── replayer-log.csv               ← Data replay timing
    ├── complex_oscillation_freq_0.5/
    ├── complex_oscillation_freq_1.0/
    ├── complex_oscillation_freq_1.5/
    └── complex_oscillation_freq_2.0/
```

### Key Files

#### `*_results.csv` - Query Results
Contains actual query results captured from MQTT:
```csv
timestamp,window_number,result_value,latency_from_start_ms
1767788920123,1,55.234567,2741
1767788980456,2,58.901234,63074
...
```

#### `*_metadata.json` - Test Metadata
```json
{
  "approach": "fetching",
  "frequency": "0.1",
  "startTime": 1767788917382,
  "firstResultTime": 1767788920123,
  "totalResults": 8,
  "firstEventLatency": 2741,
  "resultTopic": "output",
  "captureDate": "2026-01-07T12:28:40.123Z"
}
```

## Analysis

### Run Accuracy Comparison

After experiments complete, analyze accuracy:

```bash
node analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js
```

This script:
- Reads `*_results.csv` files from both approaches
- Uses fetching as ground truth baseline
- Calculates accuracy metrics for each frequency:
  - **MAPE** (Mean Absolute Percentage Error)
  - **MAE** (Mean Absolute Error)
  - **RMSE** (Root Mean Square Error)
  - **Correlation coefficient**
- Generates `logs/accuracy_comparison_results.csv`

### Expected Results

The analysis should show:

1. **Low Frequencies (0.1-0.5 Hz)**:
   - High approximation accuracy (low MAPE)
   - Strong correlation with fetching
   - Minimal error due to good sampling ratio

2. **Medium Frequencies (1.0-1.5 Hz)**:
   - Moderate approximation accuracy
   - Some error accumulation from pre-aggregation
   - Approaching Nyquist considerations

3. **High Frequencies (1.5-2.0 Hz)**:
   - **Degraded approximation accuracy**
   - Near Nyquist limit (2 Hz with 4 Hz sampling)
   - Aliasing effects become significant
   - Fetching approach remains more accurate

### First Event Latency

Compare first event latency from metadata files:
- Fetching: Direct query execution, predictable latency
- Approximation: Additional overhead from sub-query coordination

## Experimental Setup

### Signal Properties
- **Oscillation Type**: Complex oscillation with harmonics
- **Frequencies**: 0.1, 0.5, 1.0, 1.5, 2.0 Hz
- **Sampling Rate**: ~4 Hz (250ms intervals)
- **Nyquist Limit**: 2.0 Hz (frequency at which aliasing becomes critical)

### Query Configuration
- **Window Range**: 120 seconds
- **Window Slide**: 60 seconds
- **Aggregation**: AVG + COUNT
- **Sensors**: wearableX, smartphoneX (2 streams)

### Approaches Tested

#### Fetching Client-Side (Baseline)
- Fetches all raw data to client
- Computes aggregations locally
- Publishes to topic: `output`
- **Advantage**: No approximation error, full data access
- **Disadvantage**: Higher network overhead

#### Approximation
- Pre-computes sub-queries on smaller windows
- Combines results using approximation operator
- Publishes to topic: `approximation/output`
- **Advantage**: Reduced network overhead, distributed computation
- **Disadvantage**: Approximation error, especially at high frequencies

## Interpretation

### Which Approach is Better?

**Fetching Client-Side Approach is superior for accuracy** because:

1. **Ground Truth**: It processes all raw data without approximation
2. **Frequency Independence**: Maintains accuracy across all frequencies
3. **Predictable Behavior**: No error accumulation from pre-aggregation
4. **Near-Nyquist Performance**: Remains accurate even at 2.0 Hz

**Approximation Approach trades accuracy for efficiency** but:

1. **Low Frequency Acceptable**: Performs well at 0.1-0.5 Hz
2. **High Frequency Problematic**: Degrades significantly at 1.5-2.0 Hz
3. **Aliasing Susceptible**: Pre-aggregation amplifies Nyquist effects
4. **Error Accumulation**: Combining approximate sub-results compounds errors

### Recommendation

Use **Fetching Client-Side** as baseline when:
- Accuracy is critical
- Operating near Nyquist frequencies
- Network bandwidth is sufficient

Use **Approximation** only when:
- Frequencies are well below Nyquist (< 0.5x Nyquist)
- Network/compute constraints require distribution
- Acceptable error tolerance exists (validate with MAPE)

## Troubleshooting

### No results captured
- Check MQTT broker is running: `mqtt://localhost:1883`
- Verify approaches are publishing to correct topics
- Check `capture_log.txt` for subscription errors

### Experiments timeout
- Default timeout: 3 minutes per experiment
- Increase in experiment scripts if needed
- Check data publisher is completing

### Missing result files
- Ensure capture process starts before approach
- Check file permissions in logs directory
- Verify CSV files aren't being overwritten

## Technical Details

### MQTT Topics
- **Fetching results**: `output`
- **Approximation results**: `approximation/output`
- **Sub-query results**: `chunked/<query_hash>`

### Timing Sequence
1. Results capture starts and subscribes to MQTT (t=0)
2. Approach orchestrator starts (t=1s)
3. Data publisher starts replaying (t=3s)
4. Results flow through MQTT and are captured
5. Publisher completes, processes are stopped
6. Graceful shutdown saves metadata

### CSV Format
The results CSV uses a simple, analysis-friendly format:
- `timestamp`: Unix epoch milliseconds
- `window_number`: Sequential window counter (1, 2, 3, ...)
- `result_value`: Computed aggregation result
- `latency_from_start_ms`: Time since capture started

This format enables direct comparison between approaches using the same window numbers.

## References

- Main experiment runners: `experiment-frequency-*-with-capture.js`
- Results capture utility: `capture-results.js`
- Master runner: `run-frequency-comparison-with-capture.js`
- Analysis script: `../../analysis/accuracy/accuracy-comparison-approximation-vs-fetching.js`
- Data generation: Check `src/streamer/data/frequency_comparison/`
