# Three-Approach Frequency Comparison Experiment

This experiment compares three different streaming query processing approaches across various signal frequencies to evaluate their accuracy, latency, and performance characteristics.

---

## Overview

### Approaches Tested

1. **Fetching (Client-Side)** - Baseline/Ground Truth
   - Fetches all raw data points from MQTT streams
   - Performs aggregation client-side after collection
   - Most accurate but potentially resource-intensive
   - Orchestrator: `StreamingQueryFetchingClientSideApproachOrchestrator.ts`

2. **Approximation (Rate-Based)**
   - Uses pre-aggregated sub-queries with smaller windows
   - Combines overlapping sub-query results incrementally
   - Lower resource usage, minimal accuracy loss
   - Orchestrator: `StreamingQueryApproximationApproachOrchestrator.ts`

3. **Chunked (Aggregation)**
   - Chunks data into time intervals and aggregates incrementally
   - Publishes combined results from multiple sensors
   - Balance between accuracy and efficiency
   - Orchestrator: `StreamingQueryChunkedApproachOrchestrator.ts`

---

## Experimental Setup

### Query Configuration

All three approaches run the same logical query:

```sparql
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
```

**Window Configuration:**
- **RANGE**: 120 seconds (120000ms)
- **STEP**: 60 seconds (60000ms)
- Two sensor streams: `wearableX` and `smartphoneX`

### Test Data

- **Signal Type**: Complex oscillation (sum of multiple sine waves)
- **Frequencies Tested**: 0.1, 0.5, 1.0, 1.5, 2.0 Hz
- **Sampling Rate**: ~4 Hz (250ms intervals)
- **Nyquist Frequency**: 2.0 Hz
- **Duration**: ~3 minutes per test

### Metrics Collected

#### Accuracy Metrics (vs Fetching Baseline)
- **MAPE** (Mean Absolute Percentage Error): Average percentage deviation
- **MAE** (Mean Absolute Error): Average absolute difference
- **RMSE** (Root Mean Square Error): Square root of average squared errors

#### Latency Metrics
- **First-Event Latency**: Time from query registration to first result
- **Query Registration Time**: When the query is registered with the RSP engine
- **First Result Time**: When the first window result is emitted

---

## Running the Experiment

### Prerequisites

1. Build the project:
   ```bash
   npm run build
   ```

2. Ensure MQTT broker is running:
   ```bash
   mosquitto -c /opt/homebrew/etc/mosquitto/mosquitto.conf
   ```

3. Generate test data (if not already available):
   ```bash
   node dist/streamer/src/generator.js
   ```

### Run Single Frequency (All Approaches)

```bash
node experiments/frequency-comparison/run-all-approaches-comparison.js 0.1
```

This will:
1. Run fetching approach at 0.1 Hz
2. Run approximation approach at 0.1 Hz
3. Run chunked approach at 0.1 Hz
4. Extract results from logs
5. Generate comparison analysis

### Run All Frequencies (All Approaches)

```bash
node experiments/frequency-comparison/run-all-approaches-comparison.js
```

**Warning**: This runs 15 tests total (3 approaches × 5 frequencies) and takes approximately 45-60 minutes.

### Run Individual Approaches

If you need to run approaches separately:

```bash
# Fetching only
node experiments/frequency-comparison/experiment-frequency-fetching-with-capture.js 0.1

# Approximation only
node experiments/frequency-comparison/experiment-frequency-approximation-with-capture.js 0.1

# Chunked only
node experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js 0.1
```

---

## Results and Analysis

### Output Locations

After running experiments, results are saved to:

```
logs/
├── frequency-comparison-fetching/
│   └── complex_oscillation_freq_0.1/
│       └── iteration1/
│           ├── fetching_results.csv
│           ├── fetching_metadata.json
│           ├── fetching_client_side_log.csv
│           └── fetching_latency_log.csv
├── frequency-comparison-approximation/
│   └── complex_oscillation_freq_0.1/
│       └── iteration1/
│           ├── approximation_results.csv
│           ├── approximation_metadata.json
│           ├── approximation_approach_log.csv
│           └── approximation_latency_log.csv
├── frequency-comparison-chunked/
│   └── complex_oscillation_freq_0.1/
│       └── iteration1/
│           ├── chunked_results.csv
│           ├── chunked_metadata.json
│           ├── streaming_query_chunk_aggregator_log.csv
│           └── chunked_latency_log.csv
├── accuracy_comparison_all_approaches.csv
├── latency_comparison_all_approaches.csv
└── comparison_summary_all_approaches.json
```

### Extracting Results from Logs

If results weren't captured via MQTT, extract them from logs:

```bash
node experiments/frequency-comparison/extract-results-from-logs.js fetching 0.1
node experiments/frequency-comparison/extract-results-from-logs.js approximation 0.1
node experiments/frequency-comparison/extract-results-from-logs.js chunked 0.1
```

### Running Analysis

Generate comparison tables and CSV files:

```bash
node analysis/accuracy/accuracy-comparison-all-approaches.js
```

This produces:
- Console output with formatted comparison tables
- `accuracy_comparison_all_approaches.csv`: Accuracy metrics per approach/frequency
- `latency_comparison_all_approaches.csv`: Latency metrics per approach/frequency
- `comparison_summary_all_approaches.json`: Complete JSON summary

---

## Expected Results

### First-Event Latency

All three approaches should show similar first-event latency (~60-62 seconds) because:
- The STEP parameter (60 seconds) controls when results are emitted
- All approaches must wait for the first window boundary
- Query semantics are the same across all approaches

**Example:**
```
Frequency | Approach      | First-Event Latency | Difference vs Fetching
0.1 Hz    | Fetching      | 61.75s              | Baseline
          | Approximation | 61.61s              | -0.14s
          | Chunked       | 61.50s              | -0.25s
```

### Accuracy Comparison

- **Low Frequencies (0.1, 0.5 Hz)**: All approaches should be highly accurate (MAPE < 0.1%)
- **Mid Frequencies (1.0 Hz)**: Approximation and chunked may show slight degradation
- **Near Nyquist (1.5, 2.0 Hz)**: Approximation may show higher error due to aliasing effects
- **Fetching**: Always 0% error (baseline/ground truth)

**Example:**
```
Frequency | Approach      | MAPE (%)   | MAE        | RMSE
0.1 Hz    | Fetching      | Baseline   | Baseline   | Baseline
          | Approximation | 0.0129     | 0.006447   | 0.006447
          | Chunked       | 0.0085     | 0.004252   | 0.004252
```

---

## Understanding the Results

### Why Similar Latency?

The first-event latency is determined by the window configuration, not the processing approach:
- **RANGE 120000**: Window collects 120 seconds of data
- **STEP 60000**: Results emitted every 60 seconds
- First result appears at the first STEP boundary (~60s)

### Accuracy vs Efficiency Trade-off

| Approach      | Accuracy | Resource Usage | Network Overhead | Scalability |
|---------------|----------|----------------|------------------|-------------|
| Fetching      | Highest  | High           | High             | Limited     |
| Approximation | High     | Low            | Low              | Excellent   |
| Chunked       | High     | Medium         | Medium           | Good        |

### When to Use Each Approach

**Fetching (Client-Side)**
- Need exact results (no approximation acceptable)
- Low query volume
- Sufficient resources available
- Simple to implement

**Approximation (Rate-Based)**
- High query volume
- Resource constraints
- Near-real-time requirements
- Acceptable accuracy loss (< 1%)
- Multiple concurrent queries

**Chunked (Aggregation)**
- Balance between accuracy and efficiency needed
- Moderate query volume
- Time-windowed aggregations
- Cross-sensor computations

---

## Troubleshooting

### No Results Captured

If MQTT capture fails, results can be extracted from logs:
```bash
node experiments/frequency-comparison/extract-results-from-logs.js <approach> <frequency>
```

### Negative Latency Values

Seeing negative latency in logs (e.g., `-58388ms`) is normal and indicates early result emission:
- The "expected close" time is when the window should fully close (registration + RANGE)
- Results can be emitted earlier at STEP boundaries
- Negative value = how much earlier the result arrived

### Missing Metadata Files

Run the extraction script to generate metadata from logs:
```bash
node experiments/frequency-comparison/extract-results-from-logs.js <approach> <frequency>
```

### Process Timeout

Default timeout is 3 minutes per test. If data replay takes longer:
1. Check data file size in `src/streamer/data/frequency_comparison/`
2. Increase timeout in experiment scripts
3. Verify MQTT broker is running

---

## Data Files Structure

Test data is located in:
```
src/streamer/data/frequency_comparison/
├── complex_oscillation_freq_0.1/
│   ├── smartphone.acceleration.x/
│   │   └── data.nt
│   └── wearable.acceleration.x/
│       └── data.nt
├── complex_oscillation_freq_0.5/
├── complex_oscillation_freq_1.0/
├── complex_oscillation_freq_1.5/
└── complex_oscillation_freq_2.0/
```

Each dataset contains:
- RDF triples in N-Triples format
- Timestamps for each data point
- Two sensor streams (smartphone and wearable)

---

## Architecture Notes

### Sub-Query Configuration

**Approximation Approach:**
- Main query: RANGE 120000, STEP 60000
- Sub-queries: RANGE 60000, STEP 30000
- Combines overlapping sub-query windows

**Chunked Approach:**
- Main query: RANGE 120000, STEP 60000
- Sub-queries: RANGE 60000, STEP 30000
- Aggregates chunks incrementally

### MQTT Topics

- **Fetching**: Publishes to `output`
- **Approximation**: Publishes to `approximation/output`
- **Chunked**: Publishes to `chunked/output`

### Latency Calculation

For all approaches:
```
queryRegisteredTime = expectedCloseTime - RANGE (120000ms)
firstEventLatency = firstResultTime - queryRegisteredTime
```

---

## References

- Fetching baseline: See `FIRST_EVENT_LATENCY.md`
- Approximation details: See `RESULTS_SUMMARY.md`
- Query syntax: RSP-QL specification

---

## Future Work

- Add resource usage comparison (CPU, memory, network)
- Test with higher frequencies (beyond Nyquist)
- Multi-iteration runs for statistical significance
- Real-world sensor data comparison
- Adaptive approach selection based on frequency