# Approach Comparison Experiment

This experiment compares three approaches for processing streaming queries in the Streaming Query Hive:

1. **Fetching Client-Side Approach** (Ground Truth)
2. **Approximation Approach**
3. **Chunked Query Approach**

## Metrics Measured

### 1. First Event Latency
The time between the window closing and the result being available. This measures how quickly each approach can produce results after a window boundary is crossed.

### 2. Accuracy
Comparison of results against the Fetching Client-Side approach, which serves as the ground truth since it processes all raw data locally using the RSP-JS engine.

## Queries Used

### Sub-Query 1 (Wearable X Stream)
```sparql
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
```

### Sub-Query 2 (Smartphone X Stream)
```sparql
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
```

### Main Query (Combined)
```sparql
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (MAX(?value) AS ?avgValue)
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

## Prerequisites

1. **MQTT Broker**: Mosquitto or equivalent running on `localhost:1883`
   ```bash
   # macOS
   brew services start mosquitto
   
   # Linux
   sudo systemctl start mosquitto
   
   # Or run directly
   mosquitto -v
   ```

2. **Node.js**: Version 16 or higher

3. **Python 3**: For analysis scripts (optional but recommended)

## Running the Experiment

### Quick Start

```bash
cd streaming-query-hive
./experiments/approach-comparison/run-experiment.sh
```

### Manual Execution

```bash
# 1. Compile TypeScript
npm run build

# 2. Run the experiment
npx ts-node experiments/approach-comparison/ApproachComparisonExperiment.ts ./experiments/approach-comparison/results

# 3. Analyze results
python3 experiments/approach-comparison/analyze_results.py ./experiments/approach-comparison/results/run_<timestamp>
```

### Options

```bash
./run-experiment.sh --help
./run-experiment.sh --skip-mqtt-check    # Skip MQTT broker verification
./run-experiment.sh --skip-compile       # Skip TypeScript compilation
```

## Output Files

Results are saved in `./results/run_<timestamp>/`:

| File | Description |
|------|-------------|
| `latency_results_<timestamp>.csv` | Per-window latency measurements for all approaches |
| `accuracy_results_<timestamp>.csv` | Accuracy comparison against ground truth |
| `ANALYSIS_REPORT.md` | Generated summary report |

### Latency CSV Format
```csv
approach,window_number,window_close_time,result_available_time,first_event_latency_ms,result_value,timestamp
```

### Accuracy CSV Format
```csv
approach,window_number,ground_truth_value,approach_value,absolute_error,percentage_error
```

## Configuration

Default experiment configuration (modifiable in `ApproachComparisonExperiment.ts`):

| Parameter | Value | Description |
|-----------|-------|-------------|
| `mqttBroker` | `mqtt://localhost:1883` | MQTT broker URL |
| `dataFrequency` | 4 | Events per second per stream |
| `experimentDurationMs` | 180000 (3 min) | Total experiment duration |
| `windowWidthMs` | 120000 (2 min) | Main query window width |
| `windowSlideMs` | 60000 (1 min) | Window slide interval |
| `subQueryWindowWidthMs` | 60000 (1 min) | Sub-query window width |
| `subQueryWindowSlideMs` | 60000 (1 min) | Sub-query slide interval |

## Approach Descriptions

### Fetching Client-Side Approach
- Fetches all raw data to the client
- Processes using local RSP-JS engine
- Highest accuracy (serves as ground truth)
- Higher latency and resource usage

### Approximation Approach
- Maintains sliding window buffers per stream
- Computes aggregation on each slide interval
- Fast computation with potential timing drift
- Good for stable, predictable streams

### Chunked Query Approach
- Divides windows into GCD-based chunks
- Aggregates chunk results for final output
- Balance between accuracy and efficiency
- Best for high-frequency regular patterns

## Interpreting Results

### Latency
- Lower is better
- Compare against ground truth baseline
- Consider P95/P99 for production scenarios

### Accuracy
- Measured as percentage error vs ground truth
- 0% error = perfect match
- Consider both mean and max error
- "Within X% error" shows consistency

## Troubleshooting

### MQTT Connection Issues
```bash
# Check if broker is running
nc -z localhost 1883

# Check broker logs
mosquitto -v
```

### No Results Generated
- Verify MQTT broker is accepting connections
- Check experiment duration is sufficient (at least 2x window width)
- Ensure no other processes are consuming from same topics

### High Error Rates
- Check data generation is consistent
- Verify window timing alignment
- Consider increasing experiment duration for more samples

## Files

```
experiments/approach-comparison/
├── README.md                           # This file
├── ApproachComparisonExperiment.ts     # Main experiment code
├── run-experiment.sh                   # Runner script
├── analyze_results.py                  # Analysis script
└── results/                            # Output directory
    └── run_<timestamp>/
        ├── latency_results_<ts>.csv
        ├── accuracy_results_<ts>.csv
        └── ANALYSIS_REPORT.md
```

## References

- [RSP-QL Semantics](https://www.igi-global.com/article/rsp-ql-semantics/129761)
- [Query Containment](https://link.springer.com/referenceworkentry/10.1007/978-0-387-39940-9_1269)
- [RSP-JS Library](https://github.com/SolidLabResearch/rsp-js)