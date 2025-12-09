# Debug Prompt for Streaming Query MAPE Experiment

## Problem Statement

I have a pattern accuracy experiment that streams RDF N-Quads data through MQTT to two different query processing approaches (Approximation and Fetching/Ground Truth), then calculates MAPE by comparing their results. 

**The issue:** The experiment runs successfully but collects **0 results** from both approaches, resulting in MAPE = N/A for all patterns.

**Important context:** When I test with the original repository data files (e.g., `src/streamer/data/frequency_variants/2mins/wearable/4Hz/data.nt`), the system works and produces results. But with my generated pattern data, I get 0 results.

## What Works

1. ✅ Data generation creates valid N-Quads files (format identical to working data)
2. ✅ Publishers load and stream data successfully (480 observations per pattern)
3. ✅ Both orchestrators initialize without errors
4. ✅ Orchestrators process incoming quads (logs show "Adding [object Object] at time X")
5. ✅ Experiment completes all 7 patterns without crashing
6. ✅ MQTT broker is running and receiving data

## What Doesn't Work

1. ❌ No results published to MQTT topic "output" (Approximation approach)
2. ❌ No results published to MQTT topic "client_operation_output" (Fetching approach)
3. ❌ Result counts: Approx = 0, Ground Truth = 0
4. ❌ MAPE calculation fails due to empty result arrays

## Architecture Overview

```
Generated N-Quads Files
    ↓
experiment-publisher.js (StreamToMQTT class)
    ↓ (replaces timestamps with Date.now())
MQTT Topics: wearableX, smartphoneX
    ↓
    ├─→ ApproximationApproachOrchestrator
    │   - Sub-queries: 60s window, 30s slide
    │   - Main query: 120s window, 60s slide
    │   - Output topic: "output"
    │
    └─→ FetchingClientSideApproachOrchestrator
        - Single query: 120s window, 60s slide
        - Output topic: "client_operation_output"
```

## Key Files

### Data Publisher
- **File:** `src/streamer/src/publishing/StreamToMQTT.ts`
- **Key behavior:** Replaces original timestamps with `new Date().toISOString()` during replay
- **Publishing:** Uses QoS 2 to MQTT topics

### Approximation Approach
- **File:** `src/approaches/ApproximationApproachOrchestrator.ts`
- **Sub-queries:**
```sparql
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
```

### Fetching Approach
- **File:** `src/approaches/FetchingClientSideApproachOrchestrator.ts`
- **Main query:**
```sparql
SELECT (MAX(?value) AS ?avgValue)
WHERE {
    {
        GRAPH <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        GRAPH <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
```

## Experiment Timing

1. Wait 10s for orchestrators to initialize
2. Start data publishers
3. Stream data for 120s at 4Hz (480 observations total)
4. Stop publishers
5. Wait 20s for results collection
6. Kill orchestrators and cleanup

## Data Format Comparison

### Working Data (original):
```
<https://dahcc.idlab.ugent.be/Protego/_participant1/obs0> <https://saref.etsi.org/core/hasValue> "-23.0"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/obs0> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .
```

### Generated Data (not working):
```
<https://dahcc.idlab.ugent.be/Protego/_participant1/obs0> <https://saref.etsi.org/core/hasValue> "-22.9"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/obs0> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .
```

**Note:** Format is identical except for the actual values.

## Questions to Investigate

1. **Window Timing:** With 120s of data streaming and a 60s slide window, we should get at least 2 results. Why 0?

2. **Graph Names:** The queries use `WINDOW <mqtt://localhost:1883/wearableX>` and `GRAPH <mqtt://localhost:1883/wearableX>`. Does the MQTT topic "wearableX" correctly map to graph URI `mqtt://localhost:1883/wearableX`?

3. **Timestamp Handling:** StreamToMQTT replaces timestamps with current time during replay. Could this cause window evaluation issues?

4. **Data Accumulation:** Are the windows actually receiving data, or is there a subscription/routing issue?

5. **Result Publishing:** Even if windows evaluate, are the results being published correctly to MQTT output topics?

## Logs Show

```
[FetchingOrch]: Adding [" + [object Object] + "] at time : 1765222913249 and watermark 1765222913249
```

This confirms data is being processed, but no "Result (Approx)" or "Result (Ground Truth)" messages appear.

## Expected vs Actual

**Expected:** 2-4 results per pattern (based on 60s slide over 120s duration)
**Actual:** 0 results for all patterns

## Your Task

Please analyze this system and identify:

1. **Root cause:** Why are 0 results being produced when the orchestrators clearly receive and process data?

2. **Specific bug location:** Which file(s) and line(s) contain the issue?

3. **Window evaluation:** Is the window timing logic correct? Should we see results with 120s of streamed data?

4. **Graph/Stream mapping:** Is there a mismatch between MQTT topics and graph URIs?

5. **Suggested fix:** What code changes are needed to make results appear?

## Additional Context

- RSP-JS library is used for stream processing
- The working benchmark uses the same orchestrators and queries
- Only difference is the data source (original files vs generated files)
- Generated files are structurally identical to original files

Please provide a detailed analysis with specific code references and proposed solutions.