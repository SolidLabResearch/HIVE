# Pattern Accuracy Experiment - Fixes and Improvements

## Overview

This document details the fixes applied to the `pattern-accuracy-experiment.ts` to resolve execution issues and align data generation with the existing repository infrastructure.

## Issues Fixed

### 1. Port Conflict (EADDRINUSE Error)

**Problem:** The ApproximationApproachOrchestrator attempted to start an HTTP server on port 8080, which was already in use from previous experiment runs.

**Solution:**
- Added `HTTP_PORT` environment variable support to both `Orchestrator.ts` and `IntelligentOrchestrator.ts`
- Modified the experiment to launch ApproximationApproachOrchestrator on port 8081
- The port configuration now checks `process.env.HTTP_PORT` before falling back to the default config value

**Files Modified:**
- `src/orchestrator/Orchestrator.ts`
- `src/orchestrator/IntelligentOrchestrator.ts`
- `examples/pattern-accuracy-experiment.ts`

### 2. Data Generation Method

**Problem:** The experiment was generating synthetic data manually using custom quad creation, which didn't match the format and frequency of real data in the repository.

**Solution:**
- Replaced manual data generation with the existing `StreamToMQTT` infrastructure
- Now uses real sensor data from `src/streamer/data/frequency_variants/2mins/`
- Data is published using the same `experiment-publisher.js` mechanism used in other benchmarks
- Frequency set to 4Hz to match existing data files

**Changes:**
- Removed `generateAndPublishData()` and `createQuad()` methods
- Added `startDataPublishers()` method that spawns publisher processes using real data files
- Changed from 10Hz synthetic data to 4Hz real data from N-Quads files

### 3. Result Collection

**Problem:** No results were being captured - MAPE values showed as "N/A" for all patterns.

**Solution:**
- Improved regex pattern for parsing MQTT result messages
- Changed from `/"(.*?)"/ ` to `/"([0-9.+-eE]+)"\^\^/` to properly match XSD typed literals
- Added validation to ensure parsed values are valid numbers
- Increased post-processing wait time from 15s to 20s to allow all results to be collected

### 4. Experiment Duration

**Problem:** The experiment ran for only 20 seconds, which didn't match the 2-minute data files available.

**Solution:**
- Increased `EXPERIMENT_DURATION_S` from 20 to 120 seconds (2 minutes)
- This aligns with the duration of data files in `frequency_variants/2mins/`
- Allows for more stable statistical analysis and better MAPE calculations

### 5. Output Filtering

**Problem:** Console output was flooded with debug messages and watermark errors.

**Solution:**
- Added output filtering in `spawnOrchestrator()` to suppress:
  - "Watermark is not increasing" errors (known non-critical issue)
  - "DEBUG: Adding quad" messages (verbose logging)
  - "Published observation" messages from data publishers
  - Port conflict errors (when using different ports intentionally)

### 6. Pattern Definitions

**Problem:** Patterns were defined as generator functions, making it impossible to use real data.

**Solution:**
- Changed pattern interface from `generator: (time: number) => number` to file paths
- Now patterns specify `wearablePath` and `smartphonePath` to data files
- Currently uses single pattern "4Hz Standard Data" with real sensor data
- Can be extended to include multiple frequency variants (8Hz, 16Hz, 32Hz, 64Hz, 128Hz)

## Data Format

The experiment now uses real RDF data in N-Quads format from the repository:

```
<subject> <predicate> "value"^^<datatype> .
```

Example:
```
<https://dahcc.idlab.ugent.be/Protego/_participant1/obs0> <https://saref.etsi.org/core/hasValue> "-23.0"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://dahcc.idlab.ugent.be/Protego/_participant1/obs0> <https://saref.etsi.org/core/hasTimestamp> "2025-07-15T08:48:24.620Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
```

## Usage

### Prerequisites

1. MQTT broker running on `localhost:1883`
2. Project built with `npm run build`
3. No processes using ports 8080-8081

### Running the Experiment

```bash
npx ts-node examples/pattern-accuracy-experiment.ts
```

### Expected Output

The experiment will:
1. Start two orchestrators (Approximation and Fetching approaches)
2. Launch data publishers for wearable and smartphone streams
3. Stream data for 2 minutes at 4Hz
4. Collect results from both approaches
5. Calculate MAPE (Mean Absolute Percentage Error)
6. Display summary table with:
   - Pattern name
   - Number of approximation results
   - Number of ground truth results
   - MAPE percentage

## Future Enhancements

Potential improvements to consider:

1. **Multiple Frequency Patterns**: Add patterns using 8Hz, 16Hz, 32Hz, 64Hz, and 128Hz data files
2. **Statistical Analysis**: Add standard deviation, min/max error, and confidence intervals
3. **Visualization**: Generate plots comparing approximation vs ground truth over time
4. **Automated Cleanup**: Ensure MQTT topics are cleared between pattern runs
5. **Resource Monitoring**: Track CPU and memory usage during experiments
6. **Result Persistence**: Save results to CSV or JSON for later analysis

## Technical Details

### Environment Variables

- `HTTP_PORT`: Override default HTTP server port (8080) for orchestrators

### File Paths

- Data files: `src/streamer/data/frequency_variants/2mins/{wearable|smartphone}/{frequency}/data.nt`
- Publisher script: `dist/streamer/src/experiment-publisher.js`

### MQTT Topics

- Wearable data: `wearableX`
- Smartphone data: `smartphoneX`
- Approximation results: `output`
- Ground truth results: `client_operation_output`

## Known Issues

- Watermark warnings are non-critical and expected due to concurrent stream processing
- First window may not produce results due to insufficient data accumulation
- Port cleanup may require manual intervention if processes crash