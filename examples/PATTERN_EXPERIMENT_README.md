# Pattern Accuracy Experiment - Complete Guide

## Overview

This experiment evaluates the accuracy of the Approximation Approach vs. the Fetching Client-Side (Ground Truth) approach across different stream patterns by calculating Mean Absolute Percentage Error (MAPE).

## What Was Created

### 1. Data Generator (`generate-pattern-data.ts`)

Generates synthetic sensor data in N-Quads RDF format for 7 different stream patterns:

- **Low Variability**: Stable values with minimal noise (±0.25 variation around -23.0)
- **Step Pattern**: Sharp step change from -23.0 to -15.0 at midpoint
- **Spike Pattern**: Sudden spike to -5.0 in the middle, otherwise stable at -23.0
- **Low Frequency Oscillation**: Slow sinusoidal wave (6 cycles over 2 minutes)
- **High Frequency Oscillation**: Fast sinusoidal wave (60 cycles over 2 minutes)
- **Gradual Drift**: Linear drift from -25.0 to -15.0 over time
- **Random Walk**: Random walk with bounded values between -35 and -10

**Parameters:**
- Duration: 120 seconds (2 minutes)
- Frequency: 4Hz
- Total observations per stream: 480
- Devices: Wearable and Smartphone sensors

### 2. Pattern Accuracy Experiment (`pattern-accuracy-experiment.ts`)

Runs the complete experiment across all 7 patterns:
- Launches both Approximation and Fetching orchestrators
- Publishes data using real RDF N-Quads format
- Collects results from both approaches
- Calculates MAPE for each pattern
- Generates summary table

### 3. Quick Test Version (`pattern-accuracy-experiment-quick.ts`)

Single-pattern test version for validation (runs Low Variability pattern only).

## Setup

### Prerequisites

1. **MQTT Broker Running**
   ```bash
   # Make sure MQTT broker is running on localhost:1883
   mosquitto -v
   ```

2. **Project Built**
   ```bash
   npm run build
   ```

3. **Clean Environment**
   ```bash
   # Kill any existing experiment processes
   pkill -f "pattern-accuracy-experiment"
   pkill -f "ApproximationApproachOrchestrator"
   pkill -f "FetchingClientSideApproachOrchestrator"
   pkill -f "experiment-publisher"
   
   # Clean up ports
   lsof -ti :8080,8081 | xargs kill -9 2>/dev/null
   ```

## Usage

### Step 1: Generate Pattern Data

```bash
cd /Users/kushbisen/Code/streaming-query-hive

# Generate all 7 patterns
npx ts-node examples/generate-pattern-data.ts
```

**Output:**
- Creates `src/streamer/data/pattern_experiments/` directory
- Generates wearable and smartphone data for each pattern
- Creates metadata.json for each pattern
- Total files: 21 (7 patterns × 3 files each)

**Verify Generation:**
```bash
# Check that patterns were created
ls -la src/streamer/data/pattern_experiments/

# View a sample
head -3 src/streamer/data/pattern_experiments/low_variability/wearable/data.nt

# Check metadata
cat src/streamer/data/pattern_experiments/step_pattern/metadata.json
```

### Step 2: Run the Experiment

**Option A: Full Experiment (All 7 Patterns)**

⚠️ **Total time: ~17-18 minutes** (2.5 minutes per pattern)

```bash
# Run in background and save output
nohup npx ts-node examples/pattern-accuracy-experiment.ts > pattern_results.log 2>&1 &

# Monitor progress
tail -f pattern_results.log

# Or use the monitoring script
./examples/monitor-experiment.sh
```

**Option B: Quick Test (Single Pattern)**

⏱️ **Total time: ~2.5 minutes**

```bash
npx ts-node examples/pattern-accuracy-experiment-quick.ts
```

### Step 3: View Results

```bash
# Wait for completion message
grep "Experiment Summary" pattern_results.log

# View final results
tail -20 pattern_results.log
```

## Expected Output

```
--- Experiment Summary ---
Pattern                         | Approx | Ground | MAPE (%)
--------------------------------|--------|--------|----------
Low Variability                 | 4      | 4      | 0.05
Step Pattern                    | 4      | 4      | 2.34
Spike Pattern                   | 4      | 4      | 15.67
Low Frequency Oscillation       | 4      | 4      | 3.21
High Frequency Oscillation      | 4      | 4      | 8.92
Gradual Drift                   | 4      | 4      | 1.89
Random Walk                     | 4      | 4      | 4.56
```

**Note:** MAPE values shown above are illustrative. Actual values will vary based on:
- Window configurations (60s range, 30s step for sub-queries; 120s range, 60s step for main)
- Approximation algorithm behavior
- Timing of window evaluations

## Understanding Results

### MAPE (Mean Absolute Percentage Error)

```
MAPE = (1/n) × Σ |Ground_Truth - Approximation| / |Ground_Truth| × 100%
```

**Interpretation:**
- **< 5%**: Excellent approximation accuracy
- **5-10%**: Good approximation accuracy  
- **10-20%**: Moderate approximation accuracy
- **> 20%**: Poor approximation accuracy

### Result Counts

The "Approx" and "Ground" columns show how many result windows were collected:
- Expected: 2-4 results per pattern (based on 60s slide window)
- Low counts may indicate:
  - Data not arriving in time for window evaluation
  - Query processing delays
  - Window timing issues

### Pattern-Specific Expectations

1. **Low Variability**: Should have lowest MAPE (stable data)
2. **Step Pattern**: Moderate MAPE (sudden change may be captured differently)
3. **Spike Pattern**: Higher MAPE (short spike may be missed or averaged differently)
4. **Oscillations**: MAPE depends on sampling alignment with wave phase
5. **Gradual Drift**: Low MAPE (smooth change approximates well)
6. **Random Walk**: Variable MAPE (depends on walk trajectory)

## Troubleshooting

### No Results (MAPE shows N/A)

**Causes:**
- Orchestrators not fully initialized
- MQTT messages not being received
- Window timing issues

**Solutions:**
```bash
# Increase initialization time in the experiment file
ORCHESTRATOR_INIT_TIME_MS = 15000  # from 10000

# Check MQTT broker
mosquitto_sub -t "output" -v
mosquitto_sub -t "client_operation_output" -v
```

### Port Conflicts

**Error:** `EADDRINUSE: address already in use :::8080`

**Solution:**
```bash
# Kill processes on ports
lsof -ti :8080,8081 | xargs kill -9

# Verify ports are free
lsof -i :8080
lsof -i :8081
```

### Process Hangs

```bash
# Kill all related processes
pkill -9 -f "pattern-accuracy-experiment"
pkill -9 -f "ts-node"
pkill -9 -f "experiment-publisher"

# Clean up MQTT subscriptions
# Restart MQTT broker if needed
```

### Data Files Missing

```bash
# Regenerate pattern data
npx ts-node examples/generate-pattern-data.ts

# Verify files exist
ls -R src/streamer/data/pattern_experiments/
```

## Architecture

### Data Flow

```
Pattern Generator
    ↓
N-Quads Files (.nt)
    ↓
experiment-publisher.js (StreamToMQTT)
    ↓
MQTT Topics (wearableX, smartphoneX)
    ↓
    ├─→ ApproximationApproachOrchestrator (port 8081)
    │       ↓
    │   Sub-queries (60s windows)
    │       ↓
    │   Main query (120s window)
    │       ↓
    │   MQTT: "output" topic
    │
    └─→ FetchingClientSideApproachOrchestrator (port 8080)
            ↓
        Full query (120s window)
            ↓
        MQTT: "client_operation_output" topic
            ↓
        Experiment Collector
            ↓
        MAPE Calculation
```

### File Structure

```
streaming-query-hive/
├── examples/
│   ├── generate-pattern-data.ts          # Data generator
│   ├── pattern-accuracy-experiment.ts     # Full experiment
│   ├── pattern-accuracy-experiment-quick.ts # Quick test
│   ├── monitor-experiment.sh              # Progress monitor
│   ├── PATTERN_EXPERIMENT_README.md       # This file
│   └── PATTERN_EXPERIMENT_FIXES.md        # Technical fixes doc
│
└── src/streamer/data/pattern_experiments/
    ├── low_variability/
    │   ├── wearable/data.nt
    │   ├── smartphone/data.nt
    │   └── metadata.json
    ├── step_pattern/
    ├── spike_pattern/
    ├── low_frequency_oscillation/
    ├── high_frequency_oscillation/
    ├── gradual_drift/
    └── random_walk/
```

## Customization

### Add New Patterns

Edit `generate-pattern-data.ts`:

```typescript
const PATTERNS: PatternConfig[] = [
  // ... existing patterns ...
  {
    name: 'my_custom_pattern',
    description: 'My custom pattern description',
    generator: (i: number, total: number) => {
      // Your pattern logic here
      return someValue;
    }
  }
];
```

Then update `pattern-accuracy-experiment.ts`:

```typescript
const streamPatterns: StreamPattern[] = [
  // ... existing patterns ...
  {
    name: "My Custom Pattern",
    wearablePath: `${DATA_BASE_PATH}/my_custom_pattern/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/my_custom_pattern/smartphone/data.nt`,
  },
];
```

### Adjust Timing

In experiment file:

```typescript
const EXPERIMENT_DURATION_S = 120;        // Data streaming duration
const ORCHESTRATOR_INIT_TIME_MS = 10000;  // Orchestrator startup wait
const POST_PROCESSING_TIME_MS = 20000;    // Result collection wait
```

### Change Frequency

In data generator:

```typescript
const FREQUENCY_HZ = 4;  // Change to 8, 16, etc.
```

## Known Issues

1. **Watermark Warnings**: Non-critical, filtered out in output
2. **First Window**: May not produce results due to insufficient data
3. **Port Cleanup**: May require manual intervention after crashes
4. **MQTT Persistence**: Old messages may interfere; restart broker if needed

## References

- **Original Data Format**: `src/streamer/data/frequency_variants/2mins/`
- **Publisher Implementation**: `src/streamer/src/experiment-publisher.ts`
- **Orchestrator Code**: `src/approaches/` directory
- **Benchmark Reference**: `scripts/benchmarks/experiment-evaluation-independent-stream-processing.ts`

## Summary

This experiment framework provides:
- ✅ Automated generation of diverse stream patterns
- ✅ Proper RDF N-Quads format matching repository standards
- ✅ Side-by-side comparison of approximation vs. ground truth
- ✅ Quantitative accuracy metrics (MAPE)
- ✅ Support for multiple pattern types
- ✅ Clean architecture using existing infrastructure

Total execution time: **~20 minutes** for complete 7-pattern experiment.