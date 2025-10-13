# Frequency-Based Streaming Query Experiments

This directory contains a comprehensive experiment framework for comparing different streaming query aggregator approaches across multiple data frequencies.

## Overview

The experiment compares **4 approaches** across **6 frequencies** and **2 device types** with **5 iterations** each:

### Approaches Tested
1. **Fetching Client Side** (Ground Truth)
2. **Streaming Query Hive** 
3. **Chunked Approach**
4. **Approximation Approach**

### Frequencies Tested
- 4Hz (250ms intervals, ~480 observations)
- 8Hz (125ms intervals, ~960 observations)  
- 16Hz (62.5ms intervals, ~1,920 observations)
- 32Hz (31.2ms intervals, ~3,840 observations)
- 64Hz (15.6ms intervals, ~7,679 observations)
- 128Hz (7.8ms intervals, ~15,358 observations)

### Device Types
- **Smartphone** (Samsung SM-G950F acceleration X-axis)
- **Wearable** (E4.A03846.Accelerometer acceleration X-axis)

### Metrics Measured
- **Latency**: Query response time (ms)
- **Memory Usage**: Peak memory consumption (MB)
- **Accuracy**: Deviation from ground truth (%)
- **Throughput**: Observations processed per second

## Quick Start

### 1. Test MQTT Infrastructure
```bash
npm run experiment:test-mqtt
```

### 2. Validate Environment
```bash
npm run experiment:quick-test
```

### 3. Run Real-World Experiment (Recommended)
```bash
npm run experiment:run-realworld
```

### 4. Run Mock Experiment (Testing Only)
```bash
npm run experiment:run
```

### 5. Analyze Results
```bash
npm run experiment:analyze
```

## Detailed Usage

### Setup and Validation
```bash
# Test MQTT broker connectivity
npm run experiment:test-mqtt

# Validate project structure and data
npm run experiment:setup

# Quick test with single combination
npm run experiment:quick-test
```

### Running Experiments

#### Real-World Experiment Suite (Recommended)
```bash
# Uses actual MQTT streaming with real data publishers and processors
# Runs all approaches × frequencies × device types × iterations
# Total: 4 approaches × 6 frequencies × 2 devices × 5 iterations = 240 experiments
npm run experiment:run-realworld
```

#### Mock Experiment Suite (Testing/Development)
```bash
# Uses mock orchestrators for rapid testing
npm run experiment:run
```

#### Manual Execution
```bash
# Setup
npx ts-node tools/experiments/setup-frequency-experiment.ts

# Test MQTT infrastructure
npx ts-node tools/experiments/test-mqtt-infrastructure.ts

# Run real-world experiments  
npx ts-node tools/experiments/run-realworld-frequency-experiment.ts

# Analyze results
npx ts-node tools/experiments/analyze-frequency-results.ts
```

### Results Analysis
```bash
# Generate comprehensive analysis report
npm run experiment:analyze
```

## Experiment Configuration

The experiment configuration is automatically generated in:
```
tools/experiments/frequency-experiment-config.json
```

Key configuration parameters:
- **iterations**: 5 (configurable for future 35-iteration runs)
- **windowSize**: 1000 observations
- **query**: Average acceleration SPARQL query
- **dataBasePath**: `src/streamer/data/frequency_variants/2mins`

## Data Structure

The experiment uses 2-minute datasets organized by frequency:

```
src/streamer/data/frequency_variants/2mins/
├── smartphone/
│   ├── 4Hz/data.nt
│   ├── 8Hz/data.nt
│   ├── 16Hz/data.nt
│   ├── 32Hz/data.nt
│   ├── 64Hz/data.nt
│   └── 128Hz/data.nt
└── wearable/
    ├── 4Hz/data.nt
    ├── 8Hz/data.nt
    ├── 16Hz/data.nt
    ├── 32Hz/data.nt
    ├── 64Hz/data.nt
    └── 128Hz/data.nt
```

## Results Output

### Directory Structure
```
results/frequency-experiments/
├── detailed-results-{timestamp}.json
├── results-{timestamp}.csv
└── analysis/
    ├── analysis-report-{timestamp}.json
    └── summary-{timestamp}.txt
```

### Result Files

#### detailed-results-{timestamp}.json
Complete experiment data including:
- Configuration used
- Ground truth results
- All experiment results with metrics
- Summary statistics

#### results-{timestamp}.csv  
CSV format for easy analysis in Excel/R/Python:
```csv
approach,frequency,deviceType,iteration,timestamp,latency_ms,memory_mb,accuracy_percent,throughput_obs_sec,observations_processed,execution_time_ms,error
```

#### analysis-report-{timestamp}.json
Statistical analysis including:
- Overall performance statistics per approach
- Performance breakdown by frequency and device
- Best/worst performance comparisons
- Recommendations

#### summary-{timestamp}.txt
Human-readable summary with key findings and recommendations

## Expected Results

### Performance Expectations

**Latency by Frequency:**
- 4Hz: Lowest latency (~10-50ms expected)
- 128Hz: Highest latency (~100-500ms expected)

**Memory Usage:**
- Should scale with frequency and dataset size
- Chunked approach may show better memory efficiency

**Accuracy:**
- Fetching Client Side: 100% (ground truth)
- Other approaches: 85-99% expected

**Throughput:**
- Should scale inversely with latency
- Higher frequencies may show throughput limits

### Scalability Analysis

The experiment enables analysis of:
- **Frequency Impact**: How performance changes with data rate
- **Device Differences**: Smartphone vs. wearable processing characteristics  
- **Approach Comparison**: Which approach performs best at different scales
- **Resource Scaling**: Memory and CPU usage patterns

## Troubleshooting

### Common Issues

**"Experiment configuration not found"**
```bash
npm run experiment:setup
```

**"Data file not found"**
```bash
# Verify 2-minute datasets exist
ls -la src/streamer/data/frequency_variants/2mins/smartphone/4Hz/
```

**"TypeScript compilation failed"**
```bash
npm run build
```

**"Approach file not found"**
```bash
# Verify approach files exist
ls -la src/approaches/Streaming*
```

### Validation Commands

```bash
# Check data integrity
find src/streamer/data/frequency_variants/2mins -name "*.nt" | wc -l
# Should return 12 (6 frequencies × 2 devices)

# Verify observation counts
wc -l src/streamer/data/frequency_variants/2mins/smartphone/*/data.nt
wc -l src/streamer/data/frequency_variants/2mins/wearable/*/data.nt
```

## Future Enhancements

### Scaling to 35 Iterations
To run 35 iterations instead of 5:
1. Update `frequency-experiment-config.json`:
   ```json
   "iterations": 35
   ```
2. Total experiments: 4 × 6 × 2 × 35 = 1,680 experiments

### Additional Metrics
- **CPU Usage**: Process CPU consumption
- **Network I/O**: Data transfer measurements  
- **Query Complexity**: Multiple query types
- **Window Size Variations**: Different aggregation windows

### Extended Analysis
- **Statistical Significance**: Confidence intervals and p-values
- **Performance Regression**: Trend analysis over frequency ranges
- **Cost-Benefit Analysis**: Performance vs. accuracy trade-offs
- **Visualization**: Automated chart generation

## Example Workflow

```bash
# 1. Initial setup and validation
npm run experiment:quick-test

# 2. Run full experiment (takes 10-30 minutes)
npm run experiment:run

# 3. Analyze results
npm run experiment:analyze

# 4. Review results
cat results/frequency-experiments/analysis/summary-*.txt
```

This comprehensive framework provides rigorous performance comparison across all streaming query approaches with detailed statistical analysis and clear recommendations for production deployment.

- **frequency-comparison/**: Experiments comparing different frequency scenarios
- **rate-comparison/**: Experiments comparing different data rate scenarios  
- **pattern-analysis/**: Experiments analyzing various data patterns

## Running Experiments

Each subdirectory contains specific experiments that can be run independently to test different aspects of the streaming query system.
