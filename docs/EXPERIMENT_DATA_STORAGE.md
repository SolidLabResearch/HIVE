# Experiment Data Storage Guide

## Overview

This document explains where experiment data is stored during and after execution of streaming query experiments.

## Output Directory Structure

All experiment results are stored in the `results/` directory at the project root:

```
results/
└── frequency-experiments/
    ├── detailed-results-[timestamp].json        # Complete experiment data
    ├── results-all-[timestamp].csv              # All iterations including warmup/cooldown
    ├── results-valid-[timestamp].csv            # Only valid iterations (warmup/cooldown excluded)
    ├── analysis/                                # Analysis outputs (optional)
    ├── fetching-client-side/                    # Per-iteration data for this approach
    │   ├── 4Hz/
    │   │   ├── smartphone/
    │   │   │   ├── iteration_1/
    │   │   │   │   └── result.json
    │   │   │   ├── iteration_2/
    │   │   │   │   └── result.json
    │   │   │   └── ...
    │   │   └── wearable/
    │   │       └── ...
    │   └── ...
    ├── chunked-query-approach/                  # Per-iteration data for this approach
    │   └── ... (same structure)
    └── approximation-approach/                  # Per-iteration data for this approach
        └── ... (same structure)
```

## Configuration

Output path is configured in `scripts/benchmarks/frequency-experiment-config.json`:

```json
{
  "outputPath": "/Users/kushbisen/Code/streaming-query-hive/results/frequency-experiments"
}
```

You can change this path to store results elsewhere.

### Iteration Configuration

The number of iterations and warmup/cooldown settings are configured in the same file:

```json
{
  "experiment": {
    "iterations": 35,
    "warmupIterations": 3,
    "cooldownIterations": 2
  },
  "saveIterationsSeparately": true
}
```

**Settings**:
- `iterations`: Total number of iterations per experiment (35)
- `warmupIterations`: First N iterations discarded (3) - for system warmup
- `cooldownIterations`: Last N iterations discarded (2) - for system cooldown
- `saveIterationsSeparately`: Save each iteration in separate folder (true)

**Valid Iterations**: iterations - warmupIterations - cooldownIterations = 35 - 3 - 2 = 30

Only the 30 valid iterations (4-33) are used for analysis.

## Experiment Result Files

### 1. Detailed Results JSON

**File**: `detailed-results-[timestamp].json`

**Location**: `results/frequency-experiments/`

**Format**: JSON with complete experiment data

**Contents**:
```json
{
  "config": {
    "experiment": { ... },
    "frequencies": ["4Hz", "8Hz", ...],
    "deviceTypes": ["smartphone", "wearable"],
    "approaches": ["fetching-client-side", "chunked-query-approach", "approximation-approach"]
  },
  "groundTruth": {
    "4Hz-smartphone": { ... },
    "4Hz-wearable": { ... },
    ...
  },
  "warmupIterations": 3,
  "cooldownIterations": 2,
  "validIterations": 30,
  "allResults": [
    {
      "approach": "chunked-query-approach",
      "frequency": "4Hz",
      "deviceType": "smartphone",
      "iteration": 1,
      "isWarmup": true,
      "isCooldown": false,
      "timestamp": "2025-12-05T20:30:00.000Z",
      "metrics": { ... },
      "queryResult": { ... },
      "error": null
    },
    ...
  ],
  "validResults": [
    {
      "approach": "chunked-query-approach",
      "frequency": "4Hz",
      "deviceType": "smartphone",
      "iteration": 4,
      "isWarmup": false,
      "isCooldown": false,
      "timestamp": "2025-12-05T20:31:00.000Z",
      "metrics": {
        "latency": 125.5,
        "memoryUsage": 45.2,
        "accuracy": 99.8,
        "throughput": 850.3,
        "observationsProcessed": 480,
        "executionTime": 564.2
      },
      "queryResult": { ... },
      "error": null
    },
    ...
  ],
  "summary": {
    "totalIterations": 35,
    "warmupIterations": 3,
    "validIterations": 30,
    "cooldownIterations": 2,
    "totalExperiments": 1260,
    "warmupExperiments": 108,
    "validExperiments": 1080,
    "cooldownExperiments": 72,
    "validSuccessful": 1078,
    "validFailed": 2,
    "approaches": [...],
    "frequencies": [...],
    "deviceTypes": [...]
  }
}
```

### 2. Results CSV Files

Two CSV files are generated:

#### A. All Results CSV

**File**: `results-all-[timestamp].csv`

**Location**: `results/frequency-experiments/`

**Contains**: All iterations including warmup and cooldown

#### B. Valid Results CSV

**File**: `results-valid-[timestamp].csv`

**Location**: `results/frequency-experiments/`

**Contains**: Only valid iterations (excludes warmup and cooldown)

**Format**: CSV with headers

**Columns**:
- `approach` - Approach identifier
- `frequency` - Data frequency (4Hz, 8Hz, etc.)
- `deviceType` - Device type (smartphone, wearable)
- `iteration` - Iteration number (1-35)
- `is_warmup` - Boolean, true for warmup iterations (1-3)
- `is_cooldown` - Boolean, true for cooldown iterations (34-35)
- `timestamp` - ISO timestamp
- `latency_ms` - Latency in milliseconds
- `memory_mb` - Memory usage in MB
- `accuracy_percent` - Accuracy percentage vs ground truth
- `throughput_obs_sec` - Observations per second
- `observations_processed` - Total observations processed
- `execution_time_ms` - Total execution time
- `error` - Error message (if failed)

**Example (All Results)**:
```csv
approach,frequency,deviceType,iteration,is_warmup,is_cooldown,timestamp,latency_ms,memory_mb,accuracy_percent,throughput_obs_sec,observations_processed,execution_time_ms,error
chunked-query-approach,4Hz,smartphone,1,true,false,2025-12-05T20:30:00.000Z,125.50,45.20,99.80,850.30,480,564.20,
chunked-query-approach,4Hz,smartphone,2,true,false,2025-12-05T20:31:00.000Z,128.30,46.10,99.75,845.20,480,568.50,
chunked-query-approach,4Hz,smartphone,3,true,false,2025-12-05T20:32:00.000Z,127.80,45.80,99.82,848.10,480,566.30,
chunked-query-approach,4Hz,smartphone,4,false,false,2025-12-05T20:33:00.000Z,126.20,45.50,99.85,851.00,480,565.10,
...
chunked-query-approach,4Hz,smartphone,34,false,true,2025-12-05T20:50:00.000Z,129.10,46.20,99.70,843.50,480,570.20,
chunked-query-approach,4Hz,smartphone,35,false,true,2025-12-05T20:51:00.000Z,130.50,46.80,99.65,840.20,480,572.80,
```

**Example (Valid Results)**: Same format but only includes iterations 4-33 where `is_warmup=false` and `is_cooldown=false`.

## Resource Usage Logs

Each approach generates its own resource usage logs during execution:

### Fetching Client Side
**File**: `fetching_client_side_resource_usage.csv`
**Location**: Project root (where experiment is run)

### Chunked Query Approach
**File**: `chunked_query_approach_resource_log.csv`
**Location**: Project root (where experiment is run)

### Approximation Approach
**File**: `approximation_approach_resource_usage.csv`
**Location**: Project root (where experiment is run)

**Format** (all resource logs):
```csv
timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external
1701820800000,100.50,25.30,52428800,26214400,20971520,20.00,1048576
1701820800100,102.30,26.10,52428800,26214400,21000000,20.03,1048576
...
```

**Columns**:
- `timestamp` - Unix timestamp in milliseconds
- `cpu_user` - User CPU time in milliseconds
- `cpu_system` - System CPU time in milliseconds
- `rss` - Resident Set Size (total memory)
- `heapTotal` - Total heap allocated
- `heapUsed` - Heap memory used
- `heapUsedMB` - Heap used in megabytes
- `external` - External memory used

**Logging Interval**: Every 100ms

### 3. Per-Iteration Folders

When `saveIterationsSeparately` is set to `true` in the config, each iteration is saved in a separate folder:

**Structure**: `results/frequency-experiments/[approach]/[frequency]/[deviceType]/iteration_[N]/`

**File**: `result.json` - Complete result data for that iteration

**Example**:
```
results/frequency-experiments/
└── chunked-query-approach/
    └── 4Hz/
        └── smartphone/
            ├── iteration_1/
            │   └── result.json
            ├── iteration_2/
            │   └── result.json
            ├── iteration_3/
            │   └── result.json
            ├── iteration_4/
            │   └── result.json
            ...
            └── iteration_35/
                └── result.json
```

**result.json contents**:
```json
{
  "approach": "chunked-query-approach",
  "frequency": "4Hz",
  "deviceType": "smartphone",
  "iteration": 4,
  "isWarmup": false,
  "isCooldown": false,
  "timestamp": "2025-12-05T20:33:00.000Z",
  "metrics": {
    "latency": 126.2,
    "memoryUsage": 45.5,
    "accuracy": 99.85,
    "throughput": 851.0,
    "observationsProcessed": 480,
    "executionTime": 565.1
  },
  "queryResult": { ... },
  "error": null
}
```

This allows for detailed per-iteration analysis and debugging.

## Per-Iteration Data

For each iteration of the experiment, the following data is collected:

### Iteration Metrics

Each iteration produces one row in the results with:
- Execution time
- Memory usage (peak and average)
- CPU usage
- Query results
- Accuracy (compared to ground truth)
- Throughput
- Error status

### Example Flow

For a single iteration (e.g., chunked-query-approach, 4Hz, smartphone, iteration 1):

1. **Start**: Iteration begins, resource logging starts
2. **Execute**: Approach runs query processing
3. **Monitor**: CPU and memory logged every 100ms to resource log
4. **Complete**: Iteration finishes, metrics calculated
5. **Record**: Results saved to both JSON and CSV
6. **Next**: Move to next iteration

## Experiment Execution Summary

### Total Data Generated

For the current configuration:
- **Total Iterations**: 35 per combination
- **Warmup Iterations**: 3 (discarded)
- **Valid Iterations**: 30 (used for analysis)
- **Cooldown Iterations**: 2 (discarded)
- **Frequencies**: 6 (4Hz, 8Hz, 16Hz, 32Hz, 64Hz, 128Hz)
- **Device Types**: 2 (smartphone, wearable)
- **Approaches**: 3 (fetching-client-side, chunked-query-approach, approximation-approach)

**Total Experiments**: 3 × 6 × 2 × 35 = 1,260
**Valid Experiments** (for analysis): 3 × 6 × 2 × 30 = 1,080

### Files Created

After running all experiments:
- 1 detailed results JSON file (contains all 1,260 experiments)
- 1 all-results CSV file (1,260 rows + header)
- 1 valid-results CSV file (1,080 rows + header, warmup/cooldown excluded)
- 3 resource usage logs (1 per approach, continuous logging during execution)
- 1,260 individual iteration folders (if `saveIterationsSeparately` is true)
  - Each containing a `result.json` file

## Accessing Experiment Data

### View Results

**JSON format**:
```bash
cat results/frequency-experiments/detailed-results-*.json | jq
```

**All results CSV**:
```bash
cat results/frequency-experiments/results-all-*.csv
```

**Valid results only (recommended for analysis)**:
```bash
cat results/frequency-experiments/results-valid-*.csv
```

**Individual iteration**:
```bash
cat results/frequency-experiments/chunked-query-approach/4Hz/smartphone/iteration_4/result.json | jq
```

### Filter Specific Approach

```bash
# Get all chunked-query-approach valid results
grep "chunked-query-approach" results/frequency-experiments/results-valid-*.csv
```

### Filter Specific Frequency

```bash
# Get all 4Hz valid results
grep "4Hz" results/frequency-experiments/results-valid-*.csv
```

### Filter by Iteration Type

```bash
# Get only warmup iterations
awk -F',' '$5 == "true"' results/frequency-experiments/results-all-*.csv

# Get only valid iterations (exclude warmup and cooldown)
awk -F',' '$5 == "false" && $6 == "false"' results/frequency-experiments/results-all-*.csv

# Get only cooldown iterations
awk -F',' '$6 == "true"' results/frequency-experiments/results-all-*.csv
```

### Filter by Iteration Number

```bash
# Get specific iteration (e.g., iteration 10)
awk -F',' '$4 == 10' results/frequency-experiments/results-all-*.csv

# Get valid iterations only (4-33)
awk -F',' '$4 >= 4 && $4 <= 33' results/frequency-experiments/results-all-*.csv
```

## Analyzing Results

### Using Built-in Scripts

```bash
# Run analysis on latest results
npm run experiment:analyze
```

### Manual Analysis

**Load into Python**:
```python
import pandas as pd

# Load valid results only (recommended)
df = pd.read_csv('results/frequency-experiments/results-valid-2025-12-05T20-30-00-000Z.csv')

# Load all results (including warmup/cooldown)
df_all = pd.read_csv('results/frequency-experiments/results-all-2025-12-05T20-30-00-000Z.csv')

# Filter valid iterations from all results
df_valid = df_all[(df_all['is_warmup'] == False) & (df_all['is_cooldown'] == False)]

# Group by approach (using valid results)
by_approach = df.groupby('approach')['latency_ms'].mean()
print(by_approach)

# Compare frequencies
by_freq = df.groupby('frequency')['throughput_obs_sec'].mean()
print(by_freq)

# Analyze by iteration type
warmup = df_all[df_all['is_warmup'] == True]
valid = df_all[(df_all['is_warmup'] == False) & (df_all['is_cooldown'] == False)]
cooldown = df_all[df_all['is_cooldown'] == True]

print(f"Warmup mean latency: {warmup['latency_ms'].mean()}")
print(f"Valid mean latency: {valid['latency_ms'].mean()}")
print(f"Cooldown mean latency: {cooldown['latency_ms'].mean()}")
```

**Load into R**:
```r
# Load valid results only (recommended)
results <- read.csv('results/frequency-experiments/results-valid-2025-12-05T20-30-00-000Z.csv')

# Load all results
results_all <- read.csv('results/frequency-experiments/results-all-2025-12-05T20-30-00-000Z.csv')

# Filter valid iterations
results_valid <- subset(results_all, is_warmup == FALSE & is_cooldown == FALSE)

# Summary statistics
summary(results)

# Average latency by approach (using valid results)
aggregate(latency_ms ~ approach, data=results, mean)

# Compare iteration types
warmup <- subset(results_all, is_warmup == TRUE)
valid <- subset(results_all, is_warmup == FALSE & is_cooldown == FALSE)
cooldown <- subset(results_all, is_cooldown == TRUE)

cat("Warmup mean latency:", mean(warmup$latency_ms), "\n")
cat("Valid mean latency:", mean(valid$latency_ms), "\n")
cat("Cooldown mean latency:", mean(cooldown$latency_ms), "\n")
```

## Data Retention

### Automatic Cleanup

Results are NOT automatically cleaned up. Each experiment run creates new timestamped files.

### Manual Cleanup

To remove old results:
```bash
# Remove results older than 30 days
find results/frequency-experiments/ -name "*.json" -mtime +30 -delete
find results/frequency-experiments/ -name "*.csv" -mtime +30 -delete
```

### Archiving Results

To archive important results:
```bash
# Create archive
tar -czf experiment-results-$(date +%Y%m%d).tar.gz results/frequency-experiments/

# Move to archive directory
mkdir -p archives/
mv experiment-results-*.tar.gz archives/
```

## Troubleshooting

### Results Directory Missing

If `results/frequency-experiments/` doesn't exist:
```bash
npm run experiment:setup
```

This creates the directory structure.

### Permission Issues

Ensure write permissions:
```bash
chmod 755 results/
chmod 755 results/frequency-experiments/
```

### Disk Space

Check available disk space before running experiments:
```bash
df -h .
```

Large experiments can generate significant data, especially resource logs.

### Results Not Saved

If results aren't being saved, check:
1. Output path in `frequency-experiment-config.json` exists
2. Write permissions on output directory
3. Disk space available
4. No errors in experiment execution

## Best Practices

1. **Backup Results**: Copy important results before running new experiments
2. **Use Timestamps**: File names include timestamps to prevent overwrites
3. **Monitor Disk Space**: Resource logs can grow large with long-running experiments
4. **Archive Old Results**: Move completed experiments to archive directory
5. **Version Config**: Save a copy of config file with results for reproducibility

## Summary

- **Primary Output**: `results/frequency-experiments/`
- **Main Files**: 
  - Detailed JSON (all results)
  - All-results CSV (includes warmup/cooldown)
  - Valid-results CSV (warmup/cooldown excluded)
- **Per-Iteration Folders**: Individual result.json for each iteration (if enabled)
- **Resource Logs**: Project root, one per approach
- **Total Iterations**: 35 per experiment
  - Warmup: 3 (discarded)
  - Valid: 30 (used for analysis)
  - Cooldown: 2 (discarded)
- **Total Experiments**: 1,260 (3 approaches × 6 frequencies × 2 devices × 35 iterations)
- **Valid Experiments**: 1,080 (only valid iterations)
- **No Auto-Cleanup**: Manual management required
- **Analysis**: Use valid-results CSV for accurate metrics

All experiment data is preserved for reproducibility and analysis. Use the valid-results CSV or filter by `is_warmup=false` and `is_cooldown=false` for accurate performance analysis.