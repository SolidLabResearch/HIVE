# Scripts Directory Organization

This directory contains all scripts for running experiments, benchmarks, analysis, and setup tasks for the Streaming Query Hive project.

## Directory Structure

```
scripts/
├── setup/              # Infrastructure and environment setup scripts
├── benchmarks/         # Performance benchmarks and experiment runners
├── analysis/           # Data analysis and result processing tools
├── legacy/             # Deprecated/old experiments (archived for reference)
└── tools/              # Utility tools and helper scripts
```

## Setup Scripts (`scripts/setup/`)

Scripts for setting up the experimental environment and infrastructure.

- **`setup-frequency-experiment.ts`** - TypeScript setup script for frequency experiments
- **`setup-frequency-experiment.sh`** - Shell script for frequency experiment infrastructure
- **`test-mqtt-infrastructure.ts`** - Tests the MQTT infrastructure setup

### Usage

```bash
# Setup frequency experiment environment
npm run experiment:setup

# Test MQTT infrastructure
npm run experiment:test-mqtt
```

## Benchmarks (`scripts/benchmarks/`)

Performance benchmarks and experimental evaluations.

### Main Benchmarks

- **`run-frequency-experiment.ts`** - Main frequency experiment runner
- **`run-realworld-frequency-experiment.ts`** - Real-world scenario frequency experiments
- **`run-5-iterations.ts`** - 5-iteration multi-approach test (quick validation)
- **`run-35-iterations.ts`** - 35-iteration multi-approach test (comprehensive)
- **`run-patterns-approx-vs-fetching-5.ts`** - 5-iteration pattern test (quick)
- **`run-patterns-approx-vs-fetching.ts`** - 35-iteration pattern test (comprehensive)
- **`run-4hz-comparison-35-iterations.js`** - 4Hz frequency comparison benchmark
- **`run-4hz-noise-experiments.js`** - Noise tolerance experiments at 4Hz
- **`experiment-evaluation-independent-stream-processing.ts`** - Independent stream processing evaluation
- **`quick-test.ts`** - Quick validation tests

### Rate Comparison Benchmarks

Located in `benchmarks/rate-comparison/`:
- **`run-all-rate-tests.js`** - Runs all rate comparison tests
- **`run-exponential-rate-tests.js`** - Exponential rate tests
- **`run-exponential-rate-tests-fetching.js`** - Exponential rate tests for fetching approach
- **`run-rate-comparison-experiments.js`** - Rate comparison experiment runner

### Frequency Comparison Benchmarks

Located in `benchmarks/frequency-comparison/`:
- **`run-frequency-comparison-experiments.js`** - Frequency comparison experiment runner
- **`experiment-frequency-comparison-approximation.js`** - Approximation approach frequency comparison
- **`experiment-frequency-comparison-fetching.js`** - Fetching approach frequency comparison

### Scalability Tests

Located in `benchmarks/scalability/`:
- Tests for system scalability under various loads

### Usage

```bash
# Run main frequency experiment
npm run experiment:run

# Run real-world frequency experiment
npm run experiment:run-realworld

# Multi-approach verification tests
npm run experiment:5-iterations      # Quick (5 runs, ~15-20 min)
npm run experiment:35-iterations     # Comprehensive (35 runs, ~1.5-2 hours)

# Pattern-based approximation tests
npm run experiment:patterns-approx-vs-fetching-5   # Quick (5 iterations/pattern)
npm run experiment:patterns-approx-vs-fetching     # Comprehensive (35 iterations/pattern)

# Run 4Hz comparison
npm run experiment:4hz-comparison

# Run 4Hz noise experiments
npm run experiment:4hz-noise

# Quick test
npm run experiment:quick-test

# Independent stream processing
npm run experiment:independent
```

## Utility Scripts

### Cleanup

- **`cleanup-logs.ts`** - Removes all log files and experimental results
- **`cleanup-logs.sh`** - Shell version of cleanup script

Cleans up:
- CSV log files (orchestrator logs, resource usage)
- Results directories
- Unified log directories
- Publisher/replayer logs
- Temporary experiment files

### Usage

```bash
# Clean all logs and results for a fresh start
npm run clean-logs
```

**When to use:**
- Before starting new experiments
- When troubleshooting issues
- When switching between different experiment types
- To free up disk space

## Analysis Scripts (`scripts/analysis/`)

Scripts for analyzing experimental results and generating insights.

- **`analyze-frequency-results.ts`** - Analyzes frequency experiment results
- **`analyze-frequency-experiment-results.js`** - Alternative frequency analysis script
- **`experiment-complex-oscillation-comparison.js`** - Complex oscillation pattern analysis

### Pattern Analysis

Located in `analysis/pattern-analysis/`:
- **`analyze-pattern-results.js`** - Pattern result analysis
- **`compare_pattern_results.js`** - Pattern comparison tool
- **`detailed_pattern_analysis.js`** - Detailed pattern analysis
- Various test scripts for pattern evaluation

### Usage

```bash
# Analyze frequency experiment results
npm run experiment:analyze
```

## Legacy Scripts (`scripts/legacy/`)

This directory contains deprecated or superseded experiments that are kept for historical reference and reproducibility of older results.

**Note:** These scripts may not work with the current codebase and are not maintained. They are preserved for:
- Historical context
- Research reproducibility
- Understanding evolution of the project

Contents include:
- Old experiment runners (`experiment-evaluation-*.js`)
- Deprecated debug scripts
- Superseded benchmark implementations
- Archived comparison tools

## Tools (`scripts/tools/`)

Utility scripts and helper tools organized by function:
- `tools/analysis/` - Analysis utilities
- `tools/data-generation/` - Test data generation tools
- `tools/evaluation/` - Evaluation helpers

## Configuration Files

- **`benchmarks/frequency-experiment-config.json`** - Configuration for frequency experiments

## Best Practices

### Running Experiments

1. Always run setup scripts before experiments:
   ```bash
   npm run experiment:setup
   npm run experiment:test-mqtt
   ```

2. Use TypeScript scripts for new experiments (better type safety)

3. Store results in appropriate directories (typically `logs/` or experiment-specific output directories)

### Adding New Scripts

1. Place scripts in the appropriate directory:
   - Setup/infrastructure → `setup/`
   - Performance tests → `benchmarks/`
   - Result processing → `analysis/`

2. Update this README with script documentation

3. Add npm scripts to `package.json` if the script will be run frequently

4. Use TypeScript for new scripts when possible

### Deprecating Scripts

When a script becomes obsolete:

1. Move it to `legacy/` with a clear comment about why it was deprecated
2. Update this README to remove it from active documentation
3. Remove any associated npm scripts from `package.json`

## Migration Notes

This directory structure was reorganized from the previous `scripts/experiments/` structure to improve organization and clarity. The mapping is:

- `scripts/experiments/setup-*.{ts,sh}` → `scripts/setup/`
- `scripts/experiments/run-*.{ts,js}` → `scripts/benchmarks/`
- `scripts/experiments/analyze-*.{ts,js}` → `scripts/analysis/`
- Old/deprecated experiments → `scripts/legacy/`

All npm scripts in `package.json` have been updated to reflect the new paths.

## Getting Started

To run a complete experiment workflow:

```bash
# 0. Clean previous results (recommended for fresh start)
npm run clean-logs

# 1. Setup the environment
npm run experiment:setup

# 2. Test infrastructure
npm run experiment:test-mqtt

# 3. Run quick validation (choose one)
npm run experiment:5-iterations                   # Quick multi-approach test
npm run experiment:patterns-approx-vs-fetching-5  # Quick pattern test

# 4. Run comprehensive experiments (choose based on needs)
npm run experiment:35-iterations                  # Full multi-approach test
npm run experiment:patterns-approx-vs-fetching    # Full pattern test
npm run experiment:run-realworld                  # Real-world frequency test

# 5. Analyze results
npm run experiment:analyze
```

**Tip:** Always start with 5-iteration experiments for quick validation before running the comprehensive 35-iteration tests.

## Troubleshooting

If scripts fail to run:

1. Clean previous logs: `npm run clean-logs`
2. Ensure all dependencies are installed: `npm install`
3. Build the TypeScript project: `npm run build`
4. Check that MQTT infrastructure is running
5. Verify configuration files are present
6. Check logs for specific error messages

## Contributing

When adding new scripts:
- Follow the existing naming conventions
- Add appropriate documentation
- Update package.json scripts if needed
- Keep legacy code in the legacy folder