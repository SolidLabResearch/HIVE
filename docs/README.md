# Documentation Index

## Overview

This directory contains comprehensive documentation for the Streaming Query Hive project.

## Core Documentation

### Architecture & Design
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture, approach-operator mapping, factory patterns, and extension guide
- **[APPROACH_ORCHESTRATORS.md](APPROACH_ORCHESTRATORS.md)** - Detailed documentation of all approach orchestrators, usage examples, and troubleshooting
- **[APPROACH_COMPARISON.md](APPROACH_COMPARISON.md)** - Comparison of different streaming query approaches, performance characteristics, and use cases

### Setup & Configuration
- **[SETUP_GUIDE.md](SETUP_GUIDE.md)** - Complete setup guide for approach orchestrators, configuration, and verification steps
- **[EXPERIMENT_DATA_STORAGE.md](EXPERIMENT_DATA_STORAGE.md)** - Where experiment data is stored, file formats, and how to access results

### Performance & Analysis
- **[PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md)** - Performance benchmarks, resource usage analysis, and optimization recommendations

### Integration Guides
- **[HIVE_SCOUT_BEE_INTEGRATION.md](HIVE_SCOUT_BEE_INTEGRATION.md)** - Integration details for Hive Scout Bee library
- **[MODE_SWITCHING_SUMMARY.md](MODE_SWITCHING_SUMMARY.md)** - Mode switching implementation and usage

### Historical Analysis
- **[FIRST_RESULT_ANALYSIS_REPORT.md](FIRST_RESULT_ANALYSIS_REPORT.md)** - Initial experimental results and findings

## Quick Links

### Getting Started
1. Read [SETUP_GUIDE.md](SETUP_GUIDE.md) for initial configuration
2. Review [APPROACH_COMPARISON.md](APPROACH_COMPARISON.md) to understand different approaches
3. Check [ARCHITECTURE.md](ARCHITECTURE.md) for system design details

### Running Experiments
1. Configure approaches in `scripts/benchmarks/frequency-experiment-config.json`
2. Run setup: `npm run experiment:setup`
3. Execute experiments: `npm run experiment:run`
4. Analyze results: `npm run experiment:analyze`

### Development
1. See [ARCHITECTURE.md](ARCHITECTURE.md) for extending the system
2. Review [APPROACH_ORCHESTRATORS.md](APPROACH_ORCHESTRATORS.md) for creating new approaches
3. Check [PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md) for optimization guidelines

## Approach Overview

The project implements three distinct streaming query processing approaches:

### 1. Fetching Client Side
- **Identifier**: `fetching-client-side`
- **File**: `FetchingClientSideApproachOrchestrator.ts`
- **Purpose**: Ground truth baseline with centralized processing
- **Use Case**: Accuracy comparison and baseline metrics

### 2. Chunked Query Approach
- **Identifier**: `chunked-query-approach`
- **File**: `ChunkedQueryApproachOrchestrator.ts`
- **Purpose**: Scalable distributed chunk-based aggregation
- **Use Case**: Production streaming query processing

### 3. Approximation Approach
- **Identifier**: `approximation-approach`
- **File**: `ApproximationApproachOrchestrator.ts`
- **Purpose**: Fast approximate processing
- **Use Case**: Real-time dashboards where approximation is acceptable

## Project Structure

```
streaming-query-hive/
├── src/
│   ├── approaches/          # Approach orchestrators
│   ├── orchestrator/        # Core orchestration logic
│   ├── services/            # Worker factory and services
│   ├── operators/           # Stream processing operators
│   ├── config/              # Configuration and mappings
│   └── util/                # Utilities and helpers
├── scripts/
│   ├── benchmarks/          # Benchmark and experiment scripts
│   ├── setup/               # Setup and configuration scripts
│   ├── analysis/            # Analysis and visualization
│   └── tools/               # Development tools
├── docs/                    # Documentation (this directory)
└── results/                 # Experiment results output

```

## Key Concepts

### Approach vs Operator
- **Approach**: High-level orchestrator that manages query decomposition and coordination
- **Operator**: Low-level worker that executes specific processing logic
- **Mapping**: Each approach uses a specific operator (configured in `src/config/approach-operator-mapping.ts`)

### Orchestration Pattern
1. Orchestrator receives streaming query
2. Decomposes into subqueries
3. Distributes to workers (operators)
4. Aggregates results
5. Publishes final output

### Resource Logging
Each approach logs CPU and memory usage:
- `fetching_client_side_resource_usage.csv`
- `chunked_query_approach_resource_log.csv`
- `approximation_approach_resource_usage.csv`

## Common Tasks

### Adding a New Approach
1. Create orchestrator in `src/approaches/[Name]ApproachOrchestrator.ts`
2. Implement interface: `getName()`, `runExperiment()`, `cleanup()`
3. Add to `frequency-experiment-config.json`
4. Update orchestrator map in `run-frequency-experiment.ts`
5. Test with `npx ts-node scripts/test-all-approaches.ts`

### Running Specific Approach
```bash
# Standalone execution
npx ts-node src/approaches/ChunkedQueryApproachOrchestrator.ts

# Via experiment runner
npm run experiment:run
```

### Analyzing Results
```bash
# View experiment results
cat results/frequency-experiments/results-*.csv

# Analyze with provided scripts
npm run experiment:analyze
```

See [EXPERIMENT_DATA_STORAGE.md](EXPERIMENT_DATA_STORAGE.md) for detailed information on result file formats and locations.

## Support

For issues or questions:
1. Check relevant documentation files above
2. Review [ARCHITECTURE.md](ARCHITECTURE.md) for system design
3. See [APPROACH_ORCHESTRATORS.md](APPROACH_ORCHESTRATORS.md) for approach-specific details
4. Consult [SETUP_GUIDE.md](SETUP_GUIDE.md) for configuration help

## Contributing

When adding documentation:
1. Place in appropriate category
2. Update this README.md index
3. Use clear, descriptive titles
4. Include code examples where relevant
5. Keep formatting consistent