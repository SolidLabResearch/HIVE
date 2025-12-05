# Approach Orchestrators Documentation

This document describes the architecture and usage of approach orchestrators in the Streaming Query Hive project.

## Overview

All streaming query approaches have been standardized as class-based orchestrators that can be:
- Imported and used programmatically by experiment runners
- Executed as standalone scripts for manual testing
- Integrated into the benchmark framework

## Available Approaches

### 1. Independent Stream Processing Approach
- **File**: `src/approaches/IndependentStreamProcessingApproach.ts`
- **Name**: `independent-stream-processing`
- **Type**: Module-based class (active production approach)
- **Description**: Creates independent processors that work like client-side fetching but for subqueries and superqueries independently

### 2. Streaming Query Chunked Approach
- **File**: `src/approaches/StreamingQueryChunkedApproachOrchestrator.ts`
- **Name**: `chunked-approach`
- **Type**: Class-based orchestrator
- **Description**: Uses chunk-based aggregation to process streaming queries
- **Operator**: `StreamingQueryChunkAggregatorOperator`

### 3. Streaming Query Approximation Approach
- **File**: `src/approaches/StreamingQueryApproximationApproachOrchestrator.ts`
- **Name**: `approximation-approach`
- **Type**: Class-based orchestrator
- **Description**: Uses approximation techniques to process streaming queries
- **Operator**: `ApproximationApproachOperator`

### 4. Streaming Query Fetching Client Side Approach
- **File**: `src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts`
- **Name**: `fetching-client-side`
- **Type**: Class-based orchestrator
- **Description**: Fetches all data on the client side for processing (used as ground truth)
- **Operator**: N/A (direct client-side processing)

## Orchestrator Interface

All approach orchestrators implement a consistent interface:

```typescript
interface ApproachOrchestrator {
  /**
   * Get the name/identifier of this approach
   * @returns The approach name (e.g., "chunked-approach")
   */
  getName(): string;

  /**
   * Run the experiment with specified data and configuration
   * @param dataPath - Path to the experiment data file
   * @param config - Experiment configuration object
   * @returns Promise resolving to experiment results
   */
  runExperiment(dataPath: string, config: any): Promise<any>;

  /**
   * Clean up resources (optional but recommended)
   */
  cleanup(): void;
}
```

## Usage

### 1. Programmatic Usage (in Experiments)

```typescript
import StreamingQueryChunkedApproachOrchestrator from 
  "../src/approaches/StreamingQueryChunkedApproachOrchestrator";

// Create instance
const orchestrator = new StreamingQueryChunkedApproachOrchestrator();

// Get approach name
console.log(orchestrator.getName()); // "chunked-approach"

// Run experiment
const result = await orchestrator.runExperiment("/path/to/data.nt", {
  query: "...",
  windowSize: 1000
});

// Clean up when done
orchestrator.cleanup();
```

### 2. Standalone Execution

Each orchestrator can be run directly as a standalone script:

```bash
# Run chunked approach standalone
npx ts-node src/approaches/StreamingQueryChunkedApproachOrchestrator.ts

# Run approximation approach standalone
npx ts-node src/approaches/StreamingQueryApproximationApproachOrchestrator.ts

# Run fetching client side approach standalone
npx ts-node src/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.ts
```

When run standalone:
- Each approach sets up its own queries
- Starts resource usage logging
- Runs for a defined timeout (default 2 minutes)
- Automatically cleans up and exits

### 3. Integration with Experiment Runner

The experiment runner (`scripts/benchmarks/run-frequency-experiment.ts`) automatically loads and uses orchestrators:

```typescript
// Orchestrator mapping (in run-frequency-experiment.ts)
const orchestratorMap = {
  "fetching-client-side": 
    "../approaches/StreamingQueryFetchingClientSideApproachOrchestrator",
  "streaming-query-hive": 
    "../approaches/StreamingQueryHiveApproachOrchestrator",
  "chunked-approach": 
    "../approaches/StreamingQueryChunkedApproachOrchestrator",
  "approximation-approach": 
    "../approaches/StreamingQueryApproximationApproachOrchestrator",
};
```

To run experiments with all approaches:

```bash
# Run full experiment suite
npm run experiment:run

# Run with specific configuration
npx ts-node scripts/benchmarks/run-frequency-experiment.ts
```

## Architecture Details

### Class Structure

Each orchestrator follows this structure:

```typescript
export class StreamingQueryXApproachOrchestrator {
  private logger: CSVLogger;
  private orchestrator: Orchestrator;
  private resourceLogStream?: fs.WriteStream;
  private resourceLogInterval?: ReturnType<typeof setInterval>;

  constructor() {
    // Initialize logger and orchestrator
  }

  public getName(): string {
    // Return approach identifier
  }

  public async runExperiment(dataPath: string, config: any): Promise<any> {
    // 1. Setup subqueries
    // 2. Register main query
    // 3. Start resource logging
    // 4. Run query
    // 5. Return results
  }

  private async setupSubQueries(): Promise<void> {
    // Define and add subqueries to orchestrator
  }

  private async registerMainQuery(): Promise<void> {
    // Define and register the main aggregation query
  }

  private startResourceUsageLogging(filePath?: string, intervalMs?: number): void {
    // Start CPU and memory monitoring
  }

  public cleanup(): void {
    // Stop logging intervals and close streams
  }
}

// Standalone execution function
async function runStandaloneXApproach() {
  const orchestrator = new StreamingQueryXApproachOrchestrator();
  // ... run with timeout and cleanup
}

// Execute if run directly
if (require.main === module) {
  runStandaloneXApproach();
}

// Export for programmatic use
export default StreamingQueryXApproachOrchestrator;
```

### Resource Logging

Each orchestrator logs resource usage to CSV files:

- **Chunked**: `streaming_query_hive_resource_log.csv`
- **Approximation**: `approximation_approach_resource_usage.csv`
- **Fetching Client Side**: `fetching_client_side_resource_usage.csv`

Format:
```csv
timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external
1234567890,100.50,25.30,52428800,26214400,20971520,20.00,1048576
```

### Query Structure

All approaches use similar SPARQL queries with variations:

**Subqueries** (process individual streams):
```sparql
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> 
  ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
```

**Main Query** (aggregates results from multiple streams):
```sparql
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> 
  ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> 
  ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
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

## Testing

### Verify All Approaches

Run the test script to verify all orchestrators are properly configured:

```bash
npx ts-node scripts/test-all-approaches.ts
```

This checks:
- Module can be imported
- Default export exists
- Instance can be created
- Required methods (getName, runExperiment) exist
- cleanup method exists
- getName returns correct identifier

### Quick Experiment Test

Run a quick experiment with one approach:

```bash
# Test independent approach
npm run experiment:independent -- --frequency 4Hz --iterations 1

# Test with experiment runner
npm run experiment:run
```

## Troubleshooting

### Common Issues

**Issue**: `Unknown approach: X`
- **Solution**: Check that the approach name in the config matches the orchestrator map in `run-frequency-experiment.ts`

**Issue**: `Failed to load orchestrator for X`
- **Solution**: Verify the orchestrator file path is correct and the file exports a default class

**Issue**: `Missing getName() method`
- **Solution**: Ensure the orchestrator class implements the required interface

**Issue**: `SPARQL parsing error: Unknown prefix`
- **Solution**: Check that all required prefixes are declared in the SPARQL query (especially `saref`, `dahccsensors`)

### Debugging

Enable verbose logging:
```typescript
// In orchestrator
console.log(`[ApproachName] Debug info:`, data);
```

Check resource logs:
```bash
# View resource usage
tail -f approximation_approach_resource_usage.csv
```

Check experiment results:
```bash
# View experiment output
ls -la results/frequency-experiments/
cat results/frequency-experiments/detailed-results-*.json
```

## Migration Notes

### Before (Old Structure)
- Approaches were standalone async functions
- Immediately executed when imported
- Could not be used programmatically
- No standard interface

### After (New Structure)
- Approaches are classes with standard interface
- Can be imported without execution
- Support both programmatic and standalone usage
- Implement consistent `runExperiment()` and `getName()` methods

### Breaking Changes
None - backward compatibility maintained through standalone execution mode.

## Future Extensions

To add a new approach:

1. Create a new orchestrator file in `src/approaches/`
2. Implement the standard interface
3. Add standalone execution wrapper
4. Export default class
5. Update orchestrator map in `run-frequency-experiment.ts`
6. Update `frequency-experiment-config.json` to include the new approach
7. Test with `scripts/test-all-approaches.ts`

Example template:
```typescript
import { Orchestrator } from "../orchestrator/Orchestrator";
import fs from "fs";
import { CSVLogger } from "../util/logger/CSVLogger";

export class NewApproachOrchestrator {
  private logger: CSVLogger;
  private orchestrator: Orchestrator;

  constructor() {
    this.logger = new CSVLogger("new_approach_log.csv");
    this.orchestrator = new Orchestrator("NewApproachOperator");
  }

  public getName(): string {
    return "new-approach";
  }

  public async runExperiment(_dataPath: string, _config: any): Promise<any> {
    // Implementation
  }

  public cleanup(): void {
    // Cleanup
  }
}

async function runStandaloneNewApproach() {
  const orchestrator = new NewApproachOrchestrator();
  // ...
}

if (require.main === module) {
  runStandaloneNewApproach();
}

export default NewApproachOrchestrator;
```

## References

- Main documentation: `docs/ARCHITECTURE.md`
- Refactoring summary: `REFACTORING_SUMMARY.md`
- Operator mapping: `src/config/approach-operator-mapping.ts`
- Worker factory: `src/services/WorkerFactory.ts`
- Experiment setup: `scripts/setup/setup-frequency-experiment.ts`
