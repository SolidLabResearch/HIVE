# Streaming Query Hive - Architecture Documentation

## Overview

Streaming Query Hive is a distributed streaming query processing system that combines multiple streaming queries to provide actionable insights. The system uses a worker-orchestrator pattern where orchestrators coordinate query execution strategies while workers handle the actual stream processing.

## Table of Contents

1. [Architectural Principles](#architectural-principles)
2. [Core Components](#core-components)
3. [Approach-Operator Mapping](#approach-operator-mapping)
4. [Worker Factory Pattern](#worker-factory-pattern)
5. [Type Safety](#type-safety)
6. [Directory Structure](#directory-structure)

---

## Architectural Principles

### Separation of Concerns

The architecture separates orchestration logic from execution logic:

- **Orchestrators** (in `src/approaches/`) - Coordinate query processing strategies, manage subquery discovery, and spawn workers
- **Operators** (in `src/services/operators/`) - Execute actual stream processing, aggregation, and query evaluation

### Explicit Mapping

The system enforces an explicit mapping between orchestrator approaches and worker operators to prevent runtime errors where an orchestrator selects a strategy that workers cannot execute.

### Type Safety

All components use TypeScript enums and interfaces to ensure compile-time type checking and prevent invalid configurations.

---

## Core Components

### 1. Orchestrator Approaches (`src/approaches/`)

Approaches define the high-level strategy for processing streaming queries.

#### Available Approaches:

- **StreamingQueryChunkedApproach** - Processes queries in chunks with aggregation
- **ApproximationApproach** - Uses rate-based approximation for streaming queries
- **FetchingClientSideApproach** - Client-side fetching with aggregation
- **IndependentStreamProcessing** - Independent processing of multiple streams

#### Example:

```typescript
// src/approaches/StreamingQueryChunkedApproachOrchestrator.ts
export class StreamingQueryChunkedApproachOrchestrator {
  getApproachType(): ApproachType {
    return ApproachType.STREAMING_QUERY_CHUNKED;
  }
  
  getOperatorType(): OperatorType {
    return getOperatorForApproach(ApproachType.STREAMING_QUERY_CHUNKED);
  }
}
```

### 2. Worker Operators (`src/services/operators/`)

Operators implement the actual stream processing logic.

#### Available Operators:

- **StreamingQueryChunkAggregatorOperator** - Aggregates chunked query results
- **RateBasedApproximationApproachOperator** - Implements rate-based approximation
- **NaiveApproximationApproachOperator** - Simple approximation implementation

#### Operator Interface:

```typescript
export interface IStreamQueryOperator {
  addSubQuery(query: string): void;
  addOutputQuery(query: string): void;
  getSubQueries(): string[];
  init(): Promise<void>;
  handleAggregation(): Promise<void>;
}
```

### 3. BeeWorker (`src/services/BeeWorker.ts`)

The worker process that instantiates and runs operators based on orchestrator instructions.

**Key Features:**
- Uses WorkerFactory to instantiate operators
- Runs as a child process forked by HiveQueryBee
- Receives configuration via environment variables
- Handles containment checking and query combination

### 4. HiveQueryBee (`src/services/HiveQueryBee.ts`)

Manages worker processes and communicates with them.

**Responsibilities:**
- Forks BeeWorker processes
- Passes query and operator configuration
- Manages worker lifecycle

---

## Approach-Operator Mapping

### Configuration (`src/config/approach-operator-mapping.ts`)

The mapping configuration explicitly defines which operator should be used for each approach:

```typescript
export const APPROACH_OPERATOR_MAPPINGS: ApproachOperatorMapping[] = [
  {
    approach: ApproachType.STREAMING_QUERY_CHUNKED,
    operator: OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR,
    description: "Chunked query processing with aggregation on worker side",
  },
  {
    approach: ApproachType.APPROXIMATION,
    operator: OperatorType.RATE_BASED_APPROXIMATION,
    description: "Rate-based approximation approach for streaming queries",
  },
  // ... more mappings
];
```

### Helper Functions

#### `getOperatorForApproach(approach: ApproachType): OperatorType`

Returns the operator type for a given approach. Throws an error if no mapping exists.

**Usage:**
```typescript
const operatorType = getOperatorForApproach(ApproachType.STREAMING_QUERY_CHUNKED);
// Returns: OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR
```

#### `getApproachesForOperator(operator: OperatorType): ApproachType[]`

Returns all approaches that use a given operator.

**Usage:**
```typescript
const approaches = getApproachesForOperator(OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR);
// Returns: [ApproachType.STREAMING_QUERY_CHUNKED, ApproachType.FETCHING_CLIENT_SIDE, ...]
```

#### `isValidApproachOperatorCombination(approach, operator): boolean`

Validates if an approach-operator combination is valid.

**Usage:**
```typescript
const isValid = isValidApproachOperatorCombination(
  ApproachType.STREAMING_QUERY_CHUNKED,
  OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR
);
// Returns: true
```

### Benefits

1. **Compile-time Safety** - TypeScript enums prevent typos and invalid values
2. **Runtime Validation** - Functions throw clear errors for invalid combinations
3. **Single Source of Truth** - All mappings defined in one place
4. **Documentation** - Each mapping includes a description
5. **Extensibility** - Easy to add new approaches/operators

---

## Worker Factory Pattern

### Overview (`src/services/WorkerFactory.ts`)

The WorkerFactory centralizes operator instantiation and provides type-safe creation methods.

### Methods

#### `createOperator(operatorType: OperatorType): IStreamQueryOperator`

Creates an operator from an enum value.

```typescript
const operator = WorkerFactory.createOperator(
  OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR
);
```

#### `createOperatorFromString(operatorTypeString: string): IStreamQueryOperator`

Creates an operator from a string (useful for environment variables).

```typescript
const operator = WorkerFactory.createOperatorFromString(
  "StreamingQueryChunkAggregatorOperator"
);
```

Includes legacy compatibility mappings for old string names.

#### `isValidOperatorType(operatorTypeString: string): boolean`

Checks if a string represents a valid operator type.

```typescript
if (WorkerFactory.isValidOperatorType("StreamingQueryChunkAggregatorOperator")) {
  // Valid operator type
}
```

#### `getAvailableOperatorTypes(): OperatorType[]`

Returns all supported operator types.

```typescript
const types = WorkerFactory.getAvailableOperatorTypes();
// Returns: [OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR, ...]
```

### Benefits

1. **Centralized Creation Logic** - All operator instantiation in one place
2. **Type Safety** - Uses enums instead of strings where possible
3. **Legacy Support** - Maintains backward compatibility with old string names
4. **Clear Error Messages** - Helpful errors when invalid types are provided
5. **Testability** - Easy to mock and test operator creation

---

## Type Safety

### Enums (`src/util/Interfaces.ts`)

```typescript
// Approach types used by orchestrators
export enum ApproachType {
  STREAMING_QUERY_CHUNKED = "StreamingQueryChunked",
  APPROXIMATION = "Approximation",
  FETCHING_CLIENT_SIDE = "FetchingClientSide",
  INDEPENDENT_STREAM_PROCESSING = "IndependentStreamProcessing",
}

// Operator types used by workers
export enum OperatorType {
  STREAMING_QUERY_CHUNK_AGGREGATOR = "StreamingQueryChunkAggregatorOperator",
  RATE_BASED_APPROXIMATION = "RateBasedApproximationApproachOperator",
  NAIVE_APPROXIMATION = "NaiveApproximationApproachOperator",
}
```

### Interfaces

```typescript
// Mapping configuration
export interface ApproachOperatorMapping {
  approach: ApproachType;
  operator: OperatorType;
  description: string;
}

// Operator interface
export interface IStreamQueryOperator {
  addSubQuery(query: string): void;
  addOutputQuery(query: string): void;
  getSubQueries(): string[];
  init(): Promise<void>;
  handleAggregation(): Promise<void>;
}

// Orchestrator interface
export interface IOrchestrationApproach {
  getApproachType(): ApproachType;
  getOperatorType(): OperatorType;
  execute(): Promise<void>;
}
```

---

## Directory Structure

```
streaming-query-hive/
├── src/
│   ├── approaches/              # Orchestrator approaches
│   │   ├── StreamingQueryChunkedApproachOrchestrator.ts
│   │   ├── StreamingQueryApproximationApproachOrchestrator.ts
│   │   ├── StreamingQueryFetchingClientSideApproachOrchestrator.ts
│   │   └── IndependentStreamProcessingApproach.ts
│   │
│   ├── services/
│   │   ├── operators/           # Worker operators
│   │   │   ├── StreamingQueryChunkAggregatorOperator.ts
│   │   │   ├── RateBasedApproximationApproachOperator.ts
│   │   │   ├── NaiveApproximationApproachOperator.ts
│   │   │   ├── r2r.ts          # R2R operator (Relation-to-Relation)
│   │   │   └── s2r.ts          # S2R operator (Stream-to-Relation)
│   │   │
│   │   ├── BeeWorker.ts        # Worker process entry point
│   │   ├── HiveQueryBee.ts     # Worker process manager
│   │   ├── WorkerFactory.ts    # Operator factory
│   │   └── BeeKeeper.ts        # Worker orchestration
│   │
│   ├── config/
│   │   ├── approach-operator-mapping.ts  # Approach-Operator mappings
│   │   └── log_config.json
│   │
│   └── util/
│       ├── Interfaces.ts       # Type definitions and interfaces
│       └── Types.ts            # Type aliases
│
├── scripts/
│   ├── setup/                  # Setup scripts
│   ├── benchmarks/             # Performance benchmarks
│   ├── analysis/               # Result analysis tools
│   ├── legacy/                 # Deprecated experiments
│   └── tools/                  # Utility tools
│
├── tests/                      # Test files
├── dist/                       # Compiled output
└── docs/                       # Documentation
```

---

## Data Flow

### 1. Query Registration

```
User/Client
    ↓
Orchestrator (selects Approach)
    ↓
HiveQueryBee (creates worker process)
    ↓
BeeWorker (uses WorkerFactory to create Operator)
    ↓
Operator (processes streams)
```

### 2. Approach Selection Flow

```
1. Orchestrator determines ApproachType
2. getOperatorForApproach(ApproachType) → OperatorType
3. HiveQueryBee spawns BeeWorker with OperatorType
4. BeeWorker uses WorkerFactory.createOperatorFromString(OperatorType)
5. Operator instance is created and initialized
```

### 3. Query Processing Flow

```
1. BeeWorker receives query and subqueries
2. Operator.addOutputQuery(mainQuery)
3. For each subquery: Operator.addSubQuery(subQuery)
4. Operator.init() - Initialize operator
5. Operator.handleAggregation() - Process streams
6. Results published to MQTT topics
```

---

## Extension Guide

### Adding a New Approach

1. Create a new approach class in `src/approaches/`:
   ```typescript
   export class NewApproachOrchestrator implements IOrchestrationApproach {
     getApproachType(): ApproachType {
       return ApproachType.NEW_APPROACH;
     }
     
     getOperatorType(): OperatorType {
       return getOperatorForApproach(ApproachType.NEW_APPROACH);
     }
     
     async execute(): Promise<void> {
       // Implementation
     }
   }
   ```

2. Add to `ApproachType` enum in `src/util/Interfaces.ts`:
   ```typescript
   export enum ApproachType {
     // ... existing types
     NEW_APPROACH = "NewApproach",
   }
   ```

3. Add mapping in `src/config/approach-operator-mapping.ts`:
   ```typescript
   {
     approach: ApproachType.NEW_APPROACH,
     operator: OperatorType.SOME_OPERATOR,
     description: "Description of new approach",
   }
   ```

### Adding a New Operator

1. Create operator class in `src/services/operators/`:
   ```typescript
   export class NewOperator implements IStreamQueryOperator {
     // Implement interface methods
   }
   ```

2. Add to `OperatorType` enum in `src/util/Interfaces.ts`:
   ```typescript
   export enum OperatorType {
     // ... existing types
     NEW_OPERATOR = "NewOperator",
   }
   ```

3. Add to `WorkerFactory.createOperator()`:
   ```typescript
   case OperatorType.NEW_OPERATOR:
     return new NewOperator();
   ```

4. Update mappings in `src/config/approach-operator-mapping.ts`

---

## Best Practices

### 1. Always Use Enums

[FAIL] **Bad:**
```typescript
const operator = "StreamingQueryChunkAggregatorOperator";
```

[DONE] **Good:**
```typescript
const operator = OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR;
```

### 2. Validate Mappings

[FAIL] **Bad:**
```typescript
const operator = new StreamingQueryChunkAggregatorOperator();
```

[DONE] **Good:**
```typescript
const operatorType = getOperatorForApproach(approach);
const operator = WorkerFactory.createOperator(operatorType);
```

### 3. Use Factory Pattern

[FAIL] **Bad:**
```typescript
if (type === "StreamingQueryChunkAggregatorOperator") {
  operator = new StreamingQueryChunkAggregatorOperator();
} else if (type === "ApproximationApproachOperator") {
  operator = new ApproximationApproachOperator();
}
```

[DONE] **Good:**
```typescript
const operator = WorkerFactory.createOperatorFromString(type);
```

### 4. Document Mappings

Always include descriptions in mapping configurations to explain the purpose and use case of each approach-operator combination.

---

## Troubleshooting

### Error: "Unsupported operator type"

**Cause:** Trying to create an operator with an invalid or unknown type.

**Solution:** 
- Check that the operator type exists in `OperatorType` enum
- Verify the operator is added to `WorkerFactory.createOperator()`
- Use `WorkerFactory.getAvailableOperatorTypes()` to see valid types

### Error: "No operator mapping found for approach"

**Cause:** Trying to get an operator for an approach that has no mapping.

**Solution:**
- Add mapping to `APPROACH_OPERATOR_MAPPINGS` in `approach-operator-mapping.ts`
- Verify the approach type is spelled correctly

### Worker Process Fails to Start

**Cause:** Environment variables not set or BeeWorker cannot instantiate operator.

**Solution:**
- Ensure `OPERATOR_TYPE`, `QUERY`, and `TOPIC` environment variables are set
- Check that operator type string matches expected values
- Review BeeWorker logs for specific error messages

---

## Migration from Legacy Code

The codebase has been refactored from implicit string-based operator selection to an explicit, type-safe approach. Legacy code compatibility is maintained through:

1. **WorkerFactory legacy mappings** - Old operator string names are mapped to new enum values
2. **Gradual migration** - Both old and new patterns work during transition
3. **Clear deprecation path** - Legacy code is marked and can be safely removed

When updating code:
- Replace string literals with enum values
- Use WorkerFactory instead of manual instantiation
- Verify approach-operator mappings are correct