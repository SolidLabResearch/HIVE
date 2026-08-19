# Hive Scout Bee Integration

The **Hive Scout Bee** is an intelligent approach selector that analyzes stream characteristics to recommend the optimal query processing approach.

## Architecture Integration

### **Integration Points:**

1. **Primary: Enhanced Orchestrator** (`IntelligentOrchestrator`)
   - Integrates stream analysis into query execution flow
   - Automatically selects optimal approach based on real-time analysis
   - Provides backward compatibility with existing code

2. **Direct Usage: Standalone Scout Bee** (`HiveScoutBee`)
   - Can be used independently for stream analysis
   - Provides detailed signatures and recommendations
   - Useful for debugging and fine-tuning

### **Data Flow:**

```
Stream Data → Scout Bee → Signature Extraction → Approach Prediction → Orchestrator → Selected Approach
      ↓              ↓              ↓                    ↓                ↓              ↓
   Analysis     Pattern         Stream            Confidence         Decision      Execution
   Window      Detection      Signature           Scoring           Making        
```

## Key Features

### **Stream Signature Extraction:**
- **Statistical Analysis**: Mean, variance, min/max, standard deviation
- **Temporal Analysis**: Data rate, interval variance, regularity patterns
- **Pattern Detection**: Frequency analysis, trend detection, seasonality
- **Classification**: Stream type, complexity scoring, predictability

### **Intelligent Approach Selection:**
- **Approximation Approach**: Best for stable, low-volatility streams
- **Fetching Approach**: Optimal for volatile, irregular streams  
- **Chunked Approach**: Ideal for high-frequency, regular streams
- **Hive Approach**: Suited for mixed/complex stream patterns

### **Confidence Scoring:**
- Provides confidence levels (0-1) for recommendations
- Explains reasoning behind each recommendation
- Tracks historical performance

## Usage Examples

### **1. Enhanced Orchestrator (Recommended)**

```typescript
import { IntelligentOrchestrator } from "./src/orchestrator/IntelligentOrchestrator";

// Create orchestrator with stream analysis enabled
const orchestrator = new IntelligentOrchestrator("default", true);

// Add queries
orchestrator.addSubQuery("SELECT (AVG(?value) AS ?avg) ...");
orchestrator.registerOutputQuery("SELECT (MAX(?value) AS ?result) ...");

// Stream data gets automatically analyzed
orchestrator.analyzeStreamData(timestamp, value, topic);

// Intelligent execution (automatically selects best approach)
await orchestrator.runRegisteredQueryIntelligent();
```

### **2. Direct Scout Bee Usage**

```typescript
import { HiveScoutBee } from "./src/services/HiveScoutBee";

const scoutBee = new HiveScoutBee();

// Extract signature from stream data
const signature = scoutBee.extractSignature(streamData, windowSize);

// Get approach recommendation
const recommendation = scoutBee.predictApproach(signature, queryComplexity);

console.log(`Recommended: ${recommendation.recommendedApproach}`);
console.log(`Confidence: ${recommendation.confidence * 100}%`);
```

### **3. Integration with Existing Code**

```typescript
// Minimal changes to existing orchestrator usage
const orchestrator = new IntelligentOrchestrator("chunked-approach", false); // Disabled analysis = backward compatible

// OR enable analysis for intelligent selection
const smartOrchestrator = new IntelligentOrchestrator("default", true);
```

## Stream Analysis Metrics

### **Stream Signature Components:**

1. **Statistics**: Basic statistical measures
2. **Temporal**: Time-based characteristics 
3. **Patterns**: Frequency, trends, oscillations
4. **Classification**: Type, complexity, risks

### **Approach Scoring Factors:**

- **Approximation**: Stability, low volatility, regular intervals
- **Fetching**: High volatility, low frequency, complex queries
- **Chunked**: High frequency, large data volume, regular patterns  
- **Hive**: Mixed characteristics, medium complexity

## Configuration Options

### **Scout Bee Parameters:**
```typescript
const scoutBee = new HiveScoutBee(
    maxWindowSize: 1000,      // Maximum analysis window size
    signatureUpdateInterval: 5000  // How often to update signatures (ms)
);
```

### **Orchestrator Options:**
```typescript
const orchestrator = new IntelligentOrchestrator(
    defaultOperatorType: "chunked-approach",  // Fallback approach
    enableStreamAnalysis: true                // Enable/disable intelligence
);
```

## Benefits

1. **Automated Optimization**: No manual approach selection needed
2. **Performance Improvement**: Optimal approach for each stream pattern
3. **Deep Insights**: Detailed stream analysis and reasoning
4. **Adaptive**: Continuously learns from stream patterns
5. **Real-time**: Low-latency decision making
6. **🛡️ Backward Compatible**: Works with existing code

## 📁 File Locations

- **Core Implementation**: `src/services/HiveScoutBee.ts`
- **Enhanced Orchestrator**: `src/orchestrator/IntelligentOrchestrator.ts`
- **Type Definitions**: `src/util/Types.ts`
- **Usage Examples**: `examples/intelligent-orchestrator-demo.ts`

## Getting Started

1. **Import the enhanced orchestrator**:
   ```typescript
   import { IntelligentOrchestrator } from "./src/orchestrator/IntelligentOrchestrator";
   ```

2. **Create instance with analysis enabled**:
   ```typescript
   const orchestrator = new IntelligentOrchestrator("default", true);
   ```

3. **Use as normal orchestrator** - intelligence is automatic!

The Hive Scout Bee seamlessly integrates with your existing streaming query architecture while providing intelligent, data-driven approach selection.
