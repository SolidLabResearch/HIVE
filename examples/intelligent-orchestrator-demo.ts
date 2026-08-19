/**
 * Example: Intelligent Streaming Query Hive with Scout Bee Integration
 * 
 * This example demonstrates how to use the HiveScoutBee for intelligent
 * approach selection based on stream analysis.
 */

import { IntelligentOrchestrator } from "../src/orchestrator/IntelligentOrchestrator";
import { HiveScoutBeeWrapper } from "../src/services/HiveScoutBee";

async function demonstrateIntelligentOrchestrator() {
    console.log("=== Intelligent Streaming Query Hive Demo ===\n");

    // 1. Create orchestrator with stream analysis enabled
    const orchestrator = new IntelligentOrchestrator("default", true);

    // 2. Add sub-queries
    orchestrator.addSubQuery(`
        REGISTER RStream <sensor1> AS
        SELECT (AVG(?value) AS ?avg_value)
        FROM NAMED WINDOW :win1 ON :sensor1 [RANGE PT5S STEP PT1S]
        WHERE {
            ?obs saref:hasValue ?value .
        }
    `);

    orchestrator.addSubQuery(`
        REGISTER RStream <sensor2> AS  
        SELECT (MAX(?value) AS ?max_value)
        FROM NAMED WINDOW :win2 ON :sensor2 [RANGE PT10S STEP PT2S]
        WHERE {
            ?obs saref:hasValue ?value .
        }
    `);

    // 3. Register output query
    orchestrator.registerOutputQuery(`
        REGISTER RStream <sensor_averages> AS
        SELECT (MAX(?value) AS ?result)
        FROM NAMED WINDOW :win ON :smartphoneX [RANGE PT60S STEP PT30S]
        WHERE {
            ?obs saref:hasValue ?value .
        }
    `);

    // 4. Simulate stream data analysis
    console.log("Simulating stream data for analysis...\n");
    
    // Simulate different stream patterns
    await simulateStreamPattern(orchestrator, "stable", 100);
    await simulateStreamPattern(orchestrator, "volatile", 200);
    await simulateStreamPattern(orchestrator, "periodic", 150);

    // 5. Get recommendations and run queries
    console.log("Getting approach recommendations...\n");
    
    const recommendation = await orchestrator.getApproachRecommendation(6);
    if (recommendation) {
        console.log("Recommendation received - running with intelligent selection");
        await orchestrator.runRegisteredQueryIntelligent();
    } else {
        console.log("No recommendation available - using default approach");
        orchestrator.runRegisteredQuery();
    }

    // 6. Show analysis summary
    console.log("\nAnalysis Summary:");
    console.log(JSON.stringify(orchestrator.getAnalysisSummary(), null, 2));
}

async function simulateStreamPattern(
    orchestrator: IntelligentOrchestrator, 
    pattern: string, 
    baseValue: number
) {
    console.log(`🌊 Simulating ${pattern} stream pattern...`);
    
    const now = Date.now();
    const points = 50;
    
    for (let i = 0; i < points; i++) {
        const timestamp = now + (i * 100); // 100ms intervals
        let value = baseValue;
        
        switch (pattern) {
            case "stable":
                value = baseValue + (Math.random() - 0.5) * 2; // Low volatility
                break;
            case "volatile":
                value = baseValue + (Math.random() - 0.5) * 50; // High volatility
                break;
            case "periodic":
                value = baseValue + Math.sin(i * 0.3) * 20; // Periodic oscillation
                break;
        }
        
        orchestrator.analyzeStreamData(timestamp, value, "smartphoneX");
        
        // Small delay to simulate real-time data
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    
    console.log(`${pattern} pattern simulation complete\n`);
}

async function demonstrateDirectScoutBeeUsage() {
    console.log("=== Direct Hive Scout Bee Usage Demo ===\n");

    const scoutBee = new HiveScoutBeeWrapper();
    
    // Simulate stream data
    const streamData: Array<{timestamp: number, value: number}> = [];
    const baseTime = Date.now();
    
    for (let i = 0; i < 100; i++) {
        streamData.push({
            timestamp: baseTime + (i * 250), // 4Hz sampling
            value: 75 + Math.sin(i * 0.2) * 5 + (Math.random() - 0.5) * 2
        });
    }
    
    // Add data to the scout bee buffer
    console.log("Adding stream data to HiveScoutBee...");
    streamData.forEach((dataPoint, index) => {
        scoutBee.addDataPoint(dataPoint.timestamp, dataPoint.value, `sensor_${index % 3}`);
    });
    
    // Extract signature
    console.log("Extracting stream signature...");
    const signature = scoutBee.getStreamSignature();
    
    if (!signature) {
        console.log("Failed to extract signature from stream");
        return;
    }
    
    console.log("Stream Signature extracted successfully");
    console.log("   Signature details:", JSON.stringify(signature, null, 2));
    
    // Get approach recommendation
    console.log("\nGetting approach recommendation...");
    const recommendation = scoutBee.getApproachRecommendation();
    
    if (!recommendation) {
        console.log("Failed to get approach recommendation");
        return;
    }
    
    console.log(`Recommended Approach: ${recommendation.recommendedApproach.toUpperCase()}`);
    console.log(`Confidence: ${(recommendation.confidence * 100).toFixed(1)}%`);
    console.log(`Key Reasons:`);
    recommendation.reasoning.forEach(reason => {
        console.log(`   • ${reason}`);
    });
}

// Run demonstrations
async function runDemo() {
    try {
        await demonstrateDirectScoutBeeUsage();
        console.log("\n" + "=".repeat(60) + "\n");
        await demonstrateIntelligentOrchestrator();
    } catch (error) {
        console.error("Demo error:", error);
    }
}

// Export for use
export { demonstrateIntelligentOrchestrator, demonstrateDirectScoutBeeUsage };

// Run if called directly
if (require.main === module) {
    runDemo();
}
