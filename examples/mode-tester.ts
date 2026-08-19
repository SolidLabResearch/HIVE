#!/usr/bin/env node

import { IntelligentOrchestrator } from "../src/orchestrator/IntelligentOrchestrator";
import { HiveScoutBeeWrapper } from "../src/services/HiveScoutBee";

/**
 * Simple CLI utility for testing approach selection modes
 * Usage: npx ts-node examples/mode-tester.ts [mode] [approach]
 */

async function main() {
    const args = process.argv.slice(2);
    const mode = args[0] as 'automatic' | 'manual' | 'hybrid' || 'automatic';
    const manualApproach = args[1] as 'approximation-approach' | 'fetching-client-side' | 'chunked-approach' | 'streaming-query-hive';
    const pattern = args[2] as 'stable' | 'volatile' | 'periodic' | 'mixed' || 'mixed';

    console.log("=== Mode Tester ===");
    console.log(`Mode: ${mode}`);
    console.log(`Manual Approach: ${manualApproach || 'None'}`);
    console.log(`Stream Pattern: ${pattern}`);
    console.log("========================\n");

    // Test HiveScoutBeeWrapper directly
    console.log("Testing HiveScoutBeeWrapper directly:");
    const scoutBee = new HiveScoutBeeWrapper();
    
    // Simulate stream data
    scoutBee.simulateStreamPattern(pattern, 50);
    
    // Get recommendation
    const recommendation = scoutBee.getApproachRecommendation();
    if (recommendation) {
        console.log(`   Recommended: ${recommendation.recommendedApproach}`);
        console.log(`   Confidence: ${(recommendation.confidence * 100).toFixed(1)}%`);
        console.log(`   Reasoning: ${recommendation.reasoning.join(', ')}`);
    } else {
        console.log("   No recommendation available");
    }
    
    // Get buffer stats
    const stats = scoutBee.getBufferStats();
    console.log(`   Data Points: ${stats.bufferSize}`);
    console.log(`   Variance: ${stats.valueRange.variance}`);
    
    console.log("\n" + "=".repeat(50));
    
    // Test IntelligentOrchestrator modes
    console.log("Testing IntelligentOrchestrator:");
    const orchestrator = new IntelligentOrchestrator("default", true);
    
    // Set mode
    orchestrator.setAnalysisMode(mode);
    if (manualApproach) {
        orchestrator.setManualApproach(manualApproach);
    }
    
    // Add stream data
    for (let i = 0; i < 50; i++) {
        const timestamp = Date.now() + i * 1000;
        let value: number;
        
        switch (pattern) {
            case 'stable':
                value = 50 + Math.sin(i * 0.1) * 2 + (Math.random() - 0.5) * 0.5;
                break;
            case 'volatile':
                value = 50 + (Math.random() - 0.5) * 100 + Math.sin(i * 0.5) * 30;
                break;
            case 'periodic':
                value = 50 + Math.sin(i * 0.3) * 20 + Math.cos(i * 0.1) * 10;
                break;
            case 'mixed':
                value = 50 + Math.sin(i * 0.2) * 15 + (Math.random() - 0.5) * 10 + Math.cos(i * 0.05) * 5;
                break;
            default:
                value = 50;
        }
        
        orchestrator.analyzeStreamData(timestamp, value, `sensor_${i % 3}`);
    }
    
    // Get analysis summary
    const summary = orchestrator.getAnalysisSummary();
    console.log(`   Mode: ${summary.analysisMode}`);
    console.log(`   Stream Analysis: ${summary.streamAnalysisEnabled ? 'Enabled' : 'Disabled'}`);
    console.log(`   Manual Approach: ${summary.manualApproach || 'None'}`);
    
    if (summary.warning) {
        console.log(`   Warning: ${summary.warning}`);
    }
    
    // Register and run a query
    const sampleQuery = `
        REGISTER RStream <test_output> AS
        SELECT (AVG(?value) AS ?avg_value)
        FROM NAMED WINDOW :win ON :sensor1 [RANGE PT10S STEP PT5S]
        WHERE {
            ?obs saref:hasValue ?value .
        }
    `;
    
    orchestrator.registerOutputQuery(sampleQuery);
    
    console.log("\nWould execute query with the following logic:");
    
    switch (mode) {
        case 'automatic':
            console.log("   → Uses stream signature analysis to select approach");
            break;
        case 'manual':
            console.log(`   → Uses manually specified approach: ${manualApproach || 'default'}`);
            break;
        case 'hybrid':
            console.log("   → Tries automatic analysis first, falls back to manual if confidence is low");
            break;
    }
    
    console.log("\nTest completed!");
}

function showUsage() {
    console.log(`
Usage: npx ts-node examples/mode-tester.ts [mode] [manual-approach] [pattern]

Modes:
  automatic  - Use stream signature analysis (default)
  manual     - Use specified approach
  hybrid     - Automatic with manual fallback

Manual Approaches:
  approximation-approach  - For stable, predictable streams
  fetching-client-side   - For high-variance, complex streams
  chunked-approach       - For high-volume, regular streams
  streaming-query-hive   - For mixed characteristics

Stream Patterns:
  stable     - Low variance, predictable (default)
  volatile   - High variance, unpredictable
  periodic   - Regular oscillations
  mixed      - Combination of patterns (default)

Examples:
  npx ts-node examples/mode-tester.ts automatic
  npx ts-node examples/mode-tester.ts manual approximation-approach stable
  npx ts-node examples/mode-tester.ts hybrid fetching-client-side volatile
`);
}

if (require.main === module) {
    const args = process.argv.slice(2);
    if (args.includes('--help') || args.includes('-h')) {
        showUsage();
    } else {
        main().catch(console.error);
    }
}
