import { IntelligentOrchestrator } from "../src/orchestrator/IntelligentOrchestrator";

/**
 * Demo showing how to switch between different analysis modes
 * in the Intelligent Orchestrator
 */
async function demonstrateAnalysisModes() {
    console.log("=== Intelligent Orchestrator Analysis Modes Demo ===\n");

    // Create orchestrator with stream analysis enabled
    const orchestrator = new IntelligentOrchestrator("default", true);

    // Simulate adding some stream data for analysis
    console.log("Adding stream data for analysis...");
    for (let i = 0; i < 50; i++) {
        const timestamp = Date.now() + i * 1000;
        const value = Math.sin(i * 0.1) * 10 + Math.random() * 5 + 50;
        orchestrator.analyzeStreamData(timestamp, value, `sensor_${i % 3}`);
    }

    // Register a sample query
    const sampleQuery = `
        REGISTER RStream <sensor_averages> AS
        SELECT (AVG(?value) AS ?avg_value)
        FROM NAMED WINDOW :win ON :smartphoneX [RANGE PT30S STEP PT10S]
        WHERE {
            ?obs saref:hasValue ?value .
        }
    `;
    
    orchestrator.registerOutputQuery(sampleQuery);

    console.log("\n" + "=".repeat(60));
    console.log("MODE 1: AUTOMATIC ANALYSIS");
    console.log("=".repeat(60));
    
    // Mode 1: Automatic stream signature analysis
    orchestrator.setAnalysisMode('automatic');
    
    console.log("Current settings:");
    console.log(`   Analysis Mode: ${orchestrator.getAnalysisMode()}`);
    console.log(`   Manual Approach: ${orchestrator.getManualApproach() || 'None'}`);
    
    // Get recommendation
    const autoRecommendation = await orchestrator.getApproachRecommendation();
    if (autoRecommendation) {
        console.log("\nAutomatic Recommendation:");
        console.log(`   Approach: ${autoRecommendation.recommendedApproach}`);
        console.log(`   Confidence: ${(autoRecommendation.confidence * 100).toFixed(1)}%`);
        console.log(`   Reasoning: ${autoRecommendation.reasoning.join(', ')}`);
    }

    console.log("\n" + "=".repeat(60));
    console.log("MODE 2: MANUAL APPROACH SPECIFICATION");
    console.log("=".repeat(60));
    
    // Mode 2: Manual approach specification
    orchestrator.setAnalysisMode('manual');
    orchestrator.setManualApproach('approximation-approach');
    
    console.log("Current settings:");
    console.log(`   Analysis Mode: ${orchestrator.getAnalysisMode()}`);
    console.log(`   Manual Approach: ${orchestrator.getManualApproach()}`);
    
    // Simulate running query with manual approach
    console.log("\nWith manual mode, the orchestrator will use the specified approach regardless of stream characteristics");

    console.log("\n" + "=".repeat(60));
    console.log("MODE 3: HYBRID MODE");
    console.log("=".repeat(60));
    
    // Mode 3: Hybrid mode (automatic with manual fallback)
    orchestrator.setAnalysisMode('hybrid');
    orchestrator.setManualApproach('fetching-client-side');
    
    console.log("Current settings:");
    console.log(`   Analysis Mode: ${orchestrator.getAnalysisMode()}`);
    console.log(`   Manual Approach: ${orchestrator.getManualApproach()}`);
    
    const hybridRecommendation = await orchestrator.getApproachRecommendation();
    if (hybridRecommendation) {
        console.log("\nHybrid Mode Behavior:");
        console.log("   1. First tries automatic analysis");
        console.log("   2. Uses manual approach if confidence is low");
        console.log(`   Current recommendation: ${hybridRecommendation.recommendedApproach} (confidence: ${(hybridRecommendation.confidence * 100).toFixed(1)}%)`);
    }

    console.log("\n" + "=".repeat(60));
    console.log("RUNTIME CONFIGURATION CHANGES");
    console.log("=".repeat(60));
    
    // Demonstrate runtime configuration changes
    console.log("Changing approaches at runtime:");
    
    // Toggle stream analysis
    console.log("\n1. Disabling stream analysis:");
    orchestrator.toggleStreamAnalysis(false);
    
    // Change manual approach
    console.log("\n2. Changing manual approach:");
    orchestrator.setManualApproach('streaming-query-hive');
    
    // Change mode
    console.log("\n3. Switching to manual mode:");
    orchestrator.setAnalysisMode('manual');

    console.log("\n" + "=".repeat(60));
    console.log("ANALYSIS SUMMARY");
    console.log("=".repeat(60));
    
    // Get comprehensive analysis summary
    const summary = orchestrator.getAnalysisSummary();
    console.log("Current Configuration:");
    console.log(`   Analysis Mode: ${summary.analysisMode}`);
    console.log(`   Stream Analysis: ${summary.streamAnalysisEnabled ? 'Enabled' : 'Disabled'}`);
    console.log(`   Manual Approach: ${summary.manualApproach || 'None'}`);
    console.log(`   Recommendation History: ${summary.historyLength} entries`);
    
    if (summary.warning) {
        console.log(`Warning: ${summary.warning}`);
    }
    
    if (summary.recentApproaches) {
        console.log("\nRecent Approach Usage:");
        Object.entries(summary.recentApproaches).forEach(([approach, count]) => {
            console.log(`   ${approach}: ${count} times`);
        });
        
        if (summary.averageConfidence !== undefined) {
            console.log(`   Average Confidence: ${(summary.averageConfidence * 100).toFixed(1)}%`);
        }
    }

    console.log("\n" + "=".repeat(60));
    console.log("USAGE RECOMMENDATIONS");
    console.log("=".repeat(60));
    
    console.log("Use AUTOMATIC mode when:");
    console.log("   • You want the system to analyze stream patterns automatically");
    console.log("   • Stream characteristics change frequently");
    console.log("   • You trust the AI-driven approach selection");
    
    console.log("\nUse MANUAL mode when:");
    console.log("   • You know exactly which approach works best for your use case");
    console.log("   • You want consistent, predictable behavior");
    console.log("   • Stream characteristics are well-understood and stable");
    
    console.log("\nUse HYBRID mode when:");
    console.log("   • You want automatic optimization but with a safety net");
    console.log("   • You're experimenting with different approaches");
    console.log("   • You want to gradually transition from manual to automatic");
    
    console.log("\nDemo completed!");
}

// API Usage Examples
function showAPIUsageExamples() {
    console.log("\n" + "=".repeat(60));
    console.log("📚 API USAGE EXAMPLES");
    console.log("=".repeat(60));
    
    console.log(`
// 1. Initialize with automatic analysis
const orchestrator = new IntelligentOrchestrator("default", true);

// 2. Switch to manual mode and specify approach
orchestrator.setAnalysisMode('manual');
orchestrator.setManualApproach('approximation-approach');

// 3. Switch to hybrid mode with fallback
orchestrator.setAnalysisMode('hybrid');
orchestrator.setManualApproach('fetching-client-side');

// 4. Toggle stream analysis on/off
orchestrator.toggleStreamAnalysis(false);

// 5. Get current configuration
const mode = orchestrator.getAnalysisMode();
const manualApproach = orchestrator.getManualApproach();

// 6. Get comprehensive analysis summary
const summary = orchestrator.getAnalysisSummary();

// 7. Run query with specific approach (overrides all modes)
await orchestrator.runRegisteredQueryIntelligent('chunked-approach');

// 8. Add stream data for analysis
orchestrator.analyzeStreamData(timestamp, value, topic);

// 9. Get approach recommendation
const recommendation = await orchestrator.getApproachRecommendation();
`);
}

// Run the demo
async function runDemo() {
    try {
        await demonstrateAnalysisModes();
        showAPIUsageExamples();
    } catch (error) {
        console.error("Demo error:", error);
    }
}

if (require.main === module) {
    runDemo();
}

export { demonstrateAnalysisModes, showAPIUsageExamples };
