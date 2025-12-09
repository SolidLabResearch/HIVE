import { IntelligentOrchestrator } from '../src/orchestrator/IntelligentOrchestrator';

/**
 * Example demonstrating the new preAnalyzeStreams functionality
 * This shows how to pre-analyze streams before query execution.
 */
async function demonstratePreAnalysis() {
    console.log('\nPre-Analysis Streams Demo');
    console.log('=' .repeat(50));

    // Create orchestrator with stream analysis enabled
    const orchestrator = new IntelligentOrchestrator("default", true);

    // Simulate some stream data flowing in
    console.log('\nSimulating stream data...');
    for (let i = 0; i < 50; i++) {
        // Mix of stable and volatile data
        const value = i < 25 ? 50 + Math.random() * 2 : Math.random() * 100;
        orchestrator.analyzeStreamData(Date.now() + i * 1000, value, 'demo_sensor');
    }

    // Pre-analyze streams (this caches the recommendation)
    console.log('\nPre-analyzing streams...');
    const preAnalysisResult = await orchestrator.preAnalyzeStreams(5);

    if (preAnalysisResult) {
        console.log(`Pre-analysis complete:`);
        console.log(`   Recommended: ${preAnalysisResult.recommendedApproach}`);
        console.log(`   Confidence: ${(preAnalysisResult.confidence * 100).toFixed(1)}%`);
        console.log(`   Reasoning: ${preAnalysisResult.reasoning.join(', ')}`);
    }

    // Check analysis summary
    const summary = orchestrator.getAnalysisSummary();
    console.log('\nAnalysis Summary:');
    console.log(`   Mode: ${summary.analysisMode}`);
    console.log(`   Cached Recommendation: ${summary.cachedRecommendation ? 'YES' : 'NO'}`);

    // Register a query
    console.log('\nRegistering query...');
    orchestrator.registerOutputQuery(`
        PREFIX saref: <https://saref.etsi.org/core/>
        REGISTER RStream <results> AS
        SELECT (AVG(?value) AS ?avg) WHERE { ?s saref:hasValue ?value }
    `);

    // Run query (will use cached recommendation)
    console.log('\nRunning query with pre-analyzed recommendation...');
    await orchestrator.runRegisteredQueryIntelligent();

    // Clear cache if needed
    console.log('\n🧹 Clearing cached recommendation...');
    orchestrator.clearCachedRecommendation();

    const finalSummary = orchestrator.getAnalysisSummary();
    console.log(`   Cached Recommendation: ${finalSummary.cachedRecommendation ? 'YES' : 'NO'}`);
}

// Run the demo
demonstratePreAnalysis().catch(console.error);
