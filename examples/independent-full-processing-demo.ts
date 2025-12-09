import { IndependentStreamProcessingApproach } from '../src/approaches/IndependentStreamProcessingApproach';

/**
 * Demo showing how the independent approach works like FetchingAllDataClientSide
 * Each processor independently fetches from MQTT, processes, and publishes results.
 */
async function fullProcessingDemo() {
    console.log('\n Independent Full Processing Demo (Like Client-Side Approach)');
    console.log('\n  === Creating Independent Processors ===');
    console.log(' Each processor will:');
    console.log('   • Connect to MQTT streams independently');
    console.log('   • Process data using RSP-QL queries');
    console.log('   • Publish results to output topics');
    console.log('   • No HTTP server dependency');

    // Define queries like in the original approaches
    const subQueries = [
        // SubQuery 1: Process wearable data
        `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 

REGISTER RStream <wearable_averages> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
        `.trim(),

        // SubQuery 2: Process smartphone data
        `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 

REGISTER RStream <smartphone_averages> AS
SELECT (AVG(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
        `.trim()
    ];

    // SuperQuery: Combine results from both streams
    const superQuery = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js> 

REGISTER RStream <sensor_averages> AS
SELECT (MAX(?value) AS ?maxValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
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
    `.trim();

    // Define output topics for each processor
    const outputTopics = [
        'wearable_subquery_output',
        'smartphone_subquery_output', 
        'combined_superquery_output'
    ];

    try {
        // Create the independent processing approach
        const processor = new IndependentStreamProcessingApproach();
        
        console.log('\n Creating independent processors...');
        const result = await processor.createIndependentProcessors(
            subQueries, 
            superQuery, 
            outputTopics
        );

        console.log('\n Processor Creation Results:');
        console.log(`   - SubQuery processors: ${result.subQueryProcessors.length}`);
        console.log(`   - SuperQuery processors: 1`);
        console.log(`   - Total processors: ${result.totalProcessors}`);

        console.log('\n Starting all processors...');
        processor.startAllProcessors();

        console.log('\n Each processor is now:');
        console.log('   • Connecting to MQTT broker (localhost:1883)');
        console.log('   • Subscribing to relevant topics');
        console.log('   • Processing incoming stream data');
        console.log('   • Publishing results to output topics');

        console.log('\n Monitoring processing activity...');
        await processor.monitorProcessing(15000); // Monitor for 15 seconds

        console.log('\n Stopping all processors...');
        processor.stopAllProcessors();

        console.log('\n Independent full processing demo completed!');
        console.log(' All processors worked independently like client-side approach');

    } catch (error) {
        console.error(' Error in full processing demo:', error);
    }
}

// Performance comparison with original approach
/**
 *
 */
async function performanceComparison() {
    console.log('\n Performance Comparison Demo');
    console.log('\n--- Independent Approach (Like Client-Side) ---');
    
    const processor = new IndependentStreamProcessingApproach();
    
    const subQueries = ['SELECT * WHERE { ?s ?p ?o }', 'SELECT * WHERE { ?s ?p ?o }'];
    const superQuery = 'SELECT * WHERE { ?s ?p ?o }';
    
    const startTime = Date.now();
    const result = await processor.createIndependentProcessors(subQueries, superQuery);
    const endTime = Date.now();
    
    console.log(` All ${result.totalProcessors} processors created in ${endTime - startTime}ms`);
    console.log(' Each processor independently handles:');
    console.log('   • MQTT connections');
    console.log('   • Stream processing');
    console.log('   • Result publishing');
    console.log('   • No shared resources or HTTP dependency');
    
    processor.stopAllProcessors();
    console.log(' Independent approach completed');
    
    const stats = processor.getProcessorStats();
    console.log(` Final stats: ${stats.totalProcessors} independent processors`);
    console.log(' Like FetchingAllDataClientSide but in parallel!');
}

// Main execution
/**
 *
 */
async function main() {
    try {
        await fullProcessingDemo();
        await performanceComparison();
        
        console.log('\n All demos completed!');
        console.log(' You now have independent processors that work like client-side approach!');
        console.log(' Each processor fetches, processes, and publishes independently');
        
    } catch (error) {
        console.error(' Demo failed:', error);
    }
}

main().catch(console.error);
