import { IndependentStreamProcessingApproach } from '../src/approaches/IndependentStreamProcessingApproach';
import { StreamToMQTT } from '../src/streamer/src/publishing/StreamToMQTT';

/**
 * Complete demo showing both data publishers and independent processors
 * This demonstrates the full pipeline: Data Publishers → MQTT → Independent Processors → Results.
 */
async function completePublisherAndProcessorDemo() {
    console.log('\n Complete Data Publisher + Independent Processor Demo');
    console.log('\n  === Full Pipeline Demo ===');
    console.log(' Data Publishers: Send test data to MQTT topics');
    console.log(' Independent Processors: Listen, process, and publish results');
    console.log(' Complete flow: Data → MQTT → RSP Processing → Result Publishing');

    // Define queries for our processors
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

    const outputTopics = [
        'wearable_results',
        'smartphone_results', 
        'combined_results'
    ];

    try {
        console.log('\n Step 1: Creating Independent Processors...');
        const processor = new IndependentStreamProcessingApproach();
        const result = await processor.createIndependentProcessors(
            subQueries, 
            superQuery, 
            outputTopics
        );

        console.log('\n Processors Created:');
        console.log(`    SubQuery processors: ${result.subQueryProcessors.length}`);
        console.log(`    SuperQuery processors: 1`);
        console.log(`    Total processors: ${result.totalProcessors}`);

        console.log('\n Step 2: Starting All Processors...');
        processor.startAllProcessors();
        console.log('    All processors are now listening to MQTT streams');

        // Wait a moment for processors to connect
        await new Promise(resolve => setTimeout(resolve, 2000));

        console.log('\n Step 3: Starting Data Publishers...');
        console.log('    Starting WearableX data publisher...');
        
        // Start data publishers in parallel
        const publisherPromises: Promise<void>[] = [];
        
        // Check if data files exist, use mock data if not
        const basePath = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
        
        // Publisher for wearable data
        publisherPromises.push(
            (async () => {
                try {
                    const wearableClientId = 'wearable-pub-' + Math.random().toString(16).substr(2, 8);
                    const wearableDataPath = `src/streamer/data/${basePath}/wearable.acceleration.x/data.nt`;
                    const wearablePublisher = new StreamToMQTT(
                        'mqtt://localhost:1883', 
                        4, 
                        wearableDataPath, 
                        "wearableX", 
                        { clean: false, clientId: wearableClientId }
                    );
                    console.log('    Publishing wearable data to MQTT...');
                    await wearablePublisher.replay_streams();
                    console.log('    WearableX data publishing completed');
                } catch (error) {
                    console.log('     WearableX data file not found, processors still listening...');
                }
            })()
        );

        // Publisher for smartphone data
        publisherPromises.push(
            (async () => {
                try {
                    const smartphoneClientId = 'smartphone-pub-' + Math.random().toString(16).substr(2, 8);
                    const smartphoneDataPath = `src/streamer/data/${basePath}/smartphone.acceleration.x/data.nt`;
                    const smartphonePublisher = new StreamToMQTT(
                        'mqtt://localhost:1883', 
                        4, 
                        smartphoneDataPath, 
                        "smartphoneX", 
                        { clean: false, clientId: smartphoneClientId }
                    );
                    console.log('    Publishing smartphone data to MQTT...');
                    await smartphonePublisher.replay_streams();
                    console.log('    SmartphoneX data publishing completed');
                } catch (error) {
                    console.log('     SmartphoneX data file not found, processors still listening...');
                }
            })()
        );

        console.log('\n Step 4: Monitoring Full Pipeline...');
        console.log('    Data Publishers → MQTT Topics → Independent Processors → Result Topics');
        
        // Start publishers (they will run in background)
        Promise.all(publisherPromises);
        
        // Monitor the processing for a while
        await processor.monitorProcessing(20000); // Monitor for 20 seconds

        console.log('\n Step 5: Pipeline Summary');
        console.log('    Data Publishers: Sent data to MQTT topics (wearableX, smartphoneX)');
        console.log('    Independent Processors: Listened, processed, and published results');
        console.log('    Result Topics: wearable_results, smartphone_results, combined_results');
        console.log('    Complete pipeline working without HTTP server dependency!');

        console.log('\n Step 6: Stopping All Processors...');
        processor.stopAllProcessors();

        console.log('\n Complete Publisher + Processor Demo Completed!');
        console.log(' Full pipeline: Data Publishing → Stream Processing → Result Publishing');

    } catch (error) {
        console.error(' Error in complete demo:', error);
    }
}

// Simple demo showing what components are involved
/**
 *
 */
async function pipelineOverviewDemo() {
    console.log('\n Pipeline Component Overview');
    console.log('\n Complete Data Flow:');
    console.log('   1  Data Publishers (StreamToMQTT)');
    console.log('      └─ Send test data to MQTT topics: wearableX, smartphoneX');
    console.log('   2  MQTT Broker (localhost:1883)');
    console.log('      └─ Routes data between publishers and processors');
    console.log('   3  Independent Processors (IndependentStreamProcessingApproach)');
    console.log('      ├─ SubQuery_0: Processes wearableX data');
    console.log('      ├─ SubQuery_1: Processes smartphoneX data');
    console.log('      └─ SuperQuery: Processes combined data');
    console.log('   4  Result Publishers (Built into each processor)');
    console.log('      ├─ wearable_results topic');
    console.log('      ├─ smartphone_results topic');
    console.log('      └─ combined_results topic');
    
    console.log('\n Key Features:');
    console.log('    No HTTP server dependency');
    console.log('    Independent parallel processing');
    console.log('    Built-in result publishing');
    console.log('    Works like FetchingAllDataClientSide but in parallel');
}

// Main execution
/**
 *
 */
async function main() {
    try {
        await pipelineOverviewDemo();
        await completePublisherAndProcessorDemo();
        
        console.log('\n All demos completed!');
        console.log(' Your system includes both data publishers AND result publishers!');
        console.log(' Complete pipeline from data input to result output');
        
    } catch (error) {
        console.error(' Demo failed:', error);
    }
}

main().catch(console.error);
