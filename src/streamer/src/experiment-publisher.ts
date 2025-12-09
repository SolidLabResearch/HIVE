import { StreamToMQTT } from './publishing/StreamToMQTT';

/**
 * Experiment publisher that takes data file path and topic as command line arguments
 * Usage: node dist/streamer/src/experiment-publisher.js <dataFilePath> <topicName> [frequency].
 */

/**
 *
 */
async function publishExperimentData() {
    const args = process.argv.slice(2);
    
    if (args.length < 2) {
        console.error('Usage: node experiment-publisher.js <dataFilePath> <topicName> [frequency]');
        process.exit(1);
    }
    
    const dataFilePath = args[0];
    const topicName = args[1];
    console.log(`Debug: raw args: ${JSON.stringify(args)}`);
    const frequency = args[2] ? parseInt(args[2]) : 4; // Default to 4Hz
    console.log(`Debug: parsed frequency: ${frequency}`);
    
    try {
        // Create unique client ID for each publisher
        const clientId = `exp-pub-${Math.random().toString(16).substr(2, 8)}`;
        const mqttOptions = { clean: false, clientId };
        
        console.log(`Starting publisher: ${dataFilePath} → ${topicName} (${frequency}Hz)`);
        
        const publisher = new StreamToMQTT(
            'mqtt://localhost:1883', 
            frequency, 
            dataFilePath, 
            topicName, 
            mqttOptions
        );
        
        await publisher.replay_streams();
        console.log(`Publisher completed: ${topicName}`);
        
    } catch (error) {
        console.error(`Publisher error for ${topicName}:`, error);
        process.exit(1);
    }
}

publishExperimentData().catch((error) => {
    console.error("Error during experiment publishing:", error);
    process.exit(1);
});
