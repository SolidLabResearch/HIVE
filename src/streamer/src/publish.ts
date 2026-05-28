
import { CSVLogger } from '../../util/logger/CSVLogger';
import { StreamToMQTT } from './publishing/StreamToMQTT';

/**
 *
 */
const logger = new CSVLogger('replayer-log.csv');

function getReplayFrequency(): number {
    const configured = Number.parseFloat(process.env.WEARABLE_FREQUENCY || '');
    return Number.isFinite(configured) && configured > 0 ? configured : 4;
}

/**
 *
 */

async function replaySmartphoneXStream() {
    // Pass a unique clientId for persistent MQTT session
    const clientId = 'pub-' + Math.random().toString(16).substr(2, 8);
    const mqttOptions = { clean: false, clientId };
    
    // Use DATA_PATH environment variable or default to noisy datasets
    const basePath = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
    const dataPath = `src/streamer/data/${basePath}/smartphone.acceleration.x/data.nt`;
    
    const publisher = new StreamToMQTT('mqtt://localhost:1883', getReplayFrequency(), dataPath, "smartphoneX", mqttOptions);
    logger.log("Starting replay for SmartphoneX stream");
    await publisher.replay_streams();
    logger.log("Replay completed for SmartphoneX stream");

}

/**
 *
 */
async function replayWearableXStream() {
    // Pass a unique clientId for persistent MQTT session
    const clientId = 'pub-' + Math.random().toString(16).substr(2, 8);
    const mqttOptions = { clean: false, clientId };
    
    // Use DATA_PATH environment variable or default to noisy datasets
    const basePath = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
    const dataPath = `src/streamer/data/${basePath}/wearable.acceleration.x/data.nt`;
    
    const publisher = new StreamToMQTT('mqtt://localhost:1883', getReplayFrequency(), dataPath, "wearableX", mqttOptions);
    logger.log("Starting replay for WearableX stream");
    await publisher.replay_streams();
    logger.log("Replay completed for WearableX stream");
}

/**
 *
 */
async function replayStreams() {
    await Promise.all([
        replaySmartphoneXStream(),
        replayWearableXStream()
    ]);
    logger.log("All streams replayed successfully");
    process.exit(0);
}

replayStreams().catch((error) => {
    console.error("Error during stream replay:", error);
});
