
import { CSVLogger } from '../../util/logger/CSVLogger';
import * as mqtt from "mqtt";
import { StreamToMQTT } from './publishing/StreamToMQTT';
import { buildBenchmarkTopicName, useCleanMqttSessionsForBenchmark } from "../../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../../util/mqttTraffic";
import { profileCount, writeProfileArtifact } from "../../util/profiling";

/**
 *
 */
const logger = new CSVLogger('replayer-log.csv');
const lifecycleTs = () => new Date().toISOString();
const lifecycleLog = (event: string, details: Record<string, unknown> = {}) => {
    const parts = Object.entries(details)
        .filter(([, value]) => value !== undefined && value !== null && value !== "")
        .map(([key, value]) => `${key}=${value}`);
    const suffix = parts.length > 0 ? ` ${parts.join(" ")}` : "";
    console.log(`[publisher-lifecycle ${lifecycleTs()}] ${event}${suffix}`);
};
const isBenchmarkFiniteReplayMode = (): boolean => {
    const raw = process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY || "";
    return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
};

async function publishFiniteReplayCompleteSignal(): Promise<void> {
    if (!isBenchmarkFiniteReplayMode()) {
        return;
    }

    const controlTopic = buildBenchmarkTopicName("__benchmark_control__");
    const clientId = 'pub-control-' + Math.random().toString(16).substr(2, 8);
    const client = mqtt.connect('mqtt://localhost:1883', {
        clean: useCleanMqttSessionsForBenchmark(),
        clientId,
    });
    profileCount("mqtt_clients_created");

    await new Promise<void>((resolve, reject) => {
        let settled = false;
        const finish = (fn: () => void) => {
            if (settled) {
                return;
            }
            settled = true;
            fn();
        };

        const timeoutHandle = setTimeout(() => {
            finish(() => {
                client.end(true);
                reject(new Error(`Timed out publishing finite replay completion signal to ${controlTopic}`));
            });
        }, 10000);

        client.on("connect", () => {
            const payload = JSON.stringify({
                type: "finite_replay_complete",
                source: "benchmark_publisher",
                topicPrefix: process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || "",
                timestamp: Date.now(),
            });
            client.publish(
                controlTopic,
                payload,
                { qos: 1, retain: false },
                (err) => {
                    if (err) {
                        finish(() => {
                            clearTimeout(timeoutHandle);
                            client.end(true);
                            reject(err);
                        });
                        return;
                    }

                    finish(() => {
                        recordPublishedMqttMessage({
                            topic: controlTopic,
                            payload,
                            messageType: "control",
                        });
                        clearTimeout(timeoutHandle);
                        client.end(false, () => resolve());
                    });
                },
            );
        });

        client.on("error", (err) => {
            finish(() => {
                clearTimeout(timeoutHandle);
                client.end(true);
                reject(err);
            });
        });
    });
}

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
    const mqttOptions = { clean: useCleanMqttSessionsForBenchmark(), clientId };
    
    // Use DATA_PATH environment variable or default to noisy datasets
    const basePath = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
    const dataPath = `src/streamer/data/${basePath}/smartphone.acceleration.x/data.nt`;
    
    const publisher = new StreamToMQTT('mqtt://localhost:1883', getReplayFrequency(), dataPath, buildBenchmarkTopicName("smartphoneX"), mqttOptions);
    lifecycleLog("replay.stream.start", { stream: "smartphoneX", dataPath, topic: buildBenchmarkTopicName("smartphoneX") });
    logger.log("Starting replay for SmartphoneX stream");
    await publisher.replay_streams();
    lifecycleLog("replay.stream.complete", { stream: "smartphoneX", dataPath, topic: buildBenchmarkTopicName("smartphoneX") });
    lifecycleLog("replay.stream.end", { stream: "smartphoneX", dataPath, topic: buildBenchmarkTopicName("smartphoneX") });
    logger.log("Replay completed for SmartphoneX stream");

}

/**
 *
 */
async function replayWearableXStream() {
    // Pass a unique clientId for persistent MQTT session
    const clientId = 'pub-' + Math.random().toString(16).substr(2, 8);
    const mqttOptions = { clean: useCleanMqttSessionsForBenchmark(), clientId };
    
    // Use DATA_PATH environment variable or default to noisy datasets
    const basePath = process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
    const dataPath = `src/streamer/data/${basePath}/wearable.acceleration.x/data.nt`;
    
    const publisher = new StreamToMQTT('mqtt://localhost:1883', getReplayFrequency(), dataPath, buildBenchmarkTopicName("wearableX"), mqttOptions);
    lifecycleLog("replay.stream.start", { stream: "wearableX", dataPath, topic: buildBenchmarkTopicName("wearableX") });
    logger.log("Starting replay for WearableX stream");
    await publisher.replay_streams();
    lifecycleLog("replay.stream.complete", { stream: "wearableX", dataPath, topic: buildBenchmarkTopicName("wearableX") });
    lifecycleLog("replay.stream.end", { stream: "wearableX", dataPath, topic: buildBenchmarkTopicName("wearableX") });
    logger.log("Replay completed for WearableX stream");
}

/**
 *
 */
async function replayStreams() {
    lifecycleLog("replay.start", {
        dataPath: process.env.DATA_PATH || 'noisy_datasets/noise_0.5',
        replayFrequency: getReplayFrequency(),
        smokeMode: process.env.PAPER_BENCHMARK_SMOKE || "0",
        finiteReplayMode: isBenchmarkFiniteReplayMode(),
    });
    await Promise.all([
        replaySmartphoneXStream(),
        replayWearableXStream()
    ]);
    if (isBenchmarkFiniteReplayMode()) {
        lifecycleLog("benchmark.finite_replay_complete.signal.requested", {
            topicPrefix: process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || "",
        });
        await publishFiniteReplayCompleteSignal();
        lifecycleLog("benchmark.finite_replay_complete.signal.published", {
            topicPrefix: process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || "",
        });
    }
    lifecycleLog("replay.all_streams_complete", {
        dataPath: process.env.DATA_PATH || 'noisy_datasets/noise_0.5',
    });
    logger.log("All streams replayed successfully");
}

async function main() {
    try {
        lifecycleLog("publisher.main.entered", {
            dataPath: process.env.DATA_PATH || 'noisy_datasets/noise_0.5',
            topicPrefix: process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || "",
            finiteReplayMode: isBenchmarkFiniteReplayMode(),
        });
        await replayStreams();
        lifecycleLog("publisher.before_exit", { exitCode: 0 });
        process.exitCode = 0;
    } catch (error) {
        console.error("Error during stream replay:", error);
        lifecycleLog("publisher.before_exit", { exitCode: 1, error: error instanceof Error ? error.message : String(error) });
        process.exitCode = 1;
    } finally {
        try {
            await logger.close();
        } catch (closeError) {
            console.error("Error closing replay logger:", closeError);
            if (process.exitCode === undefined || process.exitCode === 0) {
                process.exitCode = 1;
            }
        }
        writeProfileArtifact();
        lifecycleLog("publisher.complete", { exitCode: process.exitCode ?? 0, finiteReplayMode: isBenchmarkFiniteReplayMode() });
        lifecycleLog("publisher.exit", { exitCode: process.exitCode ?? 0 });
    }
}

main();
