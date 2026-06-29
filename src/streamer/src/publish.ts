
import { CSVLogger } from '../../util/logger/CSVLogger';
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as mqtt from "mqtt";
import { StreamToMQTT } from './publishing/StreamToMQTT';
import { buildBenchmarkTopicName, useCleanMqttSessionsForBenchmark } from "../../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../../util/mqttTraffic";
import { profileCount, writeProfileArtifact } from "../../util/profiling";
import {
    buildSyntheticBenchmarkTargets,
    getConfiguredBenchmarkTargetNames,
    getConfiguredBenchmarkTargetSource,
    getConfiguredBenchmarkTargets,
    getRealBenchmarkTargets,
} from "../../util/queryTargets";

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

function getConfiguredBasePath(): string {
    return process.env.DATA_PATH || 'noisy_datasets/noise_0.5';
}

function getRealStreamDataPath(topicName: string): string {
    const basePath = getConfiguredBasePath();
    if (topicName === "smartphoneX") {
        return `src/streamer/data/${basePath}/smartphone.acceleration.x/data.nt`;
    }
    if (topicName === "wearableX") {
        return `src/streamer/data/${basePath}/wearable.acceleration.x/data.nt`;
    }
    throw new Error(`No real replay dataset path configured for target ${topicName}`);
}

function formatSyntheticValue(baseValue: string, syntheticIndex: number): string {
    const parsed = Number.parseFloat(baseValue);
    if (!Number.isFinite(parsed)) {
        return baseValue;
    }
    return (parsed + syntheticIndex).toFixed(6);
}

function createSyntheticReplayFile(params: {
    syntheticTargetName: string;
    syntheticIndex: number;
    baseFilePath: string;
    outputDir: string;
}): string {
    const { syntheticTargetName, syntheticIndex, baseFilePath, outputDir } = params;
    const input = fs.readFileSync(baseFilePath, "utf8");
    const transformed = input
        .split(/\r?\n/)
        .filter(Boolean)
        .map((line, lineIndex) => {
            const subjectSuffix = `${syntheticTargetName}_obs${lineIndex}`;
            const datasetSuffix = `${syntheticTargetName}_dataset`;
            const withSubject = line
                .replace(/<https:\/\/dahcc\.idlab\.ugent\.be\/Protego\/_participant1\/obs\d+>/g, `<https://dahcc.idlab.ugent.be/Protego/_participant1/${subjectSuffix}>`)
                .replace(/<https:\/\/dahcc\.idlab\.ugent\.be\/Protego\/_participant1>/g, `<https://dahcc.idlab.ugent.be/Protego/_participant1/${datasetSuffix}>`);
            const withProperty = withSubject.replace(
                /<https:\/\/dahcc\.idlab\.ugent\.be\/Homelab\/SensorsAndActuators\/(?:wearableX|smartphoneX)>/g,
                `<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${syntheticTargetName}>`,
            );
            return withProperty.replace(
                /"([+-]?\d+(?:\.\d+)?)"\^\^<http:\/\/www\.w3\.org\/2001\/XMLSchema#float>/,
                (_match, baseValue) =>
                    `"${formatSyntheticValue(baseValue, syntheticIndex)}"^^<http://www.w3.org/2001/XMLSchema#float>`,
            );
        })
        .join("\n");

    const outputPath = path.join(outputDir, `${syntheticTargetName}.data.nt`);
    fs.writeFileSync(outputPath, `${transformed}\n`);
    return outputPath;
}

async function replayTargetStream(
    streamName: string,
    dataPath: string,
): Promise<void> {
    const clientId = 'pub-' + Math.random().toString(16).substr(2, 8);
    const mqttOptions = { clean: useCleanMqttSessionsForBenchmark(), clientId };
    const topic = buildBenchmarkTopicName(streamName);
    const publisher = new StreamToMQTT(
        'mqtt://localhost:1883',
        getReplayFrequency(),
        dataPath,
        topic,
        mqttOptions,
    );
    lifecycleLog("replay.stream.start", { stream: streamName, dataPath, topic });
    logger.log(`Starting replay for ${streamName} stream`);
    await publisher.replay_streams();
    lifecycleLog("replay.stream.complete", { stream: streamName, dataPath, topic });
    lifecycleLog("replay.stream.end", { stream: streamName, dataPath, topic });
    logger.log(`Replay completed for ${streamName} stream`);
}

/**
 *
 */

async function replaySmartphoneXStream() {
    await replayTargetStream("smartphoneX", getRealStreamDataPath("smartphoneX"));
}

/**
 *
 */
async function replayWearableXStream() {
    await replayTargetStream("wearableX", getRealStreamDataPath("wearableX"));
}

async function replayConfiguredRealTargetStreams(): Promise<void> {
    const configuredNames = getConfiguredBenchmarkTargetNames();
    const targets = configuredNames.length > 0
        ? getConfiguredBenchmarkTargets()
        : getRealBenchmarkTargets();
    await Promise.all(
        targets.map((target) => replayTargetStream(target.topicName, getRealStreamDataPath(target.topicName))),
    );
}

async function replaySyntheticTargetStreams(): Promise<void> {
    const configuredTargets = getConfiguredBenchmarkTargets();
    const targets = configuredTargets.length > 0
        ? configuredTargets
        : buildSyntheticBenchmarkTargets(
            Number.parseInt(process.env.BENCHMARK_TARGET_COUNT || "2", 10) || 2,
        );
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "streaming-query-hive-synth-"));
    const cleanup = () => {
        try {
            fs.rmSync(tempDir, { recursive: true, force: true });
        } catch {
            // ignore cleanup errors
        }
    };

    try {
        const baseSourcePaths = [
            getRealStreamDataPath("wearableX"),
            getRealStreamDataPath("smartphoneX"),
        ];
        const syntheticFiles = targets.map((target, index) => ({
            targetName: target.topicName,
            filePath: createSyntheticReplayFile({
                syntheticTargetName: target.name,
                syntheticIndex: index + 1,
                baseFilePath: baseSourcePaths[index % baseSourcePaths.length],
                outputDir: tempDir,
            }),
        }));

        lifecycleLog("replay.synthetic.prepared", {
            targetCount: targets.length,
            targetNames: targets.map((target) => target.name).join(","),
            tempDir,
        });

        await Promise.all(
            syntheticFiles.map((entry) => replayTargetStream(entry.targetName, entry.filePath)),
        );
    } finally {
        cleanup();
    }
}

/**
 *
 */
async function replayStreams() {
    const targetSource = getConfiguredBenchmarkTargetSource();
    lifecycleLog("replay.start", {
        dataPath: getConfiguredBasePath(),
        replayFrequency: getReplayFrequency(),
        smokeMode: process.env.PAPER_BENCHMARK_SMOKE || "0",
        finiteReplayMode: isBenchmarkFiniteReplayMode(),
        targetSource,
    });
    if (targetSource === "synthetic") {
        await replaySyntheticTargetStreams();
    } else if (process.env.BENCHMARK_QUERY_TARGET_NAMES || process.env.BENCHMARK_TARGET_NAMES) {
        await replayConfiguredRealTargetStreams();
    } else {
        await Promise.all([
            replaySmartphoneXStream(),
            replayWearableXStream()
        ]);
    }
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
        dataPath: getConfiguredBasePath(),
        targetSource,
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
