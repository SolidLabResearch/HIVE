import * as fs from 'fs';
import * as mqtt from 'mqtt';
import * as path from 'path';
import { StreamConsumer } from "./StreamConsumer";
import { getOutputWindowRange, getOutputWindowStep } from "../../../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../../../util/mqttTraffic";
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, literal } = DataFactory;

/**
 *
 */
export class StreamToMQTT {

    private stream_consumer: StreamConsumer;
    private store: any;
    private mqtt_client: mqtt.MqttClient;
    private file_location: string;
    private initialize_promise: Promise<void> | null = null;
    private sorted_observation_subjects!: string[];
    private observation_pointer: number = 0;
    private number_of_publish: number = 0;
    private queue: any[] = [];
    private topic_to_publish: string;
    private sort_subject_length: number = 0;
    private frequency: number;
    private successfulPublishes: number = 0;
    private failedPublishes: number = 0;
    private publishAttempts: number = 0;
    private deterministicEventTime: boolean = process.env.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME === "1";
    private benchmarkStartTime: number = Number(process.env.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME || Date.now());
    private datasetStartTime: number | null = null;
    private datasetDuration: number = 0;
    private loopDurationMs: number = 0;
    private replayLoopIndex: number = 0;
    private originalObservationTimestamps: Map<string, string> = new Map();
    private originalObservationOffsets: Map<string, number> = new Map();
    private debugChunksEnabled: boolean = process.env.STREAMING_QUERY_HIVE_DEBUG_CHUNKS === "1";
    private firstPublishedTimestamp: string | null = null;
    private lastPublishedTimestamp: string | null = null;
    private finiteReplayMode: boolean = ["1", "true", "yes", "on"].includes(
        (process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY || "").trim().toLowerCase(),
    );
    private pendingPublishCount: number = 0;
    private mqttConnectWaitPromise: Promise<void> | null = null;
    private targetReplayIntervalMs: number;
    private selectedObservationCount: number = 0;
    private sourceObservationIntervalMs: number | null = null;
    private replayStartWallClockTime: number | null = null;

    private lifecycleLog(event: string, details: Record<string, unknown> = {}): void {
        const parts = Object.entries(details)
            .filter(([, value]) => value !== undefined && value !== null && value !== "")
            .map(([key, value]) => `${key}=${value}`);
        const suffix = parts.length > 0 ? ` ${parts.join(" ")}` : "";
        console.log(`[publisher-lifecycle ${new Date().toISOString()}] ${event}${suffix}`);
    }

    /**
     *
     * @param mqtt_broker
     * @param frequency
     * @param file_location
     * @param topic_to_publish
     */
    constructor(mqtt_broker: string, frequency: number, file_location: string, topic_to_publish: string, mqttOptions?: mqtt.IClientOptions) {
        this.store = new N3.Store();
        this.stream_consumer = new StreamConsumer(this.store);
        this.file_location = file_location;
        this.frequency = frequency;
        this.targetReplayIntervalMs = 1000 / this.frequency;
        this.topic_to_publish = topic_to_publish;
        const brokerHostname = new URL(mqtt_broker).hostname;
        const resolvedBrokerUrl = brokerHostname === "localhost"
            ? `${new URL(mqtt_broker).protocol}//127.0.0.1:${new URL(mqtt_broker).port || "1883"}`
            : mqtt_broker;
        const resolvedMqttOptions = mqttOptions ? { ...mqttOptions } : { clean: false };
        // For benchmark isolation, callers can pass clean:true to avoid broker-side session replay.
        if (mqttOptions) {
            this.mqtt_client = mqtt.connect(resolvedBrokerUrl, resolvedMqttOptions);
        } else {
            this.mqtt_client = mqtt.connect(resolvedBrokerUrl, resolvedMqttOptions);
        }
        this.lifecycleLog("mqtt.client.created", {
            topic: this.topic_to_publish,
            brokerUrl: mqtt_broker,
            resolvedBrokerUrl,
            connected: this.mqtt_client.connected,
            reconnecting: this.mqtt_client.reconnecting,
        });
        this.mqtt_client.on("connect", () => {
            this.lifecycleLog("mqtt.client.connected", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
            });
        });
        this.mqtt_client.on("reconnect", () => {
            this.lifecycleLog("mqtt.client.reconnecting", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
            });
        });
        this.mqtt_client.on("close", () => {
            this.lifecycleLog("mqtt.client.closed", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
            });
        });
        this.mqtt_client.on("offline", () => {
            this.lifecycleLog("mqtt.client.offline", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
            });
        });
        this.mqtt_client.on("error", (error: Error) => {
            this.lifecycleLog("mqtt.client.error", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
                error: error.message,
            });
        });
    }

    /**
     *
     */
    async initialize(): Promise<void> {
        try {
            this.lifecycleLog("initialize.entered", { topic: this.topic_to_publish, fileLocation: this.file_location });
            const store: typeof N3.Store = await this.load_dataset(this.file_location);
            this.sorted_observation_subjects = await this.sort_observations(store);
            this.captureOriginalObservationTiming();
            this.applyFrequencySampling();
            this.lifecycleLog("initialize.completed", {
                topic: this.topic_to_publish,
                subjects: this.sorted_observation_subjects.length,
                selectedObservationCount: this.selectedObservationCount,
                sourceObservationIntervalMs: this.sourceObservationIntervalMs ?? "",
                targetReplayIntervalMs: this.targetReplayIntervalMs,
                loopDurationMs: this.loopDurationMs,
            });
        } catch (error) {
            console.error('Error initializing StreamToMQTT:', error);
            throw error;
        }
    }

    /**
     *
     * @param store
     */
    async sort_observations(store: any): Promise<string[]> {
        const temp: string[] = [];

        for (const quad of store.match(null, 'https://saref.etsi.org/core/measurementMadeBy', null)) {
            temp.push(quad.subject.id);
        }

        const sorted = this.merge_sort(temp, store).reverse();
        this.sort_subject_length = sorted.length;
        return sorted;
    }

    /**
     *
     * @param array
     * @param store
     */
    merge_sort(array: string[], store: any): string[] {
        if (array.length <= 1) return array;

        const mid = Math.floor(array.length / 2);
        const left = this.merge_sort(array.slice(0, mid), store);
        const right = this.merge_sort(array.slice(mid), store);
        return this.merge(left, right, store);
    }

    /**
     *
     * @param left
     * @param right
     * @param store
     */
    merge(left: string[], right: string[], store: any): string[] {
        const merged: string[] = [];
        let i = 0, j = 0;

        while (i < left.length && j < right.length) {
            const t1 = store.getObjects(namedNode(left[i]).id, namedNode('https://saref.etsi.org/core/hasTimestamp'));
            const t2 = store.getObjects(namedNode(right[j]).id, namedNode('https://saref.etsi.org/core/hasTimestamp'));

            if (t1 > t2) merged.push(left[i++]);
            else merged.push(right[j++]);
        }

        return merged.concat(left.slice(i)).concat(right.slice(j));
    }

    /**
     *
     * @param file_location
     */
    async load_dataset(file_location: string): Promise<typeof N3.Store> {
        const topic = path.basename(file_location);
        console.log(`Loading file: ${topic}`);

        return new Promise((resolve, reject) => {
            const parser = new N3.StreamParser();
            const stream = fs.createReadStream(file_location);
            const writer = this.stream_consumer.get_writer();

            parser.on('data', (quad: any) => writer.write(quad));
            parser.on('end', () => {
                this.lifecycleLog("replay.dataset.eof", {
                    topic: this.topic_to_publish,
                    fileLocation: file_location,
                });
                resolve(this.store);
            });
            parser.on('error', reject);

            stream.pipe(parser);
        });
    }

    /**
     *
     */
    async replay_streams(): Promise<void> {
        this.lifecycleLog("replay.start", {
            topic: this.topic_to_publish,
            fileLocation: this.file_location,
            frequency: this.frequency,
            finiteReplayMode: this.finiteReplayMode,
        });
        if (!this.initialize_promise) {
            this.initialize_promise = this.initialize();
        }
        await this.initialize_promise;
        await this.waitForMqttConnected();

        if (!this.store || this.sorted_observation_subjects.length === 0) {
            console.log('No observations to replay.');
            this.lifecycleLog("replay.empty", { topic: this.topic_to_publish });
            return;
        }

        const durationSeconds = this.finiteReplayMode
            ? Number(process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS || Math.ceil((getOutputWindowRange() + (2 * getOutputWindowStep())) / 1000))
            : (process.env.PAPER_BENCHMARK_SMOKE === "1" ? 120 : 300); // Smoke mode uses a shorter replay window.
        const startTime = Date.now();
        this.replayStartWallClockTime = startTime;

        if (this.finiteReplayMode) {
            while ((Date.now() - startTime) < durationSeconds * 1000) {
                if (this.observation_pointer >= this.sorted_observation_subjects.length) {
                    // Reset pointer to loop data
                    this.observation_pointer = 0;
                    this.number_of_publish = 0; // Reset publish count for next loop logic check if needed
                    this.replayLoopIndex++;
                    console.log('Looping data stream...');
                    this.lifecycleLog("replay.loop.reset", {
                        topic: this.topic_to_publish,
                        loopIndex: this.replayLoopIndex,
                    });
                }
                await this.waitForScheduledPublishTime();
                await this.publish_one_observation();
            }
            this.lifecycleLog("replay.duration.complete", {
                topic: this.topic_to_publish,
                durationSeconds,
                publishAttempts: this.publishAttempts,
                successfulPublishes: this.successfulPublishes,
                failedPublishes: this.failedPublishes,
            });
        } else {
            while ((Date.now() - startTime) < durationSeconds * 1000) {
                if (this.observation_pointer >= this.sorted_observation_subjects.length) {
                    // Reset pointer to loop data
                    this.observation_pointer = 0;
                    this.number_of_publish = 0; // Reset publish count for next loop logic check if needed
                    this.replayLoopIndex++;
                    console.log('Looping data stream...');
                    this.lifecycleLog("replay.loop.reset", {
                        topic: this.topic_to_publish,
                        loopIndex: this.replayLoopIndex,
                    });
                }

                await this.waitForScheduledPublishTime();
                await this.publish_one_observation();
            }
            this.lifecycleLog("replay.duration.complete", {
                topic: this.topic_to_publish,
                durationSeconds,
                publishAttempts: this.publishAttempts,
                successfulPublishes: this.successfulPublishes,
                failedPublishes: this.failedPublishes,
            });
        }

        this.lifecycleLog("mqtt.pending_publish_count", {
            topic: this.topic_to_publish,
            pendingPublishCount: this.pendingPublishCount,
            finiteReplayMode: this.finiteReplayMode,
        });
        await this.closeMqttClient();
        this.lifecycleLog("mqtt.client.end.called", { topic: this.topic_to_publish });
        const completedAllSourceObservations =
            this.successfulPublishes >= this.sort_subject_length;
        const completionStatus = completedAllSourceObservations ? "completed" : "incomplete";
        const publisherExitReason = this.finiteReplayMode
            ? "finite_replay_duration_reached"
            : (completedAllSourceObservations
                ? "source_dataset_exhausted"
                : "insufficient_publishes");
        console.log(
            `[STREAM SUMMARY] topic=${this.topic_to_publish} expectedRecords=${this.sort_subject_length} publishAttempts=${this.publishAttempts} publishSuccesses=${this.successfulPublishes} publishFailures=${this.failedPublishes} firstTimestamp=${this.firstPublishedTimestamp || "none"} lastTimestamp=${this.lastPublishedTimestamp || "none"} completionStatus=${completionStatus}`,
        );
        console.log('All observations published.');
        const summary = `Summary: Intended: ${this.sort_subject_length}, Successful: ${this.successfulPublishes}, Failed: ${this.failedPublishes}`;
        console.log(summary);
        try {
            const logPath = path.resolve(process.cwd(), 'replayer-log.csv');
            const header = 'timestamp,topic,intended,successful,failed,first_published_timestamp,last_published_timestamp\n';
            const line = `${Date.now()},${this.topic_to_publish},${this.sort_subject_length},${this.successfulPublishes},${this.failedPublishes},${this.firstPublishedTimestamp || ""},${this.lastPublishedTimestamp || ""}\n`;
            if (!fs.existsSync(logPath)) {
                fs.appendFileSync(logPath, header);
            }
            fs.appendFileSync(logPath, line);
        } catch (err) {
            console.error('Error writing summary to replayer-log.csv:', err);
        }

        try {
            const summaryDir = process.env.LOG_PATH || process.cwd();
            const runSummaryPath = path.join(summaryDir, "run_summary.json");
            fs.mkdirSync(path.dirname(runSummaryPath), { recursive: true });
            fs.writeFileSync(
                runSummaryPath,
                JSON.stringify(
                    {
                        publisherExitReason,
                        completionStatus,
                        publishedObservations: this.successfulPublishes,
                        totalSourceObservations: this.sort_subject_length,
                        publishAttempts: this.publishAttempts,
                        failedPublishes: this.failedPublishes,
                        finiteReplayMode: this.finiteReplayMode,
                        finiteReplayDurationSeconds: this.finiteReplayMode
                            ? Number(process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS || Math.ceil((getOutputWindowRange() + (2 * getOutputWindowStep())) / 1000))
                            : null,
                        replayLoopCount: this.replayLoopIndex,
                        topic: this.topic_to_publish,
                        firstPublishedTimestamp: this.firstPublishedTimestamp || null,
                        lastPublishedTimestamp: this.lastPublishedTimestamp || null,
                    },
                    null,
                    2,
                ),
            );
        } catch (error) {
            console.error('Error writing run_summary.json:', error);
        }

        if (!this.finiteReplayMode && this.successfulPublishes < this.sort_subject_length) {
            throw new Error(
                `Replay completed with insufficient publishes for ${this.topic_to_publish}: expected at least ${this.sort_subject_length}, got ${this.successfulPublishes}`,
            );
        }
        if (this.finiteReplayMode && !completedAllSourceObservations) {
            this.lifecycleLog("replay.finite_duration.reached", {
                topic: this.topic_to_publish,
                publisherExitReason,
                publishedObservations: this.successfulPublishes,
                totalSourceObservations: this.sort_subject_length,
                finiteReplayDurationSeconds: Number(process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS || Math.ceil((getOutputWindowRange() + (2 * getOutputWindowStep())) / 1000)),
            });
        }
        this.lifecycleLog("replay.completed", {
            topic: this.topic_to_publish,
            completionStatus,
        });
    }

    private async waitForMqttConnected(): Promise<void> {
        if (this.mqtt_client.connected) {
            this.lifecycleLog("mqtt.connect.ready", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
            });
            return;
        }

        if (!this.mqttConnectWaitPromise) {
            this.lifecycleLog("mqtt.connect.wait.start", {
                topic: this.topic_to_publish,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
                timeoutMs: Number(process.env.STREAMING_QUERY_HIVE_MQTT_CONNECT_TIMEOUT_MS || 10000),
            });
            this.mqttConnectWaitPromise = new Promise<void>((resolve, reject) => {
                const timeoutMs = Number(process.env.STREAMING_QUERY_HIVE_MQTT_CONNECT_TIMEOUT_MS || 10000);
                const startTimestamp = Date.now();
                let settled = false;

                const finish = (kind: "ready" | "timeout" | "error", error?: Error) => {
                    if (settled) {
                        return;
                    }
                    settled = true;
                    clearTimeout(timeoutHandle);
                    this.mqtt_client.removeListener("connect", onConnect);
                    this.mqtt_client.removeListener("error", onError);

                    if (kind === "ready") {
                        this.lifecycleLog("mqtt.connect.ready", {
                            topic: this.topic_to_publish,
                            connected: this.mqtt_client.connected,
                            reconnecting: this.mqtt_client.reconnecting,
                            elapsedMs: Date.now() - startTimestamp,
                        });
                        resolve();
                        return;
                    }

                    if (kind === "timeout") {
                        const timeoutTimestamp = Date.now();
                        this.lifecycleLog("mqtt.connect.timeout", {
                            topic: this.topic_to_publish,
                            connected: this.mqtt_client.connected,
                            reconnecting: this.mqtt_client.reconnecting,
                            timeoutMs,
                            elapsedMs: timeoutTimestamp - startTimestamp,
                        });
                        reject(new Error(`Timed out waiting for MQTT connect on ${this.topic_to_publish}`));
                        return;
                    }

                    this.lifecycleLog("mqtt.connect.error", {
                        topic: this.topic_to_publish,
                        connected: this.mqtt_client.connected,
                        reconnecting: this.mqtt_client.reconnecting,
                        error: error?.message || String(error),
                        elapsedMs: Date.now() - startTimestamp,
                    });
                    reject(error ?? new Error(`MQTT connect failed for ${this.topic_to_publish}`));
                };

                const onConnect = () => finish("ready");
                const onError = (error: Error) => finish("error", error);
                const timeoutHandle = setTimeout(() => finish("timeout"), timeoutMs);

                this.mqtt_client.on("connect", onConnect);
                this.mqtt_client.on("error", onError);
            });
        }

        return this.mqttConnectWaitPromise;
    }

    /**
     *
     * @param ms
     */
    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    private applyFrequencySampling(): void {
        const offsets = this.sorted_observation_subjects
            .map((id) => ({
                id,
                offset: this.originalObservationOffsets.get(id),
            }))
            .filter((entry): entry is { id: string; offset: number } => entry.offset !== undefined)
            .sort((left, right) => left.offset - right.offset);

        if (offsets.length < 2 || !(this.targetReplayIntervalMs > 0)) {
            this.selectedObservationCount = this.sorted_observation_subjects.length;
            return;
        }

        const deltas: number[] = [];
        for (let index = 1; index < offsets.length; index += 1) {
            const delta = offsets[index].offset - offsets[index - 1].offset;
            if (delta > 0) {
                deltas.push(delta);
            }
        }

        if (deltas.length === 0) {
            this.selectedObservationCount = this.sorted_observation_subjects.length;
            return;
        }

        this.sourceObservationIntervalMs = Math.min(...deltas);
        if (this.sourceObservationIntervalMs >= this.targetReplayIntervalMs) {
            this.selectedObservationCount = this.sorted_observation_subjects.length;
            return;
        }

        const sampledSubjects: string[] = [];
        let lastBucket: number | null = null;
        for (const entry of offsets) {
            const bucket = Math.floor(entry.offset / this.targetReplayIntervalMs);
            if (bucket === lastBucket) {
                continue;
            }
            sampledSubjects.push(entry.id);
            lastBucket = bucket;
        }

        this.sorted_observation_subjects = sampledSubjects;
        this.sort_subject_length = sampledSubjects.length;
        this.selectedObservationCount = sampledSubjects.length;
        const lastSampledOffset = sampledSubjects.length > 0
            ? this.originalObservationOffsets.get(sampledSubjects[sampledSubjects.length - 1]) ?? 0
            : 0;
        this.loopDurationMs = Math.max(
            this.datasetDuration,
            lastSampledOffset + this.targetReplayIntervalMs,
        );
        this.lifecycleLog("replay.frequency_sampling.applied", {
            topic: this.topic_to_publish,
            sourceObservationCount: offsets.length,
            selectedObservationCount: sampledSubjects.length,
            sourceObservationIntervalMs: this.sourceObservationIntervalMs,
            targetReplayIntervalMs: this.targetReplayIntervalMs,
            loopDurationMs: this.loopDurationMs,
        });
    }

    private getCurrentObservationSchedulingContext(): {
        id: string;
        eventOffsetMs: number;
        targetPublishTime: number;
    } | null {
        if (this.observation_pointer >= this.sorted_observation_subjects.length) {
            return null;
        }

        const id = this.sorted_observation_subjects[this.observation_pointer];
        const eventOffsetMs = this.originalObservationOffsets.get(id);
        if (eventOffsetMs === undefined || this.replayStartWallClockTime === null) {
            return null;
        }

        return {
            id,
            eventOffsetMs,
            targetPublishTime:
                this.replayStartWallClockTime +
                (this.replayLoopIndex * this.loopDurationMs) +
                eventOffsetMs,
        };
    }

    private async waitForScheduledPublishTime(): Promise<void> {
        const schedulingContext = this.getCurrentObservationSchedulingContext();
        if (!schedulingContext) {
            return;
        }

        const delayMs = schedulingContext.targetPublishTime - Date.now();
        if (delayMs > 0) {
            await this.sleep(delayMs);
        }
    }

    private getCumulativeEventTimeSpanMs(originalOffset: number | undefined): number | undefined {
        if (originalOffset === undefined) {
            return undefined;
        }

        return (this.replayLoopIndex * this.loopDurationMs) + originalOffset;
    }

    /**
     *
     */
    private async publish_one_observation() {
        if (this.number_of_publish >= this.sort_subject_length) {
            // Logic handled in replay_streams loop
            // console.log('No more observations to publish.');
            // process.exit();
        }

        try {
            this.publishAttempts++;
            const id = this.sorted_observation_subjects[this.observation_pointer];
            const node = namedNode(id);

            // Remove old timestamp
            const old = this.store.getQuads(node, namedNode('https://saref.etsi.org/core/hasTimestamp'), null, null);
            const originalTimestamp = this.originalObservationTimestamps.get(id) ?? old[0]?.object?.value;
            const originalOffset = this.originalObservationOffsets.get(id);
            this.store.removeQuads(old);

            // Add new timestamp
            const emittedTimestamp = this.computeEmittedTimestamp(originalTimestamp, originalOffset);
            this.store.addQuad(node, namedNode('https://saref.etsi.org/core/hasTimestamp'), literal(emittedTimestamp));

            // Extract quads for this observation
            const quads = this.store.getQuads(node, null, null, null);
            const subStore = new N3.Store(quads);
            const data = await this.storeToString(subStore);

            if (data && data.trim() !== '') {
                this.pendingPublishCount += 1;
                this.lifecycleLog("mqtt.pending_publish_count", {
                    topic: this.topic_to_publish,
                    pendingPublishCount: this.pendingPublishCount,
                    phase: "before_publish",
                });
                await this.publishWithTimeout(data, emittedTimestamp, id, originalTimestamp, originalOffset);
                this.number_of_publish++;
                this.observation_pointer++;
            }
        } catch (error) {
            this.failedPublishes++;
            console.error('Error publishing observation:', error);
            throw error;
        }
    }

    private async publishWithTimeout(
        data: string,
        emittedTimestamp: string,
        id: string,
        originalTimestamp: string | undefined,
        originalOffset: number | undefined,
    ): Promise<void> {
        const timeoutMs = Number(process.env.STREAMING_QUERY_HIVE_PUBLISH_TIMEOUT_MS || 10000);
        const qos = 0;
        if (this.publishAttempts === 1) {
            this.lifecycleLog("mqtt.publish.first_requested_after_connect", {
                topic: this.topic_to_publish,
                qos,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
                pendingPublishCount: this.pendingPublishCount,
            });
        }
        await new Promise<void>((resolve, reject) => {
            let settled = false;
            const publishRequestedAt = Date.now();
            this.lifecycleLog("mqtt.publish.requested", {
                topic: this.topic_to_publish,
                qos,
                timeoutMs,
                connected: this.mqtt_client.connected,
                reconnecting: this.mqtt_client.reconnecting,
                pendingPublishCount: this.pendingPublishCount,
                publishRequestedAt,
            });
            const timeoutHandle = setTimeout(() => {
                if (settled) {
                    return;
                }
                settled = true;
                const timeoutTimestamp = Date.now();
                this.lifecycleLog("mqtt.publish.timeout", {
                    topic: this.topic_to_publish,
                    qos,
                    timeoutMs,
                    connected: this.mqtt_client.connected,
                    reconnecting: this.mqtt_client.reconnecting,
                    pendingPublishCount: this.pendingPublishCount,
                    elapsedMs: timeoutTimestamp - publishRequestedAt,
                    timeoutTimestamp,
                });
                reject(new Error(`Timed out publishing QoS ${qos} to ${this.topic_to_publish}`));
            }, timeoutMs);

            this.mqtt_client.publish(this.topic_to_publish, data, { qos, retain: false }, (err: any) => {
                if (settled) {
                    const callbackTimestamp = Date.now();
                    this.lifecycleLog("mqtt.publish.callback.after_timeout", {
                        topic: this.topic_to_publish,
                        qos,
                        elapsedMs: callbackTimestamp - publishRequestedAt,
                        connected: this.mqtt_client.connected,
                        reconnecting: this.mqtt_client.reconnecting,
                        pendingPublishCount: this.pendingPublishCount,
                        error: err ? (err.message || String(err)) : "",
                    });
                    return;
                }
                settled = true;
                clearTimeout(timeoutHandle);
                const callbackTimestamp = Date.now();
                this.lifecycleLog("mqtt.publish.callback", {
                    topic: this.topic_to_publish,
                    qos,
                    elapsedMs: callbackTimestamp - publishRequestedAt,
                    connected: this.mqtt_client.connected,
                    reconnecting: this.mqtt_client.reconnecting,
                    pendingPublishCount: this.pendingPublishCount,
                    error: err ? (err.message || String(err)) : "",
                });
                this.pendingPublishCount = Math.max(0, this.pendingPublishCount - 1);
                this.lifecycleLog("mqtt.pending_publish_count", {
                    topic: this.topic_to_publish,
                    pendingPublishCount: this.pendingPublishCount,
                    phase: "after_publish_callback",
                });

                if (err) {
                    this.failedPublishes++;
                    console.error(`Error publishing observation with QoS ${qos}:`, err);
                    this.lifecycleLog("mqtt.publish.acks_complete", {
                        topic: this.topic_to_publish,
                        publishAttempts: this.publishAttempts,
                        successfulPublishes: this.successfulPublishes,
                        failedPublishes: this.failedPublishes,
                        error: err.message || String(err),
                    });
                    reject(err);
                    return;
                }

                this.successfulPublishes++;
                const actualPublishTime = Date.now();
                const targetPublishTime =
                    this.getCurrentObservationSchedulingContext()?.targetPublishTime;
                const publishLagMs =
                    targetPublishTime !== undefined
                        ? actualPublishTime - targetPublishTime
                        : undefined;
                const eventTimeSpanMs =
                    this.getCumulativeEventTimeSpanMs(originalOffset);
                const wallClockPublishSpanMs =
                    this.replayStartWallClockTime !== null
                        ? actualPublishTime - this.replayStartWallClockTime
                        : undefined;
                const effectiveReplaySpeed =
                    eventTimeSpanMs !== undefined &&
                    wallClockPublishSpanMs !== undefined &&
                    wallClockPublishSpanMs > 0
                        ? eventTimeSpanMs / wallClockPublishSpanMs
                        : undefined;
                recordPublishedMqttMessage({
                    topic: this.topic_to_publish,
                    payload: data,
                    messageType: "raw_input_stream",
                    targetPublishTime,
                    actualPublishTime,
                    publishLagMs,
                    effectiveReplaySpeed,
                    eventTimeSpanMs,
                    wallClockPublishSpanMs,
                });
                this.lifecycleLog("mqtt.publish.acks_complete", {
                    topic: this.topic_to_publish,
                    publishAttempts: this.publishAttempts,
                    successfulPublishes: this.successfulPublishes,
                    failedPublishes: this.failedPublishes,
                    targetPublishTime: targetPublishTime ?? "",
                    actualPublishTime,
                    publishLagMs: publishLagMs ?? "",
                    effectiveReplaySpeed: effectiveReplaySpeed ?? "",
                });
                if (this.firstPublishedTimestamp === null) {
                    this.firstPublishedTimestamp = emittedTimestamp;
                }
                this.lastPublishedTimestamp = emittedTimestamp;
                if (this.debugChunksEnabled) {
                    console.log(
                        `[DEBUG_CHUNKS] publisher topic=${this.topic_to_publish} subject=${id} loopIndex=${this.replayLoopIndex} originalTs=${originalTimestamp || "none"} originalOffset=${originalOffset ?? "none"} emittedTs=${emittedTimestamp} datasetDuration=${this.datasetDuration}`,
                    );
                }
                console.log(`Published observation: ${id} at ${this.file_location} with QoS 2`);
                resolve();
            });
        });
    }

    private async closeMqttClient(): Promise<void> {
        await new Promise<void>((resolve, reject) => {
            let settled = false;
            const timeoutHandle = setTimeout(() => {
                if (settled) {
                    return;
                }
                settled = true;
                reject(new Error(`Timed out closing MQTT client for ${this.topic_to_publish}`));
            }, 10000);

            try {
                this.lifecycleLog("mqtt.client.end.requested", { topic: this.topic_to_publish });
                this.mqtt_client.end(false, () => {
                    if (settled) {
                        return;
                    }
                    settled = true;
                    clearTimeout(timeoutHandle);
                    this.lifecycleLog("mqtt.client.close", { topic: this.topic_to_publish });
                    resolve();
                });
            } catch (error) {
                if (settled) {
                    return;
                }
                settled = true;
                clearTimeout(timeoutHandle);
                reject(error);
            }
        });
    }

    /**
     *
     * @param store
     */
    private storeToString(store: any): Promise<string> {
        const writer = new N3.Writer();
        writer.addQuads(store.getQuads(null, null, null, null));

        return new Promise((resolve, reject) => {
            writer.end((err: any, result: string) => err ? reject(err) : resolve(result));
        });
    }

    private captureOriginalObservationTiming(): void {
        const observationsWithEpochs: Array<{ id: string; timestamp: string; epoch: number }> = [];

        for (const id of this.sorted_observation_subjects) {
            const timestamp = this.store.getObjects(
                namedNode(id).id,
                namedNode('https://saref.etsi.org/core/hasTimestamp'),
            )[0]?.value;

            if (!timestamp) {
                continue;
            }

            this.originalObservationTimestamps.set(id, timestamp);

            const epoch = Date.parse(timestamp);
            if (Number.isFinite(epoch)) {
                observationsWithEpochs.push({ id, timestamp, epoch });
            }
        }

        if (observationsWithEpochs.length === 0) {
            this.datasetStartTime = null;
            this.datasetDuration = 0;
            this.loopDurationMs = 0;
            this.originalObservationOffsets.clear();
            return;
        }

        const epochs = observationsWithEpochs.map(({ epoch }) => epoch);
        this.datasetStartTime = Math.min(...epochs);
        this.datasetDuration = Math.max(...epochs) - this.datasetStartTime;
        this.loopDurationMs = this.datasetDuration;
        this.originalObservationOffsets.clear();

        for (const { id, epoch } of observationsWithEpochs) {
            this.originalObservationOffsets.set(id, epoch - this.datasetStartTime);
        }
    }

    private computeEmittedTimestamp(originalTimestamp: string | undefined, originalOffset?: number): string {
        if (!this.deterministicEventTime || !originalTimestamp) {
            return new Date().toISOString();
        }

        const resolvedOffset = originalOffset ?? (() => {
            const originalEpoch = Date.parse(originalTimestamp);
            if (!Number.isFinite(originalEpoch)) {
                return undefined;
            }
            if (this.datasetStartTime === null) {
                this.datasetStartTime = originalEpoch;
            }
            return originalEpoch - this.datasetStartTime;
        })();

        if (resolvedOffset === undefined) {
            return new Date().toISOString();
        }

        const loopBaseTime = this.benchmarkStartTime + (this.replayLoopIndex * this.loopDurationMs);
        return new Date(loopBaseTime + resolvedOffset).toISOString();
    }
}
