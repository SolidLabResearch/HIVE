import { CSVLogger } from "../../util/logger/CSVLogger";
import { StreamToMQTT } from "./publishing/StreamToMQTT";
import {
  buildBenchmarkTopicName,
  useCleanMqttSessionsForBenchmark,
} from "../../util/runtimeConfig";
import * as mqtt from "mqtt";

const logger = new CSVLogger("replayer-log.csv");

function lifecycleLog(event: string, details: Record<string, unknown> = {}) {
  const parts = Object.entries(details)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([key, value]) => `${key}=${value}`);
  const suffix = parts.length > 0 ? ` ${parts.join(" ")}` : "";
  console.log(`[publisher-lifecycle ${new Date().toISOString()}] ${event}${suffix}`);
}

function isFiniteReplayMode(): boolean {
  const raw = process.env.STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY || "";
  return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
}

function getReplayFrequency(): number {
  const configured = Number.parseFloat(process.env.WEARABLE_FREQUENCY || "");
  return Number.isFinite(configured) && configured > 0 ? configured : 4;
}

async function publishFiniteReplayCompleteSignal(): Promise<void> {
  if (!isFiniteReplayMode()) {
    return;
  }

  const controlTopic = buildBenchmarkTopicName("__benchmark_control__");
  const clientId = `pub-control-${Math.random().toString(16).slice(2, 10)}`;
  const client = mqtt.connect("mqtt://localhost:1883", {
    clean: useCleanMqttSessionsForBenchmark(),
    clientId,
  });

  await new Promise<void>((resolve, reject) => {
    const payload = JSON.stringify({
      type: "finite_replay_complete",
      source: "benchmark_publisher",
      topicPrefix: process.env.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX || "",
      timestamp: Date.now(),
    });

    const timeout = setTimeout(() => {
      client.end(true);
      reject(new Error(`Timed out publishing finite replay completion signal to ${controlTopic}`));
    }, 10000);

    client.on("connect", () => {
      client.publish(controlTopic, payload, { qos: 1, retain: false }, (err) => {
        clearTimeout(timeout);
        if (err) {
          client.end(true);
          reject(err);
          return;
        }
        client.end(false, () => resolve());
      });
    });

    client.on("error", (error) => {
      clearTimeout(timeout);
      client.end(true);
      reject(error);
    });
  });
}

async function replaySmartphoneStream() {
  const clientId = `pub-${Math.random().toString(16).slice(2, 10)}`;
  const mqttOptions = { clean: useCleanMqttSessionsForBenchmark(), clientId };
  const basePath = process.env.DATA_PATH || "custom_patterns/low_variability";
  const dataPath = `src/streamer/data/${basePath}/smartphone.acceleration.x/data.nt`;
  const topic = buildBenchmarkTopicName("smartphoneX");
  const publisher = new StreamToMQTT(
    "mqtt://localhost:1883",
    getReplayFrequency(),
    dataPath,
    topic,
    mqttOptions,
  );

  lifecycleLog("replay.stream.start", { stream: "smartphoneX", dataPath, topic });
  logger.log("Starting replay for SmartphoneX stream");
  await publisher.replay_streams();
  lifecycleLog("replay.stream.complete", { stream: "smartphoneX", dataPath, topic });
  logger.log("Replay completed for SmartphoneX stream");
}

async function main() {
  try {
    lifecycleLog("replay.start", {
      dataPath: process.env.DATA_PATH || "custom_patterns/low_variability",
      replayFrequency: getReplayFrequency(),
      finiteReplayMode: isFiniteReplayMode(),
      streamScope: "smartphone-only",
    });
    await replaySmartphoneStream();
    await publishFiniteReplayCompleteSignal();
    logger.log("All streams replayed successfully");
    process.exitCode = 0;
  } catch (error) {
    console.error("Error during smartphone-only replay:", error);
    process.exitCode = 1;
  } finally {
    await logger.close();
    lifecycleLog("publisher.complete", {
      exitCode: process.exitCode ?? 0,
      streamScope: "smartphone-only",
    });
  }
}

main();
