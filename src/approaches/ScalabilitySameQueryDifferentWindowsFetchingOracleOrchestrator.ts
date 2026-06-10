import fs from "fs";
import { RSPEngine, RSPQLParser } from "rsp-js";
import { turtleStringToStore } from "../util/Util";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  getConfiguredAggregation,
  getResultTopic,
  getSessionId,
} from "../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
import { buildScalabilitySuperQuery } from "./scalability/sameQueryDifferentWindows";
const N3 = require("n3");
const mqtt = require("mqtt");
const { DataFactory } = N3;

function initLog(logFilePath: string): (message: string) => void {
  const writeHeader = !fs.existsSync(logFilePath);
  const stream = fs.createWriteStream(logFilePath, { flags: "a" });
  if (writeHeader) {
    stream.write("timestamp,message\n");
  }

  return (message: string) => {
    const ts = Date.now();
    stream.write(`${ts},"${message}"\n`);
    console.log(`LOG: ${ts} - ${message}`);
  };
}

function initLatencyLog(filePath: string) {
  const writeHeader = !fs.existsSync(filePath);
  const stream = fs.createWriteStream(filePath, { flags: "a" });
  if (writeHeader) {
    stream.write(
      "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,delay_past_expected_close_ms,delay_past_data_start_ms,delay_past_last_obs_ms,result_value\n",
    );
  }

  return (
    windowNumber: number,
    queryRegisteredAt: number,
    firstDataReceivedAt: number,
    expectedWindowClose: number,
    lastObsReceivedAt: number,
    resultEmittedAt: number,
    value: string,
  ) => {
    stream.write(
      [
        windowNumber,
        queryRegisteredAt,
        firstDataReceivedAt,
        expectedWindowClose,
        lastObsReceivedAt,
        resultEmittedAt,
        resultEmittedAt - expectedWindowClose,
        resultEmittedAt - firstDataReceivedAt,
        resultEmittedAt - lastObsReceivedAt,
        value,
      ].join(",") + "\n",
    );
  };
}

async function subscribeEngineToMQTT(
  engine: typeof RSPEngine.prototype,
  query: string,
  log: (message: string) => void,
  onData?: (wallClockTime: number) => void,
) {
  const parser = new RSPQLParser();
  const parsed = parser.parse(query);
  const streams: Array<{ stream_name: string }> = [...parsed.s2r];

  for (const streamConfig of streams) {
    const streamName = streamConfig.stream_name;
    const url = new URL(streamName);
    const broker = `${url.protocol}//${url.hostname}:${url.port}/`;
    const topic = url.pathname.slice(1);
    const rdfStream = engine.getStream(streamName);

    if (!rdfStream) {
      log(`Warning: no stream object registered for ${streamName}`);
      continue;
    }

    const clientId = `fetching-sub-${Math.random().toString(16).slice(2, 10)}`;
    const client = mqtt.connect(broker, { clean: false, clientId });

    client.on("connect", () => {
      log(`Connected to ${broker}, subscribing to ${topic}`);
      client.subscribe(topic, { qos: 1 }, (err: Error | null) => {
        if (err) {
          log(`Subscribe error on ${topic}: ${err.message}`);
        } else {
          log(`Subscribed to ${topic}`);
        }
      });
    });

    client.on("message", async (_topic: string, message: Buffer) => {
      try {
        const receivedAt = Date.now();
        const store = await turtleStringToStore(message.toString());
        const tsQuads = store.getQuads(
          null,
          DataFactory.namedNode("https://saref.etsi.org/core/hasTimestamp"),
          null,
          null,
        );
        if (tsQuads.length === 0) {
          return;
        }
        const timestamp = Date.parse(tsQuads[0].object.value);
        const quads = store.getQuads(null, null, null, null);
        const graph = DataFactory.namedNode(rdfStream.name);
        for (const quad of quads) {
          rdfStream.add(
            DataFactory.quad(quad.subject, quad.predicate, quad.object, graph),
            timestamp,
          );
        }
        if (onData) {
          onData(receivedAt);
        }
      } catch (error) {
        log(`Error processing MQTT message on ${topic}: ${error instanceof Error ? error.message : String(error)}`);
      }
    });
  }
}

async function runFetchingOracle() {
  const log = initLog("fetching_client_side_log.csv");
  const logLatency = initLatencyLog("fetching_latency_log.csv");
  const aggregationFunction = getConfiguredAggregation();
  const sessionId = getSessionId();
  const resultTopic = getResultTopic("client_operation_output");
  const query = buildScalabilitySuperQuery(aggregationFunction);

  console.log(new RSPQLParser().parse(query).sparql);

  const queryRegisteredTime = Date.now();
  let firstDataReceivedTime = 0;
  let lastObsReceivedTime = 0;
  const onData = (wallClock: number) => {
    if (firstDataReceivedTime === 0) {
      firstDataReceivedTime = wallClock;
    }
    lastObsReceivedTime = wallClock;
  };

  const engine = new RSPEngine(query);
  const emitter = engine.register();
  let windowCount = 0;
  const windowRange = 120000;
  const windowStep = 60000;

  emitter.on("RStream", (object: any) => {
    if (!object || !object.bindings) {
      return;
    }

    const bindings = Array.isArray(object.bindings)
      ? object.bindings
      : [object.bindings];

    for (const binding of bindings) {
      let resultValue: string | null = null;
      if (binding instanceof Map) {
        resultValue = binding.get("?resultValue")?.value ?? binding.get("?avgValue")?.value ?? null;
      } else if (binding.entries) {
        resultValue = typeof binding.entries.get === "function"
          ? binding.entries.get("resultValue")?.value ?? binding.entries.get("avgValue")?.value ?? null
          : binding.entries.resultValue?.value ?? binding.entries.avgValue?.value ?? null;
      } else {
        resultValue = binding.resultValue?.value ?? binding.avgValue?.value ?? null;
      }

      if (!resultValue) {
        continue;
      }

      windowCount += 1;
      const resultTime = Date.now();
      const expectedClose = queryRegisteredTime + windowRange + ((windowCount - 1) * windowStep);
      logLatency(
        windowCount,
        queryRegisteredTime,
        firstDataReceivedTime || queryRegisteredTime,
        expectedClose,
        lastObsReceivedTime || resultTime,
        resultTime,
        resultValue,
      );

      const payload = JSON.stringify({
        ...buildBenchmarkResultPayload(
          "fetching",
          aggregationFunction,
          sessionId,
          Number.parseFloat(resultValue),
          windowCount,
        ),
        windowNumber: windowCount,
      });

      const pubClientId = `fetching-pub-${Math.random().toString(16).slice(2, 10)}`;
      const pubClient = mqtt.connect("mqtt://localhost:1883", {
        clean: false,
        clientId: pubClientId,
      });
      pubClient.on("connect", () => {
        pubClient.publish(resultTopic, payload, { qos: 1 }, (err: Error | null) => {
          if (!err) {
            recordPublishedMqttMessage({
              topic: resultTopic,
              payload,
              messageType: "superquery_result",
              warmup: windowCount === 1,
            });
          }
          pubClient.end();
        });
      });
    }
  });

  await subscribeEngineToMQTT(engine, query, log, onData);
  log("Fetching oracle started.");
}

function startResourceUsageLogging(
  filePath = "fetching_client_side_resource_usage.csv",
  intervalMs = 100,
) {
  const writeHeader = !fs.existsSync(filePath);
  const logStream = fs.createWriteStream(filePath, { flags: "a" });
  if (writeHeader) {
    logStream.write("timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n");
  }

  setInterval(() => {
    const mem = process.memoryUsage();
    const cpu = process.cpuUsage();
    const now = Date.now();
    logStream.write(
      [
        now,
        (cpu.user / 1000).toFixed(2),
        (cpu.system / 1000).toFixed(2),
        mem.rss,
        mem.heapTotal,
        mem.heapUsed,
        (mem.heapUsed / 1024 / 1024).toFixed(2),
        mem.external,
      ].join(",") + "\n",
    );
  }, intervalMs);
}

startResourceUsageLogging();
runFetchingOracle().catch((error) => {
  console.error("Error in scalability fetching oracle:", error);
});
