import fs from "fs";
import { RSPEngine } from "rsp-js";
import { turtleStringToStore } from "../util/Util";
import {
  AggregationFunction,
  buildBenchmarkResultPayload,
  getConfiguredAggregation,
  getResultTopic,
  getSessionId,
} from "../util/runtimeConfig";
import { recordPublishedMqttMessage } from "../util/mqttTraffic";
import {
  buildScalabilitySubQuery,
  buildScalabilitySuperQuery,
  getConfiguredScale,
  getReusableWindowsForScale,
} from "./scalability/sameQueryDifferentWindows";
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
      "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_obs_ms,result_value\n",
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
    const latencyFromQueryReg = resultEmittedAt - queryRegisteredAt;
    const latencyFromDataStart = resultEmittedAt - firstDataReceivedAt;
    const latencyFromLastObs = resultEmittedAt - lastObsReceivedAt;
    stream.write(
      [
        windowNumber,
        queryRegisteredAt,
        firstDataReceivedAt,
        expectedWindowClose,
        lastObsReceivedAt,
        resultEmittedAt,
        latencyFromQueryReg,
        latencyFromDataStart,
        latencyFromLastObs,
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
  const parser = new (require("rsp-js").RSPQLParser)();
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

    const clientId = `naive-sub-${Math.random().toString(16).slice(2, 10)}`;
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

    client.on("error", (error: Error) => {
      log(`MQTT client error on ${broker}: ${error.message}`);
    });
  }
}

async function runNaiveDistributedScalabilityOrchestrator() {
  const log = initLog("naive_distributed_approach_log.csv");
  const logLatency = initLatencyLog("naive_distributed_latency_log.csv");
  const aggregationFunction = getConfiguredAggregation();
  const scale = getConfiguredScale();
  const sessionId = getSessionId();
  const resultTopic = getResultTopic("naive_distributed/output");
  const reusableQueries = getReusableWindowsForScale(scale).map((window, index) =>
    buildScalabilitySubQuery(index + 1, window, aggregationFunction),
  );
  const superQuery = buildScalabilitySuperQuery(aggregationFunction);

  const queryRegisteredTime = Date.now();
  log(`naive_distributed_query_registered scale=${scale}`);

  let firstDataReceivedTime = 0;
  let lastObsReceivedTime = 0;
  const onData = (wallClock: number) => {
    if (firstDataReceivedTime === 0) {
      firstDataReceivedTime = wallClock;
    }
    lastObsReceivedTime = wallClock;
  };

  const subEngines = reusableQueries.map((query) => new RSPEngine(query));
  const superEngine = new RSPEngine(superQuery);
  const superEmitter = superEngine.register();

  subEngines.forEach((engine, index) => {
    const emitter = engine.register();
    emitter.on("RStream", (object: unknown) => {
      log(`SubQuery${index + 1} result: ${JSON.stringify(object)}`);
    });
  });

  const windowRange = 120000;
  const windowStep = 60000;
  let windowCount = 0;

  superEmitter.on("RStream", (object: any) => {
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
          "naive_distributed",
          aggregationFunction,
          sessionId,
          Number.parseFloat(resultValue),
          windowCount,
        ),
        windowNumber: windowCount,
      });

      const pubClientId = `naive-pub-${Math.random().toString(16).slice(2, 10)}`;
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

  for (let index = 0; index < subEngines.length; index += 1) {
    log(`Starting reusable query engine ${index + 1}/${subEngines.length}...`);
    await subscribeEngineToMQTT(subEngines[index], reusableQueries[index], log, onData);
  }

  log("Starting super-query engine (raw stream, no result reuse)...");
  await subscribeEngineToMQTT(superEngine, superQuery, log, onData);
  log(`Naive Distributed scalability run active with scale=${scale}`);
}

function startResourceUsageLogging(
  filePath = "naive_distributed_approach_resource_usage.csv",
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
runNaiveDistributedScalabilityOrchestrator().catch((error) => {
  console.error("Error in scalability naive distributed orchestrator:", error);
});
