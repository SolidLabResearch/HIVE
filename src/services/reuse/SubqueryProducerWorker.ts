import { RSPAgent } from "../../agent/RSPAgent";

const query = process.env.PRODUCER_QUERY;
const topic = process.env.PRODUCER_TOPIC;
const canonicalProducerId = process.env.CANONICAL_PRODUCER_ID;
const runtimeProducerId = process.env.RUNTIME_PRODUCER_ID;
const producerCoverageOrigin = Number(process.env.PRODUCER_ALIGNMENT_ORIGIN);

if (!query || !topic || !canonicalProducerId || !runtimeProducerId) {
  throw new Error("Missing manager-owned producer runtime configuration");
}
if (!Number.isFinite(producerCoverageOrigin)) {
  throw new Error(
    `Managed producer coverage origin must be finite; received ${String(process.env.PRODUCER_ALIGNMENT_ORIGIN)}`,
  );
}

process.send?.({
  type: "producer_starting",
  canonicalProducerId,
  runtimeProducerId,
  topic,
  pid: process.pid,
  parentPid: process.ppid,
});

const agent = new RSPAgent(query, topic, {
  registerQueryDefinition: false,
  managedProducer: true,
  producerCoverageOrigin,
  mqttClientId: process.env.PRODUCER_MQTT_CLIENT_ID,
  producerIdentity: {
    canonicalProducerId,
    runtimeProducerId,
  },
  onRuntimeError: (error) => {
    process.send?.({ type: "producer_failed", reason: error.message });
  },
});

let stopping = false;

async function start(): Promise<void> {
  try {
    await agent.process_streams();
    process.send?.({
      type: "producer_ready",
      canonicalProducerId,
      runtimeProducerId,
      topic,
      pid: process.pid,
    });
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    process.send?.({ type: "producer_failed", reason });
    process.exitCode = 1;
  }
}

function stop(): void {
  if (stopping) {
    return;
  }
  stopping = true;
  process.send?.({ type: "producer_stopping", canonicalProducerId, runtimeProducerId });
  agent.stop();
  process.send?.({ type: "producer_stopped", canonicalProducerId, runtimeProducerId });
  process.exit(0);
}

process.on("SIGTERM", stop);
process.on("SIGINT", stop);
void start();
