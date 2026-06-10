import fs from "fs";
import path from "path";

export type MqttTrafficMessageType =
  | "raw_input_stream"
  | "subquery_result"
  | "reusable_result"
  | "chunk_result"
  | "superquery_result"
  | "control"
  | "unknown";

export type MqttTrafficRecord = {
  timestamp: number;
  scenario: string;
  approach: string;
  scale: string;
  iteration: string;
  topic: string;
  messageType: MqttTrafficMessageType;
  topicBytes: number;
  payloadBytes: number;
  publishedBytes: number;
  subscriberCount: number;
  estimatedDeliveryBytes: number;
  warmup?: boolean;
};

type RecordMqttTrafficInput = Omit<Partial<MqttTrafficRecord>, "iteration"> & {
  topic: string;
  messageType?: MqttTrafficMessageType;
  iteration?: string | number;
};

type RecordPublishedMqttMessageInput = {
  topic: string;
  payload: unknown;
  serializedPayload?: string;
  messageType?: MqttTrafficMessageType;
  subscriberCount?: number;
  timestamp?: number;
  warmup?: boolean;
  scenario?: string;
  approach?: string;
  scale?: string;
  iteration?: string | number;
  logDir?: string;
};

type FinalizeMqttTrafficArtifactsInput = {
  logDir?: string;
};

type NumericSummary = Record<string, number>;

const MQTT_TRAFFIC_INTERNAL_FILE = "mqtt_traffic.ndjson";
const MQTT_TRAFFIC_CSV_FILE = "mqtt_traffic.csv";
const MQTT_TRAFFIC_SUMMARY_FILE = "mqtt_traffic_summary.json";
const CSV_COLUMNS = [
  "timestamp",
  "scenario",
  "approach",
  "scale",
  "iteration",
  "topic",
  "messageType",
  "topicBytes",
  "payloadBytes",
  "publishedBytes",
  "subscriberCount",
  "estimatedDeliveryBytes",
  "warmup",
] as const;

function resolveLogDir(logDir?: string): string {
  const resolved = logDir || process.env.LOG_PATH || ".";
  fs.mkdirSync(resolved, { recursive: true });
  return resolved;
}

function getInternalLogPath(logDir?: string): string {
  return path.join(resolveLogDir(logDir), MQTT_TRAFFIC_INTERNAL_FILE);
}

function getCsvPath(logDir?: string): string {
  return path.join(resolveLogDir(logDir), MQTT_TRAFFIC_CSV_FILE);
}

function getSummaryPath(logDir?: string): string {
  return path.join(resolveLogDir(logDir), MQTT_TRAFFIC_SUMMARY_FILE);
}

function normalizeString(value: unknown, fallback: string): string {
  if (value === undefined || value === null) {
    return fallback;
  }

  const trimmed = String(value).trim();
  return trimmed === "" ? fallback : trimmed;
}

function escapeCsv(value: unknown): string {
  const stringValue = String(value ?? "");
  if (!/[",\n]/.test(stringValue)) {
    return stringValue;
  }
  return `"${stringValue.replace(/"/g, '""')}"`;
}

function normalizeInteger(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(0, Math.trunc(parsed));
}

function getMetadataFromEnv() {
  return {
    scenario: normalizeString(
      process.env.BENCHMARK_SCENARIO,
      process.env.DATA_PATH ? "benchmark-replay" : "default",
    ),
    approach: normalizeString(
      process.env.BENCHMARK_APPROACH,
      process.env.OPERATOR_TYPE || "unknown",
    ),
    scale: normalizeString(
      process.env.BENCHMARK_SCALE,
      process.env.DATA_PATH || "default",
    ),
    iteration: normalizeString(process.env.BENCHMARK_ITERATION, "0"),
  };
}

export function serializePayloadForMqttMeasurement(
  payload: unknown,
  serializedPayload?: string,
): string | Buffer {
  if (Buffer.isBuffer(payload)) {
    return payload;
  }

  if (typeof payload === "string") {
    return payload;
  }

  if (serializedPayload !== undefined) {
    return serializedPayload;
  }

  if (payload === undefined || payload === null) {
    return "";
  }

  if (typeof payload === "object") {
    return JSON.stringify(payload);
  }

  return String(payload);
}

export function measureMqttPublish(
  topic: string,
  payload: unknown,
  serializedPayload?: string,
) {
  // publishedBytes counts a single application-level publish once.
  // estimatedDeliveryBytes is computed later by multiplying by the estimated
  // active subscriber count for that topic.
  const normalizedPayload = serializePayloadForMqttMeasurement(
    payload,
    serializedPayload,
  );
  const topicBytes = Buffer.byteLength(topic, "utf8");
  const payloadBytes = Buffer.isBuffer(normalizedPayload)
    ? normalizedPayload.length
    : Buffer.byteLength(normalizedPayload, "utf8");

  return {
    topicBytes,
    payloadBytes,
    publishedBytes: topicBytes + payloadBytes,
  };
}

function inferMessageType(
  topic: string,
  messageType?: MqttTrafficMessageType,
): MqttTrafficMessageType {
  if (messageType) {
    return messageType;
  }

  if (topic.includes("__benchmark_control__") || topic.includes("control")) {
    return "control";
  }
  if (topic.includes("smartphoneX") || topic.includes("wearableX")) {
    return "raw_input_stream";
  }
  if (topic.includes("chunked/")) {
    return "chunk_result";
  }
  if (topic.includes("output") || topic.includes("benchmark/results/")) {
    return "superquery_result";
  }
  return "unknown";
}

function estimateRawInputSubscribers(approach: string): number {
  const configuredOverride = Number.parseInt(
    process.env.BENCHMARK_RAW_INPUT_SUBSCRIBERS || "",
    10,
  );
  if (Number.isFinite(configuredOverride) && configuredOverride >= 0) {
    return configuredOverride;
  }

  switch (approach) {
    case "fetching":
      return 1;
    case "naive_distributed":
      return 2;
    case "approximation":
      return 1;
    case "chunked":
      // The current chunked pipeline starts one reusable subquery process and
      // one rewritten chunk-query process per raw stream, so the estimate is 2.
      return 2;
    default:
      return 1;
  }
}

export function estimateSubscriberCountForMessage(input: {
  approach?: string;
  topic: string;
  messageType?: MqttTrafficMessageType;
}): number {
  const approach = normalizeString(input.approach, "unknown");
  const messageType = inferMessageType(input.topic, input.messageType);

  switch (messageType) {
    case "raw_input_stream":
      // This is an application-level estimate based on benchmark-configured
      // active subscribers, not broker-internal delivery counters.
      return estimateRawInputSubscribers(approach);
    case "subquery_result":
      return 1;
    case "reusable_result":
      return approach === "chunked" ? 0 : 1;
    case "chunk_result":
      return 1;
    case "superquery_result":
      return 1;
    case "control":
      return approach === "fetching" ? 1 : 0;
    case "unknown":
    default:
      return 1;
  }
}

export function recordMqttTraffic(
  input: RecordMqttTrafficInput & { logDir?: string },
): MqttTrafficRecord {
  const metadata = getMetadataFromEnv();
  const messageType = inferMessageType(input.topic, input.messageType);
  const topicBytes =
    input.topicBytes ?? Buffer.byteLength(input.topic || "", "utf8");
  const payloadBytes = normalizeInteger(input.payloadBytes);
  const publishedBytes =
    input.publishedBytes ?? normalizeInteger(topicBytes + payloadBytes);
  const subscriberCount =
    input.subscriberCount ??
    estimateSubscriberCountForMessage({
      approach: input.approach ?? metadata.approach,
      topic: input.topic,
      messageType,
    });
  const estimatedDeliveryBytes =
    input.estimatedDeliveryBytes ??
    normalizeInteger(publishedBytes * Math.max(0, subscriberCount));

  const record: MqttTrafficRecord = {
    timestamp: normalizeInteger(input.timestamp, Date.now()),
    scenario: normalizeString(input.scenario, metadata.scenario),
    approach: normalizeString(input.approach, metadata.approach),
    scale: normalizeString(input.scale, metadata.scale),
    iteration: normalizeString(input.iteration, metadata.iteration),
    topic: input.topic,
    messageType,
    topicBytes: normalizeInteger(topicBytes),
    payloadBytes,
    publishedBytes: normalizeInteger(publishedBytes),
    subscriberCount: normalizeInteger(subscriberCount),
    estimatedDeliveryBytes: normalizeInteger(estimatedDeliveryBytes),
    warmup: typeof input.warmup === "boolean" ? input.warmup : undefined,
  };

  fs.appendFileSync(getInternalLogPath(input.logDir), `${JSON.stringify(record)}\n`);
  return record;
}

export function recordPublishedMqttMessage(
  input: RecordPublishedMqttMessageInput,
): MqttTrafficRecord {
  const measured = measureMqttPublish(
    input.topic,
    input.payload,
    input.serializedPayload,
  );
  return recordMqttTraffic({
    ...input,
    topicBytes: measured.topicBytes,
    payloadBytes: measured.payloadBytes,
    publishedBytes: measured.publishedBytes,
  });
}

function getEmptySummary(): NumericSummary {
  return {
    published_application_bytes: 0,
    estimated_delivery_bytes: 0,
    published_bandwidth_kb_s: 0,
    estimated_delivery_bandwidth_kb_s: 0,
    raw_input_published_bytes: 0,
    raw_input_estimated_delivery_bytes: 0,
    raw_input_subscriber_count: 0,
    subquery_result_published_bytes: 0,
    subquery_result_estimated_delivery_bytes: 0,
    reusable_result_published_bytes: 0,
    reusable_result_estimated_delivery_bytes: 0,
    chunk_result_published_bytes: 0,
    chunk_result_estimated_delivery_bytes: 0,
    chunk_result_count: 0,
    chunk_bandwidth_kb_s: 0,
    superquery_result_published_bytes: 0,
    superquery_result_estimated_delivery_bytes: 0,
    control_published_bytes: 0,
    control_estimated_delivery_bytes: 0,
    unknown_published_bytes: 0,
    unknown_estimated_delivery_bytes: 0,
    reuse_layer_published_bytes: 0,
    reuse_layer_estimated_delivery_bytes: 0,
    reuse_layer_bandwidth_kb_s: 0,
    steady_state_duration_seconds: 0,
  };
}

function getSummaryCategoryPrefix(messageType: MqttTrafficMessageType): string {
  switch (messageType) {
    case "raw_input_stream":
      return "raw_input";
    case "subquery_result":
      return "subquery_result";
    case "reusable_result":
      return "reusable_result";
    case "chunk_result":
      return "chunk_result";
    case "superquery_result":
      return "superquery_result";
    case "control":
      return "control";
    case "unknown":
    default:
      return "unknown";
  }
}

function readInternalRecords(logDir?: string): MqttTrafficRecord[] {
  const internalLogPath = getInternalLogPath(logDir);
  if (!fs.existsSync(internalLogPath)) {
    return [];
  }

  const content = fs.readFileSync(internalLogPath, "utf8");
  return content
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as MqttTrafficRecord)
    .sort((a, b) => a.timestamp - b.timestamp);
}

function writeCsv(logDir: string, records: Array<MqttTrafficRecord & { warmup: boolean }>) {
  const header = `${CSV_COLUMNS.join(",")}\n`;
  const body = records
    .map((record) =>
      [
        record.timestamp,
        record.scenario,
        record.approach,
        record.scale,
        record.iteration,
        record.topic,
        record.messageType,
        record.topicBytes,
        record.payloadBytes,
        record.publishedBytes,
        record.subscriberCount,
        record.estimatedDeliveryBytes,
        record.warmup,
      ]
        .map(escapeCsv)
        .join(","),
    )
    .join("\n");

  fs.writeFileSync(
    getCsvPath(logDir),
    body ? `${header}${body}\n` : header,
  );
}

function divideKbPerSecond(bytes: number, durationSeconds: number): number {
  if (!(durationSeconds > 0)) {
    return 0;
  }
  return bytes / durationSeconds / 1024;
}

export function finalizeMqttTrafficArtifacts(
  input: FinalizeMqttTrafficArtifactsInput = {},
) {
  const logDir = resolveLogDir(input.logDir);
  const records = readInternalRecords(logDir);
  const summary = getEmptySummary();

  if (records.length === 0) {
    writeCsv(logDir, []);
    fs.writeFileSync(getSummaryPath(logDir), JSON.stringify(summary, null, 2));
    return summary;
  }

  const firstSuperqueryRecord = records.find(
    (record) => record.messageType === "superquery_result",
  );
  const warmupCutoff = firstSuperqueryRecord?.timestamp ?? null;
  const normalizedRecords = records.map((record) => ({
    ...record,
    warmup:
      warmupCutoff !== null
        ? record.timestamp <= warmupCutoff || Boolean(record.warmup)
        : Boolean(record.warmup),
  }));
  const steadyRecords = normalizedRecords.filter((record) => !record.warmup);
  const aggregationRecords = steadyRecords.length > 0 ? steadyRecords : normalizedRecords;

  if (aggregationRecords.length > 0) {
    const startTimestamp = aggregationRecords[0].timestamp;
    const endTimestamp = aggregationRecords[aggregationRecords.length - 1].timestamp;
    summary.steady_state_duration_seconds = Math.max(
      0,
      (endTimestamp - startTimestamp) / 1000,
    );
  }

  for (const record of aggregationRecords) {
    summary.published_application_bytes += record.publishedBytes;
    summary.estimated_delivery_bytes += record.estimatedDeliveryBytes;

    const categoryPrefix = getSummaryCategoryPrefix(record.messageType);
    const publishedKey = `${categoryPrefix}_published_bytes`;
    const estimatedKey = `${categoryPrefix}_estimated_delivery_bytes`;
    if (publishedKey in summary) {
      summary[publishedKey] += record.publishedBytes;
    }
    if (estimatedKey in summary) {
      summary[estimatedKey] += record.estimatedDeliveryBytes;
    }

    if (record.messageType === "raw_input_stream") {
      summary.raw_input_subscriber_count = Math.max(
        summary.raw_input_subscriber_count,
        record.subscriberCount,
      );
    }
    if (record.messageType === "chunk_result") {
      summary.chunk_result_count += 1;
    }
  }

  summary.published_bandwidth_kb_s = divideKbPerSecond(
    summary.published_application_bytes,
    summary.steady_state_duration_seconds,
  );
  summary.estimated_delivery_bandwidth_kb_s = divideKbPerSecond(
    summary.estimated_delivery_bytes,
    summary.steady_state_duration_seconds,
  );
  summary.chunk_bandwidth_kb_s = divideKbPerSecond(
    summary.chunk_result_estimated_delivery_bytes,
    summary.steady_state_duration_seconds,
  );
  summary.reuse_layer_published_bytes =
    summary.subquery_result_published_bytes +
    summary.reusable_result_published_bytes +
    summary.chunk_result_published_bytes +
    summary.superquery_result_published_bytes +
    summary.control_published_bytes;
  summary.reuse_layer_estimated_delivery_bytes =
    summary.subquery_result_estimated_delivery_bytes +
    summary.reusable_result_estimated_delivery_bytes +
    summary.chunk_result_estimated_delivery_bytes +
    summary.superquery_result_estimated_delivery_bytes +
    summary.control_estimated_delivery_bytes;
  summary.reuse_layer_bandwidth_kb_s = divideKbPerSecond(
    summary.reuse_layer_estimated_delivery_bytes,
    summary.steady_state_duration_seconds,
  );

  writeCsv(logDir, normalizedRecords);
  fs.writeFileSync(getSummaryPath(logDir), JSON.stringify(summary, null, 2));
  return summary;
}
