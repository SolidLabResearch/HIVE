#!/usr/bin/env node

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  measureMqttPublish,
  recordPublishedMqttMessage,
  finalizeMqttTrafficArtifacts,
} = require("../../dist/util/mqttTraffic");

const tempDir = fs.mkdtempSync(
  path.join(os.tmpdir(), "mqtt-traffic-verify-"),
);

function run() {
  const topic = "bench/test/wearableX";
  const topicBytes = Buffer.byteLength(topic, "utf8");

  const bufferPayload = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
  const bufferMeasure = measureMqttPublish(topic, bufferPayload);
  assert.strictEqual(bufferMeasure.topicBytes, topicBytes);
  assert.strictEqual(bufferMeasure.payloadBytes, 4);
  assert.strictEqual(bufferMeasure.publishedBytes, topicBytes + 4);

  const stringPayload = "abc";
  const stringMeasure = measureMqttPublish(topic, stringPayload);
  assert.strictEqual(stringMeasure.payloadBytes, 3);

  const objectPayload = { value: 12.5, unit: "m/s2" };
  const serializedObjectPayload = JSON.stringify(objectPayload);
  const objectMeasure = measureMqttPublish(topic, objectPayload);
  assert.strictEqual(
    objectMeasure.payloadBytes,
    Buffer.byteLength(serializedObjectPayload, "utf8"),
  );

  recordPublishedMqttMessage({
    logDir: tempDir,
    topic,
    payload: bufferPayload,
    messageType: "raw_input_stream",
    subscriberCount: 2,
    timestamp: 1000,
  });
  recordPublishedMqttMessage({
    logDir: tempDir,
    topic: "benchmark/results/approximation/test",
    payload: JSON.stringify({ value: 1 }),
    messageType: "superquery_result",
    subscriberCount: 1,
    timestamp: 2000,
    warmup: true,
  });
  recordPublishedMqttMessage({
    logDir: tempDir,
    topic: "benchmark/results/approximation/test",
    payload: JSON.stringify({ value: 2 }),
    messageType: "superquery_result",
    subscriberCount: 1,
    timestamp: 3000,
  });

  const summary = finalizeMqttTrafficArtifacts({ logDir: tempDir });
  const expectedSuperqueryBytes =
    Buffer.byteLength("benchmark/results/approximation/test", "utf8") +
    Buffer.byteLength(JSON.stringify({ value: 2 }), "utf8");

  assert.strictEqual(summary.superquery_result_published_bytes, expectedSuperqueryBytes);
  assert.strictEqual(summary.raw_input_published_bytes, 0);
  assert.strictEqual(summary.raw_input_subscriber_count, 0);
  assert.ok(fs.existsSync(path.join(tempDir, "mqtt_traffic.csv")));
  assert.ok(fs.existsSync(path.join(tempDir, "mqtt_traffic_summary.json")));

  console.log(`mqtt traffic verification passed: ${tempDir}`);
}

run();
