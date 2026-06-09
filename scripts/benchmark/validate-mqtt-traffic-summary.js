#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exitCode = 1;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function main() {
  const targetDir = process.argv[2];
  if (!targetDir) {
    console.error("Usage: node scripts/benchmark/validate-mqtt-traffic-summary.js <iteration-dir>");
    process.exit(1);
  }

  const summaryPath = path.join(targetDir, "mqtt_traffic_summary.json");
  if (!fs.existsSync(summaryPath)) {
    console.error(`Missing summary: ${summaryPath}`);
    process.exit(1);
  }

  const summary = readJson(summaryPath);
  const rawPublished = Number(summary.raw_input_published_bytes || 0);
  const rawSubscriberCount = Number(summary.raw_input_subscriber_count || 0);
  const rawEstimated = Number(summary.raw_input_estimated_delivery_bytes || 0);
  const expectedRawEstimated = rawPublished * rawSubscriberCount;
  const delta = Math.abs(rawEstimated - expectedRawEstimated);

  if (!(summary.steady_state_duration_seconds > 0)) {
    fail(`steady_state_duration_seconds must be > 0 in ${summaryPath}`);
  }

  if (delta > 0.5) {
    fail(
      `raw_input_estimated_delivery_bytes mismatch in ${summaryPath}: expected ${expectedRawEstimated}, got ${rawEstimated}`,
    );
  } else {
    console.log(
      `PASS: raw fan-out validated for ${summaryPath} (${rawPublished} * ${rawSubscriberCount} = ${rawEstimated})`,
    );
  }
}

main();
