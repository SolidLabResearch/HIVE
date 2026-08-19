const fs = require("fs");
const path = require("path");

function readJson(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readNdjson(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  return fs.readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function isSymlink(filePath) {
  try {
    return fs.lstatSync(filePath).isSymbolicLink();
  } catch {
    return false;
  }
}

function requireRegularEvidenceFile(filePath, failures, label) {
  if (!fs.existsSync(filePath)) {
    failures.push(`${label} missing: ${path.basename(filePath)}`);
    return false;
  }
  const stat = fs.lstatSync(filePath);
  if (stat.isSymbolicLink()) {
    failures.push(`${label} is symlink: ${path.basename(filePath)}`);
    return false;
  }
  if (!stat.isFile()) {
    failures.push(`${label} is not a regular file: ${path.basename(filePath)}`);
    return false;
  }
  return true;
}

function distinctCount(values) {
  return new Set(values.filter((value) => value !== null && value !== undefined && value !== "")).size;
}

function validateExactFinalRun(runRoot, kValue, options = {}) {
  const failures = [];
  const topologyPath = path.join(runRoot, "exact_final_topology.json");
  const deliveryPath = path.join(runRoot, "exact_final_delivery_events.ndjson");
  const cachePath = path.join(runRoot, "exact_final_result_cache.ndjson");

  requireRegularEvidenceFile(topologyPath, failures, "topology evidence");
  requireRegularEvidenceFile(deliveryPath, failures, "delivery evidence");
  requireRegularEvidenceFile(cachePath, failures, "cache evidence");

  const topology = readJson(topologyPath) || {};
  const deliveries = readNdjson(deliveryPath);
  const cacheEntries = readNdjson(cachePath);
  const expectedValue = options.expectedValue;
  const valueTolerance = options.valueTolerance ?? 0;

  if (topology.sharedQueryExecutionCount !== 1) {
    failures.push(`sharedQueryExecutionCount=${topology.sharedQueryExecutionCount}, expected 1`);
  }
  if (!Number.isFinite(topology.BeeWorkerCount) || topology.BeeWorkerCount > 1) {
    failures.push(`BeeWorkerCount=${topology.BeeWorkerCount}, expected <= 1`);
  }
  if (topology.subscriberCount !== kValue) {
    failures.push(`subscriberCount=${topology.subscriberCount}, expected ${kValue}`);
  }
  if (topology.deliveryEventCount !== kValue) {
    failures.push(`deliveryEventCount=${topology.deliveryEventCount}, expected ${kValue}`);
  }
  if (topology.uniqueConsumerCount !== kValue) {
    failures.push(`uniqueConsumerCount=${topology.uniqueConsumerCount}, expected ${kValue}`);
  }
  if (topology.cachedFinalResultCount !== 1) {
    failures.push(`cachedFinalResultCount=${topology.cachedFinalResultCount}, expected 1`);
  }
  if (!Number.isFinite(topology.sharedQueryRegisteredAt)) {
    failures.push("sharedQueryRegisteredAt missing from topology");
  }

  if (deliveries.length !== kValue) {
    failures.push(`delivery records=${deliveries.length}, expected ${kValue}`);
  }
  if (cacheEntries.length !== 1) {
    failures.push(`cache records=${cacheEntries.length}, expected 1`);
  }

  const uniqueConsumers = distinctCount(deliveries.map((entry) => entry.consumerId));
  const uniqueSubscriptions = distinctCount(deliveries.map((entry) => entry.subscriptionId));
  const uniqueDeliveryTimestamps = distinctCount(deliveries.map((entry) => entry.deliveryTimestamp));
  const uniqueSharedExecutions = distinctCount(deliveries.map((entry) => entry.sharedExecutionId));
  const uniqueResultIds = distinctCount(deliveries.map((entry) => entry.resultId));

  if (uniqueConsumers !== kValue) {
    failures.push(`unique delivery consumers=${uniqueConsumers}, expected ${kValue}`);
  }
  if (uniqueSubscriptions !== kValue) {
    failures.push(`unique subscription ids=${uniqueSubscriptions}, expected ${kValue}`);
  }
  if (uniqueDeliveryTimestamps !== kValue) {
    failures.push(`unique delivery timestamps=${uniqueDeliveryTimestamps}, expected ${kValue}`);
  }
  if (uniqueSharedExecutions !== 1) {
    failures.push(`shared execution ids=${uniqueSharedExecutions}, expected 1`);
  }
  if (uniqueResultIds !== 1) {
    failures.push(`result ids=${uniqueResultIds}, expected 1`);
  }

  for (const entry of deliveries) {
    for (const field of [
      "consumerId",
      "subscriptionId",
      "sharedExecutionId",
      "resultId",
      "resultValue",
      "consumerQueryRegisteredAt",
      "sharedQueryRegisteredAt",
      "cacheEntryCreatedAt",
      "deliveryTimestamp",
      "sourceResultTimestamp",
      "K",
      "iteration",
    ]) {
      if (entry[field] === undefined || entry[field] === null || entry[field] === "") {
        failures.push(`delivery missing ${field} for consumer=${entry.consumerId ?? "unknown"}`);
      }
    }
    if (entry.K !== kValue) {
      failures.push(`delivery K=${entry.K}, expected ${kValue}`);
    }
    if (expectedValue !== undefined) {
      const actual = Number(entry.resultValue);
      if (!Number.isFinite(actual) || Math.abs(actual - expectedValue) > valueTolerance) {
        failures.push(`delivery value ${entry.resultValue} differs from expected ${expectedValue}`);
      }
    }
  }

  for (const cacheEntry of cacheEntries) {
    for (const field of [
      "sharedExecutionId",
      "resultId",
      "resultValue",
      "sourceResultTimestamp",
      "cachedAt",
    ]) {
      if (cacheEntry[field] === undefined || cacheEntry[field] === null || cacheEntry[field] === "") {
        failures.push(`cache missing ${field}`);
      }
    }
  }

  const latencyFiles = [];
  for (let consumerIndex = 1; consumerIndex <= kValue; consumerIndex += 1) {
    const latencyPath = path.join(runRoot, `exact_final_latency_log_consumer_${consumerIndex}.csv`);
    latencyFiles.push(latencyPath);
    requireRegularEvidenceFile(latencyPath, failures, "latency evidence");
  }

  const exactCount = expectedValue === undefined
    ? deliveries.length
    : deliveries.filter((entry) => {
        const actual = Number(entry.resultValue);
        return Number.isFinite(actual) && Math.abs(actual - expectedValue) <= valueTolerance;
      }).length;
  const errors = expectedValue === undefined
    ? []
    : deliveries.map((entry) => Math.abs(Number(entry.resultValue) - expectedValue)).filter(Number.isFinite);

  return {
    ok: failures.length === 0,
    failures,
    topology,
    deliveries,
    cacheEntries,
    latencyFiles,
    metrics: {
      exactCount,
      expectedCount: kValue,
      mae: errors.length > 0 ? errors.reduce((sum, value) => sum + value, 0) / errors.length : 0,
      maxAbsoluteError: errors.length > 0 ? Math.max(...errors) : 0,
      uniqueConsumers,
      uniqueSubscriptions,
      uniqueDeliveryTimestamps,
      uniqueSharedExecutions,
      uniqueResultIds,
    },
  };
}

module.exports = {
  isSymlink,
  readNdjson,
  validateExactFinalRun,
};
