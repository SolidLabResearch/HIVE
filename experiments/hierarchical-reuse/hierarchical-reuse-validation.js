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

function readCsv(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  const lines = fs.readFileSync(filePath, "utf8").trim().split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    return Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""]));
  });
}

function isRegularFile(filePath) {
  try {
    const stat = fs.lstatSync(filePath);
    return stat.isFile() && !stat.isSymbolicLink();
  } catch {
    return false;
  }
}

function distinct(values) {
  return [...new Set(values.filter((value) => value !== undefined && value !== null && value !== ""))];
}

function validateRequiredFile(filePath, failures, label) {
  if (!isRegularFile(filePath)) {
    failures.push(`${label} missing or not regular file: ${path.basename(filePath)}`);
    return false;
  }
  return true;
}

function validateHierarchicalReuseRun(runRoot, kValue, options = {}) {
  const failures = [];
  const summaryPath = path.join(runRoot, "hierarchical_reuse_summary.json");
  const topologyPath = path.join(runRoot, "hierarchical_reuse_topology.json");
  const chunkProducerPath = path.join(runRoot, "chunk_producer_topology.json");
  const reconstructionPath = path.join(runRoot, "chunk_reconstruction_events.ndjson");
  const registryPath = path.join(runRoot, "active_final_query_registry.json");
  const cachePath = path.join(runRoot, "exact_final_result_cache.ndjson");
  const subscriberPath = path.join(runRoot, "exact_final_subscriber_events.ndjson");
  const deliveryPath = path.join(runRoot, "exact_final_delivery_events.ndjson");
  const capSummaryPath = path.join(runRoot, "benchmark_window_cap_summary.json");

  for (const [filePath, label] of [
    [summaryPath, "hierarchical summary"],
    [topologyPath, "hierarchical topology"],
    [chunkProducerPath, "chunk producer topology"],
    [reconstructionPath, "reconstruction events"],
    [registryPath, "active final-query registry"],
    [cachePath, "final-result cache"],
    [subscriberPath, "subscriber events"],
    [deliveryPath, "delivery events"],
    [capSummaryPath, "benchmark cap summary"],
  ]) {
    validateRequiredFile(filePath, failures, label);
  }

  const summary = readJson(summaryPath) || {};
  const topology = readJson(topologyPath) || {};
  const chunkProducerTopology = readJson(chunkProducerPath) || {};
  const registry = readJson(registryPath) || {};
  const reconstructionEvents = readNdjson(reconstructionPath);
  const cacheEntries = readNdjson(cachePath);
  const subscriberEvents = readNdjson(subscriberPath);
  const deliveryEvents = readNdjson(deliveryPath);
  const capSummary = readJson(capSummaryPath) || {};
  const expectedReuseHits = Math.max(0, kValue - 1);

  const expectedSummary = {
    source: "chunked-reconstruction",
    finalResultSource: "chunked-reconstruction",
    chunkProducerCount: 2,
    uniqueFinalQueryCount: 1,
    reconstructionWorkerCount: 1,
    reconstructionExecutionCount: 1,
    directFinalQueryExecutionCount: 0,
    cacheEntries: 1,
    subscribers: kValue,
    reuseHits: expectedReuseHits,
    deliveries: kValue,
    exactResults: kValue,
    expectedConsumers: kValue,
    allConsumersDelivered: true,
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    producedByDirectFinalQuery: false,
  };
  for (const [field, expected] of Object.entries(expectedSummary)) {
    if (summary[field] !== expected) {
      failures.push(`${field}=${summary[field]}, expected ${expected}`);
    }
  }

  if (topology.directFinalQueryExecutionCount !== 0) {
    failures.push(`topology directFinalQueryExecutionCount=${topology.directFinalQueryExecutionCount}, expected 0`);
  }
  if (chunkProducerTopology.chunkProducerCount !== 2) {
    failures.push(`chunkProducerCount=${chunkProducerTopology.chunkProducerCount}, expected 2`);
  }
  if (reconstructionEvents.length !== 1) {
    failures.push(`reconstruction events=${reconstructionEvents.length}, expected 1`);
  }
  if (cacheEntries.length !== 1) {
    failures.push(`cache entries=${cacheEntries.length}, expected 1`);
  }
  if (subscriberEvents.length !== kValue) {
    failures.push(`subscriber events=${subscriberEvents.length}, expected ${kValue}`);
  }
  if (deliveryEvents.length !== kValue) {
    failures.push(`delivery events=${deliveryEvents.length}, expected ${kValue}`);
  }
  if (capSummary.directFinalQueryExecutionCount !== 0) {
    failures.push(`cap summary directFinalQueryExecutionCount=${capSummary.directFinalQueryExecutionCount}, expected 0`);
  }

  const registryEntries = registry.activeFinalQueries || [];
  if (registryEntries.length !== 1) {
    failures.push(`active final-query registry entries=${registryEntries.length}, expected 1`);
  }
  const canonicalIds = distinct([
    summary.canonical_query_id,
    ...reconstructionEvents.map((entry) => entry.canonical_query_id),
    ...cacheEntries.map((entry) => entry.canonical_query_id),
    ...subscriberEvents.map((entry) => entry.canonical_query_id),
    ...deliveryEvents.map((entry) => entry.canonical_query_id),
  ]);
  if (canonicalIds.length !== 1) {
    failures.push(`canonical query ids=${canonicalIds.length}, expected 1`);
  }
  const reconstructionWorkers = distinct(reconstructionEvents.map((entry) => entry.worker_id));
  if (reconstructionWorkers.length !== 1) {
    failures.push(`reconstruction worker ids=${reconstructionWorkers.length}, expected 1`);
  }
  const deliveryConsumers = distinct(deliveryEvents.map((entry) => entry.consumer_id));
  if (deliveryConsumers.length !== kValue) {
    failures.push(`delivery consumers=${deliveryConsumers.length}, expected ${kValue}`);
  }
  const subscriberConsumers = distinct(subscriberEvents.map((entry) => entry.consumer_id));
  if (subscriberConsumers.length !== kValue) {
    failures.push(`subscriber consumers=${subscriberConsumers.length}, expected ${kValue}`);
  }
  const reuseHits = subscriberEvents.filter((entry) => entry.reuse_hit === true).length;
  if (reuseHits !== expectedReuseHits) {
    failures.push(`subscriber reuse hits=${reuseHits}, expected ${expectedReuseHits}`);
  }
  const creatorMisses = subscriberEvents.filter((entry) => entry.reuse_hit === false).length;
  if (creatorMisses !== 1) {
    failures.push(`subscriber reuse misses=${creatorMisses}, expected 1`);
  }

  const latencyFiles = [];
  for (let index = 1; index <= kValue; index += 1) {
    const latencyPath = path.join(runRoot, `chunked_exact_final_latency_log_consumer_${index}.csv`);
    latencyFiles.push(latencyPath);
    validateRequiredFile(latencyPath, failures, "consumer latency evidence");
    const rows = readCsv(latencyPath);
    const row = rows.find((entry) =>
      entry.coverage_complete === "true" &&
      entry.is_partial_window === "false" &&
      entry.is_comparable_window === "true"
    );
    if (!row) {
      failures.push(`consumer ${index} missing complete comparable latency row`);
    }
  }

  const expectedValue = options.expectedValue;
  const valueTolerance = options.valueTolerance ?? 0;
  const errors = [];
  if (expectedValue !== undefined) {
    for (const entry of deliveryEvents) {
      const actual = Number(entry.result_value);
      if (!Number.isFinite(actual)) {
        failures.push(`delivery result non-numeric for ${entry.consumer_id}`);
        continue;
      }
      const error = Math.abs(actual - expectedValue);
      errors.push(error);
      if (error > valueTolerance) {
        failures.push(`delivery result ${actual} differs from expected ${expectedValue}`);
      }
    }
  }

  return {
    ok: failures.length === 0,
    failures,
    summary,
    topology,
    chunkProducerTopology,
    reconstructionEvents,
    registry,
    cacheEntries,
    subscriberEvents,
    deliveryEvents,
    latencyFiles,
    metrics: {
      exactCount: expectedValue === undefined
        ? deliveryEvents.length
        : errors.filter((value) => value <= valueTolerance).length,
      expectedCount: kValue,
      mae: errors.length ? errors.reduce((sum, value) => sum + value, 0) / errors.length : 0,
      maxAbsoluteError: errors.length ? Math.max(...errors) : 0,
      uniqueConsumers: deliveryConsumers.length,
      uniqueCanonicalQueries: canonicalIds.length,
      uniqueReconstructionWorkers: reconstructionWorkers.length,
    },
  };
}

module.exports = {
  readNdjson,
  validateHierarchicalReuseRun,
};
