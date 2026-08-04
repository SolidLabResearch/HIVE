const fs = require("fs");
const path = require("path");
const {
  readNdjson,
  validateHierarchicalReuseRun,
} = require("../hierarchical-reuse/hierarchical-reuse-validation");

function readJson(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
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

function validateApproximationReuseRun(runRoot, kValue, options = {}) {
  const failures = [];
  const summaryPath = path.join(runRoot, "approximation_reuse_summary.json");
  const executionPath = path.join(runRoot, "approximation_execution_events.ndjson");
  const registryPath = path.join(runRoot, "active_approximation_query_registry.json");
  const cachePath = path.join(runRoot, "approximation_result_cache.ndjson");
  const subscriberPath = path.join(runRoot, "approximation_subscriber_events.ndjson");
  const deliveryPath = path.join(runRoot, "approximation_delivery_events.ndjson");
  const capSummaryPath = path.join(runRoot, "benchmark_window_cap_summary.json");

  for (const [filePath, label] of [
    [summaryPath, "approximation reuse summary"],
    [executionPath, "approximation execution events"],
    [registryPath, "active approximation registry"],
    [cachePath, "approximation result cache"],
    [subscriberPath, "approximation subscriber events"],
    [deliveryPath, "approximation delivery events"],
    [capSummaryPath, "benchmark cap summary"],
  ]) {
    validateRequiredFile(filePath, failures, label);
  }

  const summary = readJson(summaryPath) || {};
  const executionEvents = readNdjson(executionPath);
  const registry = readJson(registryPath) || {};
  const cacheEntries = readNdjson(cachePath);
  const subscriberEvents = readNdjson(subscriberPath);
  const deliveryEvents = readNdjson(deliveryPath);
  const capSummary = readJson(capSummaryPath) || {};
  const expectedReuseHits = Math.max(0, kValue - 1);

  const expectedSummary = {
    source: "shared-approximation",
    uniqueApproximationQueryCount: 1,
    approximationWorkerCount: 1,
    approximationExecutionCount: 1,
    cacheEntries: 1,
    subscribers: kValue,
    reuseHits: expectedReuseHits,
    deliveries: kValue,
    comparableResults: kValue,
    expectedConsumers: kValue,
    allConsumersDelivered: true,
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
  };
  for (const [field, expected] of Object.entries(expectedSummary)) {
    if (summary[field] !== expected) {
      failures.push(`${field}=${summary[field]}, expected ${expected}`);
    }
  }

  if (executionEvents.length !== 1) {
    failures.push(`approximation execution events=${executionEvents.length}, expected 1`);
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
  if (capSummary.approximationExecutionCount !== 1) {
    failures.push(`cap summary approximationExecutionCount=${capSummary.approximationExecutionCount}, expected 1`);
  }
  if (capSummary.cacheEntryCount !== 1) {
    failures.push(`cap summary cacheEntryCount=${capSummary.cacheEntryCount}, expected 1`);
  }

  const registryEntries = registry.activeApproximationQueries || [];
  if (registryEntries.length !== 1) {
    failures.push(`active approximation registry entries=${registryEntries.length}, expected 1`);
  }
  const canonicalIds = distinct([
    summary.canonical_query_id,
    ...executionEvents.map((entry) => entry.canonical_query_id),
    ...cacheEntries.map((entry) => entry.canonical_query_id),
    ...subscriberEvents.map((entry) => entry.canonical_query_id),
    ...deliveryEvents.map((entry) => entry.canonical_query_id),
  ]);
  if (canonicalIds.length !== 1) {
    failures.push(`canonical query ids=${canonicalIds.length}, expected 1`);
  }
  const executionIds = distinct([
    ...executionEvents.map((entry) => entry.execution_id || entry.worker_id),
    ...subscriberEvents.map((entry) => entry.execution_id),
    ...cacheEntries.map((entry) => entry.approximation_execution_id),
  ]);
  if (executionIds.length !== 1) {
    failures.push(`approximation execution ids=${executionIds.length}, expected 1`);
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

  for (let index = 1; index <= kValue; index += 1) {
    const latencyPath = path.join(runRoot, `approximation_shared_latency_log_consumer_${index}.csv`);
    validateRequiredFile(latencyPath, failures, "approximation shared latency evidence");
    const rows = readCsv(latencyPath);
    const row = rows.find((entry) =>
      entry.coverage_complete === "true" &&
      entry.is_partial_window === "false" &&
      entry.is_comparable_window === "true"
    );
    if (!row) {
      failures.push(`consumer ${index} missing complete comparable approximation latency row`);
    }
  }

  const expectedValue = options.expectedValue;
  const errors = [];
  if (expectedValue !== undefined) {
    for (const entry of deliveryEvents) {
      const actual = Number(entry.result_value);
      if (!Number.isFinite(actual)) {
        failures.push(`delivery result non-numeric for ${entry.consumer_id}`);
        continue;
      }
      errors.push(Math.abs(actual - expectedValue));
    }
  }

  return {
    ok: failures.length === 0,
    failures,
    summary,
    executionEvents,
    registry,
    cacheEntries,
    subscriberEvents,
    deliveryEvents,
    metrics: {
      comparableCount: deliveryEvents.length,
      expectedCount: kValue,
      mae: errors.length ? errors.reduce((sum, value) => sum + value, 0) / errors.length : 0,
      medianAbsoluteError: errors.length ? [...errors].sort((a, b) => a - b)[Math.floor(errors.length / 2)] : 0,
      maxAbsoluteError: errors.length ? Math.max(...errors) : 0,
      mape: 0,
      maxPercentageError: 0,
      uniqueConsumers: deliveryConsumers.length,
      uniqueCanonicalQueries: canonicalIds.length,
      uniqueApproximationExecutions: executionIds.length,
    },
  };
}

function validateUnifiedSuperqueryRun(runRoot, approach, kValue, options = {}) {
  if (approach === "approximation") {
    return validateApproximationReuseRun(runRoot, kValue, options);
  }
  if (approach === "chunked") {
    return validateHierarchicalReuseRun(runRoot, kValue, options);
  }
  throw new Error(`No unified superquery validator for approach ${approach}`);
}

module.exports = {
  readJson,
  readCsv,
  readNdjson,
  validateApproximationReuseRun,
  validateUnifiedSuperqueryRun,
};
