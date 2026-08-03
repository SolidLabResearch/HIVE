const fs = require("fs");
const os = require("os");
const path = require("path");
const { validateExactFinalRun } = require("./exact-final-validation");

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function appendNdjson(filePath, value) {
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`);
}

function writeValidRun(root, K, options = {}) {
  fs.mkdirSync(root, { recursive: true });
  const sharedExecutionId = "shared-execution-1";
  const resultId = "result-1";
  const value = options.value ?? 42.25;
  writeJson(path.join(root, "exact_final_topology.json"), {
    sharedQueryExecutionCount: 1,
    BeeWorkerCount: options.beeWorkerCount ?? 0,
    subscriberCount: K,
    deliveryEventCount: K,
    uniqueConsumerCount: K,
    cachedFinalResultCount: 1,
  });
  appendNdjson(path.join(root, "exact_final_result_cache.ndjson"), {
    sharedExecutionId,
    resultId,
    resultValue: value,
    sourceResultTimestamp: 1000,
  });
  for (let index = 1; index <= K; index += 1) {
    appendNdjson(path.join(root, "exact_final_delivery_events.ndjson"), {
      consumerId: `consumer_${index}`,
      subscriptionId: `subscription_${index}`,
      sharedExecutionId,
      resultId,
      resultValue: value,
      deliveryTimestamp: 2000 + index,
      sourceResultTimestamp: 1000,
      K,
      iteration: 1,
    });
    fs.writeFileSync(
      path.join(root, `exact_final_latency_log_consumer_${index}.csv`),
      "window_number,result_value\n1,42.25\n",
    );
  }
}

describe("exact-final S1 delivery validation", () => {
  for (const K of [1, 2, 10, 32]) {
    test(`validates one shared execution and ${K} real deliveries`, () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), `exact-final-K${K}-`));
      writeValidRun(root, K);

      const validation = validateExactFinalRun(root, K, { expectedValue: 42.25 });

      expect(validation.ok).toBe(true);
      expect(validation.metrics.exactCount).toBe(K);
      expect(validation.metrics.uniqueConsumers).toBe(K);
      expect(validation.metrics.uniqueSubscriptions).toBe(K);
      expect(validation.metrics.uniqueSharedExecutions).toBe(1);
      expect(validation.metrics.uniqueResultIds).toBe(1);
      expect(validation.topology.sharedQueryExecutionCount).toBe(1);
      expect(validation.topology.BeeWorkerCount).toBeLessThanOrEqual(1);
    });
  }

  test("keeps multi-digit consumer identifiers distinct", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "exact-final-multidigit-"));
    writeValidRun(root, 10);

    const validation = validateExactFinalRun(root, 10, { expectedValue: 42.25 });

    expect(validation.ok).toBe(true);
    expect(validation.deliveries.map((entry) => entry.consumerId)).toContain("consumer_10");
    expect(validation.metrics.uniqueConsumers).toBe(10);
  });

  test("rejects symlink latency evidence", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "exact-final-symlink-"));
    writeValidRun(root, 2);
    fs.unlinkSync(path.join(root, "exact_final_latency_log_consumer_2.csv"));
    fs.symlinkSync(
      "exact_final_latency_log_consumer_1.csv",
      path.join(root, "exact_final_latency_log_consumer_2.csv"),
    );

    const validation = validateExactFinalRun(root, 2, { expectedValue: 42.25 });

    expect(validation.ok).toBe(false);
    expect(validation.failures.join("\n")).toMatch(/symlink/);
  });

  test("rejects K-dependent BeeWorker counts and missing deliveries", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "exact-final-invalid-"));
    writeValidRun(root, 10, { beeWorkerCount: 10 });
    fs.truncateSync(path.join(root, "exact_final_delivery_events.ndjson"), 0);

    const validation = validateExactFinalRun(root, 10, { expectedValue: 42.25 });

    expect(validation.ok).toBe(false);
    expect(validation.failures.join("\n")).toMatch(/BeeWorkerCount/);
    expect(validation.failures.join("\n")).toMatch(/delivery records=0/);
  });

  test("requires exact delivered values", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "exact-final-value-"));
    writeValidRun(root, 1, { value: 41 });

    const validation = validateExactFinalRun(root, 1, { expectedValue: 42.25 });

    expect(validation.ok).toBe(false);
    expect(validation.metrics.exactCount).toBe(0);
    expect(validation.metrics.maxAbsoluteError).toBeCloseTo(1.25);
  });
});
