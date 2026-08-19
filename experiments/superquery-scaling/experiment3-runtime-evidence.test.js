const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  discoverFetchingOracle,
  summarizePhysicalFinalComputations,
  compareAgainstFetchingOracle,
} = require("./experiment3-runtime-evidence");

function oracle(value = 42, overrides = {}) {
  return {
    summaryVersion: 1, consumerIndex: 1, emittedFinalWindowCount: 1,
    windowNumber: 1, windowStart: 100, windowEnd: 220, coverageComplete: true,
    isPartialWindow: false, isComparableWindow: true, resultValue: value,
    resultEmittedAt: 300, stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached", ...overrides,
  };
}
function write(root, name, value) { fs.writeFileSync(path.join(root, name), JSON.stringify(value)); }
function root() { return fs.mkdtempSync(path.join(os.tmpdir(), "e3-runtime-evidence-")); }

describe("durable Fetching oracle discovery", () => {
  test.each([0, 1, 17])("discovers consumer %i by validated content, not filename", (index) => {
    const dir = root();
    write(dir, `benchmark_window_cap_summary_consumer_${index}.json`, oracle());
    expect(discoverFetchingOracle(dir).resultValue).toBe(42);
  });
  test("fails closed for no valid oracle and ignores irrelevant artifacts", () => {
    const dir = root();
    write(dir, "benchmark_window_cap_summary_consumer_9.json", oracle(42, { coverageComplete: false }));
    write(dir, "unrelated.json", oracle(77));
    expect(() => discoverFetchingOracle(dir)).toThrow("No valid durable Fetching oracle");
  });
  test("deduplicates identical validated oracle artifacts and rejects conflicts", () => {
    const dir = root();
    write(dir, "benchmark_window_cap_summary_consumer_1.json", oracle());
    write(dir, "benchmark_window_cap_summary_consumer_4.json", oracle());
    expect(discoverFetchingOracle(dir).artifactPaths).toHaveLength(2);
    write(dir, "benchmark_window_cap_summary_consumer_8.json", oracle(43));
    expect(() => discoverFetchingOracle(dir)).toThrow("Conflicting durable Fetching oracles");
  });
});

test("exact Chunked comparison uses the discovered durable Fetching oracle", () => {
  const dir = root();
  write(dir, "benchmark_window_cap_summary_consumer_17.json", oracle(10));
  const comparison = compareAgainstFetchingOracle([10 + 1e-12, 10 - 1e-12], discoverFetchingOracle(dir).resultValue);
  expect(comparison.exactCount).toBe(2);
  expect(comparison.maxAbsoluteError).toBeLessThanOrEqual(1e-9);
});

describe("physical final computation aggregation", () => {
  const profile = (role, counters, artifactPath) => ({ processRole: role, counters, artifactPath });
  test("counts legacy and optimized Chunked roles while ignoring deliveries and irrelevant roles", () => {
    const result = summarizePhysicalFinalComputations([
      profile("chunked_bee_worker", { reconstructed_superquery_results: 1, emitted_results: 1 }, "legacy"),
      profile("shared_chunked_reconstruction_runtime", { reconstructed_superquery_results: 1, emitted_results: 1, chunked_logical_fanout_deliveries: 32 }, "optimized"),
      profile("manager_owned_subquery_producer", { reconstructed_superquery_results: 99 }, "producer"),
    ], "chunked");
    expect(result.reconstructedSuperqueryResults).toBe(2);
    expect(result.emittedResults).toBe(2);
  });
  test("deduplicates duplicate profile artifact records", () => {
    const result = summarizePhysicalFinalComputations([
      profile("shared_chunked_reconstruction_runtime", { reconstructed_superquery_results: 1 }, "same-artifact"),
      profile("shared_chunked_reconstruction_runtime", { reconstructed_superquery_results: 1 }, "same-artifact"),
    ], "chunked");
    expect(result.reconstructedSuperqueryResults).toBe(1);
  });
  test("keeps Approximation aggregation unchanged", () => {
    expect(summarizePhysicalFinalComputations([
      profile("approximation_bee_worker", { emitted_results: 1 }, "approx"),
      profile("shared_chunked_reconstruction_runtime", { emitted_results: 4 }, "chunk"),
    ], "approximation").emittedResults).toBe(1);
  });
});
