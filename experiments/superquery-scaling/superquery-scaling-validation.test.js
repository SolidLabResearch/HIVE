const fs = require("fs");
const os = require("os");
const path = require("path");
const { validateApproximationReuseRun } = require("./superquery-scaling-validation");

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function appendNdjson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.appendFileSync(filePath, `${JSON.stringify(value)}\n`);
}

function writeLatency(filePath, consumerIndex) {
  fs.writeFileSync(
    filePath,
    [
      "window_number,consumer_query_registered_at,shared_execution_started_at,last_required_observation_received_at,shared_result_created_at,cache_entry_created_at,consumer_result_delivered_at,consumer_query_to_result_ms,shared_execution_to_result_ms,last_observation_to_shared_result_ms,fanout_delivery_latency_ms,reuse_lookup_latency_ms,subscriber_attachment_latency_ms,window_start,window_end,window_duration_ms,coverage_complete,is_partial_window,is_comparable_window,result_value,consumer_id,subscription_id,canonical_query_id,result_id",
      `1,1000,1000,1300,1300,1301,130${consumerIndex},30${consumerIndex},300,0,${consumerIndex},0,0,10,130,120,true,false,true,40,consumer_${consumerIndex},sub_${consumerIndex},qid,result`,
    ].join("\n") + "\n",
  );
}

function makeApproximationRunRoot(kValue, mutator = () => {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "e3-approx-validation-"));
  const summary = {
    experiment: "Experiment 3: Increasing Number of Same Superqueries",
    approach: "approximation",
    scenario_id: `K${kValue}-iteration1`,
    canonical_query_id: "qid",
    source: "shared-approximation",
    uniqueApproximationQueryCount: 1,
    approximationWorkerCount: 1,
    approximationExecutionCount: 1,
    cacheEntries: 1,
    subscribers: kValue,
    reuseHits: Math.max(0, kValue - 1),
    deliveries: kValue,
    comparableResults: kValue,
    expectedConsumers: kValue,
    allConsumersDelivered: true,
    targetWindowCount: 1,
    emittedFinalWindowCount: 1,
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    aggregateWrittenAt: "2026-08-04T00:00:00.000Z",
    approximationConfiguration: {
      completedWindowMode: true,
      earlyTriggerMode: false,
      policy: "rate-based-completed-window",
      rate: null,
    },
  };
  mutator(summary, root);
  writeJson(path.join(root, "approximation_reuse_summary.json"), summary);
  writeJson(path.join(root, "active_approximation_query_registry.json"), {
    activeApproximationQueries: [{
      canonicalQueryId: "qid",
      source: "shared-approximation",
      approximationExecutionId: "exec_1",
      outputTopic: "topic",
      cacheEntryCount: 1,
      subscribers: Array.from({ length: kValue }, (_, index) => `consumer_${index + 1}`),
    }],
  });
  writeJson(path.join(root, "benchmark_window_cap_summary.json"), {
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    emittedFinalWindowCount: 1,
    targetWindowCount: 1,
    approximationExecutionCount: 1,
    cacheEntryCount: 1,
  });
  appendNdjson(path.join(root, "approximation_execution_events.ndjson"), {
    scenario_id: `K${kValue}-iteration1`,
    canonical_query_id: "qid",
    execution_id: "exec_1",
    worker_id: "exec_1",
    consumer_that_created_it: "consumer_1",
    output_topic: "topic",
  });
  appendNdjson(path.join(root, "approximation_result_cache.ndjson"), {
    canonical_query_id: "qid",
    approximation_execution_id: "exec_1",
    result_id: "result",
    result_value: 40,
  });
  for (let index = 1; index <= kValue; index += 1) {
    appendNdjson(path.join(root, "approximation_subscriber_events.ndjson"), {
      canonical_query_id: "qid",
      consumer_id: `consumer_${index}`,
      execution_id: "exec_1",
      reuse_hit: index > 1,
    });
    appendNdjson(path.join(root, "approximation_delivery_events.ndjson"), {
      canonical_query_id: "qid",
      consumer_id: `consumer_${index}`,
      result_value: 40,
      delivery_timestamp: 1300 + index,
    });
    writeLatency(path.join(root, `approximation_shared_latency_log_consumer_${index}.csv`), index);
  }
  return root;
}

describe("validateApproximationReuseRun", () => {
  test.each([1, 2, 10, 32])("accepts valid K=%i shared approximation topology", (kValue) => {
    const root = makeApproximationRunRoot(kValue);
    const validation = validateApproximationReuseRun(root, kValue, {
      expectedValue: 42,
    });

    expect(validation.ok).toBe(true);
    expect(validation.metrics.comparableCount).toBe(kValue);
    expect(validation.metrics.uniqueApproximationExecutions).toBe(1);
  });

  test("rejects duplicate approximation worker creation", () => {
    const root = makeApproximationRunRoot(2, (summary) => {
      summary.approximationWorkerCount = 2;
      summary.approximationExecutionCount = 2;
    });
    appendNdjson(path.join(root, "approximation_execution_events.ndjson"), {
      canonical_query_id: "qid",
      execution_id: "exec_2",
      worker_id: "exec_2",
    });

    const validation = validateApproximationReuseRun(root, 2);

    expect(validation.ok).toBe(false);
    expect(validation.failures).toEqual(expect.arrayContaining([
      "approximationWorkerCount=2, expected 1",
      "approximation execution events=2, expected 1",
    ]));
  });

  test("rejects missing subscriber delivery", () => {
    const root = makeApproximationRunRoot(2);
    fs.writeFileSync(path.join(root, "approximation_delivery_events.ndjson"), "");
    appendNdjson(path.join(root, "approximation_delivery_events.ndjson"), {
      canonical_query_id: "qid",
      consumer_id: "consumer_1",
      result_value: 40,
    });

    const validation = validateApproximationReuseRun(root, 2);

    expect(validation.ok).toBe(false);
    expect(validation.failures).toEqual(expect.arrayContaining([
      "delivery events=1, expected 2",
      "delivery consumers=1, expected 2",
    ]));
  });

  test("rejects incorrect cache topology", () => {
    const root = makeApproximationRunRoot(1, (summary) => {
      summary.cacheEntries = 0;
    });
    fs.writeFileSync(path.join(root, "approximation_result_cache.ndjson"), "");

    const validation = validateApproximationReuseRun(root, 1);

    expect(validation.ok).toBe(false);
    expect(validation.failures).toEqual(expect.arrayContaining([
      "cacheEntries=0, expected 1",
      "cache entries=0, expected 1",
    ]));
  });
});
