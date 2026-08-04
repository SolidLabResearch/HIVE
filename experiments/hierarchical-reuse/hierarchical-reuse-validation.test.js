const fs = require("fs");
const os = require("os");
const path = require("path");
const { validateHierarchicalReuseRun } = require("./hierarchical-reuse-validation");

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
      "window_number,consumer_query_registered_at,reconstruction_worker_created_at,reconstruction_ready_at,last_required_observation_received_at,shared_result_created_at,cache_entry_created_at,consumer_result_delivered_at,consumer_query_to_result_ms,shared_reconstruction_to_result_ms,last_observation_to_shared_result_ms,fanout_delivery_latency_ms,reuse_lookup_latency_ms,subscriber_attachment_latency_ms,window_start,window_end,window_duration_ms,coverage_complete,is_partial_window,is_comparable_window,result_value,consumer_id,subscription_id,canonical_query_id,result_id",
      `1,1000,1000,1200,1300,1300,1301,130${consumerIndex},30${consumerIndex},300,0,${consumerIndex},0,0,10,130,120,true,false,true,42,consumer_${consumerIndex},sub_${consumerIndex},qid,result`,
    ].join("\n") + "\n",
  );
}

function makeRunRoot(kValue, mutator = () => {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "h1-validation-"));
  const summary = {
    experiment: "H1: Hierarchical Chunked and Exact-Final Reuse",
    approach: "chunked-exact-final",
    scenario_id: `K${kValue}-iteration1`,
    canonical_query_id: "qid",
    source: "chunked-reconstruction",
    chunkProducerCount: 2,
    uniqueFinalQueryCount: 1,
    reconstructionWorkerCount: 1,
    reconstructionExecutionCount: 1,
    directFinalQueryExecutionCount: 0,
    cacheEntries: 1,
    subscribers: kValue,
    reuseHits: Math.max(0, kValue - 1),
    deliveries: kValue,
    exactResults: kValue,
    expectedConsumers: kValue,
    allConsumersDelivered: true,
    finalResultSource: "chunked-reconstruction",
    targetWindowCount: 1,
    emittedFinalWindowCount: 1,
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    producedByDirectFinalQuery: false,
    aggregateWrittenAt: "2026-08-04T00:00:00.000Z",
  };
  mutator(summary, root);
  writeJson(path.join(root, "hierarchical_reuse_summary.json"), summary);
  writeJson(path.join(root, "hierarchical_reuse_topology.json"), summary);
  writeJson(path.join(root, "chunk_producer_topology.json"), {
    chunkProducerCount: 2,
  });
  writeJson(path.join(root, "active_final_query_registry.json"), {
    activeFinalQueries: [{
      canonicalQueryId: "qid",
      source: "chunked-reconstruction",
      reconstructionWorkerId: "worker_1",
      finalResultTopic: "topic",
      cacheEntryCount: 1,
      subscribers: Array.from({ length: kValue }, (_, index) => `consumer_${index + 1}`),
    }],
  });
  writeJson(path.join(root, "benchmark_window_cap_summary.json"), {
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    emittedFinalWindowCount: 1,
    targetWindowCount: 1,
    directFinalQueryExecutionCount: 0,
  });
  appendNdjson(path.join(root, "chunk_reconstruction_events.ndjson"), {
    scenario_id: `K${kValue}-iteration1`,
    canonical_query_id: "qid",
    worker_id: "worker_1",
    consumer_that_created_it: "consumer_1",
    output_topic: "topic",
  });
  appendNdjson(path.join(root, "exact_final_result_cache.ndjson"), {
    canonical_query_id: "qid",
    result_id: "result",
    result_value: 42,
  });
  for (let index = 1; index <= kValue; index += 1) {
    appendNdjson(path.join(root, "exact_final_subscriber_events.ndjson"), {
      canonical_query_id: "qid",
      consumer_id: `consumer_${index}`,
      reuse_hit: index > 1,
    });
    appendNdjson(path.join(root, "exact_final_delivery_events.ndjson"), {
      canonical_query_id: "qid",
      consumer_id: `consumer_${index}`,
      result_value: 42,
      delivery_timestamp: 1300 + index,
    });
    writeLatency(path.join(root, `chunked_exact_final_latency_log_consumer_${index}.csv`), index);
  }
  return root;
}

describe("validateHierarchicalReuseRun", () => {
  test.each([1, 2, 10, 32])("accepts valid K=%i topology", (kValue) => {
    const root = makeRunRoot(kValue);
    const validation = validateHierarchicalReuseRun(root, kValue, {
      expectedValue: 42,
      valueTolerance: 0,
    });

    expect(validation.ok).toBe(true);
    expect(validation.metrics.exactCount).toBe(kValue);
  });

  test("rejects missing delivery", () => {
    const root = makeRunRoot(2);
    fs.writeFileSync(path.join(root, "exact_final_delivery_events.ndjson"), "");
    appendNdjson(path.join(root, "exact_final_delivery_events.ndjson"), {
      canonical_query_id: "qid",
      consumer_id: "consumer_1",
      result_value: 42,
    });

    const validation = validateHierarchicalReuseRun(root, 2);

    expect(validation.ok).toBe(false);
    expect(validation.failures).toEqual(expect.arrayContaining([
      "delivery events=1, expected 2",
      "delivery consumers=1, expected 2",
    ]));
  });

  test("rejects duplicate reconstruction worker", () => {
    const root = makeRunRoot(2, (summary) => {
      summary.reconstructionWorkerCount = 2;
      summary.reconstructionExecutionCount = 2;
    });
    appendNdjson(path.join(root, "chunk_reconstruction_events.ndjson"), {
      canonical_query_id: "qid",
      worker_id: "worker_2",
    });

    const validation = validateHierarchicalReuseRun(root, 2);

    expect(validation.ok).toBe(false);
    expect(validation.failures).toEqual(expect.arrayContaining([
      "reconstructionWorkerCount=2, expected 1",
      "reconstruction events=2, expected 1",
    ]));
  });

  test("rejects direct-query fallback", () => {
    const root = makeRunRoot(1, (summary) => {
      summary.directFinalQueryExecutionCount = 1;
      summary.producedByDirectFinalQuery = true;
    });
    writeJson(path.join(root, "benchmark_window_cap_summary.json"), {
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      emittedFinalWindowCount: 1,
      targetWindowCount: 1,
      directFinalQueryExecutionCount: 1,
    });

    const validation = validateHierarchicalReuseRun(root, 1);

    expect(validation.ok).toBe(false);
    expect(validation.failures).toEqual(expect.arrayContaining([
      "directFinalQueryExecutionCount=1, expected 0",
      "producedByDirectFinalQuery=true, expected false",
      "cap summary directFinalQueryExecutionCount=1, expected 0",
    ]));
  });
});
