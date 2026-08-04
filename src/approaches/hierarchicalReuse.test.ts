import {
  canonicalizeHierarchicalFinalQuery,
  validateHierarchicalSummaryShape,
} from "./hierarchicalReuse";

function query(range = "120000", step = "60000", spacing = "\n"): string {
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>

REGISTER RStream <sensor_averages_1> AS${spacing}
SELECT (AVG(?value) AS ?resultValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE ${range} STEP ${step}]
WHERE {
  WINDOW <mqtt://localhost:1883/wearableX> {
    ?s saref:hasValue ?value .
  }
}
`;
}

describe("hierarchical reuse query identity", () => {
  test("treats equivalent formatting and output stream names as the same final query", () => {
    const first = canonicalizeHierarchicalFinalQuery(query());
    const second = canonicalizeHierarchicalFinalQuery(
      query().replace("<sensor_averages_1>", "<sensor_averages_32>").replace(/\s+/g, " "),
    );

    expect(second.canonicalQueryId).toBe(first.canonicalQueryId);
  });

  test("keeps different window alignment out of the same exact-final identity", () => {
    const baseline = canonicalizeHierarchicalFinalQuery(query("120000", "60000"));
    const differentAlignment = canonicalizeHierarchicalFinalQuery(query("120000", "30000"));

    expect(differentAlignment.canonicalQueryId).not.toBe(baseline.canonicalQueryId);
  });
});

describe("hierarchical reuse summary shape", () => {
  test("accepts one reconstruction and K subscribers", () => {
    const failures = validateHierarchicalSummaryShape({
      experiment: "H1: Hierarchical Chunked and Exact-Final Reuse",
      approach: "chunked-exact-final",
      scenario_id: "K32-iteration1",
      canonical_query_id: "abc",
      source: "chunked-reconstruction",
      chunkProducerCount: 2,
      uniqueFinalQueryCount: 1,
      reconstructionWorkerCount: 1,
      reconstructionExecutionCount: 1,
      directFinalQueryExecutionCount: 0,
      cacheEntries: 1,
      subscribers: 32,
      reuseHits: 31,
      deliveries: 32,
      exactResults: 32,
      expectedConsumers: 32,
      allConsumersDelivered: true,
      finalResultSource: "chunked-reconstruction",
      targetWindowCount: 1,
      emittedFinalWindowCount: 1,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      producedByDirectFinalQuery: false,
      aggregateWrittenAt: "2026-08-04T00:00:00.000Z",
    }, 32);

    expect(failures).toEqual([]);
  });

  test("rejects duplicated reconstruction workers", () => {
    const failures = validateHierarchicalSummaryShape({
      scenario_id: "K2-iteration1",
      canonical_query_id: "abc",
      source: "chunked-reconstruction",
      finalResultSource: "chunked-reconstruction",
      chunkProducerCount: 2,
      uniqueFinalQueryCount: 1,
      reconstructionWorkerCount: 2,
      reconstructionExecutionCount: 2,
      directFinalQueryExecutionCount: 0,
      cacheEntries: 1,
      subscribers: 2,
      reuseHits: 1,
      deliveries: 2,
      exactResults: 2,
      expectedConsumers: 2,
      allConsumersDelivered: true,
      emittedFinalWindowCount: 1,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      producedByDirectFinalQuery: false,
    }, 2);

    expect(failures).toEqual(expect.arrayContaining([
      "reconstructionWorkerCount=2, expected 1",
      "reconstructionExecutionCount=2, expected 1",
    ]));
  });

  test("rejects direct final-query fallback", () => {
    const failures = validateHierarchicalSummaryShape({
      scenario_id: "K1-iteration1",
      canonical_query_id: "abc",
      source: "chunked-reconstruction",
      finalResultSource: "chunked-reconstruction",
      chunkProducerCount: 2,
      uniqueFinalQueryCount: 1,
      reconstructionWorkerCount: 0,
      reconstructionExecutionCount: 0,
      directFinalQueryExecutionCount: 1,
      cacheEntries: 1,
      subscribers: 1,
      reuseHits: 0,
      deliveries: 1,
      exactResults: 1,
      expectedConsumers: 1,
      allConsumersDelivered: true,
      emittedFinalWindowCount: 1,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      producedByDirectFinalQuery: true,
    }, 1);

    expect(failures).toEqual(expect.arrayContaining([
      "reconstructionWorkerCount=0, expected 1",
      "directFinalQueryExecutionCount=1, expected 0",
      "producedByDirectFinalQuery=true, expected false",
    ]));
  });
});
