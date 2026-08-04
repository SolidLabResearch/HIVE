import {
  ApproximationReuseConfig,
  canonicalizeApproximationQuery,
  canonicalizeHierarchicalFinalQuery,
  validateApproximationReuseSummaryShape,
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

describe("approximation reuse query identity", () => {
  const approximationConfig: ApproximationReuseConfig = {
    completedWindowMode: true,
    earlyTriggerMode: false,
    policy: "rate-based-completed-window",
    rate: null,
    samplingParameters: {},
    errorConfiguration: { oracle: "fetching" },
  };

  test("reuses equivalent query text under the same approximation configuration", () => {
    const first = canonicalizeApproximationQuery(query(), approximationConfig);
    const second = canonicalizeApproximationQuery(
      query().replace("<sensor_averages_1>", "<sensor_averages_32>").replace(/\s+/g, " "),
      approximationConfig,
    );

    expect(second.canonicalQueryId).toBe(first.canonicalQueryId);
  });

  test.each([
    ["different window range", query("60000", "60000"), approximationConfig],
    ["different window step", query("120000", "30000"), approximationConfig],
    ["different approximation policy", query(), { ...approximationConfig, policy: "early-trigger" }],
    ["different approximation rate", query(), { ...approximationConfig, rate: 0.5 }],
  ])("rejects %s from the same approximation identity", (_label, candidateQuery, candidateConfig) => {
    const baseline = canonicalizeApproximationQuery(query(), approximationConfig);
    const candidate = canonicalizeApproximationQuery(
      candidateQuery,
      candidateConfig as ApproximationReuseConfig,
    );

    expect(candidate.canonicalQueryId).not.toBe(baseline.canonicalQueryId);
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

describe("approximation reuse summary shape", () => {
  const approximationConfiguration: ApproximationReuseConfig = {
    completedWindowMode: true,
    earlyTriggerMode: false,
    policy: "rate-based-completed-window",
    rate: null,
    samplingParameters: {},
    errorConfiguration: { oracle: "fetching" },
  };

  test("accepts one approximation execution and K subscribers", () => {
    const failures = validateApproximationReuseSummaryShape({
      experiment: "Experiment 3: Increasing Number of Same Superqueries",
      approach: "approximation",
      scenario_id: "K32-iteration1",
      canonical_query_id: "abc",
      source: "shared-approximation",
      uniqueApproximationQueryCount: 1,
      approximationWorkerCount: 1,
      approximationExecutionCount: 1,
      cacheEntries: 1,
      subscribers: 32,
      reuseHits: 31,
      deliveries: 32,
      comparableResults: 32,
      expectedConsumers: 32,
      allConsumersDelivered: true,
      targetWindowCount: 1,
      emittedFinalWindowCount: 1,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      aggregateWrittenAt: "2026-08-04T00:00:00.000Z",
      approximationConfiguration,
    }, 32);

    expect(failures).toEqual([]);
  });

  test("rejects duplicate approximation workers", () => {
    const failures = validateApproximationReuseSummaryShape({
      scenario_id: "K2-iteration1",
      canonical_query_id: "abc",
      source: "shared-approximation",
      uniqueApproximationQueryCount: 1,
      approximationWorkerCount: 2,
      approximationExecutionCount: 2,
      cacheEntries: 1,
      subscribers: 2,
      reuseHits: 1,
      deliveries: 2,
      comparableResults: 2,
      expectedConsumers: 2,
      allConsumersDelivered: true,
      emittedFinalWindowCount: 1,
      stoppedAfterTargetWindows: true,
      stopReason: "target_window_count_reached",
      approximationConfiguration,
    }, 2);

    expect(failures).toEqual(expect.arrayContaining([
      "approximationWorkerCount=2, expected 1",
      "approximationExecutionCount=2, expected 1",
    ]));
  });
});
