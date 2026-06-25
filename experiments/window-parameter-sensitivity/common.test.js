const {
  buildQueryTargetScalingScenarioConfig,
  buildScenarioConfig,
  computeExpectedChunkStatesPerResult,
  getRealQueryTargetDefinitions,
  normalizeExperimentName,
  resolveQueryTargetScalingScenarioDefinitions,
} = require("./common");

describe("window-parameter-sensitivity common helpers", () => {
  test("normalizes experiment aliases", () => {
    expect(normalizeExperimentName("range")).toBe("superquery-range-scaling");
    expect(normalizeExperimentName("chunk-size")).toBe(
      "chunk-granularity-sensitivity",
    );
    expect(normalizeExperimentName("query-target-scaling")).toBe(
      "query-target-scaling",
    );
  });

  test("builds superquery range scaling metadata with fixed 30s chunks", () => {
    const scenario = buildScenarioConfig({
      experimentName: "superquery-range-scaling",
      scenarioSeconds: 180,
      pattern: "low_variability",
      iteration: 1,
    });

    expect(scenario.metadata.superquery_range_seconds).toBe(180);
    expect(scenario.metadata.superquery_step_seconds).toBe(60);
    expect(scenario.metadata.chunk_size_seconds).toBe(30);
    expect(scenario.metadata.expected_chunk_states_per_result).toBe(12);
  });

  test("builds chunk granularity sensitivity metadata from the selected chunk size", () => {
    const scenario = buildScenarioConfig({
      experimentName: "chunk-granularity-sensitivity",
      scenarioSeconds: 5,
      pattern: "low_variability",
      iteration: 1,
    });

    expect(scenario.metadata.superquery_range_seconds).toBe(120);
    expect(scenario.metadata.superquery_step_seconds).toBe(60);
    expect(scenario.metadata.chunk_size_seconds).toBe(5);
    expect(scenario.metadata.expected_chunk_states_per_result).toBe(48);
  });

  test("computes expected chunk states per result", () => {
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 30,
      }),
    ).toBe(8);
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 180,
        chunkSizeSeconds: 30,
      }),
    ).toBe(12);
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 60,
      }),
    ).toBe(4);
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 1,
      }),
    ).toBe(240);
  });

  test("query-target-scaling creates the K=2 scenario from the existing real targets", () => {
    const scenarios = resolveQueryTargetScalingScenarioDefinitions({
      targetSource: "real",
      availableTargets: getRealQueryTargetDefinitions(),
    });

    expect(scenarios).toHaveLength(1);
    expect(scenarios[0].targetSource).toBe("real");
    expect(scenarios[0].targetCount).toBe(2);
    expect(scenarios[0].targetSet).toBe("wearableX|smartphoneX");
    expect(scenarios[0].targetNames).toBe("wearableX,smartphoneX");
    expect(scenarios[0].scenarioLabel).toBe("real-targets-k2");
  });

  test("query-target-scaling creates K=4 only when four real targets are configured", () => {
    const scenarios = resolveQueryTargetScalingScenarioDefinitions({
      targetSource: "real",
      targetCounts: [2, 4],
      availableTargets: [
        { name: "wearableX", topicName: "wearableX", propertyName: "wearableX" },
        { name: "smartphoneX", topicName: "smartphoneX", propertyName: "smartphoneX" },
        { name: "wearableY", topicName: "wearableY", propertyName: "wearableY" },
        { name: "smartphoneY", topicName: "smartphoneY", propertyName: "smartphoneY" },
      ],
    });

    expect(scenarios.map((scenario) => scenario.targetCount)).toEqual([2, 4]);
    expect(scenarios[1].targetSet).toBe(
      "wearableX|smartphoneX|wearableY|smartphoneY",
    );
  });

  test("query-target-scaling creates synthetic K=8 scenario", () => {
    const scenarios = resolveQueryTargetScalingScenarioDefinitions({
      targetSource: "synthetic",
      targetCounts: [2, 4, 6, 8],
    });

    expect(scenarios.map((scenario) => scenario.targetCount)).toEqual([2, 4, 6, 8]);
    expect(scenarios[3].targetSource).toBe("synthetic");
    expect(scenarios[3].scenarioLabel).toBe("synthetic-targets-k8");
    expect(scenarios[3].targetNames).toBe(
      "syntheticTarget1,syntheticTarget2,syntheticTarget3,syntheticTarget4,syntheticTarget5,syntheticTarget6,syntheticTarget7,syntheticTarget8",
    );
  });

  test("query-target-scaling metadata keeps window parameters fixed and exact-final disabled", () => {
    const scenario = buildQueryTargetScalingScenarioConfig({
      targetDefinitions: getRealQueryTargetDefinitions(),
      targetSource: "real",
      pattern: "low_variability",
      iteration: 1,
    });

    expect(scenario.metadata.superquery_range_seconds).toBe(120);
    expect(scenario.metadata.superquery_step_seconds).toBe(60);
    expect(scenario.metadata.chunk_size_seconds).toBe(30);
    expect(scenario.metadata.target_count).toBe(2);
    expect(scenario.metadata.target_source).toBe("real");
    expect(scenario.metadata.real_target_count).toBe(2);
    expect(scenario.metadata.synthetic_target_count).toBe(0);
    expect(scenario.metadata.target_set).toBe("wearableX|smartphoneX");
    expect(scenario.metadata.target_names).toBe("wearableX,smartphoneX");
    expect(scenario.metadata.expected_chunk_states_per_result).toBe(8);
    expect(scenario.metadata.exact_final_reuse_enabled).toBe(false);
    expect(scenario.metadata.target_window_count).toBe(35);
    expect(scenario.metadata.trimmed_window_start).toBe(4);
    expect(scenario.metadata.trimmed_window_end).toBe(33);
    expect(scenario.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("35");
    expect(scenario.env.STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD).toBe("1");
    expect(scenario.env.STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE).toBe("1");
    expect(scenario.env.STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE).toBe("0");
    expect(scenario.env.STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY).toBe("0");
    expect(scenario.env.STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY).toBe("0");
    expect(scenario.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER).toBe("1");
  });

  test("synthetic target metadata populates synthetic counts", () => {
    const scenarios = resolveQueryTargetScalingScenarioDefinitions({
      targetSource: "synthetic",
      targetCounts: [4],
    });
    const scenario = buildQueryTargetScalingScenarioConfig({
      targetDefinitions: scenarios[0].targetDefinitions,
      targetSource: "synthetic",
      pattern: "low_variability",
      iteration: 1,
    });

    expect(scenario.metadata.target_source).toBe("synthetic");
    expect(scenario.metadata.target_count).toBe(4);
    expect(scenario.metadata.unique_target_count).toBe(4);
    expect(scenario.metadata.real_target_count).toBe(0);
    expect(scenario.metadata.synthetic_target_count).toBe(4);
    expect(scenario.metadata.is_synthetic_target_scaling).toBe(true);
    expect(scenario.metadata.expected_chunk_states_per_result).toBe(16);
  });

  test("range and granularity scenarios expose explicit paper-mode env", () => {
    const rangeScenario = buildScenarioConfig({
      experimentName: "superquery-range-scaling",
      scenarioSeconds: 120,
      pattern: "low_variability",
      iteration: 1,
    });

    expect(rangeScenario.metadata.target_window_count).toBe(35);
    expect(rangeScenario.metadata.trimmed_window_start).toBe(4);
    expect(rangeScenario.metadata.trimmed_window_end).toBe(33);
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("35");
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD).toBe("1");
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE).toBe("1");
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE).toBe("0");
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY).toBe("0");
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY).toBe("0");
    expect(rangeScenario.env.STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER).toBe("1");
  });

  test("query-target-scaling expected chunk states scale with target count", () => {
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 30,
        streamCount: 2,
      }),
    ).toBe(8);
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 30,
        streamCount: 4,
      }),
    ).toBe(16);
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 30,
        streamCount: 6,
      }),
    ).toBe(24);
    expect(
      computeExpectedChunkStatesPerResult({
        superqueryRangeSeconds: 120,
        chunkSizeSeconds: 30,
        streamCount: 8,
      }),
    ).toBe(32);
  });
});
