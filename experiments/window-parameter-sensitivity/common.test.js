const {
  buildScenarioConfig,
  computeExpectedChunkStatesPerResult,
  normalizeExperimentName,
} = require("./common");

describe("window-parameter-sensitivity common helpers", () => {
  test("normalizes experiment aliases", () => {
    expect(normalizeExperimentName("range")).toBe("superquery-range-scaling");
    expect(normalizeExperimentName("chunk-size")).toBe(
      "chunk-granularity-sensitivity",
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
});
