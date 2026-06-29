const path = require("path");
const {
  buildPatternAnalysisJobs,
  buildTrimmedIterationSelection,
  createJobDefinitions,
  formatCommandLine,
  parseArgs,
} = require("./run-all-paper-benchmarks");

describe("paper benchmark runner command construction", () => {
  test("formats the real-data command with a separated iterations argument", () => {
    const config = {
      outputDir: path.join("/tmp", "paper-benchmarks"),
      iterations: 35,
      dropWarmup: 3,
      dropCooldown: 2,
      broker: "mqtt://localhost:1883",
      windowWidth: 120000,
      windowSlide: 60000,
      subWindowRange: 60000,
      subWindowStep: 30000,
      frequency: 4,
      aggregation: "AVG",
      timeout: 300000,
      patternTestTimeout: 300000,
      smoke: false,
      patterns: null,
      approaches: null,
      targetWindows: null,
    };

    const jobs = createJobDefinitions(config);
    const commandLine = formatCommandLine([
      jobs["real-data-core"].command[0],
      ...jobs["real-data-core"].command[1],
    ]);

    expect(commandLine).toContain("node experiments/real-data-comparison/run-real-data-4-approaches.js");
    expect(commandLine).toContain("--iterations 35");
    expect(commandLine).not.toContain("--iterations35");
  });

  test("forwards the selected real-data approaches into the real-data job", () => {
    const config = {
      outputDir: path.join("/tmp", "paper-benchmarks"),
      iterations: 3,
      dropWarmup: 3,
      dropCooldown: 2,
      broker: "mqtt://localhost:1883",
      windowWidth: 120000,
      windowSlide: 60000,
      subWindowRange: 60000,
      subWindowStep: 30000,
      frequency: 4,
      aggregation: "AVG",
      timeout: 300000,
      patternTestTimeout: 300000,
      smoke: false,
      patterns: null,
      approaches: ["fetching", "approximation", "chunked"],
      targetWindows: 35,
    };

    const jobs = createJobDefinitions(config);
    const commandLine = formatCommandLine([
      jobs["real-data-core"].command[0],
      ...jobs["real-data-core"].command[1],
    ]);

    expect(commandLine).toContain("--approaches fetching,approximation,chunked");
    expect(commandLine).toContain("--target-windows 35");
    expect(commandLine).not.toContain("naive_distributed");
    expect(jobs["real-data-core"].env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("35");
  });

  test("builds a trimmed 4..33 iteration selection for the paper summary", () => {
    const config = {
      outputDir: path.join("/tmp", "paper-benchmarks"),
      iterations: 35,
      dropWarmup: 3,
      dropCooldown: 2,
      broker: "mqtt://localhost:1883",
      windowWidth: 120000,
      windowSlide: 60000,
      subWindowRange: 60000,
      subWindowStep: 30000,
      frequency: 4,
      aggregation: "AVG",
      timeout: 300000,
      patternTestTimeout: 300000,
      smoke: false,
      patterns: null,
      approaches: null,
      targetWindows: null,
    };

    const trimmedSelection = buildTrimmedIterationSelection(config);
    expect(trimmedSelection.iterations).toHaveLength(30);
    expect(trimmedSelection.iterations[0]).toBe(4);
    expect(trimmedSelection.iterations[trimmedSelection.iterations.length - 1]).toBe(33);
    expect(trimmedSelection.label).toBe("trimmed-4-33");

    const analysisJobs = buildPatternAnalysisJobs(config);
    expect(analysisJobs.raw.command[1]).toContain("--iterations");
    expect(analysisJobs.raw.command[1]).toContain("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35");
    expect(analysisJobs.trimmed.command[1]).toContain("--iterations");
    expect(analysisJobs.trimmed.command[1]).toContain("4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33");
    expect(analysisJobs.trimmed.summaryTag).toBe("trimmed-4-33");
  });

  test("defaults pattern test timeout to the paper configuration", () => {
    const args = parseArgs(["--suite", "real-data"]);
    expect(args.patternTestTimeout).toBe(300000);
  });

  test("parseArgs accepts a real-data target-window override", () => {
    const args = parseArgs([
      "--suite",
      "real-data",
      "--approaches",
      "fetching,approximation,chunked",
      "--target-windows",
      "1",
    ]);

    expect(args.approaches).toEqual(["fetching", "approximation", "chunked"]);
    expect(args.targetWindows).toBe(1);
  });
});
