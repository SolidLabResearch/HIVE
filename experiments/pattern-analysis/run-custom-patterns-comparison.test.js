const path = require("path");

const {
  CustomPatternComparisonRunner,
  parseCliArgs,
} = require("./run-custom-patterns-comparison.js");

describe("custom-pattern runner CLI alignment", () => {
  test("parseCliArgs accepts the three paper-ready approaches and excludes naive_distributed", () => {
    expect(parseCliArgs([
      "--iterations",
      "3",
      "--approaches",
      "fetching,approximation,chunked",
    ])).toEqual({
      iterations: 3,
      patternTestTimeoutMs: undefined,
      retries: 0,
      patternTypes: null,
      approachNames: ["fetching", "approximation", "chunked"],
      targetWindowCount: null,
      outputDir: null,
      patternType: null,
      help: false,
    });

    const runner = new CustomPatternComparisonRunner(3, {
      approachNames: ["fetching", "approximation", "chunked"],
    });

    expect(runner.approaches).toEqual(["fetching", "approximation", "chunked"]);
    expect(runner.approaches.includes("naive_distributed")).toBe(false);
  });

  test("parseCliArgs accepts target-window counts of 1 and 35", () => {
    expect(parseCliArgs(["--target-windows", "1"]).targetWindowCount).toBe(1);
    expect(parseCliArgs(["--target-windows", "35"]).targetWindowCount).toBe(35);
  });

  test("parseCliArgs accepts selecting only low_variability via --patterns", () => {
    expect(parseCliArgs(["--patterns", "low_variability"])).toMatchObject({
      patternTypes: ["low_variability"],
      patternType: null,
    });

    const runner = new CustomPatternComparisonRunner(1, {
      patternTypes: ["low_variability"],
      approachNames: ["fetching", "approximation", "chunked"],
    });

    expect(runner.patterns.map((pattern) => pattern.type)).toEqual([
      "low_variability",
    ]);
  });

  test("runner preserves selected pattern list order and output dir", () => {
    const outputDir = path.join("/tmp", "custom-pattern-paper-ready");
    const runner = new CustomPatternComparisonRunner(1, {
      patternTypes: ["spike_pattern", "low_variability"],
      approachNames: ["fetching", "approximation", "chunked"],
      outputDir,
      targetWindowCount: 35,
    });

    expect(runner.patterns.map((pattern) => pattern.type)).toEqual([
      "spike_pattern",
      "low_variability",
    ]);
    expect(runner.baseLogDir).toBe(outputDir);
    expect(runner.targetWindowCount).toBe(35);
  });
});
