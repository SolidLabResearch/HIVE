const path = require("path");
const fs = require("fs");
const os = require("os");
const {
  buildPatternAnalysisJobs,
  buildTrimmedIterationSelection,
  createJobDefinitions,
  formatCommandLine,
  parseArgs,
  snapshotRealDataArtifacts,
  shouldMirrorChildOutput,
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

  test("forwards the selected custom-pattern approaches and target windows into the custom-pattern job", () => {
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
      patterns: ["low_variability"],
      approaches: ["fetching", "approximation", "chunked"],
      targetWindows: 1,
      retries: 0,
    };

    const jobs = createJobDefinitions(config);
    const commandLine = formatCommandLine([
      jobs["custom-pattern-core"].command[0],
      ...jobs["custom-pattern-core"].command[1],
    ]);

    expect(commandLine).toContain("--patterns low_variability");
    expect(commandLine).toContain("--approaches fetching,approximation,chunked");
    expect(commandLine).toContain("--target-windows 1");
    expect(commandLine).not.toContain("naive_distributed");
    expect(jobs["custom-pattern-core"].env.STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS).toBe("1");
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

  test("snapshotRealDataArtifacts copies only the current rawResults iteration directories", () => {
    const sourceRoot = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-snapshot-source-"));
    const outputDir = fs.mkdtempSync(path.join(os.tmpdir(), "real-data-snapshot-out-"));

    try {
      const fetchingIteration1 = path.join(sourceRoot, "fetching", "iteration1");
      const staleFetchingIteration2 = path.join(sourceRoot, "fetching", "iteration2");
      const staleNaiveIteration1 = path.join(sourceRoot, "naive_distributed", "iteration1");
      const backupDir = path.join(sourceRoot, "manual-backups", "fetching-iteration1");

      fs.mkdirSync(fetchingIteration1, { recursive: true });
      fs.mkdirSync(staleFetchingIteration2, { recursive: true });
      fs.mkdirSync(staleNaiveIteration1, { recursive: true });
      fs.mkdirSync(backupDir, { recursive: true });

      fs.writeFileSync(path.join(fetchingIteration1, "fetching_latency_log.csv"), "window_number\n1\n");
      fs.writeFileSync(path.join(fetchingIteration1, "run_summary.json"), "{\"completionStatus\":\"completed\"}\n");
      fs.writeFileSync(path.join(staleFetchingIteration2, "fetching_latency_log.csv"), "stale\n");
      fs.writeFileSync(path.join(staleNaiveIteration1, "naive_distributed_latency_log.csv"), "stale\n");
      fs.writeFileSync(path.join(backupDir, "fetching_latency_log.csv"), "backup\n");
      fs.writeFileSync(
        path.join(sourceRoot, "real_data_comparison_results.json"),
        `${JSON.stringify({
          rawResults: [
            {
              approach: "fetching",
              iteration: 1,
              logDir: fetchingIteration1,
            },
          ],
        }, null, 2)}\n`,
      );

      const snapshotResult = snapshotRealDataArtifacts(sourceRoot, outputDir);

      expect(snapshotResult.copiedCaseCount).toBe(1);
      expect(fs.existsSync(path.join(outputDir, "real-data", "raw", "fetching", "iteration1", "fetching_latency_log.csv"))).toBe(true);
      expect(fs.existsSync(path.join(outputDir, "real-data", "raw", "fetching", "iteration2"))).toBe(false);
      expect(fs.existsSync(path.join(outputDir, "real-data", "raw", "naive_distributed"))).toBe(false);
      expect(fs.existsSync(path.join(outputDir, "real-data", "raw", "manual-backups"))).toBe(false);
    } finally {
      fs.rmSync(sourceRoot, { recursive: true, force: true });
      fs.rmSync(outputDir, { recursive: true, force: true });
    }
  });

  test("child benchmark output mirroring is disabled by default", () => {
    expect(shouldMirrorChildOutput({})).toBe(false);
    expect(shouldMirrorChildOutput({
      STREAMING_QUERY_HIVE_BENCHMARK_MIRROR_CHILD_OUTPUT: "",
    })).toBe(false);
  });

  test("child benchmark output mirroring can be re-enabled explicitly", () => {
    expect(shouldMirrorChildOutput({
      STREAMING_QUERY_HIVE_BENCHMARK_MIRROR_CHILD_OUTPUT: "1",
    })).toBe(true);
    expect(shouldMirrorChildOutput({
      STREAMING_QUERY_HIVE_BENCHMARK_MIRROR_CHILD_OUTPUT: "true",
    })).toBe(true);
  });
});
