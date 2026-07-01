const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  analyzeCustomPatternResults,
  renderReport,
} = require("./generate-custom-pattern-3approach-metrics.js");

const APPROACHES = ["fetching", "approximation", "chunked"];
const PATTERN = "low_variability";

function writeResults(iterationDir, approach, windows, options = {}) {
  const fileName = `${approach}_results.csv`;
  const rows = windows.map((windowNumber) => {
    const resultValue = (windowNumber * 10) + (options.valueOffset || 0);
    const latency = (options.latencyByWindow && options.latencyByWindow[windowNumber])
      || options.latency
      || 500;
    return [
      1000 + (windowNumber * 10),
      windowNumber,
      1000 + ((windowNumber - 1) * 60000),
      121000 + ((windowNumber - 1) * 60000),
      resultValue,
      latency + 100,
      latency,
    ].join(",");
  });

  fs.writeFileSync(
    path.join(iterationDir, fileName),
    [
      "timestamp,window_number,window_start,window_end,result_value,elapsed_since_registration_ms,delay_past_expected_close_ms",
      ...rows,
    ].join("\n") + "\n",
  );

  fs.writeFileSync(
    path.join(iterationDir, `${approach}_metadata.json`),
    `${JSON.stringify({
      firstEventLatency: (options.latency || 500) + 100,
    }, null, 2)}\n`,
  );

  fs.writeFileSync(
    path.join(iterationDir, "resource_summary.json"),
    `${JSON.stringify({
      cpuSeconds: options.cpuSeconds || 5,
      meanRssMb: options.meanRssMb || 120,
      peakRssMb: options.peakRssMb || 150,
    }, null, 2)}\n`,
  );
}

function writeIteration(rootDir, approach, iteration, windows, options = {}) {
  const iterationDir = path.join(rootDir, approach, PATTERN, `iteration${iteration}`);
  fs.mkdirSync(iterationDir, { recursive: true });
  writeResults(iterationDir, approach, windows, options);
  return iterationDir;
}

describe("custom-pattern 3-approach report helper", () => {
  test("steady-state mode uses windows 4..33 only", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-steady-"));
    const windows = Array.from({ length: 35 }, (_, index) => index + 1);

    try {
      const approximationLatencyByWindow = Object.fromEntries(
        windows.map((windowNumber) => [
          windowNumber,
          windowNumber >= 4 && windowNumber <= 33 ? 700 : 900,
        ]),
      );
      approximationLatencyByWindow[34] = 1100;
      approximationLatencyByWindow[35] = 1100;

      writeIteration(tempRoot, "fetching", 1, windows, { latency: 500 });
      writeIteration(tempRoot, "approximation", 1, windows, {
        latencyByWindow: approximationLatencyByWindow,
        valueOffset: 1,
        cpuSeconds: 6,
      });
      writeIteration(tempRoot, "chunked", 1, windows, { latency: 650, cpuSeconds: 5.5 });

      const result = analyzeCustomPatternResults({
        mode: "steady-state",
        inputRoot: tempRoot,
        patterns: [PATTERN],
        approaches: APPROACHES,
      });

      expect(result.selectedApproachesExact).toBe(true);
      expect(result.errors).toEqual([]);

      const approximation = result.perPattern[0].approaches.find((row) => row.approach === "approximation");
      expect(approximation.steadyLatency.count).toBe(30);
      expect(approximation.steadyLatency.mean).toBe(700);
      expect(approximation.accuracy.matchedWindowCount).toBe(30);
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("startup-cost mode uses first-window outputs only across iterations", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-startup-"));

    try {
      for (let iteration = 1; iteration <= 2; iteration += 1) {
        writeIteration(tempRoot, "fetching", iteration, [1], {
          latency: 500 + iteration,
          cpuSeconds: 2 + iteration,
        });
        writeIteration(tempRoot, "approximation", iteration, [1], {
          latency: 550 + iteration,
          valueOffset: 1,
          cpuSeconds: 3 + iteration,
        });
        writeIteration(tempRoot, "chunked", iteration, [1], {
          latency: 520 + iteration,
          cpuSeconds: 2.5 + iteration,
        });
      }

      const result = analyzeCustomPatternResults({
        mode: "startup-cost",
        inputRoot: tempRoot,
        expectedIterations: 2,
        targetWindows: 1,
        patterns: [PATTERN],
        approaches: APPROACHES,
      });

      expect(result.selectedApproachesExact).toBe(true);
      expect(result.errors).toEqual([]);

      const fetching = result.perPattern[0].approaches.find((row) => row.approach === "fetching");
      const approximation = result.perPattern[0].approaches.find((row) => row.approach === "approximation");
      expect(fetching.startupLatency.count).toBe(2);
      expect(approximation.startupLatency.count).toBe(2);
      expect(approximation.accuracy.matchedWindowCount).toBe(2);

      const report = renderReport(result);
      expect(report).toContain("Startup latency mean ± sd ms");
      expect(report).toContain("CPU seconds mean ± sd");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("pattern filter restricts reporting to low_variability without errors for unselected defaults", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "custom-pattern-filtered-"));

    try {
      writeIteration(tempRoot, "fetching", 1, [1], { latency: 501, cpuSeconds: 3 });
      writeIteration(tempRoot, "approximation", 1, [1], {
        latency: 551,
        valueOffset: 1,
        cpuSeconds: 4,
      });
      writeIteration(tempRoot, "chunked", 1, [1], {
        latency: 521,
        cpuSeconds: 3.5,
      });

      const result = analyzeCustomPatternResults({
        mode: "startup-cost",
        inputRoot: tempRoot,
        expectedIterations: 1,
        targetWindows: 1,
        patterns: [PATTERN],
        approaches: APPROACHES,
      });

      expect(result.selectedPatterns).toEqual([PATTERN]);
      expect(result.errors).toEqual([]);
      expect(result.perPattern).toHaveLength(1);
      expect(result.perPattern[0].pattern).toBe(PATTERN);

      const report = renderReport(result);
      expect(report).toContain("`low_variability`");
      expect(report).not.toContain("`step_pattern`");
      expect(report).not.toContain("`spike_pattern`");
      expect(report).not.toContain("expected 1 iterations");
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
