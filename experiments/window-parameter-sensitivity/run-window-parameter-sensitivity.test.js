const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  classifyBoundedSuccess,
  verifyExpectedOutputFiles,
} = require("./run-window-parameter-sensitivity");

describe("window-parameter-sensitivity bounded success classification", () => {
  test("treats target-window cap summary with emitted results as bounded success", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "window-param-runner-"));

    try {
      fs.writeFileSync(
        path.join(tempRoot, "fetching_latency_log.csv"),
        [
          "window_number,result_value,expected_window_close,result_emitted_at",
          "1,42,0,0",
        ].join("\n"),
      );
      fs.writeFileSync(
        path.join(tempRoot, "benchmark_window_cap_summary.json"),
        `${JSON.stringify({
          targetWindowCount: 35,
          emittedFinalWindowCount: 1,
          finalWindowNumbers: [1],
          stoppedAfterTargetWindows: false,
        }, null, 2)}\n`,
      );

      const result = classifyBoundedSuccess({
        approach: "fetching",
        logDir: tempRoot,
        orchestratorExitCode: 0,
        orchestratorExitSignal: null,
        publisherExitCode: 0,
        publisherExitSignal: null,
        failureReason: "Orchestrator exited unexpectedly before publisher completion",
      });

      expect(result.boundedSuccess).toBe(true);
      expect(result.reason).toBe("bounded_smoke_windows_emitted");
      expect(result.fileCheck).toMatchObject({
        valid: true,
        emittedCount: 1,
      });
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("treats orchestrator exit 143 with emitted bounded windows as bounded success", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "window-param-runner-143-"));

    try {
      fs.writeFileSync(
        path.join(tempRoot, "fetching_latency_log.csv"),
        [
          "window_number,result_value,expected_window_close,result_emitted_at",
          "1,42,0,0",
        ].join("\n"),
      );

      const result = classifyBoundedSuccess({
        approach: "fetching",
        logDir: tempRoot,
        orchestratorExitCode: 143,
        orchestratorExitSignal: null,
        publisherExitCode: 0,
        publisherExitSignal: null,
        failureReason: "Orchestrator exited unexpectedly before publisher completion",
      });

      expect(result.boundedSuccess).toBe(true);
      expect(result.reason).toBe("bounded_smoke_windows_emitted");
      expect(result.fileCheck).toMatchObject({
        valid: true,
        emittedCount: 1,
      });
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  test("verifies approximation outputs via approximation latency logs", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "window-param-approx-"));

    try {
      fs.writeFileSync(
        path.join(tempRoot, "approximation_latency_log.csv"),
        [
          "window_number,result_value,expected_window_close,result_emitted_at",
          "1,42,0,0",
        ].join("\n"),
      );

      expect(verifyExpectedOutputFiles("approximation", tempRoot)).toMatchObject({
        valid: true,
        emittedCount: 1,
      });
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
});
