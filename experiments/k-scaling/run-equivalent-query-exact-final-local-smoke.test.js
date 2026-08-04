const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  ATTEMPT_STATES,
  buildExecutionPlan,
  classifyExistingExecution,
  collectScenarioAnchorState,
  ensureScenarioManifest,
  shouldTerminateBoundedRun,
  waitForStructuralValidation,
} = require("./run-equivalent-query-exact-final-local-smoke");
const { buildScenarioKey } = require("./local-k-scaling-smoke-common");

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function buildResultRoot() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "s1-local-smoke-"));
}

function buildRunRoot(resultRoot, approach, kValue, iteration) {
  return path.join(resultRoot, "raw", approach, `K${kValue}`, `iteration${iteration}`);
}

function writeScenarioManifest(resultRoot, kValue, iteration, replayAnchor) {
  writeJson(
    path.join(resultRoot, "scenarios", `K${kValue}`, `iteration${iteration}`, "scenario-manifest.json"),
    {
      scenario_id: buildScenarioKey(kValue, iteration),
      K: kValue,
      iteration,
      replay_anchor: replayAnchor,
      fixture: "custom_patterns/low_variability",
      seed: null,
      window_range_ms: 120000,
      window_step_ms: 60000,
      aggregation: "AVG",
      target_streams: ["wearableX", "smartphoneX"],
      created_at: "2026-08-04T00:00:00.000Z",
    },
  );
}

function writeAttempt(resultRoot, {
  approach,
  kValue,
  iteration,
  replayAnchor,
  success = true,
  validationOk = true,
  includeExecutionResult = true,
  includeRunMetadata = true,
  includeCheckpoint = false,
}) {
  const runRoot = buildRunRoot(resultRoot, approach, kValue, iteration);
  fs.mkdirSync(runRoot, { recursive: true });
  if (includeRunMetadata) {
    writeJson(path.join(runRoot, "run_metadata.json"), {
      approach,
      kValue,
      iteration,
      replayAnchor,
      environment: {
        STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR: replayAnchor,
        K_SCALING_K: String(kValue),
      },
    });
  }
  if (includeExecutionResult) {
    writeJson(path.join(runRoot, "execution_result.json"), {
      success,
      validation: {
        ok: validationOk,
        failures: validationOk ? [] : ["missing complete comparable first-window result for one or more consumers"],
      },
      processTree: {
        averageCpuPct: 1,
        peakRssMb: 1,
      },
    });
  }
  if (includeCheckpoint) {
    writeJson(
      path.join(resultRoot, "checkpoints", `${approach}-K${kValue}-iteration${iteration}.json`),
      {
        success,
        validation: {
          ok: validationOk,
        },
        runRoot,
      },
    );
  }
  return runRoot;
}

describe("run-equivalent-query-exact-final-local-smoke bounded settling", () => {
  test("waits for a delayed consumer before bounded teardown", () => {
    const targetReachedAt = 1000;
    expect(
      shouldTerminateBoundedRun({
        validationOk: false,
        targetReachedAt,
        now: 1000 + 5000,
        settleTimeoutMs: 15000,
      }),
    ).toBe(false);
    expect(
      shouldTerminateBoundedRun({
        validationOk: true,
        targetReachedAt,
        now: 1000 + 5000,
        settleTimeoutMs: 15000,
      }),
    ).toBe(true);
  });

  test("waits for delayed artifact flush after process completion", async () => {
    const validations = [
      { ok: false, token: "first" },
      { ok: false, token: "second" },
      { ok: true, token: "final" },
    ];
    const result = await waitForStructuralValidation({
      runRoot: "/tmp/unused",
      approach: "fetching",
      kValue: 32,
      timeoutMs: 20,
      requireTargetSummary: false,
      validateRunImpl: () => validations.shift() || { ok: true, token: "done" },
      readSummaryImpl: () => ({
        stoppedAfterTargetWindows: true,
        stopReason: "target_window_count_reached",
        targetWindowCount: 1,
        emittedFinalWindowCount: 1,
      }),
      delayImpl: async () => undefined,
      pollIntervalMs: 0,
    });

    expect(result.ok).toBe(true);
    expect(result.validation).toMatchObject({ ok: true, token: "final" });
  });
});

describe("attempt classification", () => {
  test("classifies VALID attempts only when execution result and metadata are complete", () => {
    const resultRoot = buildResultRoot();
    writeScenarioManifest(resultRoot, 32, 2, "1785780943097");
    writeAttempt(resultRoot, {
      approach: "fetching",
      kValue: 32,
      iteration: 2,
      replayAnchor: "1785780943097",
      success: true,
      validationOk: true,
      includeExecutionResult: true,
      includeRunMetadata: true,
    });

    expect(classifyExistingExecution(resultRoot, "fetching", 32, 2)).toMatchObject({
      state: ATTEMPT_STATES.VALID,
      reason: null,
    });
  });

  test("classifies INVALID attempts when execution result reports failure", () => {
    const resultRoot = buildResultRoot();
    writeScenarioManifest(resultRoot, 32, 2, "1785780943097");
    writeAttempt(resultRoot, {
      approach: "fetching",
      kValue: 32,
      iteration: 2,
      replayAnchor: "1785780943097",
      success: false,
      validationOk: false,
    });

    expect(classifyExistingExecution(resultRoot, "fetching", 32, 2)).toMatchObject({
      state: ATTEMPT_STATES.INVALID,
    });
  });

  test("classifies PARTIAL attempts when a directory exists without execution result", () => {
    const resultRoot = buildResultRoot();
    writeScenarioManifest(resultRoot, 1, 1, "1785780943097");
    writeAttempt(resultRoot, {
      approach: "exact-final",
      kValue: 1,
      iteration: 1,
      replayAnchor: "1785780943097",
      includeExecutionResult: false,
      includeRunMetadata: true,
    });

    expect(classifyExistingExecution(resultRoot, "exact-final", 1, 1)).toMatchObject({
      state: ATTEMPT_STATES.PARTIAL,
    });
  });

  test("classifies MISSING attempts when no directory exists", () => {
    const resultRoot = buildResultRoot();

    expect(classifyExistingExecution(resultRoot, "approximation", 8, 3)).toMatchObject({
      state: ATTEMPT_STATES.MISSING,
    });
  });
});

describe("resume prioritization and scenario anchors", () => {
  test("schedules INVALID before PARTIAL before MISSING in scenario-major order", () => {
    const resultRoot = buildResultRoot();
    const kValues = [1, 32];
    const approaches = ["fetching", "exact-final", "approximation"];
    const replayAnchors = {
      [buildScenarioKey(1, 1)]: "1000",
      [buildScenarioKey(1, 2)]: "2000",
      [buildScenarioKey(32, 1)]: "3000",
      [buildScenarioKey(32, 2)]: "4000",
    };

    for (const kValue of kValues) {
      for (let iteration = 1; iteration <= 2; iteration += 1) {
        writeScenarioManifest(resultRoot, kValue, iteration, replayAnchors[buildScenarioKey(kValue, iteration)]);
      }
    }

    writeAttempt(resultRoot, {
      approach: "exact-final",
      kValue: 1,
      iteration: 1,
      replayAnchor: "1000",
      includeExecutionResult: false,
    });
    writeAttempt(resultRoot, {
      approach: "fetching",
      kValue: 32,
      iteration: 2,
      replayAnchor: "4000",
      success: false,
      validationOk: false,
    });
    writeAttempt(resultRoot, {
      approach: "fetching",
      kValue: 1,
      iteration: 2,
      replayAnchor: "2000",
      success: true,
      validationOk: true,
    });
    writeAttempt(resultRoot, {
      approach: "approximation",
      kValue: 1,
      iteration: 2,
      replayAnchor: "2000",
      success: true,
      validationOk: true,
    });

    const plan = buildExecutionPlan({
      resultRoot,
      approaches,
      kValues,
      iterations: 2,
      replayAnchors,
      allowCreateManifests: false,
    });

    expect(plan[0]).toMatchObject({
      state: ATTEMPT_STATES.INVALID,
      approach: "fetching",
      kValue: 32,
      iteration: 2,
    });
    expect(plan[1]).toMatchObject({
      state: ATTEMPT_STATES.PARTIAL,
      approach: "exact-final",
      kValue: 1,
      iteration: 1,
    });

    const missingEntries = plan.filter((entry) => entry.state === ATTEMPT_STATES.MISSING);
    expect(missingEntries.length).toBeGreaterThan(0);
    expect(missingEntries[0]).toMatchObject({
      approach: "fetching",
      kValue: 1,
      iteration: 1,
    });
  });

  test("persists one shared manifest anchor per scenario", () => {
    const resultRoot = buildResultRoot();
    const manifest = ensureScenarioManifest({
      resultRoot,
      kValue: 8,
      iteration: 3,
      replayAnchor: "9000",
      allowCreate: true,
    });

    expect(manifest).toMatchObject({
      scenario_id: buildScenarioKey(8, 3),
      replay_anchor: "9000",
      K: 8,
      iteration: 3,
    });

    const reread = ensureScenarioManifest({
      resultRoot,
      kValue: 8,
      iteration: 3,
      replayAnchor: "different-anchor",
      allowCreate: true,
    });
    expect(reread.replay_anchor).toBe("9000");
  });

  test("marks every approach in a scenario invalid when anchors conflict", () => {
    const resultRoot = buildResultRoot();
    const replayAnchors = {
      [buildScenarioKey(32, 2)]: "1785780943097",
    };
    writeScenarioManifest(resultRoot, 32, 2, "1785780943097");
    writeAttempt(resultRoot, {
      approach: "fetching",
      kValue: 32,
      iteration: 2,
      replayAnchor: "1785780943097",
      success: true,
      validationOk: true,
    });
    writeAttempt(resultRoot, {
      approach: "exact-final",
      kValue: 32,
      iteration: 2,
      replayAnchor: "1785780944000",
      success: true,
      validationOk: true,
    });

    const scenarioState = collectScenarioAnchorState(resultRoot, 32, 2, ["fetching", "exact-final", "approximation"]);
    expect(scenarioState.conflict).toBe(true);

    const plan = buildExecutionPlan({
      resultRoot,
      approaches: ["fetching", "exact-final", "approximation"],
      kValues: [32],
      iterations: 2,
      replayAnchors,
      allowCreateManifests: false,
    });

    const invalidEntries = plan.filter((entry) => entry.state === ATTEMPT_STATES.INVALID);
    expect(invalidEntries).toHaveLength(3);
    expect(invalidEntries.every((entry) => entry.kValue === 32 && entry.iteration === 2)).toBe(true);
    expect(invalidEntries.map((entry) => entry.approach)).toEqual([
      "fetching",
      "exact-final",
      "approximation",
    ]);
  });
});
