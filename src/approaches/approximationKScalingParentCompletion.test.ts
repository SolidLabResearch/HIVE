import fs from "fs";
import os from "os";
import path from "path";
import { writeAtomicJson } from "./approximationKScalingArtifacts";
import { waitForApproximationParentCompletion } from "./approximationKScalingParentCompletion";

function buildTmpRoot(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "approximation-parent-completion-"));
}

function buildSummary(consumerIndex: number) {
  return {
    summaryVersion: 1 as const,
    approach: "approximation" as const,
    consumerIndex,
    stateObjectId: `state-${consumerIndex}`,
    queryRegisteredAt: 1000,
    firstDataReceivedAt: 1100,
    emittedFinalWindowCount: 1,
    windowNumber: 1,
    windowStart: 2000,
    windowEnd: 122000,
    coverageComplete: true,
    isPartialWindow: false,
    isComparableWindow: true,
    resultValue: 42.25,
    resultEmittedAt: 4000,
    writeCompletedAt: "2026-08-04T00:00:00.000Z",
    stopReason: "target_window_count_reached",
    finalWindowNumbers: [1],
  };
}

describe("approximation parent completion", () => {
  test("K1 normal completion resolves cleanly", async () => {
    const root = buildTmpRoot();
    const completionPromisesResolved = new Set<number>([1]);
    const completionEventsObserved = new Set<number>([1]);
    const consumerSummaryValid = new Set<number>();
    await writeAtomicJson(
      path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
      buildSummary(1),
    );

    await expect(waitForApproximationParentCompletion({
      expectedConsumerCount: 1,
      logRoot: root,
      completionPromises: [Promise.resolve()],
      completionPromisesResolved,
      completionEventsObserved,
      consumerSummaryValid,
      reconciliationEligible: () => true,
      appendParentTrace: () => undefined,
      dumpParentTimeoutState: () => undefined,
      delay: async () => undefined,
      now: () => 0,
    })).resolves.toMatchObject({
      reconciledFromDurableState: false,
      completionStateMismatch: false,
    });
  });

  test("waits for a delayed durable summary before resolving", async () => {
    const root = buildTmpRoot();
    const completionPromisesResolved = new Set<number>();
    const completionEventsObserved = new Set<number>();
    const consumerSummaryValid = new Set<number>();
    let nowMs = 0;
    let resolveCompletion!: () => void;
    const completionPromise = new Promise<void>((resolve) => {
      resolveCompletion = resolve;
    });

    const waitPromise = waitForApproximationParentCompletion({
      expectedConsumerCount: 1,
      logRoot: root,
      completionPromises: [completionPromise],
      completionPromisesResolved,
      completionEventsObserved,
      consumerSummaryValid,
      reconciliationEligible: () => true,
      appendParentTrace: () => undefined,
      dumpParentTimeoutState: () => undefined,
      delay: async () => {
        nowMs += 5;
        if (nowMs === 10) {
          await writeAtomicJson(
            path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
            buildSummary(1),
          );
          completionPromisesResolved.add(1);
          completionEventsObserved.add(1);
          resolveCompletion();
        }
      },
      now: () => nowMs,
      pollIntervalMs: 1,
    });

    await expect(waitPromise).resolves.toMatchObject({
      completionStateMismatch: false,
    });
  });

  test("reconciles when completion event is missing but valid durable summary exists", async () => {
    const root = buildTmpRoot();
    const completionPromisesResolved = new Set<number>();
    const completionEventsObserved = new Set<number>();
    const consumerSummaryValid = new Set<number>();
    await writeAtomicJson(
      path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
      buildSummary(1),
    );

    await expect(waitForApproximationParentCompletion({
      expectedConsumerCount: 1,
      logRoot: root,
      completionPromises: [new Promise<void>(() => {})],
      completionPromisesResolved,
      completionEventsObserved,
      consumerSummaryValid,
      reconciliationEligible: () => true,
      appendParentTrace: () => undefined,
      dumpParentTimeoutState: () => undefined,
      delay: async () => undefined,
      now: () => 0,
    })).resolves.toMatchObject({
      reconciledFromDurableState: true,
      completionStateMismatch: true,
    });
  });

  test("propagates parent failure when durable completion never materializes", async () => {
    const root = buildTmpRoot();
    const completionPromisesResolved = new Set<number>();
    const completionEventsObserved = new Set<number>();
    const consumerSummaryValid = new Set<number>();
    let nowMs = 0;

    await expect(waitForApproximationParentCompletion({
      expectedConsumerCount: 1,
      logRoot: root,
      completionPromises: [new Promise<void>(() => {})],
      completionPromisesResolved,
      completionEventsObserved,
      consumerSummaryValid,
      reconciliationEligible: () => true,
      appendParentTrace: () => undefined,
      dumpParentTimeoutState: () => undefined,
      delay: async () => {
        nowMs += 5;
      },
      now: () => nowMs,
      pollIntervalMs: 1,
      durableStateMismatchTimeoutMs: 5,
    })).rejects.toThrow("APPROXIMATION_PARENT_COMPLETION_STATE_MISMATCH");
  });
});
