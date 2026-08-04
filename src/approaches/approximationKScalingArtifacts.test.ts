import fs from "fs";
import os from "os";
import path from "path";
import {
  aggregateApproximationConsumerSummaries,
  buildApproximationAggregateSummaryPath,
  writeApproximationAggregateSummaryAtomic,
  writeAtomicJson,
} from "./approximationKScalingArtifacts";

function buildTmpRoot(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "approximation-artifacts-"));
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

describe("approximation artifact helpers", () => {
  test("aggregates a normal K1 completion into a 1/1 aggregate summary", async () => {
    const root = buildTmpRoot();
    await writeAtomicJson(
      path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
      buildSummary(1),
    );

    await expect(
      aggregateApproximationConsumerSummaries({
        logRoot: root,
        expectedConsumerCount: 1,
      }),
    ).resolves.toMatchObject({
      targetWindowCount: 1,
      stoppedAfterTargetWindows: true,
      completedConsumerCount: 1,
      comparableConsumerCount: 1,
      allConsumersComplete: true,
      allConsumersComparable: true,
      failedConsumerIndices: [],
    });
  });

  test("surfaces aggregate-write failures without leaving a successful aggregate artifact", async () => {
    const root = buildTmpRoot();
    const summary = await aggregateApproximationConsumerSummaries({
      logRoot: root,
      expectedConsumerCount: 0,
    });
    const targetPath = buildApproximationAggregateSummaryPath(root);

    await expect(
      writeApproximationAggregateSummaryAtomic(targetPath, summary, async () => {
        throw new Error("aggregate write failed");
      }),
    ).rejects.toThrow("aggregate write failed");

    expect(fs.existsSync(targetPath)).toBe(false);
  });

  test("writes an aggregate summary atomically for clean shutdown readers", async () => {
    const root = buildTmpRoot();
    await writeAtomicJson(
      path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
      buildSummary(1),
    );
    const summary = await aggregateApproximationConsumerSummaries({
      logRoot: root,
      expectedConsumerCount: 1,
    });
    const targetPath = buildApproximationAggregateSummaryPath(root);

    await writeApproximationAggregateSummaryAtomic(targetPath, summary);

    expect(JSON.parse(fs.readFileSync(targetPath, "utf8"))).toMatchObject({
      targetWindowCount: 1,
      stoppedAfterTargetWindows: true,
      completedConsumerCount: 1,
      comparableConsumerCount: 1,
    });
  });
});
