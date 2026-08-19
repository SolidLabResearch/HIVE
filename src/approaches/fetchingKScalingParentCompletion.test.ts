import fs from "fs";
import os from "os";
import path from "path";
import {
  FetchingConsumerSummary,
  writeFetchingConsumerSummaryAtomic,
} from "./fetchingKScalingArtifacts";
import { waitForFetchingParentCompletion } from "./fetchingKScalingParentCompletion";

function buildTmpRoot(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "fetching-parent-completion-"));
}

function buildSummary(
  consumerIndex: number,
  overrides: Partial<FetchingConsumerSummary> = {},
): FetchingConsumerSummary {
  return {
    summaryVersion: 1,
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
    stoppedAfterTargetWindows: true,
    stopReason: "target_window_count_reached",
    finalWindowNumbers: [1],
    ...overrides,
  };
}

describe("fetching parent completion", () => {
  test("does not time out before any durable artifacts exist", async () => {
    const root = buildTmpRoot();
    const durableEventsObserved = new Set<number>();
    const completionPromisesResolved = new Set<number>();
    const consumerSummaryValid = new Set<number>();
    const traces: string[] = [];
    let nowMs = 0;
    let pollCount = 0;
    let resolveOne!: () => void;
    let resolveTwo!: () => void;
    const completionPromises = [
      new Promise<void>((resolve) => { resolveOne = resolve; }),
      new Promise<void>((resolve) => { resolveTwo = resolve; }),
    ];

    const waitPromise = waitForFetchingParentCompletion({
      expectedConsumerCount: 2,
      logRoot: root,
      completionPromises,
      durableEventsObserved,
      completionPromisesResolved,
      consumerSummaryValid,
      appendParentTrace: (event) => traces.push(event),
      dumpParentTimeoutState: (reason) => traces.push(`dump:${reason}`),
      pollIntervalMs: 1,
      durableStateMismatchTimeoutMs: 10,
      now: () => nowMs,
      delay: async () => {
        pollCount += 1;
        nowMs += 1000;
        if (pollCount === 5) {
          durableEventsObserved.add(1);
          durableEventsObserved.add(2);
          completionPromisesResolved.add(1);
          completionPromisesResolved.add(2);
          await writeFetchingConsumerSummaryAtomic(
            path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
            buildSummary(1),
          );
          await writeFetchingConsumerSummaryAtomic(
            path.join(root, "benchmark_window_cap_summary_consumer_2.json"),
            buildSummary(2),
          );
          resolveOne();
          resolveTwo();
        }
      },
    });

    await expect(waitPromise).resolves.toMatchObject({
      reconciledFromDurableState: false,
      completionStateMismatch: false,
    });
    expect(traces).not.toContain("dump:completion_wait_deadline_exceeded");
  });

  test("reconciles from durable summaries when one completion promise stays unresolved", async () => {
    const root = buildTmpRoot();
    const durableEventsObserved = new Set<number>([1, 2]);
    const completionPromisesResolved = new Set<number>([1]);
    const consumerSummaryValid = new Set<number>();
    const traces: string[] = [];
    let resolveOne!: () => void;
    const completionPromises = [
      new Promise<void>((resolve) => { resolveOne = resolve; }),
      new Promise<void>(() => {}),
    ];
    resolveOne();
    await writeFetchingConsumerSummaryAtomic(
      path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
      buildSummary(1),
    );
    await writeFetchingConsumerSummaryAtomic(
      path.join(root, "benchmark_window_cap_summary_consumer_2.json"),
      buildSummary(2),
    );

    await expect(waitForFetchingParentCompletion({
      expectedConsumerCount: 2,
      logRoot: root,
      completionPromises,
      durableEventsObserved,
      completionPromisesResolved,
      consumerSummaryValid,
      appendParentTrace: (event) => traces.push(event),
      dumpParentTimeoutState: (reason) => traces.push(`dump:${reason}`),
      pollIntervalMs: 1,
      durableStateMismatchTimeoutMs: 10,
      delay: async () => {},
      now: () => 0,
    })).resolves.toMatchObject({
      reconciledFromDurableState: true,
      completionStateMismatch: true,
    });

    expect(traces).toContain("aggregate_read_started");
    expect(traces).not.toContain("dump:completion_wait_deadline_exceeded");
  });

  test("fails quickly when durable reconciliation never reaches valid K/K summaries", async () => {
    const root = buildTmpRoot();
    const durableEventsObserved = new Set<number>([1, 2]);
    const completionPromisesResolved = new Set<number>([1]);
    const consumerSummaryValid = new Set<number>();
    const traces: string[] = [];
    let nowMs = 0;
    await writeFetchingConsumerSummaryAtomic(
      path.join(root, "benchmark_window_cap_summary_consumer_1.json"),
      buildSummary(1),
    );

    await expect(waitForFetchingParentCompletion({
      expectedConsumerCount: 2,
      logRoot: root,
      completionPromises: [Promise.resolve(), new Promise<void>(() => {})],
      durableEventsObserved,
      completionPromisesResolved,
      consumerSummaryValid,
      appendParentTrace: (event) => traces.push(event),
      dumpParentTimeoutState: (reason) => traces.push(`dump:${reason}`),
      pollIntervalMs: 1,
      durableStateMismatchTimeoutMs: 5,
      delay: async () => { nowMs += 5; },
      now: () => nowMs,
    })).rejects.toThrow("PARENT_COMPLETION_STATE_MISMATCH");

    expect(traces).toContain("dump:completion_wait_deadline_exceeded");
  });
});
