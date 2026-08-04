import fs from "fs";
import os from "os";
import path from "path";
import {
  aggregateFetchingConsumerSummaries,
  buildFetchingAggregateSummaryPath,
  buildFetchingConsumerSummaryPath,
  FetchingConsumerSummary,
  readJsonIfExists,
  writeAtomicJson,
  writeFetchingConsumerArtifactsAtomic,
  writeFetchingConsumerSummaryAtomic,
} from "./fetchingKScalingArtifacts";

function buildTmpRoot(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "fetching-artifacts-"));
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

describe("fetching K-scaling artifact durability", () => {
  test("aggregates 32 consumer summaries and resists stale overwrite", async () => {
    const root = buildTmpRoot();

    for (let consumerIndex = 1; consumerIndex <= 32; consumerIndex += 1) {
      await writeFetchingConsumerSummaryAtomic(
        buildFetchingConsumerSummaryPath(root, consumerIndex),
        buildSummary(consumerIndex),
      );
    }

    await writeFetchingConsumerSummaryAtomic(
      buildFetchingConsumerSummaryPath(root, 26),
      buildSummary(26, {
        emittedFinalWindowCount: 0,
        isComparableWindow: false,
        isPartialWindow: true,
        coverageComplete: false,
        finalWindowNumbers: [],
      }),
    );

    const preserved = await readJsonIfExists<FetchingConsumerSummary>(
      buildFetchingConsumerSummaryPath(root, 26),
    );
    expect(preserved).toMatchObject({
      consumerIndex: 26,
      emittedFinalWindowCount: 1,
      isComparableWindow: true,
    });

    const aggregate = await aggregateFetchingConsumerSummaries({
      logRoot: root,
      expectedConsumerCount: 32,
    });
    expect(aggregate.completedConsumerCount).toBe(32);
    expect(aggregate.comparableConsumerCount).toBe(32);
    expect(aggregate.failedConsumerIndices).toEqual([]);

    const aggregatePath = buildFetchingAggregateSummaryPath(root);
    await writeAtomicJson(aggregatePath, aggregate);
    const parsed = JSON.parse(fs.readFileSync(aggregatePath, "utf8"));
    expect(parsed.completedConsumerCount).toBe(32);
    expect(parsed.comparableConsumerCount).toBe(32);
  });

  test("does not write consumer summary before delayed latency write completes", async () => {
    const root = buildTmpRoot();
    const latencyPath = path.join(root, "fetching_latency_log_consumer_1.csv");
    const summaryPath = buildFetchingConsumerSummaryPath(root, 1);
    let releaseLatency: (() => void) | undefined;
    const latencyStarted = new Promise<void>((resolve) => {
      releaseLatency = resolve;
    });

    const writePromise = writeFetchingConsumerArtifactsAtomic({
      latencyPath,
      latencyContent: "header\n1\n",
      summaryPath,
      summary: buildSummary(1),
      writeLatencyImpl: async (targetPath, content) => {
        await latencyStarted;
        await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
        await fs.promises.writeFile(targetPath, content, "utf8");
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(fs.existsSync(summaryPath)).toBe(false);
    if (releaseLatency) {
      releaseLatency();
    }
    await writePromise;
    expect(fs.existsSync(latencyPath)).toBe(true);
    expect(fs.existsSync(summaryPath)).toBe(true);
  });

  test("fails explicitly when latency write fails and produces no successful summary", async () => {
    const root = buildTmpRoot();
    const summaryPath = buildFetchingConsumerSummaryPath(root, 1);

    await expect(writeFetchingConsumerArtifactsAtomic({
      latencyPath: path.join(root, "fetching_latency_log_consumer_1.csv"),
      latencyContent: "header\n1\n",
      summaryPath,
      summary: buildSummary(1),
      writeLatencyImpl: async () => {
        throw new Error("latency write failed");
      },
    })).rejects.toThrow("latency write failed");

    expect(fs.existsSync(summaryPath)).toBe(false);
  });

  test("atomic JSON writes never expose partial JSON to readers", async () => {
    const root = buildTmpRoot();
    const targetPath = buildFetchingAggregateSummaryPath(root);
    await writeAtomicJson(targetPath, { version: "old" });

    const realRename = fs.promises.rename.bind(fs.promises);
    const renameSpy = jest.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      return realRename(from, to);
    });

    const reads: string[] = [];
    const writer = writeAtomicJson(targetPath, { version: "new", values: Array.from({ length: 1000 }, (_, i) => i) });
    for (let attempt = 0; attempt < 5; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      reads.push(fs.readFileSync(targetPath, "utf8"));
    }
    await writer;
    renameSpy.mockRestore();

    for (const content of reads) {
      expect(() => JSON.parse(content)).not.toThrow();
    }
    expect(JSON.parse(fs.readFileSync(targetPath, "utf8"))).toMatchObject({ version: "new" });
  });
});
