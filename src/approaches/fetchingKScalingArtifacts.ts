import fs from "fs";
import path from "path";

let atomicWriteCounter = 0;

export type FetchingTraceEvent =
  | "candidate_received"
  | "candidate_suppressed"
  | "complete_window_finalized"
  | "final_window_counter_incremented"
  | "latency_write_started"
  | "latency_write_completed"
  | "consumer_summary_write_started"
  | "consumer_summary_write_completed"
  | "aggregate_summary_write_started"
  | "aggregate_summary_write_completed"
  | "shutdown_requested"
  | "stream_end_started"
  | "stream_end_completed"
  | "process_exit_requested";

export type FetchingArtifactTraceRecord = {
  sequence: number;
  timestamp: number;
  pid: number;
  consumerIndex: number | null;
  event: FetchingTraceEvent;
  windowNumber?: number | null;
  finalWindowCount?: number | null;
  coverageComplete?: boolean | null;
  comparable?: boolean | null;
  targetPath?: string | null;
  stateObjectId: string;
};

export type FetchingConsumerSummary = {
  summaryVersion: 1;
  consumerIndex: number;
  stateObjectId: string;
  queryRegisteredAt: number;
  firstDataReceivedAt: number;
  emittedFinalWindowCount: number;
  windowNumber: number;
  windowStart: number;
  windowEnd: number;
  coverageComplete: boolean;
  isPartialWindow: boolean;
  isComparableWindow: boolean;
  resultValue: number;
  resultEmittedAt: number;
  writeCompletedAt: string;
  stoppedAfterTargetWindows: boolean;
  stopReason: string;
  finalWindowNumbers: number[];
};

export type FetchingAggregateSummary = {
  summaryVersion: 1;
  approach: "fetching";
  targetWindowCount: number;
  emittedFinalWindowCount: number;
  finalWindowNumbers: number[];
  stoppedAfterTargetWindows: boolean;
  stopReason: string;
  expectedConsumerCount: number;
  completedConsumerCount: number;
  comparableConsumerCount: number;
  failedConsumerIndices: number[];
  allConsumersComplete: boolean;
  allConsumersComparable: boolean;
  aggregateWrittenAt: string;
};

export function buildFetchingConsumerSummaryPath(
  logRoot: string,
  consumerIndex: number,
): string {
  return path.join(
    logRoot,
    `benchmark_window_cap_summary_consumer_${consumerIndex}.json`,
  );
}

export function buildFetchingAggregateSummaryPath(logRoot: string): string {
  return path.join(logRoot, "benchmark_window_cap_summary.json");
}

export function buildFetchingArtifactTracePath(logRoot: string): string {
  return path.join(logRoot, "fetching_artifact_write_trace.ndjson");
}

export async function writeAtomicFile(
  targetPath: string,
  content: string,
): Promise<void> {
  await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
  const tempPath = `${targetPath}.tmp-${process.pid}-${Date.now()}-${++atomicWriteCounter}`;
  const handle = await fs.promises.open(tempPath, "w");
  try {
    await handle.writeFile(content, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fs.promises.rename(tempPath, targetPath);
}

export async function writeAtomicJson(
  targetPath: string,
  value: unknown,
): Promise<void> {
  await writeAtomicFile(targetPath, `${JSON.stringify(value, null, 2)}\n`);
}

export function appendFetchingArtifactTrace(
  tracePath: string,
  record: FetchingArtifactTraceRecord,
): void {
  fs.mkdirSync(path.dirname(tracePath), { recursive: true });
  fs.appendFileSync(tracePath, `${JSON.stringify(record)}\n`);
}

export function isCompleteFetchingConsumerSummary(
  value: FetchingConsumerSummary | null | undefined,
): value is FetchingConsumerSummary {
  return Boolean(
    value &&
    value.summaryVersion === 1 &&
    Number.isFinite(value.consumerIndex) &&
    Number.isFinite(value.queryRegisteredAt) &&
    Number.isFinite(value.firstDataReceivedAt) &&
    Number.isFinite(value.emittedFinalWindowCount) &&
    value.emittedFinalWindowCount >= 1 &&
    Number.isFinite(value.windowNumber) &&
    Number.isFinite(value.windowStart) &&
    Number.isFinite(value.windowEnd) &&
    value.coverageComplete === true &&
    value.isPartialWindow === false &&
    value.isComparableWindow === true &&
    Number.isFinite(value.resultValue) &&
    Number.isFinite(value.resultEmittedAt) &&
    typeof value.writeCompletedAt === "string" &&
    typeof value.stateObjectId === "string" &&
    Array.isArray(value.finalWindowNumbers) &&
    value.finalWindowNumbers.includes(value.windowNumber),
  );
}

export async function writeFetchingConsumerSummaryAtomic(
  summaryPath: string,
  summary: FetchingConsumerSummary,
): Promise<void> {
  const existing = await readJsonIfExists<FetchingConsumerSummary>(summaryPath);
  if (
    existing &&
    isCompleteFetchingConsumerSummary(existing) &&
    !isCompleteFetchingConsumerSummary(summary)
  ) {
    return;
  }
  await writeAtomicJson(summaryPath, summary);
}

export async function writeFetchingConsumerArtifactsAtomic({
  latencyPath,
  latencyContent,
  summaryPath,
  summary,
  writeLatencyImpl = writeAtomicFile,
  writeSummaryImpl = writeFetchingConsumerSummaryAtomic,
}: {
  latencyPath: string;
  latencyContent: string;
  summaryPath: string;
  summary: FetchingConsumerSummary;
  writeLatencyImpl?: (targetPath: string, content: string) => Promise<void>;
  writeSummaryImpl?: (summaryPath: string, summary: FetchingConsumerSummary) => Promise<void>;
}): Promise<void> {
  await writeLatencyImpl(latencyPath, latencyContent);
  await writeSummaryImpl(summaryPath, summary);
}

export async function readJsonIfExists<T>(filePath: string): Promise<T | null> {
  try {
    const content = await fs.promises.readFile(filePath, "utf8");
    return JSON.parse(content) as T;
  } catch (error: any) {
    if (error?.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

export async function aggregateFetchingConsumerSummaries({
  logRoot,
  expectedConsumerCount,
}: {
  logRoot: string;
  expectedConsumerCount: number;
}): Promise<FetchingAggregateSummary> {
  const summaries: Array<FetchingConsumerSummary | null> = await Promise.all(
    Array.from({ length: expectedConsumerCount }, (_, offset) => (
      readJsonIfExists<FetchingConsumerSummary>(
        buildFetchingConsumerSummaryPath(logRoot, offset + 1),
      )
    )),
  );

  const failedConsumerIndices: number[] = [];
  const completedSummaries: FetchingConsumerSummary[] = [];

  summaries.forEach((summary, index) => {
    if (isCompleteFetchingConsumerSummary(summary)) {
      completedSummaries.push(summary);
      return;
    }
    failedConsumerIndices.push(index + 1);
  });

  const comparableConsumerCount = completedSummaries.filter(
    (summary) => summary.isComparableWindow,
  ).length;
  const completedConsumerCount = completedSummaries.length;

  return {
    summaryVersion: 1,
    approach: "fetching",
    targetWindowCount: expectedConsumerCount,
    emittedFinalWindowCount: comparableConsumerCount,
    finalWindowNumbers: completedSummaries
      .map((summary) => summary.windowNumber)
      .sort((left, right) => left - right),
    stoppedAfterTargetWindows: comparableConsumerCount === expectedConsumerCount,
    stopReason:
      comparableConsumerCount === expectedConsumerCount
        ? "target_window_count_reached"
        : "consumer_artifact_incomplete",
    expectedConsumerCount,
    completedConsumerCount,
    comparableConsumerCount,
    failedConsumerIndices,
    allConsumersComplete: completedConsumerCount === expectedConsumerCount,
    allConsumersComparable: comparableConsumerCount === expectedConsumerCount,
    aggregateWrittenAt: new Date().toISOString(),
  };
}
