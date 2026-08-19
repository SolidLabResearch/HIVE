import fs from "fs";
import path from "path";

let atomicWriteCounter = 0;
let traceSequence = 0;

export type ApproximationTraceEvent =
  | "startup_ready"
  | "publisher_started"
  | "consumer_result_observed"
  | "consumer_latency_write_started"
  | "consumer_latency_write_completed"
  | "consumer_summary_write_completed"
  | "completion_event_received"
  | "all_completion_promises_resolved"
  | "aggregate_write_started"
  | "aggregate_write_completed"
  | "shutdown_started"
  | "parent_exit"
  | "aggregate_read_started"
  | "consumer_summary_read_completed"
  | "completion_wait_deadline_exceeded"
  | "worker_exit_observed";

export type ApproximationTraceRecord = {
  sequence: number;
  timestamp: number;
  pid: number;
  consumerIndex: number | null;
  event: ApproximationTraceEvent;
  targetPath?: string | null;
  detail?: Record<string, unknown>;
};

export type ApproximationConsumerSummary = {
  summaryVersion: 1;
  approach: "approximation";
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
  stopReason: string;
  finalWindowNumbers: number[];
};

export type ApproximationAggregateSummary = {
  summaryVersion: 1;
  approach: "approximation";
  targetWindowCount: number;
  stoppedAfterTargetWindows: boolean;
  expectedConsumerCount: number;
  completedConsumerCount: number;
  comparableConsumerCount: number;
  failedConsumerIndices: number[];
  allConsumersComplete: boolean;
  allConsumersComparable: boolean;
  emittedFinalWindowCount: number;
  finalWindowNumbers: number[];
  aggregateWrittenAt: string;
  stopReason: string;
};

export function buildApproximationConsumerSummaryPath(
  logRoot: string,
  consumerIndex: number,
): string {
  return path.join(
    logRoot,
    `benchmark_window_cap_summary_consumer_${consumerIndex}.json`,
  );
}

export function buildApproximationAggregateSummaryPath(logRoot: string): string {
  return path.join(logRoot, "benchmark_window_cap_summary.json");
}

export function buildApproximationStartupReadyPath(logRoot: string): string {
  return path.join(logRoot, "startup_ready.json");
}

export function buildApproximationTracePath(logRoot: string): string {
  return path.join(logRoot, "approximation_completion_trace.ndjson");
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

export function appendApproximationTrace(
  tracePath: string,
  record: Omit<ApproximationTraceRecord, "sequence" | "timestamp" | "pid">,
): void {
  fs.mkdirSync(path.dirname(tracePath), { recursive: true });
  fs.appendFileSync(
    tracePath,
    `${JSON.stringify({
      sequence: ++traceSequence,
      timestamp: Date.now(),
      pid: process.pid,
      ...record,
    })}\n`,
  );
}

export function isCompleteApproximationConsumerSummary(
  value: ApproximationConsumerSummary | null | undefined,
): value is ApproximationConsumerSummary {
  return Boolean(
    value &&
      value.summaryVersion === 1 &&
      value.approach === "approximation" &&
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

export async function aggregateApproximationConsumerSummaries({
  logRoot,
  expectedConsumerCount,
}: {
  logRoot: string;
  expectedConsumerCount: number;
}): Promise<ApproximationAggregateSummary> {
  const summaries = await Promise.all(
    Array.from({ length: expectedConsumerCount }, (_, offset) =>
      readJsonIfExists<ApproximationConsumerSummary>(
        buildApproximationConsumerSummaryPath(logRoot, offset + 1),
      ),
    ),
  );

  const failedConsumerIndices: number[] = [];
  const completedSummaries: ApproximationConsumerSummary[] = [];
  summaries.forEach((summary, index) => {
    if (isCompleteApproximationConsumerSummary(summary)) {
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
    approach: "approximation",
    targetWindowCount: expectedConsumerCount,
    stoppedAfterTargetWindows: comparableConsumerCount === expectedConsumerCount,
    expectedConsumerCount,
    completedConsumerCount,
    comparableConsumerCount,
    failedConsumerIndices,
    allConsumersComplete: completedConsumerCount === expectedConsumerCount,
    allConsumersComparable: comparableConsumerCount === expectedConsumerCount,
    emittedFinalWindowCount: comparableConsumerCount,
    finalWindowNumbers: completedSummaries
      .map((summary) => summary.windowNumber)
      .sort((left, right) => left - right),
    aggregateWrittenAt: new Date().toISOString(),
    stopReason:
      comparableConsumerCount === expectedConsumerCount
        ? "target_window_count_reached"
        : "consumer_artifact_incomplete",
  };
}

export async function writeApproximationAggregateSummaryAtomic(
  targetPath: string,
  summary: ApproximationAggregateSummary,
  writeSummaryImpl: (targetPath: string, value: unknown) => Promise<void> = writeAtomicJson,
): Promise<void> {
  await writeSummaryImpl(targetPath, summary);
}
