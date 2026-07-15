import fs from "fs";
import { profileStageSync, profileSync } from "../../../util/profiling";
import { buildBenchmarkWindowMetadata } from "../../../util/runtimeConfig";
import type { CSVLogger } from "../../../util/logger/CSVLogger";
import type {
  ChunkEmissionProofEntry,
  ComparableWindowDiagnostics,
  ParentPartialDiagnostics,
} from "./types";

export type ChunkedLatencyDomainResolution = {
  wallClockWindowClose: number | null;
  latencyDomainStatus:
    | "wall_clock_mapped"
    | "runtime_anchor_missing"
    | "event_time_missing"
    | "domain_mismatch";
};

export function resolveChunkedWallClockWindowClose(args: {
  eventTimeWindowClose: number | null;
  runtimeReplayStartWallClockTime: number | null;
  benchmarkEventTimeAnchor: number | null;
  queryRegisteredTime: number;
}): ChunkedLatencyDomainResolution {
  if (!Number.isFinite(args.eventTimeWindowClose)) {
    return {
      wallClockWindowClose: null,
      latencyDomainStatus: "event_time_missing",
    };
  }

  if (
    !Number.isFinite(args.runtimeReplayStartWallClockTime) ||
    !Number.isFinite(args.benchmarkEventTimeAnchor)
  ) {
    return {
      wallClockWindowClose: null,
      latencyDomainStatus: "runtime_anchor_missing",
    };
  }

  const wallClockWindowClose =
    Number(args.runtimeReplayStartWallClockTime) +
    (Number(args.eventTimeWindowClose) - Number(args.benchmarkEventTimeAnchor));

  if (Math.abs(wallClockWindowClose - args.queryRegisteredTime) > 86400000) {
    return {
      wallClockWindowClose: null,
      latencyDomainStatus: "domain_mismatch",
    };
  }

  return {
    wallClockWindowClose,
    latencyDomainStatus: "wall_clock_mapped",
  };
}

export function initializeLatencyLogging(
  resolveLogFilePath: (fileName: string) => string,
): {
  latencyLogStream: fs.WriteStream;
  diagnosticsLogStream: fs.WriteStream;
  parentPartialDiagnosticsStream: fs.WriteStream;
  parentPartialDiagnosticsFilePath: string;
} {
  const consumerIdx = process.env.K_SCALING_CONSUMER_INDEX ? `_consumer_${process.env.K_SCALING_CONSUMER_INDEX}` : "";
  const latencyLogFilePath = resolveLogFilePath(`chunked_latency_log${consumerIdx}.csv`);
  const writeLatencyHeader = !fs.existsSync(latencyLogFilePath);
  const latencyLogStream = fs.createWriteStream(latencyLogFilePath, {
    flags: "a",
  });

    if (writeLatencyHeader) {
      latencyLogStream.write(
        "window_number,query_registered_at,first_data_received_at,expected_window_close,registration_anchored_expected_close,event_time_window_start,event_time_window_end,event_time_window_close,wall_clock_window_close,anchor_aligned_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at,delay_past_expected_close_ms,delay_past_data_start_ms,interval_wait_ms,computation_ms,result_value,required_chunk_intervals,last_required_chunk_received_at,semantic_ready_at,window_close_to_ready_ms,ready_to_emit_ms,wall_clock_close_to_result_ms,anchor_aligned_window_close_to_result_ms,latency_domain_status,trigger_type,emission_reason,window_semantics,logical_trigger_time,window_start,window_end,window_duration_ms,window_data_close_time,latency_from_logical_trigger_ms,latency_from_window_close_ms,coverage_complete,is_partial_window,is_comparable_window,metadata_source\n",
      );
    }

  const diagnosticsLogFilePath = resolveLogFilePath(`chunked_window_diagnostics${consumerIdx}.csv`);
  const writeDiagnosticsHeader = !fs.existsSync(diagnosticsLogFilePath);
  const diagnosticsLogStream = fs.createWriteStream(diagnosticsLogFilePath, {
    flags: "a",
  });

  if (writeDiagnosticsHeader) {
    diagnosticsLogStream.write(
      "benchmark_event_time_anchor,external_window_number,external_window_start,external_window_end,internal_chunk_ids,internal_chunks_json,recomposed_count,recomposed_sum,recomposed_avg,recomposed_min,recomposed_max,result_value\n",
    );
  }

  const parentPartialDiagnosticsFilePath = resolveLogFilePath(
    `chunked_parent_partial_latency_log${consumerIdx}.csv`,
  );
  const writeParentPartialHeader = !fs.existsSync(
    parentPartialDiagnosticsFilePath,
  );
  const parentPartialDiagnosticsStream = fs.createWriteStream(
    parentPartialDiagnosticsFilePath,
    { flags: "a" },
  );
  if (writeParentPartialHeader) {
    parentPartialDiagnosticsStream.write(
      "output_type,comparable,benchmark_event_time_anchor,parent_window_number,parent_window_start,parent_window_end_or_covered_until,parent_range_ms,covered_duration_ms,chunks_used,event_count,sum,avg,min,max,result_value,emitted_at_ms,elapsed_since_registration_ms,delay_past_partial_trigger_ms,internal_chunk_ids,internal_chunks_json\n",
    );
  }

  return {
    latencyLogStream,
    diagnosticsLogStream,
    parentPartialDiagnosticsStream,
    parentPartialDiagnosticsFilePath,
  };
}

export function logLatency(args: {
  windowNumber: number;
  expectedWindowClose: number;
  lastChunkReceivedAt: number;
  intervalTriggerAt: number;
  resultTime: number;
  value: string;
  proofEntry?: ChunkEmissionProofEntry | null;
  triggerSource?: string;
  queryRegisteredTime: number;
  firstDataReceivedTime: number;
  windowRange: number;
  windowSlide: number;
  comparableOutputCadenceOnly: boolean;
  useImmediateTrigger: boolean;
  chunkWindowMap: Map<string, { start: number; end: number }>;
  chunkArrivalTimes: Map<string, number>;
  runtimeReplayStartWallClockTime: number | null;
  benchmarkEventTimeAnchor: number | null;
  latencyLogStream?: fs.WriteStream;
  metadata?: ReturnType<typeof buildBenchmarkWindowMetadata>;
}): void {
  const latencyFromQueryReg = args.resultTime - args.expectedWindowClose;
  const expectedFromDataStart =
    args.firstDataReceivedTime +
    args.windowRange +
    (args.windowNumber - 1) * args.windowSlide;
  const latencyFromDataStart = args.resultTime - expectedFromDataStart;
  const intervalWaitTime = args.intervalTriggerAt - args.lastChunkReceivedAt;
  const computationTime = args.resultTime - args.intervalTriggerAt;

  let requiredChunkIntervals = "";
  let lastRequiredChunkReceivedAt = args.lastChunkReceivedAt;
  let semanticReadyAt = args.lastChunkReceivedAt;
  let windowCloseToReadyMs: number | "" = "";
  let readyToEmitMs = 0;
  const maxPlausibleLatencyMs = 120000;
  const triggerType = args.triggerSource ? args.triggerSource.toLowerCase() : "interval";
  const emissionReason = args.proofEntry?.emissionReason ?? "unknown";
  const metadata = args.metadata ?? buildBenchmarkWindowMetadata({
    windowSemantics: process.env.RSP_WINDOW_SEMANTICS || "trailing",
    logicalTriggerTime: args.expectedWindowClose - 60000,
    windowStart: args.expectedWindowClose - 120000,
    windowEnd: args.expectedWindowClose,
    windowDataCloseTime: args.expectedWindowClose,
    resultEmittedAt: args.resultTime,
    metadataSource: "reconstructed",
  });
  const eventTimeWindowStart = metadata.windowStart ?? null;
  const eventTimeWindowEnd = metadata.windowEnd ?? null;
  const windowDurationMs =
    Number.isFinite(metadata.windowStart) && Number.isFinite(metadata.windowEnd)
      ? Number(metadata.windowEnd) - Number(metadata.windowStart)
      : "";
  const coverageComplete = args.proofEntry?.coverageComplete ?? true;
  const isComparableWindow =
    (metadata as Record<string, unknown>).isComparableWindow ??
    coverageComplete;
  const isPartialWindow =
    (metadata as Record<string, unknown>).isPartialWindow ??
    !Boolean(isComparableWindow);
  const eventTimeWindowClose = metadata.windowDataCloseTime ?? null;
  const { wallClockWindowClose, latencyDomainStatus } =
    resolveChunkedWallClockWindowClose({
      eventTimeWindowClose,
      runtimeReplayStartWallClockTime: args.runtimeReplayStartWallClockTime,
      benchmarkEventTimeAnchor: args.benchmarkEventTimeAnchor,
      queryRegisteredTime: args.queryRegisteredTime,
    });
  const wallClockCloseToResultMs =
    wallClockWindowClose !== null ? args.resultTime - wallClockWindowClose : "";
  const anchorAlignedWindowClose = wallClockWindowClose ?? "";
  const anchorAlignedWindowCloseToResultMs = wallClockCloseToResultMs;
  const latencyFromLogicalTriggerMs =
    wallClockWindowClose !== null &&
    Number.isFinite(metadata.logicalTriggerTime) &&
    Number.isFinite(args.benchmarkEventTimeAnchor) &&
    Number.isFinite(args.runtimeReplayStartWallClockTime)
      ? args.resultTime -
        (Number(args.runtimeReplayStartWallClockTime) +
          (Number(metadata.logicalTriggerTime) -
            Number(args.benchmarkEventTimeAnchor)))
      : "";
  const latencyFromWindowCloseMs =
    wallClockWindowClose !== null ? args.resultTime - wallClockWindowClose : "";

  if (args.proofEntry && args.proofEntry.receivedChunksUsedBySubquery) {
    const intervals: string[] = [];
    let maxArrival = 0;
    for (const chunkIds of Object.values(args.proofEntry.receivedChunksUsedBySubquery)) {
      for (const chunkId of chunkIds) {
        const win = args.chunkWindowMap.get(chunkId);
        if (win) {
          intervals.push(`${win.start}-${win.end}`);
        }
        const arrival = args.chunkArrivalTimes.get(chunkId);
        if (arrival && arrival > maxArrival) {
          maxArrival = arrival;
        }
      }
    }
    requiredChunkIntervals = Array.from(new Set(intervals)).sort().join("|");
    if (maxArrival > 0) {
      lastRequiredChunkReceivedAt = maxArrival;
      semanticReadyAt = maxArrival;
    }
    windowCloseToReadyMs =
      wallClockWindowClose !== null ? semanticReadyAt - wallClockWindowClose : "";
    readyToEmitMs = args.resultTime - semanticReadyAt;
  }

  if (
    wallClockWindowClose !== null &&
    Math.abs(Number(wallClockCloseToResultMs)) > maxPlausibleLatencyMs
  ) {
    console.warn(
      `Chunked latency plausibility check failed for window ${args.windowNumber}: wall_clock_close_to_result_ms=${wallClockCloseToResultMs}`,
    );
  }

  profileStageSync("chunked.diagnostics_write_ms", () => {
    if (args.latencyLogStream) {
      args.latencyLogStream.write(
        `${args.windowNumber},${args.queryRegisteredTime},${args.firstDataReceivedTime},${args.expectedWindowClose},${args.expectedWindowClose},${eventTimeWindowStart ?? ""},${eventTimeWindowEnd ?? ""},${eventTimeWindowClose ?? ""},${wallClockWindowClose ?? ""},${anchorAlignedWindowClose},${args.lastChunkReceivedAt},${args.intervalTriggerAt},${args.resultTime},${latencyFromQueryReg},${latencyFromDataStart},${intervalWaitTime},${computationTime},${args.value},${requiredChunkIntervals},${lastRequiredChunkReceivedAt},${semanticReadyAt},${windowCloseToReadyMs},${readyToEmitMs},${wallClockCloseToResultMs},${anchorAlignedWindowCloseToResultMs},${latencyDomainStatus},${triggerType},${emissionReason},${metadata.windowSemantics},${metadata.logicalTriggerTime ?? ""},${metadata.windowStart ?? ""},${metadata.windowEnd ?? ""},${windowDurationMs},${metadata.windowDataCloseTime ?? ""},${latencyFromLogicalTriggerMs},${latencyFromWindowCloseMs},${coverageComplete},${isPartialWindow},${isComparableWindow},${metadata.metadataSource}\n`,
      );
    }
  });
  console.log(`LATENCY: Window ${args.windowNumber}:`);
  console.log(
    `  - Delay past expected close: ${latencyFromQueryReg}ms (expected close: ${args.expectedWindowClose}, result: ${args.resultTime})`,
  );
  console.log(
    `  - Delay past data start: ${latencyFromDataStart}ms (first data: ${args.firstDataReceivedTime}, expected: ${expectedFromDataStart}, result: ${args.resultTime})`,
  );
  console.log(
    `  - Interval wait time (last chunk to interval trigger): ${intervalWaitTime}ms`,
  );
  console.log(
    `  - Actual computation time (interval trigger to result): ${computationTime}ms`,
  );
  console.log(`LATENCY CONFIGURATION & TRIGGERS:`);
  console.log(`  - comparableOutputCadenceOnly: ${args.comparableOutputCadenceOnly}`);
  console.log(`  - useImmediateTrigger: ${args.useImmediateTrigger}`);
  console.log(`  - triggerSource: ${triggerType}`);
  console.log(`  - Value: ${args.value}`);
}

export function writeComparableDiagnostics(args: {
  diagnostics: ComparableWindowDiagnostics;
  benchmarkEventTimeAnchor: number | null;
  diagnosticsLogStream?: fs.WriteStream;
  debugChunksEnabled: boolean;
  logger: CSVLogger;
}): void {
  profileStageSync("chunked.diagnostics_write_ms", () => profileSync("diagnostics_write_time_ms", () => {
    if (args.diagnosticsLogStream) {
      args.diagnosticsLogStream.write(
        `${args.benchmarkEventTimeAnchor ?? ""},${args.diagnostics.externalWindowNumber},${args.diagnostics.externalWindowStart},${args.diagnostics.externalWindowEnd},"${args.diagnostics.internalChunkGroupIds.join("|")}","${JSON.stringify(args.diagnostics.internalChunks).replace(/"/g, '""')}",${args.diagnostics.recomposedCount ?? ""},${args.diagnostics.recomposedSum ?? ""},${args.diagnostics.recomposedAvg ?? ""},${args.diagnostics.recomposedMin ?? ""},${args.diagnostics.recomposedMax ?? ""},${args.diagnostics.resultValue}\n`,
      );
    }
  }));
  if (args.debugChunksEnabled) {
    args.logger.log(
      `Chunked window diagnostics: ${JSON.stringify({
        benchmark_event_time_anchor: args.benchmarkEventTimeAnchor,
        external_window_number: args.diagnostics.externalWindowNumber,
        external_window_start: args.diagnostics.externalWindowStart,
        external_window_end: args.diagnostics.externalWindowEnd,
        internal_chunks_used: args.diagnostics.internalChunkGroupIds,
        internal_chunks: args.diagnostics.internalChunks,
        recomposed_count: args.diagnostics.recomposedCount,
        recomposed_sum: args.diagnostics.recomposedSum,
        recomposed_avg: args.diagnostics.recomposedAvg,
        recomposed_min: args.diagnostics.recomposedMin,
        recomposed_max: args.diagnostics.recomposedMax,
        result_value: args.diagnostics.resultValue,
      })}`,
    );
  }
}

export function writeParentPartialDiagnostics(args: {
  diagnostics: ParentPartialDiagnostics;
  parentPartialDiagnosticsStream?: fs.WriteStream;
  debugChunksEnabled: boolean;
  logger: CSVLogger;
}): void {
  profileStageSync("chunked.diagnostics_write_ms", () => profileSync("diagnostics_write_time_ms", () => {
    if (args.parentPartialDiagnosticsStream) {
      const internalChunksJson = JSON.stringify(args.diagnostics.internalChunks).replace(
        /"/g,
        '""',
      );
      const line = [
        args.diagnostics.outputType,
        String(args.diagnostics.comparable),
        args.diagnostics.benchmarkEventTimeAnchor ?? "",
        args.diagnostics.parentWindowNumber,
        args.diagnostics.parentWindowStart,
        args.diagnostics.parentWindowEndOrCoveredUntil,
        args.diagnostics.parentRangeMs,
        args.diagnostics.coveredDurationMs,
        args.diagnostics.chunksUsed,
        args.diagnostics.eventCount ?? "",
        args.diagnostics.sum ?? "",
        args.diagnostics.avg ?? "",
        args.diagnostics.min ?? "",
        args.diagnostics.max ?? "",
        args.diagnostics.resultValue,
        args.diagnostics.emittedAtMs,
        args.diagnostics.elapsedSinceRegistrationMs,
        args.diagnostics.delayPastPartialTriggerMs,
        `"${args.diagnostics.internalChunkIds.join("|")}"`,
        `"${internalChunksJson}"`,
      ].join(",");
      args.parentPartialDiagnosticsStream.write(`${line}\n`);
    }
  }));

  if (args.debugChunksEnabled) {
    args.logger.log(
      `Chunked parent partial diagnostics: ${JSON.stringify({
        output_type: args.diagnostics.outputType,
        comparable: args.diagnostics.comparable,
        benchmark_event_time_anchor: args.diagnostics.benchmarkEventTimeAnchor,
        parent_window_number: args.diagnostics.parentWindowNumber,
        parent_window_start: args.diagnostics.parentWindowStart,
        parent_window_end_or_covered_until: args.diagnostics.parentWindowEndOrCoveredUntil,
        parent_range_ms: args.diagnostics.parentRangeMs,
        covered_duration_ms: args.diagnostics.coveredDurationMs,
        chunks_used: args.diagnostics.chunksUsed,
        event_count: args.diagnostics.eventCount,
        sum: args.diagnostics.sum,
        avg: args.diagnostics.avg,
        result_value: args.diagnostics.resultValue,
        emitted_at_ms: args.diagnostics.emittedAtMs,
        elapsed_since_registration_ms: args.diagnostics.elapsedSinceRegistrationMs,
        delay_past_partial_trigger_ms: args.diagnostics.delayPastPartialTriggerMs,
        internal_chunk_ids: args.diagnostics.internalChunkIds,
        internal_chunks: args.diagnostics.internalChunks,
      })}`,
    );
  }
}
