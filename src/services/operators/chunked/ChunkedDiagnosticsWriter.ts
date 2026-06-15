import fs from "fs";
import { profileSync } from "../../../util/profiling";
import type { CSVLogger } from "../../../util/logger/CSVLogger";
import type {
  ChunkEmissionProofEntry,
  ComparableWindowDiagnostics,
  ParentPartialDiagnostics,
} from "./types";

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
      "window_number,query_registered_at,first_data_received_at,expected_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at,delay_past_expected_close_ms,delay_past_data_start_ms,interval_wait_ms,computation_ms,result_value,required_chunk_intervals,last_required_chunk_received_at,semantic_ready_at,window_close_to_ready_ms,ready_to_emit_ms,trigger_type,emission_reason\n",
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
  latencyLogStream?: fs.WriteStream;
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
  let windowCloseToReadyMs = args.resultTime - args.expectedWindowClose;
  let readyToEmitMs = 0;
  const triggerType = args.triggerSource ? args.triggerSource.toLowerCase() : "interval";
  const emissionReason = args.proofEntry?.emissionReason ?? "unknown";

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
    windowCloseToReadyMs = semanticReadyAt - args.expectedWindowClose;
    readyToEmitMs = args.resultTime - semanticReadyAt;
  }

  if (args.latencyLogStream) {
    args.latencyLogStream.write(
      `${args.windowNumber},${args.queryRegisteredTime},${args.firstDataReceivedTime},${args.expectedWindowClose},${args.lastChunkReceivedAt},${args.intervalTriggerAt},${args.resultTime},${latencyFromQueryReg},${latencyFromDataStart},${intervalWaitTime},${computationTime},${args.value},${requiredChunkIntervals},${lastRequiredChunkReceivedAt},${semanticReadyAt},${windowCloseToReadyMs},${readyToEmitMs},${triggerType},${emissionReason}\n`,
    );
  }
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
  profileSync("diagnostics_write_time_ms", () => {
    if (args.diagnosticsLogStream) {
      args.diagnosticsLogStream.write(
        `${args.benchmarkEventTimeAnchor ?? ""},${args.diagnostics.externalWindowNumber},${args.diagnostics.externalWindowStart},${args.diagnostics.externalWindowEnd},"${args.diagnostics.internalChunkGroupIds.join("|")}","${JSON.stringify(args.diagnostics.internalChunks).replace(/"/g, '""')}",${args.diagnostics.recomposedCount ?? ""},${args.diagnostics.recomposedSum ?? ""},${args.diagnostics.recomposedAvg ?? ""},${args.diagnostics.recomposedMin ?? ""},${args.diagnostics.recomposedMax ?? ""},${args.diagnostics.resultValue}\n`,
      );
    }
  });
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
  profileSync("diagnostics_write_time_ms", () => {
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
  });

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
