import fs from "fs";
import { buildBenchmarkWindowMetadata } from "../../../util/runtimeConfig";
import { profileStageSync } from "../../../util/profiling";

export class ApproximationDiagnosticsWriter {
  private latencyLogStream!: fs.WriteStream;
  private runtimeReplayStartWallClockTime: number | null = null;
  private benchmarkEventTimeAnchor: number | null = null;
  private warnedAboutMissingWallClockAnchor: boolean = false;
  private readonly maxPlausibleLatencyMs = 120000;
  private readonly verboseConsoleLogging: boolean =
    process.env.STREAMING_QUERY_HIVE_VERBOSE_LATENCY_LOGS === "1";

  constructor(
    private readonly queryRegisteredTime: number,
    private windowRange: number,
    private windowSlide: number,
    latencyLogFilePath = "approximation_latency_log.csv",
  ) {
    this.initializeLatencyLogging(latencyLogFilePath);
  }

  updateWindowConfig(windowRange: number, windowSlide: number): void {
    this.windowRange = windowRange;
    this.windowSlide = windowSlide;
  }

  updateTimeAnchors(args: {
    runtimeReplayStartWallClockTime: number | null;
    benchmarkEventTimeAnchor: number | null;
  }): void {
    this.runtimeReplayStartWallClockTime =
      Number.isFinite(args.runtimeReplayStartWallClockTime)
        ? Number(args.runtimeReplayStartWallClockTime)
        : null;
    this.benchmarkEventTimeAnchor = Number.isFinite(args.benchmarkEventTimeAnchor)
      ? Number(args.benchmarkEventTimeAnchor)
      : null;
  }

  cleanup(): void {
    if (this.latencyLogStream) {
      this.latencyLogStream.end();
    }
  }

  /**
   * Initialize latency logging
   */
  private initializeLatencyLogging(latencyLogFilePath: string): void {
    const writeLatencyHeader = !fs.existsSync(latencyLogFilePath);
    this.latencyLogStream = fs.createWriteStream(latencyLogFilePath, {
      flags: "a",
    });

    if (writeLatencyHeader) {
      this.latencyLogStream.write(
        "window_number,query_registered_at,first_data_received_at,expected_window_close,registration_anchored_expected_close,event_time_window_close,wall_clock_window_close,last_data_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_data_ms,wall_clock_close_to_result_ms,latency_domain_status,approximation_status,window_semantics,logical_trigger_time,window_start,window_end,window_duration_ms,window_data_close_time,latency_from_logical_trigger_ms,latency_from_window_close_ms,coverage_complete,is_partial_window,is_comparable_window,metadata_source,result_value\n",
      );
    }
  }

  private resolveWallClockWindowClose(eventTimeWindowClose: number | null): {
    wallClockWindowClose: number | null;
    latencyDomainStatus:
      | "wall_clock_mapped"
      | "runtime_anchor_missing"
      | "event_time_missing"
      | "domain_mismatch";
  } {
    if (!Number.isFinite(eventTimeWindowClose)) {
      return {
        wallClockWindowClose: null,
        latencyDomainStatus: "event_time_missing",
      };
    }

    if (
      !Number.isFinite(this.runtimeReplayStartWallClockTime) ||
      !Number.isFinite(this.benchmarkEventTimeAnchor)
    ) {
      return {
        wallClockWindowClose: null,
        latencyDomainStatus: "runtime_anchor_missing",
      };
    }

    const wallClockWindowClose =
      Number(this.runtimeReplayStartWallClockTime) +
      (Number(eventTimeWindowClose) - Number(this.benchmarkEventTimeAnchor));

    if (Math.abs(wallClockWindowClose - this.queryRegisteredTime) > 86400000) {
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

  private validateWallClockLatency(
    wallClockWindowClose: number | null,
    resultTime: number,
    latencyDomainStatus:
      | "wall_clock_mapped"
      | "runtime_anchor_missing"
      | "event_time_missing"
      | "domain_mismatch",
  ): {
    wallClockWindowClose: number | null;
    latencyDomainStatus:
      | "wall_clock_mapped"
      | "runtime_anchor_missing"
      | "event_time_missing"
      | "domain_mismatch";
  } {
    if (
      latencyDomainStatus !== "wall_clock_mapped" ||
      wallClockWindowClose === null
    ) {
      return { wallClockWindowClose, latencyDomainStatus };
    }

    const wallClockCloseToResultMs = resultTime - wallClockWindowClose;
    if (Math.abs(wallClockCloseToResultMs) > this.maxPlausibleLatencyMs) {
      return {
        wallClockWindowClose: null,
        latencyDomainStatus: "domain_mismatch",
      };
    }

    return { wallClockWindowClose, latencyDomainStatus };
  }

  /**
   * Calculate expected window close time for a given window number
   * Window N closes at: queryRegisteredTime + RANGE + (N-1) * STEP
   */
  getExpectedWindowCloseTime(windowNumber: number): number {
    return (
      this.queryRegisteredTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide
    );
  }

  /**
   * Log latency measurement with multiple metrics
   */
  logLatency(
    windowNumber: number,
    firstDataReceivedTime: number,
    expectedWindowClose: number,
    lastDataReceivedAt: number,
    resultTime: number,
    value: string,
    metadata = buildBenchmarkWindowMetadata({
      windowSemantics: process.env.RSP_WINDOW_SEMANTICS || "trailing",
      logicalTriggerTime: expectedWindowClose - (this.windowRange / 2),
      windowStart: expectedWindowClose - this.windowRange,
      windowEnd: expectedWindowClose,
      windowDataCloseTime: expectedWindowClose,
      resultEmittedAt: resultTime,
      metadataSource: "reconstructed",
    }),
  ): void {
    const latencyFromQueryReg = resultTime - expectedWindowClose;
    const expectedFromDataStart =
      firstDataReceivedTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide;
    const latencyFromDataStart = resultTime - expectedFromDataStart;
    const latencyFromLastData = resultTime - lastDataReceivedAt;
    const eventTimeWindowClose = metadata.windowDataCloseTime ?? null;
    const resolvedWallClock =
      this.resolveWallClockWindowClose(eventTimeWindowClose);
    const { wallClockWindowClose, latencyDomainStatus } =
      this.validateWallClockLatency(
        resolvedWallClock.wallClockWindowClose,
        resultTime,
        resolvedWallClock.latencyDomainStatus,
      );
    const wallClockCloseToResultMs =
      wallClockWindowClose !== null ? resultTime - wallClockWindowClose : "";
    const approximationStatus =
      (metadata as Record<string, unknown>).approximationStatus ??
      "completed_window_approximation";
    const windowDurationMs =
      Number.isFinite(metadata.windowStart) && Number.isFinite(metadata.windowEnd)
        ? Number(metadata.windowEnd) - Number(metadata.windowStart)
        : "";
    const coverageComplete =
      (metadata as Record<string, unknown>).coverageComplete ?? true;
    const isComparableWindow =
      (metadata as Record<string, unknown>).isComparableWindow ?? true;
    const isPartialWindow =
      (metadata as Record<string, unknown>).isPartialWindow ?? false;
    const latencyFromLogicalTriggerMs =
      wallClockWindowClose !== null &&
      Number.isFinite(metadata.logicalTriggerTime) &&
      Number.isFinite(this.benchmarkEventTimeAnchor) &&
      Number.isFinite(this.runtimeReplayStartWallClockTime)
        ? resultTime -
          (Number(this.runtimeReplayStartWallClockTime) +
            (Number(metadata.logicalTriggerTime) -
              Number(this.benchmarkEventTimeAnchor)))
        : "";
    const latencyFromWindowCloseMs =
      wallClockWindowClose !== null ? resultTime - wallClockWindowClose : "";

    if (
      latencyDomainStatus !== "wall_clock_mapped" &&
      !this.warnedAboutMissingWallClockAnchor
    ) {
      this.warnedAboutMissingWallClockAnchor = true;
      console.error(
        `Approximation latency wall-clock anchor unavailable; runtime close-to-result latency omitted (status=${latencyDomainStatus}).`,
      );
    }

    profileStageSync("approximation.diagnostics_write_ms", () => {
      if (this.latencyLogStream) {
        this.latencyLogStream.write(
          `${windowNumber},${this.queryRegisteredTime},${firstDataReceivedTime},${expectedWindowClose},${expectedWindowClose},${eventTimeWindowClose ?? ""},${wallClockWindowClose ?? ""},${lastDataReceivedAt},${resultTime},${latencyFromQueryReg},${latencyFromDataStart},${latencyFromLastData},${wallClockCloseToResultMs},${latencyDomainStatus},${approximationStatus},${metadata.windowSemantics},${metadata.logicalTriggerTime ?? ""},${metadata.windowStart ?? ""},${metadata.windowEnd ?? ""},${windowDurationMs},${metadata.windowDataCloseTime ?? ""},${latencyFromLogicalTriggerMs},${latencyFromWindowCloseMs},${coverageComplete},${isPartialWindow},${isComparableWindow},${metadata.metadataSource},${value}\n`,
        );
      }
    });
    if (this.verboseConsoleLogging) {
      console.log(`LATENCY: Window ${windowNumber}:`);
      console.log(
        `  - From query registration: ${latencyFromQueryReg}ms (expected close: ${expectedWindowClose}, result: ${resultTime})`,
      );
      console.log(
        `  - From data start: ${latencyFromDataStart}ms (first data: ${firstDataReceivedTime}, expected: ${expectedFromDataStart}, result: ${resultTime})`,
      );
      console.log(
        `  - Processing time (last data to result): ${latencyFromLastData}ms`,
      );
      console.log(`  - Value: ${value}`);
    }
  }
}
