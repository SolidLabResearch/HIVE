import fs from "fs";

export class ApproximationDiagnosticsWriter {
  private latencyLogStream!: fs.WriteStream;

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
        "window_number,query_registered_at,first_data_received_at,expected_window_close,last_data_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_data_ms,result_value\n",
      );
    }
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
  ): void {
    const latencyFromQueryReg = resultTime - expectedWindowClose;
    const expectedFromDataStart =
      firstDataReceivedTime +
      this.windowRange +
      (windowNumber - 1) * this.windowSlide;
    const latencyFromDataStart = resultTime - expectedFromDataStart;
    const latencyFromLastData = resultTime - lastDataReceivedAt;

    if (this.latencyLogStream) {
      this.latencyLogStream.write(
        `${windowNumber},${this.queryRegisteredTime},${firstDataReceivedTime},${expectedWindowClose},${lastDataReceivedAt},${resultTime},${latencyFromQueryReg},${latencyFromDataStart},${latencyFromLastData},${value}\n`,
      );
    }
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
