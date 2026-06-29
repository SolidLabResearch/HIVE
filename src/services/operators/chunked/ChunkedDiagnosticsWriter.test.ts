import { PassThrough } from "stream";
import { logLatency } from "./ChunkedDiagnosticsWriter";

describe("ChunkedDiagnosticsWriter logLatency", () => {
  test("warns instead of throwing when mapped wall-clock latency is implausibly large", () => {
    const latencyLogStream = new PassThrough();
    const writtenChunks: string[] = [];
    latencyLogStream.on("data", (chunk) => {
      writtenChunks.push(chunk.toString());
    });

    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    expect(() =>
      logLatency({
        windowNumber: 1,
        expectedWindowClose: 2_000,
        lastChunkReceivedAt: 3_000,
        intervalTriggerAt: 3_100,
        resultTime: 500_000,
        value: "1.23",
        queryRegisteredTime: 1_000,
        firstDataReceivedTime: 1_500,
        windowRange: 30_000,
        windowSlide: 15_000,
        comparableOutputCadenceOnly: false,
        useImmediateTrigger: true,
        chunkWindowMap: new Map(),
        chunkArrivalTimes: new Map(),
        runtimeReplayStartWallClockTime: 10_000,
        benchmarkEventTimeAnchor: 1_000,
        latencyLogStream: latencyLogStream as any,
        metadata: {
          windowSemantics: "trailing",
          logicalTriggerTime: 2_000,
          windowStart: 1_000,
          windowEnd: 2_000,
          windowDataCloseTime: 2_000,
          resultEmittedAt: 500_000,
          latencyFromLogicalTriggerMs: null,
          latencyFromWindowCloseMs: null,
          metadataSource: "reconstructed",
        },
      }),
    ).not.toThrow();

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(writtenChunks.join("")).toContain("1,1000,1500");

    warnSpy.mockRestore();
  });
});
