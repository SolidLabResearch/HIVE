const writeMock = jest.fn();
const endMock = jest.fn();

jest.mock("fs", () => ({
  ...jest.requireActual("fs"),
  existsSync: jest.fn().mockReturnValue(false),
  createWriteStream: jest.fn().mockReturnValue({
    write: writeMock,
    end: endMock,
  }),
}));

import { ApproximationDiagnosticsWriter } from "./ApproximationDiagnosticsWriter";

describe("ApproximationDiagnosticsWriter", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("omits close-to-result latency when only event-time window metadata is available", () => {
    const errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});
    const writer = new ApproximationDiagnosticsWriter(1782236789582, 120000, 60000);

    writer.updateTimeAnchors({
      runtimeReplayStartWallClockTime: 1756122905256,
      benchmarkEventTimeAnchor: 1756122905256,
    });

    writer.logLatency(
      1,
      1782236822084,
      1782236909582,
      1782236914575,
      1782236914575,
      "1.0025041166666666",
      {
        windowSemantics: "trailing",
        logicalTriggerTime: 1756122965256,
        windowStart: 1756122905256,
        windowEnd: 1756123025256,
        windowDataCloseTime: 1756123025256,
        resultEmittedAt: 1782236914575,
        latencyFromLogicalTriggerMs: null,
        latencyFromWindowCloseMs: null,
        metadataSource: "reconstructed",
      },
    );

    const dataLine = writeMock.mock.calls[1][0] as string;
    expect(dataLine).toContain(",domain_mismatch,");
    expect(dataLine).toContain(",1756123025256,,1782236914575,");
    expect(dataLine).not.toContain("26105071228");
    expect(errorSpy).toHaveBeenCalledWith(
      "Approximation latency wall-clock anchor unavailable; runtime close-to-result latency omitted (status=domain_mismatch).",
    );

    errorSpy.mockRestore();
    writer.cleanup();
  });

  test("writes wall-clock-mapped close-to-result latency when anchors share the same domain", () => {
    const writer = new ApproximationDiagnosticsWriter(1782236789582, 120000, 60000);

    writer.updateTimeAnchors({
      runtimeReplayStartWallClockTime: 1782236791119,
      benchmarkEventTimeAnchor: 1756122905256,
    });

    writer.logLatency(
      1,
      1782236822084,
      1782236909582,
      1782236914575,
      1782236914575,
      "1.0025041166666666",
      {
        windowSemantics: "trailing",
        logicalTriggerTime: 1756122965256,
        windowStart: 1756122905256,
        windowEnd: 1756123025256,
        windowDataCloseTime: 1756123025256,
        resultEmittedAt: 1782236914575,
        latencyFromLogicalTriggerMs: null,
        latencyFromWindowCloseMs: null,
        metadataSource: "reconstructed",
      },
    );

    const dataLine = writeMock.mock.calls[1][0] as string;
    expect(dataLine).toContain(",1782236911119,");
    expect(dataLine).toContain(",3456,wall_clock_mapped,");

    writer.cleanup();
  });

  test("downgrades implausible wall-clock latency to domain_mismatch instead of throwing", () => {
    const errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});
    const writer = new ApproximationDiagnosticsWriter(1782236789582, 120000, 60000);

    writer.updateTimeAnchors({
      runtimeReplayStartWallClockTime: 1782236791119,
      benchmarkEventTimeAnchor: 1756122905256,
    });

    expect(() => writer.logLatency(
      1,
      1782236822084,
      1782236909582,
      1782236914575,
      1782237340575,
      "1.0025041166666666",
      {
        windowSemantics: "trailing",
        logicalTriggerTime: 1756122965256,
        windowStart: 1756122905256,
        windowEnd: 1756123025256,
        windowDataCloseTime: 1756123025256,
        resultEmittedAt: 1782237340575,
        latencyFromLogicalTriggerMs: null,
        latencyFromWindowCloseMs: null,
        metadataSource: "reconstructed",
      },
    )).not.toThrow();

    const dataLine = writeMock.mock.calls[1][0] as string;
    expect(dataLine).toContain(",domain_mismatch,");
    expect(dataLine).toContain(",,domain_mismatch,");
    expect(errorSpy).toHaveBeenCalledWith(
      "Approximation latency wall-clock anchor unavailable; runtime close-to-result latency omitted (status=domain_mismatch).",
    );

    errorSpy.mockRestore();
    writer.cleanup();
  });
});
