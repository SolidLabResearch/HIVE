jest.mock("fs", () => ({
    ...jest.requireActual("fs"),
    writeFileSync: jest.fn(),
}));

import fs from "fs";
import { maybeFinalizeNaiveBenchmarkTargetWindowCount } from "./StreamingQueryNaiveDistributedApproachOrchestrator";

describe("StreamingQueryNaiveDistributedApproachOrchestrator benchmark target cap", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    test("writes the benchmark summary and schedules shutdown when the target window count is reached", () => {
        const scheduleShutdown = jest.fn();
        const finalizedWindowNumbers = new Set([1, 2, 3, 4, 5]);

        const finalized = maybeFinalizeNaiveBenchmarkTargetWindowCount({
            finalizedWindowNumbers,
            benchmarkTargetWindowCount: 5,
            benchmarkWindowSummaryPath: "/tmp/benchmark_window_cap_summary.json",
            scheduleShutdown,
        });

        expect(finalized).toBe(true);
        expect(fs.writeFileSync).toHaveBeenCalledTimes(1);
        expect(fs.writeFileSync).toHaveBeenCalledWith(
            "/tmp/benchmark_window_cap_summary.json",
            expect.stringContaining('"finalWindowNumbers": [\n    1,\n    2,\n    3,\n    4,\n    5\n  ]'),
        );
        expect(scheduleShutdown).toHaveBeenCalledTimes(1);
    });

    test("does nothing before the target window count is reached", () => {
        const scheduleShutdown = jest.fn();

        const finalized = maybeFinalizeNaiveBenchmarkTargetWindowCount({
            finalizedWindowNumbers: new Set([1, 2, 3, 4]),
            benchmarkTargetWindowCount: 5,
            benchmarkWindowSummaryPath: "/tmp/benchmark_window_cap_summary.json",
            scheduleShutdown,
        });

        expect(finalized).toBe(false);
        expect(fs.writeFileSync).not.toHaveBeenCalled();
        expect(scheduleShutdown).not.toHaveBeenCalled();
    });
});
