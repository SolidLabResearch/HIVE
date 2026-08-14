import { EventEmitter } from "events";

const worker = Object.assign(new EventEmitter(), {
  pid: 4242,
  send: jest.fn((_message: unknown, callback?: (error?: Error) => void) => callback?.()),
});

jest.mock("child_process", () => ({ fork: jest.fn(() => worker) }));

import { SharedApproximationRuntime } from "./SharedApproximationRuntime";

describe("SharedApproximationRuntime completion ownership", () => {
  beforeEach(() => {
    worker.send.mockClear();
  });

  test("completion of one plan retains the worker and its other active plans", async () => {
    const onRuntimeFailed = jest.fn();
    const runtime = new SharedApproximationRuntime(onRuntimeFailed);
    const planA = { planId: "plan-a", query: "A", subQueries: [], outputTopic: "out/a", executionId: "a" };
    const planB = { planId: "plan-b", query: "B", subQueries: [], outputTopic: "out/b", executionId: "b" };
    const readyA = runtime.registerPlan(planA);
    const readyB = runtime.registerPlan(planB);
    worker.emit("message", { type: "planReady", planId: planA.planId });
    worker.emit("message", { type: "planReady", planId: planB.planId });
    await Promise.all([readyA, readyB]);

    worker.emit("message", { type: "planComplete", planId: planA.planId });

    expect(runtime.activePlanCount).toBe(1);
    expect(runtime.getPid()).toBe(4242);
    expect(onRuntimeFailed).not.toHaveBeenCalled();
    expect(worker.send).toHaveBeenCalledWith(expect.objectContaining({ type: "registerPlan", planId: planB.planId }), expect.any(Function));
  });
});
