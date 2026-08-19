import { ApproximationApproachOperator } from "./RateBasedApproximationApproachOperator";

describe("Approximation shared process lifecycle", () => {
  const prepareCompletedOperator = (runtimeOverrides: any) => {
    const operator = new ApproximationApproachOperator(undefined, runtimeOverrides) as any;
    operator.finalizedWindowState = {
      consumerIndex: 1,
      stateObjectId: "plan-a",
      windowNumber: 1,
      windowStart: 0,
      windowEnd: 120000,
      resultValue: 1,
      resultEmittedAt: 1,
      coverageComplete: true,
      isPartialWindow: false,
      isComparableWindow: true,
    };
    operator.writeBenchmarkWindowSummary = jest.fn();
    operator.diagnosticsWriter = { flushToDisk: jest.fn().mockResolvedValue(undefined) };
    operator.writeConsumerSummary = jest.fn().mockResolvedValue("summary.json");
    operator.notifyCompletion = jest.fn().mockResolvedValue(undefined);
    operator.cleanup = jest.fn().mockResolvedValue(undefined);
    operator.trace = jest.fn();
    return operator;
  };

  test("a shared logical plan completes without exiting its physical worker", async () => {
    const onPlanComplete = jest.fn().mockResolvedValue(undefined);
    const operator = prepareCompletedOperator({ processLifecycle: "shared", onPlanComplete });
    const requestExitSpy = jest.spyOn(operator, "requestProcessExit");

    await operator.finalizeDurableCompletion();

    expect(operator.cleanup).toHaveBeenCalledTimes(1);
    expect(onPlanComplete).toHaveBeenCalledTimes(1);
    expect(requestExitSpy).not.toHaveBeenCalled();
  });

  test("the dedicated lifecycle retains its process exit behavior", async () => {
    const operator = prepareCompletedOperator({ processLifecycle: "dedicated" });
    const requestExitSpy = jest.spyOn(operator, "requestProcessExit");

    await operator.finalizeDurableCompletion();

    expect(operator.cleanup).toHaveBeenCalledTimes(1);
    expect(requestExitSpy).toHaveBeenCalledWith(0);
  });
});
