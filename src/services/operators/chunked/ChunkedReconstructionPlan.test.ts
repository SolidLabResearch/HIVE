import {
  ChunkedPlanState,
  createChunkedPlanConfigFromEnvironment,
} from "./ChunkedReconstructionPlan";

describe("Chunked reconstruction plan isolation", () => {
  test("keeps immutable plan configuration and mutable state isolated", () => {
    const planA = createChunkedPlanConfigFromEnvironment({ planId: "q1", executionId: "q1", outputTopic: "results/q1" });
    const planB = createChunkedPlanConfigFromEnvironment({ planId: "q2", executionId: "q2", outputTopic: "results/q2" });
    const stateA = new ChunkedPlanState();
    const stateB = new ChunkedPlanState();

    stateA.latestWatermarkByProducer.set("producer-1", 120);
    stateA.acceptedContributions.add("producer-1:0:60");
    stateA.completed = true;

    expect(planA.outputTopic).toBe("results/q1");
    expect(planB.outputTopic).toBe("results/q2");
    expect(stateB.latestWatermarkByProducer.has("producer-1")).toBe(false);
    expect(stateB.acceptedContributions.size).toBe(0);
    expect(stateB.completed).toBe(false);
  });

  test("does not retain caller-owned producer mapping arrays", () => {
    const mappings: any[] = [];
    const plan = createChunkedPlanConfigFromEnvironment({ planId: "q1", executionId: "q1", managerOwnedProducerMappings: mappings });
    mappings.push({ runtimeProducerId: "unexpected" });
    // The plan is immutable at the boundary; callers must not mutate its input.
    expect(plan.managerOwnedProducerMappings).toHaveLength(0);
  });
});
