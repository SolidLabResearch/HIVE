import { StreamingQueryChunkAggregatorOperator } from "../operators/StreamingQueryChunkAggregatorOperator";
import type { ChunkedReconstructionPlanConfig } from "../operators/chunked/ChunkedReconstructionPlan";
import { profileCount } from "../../util/profiling";
import { SharedChunkedProducerSubscriptionRegistry } from "./SharedChunkedProducerSubscriptionRegistry";

type RegisterMessage = {
  type: "registerPlan";
  planId: string;
  query: string;
  subQueries: string[];
  config: ChunkedReconstructionPlanConfig;
};

const plans = new Map<string, StreamingQueryChunkAggregatorOperator>();
const producerSubscriptions = new SharedChunkedProducerSubscriptionRegistry();
profileCount("chunked_reconstruction_runtime_processes_created");

async function registerPlan(message: RegisterMessage): Promise<void> {
  if (plans.has(message.planId)) throw new Error(`Duplicate Chunked plan ${message.planId}`);
  const operator = new StreamingQueryChunkAggregatorOperator(message.config, {
    onReady: (details) => process.send?.({ type: "planReady", planId: message.planId, ...details }),
    onComplete: () => {
      plans.delete(message.planId);
      profileCount("chunked_reconstruction_plans_active", -1);
      profileCount("chunked_reconstruction_plans_completed");
      process.send?.({ type: "planComplete", planId: message.planId });
    },
    onFailure: (error) => {
      plans.delete(message.planId);
      profileCount("chunked_reconstruction_plans_active", -1);
      profileCount("chunked_reconstruction_plans_failed");
      process.send?.({ type: "planFailed", planId: message.planId, error: error.message });
    },
    producerSubscriptionRegistry: producerSubscriptions,
  });
  plans.set(message.planId, operator);
  profileCount("chunked_reconstruction_plans_created");
  profileCount("chunked_reconstruction_plans_active");
  operator.addOutputQuery(message.query);
  for (const subQuery of message.subQueries) operator.addSubQuery(subQuery);
  await operator.init();
  await operator.handleAggregation();
}

process.on("message", (message: RegisterMessage | { type: "releasePlan"; planId: string } | { type: "shutdown" }) => {
  void (async () => {
    if (message.type === "registerPlan") {
      await registerPlan(message);
      return;
    }
    if (message.type === "releasePlan") {
      const plan = plans.get(message.planId);
      if (plan) {
        plan.cleanup();
        plans.delete(message.planId);
        profileCount("chunked_reconstruction_plans_active", -1);
      }
      process.send?.({ type: "planReleased", planId: message.planId });
      return;
    }
    for (const plan of plans.values()) plan.cleanup();
    plans.clear();
    producerSubscriptions.close();
    process.exit(0);
  })().catch((error) => {
    const planId = "planId" in message ? message.planId : undefined;
    process.send?.({ type: "planFailed", planId, error: error instanceof Error ? error.message : String(error) });
  });
});
