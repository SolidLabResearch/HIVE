import { ApproximationApproachOperator } from "../operators/RateBasedApproximationApproachOperator";
import { profileCount } from "../../util/profiling";
import { SharedApproximationProducerSubscriptionRegistry } from "./SharedApproximationProducerSubscriptionRegistry";

type RegisterMessage = { type: "registerPlan"; planId: string; query: string; subQueries: string[]; outputTopic: string; executionId: string };
const plans = new Map<string, ApproximationApproachOperator>();
const subscriptions = new SharedApproximationProducerSubscriptionRegistry();
profileCount("approximation_reconstruction_runtime_processes_created");

async function registerPlan(message: RegisterMessage): Promise<void> {
  if (plans.has(message.planId)) throw new Error(`Duplicate Approximation plan ${message.planId}`);
  const operator = new ApproximationApproachOperator(undefined, {
    mqttClient: subscriptions.createPlanClient(message.planId),
    resultTopic: message.outputTopic,
    executionId: message.executionId,
    processLifecycle: "shared",
    onPlanComplete: () => completePlan(message.planId),
  });
  plans.set(message.planId, operator);
  profileCount("approximation_reconstruction_plans_created"); profileCount("approximation_reconstruction_plans_active");
  operator.addOutputQuery(message.query);
  for (const subQuery of message.subQueries) operator.addSubQuery(subQuery);
  await operator.init();
  await operator.handleAggregation();
  process.send?.({ type: "planReady", planId: message.planId, activePlanIds: [...plans.keys()] });
}

async function completePlan(planId: string): Promise<void> {
  if (!plans.delete(planId)) return;
  profileCount("approximation_reconstruction_plans_active", -1);
  process.send?.({ type: "planComplete", planId, activePlanIds: [...plans.keys()] });
}

process.on("message", (message: RegisterMessage | { type: "releasePlan"; planId: string } | { type: "shutdown" }) => {
  void (async () => {
    if (message.type === "registerPlan") return registerPlan(message);
    if (message.type === "releasePlan") { const plan = plans.get(message.planId); if (plan) { await plan.cleanup(); plans.delete(message.planId); profileCount("approximation_reconstruction_plans_active", -1); } process.send?.({ type: "planReleased", planId: message.planId, activePlanIds: [...plans.keys()] }); return; }
    for (const plan of plans.values()) await plan.cleanup(); plans.clear(); subscriptions.close(); process.exit(0);
  })().catch((error) => { const id = "planId" in message ? message.planId : undefined; process.send?.({ type: "planFailed", planId: id, error: error instanceof Error ? error.message : String(error) }); });
});
