import { ChildProcess, fork } from "child_process";
import path from "path";

export type SharedApproximationPlanRegistration = { planId: string; query: string; subQueries: string[]; outputTopic: string; executionId: string };

export class SharedApproximationRuntime {
  private readonly worker: ChildProcess;
  private readonly ready = new Map<string, { resolve: () => void; reject: (error: Error) => void }>();
  private readonly release = new Map<string, () => void>();
  private activePlanIds = new Set<string>();
  constructor(private readonly onRuntimeFailed: (reason: string, planIds: string[]) => void) {
    this.worker = fork(path.resolve(__dirname, "SharedApproximationRuntimeWorker.js"), [], { env: { ...process.env, HIVE_PROCESS_ROLE: "shared_approximation_runtime", BENCHMARK_APPROACH: "approximation" } });
    this.worker.on("message", (message: any) => this.handleMessage(message));
    this.worker.on("error", (error) => this.failRuntime(error.message));
    this.worker.on("exit", (code, signal) => this.failRuntime(`shared Approximation runtime exited code=${code} signal=${signal}`));
  }
  getPid(): number | undefined { return this.worker.pid; }
  get activePlanCount(): number { return this.activePlanIds.size; }
  registerPlan(registration: SharedApproximationPlanRegistration): Promise<void> {
    if (this.activePlanIds.has(registration.planId)) return Promise.reject(new Error(`Duplicate Approximation plan ${registration.planId}`));
    this.activePlanIds.add(registration.planId);
    return new Promise<void>((resolve, reject) => { this.ready.set(registration.planId, { resolve, reject }); this.worker.send({ type: "registerPlan", ...registration }, (error) => { if (error) this.failPlan(registration.planId, error.message); }); });
  }
  releasePlan(planId: string): Promise<void> {
    if (!this.activePlanIds.has(planId)) return Promise.resolve();
    return new Promise<void>((resolve) => { this.release.set(planId, resolve); this.worker.send({ type: "releasePlan", planId }, () => undefined); });
  }
  shutdown(): void { this.worker.send({ type: "shutdown" }, () => undefined); }
  private handleMessage(message: any): void { const id = message?.planId as string | undefined; if (message?.type === "planReady" && id) this.ready.get(id)?.resolve(); if (message?.type === "planFailed" && id) this.failPlan(id, message.error || "Approximation plan failed"); if (message?.type === "planComplete" && id) this.activePlanIds.delete(id); if (message?.type === "planReleased" && id) { this.activePlanIds.delete(id); this.release.get(id)?.(); this.release.delete(id); } }
  private failPlan(id: string, reason: string): void { this.activePlanIds.delete(id); const pending = this.ready.get(id); this.ready.delete(id); pending?.reject(new Error(reason)); }
  private failRuntime(reason: string): void { const ids = [...this.activePlanIds]; if (!ids.length) return; for (const id of ids) this.failPlan(id, reason); this.onRuntimeFailed(reason, ids); }
}
