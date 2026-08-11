import { ChildProcess, fork } from "child_process";
import path from "path";
import type { ChunkedReconstructionPlanConfig } from "../operators/chunked/ChunkedReconstructionPlan";

export type SharedChunkedPlanRegistration = {
  planId: string;
  query: string;
  subQueries: string[];
  config: ChunkedReconstructionPlanConfig;
};

export class SharedChunkedReconstructionRuntime {
  private readonly worker: ChildProcess;
  private readonly ready = new Map<string, { resolve: () => void; reject: (error: Error) => void }>();
  private readonly release = new Map<string, () => void>();
  private activePlanIds = new Set<string>();

  constructor(private readonly onRuntimeFailed: (reason: string, planIds: string[]) => void) {
    this.worker = fork(path.resolve(__dirname, "SharedChunkedReconstructionRuntimeWorker.js"), [], {
      env: { ...process.env, HIVE_PROCESS_ROLE: "shared_chunked_reconstruction_runtime" },
    });
    this.worker.on("message", (message: any) => this.handleMessage(message));
    this.worker.on("error", (error) => this.failRuntime(error.message));
    this.worker.on("exit", (code, signal) => this.failRuntime(`shared Chunked runtime exited code=${code} signal=${signal}`));
  }

  getPid(): number | undefined { return this.worker.pid; }
  get activePlanCount(): number { return this.activePlanIds.size; }

  registerPlan(registration: SharedChunkedPlanRegistration): Promise<void> {
    if (this.activePlanIds.has(registration.planId)) return Promise.reject(new Error(`Duplicate Chunked plan ${registration.planId}`));
    this.activePlanIds.add(registration.planId);
    return new Promise<void>((resolve, reject) => {
      this.ready.set(registration.planId, { resolve, reject });
      this.worker.send({ type: "registerPlan", ...registration }, (error) => {
        if (error) this.failPlan(registration.planId, error.message);
      });
    });
  }

  releasePlan(planId: string): Promise<void> {
    if (!this.activePlanIds.has(planId)) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.release.set(planId, resolve);
      this.worker.send({ type: "releasePlan", planId }, () => undefined);
    });
  }

  shutdown(): void { this.worker.send({ type: "shutdown" }, () => undefined); }

  private handleMessage(message: any): void {
    const planId = message?.planId as string | undefined;
    if (message?.type === "planReady" && planId) this.ready.get(planId)?.resolve();
    if (message?.type === "planFailed" && planId) this.failPlan(planId, message.error || "Chunked plan failed");
    if (message?.type === "planComplete" && planId) this.activePlanIds.delete(planId);
    if (message?.type === "planReleased" && planId) {
      this.activePlanIds.delete(planId);
      this.release.get(planId)?.();
      this.release.delete(planId);
    }
  }

  private failPlan(planId: string, reason: string): void {
    this.activePlanIds.delete(planId);
    const pending = this.ready.get(planId);
    this.ready.delete(planId);
    pending?.reject(new Error(reason));
  }

  private failRuntime(reason: string): void {
    const planIds = [...this.activePlanIds];
    if (planIds.length === 0) return;
    for (const planId of planIds) this.failPlan(planId, reason);
    this.onRuntimeFailed(reason, planIds);
  }
}
