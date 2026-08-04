import crypto from "crypto";
import fs from "fs";
import path from "path";
import { getCanonicalRSPQLQueryHash } from "../reuse/normalizeRSPQLForExactReuse";

export type ActiveFinalQuerySource = "chunked-reconstruction";

export type HierarchicalDeliveryEvent = {
  scenario_id: string;
  canonical_query_id: string;
  consumer_id: string;
  subscription_id: string;
  window_start: number | null;
  window_end: number | null;
  result_value: number;
  source_result_timestamp: number;
  delivery_timestamp: number;
  shared_result_created_at: number;
  cache_entry_created_at: number;
  fanout_delivery_latency_ms: number;
  last_required_observation_received_at: number | null;
  consumer_query_registered_at: number;
  reconstruction_worker_created_at: number;
  reconstruction_ready_at: number | null;
  source_result_topic: string;
  result_id: string;
};

export type HierarchicalSummary = {
  experiment: "H1: Hierarchical Chunked and Exact-Final Reuse";
  approach: "chunked-exact-final";
  scenario_id: string;
  canonical_query_id: string;
  source: ActiveFinalQuerySource;
  chunkProducerCount: number;
  uniqueFinalQueryCount: number;
  reconstructionWorkerCount: number;
  reconstructionExecutionCount: number;
  directFinalQueryExecutionCount: number;
  cacheEntries: number;
  subscribers: number;
  reuseHits: number;
  deliveries: number;
  exactResults: number;
  expectedConsumers: number;
  allConsumersDelivered: boolean;
  finalResultSource: ActiveFinalQuerySource;
  targetWindowCount: number;
  emittedFinalWindowCount: number;
  stoppedAfterTargetWindows: boolean;
  stopReason: "target_window_count_reached" | "other";
  producedByDirectFinalQuery: boolean;
  aggregateWrittenAt: string;
};

export type RegistryEntry = {
  canonicalQueryId: string;
  source: ActiveFinalQuerySource;
  reconstructionWorkerId: string;
  finalResultTopic: string;
  cacheEntryCount: number;
  subscribers: string[];
  createdAt: number;
};

export function canonicalizeHierarchicalFinalQuery(query: string): {
  canonicalQueryId: string;
  normalizedQuery: string;
} {
  const { canonicalQueryHash, normalizedQuery } = getCanonicalRSPQLQueryHash(query);
  return {
    canonicalQueryId: canonicalQueryHash,
    normalizedQuery,
  };
}

export function buildHierarchicalResultId(params: {
  canonicalQueryId: string;
  value: number;
  timestamp: number;
  windowStart: number | null;
  windowEnd: number | null;
}): string {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(params))
    .digest("hex");
}

let atomicCounter = 0;

export async function writeAtomicJson(filePath: string, value: unknown): Promise<void> {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}-${++atomicCounter}`;
  await fs.promises.writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await fs.promises.rename(tempPath, filePath);
}

export function validateHierarchicalSummaryShape(
  summary: Partial<HierarchicalSummary> | null,
  expectedK: number,
): string[] {
  const failures: string[] = [];
  if (!summary) {
    return ["hierarchical summary missing"];
  }
  const expectedReuseHits = Math.max(0, expectedK - 1);
  const checks: Array<[keyof HierarchicalSummary, unknown]> = [
    ["source", "chunked-reconstruction"],
    ["finalResultSource", "chunked-reconstruction"],
    ["chunkProducerCount", 2],
    ["uniqueFinalQueryCount", 1],
    ["reconstructionWorkerCount", 1],
    ["reconstructionExecutionCount", 1],
    ["directFinalQueryExecutionCount", 0],
    ["cacheEntries", 1],
    ["subscribers", expectedK],
    ["reuseHits", expectedReuseHits],
    ["deliveries", expectedK],
    ["exactResults", expectedK],
    ["expectedConsumers", expectedK],
    ["emittedFinalWindowCount", 1],
    ["stoppedAfterTargetWindows", true],
    ["stopReason", "target_window_count_reached"],
    ["producedByDirectFinalQuery", false],
  ];
  for (const [field, expected] of checks) {
    if ((summary as Record<string, unknown>)[field] !== expected) {
      failures.push(`${String(field)}=${(summary as Record<string, unknown>)[field]}, expected ${expected}`);
    }
  }
  if (summary.allConsumersDelivered !== true) {
    failures.push("allConsumersDelivered=false, expected true");
  }
  if (!summary.canonical_query_id) {
    failures.push("canonical_query_id missing");
  }
  if (!summary.scenario_id) {
    failures.push("scenario_id missing");
  }
  return failures;
}
