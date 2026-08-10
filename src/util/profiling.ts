import fs from "fs";
import path from "path";

type ProfileBucket = {
  counters: Map<string, number>;
  timingsMs: Map<string, number>;
};

type ProfileSummary = {
  processId: number;
  parentProcessId: number;
  processRole: string;
  processRoleGroup: string;
  approach: string | null;
  pattern: string | null;
  iteration: string | null;
  benchmarkTimestamp: string;
  startedAt: string | null;
  finishedAt: string;
  artifactPath: string | null;
  counters: Record<string, number>;
  timingsMs: Record<string, number>;
  mqtt_clients_created: number;
  mqtt_messages_received: number;
  mqtt_messages_published: number;
  compatible_queries_detected: number;
  original_agent_outputs_derived_from_chunks: number;
  original_agent_rsps_skipped: number;
  fallback_original_agent_rsps_started: number;
  shared_chunk_producers_created: number;
  chunk_state_messages_published: number;
  chunk_consumers_registered: number;
  exact_final_result_reuse_hits: number;
  final_result_topics_created: number;
  final_result_topics_reused: number;
  final_result_subscribers_registered: number;
  chunk_reuse_paths_created: number;
  chunk_reuse_paths_skipped_due_to_exact_hit: number;
  reconstruction_paths_created: number;
  reconstruction_paths_skipped: number;
  fresh_executions_started: number;
  canonical_query_hashes_seen: number;
  rsp_engines_created: number;
  emitted_results: number;
  query_rewrites: number;
  query_rewrite_cache_hits: number;
  query_rewrite_cache_misses: number;
  parsed_query_cache_hits: number;
  parsed_query_cache_misses: number;
  rsp_query_processes_started: number;
  chunk_groups_completed: number;
  comparable_windows_emitted: number;
  reconstructed_superquery_results: number;
  duplicateChunkCount: number;
  missingChunkGroups: number;
  rdf_parse_time_ms: number;
  r2r_execution_time_ms: number;
  structured_recomposition_time_ms: number;
  diagnostics_write_time_ms: number;
  cleanup_time_ms: number;
  derived: {
    buffer_scans_per_emitted_result: number;
  };
};

type StageStats = {
  count: number;
  total_ms: number;
  mean_ms: number;
  p95_ms: number;
  max_ms: number;
};

type StageProfileSummary = {
  processId: number;
  parentProcessId: number;
  processRole: string;
  processRoleGroup: string;
  approach: string | null;
  pattern: string | null;
  iteration: string | null;
  benchmarkTimestamp: string;
  startedAt: string | null;
  finishedAt: string;
  artifactPath: string | null;
  stages: Record<string, StageStats>;
};

const enabled = ["1", "true", "yes", "on"].includes(
  (process.env.HIVE_PROFILE || "").trim().toLowerCase(),
);

const bucket: ProfileBucket = {
  counters: new Map<string, number>(),
  timingsMs: new Map<string, number>(),
};

const stageProfileEnabled = ["1", "true", "yes", "on"].includes(
  (process.env.STREAMING_QUERY_HIVE_PROFILE_APPROXIMATION || "")
    .trim()
    .toLowerCase(),
);

const stageDurationsMs: Map<string, number[]> = new Map();

let exitHookRegistered = false;
let flushed = false;
let signalHooksRegistered = false;
let stageFlushed = false;
let artifactWriteSequence = 0;

function sanitize(value: string): string {
  return value.replace(/[^a-zA-Z0-9_.-]+/g, "_");
}

function getProcessRole(): string {
  const raw = (process.env.HIVE_PROCESS_ROLE || "unknown").trim();
  return raw || "unknown";
}

function getProcessRoleGroup(role: string): string {
  if (role.includes("orchestrator")) {
    return "orchestrator";
  }
  if (role.includes("worker")) {
    return "worker";
  }
  if (role.includes("publisher")) {
    return "publisher";
  }
  return sanitize(role);
}

function add(map: Map<string, number>, key: string, delta: number) {
  map.set(key, (map.get(key) ?? 0) + delta);
}

function toSortedObject(map: Map<string, number>): Record<string, number> {
  return Object.fromEntries(Array.from(map.entries()).sort(([a], [b]) => a.localeCompare(b)));
}

function buildSummary(
  counters: Record<string, number>,
  timingsMs: Record<string, number>,
  artifactPath: string | null,
  finishedAt: string,
): ProfileSummary {
  const emittedResults = counters.emitted_results ?? 0;
  const bufferScans = counters.buffer_scans ?? 0;
  const flattenCounter = (name: string) => counters[name] ?? 0;
  const flattenTiming = (name: string) => timingsMs[name] ?? 0;
  const processRole = getProcessRole();
  return {
    processId: process.pid,
    parentProcessId: process.ppid,
    processRole,
    processRoleGroup: getProcessRoleGroup(processRole),
    approach: process.env.BENCHMARK_APPROACH || null,
    pattern: process.env.BENCHMARK_SCALE || null,
    iteration: process.env.BENCHMARK_ITERATION || null,
    benchmarkTimestamp: new Date().toISOString(),
    startedAt: process.env.HIVE_PROFILE_STARTED_AT || null,
    finishedAt,
    artifactPath,
    counters,
    timingsMs,
    mqtt_clients_created: flattenCounter("mqtt_clients_created"),
    mqtt_messages_received: flattenCounter("mqtt_messages_received"),
    mqtt_messages_published: flattenCounter("mqtt_messages_published"),
    compatible_queries_detected: flattenCounter("compatible_queries_detected"),
    original_agent_outputs_derived_from_chunks: flattenCounter(
      "original_agent_outputs_derived_from_chunks",
    ),
    original_agent_rsps_skipped: flattenCounter("original_agent_rsps_skipped"),
    fallback_original_agent_rsps_started: flattenCounter(
      "fallback_original_agent_rsps_started",
    ),
    shared_chunk_producers_created: flattenCounter(
      "shared_chunk_producers_created",
    ),
    chunk_state_messages_published: flattenCounter(
      "chunk_state_messages_published",
    ),
    chunk_consumers_registered: flattenCounter("chunk_consumers_registered"),
    exact_final_result_reuse_hits: flattenCounter(
      "exact_final_result_reuse_hits",
    ),
    final_result_topics_created: flattenCounter(
      "final_result_topics_created",
    ),
    final_result_topics_reused: flattenCounter("final_result_topics_reused"),
    final_result_subscribers_registered: flattenCounter(
      "final_result_subscribers_registered",
    ),
    chunk_reuse_paths_created: flattenCounter("chunk_reuse_paths_created"),
    chunk_reuse_paths_skipped_due_to_exact_hit: flattenCounter(
      "chunk_reuse_paths_skipped_due_to_exact_hit",
    ),
    reconstruction_paths_created: flattenCounter(
      "reconstruction_paths_created",
    ),
    reconstruction_paths_skipped: flattenCounter(
      "reconstruction_paths_skipped",
    ),
    fresh_executions_started: flattenCounter("fresh_executions_started"),
    canonical_query_hashes_seen: flattenCounter("canonical_query_hashes_seen"),
    rsp_engines_created: flattenCounter("rsp_engines_created"),
    emitted_results: emittedResults,
    query_rewrites: flattenCounter("query_rewrites"),
    query_rewrite_cache_hits: flattenCounter("query_rewrite_cache_hits"),
    query_rewrite_cache_misses: flattenCounter("query_rewrite_cache_misses"),
    parsed_query_cache_hits: flattenCounter("parsed_query_cache_hits"),
    parsed_query_cache_misses: flattenCounter("parsed_query_cache_misses"),
    rsp_query_processes_started: flattenCounter("rsp_query_processes_started"),
    chunk_groups_completed: flattenCounter("chunk_groups_completed"),
    comparable_windows_emitted: flattenCounter("comparable_windows_emitted"),
    reconstructed_superquery_results: flattenCounter(
      "reconstructed_superquery_results",
    ),
    duplicateChunkCount: flattenCounter("duplicateChunkCount"),
    missingChunkGroups: flattenCounter("missingChunkGroups"),
    rdf_parse_time_ms: flattenTiming("rdf_parse_time_ms"),
    r2r_execution_time_ms: flattenTiming("r2r_execution_time_ms"),
    structured_recomposition_time_ms: flattenTiming(
      "structured_recomposition_time_ms",
    ),
    diagnostics_write_time_ms: flattenTiming("diagnostics_write_time_ms"),
    cleanup_time_ms: flattenTiming("cleanup_time_ms"),
    derived: {
      buffer_scans_per_emitted_result:
        emittedResults > 0 ? bufferScans / emittedResults : 0,
    },
  };
}

function flushProfileSummary() {
  if (!enabled || flushed) {
    return;
  }

  flushed = true;
  const finishedAt = new Date().toISOString();
  const counters = toSortedObject(bucket.counters);
  const timingsMs = toSortedObject(bucket.timingsMs);
  const artifactPath = resolveArtifactPath();
  const summary = buildSummary(counters, timingsMs, artifactPath, finishedAt);

  if (artifactPath) {
    writeJsonArtifactAtomically(artifactPath, summary);
  }

  console.error(`HIVE_PROFILE ${JSON.stringify(summary)}`);
}

export function resolveStageArtifactPath(processId = process.pid): string | null {
  const explicitFile = (process.env.HIVE_STAGE_PROFILE_OUTPUT_FILE || "").trim();
  if (explicitFile) {
    return path.resolve(explicitFile);
  }

  const outputDir = (process.env.LOG_PATH || "").trim();
  if (!outputDir) {
    return null;
  }

  const processRole = sanitize(getProcessRole());
  const approach = sanitize(process.env.BENCHMARK_APPROACH || "unknown");
  if (approach === "approximation") {
    if (processRole.includes("approximation_orchestrator")) {
      return path.resolve(outputDir, "approximation_root_cpu_attribution_summary.json");
    }
    if (processRole.includes("approximation_bee_worker")) {
      return path.resolve(
        outputDir,
        `approximation_cpu_attribution_summary.${processId}.json`,
      );
    }
    return path.resolve(
      outputDir,
      `approximation_cpu_attribution_summary.${processRole}.${process.pid}.json`,
    );
  }
  if (approach === "chunked") {
    if (processRole.includes("chunked_orchestrator")) {
      return path.resolve(outputDir, "chunked_root_cpu_attribution_summary.json");
    }
    if (processRole.includes("chunked_bee_worker")) {
      return path.resolve(
        outputDir,
        `chunked_cpu_attribution_summary.${processId}.json`,
      );
    }
    return path.resolve(
      outputDir,
      `chunked_cpu_attribution_summary.${processRole}.${process.pid}.json`,
    );
  }
  return path.resolve(outputDir, `${approach}_cpu_attribution_summary.${processRole}.${process.pid}.json`);
}

function buildStageSummary(
  artifactPath: string | null,
  finishedAt: string,
): StageProfileSummary {
  const processRole = getProcessRole();
  const stages = Object.fromEntries(
    Array.from(stageDurationsMs.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, values]) => {
        const sorted = [...values].sort((left, right) => left - right);
        const count = sorted.length;
        const total_ms = sorted.reduce((sum, value) => sum + value, 0);
        const mean_ms = count > 0 ? total_ms / count : 0;
        const p95Index = count > 0 ? Math.min(count - 1, Math.max(0, Math.ceil(count * 0.95) - 1)) : 0;
        const p95_ms = count > 0 ? sorted[p95Index] : 0;
        const max_ms = count > 0 ? sorted[count - 1] : 0;
        return [
          name,
          {
            count,
            total_ms,
            mean_ms,
            p95_ms,
            max_ms,
          } satisfies StageStats,
        ];
      }),
  );

  return {
    processId: process.pid,
    parentProcessId: process.ppid,
    processRole,
    processRoleGroup: getProcessRoleGroup(processRole),
    approach: process.env.BENCHMARK_APPROACH || null,
    pattern: process.env.BENCHMARK_SCALE || null,
    iteration: process.env.BENCHMARK_ITERATION || null,
    benchmarkTimestamp: new Date().toISOString(),
    startedAt: process.env.HIVE_PROFILE_STARTED_AT || null,
    finishedAt,
    artifactPath,
    stages,
  };
}

function flushStageProfileSummary() {
  if (!stageProfileEnabled || stageFlushed) {
    return;
  }

  stageFlushed = true;
  const finishedAt = new Date().toISOString();
  const artifactPath = resolveStageArtifactPath();
  const summary = buildStageSummary(artifactPath, finishedAt);

  if (artifactPath) {
    writeJsonArtifactAtomically(artifactPath, summary);
  }
}

export function resolveArtifactPath(processId = process.pid): string | null {
  const explicitFile = (process.env.HIVE_PROFILE_OUTPUT_FILE || "").trim();
  if (explicitFile) {
    return path.resolve(explicitFile);
  }

  const outputDir = (process.env.HIVE_PROFILE_OUTPUT_DIR || process.env.LOG_PATH || "").trim();
  if (!outputDir) {
    return null;
  }
  const processRoleGroup = getProcessRoleGroup(getProcessRole());
  const consumerIdx = process.env.K_SCALING_CONSUMER_INDEX ? `_consumer_${process.env.K_SCALING_CONSUMER_INDEX}` : "";
  return path.resolve(
    outputDir,
    `hive_profile_summary.${sanitize(processRoleGroup)}${consumerIdx}.${processId}.json`,
  );
}

function writeJsonArtifactAtomically(artifactPath: string, summary: unknown): void {
  const directory = path.dirname(artifactPath);
  const temporaryPath = path.join(
    directory,
    `.${path.basename(artifactPath)}.${process.pid}.${artifactWriteSequence++}.tmp`,
  );
  fs.mkdirSync(directory, { recursive: true });
  try {
    fs.writeFileSync(temporaryPath, `${JSON.stringify(summary, null, 2)}\n`);
    fs.renameSync(temporaryPath, artifactPath);
  } finally {
    if (fs.existsSync(temporaryPath)) {
      fs.unlinkSync(temporaryPath);
    }
  }
}

function registerExitHook() {
  if (!enabled || exitHookRegistered) {
    return;
  }

  exitHookRegistered = true;
  process.once("exit", flushProfileSummary);
}

function registerSignalHooks() {
  if (!enabled || signalHooksRegistered) {
    return;
  }

  signalHooksRegistered = true;
  const handleSignal = (signal: NodeJS.Signals) => {
    flushProfileSummary();
    process.exit(signal === "SIGTERM" ? 143 : 130);
  };

  process.once("SIGINT", () => handleSignal("SIGINT"));
  process.once("SIGTERM", () => handleSignal("SIGTERM"));
}

registerExitHook();
registerSignalHooks();

export function isProfileEnabled(): boolean {
  return enabled;
}

export function profileCount(name: string, delta = 1): void {
  if (!enabled) {
    return;
  }
  add(bucket.counters, name, delta);
}

export function profileTime(name: string, durationMs: number): void {
  if (!enabled) {
    return;
  }
  add(bucket.timingsMs, name, durationMs);
}

export function profileSync<T>(name: string, fn: () => T): T {
  if (!enabled) {
    return fn();
  }

  const start = process.hrtime.bigint();
  try {
    return fn();
  } finally {
    profileTime(name, Number(process.hrtime.bigint() - start) / 1e6);
  }
}

export async function profileAsync<T>(name: string, fn: () => Promise<T>): Promise<T> {
  if (!enabled) {
    return fn();
  }

  const start = process.hrtime.bigint();
  try {
    return await fn();
  } finally {
    profileTime(name, Number(process.hrtime.bigint() - start) / 1e6);
  }
}

export function flushProfileIfEnabled(): void {
  writeProfileArtifact();
  writeStageProfileArtifact();
}

export function writeProfileArtifact(): void {
  if (!enabled) {
    return;
  }

  const artifactPath = resolveArtifactPath();
  if (!artifactPath) {
    return;
  }

  const counters = toSortedObject(bucket.counters);
  const timingsMs = toSortedObject(bucket.timingsMs);
  const summary = buildSummary(
    counters,
    timingsMs,
    artifactPath,
    new Date().toISOString(),
  );

  writeJsonArtifactAtomically(artifactPath, summary);
}

export function isStageProfileEnabled(): boolean {
  return stageProfileEnabled;
}

export function profileStageTime(name: string, durationMs: number): void {
  if (!stageProfileEnabled) {
    return;
  }
  const values = stageDurationsMs.get(name) || [];
  values.push(durationMs);
  stageDurationsMs.set(name, values);
}

export function profileStageSync<T>(name: string, fn: () => T): T {
  if (!stageProfileEnabled) {
    return fn();
  }

  const start = process.hrtime.bigint();
  try {
    return fn();
  } finally {
    profileStageTime(name, Number(process.hrtime.bigint() - start) / 1e6);
  }
}

export async function profileStageAsync<T>(name: string, fn: () => Promise<T>): Promise<T> {
  if (!stageProfileEnabled) {
    return fn();
  }

  const start = process.hrtime.bigint();
  try {
    return await fn();
  } finally {
    profileStageTime(name, Number(process.hrtime.bigint() - start) / 1e6);
  }
}

export function startStageTimer(): bigint {
  return process.hrtime.bigint();
}

export function endStageTimer(name: string, start: bigint): void {
  if (!stageProfileEnabled) {
    return;
  }
  profileStageTime(name, Number(process.hrtime.bigint() - start) / 1e6);
}

export function writeStageProfileArtifact(): void {
  flushStageProfileSummary();
}
