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

const enabled = ["1", "true", "yes", "on"].includes(
  (process.env.HIVE_PROFILE || "").trim().toLowerCase(),
);

const bucket: ProfileBucket = {
  counters: new Map<string, number>(),
  timingsMs: new Map<string, number>(),
};

let exitHookRegistered = false;
let flushed = false;
let signalHooksRegistered = false;

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
    fs.mkdirSync(path.dirname(artifactPath), { recursive: true });
    fs.writeFileSync(artifactPath, `${JSON.stringify(summary, null, 2)}\n`);
  }

  console.error(`HIVE_PROFILE ${JSON.stringify(summary)}`);
}

function resolveArtifactPath(): string | null {
  const explicitFile = (process.env.HIVE_PROFILE_OUTPUT_FILE || "").trim();
  if (explicitFile) {
    return path.resolve(explicitFile);
  }

  const outputDir = (process.env.HIVE_PROFILE_OUTPUT_DIR || process.env.LOG_PATH || "").trim();
  if (!outputDir) {
    return null;
  }
  const processRoleGroup = getProcessRoleGroup(getProcessRole());
  return path.resolve(
    outputDir,
    `hive_profile_summary.${sanitize(processRoleGroup)}.json`,
  );
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

  fs.mkdirSync(path.dirname(artifactPath), { recursive: true });
  fs.writeFileSync(artifactPath, `${JSON.stringify(summary, null, 2)}\n`);
}
