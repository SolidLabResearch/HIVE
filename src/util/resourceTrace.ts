import fs from "fs";
import path from "path";

const enabled = ["1", "true", "yes", "on"].includes(
  (process.env.HIVE_RESOURCE_TRACE || "").trim().toLowerCase(),
);

const role = (process.env.HIVE_PROCESS_ROLE || "unknown").trim() || "unknown";
const logDir = (process.env.LOG_PATH || "").trim();
const traceFilePath =
  enabled && logDir
    ? path.join(logDir, `resource_trace_${sanitize(role)}_${process.pid}.ndjson`)
    : null;
const metaFilePath =
  enabled && logDir
    ? path.join(logDir, `resource_trace_process_${process.pid}.json`)
    : null;

let exitHookRegistered = false;

function sanitize(value: string): string {
  return value.replace(/[^a-zA-Z0-9_.-]+/g, "_");
}

function writeRecord(record: Record<string, unknown>): void {
  if (!enabled || !traceFilePath) {
    return;
  }

  fs.mkdirSync(path.dirname(traceFilePath), { recursive: true });
  fs.appendFileSync(traceFilePath, `${JSON.stringify(record)}\n`);
}

function writeMeta(): void {
  if (!enabled || !metaFilePath) {
    return;
  }

  fs.mkdirSync(path.dirname(metaFilePath), { recursive: true });
  fs.writeFileSync(
    metaFilePath,
    `${JSON.stringify(
      {
        pid: process.pid,
        ppid: process.ppid,
        role,
        command: process.argv.join(" "),
        startedAt: new Date().toISOString(),
      },
      null,
      2,
    )}\n`,
  );
}

function registerExitHook(): void {
  if (!enabled || exitHookRegistered) {
    return;
  }

  exitHookRegistered = true;
  process.once("exit", () => {
    resourceTraceSnapshot("process_exit");
  });
}

export function isResourceTraceEnabled(): boolean {
  return enabled;
}

export function resourceTraceSnapshot(
  stage: string,
  notes?: string,
  extra: Record<string, unknown> = {},
): void {
  if (!enabled) {
    return;
  }

  const mem = process.memoryUsage();
  writeRecord({
    timestamp: Date.now(),
    isoTime: new Date().toISOString(),
    pid: process.pid,
    ppid: process.ppid,
    role,
    stage,
    rssMb: Number((mem.rss / 1024 / 1024).toFixed(3)),
    heapTotalMb: Number((mem.heapTotal / 1024 / 1024).toFixed(3)),
    heapUsedMb: Number((mem.heapUsed / 1024 / 1024).toFixed(3)),
    externalMb: Number((mem.external / 1024 / 1024).toFixed(3)),
    arrayBuffersMb: Number((mem.arrayBuffers / 1024 / 1024).toFixed(3)),
    notes: notes || "",
    ...extra,
  });
}

writeMeta();
registerExitHook();
