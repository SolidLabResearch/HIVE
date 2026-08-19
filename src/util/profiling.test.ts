import path from "path";
import {
  resolveArtifactPath,
  resolveStageArtifactPath,
} from "./profiling";

const environmentKeys = [
  "HIVE_PROFILE_OUTPUT_FILE",
  "HIVE_PROFILE_OUTPUT_DIR",
  "HIVE_STAGE_PROFILE_OUTPUT_FILE",
  "LOG_PATH",
  "HIVE_PROCESS_ROLE",
  "BENCHMARK_APPROACH",
  "K_SCALING_CONSUMER_INDEX",
] as const;

const originalEnvironment = Object.fromEntries(
  environmentKeys.map((key) => [key, process.env[key]]),
);

function restoreEnvironment(): void {
  for (const key of environmentKeys) {
    const value = originalEnvironment[key];
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}

describe("profile artifact paths", () => {
  beforeEach(() => {
    for (const key of environmentKeys) {
      delete process.env[key];
    }
  });

  afterEach(restoreEnvironment);

  test("uses PID-unique general worker paths while preserving consumer index", () => {
    process.env.HIVE_PROFILE_OUTPUT_DIR = "/tmp/hive-profiles";
    process.env.HIVE_PROCESS_ROLE = "approximation_bee_worker";
    process.env.K_SCALING_CONSUMER_INDEX = "3";

    const firstWorker = resolveArtifactPath(12001);
    const secondWorker = resolveArtifactPath(12002);

    expect(firstWorker).toBe(
      path.resolve("/tmp/hive-profiles/hive_profile_summary.worker_consumer_3.12001.json"),
    );
    expect(secondWorker).toBe(
      path.resolve("/tmp/hive-profiles/hive_profile_summary.worker_consumer_3.12002.json"),
    );
    expect(secondWorker).not.toBe(firstWorker);
  });

  test("uses PID-unique stage paths for approximation and chunked Bee workers", () => {
    process.env.LOG_PATH = "/tmp/hive-profiles";
    process.env.HIVE_PROCESS_ROLE = "approximation_bee_worker";
    process.env.BENCHMARK_APPROACH = "approximation";
    expect(resolveStageArtifactPath(12001)).toBe(
      path.resolve("/tmp/hive-profiles/approximation_cpu_attribution_summary.12001.json"),
    );
    expect(resolveStageArtifactPath(12002)).not.toBe(resolveStageArtifactPath(12001));

    process.env.HIVE_PROCESS_ROLE = "chunked_bee_worker";
    process.env.BENCHMARK_APPROACH = "chunked";
    expect(resolveStageArtifactPath(12003)).toBe(
      path.resolve("/tmp/hive-profiles/chunked_cpu_attribution_summary.12003.json"),
    );
    expect(resolveStageArtifactPath(12004)).not.toBe(resolveStageArtifactPath(12003));
  });

  test("keeps explicit profile output paths unchanged", () => {
    process.env.HIVE_PROFILE_OUTPUT_FILE = "profiles/explicit-profile.json";
    process.env.HIVE_STAGE_PROFILE_OUTPUT_FILE = "profiles/explicit-stage.json";

    expect(resolveArtifactPath(12001)).toBe(path.resolve("profiles/explicit-profile.json"));
    expect(resolveStageArtifactPath(12001)).toBe(path.resolve("profiles/explicit-stage.json"));
  });
});
