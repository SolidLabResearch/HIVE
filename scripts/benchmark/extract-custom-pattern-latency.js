#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const CANONICAL_PATTERNS = [
  "low_variability",
  "step_pattern",
  "spike_pattern",
  "low_freq_oscillation",
  "high_freq_oscillation",
  "spike_boundary_short",
  "spike_boundary_medium",
  "spike_asymmetric_long",
  "late_burst",
  "multiple_bursts",
  "step_misaligned_45",
  "step_misaligned_75",
  "linear_ramp",
  "asymmetric_activity",
];

const APPROACHES = ["fetching", "chunked"];

const LATENCY_FILE_BY_APPROACH = {
  fetching: "fetching_latency_log.csv",
  chunked: "chunked_latency_log.csv",
};

const PATTERN_ALIASES = new Map([
  ["low_variability", "low_variability"],
  ["low-variability", "low_variability"],
  ["low_frequency_oscillation", "low_freq_oscillation"],
  ["low-frequency-oscillation", "low_freq_oscillation"],
  ["low_freq_oscillation", "low_freq_oscillation"],
  ["step", "step_pattern"],
  ["step_pattern", "step_pattern"],
  ["spike", "spike_pattern"],
  ["spike_pattern", "spike_pattern"],
  ["spike_boundary_short", "spike_boundary_short"],
  ["spike_boundary_medium", "spike_boundary_medium"],
  ["spike_asymmetric_long", "spike_asymmetric_long"],
  ["late_burst", "late_burst"],
  ["multiple_bursts", "multiple_bursts"],
  ["step_misaligned_45", "step_misaligned_45"],
  ["step_misaligned_75", "step_misaligned_75"],
  ["linear_ramp", "linear_ramp"],
  ["asymmetric_activity", "asymmetric_activity"],
  ["high-frequency-oscillation", "high_freq_oscillation"],
  ["high_frequency_oscillation", "high_freq_oscillation"],
  ["high_freq_oscillation", "high_freq_oscillation"],
]);

const PATTERN_DIR_NAMES = {
  low_variability: ["low_variability", "low-variability"],
  step_pattern: ["step_pattern", "step"],
  spike_pattern: ["spike_pattern", "spike"],
  spike_boundary_short: ["spike_boundary_short"],
  spike_boundary_medium: ["spike_boundary_medium"],
  spike_asymmetric_long: ["spike_asymmetric_long"],
  late_burst: ["late_burst"],
  multiple_bursts: ["multiple_bursts"],
  step_misaligned_45: ["step_misaligned_45"],
  step_misaligned_75: ["step_misaligned_75"],
  linear_ramp: ["linear_ramp"],
  asymmetric_activity: ["asymmetric_activity"],
  low_freq_oscillation: [
    "low_freq_oscillation",
    "low-frequency-oscillation",
    "low_frequency_oscillation",
  ],
  high_freq_oscillation: [
    "high_freq_oscillation",
    "high-frequency-oscillation",
    "high_frequency_oscillation",
  ],
};

function parseArgs(argv) {
  const args = {
    inputRoot: null,
    outputDir: null,
    patterns: [...CANONICAL_PATTERNS],
    approaches: [...APPROACHES],
    iterations: [1, 2, 3],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    switch (arg) {
      case "--input-root":
        if (!next) throw new Error("--input-root requires a value");
        args.inputRoot = resolvePath(next);
        index += 1;
        break;
      case "--output-dir":
        if (!next) throw new Error("--output-dir requires a value");
        args.outputDir = resolvePath(next);
        index += 1;
        break;
      case "--patterns":
        if (!next) throw new Error("--patterns requires a value");
        args.patterns = next.split(",").map((entry) => entry.trim()).filter(Boolean);
        index += 1;
        break;
      case "--approaches":
        if (!next) throw new Error("--approaches requires a value");
        args.approaches = next.split(",").map((entry) => entry.trim()).filter(Boolean);
        index += 1;
        break;
      case "--iterations":
        if (!next) throw new Error("--iterations requires a value");
        args.iterations = next
          .split(",")
          .map((entry) => Number.parseInt(entry.trim(), 10))
          .filter((entry) => Number.isFinite(entry) && entry > 0);
        index += 1;
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!args.inputRoot) {
    throw new Error("--input-root is required");
  }
  if (!args.outputDir) {
    throw new Error("--output-dir is required");
  }

  return args;
}

function printHelp() {
  console.log(`Usage: node scripts/benchmark/extract-custom-pattern-latency.js [options]

Options:
  --input-root <path>   Benchmark result root to scan
  --output-dir <path>   Directory for CSV outputs
  --patterns <list>     Comma-separated pattern list
  --approaches <list>   Comma-separated approach list
  --iterations <list>   Comma-separated iteration list
  --help                Show this help
`);
}

function resolvePath(inputPath) {
  return path.isAbsolute(inputPath) ? inputPath : path.resolve(process.cwd(), inputPath);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function exists(filePath) {
  try {
    return fs.existsSync(filePath);
  } catch {
    return false;
  }
}

function readJson(filePath) {
  if (!exists(filePath)) {
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    throw new Error(`Failed to parse JSON file ${filePath}: ${error.message}`);
  }
}

function readCsvRows(filePath) {
  if (!exists(filePath)) {
    return null;
  }

  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) {
    return null;
  }

  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    return null;
  }

  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? "";
    });
    return row;
  });
}

function parseNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function round(value, digits = 6) {
  if (!Number.isFinite(value)) {
    return "";
  }
  return Number(value.toFixed(digits));
}

function formatCsvValue(value) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (Number.isFinite(value)) {
    return String(round(value));
  }
  return String(value);
}

function mean(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length === 0) {
    return null;
  }
  return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function std(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length <= 1) {
    return 0;
  }
  const avg = mean(filtered);
  const variance = filtered.reduce((sum, value) => sum + (value - avg) ** 2, 0) / filtered.length;
  return Math.sqrt(variance);
}

function percentile(values, p) {
  const filtered = values.filter((value) => Number.isFinite(value)).sort((left, right) => left - right);
  if (filtered.length === 0) {
    return null;
  }
  if (filtered.length === 1) {
    return filtered[0];
  }

  const index = (filtered.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) {
    return filtered[lower];
  }
  return filtered[lower] + (index - lower) * (filtered[upper] - filtered[lower]);
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => formatCsvValue(row[header])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function pickNearbyFile(runDir, names) {
  for (const name of names) {
    const direct = path.join(runDir, name);
    if (exists(direct)) {
      return direct;
    }
  }

  const parentDir = path.dirname(runDir);
  for (const name of names) {
    const parent = path.join(parentDir, name);
    if (exists(parent)) {
      return parent;
    }
  }

  return null;
}

function readProfileCounter(profile, counterName) {
  if (!profile) {
    return null;
  }

  if (Number.isFinite(Number(profile[counterName]))) {
    return Number(profile[counterName]);
  }

  if (profile.summedCounters && Number.isFinite(Number(profile.summedCounters[counterName]))) {
    return Number(profile.summedCounters[counterName]);
  }

  if (profile.counters && Number.isFinite(Number(profile.counters[counterName]))) {
    return Number(profile.counters[counterName]);
  }

  if (Array.isArray(profile.processProfiles)) {
    const sum = profile.processProfiles.reduce((accumulator, entry) => {
      const value = Number(entry?.[counterName]);
      return accumulator + (Number.isFinite(value) ? value : 0);
    }, 0);
    return sum;
  }

  return null;
}

function getCell(row, names) {
  for (const name of names) {
    if (Object.prototype.hasOwnProperty.call(row, name)) {
      const value = parseNumber(row[name]);
      if (value !== null) {
        return value;
      }
    }
  }
  return null;
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function readChunkedCounters(runDir) {
  const debugSummary = readJson(pickNearbyFile(runDir, ["chunked_debug_summary.json"]));
  const emissionProof = readJson(pickNearbyFile(runDir, ["chunked_emission_proof.json"]));
  const profileAggregate = readJson(
    pickNearbyFile(runDir, ["hive_profile_summary.aggregate.json"]),
  );
  const profileWorker = readJson(pickNearbyFile(runDir, ["hive_profile_summary.worker.json"]));
  const profileOrchestrator = readJson(
    pickNearbyFile(runDir, ["hive_profile_summary.orchestrator.json"]),
  );

  const proofRows = Array.isArray(emissionProof) ? emissionProof : [];
  const proofEmittedRows = proofRows.filter((row) => row && row.emitted !== false);
  const proofCoverageCompleteRows = proofEmittedRows.filter((row) => row.coverageComplete !== false);
  const proofIncompleteRows = proofRows.filter((row) => row && row.emitted === true && row.coverageComplete === false);

  const debug = debugSummary || {};
  const aggregate = profileAggregate || profileWorker || profileOrchestrator || {};

  if (!debugSummary || !emissionProof) {
    console.warn(
      `[latency-extract] proof counters unavailable for run dir ${runDir}`,
    );
  }

  return {
    emissionProofWindowCount: Number.isFinite(Number(debug.emissionProofWindowCount))
      ? Number(debug.emissionProofWindowCount)
      : firstFiniteNumber(proofRows.length, readProfileCounter(aggregate, "emitted_results")) ?? "",
    coverageCompleteEmissionCount: Number.isFinite(Number(debug.coverageCompleteEmissionCount))
      ? Number(debug.coverageCompleteEmissionCount)
      : firstFiniteNumber(proofCoverageCompleteRows.length) ?? "",
    coverageIncompleteBlockedCount: Number.isFinite(Number(debug.coverageIncompleteBlockedCount))
      ? Number(debug.coverageIncompleteBlockedCount)
      : "",
    emittedIncompleteWindowCount: Number.isFinite(Number(debug.emittedIncompleteWindowCount))
      ? Number(debug.emittedIncompleteWindowCount)
      : firstFiniteNumber(proofIncompleteRows.length) ?? "",
    intervalTriggersWithEmission: Number.isFinite(Number(debug.intervalTriggersWithEmission))
      ? Number(debug.intervalTriggersWithEmission)
      : firstFiniteNumber(
          readProfileCounter(aggregate, "comparable_windows_emitted"),
          readProfileCounter(aggregate, "comparableWindowEmissionCount"),
          proofEmittedRows.length,
        ) ?? "",
    intervalTriggersWithoutEmission: Number.isFinite(Number(debug.intervalTriggersWithoutEmission))
      ? Number(debug.intervalTriggersWithoutEmission)
      : "",
    duplicateChunkCount: Number.isFinite(Number(debug.duplicateChunkCount))
      ? Number(debug.duplicateChunkCount)
      : firstFiniteNumber(readProfileCounter(aggregate, "duplicateChunkCount")) ?? "",
    comparableWindowEmissionCount: Number.isFinite(Number(debug.comparableWindowEmissionCount))
      ? Number(debug.comparableWindowEmissionCount)
      : firstFiniteNumber(
          readProfileCounter(aggregate, "comparable_windows_emitted"),
          readProfileCounter(aggregate, "comparableWindowEmissionCount"),
          proofEmittedRows.length,
        ) ?? "",
    reconstructedSuperqueryResultCount: Number.isFinite(Number(debug.reconstructedSuperqueryResultCount))
      ? Number(debug.reconstructedSuperqueryResultCount)
      : firstFiniteNumber(
          readProfileCounter(aggregate, "reconstructed_superquery_results"),
          readProfileCounter(aggregate, "reconstructedSuperqueryResultCount"),
          proofEmittedRows.length,
        ) ?? "",
    comparableOutputCadenceOnly: debug.comparableOutputCadenceOnly !== undefined ? String(debug.comparableOutputCadenceOnly) : "",
    useImmediateTrigger: debug.useImmediateTrigger !== undefined ? String(debug.useImmediateTrigger) : "",
  };
}

function buildRunRow({ pattern, approach, iteration, runDir }) {
  const latencyPath = path.join(runDir, LATENCY_FILE_BY_APPROACH[approach]);
  const rows = readCsvRows(latencyPath);
  if (!rows || rows.length === 0) {
    return null;
  }

  const queryRegisteredTimes = rows.map((row) =>
    getCell(row, ["query_registered_at", "queryRegisteredAt"]),
  ).filter((value) => Number.isFinite(value));
  const resultEmissionTimes = rows.map((row) =>
    getCell(row, ["result_emitted_at", "resultEmittedAt"]),
  ).filter((value) => Number.isFinite(value));
  const queryRegisteredAt = queryRegisteredTimes[0] ?? null;
  const firstResultLatencyMs = Number.isFinite(queryRegisteredAt) && resultEmissionTimes.length > 0
    ? resultEmissionTimes[0] - queryRegisteredAt
    : null;
  const lastResultLatencyMs = Number.isFinite(queryRegisteredAt) && resultEmissionTimes.length > 0
    ? resultEmissionTimes[resultEmissionTimes.length - 1] - queryRegisteredAt
    : null;

  const windowAdjustedLatencies = rows.map((row) => {
    const explicitDelay = getCell(row, ["delay_past_expected_close_ms", "latency_from_expected_close_ms"]);
    if (Number.isFinite(explicitDelay)) {
      return explicitDelay;
    }

    const emittedAt = getCell(row, ["result_emitted_at", "resultEmittedAt"]);
    const expectedClose = getCell(row, ["expected_window_close", "expectedWindowClose"]);
    if (Number.isFinite(emittedAt) && Number.isFinite(expectedClose)) {
      return emittedAt - expectedClose;
    }
    return null;
  });

  const meanComputationMs = approach === "chunked"
    ? mean(rows.map((row) => getCell(row, ["computation_ms"])))
    : null;

  const meanDelayPastLastObsOrChunkMs = approach === "fetching"
    ? mean(rows.map((row) => getCell(row, ["delay_past_last_obs_ms", "latency_from_last_obs_ms"])))
    : mean(rows.map((row) => {
        const emittedAt = getCell(row, ["result_emitted_at", "resultEmittedAt"]);
        const lastChunkReceivedAt = getCell(row, ["last_chunk_received_at"]);
        return Number.isFinite(emittedAt) && Number.isFinite(lastChunkReceivedAt)
          ? emittedAt - lastChunkReceivedAt
          : getCell(row, ["delay_past_last_obs_ms", "latency_from_last_obs_ms"]);
      }));

  const meanIntervalWaitMs = approach === "chunked"
    ? mean(rows.map((row) => getCell(row, ["interval_wait_ms"])))
    : null;

  const delayPastExpectedCloseMs = mean(rows.map((row) => getCell(row, ["delay_past_expected_close_ms", "latency_from_expected_close_ms"])));

  const requiredChunkIntervals = approach === "chunked"
    ? (rows[rows.length - 1]?.required_chunk_intervals ?? "")
    : "";
  const lastRequiredChunkReceivedAt = approach === "chunked"
    ? mean(rows.map((row) => getCell(row, ["last_required_chunk_received_at"])))
    : null;
  const semanticReadyAt = approach === "chunked"
    ? mean(rows.map((row) => getCell(row, ["semantic_ready_at"])))
    : null;
  const windowCloseToReadyMs = approach === "chunked"
    ? mean(rows.map((row) => getCell(row, ["window_close_to_ready_ms"])))
    : null;
  const readyToEmitMs = approach === "chunked"
    ? mean(rows.map((row) => getCell(row, ["ready_to_emit_ms"])))
    : null;
  const triggerType = approach === "chunked"
    ? (rows[rows.length - 1]?.trigger_type ?? "")
    : "";
  const emissionReason = approach === "chunked"
    ? (rows[rows.length - 1]?.emission_reason ?? "")
    : "";

  const baseRow = {
    pattern,
    approach,
    iteration,
    emitted_results: rows.length,
    first_result_latency_ms: firstResultLatencyMs,
    last_result_latency_ms: lastResultLatencyMs,
    mean_window_adjusted_latency_ms: mean(windowAdjustedLatencies),
    p95_window_adjusted_latency_ms: percentile(windowAdjustedLatencies, 0.95),
    mean_computation_ms: meanComputationMs,
    mean_delay_past_last_obs_or_chunk_ms: meanDelayPastLastObsOrChunkMs,
    mean_interval_wait_ms: meanIntervalWaitMs,
    emissionProofWindowCount: "",
    coverageCompleteEmissionCount: "",
    coverageIncompleteBlockedCount: "",
    emittedIncompleteWindowCount: "",
    intervalTriggersWithEmission: "",
    intervalTriggersWithoutEmission: "",
    duplicateChunkCount: "",
    comparableWindowEmissionCount: "",
    reconstructedSuperqueryResultCount: "",
    required_chunk_intervals: requiredChunkIntervals,
    last_required_chunk_received_at: lastRequiredChunkReceivedAt,
    semantic_ready_at: semanticReadyAt,
    window_close_to_ready_ms: windowCloseToReadyMs,
    ready_to_emit_ms: readyToEmitMs,
    trigger_type: triggerType,
    emission_reason: emissionReason,
    comparableOutputCadenceOnly: "",
    useImmediateTrigger: "",
    computation_ms: meanComputationMs,
    delay_past_expected_close_ms: delayPastExpectedCloseMs,
  };

  if (approach === "chunked") {
    Object.assign(baseRow, readChunkedCounters(runDir));
  }

  return baseRow;
}

function resolveRunDir(inputRoot, pattern, approach, iteration) {
  const patternDirs = PATTERN_DIR_NAMES[pattern] || [pattern];
  const candidates = [];

  for (const patternDir of patternDirs) {
    candidates.push(
      path.join(inputRoot, approach, patternDir, `iteration${iteration}`),
      path.join(inputRoot, "patterns", patternDir, approach, `iteration${iteration}`),
      path.join(inputRoot, patternDir, approach, `iteration${iteration}`),
    );
  }

  for (const candidate of candidates) {
    if (exists(candidate)) {
      return candidate;
    }
  }

  return null;
}

function buildAggregateRows(runRows) {
  const grouped = new Map();
  for (const row of runRows) {
    const key = `${row.pattern}::${row.approach}`;
    const list = grouped.get(key) || [];
    list.push(row);
    grouped.set(key, list);
  }

  return [...grouped.entries()]
    .map(([key, rows]) => {
      const [pattern, approach] = key.split("::");
      return {
        input_root: rows[0]?.input_root || "",
        pattern,
        approach,
        first_result_latency_ms_mean: mean(rows.map((row) => row.first_result_latency_ms)),
        first_result_latency_ms_std: std(rows.map((row) => row.first_result_latency_ms)),
        last_result_latency_ms_mean: mean(rows.map((row) => row.last_result_latency_ms)),
        last_result_latency_ms_std: std(rows.map((row) => row.last_result_latency_ms)),
        mean_window_adjusted_latency_ms_mean: mean(
          rows.map((row) => row.mean_window_adjusted_latency_ms),
        ),
        mean_window_adjusted_latency_ms_std: std(
          rows.map((row) => row.mean_window_adjusted_latency_ms),
        ),
        p95_window_adjusted_latency_ms_mean: mean(
          rows.map((row) => row.p95_window_adjusted_latency_ms),
        ),
        mean_computation_ms_mean: mean(rows.map((row) => row.mean_computation_ms)),
        mean_delay_past_last_obs_or_chunk_ms_mean: mean(
          rows.map((row) => row.mean_delay_past_last_obs_or_chunk_ms),
        ),
        mean_interval_wait_ms_mean: mean(rows.map((row) => row.mean_interval_wait_ms)),
        window_close_to_ready_ms_mean: mean(rows.map((row) => row.window_close_to_ready_ms)),
        ready_to_emit_ms_mean: mean(rows.map((row) => row.ready_to_emit_ms)),
        delay_past_expected_close_ms_mean: mean(rows.map((row) => row.delay_past_expected_close_ms)),
      };
    })
    .sort((left, right) => {
      const patternCompare =
        CANONICAL_PATTERNS.indexOf(left.pattern) - CANONICAL_PATTERNS.indexOf(right.pattern);
      if (patternCompare !== 0) {
        return patternCompare;
      }
      return APPROACHES.indexOf(left.approach) - APPROACHES.indexOf(right.approach);
    });
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!exists(args.inputRoot)) {
    throw new Error(`Input root does not exist: ${args.inputRoot}`);
  }
  ensureDir(args.outputDir);

  const runRows = [];
  const missingRuns = [];

  for (const pattern of args.patterns) {
    const canonicalPattern = PATTERN_ALIASES.get(pattern) || pattern;
    if (!CANONICAL_PATTERNS.includes(canonicalPattern)) {
      throw new Error(`Unsupported pattern: ${pattern}`);
    }

    for (const approach of args.approaches) {
      if (!APPROACHES.includes(approach)) {
        throw new Error(`Unsupported approach: ${approach}`);
      }

      for (const iteration of args.iterations) {
        const runDir = resolveRunDir(args.inputRoot, canonicalPattern, approach, iteration);
        if (!runDir) {
          missingRuns.push({ pattern: canonicalPattern, approach, iteration });
          console.warn(
            `[latency-extract] missing run directory: ${path.join(
              args.inputRoot,
              approach,
              canonicalPattern,
              `iteration${iteration}`,
            )}`,
          );
          continue;
        }

        const row = buildRunRow({
          pattern: canonicalPattern,
          approach,
          iteration,
          runDir,
        });
        if (row) {
          row.input_root = args.inputRoot;
          runRows.push(row);
        }
      }
    }
  }

  if (runRows.length === 0) {
    const details = missingRuns.length > 0
      ? ` Missing runs: ${missingRuns.map((run) => `${run.pattern}/${run.approach}/iteration${run.iteration}`).join(", ")}.`
      : "";
    throw new Error(`No valid runs found under input root ${args.inputRoot}.${details}`);
  }

  runRows.sort((left, right) => {
    const patternCompare =
      CANONICAL_PATTERNS.indexOf(left.pattern) - CANONICAL_PATTERNS.indexOf(right.pattern);
    if (patternCompare !== 0) {
      return patternCompare;
    }
    const approachCompare = APPROACHES.indexOf(left.approach) - APPROACHES.indexOf(right.approach);
    if (approachCompare !== 0) {
      return approachCompare;
    }
    return left.iteration - right.iteration;
  });

  const perRunCsvPath = path.join(args.outputDir, "custom_pattern_validation_immediate_latency_per_run.csv");
  const aggregateCsvPath = path.join(args.outputDir, "custom_pattern_validation_immediate_latency_aggregate.csv");

  writeCsv(perRunCsvPath, [
    "input_root",
    "pattern",
    "approach",
    "iteration",
    "emitted_results",
    "first_result_latency_ms",
    "last_result_latency_ms",
    "mean_window_adjusted_latency_ms",
    "p95_window_adjusted_latency_ms",
    "mean_computation_ms",
    "mean_delay_past_last_obs_or_chunk_ms",
    "mean_interval_wait_ms",
    "emissionProofWindowCount",
    "coverageCompleteEmissionCount",
    "coverageIncompleteBlockedCount",
    "emittedIncompleteWindowCount",
    "intervalTriggersWithEmission",
    "intervalTriggersWithoutEmission",
    "duplicateChunkCount",
    "comparableWindowEmissionCount",
    "reconstructedSuperqueryResultCount",
    "required_chunk_intervals",
    "last_required_chunk_received_at",
    "semantic_ready_at",
    "window_close_to_ready_ms",
    "ready_to_emit_ms",
    "trigger_type",
    "emission_reason",
    "comparableOutputCadenceOnly",
    "useImmediateTrigger",
    "computation_ms",
    "delay_past_expected_close_ms",
  ], runRows);

  writeCsv(aggregateCsvPath, [
    "input_root",
    "pattern",
    "approach",
    "first_result_latency_ms_mean",
    "first_result_latency_ms_std",
    "last_result_latency_ms_mean",
    "last_result_latency_ms_std",
    "mean_window_adjusted_latency_ms_mean",
    "mean_window_adjusted_latency_ms_std",
    "p95_window_adjusted_latency_ms_mean",
    "mean_computation_ms_mean",
    "mean_delay_past_last_obs_or_chunk_ms_mean",
    "mean_interval_wait_ms_mean",
    "window_close_to_ready_ms_mean",
    "ready_to_emit_ms_mean",
    "delay_past_expected_close_ms_mean",
  ], buildAggregateRows(runRows));

  console.log(`Wrote: ${perRunCsvPath}`);
  console.log(`Wrote: ${aggregateCsvPath}`);
}

main();
