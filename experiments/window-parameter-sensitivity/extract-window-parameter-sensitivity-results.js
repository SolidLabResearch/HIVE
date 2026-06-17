#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const {
  DEFAULT_APPROACHES,
  DEFAULT_PATTERNS,
  EXPERIMENTS,
  computeExpectedChunkStatesPerResult,
  normalizeExperimentName,
  parseCsvList,
  parsePositiveIntList,
} = require("./common");

const EXACT_RECONSTRUCTION_ERROR_TOLERANCE = 1e-3;

function printHelp() {
  console.log(`Usage: node experiments/window-parameter-sensitivity/extract-window-parameter-sensitivity-results.js [options]

Required:
  --experiment <name>      superquery-range-scaling | chunk-granularity-sensitivity
  --input-root <path>      Input log root for the selected experiment
  --output-dir <path>      Directory for CSV outputs

Optional:
  --patterns <list>        Default: low_variability
  --approaches <list>      Default: fetching,chunked
  --ranges <list>          Filter for experiment 2
  --chunk-sizes <list>     Filter for experiment 3
`);
}

function parseArgs(argv) {
  const args = {
    experimentName: null,
    inputRoot: null,
    outputDir: null,
    patterns: [...DEFAULT_PATTERNS],
    approaches: [...DEFAULT_APPROACHES],
    ranges: [],
    chunkSizes: [],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    switch (arg) {
      case "--experiment":
        if (!next) throw new Error("--experiment requires a value");
        args.experimentName = normalizeExperimentName(next);
        index += 1;
        break;
      case "--input-root":
        if (!next) throw new Error("--input-root requires a value");
        args.inputRoot = path.resolve(process.cwd(), next);
        index += 1;
        break;
      case "--output-dir":
        if (!next) throw new Error("--output-dir requires a value");
        args.outputDir = path.resolve(process.cwd(), next);
        index += 1;
        break;
      case "--patterns":
        if (!next) throw new Error("--patterns requires a value");
        args.patterns = parseCsvList(next);
        index += 1;
        break;
      case "--approaches":
        if (!next) throw new Error("--approaches requires a value");
        args.approaches = parseCsvList(next).map((value) => value.toLowerCase());
        index += 1;
        break;
      case "--ranges":
        if (!next) throw new Error("--ranges requires a value");
        args.ranges = parsePositiveIntList(next);
        index += 1;
        break;
      case "--chunk-sizes":
        if (!next) throw new Error("--chunk-sizes requires a value");
        args.chunkSizes = parsePositiveIntList(next);
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

  if (!args.experimentName) {
    throw new Error("--experiment is required");
  }
  if (!args.inputRoot) {
    throw new Error("--input-root is required");
  }
  if (!args.outputDir) {
    throw new Error("--output-dir is required");
  }

  return args;
}

function exists(filePath) {
  try {
    return fs.existsSync(filePath);
  } catch {
    return false;
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJson(filePath) {
  if (!exists(filePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function parseCsv(filePath) {
  if (!exists(filePath)) {
    return [];
  }
  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) {
    return [];
  }

  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    return [];
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

function mean(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length === 0) {
    return null;
  }
  return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function median(values) {
  const filtered = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (filtered.length === 0) {
    return null;
  }
  const mid = Math.floor(filtered.length / 2);
  if (filtered.length % 2 === 0) {
    return (filtered[mid - 1] + filtered[mid]) / 2;
  }
  return filtered[mid];
}

function percentile(values, percentileRank) {
  const filtered = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (filtered.length === 0) {
    return null;
  }
  if (filtered.length === 1) {
    return filtered[0];
  }
  const rank = (percentileRank / 100) * (filtered.length - 1);
  const lower = Math.floor(rank);
  const upper = Math.ceil(rank);
  if (lower === upper) {
    return filtered[lower];
  }
  const weight = rank - lower;
  return filtered[lower] * (1 - weight) + filtered[upper] * weight;
}

function std(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length <= 1) {
    return 0;
  }
  const avg = mean(filtered);
  const variance =
    filtered.reduce((sum, value) => sum + (value - avg) ** 2, 0) / filtered.length;
  return Math.sqrt(variance);
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
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (Number.isFinite(value)) {
    return String(round(value));
  }
  const stringValue = String(value);
  if (/[",\n]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => formatCsvValue(row[header])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function safeDivide(numerator, denominator) {
  if (
    !Number.isFinite(numerator) ||
    !Number.isFinite(denominator) ||
    denominator === 0
  ) {
    return null;
  }
  return numerator / denominator;
}

function getLatencyFilePath(approach, runDir) {
  return path.join(
    runDir,
    approach === "fetching" ? "fetching_latency_log.csv" : "chunked_latency_log.csv",
  );
}

function getProfileCounters(runDir) {
  const aggregate = readJson(path.join(runDir, "hive_profile_summary.aggregate.json"));
  if (aggregate && aggregate.summedCounters) {
    return aggregate.summedCounters;
  }

  const profileFiles = exists(runDir)
    ? fs
        .readdirSync(runDir)
        .filter(
          (fileName) =>
            /^hive_profile_summary\.[^.]+(?:_consumer_\d+)?\.json$/.test(fileName) &&
            fileName !== "hive_profile_summary.aggregate.json",
        )
    : [];
  const counters = {};
  for (const fileName of profileFiles) {
    const summary = readJson(path.join(runDir, fileName));
    if (!summary || !summary.counters) {
      continue;
    }
    for (const [key, value] of Object.entries(summary.counters)) {
      if (!Number.isFinite(Number(value))) {
        continue;
      }
      counters[key] = (counters[key] || 0) + Number(value);
    }
  }
  return counters;
}

function readChunkStatesConsumed(runDir) {
  const proof = readJson(path.join(runDir, "chunked_emission_proof.json"));
  if (!Array.isArray(proof) || proof.length === 0) {
    return {
      chunkStatesConsumedPerResultMean: null,
      chunkStatesConsumedPerResultMin: null,
      chunkStatesConsumedPerResultMax: null,
      reconstructedResultCount: null,
    };
  }

  const counts = proof
    .map((entry) => {
      const groups = Object.values(entry?.receivedChunksUsedBySubquery || {});
      return groups.reduce(
        (sum, chunkIds) => sum + (Array.isArray(chunkIds) ? chunkIds.length : 0),
        0,
      );
    })
    .filter((value) => Number.isFinite(value));

  if (counts.length === 0) {
    return {
      chunkStatesConsumedPerResultMean: null,
      chunkStatesConsumedPerResultMin: null,
      chunkStatesConsumedPerResultMax: null,
      reconstructedResultCount: null,
    };
  }

  return {
    chunkStatesConsumedPerResultMean: mean(counts),
    chunkStatesConsumedPerResultMin: Math.min(...counts),
    chunkStatesConsumedPerResultMax: Math.max(...counts),
    reconstructedResultCount: counts.length,
  };
}

function deduplicateLatencyRows(rows) {
  const keyedRows = new Map();
  for (const row of rows) {
    const windowNumber = parseNumber(row.window_number);
    if (!Number.isFinite(windowNumber)) {
      continue;
    }
    const emittedAt = parseNumber(row.result_emitted_at) ?? parseNumber(row.expected_window_close) ?? 0;
    const existing = keyedRows.get(windowNumber);
    if (!existing || emittedAt >= existing.emittedAt) {
      keyedRows.set(windowNumber, { row, emittedAt });
    }
  }

  return [...keyedRows.entries()]
    .sort((left, right) => left[0] - right[0])
    .map((entry) => entry[1].row);
}

function buildLatencySummary(approach, runDir) {
  const parsedRows = parseCsv(getLatencyFilePath(approach, runDir));
  const rows = deduplicateLatencyRows(parsedRows);
  const windowAdjustedLatencies = [];
  const readyToEmitValues = [];
  const computationValues = [];
  const resultValuesByWindow = new Map();

  for (const row of rows) {
    const windowNumber = parseNumber(row.window_number);
    const resultValue = parseNumber(row.result_value);
    if (Number.isFinite(windowNumber) && resultValue !== null) {
      resultValuesByWindow.set(windowNumber, resultValue);
    }

    const delay = parseNumber(row.delay_past_expected_close_ms);
    if (delay !== null) {
      windowAdjustedLatencies.push(delay);
    }

    const readyToEmit = parseNumber(row.ready_to_emit_ms);
    if (readyToEmit !== null) {
      readyToEmitValues.push(readyToEmit);
    }

    const computation =
      parseNumber(row.computation_ms) ?? parseNumber(row.delay_past_last_obs_ms);
    if (computation !== null) {
      computationValues.push(computation);
    }
  }

  return {
    rows,
    emittedWindowCount: rows.length,
    resultValuesByWindow,
    meanWindowAdjustedLatencyMs: mean(windowAdjustedLatencies),
    stdWindowAdjustedLatencyMs: std(windowAdjustedLatencies),
    medianWindowAdjustedLatencyMs: median(windowAdjustedLatencies),
    p95WindowAdjustedLatencyMs: percentile(windowAdjustedLatencies, 95),
    meanReadyToEmitMs: mean(readyToEmitValues),
    stdReadyToEmitMs: std(readyToEmitValues),
    medianReadyToEmitMs: median(readyToEmitValues),
    p95ReadyToEmitMs: percentile(readyToEmitValues, 95),
    meanComputationMs: mean(computationValues),
    stdComputationMs: std(computationValues),
    medianComputationMs: median(computationValues),
    p95ComputationMs: percentile(computationValues, 95),
  };
}

function computeErrorStats(run, baselineRun) {
  if (run.approach === "fetching") {
    return {
      mean_error: 0,
      mae: 0,
      rmse: 0,
      mape: 0,
      max_abs_error: 0,
      matched_window_count: run.emitted_window_count,
    };
  }

  if (!baselineRun) {
    return {
      mean_error: null,
      mae: null,
      rmse: null,
      mape: null,
      max_abs_error: null,
      matched_window_count: 0,
    };
  }

  let absoluteErrorSum = 0;
  let squaredErrorSum = 0;
  let mapeSum = 0;
  let mapeCount = 0;
  let matchedWindowCount = 0;
  let maxAbsError = 0;

  for (const [windowNumber, runValue] of run.result_values_by_window.entries()) {
    const baselineValue = baselineRun.result_values_by_window.get(windowNumber);
    if (baselineValue === undefined) {
      continue;
    }

    const absoluteError = Math.abs(runValue - baselineValue);
    absoluteErrorSum += absoluteError;
    squaredErrorSum += absoluteError ** 2;
    matchedWindowCount += 1;
    maxAbsError = Math.max(maxAbsError, absoluteError);

    if (baselineValue !== 0) {
      mapeSum += (absoluteError / Math.abs(baselineValue)) * 100;
      mapeCount += 1;
    }
  }

  if (matchedWindowCount === 0) {
    return {
      mean_error: null,
      mae: null,
      rmse: null,
      mape: null,
      max_abs_error: null,
      matched_window_count: 0,
    };
  }

  return {
    mean_error: absoluteErrorSum / matchedWindowCount,
    mae: absoluteErrorSum / matchedWindowCount,
    rmse: Math.sqrt(squaredErrorSum / matchedWindowCount),
    mape: mapeCount > 0 ? mapeSum / mapeCount : null,
    max_abs_error: maxAbsError,
    matched_window_count: matchedWindowCount,
  };
}

function getScenarioFilter(args) {
  if (args.experimentName === "superquery-range-scaling") {
    return args.ranges.length > 0
      ? new Set(args.ranges.map((value) => String(value)))
      : null;
  }
  return args.chunkSizes.length > 0
    ? new Set(args.chunkSizes.map((value) => String(value)))
    : null;
}

function loadRunRecords(args) {
  const records = [];
  const scenarioFilter = getScenarioFilter(args);

  for (const approach of args.approaches) {
    for (const pattern of args.patterns) {
      const patternRoot = path.join(args.inputRoot, approach, pattern);
      if (!exists(patternRoot)) {
        continue;
      }

      for (const scenarioEntry of fs.readdirSync(patternRoot)) {
        const scenarioRoot = path.join(patternRoot, scenarioEntry);
        if (!fs.statSync(scenarioRoot).isDirectory()) {
          continue;
        }

        const scenarioSecondsMatch = scenarioEntry.match(/(\d+)s$/);
        const scenarioSeconds = scenarioSecondsMatch
          ? Number.parseInt(scenarioSecondsMatch[1], 10)
          : null;
        if (scenarioFilter && !scenarioFilter.has(String(scenarioSeconds))) {
          continue;
        }

        for (const iterationEntry of fs.readdirSync(scenarioRoot)) {
          if (!/^iteration\d+$/.test(iterationEntry)) {
            continue;
          }
          const runDir = path.join(scenarioRoot, iterationEntry);
          const runMetadata = readJson(path.join(runDir, "run_metadata.json"));
          const resourceSummary = readJson(path.join(runDir, "resource_summary.json"));
          if (!runMetadata || !resourceSummary) {
            continue;
          }

          const latencySummary = buildLatencySummary(approach, runDir);
          const profileCounters = getProfileCounters(runDir);
          const chunkStatesConsumed = approach === "chunked"
            ? readChunkStatesConsumed(runDir)
            : {
                chunkStatesConsumedPerResultMean: null,
                chunkStatesConsumedPerResultMin: null,
                chunkStatesConsumedPerResultMax: null,
                reconstructedResultCount: null,
              };
          const emittedResultCount =
            Number(profileCounters.emitted_results || 0) ||
            latencySummary.emittedWindowCount ||
            parseNumber(runMetadata.emitted_result_count) ||
            0;
          const reconstructedResultCount =
            chunkStatesConsumed.reconstructedResultCount !== null &&
            chunkStatesConsumed.reconstructedResultCount !== undefined
              ? chunkStatesConsumed.reconstructedResultCount
              : Number(profileCounters.reconstructed_superquery_results || 0);
          const superqueryResultCount =
            Number(profileCounters.emitted_results || 0) || emittedResultCount;
          const expectedChunkStatesPerResult =
            runMetadata.expected_chunk_states_per_result ??
            computeExpectedChunkStatesPerResult({
              superqueryRangeSeconds: Number(runMetadata.superquery_range_seconds),
              chunkSizeSeconds: Number(runMetadata.chunk_size_seconds),
            });
          const chunkStatesConsumedPerEmittedResult =
            approach === "chunked"
              ? chunkStatesConsumed.chunkStatesConsumedPerResultMean ??
                expectedChunkStatesPerResult
              : null;

          records.push({
            ...runMetadata,
            cpu_seconds: parseNumber(resourceSummary.cpuSeconds),
            peak_rss_mb: parseNumber(resourceSummary.peakRssMb),
            mean_rss_mb: parseNumber(resourceSummary.meanRssMb),
            wall_time_seconds: parseNumber(resourceSummary.wallTimeSec),
            peak_cpu_percent: parseNumber(resourceSummary.peakCpuPct),
            peak_process_count: parseNumber(resourceSummary.peakProcessCount),
            emitted_window_count: latencySummary.emittedWindowCount,
            emitted_result_count: emittedResultCount,
            reconstructed_result_count: reconstructedResultCount,
            superquery_result_count: superqueryResultCount,
            result_values_by_window: latencySummary.resultValuesByWindow,
            mean_window_adjusted_latency_ms:
              latencySummary.meanWindowAdjustedLatencyMs,
            std_window_adjusted_latency_ms:
              latencySummary.stdWindowAdjustedLatencyMs,
            median_window_adjusted_latency_ms:
              latencySummary.medianWindowAdjustedLatencyMs,
            p95_window_adjusted_latency_ms:
              latencySummary.p95WindowAdjustedLatencyMs,
            mean_ready_to_emit_ms: latencySummary.meanReadyToEmitMs,
            std_ready_to_emit_ms: latencySummary.stdReadyToEmitMs,
            median_ready_to_emit_ms: latencySummary.medianReadyToEmitMs,
            p95_ready_to_emit_ms: latencySummary.p95ReadyToEmitMs,
            mean_computation_ms: latencySummary.meanComputationMs,
            std_computation_ms: latencySummary.stdComputationMs,
            median_computation_ms: latencySummary.medianComputationMs,
            p95_computation_ms: latencySummary.p95ComputationMs,
            process_cleanup_ok: Boolean(runMetadata.process_cleanup_ok),
            cleanup_forced_sigkill_required: Boolean(
              runMetadata.cleanup_forced_sigkill_required,
            ),
            stale_processes_found_after_cleanup: Boolean(
              runMetadata.stale_processes_found_after_cleanup,
            ),
            shared_chunk_producers_created:
              Number(profileCounters.shared_chunk_producers_created || 0),
            chunk_state_messages_published:
              Number(profileCounters.chunk_state_messages_published || 0),
            fallback_original_agent_rsps_started:
              Number(profileCounters.fallback_original_agent_rsps_started || 0),
            reconstructed_superquery_results:
              Number(profileCounters.reconstructed_superquery_results || 0),
            rsp_engines_created: Number(profileCounters.rsp_engines_created || 0),
            mqtt_clients_created: Number(profileCounters.mqtt_clients_created || 0),
            compatible_queries_detected:
              Number(profileCounters.compatible_queries_detected || 0),
            original_agent_rsps_skipped:
              Number(profileCounters.original_agent_rsps_skipped || 0),
            original_agent_outputs_derived_from_chunks:
              Number(profileCounters.original_agent_outputs_derived_from_chunks || 0),
            exact_final_result_reuse_hits:
              Number(profileCounters.exact_final_result_reuse_hits || 0),
            chunk_consumers_registered:
              Number(profileCounters.chunk_consumers_registered || 0),
            chunk_groups_completed: Number(profileCounters.chunk_groups_completed || 0),
            comparable_windows_emitted:
              Number(profileCounters.comparable_windows_emitted || 0),
            chunk_states_consumed_per_result_mean:
              chunkStatesConsumed.chunkStatesConsumedPerResultMean,
            chunk_states_consumed_per_result_min:
              chunkStatesConsumed.chunkStatesConsumedPerResultMin,
            chunk_states_consumed_per_result_max:
              chunkStatesConsumed.chunkStatesConsumedPerResultMax,
            chunk_states_consumed_per_emitted_result:
              chunkStatesConsumedPerEmittedResult,
            expected_chunk_states_per_result: expectedChunkStatesPerResult,
            expected_chunk_states_total:
              Number.isFinite(expectedChunkStatesPerResult) &&
              Number.isFinite(emittedResultCount)
                ? expectedChunkStatesPerResult * emittedResultCount
                : null,
            cpu_seconds_per_emitted_result: safeDivide(
              parseNumber(resourceSummary.cpuSeconds),
              emittedResultCount,
            ),
            peak_rss_mb_per_emitted_result: safeDivide(
              parseNumber(resourceSummary.peakRssMb),
              emittedResultCount,
            ),
            chunk_state_messages_per_emitted_result: safeDivide(
              Number(profileCounters.chunk_state_messages_published || 0),
              emittedResultCount,
            ),
            chunk_size_applies_to_approach: approach === "chunked",
            log_dir: runDir,
          });
        }
      }
    }
  }

  return records;
}

function applyValidation(records) {
  const fetchingByScenario = new Map();
  for (const record of records) {
    if (record.approach !== "fetching") {
      continue;
    }
    const key = [
      record.experiment_name,
      record.pattern,
      record.iteration,
      record.superquery_range_seconds,
      record.superquery_step_seconds,
      record.chunk_size_seconds,
    ].join("::");
    fetchingByScenario.set(key, record);
  }

  for (const record of records) {
    const baselineKey = [
      record.experiment_name,
      record.pattern,
      record.iteration,
      record.superquery_range_seconds,
      record.superquery_step_seconds,
      record.chunk_size_seconds,
    ].join("::");
    const baseline = fetchingByScenario.get(baselineKey);
    const errorStats = computeErrorStats(record, baseline);
    Object.assign(record, errorStats);

    const reasons = [];
    if (!record.success) {
      reasons.push(record.validity_reason || "runner_marked_failure");
    }
    if (record.emitted_result_count <= 0) {
      reasons.push("no_result_windows_emitted");
    }
    if (!record.process_cleanup_ok) {
      reasons.push("process_cleanup_failed");
    }
    if (record.exact_final_reuse_enabled) {
      reasons.push("exact_final_reuse_enabled");
    }
    if (record.exact_final_result_reuse_hits > 0) {
      reasons.push("exact_final_result_reuse_hits_detected");
    }

    if (record.approach === "chunked") {
      if (record.fallback_original_agent_rsps_started > 0) {
        reasons.push("fallback_original_agent_rsps_started_nonzero");
      }
      if (
        record.mean_error !== null &&
        record.mean_error > EXACT_RECONSTRUCTION_ERROR_TOLERANCE
      ) {
        reasons.push(`mean_error_nonzero:${record.mean_error}`);
      }
      if (record.mae !== null && record.mae > EXACT_RECONSTRUCTION_ERROR_TOLERANCE) {
        reasons.push(`mae_exceeds_tolerance:${record.mae}`);
      }
    }

    const warnings = [];
    if (record.approach === "fetching" && record.rsp_engines_created === 2) {
      warnings.push("fetching_rsp_engines_created_equals_2_known_k_plus_1_warning");
    }

    record.success = Boolean(record.success);
    record.is_valid = reasons.length === 0;
    record.validity_reason =
      reasons.length === 0
        ? warnings.length === 0
          ? "valid"
          : `valid_with_warning:${warnings.join(";")}`
        : reasons.join(";");
  }
}

function buildPerRunRows(records) {
  return records
    .sort((left, right) => {
      if (left.pattern !== right.pattern) {
        return left.pattern.localeCompare(right.pattern);
      }
      if (left.approach !== right.approach) {
        return left.approach.localeCompare(right.approach);
      }
      if (left.superquery_range_seconds !== right.superquery_range_seconds) {
        return left.superquery_range_seconds - right.superquery_range_seconds;
      }
      if (left.chunk_size_seconds !== right.chunk_size_seconds) {
        return left.chunk_size_seconds - right.chunk_size_seconds;
      }
      return left.iteration - right.iteration;
    })
    .map((record) => ({
      experiment_name: record.experiment_name,
      approach: record.approach,
      pattern: record.pattern,
      iteration: record.iteration,
      aggregation_function: record.aggregation_function,
      superquery_range_seconds: record.superquery_range_seconds,
      superquery_step_seconds: record.superquery_step_seconds,
      chunk_size_seconds: record.chunk_size_seconds,
      replay_duration_seconds: record.replay_duration_seconds,
      exact_final_reuse_enabled: record.exact_final_reuse_enabled,
      chunk_size_applies_to_approach: record.chunk_size_applies_to_approach,
      cpu_seconds: record.cpu_seconds,
      cpu_seconds_per_emitted_result: record.cpu_seconds_per_emitted_result,
      peak_rss_mb: record.peak_rss_mb,
      peak_rss_mb_per_emitted_result: record.peak_rss_mb_per_emitted_result,
      mean_rss_mb: record.mean_rss_mb,
      mean_window_adjusted_latency_ms: record.mean_window_adjusted_latency_ms,
      std_window_adjusted_latency_ms: record.std_window_adjusted_latency_ms,
      median_window_adjusted_latency_ms: record.median_window_adjusted_latency_ms,
      p95_window_adjusted_latency_ms: record.p95_window_adjusted_latency_ms,
      mean_ready_to_emit_ms: record.mean_ready_to_emit_ms,
      std_ready_to_emit_ms: record.std_ready_to_emit_ms,
      median_ready_to_emit_ms: record.median_ready_to_emit_ms,
      p95_ready_to_emit_ms: record.p95_ready_to_emit_ms,
      mean_computation_ms: record.mean_computation_ms,
      std_computation_ms: record.std_computation_ms,
      median_computation_ms: record.median_computation_ms,
      p95_computation_ms: record.p95_computation_ms,
      mean_error: record.mean_error,
      mae: record.mae,
      rmse: record.rmse,
      mape: record.mape,
      max_abs_error: record.max_abs_error,
      chunk_state_messages_published: record.chunk_state_messages_published,
      chunk_state_messages_per_emitted_result:
        record.chunk_state_messages_per_emitted_result,
      shared_chunk_producers_created: record.shared_chunk_producers_created,
      fallback_original_agent_rsps_started:
        record.fallback_original_agent_rsps_started,
      reconstructed_superquery_results: record.reconstructed_superquery_results,
      rsp_engines_created: record.rsp_engines_created,
      mqtt_clients_created: record.mqtt_clients_created,
      compatible_queries_detected: record.compatible_queries_detected,
      original_agent_rsps_skipped: record.original_agent_rsps_skipped,
      original_agent_outputs_derived_from_chunks:
        record.original_agent_outputs_derived_from_chunks,
      exact_final_result_reuse_hits: record.exact_final_result_reuse_hits,
      emitted_result_count: record.emitted_result_count,
      reconstructed_result_count: record.reconstructed_result_count,
      superquery_result_count: record.superquery_result_count,
      emitted_window_count: record.emitted_window_count,
      matched_window_count: record.matched_window_count,
      chunk_states_consumed_per_result_mean:
        record.chunk_states_consumed_per_result_mean,
      chunk_states_consumed_per_emitted_result:
        record.chunk_states_consumed_per_emitted_result,
      expected_chunk_states_per_result: record.expected_chunk_states_per_result,
      expected_chunk_states_total: record.expected_chunk_states_total,
      success: record.success,
      is_valid: record.is_valid,
      process_cleanup_ok: record.process_cleanup_ok,
      validity_reason: record.validity_reason,
      log_dir: record.log_dir,
    }));
}

function buildAggregateRows(records) {
  const groups = new Map();
  for (const record of records) {
    const key = [
      record.experiment_name,
      record.approach,
      record.pattern,
      record.aggregation_function,
      record.superquery_range_seconds,
      record.superquery_step_seconds,
      record.chunk_size_seconds,
      record.replay_duration_seconds,
      record.exact_final_reuse_enabled,
    ].join("::");
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(record);
  }

  const metricNames = [
    "cpu_seconds",
    "cpu_seconds_per_emitted_result",
    "peak_rss_mb",
    "peak_rss_mb_per_emitted_result",
    "mean_window_adjusted_latency_ms",
    "std_window_adjusted_latency_ms",
    "median_window_adjusted_latency_ms",
    "p95_window_adjusted_latency_ms",
    "mean_ready_to_emit_ms",
    "std_ready_to_emit_ms",
    "median_ready_to_emit_ms",
    "p95_ready_to_emit_ms",
    "mean_computation_ms",
    "std_computation_ms",
    "median_computation_ms",
    "p95_computation_ms",
    "mean_error",
    "mae",
    "rmse",
    "mape",
    "max_abs_error",
    "chunk_state_messages_published",
    "chunk_state_messages_per_emitted_result",
    "shared_chunk_producers_created",
    "fallback_original_agent_rsps_started",
    "reconstructed_superquery_results",
    "emitted_result_count",
    "reconstructed_result_count",
    "superquery_result_count",
    "chunk_states_consumed_per_result_mean",
    "chunk_states_consumed_per_emitted_result",
    "expected_chunk_states_total",
  ];

  const rows = [];
  for (const groupRecords of groups.values()) {
    const first = groupRecords[0];
    const row = {
      experiment_name: first.experiment_name,
      approach: first.approach,
      pattern: first.pattern,
      aggregation_function: first.aggregation_function,
      superquery_range_seconds: first.superquery_range_seconds,
      superquery_step_seconds: first.superquery_step_seconds,
      chunk_size_seconds: first.chunk_size_seconds,
      replay_duration_seconds: first.replay_duration_seconds,
      exact_final_reuse_enabled: first.exact_final_reuse_enabled,
      iterations: groupRecords.length,
      success_rate: groupRecords.filter((record) => record.success).length / groupRecords.length,
      valid_rate: groupRecords.filter((record) => record.is_valid).length / groupRecords.length,
      expected_chunk_states_per_result: first.expected_chunk_states_per_result,
      chunk_size_applies_to_approach: first.chunk_size_applies_to_approach,
    };

    for (const metricName of metricNames) {
      const values = groupRecords.map((record) => record[metricName]);
      row[`${metricName}_mean`] = mean(values);
      row[`${metricName}_std`] = std(values);
    }

    rows.push(row);
  }

  return rows.sort((left, right) => {
    if (left.pattern !== right.pattern) {
      return left.pattern.localeCompare(right.pattern);
    }
    if (left.approach !== right.approach) {
      return left.approach.localeCompare(right.approach);
    }
    if (left.superquery_range_seconds !== right.superquery_range_seconds) {
      return left.superquery_range_seconds - right.superquery_range_seconds;
    }
    return left.chunk_size_seconds - right.chunk_size_seconds;
  });
}

function buildProfileRows(records) {
  return records.map((record) => ({
    experiment_name: record.experiment_name,
    approach: record.approach,
    pattern: record.pattern,
    iteration: record.iteration,
    aggregation_function: record.aggregation_function,
    superquery_range_seconds: record.superquery_range_seconds,
    superquery_step_seconds: record.superquery_step_seconds,
    chunk_size_seconds: record.chunk_size_seconds,
    replay_duration_seconds: record.replay_duration_seconds,
    exact_final_reuse_enabled: record.exact_final_reuse_enabled,
    chunk_size_applies_to_approach: record.chunk_size_applies_to_approach,
    shared_chunk_producers_created: record.shared_chunk_producers_created,
    chunk_state_messages_published: record.chunk_state_messages_published,
    fallback_original_agent_rsps_started:
      record.fallback_original_agent_rsps_started,
    reconstructed_superquery_results: record.reconstructed_superquery_results,
    rsp_engines_created: record.rsp_engines_created,
    mqtt_clients_created: record.mqtt_clients_created,
    compatible_queries_detected: record.compatible_queries_detected,
    original_agent_rsps_skipped: record.original_agent_rsps_skipped,
    original_agent_outputs_derived_from_chunks:
      record.original_agent_outputs_derived_from_chunks,
    exact_final_result_reuse_hits: record.exact_final_result_reuse_hits,
    chunk_consumers_registered: record.chunk_consumers_registered,
    chunk_groups_completed: record.chunk_groups_completed,
    comparable_windows_emitted: record.comparable_windows_emitted,
    success: record.success,
    is_valid: record.is_valid,
    process_cleanup_ok: record.process_cleanup_ok,
    validity_reason: record.validity_reason,
  }));
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const records = loadRunRecords(args);
  applyValidation(records);

  ensureDir(args.outputDir);

  const experimentDefinition = EXPERIMENTS[args.experimentName];
  const filePrefix =
    args.experimentName === "superquery-range-scaling"
      ? "superquery_range_scaling"
      : "chunk_granularity_sensitivity";

  const perRunRows = buildPerRunRows(records);
  const aggregateRows = buildAggregateRows(records);
  const profileRows = buildProfileRows(records);

  writeCsv(
    path.join(args.outputDir, `${filePrefix}_per_run.csv`),
    [
      "experiment_name",
      "approach",
      "pattern",
      "iteration",
      "aggregation_function",
      "superquery_range_seconds",
      "superquery_step_seconds",
      "chunk_size_seconds",
      "replay_duration_seconds",
      "exact_final_reuse_enabled",
      "chunk_size_applies_to_approach",
      "cpu_seconds",
      "cpu_seconds_per_emitted_result",
      "peak_rss_mb",
      "peak_rss_mb_per_emitted_result",
      "mean_rss_mb",
      "mean_window_adjusted_latency_ms",
      "std_window_adjusted_latency_ms",
      "median_window_adjusted_latency_ms",
      "p95_window_adjusted_latency_ms",
      "mean_ready_to_emit_ms",
      "std_ready_to_emit_ms",
      "median_ready_to_emit_ms",
      "p95_ready_to_emit_ms",
      "mean_computation_ms",
      "std_computation_ms",
      "median_computation_ms",
      "p95_computation_ms",
      "mean_error",
      "mae",
      "rmse",
      "mape",
      "max_abs_error",
      "chunk_state_messages_published",
      "chunk_state_messages_per_emitted_result",
      "shared_chunk_producers_created",
      "fallback_original_agent_rsps_started",
      "reconstructed_superquery_results",
      "rsp_engines_created",
      "mqtt_clients_created",
      "compatible_queries_detected",
      "original_agent_rsps_skipped",
      "original_agent_outputs_derived_from_chunks",
      "exact_final_result_reuse_hits",
      "emitted_result_count",
      "reconstructed_result_count",
      "superquery_result_count",
      "emitted_window_count",
      "matched_window_count",
      "chunk_states_consumed_per_result_mean",
      "chunk_states_consumed_per_emitted_result",
      "expected_chunk_states_per_result",
      "expected_chunk_states_total",
      "success",
      "is_valid",
      "process_cleanup_ok",
      "validity_reason",
      "log_dir",
    ],
    perRunRows,
  );

  writeCsv(
    path.join(args.outputDir, `${filePrefix}_aggregate.csv`),
    [
      "experiment_name",
      "approach",
      "pattern",
      "aggregation_function",
      "superquery_range_seconds",
      "superquery_step_seconds",
      "chunk_size_seconds",
      "replay_duration_seconds",
      "exact_final_reuse_enabled",
      "iterations",
      "success_rate",
      "valid_rate",
      "chunk_size_applies_to_approach",
      "expected_chunk_states_per_result",
      "cpu_seconds_per_emitted_result_mean",
      "cpu_seconds_per_emitted_result_std",
      "cpu_seconds_mean",
      "cpu_seconds_std",
      "peak_rss_mb_per_emitted_result_mean",
      "peak_rss_mb_per_emitted_result_std",
      "peak_rss_mb_mean",
      "peak_rss_mb_std",
      "mean_window_adjusted_latency_ms_mean",
      "mean_window_adjusted_latency_ms_std",
      "std_window_adjusted_latency_ms_mean",
      "std_window_adjusted_latency_ms_std",
      "median_window_adjusted_latency_ms_mean",
      "median_window_adjusted_latency_ms_std",
      "p95_window_adjusted_latency_ms_mean",
      "p95_window_adjusted_latency_ms_std",
      "mean_ready_to_emit_ms_mean",
      "mean_ready_to_emit_ms_std",
      "std_ready_to_emit_ms_mean",
      "std_ready_to_emit_ms_std",
      "median_ready_to_emit_ms_mean",
      "median_ready_to_emit_ms_std",
      "p95_ready_to_emit_ms_mean",
      "p95_ready_to_emit_ms_std",
      "mean_computation_ms_mean",
      "mean_computation_ms_std",
      "std_computation_ms_mean",
      "std_computation_ms_std",
      "median_computation_ms_mean",
      "median_computation_ms_std",
      "p95_computation_ms_mean",
      "p95_computation_ms_std",
      "mean_error_mean",
      "mean_error_std",
      "mae_mean",
      "mae_std",
      "rmse_mean",
      "rmse_std",
      "mape_mean",
      "mape_std",
      "max_abs_error_mean",
      "max_abs_error_std",
      "chunk_state_messages_published_mean",
      "chunk_state_messages_published_std",
      "chunk_state_messages_per_emitted_result_mean",
      "chunk_state_messages_per_emitted_result_std",
      "shared_chunk_producers_created_mean",
      "shared_chunk_producers_created_std",
      "fallback_original_agent_rsps_started_mean",
      "fallback_original_agent_rsps_started_std",
      "reconstructed_superquery_results_mean",
      "reconstructed_superquery_results_std",
      "emitted_result_count_mean",
      "emitted_result_count_std",
      "reconstructed_result_count_mean",
      "reconstructed_result_count_std",
      "superquery_result_count_mean",
      "superquery_result_count_std",
      "chunk_states_consumed_per_result_mean_mean",
      "chunk_states_consumed_per_result_mean_std",
      "chunk_states_consumed_per_emitted_result_mean",
      "chunk_states_consumed_per_emitted_result_std",
      "expected_chunk_states_total_mean",
      "expected_chunk_states_total_std",
    ],
    aggregateRows,
  );

  writeCsv(
    path.join(args.outputDir, `${filePrefix}_profile_counters.csv`),
    [
      "experiment_name",
      "approach",
      "pattern",
      "iteration",
      "aggregation_function",
      "superquery_range_seconds",
      "superquery_step_seconds",
      "chunk_size_seconds",
      "replay_duration_seconds",
      "exact_final_reuse_enabled",
      "chunk_size_applies_to_approach",
      "shared_chunk_producers_created",
      "chunk_state_messages_published",
      "fallback_original_agent_rsps_started",
      "reconstructed_superquery_results",
      "rsp_engines_created",
      "mqtt_clients_created",
      "compatible_queries_detected",
      "original_agent_rsps_skipped",
      "original_agent_outputs_derived_from_chunks",
      "exact_final_result_reuse_hits",
      "chunk_consumers_registered",
      "chunk_groups_completed",
      "comparable_windows_emitted",
      "success",
      "is_valid",
      "process_cleanup_ok",
      "validity_reason",
    ],
    profileRows,
  );

  console.log(
    `Saved ${experimentDefinition.scenarioLabelPrefix} experiment CSVs to ${args.outputDir}`,
  );
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}

module.exports = {
  applyValidation,
  buildAggregateRows,
  buildLatencySummary,
  buildPerRunRows,
  computeErrorStats,
  getProfileCounters,
  loadRunRecords,
  parseArgs,
  readChunkStatesConsumed,
};
