#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const DEFAULT_PATTERNS = [
  "low_variability",
  "step_pattern",
  "spike_pattern",
  "low_freq_oscillation",
  "high_freq_oscillation",
];

const DEFAULT_APPROACHES = ["fetching", "chunked"];

function parseArgs(argv) {
  const args = {
    inputRoot: path.resolve(process.cwd(), "logs/custom-pattern-comparison"),
    outputDir: path.resolve(
      process.cwd(),
      "logs/custom-pattern-validation-analysis",
    ),
    patterns: [...DEFAULT_PATTERNS],
    approaches: [...DEFAULT_APPROACHES],
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
        args.patterns = next
          .split(",")
          .map((entry) => entry.trim())
          .filter(Boolean);
        index += 1;
        break;
      case "--approaches":
        if (!next) throw new Error("--approaches requires a value");
        args.approaches = next
          .split(",")
          .map((entry) => entry.trim())
          .filter(Boolean);
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

  return args;
}

function printHelp() {
  console.log(`Usage: node scripts/benchmark/extract-custom-pattern-validation.js [options]

Options:
  --input-root <path>   Timestamped custom-pattern log root (default: logs/custom-pattern-comparison)
  --output-dir <path>   Directory for CSV outputs (default: logs/custom-pattern-validation-analysis)
  --patterns <list>     Comma-separated pattern list
  --approaches <list>   Comma-separated approach list
  --iterations <list>   Comma-separated iteration list
  --help                Show this help
`);
}

function resolvePath(inputPath) {
  return path.isAbsolute(inputPath)
    ? inputPath
    : path.resolve(process.cwd(), inputPath);
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
  const rows = [];
  for (const line of lines.slice(1)) {
    const values = line.split(",");
    if (values.length < headers.length) {
      continue;
    }

    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index];
    });
    rows.push(row);
  }

  return rows;
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

function listIterationDirs(inputRoot, approach, pattern) {
  const approachRoot = path.join(inputRoot, approach, pattern);
  if (!exists(approachRoot)) {
    return [];
  }

  return fs
    .readdirSync(approachRoot)
    .filter((entry) => /^iteration\d+$/.test(entry))
    .map((entry) => ({
      iteration: Number.parseInt(entry.replace("iteration", ""), 10),
      dir: path.join(approachRoot, entry),
    }))
    .filter(({ iteration, dir }) => Number.isFinite(iteration) && exists(dir))
    .sort((left, right) => left.iteration - right.iteration);
}

function resolveAttemptDir(iterationDir) {
  const attemptDir = path.join(iterationDir, "attempt1");
  if (exists(path.join(attemptDir, "resource_summary.json"))) {
    return attemptDir;
  }
  if (exists(path.join(iterationDir, "resource_summary.json"))) {
    return iterationDir;
  }

  const attempts = fs
    .readdirSync(iterationDir)
    .filter((entry) => /^attempt\d+$/.test(entry))
    .map((entry) => path.join(iterationDir, entry))
    .filter((dir) => exists(path.join(dir, "resource_summary.json")));

  return attempts.length > 0 ? attempts[0] : null;
}

function getRunDir(inputRoot, approach, pattern, iteration) {
  const baseDir = path.join(inputRoot, approach, pattern, `iteration${iteration}`);
  if (!exists(baseDir)) {
    return null;
  }

  return resolveAttemptDir(baseDir) || baseDir;
}

function readRunArtifacts(runDir, approach) {
  const resourceSummary = readJson(path.join(runDir, "resource_summary.json"));
  const profileAggregate = readJson(path.join(runDir, "hive_profile_summary.aggregate.json"));
  const debugSummary = approach === "chunked"
    ? readJson(path.join(runDir, "chunked_debug_summary.json"))
    : null;

  const resultCsvName = `${approach}_results.csv`;
  const resultRows = readCsvRows(path.join(runDir, resultCsvName));

  return {
    resourceSummary,
    profileAggregate,
    debugSummary,
    resultRows,
  };
}

function buildResultMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const windowStart = parseNumber(row.window_start);
    const windowEnd = parseNumber(row.window_end);
    const windowNumber = parseNumber(row.window_number);
    const resultValue = parseNumber(row.result_value);

    if (!Number.isFinite(resultValue)) {
      continue;
    }

    const key = Number.isFinite(windowStart) && Number.isFinite(windowEnd)
      ? `${windowStart}:${windowEnd}`
      : `window-number:${windowNumber}`;
    map.set(key, resultValue);
  }
  return map;
}

function compareAgainstFetching(fetchingRows, chunkedRows) {
  const baselineMap = buildResultMap(fetchingRows);
  const candidateMap = buildResultMap(chunkedRows);
  const keys = [...baselineMap.keys()].filter((key) => candidateMap.has(key));

  if (keys.length === 0) {
    return null;
  }

  let sumPercentageError = 0;
  let applicableCount = 0;

  for (const key of keys) {
    const baseline = baselineMap.get(key);
    const candidate = candidateMap.get(key);
    const absoluteError = Math.abs(candidate - baseline);

    if (Math.abs(baseline) <= Number.EPSILON) {
      if (absoluteError > Number.EPSILON) {
        sumPercentageError += 100;
        applicableCount += 1;
      }
      continue;
    }

    sumPercentageError += (absoluteError / Math.abs(baseline)) * 100;
    applicableCount += 1;
  }

  if (applicableCount === 0) {
    return 0;
  }

  return sumPercentageError / applicableCount;
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

function sumProfileCounters(profileAggregate, counterName) {
  if (!profileAggregate) {
    return null;
  }

  if (profileAggregate.summedCounters && Number.isFinite(Number(profileAggregate.summedCounters[counterName]))) {
    return Number(profileAggregate.summedCounters[counterName]);
  }

  if (profileAggregate.processProfiles) {
    return profileAggregate.processProfiles.reduce(
      (sum, profile) => sum + (Number(profile?.[counterName]) || 0),
      0,
    );
  }

  return null;
}

function buildRunRecord({ pattern, approach, iteration, runDir, fetchingRows }) {
  const artifacts = readRunArtifacts(runDir, approach);
  if (!artifacts.resourceSummary || !artifacts.profileAggregate || !artifacts.resultRows) {
    return null;
  }

  const errorVsFetching = approach === "fetching"
    ? 0
    : compareAgainstFetching(fetchingRows, artifacts.resultRows);

  const profileCounters = artifacts.profileAggregate.summedCounters || {};
  if (approach === "chunked" && artifacts.debugSummary) {
    const debugIssues = [];
    const debug = artifacts.debugSummary;
    if (debug.duplicateChunkCount !== 0) {
      debugIssues.push(`duplicateChunkCount=${debug.duplicateChunkCount}`);
    }
    if (debug.missingChunkGroups !== 0) {
      debugIssues.push(`missingChunkGroups=${debug.missingChunkGroups}`);
    }
    if (debug.comparableWindowEmissionCount !== 3) {
      debugIssues.push(`comparableWindowEmissionCount=${debug.comparableWindowEmissionCount}`);
    }
    if (debug.reconstructedSuperqueryResultCount !== 3) {
      debugIssues.push(`reconstructedSuperqueryResultCount=${debug.reconstructedSuperqueryResultCount}`);
    }

    const counterIssues = [];
    if (Number(profileCounters.fallback_original_agent_rsps_started || 0) !== 0) {
      counterIssues.push(
        `fallback_original_agent_rsps_started=${profileCounters.fallback_original_agent_rsps_started}`,
      );
    }
    if (Number(profileCounters.original_agent_rsps_skipped || 0) !== 2) {
      counterIssues.push(
        `original_agent_rsps_skipped=${profileCounters.original_agent_rsps_skipped}`,
      );
    }

    if (debugIssues.length > 0 || counterIssues.length > 0) {
      console.warn(
        [
          `[chunked-validation] ${pattern} iteration ${iteration}`,
          debugIssues.length > 0 ? `debug: ${debugIssues.join(", ")}` : null,
          counterIssues.length > 0 ? `counters: ${counterIssues.join(", ")}` : null,
        ]
          .filter(Boolean)
          .join(" | "),
      );
    }
  }

  return {
    pattern,
    approach,
    iteration,
    emitted_results: Number(profileCounters.emitted_results ?? 0),
    error_vs_fetching: errorVsFetching,
    cpu_seconds: Number(artifacts.resourceSummary.cpuSeconds ?? 0),
    peak_rss_mb: Number(artifacts.resourceSummary.peakRssMb ?? 0),
    mean_rss_mb: Number(artifacts.resourceSummary.meanRssMb ?? 0),
    peak_cpu_pct: Number(artifacts.resourceSummary.peakCpuPct ?? 0),
    wall_time_s: Number(artifacts.resourceSummary.wallTimeSec ?? 0),
    peak_process_count: Number(artifacts.resourceSummary.peakProcessCount ?? 0),
    compatible_queries_detected: Number(profileCounters.compatible_queries_detected ?? 0),
    original_agent_rsps_skipped: Number(profileCounters.original_agent_rsps_skipped ?? 0),
    fallback_original_agent_rsps_started: Number(profileCounters.fallback_original_agent_rsps_started ?? 0),
    original_agent_outputs_derived_from_chunks: Number(
      profileCounters.original_agent_outputs_derived_from_chunks ?? 0,
    ),
    shared_chunk_producers_created: Number(profileCounters.shared_chunk_producers_created ?? 0),
    chunk_state_messages_published: Number(profileCounters.chunk_state_messages_published ?? 0),
    rsp_engines_created: Number(profileCounters.rsp_engines_created ?? 0),
    mqtt_clients_created: Number(profileCounters.mqtt_clients_created ?? 0),
    debugSummary: artifacts.debugSummary,
  };
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => formatCsvValue(row[header])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
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
        pattern,
        approach,
        mean_error: mean(rows.map((row) => row.error_vs_fetching)),
        cpu_seconds_mean: mean(rows.map((row) => row.cpu_seconds)),
        cpu_seconds_std: std(rows.map((row) => row.cpu_seconds)),
        peak_rss_mb_mean: mean(rows.map((row) => row.peak_rss_mb)),
        peak_rss_mb_std: std(rows.map((row) => row.peak_rss_mb)),
        wall_time_s_mean: mean(rows.map((row) => row.wall_time_s)),
        wall_time_s_std: std(rows.map((row) => row.wall_time_s)),
      };
    })
    .sort((left, right) => {
      const patternCompare = DEFAULT_PATTERNS.indexOf(left.pattern) - DEFAULT_PATTERNS.indexOf(right.pattern);
      if (patternCompare !== 0) {
        return patternCompare;
      }
      return DEFAULT_APPROACHES.indexOf(left.approach) - DEFAULT_APPROACHES.indexOf(right.approach);
    });
}

function buildChunkedProfileRows(runRows) {
  return runRows
    .filter((row) => row.approach === "chunked")
    .map((row) => ({
      pattern: row.pattern,
      iteration: row.iteration,
      compatible_queries_detected: row.compatible_queries_detected,
      original_agent_rsps_skipped: row.original_agent_rsps_skipped,
      fallback_original_agent_rsps_started: row.fallback_original_agent_rsps_started,
      original_agent_outputs_derived_from_chunks: row.original_agent_outputs_derived_from_chunks,
      shared_chunk_producers_created: row.shared_chunk_producers_created,
      chunk_state_messages_published: row.chunk_state_messages_published,
      rsp_engines_created: row.rsp_engines_created,
      mqtt_clients_created: row.mqtt_clients_created,
    }))
    .sort((left, right) => {
      const patternCompare = DEFAULT_PATTERNS.indexOf(left.pattern) - DEFAULT_PATTERNS.indexOf(right.pattern);
      if (patternCompare !== 0) {
        return patternCompare;
      }
      return left.iteration - right.iteration;
    });
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  ensureDir(args.outputDir);

  const runRows = [];

  for (const pattern of args.patterns) {
    const fetchingAttempts = listIterationDirs(args.inputRoot, "fetching", pattern);
    const fetchingRowsByIteration = new Map();

    for (const { iteration, dir } of fetchingAttempts) {
      if (!args.iterations.includes(iteration)) {
        continue;
      }
      const runDir = getRunDir(args.inputRoot, "fetching", pattern, iteration) || dir;
      const row = buildRunRecord({
        pattern,
        approach: "fetching",
        iteration,
        runDir,
        fetchingRows: [],
      });
      if (row) {
        runRows.push(row);
        fetchingRowsByIteration.set(iteration, row);
      }
    }

    for (const approach of args.approaches) {
      if (approach === "fetching") {
        continue;
      }

      for (const { iteration } of listIterationDirs(args.inputRoot, approach, pattern)) {
        if (!args.iterations.includes(iteration)) {
          continue;
        }

        const runDir = getRunDir(args.inputRoot, approach, pattern, iteration);
        if (!runDir) {
          continue;
        }

        const fetchingRunDir = getRunDir(args.inputRoot, "fetching", pattern, iteration);
        const fetchingArtifacts = fetchingRunDir
          ? readRunArtifacts(fetchingRunDir, "fetching")
          : null;
        const fetchingRows = fetchingArtifacts?.resultRows || [];

        const row = buildRunRecord({
          pattern,
          approach,
          iteration,
          runDir,
          fetchingRows,
        });
        if (row) {
          runRows.push(row);
        }
      }
    }
  }

  runRows.sort((left, right) => {
    const patternCompare = DEFAULT_PATTERNS.indexOf(left.pattern) - DEFAULT_PATTERNS.indexOf(right.pattern);
    if (patternCompare !== 0) {
      return patternCompare;
    }
    const approachCompare = DEFAULT_APPROACHES.indexOf(left.approach) - DEFAULT_APPROACHES.indexOf(right.approach);
    if (approachCompare !== 0) {
      return approachCompare;
    }
    return left.iteration - right.iteration;
  });

  const perRunCsvPath = path.join(args.outputDir, "custom_pattern_validation_per_run.csv");
  const aggregateCsvPath = path.join(args.outputDir, "custom_pattern_validation_aggregate.csv");
  const chunkedCountersCsvPath = path.join(args.outputDir, "custom_pattern_validation_chunked_profile_counters.csv");

  writeCsv(perRunCsvPath, [
    "pattern",
    "approach",
    "iteration",
    "emitted_results",
    "error_vs_fetching",
    "cpu_seconds",
    "peak_rss_mb",
    "mean_rss_mb",
    "peak_cpu_pct",
    "wall_time_s",
    "peak_process_count",
  ], runRows);

  writeCsv(aggregateCsvPath, [
    "pattern",
    "approach",
    "mean_error",
    "cpu_seconds_mean",
    "cpu_seconds_std",
    "peak_rss_mb_mean",
    "peak_rss_mb_std",
    "wall_time_s_mean",
    "wall_time_s_std",
  ], buildAggregateRows(runRows));

  writeCsv(chunkedCountersCsvPath, [
    "pattern",
    "iteration",
    "compatible_queries_detected",
    "original_agent_rsps_skipped",
    "fallback_original_agent_rsps_started",
    "original_agent_outputs_derived_from_chunks",
    "shared_chunk_producers_created",
    "chunk_state_messages_published",
    "rsp_engines_created",
    "mqtt_clients_created",
  ], buildChunkedProfileRows(runRows));

  console.log(`Wrote: ${perRunCsvPath}`);
  console.log(`Wrote: ${aggregateCsvPath}`);
  console.log(`Wrote: ${chunkedCountersCsvPath}`);
}

main();
