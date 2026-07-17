#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const {
  compareResults,
  compareAggregateResultEquivalence,
} = require("../../analysis/accuracy/accuracy-comparison-custom-patterns.js");

const DEFAULT_PATTERNS = [
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
const DEFAULT_APPROACHES = ["fetching", "approximation", "chunked"];

const FILES = {
  fetching: {
    results: "fetching_results.csv",
    latency: "fetching_latency_log.csv",
    diagnostics: "fetching_window_diagnostics.csv",
  },
  approximation: {
    results: "approximation_results.csv",
    latency: "approximation_latency_log.csv",
    diagnostics: null,
  },
  chunked: {
    results: "chunked_results.csv",
    latency: "chunked_latency_log.csv",
    diagnostics: "chunked_window_diagnostics.csv",
  },
};

const EXECUTION_SUMMARY_FILE = "custom_pattern_comparison_summary.json";

function parseArgs(argv) {
  const args = {
    inputRoot: path.resolve(process.cwd(), "logs/custom-pattern-comparison"),
    outputDir: null,
    patterns: [...DEFAULT_PATTERNS],
    approaches: [...DEFAULT_APPROACHES],
    iterations: [1],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    switch (arg) {
      case "--input-root":
        args.inputRoot = resolvePath(requireValue("--input-root", next));
        index += 1;
        break;
      case "--output-dir":
        args.outputDir = resolvePath(requireValue("--output-dir", next));
        index += 1;
        break;
      case "--patterns":
        args.patterns = parseCsvList(requireValue("--patterns", next));
        index += 1;
        break;
      case "--approaches":
        args.approaches = parseCsvList(requireValue("--approaches", next));
        index += 1;
        break;
      case "--iterations":
        args.iterations = parseCsvList(requireValue("--iterations", next))
          .map((value) => Number.parseInt(value, 10))
          .filter((value) => Number.isFinite(value) && value > 0);
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

  if (!args.outputDir) {
    args.outputDir = path.join(args.inputRoot, "analysis", "first-window-smoke");
  }

  return args;
}

function printHelp() {
  console.log(`Usage: node scripts/benchmark/validate-custom-pattern-first-window-smoke.js [options]

Options:
  --input-root <path>   Benchmark result root (default: logs/custom-pattern-comparison)
  --output-dir <path>   Output directory for summary artifacts
  --patterns <list>     Comma-separated pattern list
  --approaches <list>   Comma-separated approach list
  --iterations <list>   Comma-separated iteration list (default: 1)
  --help                Show this help
`);
}

function requireValue(flag, value) {
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  return value;
}

function resolvePath(inputPath) {
  return path.isAbsolute(inputPath)
    ? inputPath
    : path.resolve(process.cwd(), inputPath);
}

function parseCsvList(value) {
  return value.split(",").map((entry) => entry.trim()).filter(Boolean);
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
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function parseCsv(content) {
  const lines = content.trim().split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    return [];
  }
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""]));
  });
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === "\"") {
      if (inQuotes && line[index + 1] === "\"") {
        current += "\"";
        index += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current);
  return values;
}

function readCsvRows(filePath) {
  if (!exists(filePath)) {
    return [];
  }
  return parseCsv(fs.readFileSync(filePath, "utf8"));
}

function parseNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function findConfiguredTimeoutMs(executionSummary, approach, pattern, iteration) {
  const results = Array.isArray(executionSummary?.results) ? executionSummary.results : [];
  const match = results.find((result) =>
    result?.approach === approach
      && result?.pattern === pattern
      && Number(result?.iteration) === iteration,
  );
  return parseNumber(match?.configuredTimeoutMs);
}

function resolveIterationDir(inputRoot, approach, pattern, iteration) {
  const baseDir = path.join(inputRoot, approach, pattern, `iteration${iteration}`);
  if (!exists(baseDir)) {
    return null;
  }

  if (exists(path.join(baseDir, "resource_summary.json"))) {
    return baseDir;
  }

  const attempts = fs.readdirSync(baseDir)
    .filter((entry) => /^attempt\d+$/.test(entry))
    .map((entry) => path.join(baseDir, entry))
    .filter((entry) => exists(path.join(entry, "resource_summary.json")))
    .sort();

  return attempts[attempts.length - 1] || baseDir;
}

function readFirstRawInputPublishTime(runDir) {
  const ndjsonPath = path.join(runDir, "mqtt_traffic.ndjson");
  if (!exists(ndjsonPath)) {
    return null;
  }

  const lines = fs.readFileSync(ndjsonPath, "utf8").trim().split(/\r?\n/).filter(Boolean);
  for (const line of lines) {
    try {
      const record = JSON.parse(line);
      if (record.messageType === "raw_input_stream" && Number.isFinite(Number(record.timestamp))) {
        return Number(record.timestamp);
      }
    } catch {
    }
  }

  return null;
}

function readChunkCoverage(runDir, windowNumber) {
  const diagnosticsPath = path.join(runDir, FILES.chunked.diagnostics);
  const rows = readCsvRows(diagnosticsPath);
  const row = rows.find((entry) => Number.parseInt(entry.external_window_number, 10) === windowNumber);
  if (!row) {
    return { complete: false, chunkCount: 0 };
  }

  try {
    const chunks = JSON.parse(row.internal_chunks_json || "[]");
    const complete = chunks.length > 0 && chunks.every((chunk) => chunk.coverageComplete === true);
    return {
      complete,
      chunkCount: chunks.length,
    };
  } catch {
    return { complete: false, chunkCount: 0 };
  }
}

function readFetchingCompleteness(runDir, windowNumber) {
  const diagnosticsPath = path.join(runDir, FILES.fetching.diagnostics);
  const rows = readCsvRows(diagnosticsPath);
  const row = rows.find((entry) => Number.parseInt(entry.window_number, 10) === windowNumber);
  if (!row) {
    return { complete: false, isPartialWindow: true, reason: "missing_fetching_window_diagnostics" };
  }

  const complete = row.completeness_status === "complete"
    && row.accepted_or_suppressed === "accepted";

  return {
    complete,
    isPartialWindow: !complete,
    reason: row.reason || null,
  };
}

function readApproximationCompleteness(runDir, windowNumber) {
  const latencyPath = path.join(runDir, FILES.approximation.latency);
  const rows = readCsvRows(latencyPath);
  const row = rows.find((entry) => Number.parseInt(entry.window_number, 10) === windowNumber);
  if (!row) {
    return { complete: false, isPartialWindow: true, reason: "missing_approximation_latency_row" };
  }

  const complete = row.approximation_status === "completed_window_approximation";
  return {
    complete,
    isPartialWindow: !complete,
    reason: row.approximation_status || null,
  };
}

function readApproachRow(runDir, approach, options = {}) {
  const resultsRows = readCsvRows(path.join(runDir, FILES[approach].results));
  const latencyRows = readCsvRows(path.join(runDir, FILES[approach].latency));
  const resourceSummary = readJson(path.join(runDir, "resource_summary.json")) || {};
  const capSummary = readJson(path.join(runDir, "benchmark_window_cap_summary.json"));
  const attemptMetadata = readJson(path.join(runDir, "attempt_metadata.json")) || {};
  const firstRawInputPublishedAt = readFirstRawInputPublishTime(runDir);

  const resultRow = resultsRows[0] || null;
  const windowNumber = Number.parseInt(resultRow?.window_number || latencyRows[0]?.window_number || "", 10);
  const latencyRow = latencyRows.find((row) => Number.parseInt(row.window_number, 10) === windowNumber) || latencyRows[0] || null;
  const benchmarkEventTimeAnchor = parseNumber(attemptMetadata.benchmark_event_time_anchor);
  const outputWindowRange = parseNumber(attemptMetadata.output_window_range);
  const outputWindowStep = parseNumber(attemptMetadata.output_window_step);
  const resultEmittedAt = parseNumber(latencyRow?.result_emitted_at) ?? parseNumber(resultRow?.timestamp);
  const eventWindowEnd = parseNumber(latencyRow?.window_data_close_time)
    ?? parseNumber(latencyRow?.window_end)
    ?? parseNumber(resultRow?.window_end);
  const configuredTimeoutMs =
    parseNumber(attemptMetadata.configuredTimeoutMs)
    ?? findConfiguredTimeoutMs(
      options.executionSummary,
      options.approach ?? approach,
      options.pattern,
      options.iteration,
    );

  let semanticWindowCloseAt = null;
  if (
    Number.isFinite(firstRawInputPublishedAt) &&
    Number.isFinite(benchmarkEventTimeAnchor) &&
    Number.isFinite(eventWindowEnd)
  ) {
    semanticWindowCloseAt = firstRawInputPublishedAt + (eventWindowEnd - benchmarkEventTimeAnchor);
  } else if (
    Number.isFinite(firstRawInputPublishedAt) &&
    Number.isFinite(outputWindowRange) &&
    Number.isFinite(outputWindowStep) &&
    Number.isFinite(windowNumber)
  ) {
    semanticWindowCloseAt = firstRawInputPublishedAt + outputWindowRange + ((windowNumber - 1) * outputWindowStep);
  }

  let semanticWindowCloseToResultMs = null;
  let latencyMetricSource = null;

  if (approach === "fetching") {
    const directLatencyFromWindowCloseMs = parseNumber(latencyRow?.latency_from_window_close_ms);
    const wallClockWindowDataCloseTime = parseNumber(latencyRow?.window_data_close_time);
    if (Number.isFinite(directLatencyFromWindowCloseMs)) {
      semanticWindowCloseToResultMs = directLatencyFromWindowCloseMs;
      latencyMetricSource = "fetching_latency_log";
    } else if (Number.isFinite(resultEmittedAt) && Number.isFinite(wallClockWindowDataCloseTime)) {
      semanticWindowCloseToResultMs = resultEmittedAt - wallClockWindowDataCloseTime;
      latencyMetricSource = "fetching_latency_log";
    }
  } else if (Number.isFinite(semanticWindowCloseAt) && Number.isFinite(resultEmittedAt)) {
    semanticWindowCloseToResultMs = resultEmittedAt - semanticWindowCloseAt;
    latencyMetricSource = "semantic_window_metadata";
  }

  let completeness;
  if (approach === "fetching") {
    completeness = readFetchingCompleteness(runDir, windowNumber);
  } else if (approach === "approximation") {
    completeness = readApproximationCompleteness(runDir, windowNumber);
  } else {
    const coverage = readChunkCoverage(runDir, windowNumber);
    completeness = {
      complete: coverage.complete,
      isPartialWindow: !coverage.complete,
      reason: coverage.complete ? null : "chunk_coverage_incomplete",
      chunkCount: coverage.chunkCount,
    };
  }

  return {
    runDir,
    resultRow,
    latencyRow,
    resourceSummary,
    capSummary,
    attemptMetadata,
    configuredTimeoutMs,
    firstRawInputPublishedAt,
    semanticWindowCloseAt,
    semanticWindowCloseToResultMs,
    latencyMetricSource,
    windowNumber: Number.isFinite(windowNumber) ? windowNumber : null,
    windowStart: parseNumber(resultRow?.window_start) ?? parseNumber(latencyRow?.window_start),
    windowEnd: parseNumber(resultRow?.window_end) ?? parseNumber(latencyRow?.window_end),
    resultValue: parseNumber(resultRow?.result_value),
    resultEmittedAt,
    completeWindow:
      capSummary?.stoppedAfterTargetWindows === true
      && Number(capSummary?.emittedFinalWindowCount) >= 1
      && completeness.complete === true,
    completeness,
    averageCpuPct: parseNumber(resourceSummary.meanCpuPct),
    peakRssMb: parseNumber(resourceSummary.peakRssMb),
  };
}

function validatePattern(inputRoot, pattern, iteration) {
  const perApproach = {};
  const warnings = [];
  const failures = [];
  const executionSummary = readJson(path.join(inputRoot, EXECUTION_SUMMARY_FILE));

  for (const approach of DEFAULT_APPROACHES) {
    const runDir = resolveIterationDir(inputRoot, approach, pattern, iteration);
    if (!runDir) {
      failures.push(`${approach}: missing iteration directory`);
      continue;
    }
    perApproach[approach] = readApproachRow(runDir, approach, {
      approach,
      pattern,
      iteration,
      executionSummary,
    });
  }

  const fetching = perApproach.fetching;
  const approximation = perApproach.approximation;
  const chunked = perApproach.chunked;

  const sharedWindowNumber = fetching?.windowNumber;
  const sharedWindowStart = fetching?.windowStart;
  const sharedWindowEnd = fetching?.windowEnd;

  for (const approach of DEFAULT_APPROACHES) {
    const row = perApproach[approach];
    if (!row) {
      continue;
    }

    if (!row.completeWindow) {
      failures.push(`${approach}: first result is not confirmed complete and comparable`);
    }
    if (row.completeness?.isPartialWindow) {
      failures.push(`${approach}: partial window evidence present`);
    }
    if (!Number.isFinite(row.resultValue)) {
      failures.push(`${approach}: missing result value`);
    }
    if (!Number.isFinite(row.averageCpuPct)) {
      failures.push(`${approach}: average CPU missing`);
    }
    if (!Number.isFinite(row.peakRssMb)) {
      failures.push(`${approach}: peak RSS missing`);
    }
    if (!Number.isFinite(row.semanticWindowCloseToResultMs)) {
      failures.push(`${approach}: semantic window-close latency missing or invalid`);
    } else if (row.semanticWindowCloseToResultMs < 0) {
      failures.push(`${approach}: semantic window-close latency is negative`);
    } else if (!Number.isFinite(row.configuredTimeoutMs)) {
      failures.push(`${approach}: configured pattern timeout missing`);
    } else if (row.semanticWindowCloseToResultMs > row.configuredTimeoutMs) {
      failures.push(
        `${approach}: semantic window-close latency exceeds configured timeout (${row.semanticWindowCloseToResultMs} > ${row.configuredTimeoutMs})`,
      );
    }
    if (!row.latencyMetricSource) {
      failures.push(`${approach}: latency metric source missing`);
    }
    if (row.capSummary?.emittedFinalWindowCount !== 1) {
      warnings.push(`${approach}: emittedFinalWindowCount=${row.capSummary?.emittedFinalWindowCount ?? "missing"}`);
    }
  }

  if (
    Number.isFinite(sharedWindowNumber) &&
    (approximation?.windowNumber !== sharedWindowNumber || chunked?.windowNumber !== sharedWindowNumber)
  ) {
    failures.push("window_number mismatch across approaches");
  }
  if (
    Number.isFinite(sharedWindowStart) &&
    Number.isFinite(sharedWindowEnd) &&
    (
      approximation?.windowStart !== sharedWindowStart ||
      approximation?.windowEnd !== sharedWindowEnd ||
      chunked?.windowStart !== sharedWindowStart ||
      chunked?.windowEnd !== sharedWindowEnd
    )
  ) {
    failures.push("window bounds mismatch across approaches");
  }

  let approximationAccuracy = null;
  let chunkedAccuracy = null;

  if (fetching && approximation) {
    approximationAccuracy = compareResults(
      [{
        windowNumber: fetching.windowNumber,
        windowStart: fetching.windowStart,
        windowEnd: fetching.windowEnd,
        resultValue: fetching.resultValue,
      }],
      [{
        windowNumber: approximation.windowNumber,
        windowStart: approximation.windowStart,
        windowEnd: approximation.windowEnd,
        resultValue: approximation.resultValue,
      }],
      { trimWindowStart: 1, trimWindowEnd: 1 },
    );
  }

  if (fetching && chunked) {
    chunkedAccuracy = compareResults(
      [{
        windowNumber: fetching.windowNumber,
        windowStart: fetching.windowStart,
        windowEnd: fetching.windowEnd,
        resultValue: fetching.resultValue,
      }],
      [{
        windowNumber: chunked.windowNumber,
        windowStart: chunked.windowStart,
        windowEnd: chunked.windowEnd,
        resultValue: chunked.resultValue,
      }],
      { trimWindowStart: 1, trimWindowEnd: 1 },
    );
  }

  if (fetching) {
    const fetchingComparison = compareAggregateResultEquivalence(fetching.resultValue, fetching.resultValue);
    perApproach.fetching.referenceResult = fetchingComparison.referenceResult;
    perApproach.fetching.producedResult = fetchingComparison.producedResult;
    perApproach.fetching.rawAbsoluteError = fetchingComparison.rawAbsoluteError;
    perApproach.fetching.absoluteError = fetchingComparison.rawAbsoluteError;
    perApproach.fetching.mae = fetchingComparison.rawAbsoluteError;
    perApproach.fetching.exactAgreement = fetchingComparison.exactAgreement;
    perApproach.fetching.comparisonTolerance = fetchingComparison.comparisonTolerance;
    perApproach.fetching.comparisonMethod = fetchingComparison.comparisonMethod;
  }
  if (approximation) {
    const approximationComparison = approximationAccuracy?.matchedComparisons?.[0]
      ?? compareAggregateResultEquivalence(approximation.resultValue, fetching?.resultValue);
    perApproach.approximation.referenceResult = approximationComparison.referenceResult;
    perApproach.approximation.producedResult = approximationComparison.producedResult;
    perApproach.approximation.rawAbsoluteError = approximationComparison.rawAbsoluteError;
    perApproach.approximation.absoluteError = approximationComparison.rawAbsoluteError;
    perApproach.approximation.mae = approximationAccuracy?.mae ?? approximationComparison.rawAbsoluteError;
    perApproach.approximation.exactAgreement = approximationComparison.exactAgreement;
    perApproach.approximation.comparisonTolerance = approximationComparison.comparisonTolerance;
    perApproach.approximation.comparisonMethod = approximationComparison.comparisonMethod;
  }
  if (chunked) {
    const chunkedComparison = chunkedAccuracy?.matchedComparisons?.[0]
      ?? compareAggregateResultEquivalence(chunked.resultValue, fetching?.resultValue);
    perApproach.chunked.referenceResult = chunkedComparison.referenceResult;
    perApproach.chunked.producedResult = chunkedComparison.producedResult;
    perApproach.chunked.rawAbsoluteError = chunkedComparison.rawAbsoluteError;
    perApproach.chunked.absoluteError = chunkedComparison.rawAbsoluteError;
    perApproach.chunked.mae = chunkedAccuracy?.mae ?? chunkedComparison.rawAbsoluteError;
    perApproach.chunked.exactAgreement = chunkedComparison.exactAgreement;
    perApproach.chunked.comparisonTolerance = chunkedComparison.comparisonTolerance;
    perApproach.chunked.comparisonMethod = chunkedComparison.comparisonMethod;
    if (!chunkedComparison.exactAgreement) {
      failures.push("chunked result is not exactly equal to fetching");
    }
  }

  return {
    pattern,
    iteration,
    status: failures.length === 0 ? "pass" : "fail",
    warnings,
    failures,
    perApproach,
  };
}

function buildTableRows(patternResults, approaches) {
  const rows = [];
  for (const patternResult of patternResults) {
    for (const approach of approaches) {
      const row = patternResult.perApproach[approach];
      rows.push({
        Pattern: patternResult.pattern,
        Approach: approach,
        "Complete window": row?.completeWindow === true ? "yes" : "no",
        "Window-close latency (ms)": formatNumber(row?.semanticWindowCloseToResultMs, 3),
        "Latency source": row?.latencyMetricSource ?? "",
        "Average CPU (%)": formatNumber(row?.averageCpuPct, 3),
        "Peak RSS (MiB)": formatNumber(row?.peakRssMb, 3),
        Result: formatNumber(row?.resultValue, 12),
        "Exact vs Fetching": approach === "fetching"
          ? "1/1"
          : row?.exactAgreement === true
            ? "1/1"
            : "0/1",
        "Absolute error": formatNumber(row?.absoluteError, 12),
      });
    }
  }
  return rows;
}

function formatNumber(value, digits = 6) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : "";
}

function writeCsv(filePath, rows) {
  const headers = [
    "Pattern",
    "Approach",
    "Complete window",
    "Window-close latency (ms)",
    "Latency source",
    "Average CPU (%)",
    "Peak RSS (MiB)",
    "Result",
    "Exact vs Fetching",
    "Absolute error",
  ];
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => row[header]).join(",")),
  ];
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function renderMarkdown(summary) {
  const lines = [
    "# Custom-Pattern First-Window Smoke Validation",
    "",
    `Input root: \`${summary.inputRoot}\``,
    `Patterns: \`${summary.patterns.join(",")}\``,
    `Approaches: \`${summary.approaches.join(",")}\``,
    "",
    "| Pattern | Approach | Complete window | Window-close latency (ms) | Average CPU (%) | Peak RSS (MiB) | Result | Exact vs Fetching | Absolute error |",
    "| ------- | -------- | --------------: | ------------------------: | -------------- | --------------: | -------------: | -----: | ----------------: | -------------: |",
  ];

  for (const row of summary.tableRows) {
    lines.push(
      `| ${row.Pattern} | ${row.Approach} | ${row["Complete window"]} | ${row["Window-close latency (ms)"]} | ${row["Latency source"]} | ${row["Average CPU (%)"]} | ${row["Peak RSS (MiB)"]} | ${row.Result} | ${row["Exact vs Fetching"]} | ${row["Absolute error"]} |`,
    );
  }

  lines.push("");
  lines.push(`Overall status: **${summary.status.toUpperCase()}**`);

  if (summary.warnings.length > 0) {
    lines.push("");
    lines.push("Warnings:");
    for (const warning of summary.warnings) {
      lines.push(`- ${warning}`);
    }
  }

  if (summary.failures.length > 0) {
    lines.push("");
    lines.push("Failures:");
    for (const failure of summary.failures) {
      lines.push(`- ${failure}`);
    }
  }

  return `${lines.join("\n")}\n`;
}

function summarizeSmokeValidation(args) {
  const patternResults = args.patterns.map((pattern) =>
    validatePattern(args.inputRoot, pattern, args.iterations[0] || 1),
  );
  const failures = patternResults.flatMap((result) =>
    result.failures.map((failure) => `${result.pattern}: ${failure}`),
  );
  const warnings = patternResults.flatMap((result) =>
    result.warnings.map((warning) => `${result.pattern}: ${warning}`),
  );

  const tableRows = buildTableRows(patternResults, args.approaches);

  return {
    timestamp: new Date().toISOString(),
    inputRoot: args.inputRoot,
    outputDir: args.outputDir,
    patterns: args.patterns,
    approaches: args.approaches,
    iterations: args.iterations,
    status: failures.length === 0 ? "pass" : "fail",
    failures,
    warnings,
    patternResults,
    tableRows,
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  ensureDir(args.outputDir);

  const summary = summarizeSmokeValidation(args);
  const jsonPath = path.join(args.outputDir, "summary.json");
  const csvPath = path.join(args.outputDir, "summary.csv");
  const mdPath = path.join(args.outputDir, "summary.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`);
  writeCsv(csvPath, summary.tableRows);
  fs.writeFileSync(mdPath, renderMarkdown(summary));

  console.log(`Smoke validation ${summary.status === "pass" ? "passed" : "failed"}.`);
  console.log(`Summary JSON: ${jsonPath}`);
  console.log(`Summary CSV: ${csvPath}`);
  console.log(`Summary MD: ${mdPath}`);

  if (summary.status !== "pass") {
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  parseArgs,
  readApproachRow,
  readFirstRawInputPublishTime,
  summarizeSmokeValidation,
  validatePattern,
};
