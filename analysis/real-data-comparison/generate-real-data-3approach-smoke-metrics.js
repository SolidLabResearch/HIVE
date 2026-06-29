#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const { parse } = require("csv-parse/sync");

const {
  APPROACHES,
  normalizeLatencyRows,
  summarizeProcessTreeMetrics,
} = require("../../experiments/real-data-comparison/run-real-data-4-approaches.js");
const { compareResults } = require("../accuracy/accuracy-comparison-custom-patterns.js");

const DEFAULT_APPROACHES = ["fetching", "approximation", "chunked"];
const DEFAULT_ITERATIONS = 3;
const DEFAULT_LOGS_DIR = path.resolve(
  process.cwd(),
  "experiments/real-data-comparison/logs",
);
const DEFAULT_REPORT_PATH = path.resolve(
  process.cwd(),
  "analysis/real-data-comparison/real_data_3approach_3_iteration_smoke_metrics.md",
);
const TARGET_WINDOWS = [1, 2, 3, 4, 5];
const EXCLUDED_APPROACHES = ["naive_distributed"];

function parseArgs(argv) {
  const args = {
    approaches: DEFAULT_APPROACHES,
    iterations: DEFAULT_ITERATIONS,
    logsDir: DEFAULT_LOGS_DIR,
    reportPath: DEFAULT_REPORT_PATH,
    runStartMs: null,
    runLogPath: null,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--approaches":
        args.approaches = parseCsvList(next);
        index += 1;
        break;
      case "--iterations":
        args.iterations = parsePositiveInt(next, "--iterations");
        index += 1;
        break;
      case "--logs-dir":
        args.logsDir = resolvePath(next, "--logs-dir");
        index += 1;
        break;
      case "--report":
        args.reportPath = resolvePath(next, "--report");
        index += 1;
        break;
      case "--run-start-ms":
        args.runStartMs = parsePositiveInt(next, "--run-start-ms");
        index += 1;
        break;
      case "--run-log":
        args.runLogPath = resolvePath(next, "--run-log");
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
  console.log(`Usage: node analysis/real-data-comparison/generate-real-data-3approach-smoke-metrics.js [options]

Options:
  --approaches <list>   Comma-separated approaches (default: fetching,approximation,chunked)
  --iterations <n>      Iteration count to validate (default: 3)
  --logs-dir <path>     Real-data logs root
  --report <path>       Markdown report output path
  --run-start-ms <ms>   Epoch milliseconds at the start of the smoke run
  --run-log <path>      Captured stdout/stderr log for the smoke command
`);
}

function parseCsvList(value) {
  if (!value) {
    throw new Error("Comma-separated value is required");
  }
  const items = value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  if (items.length === 0) {
    throw new Error("Comma-separated value is required");
  }
  return [...new Set(items)];
}

function parsePositiveInt(value, flag) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} requires a positive integer`);
  }
  return parsed;
}

function resolvePath(value, flag) {
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  return path.isAbsolute(value) ? value : path.resolve(process.cwd(), value);
}

function readText(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, "utf8") : "";
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function parseCsv(filePath) {
  const content = readText(filePath).trim();
  if (!content) {
    return [];
  }
  return parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  });
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mean(values) {
  const finiteValues = values.filter((value) => Number.isFinite(value));
  if (finiteValues.length === 0) {
    return null;
  }
  return finiteValues.reduce((sum, value) => sum + value, 0) / finiteValues.length;
}

function sampleStd(values) {
  const finiteValues = values.filter((value) => Number.isFinite(value));
  if (finiteValues.length < 2) {
    return finiteValues.length === 1 ? 0 : null;
  }
  const avg = mean(finiteValues);
  const variance = finiteValues.reduce((sum, value) => sum + ((value - avg) ** 2), 0)
    / (finiteValues.length - 1);
  return Math.sqrt(variance);
}

function summarize(values) {
  const finiteValues = values.filter((value) => Number.isFinite(value));
  return {
    count: finiteValues.length,
    mean: mean(finiteValues),
    std: sampleStd(finiteValues),
    min: finiteValues.length > 0 ? Math.min(...finiteValues) : null,
    max: finiteValues.length > 0 ? Math.max(...finiteValues) : null,
  };
}

function summarizeAccuracyValues(values) {
  const finiteValues = values.filter((value) => Number.isFinite(value));
  return {
    mean: mean(finiteValues),
    std: sampleStd(finiteValues),
  };
}

function formatNumber(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : "n/a";
}

function formatBool(value) {
  return value ? "yes" : "no";
}

function formatList(values) {
  return values.join(", ");
}

function buildMarkdownTable(headers, rows) {
  const headerRow = `| ${headers.join(" | ")} |`;
  const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;
  const bodyRows = rows.map((row) => `| ${row.join(" | ")} |`);
  return [headerRow, separatorRow, ...bodyRows].join("\n");
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function getApproachConfig(approachName) {
  const entry = APPROACHES.find((approach) => approach.name === approachName);
  if (!entry) {
    throw new Error(`Unknown approach: ${approachName}`);
  }
  return entry;
}

function getMtimeMs(filePath) {
  return fs.existsSync(filePath) ? fs.statSync(filePath).mtimeMs : null;
}

function loadMqttSummary(logDir) {
  const summaryPath = path.join(logDir, "mqtt_traffic_summary.json");
  return fs.existsSync(summaryPath) ? readJson(summaryPath) : null;
}

function parseRunLog(runLogPath) {
  if (!runLogPath || !fs.existsSync(runLogPath)) {
    return {
      runLabels: [],
      containsNaiveDistributed: false,
    };
  }

  const text = readText(runLogPath);
  const runLabels = [...text.matchAll(/Running:\s+(.+?)\s+-\s+Iteration\s+(\d+)/g)]
    .map((match) => ({
      label: match[1].trim(),
      iteration: Number.parseInt(match[2], 10),
    }));

  return {
    runLabels,
    containsNaiveDistributed: /Naive Distributed/.test(text),
  };
}

function collectApproachIterationArtifacts(approachName, iteration, logsDir) {
  const approach = getApproachConfig(approachName);
  const iterationDir = path.join(logsDir, approachName, `iteration${iteration}`);
  const latencyPath = path.join(iterationDir, approach.logFiles.latency);
  const processTreePath = path.join(iterationDir, approach.processTreeFile);
  const capSummaryPath = path.join(iterationDir, "benchmark_window_cap_summary.json");
  const runSummaryPath = path.join(iterationDir, "run_summary.json");
  const mqttSummaryPath = path.join(iterationDir, "mqtt_traffic_summary.json");

  const latencyRecords = parseCsv(latencyPath);
  const mqttSummary = loadMqttSummary(iterationDir);
  const normalizedRows = fs.existsSync(latencyPath)
    ? normalizeLatencyRows(approachName, latencyRecords, { summary: mqttSummary, csvRows: [] })
    : [];
  const processTreeMetrics = summarizeProcessTreeMetrics(
    iterationDir,
    approach.processTreeFile,
    normalizedRows.length,
  );
  const capSummary = fs.existsSync(capSummaryPath) ? readJson(capSummaryPath) : null;
  const runSummary = fs.existsSync(runSummaryPath) ? readJson(runSummaryPath) : null;

  const invalidRequiredColumns = normalizedRows
    .filter((row) => (
      !Number.isFinite(row.windowNumber) ||
      !Number.isFinite(row.resultEmittedAt) ||
      !Number.isFinite(row.resultValue)
    ))
    .length;

  const comparableRows = normalizedRows.filter((row) => (
    Number.isFinite(row.anchorAlignedWindowCloseToResultMs) &&
    row.latencyDomainStatus !== "domain_mismatch"
  ));
  const domainMismatchRows = normalizedRows.filter((row) => row.latencyDomainStatus === "domain_mismatch");

  return {
    approach,
    iteration,
    iterationDir,
    latencyPath,
    processTreePath,
    capSummaryPath,
    runSummaryPath,
    mqttSummaryPath,
    normalizedRows,
    processTreeMetrics,
    capSummary,
    runSummary,
    comparableRows,
    domainMismatchRows,
    invalidRequiredColumns,
    outputFiles: [
      latencyPath,
      processTreePath,
      capSummaryPath,
      runSummaryPath,
      mqttSummaryPath,
    ],
  };
}

function computeAccuracySummary(baselineIterations, candidateIterations) {
  const perIteration = [];
  const allAbsoluteErrors = [];
  const allSquaredErrors = [];
  const allPercentageErrors = [];
  let totalMatchedWindows = 0;
  let totalBaselineOnlyWindows = 0;
  let totalApproachOnlyWindows = 0;
  let totalMapeApplicableWindows = 0;

  for (const [iteration, baseline] of baselineIterations.entries()) {
    const candidate = candidateIterations.get(iteration);
    if (!candidate) {
      continue;
    }

    const comparison = compareResults(baseline.normalizedRows, candidate.normalizedRows);
    const baselineByWindow = new Map(baseline.normalizedRows.map((row) => [row.windowNumber, row]));
    const candidateByWindow = new Map(candidate.normalizedRows.map((row) => [row.windowNumber, row]));
    const matchedWindows = [...baselineByWindow.keys()]
      .filter((windowNumber) => candidateByWindow.has(windowNumber))
      .sort((left, right) => left - right);

    let maxAbsoluteError = null;
    for (const windowNumber of matchedWindows) {
      const baselineRow = baselineByWindow.get(windowNumber);
      const candidateRow = candidateByWindow.get(windowNumber);
      const absoluteError = Math.abs(candidateRow.resultValue - baselineRow.resultValue);
      allAbsoluteErrors.push(absoluteError);
      allSquaredErrors.push(absoluteError ** 2);
      maxAbsoluteError = maxAbsoluteError === null
        ? absoluteError
        : Math.max(maxAbsoluteError, absoluteError);

      if (Math.abs(baselineRow.resultValue) > Number.EPSILON) {
        allPercentageErrors.push((absoluteError / Math.abs(baselineRow.resultValue)) * 100);
        totalMapeApplicableWindows += 1;
      }
    }

    totalMatchedWindows += comparison.matchedWindowCount;
    totalBaselineOnlyWindows += comparison.baselineOnlyCount;
    totalApproachOnlyWindows += comparison.approachOnlyCount;

    perIteration.push({
      iteration,
      matchedWindowCount: comparison.matchedWindowCount,
      baselineOnlyCount: comparison.baselineOnlyCount,
      approachOnlyCount: comparison.approachOnlyCount,
      mae: comparison.mae,
      rmse: comparison.rmse,
      mape: comparison.mape,
      maxAbsoluteError,
    });
  }

  return {
    perIteration,
    matchedWindowCount: totalMatchedWindows,
    baselineOnlyCount: totalBaselineOnlyWindows,
    approachOnlyCount: totalApproachOnlyWindows,
    mae: mean(allAbsoluteErrors),
    rmse: allSquaredErrors.length > 0
      ? Math.sqrt(allSquaredErrors.reduce((sum, value) => sum + value, 0) / allSquaredErrors.length)
      : null,
    mape: allPercentageErrors.length > 0
      ? mean(allPercentageErrors)
      : null,
    mapeApplicableWindowCount: totalMapeApplicableWindows,
    maxAbsoluteError: allAbsoluteErrors.length > 0 ? Math.max(...allAbsoluteErrors) : null,
    maeStd: sampleStd(perIteration.map((row) => row.mae)),
    rmseStd: sampleStd(perIteration.map((row) => row.rmse)),
    mapeStd: sampleStd(perIteration.map((row) => row.mape)),
    exactAgainstFetching: allAbsoluteErrors.every((value) => Math.abs(value) <= Number.EPSILON),
  };
}

function buildReportData(args) {
  const runLog = parseRunLog(args.runLogPath);
  const approachArtifacts = new Map();
  const perApproachSummaries = [];
  const warnings = [];
  const errors = [];

  for (const approachName of args.approaches) {
    const iterations = new Map();
    for (let iteration = 1; iteration <= args.iterations; iteration += 1) {
      const artifacts = collectApproachIterationArtifacts(approachName, iteration, args.logsDir);
      iterations.set(iteration, artifacts);
    }
    approachArtifacts.set(approachName, iterations);
  }

  const naiveArtifacts = [];
  for (let iteration = 1; iteration <= args.iterations; iteration += 1) {
    const naiveIterationDir = path.join(args.logsDir, "naive_distributed", `iteration${iteration}`);
    const files = fs.existsSync(naiveIterationDir)
      ? fs.readdirSync(naiveIterationDir).map((entry) => path.join(naiveIterationDir, entry))
      : [];
    naiveArtifacts.push(...files);
  }
  const naiveTouchedInRun = args.runStartMs === null
    ? null
    : naiveArtifacts.some((filePath) => {
      const mtimeMs = getMtimeMs(filePath);
      return Number.isFinite(mtimeMs) && mtimeMs >= args.runStartMs;
    });

  for (const approachName of args.approaches) {
    const iterations = approachArtifacts.get(approachName);
    const iterationRows = [...iterations.values()];
    const allRows = iterationRows.flatMap((entry) => entry.normalizedRows);
    const comparableRows = iterationRows.flatMap((entry) => entry.comparableRows);
    const domainMismatchRows = iterationRows.flatMap((entry) => entry.domainMismatchRows);
    const cpuSecondsByIteration = iterationRows.map((entry) => entry.processTreeMetrics.cpuSeconds);
    const meanRssByIteration = iterationRows.map((entry) => entry.processTreeMetrics.meanRssMiB);
    const peakRssByIteration = iterationRows.map((entry) => entry.processTreeMetrics.peakRssMiB);

    const iterationsCompleted = iterationRows.filter((entry) => {
      const cap = entry.capSummary;
      return cap
        && cap.stoppedAfterTargetWindows === true
        && cap.stopReason === "target_window_count_reached"
        && JSON.stringify(cap.finalWindowNumbers || []) === JSON.stringify(TARGET_WINDOWS)
        && Number(entry.invalidRequiredColumns) === 0;
    }).length;

    if (domainMismatchRows.length > 0) {
      warnings.push(
        `${approachName}: ${domainMismatchRows.length} latency rows reported latency_domain_status=domain_mismatch and were excluded from the comparable latency table.`,
      );
    }

    for (const iterationEntry of iterationRows) {
      if (!fs.existsSync(iterationEntry.latencyPath)) {
        errors.push(`${approachName}/iteration${iterationEntry.iteration}: missing latency CSV ${iterationEntry.latencyPath}`);
      }
      if (!fs.existsSync(iterationEntry.processTreePath)) {
        errors.push(`${approachName}/iteration${iterationEntry.iteration}: missing process-tree CSV ${iterationEntry.processTreePath}`);
      }
      if (!fs.existsSync(iterationEntry.capSummaryPath)) {
        errors.push(`${approachName}/iteration${iterationEntry.iteration}: missing window-cap JSON ${iterationEntry.capSummaryPath}`);
      }
      if (!fs.existsSync(iterationEntry.runSummaryPath)) {
        errors.push(`${approachName}/iteration${iterationEntry.iteration}: missing run summary JSON ${iterationEntry.runSummaryPath}`);
      }
      if (iterationEntry.invalidRequiredColumns > 0) {
        errors.push(`${approachName}/iteration${iterationEntry.iteration}: ${iterationEntry.invalidRequiredColumns} rows have NaN/null/empty required result fields.`);
      }
    }

    perApproachSummaries.push({
      approachName,
      label: getApproachConfig(approachName).label,
      iterationsCompleted,
      iterationRows,
      allRows,
      comparableRows,
      domainMismatchRows,
      latencyComparable: summarize(comparableRows.map((row) => row.anchorAlignedWindowCloseToResultMs)),
      latencyDomainMismatch: summarize(domainMismatchRows.map((row) => row.latencyFromWindowCloseMs)),
      cpuSeconds: summarize(cpuSecondsByIteration),
      meanRssMiB: summarize(meanRssByIteration),
      peakRssMiB: summarize(peakRssByIteration),
    });
  }

  const fetchingIterations = approachArtifacts.get("fetching") || new Map();
  const approximationAccuracy = computeAccuracySummary(
    fetchingIterations,
    approachArtifacts.get("approximation") || new Map(),
  );
  const chunkedAccuracy = computeAccuracySummary(
    fetchingIterations,
    approachArtifacts.get("chunked") || new Map(),
  );

  const completenessRows = perApproachSummaries.map((summary) => {
    const finalWindowChecks = summary.iterationRows.map((entry) => {
      const cap = entry.capSummary;
      return cap
        && JSON.stringify(cap.finalWindowNumbers || []) === JSON.stringify(TARGET_WINDOWS)
        && cap.stopReason === "target_window_count_reached";
    });
    const runTouched = args.runStartMs === null
      ? "n/a"
      : formatBool(summary.iterationRows.every((entry) => (
        [
          entry.latencyPath,
          entry.processTreePath,
          entry.capSummaryPath,
          entry.runSummaryPath,
        ].every((filePath) => {
          const mtimeMs = getMtimeMs(filePath);
          return Number.isFinite(mtimeMs) && mtimeMs >= args.runStartMs;
        })
      )));

    return {
      approach: summary.approachName,
      iterationsCompleted: `${summary.iterationsCompleted}/${args.iterations}`,
      finalWindows: finalWindowChecks.every(Boolean) ? "[1,2,3,4,5]" : "failed",
      stopReason: finalWindowChecks.every(Boolean) ? "target_window_count_reached" : "mismatch",
      latencyRows: summary.iterationRows.map((entry) => entry.normalizedRows.length).join("/"),
      requiredColumnsValid: formatBool(summary.iterationRows.every((entry) => entry.invalidRequiredColumns === 0)),
      filesTouchedThisRun: runTouched,
    };
  });

  const allSelectedApproachesTouched = args.runStartMs === null
    ? null
    : perApproachSummaries.every((summary) => (
      summary.iterationRows.every((entry) => (
        [entry.latencyPath, entry.processTreePath, entry.capSummaryPath, entry.runSummaryPath].every((filePath) => {
          const mtimeMs = getMtimeMs(filePath);
          return Number.isFinite(mtimeMs) && mtimeMs >= args.runStartMs;
        })
      ))
    ));

  const exactlyThreeApproachesRan = perApproachSummaries.length === 3
    && perApproachSummaries.every((summary) => summary.iterationsCompleted === args.iterations)
    && naiveTouchedInRun === false
    && runLog.containsNaiveDistributed === false;

  if (naiveTouchedInRun === true) {
    errors.push("naive_distributed artifacts were modified during the smoke run window, so the exclusion check failed.");
  }
  if (runLog.containsNaiveDistributed) {
    errors.push("Captured smoke log contains a Naive Distributed run label.");
  }

  const smokePassed = exactlyThreeApproachesRan
    && completenessRows.every((row) => (
      row.iterationsCompleted === `${args.iterations}/${args.iterations}`
      && row.finalWindows === "[1,2,3,4,5]"
      && row.requiredColumnsValid === "yes"
    ))
    && errors.length === 0;

  const gitBranch = runGit(["branch", "--show-current"]);
  const gitCommit = runGit(["rev-parse", "HEAD"]);
  const gitStatusShort = runGit(["status", "--short"]);
  const localDateTime = runShellText("date", ["+%Y-%m-%dT%H:%M:%S%z"]);
  const utcDateTime = runShellText("date", ["-u", "+%Y-%m-%dT%H:%M:%SZ"]);
  const outputDirectory = args.logsDir;

  const directCommand = [
    "STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1",
    "STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1",
    "STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0",
    "STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0",
    "STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0",
    "STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1",
    "STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1",
    "STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5",
    "OUTPUT_WINDOW_RANGE=30000",
    "OUTPUT_WINDOW_STEP=15000",
    "SUB_WINDOW_RANGE=30000",
    "SUB_WINDOW_STEP=15000",
    "node experiments/real-data-comparison/run-real-data-4-approaches.js",
    `  --iterations ${args.iterations}`,
    `  --approaches ${args.approaches.join(",")}`,
  ].join(" \\\n");

  const topLevelEquivalentCommand = [
    "STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME=1",
    "STREAMING_QUERY_HIVE_APPROXIMATION_COMPLETED_WINDOW_MODE=1",
    "STREAMING_QUERY_HIVE_APPROXIMATION_EARLY_TRIGGER_MODE=0",
    "STREAMING_QUERY_HIVE_CHUNKED_COMPARABLE_OUTPUT_ONLY=0",
    "STREAMING_QUERY_HIVE_CHUNKED_CADENCE_ONLY=0",
    "STREAMING_QUERY_HIVE_CHUNKED_USE_IMMEDIATE_TRIGGER=1",
    "STREAMING_QUERY_HIVE_COMPACT_REUSABLE_RESULT_PAYLOAD=1",
    "STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS=5",
    "OUTPUT_WINDOW_RANGE=30000",
    "OUTPUT_WINDOW_STEP=15000",
    "SUB_WINDOW_RANGE=30000",
    "SUB_WINDOW_STEP=15000",
    "node scripts/benchmark/run-all-paper-benchmarks.js",
    "  --suite real-data",
    `  --iterations ${args.iterations}`,
    `  --approaches ${args.approaches.join(",")}`,
  ].join(" \\\n");

  return {
    localDateTime,
    utcDateTime,
    gitBranch,
    gitCommit,
    gitStatusShort,
    outputDirectory,
    directCommand,
    topLevelEquivalentCommand,
    warnings,
    errors,
    smokePassed,
    exactlyThreeApproachesRan,
    allSelectedApproachesTouched,
    naiveTouchedInRun,
    runLog,
    completenessRows,
    latencyRows: perApproachSummaries
      .filter((summary) => summary.latencyComparable.count > 0)
      .map((summary) => ({
        approach: summary.approachName,
        count: summary.latencyComparable.count,
        mean: summary.latencyComparable.mean,
        std: summary.latencyComparable.std,
        min: summary.latencyComparable.min,
        max: summary.latencyComparable.max,
      })),
    latencyMismatchRows: perApproachSummaries
      .filter((summary) => summary.latencyDomainMismatch.count > 0)
      .map((summary) => ({
        approach: summary.approachName,
        count: summary.latencyDomainMismatch.count,
        mean: summary.latencyDomainMismatch.mean,
        std: summary.latencyDomainMismatch.std,
        min: summary.latencyDomainMismatch.min,
        max: summary.latencyDomainMismatch.max,
      })),
    accuracyRows: [
      {
        approach: "approximation",
        summary: approximationAccuracy,
      },
      {
        approach: "chunked",
        summary: chunkedAccuracy,
      },
    ],
    resourceRows: perApproachSummaries.map((summary) => ({
      approach: summary.approachName,
      cpuSeconds: summary.cpuSeconds,
      meanRssMiB: summary.meanRssMiB,
      peakRssMiB: summary.peakRssMiB,
    })),
    recommended35IterationRun: smokePassed,
  };
}

function runGit(args) {
  const result = spawnSync("git", args, {
    cwd: process.cwd(),
    encoding: "utf8",
  });
  return result.status === 0 ? result.stdout.trim() : "";
}

function runShellText(command, args) {
  const result = spawnSync(command, args, {
    cwd: process.cwd(),
    encoding: "utf8",
  });
  return result.status === 0 ? result.stdout.trim() : "";
}

function renderReport(reportData, args) {
  const completenessTable = buildMarkdownTable(
    [
      "Approach",
      "Iterations complete",
      "Final windows",
      "Stop reason",
      "Latency rows per iteration",
      "Required result fields valid",
      "Artifacts touched in run window",
    ],
    reportData.completenessRows.map((row) => [
      `\`${row.approach}\``,
      row.iterationsCompleted,
      row.finalWindows,
      `\`${row.stopReason}\``,
      row.latencyRows,
      row.requiredColumnsValid,
      row.filesTouchedThisRun,
    ]),
  );

  const latencyTable = buildMarkdownTable(
    ["Approach", "Comparable windows", "Mean ms", "Std ms", "Min ms", "Max ms"],
    reportData.latencyRows.map((row) => [
      `\`${row.approach}\``,
      String(row.count),
      formatNumber(row.mean),
      formatNumber(row.std),
      formatNumber(row.min),
      formatNumber(row.max),
    ]),
  );

  const latencyMismatchTable = reportData.latencyMismatchRows.length > 0
    ? buildMarkdownTable(
      ["Approach", "Excluded windows", "Mean raw ms", "Std raw ms", "Min raw ms", "Max raw ms"],
      reportData.latencyMismatchRows.map((row) => [
        `\`${row.approach}\``,
        String(row.count),
        formatNumber(row.mean),
        formatNumber(row.std),
        formatNumber(row.min),
        formatNumber(row.max),
      ]),
    )
    : "";

  const accuracyTable = buildMarkdownTable(
    [
      "Approach vs fetching",
      "Matched windows",
      "Baseline-only windows",
      "Approach-only windows",
      "MAE",
      "RMSE",
      "MAPE",
      "MAPE windows",
      "Max abs error",
      "Chunked exact",
    ],
    reportData.accuracyRows.map(({ approach, summary }) => [
      `\`${approach}\``,
      String(summary.matchedWindowCount),
      String(summary.baselineOnlyCount),
      String(summary.approachOnlyCount),
      formatNumber(summary.mae, 6),
      formatNumber(summary.rmse, 6),
      formatNumber(summary.mape, 6),
      String(summary.mapeApplicableWindowCount),
      formatNumber(summary.maxAbsoluteError, 6),
      approach === "chunked" ? formatBool(summary.exactAgainstFetching) : "n/a",
    ]),
  );

  const resourceTable = buildMarkdownTable(
    [
      "Approach",
      "CPU seconds mean",
      "CPU seconds std",
      "CPU seconds min",
      "CPU seconds max",
      "Mean RSS MiB mean",
      "Mean RSS MiB std",
      "Peak RSS MiB mean",
      "Peak RSS MiB std",
    ],
    reportData.resourceRows.map((row) => [
      `\`${row.approach}\``,
      formatNumber(row.cpuSeconds.mean, 3),
      formatNumber(row.cpuSeconds.std, 3),
      formatNumber(row.cpuSeconds.min, 3),
      formatNumber(row.cpuSeconds.max, 3),
      formatNumber(row.meanRssMiB.mean, 2),
      formatNumber(row.meanRssMiB.std, 2),
      formatNumber(row.peakRssMiB.mean, 2),
      formatNumber(row.peakRssMiB.std, 2),
    ]),
  );

  const warnings = reportData.warnings.length > 0
    ? reportData.warnings.map((warning) => `- ${warning}`).join("\n")
    : "- none";
  const errors = reportData.errors.length > 0
    ? reportData.errors.map((error) => `- ${error}`).join("\n")
    : "- none";

  return `# Real-Data 3-Approach 3-Iteration Smoke Metrics

Date/time:
\`${reportData.localDateTime}\` local
\`${reportData.utcDateTime}\` UTC

Branch:
\`${reportData.gitBranch}\`

Commit hash:
\`${reportData.gitCommit}\`

Dirty state:
\`${reportData.gitStatusShort ? "yes" : "no"}\`

Exact command run:

\`\`\`bash
${reportData.directCommand}
\`\`\`

Top-level equivalent command:

\`\`\`bash
${reportData.topLevelEquivalentCommand}
\`\`\`

Selected approaches:
\`${formatList(args.approaches)}\`

Excluded approaches:
\`${formatList(EXCLUDED_APPROACHES)}\`

Output directory:
\`${reportData.outputDirectory}\`

Run log:
\`${args.runLogPath || "not captured"}\`

## Completeness

- Exactly 3 selected approaches ran: \`${formatBool(reportData.exactlyThreeApproachesRan)}\`
- Selected-approach artifacts touched in this run window: \`${reportData.allSelectedApproachesTouched === null ? "n/a" : formatBool(reportData.allSelectedApproachesTouched)}\`
- naive_distributed touched in this run window: \`${reportData.naiveTouchedInRun === null ? "n/a" : formatBool(reportData.naiveTouchedInRun)}\`
- Captured run log mentions Naive Distributed: \`${formatBool(reportData.runLog.containsNaiveDistributed)}\`

${completenessTable}

## Latency

Comparable latency column used in the main table:
\`anchorAlignedWindowCloseToResultMs\`

Main comparable latency table:

${latencyTable}

${latencyMismatchTable ? `Latency rows excluded from the main comparison because \`latency_domain_status=domain_mismatch\`:\n\n${latencyMismatchTable}\n` : ""}## Accuracy

Accuracy baseline:
\`fetching\`

Alignment:
\`iteration + window_number\`

${accuracyTable}

## Resource Usage

Preferred CPU metric:
\`tree_cpu_seconds\` from the process-tree resource logs

RSS metric:
\`tree_rss_bytes\`, reported here as MiB

${resourceTable}

## Warnings

${warnings}

## Errors

${errors}

## Recommendation

- 3-approach smoke passed: \`${formatBool(reportData.smokePassed)}\`
- 35-iteration server run recommended: \`${formatBool(reportData.recommended35IterationRun)}\`
`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const reportData = buildReportData(args);
  const markdown = renderReport(reportData, args);
  ensureDir(path.dirname(args.reportPath));
  fs.writeFileSync(args.reportPath, `${markdown}\n`);
  console.log(args.reportPath);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
