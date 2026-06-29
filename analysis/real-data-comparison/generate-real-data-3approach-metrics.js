#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse/sync");

const {
  APPROACHES,
  normalizeLatencyRows,
  summarizeProcessTreeMetrics,
} = require("../../experiments/real-data-comparison/run-real-data-4-approaches.js");
const { compareResults } = require("../accuracy/accuracy-comparison-custom-patterns.js");

const SELECTED_APPROACHES = ["fetching", "approximation", "chunked"];
const MODE_DEFAULTS = {
  "steady-state": {
    expectedIterations: 1,
    targetWindows: 35,
    steadyWindowStart: 4,
    steadyWindowEnd: 33,
    warmupWindows: [1, 2, 3],
    shutdownWindows: [34, 35],
  },
  "startup-cost": {
    expectedIterations: 35,
    targetWindows: 1,
    steadyWindowStart: 1,
    steadyWindowEnd: 1,
    warmupWindows: [1],
    shutdownWindows: [],
  },
};

function parseArgs(argv) {
  const args = {
    mode: null,
    inputRoot: null,
    outputPath: null,
    expectedIterations: null,
    targetWindows: null,
    approaches: SELECTED_APPROACHES,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    switch (arg) {
      case "--mode":
        if (!next || !MODE_DEFAULTS[next]) {
          throw new Error(`--mode requires one of: ${Object.keys(MODE_DEFAULTS).join(", ")}`);
        }
        args.mode = next;
        index += 1;
        break;
      case "--input-root":
        args.inputRoot = resolvePath(next, "--input-root");
        index += 1;
        break;
      case "--output":
        args.outputPath = resolvePath(next, "--output");
        index += 1;
        break;
      case "--expected-iterations":
        args.expectedIterations = parsePositiveInt(next, "--expected-iterations");
        index += 1;
        break;
      case "--target-windows":
        args.targetWindows = parsePositiveInt(next, "--target-windows");
        index += 1;
        break;
      case "--approaches":
        args.approaches = parseCsvList(next);
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

  if (!args.mode) {
    throw new Error("--mode is required");
  }
  if (!args.inputRoot) {
    throw new Error("--input-root is required");
  }
  if (!args.outputPath) {
    throw new Error("--output is required");
  }

  return args;
}

function printHelp() {
  console.log(`Usage: node analysis/real-data-comparison/generate-real-data-3approach-metrics.js [options]

Options:
  --mode <steady-state|startup-cost>
  --input-root <path>           Real-data logs root or top-level paper output root
  --output <path>               Markdown report path
  --expected-iterations <n>     Override expected iterations for validation
  --target-windows <n>          Override expected target windows for validation
  --approaches <list>           Comma-separated approaches (default: fetching,approximation,chunked)
`);
}

function resolvePath(value, flag) {
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  return path.isAbsolute(value) ? value : path.resolve(process.cwd(), value);
}

function parsePositiveInt(value, flag) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} requires a positive integer`);
  }
  return parsed;
}

function parseCsvList(value) {
  if (!value) {
    throw new Error("--approaches requires a value");
  }
  const entries = value.split(",").map((entry) => entry.trim()).filter(Boolean);
  if (entries.length === 0) {
    throw new Error("--approaches requires a value");
  }
  return [...new Set(entries)];
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

function mean(values) {
  const cleanValues = values.filter((value) => Number.isFinite(value));
  if (cleanValues.length === 0) {
    return null;
  }
  return cleanValues.reduce((sum, value) => sum + value, 0) / cleanValues.length;
}

function sampleStd(values) {
  const cleanValues = values.filter((value) => Number.isFinite(value));
  if (cleanValues.length < 2) {
    return cleanValues.length === 1 ? 0 : null;
  }
  const avg = mean(cleanValues);
  const variance = cleanValues.reduce((sum, value) => sum + ((value - avg) ** 2), 0)
    / (cleanValues.length - 1);
  return Math.sqrt(variance);
}

function summarize(values) {
  const cleanValues = values.filter((value) => Number.isFinite(value));
  return {
    count: cleanValues.length,
    mean: mean(cleanValues),
    std: sampleStd(cleanValues),
    min: cleanValues.length > 0 ? Math.min(...cleanValues) : null,
    max: cleanValues.length > 0 ? Math.max(...cleanValues) : null,
  };
}

function formatNumber(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : "n/a";
}

function formatBool(value) {
  return value ? "yes" : "no";
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function getApproachConfig(name) {
  const approach = APPROACHES.find((entry) => entry.name === name);
  if (!approach) {
    throw new Error(`Unknown approach: ${name}`);
  }
  return approach;
}

function resolveLogsRoot(inputRoot) {
  const directRoot = inputRoot;
  const nestedRoot = path.join(inputRoot, "real-data", "raw");
  if (fs.existsSync(path.join(directRoot, "fetching")) || fs.existsSync(path.join(directRoot, "approximation"))) {
    return directRoot;
  }
  if (fs.existsSync(path.join(nestedRoot, "fetching")) || fs.existsSync(path.join(nestedRoot, "approximation"))) {
    return nestedRoot;
  }
  throw new Error(`Could not resolve real-data logs root from ${inputRoot}`);
}

function selectRows(rows, windowNumbers) {
  const allowed = new Set(windowNumbers);
  return rows.filter((row) => allowed.has(row.windowNumber));
}

function selectWindowRange(rows, startWindow, endWindow) {
  return rows.filter((row) => row.windowNumber >= startWindow && row.windowNumber <= endWindow);
}

function collectIterationArtifacts(logsRoot, approachName, iteration) {
  const approach = getApproachConfig(approachName);
  const iterationDir = path.join(logsRoot, approachName, `iteration${iteration}`);
  const latencyPath = path.join(iterationDir, approach.logFiles.latency);
  const capSummaryPath = path.join(iterationDir, "benchmark_window_cap_summary.json");
  const runSummaryPath = path.join(iterationDir, "run_summary.json");
  const processTreePath = path.join(iterationDir, approach.processTreeFile);
  const mqttSummaryPath = path.join(iterationDir, "mqtt_traffic_summary.json");

  const latencyRows = parseCsv(latencyPath);
  const mqttSummary = fs.existsSync(mqttSummaryPath) ? readJson(mqttSummaryPath) : null;
  const normalizedRows = normalizeLatencyRows(approachName, latencyRows, { summary: mqttSummary, csvRows: [] });
  const capSummary = fs.existsSync(capSummaryPath) ? readJson(capSummaryPath) : null;
  const runSummary = fs.existsSync(runSummaryPath) ? readJson(runSummaryPath) : null;
  const processTreeMetrics = summarizeProcessTreeMetrics(iterationDir, approach.processTreeFile, normalizedRows.length);

  return {
    approachName,
    iteration,
    iterationDir,
    normalizedRows,
    capSummary,
    runSummary,
    processTreeMetrics,
    files: {
      latencyPath,
      capSummaryPath,
      runSummaryPath,
      processTreePath,
      mqttSummaryPath,
    },
  };
}

function buildExpectedConfig(mode, overrides = {}) {
  const base = MODE_DEFAULTS[mode];
  return {
    ...base,
    expectedIterations: overrides.expectedIterations || base.expectedIterations,
    targetWindows: overrides.targetWindows || base.targetWindows,
  };
}

function computeAccuracy(baselineRows, candidateRows) {
  const comparison = compareResults(baselineRows, candidateRows);
  const baselineByWindow = new Map(baselineRows.map((row) => [row.windowNumber, row]));
  const candidateByWindow = new Map(candidateRows.map((row) => [row.windowNumber, row]));
  const matchedWindows = [...baselineByWindow.keys()]
    .filter((windowNumber) => candidateByWindow.has(windowNumber))
    .sort((left, right) => left - right);

  const absoluteErrors = matchedWindows.map((windowNumber) => (
    Math.abs(candidateByWindow.get(windowNumber).resultValue - baselineByWindow.get(windowNumber).resultValue)
  ));

  return {
    matchedWindowCount: comparison.matchedWindowCount,
    baselineOnlyCount: comparison.baselineOnlyCount,
    approachOnlyCount: comparison.approachOnlyCount,
    mae: comparison.mae,
    rmse: comparison.rmse,
    mape: comparison.mape,
    mapeApplicableWindowCount: comparison.mapeApplicableWindowCount,
    maxAbsoluteError: absoluteErrors.length > 0 ? Math.max(...absoluteErrors) : null,
    exactAgainstFetching: absoluteErrors.every((value) => Math.abs(value) <= Number.EPSILON),
  };
}

function analyzeRealDataResults(options) {
  const logsRoot = resolveLogsRoot(options.inputRoot);
  const expected = buildExpectedConfig(options.mode, {
    expectedIterations: options.expectedIterations,
    targetWindows: options.targetWindows,
  });
  const selectedApproaches = options.approaches || SELECTED_APPROACHES;

  const byApproach = new Map();
  const presentApproaches = [];
  const warnings = [];
  const errors = [];

  for (const approachName of selectedApproaches) {
    const approachDir = path.join(logsRoot, approachName);
    const iterations = fs.existsSync(approachDir)
      ? fs.readdirSync(approachDir)
        .filter((entry) => /^iteration\d+$/.test(entry))
        .map((entry) => Number.parseInt(entry.replace("iteration", ""), 10))
        .sort((left, right) => left - right)
      : [];
    if (iterations.length > 0) {
      presentApproaches.push(approachName);
    }
    byApproach.set(
      approachName,
      iterations.map((iteration) => collectIterationArtifacts(logsRoot, approachName, iteration)),
    );
  }

  const knownApproachNames = APPROACHES.map((approach) => approach.name);
  const unexpectedApproaches = fs.existsSync(logsRoot)
    ? fs.readdirSync(logsRoot)
      .filter((entry) => fs.statSync(path.join(logsRoot, entry)).isDirectory())
      .filter((entry) => knownApproachNames.includes(entry))
      .filter((entry) => !selectedApproaches.includes(entry))
    : [];

  const fetchingIterations = byApproach.get("fetching") || [];
  const fetchingByIteration = new Map(fetchingIterations.map((record) => [record.iteration, record]));

  const perApproach = selectedApproaches.map((approachName) => {
    const iterationRecords = byApproach.get(approachName) || [];
    const completeness = iterationRecords.map((record) => {
      const finalWindowNumbers = record.capSummary?.finalWindowNumbers || [];
      const expectedWindows = Array.from({ length: expected.targetWindows }, (_, index) => index + 1);
      const finalWindowsMatch = JSON.stringify(finalWindowNumbers) === JSON.stringify(expectedWindows);
      const stopReasonMatch = record.capSummary?.stopReason === "target_window_count_reached";
      const runCompleted = record.runSummary?.completionStatus === "completed";
      return {
        iteration: record.iteration,
        finalWindowsMatch,
        stopReasonMatch,
        runCompleted,
        windowCount: record.normalizedRows.length,
      };
    });

    if (iterationRecords.length !== expected.expectedIterations) {
      errors.push(
        `${approachName}: expected ${expected.expectedIterations} iterations, found ${iterationRecords.length}`,
      );
    }

    const processResourceSummary = options.mode === "steady-state"
      ? {
        cpuSeconds: iterationRecords[0]?.processTreeMetrics.cpuSeconds ?? null,
        meanRssMiB: iterationRecords[0]?.processTreeMetrics.meanRssMiB ?? null,
        peakRssMiB: iterationRecords[0]?.processTreeMetrics.peakRssMiB ?? null,
      }
      : {
        cpuSeconds: summarize(iterationRecords.map((record) => record.processTreeMetrics.cpuSeconds)),
        meanRssMiB: summarize(iterationRecords.map((record) => record.processTreeMetrics.meanRssMiB)),
        peakRssMiB: summarize(iterationRecords.map((record) => record.processTreeMetrics.peakRssMiB)),
      };

    return {
      approachName,
      iterationRecords,
      completeness,
      processResourceSummary,
    };
  });

  const steadyStateLatency = [];
  const startupLatency = [];
  const accuracyRows = [];

  for (const approachSummary of perApproach) {
    const { approachName, iterationRecords } = approachSummary;

    if (options.mode === "steady-state") {
      const record = iterationRecords[0];
      const allRows = record?.normalizedRows || [];
      const warmupRows = selectRows(allRows, expected.warmupWindows);
      const steadyRows = selectWindowRange(allRows, expected.steadyWindowStart, expected.steadyWindowEnd);
      const shutdownRows = selectRows(allRows, expected.shutdownWindows);
      const domainMismatchRows = allRows.filter((row) => row.latencyDomainStatus === "domain_mismatch");
      const comparableSteadyRows = steadyRows.filter((row) => row.latencyDomainStatus !== "domain_mismatch");

      steadyStateLatency.push({
        approach: approachName,
        warmup: summarize(warmupRows.map((row) => row.anchorAlignedWindowCloseToResultMs)),
        steady: summarize(comparableSteadyRows.map((row) => row.anchorAlignedWindowCloseToResultMs)),
        shutdown: summarize(shutdownRows.map((row) => row.anchorAlignedWindowCloseToResultMs)),
        domainMismatch: summarize(domainMismatchRows.map((row) => row.latencyFromWindowCloseMs)),
      });

      if (domainMismatchRows.length > 0) {
        warnings.push(
          `${approachName}: ${domainMismatchRows.length} rows have latency_domain_status=domain_mismatch and were excluded from the steady-state main latency table.`,
        );
      }

      if (approachName !== "fetching") {
        const baseline = fetchingByIteration.get(1);
        if (baseline) {
          accuracyRows.push({
            approach: approachName,
            ...computeAccuracy(
              selectWindowRange(baseline.normalizedRows, expected.steadyWindowStart, expected.steadyWindowEnd),
              steadyRows,
            ),
          });
        }
      }

      if (record && record.normalizedRows.length !== expected.targetWindows) {
        errors.push(
          `${approachName}: expected ${expected.targetWindows} windows, found ${record.normalizedRows.length}`,
        );
      }
    } else {
      const comparableStartupLatency = [];
      const registrationToResult = [];
      const dataStartToResult = [];
      const lastDataToResult = [];

      for (const record of iterationRecords) {
        const firstRow = record.normalizedRows[0];
        if (!firstRow) {
          errors.push(`${approachName}/iteration${record.iteration}: missing first-window result row`);
          continue;
        }
        comparableStartupLatency.push(firstRow.anchorAlignedWindowCloseToResultMs);
        registrationToResult.push(firstRow.registrationToResultMs);
        dataStartToResult.push(firstRow.dataStartToResultMs);
        lastDataToResult.push(firstRow.lastDataToResultMs);
        if (firstRow.windowNumber !== 1 || record.normalizedRows.length !== 1) {
          errors.push(
            `${approachName}/iteration${record.iteration}: expected exactly window 1, found ${record.normalizedRows.map((row) => row.windowNumber).join(",")}`,
          );
        }
      }

      startupLatency.push({
        approach: approachName,
        startupCostMs: summarize(registrationToResult),
        closeToResultMs: summarize(comparableStartupLatency),
        dataStartToResultMs: summarize(dataStartToResult),
        lastDataToResultMs: summarize(lastDataToResult),
      });

      if (approachName !== "fetching") {
        const perIterationAccuracy = [];
        for (const record of iterationRecords) {
          const baseline = fetchingByIteration.get(record.iteration);
          if (!baseline) {
            continue;
          }
          perIterationAccuracy.push(
            computeAccuracy([baseline.normalizedRows[0]].filter(Boolean), [record.normalizedRows[0]].filter(Boolean)),
          );
        }

        accuracyRows.push({
          approach: approachName,
          matchedWindowCount: perIterationAccuracy.reduce((sum, row) => sum + row.matchedWindowCount, 0),
          baselineOnlyCount: perIterationAccuracy.reduce((sum, row) => sum + row.baselineOnlyCount, 0),
          approachOnlyCount: perIterationAccuracy.reduce((sum, row) => sum + row.approachOnlyCount, 0),
          mae: mean(perIterationAccuracy.map((row) => row.mae)),
          rmse: mean(perIterationAccuracy.map((row) => row.rmse)),
          mape: mean(perIterationAccuracy.map((row) => row.mape)),
          mapeApplicableWindowCount: perIterationAccuracy.reduce((sum, row) => sum + row.mapeApplicableWindowCount, 0),
          maxAbsoluteError: Math.max(...perIterationAccuracy.map((row) => row.maxAbsoluteError || 0)),
          exactAgainstFetching: perIterationAccuracy.every((row) => row.exactAgainstFetching),
        });
      }
    }
  }

  const selectedApproachesExact = presentApproaches.length === selectedApproaches.length
    && selectedApproaches.every((approachName) => presentApproaches.includes(approachName))
    && unexpectedApproaches.every((entry) => entry !== "naive_distributed");

  return {
    mode: options.mode,
    logsRoot,
    selectedApproaches,
    unexpectedApproaches,
    expected,
    perApproach,
    steadyStateLatency,
    startupLatency,
    accuracyRows,
    warnings,
    errors,
    selectedApproachesExact,
  };
}

function buildMarkdownTable(headers, rows) {
  return [
    `| ${headers.join(" | ")} |`,
    `| ${headers.map(() => "---").join(" | ")} |`,
    ...rows.map((row) => `| ${row.join(" | ")} |`),
  ].join("\n");
}

function renderReport(result) {
  const completenessTable = buildMarkdownTable(
    ["Approach", "Iterations found", "All stop reasons ok", "All final windows ok", "All run summaries completed"],
    result.perApproach.map((entry) => [
      `\`${entry.approachName}\``,
      String(entry.iterationRecords.length),
      formatBool(entry.completeness.every((row) => row.stopReasonMatch)),
      formatBool(entry.completeness.every((row) => row.finalWindowsMatch)),
      formatBool(entry.completeness.every((row) => row.runCompleted)),
    ]),
  );

  const accuracyTable = buildMarkdownTable(
    ["Approach vs fetching", "Matched windows", "Baseline-only", "Approach-only", "MAE", "RMSE", "MAPE", "Max abs error", "Chunked exact"],
    result.accuracyRows.map((row) => [
      `\`${row.approach}\``,
      String(row.matchedWindowCount),
      String(row.baselineOnlyCount),
      String(row.approachOnlyCount),
      formatNumber(row.mae, 6),
      formatNumber(row.rmse, 6),
      formatNumber(row.mape, 6),
      formatNumber(row.maxAbsoluteError, 6),
      row.approach === "chunked" ? formatBool(row.exactAgainstFetching) : "n/a",
    ]),
  );

  const warnings = result.warnings.length > 0
    ? result.warnings.map((warning) => `- ${warning}`).join("\n")
    : "- none";
  const errors = result.errors.length > 0
    ? result.errors.map((error) => `- ${error}`).join("\n")
    : "- none";

  if (result.mode === "steady-state") {
    const latencyTable = buildMarkdownTable(
      ["Approach", "Warm-up windows 1-3 mean ms", "Steady windows 4-33 mean ms", "Steady std", "Steady min", "Steady max", "Shutdown windows 34-35 mean ms"],
      result.steadyStateLatency.map((row) => [
        `\`${row.approach}\``,
        formatNumber(row.warmup.mean),
        formatNumber(row.steady.mean),
        formatNumber(row.steady.std),
        formatNumber(row.steady.min),
        formatNumber(row.steady.max),
        formatNumber(row.shutdown.mean),
      ]),
    );

    const resourceTable = buildMarkdownTable(
      ["Approach", "CPU seconds", "Mean RSS MiB", "Peak RSS MiB"],
      result.perApproach.map((entry) => [
        `\`${entry.approachName}\``,
        formatNumber(entry.processResourceSummary.cpuSeconds, 3),
        formatNumber(entry.processResourceSummary.meanRssMiB, 2),
        formatNumber(entry.processResourceSummary.peakRssMiB, 2),
      ]),
    );

    return `# Real-Data 3-Approach 35-Window Steady-State Summary

Input root:
\`${result.logsRoot}\`

Selected approaches exact:
\`${formatBool(result.selectedApproachesExact)}\`

Window partition:
- dropped warm-up windows: 1, 2, 3
- main steady-state windows: 4..33
- dropped shutdown windows: 34, 35
- steady-state sample size: 30 windows

## Completeness

${completenessTable}

## Latency

Main latency metric:
\`anchorAlignedWindowCloseToResultMs\` using only windows 4..33 and excluding \`domain_mismatch\` rows

${latencyTable}

## Accuracy

${accuracyTable}

## Resources

This is one long run per approach, so the resource table reports single-run values rather than across-run variability.

${resourceTable}

## Warnings

${warnings}

## Errors

${errors}
`;
  }

  const startupTable = buildMarkdownTable(
    ["Approach", "Startup cost mean ms", "Startup cost std", "Min", "Max", "Close-to-result mean ms", "Data-start-to-result mean ms"],
    result.startupLatency.map((row) => [
      `\`${row.approach}\``,
      formatNumber(row.startupCostMs.mean),
      formatNumber(row.startupCostMs.std),
      formatNumber(row.startupCostMs.min),
      formatNumber(row.startupCostMs.max),
      formatNumber(row.closeToResultMs.mean),
      formatNumber(row.dataStartToResultMs.mean),
    ]),
  );

  const resourceTable = buildMarkdownTable(
    ["Approach", "CPU seconds mean", "CPU seconds std", "Mean RSS MiB mean", "Mean RSS MiB std", "Peak RSS MiB mean", "Peak RSS MiB std"],
    result.perApproach.map((entry) => [
      `\`${entry.approachName}\``,
      formatNumber(entry.processResourceSummary.cpuSeconds.mean, 3),
      formatNumber(entry.processResourceSummary.cpuSeconds.std, 3),
      formatNumber(entry.processResourceSummary.meanRssMiB.mean, 2),
      formatNumber(entry.processResourceSummary.meanRssMiB.std, 2),
      formatNumber(entry.processResourceSummary.peakRssMiB.mean, 2),
      formatNumber(entry.processResourceSummary.peakRssMiB.std, 2),
    ]),
  );

  return `# Real-Data 3-Approach 35x1 Startup-Cost Summary

Input root:
\`${result.logsRoot}\`

Selected approaches exact:
\`${formatBool(result.selectedApproachesExact)}\`

Startup metric:
\`registrationToResultMs\` for the first and only completed window in each independent iteration

Containment/planning timing:
No separate containment-check timing field is currently extracted in this helper, so first-window startup cost is treated as the aggregate startup cost.

## Completeness

${completenessTable}

## Startup Latency

${startupTable}

## Accuracy

${accuracyTable}

## Resources

${resourceTable}

## Warnings

${warnings}

## Errors

${errors}
`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const result = analyzeRealDataResults(args);
  const markdown = renderReport(result);
  ensureDir(path.dirname(args.outputPath));
  fs.writeFileSync(args.outputPath, `${markdown}\n`);
  console.log(args.outputPath);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exit(1);
  }
} else {
  module.exports = {
    MODE_DEFAULTS,
    analyzeRealDataResults,
    buildExpectedConfig,
    renderReport,
    resolveLogsRoot,
  };
}
