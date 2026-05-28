#!/usr/bin/env node

/**
 * Custom Pattern Accuracy Comparison
 *
 * Aggregates per-iteration custom-pattern experiment outputs into paper-ready
 * per-pattern accuracy summaries against the fetching client-side baseline.
 *
 * Expected input layout:
 *   <input-root>/fetching/<pattern>/iterationN/fetching_results.csv
 *   <input-root>/approximation/<pattern>/iterationN/approximation_results.csv
 *   <input-root>/chunked/<pattern>/iterationN/chunked_results.csv
 *   <input-root>/naive_distributed/<pattern>/iterationN/...
 *
 * Outputs:
 *   <output-dir>/summary.json
 *   <output-dir>/summary.csv
 */

const fs = require("fs");
const path = require("path");

const DEFAULT_INPUT_ROOT = path.resolve(process.cwd(), "logs/custom-pattern-comparison");
const DEFAULT_OUTPUT_DIR = path.resolve(
  process.cwd(),
  "logs/custom-pattern-comparison/analysis/custom-pattern-accuracy",
);

const ALL_PATTERNS = [
  { type: "low_variability", name: "Low Variability" },
  { type: "step_pattern", name: "Step Pattern" },
  { type: "spike_pattern", name: "Spike Pattern" },
  { type: "low_freq_oscillation", name: "Low Freq. Oscillation" },
  { type: "high_freq_oscillation", name: "High Freq. Oscillation" },
];

const SMOKE_PATTERNS = [ALL_PATTERNS[0]];
const FULL_REQUIRED_APPROACHES = ["fetching", "approximation", "chunked"];
const FULL_OPTIONAL_APPROACHES = ["naive_distributed"];
const SMOKE_REQUIRED_APPROACHES = ["fetching", "approximation"];
const SMOKE_OPTIONAL_APPROACHES = ["chunked", "naive_distributed"];

const FILE_MAP = {
  fetching: {
    result: "fetching_results.csv",
    metadata: "fetching_metadata.json",
    latency: "fetching_latency_log.csv",
    resource: "fetching_resource_usage.csv",
  },
  approximation: {
    result: "approximation_results.csv",
    metadata: "approximation_metadata.json",
    latency: "approximation_latency_log.csv",
    resource: "approximation_approach_resource_usage.csv",
  },
  chunked: {
    result: "chunked_results.csv",
    metadata: "chunked_metadata.json",
    latency: "chunked_latency_log.csv",
    resource: "streaming_query_hive_resource_log.csv",
  },
  naive_distributed: {
    result: "naive_distributed_results.csv",
    metadata: "naive_distributed_metadata.json",
    latency: "naive_distributed_latency_log.csv",
    resource: "naive_distributed_approach_resource_usage.csv",
  },
};

function parseArgs(argv) {
  const args = {
    inputRoot: DEFAULT_INPUT_ROOT,
    outputDir: DEFAULT_OUTPUT_DIR,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    switch (arg) {
      case "--input-root":
        if (!next) {
          throw new Error("--input-root requires a value");
        }
        args.inputRoot = resolveMaybeRelativePath(next);
        index += 1;
        break;
      case "--output-dir":
        if (!next) {
          throw new Error("--output-dir requires a value");
        }
        args.outputDir = resolveMaybeRelativePath(next);
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
  console.log(`Usage: node analysis/accuracy/accuracy-comparison-custom-patterns.js [options]

Options:
  --input-root <path>   Pattern benchmark output root (default: ${DEFAULT_INPUT_ROOT})
  --output-dir <path>   Directory for summary.json and summary.csv (default: ${DEFAULT_OUTPUT_DIR})
  --help                Show this help
`);
}

function resolveMaybeRelativePath(inputPath) {
  return path.isAbsolute(inputPath) ? inputPath : path.resolve(process.cwd(), inputPath);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function pathExists(filePath) {
  try {
    return fs.existsSync(filePath);
  } catch {
    return false;
  }
}

function listIterationNumbers(rootDir) {
  if (!pathExists(rootDir)) {
    return [];
  }

  return fs
    .readdirSync(rootDir)
    .filter((entry) => /^iteration\d+$/.test(entry))
    .map((entry) => Number.parseInt(entry.replace("iteration", ""), 10))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right);
}

function readResultsCsv(filePath) {
  if (!pathExists(filePath)) {
    return null;
  }

  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) {
    return null;
  }

  const lines = content.split(/\r?\n/);
  if (lines.length < 2) {
    return null;
  }

  const results = [];
  for (let index = 1; index < lines.length; index += 1) {
    const line = lines[index].trim();
    if (!line) {
      continue;
    }

    const parts = line.split(",");
    if (parts.length < 3) {
      continue;
    }

    const timestamp = Number.parseInt(parts[0], 10);
    const windowNumber = Number.parseInt(parts[1], 10);
    const resultValue = Number.parseFloat(parts[2]);
    const latencyValue = parts[3] && parts[3] !== "N/A" ? Number.parseFloat(parts[3]) : null;

    if (!Number.isFinite(windowNumber) || !Number.isFinite(resultValue)) {
      continue;
    }

    results.push({
      timestamp: Number.isFinite(timestamp) ? timestamp : null,
      windowNumber,
      resultValue,
      latency: Number.isFinite(latencyValue) ? latencyValue : null,
    });
  }

  return results.length > 0 ? results : null;
}

function readMetadataJson(filePath) {
  if (!pathExists(filePath)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    return {
      parseError: error.message,
    };
  }
}

function calculateStats(values) {
  if (!values || values.length === 0) {
    return {
      count: 0,
      mean: null,
      std: null,
      min: null,
      max: null,
    };
  }

  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;

  return {
    count: values.length,
    mean,
    std: Math.sqrt(variance),
    min: Math.min(...values),
    max: Math.max(...values),
  };
}

function compareResults(baselineResults, approachResults) {
  const baselineByWindow = new Map();
  const approachByWindow = new Map();

  for (const result of baselineResults) {
    baselineByWindow.set(result.windowNumber, result);
  }

  for (const result of approachResults) {
    approachByWindow.set(result.windowNumber, result);
  }

  const matchedWindowNumbers = [...baselineByWindow.keys()]
    .filter((windowNumber) => approachByWindow.has(windowNumber))
    .sort((left, right) => left - right);

  let sumAbsoluteError = 0;
  let sumSquaredError = 0;
  let sumPercentageError = 0;
  let mapeApplicableWindowCount = 0;

  for (const windowNumber of matchedWindowNumbers) {
    const baseline = baselineByWindow.get(windowNumber).resultValue;
    const approach = approachByWindow.get(windowNumber).resultValue;
    const absoluteError = Math.abs(approach - baseline);

    sumAbsoluteError += absoluteError;
    sumSquaredError += absoluteError ** 2;

    if (Math.abs(baseline) > Number.EPSILON) {
      sumPercentageError += (absoluteError / Math.abs(baseline)) * 100;
      mapeApplicableWindowCount += 1;
    }
  }

  const matchedWindowCount = matchedWindowNumbers.length;
  const baselineOnlyCount = baselineResults.length - matchedWindowCount;
  const approachOnlyCount = approachResults.length - matchedWindowCount;

  return {
    matchedWindowCount,
    baselineOnlyCount: baselineOnlyCount > 0 ? baselineOnlyCount : 0,
    approachOnlyCount: approachOnlyCount > 0 ? approachOnlyCount : 0,
    mapeApplicableWindowCount,
    mae: matchedWindowCount > 0 ? sumAbsoluteError / matchedWindowCount : null,
    rmse: matchedWindowCount > 0 ? Math.sqrt(sumSquaredError / matchedWindowCount) : null,
    mape:
      mapeApplicableWindowCount > 0
        ? sumPercentageError / mapeApplicableWindowCount
        : null,
  };
}

function getApproachRoot(inputRoot, approach, patternType) {
  return path.join(inputRoot, approach, patternType);
}

function collectFileInventory(approachRoot, expectedFiles) {
  const inventory = {
    observedFiles: [],
    missingExpectedFiles: [...expectedFiles],
  };

  if (!pathExists(approachRoot)) {
    return inventory;
  }

  const observed = new Set();
  const missingExpected = new Set(expectedFiles);

  const iterationNumbers = listIterationNumbers(approachRoot);
  for (const iterationNumber of iterationNumbers) {
    const iterationDir = path.join(approachRoot, `iteration${iterationNumber}`);
    if (!pathExists(iterationDir)) {
      continue;
    }

    const fileNames = fs
      .readdirSync(iterationDir)
      .filter((entry) => fs.statSync(path.join(iterationDir, entry)).isFile());

    for (const fileName of fileNames) {
      observed.add(fileName);
      missingExpected.delete(fileName);
    }
  }

  inventory.observedFiles = [...observed].sort((left, right) => left.localeCompare(right));
  inventory.missingExpectedFiles = [...missingExpected].sort((left, right) => left.localeCompare(right));
  return inventory;
}

function buildPatternSummary(inputRoot, pattern, requiredApproaches, optionalApproaches) {
  const fetchingRoot = getApproachRoot(inputRoot, "fetching", pattern.type);
  const baselineIterations = listIterationNumbers(fetchingRoot);
  const baselineIterationSet = new Set(baselineIterations);
  const baselineInventory = collectFileInventory(fetchingRoot, Object.values(FILE_MAP.fetching));

  const patternSummary = {
    patternType: pattern.type,
    patternDisplayName: pattern.name,
    status: "missing",
    baseline: {
      approach: "fetching",
      rootDir: fetchingRoot,
      iterations: baselineIterations,
      iterationCount: baselineIterations.length,
      files: baselineInventory,
    },
    comparisons: [],
    optionalApproaches: [],
    inputIssues: [],
  };

  if (!pathExists(fetchingRoot) || baselineIterations.length === 0) {
    patternSummary.inputIssues.push({
      approach: "fetching",
      reason: pathExists(fetchingRoot)
        ? "No iteration directories found"
        : "Fetching baseline directory missing",
    });
    return patternSummary;
  }

  let patternHasCompleteRequiredComparison = true;

  const allApproaches = [...requiredApproaches, ...optionalApproaches];

  for (const approach of allApproaches) {
    const approachRoot = getApproachRoot(inputRoot, approach, pattern.type);
    const expectedFiles = Object.values(FILE_MAP[approach]);
    const inventory = collectFileInventory(approachRoot, expectedFiles);
    const iterationNumbers = listIterationNumbers(approachRoot);
    const commonIterations = iterationNumbers.filter((iterationNumber) => baselineIterationSet.has(iterationNumber));
    const missingIterations = baselineIterations.filter((iterationNumber) => !commonIterations.includes(iterationNumber));

    const approachSummary = {
      approach,
      rootDir: approachRoot,
      iterations: iterationNumbers,
      iterationCount: iterationNumbers.length,
      commonIterations,
      commonIterationCount: commonIterations.length,
      missingIterations,
      files: inventory,
      status: "missing",
    };

    if (!pathExists(approachRoot) || iterationNumbers.length === 0) {
      approachSummary.inputIssue = pathExists(approachRoot)
        ? "No iteration directories found"
        : "Approach directory missing";

      if (requiredApproaches.includes(approach)) {
        patternHasCompleteRequiredComparison = false;
      }

      if (approach === "fetching") {
        patternSummary.inputIssues.push({
          approach,
          reason: approachSummary.inputIssue,
        });
      } else if (optionalApproaches.includes(approach)) {
        patternSummary.optionalApproaches.push(approachSummary);
      } else {
        patternSummary.comparisons.push(approachSummary);
      }

      continue;
    }

    const comparableIterationDetails = [];
    const matchedWindowsTotals = [];
    const baselineOnlyTotals = [];
    const approachOnlyTotals = [];
    const mapeValues = [];
    const maeValues = [];
    const rmseValues = [];
    const missingArtifacts = [];

    for (const iterationNumber of commonIterations) {
      const iterationDir = path.join(approachRoot, `iteration${iterationNumber}`);
      const baselineIterationDir = path.join(fetchingRoot, `iteration${iterationNumber}`);

      const baselineResultsPath = path.join(baselineIterationDir, FILE_MAP.fetching.result);
      const approachResultsPath = path.join(iterationDir, FILE_MAP[approach].result);

      const baselineResults = readResultsCsv(baselineResultsPath);
      const approachResults = readResultsCsv(approachResultsPath);

      if (!baselineResults || !approachResults) {
        missingArtifacts.push({
          iteration: iterationNumber,
          missingFiles: [
            !baselineResults ? path.relative(inputRoot, baselineResultsPath) : null,
            !approachResults ? path.relative(inputRoot, approachResultsPath) : null,
          ].filter(Boolean),
        });
        continue;
      }

      const comparison = compareResults(baselineResults, approachResults);

      matchedWindowsTotals.push(comparison.matchedWindowCount);
      baselineOnlyTotals.push(comparison.baselineOnlyCount);
      approachOnlyTotals.push(comparison.approachOnlyCount);
      if (comparison.mape !== null) {
        mapeValues.push(comparison.mape);
      }
      if (comparison.mae !== null) {
        maeValues.push(comparison.mae);
      }
      if (comparison.rmse !== null) {
        rmseValues.push(comparison.rmse);
      }

      comparableIterationDetails.push({
        iteration: iterationNumber,
        baselineResultCount: baselineResults.length,
        approachResultCount: approachResults.length,
        matchedWindowCount: comparison.matchedWindowCount,
        baselineOnlyCount: comparison.baselineOnlyCount,
        approachOnlyCount: comparison.approachOnlyCount,
        mapeApplicableWindowCount: comparison.mapeApplicableWindowCount,
        mae: comparison.mae,
        rmse: comparison.rmse,
        mape: comparison.mape,
      });
    }

    const approachStatus = comparableIterationDetails.length > 0
      ? (missingArtifacts.length > 0 ? "partial" : "complete")
      : "missing";

    if (requiredApproaches.includes(approach) && approachStatus !== "complete") {
      patternHasCompleteRequiredComparison = false;
    }

    approachSummary.status = approachStatus;
    approachSummary.iterationsCompared = comparableIterationDetails.length;
    approachSummary.metrics = {
      mae: calculateStats(maeValues),
      rmse: calculateStats(rmseValues),
      mape: calculateStats(mapeValues),
    };
    approachSummary.counts = {
      matchedWindowsTotal: matchedWindowsTotals.reduce((sum, value) => sum + value, 0),
      baselineOnlyTotal: baselineOnlyTotals.reduce((sum, value) => sum + value, 0),
      approachOnlyTotal: approachOnlyTotals.reduce((sum, value) => sum + value, 0),
      baselineResultTotal: comparableIterationDetails.reduce((sum, iteration) => sum + iteration.baselineResultCount, 0),
      approachResultTotal: comparableIterationDetails.reduce((sum, iteration) => sum + iteration.approachResultCount, 0),
      mapeApplicableWindowTotal: comparableIterationDetails.reduce((sum, iteration) => sum + iteration.mapeApplicableWindowCount, 0),
    };
    approachSummary.iterationMetrics = comparableIterationDetails;
    approachSummary.missingArtifacts = missingArtifacts;

    if (approach === "fetching") {
      patternSummary.baseline.metrics = {
        resultCount: calculateStats(comparableIterationDetails.map((iteration) => iteration.baselineResultCount)),
        matchedWindows: calculateStats(matchedWindowsTotals),
      };
      patternSummary.baseline.status = approachStatus;
      patternSummary.baseline.iterationsCompared = comparableIterationDetails.length;
      patternSummary.baseline.missingArtifacts = missingArtifacts;
      patternSummary.baseline.counts = approachSummary.counts;
      patternSummary.baseline.iterationMetrics = comparableIterationDetails;
      continue;
    }

    if (optionalApproaches.includes(approach)) {
      patternSummary.optionalApproaches.push(approachSummary);
    } else {
      patternSummary.comparisons.push(approachSummary);
    }
  }

  if (patternSummary.baseline.status === "missing") {
    patternHasCompleteRequiredComparison = false;
  }

  patternSummary.status = patternHasCompleteRequiredComparison ? "complete" : "partial";
  return patternSummary;
}

function summarizePatterns(inputRoot, patterns, requiredApproaches, optionalApproaches) {
  return patterns.map((pattern) => buildPatternSummary(inputRoot, pattern, requiredApproaches, optionalApproaches));
}

function buildCsvRows(patternSummaries) {
  const rows = [];

  for (const patternSummary of patternSummaries) {
    for (const approachSummary of patternSummary.comparisons) {
      rows.push({
        patternType: patternSummary.patternType,
        patternDisplayName: patternSummary.patternDisplayName,
        approach: approachSummary.approach,
        status: approachSummary.status,
        iterationsCompared: approachSummary.iterationsCompared || 0,
        matchedWindowsTotal: approachSummary.counts ? approachSummary.counts.matchedWindowsTotal : 0,
        baselineOnlyTotal: approachSummary.counts ? approachSummary.counts.baselineOnlyTotal : 0,
        approachOnlyTotal: approachSummary.counts ? approachSummary.counts.approachOnlyTotal : 0,
        maeMean: approachSummary.metrics && approachSummary.metrics.mae ? approachSummary.metrics.mae.mean : null,
        maeStd: approachSummary.metrics && approachSummary.metrics.mae ? approachSummary.metrics.mae.std : null,
        rmseMean: approachSummary.metrics && approachSummary.metrics.rmse ? approachSummary.metrics.rmse.mean : null,
        rmseStd: approachSummary.metrics && approachSummary.metrics.rmse ? approachSummary.metrics.rmse.std : null,
        mapeMean: approachSummary.metrics && approachSummary.metrics.mape ? approachSummary.metrics.mape.mean : null,
        mapeStd: approachSummary.metrics && approachSummary.metrics.mape ? approachSummary.metrics.mape.std : null,
        mapeApplicableWindowsTotal: approachSummary.counts ? approachSummary.counts.mapeApplicableWindowTotal : 0,
        missingIterations: (approachSummary.missingIterations || []).join(";"),
        missingArtifacts: JSON.stringify(approachSummary.missingArtifacts || []),
      });
    }
  }

  return rows;
}

function csvEscape(value) {
  if (value === null || value === undefined) {
    return "";
  }

  const stringValue = String(value);
  if (/[",\n\r]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }

  return stringValue;
}

function writeCsv(filePath, rows) {
  const header = [
    "pattern_type",
    "pattern_display_name",
    "approach",
    "status",
    "iterations_compared",
    "matched_windows_total",
    "baseline_only_total",
    "approach_only_total",
    "mae_mean",
    "mae_std",
    "rmse_mean",
    "rmse_std",
    "mape_mean",
    "mape_std",
    "mape_applicable_windows_total",
    "missing_iterations",
    "missing_artifacts",
  ];

  const lines = [header.join(",")];

  for (const row of rows) {
    lines.push(
      [
        row.patternType,
        row.patternDisplayName,
        row.approach,
        row.status,
        row.iterationsCompared,
        row.matchedWindowsTotal,
        row.baselineOnlyTotal,
        row.approachOnlyTotal,
        formatCsvNumber(row.maeMean),
        formatCsvNumber(row.maeStd),
        formatCsvNumber(row.rmseMean),
        formatCsvNumber(row.rmseStd),
        formatCsvNumber(row.mapeMean),
        formatCsvNumber(row.mapeStd),
        row.mapeApplicableWindowsTotal,
        csvEscape(row.missingIterations),
        csvEscape(row.missingArtifacts),
      ].join(","),
    );
  }

  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function formatCsvNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "";
  }

  return Number.isFinite(value) ? value.toFixed(6) : "";
}

function buildSummary(patternSummaries, inputRoot, outputDir, csvPath, expectedPatterns, requiredApproaches, optionalApproaches, smokeMode) {
  const comparisonRows = patternSummaries.flatMap((patternSummary) => patternSummary.comparisons);
  const requiredRows = comparisonRows.filter((row) => requiredApproaches.includes(row.approach));
  const missingRequiredRows = requiredRows.filter((row) => row.status !== "complete");

  return {
    generatedAt: new Date().toISOString(),
    inputRoot,
    outputDir,
    csvPath,
    smokeMode,
    expectedPatterns,
    requiredApproaches,
    optionalApproaches,
    summary: {
      patternCount: patternSummaries.length,
      completePatterns: patternSummaries.filter((pattern) => pattern.status === "complete").length,
      partialPatterns: patternSummaries.filter((pattern) => pattern.status === "partial").length,
      missingRequiredComparisons: missingRequiredRows.length,
      totalMatchedWindows: requiredRows.reduce((sum, row) => sum + (row.counts ? row.counts.matchedWindowsTotal : 0), 0),
      totalBaselineOnlyWindows: requiredRows.reduce((sum, row) => sum + (row.counts ? row.counts.baselineOnlyTotal : 0), 0),
      totalApproachOnlyWindows: requiredRows.reduce((sum, row) => sum + (row.counts ? row.counts.approachOnlyTotal : 0), 0),
    },
    patterns: patternSummaries,
    requiredComparisonRows: requiredRows,
    missingRequiredComparisons: missingRequiredRows,
  };
}

function resolvePatternInputRoot(inputRoot, requiredApproaches, optionalApproaches) {
  const candidates = [
    inputRoot,
    path.join(inputRoot, "raw"),
    path.join(inputRoot, "patterns", "raw"),
  ];

  for (const candidate of candidates) {
    if (!pathExists(candidate)) {
      continue;
    }

    const hasApproachDirectories = requiredApproaches.some((approach) =>
      pathExists(path.join(candidate, approach)),
    ) || optionalApproaches.some((approach) => pathExists(path.join(candidate, approach)));

    if (hasApproachDirectories) {
      return candidate;
    }
  }

  return inputRoot;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const smokeMode = process.env.PAPER_BENCHMARK_SMOKE === "1";
  const expectedPatterns = smokeMode ? SMOKE_PATTERNS : ALL_PATTERNS;
  const requiredApproaches = smokeMode ? SMOKE_REQUIRED_APPROACHES : FULL_REQUIRED_APPROACHES;
  const optionalApproaches = smokeMode ? SMOKE_OPTIONAL_APPROACHES : FULL_OPTIONAL_APPROACHES;
  const resolvedInputRoot = resolvePatternInputRoot(args.inputRoot, requiredApproaches, optionalApproaches);

  const patternSummaries = summarizePatterns(resolvedInputRoot, expectedPatterns, requiredApproaches, optionalApproaches);
  const csvRows = buildCsvRows(patternSummaries);

  ensureDir(args.outputDir);
  const jsonPath = path.join(args.outputDir, "summary.json");
  const csvPath = path.join(args.outputDir, "summary.csv");

  const summary = buildSummary(
    patternSummaries,
    resolvedInputRoot,
    args.outputDir,
    csvPath,
    expectedPatterns,
    requiredApproaches,
    optionalApproaches,
    smokeMode,
  );

  fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`);
  writeCsv(csvPath, csvRows);

  console.log("Custom pattern accuracy summary generated.");
  console.log(`Input root: ${resolvedInputRoot}`);
  console.log(`JSON summary: ${jsonPath}`);
  console.log(`CSV summary: ${csvPath}`);

  const missingRequired = summary.missingRequiredComparisons;
  if (missingRequired.length > 0) {
    if (smokeMode) {
      console.warn("Smoke mode: partial comparison coverage is expected for pipeline validation.");
      for (const row of missingRequired) {
        console.warn(`- ${row.patternType || "unknown"} / ${row.approach}: ${row.inputIssue || row.status}`);
      }
    } else {
      console.warn("Missing required comparison inputs:");
      for (const row of missingRequired) {
        console.warn(`- ${row.patternType} / ${row.approach}: ${row.inputIssue || row.status}`);
      }
      process.exitCode = 1;
    }
  }
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exit(1);
  }
}