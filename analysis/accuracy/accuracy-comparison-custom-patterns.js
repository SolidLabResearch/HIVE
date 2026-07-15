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

function parseSelectionList(value, allowedValues) {
  if (!value) {
    return null;
  }

  const entries = value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);

  if (entries.length === 0) {
    return null;
  }

  const filtered = entries.filter((entry) => allowedValues.includes(entry));
  return [...new Set(filtered)];
}

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
    executionSummaryPath: null,
    selectedPatterns: null,
    selectedApproaches: null,
    selectedIterations: null,
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
      case "--execution-summary":
        if (!next) {
          throw new Error("--execution-summary requires a value");
        }
        args.executionSummaryPath = resolveMaybeRelativePath(next);
        index += 1;
        break;
      case "--patterns":
        args.selectedPatterns = parseSelectionList(
          next,
          ALL_PATTERNS.map((pattern) => pattern.type),
        );
        index += 1;
        break;
      case "--approaches":
        args.selectedApproaches = parseSelectionList(
          next,
          [...FULL_REQUIRED_APPROACHES, ...FULL_OPTIONAL_APPROACHES],
        );
        index += 1;
        break;
      case "--iterations":
        args.selectedIterations = parseIntegerSelectionList(next);
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
  --execution-summary <path>
                        Explicit custom_pattern_comparison_summary.json to use
  --patterns <list>     Comma-separated pattern types to analyze
  --approaches <list>   Comma-separated approaches to analyze
  --iterations <list>   Comma-separated iteration numbers to expect
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

function parseIntegerSelectionList(value) {
  if (!value) {
    return null;
  }

  const parsed = value
    .split(",")
    .map((entry) => Number.parseInt(entry.trim(), 10))
    .filter((entry) => Number.isFinite(entry) && entry > 0);

  if (parsed.length === 0) {
    return null;
  }

  return [...new Set(parsed)].sort((left, right) => left - right);
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
    if (parts.length < 4) {
      continue;
    }

    const timestamp = Number.parseInt(parts[0], 10);
    const windowNumber = Number.parseInt(parts[1], 10);
    const windowStart = Number.parseInt(parts[2], 10);
    const windowEnd = Number.parseInt(parts[3], 10);
    const resultValue = Number.parseFloat(parts[4] ?? parts[2]);
    const latencyValue = parts[5] && parts[5] !== "N/A"
      ? Number.parseFloat(parts[5])
      : (parts[3] && parts[3] !== "N/A" ? Number.parseFloat(parts[3]) : null);

    if (!Number.isFinite(windowNumber) || !Number.isFinite(resultValue)) {
      continue;
    }

    results.push({
      timestamp: Number.isFinite(timestamp) ? timestamp : null,
      windowNumber,
      windowStart: Number.isFinite(windowStart) ? windowStart : null,
      windowEnd: Number.isFinite(windowEnd) ? windowEnd : null,
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

function readIterationMetadata(inputRoot) {
  const candidates = [path.join(inputRoot, "metadata.json")];

  for (const candidate of candidates) {
    const metadata = readMetadataJson(candidate);
    if (metadata) {
      return metadata;
    }
  }

  return null;
}

function readFirstJson(candidates) {
  for (const candidate of candidates) {
    const value = readMetadataJson(candidate);
    if (value) {
      return {
        path: candidate,
        value,
      };
    }
  }

  return null;
}

function readExecutionSummary(inputRoot, explicitSummaryPath = null) {
  const candidates = [];
  if (explicitSummaryPath) {
    candidates.push(explicitSummaryPath);
  }
  candidates.push(
    path.join(inputRoot, "custom_pattern_comparison_summary.json"),
  );

  const resolved = readFirstJson([...new Set(candidates)]);

  if (!resolved || !Array.isArray(resolved.value?.results)) {
    return {
      sourcePath: null,
      summary: null,
      cases: [],
      caseMap: new Map(),
    };
  }

  const cases = resolved.value.results.map((entry) => normalizeExecutionCase(entry));
  const caseMap = new Map(
    cases.map((entry) => [buildExecutionCaseKey(entry.approach, entry.pattern, entry.iteration), entry]),
  );

  return {
    sourcePath: resolved.path,
    summary: resolved.value,
    cases,
    caseMap,
  };
}

function buildExecutionCaseKey(approach, patternType, iterationNumber) {
  return `${approach}::${patternType}::${iterationNumber}`;
}

function normalizeLegacyExecutionSemantics(entry) {
  const rawBenchmarkStatus = entry?.benchmarkStatus || null;
  const rawExtractionStatus = entry?.finalExtractionStatus || entry?.extractionStatus || null;
  const rawTimedOut = Boolean(entry?.timedOut);
  const success = Boolean(entry?.success);
  const reachedDurationLimit = Boolean(entry?.reachedDurationLimit)
    || (rawTimedOut && success && ["success", "skipped"].includes(rawExtractionStatus));

  if (entry?.terminationReason) {
    return {
      benchmarkStatus: rawBenchmarkStatus,
      terminationReason: entry.terminationReason,
      reachedDurationLimit,
      legacyTimedOut: rawTimedOut,
    };
  }

  if (rawBenchmarkStatus === "timedOut") {
    if (success && ["success", "skipped"].includes(rawExtractionStatus)) {
      return {
        benchmarkStatus: "completed",
        terminationReason: "duration_limit_reached",
        reachedDurationLimit: true,
        legacyTimedOut: true,
      };
    }

    return {
      benchmarkStatus: "failed",
      terminationReason: rawTimedOut ? "startup_timeout" : "process_error",
      reachedDurationLimit,
      legacyTimedOut: rawTimedOut,
    };
  }

  return {
    benchmarkStatus: rawBenchmarkStatus,
    terminationReason: rawBenchmarkStatus === "completed" ? "process_exit" : null,
    reachedDurationLimit,
    legacyTimedOut: rawTimedOut,
  };
}

function determineFailureStage(entry) {
  if (entry.success) {
    return null;
  }

  if (entry.extractionStatus && !["success", "skipped"].includes(entry.extractionStatus)) {
    return "extraction";
  }

  if (entry.benchmarkStatus && entry.benchmarkStatus !== "completed") {
    return "benchmark";
  }

  if (entry.error) {
    return "benchmark";
  }

  return "unknown";
}

function buildFailureReason(entry) {
  if (entry.error) {
    return entry.error;
  }

  if (entry.extractionStatus && !["success", "skipped"].includes(entry.extractionStatus)) {
    return `Extraction ${entry.extractionStatus}`;
  }

  if (entry.benchmarkStatus && entry.benchmarkStatus !== "completed") {
    return `Benchmark ${entry.benchmarkStatus}`;
  }

  if (entry.terminationReason && entry.terminationReason !== "duration_limit_reached") {
    return `Termination ${entry.terminationReason}`;
  }

  return null;
}

function normalizeExecutionCase(entry) {
  const iteration = Number.parseInt(entry?.iteration, 10);
  const durationMs = Number.parseInt(entry?.durationMs ?? entry?.duration, 10);
  const finalAttemptNumber = Number.parseInt(entry?.finalAttemptNumber ?? entry?.attemptCount, 10);
  const attemptCount = Number.parseInt(entry?.attemptCount, 10);
  const configuredTimeoutMs = Number.parseInt(entry?.configuredTimeoutMs, 10);
  const normalizedSemantics = normalizeLegacyExecutionSemantics(entry);

  return {
    approach: entry?.approach || null,
    pattern: entry?.pattern || null,
    patternDisplayName: entry?.patternDisplayName || null,
    iteration: Number.isFinite(iteration) ? iteration : null,
    success: Boolean(entry?.success),
    finalStatus: entry?.finalStatus || (entry?.success ? "success" : "failed"),
    benchmarkStatus: normalizedSemantics.benchmarkStatus || null,
    terminationReason: normalizedSemantics.terminationReason,
    extractionStatus: entry?.finalExtractionStatus || entry?.extractionStatus || null,
    timedOut: Boolean(entry?.timedOut),
    legacyTimedOut: normalizedSemantics.legacyTimedOut,
    reachedDurationLimit: normalizedSemantics.reachedDurationLimit,
    exitCode: entry?.exitCode ?? null,
    durationMs: Number.isFinite(durationMs) ? durationMs : null,
    configuredTimeoutMs: Number.isFinite(configuredTimeoutMs) ? configuredTimeoutMs : null,
    logDir: entry?.finalLogDir || entry?.logDir || null,
    finalLogDir: entry?.finalLogDir || entry?.logDir || null,
    finalAttemptNumber: Number.isFinite(finalAttemptNumber) ? finalAttemptNumber : 1,
    attemptCount: Number.isFinite(attemptCount) ? attemptCount : 1,
    retryUsed: Boolean(entry?.retryUsed) || (Number.isFinite(attemptCount) && attemptCount > 1),
    retriesConfigured: Number.parseInt(entry?.retriesConfigured, 10) || 0,
    attempts: Array.isArray(entry?.attempts) ? entry.attempts : [],
    firstFailureReason: entry?.firstFailureReason || null,
    error: entry?.error || null,
    failureStage: determineFailureStage(entry),
    failureReason: buildFailureReason(entry),
  };
}

function getCaseAttemptDir(inputRoot, executionCase) {
  if (!executionCase?.approach || !executionCase?.pattern || !executionCase?.iteration) {
    return null;
  }

  const iterationDir = path.join(
    inputRoot,
    executionCase.approach,
    executionCase.pattern,
    `iteration${executionCase.iteration}`,
  );

  if (executionCase.finalAttemptNumber > 1 || executionCase.retryUsed) {
    return path.join(iterationDir, `attempt${executionCase.finalAttemptNumber}`);
  }

  const attemptOneDir = path.join(iterationDir, "attempt1");
  if (pathExists(attemptOneDir)) {
    return attemptOneDir;
  }

  return iterationDir;
}

function readSelectedNamesFromMetadata(metadata, key) {
  const value = metadata?.cliConfiguration?.[key] || metadata?.[key];
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  return value.map((entry) => String(entry)).filter(Boolean);
}

function getConfiguredIterationCount(metadata) {
  const countCandidates = [
    metadata?.cliConfiguration?.iterations,
    metadata?.iterationCount,
    metadata?.iterations,
  ];

  for (const candidate of countCandidates) {
    const parsed = Number.parseInt(candidate, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return null;
}

function getConfiguredIterationCountFromExecutionSummary(summary) {
  const countCandidates = [
    summary?.iterations,
    summary?.iterationCount,
    summary?.configuredIterationCount,
  ];

  for (const candidate of countCandidates) {
    const parsed = Number.parseInt(candidate, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return null;
}

function buildExpectedIterationNumbers(configuredIterationCount, observedIterations, explicitIterations = null) {
  if (Array.isArray(explicitIterations) && explicitIterations.length > 0) {
    return explicitIterations;
  }

  if (Number.isFinite(configuredIterationCount) && configuredIterationCount > 0) {
    return Array.from({ length: configuredIterationCount }, (_, index) => index + 1);
  }

  return observedIterations;
}

function getIgnoredObservedIterations(observedIterations, expectedIterations) {
  const expectedIterationSet = new Set(expectedIterations || []);
  return observedIterations.filter((iterationNumber) => !expectedIterationSet.has(iterationNumber));
}

function buildExecutionSummaryForCases(expectedCaseCount, executionCases) {
  const observedCases = executionCases.filter((entry) => !entry.missing);
  const failedExecutionCases = observedCases.filter((entry) => !entry.success);
  const missingExecutionCases = executionCases.filter((entry) => entry.missing);
  const successfulCases = observedCases.filter((entry) => entry.success);

  return {
    expectedCaseCount,
    observedCaseCount: observedCases.length,
    successfulCaseCount: successfulCases.length,
    failedCaseCount: failedExecutionCases.length,
    missingCaseCount: missingExecutionCases.length,
    timedOutCaseCount: observedCases.filter((entry) => entry.terminationReason === "startup_timeout").length,
    reachedDurationLimitCaseCount: observedCases.filter((entry) => entry.reachedDurationLimit).length,
    extractionFailureCount: observedCases.filter(
      (entry) => entry.extractionStatus && !["success", "skipped"].includes(entry.extractionStatus),
    ).length,
    nonCompletedBenchmarkCaseCount: observedCases.filter(
      (entry) => entry.benchmarkStatus && entry.benchmarkStatus !== "completed",
    ).length,
    totalObservedDurationMs: observedCases.reduce(
      (sum, entry) => sum + (entry.durationMs || 0),
      0,
    ),
    status:
      missingExecutionCases.length > 0 || failedExecutionCases.length > 0
        ? "partial"
        : (expectedCaseCount > 0 ? "complete" : "missing"),
    failedExecutionCases,
    missingExecutionCases,
  };
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

function compareResults(baselineResults, approachResults, options = {}) {
  const trimStart = options.trimWindowStart;
  const trimEnd = options.trimWindowEnd;
  const methodologyLabel = options.methodologyLabel || "raw";

  let filteredBaseline = baselineResults;
  let filteredApproach = approachResults;

  if (Number.isFinite(trimStart)) {
    filteredBaseline = filteredBaseline.filter((r) => r.windowNumber >= trimStart);
    filteredApproach = filteredApproach.filter((r) => r.windowNumber >= trimStart);
  }
  if (Number.isFinite(trimEnd)) {
    filteredBaseline = filteredBaseline.filter((r) => r.windowNumber <= trimEnd);
    filteredApproach = filteredApproach.filter((r) => r.windowNumber <= trimEnd);
  }

  const baselineByWindow = new Map();
  const approachByWindow = new Map();
  const hasWindowBounds = (result) =>
    Number.isFinite(result.windowStart) && Number.isFinite(result.windowEnd);
  const bothSidesHaveWindowNumbers =
    filteredBaseline.every((result) => Number.isFinite(result.windowNumber)) &&
    filteredApproach.every((result) => Number.isFinite(result.windowNumber));
  const bothSidesHaveWindowBounds =
    filteredBaseline.every((result) => hasWindowBounds(result)) &&
    filteredApproach.every((result) => hasWindowBounds(result));
  const getWindowKey = (result) =>
    bothSidesHaveWindowBounds
      ? `${result.windowStart}:${result.windowEnd}`
      : bothSidesHaveWindowNumbers
        ? `window-number:${result.windowNumber}`
        : hasWindowBounds(result)
          ? `${result.windowStart}:${result.windowEnd}`
          : `window-number:${result.windowNumber}`;

  for (const result of filteredBaseline) {
    baselineByWindow.set(getWindowKey(result), result);
  }

  for (const result of filteredApproach) {
    approachByWindow.set(getWindowKey(result), result);
  }

  const matchedWindowKeys = [...baselineByWindow.keys()]
    .filter((windowKey) => approachByWindow.has(windowKey))
    .sort();

  let sumAbsoluteError = 0;
  let sumSquaredError = 0;
  let sumPercentageError = 0;
  let mapeApplicableWindowCount = 0;

  for (const windowKey of matchedWindowKeys) {
    const baseline = baselineByWindow.get(windowKey).resultValue;
    const approach = approachByWindow.get(windowKey).resultValue;
    const absoluteError = Math.abs(approach - baseline);

    sumAbsoluteError += absoluteError;
    sumSquaredError += absoluteError ** 2;

    if (Math.abs(baseline) > Number.EPSILON) {
      sumPercentageError += (absoluteError / Math.abs(baseline)) * 100;
      mapeApplicableWindowCount += 1;
    }
  }

  const matchedWindowCount = matchedWindowKeys.length;
  const baselineOnlyCount = filteredBaseline.length - matchedWindowCount;
  const approachOnlyCount = filteredApproach.length - matchedWindowCount;

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
    methodologyLabel,
    baselineResultCount: filteredBaseline.length,
    approachResultCount: filteredApproach.length,
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

  function collectFilesRecursively(dirPath, relativePrefix = "") {
    const entries = fs.readdirSync(dirPath);
    for (const entry of entries) {
      const absolutePath = path.join(dirPath, entry);
      const relativePath = relativePrefix ? path.join(relativePrefix, entry) : entry;
      const stat = fs.statSync(absolutePath);
      if (stat.isDirectory()) {
        collectFilesRecursively(absolutePath, relativePath);
        continue;
      }

      observed.add(relativePath);
      missingExpected.delete(entry);
    }
  }

  const iterationNumbers = listIterationNumbers(approachRoot);
  for (const iterationNumber of iterationNumbers) {
    const iterationDir = path.join(approachRoot, `iteration${iterationNumber}`);
    if (!pathExists(iterationDir)) {
      continue;
    }
    collectFilesRecursively(iterationDir, `iteration${iterationNumber}`);
  }

  inventory.observedFiles = [...observed].sort((left, right) => left.localeCompare(right));
  inventory.missingExpectedFiles = [...missingExpected].sort((left, right) => left.localeCompare(right));
  return inventory;
}

function buildPatternSummary(
  inputRoot,
  pattern,
  requiredApproaches,
  optionalApproaches,
  executionContext,
  configuredIterationCountOverride,
  explicitIterations,
  options = {},
) {
  const fetchingRoot = getApproachRoot(inputRoot, "fetching", pattern.type);
  const configuredIterationCount = configuredIterationCountOverride;
  const baselineIterations = listIterationNumbers(fetchingRoot);
  const expectedIterations = buildExpectedIterationNumbers(
    configuredIterationCount,
    baselineIterations,
    explicitIterations,
  );
  const baselineIterationSet = new Set(baselineIterations);
  const baselineInventory = collectFileInventory(fetchingRoot, Object.values(FILE_MAP.fetching));
  const baselineIgnoredObservedIterations = getIgnoredObservedIterations(baselineIterations, expectedIterations);

  const patternSummary = {
    patternType: pattern.type,
    patternDisplayName: pattern.name,
    status: "missing",
    dataStatus: "missing",
    analysisStatus: "missing",
    executionStatus: "missing",
    configuredIterationCount: configuredIterationCount,
    baseline: {
      approach: "fetching",
      rootDir: fetchingRoot,
      iterations: baselineIterations,
      expectedIterations,
      ignoredObservedIterations: baselineIgnoredObservedIterations,
      iterationCount: baselineIterations.length,
      files: baselineInventory,
    },
    comparisons: [],
    optionalApproaches: [],
    inputIssues: [],
    ignoredObservedIterations: [],
    executionSummary: null,
    failedExecutionCases: [],
    missingExecutionCases: [],
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
    const ignoredObservedIterations = getIgnoredObservedIterations(iterationNumbers, expectedIterations);
    const commonIterations = expectedIterations.filter((iterationNumber) =>
      iterationNumbers.includes(iterationNumber) && baselineIterationSet.has(iterationNumber),
    );
    const missingIterations = expectedIterations.filter(
      (iterationNumber) => !commonIterations.includes(iterationNumber),
    );

    const approachSummary = {
      patternType: pattern.type,
      patternDisplayName: pattern.name,
      approach,
      rootDir: approachRoot,
      iterations: iterationNumbers,
      expectedIterations,
      ignoredObservedIterations,
      iterationCount: iterationNumbers.length,
      commonIterations,
      commonIterationCount: commonIterations.length,
      missingIterations,
      files: inventory,
      status: "missing",
      dataStatus: "missing",
      analysisStatus: "missing",
      executionStatus: "missing",
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

    if (ignoredObservedIterations.length > 0) {
      patternSummary.ignoredObservedIterations.push({
        patternType: pattern.type,
        patternDisplayName: pattern.name,
        approach,
        rootDir: approachRoot,
        expectedIterations,
        ignoredObservedIterations,
      });
      if (approach === "fetching") {
        patternSummary.baseline.ignoredObservedIterations = ignoredObservedIterations;
      }
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
      const baselineExecutionCase = executionContext.caseMap.get(
        buildExecutionCaseKey("fetching", pattern.type, iterationNumber),
      );
      const approachExecutionCase = executionContext.caseMap.get(
        buildExecutionCaseKey(approach, pattern.type, iterationNumber),
      );
      const iterationDir = getCaseAttemptDir(inputRoot, approachExecutionCase)
        || path.join(approachRoot, `iteration${iterationNumber}`);
      const baselineIterationDir = getCaseAttemptDir(inputRoot, baselineExecutionCase)
        || path.join(fetchingRoot, `iteration${iterationNumber}`);

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

      const comparison = compareResults(baselineResults, approachResults, options);

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
        baselineResultCount: comparison.baselineResultCount,
        approachResultCount: comparison.approachResultCount,
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
    approachSummary.dataStatus = approachStatus;
    approachSummary.analysisStatus = approachStatus;
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
      patternSummary.baseline.expectedIterations = expectedIterations;
      patternSummary.baseline.ignoredObservedIterations = ignoredObservedIterations;
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

  const expectedExecutionCases = [];
  const executionApproaches = [...new Set(requiredApproaches)];
  for (const approach of executionApproaches) {
    for (const iterationNumber of expectedIterations) {
      const executionCase = executionContext.caseMap.get(
        buildExecutionCaseKey(approach, pattern.type, iterationNumber),
      );
      expectedExecutionCases.push(
        executionCase || {
          approach,
          pattern: pattern.type,
          patternDisplayName: pattern.name,
          iteration: iterationNumber,
          success: false,
          benchmarkStatus: null,
          terminationReason: null,
          extractionStatus: null,
          timedOut: false,
          legacyTimedOut: false,
          reachedDurationLimit: false,
          exitCode: null,
          durationMs: null,
          configuredTimeoutMs: null,
          logDir: null,
          error: null,
          failureStage: "missing",
          failureReason: "No execution case recorded in benchmark summary",
          missing: true,
        },
      );
    }
  }

  const executionSummary = buildExecutionSummaryForCases(
    executionApproaches.length * expectedIterations.length,
    expectedExecutionCases,
  );

  for (const approachSummary of [patternSummary.baseline, ...patternSummary.comparisons, ...patternSummary.optionalApproaches]) {
    if (!approachSummary?.approach) {
      continue;
    }

    const casesForApproach = expectedExecutionCases.filter(
      (entry) => entry.approach === approachSummary.approach,
    );
    const approachExecutionSummary = buildExecutionSummaryForCases(
      expectedIterations.length,
      casesForApproach,
    );

    approachSummary.executionSummary = approachExecutionSummary;
    approachSummary.executionStatus = approachExecutionSummary.status;
    approachSummary.failedExecutionCases = approachExecutionSummary.failedExecutionCases;
    approachSummary.missingExecutionCases = approachExecutionSummary.missingExecutionCases;
  }

  patternSummary.executionSummary = executionSummary;
  patternSummary.failedExecutionCases = executionSummary.failedExecutionCases;
  patternSummary.missingExecutionCases = executionSummary.missingExecutionCases;
  patternSummary.dataStatus = patternHasCompleteRequiredComparison ? "complete" : "partial";
  patternSummary.analysisStatus = patternHasCompleteRequiredComparison ? "complete" : "partial";
  patternSummary.executionStatus = executionSummary.status;
  patternSummary.status =
    patternSummary.dataStatus === "complete" &&
    patternSummary.analysisStatus === "complete" &&
    patternSummary.executionStatus === "complete"
      ? "complete"
      : "partial";
  return patternSummary;
}

function summarizePatterns(
  inputRoot,
  patterns,
  requiredApproaches,
  optionalApproaches,
  executionContext,
  configuredIterationCount,
  explicitIterations,
  options = {},
) {
  return patterns.map((pattern) =>
    buildPatternSummary(
      inputRoot,
      pattern,
      requiredApproaches,
      optionalApproaches,
      executionContext,
      configuredIterationCount,
      explicitIterations,
      options,
    ),
  );
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

function buildSummary(patternSummaries, inputRoot, outputDir, csvPath, expectedPatterns, requiredApproaches, optionalApproaches, smokeMode, executionContext) {
  const comparisonRows = patternSummaries.flatMap((patternSummary) => patternSummary.comparisons);
  const requiredRows = comparisonRows.filter((row) => requiredApproaches.includes(row.approach));
  const missingRequiredRows = requiredRows.filter((row) => row.status !== "complete");
  const ignoredObservedIterations = patternSummaries.flatMap((patternSummary) => patternSummary.ignoredObservedIterations || []);
  const failedExecutionCases = patternSummaries.flatMap(
    (patternSummary) => patternSummary.failedExecutionCases || [],
  );
  const missingExecutionCases = patternSummaries.flatMap(
    (patternSummary) => patternSummary.missingExecutionCases || [],
  );
  const dataCompletePatterns = patternSummaries.filter(
    (pattern) => pattern.dataStatus === "complete",
  ).length;
  const executionCompletePatterns = patternSummaries.filter(
    (pattern) => pattern.executionStatus === "complete",
  ).length;
  const analysisCompletePatterns = patternSummaries.filter(
    (pattern) => pattern.analysisStatus === "complete",
  ).length;

  return {
    generatedAt: new Date().toISOString(),
    inputRoot,
    outputDir,
    csvPath,
    smokeMode,
    configuredIterationCount: patternSummaries[0]?.configuredIterationCount || null,
    expectedPatterns,
    selectedPatterns: patternSummaries.map((pattern) => pattern.patternType),
    selectedApproaches: [
      ...new Set(
        patternSummaries.flatMap((pattern) => [
          pattern.baseline?.approach,
          ...(pattern.comparisons || []).map((comparison) => comparison.approach),
          ...(pattern.optionalApproaches || []).map((comparison) => comparison.approach),
        ]).filter(Boolean),
      ),
    ],
    requiredApproaches,
    optionalApproaches,
    executionSummarySourcePath: executionContext.sourcePath,
    ignoredObservedIterations,
    retryCountConfigured: executionContext.summary?.retryCountConfigured ?? 0,
    retryUsed: Boolean(executionContext.summary?.retryUsed),
    retriedCases: executionContext.summary?.retriedCases || [],
    failedAfterRetries: executionContext.summary?.failedAfterRetries || [],
    executionSummary: {
      totalExpectedCases: patternSummaries.reduce(
        (sum, pattern) => sum + (pattern.executionSummary?.expectedCaseCount || 0),
        0,
      ),
      observedCases: executionContext.cases.length,
      successfulCases: executionContext.cases.filter((entry) => entry.success).length,
      failedCases: failedExecutionCases.length,
      missingCases: missingExecutionCases.length,
      timedOutCases: executionContext.cases.filter((entry) => entry.terminationReason === "startup_timeout").length,
      reachedDurationLimitCases: executionContext.cases.filter((entry) => entry.reachedDurationLimit).length,
      extractionFailures: executionContext.cases.filter(
        (entry) => entry.extractionStatus && !["success", "skipped"].includes(entry.extractionStatus),
      ).length,
      nonCompletedBenchmarkCases: executionContext.cases.filter(
        (entry) => entry.benchmarkStatus && entry.benchmarkStatus !== "completed",
      ).length,
      totalAttempts: executionContext.summary?.totalAttempts ?? executionContext.cases.reduce(
        (sum, entry) => sum + (entry.attemptCount || 1),
        0,
      ),
      totalObservedDurationMs: executionContext.cases.reduce(
        (sum, entry) => sum + (entry.durationMs || 0),
        0,
      ),
      status:
        failedExecutionCases.length > 0 || missingExecutionCases.length > 0
          ? "partial"
          : "complete",
    },
    summary: {
      patternCount: patternSummaries.length,
      completePatterns: patternSummaries.filter((pattern) => pattern.status === "complete").length,
      partialPatterns: patternSummaries.filter((pattern) => pattern.status === "partial").length,
      dataCompletePatterns,
      executionCompletePatterns,
      analysisCompletePatterns,
      executionFailedPatterns: patternSummaries.filter((pattern) => pattern.failedExecutionCases.length > 0).length,
      cleanCompletePatterns: patternSummaries.filter((pattern) => pattern.status === "complete").length,
      retriedCaseCount: (executionContext.summary?.retriedCases || []).length,
      failedAfterRetriesCount: (executionContext.summary?.failedAfterRetries || []).length,
      ignoredObservedIterationCount: ignoredObservedIterations.length,
      missingRequiredComparisons: missingRequiredRows.length,
      totalMatchedWindows: requiredRows.reduce((sum, row) => sum + (row.counts ? row.counts.matchedWindowsTotal : 0), 0),
      totalBaselineOnlyWindows: requiredRows.reduce((sum, row) => sum + (row.counts ? row.counts.baselineOnlyTotal : 0), 0),
      totalApproachOnlyWindows: requiredRows.reduce((sum, row) => sum + (row.counts ? row.counts.approachOnlyTotal : 0), 0),
      validationClean: failedExecutionCases.length === 0 && missingExecutionCases.length === 0 && missingRequiredRows.length === 0,
    },
    patterns: patternSummaries,
    requiredComparisonRows: requiredRows,
    missingRequiredComparisons: missingRequiredRows,
    failedExecutionCases,
    executionFailures: failedExecutionCases,
    missingExecutionCases,
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
  const envSelectedPatternTypes = parseSelectionList(
    process.env.CUSTOM_PATTERN_SELECTED_PATTERNS,
    ALL_PATTERNS.map((pattern) => pattern.type),
  );
  const envSelectedApproachTypes = parseSelectionList(
    process.env.CUSTOM_PATTERN_SELECTED_APPROACHES,
    [...FULL_REQUIRED_APPROACHES, ...FULL_OPTIONAL_APPROACHES],
  );
  const resolvedInputRoot = resolvePatternInputRoot(args.inputRoot, requiredApproaches, optionalApproaches);
  const benchmarkMetadata = readIterationMetadata(resolvedInputRoot);
  const executionContext = readExecutionSummary(resolvedInputRoot, args.executionSummaryPath);
  const metadataSelectedPatterns = readSelectedNamesFromMetadata(benchmarkMetadata, "selectedPatterns");
  const metadataSelectedApproaches = readSelectedNamesFromMetadata(benchmarkMetadata, "selectedApproaches");
  const summarySelectedPatterns = Array.isArray(executionContext.summary?.selectedPatterns)
    ? executionContext.summary.selectedPatterns
    : null;
  const summarySelectedApproaches = Array.isArray(executionContext.summary?.selectedApproaches)
    ? executionContext.summary.selectedApproaches
    : null;
  const selectedPatternTypes = args.selectedPatterns
    || envSelectedPatternTypes
    || metadataSelectedPatterns
    || summarySelectedPatterns;
  const selectedApproachTypes = args.selectedApproaches
    || envSelectedApproachTypes
    || metadataSelectedApproaches
    || summarySelectedApproaches;
  const configuredIterationCount = getConfiguredIterationCount(benchmarkMetadata)
    || getConfiguredIterationCountFromExecutionSummary(executionContext.summary);
  if (!configuredIterationCount) {
    console.warn(
      "Could not determine configured iteration count from benchmark metadata; falling back to observed iteration directories.",
    );
  }

  let effectivePatterns = expectedPatterns;
  if (selectedPatternTypes && selectedPatternTypes.length > 0) {
    effectivePatterns = expectedPatterns.filter((pattern) => selectedPatternTypes.includes(pattern.type));
  } else if (metadataSelectedPatterns && metadataSelectedPatterns.length > 0) {
    effectivePatterns = expectedPatterns.filter((pattern) => metadataSelectedPatterns.includes(pattern.type));
  }

  let effectiveRequiredApproaches = requiredApproaches;
  let effectiveOptionalApproaches = optionalApproaches;
  if (selectedApproachTypes && selectedApproachTypes.length > 0) {
    effectiveRequiredApproaches = requiredApproaches.filter((approach) => selectedApproachTypes.includes(approach));
    effectiveOptionalApproaches = optionalApproaches.filter((approach) => selectedApproachTypes.includes(approach));
  } else if (metadataSelectedApproaches && metadataSelectedApproaches.length > 0) {
    effectiveRequiredApproaches = requiredApproaches.filter((approach) => metadataSelectedApproaches.includes(approach));
    effectiveOptionalApproaches = optionalApproaches.filter((approach) => metadataSelectedApproaches.includes(approach));
  }

  const rawPatternSummaries = summarizePatterns(
    resolvedInputRoot,
    effectivePatterns,
    effectiveRequiredApproaches,
    effectiveOptionalApproaches,
    executionContext,
    configuredIterationCount,
    args.selectedIterations,
    {}
  );
  const rawCsvRows = buildCsvRows(rawPatternSummaries);

  ensureDir(args.outputDir);
  const rawJsonPath = path.join(args.outputDir, "summary.raw.json");
  const rawCsvPath = path.join(args.outputDir, "summary.raw.csv");

  const rawSummary = buildSummary(
    rawPatternSummaries,
    resolvedInputRoot,
    args.outputDir,
    rawCsvPath,
    effectivePatterns,
    effectiveRequiredApproaches,
    effectiveOptionalApproaches,
    smokeMode,
    executionContext,
  );
  rawSummary.selectedPatterns = effectivePatterns.map((pattern) => pattern.type);
  rawSummary.selectedApproaches = selectedApproachTypes || rawSummary.selectedApproaches;
  rawSummary.selectedIterations = buildExpectedIterationNumbers(
    configuredIterationCount,
    [],
    args.selectedIterations,
  );

  const trimmedPatternSummaries = summarizePatterns(
    resolvedInputRoot,
    effectivePatterns,
    effectiveRequiredApproaches,
    effectiveOptionalApproaches,
    executionContext,
    configuredIterationCount,
    args.selectedIterations,
    {
      trimWindowStart: 4,
      trimWindowEnd: 33,
      methodologyLabel: "trimmed-4-33",
    }
  );
  const trimmedCsvRows = buildCsvRows(trimmedPatternSummaries);
  const trimmedJsonPath = path.join(args.outputDir, "summary.trimmed-4-33.json");
  const trimmedCsvPath = path.join(args.outputDir, "summary.trimmed-4-33.csv");

  const trimmedSummary = buildSummary(
    trimmedPatternSummaries,
    resolvedInputRoot,
    args.outputDir,
    trimmedCsvPath,
    effectivePatterns,
    effectiveRequiredApproaches,
    effectiveOptionalApproaches,
    smokeMode,
    executionContext,
  );
  trimmedSummary.selectedPatterns = effectivePatterns.map((pattern) => pattern.type);
  trimmedSummary.selectedApproaches = selectedApproachTypes || trimmedSummary.selectedApproaches;
  trimmedSummary.selectedIterations = buildExpectedIterationNumbers(
    configuredIterationCount,
    [],
    args.selectedIterations,
  );

  const compatJsonPath = path.join(args.outputDir, "summary.json");
  const compatCsvPath = path.join(args.outputDir, "summary.csv");

  const compatSummary = {
    ...trimmedSummary,
    csvPath: compatCsvPath,
  };

  fs.writeFileSync(rawJsonPath, `${JSON.stringify(rawSummary, null, 2)}\n`);
  writeCsv(rawCsvPath, rawCsvRows);
  fs.writeFileSync(trimmedJsonPath, `${JSON.stringify(trimmedSummary, null, 2)}\n`);
  writeCsv(trimmedCsvPath, trimmedCsvRows);
  fs.writeFileSync(compatJsonPath, `${JSON.stringify(compatSummary, null, 2)}\n`);
  writeCsv(compatCsvPath, trimmedCsvRows);

  console.log("Custom pattern accuracy summary generated.");
  console.log(`Input root: ${resolvedInputRoot}`);
  console.log(`JSON summary: ${compatJsonPath}`);
  console.log(`CSV summary: ${compatCsvPath}`);

  if (compatSummary.ignoredObservedIterations.length > 0) {
    const ignoredList = compatSummary.ignoredObservedIterations
      .map((entry) => `${entry.patternType}/${entry.approach}: [${entry.ignoredObservedIterations.join(", ")}]`)
      .join("; ");
    console.warn(`Ignoring stale iteration directories outside configured range: ${ignoredList}`);
  }

  if (compatSummary.failedExecutionCases.length > 0) {
    console.warn("Execution failures were recorded for benchmark cases:");
    for (const entry of compatSummary.failedExecutionCases) {
      console.warn(
        `- ${entry.approach} / ${entry.pattern} / iteration${entry.iteration}: ${entry.failureReason || entry.failureStage || "failed"}`,
      );
    }
    console.warn(
      `Data completeness and execution cleanliness are reported separately. validationClean=${compatSummary.summary.validationClean}`,
    );
  }

  const missingRequired = compatSummary.missingRequiredComparisons;
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
} else {
  module.exports = {
    compareResults,
  };
}
