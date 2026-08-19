#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const { compareResults } = require("../accuracy/accuracy-comparison-custom-patterns.js");

const SELECTED_APPROACHES = ["fetching", "approximation", "chunked"];
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
const FILE_MAP = {
  fetching: {
    results: "fetching_results.csv",
    metadata: "fetching_metadata.json",
  },
  approximation: {
    results: "approximation_results.csv",
    metadata: "approximation_metadata.json",
  },
  chunked: {
    results: "chunked_results.csv",
    metadata: "chunked_metadata.json",
  },
};
const MODE_DEFAULTS = {
  "steady-state": {
    expectedIterations: 1,
    targetWindows: 35,
    trimWindowStart: 4,
    trimWindowEnd: 33,
  },
  "startup-cost": {
    expectedIterations: 35,
    targetWindows: 1,
    trimWindowStart: 1,
    trimWindowEnd: 1,
  },
};

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

function parseCsvList(value, flag) {
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  const values = value.split(",").map((entry) => entry.trim()).filter(Boolean);
  if (values.length === 0) {
    throw new Error(`${flag} requires a value`);
  }
  return [...new Set(values)];
}

function parseArgs(argv) {
  const args = {
    mode: null,
    inputRoot: null,
    outputPath: null,
    expectedIterations: null,
    targetWindows: null,
    approaches: [...SELECTED_APPROACHES],
    patterns: [...DEFAULT_PATTERNS],
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
        args.approaches = parseCsvList(next, "--approaches");
        index += 1;
        break;
      case "--patterns":
        args.patterns = parseCsvList(next, "--patterns");
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
  console.log(`Usage: node analysis/pattern-analysis/generate-custom-pattern-3approach-metrics.js [options]

Options:
  --mode <steady-state|startup-cost>
  --input-root <path>           Custom-pattern logs root
  --output <path>               Markdown report path
  --expected-iterations <n>     Override expected iterations
  --target-windows <n>          Override expected target windows
  --approaches <list>           Comma-separated approaches
  --patterns <list>             Comma-separated patterns
`);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJson(filePath) {
  return fs.existsSync(filePath)
    ? JSON.parse(fs.readFileSync(filePath, "utf8"))
    : null;
}

function readResultsCsv(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }

  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) {
    return [];
  }

  const lines = content.split(/\r?\n/);
  const rows = [];
  for (const line of lines.slice(1)) {
    if (!line.trim()) {
      continue;
    }
    const [
      timestamp,
      windowNumber,
      windowStart,
      windowEnd,
      resultValue,
      elapsedSinceRegistrationMs,
      delayPastExpectedCloseMs,
    ] = line.split(",");
    rows.push({
      timestamp: Number(timestamp),
      windowNumber: Number(windowNumber),
      windowStart: Number(windowStart),
      windowEnd: Number(windowEnd),
      resultValue: Number(resultValue),
      elapsedSinceRegistrationMs: Number(elapsedSinceRegistrationMs),
      delayPastExpectedCloseMs: Number(delayPastExpectedCloseMs),
    });
  }
  return rows.filter((row) => Number.isFinite(row.windowNumber) && Number.isFinite(row.resultValue));
}

function mean(values) {
  const clean = values.filter((value) => Number.isFinite(value));
  if (clean.length === 0) {
    return null;
  }
  return clean.reduce((sum, value) => sum + value, 0) / clean.length;
}

function sampleStd(values) {
  const clean = values.filter((value) => Number.isFinite(value));
  if (clean.length < 2) {
    return clean.length === 1 ? 0 : null;
  }
  const avg = mean(clean);
  const variance = clean.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (clean.length - 1);
  return Math.sqrt(variance);
}

function summarize(values) {
  const clean = values.filter((value) => Number.isFinite(value));
  return {
    count: clean.length,
    mean: mean(clean),
    std: sampleStd(clean),
    min: clean.length > 0 ? Math.min(...clean) : null,
    max: clean.length > 0 ? Math.max(...clean) : null,
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

function resolveLogsRoot(inputRoot) {
  const candidates = [
    inputRoot,
    path.join(inputRoot, "patterns", "raw"),
    path.join(inputRoot, "raw"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, "fetching"))) {
      return candidate;
    }
  }

  throw new Error(`Could not resolve custom-pattern logs root from ${inputRoot}`);
}

function listIterations(logsRoot, approach, pattern) {
  const patternRoot = path.join(logsRoot, approach, pattern);
  if (!fs.existsSync(patternRoot)) {
    return [];
  }

  return fs.readdirSync(patternRoot)
    .filter((entry) => /^iteration\d+$/.test(entry))
    .map((entry) => Number.parseInt(entry.replace("iteration", ""), 10))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right);
}

function resolveIterationDir(logsRoot, approach, pattern, iteration) {
  return path.join(logsRoot, approach, pattern, `iteration${iteration}`);
}

function buildTable(headers, rows) {
  return [
    `| ${headers.join(" | ")} |`,
    `| ${headers.map(() => "---").join(" | ")} |`,
    ...rows.map((row) => `| ${row.join(" | ")} |`),
  ].join("\n");
}

function formatNumber(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : "n/a";
}

function formatMeanStd(summary, digits = 3) {
  if (!summary) {
    return "n/a";
  }
  return `${formatNumber(summary.mean, digits)} ± ${formatNumber(summary.std, digits)}`;
}

function analyzeCustomPatternResults(options) {
  const logsRoot = resolveLogsRoot(options.inputRoot);
  const expected = buildExpectedConfig(options.mode, {
    expectedIterations: options.expectedIterations,
    targetWindows: options.targetWindows,
  });
  const selectedApproaches = options.approaches || SELECTED_APPROACHES;
  const selectedPatterns = options.patterns || DEFAULT_PATTERNS;
  const errors = [];
  const perPattern = [];

  for (const pattern of selectedPatterns) {
    const fetchingIterations = listIterations(logsRoot, "fetching", pattern);
    const patternSummary = {
      pattern,
      approaches: [],
    };

    for (const approach of selectedApproaches) {
      const iterations = listIterations(logsRoot, approach, pattern);
      if (iterations.length !== expected.expectedIterations) {
        errors.push(`${pattern}/${approach}: expected ${expected.expectedIterations} iterations, found ${iterations.length}`);
      }

      const startupLatencyValues = [];
      const steadyLatencyValues = [];
      const cpuSecondsValues = [];
      const meanRssValues = [];
      const peakRssValues = [];
      const perIterationAccuracy = [];

      for (const iteration of iterations) {
        const iterationDir = resolveIterationDir(logsRoot, approach, pattern, iteration);
        const baselineDir = resolveIterationDir(logsRoot, "fetching", pattern, iteration);
        const resultRows = readResultsCsv(path.join(iterationDir, FILE_MAP[approach].results));
        const baselineRows = readResultsCsv(path.join(baselineDir, FILE_MAP.fetching.results));
        const metadata = readJson(path.join(iterationDir, FILE_MAP[approach].metadata)) || {};
        const resourceSummary = readJson(path.join(iterationDir, "resource_summary.json")) || {};

        cpuSecondsValues.push(resourceSummary.cpuSeconds);
        meanRssValues.push(resourceSummary.meanRssMb);
        peakRssValues.push(resourceSummary.peakRssMb);

        if (options.mode === "startup-cost") {
          const firstRow = resultRows.find((row) => row.windowNumber === 1);
          const baselineFirstRow = baselineRows.find((row) => row.windowNumber === 1);
          if (!firstRow || resultRows.length !== 1) {
            errors.push(`${pattern}/${approach}/iteration${iteration}: expected exactly one first-window result`);
            continue;
          }
          startupLatencyValues.push(
            Number.isFinite(firstRow.elapsedSinceRegistrationMs)
              ? firstRow.elapsedSinceRegistrationMs
              : metadata.firstEventLatency,
          );
          if (approach !== "fetching" && baselineFirstRow) {
            perIterationAccuracy.push(compareResults([baselineFirstRow], [firstRow], {
              trimWindowStart: 1,
              trimWindowEnd: 1,
            }));
          }
          continue;
        }

        if (resultRows.length !== expected.targetWindows) {
          errors.push(`${pattern}/${approach}/iteration${iteration}: expected ${expected.targetWindows} windows, found ${resultRows.length}`);
        }

        const trimmedRows = resultRows.filter(
          (row) => row.windowNumber >= expected.trimWindowStart && row.windowNumber <= expected.trimWindowEnd,
        );
        steadyLatencyValues.push(
          ...trimmedRows.map((row) => row.delayPastExpectedCloseMs).filter((value) => Number.isFinite(value)),
        );

        if (approach !== "fetching") {
          perIterationAccuracy.push(compareResults(
            baselineRows,
            resultRows,
            {
              trimWindowStart: expected.trimWindowStart,
              trimWindowEnd: expected.trimWindowEnd,
            },
          ));
        }
      }

      patternSummary.approaches.push({
        approach,
        iterationsFound: iterations.length,
        startupLatency: summarize(startupLatencyValues),
        steadyLatency: summarize(steadyLatencyValues),
        cpuSeconds: summarize(cpuSecondsValues),
        meanRssMb: summarize(meanRssValues),
        peakRssMb: summarize(peakRssValues),
        accuracy: approach === "fetching"
          ? null
          : {
            matchedWindowCount: perIterationAccuracy.reduce((sum, row) => sum + (row.matchedWindowCount || 0), 0),
            baselineOnlyCount: perIterationAccuracy.reduce((sum, row) => sum + (row.baselineOnlyCount || 0), 0),
            approachOnlyCount: perIterationAccuracy.reduce((sum, row) => sum + (row.approachOnlyCount || 0), 0),
            mae: mean(perIterationAccuracy.map((row) => row.mae)),
            rmse: mean(perIterationAccuracy.map((row) => row.rmse)),
            mape: mean(perIterationAccuracy.map((row) => row.mape)),
          },
      });
    }

    perPattern.push(patternSummary);
  }

  const presentApproaches = [...new Set(
    perPattern.flatMap((pattern) =>
      pattern.approaches
        .filter((entry) => entry.iterationsFound > 0)
        .map((entry) => entry.approach)
    ),
  )];

  return {
    mode: options.mode,
    logsRoot,
    selectedApproaches,
    selectedPatterns,
    expected,
    selectedApproachesExact:
      presentApproaches.length === selectedApproaches.length
      && selectedApproaches.every((approach) => presentApproaches.includes(approach)),
    perPattern,
    errors,
  };
}

function renderReport(result) {
  const rows = [];
  for (const patternSummary of result.perPattern) {
    for (const approachSummary of patternSummary.approaches) {
      const latencySummary = result.mode === "startup-cost"
        ? approachSummary.startupLatency
        : approachSummary.steadyLatency;
      rows.push([
        `\`${patternSummary.pattern}\``,
        `\`${approachSummary.approach}\``,
        String(approachSummary.iterationsFound),
        String(latencySummary.count),
        formatMeanStd(latencySummary, 3),
        formatMeanStd(approachSummary.cpuSeconds, 3),
        formatMeanStd(approachSummary.meanRssMb, 2),
        formatMeanStd(approachSummary.peakRssMb, 2),
        approachSummary.accuracy ? formatNumber(approachSummary.accuracy.mae, 6) : "0.000000",
        approachSummary.accuracy ? formatNumber(approachSummary.accuracy.rmse, 6) : "0.000000",
        approachSummary.accuracy ? formatNumber(approachSummary.accuracy.mape, 6) : "0.000000",
      ]);
    }
  }

  const table = buildTable(
    [
      "Pattern",
      "Approach",
      "Iterations",
      result.mode === "startup-cost" ? "First-window samples" : "Windows used",
      result.mode === "startup-cost" ? "Startup latency mean ± sd ms" : "Steady latency mean ± sd ms",
      "CPU seconds mean ± sd",
      "Mean RSS MiB mean ± sd",
      "Peak RSS MiB mean ± sd",
      "MAE vs fetching",
      "RMSE vs fetching",
      "MAPE vs fetching",
    ],
    rows,
  );

  const errors = result.errors.length > 0
    ? result.errors.map((error) => `- ${error}`).join("\n")
    : "- none";

  return `# Custom-Pattern 3-Approach ${result.mode === "startup-cost" ? "35x1 Startup-Cost" : "35-Window Steady-State"} Summary

Input root:
\`${result.logsRoot}\`

Selected approaches exact:
\`${result.selectedApproachesExact ? "yes" : "no"}\`

Selected patterns:
\`${result.selectedPatterns.join(", ")}\`

Method:
- startup-cost mode: first and only completed window per independent iteration
- steady-state mode: windows 4..33 only, with windows 1..3 and 34..35 excluded

## Metrics

${table}

## Errors

${errors}
`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const result = analyzeCustomPatternResults(args);
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
    analyzeCustomPatternResults,
    buildExpectedConfig,
    renderReport,
    resolveLogsRoot,
  };
}
