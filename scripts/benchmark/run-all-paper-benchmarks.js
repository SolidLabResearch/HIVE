#!/usr/bin/env node

const fs = require("fs");
const net = require("net");
const os = require("os");
const path = require("path");
const { spawn, spawnSync } = require("child_process");
const {
  cleanupStaleBenchmarkProcesses,
  terminateChildProcessTree,
} = require("../../experiments/utils/processCleanup");

const REPO_ROOT = path.resolve(__dirname, "..", "..");
const DEFAULT_TIMESTAMP = new Date()
  .toISOString()
  .replace(/[:]/g, "-")
  .replace(/\..+$/, "");

const PATTERN_DIR_MAP = {
  low_variability: "low-variability",
  step_pattern: "step",
  spike_pattern: "spike",
  low_freq_oscillation: "low-frequency-oscillation",
  high_freq_oscillation: "high-frequency-oscillation",
};

const VALID_SUITES = [
  "all",
  "real-data",
  "patterns",
  "latency",
  "resources",
  "accuracy",
  "naive-distributed",
];

const SUITE_TO_JOBS = {
  "real-data": ["real-data-core"],
  patterns: ["custom-pattern-core"],
  latency: ["real-data-core"],
  resources: ["real-data-core"],
  accuracy: ["custom-pattern-core"],
  "naive-distributed": ["real-data-core", "custom-pattern-core"],
};

let activeBenchmarkJobCleanup = null;
let benchmarkSignalHandlersInstalled = false;

function parseArgs(argv) {
  const args = {
    suites: [],
    iterations: 35,
    dropWarmup: 3,
    dropCooldown: 2,
    broker: "mqtt://localhost:1883",
    windowWidth: 120000,
    windowSlide: 60000,
    subWindowRange: 60000,
    subWindowStep: 30000,
    frequency: 4,
    aggregation: "AVG",
    timeout: 300000,
    patternTestTimeout: 300000,
    retries: 0,
    outputDir: `results/paper-benchmarks/${DEFAULT_TIMESTAMP}`,
    outputDirProvided: false,
    dryRun: false,
    failFast: false,
    skipAnalysis: false,
    smoke: false,
    refreshSummaryOnly: null,
    skipBrokerPreflight: false,
    targetWindows: null,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    switch (arg) {
      case "--suite":
        if (!next) {
          throw new Error("--suite requires a value");
        }
        args.suites.push(...next.split(",").map((value) => value.trim()).filter(Boolean));
        index += 1;
        break;
      case "--iterations":
        args.iterations = parseIntegerFlag("--iterations", next);
        index += 1;
        break;
      case "--drop-warmup":
        args.dropWarmup = parseIntegerFlag("--drop-warmup", next);
        index += 1;
        break;
      case "--drop-cooldown":
        args.dropCooldown = parseIntegerFlag("--drop-cooldown", next);
        index += 1;
        break;
      case "--broker":
        args.broker = requireValue("--broker", next);
        index += 1;
        break;
      case "--window-width":
        args.windowWidth = parseIntegerFlag("--window-width", next);
        index += 1;
        break;
      case "--window-slide":
        args.windowSlide = parseIntegerFlag("--window-slide", next);
        index += 1;
        break;
      case "--sub-window-range":
        args.subWindowRange = parseIntegerFlag("--sub-window-range", next);
        index += 1;
        break;
      case "--sub-window-step":
        args.subWindowStep = parseIntegerFlag("--sub-window-step", next);
        index += 1;
        break;
      case "--frequency":
        args.frequency = parseNumberFlag("--frequency", next);
        index += 1;
        break;
      case "--aggregation":
        args.aggregation = requireValue("--aggregation", next).toUpperCase();
        index += 1;
        break;
      case "--timeout":
        args.timeout = parseIntegerFlag("--timeout", next);
        index += 1;
        break;
      case "--pattern-test-timeout":
        args.patternTestTimeout = parseIntegerFlag("--pattern-test-timeout", next);
        index += 1;
        break;
      case "--retries":
        args.retries = parseIntegerFlag("--retries", next);
        index += 1;
        break;
      case "--patterns":
        args.patterns = parseCsvListFlag("--patterns", next, Object.keys(PATTERN_DIR_MAP));
        index += 1;
        break;
      case "--approaches":
        args.approaches = parseCsvListFlag("--approaches", next, [
          "fetching",
          "naive_distributed",
          "approximation",
          "chunked",
        ]);
        index += 1;
        break;
      case "--target-windows":
        args.targetWindows = parseIntegerFlag("--target-windows", next);
        index += 1;
        break;
      case "--output-dir":
        args.outputDir = requireValue("--output-dir", next);
        args.outputDirProvided = true;
        index += 1;
        break;
      case "--dry-run":
        args.dryRun = true;
        break;
      case "--fail-fast":
        args.failFast = true;
        break;
      case "--skip-analysis":
        args.skipAnalysis = true;
        break;
      case "--smoke":
        args.smoke = true;
        break;
      case "--refresh-summary-only":
        args.refreshSummaryOnly = requireValue("--refresh-summary-only", next);
        index += 1;
        break;
      case "--skip-broker-preflight":
        args.skipBrokerPreflight = true;
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

  if (args.suites.length === 0) {
    args.suites = ["all"];
  }

  args.outputDir = resolveOutputDir(args.outputDir, args.smoke, args.outputDirProvided);
  args.suites = expandSuites(args.suites);
  return args;
}

function printHelp() {
  console.log(`Usage: node scripts/benchmark/run-all-paper-benchmarks.js [options]

Options:
  --suite <name>              all | real-data | patterns | latency | resources | accuracy | naive-distributed
  --iterations <n>            Default: 35
  --drop-warmup <n>           Default: 3
  --drop-cooldown <n>         Default: 2
  --broker <url>              Default: mqtt://localhost:1883
  --window-width <ms>         Default: 120000
  --window-slide <ms>         Default: 60000
  --sub-window-range <ms>     Default: 60000
  --sub-window-step <ms>      Default: 30000
  --frequency <hz>            Default replay frequency: 4
  --aggregation <type>        Default: AVG
  --timeout <ms>              Default: 300000
  --pattern-test-timeout <ms> Default: 300000
  --retries <n>               Retry failed custom-pattern cases N additional times
  --patterns <list>           Comma-separated custom-pattern types
  --approaches <list>         Comma-separated approaches
  --target-windows <n>        Override real-data target window count
  --output-dir <path>         Default: results/paper-benchmarks/<timestamp>
                              or results/paper-benchmarks/smoke-<timestamp> with --smoke
  --dry-run                   Print commands without executing them
  --fail-fast                 Stop on the first failed benchmark command
  --skip-analysis             Skip custom-pattern post-analysis after benchmarks
  --skip-broker-preflight     Skip only the MQTT broker preflight probe
  --smoke                     Run a reduced custom-pattern pipeline smoke test
  --refresh-summary-only <path>
                              Refresh top-level summary.json from existing outputs only
  --help                      Show this help
`);
}

function requireValue(flag, value) {
  if (!value) {
    throw new Error(`${flag} requires a value`);
  }
  return value;
}

function parseIntegerFlag(flag, value) {
  const parsed = Number.parseInt(requireValue(flag, value), 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${flag} must be an integer`);
  }
  return parsed;
}

function parseNumberFlag(flag, value) {
  const parsed = Number.parseFloat(requireValue(flag, value));
  if (!Number.isFinite(parsed)) {
    throw new Error(`${flag} must be a number`);
  }
  return parsed;
}

function parseCsvListFlag(flag, value, allowedValues = null) {
  const items = requireValue(flag, value)
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);

  if (items.length === 0) {
    throw new Error(`${flag} requires at least one comma-separated value`);
  }

  if (allowedValues) {
    const invalid = items.filter((item) => !allowedValues.includes(item));
    if (invalid.length > 0) {
      throw new Error(`${flag} contains unsupported value(s): ${invalid.join(", ")}`);
    }
  }

  return [...new Set(items)];
}

function expandSuites(inputSuites) {
  const suites = [];
  for (const suite of inputSuites) {
    if (!VALID_SUITES.includes(suite)) {
      throw new Error(`Unsupported suite: ${suite}`);
    }
    if (suite === "all") {
      suites.push("real-data", "patterns", "latency", "resources", "accuracy", "naive-distributed");
      continue;
    }
    suites.push(suite);
  }
  return [...new Set(suites)];
}

function resolveOutputDir(outputDir, smoke, outputDirProvided) {
  const withTimestamp = outputDir.replace(/<timestamp>/g, DEFAULT_TIMESTAMP);
  if (smoke && !outputDirProvided) {
    const defaultDir = `results/paper-benchmarks/${DEFAULT_TIMESTAMP}`;
    if (withTimestamp === defaultDir) {
      return `results/paper-benchmarks/smoke-${DEFAULT_TIMESTAMP}`;
    }
  }
  return path.isAbsolute(withTimestamp)
    ? withTimestamp
    : path.join(REPO_ROOT, withTimestamp);
}

function fileExists(relativePath) {
  return fs.existsSync(path.join(REPO_ROOT, relativePath));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function copyFileIfExists(sourcePath, destinationPath) {
  if (!fs.existsSync(sourcePath)) {
    return false;
  }
  ensureDir(path.dirname(destinationPath));
  fs.copyFileSync(sourcePath, destinationPath);
  return true;
}

function formatCommandLine(commandParts) {
  return commandParts
    .map((part) => {
      if (part === "") {
        return '""';
      }
      return /\s/.test(part) ? JSON.stringify(part) : part;
    })
    .join(" ");
}

function copyDirectory(sourceDir, destinationDir) {
  if (!fs.existsSync(sourceDir)) {
    return false;
  }
  ensureDir(path.dirname(destinationDir));
  fs.cpSync(sourceDir, destinationDir, { recursive: true });
  return true;
}

function readPatternRunSummary(sourceRoot) {
  return readJsonIfExists(path.join(sourceRoot, "custom_pattern_comparison_summary.json"));
}

function getPatternCaseIterationSourceDir(sourceRoot, result) {
  return path.join(sourceRoot, result.approach, result.pattern, `iteration${result.iteration}`);
}

function copyPatternIterationArtifactsForCase(sourceRoot, outputDir, result) {
  if (result.finalStatus !== "success") {
    return null;
  }

  const finalLogDir = result.finalLogDir || result.logDir;
  if (!finalLogDir || !fs.existsSync(finalLogDir)) {
    return null;
  }

  const finalLogDirRelative = path.relative(sourceRoot, finalLogDir);
  if (finalLogDirRelative.startsWith("..")) {
    return null;
  }

  const iterationDestination = path.join(
    outputDir,
    "patterns",
    "raw",
    result.approach,
    result.pattern,
    `iteration${result.iteration}`,
    `attempt${Number.isFinite(Number.parseInt(result.finalAttemptNumber, 10)) ? Number.parseInt(result.finalAttemptNumber, 10) : 1}`,
  );

  fs.rmSync(iterationDestination, { recursive: true, force: true });
  copyDirectory(finalLogDir, iterationDestination);

  return {
    iterationSource: finalLogDir,
    iterationDestination,
    sourceLogDirs: [path.relative(REPO_ROOT, finalLogDir)],
    usedAttemptDirs: true,
  };
}

function mirrorPatternCaseIntoViews(outputDir, result) {
  const patternDir = PATTERN_DIR_MAP[result.pattern] || result.pattern.replaceAll("_", "-");
  const rawIterationDir = path.join(
    outputDir,
    "patterns",
    "raw",
    result.approach,
    result.pattern,
    `iteration${result.iteration}`,
  );

  if (!fs.existsSync(rawIterationDir)) {
    return;
  }

  const structuredDestination = path.join(
    outputDir,
    "patterns",
    patternDir,
    result.approach,
    `iteration${result.iteration}`,
  );
  fs.rmSync(structuredDestination, { recursive: true, force: true });
  copyDirectory(rawIterationDir, structuredDestination);

  const accuracyDestination = path.join(
    outputDir,
    "accuracy",
    "patterns",
    patternDir,
    result.approach,
    `iteration${result.iteration}`,
  );
  fs.rmSync(accuracyDestination, { recursive: true, force: true });
  copyDirectory(rawIterationDir, accuracyDestination);

  if (result.approach === "naive_distributed") {
    const naiveDestination = path.join(
      outputDir,
      "naive-distributed",
      "patterns",
      patternDir,
      result.approach,
      `iteration${result.iteration}`,
    );
    fs.rmSync(naiveDestination, { recursive: true, force: true });
    copyDirectory(rawIterationDir, naiveDestination);
  }
}

function buildPatternReportingOverview(outputDir) {
  const accuracySummaryPath = path.join(
    outputDir,
    "accuracy",
    "patterns",
    "custom-pattern-accuracy",
    "summary.json",
  );
  const executionSummaryPathCandidates = [
    path.join(outputDir, "patterns", "raw", "custom_pattern_comparison_summary.json"),
    path.join(outputDir, "patterns", "summary.json"),
  ];
  const executionSummaryPath = executionSummaryPathCandidates.find((candidate) => fs.existsSync(candidate));
  const accuracySummary = readJsonIfExists(accuracySummaryPath);
  const executionSummary = executionSummaryPath ? readJsonIfExists(executionSummaryPath) : null;

  if (!accuracySummary && !executionSummary) {
    return null;
  }

  const failedExecutionCases = accuracySummary?.failedExecutionCases
    || executionSummary?.results?.filter((entry) => !entry.success)
    || [];

  return {
    configuredIterationCount: accuracySummary?.configuredIterationCount
      ?? executionSummary?.iterations
      ?? null,
    retryCountConfigured: accuracySummary?.retryCountConfigured
      ?? executionSummary?.retryCountConfigured
      ?? 0,
    retryUsed: accuracySummary?.retryUsed
      ?? executionSummary?.retryUsed
      ?? false,
    retriedCases: accuracySummary?.retriedCases
      ?? executionSummary?.retriedCases
      ?? [],
    failedAfterRetries: accuracySummary?.failedAfterRetries
      ?? executionSummary?.failedAfterRetries
      ?? [],
    dataCompletePatterns: accuracySummary?.summary?.dataCompletePatterns ?? null,
    executionCompletePatterns: accuracySummary?.summary?.executionCompletePatterns ?? null,
    analysisCompletePatterns: accuracySummary?.summary?.analysisCompletePatterns ?? null,
    cleanCompletePatterns: accuracySummary?.summary?.cleanCompletePatterns
      ?? accuracySummary?.summary?.completePatterns
      ?? null,
    validationClean: accuracySummary?.summary?.validationClean ?? null,
    executionFailedCaseCount: failedExecutionCases.length,
    failedExecutionCases,
    executionSummary: accuracySummary?.executionSummary || null,
    executionSummarySourcePath: executionSummaryPath
      ? path.relative(outputDir, executionSummaryPath)
      : null,
    accuracySummaryPath: fs.existsSync(accuracySummaryPath)
      ? path.relative(outputDir, accuracySummaryPath)
      : null,
  };
}

function attachPatternReporting(summary, outputDir) {
  const patternReporting = buildPatternReportingOverview(outputDir);
  if (!patternReporting) {
    return summary;
  }

  return {
    ...summary,
    patternReporting,
  };
}

function refreshExistingSummary(outputDir) {
  const summaryPath = path.join(outputDir, "summary.json");
  const existingSummary = readJsonIfExists(summaryPath);

  if (!existingSummary) {
    throw new Error(`Existing summary not found or invalid: ${summaryPath}`);
  }

  const refreshedSummary = attachPatternReporting(existingSummary, outputDir);
  writeJson(summaryPath, refreshedSummary);
  return summaryPath;
}

function buildIterationList(iterationCount, startIteration = 1, endIteration = iterationCount) {
  if (!Number.isFinite(iterationCount) || iterationCount <= 0) {
    return [];
  }

  const firstIteration = Math.max(1, startIteration);
  const lastIteration = Math.min(iterationCount, endIteration);
  if (lastIteration < firstIteration) {
    return [];
  }

  return Array.from(
    { length: lastIteration - firstIteration + 1 },
    (_, index) => firstIteration + index,
  );
}

function buildTrimmedIterationSelection(config) {
  const iterations = buildIterationList(
    config.iterations,
    config.dropWarmup + 1,
    config.iterations - config.dropCooldown,
  );

  return {
    iterations,
    startIteration: iterations[0] || null,
    endIteration: iterations[iterations.length - 1] || null,
    label: iterations.length > 0
      ? `trimmed-${iterations[0]}-${iterations[iterations.length - 1]}`
      : "trimmed-empty",
  };
}

function buildPatternAnalysisJob(config, options = {}) {
  const analysisArgs = [
    "analysis/accuracy/accuracy-comparison-custom-patterns.js",
    "--input-root",
    options.inputRoot || path.join(config.outputDir, "patterns", "raw"),
    "--output-dir",
    options.outputDir || path.join(config.outputDir, "accuracy", "patterns", "custom-pattern-accuracy"),
    "--execution-summary",
    options.executionSummary || path.join(config.outputDir, "patterns", "raw", "custom_pattern_comparison_summary.json"),
  ];

  if (config.patterns && config.patterns.length > 0) {
    analysisArgs.push("--patterns", config.patterns.join(","));
  }

  if (config.approaches && config.approaches.length > 0) {
    analysisArgs.push("--approaches", config.approaches.join(","));
  }

  if (Array.isArray(options.iterations) && options.iterations.length > 0) {
    analysisArgs.push("--iterations", options.iterations.join(","));
  } else if (Number.isFinite(config.iterations) && config.iterations > 0) {
    const explicitIterations = Array.from({ length: config.iterations }, (_, index) => String(index + 1));
    analysisArgs.push("--iterations", explicitIterations.join(","));
  }

  return {
    name: options.name || "custom-pattern-accuracy-analysis",
    description: options.description || "Aggregate custom-pattern accuracy against the fetching baseline",
    command: [
      "node",
      analysisArgs,
    ],
    env: buildBenchmarkEnv(config),
    outputDir: options.outputDir || path.join(config.outputDir, "accuracy", "patterns", "custom-pattern-accuracy"),
    summaryTag: options.summaryTag || null,
  };
}

function buildPatternAnalysisJobs(config) {
  const rawIterations = buildIterationList(config.iterations);
  const trimmedSelection = buildTrimmedIterationSelection(config);
  const baseOutputDir = path.join(config.outputDir, "accuracy", "patterns", "custom-pattern-accuracy");
  const rawOutputDir = path.join(baseOutputDir, "_analysis", `raw-${config.iterations}`);

  return {
    raw: buildPatternAnalysisJob(config, {
      name: "custom-pattern-accuracy-raw",
      description: "Aggregate custom-pattern accuracy against the fetching baseline using all iterations",
      outputDir: rawOutputDir,
      inputRoot: path.join(config.outputDir, "patterns", "raw"),
      executionSummary: path.join(config.outputDir, "patterns", "raw", "custom_pattern_comparison_summary.json"),
      iterations: rawIterations.map(String),
      summaryTag: `raw-${config.iterations}`,
    }),
    trimmed: buildPatternAnalysisJob(config, {
      name: "custom-pattern-accuracy-trimmed",
      description: `Aggregate custom-pattern accuracy against the fetching baseline using kept iterations ${trimmedSelection.startIteration}-${trimmedSelection.endIteration}`,
      outputDir: baseOutputDir,
      inputRoot: path.join(config.outputDir, "patterns", "raw"),
      executionSummary: path.join(config.outputDir, "patterns", "raw", "custom_pattern_comparison_summary.json"),
      iterations: trimmedSelection.iterations.map(String),
      summaryTag: trimmedSelection.label,
    }),
    trimmedSelection,
    rawOutputDir,
    baseOutputDir,
  };
}

function copyAnalysisSummaryArtifacts(sourceDir, destinationDir, tag) {
  ensureDir(destinationDir);
  const jsonSource = path.join(sourceDir, "summary.json");
  const csvSource = path.join(sourceDir, "summary.csv");
  const jsonDestination = path.join(destinationDir, `summary.${tag}.json`);
  const csvDestination = path.join(destinationDir, `summary.${tag}.csv`);

  const copiedJson = copyFileIfExists(jsonSource, jsonDestination);
  const copiedCsv = copyFileIfExists(csvSource, csvDestination);

  return {
    jsonSource: copiedJson ? jsonSource : null,
    csvSource: copiedCsv ? csvSource : null,
    jsonDestination: copiedJson ? jsonDestination : null,
    csvDestination: copiedCsv ? csvDestination : null,
  };
}

function getSelectedCustomPatternCounts(config) {
  const patterns = config.patterns && config.patterns.length > 0
    ? config.patterns
    : Object.keys(PATTERN_DIR_MAP);
  const approaches = config.approaches && config.approaches.length > 0
    ? config.approaches
    : ["fetching", "naive_distributed", "approximation", "chunked"];

  return {
    patterns,
    approaches,
    testCount: patterns.length * approaches.length * config.iterations,
  };
}

function formatEstimatedRuntime(config) {
  const counts = getSelectedCustomPatternCounts(config);
  const estimatedMs = counts.testCount * config.patternTestTimeout;
  const estimatedMinutes = estimatedMs / 60000;

  return {
    counts,
    estimatedMs,
    estimatedMinutes,
    display: estimatedMinutes >= 60
      ? `${(estimatedMinutes / 60).toFixed(1)} hours`
      : `${Math.ceil(estimatedMinutes)} minutes`,
  };
}

function listIterationDirs(rootDir) {
  if (!fs.existsSync(rootDir)) {
    return [];
  }
  return fs
    .readdirSync(rootDir)
    .filter((entry) => entry.startsWith("iteration"))
    .sort((left, right) => left.localeCompare(right, undefined, { numeric: true }));
}

function getGitCommitHash() {
  const result = spawnSync("git", ["rev-parse", "HEAD"], {
    cwd: REPO_ROOT,
    encoding: "utf8",
  });
  return result.status === 0 ? result.stdout.trim() : null;
}

function checkDependency(moduleName) {
  try {
    require.resolve(moduleName, { paths: [REPO_ROOT] });
    return true;
  } catch {
    return false;
  }
}

function parseBrokerUrl(brokerUrl) {
  const url = new URL(brokerUrl);
  const protocol = url.protocol;
  const defaultPort = protocol === "mqtts:" ? 8883 : 1883;
  return {
    host: url.hostname,
    port: Number(url.port || defaultPort),
    protocol,
  };
}

function checkBrokerReachable(brokerUrl, timeoutMs = 2000) {
  const { host, port, protocol } = parseBrokerUrl(brokerUrl);
  const candidates = host === "localhost" ? ["127.0.0.1", "::1"] : [host];
  const startTimestamp = Date.now();
  console.log(
    `[preflight:mqtt-broker] brokerUrl=${brokerUrl} parsedProtocol=${protocol} parsedHostname=${host} parsedPort=${port} candidates=${candidates.join(",")} timeoutMs=${timeoutMs} startTimestamp=${startTimestamp}`,
  );

  const probeCandidate = (candidateHost) =>
    new Promise((resolve) => {
      const candidateStartTimestamp = Date.now();
      let settled = false;
      const socket = net.createConnection({ host: candidateHost, port });

      const settle = (result) => {
        if (settled) {
          console.log(
            `[preflight:mqtt-broker] duplicate-settle-ignored candidate=${candidateHost}`,
          );
          return;
        }
        settled = true;
        socket.destroy();
        resolve(result);
      };

      socket.setTimeout(timeoutMs);
      socket.on("connect", () => {
        const connectTimestamp = Date.now();
        console.log(
          `[preflight:mqtt-broker] candidate=${candidateHost} connect timestamp=${connectTimestamp} elapsedMs=${connectTimestamp - candidateStartTimestamp}`,
        );
        settle({ ok: true, candidate: candidateHost });
      });
      socket.on("timeout", () => {
        const timeoutTimestamp = Date.now();
        const elapsedMs = timeoutTimestamp - candidateStartTimestamp;
        console.log(
          `[preflight:mqtt-broker] candidate=${candidateHost} timeout timestamp=${timeoutTimestamp} elapsedMs=${elapsedMs}`,
        );
        settle({
          ok: false,
          candidate: candidateHost,
          message: `Timed out connecting to ${candidateHost}:${port} for ${brokerUrl}`,
        });
      });
      socket.on("error", (error) => {
        const errorTimestamp = Date.now();
        const elapsedMs = errorTimestamp - candidateStartTimestamp;
        console.log(
          `[preflight:mqtt-broker] candidate=${candidateHost} error timestamp=${errorTimestamp} elapsedMs=${elapsedMs} name=${error.name || ""} code=${error.code || ""} message=${error.message || ""} stack=${error.stack || ""}`,
        );
        settle({
          ok: false,
          candidate: candidateHost,
          message: error.message || `Failed connecting to ${candidateHost}:${port}`,
          error,
        });
      });
    });

  return (async () => {
    const failures = [];
    for (const candidateHost of candidates) {
      const result = await probeCandidate(candidateHost);
      if (result.ok) {
        const finishTimestamp = Date.now();
        console.log(
          `[preflight:mqtt-broker] success candidate=${result.candidate} finishTimestamp=${finishTimestamp} elapsedMs=${finishTimestamp - startTimestamp}`,
        );
        return {
          ok: true,
          message: `Connected to ${brokerUrl} via ${result.candidate}:${port}`,
        };
      }
      failures.push(`${result.candidate}: ${result.message}`);
    }

    const finishTimestamp = Date.now();
    console.log(
      `[preflight:mqtt-broker] all-candidates-failed finishTimestamp=${finishTimestamp} elapsedMs=${finishTimestamp - startTimestamp} failures=${failures.join(" | ")}`,
    );
    return {
      ok: false,
      message: `Failed to connect to ${brokerUrl}; tried ${candidates.join(", ")}; ${failures.join(" | ")}`,
    };
  })();
}

function buildBenchmarkEnv(config) {
  const smokeWindowWidth = 30000;
  const smokeWindowSlide = 15000;
  const smokeSubWindowRange = 30000;
  const smokeSubWindowStep = 15000;
  const benchmarkFiniteReplayDurationSeconds = Math.ceil(
    ((config.smoke ? smokeWindowWidth : config.windowWidth) +
      (3 * (config.smoke ? smokeWindowSlide : config.windowSlide))) / 1000,
  );
  const skipBrokerPreflight = config.skipBrokerPreflight || process.env.STREAMING_QUERY_HIVE_SKIP_BROKER_PREFLIGHT === "1";

  return {
    ...process.env,
    AGGREGATION_FUNCTION: config.aggregation,
    AGGREGATION_FUNC: config.aggregation,
    OUTPUT_WINDOW_RANGE: String(config.smoke ? smokeWindowWidth : config.windowWidth),
    OUTPUT_WINDOW_STEP: String(config.smoke ? smokeWindowSlide : config.windowSlide),
    SUB_WINDOW_RANGE: String(config.smoke ? smokeSubWindowRange : config.subWindowRange),
    SUB_WINDOW_STEP: String(config.smoke ? smokeSubWindowStep : config.subWindowStep),
    WEARABLE_FREQUENCY: String(config.frequency),
    MQTT_BROKER_URL: config.broker,
    CUSTOM_PATTERN_TEST_TIMEOUT_MS: String(config.patternTestTimeout),
    CUSTOM_PATTERN_RETRIES: String(config.retries),
    PAPER_BENCHMARK_SMOKE: config.smoke ? "1" : "0",
    STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY_DURATION_SECONDS: String(benchmarkFiniteReplayDurationSeconds),
    STREAMING_QUERY_HIVE_SKIP_BROKER_PREFLIGHT: skipBrokerPreflight ? "1" : "0",
    CUSTOM_PATTERN_SELECTED_PATTERNS: config.patterns ? config.patterns.join(",") : "",
    CUSTOM_PATTERN_SELECTED_APPROACHES: config.approaches ? config.approaches.join(",") : "",
  };
}

function createJobDefinitions(config) {
  const sharedEnv = buildBenchmarkEnv(config);
  return {
    "real-data-core": {
      name: "real-data-core",
      description: "Real-world DAHCC 4-approach comparison",
      suites: ["real-data", "latency", "resources", "naive-distributed"],
      command: [
        "node",
        [
          "experiments/real-data-comparison/run-real-data-4-approaches.js",
          "--iterations",
          String(config.iterations),
          ...(config.approaches && config.approaches.length > 0
            ? ["--approaches", config.approaches.join(",")]
            : []),
          ...(Number.isFinite(config.targetWindows) && config.targetWindows > 0
            ? ["--target-windows", String(config.targetWindows)]
            : []),
        ],
      ],
      env: {
        ...sharedEnv,
        ...(Number.isFinite(config.targetWindows) && config.targetWindows > 0
          ? { STREAMING_QUERY_HIVE_BENCHMARK_TARGET_WINDOWS: String(config.targetWindows) }
          : {}),
      },
      sourceLogs: path.join(REPO_ROOT, "experiments/real-data-comparison/logs"),
      snapshot(outputDir) {
        return snapshotRealDataArtifacts(this.sourceLogs, outputDir);
      },
    },
    "custom-pattern-core": {
      name: "custom-pattern-core",
      description: "Synthetic custom-pattern 4-approach comparison",
      suites: ["patterns", "accuracy", "naive-distributed"],
      command: [
        "node",
        [
          "experiments/pattern-analysis/run-custom-patterns-comparison.js",
          "--iterations",
          String(config.iterations),
          "--pattern-test-timeout",
          String(config.patternTestTimeout),
          "--retries",
          String(config.retries),
        ],
      ],
      env: {
        ...sharedEnv,
        STREAMING_QUERY_HIVE_BENCHMARK_FINITE_REPLAY: "1",
      },
      sourceLogs: path.join(REPO_ROOT, "logs/custom-pattern-comparison"),
      snapshot(outputDir) {
        return snapshotPatternArtifacts(this.sourceLogs, outputDir);
      },
    },
  };
}

function snapshotRealDataArtifacts(sourceRoot, outputDir) {
  copyDirectory(sourceRoot, path.join(outputDir, "real-data", "raw"));

  const approachDirs = fs.existsSync(sourceRoot)
    ? fs.readdirSync(sourceRoot).filter((entry) => fs.statSync(path.join(sourceRoot, entry)).isDirectory())
    : [];

  for (const approach of approachDirs) {
    const approachSource = path.join(sourceRoot, approach);
    const iterationDirs = listIterationDirs(approachSource);
    for (const iterationDir of iterationDirs) {
      const iterationSource = path.join(approachSource, iterationDir);
      const latencyDestination = path.join(outputDir, "latency", "real-data", approach, iterationDir);
      const resourceDestination = path.join(outputDir, "resources", "real-data", approach, iterationDir);
      const naiveDestination = path.join(outputDir, "naive-distributed", "real-data", approach, iterationDir);

      const files = fs.readdirSync(iterationSource);
      for (const fileName of files) {
        const sourceFile = path.join(iterationSource, fileName);
        if (!fs.statSync(sourceFile).isFile()) {
          continue;
        }
        if (fileName.includes("latency")) {
          copyFileIfExists(sourceFile, path.join(latencyDestination, fileName));
        }
        if (fileName.includes("resource")) {
          copyFileIfExists(sourceFile, path.join(resourceDestination, fileName));
        }
        if (approach === "naive_distributed") {
          copyFileIfExists(sourceFile, path.join(naiveDestination, fileName));
        }
      }
    }
  }
}

function snapshotPatternArtifacts(sourceRoot, outputDir) {
  const patternSummary = readPatternRunSummary(sourceRoot);
  if (!patternSummary || !Array.isArray(patternSummary.results)) {
    return {
      copiedCaseCount: 0,
      copiedSourceLogDirs: [],
      copiedCases: [],
      sourceSummaryPath: null,
    };
  }

  const copiedSourceLogDirs = [];
  const copiedCases = [];

  for (const result of patternSummary.results) {
    if (!result?.approach || !result?.pattern || !result?.iteration) {
      continue;
    }

    const snapshotRecord = copyPatternIterationArtifactsForCase(sourceRoot, outputDir, result);
    if (!snapshotRecord) {
      continue;
    }

    mirrorPatternCaseIntoViews(outputDir, result);

    copiedSourceLogDirs.push(...snapshotRecord.sourceLogDirs);
    copiedCases.push({
      approach: result.approach,
      pattern: result.pattern,
      iteration: result.iteration,
      finalStatus: result.finalStatus || (result.success ? "success" : "failed"),
      finalLogDir: result.finalLogDir || result.logDir || null,
      sourceIterationDir: path.relative(REPO_ROOT, snapshotRecord.iterationSource),
      copiedAttemptDirectories: snapshotRecord.sourceLogDirs,
    });
  }

  const summaryDestination = path.join(outputDir, "patterns", "raw", "custom_pattern_comparison_summary.json");
  writeJson(summaryDestination, patternSummary);
  copyFileIfExists(summaryDestination, path.join(outputDir, "patterns", "summary.json"));
  copyFileIfExists(summaryDestination, path.join(outputDir, "accuracy", "patterns-summary.json"));

  return {
    copiedCaseCount: copiedCases.length,
    copiedSourceLogDirs: [...new Set(copiedSourceLogDirs)].sort((left, right) => left.localeCompare(right)),
    copiedCases,
    sourceSummaryPath: path.relative(REPO_ROOT, path.join(sourceRoot, "custom_pattern_comparison_summary.json")),
  };
}

function installBenchmarkSignalHandlers() {
  if (benchmarkSignalHandlersInstalled) {
    return;
  }

  benchmarkSignalHandlersInstalled = true;

  const handleSignal = (signal, exitCode) => {
    void (async () => {
      if (activeBenchmarkJobCleanup) {
        console.log(`Received ${signal}; cleaning up benchmark job...`);
        const cleanup = activeBenchmarkJobCleanup;
        activeBenchmarkJobCleanup = null;
        await cleanup();
      }
      process.exit(exitCode);
    })();
  };

  process.on("SIGINT", () => handleSignal("SIGINT", 130));
  process.on("SIGTERM", () => handleSignal("SIGTERM", 143));
}

async function runCommand(job, logDir, dryRun) {
  const stdoutPath = path.join(logDir, `${job.name}.stdout.log`);
  const stderrPath = path.join(logDir, `${job.name}.stderr.log`);
  const combinedPath = path.join(logDir, `${job.name}.combined.log`);
  const commandLine = formatCommandLine([job.command[0], ...job.command[1]]);

  if (dryRun) {
    return {
      ok: true,
      commandLine,
      code: 0,
      stdoutPath,
      stderrPath,
      combinedPath,
      dryRun: true,
      durationMs: 0,
    };
  }

  ensureDir(logDir);
  const stdoutStream = fs.createWriteStream(stdoutPath);
  const stderrStream = fs.createWriteStream(stderrPath);
  const combinedStream = fs.createWriteStream(combinedPath);

  return new Promise((resolve) => {
    const startedAt = Date.now();
    let settled = false;
    let child = null;
    const finalize = async (result) => {
      if (settled) {
        return;
      }
      settled = true;
      if (activeBenchmarkJobCleanup === cleanupJob) {
        activeBenchmarkJobCleanup = null;
      }
      stdoutStream.end();
      stderrStream.end();
      combinedStream.end();
      resolve(result);
    };
    const cleanupJob = () => terminateChildProcessTree(child, {
      name: job.name,
      logger: (message) => console.log(message),
    });

    activeBenchmarkJobCleanup = cleanupJob;

    child = spawn(job.command[0], job.command[1], {
      cwd: REPO_ROOT,
      env: job.env,
      stdio: ["ignore", "pipe", "pipe"],
      detached: true,
    });

    child.stdout.on("data", (chunk) => {
      stdoutStream.write(chunk);
      combinedStream.write(chunk);
      process.stdout.write(chunk);
    });

    child.stderr.on("data", (chunk) => {
      stderrStream.write(chunk);
      combinedStream.write(chunk);
      process.stderr.write(chunk);
    });

    child.on("close", (code) => {
      void (async () => {
        await cleanupJob();
        await finalize({
          ok: code === 0,
          code,
          commandLine,
          stdoutPath,
          stderrPath,
          combinedPath,
          durationMs: Date.now() - startedAt,
        });
      })();
    });

    child.on("error", (error) => {
      void (async () => {
        stderrStream.write(`${error.stack || error.message}\n`);
        combinedStream.write(`${error.stack || error.message}\n`);
        await cleanupJob();
        await finalize({
          ok: false,
          code: null,
          commandLine,
          stdoutPath,
          stderrPath,
          combinedPath,
          durationMs: Date.now() - startedAt,
          error: error.message,
        });
      })();
    });
  });
}

async function runPreflight(config, jobs) {
  const checks = [];

  checks.push({
    name: "node-version",
    ok: Number.parseInt(process.versions.node.split(".")[0], 10) >= 18,
    detail: `Detected Node.js ${process.versions.node}`,
  });

  const requiredScripts = [
    "experiments/real-data-comparison/run-real-data-4-approaches.js",
    "experiments/pattern-analysis/run-custom-patterns-comparison.js",
    "experiments/pattern-analysis/extract-pattern-results.js",
    "analysis/accuracy/accuracy-comparison-custom-patterns.js",
    "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator.js",
    "dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator.js",
    "dist/approaches/StreamingQueryApproximationApproachOrchestrator.js",
    "dist/approaches/StreamingQueryChunkedApproachOrchestrator.js",
    "dist/streamer/src/publish.js",
  ];

  for (const relativePath of requiredScripts) {
    checks.push({
      name: `script:${relativePath}`,
      ok: fileExists(relativePath),
      detail: relativePath,
    });
  }

  const requiredData = [
    "src/streamer/data/smartphone.acceleration.x/data.nt",
    "src/streamer/data/wearable.acceleration.x/data.nt",
  ];

  const selectedPatterns = config.patterns && config.patterns.length > 0
    ? config.patterns
    : (config.smoke ? ["low_variability"] : Object.keys(PATTERN_DIR_MAP));

  for (const patternName of selectedPatterns) {
    requiredData.push(`src/streamer/data/custom_patterns/${patternName}/smartphone.acceleration.x/data.nt`);
  }

  for (const relativePath of requiredData) {
    checks.push({
      name: `data:${relativePath}`,
      ok: fileExists(relativePath),
      detail: relativePath,
    });
  }

  checks.push({
    name: "dependency:mqtt",
    ok: checkDependency("mqtt"),
    detail: "mqtt",
  });
  checks.push({
    name: "dependency:csv-parse/sync",
    ok: checkDependency("csv-parse/sync"),
    detail: "csv-parse/sync",
  });

  if (!config.dryRun) {
    try {
      ensureDir(config.outputDir);
      checks.push({
        name: "output-dir",
        ok: true,
        detail: config.outputDir,
      });
    } catch (error) {
      checks.push({
        name: "output-dir",
        ok: false,
        detail: error.message,
      });
    }
  }

  const skipBrokerPreflight = config.skipBrokerPreflight || process.env.STREAMING_QUERY_HIVE_SKIP_BROKER_PREFLIGHT === "1";
  if (skipBrokerPreflight) {
    console.warn(
      `[preflight:mqtt-broker] skipped broker=${config.broker} reason=skip-flag-or-env`,
    );
    checks.push({
      name: "mqtt-broker",
      ok: true,
      skipped: true,
      detail: `Skipped broker preflight for ${config.broker}`,
    });
  } else {
    const brokerReachability = await checkBrokerReachable(config.broker);
    console.log(
      `[preflight:mqtt-broker] runPreflightChecks broker=${config.broker} status=${brokerReachability.ok ? "ok" : "failed"} message=${brokerReachability.message || ""}`,
    );
    checks.push({
      name: "mqtt-broker",
      ok: brokerReachability.ok,
      detail: brokerReachability.ok
        ? `Connected to ${config.broker}`
        : brokerReachability.message,
    });
  }

  if (config.broker !== "mqtt://localhost:1883") {
    checks.push({
      name: "mqtt-broker-limit",
      ok: false,
      detail:
        "The current repo hardcodes mqtt://localhost:1883 inside the orchestrators and publisher; non-default brokers are not fully supported by the existing scripts.",
      warning: true,
    });
  }

  const requiredJobs = jobs.map((job) => ({
    name: `job:${job.name}`,
    ok: true,
    detail: job.description,
  }));

  return [...checks, ...requiredJobs];
}

async function main() {
  const config = parseArgs(process.argv.slice(2));
  installBenchmarkSignalHandlers();
  if (config.refreshSummaryOnly) {
    const refreshTarget = path.isAbsolute(config.refreshSummaryOnly)
      ? config.refreshSummaryOnly
      : path.join(REPO_ROOT, config.refreshSummaryOnly);
    const summaryPath = refreshExistingSummary(refreshTarget);
    console.log(`Refreshed summary: ${summaryPath}`);
    return;
  }
  const jobsByName = createJobDefinitions(config);
  const requiredJobNames = [...new Set(config.suites.flatMap((suite) => SUITE_TO_JOBS[suite] || []))];
  const jobs = requiredJobNames.map((name) => jobsByName[name]);

  if (!config.dryRun) {
    ensureDir(config.outputDir);
    ensureDir(path.join(config.outputDir, "logs"));
    ensureDir(path.join(config.outputDir, "real-data"));
    ensureDir(path.join(config.outputDir, "patterns"));
    ensureDir(path.join(config.outputDir, "latency"));
    ensureDir(path.join(config.outputDir, "resources"));
    ensureDir(path.join(config.outputDir, "accuracy"));
    ensureDir(path.join(config.outputDir, "naive-distributed"));
    await cleanupStaleBenchmarkProcesses({
      logger: (message) => console.log(message),
      quiescenceMs: 500,
    });
  }

  const metadata = {
    gitCommitHash: getGitCommitHash(),
    timestamp: new Date().toISOString(),
    nodeVersion: process.version,
    os: {
      platform: os.platform(),
      release: os.release(),
      arch: os.arch(),
    },
    cliConfiguration: config,
    selectedPatterns: config.patterns && config.patterns.length > 0 ? config.patterns : (config.smoke ? ["low_variability"] : Object.keys(PATTERN_DIR_MAP)),
    selectedApproaches: config.approaches && config.approaches.length > 0 ? config.approaches : ["fetching", "naive_distributed", "approximation", "chunked"],
    selectedSuites: config.suites,
    iterationCount: config.iterations,
    warmupRemoval: config.dropWarmup,
    cooldownRemoval: config.dropCooldown,
    mqttBroker: config.broker,
    windowConfiguration: {
      outputWindowRangeMs: config.windowWidth,
      outputWindowStepMs: config.windowSlide,
      subWindowRangeMs: config.subWindowRange,
      subWindowStepMs: config.subWindowStep,
    },
    frequencyHz: config.frequency,
    timeoutMs: config.timeout,
    patternTestTimeoutMs: config.patternTestTimeout,
    retryCountConfigured: config.retries,
    aggregation: config.aggregation,
    smokeMode: config.smoke,
    paperConfiguration: {
      broker: config.broker,
      windowWidth: config.windowWidth,
      windowSlide: config.windowSlide,
      subWindowRange: config.subWindowRange,
      subWindowStep: config.subWindowStep,
      frequency: config.frequency,
      aggregation: config.aggregation,
      iterations: config.iterations,
      dropWarmup: config.dropWarmup,
      dropCooldown: config.dropCooldown,
      timeout: config.timeout,
      patternTestTimeout: config.patternTestTimeout,
    },
    failedBenchmarkCommands: [],
    copiedPatternLogSources: [],
    warnings: [],
  };

  const preflightChecks = await runPreflight(config, jobs);
  metadata.preflightChecks = preflightChecks;
  metadata.warnings.push(
    "Warm-up and cool-down dropping are applied by the paper analysis step; raw 35-iteration benchmark outputs are preserved alongside trimmed summaries.",
  );
  metadata.warnings.push(
    "ASF is represented by sub-window range and step parameters because the repo does not expose a dedicated ASF sweep in the existing benchmark scripts.",
  );
  if (config.smoke) {
    metadata.warnings.push(
      "Smoke mode trims the custom-pattern workload to a reduced subset for pipeline validation only.",
    );
  }

  if (!config.dryRun) {
    writeJson(path.join(config.outputDir, "metadata.json"), metadata);
  }

  const blockingFailures = preflightChecks.filter((check) => !check.ok && !check.warning);
  if (blockingFailures.length > 0 && !config.dryRun) {
    const summary = {
      suitesRan: [],
      suitesSucceeded: [],
      suitesFailed: config.suites,
      resultPaths: {},
      runtimePerSuiteMs: {},
      totalRuntimeMs: 0,
      preflightFailed: true,
    };
    writeJson(path.join(config.outputDir, "summary.json"), summary);
    console.error("Preflight failed:");
    for (const failure of blockingFailures) {
      console.error(`- ${failure.name}: ${failure.detail}`);
    }
    process.exit(1);
  }

  if (config.dryRun) {
    console.log("Dry run plan:");
    const runtimeEstimate = formatEstimatedRuntime(config);
    console.log(
      `Custom-pattern filter: patterns=${runtimeEstimate.counts.patterns.join(",")}; approaches=${runtimeEstimate.counts.approaches.join(",")}`,
    );
    console.log(
      `Estimated custom-pattern runtime: ${runtimeEstimate.display} (${runtimeEstimate.counts.testCount} tests × ${config.patternTestTimeout} ms timeout)`,
    );
    for (const job of jobs) {
      const commandLine = formatCommandLine([job.command[0], ...job.command[1]]);
      console.log(`- ${job.name}: ${commandLine}`);
      if (job.name === "custom-pattern-core") {
        console.log(
          `  env: CUSTOM_PATTERN_SELECTED_PATTERNS=${job.env.CUSTOM_PATTERN_SELECTED_PATTERNS || "all"} CUSTOM_PATTERN_SELECTED_APPROACHES=${job.env.CUSTOM_PATTERN_SELECTED_APPROACHES || "all"}`,
        );
        console.log(`  retries: ${config.retries}`);
      }
    }
    if (config.suites.includes("patterns") && !config.skipAnalysis) {
      const analysisJobs = buildPatternAnalysisJobs(config);
      console.log(`- ${analysisJobs.raw.name}: ${formatCommandLine([analysisJobs.raw.command[0], ...analysisJobs.raw.command[1]])}`);
      console.log(`- ${analysisJobs.trimmed.name}: ${formatCommandLine([analysisJobs.trimmed.command[0], ...analysisJobs.trimmed.command[1]])}`);
    }
  }

  const overallStartedAt = Date.now();
  const jobResults = {};

  for (const job of jobs) {
    const result = await runCommand(job, path.join(config.outputDir, "logs"), config.dryRun);
    jobResults[job.name] = result;

    if (!result.ok) {
      metadata.failedBenchmarkCommands.push({
        job: job.name,
        command: result.commandLine,
        exitCode: result.code,
        error: result.error || null,
        combinedLog: path.relative(config.outputDir, result.combinedPath),
      });
      if (!config.dryRun) {
        writeJson(path.join(config.outputDir, "metadata.json"), metadata);
      }

      if (config.failFast) {
        break;
      }
    }

    if (!config.dryRun) {
      const snapshotInfo = job.snapshot(config.outputDir);
      if (job.name === "custom-pattern-core" && snapshotInfo) {
        metadata.copiedPatternLogSources = snapshotInfo.copiedSourceLogDirs || [];
        metadata.copiedPatternCases = snapshotInfo.copiedCases || [];
        metadata.patternSummarySourcePath = snapshotInfo.sourceSummaryPath || null;
        writeJson(path.join(config.outputDir, "metadata.json"), metadata);
      }
    }
  }

  let analysisResult = null;
  let analysisRuns = [];
  if (config.suites.includes("patterns") && !config.skipAnalysis) {
    const analysisJobs = buildPatternAnalysisJobs(config);

    for (const analysisJob of [analysisJobs.raw, analysisJobs.trimmed]) {
      const result = await runCommand(analysisJob, path.join(config.outputDir, "logs"), config.dryRun);
      jobResults[analysisJob.name] = result;
      analysisRuns.push({
        name: analysisJob.name,
        summaryTag: analysisJob.summaryTag,
        outputDir: analysisJob.outputDir,
        ...result,
      });

      if (!result.ok) {
        metadata.failedAnalysisCommands = metadata.failedAnalysisCommands || [];
        metadata.failedAnalysisCommands.push({
          job: analysisJob.name,
          command: result.commandLine,
          exitCode: result.code,
          error: result.error || null,
          combinedLog: path.relative(config.outputDir, result.combinedPath),
        });
        if (!config.dryRun) {
          writeJson(path.join(config.outputDir, "metadata.json"), metadata);
        }

        if (config.failFast) {
          process.exitCode = 1;
          return;
        }
        continue;
      }

      if (!config.dryRun) {
        copyAnalysisSummaryArtifacts(
          analysisJob.outputDir,
          analysisJobs.baseOutputDir,
          analysisJob.summaryTag,
        );
      }
    }

    if (analysisRuns.length > 0) {
      analysisResult = analysisRuns.find((run) => String(run.summaryTag || "").startsWith("trimmed-"))
        || analysisRuns[analysisRuns.length - 1];
      metadata.analysisRuns = analysisRuns.map((run) => ({
        name: run.name,
        summaryTag: run.summaryTag,
        outputDir: path.relative(config.outputDir, run.outputDir),
        ok: run.ok,
        code: run.code,
        durationMs: run.durationMs,
        dryRun: Boolean(run.dryRun),
        command: run.commandLine,
      }));
    }
  }

  const analysisFailed = analysisRuns.some((run) => !run.ok);

  const runtimePerSuiteMs = {};
  const resultPaths = {};
  const suitesSucceeded = [];
  const suitesFailed = [];

  for (const suite of config.suites) {
    const suiteJobs = SUITE_TO_JOBS[suite] || [];
    runtimePerSuiteMs[suite] = suiteJobs.reduce(
      (sum, jobName) => sum + (jobResults[jobName]?.durationMs || 0),
      0,
    );
    const suiteOk = suiteJobs.every((jobName) => jobResults[jobName]?.ok);
    if (suiteOk) {
      suitesSucceeded.push(suite);
    } else {
      suitesFailed.push(suite);
    }
  }

  resultPaths["real-data"] = config.suites.includes("real-data")
    ? "real-data/raw"
    : null;
  resultPaths.patterns = config.suites.includes("patterns")
    ? "patterns"
    : null;
  resultPaths.latency = config.suites.includes("latency")
    ? "latency"
    : null;
  resultPaths.resources = config.suites.includes("resources")
    ? "resources"
    : null;
  resultPaths.accuracy = config.suites.includes("accuracy")
    ? "accuracy"
    : null;
  resultPaths["naive-distributed"] = config.suites.includes("naive-distributed")
    ? "naive-distributed"
    : null;
  resultPaths.patternAccuracy = config.suites.includes("patterns") && !config.skipAnalysis
    ? path.join("accuracy", "patterns", "custom-pattern-accuracy")
    : null;

  const summary = attachPatternReporting({
    suitesRan: config.suites,
    suitesSucceeded,
    suitesFailed,
    resultPaths,
    runtimePerSuiteMs,
    totalRuntimeMs: Date.now() - overallStartedAt,
    jobs: Object.fromEntries(
      Object.entries(jobResults).map(([name, result]) => [
        name,
        {
          ok: result.ok,
          command: result.commandLine,
          durationMs: result.durationMs,
          dryRun: Boolean(result.dryRun),
        },
      ]),
    ),
    analysis: analysisResult
      ? {
          ok: analysisResult.ok,
          command: analysisResult.commandLine,
          durationMs: analysisResult.durationMs,
          dryRun: Boolean(analysisResult.dryRun),
          outputPath: resultPaths.patternAccuracy,
          summaryTag: analysisResult.summaryTag || null,
        }
      : null,
    analysisFailed,
  }, config.outputDir);

  if (!config.dryRun) {
    writeJson(path.join(config.outputDir, "metadata.json"), metadata);
    writeJson(path.join(config.outputDir, "summary.json"), summary);
  }

  if ((suitesFailed.length > 0 || analysisFailed) && !config.dryRun) {
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
} else {
  module.exports = {
    parseArgs,
    buildBenchmarkEnv,
    buildPatternAnalysisJob,
    buildPatternAnalysisJobs,
    buildTrimmedIterationSelection,
    buildIterationList,
    formatCommandLine,
    createJobDefinitions,
    runCommand,
  };
}
