#!/usr/bin/env node

const fs = require("fs");
const net = require("net");
const os = require("os");
const path = require("path");
const { spawn, spawnSync } = require("child_process");

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
    patternTestTimeout: 240000,
    outputDir: `results/paper-benchmarks/${DEFAULT_TIMESTAMP}`,
    outputDirProvided: false,
    dryRun: false,
    failFast: false,
    skipAnalysis: false,
    smoke: false,
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
  --pattern-test-timeout <ms> Default: 240000
  --output-dir <path>         Default: results/paper-benchmarks/<timestamp>
                              or results/paper-benchmarks/smoke-<timestamp> with --smoke
  --dry-run                   Print commands without executing them
  --fail-fast                 Stop on the first failed benchmark command
  --skip-analysis             Skip custom-pattern post-analysis after benchmarks
  --smoke                     Run a reduced custom-pattern pipeline smoke test
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

function copyFileIfExists(sourcePath, destinationPath) {
  if (!fs.existsSync(sourcePath)) {
    return false;
  }
  ensureDir(path.dirname(destinationPath));
  fs.copyFileSync(sourcePath, destinationPath);
  return true;
}

function copyDirectory(sourceDir, destinationDir) {
  if (!fs.existsSync(sourceDir)) {
    return false;
  }
  ensureDir(path.dirname(destinationDir));
  fs.cpSync(sourceDir, destinationDir, { recursive: true });
  return true;
}

function buildPatternAnalysisJob(config) {
  return {
    name: "custom-pattern-accuracy-analysis",
    description: "Aggregate custom-pattern accuracy against the fetching baseline",
    command: [
      "node",
      [
        "analysis/accuracy/accuracy-comparison-custom-patterns.js",
        "--input-root",
        path.join(config.outputDir, "patterns", "raw"),
        "--output-dir",
        path.join(config.outputDir, "accuracy", "patterns", "custom-pattern-accuracy"),
      ],
    ],
    env: buildBenchmarkEnv(config),
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
  };
}

function checkBrokerReachable(brokerUrl, timeoutMs = 2000) {
  return new Promise((resolve) => {
    let settled = false;
    const { host, port } = parseBrokerUrl(brokerUrl);
    const socket = net.createConnection({ host, port });

    const finish = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(result);
    };

    socket.setTimeout(timeoutMs);
    socket.on("connect", () => finish({ ok: true }));
    socket.on("timeout", () => finish({ ok: false, message: `Timed out connecting to ${brokerUrl}` }));
    socket.on("error", (error) => finish({ ok: false, message: error.message }));
  });
}

function buildBenchmarkEnv(config) {
  const smokeWindowWidth = 30000;
  const smokeWindowSlide = 15000;
  const smokeSubWindowRange = 30000;
  const smokeSubWindowStep = 15000;

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
    PAPER_BENCHMARK_SMOKE: config.smoke ? "1" : "0",
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
        ["experiments/real-data-comparison/run-real-data-4-approaches.js", "--iterations", String(config.iterations)],
      ],
      env: sharedEnv,
      sourceLogs: path.join(REPO_ROOT, "experiments/real-data-comparison/logs"),
      snapshot(outputDir) {
        snapshotRealDataArtifacts(this.sourceLogs, outputDir);
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
        ],
      ],
      env: sharedEnv,
      sourceLogs: path.join(REPO_ROOT, "logs/custom-pattern-comparison"),
      snapshot(outputDir) {
        snapshotPatternArtifacts(this.sourceLogs, outputDir);
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
  copyDirectory(sourceRoot, path.join(outputDir, "patterns", "raw"));

  const approachDirs = fs.existsSync(sourceRoot)
    ? fs.readdirSync(sourceRoot).filter((entry) => fs.statSync(path.join(sourceRoot, entry)).isDirectory())
    : [];

  const patternNames = new Set();
  for (const approach of approachDirs) {
    const approachRoot = path.join(sourceRoot, approach);
    for (const patternName of fs.readdirSync(approachRoot)) {
      const patternRoot = path.join(approachRoot, patternName);
      if (fs.statSync(patternRoot).isDirectory()) {
        patternNames.add(patternName);
      }
    }
  }

  for (const patternName of patternNames) {
    const patternDir = PATTERN_DIR_MAP[patternName] || patternName.replaceAll("_", "-");
    for (const approach of approachDirs) {
      const approachPatternRoot = path.join(sourceRoot, approach, patternName);
      const iterationDirs = listIterationDirs(approachPatternRoot);
      for (const iterationDir of iterationDirs) {
        const iterationSource = path.join(approachPatternRoot, iterationDir);
        const structuredDestination = path.join(outputDir, "patterns", patternDir, approach, iterationDir);
        copyDirectory(iterationSource, structuredDestination);

        const accuracyDestination = path.join(outputDir, "accuracy", "patterns", patternDir, approach, iterationDir);
        const naiveDestination = path.join(outputDir, "naive-distributed", "patterns", patternDir, approach, iterationDir);
        const files = fs.readdirSync(iterationSource);

        for (const fileName of files) {
          const sourceFile = path.join(iterationSource, fileName);
          if (!fs.statSync(sourceFile).isFile()) {
            continue;
          }
          if (
            fileName.endsWith("_results.csv") ||
            fileName.endsWith("_metadata.json") ||
            fileName.includes("latency") ||
            fileName.includes("resource")
          ) {
            copyFileIfExists(sourceFile, path.join(accuracyDestination, fileName));
          }
          if (approach === "naive_distributed") {
            copyFileIfExists(sourceFile, path.join(naiveDestination, fileName));
          }
        }
      }
    }
  }

  const patternSummary = path.join(sourceRoot, "custom_pattern_comparison_summary.json");
  copyFileIfExists(patternSummary, path.join(outputDir, "patterns", "summary.json"));
  copyFileIfExists(patternSummary, path.join(outputDir, "accuracy", "patterns-summary.json"));
}

async function runCommand(job, logDir, dryRun) {
  const stdoutPath = path.join(logDir, `${job.name}.stdout.log`);
  const stderrPath = path.join(logDir, `${job.name}.stderr.log`);
  const combinedPath = path.join(logDir, `${job.name}.combined.log`);
  const commandLine = [job.command[0], ...job.command[1]].join(" ");

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
    const child = spawn(job.command[0], job.command[1], {
      cwd: REPO_ROOT,
      env: job.env,
      stdio: ["ignore", "pipe", "pipe"],
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
      stdoutStream.end();
      stderrStream.end();
      combinedStream.end();
      resolve({
        ok: code === 0,
        code,
        commandLine,
        stdoutPath,
        stderrPath,
        combinedPath,
        durationMs: Date.now() - startedAt,
      });
    });

    child.on("error", (error) => {
      stderrStream.write(`${error.stack || error.message}\n`);
      combinedStream.write(`${error.stack || error.message}\n`);
      stdoutStream.end();
      stderrStream.end();
      combinedStream.end();
      resolve({
        ok: false,
        code: null,
        commandLine,
        stdoutPath,
        stderrPath,
        combinedPath,
        durationMs: Date.now() - startedAt,
        error: error.message,
      });
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

  if (config.smoke) {
    requiredData.push("src/streamer/data/custom_patterns/low_variability/smartphone.acceleration.x/data.nt");
  } else {
    requiredData.push(
      "src/streamer/data/custom_patterns/low_variability/smartphone.acceleration.x/data.nt",
      "src/streamer/data/custom_patterns/step_pattern/smartphone.acceleration.x/data.nt",
      "src/streamer/data/custom_patterns/spike_pattern/smartphone.acceleration.x/data.nt",
      "src/streamer/data/custom_patterns/low_freq_oscillation/smartphone.acceleration.x/data.nt",
      "src/streamer/data/custom_patterns/high_freq_oscillation/smartphone.acceleration.x/data.nt",
    );
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

  const brokerReachability = await checkBrokerReachable(config.broker);
  checks.push({
    name: "mqtt-broker",
    ok: brokerReachability.ok,
    detail: brokerReachability.ok
      ? `Connected to ${config.broker}`
      : brokerReachability.message,
  });

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
    aggregation: config.aggregation,
    smokeMode: config.smoke,
    failedBenchmarkCommands: [],
    warnings: [],
  };

  const preflightChecks = await runPreflight(config, jobs);
  metadata.preflightChecks = preflightChecks;
  metadata.warnings.push(
    "Warm-up and cool-down dropping are recorded in metadata for downstream analysis; the existing benchmark scripts still execute the full iteration count.",
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
    for (const job of jobs) {
      const commandLine = [job.command[0], ...job.command[1]].join(" ");
      console.log(`- ${job.name}: ${commandLine}`);
    }
    if (config.suites.includes("patterns") && !config.skipAnalysis) {
      const analysisJob = buildPatternAnalysisJob(config);
      const commandLine = [analysisJob.command[0], ...analysisJob.command[1]].join(" ");
      console.log(`- ${analysisJob.name}: ${commandLine}`);
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
      job.snapshot(config.outputDir);
    }
  }

  let analysisResult = null;
  if (config.suites.includes("patterns") && !config.skipAnalysis) {
    const analysisJob = buildPatternAnalysisJob(config);
    analysisResult = await runCommand(analysisJob, path.join(config.outputDir, "logs"), config.dryRun);
    jobResults[analysisJob.name] = analysisResult;

    if (!analysisResult.ok) {
      metadata.failedAnalysisCommands = metadata.failedAnalysisCommands || [];
      metadata.failedAnalysisCommands.push({
        job: analysisJob.name,
        command: analysisResult.commandLine,
        exitCode: analysisResult.code,
        error: analysisResult.error || null,
        combinedLog: path.relative(config.outputDir, analysisResult.combinedPath),
      });
      if (!config.dryRun) {
        writeJson(path.join(config.outputDir, "metadata.json"), metadata);
      }

      if (config.failFast) {
        process.exitCode = 1;
        return;
      }
    }
  }

  const analysisFailed = Boolean(analysisResult && !analysisResult.ok);

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

  const summary = {
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
        }
      : null,
    analysisFailed,
  };

  if (!config.dryRun) {
    writeJson(path.join(config.outputDir, "metadata.json"), metadata);
    writeJson(path.join(config.outputDir, "summary.json"), summary);
  }

  if ((suitesFailed.length > 0 || analysisFailed) && !config.dryRun) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
