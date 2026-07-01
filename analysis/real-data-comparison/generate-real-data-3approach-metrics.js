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
  "startup-first-emitted": {
    expectedIterations: 35,
    targetWindows: 5,
    steadyWindowStart: 1,
    steadyWindowEnd: 5,
    warmupWindows: [],
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
  --mode <steady-state|startup-cost|startup-first-emitted>
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

function formatOptional(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : "n/a";
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
  const rawLatencyRowsByWindow = new Map(
    latencyRows
      .map((row) => [Number.parseInt(String(row.window_number || ""), 10), row])
      .filter(([windowNumber]) => Number.isFinite(windowNumber)),
  );
  const mqttSummary = fs.existsSync(mqttSummaryPath) ? readJson(mqttSummaryPath) : null;
  const normalizedRows = normalizeLatencyRows(approachName, latencyRows, { summary: mqttSummary, csvRows: [] });
  const capSummary = fs.existsSync(capSummaryPath) ? readJson(capSummaryPath) : null;
  const runSummary = fs.existsSync(runSummaryPath) ? readJson(runSummaryPath) : null;
  const processTreeMetrics = summarizeProcessTreeMetrics(iterationDir, approach.processTreeFile, normalizedRows.length);
  const artifactWarnings = [];

  if (
    Number.isFinite(capSummary?.emittedFinalWindowCount) &&
    normalizedRows.length > 0 &&
    capSummary.emittedFinalWindowCount !== normalizedRows.length
  ) {
    artifactWarnings.push(
      `${approachName}/iteration${iteration}: benchmark_window_cap_summary.json reports ${capSummary.emittedFinalWindowCount} finalized windows but ${path.basename(latencyPath)} contains ${normalizedRows.length} rows`,
    );
  }

  return {
    approachName,
    iteration,
    iterationDir,
    normalizedRows,
    capSummary,
    runSummary,
    processTreeMetrics,
    artifactWarnings,
    rawLatencyRowsByWindow,
    files: {
      latencyPath,
      capSummaryPath,
      runSummaryPath,
      processTreePath,
      mqttSummaryPath,
    },
  };
}

function hasSuccessfulBoundedCompletionEvidence(capSummary) {
  return capSummary?.stoppedAfterTargetWindows === true &&
    capSummary?.stopReason === "target_window_count_reached" &&
    Number.isFinite(capSummary?.targetWindowCount) &&
    Number.isFinite(capSummary?.emittedFinalWindowCount) &&
    capSummary.emittedFinalWindowCount >= capSummary.targetWindowCount;
}

function isCompletedBoundedIteration(record) {
  if (record?.runSummary?.completionStatus === "completed") {
    return true;
  }

  return hasSuccessfulBoundedCompletionEvidence(record?.capSummary);
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
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

function summarizeCountsByKey(values) {
  const counts = {};
  for (const value of values) {
    const key = String(value || "unknown");
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function formatCountsByKey(counts) {
  const entries = Object.entries(counts || {}).sort((left, right) => left[0].localeCompare(right[0]));
  if (entries.length === 0) {
    return "n/a";
  }
  return entries.map(([key, count]) => `${key}:${count}`).join(", ");
}

function selectFirstUsableStartupRow(rows) {
  return rows.find((row) => row.warmup !== true) || null;
}

function computeWallClockMappedStartupLatency(row, rawLatencyRow = null) {
  const comparableLatencyUpperBoundMs = 120000 * 10;
  const rawWallClockCloseToResultMs = toNumber(rawLatencyRow?.wall_clock_close_to_result_ms);
  const rawResultEmittedAt = toNumber(rawLatencyRow?.result_emitted_at);
  const rawWindowEnd = toNumber(
    rawLatencyRow?.event_time_window_close
      ?? rawLatencyRow?.window_data_close_time
      ?? rawLatencyRow?.logical_trigger_time
      ?? rawLatencyRow?.window_end,
  );

  if (
    Number.isFinite(rawWallClockCloseToResultMs) &&
    rawWallClockCloseToResultMs >= 0 &&
    rawWallClockCloseToResultMs <= comparableLatencyUpperBoundMs
  ) {
    return {
      startupLatencyMs: rawWallClockCloseToResultMs,
      startupLatencySource: "wallClockCloseToResultMs",
      startupLatencyValid: true,
      startupLatencyFailureReason: null,
      latencyDomainStatus: "wall_clock_mapped",
      wallClockWindowCloseMs: toNumber(rawLatencyRow?.wall_clock_window_close),
    };
  }

  if (
    Number.isFinite(row.wallClockCloseToResultMs) &&
    row.wallClockCloseToResultMs >= 0 &&
    row.wallClockCloseToResultMs <= comparableLatencyUpperBoundMs &&
    row.latencyDomainStatus === "wall_clock_mapped"
  ) {
    return {
      startupLatencyMs: row.wallClockCloseToResultMs,
      startupLatencySource: "wallClockCloseToResultMs",
      startupLatencyValid: true,
      startupLatencyFailureReason: null,
      latencyDomainStatus: "wall_clock_mapped",
      wallClockWindowCloseMs: null,
    };
  }

  if (
    Number.isFinite(rawResultEmittedAt) &&
    Number.isFinite(rawWindowEnd)
  ) {
    const mappedLatencyMs = rawResultEmittedAt - rawWindowEnd;
    if (mappedLatencyMs >= 0 && mappedLatencyMs <= comparableLatencyUpperBoundMs) {
      return {
        startupLatencyMs: mappedLatencyMs,
        startupLatencySource: "resultEmittedAtMinusWindowEnd",
        startupLatencyValid: true,
        startupLatencyFailureReason: null,
        latencyDomainStatus: "wall_clock_mapped",
        wallClockWindowCloseMs: rawWindowEnd,
      };
    }
  }

  return null;
}

function selectStartupLatency(row, rawLatencyRow = null) {
  const rawLatencyFromWindowCloseMs = toNumber(rawLatencyRow?.latency_from_window_close_ms);
  const rawLatencyFromLogicalTriggerMs = toNumber(rawLatencyRow?.latency_from_logical_trigger_ms);
  const comparableLatencyUpperBoundMs = 120000 * 10;
  const buildComparableLatency = (startupLatencyMs, startupLatencySource, latencyDomainStatus, diagnosticsOnly = false) => ({
    startupLatencyMs,
    startupLatencySource,
    startupLatencyValid: !diagnosticsOnly,
    startupLatencyFailureReason: diagnosticsOnly ? "non_comparable_latency_domain" : null,
    latencyDomainStatus,
    diagnosticsOnly,
    diagnosticDirectWindowCloseMs: rawLatencyFromWindowCloseMs,
    diagnosticDirectLogicalTriggerMs: rawLatencyFromLogicalTriggerMs,
  });

  if (row.latencyDomainStatus === "domain_mismatch") {
    return {
      startupLatencyMs: null,
      startupLatencySource: "domain_mismatch",
      startupLatencyValid: false,
      startupLatencyFailureReason: "latency_domain_mismatch",
      latencyDomainStatus: "domain_mismatch",
      diagnosticsOnly: false,
      diagnosticDirectWindowCloseMs: rawLatencyFromWindowCloseMs,
      diagnosticDirectLogicalTriggerMs: rawLatencyFromLogicalTriggerMs,
    };
  }

  const mappedLatency = computeWallClockMappedStartupLatency(row, rawLatencyRow);
  if (mappedLatency) {
    return {
      ...mappedLatency,
      diagnosticsOnly: false,
      diagnosticDirectWindowCloseMs: rawLatencyFromWindowCloseMs,
      diagnosticDirectLogicalTriggerMs: rawLatencyFromLogicalTriggerMs,
    };
  }

  if (Number.isFinite(row.anchorAlignedWindowCloseToResultMs) && row.anchorAlignedWindowCloseToResultMs >= 0) {
    return buildComparableLatency(
      row.anchorAlignedWindowCloseToResultMs,
      "anchorAlignedWindowCloseToResultMs",
      "wall_clock_mapped",
    );
  }

  if (
    Number.isFinite(rawLatencyFromWindowCloseMs) &&
    rawLatencyFromWindowCloseMs >= 0 &&
    rawLatencyFromWindowCloseMs <= comparableLatencyUpperBoundMs
  ) {
    return buildComparableLatency(
      rawLatencyFromWindowCloseMs,
      "latencyFromWindowCloseMs",
      "direct_window_close",
      true,
    );
  }

  if (
    Number.isFinite(rawLatencyFromLogicalTriggerMs) &&
    rawLatencyFromLogicalTriggerMs >= 0 &&
    rawLatencyFromLogicalTriggerMs <= comparableLatencyUpperBoundMs
  ) {
    return buildComparableLatency(
      rawLatencyFromLogicalTriggerMs,
      "latencyFromLogicalTriggerMs",
      "direct_logical_trigger",
      true,
    );
  }

  return {
    startupLatencyMs: null,
    startupLatencySource: "unavailable",
    startupLatencyValid: false,
    startupLatencyFailureReason: "missing_comparable_close_to_result_latency",
    latencyDomainStatus: row.latencyDomainStatus || null,
    diagnosticsOnly: false,
    diagnosticDirectWindowCloseMs: rawLatencyFromWindowCloseMs,
    diagnosticDirectLogicalTriggerMs: rawLatencyFromLogicalTriggerMs,
  };
}

function buildStartupFirstEmittedEntry(record) {
  const firstUsableRow = selectFirstUsableStartupRow(record.normalizedRows);
  const warmupRowsSkipped = record.normalizedRows.filter((row) => row.warmup === true).length;
  if (!firstUsableRow) {
    return {
      approach: record.approachName,
      iteration: record.iteration,
      usable: false,
      noUsableResultReason: record.normalizedRows.length === 0
        ? "no_result_rows"
        : "only_warmup_result_rows",
      warmupRowsSkipped,
      firstEmittedWindowNumber: null,
      resultValue: null,
      startupLatencyMs: null,
      startupLatencySource: "unavailable",
      startupLatencyValid: false,
      startupLatencyFailureReason: record.normalizedRows.length === 0
        ? "no_result_rows"
        : "only_warmup_result_rows",
      latencyDomainStatus: null,
      registrationToResultMs: null,
      dataStartToResultMs: null,
      lastDataToResultMs: null,
      finalWindowNumbers: record.capSummary?.finalWindowNumbers || [],
      emittedFinalWindowCount: record.capSummary?.emittedFinalWindowCount ?? null,
      targetWindowCount: record.capSummary?.targetWindowCount ?? null,
      stopReason: record.capSummary?.stopReason || null,
      runCompleted: isCompletedBoundedIteration(record),
      diagnosticStopReasonTargetReached: record.capSummary?.stopReason === "target_window_count_reached",
      diagnosticReachedTargetWindowUpperBound:
        Array.isArray(record.capSummary?.finalWindowNumbers)
        && Number.isFinite(record.capSummary?.targetWindowCount)
        && record.capSummary.finalWindowNumbers.length === record.capSummary.targetWindowCount,
    };
  }

  const rawLatencyRow = record.rawLatencyRowsByWindow.get(firstUsableRow.windowNumber) || null;
  const selectedLatency = selectStartupLatency(firstUsableRow, rawLatencyRow);
  return {
    approach: record.approachName,
    iteration: record.iteration,
    usable: true,
    noUsableResultReason: null,
    warmupRowsSkipped,
    firstEmittedWindowNumber: firstUsableRow.windowNumber,
    resultValue: firstUsableRow.resultValue,
    startupLatencyMs: selectedLatency.startupLatencyMs,
    startupLatencySource: selectedLatency.startupLatencySource,
    startupLatencyValid: selectedLatency.startupLatencyValid,
    startupLatencyFailureReason: selectedLatency.startupLatencyFailureReason,
    latencyDomainStatus: selectedLatency.latencyDomainStatus,
    diagnosticDirectWindowCloseMs: selectedLatency.diagnosticDirectWindowCloseMs,
    diagnosticDirectLogicalTriggerMs: selectedLatency.diagnosticDirectLogicalTriggerMs,
    registrationToResultMs: firstUsableRow.registrationToResultMs,
    dataStartToResultMs: firstUsableRow.dataStartToResultMs,
    lastDataToResultMs: firstUsableRow.lastDataToResultMs,
    finalWindowNumbers: record.capSummary?.finalWindowNumbers || [],
    emittedFinalWindowCount: record.capSummary?.emittedFinalWindowCount ?? null,
    targetWindowCount: record.capSummary?.targetWindowCount ?? null,
    stopReason: record.capSummary?.stopReason || null,
    runCompleted: isCompletedBoundedIteration(record),
    diagnosticStopReasonTargetReached: record.capSummary?.stopReason === "target_window_count_reached",
    diagnosticReachedTargetWindowUpperBound:
      Array.isArray(record.capSummary?.finalWindowNumbers)
      && Number.isFinite(record.capSummary?.targetWindowCount)
      && record.capSummary.finalWindowNumbers.length === record.capSummary.targetWindowCount,
  };
}

function compareFirstEmittedAgainstFetching(fetchingRecord, candidateEntry) {
  if (!candidateEntry.usable) {
    return {
      comparable: false,
      alignment: "not_comparable",
      note: candidateEntry.noUsableResultReason || "candidate_missing_usable_row",
      metrics: null,
    };
  }
  if (!fetchingRecord) {
    return {
      comparable: false,
      alignment: "not_comparable",
      note: "missing_fetching_iteration",
      metrics: null,
    };
  }

  const fetchingEntry = buildStartupFirstEmittedEntry(fetchingRecord);
  if (!fetchingEntry.usable) {
    return {
      comparable: false,
      alignment: "not_comparable",
      note: fetchingEntry.noUsableResultReason || "fetching_missing_usable_row",
      metrics: null,
    };
  }

  const fetchingByWindow = new Map(fetchingRecord.normalizedRows.map((row) => [row.windowNumber, row]));
  const baselineRow = candidateEntry.firstEmittedWindowNumber === fetchingEntry.firstEmittedWindowNumber
    ? fetchingByWindow.get(fetchingEntry.firstEmittedWindowNumber)
    : fetchingByWindow.get(candidateEntry.firstEmittedWindowNumber);

  if (!baselineRow) {
    return {
      comparable: false,
      alignment: "not_comparable",
      note: `fetching_missing_window_${candidateEntry.firstEmittedWindowNumber}`,
      metrics: null,
    };
  }

  return {
    comparable: true,
    alignment: candidateEntry.firstEmittedWindowNumber === fetchingEntry.firstEmittedWindowNumber
      ? "matched_first_emitted_window"
      : "matched_candidate_window_in_fetching",
    note: candidateEntry.firstEmittedWindowNumber === fetchingEntry.firstEmittedWindowNumber
      ? "same_first_emitted_window"
      : `candidate_window_${candidateEntry.firstEmittedWindowNumber}_aligned_to_fetching`,
    metrics: computeAccuracy([baselineRow], [{
      windowNumber: candidateEntry.firstEmittedWindowNumber,
      resultValue: candidateEntry.resultValue,
    }]),
    fetchingFirstEmittedWindowNumber: fetchingEntry.firstEmittedWindowNumber,
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
      const runCompleted = isCompletedBoundedIteration(record);
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
  const startupFirstEmittedRows = [];

  for (const approachSummary of perApproach) {
    const { approachName, iterationRecords } = approachSummary;

    for (const record of iterationRecords) {
      warnings.push(...record.artifactWarnings);
    }

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
    } else if (options.mode === "startup-cost") {
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
    } else {
      const perIterationEntries = iterationRecords.map((record) => {
        const entry = buildStartupFirstEmittedEntry(record);
        const comparison = approachName === "fetching"
          ? {
            comparable: null,
            alignment: "baseline",
            note: "baseline_fetching",
            metrics: null,
          }
          : compareFirstEmittedAgainstFetching(fetchingByIteration.get(record.iteration), entry);

        if (!entry.usable) {
          errors.push(
            `${approachName}/iteration${record.iteration}: no usable non-warmup result row (${entry.noUsableResultReason})`,
          );
        }

        if (entry.usable && !entry.startupLatencyValid) {
          errors.push(
            `${approachName}/iteration${record.iteration}: first usable row has invalid comparable startup latency (${entry.startupLatencyFailureReason})`,
          );
        }

        if (!entry.runCompleted) {
          errors.push(
            `${approachName}/iteration${record.iteration}: missing bounded-run completion evidence (run_summary.json or successful benchmark_window_cap_summary.json)`,
          );
        }

        if (!entry.diagnosticStopReasonTargetReached) {
          warnings.push(
            `${approachName}/iteration${record.iteration}: diagnostic stop reason is ${entry.stopReason || "missing"}; startup-first-emitted only requires a usable first row`,
          );
        }

        if (!entry.diagnosticReachedTargetWindowUpperBound) {
          warnings.push(
            `${approachName}/iteration${record.iteration}: diagnostic final windows ${entry.finalWindowNumbers.length > 0 ? entry.finalWindowNumbers.join(",") : "none"} did not reach the target-window upper bound ${entry.targetWindowCount ?? "n/a"}`,
          );
        }

        return {
          ...entry,
          startupValid: entry.usable && entry.startupLatencyValid && entry.runCompleted,
          accuracyComparable: comparison.comparable,
          accuracyAlignment: comparison.alignment,
          accuracyNote: comparison.note,
          fetchingFirstEmittedWindowNumber: comparison.fetchingFirstEmittedWindowNumber ?? null,
          accuracyMetrics: comparison.metrics,
        };
      });

      startupFirstEmittedRows.push(...perIterationEntries);

      startupLatency.push({
        approach: approachName,
        usableFirstEmittedRows: perIterationEntries.filter((entry) => entry.startupValid).length,
        missingFirstEmittedRows: perIterationEntries.filter((entry) => !entry.startupValid).length,
        firstEmittedWindowNumber: summarize(perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.firstEmittedWindowNumber)),
        startupLatencyMs: summarize(perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.startupLatencyMs)),
        registrationToResultMs: summarize(perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.registrationToResultMs)),
        dataStartToResultMs: summarize(perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.dataStartToResultMs)),
        lastDataToResultMs: summarize(perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.lastDataToResultMs)),
        startupLatencySourceCounts: summarizeCountsByKey(
          perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.startupLatencySource),
        ),
        latencyDomainStatusCounts: summarizeCountsByKey(
          perIterationEntries.filter((entry) => entry.startupValid).map((entry) => entry.latencyDomainStatus || "missing"),
        ),
      });

      const validDomains = new Set(
        perIterationEntries
          .filter((entry) => entry.startupValid)
          .map((entry) => entry.latencyDomainStatus),
      );
      if (validDomains.size > 0 && (validDomains.size !== 1 || !validDomains.has("wall_clock_mapped"))) {
        errors.push(
          `${approachName}: startup-first-emitted comparable latency rows must all be wall_clock_mapped; found ${[...validDomains].sort().join(",")}`,
        );
      }

      if (approachName !== "fetching") {
        const comparableEntries = perIterationEntries.filter((entry) => entry.accuracyComparable && entry.accuracyMetrics);
        accuracyRows.push({
          approach: approachName,
          comparableIterationCount: comparableEntries.length,
          nonComparableIterationCount: perIterationEntries.length - comparableEntries.length,
          matchedFirstEmittedWindowCount: comparableEntries.filter((entry) => entry.accuracyAlignment === "matched_first_emitted_window").length,
          matchedCandidateWindowCount: comparableEntries.filter((entry) => entry.accuracyAlignment === "matched_candidate_window_in_fetching").length,
          matchedWindowCount: comparableEntries.reduce((sum, entry) => sum + entry.accuracyMetrics.matchedWindowCount, 0),
          baselineOnlyCount: comparableEntries.reduce((sum, entry) => sum + entry.accuracyMetrics.baselineOnlyCount, 0),
          approachOnlyCount: comparableEntries.reduce((sum, entry) => sum + entry.accuracyMetrics.approachOnlyCount, 0),
          mae: mean(comparableEntries.map((entry) => entry.accuracyMetrics.mae)),
          rmse: mean(comparableEntries.map((entry) => entry.accuracyMetrics.rmse)),
          mape: mean(comparableEntries.map((entry) => entry.accuracyMetrics.mape)),
          mapeApplicableWindowCount: comparableEntries.reduce((sum, entry) => sum + entry.accuracyMetrics.mapeApplicableWindowCount, 0),
          maxAbsoluteError: comparableEntries.length > 0
            ? Math.max(...comparableEntries.map((entry) => entry.accuracyMetrics.maxAbsoluteError || 0))
            : null,
          exactAgainstFetching: comparableEntries.length > 0
            ? comparableEntries.every((entry) => entry.accuracyMetrics.exactAgainstFetching)
            : false,
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
    startupFirstEmittedRows,
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
  const completenessHeaders = result.mode === "startup-first-emitted"
    ? ["Approach", "Iterations found", "Startup-valid first-emitted rows", "Missing startup-valid rows", "All diagnostic stop reasons hit target", "All diagnostic final windows hit upper bound", "All run summaries completed"]
    : ["Approach", "Iterations found", "All stop reasons ok", "All final windows ok", "All run summaries completed"];
  const completenessTable = buildMarkdownTable(
    completenessHeaders,
    result.perApproach.map((entry) => {
      const startupSummary = result.startupLatency.find((row) => row.approach === entry.approachName);
      return result.mode === "startup-first-emitted"
        ? [
          `\`${entry.approachName}\``,
          String(entry.iterationRecords.length),
          String(startupSummary?.usableFirstEmittedRows ?? 0),
          String(startupSummary?.missingFirstEmittedRows ?? 0),
          formatBool(entry.completeness.every((row) => row.stopReasonMatch)),
          formatBool(entry.completeness.every((row) => row.finalWindowsMatch)),
          formatBool(entry.completeness.every((row) => row.runCompleted)),
        ]
        : [
          `\`${entry.approachName}\``,
          String(entry.iterationRecords.length),
          formatBool(entry.completeness.every((row) => row.stopReasonMatch)),
          formatBool(entry.completeness.every((row) => row.finalWindowsMatch)),
          formatBool(entry.completeness.every((row) => row.runCompleted)),
        ];
    }),
  );

  const accuracyHeaders = result.mode === "startup-first-emitted"
    ? ["Approach vs fetching", "Comparable iterations", "Not comparable", "Same first-emitted window", "Window-aligned to fetching", "MAE", "RMSE", "MAPE", "Max abs error", "Chunked exact"]
    : ["Approach vs fetching", "Matched windows", "Baseline-only", "Approach-only", "MAE", "RMSE", "MAPE", "Max abs error", "Chunked exact"];
  const accuracyTable = buildMarkdownTable(
    accuracyHeaders,
    result.accuracyRows.map((row) => (
      result.mode === "startup-first-emitted"
        ? [
          `\`${row.approach}\``,
          String(row.comparableIterationCount),
          String(row.nonComparableIterationCount),
          String(row.matchedFirstEmittedWindowCount),
          String(row.matchedCandidateWindowCount),
          formatNumber(row.mae, 6),
          formatNumber(row.rmse, 6),
          formatNumber(row.mape, 6),
          formatNumber(row.maxAbsoluteError, 6),
          row.approach === "chunked" ? formatBool(row.exactAgainstFetching) : "n/a",
        ]
        : [
          `\`${row.approach}\``,
          String(row.matchedWindowCount),
          String(row.baselineOnlyCount),
          String(row.approachOnlyCount),
          formatNumber(row.mae, 6),
          formatNumber(row.rmse, 6),
          formatNumber(row.mape, 6),
          formatNumber(row.maxAbsoluteError, 6),
          row.approach === "chunked" ? formatBool(row.exactAgainstFetching) : "n/a",
        ]
    )),
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

  if (result.mode === "startup-cost") {
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

  const startupSummaryTable = buildMarkdownTable(
    ["Approach", "Startup-valid first-emitted rows", "Missing startup-valid rows", "First emitted window mean", "First emitted window min", "First emitted window max", "Startup latency mean ms", "Startup latency std", "Latency sources", "Latency domains"],
    result.startupLatency.map((row) => [
      `\`${row.approach}\``,
      String(row.usableFirstEmittedRows),
      String(row.missingFirstEmittedRows),
      formatNumber(row.firstEmittedWindowNumber.mean),
      formatNumber(row.firstEmittedWindowNumber.min),
      formatNumber(row.firstEmittedWindowNumber.max),
      formatNumber(row.startupLatencyMs.mean),
      formatNumber(row.startupLatencyMs.std),
      formatCountsByKey(row.startupLatencySourceCounts),
      formatCountsByKey(row.latencyDomainStatusCounts),
    ]),
  );

  const iterationTable = buildMarkdownTable(
    ["Approach", "Iteration", "Startup-valid", "Warmup rows skipped", "First emitted window", "Startup latency ms", "Latency source", "Latency domain", "Direct window-close diagnostic ms", "Direct logical-trigger diagnostic ms", "Result value", "Stop reason diagnostic", "Final windows diagnostic", "Accuracy comparable", "Accuracy alignment", "Accuracy note"],
    result.startupFirstEmittedRows.map((row) => [
      `\`${row.approach}\``,
      String(row.iteration),
      formatBool(row.startupValid),
      String(row.warmupRowsSkipped),
      formatOptional(row.firstEmittedWindowNumber, 0),
      formatNumber(row.startupLatencyMs),
      `\`${row.startupLatencySource}\``,
      `\`${row.latencyDomainStatus || "missing"}\``,
      formatNumber(row.diagnosticDirectWindowCloseMs),
      formatNumber(row.diagnosticDirectLogicalTriggerMs),
      formatNumber(row.resultValue, 6),
      `\`${row.stopReason || "missing"}\``,
      `\`${row.finalWindowNumbers.length > 0 ? row.finalWindowNumbers.join(",") : "none"} / ${row.targetWindowCount ?? "n/a"}\``,
      row.accuracyComparable === null ? "baseline" : formatBool(row.accuracyComparable),
      `\`${row.accuracyAlignment}\``,
      `\`${row.accuracyNote}\``,
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

  return `# Real-Data 3-Approach First-Emitted Startup Summary

Input root:
\`${result.logsRoot}\`

Selected approaches exact:
\`${formatBool(result.selectedApproachesExact)}\`

Startup metric:
First usable non-warmup emitted result row per iteration, requiring \`wall_clock_mapped\` close-to-result latency for cross-approach comparison

Window policy:
- target windows per iteration: ${result.expected.targetWindows}
- target windows are an upper bound / flush allowance, not a startup completion requirement
- warmup rows are skipped only when the row is explicitly marked \`warmup=true\`
- the first usable row does not need to be window 1
- first usable rows may be windows 1..${result.expected.targetWindows}
- final-window completeness and stop reason are diagnostic only for startup-first-emitted
- accuracy is aligned against \`fetching\` by iteration and first emitted window number where possible

## Completeness

${completenessTable}

## Startup Latency Summary

${startupSummaryTable}

## Per-Iteration First-Emitted Rows

${iterationTable}

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
