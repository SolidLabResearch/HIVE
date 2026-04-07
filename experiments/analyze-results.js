#!/usr/bin/env node

/**
 * EXPERIMENT RESULTS ANALYZER
 *
 * Reads all benchmark result directories and produces a comprehensive
 * cross-experiment analysis with focus on:
 *   1. Latency comparison
 *   2. Resource usage comparison
 *   3. Accuracy analysis (vs fetching baseline)
 *   4. Chunked reusability story
 *
 * Usage:
 *   node experiments/analyze-results.js
 *   node experiments/analyze-results.js --output-dir ./my-analysis
 */

const fs = require("fs");
const path = require("path");

const RESULTS_DIR = path.join(__dirname, "benchmark-results");
const APPROACHES = [
  "fetching",
  "naive_distributed",
  "approximation",
  "chunked",
];
const ACCURACY_APPROACHES = [
  "naive_distributed",
  "approximation",
  "chunked",
];

// ── Helpers ──────────────────────────────────────────────────────────

function mean(arr) {
  if (arr.length === 0) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function stdDev(arr) {
  if (arr.length <= 1) return 0;
  const avg = mean(arr);
  return Math.sqrt(arr.reduce((sq, n) => sq + (n - avg) ** 2, 0) / arr.length);
}

function median(arr) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function pctDiff(val, baseline) {
  if (baseline === 0) return 0;
  return ((val - baseline) / Math.abs(baseline)) * 100;
}

function pad(str, len, align = "right") {
  const s = String(str);
  return align === "right" ? s.padStart(len) : s.padEnd(len);
}

// ── Load experiment data ─────────────────────────────────────────────

function loadExperiment(dirPath) {
  const reportPath = path.join(dirPath, "benchmark-report.json");
  if (!fs.existsSync(reportPath)) return null;

  const report = JSON.parse(fs.readFileSync(reportPath, "utf-8"));

  // Also load individual iterations for per-iteration analysis
  const iterations = [];
  let i = 1;
  while (true) {
    const iterPath = path.join(dirPath, `iteration-${i}.json`);
    if (!fs.existsSync(iterPath)) break;
    iterations.push(JSON.parse(fs.readFileSync(iterPath, "utf-8")));
    i++;
  }

  return { report, iterations, dir: path.basename(dirPath) };
}

function classifyExperiment(dirName) {
  // Parse the directory name to determine experiment type
  // Format: {pattern/real-data}[-{suffix}]-{timestamp}
  const parts = dirName.split("-");
  const timestamp = parts[parts.length - 1];

  // Look at the report config to classify
  return { dirName, timestamp };
}

function getExperimentLabel(report) {
  const summary = report.summary || {};
  // Try to extract config from the report structure
  // The report JSON has the full allResults structure

  // Check if there's config info in byIteration
  const iter1 = report.byIteration?.["1"];
  if (!iter1) return "Unknown";

  // Infer from directory or report content
  return null; // Will be determined from report text
}

function loadAllExperiments() {
  if (!fs.existsSync(RESULTS_DIR)) {
    console.error(`Results directory not found: ${RESULTS_DIR}`);
    process.exit(1);
  }

  const dirs = fs.readdirSync(RESULTS_DIR)
    .filter((d) => fs.statSync(path.join(RESULTS_DIR, d)).isDirectory())
    .sort();

  const experiments = [];
  for (const dir of dirs) {
    const data = loadExperiment(path.join(RESULTS_DIR, dir));
    if (data) {
      // Extract config from the text report
      const txtPath = path.join(RESULTS_DIR, dir, "benchmark-report.txt");
      let config = {};
      if (fs.existsSync(txtPath)) {
        const txt = fs.readFileSync(txtPath, "utf-8");
        const dataMatch = txt.match(/Data: (.+)/);
        const aggMatch = txt.match(/Aggregation: (\w+)/);
        const subWinMatch = txt.match(/Sub-Window: RANGE (\d+) STEP (\d+)/);
        const freqMatch = txt.match(/Wearable Frequency: ([\d.]+)Hz/);
        const iterMatch = txt.match(/Iterations: (\d+)/);

        config = {
          data: dataMatch ? dataMatch[1].trim() : "Unknown",
          aggregation: aggMatch ? aggMatch[1] : "AVG",
          subWindowRange: subWinMatch ? parseInt(subWinMatch[1]) : 60000,
          subWindowStep: subWinMatch ? parseInt(subWinMatch[2]) : 30000,
          wearableFreq: freqMatch ? parseFloat(freqMatch[1]) : 4,
          iterations: iterMatch ? parseInt(iterMatch[1]) : 0,
        };
      }

      data.config = config;
      data.label = buildLabel(config);
      experiments.push(data);
    }
  }

  return experiments;
}

function buildLabel(config) {
  const parts = [];

  // Data source
  if (config.data && config.data.includes("Pattern:")) {
    const patternMatch = config.data.match(/Pattern: (\S+)/);
    parts.push(patternMatch ? patternMatch[1] : "pattern");
  } else {
    parts.push("real-data");
  }

  // Non-default settings
  if (config.aggregation && config.aggregation !== "AVG") {
    parts.push(`agg=${config.aggregation}`);
  }
  if (config.subWindowRange !== 60000 || config.subWindowStep !== 30000) {
    parts.push(`sw=${config.subWindowRange / 1000}/${config.subWindowStep / 1000}s`);
  }
  if (config.wearableFreq && config.wearableFreq !== 4) {
    parts.push(`freq=${config.wearableFreq}Hz`);
  }

  return parts.join(", ");
}

// ── Extract per-approach stats from iterations ───────────────────────

function extractApproachStats(iterations) {
  const stats = {};

  for (const approach of APPROACHES) {
    const latencies = [];
    const cpuValues = [];
    const memValues = [];
    const values = [];

    for (const iter of iterations) {
      const r = iter.results?.[approach];
      if (r && !r.error && r.latency) {
        latencies.push(r.latency);
        values.push(r.value);
        if (r.resources) {
          cpuValues.push(r.resources.avgCpu);
          memValues.push(r.resources.maxMemory / 1024 / 1024); // MB
        }
      }
    }

    stats[approach] = {
      n: latencies.length,
      latency: {
        mean: mean(latencies),
        stdDev: stdDev(latencies),
        median: median(latencies),
        min: latencies.length > 0 ? Math.min(...latencies) : 0,
        max: latencies.length > 0 ? Math.max(...latencies) : 0,
      },
      cpu: {
        mean: mean(cpuValues),
        max: cpuValues.length > 0 ? Math.max(...cpuValues) : 0,
      },
      memory: {
        meanMB: mean(memValues),
        maxMB: memValues.length > 0 ? Math.max(...memValues) : 0,
      },
      values,
    };
  }

  return stats;
}

// ── Accuracy analysis ────────────────────────────────────────────────

function computeAccuracy(iterations) {
  const results = Object.fromEntries(
    ACCURACY_APPROACHES.map((approach) => [approach, []]),
  );

  for (const iter of iterations) {
    const fetching = iter.results?.fetching;
    if (!fetching || fetching.error || fetching.value == null) continue;

    for (const approach of ACCURACY_APPROACHES) {
      const r = iter.results?.[approach];
      if (r && !r.error && r.value != null) {
        const absError = Math.abs(r.value - fetching.value);
        const relError = fetching.value !== 0
          ? (absError / Math.abs(fetching.value)) * 100
          : 0;
        results[approach].push({
          iteration: iter.results[approach].iteration,
          fetchingValue: fetching.value,
          approachValue: r.value,
          absError,
          relError,
          exact: absError < 1e-6,
        });
      }
    }
  }

  const summary = {};
  for (const approach of ACCURACY_APPROACHES) {
    const data = results[approach];
    if (data.length === 0) {
      summary[approach] = { n: 0, exactMatches: 0, meanRelError: 0, maxRelError: 0 };
      continue;
    }
    const relErrors = data.map((d) => d.relError);
    const exactCount = data.filter((d) => d.exact).length;
    summary[approach] = {
      n: data.length,
      exactMatches: exactCount,
      exactPct: (exactCount / data.length) * 100,
      meanRelError: mean(relErrors),
      maxRelError: Math.max(...relErrors),
      medianRelError: median(relErrors),
      details: data,
    };
  }

  return summary;
}

// ── Report generation ────────────────────────────────────────────────

function generateReport(experiments) {
  const lines = [];
  const hr = "=".repeat(100);
  const hr2 = "-".repeat(100);

  lines.push(hr);
  lines.push("STREAMING QUERY HIVE - CROSS-EXPERIMENT ANALYSIS");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push(`Experiments found: ${experiments.length}`);
  lines.push(hr);

  // ── Section 1: Overview table ────────────────────────────────────
  lines.push("");
  lines.push("1. EXPERIMENT OVERVIEW");
  lines.push(hr2);
  lines.push("");

  const overviewHeader = [
    pad("Experiment", 40, "left"),
    pad("Iters", 6),
    pad("Fetch Lat", 10),
    pad("Naive Lat", 10),
    pad("Approx Lat", 10),
    pad("Chunk Lat", 10),
    pad("Naive Err%", 11),
    pad("Approx Err%", 11),
    pad("Chunk Err%", 10),
  ].join(" | ");
  lines.push(overviewHeader);
  lines.push("-".repeat(overviewHeader.length));

  for (const exp of experiments) {
    const stats = extractApproachStats(exp.iterations);
    const accuracy = computeAccuracy(exp.iterations);

    const row = [
      pad(exp.label.substring(0, 40), 40, "left"),
      pad(stats.fetching.n.toString(), 6),
      pad(stats.fetching.latency.mean > 0 ? stats.fetching.latency.mean.toFixed(0) : "N/A", 10),
      pad(stats.naive_distributed.latency.mean > 0 ? stats.naive_distributed.latency.mean.toFixed(0) : "N/A", 10),
      pad(stats.approximation.latency.mean > 0 ? stats.approximation.latency.mean.toFixed(0) : "N/A", 10),
      pad(stats.chunked.latency.mean > 0 ? stats.chunked.latency.mean.toFixed(0) : "N/A", 10),
      pad(accuracy.naive_distributed.n > 0 ? accuracy.naive_distributed.meanRelError.toFixed(2) + "%" : "N/A", 11),
      pad(accuracy.approximation.n > 0 ? accuracy.approximation.meanRelError.toFixed(2) + "%" : "N/A", 11),
      pad(accuracy.chunked.n > 0 ? accuracy.chunked.meanRelError.toFixed(2) + "%" : "N/A", 10),
    ].join(" | ");
    lines.push(row);
  }

  // ── Section 2: Per-experiment detail ─────────────────────────────
  lines.push("");
  lines.push("");
  lines.push("2. DETAILED PER-EXPERIMENT RESULTS");
  lines.push(hr);

  for (const exp of experiments) {
    const stats = extractApproachStats(exp.iterations);
    const accuracy = computeAccuracy(exp.iterations);

    lines.push("");
    lines.push(`--- ${exp.label} (${exp.dir}) ---`);
    lines.push(`Config: ${exp.config.data} | Agg: ${exp.config.aggregation} | SubWindow: ${exp.config.subWindowRange}/${exp.config.subWindowStep}ms | Freq: ${exp.config.wearableFreq}Hz`);
    lines.push("");

    // Latency table
    lines.push("  Latency (ms):");
    lines.push(`  ${"Approach".padEnd(16)} | ${"Mean".padStart(8)} | ${"StdDev".padStart(8)} | ${"Median".padStart(8)} | ${"Min".padStart(8)} | ${"Max".padStart(8)} | ${"N".padStart(4)}`);
    lines.push(`  ${"-".repeat(75)}`);
    for (const approach of APPROACHES) {
      const s = stats[approach];
      if (s.n === 0) {
        lines.push(`  ${approach.padEnd(16)} | ${"N/A".padStart(8)} | ${"".padStart(8)} | ${"".padStart(8)} | ${"".padStart(8)} | ${"".padStart(8)} | ${pad("0", 4)}`);
      } else {
        lines.push(`  ${approach.padEnd(16)} | ${pad(s.latency.mean.toFixed(0), 8)} | ${pad(s.latency.stdDev.toFixed(0), 8)} | ${pad(s.latency.median.toFixed(0), 8)} | ${pad(s.latency.min.toFixed(0), 8)} | ${pad(s.latency.max.toFixed(0), 8)} | ${pad(s.n.toString(), 4)}`);
      }
    }

    // Resource table
    lines.push("");
    lines.push("  Resources:");
    lines.push(`  ${"Approach".padEnd(16)} | ${"Avg CPU %".padStart(10)} | ${"Max Mem MB".padStart(10)}`);
    lines.push(`  ${"-".repeat(45)}`);
    for (const approach of APPROACHES) {
      const s = stats[approach];
      if (s.n === 0) {
        lines.push(`  ${approach.padEnd(16)} | ${"N/A".padStart(10)} | ${"N/A".padStart(10)}`);
      } else {
        lines.push(`  ${approach.padEnd(16)} | ${pad(s.cpu.mean.toFixed(1), 10)} | ${pad(s.memory.maxMB.toFixed(1), 10)}`);
      }
    }

    // Accuracy table
    lines.push("");
    lines.push("  Accuracy (vs fetching baseline):");
    lines.push(`  ${"Approach".padEnd(16)} | ${"Exact".padStart(6)} | ${"Mean Err%".padStart(10)} | ${"Max Err%".padStart(10)} | ${"Median Err%".padStart(11)}`);
    lines.push(`  ${"-".repeat(65)}`);
    for (const approach of ACCURACY_APPROACHES) {
      const a = accuracy[approach];
      if (a.n === 0) {
        lines.push(`  ${approach.padEnd(16)} | ${"N/A".padStart(6)} | ${"N/A".padStart(10)} | ${"N/A".padStart(10)} | ${"N/A".padStart(11)}`);
      } else {
        lines.push(`  ${approach.padEnd(16)} | ${pad(`${a.exactMatches}/${a.n}`, 6)} | ${pad(a.meanRelError.toFixed(4), 10)} | ${pad(a.maxRelError.toFixed(4), 10)} | ${pad(a.medianRelError.toFixed(4), 11)}`);
      }
    }

    // Per-iteration values
    lines.push("");
    lines.push("  Per-iteration values:");
    lines.push(`  ${"Iter".padEnd(5)} | ${"Fetching".padStart(14)} | ${"Naive Dist".padStart(14)} | ${"Approximation".padStart(14)} | ${"Chunked".padStart(14)} | ${"Naive Err%".padStart(11)} | ${"Approx Err%".padStart(11)} | ${"Chunk Err%".padStart(10)}`);
    lines.push(`  ${"-".repeat(122)}`);
    for (let i = 0; i < exp.iterations.length; i++) {
      const iter = exp.iterations[i];
      const f = iter.results?.fetching;
      const n = iter.results?.naive_distributed;
      const a = iter.results?.approximation;
      const c = iter.results?.chunked;

      const fVal = f && !f.error && f.value != null ? f.value.toFixed(6) : "ERR";
      const nVal = n && !n.error && n.value != null ? n.value.toFixed(6) : "ERR";
      const aVal = a && !a.error && a.value != null ? a.value.toFixed(6) : "ERR";
      const cVal = c && !c.error && c.value != null ? c.value.toFixed(6) : "ERR";

      let nErr = "N/A";
      let aErr = "N/A";
      let cErr = "N/A";
      if (f && n && f.value != null && n.value != null && f.value !== 0) {
        nErr = ((Math.abs(n.value - f.value) / Math.abs(f.value)) * 100).toFixed(4) + "%";
      }
      if (f && a && f.value != null && a.value != null && f.value !== 0) {
        aErr = ((Math.abs(a.value - f.value) / Math.abs(f.value)) * 100).toFixed(4) + "%";
      }
      if (f && c && f.value != null && c.value != null && f.value !== 0) {
        cErr = ((Math.abs(c.value - f.value) / Math.abs(f.value)) * 100).toFixed(4) + "%";
      }

      lines.push(`  ${pad((i + 1).toString(), 5, "left")} | ${pad(fVal, 14)} | ${pad(nVal, 14)} | ${pad(aVal, 14)} | ${pad(cVal, 14)} | ${pad(nErr, 11)} | ${pad(aErr, 11)} | ${pad(cErr, 10)}`);
    }
  }

  // ── Section 3: Cross-experiment accuracy comparison ──────────────
  lines.push("");
  lines.push("");
  lines.push("3. ACCURACY COMPARISON ACROSS EXPERIMENTS");
  lines.push(hr);
  lines.push("");
  lines.push("This section highlights where the non-fetching approaches diverge from the");
  lines.push("ground truth baseline (fetching), while preserving the chunked accuracy story.");
  lines.push("");

  const accHeader = [
    pad("Experiment", 40, "left"),
    pad("Naive Mean Err%", 16),
    pad("Naive Max Err%", 15),
    pad("Approx Mean Err%", 17),
    pad("Approx Max Err%", 15),
    pad("Chunk Mean Err%", 15),
    pad("Chunk Max Err%", 14),
    pad("Chunk Exact", 11),
  ].join(" | ");
  lines.push(accHeader);
  lines.push("-".repeat(accHeader.length));

  for (const exp of experiments) {
    const accuracy = computeAccuracy(exp.iterations);
    const n = accuracy.naive_distributed;
    const a = accuracy.approximation;
    const c = accuracy.chunked;

    const row = [
      pad(exp.label.substring(0, 40), 40, "left"),
      pad(n.n > 0 ? n.meanRelError.toFixed(4) + "%" : "N/A", 16),
      pad(n.n > 0 ? n.maxRelError.toFixed(4) + "%" : "N/A", 15),
      pad(a.n > 0 ? a.meanRelError.toFixed(4) + "%" : "N/A", 17),
      pad(a.n > 0 ? a.maxRelError.toFixed(4) + "%" : "N/A", 15),
      pad(c.n > 0 ? c.meanRelError.toFixed(4) + "%" : "N/A", 15),
      pad(c.n > 0 ? c.maxRelError.toFixed(4) + "%" : "N/A", 14),
      pad(c.n > 0 ? `${c.exactMatches}/${c.n}` : "N/A", 11),
    ].join(" | ");
    lines.push(row);
  }

  // ── Section 4: Resource efficiency comparison ────────────────────
  lines.push("");
  lines.push("");
  lines.push("4. RESOURCE EFFICIENCY ACROSS EXPERIMENTS");
  lines.push(hr);
  lines.push("");

  const resHeader = [
    pad("Experiment", 40, "left"),
    pad("Fetch CPU%", 10),
    pad("Fetch Mem", 10),
    pad("Naive CPU%", 10),
    pad("Naive Mem", 10),
    pad("Approx CPU%", 11),
    pad("Approx Mem", 10),
    pad("Chunk CPU%", 10),
    pad("Chunk Mem", 10),
  ].join(" | ");
  lines.push(resHeader);
  lines.push("-".repeat(resHeader.length));

  for (const exp of experiments) {
    const stats = extractApproachStats(exp.iterations);

    const row = [
      pad(exp.label.substring(0, 40), 40, "left"),
      pad(stats.fetching.n > 0 ? stats.fetching.cpu.mean.toFixed(1) : "N/A", 10),
      pad(stats.fetching.n > 0 ? stats.fetching.memory.maxMB.toFixed(0) + "MB" : "N/A", 10),
      pad(stats.naive_distributed.n > 0 ? stats.naive_distributed.cpu.mean.toFixed(1) : "N/A", 10),
      pad(stats.naive_distributed.n > 0 ? stats.naive_distributed.memory.maxMB.toFixed(0) + "MB" : "N/A", 10),
      pad(stats.approximation.n > 0 ? stats.approximation.cpu.mean.toFixed(1) : "N/A", 11),
      pad(stats.approximation.n > 0 ? stats.approximation.memory.maxMB.toFixed(0) + "MB" : "N/A", 10),
      pad(stats.chunked.n > 0 ? stats.chunked.cpu.mean.toFixed(1) : "N/A", 10),
      pad(stats.chunked.n > 0 ? stats.chunked.memory.maxMB.toFixed(0) + "MB" : "N/A", 10),
    ].join(" | ");
    lines.push(row);
  }

  // ── Section 5: Key findings ──────────────────────────────────────
  lines.push("");
  lines.push("");
  lines.push("5. KEY FINDINGS");
  lines.push(hr);
  lines.push("");

  // Compute aggregate stats
  let totalNaiveExact = 0;
  let totalNaiveN = 0;
  let totalApproxExact = 0;
  let totalApproxN = 0;
  let totalChunkExact = 0;
  let totalChunkN = 0;
  let naiveErrors = [];
  let approxErrors = [];
  let chunkErrors = [];

  for (const exp of experiments) {
    const accuracy = computeAccuracy(exp.iterations);
    totalNaiveExact += accuracy.naive_distributed.exactMatches;
    totalNaiveN += accuracy.naive_distributed.n;
    totalApproxExact += accuracy.approximation.exactMatches;
    totalApproxN += accuracy.approximation.n;
    totalChunkExact += accuracy.chunked.exactMatches;
    totalChunkN += accuracy.chunked.n;
    if (accuracy.naive_distributed.n > 0) naiveErrors.push(accuracy.naive_distributed.meanRelError);
    if (accuracy.approximation.n > 0) approxErrors.push(accuracy.approximation.meanRelError);
    if (accuracy.chunked.n > 0) chunkErrors.push(accuracy.chunked.meanRelError);
  }

  lines.push(`Across ${experiments.length} experiments:`);
  lines.push("");
  lines.push(`  Accuracy:`);
  lines.push(`    Naive Distributed: ${totalNaiveExact}/${totalNaiveN} exact matches (${totalNaiveN > 0 ? ((totalNaiveExact / totalNaiveN) * 100).toFixed(1) : 0}%), mean error ${mean(naiveErrors).toFixed(4)}%`);
  lines.push(`    Approximation: ${totalApproxExact}/${totalApproxN} exact matches (${totalApproxN > 0 ? ((totalApproxExact / totalApproxN) * 100).toFixed(1) : 0}%), mean error ${mean(approxErrors).toFixed(4)}%`);
  lines.push(`    Chunked:       ${totalChunkExact}/${totalChunkN} exact matches (${totalChunkN > 0 ? ((totalChunkExact / totalChunkN) * 100).toFixed(1) : 0}%), mean error ${mean(chunkErrors).toFixed(4)}%`);
  lines.push("");

  let worstNaive = { label: "", error: 0 };
  // Find worst cases for approximation
  let worstApprox = { label: "", error: 0 };
  for (const exp of experiments) {
    const accuracy = computeAccuracy(exp.iterations);
    if (accuracy.naive_distributed.meanRelError > worstNaive.error) {
      worstNaive = { label: exp.label, error: accuracy.naive_distributed.meanRelError };
    }
    if (accuracy.approximation.meanRelError > worstApprox.error) {
      worstApprox = { label: exp.label, error: accuracy.approximation.meanRelError };
    }
  }
  lines.push(`  Worst naive distributed accuracy: ${worstNaive.label || "N/A"} (${worstNaive.error.toFixed(4)}% mean error)`);
  lines.push(`  Worst approximation accuracy: ${worstApprox.label || "N/A"} (${worstApprox.error.toFixed(4)}% mean error)`);

  // Check if chunked is always exact
  const chunkedAlwaysExact = totalChunkExact === totalChunkN && totalChunkN > 0;
  if (chunkedAlwaysExact) {
    lines.push(`  Chunked approach: EXACT match with fetching in ALL ${totalChunkN} iterations across all experiments`);
  }

  lines.push("");
  lines.push(hr);

  return lines.join("\n");
}

// ── CSV export for further analysis ──────────────────────────────────

function generateCSV(experiments) {
  const rows = [
    [
      "experiment", "iteration", "approach",
      "latency_ms", "value",
      "avg_cpu_pct", "max_memory_mb",
      "error_vs_fetching_pct", "exact_match",
      "data_source", "aggregation", "sub_window_range", "sub_window_step", "wearable_freq",
    ].join(","),
  ];

  for (const exp of experiments) {
    for (let i = 0; i < exp.iterations.length; i++) {
      const iter = exp.iterations[i];
      const fetchVal = iter.results?.fetching?.value;

      for (const approach of APPROACHES) {
        const r = iter.results?.[approach];
        if (!r || r.error) continue;

        const errPct = (approach !== "fetching" && fetchVal != null && fetchVal !== 0)
          ? ((Math.abs(r.value - fetchVal) / Math.abs(fetchVal)) * 100).toFixed(6)
          : "0";
        const exact = (approach !== "fetching" && fetchVal != null)
          ? (Math.abs(r.value - fetchVal) < 1e-6 ? "true" : "false")
          : "true";

        rows.push([
          `"${exp.label}"`,
          i + 1,
          approach,
          r.latency || "",
          r.value != null ? r.value : "",
          r.resources?.avgCpu?.toFixed(2) || "",
          r.resources?.maxMemory ? (r.resources.maxMemory / 1024 / 1024).toFixed(1) : "",
          errPct,
          exact,
          `"${exp.config.data}"`,
          exp.config.aggregation,
          exp.config.subWindowRange,
          exp.config.subWindowStep,
          exp.config.wearableFreq,
        ].join(","));
      }
    }
  }

  return rows.join("\n");
}

// ── Main ─────────────────────────────────────────────────────────────

function main() {
  console.log("Loading experiments from:", RESULTS_DIR);

  const experiments = loadAllExperiments();

  if (experiments.length === 0) {
    console.error("No experiment results found!");
    process.exit(1);
  }

  console.log(`Found ${experiments.length} experiments:\n`);
  for (const exp of experiments) {
    console.log(`  - ${exp.label} (${exp.iterations.length} iterations) [${exp.dir}]`);
  }

  // Generate text report
  const report = generateReport(experiments);
  console.log("\n" + report);

  // Determine output directory
  const args = process.argv.slice(2);
  let outputDir = RESULTS_DIR;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--output-dir" && args[i + 1]) {
      outputDir = args[i + 1];
      if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
    }
  }

  // Save report
  const reportPath = path.join(outputDir, "cross-experiment-analysis.txt");
  fs.writeFileSync(reportPath, report);
  console.log(`\nText report saved to: ${reportPath}`);

  // Save CSV
  const csv = generateCSV(experiments);
  const csvPath = path.join(outputDir, "all-experiments.csv");
  fs.writeFileSync(csvPath, csv);
  console.log(`CSV data saved to: ${csvPath}`);

  // Save JSON summary
  const jsonSummary = experiments.map((exp) => ({
    label: exp.label,
    dir: exp.dir,
    config: exp.config,
    stats: extractApproachStats(exp.iterations),
    accuracy: computeAccuracy(exp.iterations),
  }));
  const jsonPath = path.join(outputDir, "cross-experiment-summary.json");
  fs.writeFileSync(jsonPath, JSON.stringify(jsonSummary, null, 2));
  console.log(`JSON summary saved to: ${jsonPath}`);
}

main();
