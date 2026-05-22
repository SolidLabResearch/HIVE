#!/usr/bin/env node

/**
 * INTELLIGENT ROUTING BENCHMARK  (v2 — improved)
 *
 * Three-phase evaluation of HiveScoutBee stream signature analysis and routing.
 *
 * PHASE 1 — Main MQTT-based benchmark (5 conditions, real + pattern data)
 *   Layer 1: Signature validation  — does the extracted signature match expected
 *            stream properties for each known condition?
 *   Layer 2: Routing accuracy      — does the recommendation match the oracle?
 *   Oracle: lowest-latency approach among those with mean accuracy error ≤ τ vs Fetching.
 *
 * PHASE 2 — Simulated conditions (no MQTT publisher required)
 *   Uses HiveScoutBeeWrapper.simulateStreamPattern() directly to test each
 *   routing branch, including the approximation branch which is never triggered
 *   by real/pattern data due to high triple-count.
 *
 * PHASE 3 — Observation window sensitivity (simulated)
 *   Tests routing stability across increasing data volumes (≈15 s, 30 s, 60 s)
 *   to identify the minimum observation needed for stable routing decisions.
 *
 * Usage:
 *   node experiments/intelligent-routing-benchmark.js
 *   node experiments/intelligent-routing-benchmark.js --accuracy-tolerance 0.3
 *   node experiments/intelligent-routing-benchmark.js --observation-secs 35
 */

const { spawn, execSync } = require("child_process");
const fs   = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const { createBenchmarkReplayRunEnv } = require("./utils/benchmarkReplayEnv");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const replayEnv = createBenchmarkReplayRunEnv(process.env);
const RESULTS_DIR  = path.join(__dirname, "benchmark-results");

// ── CLI args ──────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let observationSecs   = 35;
let accuracyTolerance = 0.3; // default: stricter tolerance (paper-motivated)

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--observation-secs")   { observationSecs   = parseInt(args[i + 1], 10); i++; }
  if (args[i] === "--accuracy-tolerance") { accuracyTolerance = parseFloat(args[i + 1]);   i++; }
}

// ── HiveScoutBeeWrapper ───────────────────────────────────────────────────────
const { HiveScoutBeeWrapper } = require("../dist/services/HiveScoutBee");

// ── PHASE 1: MQTT-based conditions ───────────────────────────────────────────
// Real-data rows collapsed to ONE: the aggregation function does not affect
// stream characteristics — the router observes raw sensor values, not the query.
// (Note: this is a known limitation — the router is query-agnostic.)
const MAIN_CONDITIONS = [
  { label: "Real data (16 Hz)",     pattern: null,                    agg: "AVG", wearableFreq: "16" },
  { label: "Pattern: spike",        pattern: "spike_pattern",          agg: "AVG", wearableFreq: "4"  },
  { label: "Pattern: oscillation",  pattern: "high_freq_oscillation",  agg: "AVG", wearableFreq: "4"  },
  { label: "Pattern: noise σ=2",    pattern: "noise_2.0",              agg: "AVG", wearableFreq: "4"  },
  { label: "Pattern: low var.",     pattern: "low_variability",        agg: "AVG", wearableFreq: "4"  },
];

// Layer-1 expected signature properties per condition.
// These are design-time expectations derived from the known data distributions.
const EXPECTED_SIG = {
  "Real data (16 Hz)":    { streamType: "volatile", varianceMin: 100, varianceMax: Infinity, note: "accel. x, high variance" },
  "Pattern: spike":       { streamType: "stable",   varianceMin: 0,   varianceMax: 5,        note: "sudden jumps, low baseline var." },
  "Pattern: oscillation": { streamType: "stable",   varianceMin: 0,   varianceMax: 15,       note: "rapid oscillation, bounded" },
  "Pattern: noise σ=2":   { streamType: "volatile", varianceMin: 100, varianceMax: Infinity, note: "high-noise, unpredictable" },
  "Pattern: low var.":    { streamType: "stable",   varianceMin: 0,   varianceMax: 1,        note: "near-constant baseline" },
};

// ── PHASE 2: Simulated conditions (no MQTT) ───────────────────────────────────
// Directly calls simulateStreamPattern() to exercise each routing branch,
// including the approximation branch (low tripleCount forces it to win).
const SIM_CONDITIONS = [
  // approximation branch: low variance + low tripleCount (< 100) so chunked cannot match
  { label: "Sim: stable  (30 pts)",  simPattern: "stable",   simPoints: 30,  expectedApproach: "approximation",
    note: "tripleCount=60 < 100 threshold; variance ≈ 2" },
  { label: "Sim: stable  (60 pts)",  simPattern: "stable",   simPoints: 60,  expectedApproach: "approximation",
    note: "tripleCount=120 ≥ 100; chunked may now compete" },
  // fetching branch: high variance
  { label: "Sim: volatile (100 pts)",simPattern: "volatile",  simPoints: 100, expectedApproach: "fetching",
    note: "variance >> 50 threshold" },
  // chunked branch: high tripleCount + moderate variance
  { label: "Sim: periodic (100 pts)",simPattern: "periodic",  simPoints: 100, expectedApproach: "chunked",
    note: "tripleCount=200, variance ≈ moderate" },
];

// ── PHASE 3: Observation window sensitivity ───────────────────────────────────
// Point counts that approximate different observation durations at 8 pts/s
// (combined wearable + smartphone rate in MQTT benchmark).
const WINDOW_SIZES = [
  { pts: 40,  label: "~5 s"  },
  { pts: 120, label: "~15 s" },
  { pts: 240, label: "~30 s" },
  { pts: 480, label: "~60 s" },
];
const WINDOW_PATTERNS = ["stable", "volatile", "periodic", "mixed"];

// ── Approach name normaliser ──────────────────────────────────────────────────
const SHORT = {
  "approximation-approach": "approximation",
  "fetching-client-side":   "fetching",
  "chunked-approach":       "chunked",
  "streaming-query-hive":   "hive",
};

// ── Resolve DATA_PATH for publisher ──────────────────────────────────────────
function resolveDataPath(pattern) {
  if (!pattern) return ".";
  const base = path.join(PROJECT_ROOT, "src/streamer/data");
  for (const cand of [`noisy_datasets/${pattern}`, `rate_comparison/${pattern}`, `pattern_comparison/${pattern}`, pattern]) {
    if (fs.existsSync(path.join(base, cand, "smartphone.acceleration.x/data.nt"))) return cand;
  }
  return null;
}

// ── Value extractor (same logic as unified-benchmark.js) ─────────────────────
function extractValue(message) {
  const s = message.toString();
  try { const j = JSON.parse(s); if (typeof j.value === "number") return j.value; if (typeof j.avgValue === "number") return j.avgValue; } catch (e) {}
  let r = s;
  try { const p = JSON.parse(s); if (typeof p === "string") r = p; } catch (e) {}
  const m1 = r.match(/hasValue[>]?\s+"(-?[0-9.eE+]+)"/);   if (m1) return parseFloat(m1[1]);
  const m2 = r.match(/"(-?[0-9]+\.?[0-9]*(?:[eE][+-]?[0-9]+)?)"/); if (m2) return parseFloat(m2[1]);
  const m3 = r.match(/(-?[0-9]+\.[0-9]+)/);                 if (m3) return parseFloat(m3[1]);
  return null;
}

// ── Kill stray processes ──────────────────────────────────────────────────────
function cleanup() {
  try { execSync('pkill -f "node.*publish.js" 2>/dev/null || true', { stdio: "ignore" }); } catch (e) {}
  try { execSync('pkill -f "BeeWorker" 2>/dev/null || true',        { stdio: "ignore" }); } catch (e) {}
}

// ── ORACLE LOADING (same accuracy-aware logic as before) ─────────────────────
function loadOracleResults() {
  const oracle = [];
  if (!fs.existsSync(RESULTS_DIR)) return oracle;
  for (const dir of fs.readdirSync(RESULTS_DIR)) {
    if (dir.startsWith("intelligent-routing-")) continue;
    const rp = path.join(RESULTS_DIR, dir, "benchmark-report.json");
    if (!fs.existsSync(rp)) continue;
    try {
      const { config, summary, byIteration } = JSON.parse(fs.readFileSync(rp, "utf8"));
      if (!config || !summary) continue;
      const latencies = {}, errSamples = {};
      for (const [a, d] of Object.entries(summary)) {
        if (d.latency?.avg != null) latencies[a] = d.latency.avg;
      }
      if (byIteration) {
        for (const iter of Object.values(byIteration)) {
          const fval = iter.results?.fetching?.value;
          if (fval == null || fval === 0) continue;
          for (const a of Object.keys(latencies)) {
            const av = iter.results?.[a]?.value;
            if (av == null) continue;
            (errSamples[a] = errSamples[a] || []).push(Math.abs((av - fval) / fval * 100));
          }
        }
      }
      const accuracyErrors = { fetching: 0 };
      for (const [a, s] of Object.entries(errSamples)) accuracyErrors[a] = s.reduce((x, y) => x + y, 0) / s.length;
      oracle.push({ pattern: config.pattern, agg: config.aggregationFunc, wearableFreq: config.wearableFrequency, latencies, accuracyErrors, dir });
    } catch (e) {}
  }
  return oracle;
}

function findOracle(oracleResults, condition, tol) {
  const matches = oracleResults.filter(o => o.pattern === condition.pattern && o.agg === condition.agg && o.wearableFreq === condition.wearableFreq);
  if (!matches.length) return null;
  const ml = {}, me = {}, cnt = {};
  for (const m of matches) {
    for (const [a, v] of Object.entries(m.latencies))       { ml[a] = (ml[a] || 0) + v; cnt[a] = (cnt[a] || 0) + 1; }
    for (const [a, v] of Object.entries(m.accuracyErrors || {})) me[a] = (me[a] || 0) + v;
  }
  for (const a of Object.keys(ml)) { ml[a] /= cnt[a]; me[a] = (me[a] || 0) / matches.length; }
  me.fetching = 0;
  const eligible = Object.keys(ml).filter(a => (me[a] ?? 0) <= tol).sort((a, b) => ml[a] - ml[b]);
  if (!eligible.length) return null;
  return { bestApproach: eligible[0], eligibleApproaches: eligible, excludedApproaches: Object.keys(ml).filter(a => !eligible.includes(a)), latencies: ml, accuracyErrors: me, runCount: matches.length };
}

// ── PHASE 1: Observe a single MQTT condition ──────────────────────────────────
async function observeCondition(condition) {
  const dataPath = resolveDataPath(condition.pattern);
  if (condition.pattern && !dataPath) return { error: `Pattern data not found: ${condition.pattern}`, pts: 0 };

  const scoutBee = new HiveScoutBeeWrapper();
  let mqttClient = null, publisherProc = null, pts = 0;

  return new Promise(async (resolve) => {
    try {
      mqttClient = mqtt.connect("mqtt://localhost:1883", { clientId: `routing-${Date.now()}`, clean: true, reconnectPeriod: 0 });
      await new Promise((res, rej) => {
        const t = setTimeout(() => rej(new Error("MQTT timeout")), 8000);
        mqttClient.once("connect", () => { clearTimeout(t); res(); });
        mqttClient.once("error",   (e) => { clearTimeout(t); rej(e); });
      });
    } catch (e) { resolve({ error: `MQTT: ${e.message}`, pts: 0 }); return; }

    mqttClient.subscribe("wearableX",   { qos: 0 });
    mqttClient.subscribe("smartphoneX", { qos: 0 });
    mqttClient.on("message", (_t, msg) => {
      const v = extractValue(msg);
      if (v !== null && !isNaN(v)) { scoutBee.addDataPoint(Date.now(), v, _t); pts++; }
    });

    const env = replayEnv.withBenchmarkReplayEnv({ ...process.env, DATA_PATH: dataPath || ".", AGGREGATION_FUNC: condition.agg,
                  PUBLISH_MODE: "uniform", SUB_WINDOW_RANGE: "60000", SUB_WINDOW_STEP: "30000",
                  WEARABLE_FREQUENCY: condition.wearableFreq, SESSION_ID: `routing-${Date.now()}` });
    publisherProc = spawn("node", [path.join(PROJECT_ROOT, "dist/streamer/src/publish.js")],
                          { stdio: ["inherit", "pipe", "pipe"], cwd: PROJECT_ROOT, env });

    setTimeout(() => {
      try { publisherProc.kill("SIGTERM"); } catch (e) {}
      cleanup();
      try { mqttClient.unsubscribe("wearableX"); mqttClient.unsubscribe("smartphoneX"); mqttClient.end(true); } catch (e) {}

      if (pts < 5) { resolve({ error: `Too few data points (${pts})`, pts }); return; }
      const rec = scoutBee.getApproachRecommendation();
      if (!rec) { resolve({ error: `No recommendation (${pts} pts)`, pts }); return; }

      resolve({
        recommended:  SHORT[rec.recommendedApproach] || rec.recommendedApproach,
        confidence:   rec.confidence,
        streamType:   rec.signature.classification.streamType,
        variance:     rec.signature.statistics.variance,
        complexity:   rec.signature.classification.complexity,
        aliasingRisk: rec.signature.classification.aliasing_risk,
        reasoning:    rec.reasoning,
        pts,
      });
    }, observationSecs * 1000);
  });
}

// ── PHASE 2: Simulated observation (no MQTT) ──────────────────────────────────
function observeSimulated(simPattern, simPoints) {
  const scoutBee = new HiveScoutBeeWrapper();
  scoutBee.simulateStreamPattern(simPattern, simPoints, `sim_${simPattern}`);
  const rec = scoutBee.getApproachRecommendation();
  if (!rec) return { error: "No recommendation", pts: simPoints };
  return {
    recommended:  SHORT[rec.recommendedApproach] || rec.recommendedApproach,
    confidence:   rec.confidence,
    streamType:   rec.signature.classification.streamType,
    variance:     rec.signature.statistics.variance,
    tripleCount:  rec.signature.dataPoints,
    reasoning:    rec.reasoning,
    pts:          simPoints,
  };
}

// ── PHASE 1 helpers: signature validation ────────────────────────────────────
function validateSignature(label, extracted) {
  const exp = EXPECTED_SIG[label];
  if (!exp) return { valid: null, note: "no expectation defined" };
  const typeOk = extracted.streamType === exp.streamType;
  const varOk  = extracted.variance >= exp.varianceMin && extracted.variance <= exp.varianceMax;
  return {
    valid:            typeOk && varOk,
    expectedType:     exp.streamType,
    gotType:          extracted.streamType,
    typeMatch:        typeOk,
    varianceInRange:  varOk,
    varianceRange:    `[${exp.varianceMin === 0 ? "0" : exp.varianceMin}, ${exp.varianceMax === Infinity ? "∞" : exp.varianceMax}]`,
    note:             exp.note,
  };
}

// ── PHASE 3: Window sensitivity (simulated) ───────────────────────────────────
function runWindowSensitivity() {
  console.log("\n" + "─".repeat(90));
  console.log("PHASE 3 — OBSERVATION WINDOW SENSITIVITY  (simulated data)");
  console.log("Shows when the routing recommendation first stabilises as data volume grows.");
  console.log("─".repeat(90));

  const header =
    "Stream type".padEnd(12) + "| " +
    WINDOW_SIZES.map(w => w.label.padStart(7)).join(" | ") +
    "  (data points: " + WINDOW_SIZES.map(w => String(w.pts).padStart(4)).join(" / ") + ")";
  console.log(header);
  console.log("-".repeat(90));

  const results = {};
  for (const pat of WINDOW_PATTERNS) {
    const row = [];
    for (const ws of WINDOW_SIZES) {
      const r = observeSimulated(pat, ws.pts);
      row.push(r.error ? "ERROR" : r.recommended);
    }
    results[pat] = row;

    // Check where it stabilises (first point where all subsequent match)
    let stableAt = null;
    for (let i = 0; i < row.length; i++) {
      if (row.slice(i).every(v => v === row[i])) { stableAt = WINDOW_SIZES[i].label; break; }
    }

    console.log(
      pat.padEnd(12) + "| " +
      row.map(v => v.substring(0, 7).padStart(7)).join(" | ") +
      `   (stable from ${stableAt || "never"})`
    );
  }
  console.log("─".repeat(90));
  return results;
}

// ── Printing helpers ──────────────────────────────────────────────────────────
function printSignatureValidationTable(rows) {
  const W = 105;
  console.log("\n" + "─".repeat(W));
  console.log("PHASE 1 — LAYER 1: STREAM SIGNATURE VALIDATION");
  console.log("Checks whether the extracted stream signature matches known properties of each data source.");
  console.log("─".repeat(W));
  console.log(
    "Condition".padEnd(28) + "| " +
    "Exp. type ".padEnd(11) + "| " +
    "Got type  ".padEnd(11) + "| " +
    "Variance  ".padEnd(11) + "| " +
    "Exp. range    ".padEnd(15) + "| " +
    "Var ✓ ".padEnd(7) + "| " +
    "Type ✓ ".padEnd(7) + "| " +
    "Overall"
  );
  console.log("-".repeat(W));
  let pass = 0;
  for (const r of rows) {
    if (!r.validation) continue;
    const v = r.validation;
    const varChk  = v.varianceInRange ? "✓" : "✗";
    const typeChk = v.typeMatch       ? "✓" : "✗";
    const overall = v.valid           ? "PASS" : "FAIL";
    if (v.valid) pass++;
    console.log(
      r.label.padEnd(28)                              + "| " +
      (v.expectedType || "N/A").padEnd(11)            + "| " +
      (v.gotType      || "N/A").padEnd(11)            + "| " +
      (r.result.variance != null ? r.result.variance.toFixed(2).padStart(8) : "N/A".padStart(8)) + "   | " +
      v.varianceRange.padEnd(15)                      + "| " +
      varChk.padEnd(7)                                + "| " +
      typeChk.padEnd(7)                               + "| " +
      overall
    );
  }
  console.log("─".repeat(W));
  console.log(`Signature validation: ${pass}/${rows.filter(r => r.validation).length} passed`);
}

function printRoutingTable(rows, tol) {
  const W = 112;
  console.log("\n" + "─".repeat(W));
  console.log(`PHASE 1 — LAYER 2: ROUTING ACCURACY  (accuracy tolerance τ = ${tol}%)`);
  console.log("─".repeat(W));
  console.log(
    "Condition".padEnd(28) + "| " +
    "Type     ".padEnd(10) + "| " +
    "Recommended  ".padEnd(14) + "| " +
    "Oracle       ".padEnd(14) + "| " +
    "Excluded ".padEnd(10) + "| " +
    "Match ".padEnd(7) + "| " +
    "Conf   ".padEnd(8) + "| " +
    "ε approx"
  );
  console.log("-".repeat(W));
  let correct = 0, total = 0;
  for (const r of rows) {
    const matchStr = r.match === true ? "✓" : r.match === false ? "✗" : "N/A";
    const conf     = r.confidence != null ? `${(r.confidence * 100).toFixed(0)}%`.padStart(5) : "  N/A";
    const excl     = (r.oracleExcluded || []).join(",") || "none";
    const err      = r.approxErr != null ? `${r.approxErr.toFixed(3)}%` : "N/A";
    console.log(
      r.label.padEnd(28) + "| " +
      (r.streamType || "N/A").padEnd(10) + "| " +
      (r.recommended || "ERROR").padEnd(14) + "| " +
      (r.oracle || "N/A").padEnd(14) + "| " +
      excl.padEnd(10) + "| " +
      matchStr.padEnd(7) + "| " +
      conf.padEnd(8) + "| " +
      err
    );
    if (r.match === true) correct++;
    if (r.match === true || r.match === false) total++;
  }
  console.log("─".repeat(W));
  if (total > 0) console.log(`Routing accuracy: ${correct}/${total} (${(correct/total*100).toFixed(1)}%) at τ=${tol}%`);
}

function printSensitivityTable(rows, oracleResults) {
  const TOLS = [0.1, 0.2, 0.3, 0.5, 1.0];
  console.log("\n" + "─".repeat(80));
  console.log("THRESHOLD SENSITIVITY ANALYSIS");
  console.log("─".repeat(80));
  console.log("τ (%)     | Correct / Total | Accuracy | Oracle changes vs τ=0.3%");
  console.log("-".repeat(80));
  const base = rows.map(r => {
    if (!r._condition) return { ...r, baseOracle: r.oracle };
    const o = findOracle(oracleResults, r._condition, 0.3);
    return { ...r, baseOracle: o?.bestApproach || r.oracle };
  });
  for (const tol of TOLS) {
    let correct = 0, total = 0;
    const flips = [];
    for (const r of base) {
      if (!r._condition) continue;
      const o = findOracle(oracleResults, r._condition, tol);
      if (!o) continue;
      total++;
      if (o.bestApproach === r.recommended) correct++;
      if (o.bestApproach !== r.baseOracle) flips.push(`${r.label.trim().split(" ").pop()}→${o.bestApproach}`);
    }
    if (!total) { console.log(`  ${(tol+"%").padStart(6)}  | N/A`); continue; }
    const pct = (correct/total*100).toFixed(1);
    const bold = tol === 0.3 ? "◄ default" : "";
    console.log(`  ${(tol+"%").padStart(6)}  | ${String(correct).padStart(7)} / ${String(total).padEnd(5)} | ${(pct+"%").padStart(7)} | ${flips.join("; ") || "none"} ${bold}`);
  }
  console.log("─".repeat(80));
}

function printSimTable(rows) {
  const W = 105;
  console.log("\n" + "─".repeat(W));
  console.log("PHASE 2 — SIMULATED CONDITIONS  (direct HiveScoutBeeWrapper, no MQTT publisher)");
  console.log("Exercises each routing branch including the approximation branch.");
  console.log("─".repeat(W));
  console.log(
    "Condition".padEnd(28) + "| " +
    "Pts ".padEnd(6) + "| " +
    "tripleCount".padEnd(12) + "| " +
    "Variance  ".padEnd(11) + "| " +
    "Type     ".padEnd(10) + "| " +
    "Recommended  ".padEnd(14) + "| " +
    "Expected     ".padEnd(14) + "| " +
    "Match"
  );
  console.log("-".repeat(W));
  let correct = 0;
  for (const r of rows) {
    const match = r.result.recommended === r.expectedApproach ? "✓" : "✗";
    if (match === "✓") correct++;
    const tc = r.result.tripleCount != null ? String(r.result.tripleCount) : "N/A";
    const va = r.result.variance    != null ? r.result.variance.toFixed(2)  : "N/A";
    console.log(
      r.label.padEnd(28)                          + "| " +
      String(r.simPoints).padEnd(6)               + "| " +
      tc.padEnd(12)                               + "| " +
      va.padEnd(11)                               + "| " +
      (r.result.streamType || "N/A").padEnd(10)   + "| " +
      (r.result.recommended || "ERROR").padEnd(14) + "| " +
      r.expectedApproach.padEnd(14)               + "| " +
      match
    );
  }
  console.log("─".repeat(W));
  console.log(`Simulated routing: ${correct}/${rows.length} matched expected approach`);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log("\n" + "█".repeat(75));
  console.log(" INTELLIGENT ROUTING BENCHMARK  (v2)");
  console.log(` PHASE 1  observation window : ${observationSecs}s per condition`);
  console.log(` PHASE 1  accuracy tolerance : τ = ${accuracyTolerance}%`);
  console.log(` Timestamp                   : ${new Date().toISOString()}`);
  console.log("█".repeat(75) + "\n");

  const oracleResults = loadOracleResults();
  console.log(`Loaded ${oracleResults.length} fixed-approach oracle result(s) from benchmark-results/\n`);

  // ──────────────────────────────────────────────────────────────────────────
  // PHASE 1: MQTT-based conditions
  // ──────────────────────────────────────────────────────────────────────────
  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log("║  PHASE 1  —  MQTT-based stream observation                  ║");
  console.log("╚══════════════════════════════════════════════════════════════╝\n");
  console.log("NOTE: The 5 real-data aggregation variants (AVG/SUM/COUNT/MIN/MAX) are collapsed");
  console.log("to a single 'Real data (16 Hz)' row because the router observes raw sensor values,");
  console.log("not the query. Aggregation function does not affect stream signature extraction.\n");

  const phase1Rows = [];

  for (const condition of MAIN_CONDITIONS) {
    console.log(`── ${condition.label} ──`);
    console.log(`   Observing for ${observationSecs}s...`);

    let result;
    try { result = await observeCondition(condition); }
    catch (e) { result = { error: e.message, pts: 0 }; }

    const validation  = result.error ? null : validateSignature(condition.label, result);
    const oracleEntry = findOracle(oracleResults, condition, accuracyTolerance);
    const oracleBest  = oracleEntry?.bestApproach || null;
    const match       = oracleBest && !result.error ? result.recommended === oracleBest : null;
    const approxErr   = oracleEntry?.accuracyErrors?.approximation ?? null;

    if (result.error) {
      console.log(`   ⚠  ${result.error}`);
    } else {
      const sigStatus = validation ? (validation.valid ? "✓ PASS" : "✗ FAIL") : "N/A";
      console.log(`   Signature  : type=${result.streamType}, variance=${result.variance?.toFixed(2)}, complexity=${result.complexity}  [${sigStatus}]`);
      console.log(`   Recommended: ${result.recommended} (${(result.confidence * 100).toFixed(0)}% conf.)  ${result.reasoning?.[0] || ""}`);
      if (oracleBest) {
        const excl = oracleEntry.excludedApproaches.length ? ` (excluded: ${oracleEntry.excludedApproaches.join(", ")})` : "";
        console.log(`   Oracle     : ${oracleBest}${excl}  →  ${match ? "✓ MATCH" : "✗ MISMATCH"}`);
        if (approxErr != null) console.log(`   Approx err : ${approxErr.toFixed(3)}%  (τ=${accuracyTolerance}%)`);
      } else {
        console.log(`   Oracle     : N/A (no matching fixed-approach results)`);
      }
    }
    console.log("");

    phase1Rows.push({
      label:          condition.label,
      result:         result.error ? { error: result.error } : result,
      validation,
      recommended:    result.error ? "ERROR" : result.recommended,
      streamType:     result.error ? "N/A"   : result.streamType,
      oracle:         oracleBest || "N/A",
      oracleExcluded: oracleEntry?.excludedApproaches || [],
      approxErr,
      match,
      confidence:     result.error ? null : result.confidence,
      _condition:     condition,
      _oracleResults: oracleResults,
    });

    if (MAIN_CONDITIONS.indexOf(condition) < MAIN_CONDITIONS.length - 1) {
      await new Promise(res => setTimeout(res, 2000));
    }
  }

  printSignatureValidationTable(phase1Rows);
  printRoutingTable(phase1Rows, accuracyTolerance);
  printSensitivityTable(phase1Rows, oracleResults);

  // ──────────────────────────────────────────────────────────────────────────
  // PHASE 2: Simulated conditions
  // ──────────────────────────────────────────────────────────────────────────
  console.log("\n╔══════════════════════════════════════════════════════════════╗");
  console.log("║  PHASE 2  —  Simulated stream conditions (no MQTT)          ║");
  console.log("╚══════════════════════════════════════════════════════════════╝\n");
  console.log("NOTE: Uses HiveScoutBeeWrapper.simulateStreamPattern() directly.");
  console.log("Designed to trigger the approximation routing branch (tripleCount < 100),");
  console.log("which is never reached by real/pattern data due to high data volume.\n");

  const phase2Rows = [];
  for (const sc of SIM_CONDITIONS) {
    console.log(`── ${sc.label} (${sc.note}) ──`);
    const r = observeSimulated(sc.simPattern, sc.simPoints);
    const match = r.recommended === sc.expectedApproach ? "✓" : "✗";
    console.log(`   Recommended: ${r.recommended}  Expected: ${sc.expectedApproach}  ${match}`);
    if (r.reasoning?.length) console.log(`   Reasoning  : ${r.reasoning[0]}`);
    console.log(`   tripleCount=${r.tripleCount}, variance=${r.variance?.toFixed(2)}, type=${r.streamType}\n`);
    phase2Rows.push({ label: sc.label, simPoints: sc.simPoints, expectedApproach: sc.expectedApproach, result: r });
  }
  printSimTable(phase2Rows);

  // ──────────────────────────────────────────────────────────────────────────
  // PHASE 3: Observation window sensitivity
  // ──────────────────────────────────────────────────────────────────────────
  console.log("\n╔══════════════════════════════════════════════════════════════╗");
  console.log("║  PHASE 3  —  Observation window sensitivity                 ║");
  console.log("╚══════════════════════════════════════════════════════════════╝");
  const phase3Results = runWindowSensitivity();

  // ──────────────────────────────────────────────────────────────────────────
  // Save report
  // ──────────────────────────────────────────────────────────────────────────
  const ts  = Date.now();
  const dir = path.join(RESULTS_DIR, `intelligent-routing-${ts}`);
  fs.mkdirSync(dir, { recursive: true });

  const sigPass  = phase1Rows.filter(r => r.validation?.valid).length;
  const sigTotal = phase1Rows.filter(r => r.validation).length;
  const routeCorrect = phase1Rows.filter(r => r.match === true).length;
  const routeTotal   = phase1Rows.filter(r => r.match === true || r.match === false).length;
  const simCorrect   = phase2Rows.filter(r => r.result.recommended === r.expectedApproach).length;

  const report = {
    timestamp: new Date().toISOString(), observationSecs, accuracyTolerance,
    oracleMethod: `lowest latency among approaches with mean error ≤ ${accuracyTolerance}% vs Fetching`,
    phase1: {
      signatureValidation: { pass: sigPass, total: sigTotal },
      routingAccuracy:     { correct: routeCorrect, total: routeTotal,
                             accuracy: routeTotal > 0 ? parseFloat((routeCorrect/routeTotal*100).toFixed(1)) : null },
      rows: phase1Rows.map(({ _condition, _oracleResults, ...r }) => r),
    },
    phase2: { simCorrect, simTotal: phase2Rows.length, rows: phase2Rows },
    phase3: { windowSizes: WINDOW_SIZES, patterns: WINDOW_PATTERNS, results: phase3Results },
  };

  fs.writeFileSync(path.join(dir, "routing-report.json"), JSON.stringify(report, null, 2));

  console.log(`\n\n${"═".repeat(75)}`);
  console.log(" SUMMARY");
  console.log(`${"═".repeat(75)}`);
  console.log(` Phase 1 — Signature validation : ${sigPass}/${sigTotal} passed`);
  console.log(` Phase 1 — Routing accuracy     : ${routeCorrect}/${routeTotal} correct (${routeTotal > 0 ? (routeCorrect/routeTotal*100).toFixed(1) : "N/A"}%) at τ=${accuracyTolerance}%`);
  console.log(` Phase 2 — Simulated routing    : ${simCorrect}/${phase2Rows.length} matched expected approach`);
  console.log(` Report saved to: ${dir}`);
  console.log(`${"═".repeat(75)}\n`);
  console.log("🎉 Intelligent routing benchmark v2 complete!\n");
}

main().catch(err => {
  console.error("Fatal:", err);
  cleanup();
  process.exit(1);
});
