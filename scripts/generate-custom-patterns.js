#!/usr/bin/env node

/**
 * Generate Custom Stream Patterns for Experimentation
 *
 * Representative patterns:
 * 1. Low Variability: μ=-23.0, σ=0.25
 * 2. Step Pattern: v1=-23.0, v2=-15.0, t_step=60s
 * 3. Spike Pattern: v_base=-23.0, v_spike=-5.0, Δt=1.25s
 * 4. Low Freq. Oscillation: μ=-23.0, A=5.0, f=0.05Hz
 * 5. High Freq. Oscillation: μ=-23.0, A=3.0, f=0.5Hz
 *
 * Stress patterns:
 * 6. spike_boundary_short
 * 7. spike_boundary_medium
 * 8. spike_asymmetric_long
 * 9. late_burst
 * 10. multiple_bursts
 * 11. step_misaligned_45
 * 12. step_misaligned_75
 * 13. linear_ramp
 * 14. asymmetric_activity
 *
 * Duration: 2 minutes (120 seconds)
 * Sampling Rate: 4 Hz (250ms interval)
 * Total Points: 480 data points
 */

const fs = require("fs");
const path = require("path");

// Configuration
const DURATION_SECONDS = 120;
const SAMPLING_RATE_HZ = 4;
const INTERVAL_MS = 1000 / SAMPLING_RATE_HZ;
const TOTAL_POINTS = DURATION_SECONDS * SAMPLING_RATE_HZ;
const LAST_TIMESTAMP_MS = (TOTAL_POINTS - 1) * INTERVAL_MS;

// Pattern configurations
const PATTERNS = {
  low_variability: {
    name: "low_variability",
    description: "Low Variability Pattern",
    params: { mu: -23.0, sigma: 0.25 },
  },
  step_pattern: {
    name: "step_pattern",
    description: "Step Pattern",
    params: { v1: -23.0, v2: -15.0, t_step: 60 },
  },
  spike_pattern: {
    name: "spike_pattern",
    description: "Spike Pattern",
    params: { v_base: -23.0, v_spike: -5.0, delta_t: 1.25 },
  },
  low_freq_oscillation: {
    name: "low_freq_oscillation",
    description: "Low Frequency Oscillation",
    params: { mu: -23.0, A: 5.0, f: 0.05 },
  },
  high_freq_oscillation: {
    name: "high_freq_oscillation",
    description: "High Frequency Oscillation",
    params: { mu: -23.0, A: 3.0, f: 0.5 },
  },
  spike_boundary_short: {
    name: "spike_boundary_short",
    description: "Spike Boundary Short",
    params: { baseline: -23.0, spike: -5.0, start: 59, duration: 4 },
  },
  spike_boundary_medium: {
    name: "spike_boundary_medium",
    description: "Spike Boundary Medium",
    params: { baseline: -23.0, spike: -5.0, start: 55, duration: 10 },
  },
  spike_asymmetric_long: {
    name: "spike_asymmetric_long",
    description: "Spike Asymmetric Long",
    params: { baseline: -23.0, spike: -5.0, start: 50, duration: 20 },
  },
  late_burst: {
    name: "late_burst",
    description: "Late Burst",
    params: { baseline: -23.0, burst: -5.0, start: 85, duration: 20 },
  },
  multiple_bursts: {
    name: "multiple_bursts",
    description: "Multiple Bursts",
    params: {
      baseline: -23.0,
      burst: -5.0,
      bursts: [
        { start: 25, duration: 10 },
        { start: 85, duration: 10 },
      ],
    },
  },
  step_misaligned_45: {
    name: "step_misaligned_45",
    description: "Step Misaligned 45",
    params: { v1: -23.0, v2: -15.0, t_step: 45 },
  },
  step_misaligned_75: {
    name: "step_misaligned_75",
    description: "Step Misaligned 75",
    params: { v1: -23.0, v2: -15.0, t_step: 75 },
  },
  linear_ramp: {
    name: "linear_ramp",
    description: "Linear Ramp",
    params: { start_value: -23.0, end_value: -11.0 },
  },
  asymmetric_activity: {
    name: "asymmetric_activity",
    description: "Asymmetric Activity",
    params: {
      segments: [
        { start: 0, end: 40, value: -23.0 },
        { start: 40, end: 55, value: -8.0 },
        { start: 55, end: 95, value: -23.0 },
        { start: 95, end: 120, value: -15.0 },
      ],
    },
  },
};

const REPRESENTATIVE_PATTERN_ORDER = [
  "low_variability",
  "step_pattern",
  "spike_pattern",
  "low_freq_oscillation",
  "high_freq_oscillation",
];

const STRESS_PATTERN_ORDER = [
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

const ALL_PATTERN_ORDER = [
  ...REPRESENTATIVE_PATTERN_ORDER,
  ...STRESS_PATTERN_ORDER,
];

function parseArgs(argv) {
  const args = {
    seed: null,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === "--seed") {
      if (!next) {
        throw new Error("--seed requires a value");
      }
      args.seed = normalizeSeed(next);
      index += 1;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function printHelp() {
  console.log(`Usage: node scripts/generate-custom-patterns.js [options]

Options:
  --seed <n>   Fixed seed for reproducible low-variability generation
  --help       Show this help
`);
}

function normalizeSeed(value) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid seed: ${value}`);
  }
  return parsed >>> 0;
}

function createSeededRandom(seed) {
  let state = seed >>> 0;
  return function seededRandom() {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t ^= t + Math.imul(t ^ (t >>> 7), 61 | t);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function createRandomSource(seed) {
  if (!Number.isFinite(seed)) {
    return {
      seed: null,
      source: "math_random",
      next: () => Math.random(),
    };
  }

  return {
    seed,
    source: "seeded_mulberry32",
    next: createSeededRandom(seed),
  };
}

function buildTimeSeries(valueFn) {
  const data = [];
  for (let index = 0; index < TOTAL_POINTS; index += 1) {
    const timestamp = index * INTERVAL_MS;
    const tSeconds = timestamp / 1000;
    data.push({ timestamp, value: valueFn({ index, timestamp, tSeconds }) });
  }
  return data;
}

function isWithinInterval(tSeconds, start, duration) {
  return tSeconds >= start && tSeconds < (start + duration);
}

/**
 * Box-Muller transform for generating normally distributed random numbers
 */
function gaussianRandom(mean = 0, stdDev = 1, randomFn = Math.random) {
  let u1 = 0,
    u2 = 0;
  while (u1 === 0) u1 = randomFn();
  while (u2 === 0) u2 = randomFn();

  const z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
  return z0 * stdDev + mean;
}

/**
 * Generate Low Variability Pattern
 * Gaussian noise around mean: v(t) = N(μ, σ²)
 */
function generateLowVariability(params, randomFn = Math.random) {
  const { mu, sigma } = params;
  return buildTimeSeries(() => gaussianRandom(mu, sigma, randomFn));
}

/**
 * Generate Step Pattern
 * Step change at t_step: v(t) = v1 if t < t_step, else v2
 */
function generateStepPattern(params) {
  const { v1, v2, t_step } = params;
  return buildTimeSeries(({ tSeconds }) => (tSeconds < t_step ? v1 : v2));
}

/**
 * Generate Spike Pattern
 * Single spike at center: v(t) = v_spike for |t - t_center| < Δt/2, else v_base
 */
function generateSpikePattern(params) {
  const { v_base, v_spike, delta_t } = params;
  const t_center = DURATION_SECONDS / 2; // Spike at 60 seconds
  const half_delta = delta_t / 2;
  return buildTimeSeries(({ tSeconds }) => (
    Math.abs(tSeconds - t_center) < half_delta ? v_spike : v_base
  ));
}

/**
 * Generate Low Frequency Oscillation
 * Sinusoidal: v(t) = μ + A * sin(2πft)
 */
function generateLowFreqOscillation(params) {
  const { mu, A, f } = params;
  return buildTimeSeries(({ tSeconds }) => mu + A * Math.sin(2 * Math.PI * f * tSeconds));
}

/**
 * Generate High Frequency Oscillation
 * Sinusoidal: v(t) = μ + A * sin(2πft)
 */
function generateHighFreqOscillation(params) {
  const { mu, A, f } = params;
  return buildTimeSeries(({ tSeconds }) => mu + A * Math.sin(2 * Math.PI * f * tSeconds));
}

function generateTimedBurstPattern(params) {
  const { baseline, spike, burst, start, duration } = params;
  const eventValue = spike ?? burst;
  return buildTimeSeries(({ tSeconds }) => (
    isWithinInterval(tSeconds, start, duration) ? eventValue : baseline
  ));
}

function generateMultipleBurstsPattern(params) {
  const { baseline, burst, bursts } = params;
  return buildTimeSeries(({ tSeconds }) => (
    bursts.some(({ start, duration }) => isWithinInterval(tSeconds, start, duration))
      ? burst
      : baseline
  ));
}

function generateLinearRampPattern(params) {
  const { start_value, end_value } = params;
  const denominator = LAST_TIMESTAMP_MS === 0 ? 1 : LAST_TIMESTAMP_MS;
  return buildTimeSeries(({ timestamp }) => {
    const progress = timestamp / denominator;
    return start_value + ((end_value - start_value) * progress);
  });
}

function generateSegmentPattern(params) {
  const { segments } = params;
  return buildTimeSeries(({ tSeconds }) => {
    const segment = segments.find(({ start, end }) => tSeconds >= start && tSeconds < end);
    if (!segment) {
      throw new Error(`No segment configured for t=${tSeconds}`);
    }
    return segment.value;
  });
}

/**
 * Convert data to RDF N-Triples format
 * Matches the format used in existing data files
 */
function toNTriples(data, sensorType) {
  const lines = [];

  // Determine property based on sensor type
  const property =
    sensorType === "smartphone.acceleration.x" ? "smartphoneX" : "wearableX";

  // Base timestamp for generating ISO datetime strings
  // Starting from a fixed date: 2024-05-23T08:48:24.620Z
  const baseDate = new Date("2024-05-23T08:48:24.620Z");

  data.forEach(({ timestamp, value }, index) => {
    // Create ISO datetime by adding milliseconds offset
    const obsDate = new Date(baseDate.getTime() + timestamp);
    const isoTimestamp = obsDate
      .toISOString()
      .replace(/\.(\d{3})Z$/, ".$1" + "0Z");

    const obsUri = `<https://dahcc.idlab.ugent.be/Protego/_participant1/obs${index}>`;
    const datasetUri = `<https://dahcc.idlab.ugent.be/Protego/_participant1>`;
    const sensorUri = `<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/SM-G950F>`;
    const propertyUri = `<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${property}>`;
    const timestampLiteral = `"${isoTimestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime>`;
    const valueLiteral = `"${value.toFixed(6)}"^^<http://www.w3.org/2001/XMLSchema#float>`;

    // All triples on one line (matching existing format)
    const line =
      `${obsUri} <http://rdfs.org/ns/void#inDataset> ${datasetUri} . ` +
      `${obsUri} <https://saref.etsi.org/core/measurementMadeBy> ${sensorUri} . ` +
      `${obsUri} <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> . ` +
      `${obsUri} <https://saref.etsi.org/core/relatesToProperty> ${propertyUri} . ` +
      `${obsUri} <https://saref.etsi.org/core/hasTimestamp> ${timestampLiteral} . ` +
      `${obsUri} <https://saref.etsi.org/core/hasValue> ${valueLiteral} .`;

    lines.push(line);
  });

  return lines.join("\n") + "\n";
}

/**
 * Save pattern data to file
 */
function savePattern(patternName, data, generationConfig = {}) {
  const baseDir = path.join(
    "src",
    "streamer",
    "data",
    "custom_patterns",
    patternName,
  );

  // Create directories for both sensor types
  const sensorTypes = ["smartphone.acceleration.x", "wearable.acceleration.x"];

  sensorTypes.forEach((sensorType) => {
    const sensorDir = path.join(baseDir, sensorType);
    fs.mkdirSync(sensorDir, { recursive: true });

    const dataPath = path.join(sensorDir, "data.nt");
    const ntriples = toNTriples(data, sensorType);
    fs.writeFileSync(dataPath, ntriples);

    console.log(`  ✓ Saved ${dataPath}`);
  });

  // Save metadata
  const metadataPath = path.join(baseDir, "metadata.json");
  const metadata = {
    pattern: patternName,
    description: PATTERNS[patternName].description,
    parameters: PATTERNS[patternName].params,
    duration_seconds: DURATION_SECONDS,
    sampling_rate_hz: SAMPLING_RATE_HZ,
    total_points: TOTAL_POINTS,
    random_seed: generationConfig.seed ?? null,
    random_source: generationConfig.source || "math_random",
    generated_at: new Date().toISOString(),
  };
  fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2));
  console.log(`  ✓ Saved ${metadataPath}`);

  // Save CSV for easy inspection
  const csvPath = path.join(baseDir, "data.csv");
  const csvLines = ["timestamp_ms,value"];
  data.forEach(({ timestamp, value }) => {
    csvLines.push(`${timestamp},${value}`);
  });
  fs.writeFileSync(csvPath, csvLines.join("\n"));
  console.log(`  ✓ Saved ${csvPath}`);
}

/**
 * Print pattern statistics
 */
function printStatistics(patternName, data) {
  const values = data.map((d) => d.value);
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance =
    values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length;
  const stdDev = Math.sqrt(variance);
  const min = Math.min(...values);
  const max = Math.max(...values);

  console.log(`\n  Statistics for ${patternName}:`);
  console.log(`    Mean: ${mean.toFixed(4)}`);
  console.log(`    Std Dev: ${stdDev.toFixed(4)}`);
  console.log(`    Min: ${min.toFixed(4)}`);
  console.log(`    Max: ${max.toFixed(4)}`);
  console.log(`    Range: ${(max - min).toFixed(4)}`);
}

/**
 * Generate all patterns
 */
function generateAllPatterns(options = {}) {
  const generationConfig = createRandomSource(
    Number.isFinite(options.seed) ? options.seed : null,
  );
  console.log("=".repeat(80));
  console.log("GENERATING CUSTOM STREAM PATTERNS");
  console.log("=".repeat(80));
  console.log(`Duration: ${DURATION_SECONDS}s`);
  console.log(
    `Sampling Rate: ${SAMPLING_RATE_HZ} Hz (${INTERVAL_MS}ms interval)`,
  );
  console.log(`Total Points: ${TOTAL_POINTS}`);
  console.log(
    `Random source: ${generationConfig.source}${generationConfig.seed !== null ? ` (seed=${generationConfig.seed})` : ""}`,
  );
  console.log("=".repeat(80));

  const patternPlans = [
    {
      ordinal: 1,
      title: "Low Variability (μ=-23.0, σ=0.25)",
      key: "low_variability",
      generator: () => generateLowVariability(PATTERNS.low_variability.params, generationConfig.next),
    },
    {
      ordinal: 2,
      title: "Step Pattern (v1=-23.0, v2=-15.0, t_step=60s)",
      key: "step_pattern",
      generator: () => generateStepPattern(PATTERNS.step_pattern.params),
    },
    {
      ordinal: 3,
      title: "Spike Pattern (v_base=-23.0, v_spike=-5.0, Δt=1.25s)",
      key: "spike_pattern",
      generator: () => generateSpikePattern(PATTERNS.spike_pattern.params),
    },
    {
      ordinal: 4,
      title: "Low Frequency Oscillation (μ=-23.0, A=5.0, f=0.05Hz)",
      key: "low_freq_oscillation",
      generator: () => generateLowFreqOscillation(PATTERNS.low_freq_oscillation.params),
    },
    {
      ordinal: 5,
      title: "High Frequency Oscillation (μ=-23.0, A=3.0, f=0.5Hz)",
      key: "high_freq_oscillation",
      generator: () => generateHighFreqOscillation(PATTERNS.high_freq_oscillation.params),
    },
    {
      ordinal: 6,
      title: "Spike Boundary Short (baseline=-23.0, spike=-5.0, start=59s, duration=4s)",
      key: "spike_boundary_short",
      generator: () => generateTimedBurstPattern(PATTERNS.spike_boundary_short.params),
    },
    {
      ordinal: 7,
      title: "Spike Boundary Medium (baseline=-23.0, spike=-5.0, start=55s, duration=10s)",
      key: "spike_boundary_medium",
      generator: () => generateTimedBurstPattern(PATTERNS.spike_boundary_medium.params),
    },
    {
      ordinal: 8,
      title: "Spike Asymmetric Long (baseline=-23.0, spike=-5.0, start=50s, duration=20s)",
      key: "spike_asymmetric_long",
      generator: () => generateTimedBurstPattern(PATTERNS.spike_asymmetric_long.params),
    },
    {
      ordinal: 9,
      title: "Late Burst (baseline=-23.0, burst=-5.0, start=85s, duration=20s)",
      key: "late_burst",
      generator: () => generateTimedBurstPattern(PATTERNS.late_burst.params),
    },
    {
      ordinal: 10,
      title: "Multiple Bursts (baseline=-23.0, burst=-5.0, 25-35s and 85-95s)",
      key: "multiple_bursts",
      generator: () => generateMultipleBurstsPattern(PATTERNS.multiple_bursts.params),
    },
    {
      ordinal: 11,
      title: "Step Misaligned 45 (v1=-23.0, v2=-15.0, t_step=45s)",
      key: "step_misaligned_45",
      generator: () => generateStepPattern(PATTERNS.step_misaligned_45.params),
    },
    {
      ordinal: 12,
      title: "Step Misaligned 75 (v1=-23.0, v2=-15.0, t_step=75s)",
      key: "step_misaligned_75",
      generator: () => generateStepPattern(PATTERNS.step_misaligned_75.params),
    },
    {
      ordinal: 13,
      title: "Linear Ramp (start=-23.0, end=-11.0, full 120s)",
      key: "linear_ramp",
      generator: () => generateLinearRampPattern(PATTERNS.linear_ramp.params),
    },
    {
      ordinal: 14,
      title: "Asymmetric Activity (0-40:-23, 40-55:-8, 55-95:-23, 95-120:-15)",
      key: "asymmetric_activity",
      generator: () => generateSegmentPattern(PATTERNS.asymmetric_activity.params),
    },
  ];

  for (const plan of patternPlans) {
    console.log(`\n${plan.ordinal}. ${plan.title}`);
    const data = plan.generator();
    printStatistics(plan.key, data);
    savePattern(plan.key, data, generationConfig);
  }

  console.log("\n" + "=".repeat(80));
  console.log("✓ ALL PATTERNS GENERATED SUCCESSFULLY");
  console.log("=".repeat(80));
  console.log("\nData saved to: src/streamer/data/custom_patterns/");
  console.log("\nRepresentative Pattern Summary:");
  console.log("  1. low_variability       - Gaussian noise around mean");
  console.log("  2. step_pattern          - Step change at 60s");
  console.log("  3. spike_pattern         - Brief spike at center");
  console.log("  4. low_freq_oscillation  - Slow sinusoidal (0.05 Hz)");
  console.log("  5. high_freq_oscillation - Fast sinusoidal (0.5 Hz)");
  console.log("\nStress Pattern Summary:");
  console.log("  6. spike_boundary_short  - 59-63s short spike");
  console.log("  7. spike_boundary_medium - 55-65s boundary-spanning spike");
  console.log("  8. spike_asymmetric_long - 50-70s long asymmetric spike");
  console.log("  9. late_burst            - 85-105s late burst");
  console.log(" 10. multiple_bursts       - 25-35s and 85-95s bursts");
  console.log(" 11. step_misaligned_45    - Step at 45s");
  console.log(" 12. step_misaligned_75    - Step at 75s");
  console.log(" 13. linear_ramp           - Full-duration linear increase");
  console.log(" 14. asymmetric_activity   - Two uneven elevated activity regions");
  console.log("\nNext steps:");
  console.log(
    "  node experiments/pattern-analysis/run-custom-patterns-comparison.js",
  );
  console.log("=".repeat(80));
}

// Run generator
if (require.main === module) {
  const cliArgs = parseArgs(process.argv.slice(2));
  generateAllPatterns(cliArgs);
}

module.exports = {
  createRandomSource,
  createSeededRandom,
  normalizeSeed,
  parseArgs,
  gaussianRandom,
  generateAllPatterns,
  generateLowVariability,
  generateStepPattern,
  generateSpikePattern,
  generateLowFreqOscillation,
  generateHighFreqOscillation,
  generateTimedBurstPattern,
  generateMultipleBurstsPattern,
  generateLinearRampPattern,
  generateSegmentPattern,
  PATTERNS,
  REPRESENTATIVE_PATTERN_ORDER,
  STRESS_PATTERN_ORDER,
  ALL_PATTERN_ORDER,
  DURATION_SECONDS,
  SAMPLING_RATE_HZ,
  INTERVAL_MS,
  TOTAL_POINTS,
  LAST_TIMESTAMP_MS,
  isWithinInterval,
};
