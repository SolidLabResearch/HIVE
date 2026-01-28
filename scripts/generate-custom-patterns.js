#!/usr/bin/env node

/**
 * Generate Custom Stream Patterns for Experimentation
 *
 * Patterns:
 * 1. Low Variability: μ=-23.0, σ=0.25
 * 2. Step Pattern: v1=-23.0, v2=-15.0, t_step=60s
 * 3. Spike Pattern: v_base=-23.0, v_spike=-5.0, Δt=1.25s
 * 4. Low Freq. Oscillation: μ=-23.0, A=5.0, f=0.05Hz
 * 5. High Freq. Oscillation: μ=-23.0, A=3.0, f=0.5Hz
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
};

/**
 * Box-Muller transform for generating normally distributed random numbers
 */
function gaussianRandom(mean = 0, stdDev = 1) {
  let u1 = 0,
    u2 = 0;
  while (u1 === 0) u1 = Math.random();
  while (u2 === 0) u2 = Math.random();

  const z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
  return z0 * stdDev + mean;
}

/**
 * Generate Low Variability Pattern
 * Gaussian noise around mean: v(t) = N(μ, σ²)
 */
function generateLowVariability(params) {
  const { mu, sigma } = params;
  const data = [];

  for (let i = 0; i < TOTAL_POINTS; i++) {
    const timestamp = i * INTERVAL_MS;
    const value = gaussianRandom(mu, sigma);
    data.push({ timestamp, value });
  }

  return data;
}

/**
 * Generate Step Pattern
 * Step change at t_step: v(t) = v1 if t < t_step, else v2
 */
function generateStepPattern(params) {
  const { v1, v2, t_step } = params;
  const data = [];

  for (let i = 0; i < TOTAL_POINTS; i++) {
    const timestamp = i * INTERVAL_MS;
    const t_seconds = timestamp / 1000;
    const value = t_seconds < t_step ? v1 : v2;
    data.push({ timestamp, value });
  }

  return data;
}

/**
 * Generate Spike Pattern
 * Single spike at center: v(t) = v_spike for |t - t_center| < Δt/2, else v_base
 */
function generateSpikePattern(params) {
  const { v_base, v_spike, delta_t } = params;
  const data = [];
  const t_center = DURATION_SECONDS / 2; // Spike at 60 seconds
  const half_delta = delta_t / 2;

  for (let i = 0; i < TOTAL_POINTS; i++) {
    const timestamp = i * INTERVAL_MS;
    const t_seconds = timestamp / 1000;
    const inSpikeWindow = Math.abs(t_seconds - t_center) < half_delta;
    const value = inSpikeWindow ? v_spike : v_base;
    data.push({ timestamp, value });
  }

  return data;
}

/**
 * Generate Low Frequency Oscillation
 * Sinusoidal: v(t) = μ + A * sin(2πft)
 */
function generateLowFreqOscillation(params) {
  const { mu, A, f } = params;
  const data = [];

  for (let i = 0; i < TOTAL_POINTS; i++) {
    const timestamp = i * INTERVAL_MS;
    const t_seconds = timestamp / 1000;
    const value = mu + A * Math.sin(2 * Math.PI * f * t_seconds);
    data.push({ timestamp, value });
  }

  return data;
}

/**
 * Generate High Frequency Oscillation
 * Sinusoidal: v(t) = μ + A * sin(2πft)
 */
function generateHighFreqOscillation(params) {
  const { mu, A, f } = params;
  const data = [];

  for (let i = 0; i < TOTAL_POINTS; i++) {
    const timestamp = i * INTERVAL_MS;
    const t_seconds = timestamp / 1000;
    const value = mu + A * Math.sin(2 * Math.PI * f * t_seconds);
    data.push({ timestamp, value });
  }

  return data;
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
function savePattern(patternName, data) {
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
function generateAllPatterns() {
  console.log("=".repeat(80));
  console.log("GENERATING CUSTOM STREAM PATTERNS");
  console.log("=".repeat(80));
  console.log(`Duration: ${DURATION_SECONDS}s`);
  console.log(
    `Sampling Rate: ${SAMPLING_RATE_HZ} Hz (${INTERVAL_MS}ms interval)`,
  );
  console.log(`Total Points: ${TOTAL_POINTS}`);
  console.log("=".repeat(80));

  // 1. Low Variability
  console.log("\n1. Low Variability (μ=-23.0, σ=0.25)");
  const lowVar = generateLowVariability(PATTERNS.low_variability.params);
  printStatistics("low_variability", lowVar);
  savePattern("low_variability", lowVar);

  // 2. Step Pattern
  console.log("\n2. Step Pattern (v1=-23.0, v2=-15.0, t_step=60s)");
  const step = generateStepPattern(PATTERNS.step_pattern.params);
  printStatistics("step_pattern", step);
  savePattern("step_pattern", step);

  // 3. Spike Pattern
  console.log("\n3. Spike Pattern (v_base=-23.0, v_spike=-5.0, Δt=1.25s)");
  const spike = generateSpikePattern(PATTERNS.spike_pattern.params);
  printStatistics("spike_pattern", spike);
  savePattern("spike_pattern", spike);

  // 4. Low Frequency Oscillation
  console.log("\n4. Low Frequency Oscillation (μ=-23.0, A=5.0, f=0.05Hz)");
  const lowFreq = generateLowFreqOscillation(
    PATTERNS.low_freq_oscillation.params,
  );
  printStatistics("low_freq_oscillation", lowFreq);
  savePattern("low_freq_oscillation", lowFreq);

  // 5. High Frequency Oscillation
  console.log("\n5. High Frequency Oscillation (μ=-23.0, A=3.0, f=0.5Hz)");
  const highFreq = generateHighFreqOscillation(
    PATTERNS.high_freq_oscillation.params,
  );
  printStatistics("high_freq_oscillation", highFreq);
  savePattern("high_freq_oscillation", highFreq);

  console.log("\n" + "=".repeat(80));
  console.log("✓ ALL PATTERNS GENERATED SUCCESSFULLY");
  console.log("=".repeat(80));
  console.log("\nData saved to: src/streamer/data/custom_patterns/");
  console.log("\nPattern Summary:");
  console.log("  1. low_variability       - Gaussian noise around mean");
  console.log("  2. step_pattern          - Step change at 60s");
  console.log("  3. spike_pattern         - Brief spike at center");
  console.log("  4. low_freq_oscillation  - Slow sinusoidal (0.05 Hz)");
  console.log("  5. high_freq_oscillation - Fast sinusoidal (0.5 Hz)");
  console.log("\nNext steps:");
  console.log(
    "  node experiments/pattern-analysis/run-custom-patterns-comparison.js",
  );
  console.log("=".repeat(80));
}

// Run generator
if (require.main === module) {
  generateAllPatterns();
}

module.exports = {
  generateLowVariability,
  generateStepPattern,
  generateSpikePattern,
  generateLowFreqOscillation,
  generateHighFreqOscillation,
  PATTERNS,
};
