import * as fs from "fs";
import * as path from "path";

// --- Pattern Generation Configuration ---

const OUTPUT_DIR = path.join(
  __dirname,
  "../src/streamer/data/pattern_experiments",
);
const DURATION_SECONDS = 120; // 2 minutes
const FREQUENCY_HZ = 4;
const TOTAL_OBSERVATIONS = DURATION_SECONDS * FREQUENCY_HZ; // 480 observations

const BASE_TIMESTAMP = new Date("2025-07-15T08:48:24.620Z").getTime();

 
interface PatternConfig {
  name: string;
  description: string;
  generator: (_index: number, _totalObservations?: number) => number;
}
 

const PATTERNS: PatternConfig[] = [
  {
    name: "low_variability",
    description: "Stable values with minimal noise",
     
    generator: (_index: number) => {
      const baseValue = -23.0;
      const noise = (Math.random() - 0.5) * 0.5; // ±0.25 variation
      return baseValue + noise;
    },
  },
  {
    name: "step_pattern",
    description: "Sharp step change at midpoint",
    generator: (index: number, totalObservations: number) => {
      const midpoint = totalObservations! / 2;
      const baseValue = index < midpoint ? -23.0 : -15.0;
      const noise = (Math.random() - 0.5) * 0.3;
      return baseValue + noise;
    },
  },
  {
    name: "spike_pattern",
    description: "Sudden spike in the middle",
    generator: (index: number, totalObservations: number) => {
      const baseValue = -23.0;
      const spikeStart = Math.floor(totalObservations! / 2) - 2;
      const spikeEnd = Math.floor(totalObservations! / 2) + 2;

      if (index >= spikeStart && index <= spikeEnd) {
        return -5.0 + (Math.random() - 0.5) * 0.5; // Spike to -5
      }
      return baseValue + (Math.random() - 0.5) * 0.3;
    },
  },
  {
    name: "low_frequency_oscillation",
    description: "Slow sinusoidal oscillation",
    generator: (index: number) => {
      const baseValue = -23.0;
      const amplitude = 5.0;
      const frequency = 0.05; // Complete 6 cycles over 2 minutes
      const oscillation = amplitude * Math.sin(2 * Math.PI * frequency * index);
      const noise = (Math.random() - 0.5) * 0.2;
      return baseValue + oscillation + noise;
    },
  },
  {
    name: "high_frequency_oscillation",
    description: "Fast sinusoidal oscillation",
    generator: (index: number) => {
      const baseValue = -23.0;
      const amplitude = 3.0;
      const frequency = 0.5; // Complete 60 cycles over 2 minutes
      const oscillation = amplitude * Math.sin(2 * Math.PI * frequency * index);
      const noise = (Math.random() - 0.5) * 0.2;
      return baseValue + oscillation + noise;
    },
  },
  {
    name: "gradual_drift",
    description: "Slow linear drift over time",
    generator: (index: number, totalObservations: number) => {
      const startValue = -25.0;
      const endValue = -15.0;
      const drift =
        startValue + (endValue - startValue) * (index / totalObservations!);
      const noise = (Math.random() - 0.5) * 0.3;
      return drift + noise;
    },
  },
  {
    name: "random_walk",
    description: "Random walk pattern",
    generator: (function () {
      let currentValue = -23.0;
       
      return (_index: number) => {
        const step = (Math.random() - 0.5) * 1.5;
        currentValue += step;
        // Keep within bounds
        currentValue = Math.max(-35, Math.min(-10, currentValue));
        return currentValue;
      };
    })(),
  },
];

// --- N-Quads Generation Functions ---

/**
 * Generates observation quads for a given sensor reading.
 * @param {number} obsIndex - The observation index.
 * @param {number} value - The sensor value.
 * @param {Date} timestamp - The timestamp of the observation.
 * @param {"wearable" | "smartphone"} deviceType - The device type.
 */
function generateObservationQuads(
  obsIndex: number,
  value: number,
  timestamp: Date,
  deviceType: "wearable" | "smartphone",
): string {
  const propertyName = deviceType === "wearable" ? "wearableX" : "smartphoneX";
  const sensorId =
    deviceType === "wearable"
      ? "E4.A03846.Accelerometer"
      : "Samsung.Galaxy.S21.Accelerometer";

  const subject = `<https://dahcc.idlab.ugent.be/Protego/_participant1/obs${obsIndex}>`;
  const dataset = `<https://dahcc.idlab.ugent.be/Protego/_participant1>`;
  const sensor = `<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${sensorId}>`;
  const property = `<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${propertyName}>`;

  const quads = [
    `${subject} <http://rdfs.org/ns/void#inDataset> ${dataset} .`,
    `${subject} <https://saref.etsi.org/core/measurementMadeBy> ${sensor} .`,
    `${subject} <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> .`,
    `${subject} <https://saref.etsi.org/core/relatesToProperty> ${property} .`,
    `${subject} <https://saref.etsi.org/core/hasTimestamp> "${timestamp.toISOString()}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .`,
    `${subject} <https://saref.etsi.org/core/hasValue> "${value.toFixed(1)}"^^<http://www.w3.org/2001/XMLSchema#float> .`,
  ];

  return quads.join(" ") + "\n";
}

/**
 * Generates data file content for a specific pattern and device type.
 * @param {PatternConfig} pattern - The pattern configuration.
 * @param {"wearable" | "smartphone"} deviceType - The device type.
 */
function generateDataFile(
  pattern: PatternConfig,
  deviceType: "wearable" | "smartphone",
): string {
  let content = "";

  for (let i = 0; i < TOTAL_OBSERVATIONS; i++) {
    const value = pattern.generator(i, TOTAL_OBSERVATIONS);
    const timestampMs = BASE_TIMESTAMP + (i * 1000) / FREQUENCY_HZ;
    const timestamp = new Date(timestampMs);

    content += generateObservationQuads(i, value, timestamp, deviceType);
  }

  return content;
}

// --- Main Generation Logic ---

/**
 * Generates all pattern data files for wearable and smartphone devices.
 * @returns {Promise<void>} A promise that resolves when all patterns are generated.
 */
async function generateAllPatterns(): Promise<void> {
  console.log("=== Stream Pattern Data Generator ===\n");
  console.log(`Configuration:`);
  console.log(`  Duration: ${DURATION_SECONDS}s`);
  console.log(`  Frequency: ${FREQUENCY_HZ}Hz`);
  console.log(`  Total observations per stream: ${TOTAL_OBSERVATIONS}`);
  console.log(`  Output directory: ${OUTPUT_DIR}\n`);

  // Create output directory structure
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    console.log(`Created directory: ${OUTPUT_DIR}`);
  }

  // Generate data for each pattern
  for (const pattern of PATTERNS) {
    console.log(`\nGenerating pattern: ${pattern.name}`);
    console.log(`  Description: ${pattern.description}`);

    // Create pattern directory
    const patternDir = path.join(OUTPUT_DIR, pattern.name);
    if (!fs.existsSync(patternDir)) {
      fs.mkdirSync(patternDir, { recursive: true });
    }

    // Create device subdirectories
    const wearableDir = path.join(patternDir, "wearable");
    const smartphoneDir = path.join(patternDir, "smartphone");

    if (!fs.existsSync(wearableDir)) {
      fs.mkdirSync(wearableDir, { recursive: true });
    }
    if (!fs.existsSync(smartphoneDir)) {
      fs.mkdirSync(smartphoneDir, { recursive: true });
    }

    // Generate and write wearable data
    console.log(`  Generating wearable data...`);
    const wearableData = generateDataFile(pattern, "wearable");
    const wearablePath = path.join(wearableDir, "data.nt");
    fs.writeFileSync(wearablePath, wearableData);
    console.log(`  ✓ Written: ${wearablePath}`);

    // Generate and write smartphone data (same pattern, different sensor)
    console.log(`  Generating smartphone data...`);
    const smartphoneData = generateDataFile(pattern, "smartphone");
    const smartphonePath = path.join(smartphoneDir, "data.nt");
    fs.writeFileSync(smartphonePath, smartphoneData);
    console.log(`  ✓ Written: ${smartphonePath}`);

    // Generate metadata file
    const metadata = {
      pattern: pattern.name,
      description: pattern.description,
      duration_seconds: DURATION_SECONDS,
      frequency_hz: FREQUENCY_HZ,
      total_observations: TOTAL_OBSERVATIONS,
      start_timestamp: new Date(BASE_TIMESTAMP).toISOString(),
      end_timestamp: new Date(
        BASE_TIMESTAMP + DURATION_SECONDS * 1000,
      ).toISOString(),
      generated_at: new Date().toISOString(),
    };

    const metadataPath = path.join(patternDir, "metadata.json");
    fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2));
    console.log(`  ✓ Written metadata: ${metadataPath}`);
  }

  console.log(`\n=== Generation Complete ===`);
  console.log(
    `Generated ${PATTERNS.length} patterns with wearable and smartphone data.`,
  );
  console.log(
    `Total files created: ${PATTERNS.length * 3} (data.nt × 2 + metadata.json per pattern)`,
  );
}

// --- Execution ---

if (require.main === module) {
  generateAllPatterns()
    .then(() => {
      console.log("\n✓ All patterns generated successfully!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("\n✗ Error generating patterns:", error);
      process.exit(1);
    });
}

export { generateAllPatterns, PATTERNS, PatternConfig };
