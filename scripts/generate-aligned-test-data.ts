/**
 * Generate Aligned Test Data for Streaming Query Experiments
 *
 * This script generates synthetic sensor data with:
 * - Continuous timestamps (no gaps between streams)
 * - Temporal alignment (both streams overlap in time)
 * - Known ground truth values for validation
 * - Configurable duration and sampling rate
 *
 * Purpose: Create proper test data where Chunked and Fetching approaches
 * should achieve 100% accuracy as they did in August experiments.
 */

import * as fs from 'fs';
import * as path from 'path';

// ============================================================================
// Configuration
// ============================================================================

interface GeneratorConfig {
  // Duration and sampling
  durationSeconds: number;        // Total duration of the experiment
  samplingRateHz: number;          // Sampling rate (e.g., 32 Hz like original data)

  // Data characteristics
  baseTimestamp: string;           // Starting timestamp (ISO 8601)
  valueRangeMin: number;           // Minimum sensor value
  valueRangeMax: number;           // Maximum sensor value

  // Special values for validation
  insertPeakValue: boolean;        // Insert a known peak value
  peakValue: number;               // The peak value to insert
  peakTimestampOffsetMs: number;   // When to insert the peak (ms from start)
  peakStream: 'smartphone' | 'wearable' | 'both';

  // Output
  outputDir: string;               // Output directory
  datasetName: string;             // Name of the dataset
}

const config: GeneratorConfig = {
  durationSeconds: 120,            // 120 seconds = 2 minutes of data
  samplingRateHz: 32,              // 32 Hz sampling (realistic for sensors)
  baseTimestamp: '2025-07-15T08:00:00.000Z',
  valueRangeMin: -15.0,
  valueRangeMax: 10.0,
  insertPeakValue: true,
  peakValue: 12.5,                 // Known peak for validation
  peakTimestampOffsetMs: 30000,    // Insert peak at 30 seconds
  peakStream: 'smartphone',
  outputDir: 'src/streamer/data/aligned_test',
  datasetName: 'continuous_120s'
};

// ============================================================================
// Data Generation
// ============================================================================

interface Observation {
  id: string;
  timestamp: string;
  value: number;
  property: string;
  device: string;
}

/**
 * Generate random value within range (normal distribution)
 */
function generateRandomValue(min: number, max: number): number {
  // Box-Muller transform for normal distribution
  const u1 = Math.random();
  const u2 = Math.random();
  const z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);

  // Scale to range with mean at center
  const mean = (min + max) / 2;
  const stdDev = (max - min) / 6; // ~99.7% of values within range
  let value = mean + z0 * stdDev;

  // Clamp to range
  value = Math.max(min, Math.min(max, value));

  return value;
}

/**
 * Generate observations for a single stream
 */
function generateStreamData(
  streamName: 'smartphone' | 'wearable',
  config: GeneratorConfig
): Observation[] {
  const observations: Observation[] = [];
  const baseTime = new Date(config.baseTimestamp).getTime();
  const msPerSample = 1000 / config.samplingRateHz;
  const totalSamples = Math.floor(config.durationSeconds * config.samplingRateHz);

  const property = streamName === 'smartphone'
    ? 'smartphoneX'
    : 'wearableX';

  const device = streamName === 'smartphone'
    ? 'SM-G950F'
    : 'Empatica-E4';

  console.log(`Generating ${totalSamples} samples for ${streamName} stream...`);

  for (let i = 0; i < totalSamples; i++) {
    const timestampMs = baseTime + (i * msPerSample);
    const timestamp = new Date(timestampMs).toISOString();

    let value = generateRandomValue(config.valueRangeMin, config.valueRangeMax);

    // Insert peak value if configured
    if (config.insertPeakValue) {
      const shouldInsertPeak =
        (config.peakStream === streamName || config.peakStream === 'both') &&
        Math.abs((i * msPerSample) - config.peakTimestampOffsetMs) < msPerSample;

      if (shouldInsertPeak) {
        value = config.peakValue;
        console.log(`  Inserted peak value ${config.peakValue} at sample ${i} (${timestamp})`);
      }
    }

    observations.push({
      id: `obs${i}`,
      timestamp,
      value,
      property,
      device
    });
  }

  return observations;
}

/**
 * Convert observation to N-Triples format
 */
function observationToNTriples(obs: Observation, streamName: string): string {
  const baseUri = 'https://dahcc.idlab.ugent.be/Protego/_participant1';
  const subject = `<${baseUri}/${obs.id}>`;

  const deviceUri = streamName === 'smartphone'
    ? '<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/SM-G950F>'
    : '<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/Empatica-E4>';

  const propertyUri = `<https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${obs.property}>`;

  const triples = [
    `${subject} <http://rdfs.org/ns/void#inDataset> <${baseUri}> .`,
    `${subject} <https://saref.etsi.org/core/measurementMadeBy> ${deviceUri} .`,
    `${subject} <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> .`,
    `${subject} <https://saref.etsi.org/core/relatesToProperty> ${propertyUri} .`,
    `${subject} <https://saref.etsi.org/core/hasTimestamp> "${obs.timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .`,
    `${subject} <https://saref.etsi.org/core/hasValue> "${obs.value.toFixed(6)}"^^<http://www.w3.org/2001/XMLSchema#float> .`
  ];

  return triples.join(' ') + '\n';
}

/**
 * Write observations to N-Triples file
 */
function writeNTriplesFile(
  observations: Observation[],
  streamName: string,
  outputPath: string
): void {
  const lines = observations.map(obs => observationToNTriples(obs, streamName));
  fs.writeFileSync(outputPath, lines.join(''), 'utf-8');
  console.log(`Written ${observations.length} observations to ${outputPath}`);
}

/**
 * Calculate and write ground truth
 */
function writeGroundTruth(
  smartphoneObs: Observation[],
  wearableObs: Observation[],
  outputDir: string
): void {
  const allValues = [
    ...smartphoneObs.map(o => o.value),
    ...wearableObs.map(o => o.value)
  ];

  const maxValue = Math.max(...allValues);
  const minValue = Math.min(...allValues);
  const avgValue = allValues.reduce((sum, v) => sum + v, 0) / allValues.length;

  const smartphoneMax = Math.max(...smartphoneObs.map(o => o.value));
  const wearableMax = Math.max(...wearableObs.map(o => o.value));

  const groundTruth = {
    metadata: {
      generatedAt: new Date().toISOString(),
      config: config,
      totalObservations: allValues.length,
      smartphoneObservations: smartphoneObs.length,
      wearableObservations: wearableObs.length
    },
    groundTruth: {
      global: {
        max: maxValue,
        min: minValue,
        avg: avgValue
      },
      byStream: {
        smartphone: {
          max: smartphoneMax,
          min: Math.min(...smartphoneObs.map(o => o.value)),
          avg: smartphoneObs.reduce((sum, o) => sum + o.value, 0) / smartphoneObs.length
        },
        wearable: {
          max: wearableMax,
          min: Math.min(...wearableObs.map(o => o.value)),
          avg: wearableObs.reduce((sum, o) => sum + o.value, 0) / wearableObs.length
        }
      },
      expectedMainQueryResult: {
        aggregation: 'MAX',
        expectedValue: maxValue,
        description: 'MAX across both streams (smartphone and wearable) over 120s windows'
      }
    },
    validation: {
      peakValueInserted: config.insertPeakValue,
      expectedPeakValue: config.peakValue,
      peakFoundInSmartphone: smartphoneMax === config.peakValue,
      peakFoundInWearable: wearableMax === config.peakValue,
      globalMaxMatchesPeak: maxValue === config.peakValue
    }
  };

  const groundTruthPath = path.join(outputDir, 'ground_truth.json');
  fs.writeFileSync(groundTruthPath, JSON.stringify(groundTruth, null, 2), 'utf-8');

  console.log('\n=== Ground Truth ===');
  console.log(`Global MAX: ${maxValue.toFixed(6)}`);
  console.log(`Smartphone MAX: ${smartphoneMax.toFixed(6)}`);
  console.log(`Wearable MAX: ${wearableMax.toFixed(6)}`);
  console.log(`Expected Result: ${maxValue.toFixed(6)}`);
  console.log(`Ground truth saved to: ${groundTruthPath}`);
}

/**
 * Generate README for the dataset
 */
function writeReadme(outputDir: string): void {
  const readme = `# Aligned Test Dataset

## Overview

This dataset contains continuous, temporally-aligned sensor data for testing streaming query approaches.

**Generated:** ${new Date().toISOString()}

## Configuration

- **Duration:** ${config.durationSeconds} seconds
- **Sampling Rate:** ${config.samplingRateHz} Hz
- **Base Timestamp:** ${config.baseTimestamp}
- **Value Range:** [${config.valueRangeMin}, ${config.valueRangeMax}]
- **Peak Value:** ${config.peakValue} (at ${config.peakTimestampOffsetMs}ms, stream: ${config.peakStream})

## Data Files

- \`smartphone.acceleration.x/data.nt\` - Smartphone accelerometer X-axis data
- \`wearable.acceleration.x/data.nt\` - Wearable accelerometer X-axis data
- \`ground_truth.json\` - Expected results for validation

## Expected Query Results

For the main query:
\`\`\`sparql
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    { WINDOW wearableX { ?s saref:hasValue ?value } }
    UNION
    { WINDOW smartphoneX { ?s saref:hasValue ?value } }
}
\`\`\`

**Expected Result:** ${config.peakValue} (the inserted peak value)

Both Chunked Query Approach and Fetching Client-Side Approach should achieve **100% accuracy**.

## Usage

Set the DATA_PATH environment variable to use this dataset:

\`\`\`bash
export DATA_PATH=${config.datasetName}
npm run experiment:5-iterations
npm run experiment:calculate-accuracy
\`\`\`

## Validation

Expected accuracy:
- Chunked Query Approach: **100%** (result = ${config.peakValue})
- Fetching Client-Side: **100%** (result = ${config.peakValue})
- Approximation Approach: **~90-95%** (approximate result)
`;

  const readmePath = path.join(outputDir, 'README.md');
  fs.writeFileSync(readmePath, readme, 'utf-8');
  console.log(`README saved to: ${readmePath}`);
}

// ============================================================================
// Main Execution
// ============================================================================

function main() {
  console.log('='.repeat(80));
  console.log('Aligned Test Data Generator');
  console.log('='.repeat(80));
  console.log();

  // Create output directories
  const baseDir = config.outputDir;
  const datasetDir = path.join(baseDir, config.datasetName);
  const smartphoneDir = path.join(datasetDir, 'smartphone.acceleration.x');
  const wearableDir = path.join(datasetDir, 'wearable.acceleration.x');

  for (const dir of [baseDir, datasetDir, smartphoneDir, wearableDir]) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      console.log(`Created directory: ${dir}`);
    }
  }

  console.log();

  // Generate data
  console.log('Generating smartphone data...');
  const smartphoneObs = generateStreamData('smartphone', config);

  console.log('Generating wearable data...');
  const wearableObs = generateStreamData('wearable', config);

  console.log();

  // Write N-Triples files
  const smartphonePath = path.join(smartphoneDir, 'data.nt');
  const wearablePath = path.join(wearableDir, 'data.nt');

  writeNTriplesFile(smartphoneObs, 'smartphone', smartphonePath);
  writeNTriplesFile(wearableObs, 'wearable', wearablePath);

  console.log();

  // Write ground truth
  writeGroundTruth(smartphoneObs, wearableObs, datasetDir);

  console.log();

  // Write README
  writeReadme(datasetDir);

  console.log();
  console.log('='.repeat(80));
  console.log('Data generation complete!');
  console.log('='.repeat(80));
  console.log();
  console.log('To use this dataset:');
  console.log(`  export DATA_PATH=${config.datasetName}`);
  console.log('  npm run experiment:5-iterations');
  console.log('  npm run experiment:calculate-accuracy');
  console.log();
}

// Run the generator
main();
