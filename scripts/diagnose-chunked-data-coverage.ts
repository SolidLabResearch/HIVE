#!/usr/bin/env ts-node

/**
 * Diagnostic script to analyze data coverage in Chunked Query approach
 * Compares what data chunks receive vs what Client Side Processing receives
 *
 * Goal: Understand why Chunked Query returns 2.6970792 while Client Side doesn't
 */

import * as fs from "fs";
import * as path from "path";
import * as N3 from "n3";

interface DataPoint {
  timestamp: string;
  value: number;
  source: string; // 'smartphone' or 'wearable'
}

interface ChunkWindow {
  start: number;
  end: number;
  data: DataPoint[];
  max: number;
}

/**
 * Parse RDF data file and extract observations with values
 */
function parseDataFile(filePath: string, source: string): DataPoint[] {
  const content = fs.readFileSync(filePath, "utf-8");
  const parser = new N3.Parser();
  const quads = parser.parse(content);

  const observations = new Map<string, Partial<DataPoint>>();

  for (const quad of quads) {
    const subject = quad.subject.value;
    const predicate = quad.predicate.value;
    const object = quad.object.value;

    if (!observations.has(subject)) {
      observations.set(subject, { source });
    }

    const obs = observations.get(subject)!;

    if (predicate.includes("hasValue")) {
      obs.value = parseFloat(object);
    } else if (predicate.includes("hasTimestamp")) {
      obs.timestamp = object;
    }
  }

  const dataPoints: DataPoint[] = [];
  for (const [_, obs] of observations) {
    if (obs.value !== undefined && obs.timestamp) {
      dataPoints.push({
        timestamp: obs.timestamp,
        value: obs.value,
        source: obs.source!,
      });
    }
  }

  return dataPoints.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
}

/**
 * Simulate chunked processing with given window parameters
 */
function simulateChunkedProcessing(
  allData: DataPoint[],
  chunkRangeMs: number,
  chunkStepMs: number,
): ChunkWindow[] {
  if (allData.length === 0) return [];

  // Convert timestamps to milliseconds
  const getMs = (timestamp: string) => new Date(timestamp).getTime();

  const startTime = getMs(allData[0].timestamp);
  const endTime = getMs(allData[allData.length - 1].timestamp);

  const chunks: ChunkWindow[] = [];

  let windowStart = startTime;
  while (windowStart <= endTime) {
    const windowEnd = windowStart + chunkRangeMs;

    const windowData = allData.filter((d) => {
      const ts = getMs(d.timestamp);
      return ts >= windowStart && ts < windowEnd;
    });

    const max = windowData.length > 0
      ? Math.max(...windowData.map((d) => d.value))
      : -Infinity;

    if (windowData.length > 0) {
      chunks.push({
        start: windowStart,
        end: windowEnd,
        data: windowData,
        max,
      });
    }

    windowStart += chunkStepMs;
  }

  return chunks;
}

/**
 * Compute global MAX (Client Side approach)
 */
function computeGlobalMax(allData: DataPoint[]): number {
  if (allData.length === 0) return -Infinity;
  return Math.max(...allData.map((d) => d.value));
}

/**
 * Compute chunked MAX (aggregate chunk results)
 */
function computeChunkedMax(chunks: ChunkWindow[]): number {
  if (chunks.length === 0) return -Infinity;
  return Math.max(...chunks.map((c) => c.max));
}

function main() {
  console.log("\n" + "=".repeat(70));
  console.log("CHUNKED QUERY DATA COVERAGE DIAGNOSTIC");
  console.log("=".repeat(70) + "\n");

  const dataDir = path.join(__dirname, "..", "src", "streamer", "data");

  // Load both data streams
  const smartphonePath = path.join(dataDir, "smartphone.acceleration.x", "data.nt");
  const wearablePath = path.join(dataDir, "wearable.acceleration.x", "data.nt");

  console.log("Loading data files...");
  const smartphoneData = parseDataFile(smartphonePath, "smartphone");
  const wearableData = parseDataFile(wearablePath, "wearable");

  console.log(`  Smartphone observations: ${smartphoneData.length}`);
  console.log(`  Wearable observations: ${wearableData.length}`);

  // Find max values in each stream
  const smartphoneMax = Math.max(...smartphoneData.map((d) => d.value));
  const wearableMax = Math.max(...wearableData.map((d) => d.value));

  console.log(`\n  Smartphone MAX: ${smartphoneMax}`);
  console.log(`  Wearable MAX: ${wearableMax}`);

  // Combine all data
  const allData = [...smartphoneData, ...wearableData].sort((a, b) =>
    a.timestamp.localeCompare(b.timestamp)
  );

  const globalMax = computeGlobalMax(allData);
  console.log(`\n  GLOBAL MAX (Client Side): ${globalMax}`);

  // Simulate chunked processing with typical parameters
  // Based on the code, subqueries use smaller windows that get rewritten
  // Let's test with 60s chunks with 30s step (typical chunking)
  console.log("\n" + "=".repeat(70));
  console.log("SIMULATING CHUNKED PROCESSING");
  console.log("=".repeat(70) + "\n");

  const chunkConfigs = [
    { range: 60000, step: 30000, label: "60s RANGE, 30s STEP" },
    { range: 120000, step: 60000, label: "120s RANGE, 60s STEP" },
    { range: 30000, step: 15000, label: "30s RANGE, 15s STEP" },
  ];

  for (const config of chunkConfigs) {
    console.log(`\nConfiguration: ${config.label}`);
    console.log("-".repeat(70));

    const chunks = simulateChunkedProcessing(allData, config.range, config.step);
    const chunkedMax = computeChunkedMax(chunks);

    console.log(`  Total chunks: ${chunks.length}`);
    console.log(`  Chunked MAX: ${chunkedMax}`);
    console.log(`  Match global MAX? ${chunkedMax === globalMax ? "YES ✓" : "NO ✗"}`);

    // Show chunk details
    console.log(`\n  Chunk breakdown:`);
    for (let i = 0; i < Math.min(5, chunks.length); i++) {
      const chunk = chunks[i];
      const startDate = new Date(chunk.start).toISOString();
      const endDate = new Date(chunk.end).toISOString();
      console.log(`    Chunk ${i + 1}: ${startDate} → ${endDate}`);
      console.log(`      Data points: ${chunk.data.length}`);
      console.log(`      MAX: ${chunk.max}`);

      // Show distribution by source
      const smartphoneCount = chunk.data.filter((d) => d.source === "smartphone").length;
      const wearableCount = chunk.data.filter((d) => d.source === "wearable").length;
      console.log(`      Smartphone: ${smartphoneCount}, Wearable: ${wearableCount}`);
    }

    if (chunks.length > 5) {
      console.log(`    ... (${chunks.length - 5} more chunks)`);
    }

    // Find chunks that have the value 2.6970792
    const chunksWithValue = chunks.filter((c) =>
      c.data.some((d) => Math.abs(d.value - 2.6970792) < 0.0001)
    );
    console.log(`\n  Chunks containing 2.6970792: ${chunksWithValue.length}`);

    const chunksWhereItsMax = chunks.filter((c) =>
      Math.abs(c.max - 2.6970792) < 0.0001
    );
    console.log(`  Chunks where 2.6970792 IS the MAX: ${chunksWhereItsMax.length}`);

    if (chunksWhereItsMax.length > 0) {
      console.log(`\n  Details of chunks where 2.6970792 is MAX:`);
      for (const chunk of chunksWhereItsMax.slice(0, 3)) {
        console.log(`    Window: ${new Date(chunk.start).toISOString()}`);
        console.log(`      All values in chunk: ${chunk.data.map((d) => d.value.toFixed(2)).join(", ")}`);
        console.log(`      Sources: ${chunk.data.map((d) => d.source[0].toUpperCase()).join("")}`);
      }
    }
  }

  // Check time range overlap
  console.log("\n" + "=".repeat(70));
  console.log("TEMPORAL ALIGNMENT CHECK");
  console.log("=".repeat(70) + "\n");

  const smartphoneStart = new Date(smartphoneData[0].timestamp);
  const smartphoneEnd = new Date(smartphoneData[smartphoneData.length - 1].timestamp);
  const wearableStart = new Date(wearableData[0].timestamp);
  const wearableEnd = new Date(wearableData[wearableData.length - 1].timestamp);

  console.log(`Smartphone time range:`);
  console.log(`  Start: ${smartphoneStart.toISOString()}`);
  console.log(`  End:   ${smartphoneEnd.toISOString()}`);
  console.log(`  Duration: ${((smartphoneEnd.getTime() - smartphoneStart.getTime()) / 1000).toFixed(2)}s`);

  console.log(`\nWearable time range:`);
  console.log(`  Start: ${wearableStart.toISOString()}`);
  console.log(`  End:   ${wearableEnd.toISOString()}`);
  console.log(`  Duration: ${((wearableEnd.getTime() - wearableStart.getTime()) / 1000).toFixed(2)}s`);

  const gapMs = Math.max(
    smartphoneStart.getTime() - wearableEnd.getTime(),
    wearableStart.getTime() - smartphoneEnd.getTime()
  );

  if (gapMs > 0) {
    console.log(`\n⚠️  WARNING: Time gap between streams: ${(gapMs / 1000).toFixed(2)}s`);
    console.log(`   Streams do NOT overlap temporally!`);
    console.log(`   This explains why chunked processing may miss data.`);
  } else {
    console.log(`\n✓ Streams overlap temporally`);
  }

  console.log("\n" + "=".repeat(70));
  console.log("CONCLUSION");
  console.log("=".repeat(70) + "\n");

  if (Math.abs(globalMax - smartphoneMax) < 0.0001) {
    console.log(`The global MAX (${globalMax}) comes from the smartphone stream.`);
  } else if (Math.abs(globalMax - wearableMax) < 0.0001) {
    console.log(`The global MAX (${globalMax}) comes from the wearable stream.`);
  }

  console.log(`\nMathematically, MAX(chunks) should equal MAX(all_data) = ${globalMax}`);
  console.log(`IF all data is covered by the chunks.`);
  console.log(`\nIf Chunked Query returns different values (like 2.6970792),`);
  console.log(`possible causes are:`);
  console.log(`  1. Sliding window drops chunks that arrived outside the window`);
  console.log(`  2. Temporal misalignment causes data to be missed`);
  console.log(`  3. Subqueries don't receive all the data`);
  console.log(`  4. Window boundaries don't cover all data points`);
  console.log(`\nRun this script to verify the theoretical behavior.`);
  console.log(`Then compare with actual runtime logs to find the discrepancy.\n`);
}

main();
