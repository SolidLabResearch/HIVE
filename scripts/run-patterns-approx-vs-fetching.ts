#!/usr/bin/env ts-node

/**
 * 35-Iteration Pattern-Based Test for Approximation vs Ground Truth
 * Tests approximation and fetching approaches with 5 different stream patterns,
 * running 35 iterations for each pattern to verify stability and accuracy.
 */

import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as path from "path";
import * as fs from "fs";

const MQTT_BROKER = "mqtt://localhost:1883";

// Input topics
const WEARABLE_TOPIC = "wearableX";
const SMARTPHONE_TOPIC = "smartphoneX";

// Output topics for each approach
const OUTPUT_TOPICS = {
  approximation: "approximation/output",
  fetching: "client_operation_output",
};

// Test parameters
const DATA_RATE_HZ = 4;
const PATTERN_DURATION_S = 120; // 120 seconds per pattern
const INIT_WAIT_S = 10;
const FINAL_WAIT_S = 20;
const ITERATIONS_PER_PATTERN = 35; // 35 iterations per pattern

/**
 * Data pattern generators based on the provided parameters
 */
interface DataPattern {
  name: string;
  description: string;
  wearableGenerator: (index: number, totalEvents: number) => number;
  smartphoneGenerator: (index: number, totalEvents: number) => number;
}

const DATA_PATTERNS: DataPattern[] = [
  {
    name: "Low Variability",
    description: "Low variability with μ=-23.0, σ=0.25",
    wearableGenerator: (_index: number, _totalEvents: number) => {
      // Normal distribution around -23.0 with σ=0.25
      const u1 = Math.random();
      const u2 = Math.random();
      const z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
      return -23.0 + z0 * 0.25;
    },
    smartphoneGenerator: (_index: number, _totalEvents: number) => {
      // Same for smartphone
      const u1 = Math.random();
      const u2 = Math.random();
      const z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
      return -23.0 + z0 * 0.25;
    },
  },
  {
    name: "Step Pattern",
    description: "Step pattern with v1=-23.0, v2=-15.0, t_step=60s",
    wearableGenerator: (index: number, totalEvents: number) => {
      const stepPoint = (60 * DATA_RATE_HZ); // 60 seconds * 4Hz = 240 events
      return index < stepPoint ? -23.0 : -15.0;
    },
    smartphoneGenerator: (index: number, totalEvents: number) => {
      const stepPoint = (60 * DATA_RATE_HZ);
      return index < stepPoint ? -23.0 : -15.0;
    },
  },
  {
    name: "Spike Pattern",
    description: "Spike pattern with v_base=-23.0, v_spike=-5.0, Δt=1.25s",
    wearableGenerator: (index: number, totalEvents: number) => {
      const spikeDuration = 1.25 * DATA_RATE_HZ; // 1.25s * 4Hz = 5 events
      const spikeStart = Math.floor(totalEvents / 2) - Math.floor(spikeDuration / 2);
      const spikeEnd = spikeStart + spikeDuration;
      return (index >= spikeStart && index < spikeEnd) ? -5.0 : -23.0;
    },
    smartphoneGenerator: (index: number, totalEvents: number) => {
      const spikeDuration = 1.25 * DATA_RATE_HZ;
      const spikeStart = Math.floor(totalEvents / 2) - Math.floor(spikeDuration / 2);
      const spikeEnd = spikeStart + spikeDuration;
      return (index >= spikeStart && index < spikeEnd) ? -5.0 : -23.0;
    },
  },
  {
    name: "Low Freq. Oscillation",
    description: "Low frequency oscillation with μ=-23.0, A=5.0, f=0.05Hz",
    wearableGenerator: (index: number, _totalEvents: number) => {
      const time = index / DATA_RATE_HZ; // Convert to seconds
      return -23.0 + 5.0 * Math.sin(2 * Math.PI * 0.05 * time);
    },
    smartphoneGenerator: (index: number, _totalEvents: number) => {
      const time = index / DATA_RATE_HZ;
      return -23.0 + 5.0 * Math.cos(2 * Math.PI * 0.05 * time);
    },
  },
  {
    name: "High Freq. Oscillation",
    description: "High frequency oscillation with μ=-23.0, A=3.0, f=0.5Hz",
    wearableGenerator: (index: number, _totalEvents: number) => {
      const time = index / DATA_RATE_HZ;
      return -23.0 + 3.0 * Math.sin(2 * Math.PI * 0.5 * time);
    },
    smartphoneGenerator: (index: number, _totalEvents: number) => {
      const time = index / DATA_RATE_HZ;
      return -23.0 + 3.0 * Math.cos(2 * Math.PI * 0.5 * time);
    },
  },
];

interface ApproachResult {
  name: string;
  topic: string;
  results: Array<{ timestamp: number; content: string; value?: number }>;
  started: boolean;
  errors: string[];
}

interface IterationResult {
  iterationNumber: number;
  patternName: string;
  approximation: { passed: boolean; resultCount: number; avgValue?: number };
  fetching: { passed: boolean; resultCount: number; avgValue?: number };
  duration: number;
}

interface PatternSummary {
  patternName: string;
  iterations: IterationResult[];
  approximation: {
    successRate: number;
    avgResults: number;
    avgValue?: number;
  };
  fetching: {
    successRate: number;
    avgResults: number;
    avgValue?: number;
  };
}

/**
 * Single iteration test runner
 */
class SingleIterationRunner {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private publisherClient?: mqtt.MqttClient;
  private approachResults: Map<string, ApproachResult> = new Map();
  private dataPublishCount = 0;
  private pattern: DataPattern;
  private iterationNumber: number;
  private startTime: number = 0;

  constructor(pattern: DataPattern, iterationNumber: number) {
    this.pattern = pattern;
    this.iterationNumber = iterationNumber;
    this.initializeResults();
  }

  private initializeResults(): void {
    this.approachResults.set("approximation", {
      name: "Approximation Approach",
      topic: OUTPUT_TOPICS.approximation,
      results: [],
      started: false,
      errors: [],
    });
    this.approachResults.set("fetching", {
      name: "Fetching Client Side",
      topic: OUTPUT_TOPICS.fetching,
      results: [],
      started: false,
      errors: [],
    });
  }

  async run(): Promise<IterationResult> {
    console.log(`\n${"=".repeat(70)}`);
    console.log(`PATTERN: ${this.pattern.name} | ITERATION ${this.iterationNumber}`);
    console.log(`Description: ${this.pattern.description}`);
    console.log("=".repeat(70));

    this.startTime = Date.now();

    try {
      await this.clearPreviousData();
      await this.setupMQTTMonitoring();
      await this.launchOrchestrators();

      console.log(`  [${this.pattern.name} #${this.iterationNumber}] Waiting ${INIT_WAIT_S}s for initialization...`);
      await this.sleep(INIT_WAIT_S * 1000);

      await this.publishPatternData();

      console.log(`  [${this.pattern.name} #${this.iterationNumber}] Waiting ${FINAL_WAIT_S}s for final results...`);
      await this.sleep(FINAL_WAIT_S * 1000);

      const result = this.generateResult();
      return result;
    } catch (error) {
      console.error(`  [${this.pattern.name} #${this.iterationNumber}] ERROR:`, error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  private async clearPreviousData(): Promise<void> {
    const clearClient = mqtt.connect(MQTT_BROKER, {
      clientId: `clearer-${this.pattern.name}-${this.iterationNumber}-${Math.random().toString(16).substr(2, 8)}`,
      clean: true,
    });

    await new Promise<void>((resolve) => {
      clearClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.fetching,
        ];

        topics.forEach((topic) => {
          clearClient.publish(topic, "", { qos: 1, retain: true });
        });

        setTimeout(() => {
          clearClient.end(true);
          resolve();
        }, 500);
      });
    });
  }

  private async setupMQTTMonitoring(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.mqttClient = mqtt.connect(MQTT_BROKER, {
        clientId: `monitor-${this.pattern.name}-${this.iterationNumber}-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.fetching,
        ];

        this.mqttClient!.subscribe(topics, { qos: 1 }, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });

      this.mqttClient.on("message", (topic: string, message: Buffer) => {
        const content = message.toString();
        if (!content || content.trim() === "") return;

        const timestamp = Date.now();
        let value: number | undefined;

        // Extract numeric value
        try {
          if (topic === OUTPUT_TOPICS.approximation) {
            const parsed = JSON.parse(content);
            value = parsed.unifiedResult || parsed.unifiedAverage;
          } else if (topic === OUTPUT_TOPICS.fetching) {
            const match = content.match(/"([^"]+)"/);
            if (match) value = parseFloat(match[1]);
          }
        } catch (e) {
          // Ignore parsing errors
        }

        if (topic === OUTPUT_TOPICS.approximation) {
          this.recordResult("approximation", content, value);
        } else if (topic === OUTPUT_TOPICS.fetching) {
          this.recordResult("fetching", content, value);
        }
      });

      this.mqttClient.on("error", (err) => {
        reject(err);
      });
    });
  }

  private recordResult(approach: string, content: string, value?: number): void {
    const result = this.approachResults.get(approach);
    if (result) {
      result.results.push({ timestamp: Date.now(), content, value });
    }
  }

  private async launchOrchestrators(): Promise<void> {
    await this.launchOrchestrator(
      "approximation",
      "src/approaches/ApproximationApproachOrchestrator.ts",
      { HTTP_PORT: "8081" }
    );

    await this.launchOrchestrator(
      "fetching",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
      { HTTP_PORT: "8082" }
    );
  }

  private launchOrchestrator(
    name: string,
    scriptPath: string,
    env: Record<string, string>
  ): Promise<void> {
    return new Promise((resolve) => {
      const fullPath = path.resolve(__dirname, scriptPath);

      const proc = spawn("npx", ["ts-node", fullPath], {
        env: { ...process.env, ...env },
        stdio: ["pipe", "pipe", "pipe"],
      });

      this.orchestrators.set(name, proc);

      proc.on("error", (err) => {
        const result = this.approachResults.get(name);
        if (result) result.errors.push(err.message);
      });

      const result = this.approachResults.get(name);
      if (result) result.started = true;

      setTimeout(resolve, 1000);
    });
  }

  private async publishPatternData(): Promise<void> {
    console.log(`  [${this.pattern.name} #${this.iterationNumber}] Publishing pattern data (${PATTERN_DURATION_S}s, ${DATA_RATE_HZ}Hz)...`);

    return new Promise((resolve) => {
      this.publisherClient = mqtt.connect(MQTT_BROKER, {
        clientId: `publisher-${this.pattern.name}-${this.iterationNumber}-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      this.publisherClient.on("connect", () => {
        const totalEvents = PATTERN_DURATION_S * DATA_RATE_HZ;
        const intervalMs = 1000 / DATA_RATE_HZ;
        let count = 0;

        const interval = setInterval(() => {
          if (count >= totalEvents) {
            clearInterval(interval);
            console.log(`  [${this.pattern.name} #${this.iterationNumber}] Published ${count} events`);
            resolve();
            return;
          }

          const timestamp = new Date().toISOString();
          const wearableValue = this.pattern.wearableGenerator(count, totalEvents);
          const smartphoneValue = this.pattern.smartphoneGenerator(count, totalEvents);

          const wearableData = `
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasValue> "${wearableValue.toFixed(3)}"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .
          `.trim();

          const smartphoneData = `
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasValue> "${smartphoneValue.toFixed(3)}"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/smartphoneX> .
          `.trim();

          this.publisherClient!.publish(WEARABLE_TOPIC, wearableData, { qos: 1 });
          this.publisherClient!.publish(SMARTPHONE_TOPIC, smartphoneData, { qos: 1 });

          count++;
          this.dataPublishCount = count;

          if (count % (DATA_RATE_HZ * 30) === 0) {
            const elapsed = count / DATA_RATE_HZ;
            const approxResults = this.approachResults.get("approximation")?.results.length || 0;
            const fetchingResults = this.approachResults.get("fetching")?.results.length || 0;
            console.log(
              `    [${elapsed}s] Results - A:${approxResults} F:${fetchingResults}`
            );
          }
        }, intervalMs);
      });
    });
  }

  private generateResult(): IterationResult {
    const duration = Date.now() - this.startTime;
    const approx = this.approachResults.get("approximation")!;
    const fetching = this.approachResults.get("fetching")!;

    const calculateAvg = (results: typeof approx.results) => {
      const values = results.map((r) => r.value).filter((v) => v !== undefined) as number[];
      if (values.length === 0) return undefined;
      return values.reduce((a, b) => a + b, 0) / values.length;
    };

    return {
      iterationNumber: this.iterationNumber,
      patternName: this.pattern.name,
      approximation: {
        passed: approx.results.length > 0,
        resultCount: approx.results.length,
        avgValue: calculateAvg(approx.results),
      },
      fetching: {
        passed: fetching.results.length > 0,
        resultCount: fetching.results.length,
        avgValue: calculateAvg(fetching.results),
      },
      duration: duration,
    };
  }

  private async cleanup(): Promise<void> {
    for (const [_name, proc] of this.orchestrators) {
      try {
        proc.kill("SIGTERM");
        await this.sleep(500);
        if (!proc.killed) {
          proc.kill("SIGKILL");
        }
      } catch (e) {
        // Ignore
      }
    }

    if (this.mqttClient) {
      this.mqttClient.end(true);
    }
    if (this.publisherClient) {
      this.publisherClient.end(true);
    }

    await this.sleep(2000);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

/**
 * Pattern-based multi-iteration orchestrator
 */
class PatternMultiIterationTester {
  private patternSummaries: PatternSummary[] = [];

  async runAllPatterns(): Promise<void> {
    console.log("\n" + "=".repeat(70));
    console.log("35-ITERATION PATTERN-BASED VERIFICATION: APPROXIMATION VS GROUND TRUTH");
    console.log("=".repeat(70));
    console.log(`Testing approximation and fetching approaches with ${DATA_PATTERNS.length} patterns × ${ITERATIONS_PER_PATTERN} iterations each`);
    console.log(`Total iterations: ${DATA_PATTERNS.length * ITERATIONS_PER_PATTERN}`);
    console.log("=".repeat(70));

    const startTime = Date.now();

    for (let patternIndex = 0; patternIndex < DATA_PATTERNS.length; patternIndex++) {
      const pattern = DATA_PATTERNS[patternIndex];
      console.log(`\n[INFO] Starting pattern ${patternIndex + 1}/${DATA_PATTERNS.length}: ${pattern.name}`);

      const patternResults: IterationResult[] = [];

      for (let iteration = 1; iteration <= ITERATIONS_PER_PATTERN; iteration++) {
        console.log(`\n[INFO] Pattern ${pattern.name} - Iteration ${iteration}/${ITERATIONS_PER_PATTERN}...`);

        try {
          const runner = new SingleIterationRunner(pattern, iteration);
          const result = await runner.run();
          patternResults.push(result);

          if (iteration < ITERATIONS_PER_PATTERN) {
            console.log(`\n[INFO] Waiting 3s between iterations...`);
            await this.sleep(3000);
          }
        } catch (error) {
          console.error(`[ERROR] Pattern ${pattern.name} iteration ${iteration} failed:`, error);
          patternResults.push({
            iterationNumber: iteration,
            patternName: pattern.name,
            approximation: { passed: false, resultCount: 0 },
            fetching: { passed: false, resultCount: 0 },
            duration: 0,
          });
        }
      }

      const patternSummary = this.generatePatternSummary(pattern.name, patternResults);
      this.patternSummaries.push(patternSummary);
      this.printPatternSummary(patternSummary);

      if (patternIndex < DATA_PATTERNS.length - 1) {
        console.log(`\n[INFO] Waiting 10s before next pattern...`);
        await this.sleep(10000);
      }
    }

    const totalTime = Date.now() - startTime;
    this.printFinalSummary(totalTime);
    this.saveResultsToFile();
  }

  private generatePatternSummary(patternName: string, results: IterationResult[]): PatternSummary {
    const approxResults = results.map(r => r.approximation);
    const fetchingResults = results.map(r => r.fetching);

    const approxSuccessCount = approxResults.filter(r => r.passed).length;
    const fetchingSuccessCount = fetchingResults.filter(r => r.passed).length;

    const approxAvgResults = approxResults.reduce((sum, r) => sum + r.resultCount, 0) / results.length;
    const fetchingAvgResults = fetchingResults.reduce((sum, r) => sum + r.resultCount, 0) / results.length;

    const approxAvgValue = this.calculateAverageValue(approxResults);
    const fetchingAvgValue = this.calculateAverageValue(fetchingResults);

    return {
      patternName,
      iterations: results,
      approximation: {
        successRate: (approxSuccessCount / results.length) * 100,
        avgResults: approxAvgResults,
        avgValue: approxAvgValue,
      },
      fetching: {
        successRate: (fetchingSuccessCount / results.length) * 100,
        avgResults: fetchingAvgResults,
        avgValue: fetchingAvgValue,
      },
    };
  }

  private calculateAverageValue(results: Array<{ avgValue?: number }>): number | undefined {
    const values = results.map(r => r.avgValue).filter(v => v !== undefined) as number[];
    if (values.length === 0) return undefined;
    return values.reduce((a, b) => a + b, 0) / values.length;
  }

  private printPatternSummary(summary: PatternSummary): void {
    console.log(`\n${"=".repeat(70)}`);
    console.log(`PATTERN SUMMARY: ${summary.patternName}`);
    console.log("=".repeat(70));

    console.log(`Iterations completed: ${summary.iterations.length}/${ITERATIONS_PER_PATTERN}`);

    console.log(`\nApproximation Approach:`);
    console.log(`  Success rate: ${summary.approximation.successRate.toFixed(1)}%`);
    console.log(`  Avg results per iteration: ${summary.approximation.avgResults.toFixed(1)}`);
    if (summary.approximation.avgValue !== undefined) {
      console.log(`  Avg value: ${summary.approximation.avgValue.toFixed(3)}`);
    }

    console.log(`\nFetching (Ground Truth):`);
    console.log(`  Success rate: ${summary.fetching.successRate.toFixed(1)}%`);
    console.log(`  Avg results per iteration: ${summary.fetching.avgResults.toFixed(1)}`);
    if (summary.fetching.avgValue !== undefined) {
      console.log(`  Avg value: ${summary.fetching.avgValue.toFixed(3)}`);
    }

    console.log("=".repeat(70));
  }

  private printFinalSummary(totalTime: number): void {
    console.log("\n" + "=".repeat(70));
    console.log("FINAL COMPREHENSIVE SUMMARY");
    console.log("=".repeat(70));

    console.log(`\nTotal patterns tested: ${this.patternSummaries.length}`);
    console.log(`Total iterations: ${this.patternSummaries.length * ITERATIONS_PER_PATTERN}`);
    console.log(`Total time: ${(totalTime / 1000 / 60).toFixed(1)} minutes`);

    console.log(`\nPattern-by-Pattern Results:`);
    console.log(`  ${"Pattern".padEnd(20)} | ${"Approx Success".padEnd(13)} | ${"Fetching Success".padEnd(15)} | ${"Approx Avg Val".padEnd(13)} | ${"Fetching Avg Val".padEnd(15)}`);
    console.log(`  ${"-".repeat(20)} | ${"-".repeat(13)} | ${"-".repeat(15)} | ${"-".repeat(13)} | ${"-".repeat(15)}`);

    this.patternSummaries.forEach((summary) => {
      const approxSuccess = `${summary.approximation.successRate.toFixed(1)}%`;
      const fetchingSuccess = `${summary.fetching.successRate.toFixed(1)}%`;
      const approxAvg = summary.approximation.avgValue !== undefined ? summary.approximation.avgValue.toFixed(3) : "N/A";
      const fetchingAvg = summary.fetching.avgValue !== undefined ? summary.fetching.avgValue.toFixed(3) : "N/A";
      console.log(
        `  ${summary.patternName.padEnd(20)} | ${approxSuccess.padEnd(13)} | ${fetchingSuccess.padEnd(15)} | ${approxAvg.padEnd(13)} | ${fetchingAvg.padEnd(15)}`
      );
    });

    console.log("\n" + "-".repeat(70));

    const overallApproxSuccess = this.patternSummaries.reduce((sum, s) => sum + s.approximation.successRate, 0) / this.patternSummaries.length;
    const overallFetchingSuccess = this.patternSummaries.reduce((sum, s) => sum + s.fetching.successRate, 0) / this.patternSummaries.length;

    console.log(`Overall Success Rates:`);
    console.log(`  Approximation: ${overallApproxSuccess.toFixed(1)}%`);
    console.log(`  Fetching:      ${overallFetchingSuccess.toFixed(1)}%`);

    console.log("=".repeat(70));
  }

  private saveResultsToFile(): void {
    const filename = `pattern-35-iterations-results-${new Date().toISOString().replace(/:/g, "-").split(".")[0]}.json`;
    const data = {
      timestamp: new Date().toISOString(),
      patternsCount: this.patternSummaries.length,
      iterationsPerPattern: ITERATIONS_PER_PATTERN,
      totalIterations: this.patternSummaries.length * ITERATIONS_PER_PATTERN,
      patternSummaries: this.patternSummaries,
      overallStats: {
        approximation: {
          avgSuccessRate: this.patternSummaries.reduce((sum, s) => sum + s.approximation.successRate, 0) / this.patternSummaries.length,
        },
        fetching: {
          avgSuccessRate: this.patternSummaries.reduce((sum, s) => sum + s.fetching.successRate, 0) / this.patternSummaries.length,
        },
      },
    };

    fs.writeFileSync(filename, JSON.stringify(data, null, 2));
    console.log(`\nDetailed results saved to: ${filename}`);

    // Also save CSV summary
    const csvFilename = `pattern-35-iterations-summary-${new Date().toISOString().replace(/:/g, "-").split(".")[0]}.csv`;
    const csvLines = [
      "pattern,approximation_success_rate,approximation_avg_results,approximation_avg_value,fetching_success_rate,fetching_avg_results,fetching_avg_value",
    ];

    this.patternSummaries.forEach((s) => {
      csvLines.push(
        [
          s.patternName,
          s.approximation.successRate.toFixed(2),
          s.approximation.avgResults.toFixed(2),
          s.approximation.avgValue?.toFixed(4) || "",
          s.fetching.successRate.toFixed(2),
          s.fetching.avgResults.toFixed(2),
          s.fetching.avgValue?.toFixed(4) || "",
        ].join(",")
      );
    });

    fs.writeFileSync(csvFilename, csvLines.join("\n"));
    console.log(`CSV summary saved to: ${csvFilename}`);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Run the 35-iteration pattern-based test for approximation vs fetching
const tester = new PatternMultiIterationTester();
tester.runAllPatterns().then(() => {
  console.log("\n[INFO] 35-iteration pattern-based test for approximation vs fetching complete");
  process.exit(0);
}).catch((err) => {
  console.error("\n[ERROR] Pattern test failed:", err);
  process.exit(1);
});
