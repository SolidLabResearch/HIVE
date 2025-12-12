#!/usr/bin/env ts-node

/**
 * Comprehensive pattern-based test for all 3 streaming query approaches.
 * Tests different data patterns: constant, oscillating, step changes, noise, etc.
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
  chunked: "output",
  fetching: "client_operation_output",
};

// Test parameters
const DATA_RATE_HZ = 4;
const PATTERN_DURATION_S = 90; // 90 seconds per pattern
const INIT_WAIT_S = 10;
const FINAL_WAIT_S = 20;

/**
 * Data pattern generators
 */
interface DataPattern {
  name: string;
  description: string;
  wearableGenerator: (index: number, baseTime: number) => number;
  smartphoneGenerator: (index: number, baseTime: number) => number;
}

const DATA_PATTERNS: DataPattern[] = [
  {
    name: "Constant",
    description: "Stable constant values with minimal noise",
    wearableGenerator: (_index: number, _baseTime: number) => {
      return 20.0 + (Math.random() - 0.5) * 0.2; // 20 ± 0.1
    },
    smartphoneGenerator: (_index: number, _baseTime: number) => {
      return 15.0 + (Math.random() - 0.5) * 0.2; // 15 ± 0.1
    },
  },
  {
    name: "Sine Wave",
    description: "Smooth sinusoidal oscillations",
    wearableGenerator: (index: number, _baseTime: number) => {
      const frequency = 0.05; // Complete ~5 cycles over 90s at 4Hz
      return 20.0 + 10.0 * Math.sin(2 * Math.PI * frequency * index);
    },
    smartphoneGenerator: (index: number, _baseTime: number) => {
      const frequency = 0.05;
      return 15.0 + 5.0 * Math.cos(2 * Math.PI * frequency * index);
    },
  },
  {
    name: "Step Changes",
    description: "Discrete step changes in values",
    wearableGenerator: (index: number, _baseTime: number) => {
      const totalEvents = PATTERN_DURATION_S * DATA_RATE_HZ;
      const stepSize = totalEvents / 4;
      const step = Math.floor(index / stepSize);
      return 10.0 + step * 5.0; // Steps: 10, 15, 20, 25
    },
    smartphoneGenerator: (index: number, _baseTime: number) => {
      const totalEvents = PATTERN_DURATION_S * DATA_RATE_HZ;
      const stepSize = totalEvents / 3;
      const step = Math.floor(index / stepSize);
      return 12.0 + step * 3.0; // Steps: 12, 15, 18
    },
  },
  {
    name: "Linear Drift",
    description: "Gradual linear increase over time",
    wearableGenerator: (index: number, _baseTime: number) => {
      const totalEvents = PATTERN_DURATION_S * DATA_RATE_HZ;
      const progress = index / totalEvents;
      return 10.0 + progress * 20.0; // Drift from 10 to 30
    },
    smartphoneGenerator: (index: number, _baseTime: number) => {
      const totalEvents = PATTERN_DURATION_S * DATA_RATE_HZ;
      const progress = index / totalEvents;
      return 10.0 + progress * 10.0; // Drift from 10 to 20
    },
  },
  {
    name: "Random Walk",
    description: "Random walk with bounded values",
    wearableGenerator: (() => {
      let currentValue = 20.0;
      return (_index: number, _baseTime: number) => {
        const step = (Math.random() - 0.5) * 2.0;
        currentValue += step;
        currentValue = Math.max(10, Math.min(30, currentValue)); // Bound between 10-30
        return currentValue;
      };
    })(),
    smartphoneGenerator: (() => {
      let currentValue = 15.0;
      return (_index: number, _baseTime: number) => {
        const step = (Math.random() - 0.5) * 1.5;
        currentValue += step;
        currentValue = Math.max(10, Math.min(20, currentValue)); // Bound between 10-20
        return currentValue;
      };
    })(),
  },
];

interface ApproachResult {
  name: string;
  topic: string;
  results: Array<{ timestamp: number; content: string; value?: number }>;
  started: boolean;
  errors: string[];
}

interface PatternTestResult {
  patternName: string;
  approximation: { passed: boolean; resultCount: number; avgValue?: number };
  chunked: { passed: boolean; resultCount: number; avgValue?: number };
  fetching: { passed: boolean; resultCount: number; avgValue?: number };
  duration: number;
}

/**
 * Single pattern test runner
 */
class PatternTestRunner {
  private orchestrators: Map<string, ChildProcess> = new Map();
  private mqttClient?: mqtt.MqttClient;
  private publisherClient?: mqtt.MqttClient;
  private approachResults: Map<string, ApproachResult> = new Map();
  private dataPublishCount = 0;
  private pattern: DataPattern;
  private startTime: number = 0;

  constructor(pattern: DataPattern) {
    this.pattern = pattern;
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
    this.approachResults.set("chunked", {
      name: "Chunked Query Approach",
      topic: OUTPUT_TOPICS.chunked,
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

  async run(): Promise<PatternTestResult> {
    console.log(`\n${"=".repeat(70)}`);
    console.log(`PATTERN TEST: ${this.pattern.name}`);
    console.log(`Description: ${this.pattern.description}`);
    console.log("=".repeat(70));

    this.startTime = Date.now();

    try {
      await this.clearPreviousData();
      await this.setupMQTTMonitoring();
      await this.launchOrchestrators();

      console.log(`  [${this.pattern.name}] Waiting ${INIT_WAIT_S}s for initialization...`);
      await this.sleep(INIT_WAIT_S * 1000);

      await this.publishPatternData();

      console.log(`  [${this.pattern.name}] Waiting ${FINAL_WAIT_S}s for final results...`);
      await this.sleep(FINAL_WAIT_S * 1000);

      const result = this.generateResult();
      return result;
    } catch (error) {
      console.error(`  [${this.pattern.name}] ERROR:`, error);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  private async clearPreviousData(): Promise<void> {
    const clearClient = mqtt.connect(MQTT_BROKER, {
      clientId: `clearer-${this.pattern.name}-${Math.random().toString(16).substr(2, 8)}`,
      clean: true,
    });

    await new Promise<void>((resolve) => {
      clearClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
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
        clientId: `monitor-${this.pattern.name}-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      this.mqttClient.on("connect", () => {
        const topics = [
          OUTPUT_TOPICS.approximation,
          OUTPUT_TOPICS.chunked,
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
          } else if (topic === OUTPUT_TOPICS.chunked) {
            const match = content.match(/"([^"]+)"\^\^/);
            if (match) value = parseFloat(match[1]);
          } else if (topic === OUTPUT_TOPICS.fetching) {
            const match = content.match(/"([^"]+)"/);
            if (match) value = parseFloat(match[1]);
          }
        } catch (e) {
          // Ignore parsing errors
        }

        if (topic === OUTPUT_TOPICS.approximation) {
          this.recordResult("approximation", content, value);
        } else if (topic === OUTPUT_TOPICS.chunked) {
          this.recordResult("chunked", content, value);
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
      "chunked",
      "src/approaches/ChunkedQueryApproachOrchestrator.ts",
      { HTTP_PORT: "8082" }
    );

    await this.launchOrchestrator(
      "fetching",
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
      { HTTP_PORT: "8083" }
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
    console.log(`  [${this.pattern.name}] Publishing data with pattern: ${this.pattern.description}`);

    return new Promise((resolve) => {
      this.publisherClient = mqtt.connect(MQTT_BROKER, {
        clientId: `publisher-${this.pattern.name}-${Math.random().toString(16).substr(2, 8)}`,
        clean: true,
      });

      this.publisherClient.on("connect", () => {
        const totalEvents = PATTERN_DURATION_S * DATA_RATE_HZ;
        const intervalMs = 1000 / DATA_RATE_HZ;
        const baseTime = Date.now();
        let count = 0;

        const interval = setInterval(() => {
          if (count >= totalEvents) {
            clearInterval(interval);
            console.log(`  [${this.pattern.name}] Published ${count} events`);
            resolve();
            return;
          }

          const timestamp = new Date().toISOString();
          const wearableValue = this.pattern.wearableGenerator(count, baseTime);
          const smartphoneValue = this.pattern.smartphoneGenerator(count, baseTime);

          const wearableData = `
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasValue> "${wearableValue}"^^<http://www.w3.org/2001/XMLSchema#float> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/wearableX> .
          `.trim();

          const smartphoneData = `
<https://rsp.js/event/${count}> <https://saref.etsi.org/core/hasValue> "${smartphoneValue}"^^<http://www.w3.org/2001/XMLSchema#float> .
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
            const chunkedResults = this.approachResults.get("chunked")?.results.length || 0;
            const fetchingResults = this.approachResults.get("fetching")?.results.length || 0;
            console.log(
              `    [${elapsed}s] Results - A:${approxResults} C:${chunkedResults} F:${fetchingResults}`
            );
          }
        }, intervalMs);
      });
    });
  }

  private generateResult(): PatternTestResult {
    const duration = Date.now() - this.startTime;
    const approx = this.approachResults.get("approximation")!;
    const chunked = this.approachResults.get("chunked")!;
    const fetching = this.approachResults.get("fetching")!;

    const calculateAvg = (results: typeof approx.results) => {
      const values = results.map((r) => r.value).filter((v) => v !== undefined) as number[];
      if (values.length === 0) return undefined;
      return values.reduce((a, b) => a + b, 0) / values.length;
    };

    return {
      patternName: this.pattern.name,
      approximation: {
        passed: approx.results.length > 0,
        resultCount: approx.results.length,
        avgValue: calculateAvg(approx.results),
      },
      chunked: {
        passed: chunked.results.length > 0,
        resultCount: chunked.results.length,
        avgValue: calculateAvg(chunked.results),
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
 * Multi-pattern test orchestrator
 */
class MultiPatternTester {
  private results: PatternTestResult[] = [];

  async runAllPatterns(): Promise<void> {
    console.log("\n" + "=".repeat(70));
    console.log("COMPREHENSIVE PATTERN-BASED VERIFICATION TEST");
    console.log("=".repeat(70));
    console.log(`Testing ${DATA_PATTERNS.length} different data patterns`);
    console.log(`Duration per pattern: ${PATTERN_DURATION_S}s + ${INIT_WAIT_S + FINAL_WAIT_S}s overhead`);
    console.log("=".repeat(70));

    const startTime = Date.now();

    for (let i = 0; i < DATA_PATTERNS.length; i++) {
      const pattern = DATA_PATTERNS[i];
      console.log(`\n[INFO] Testing pattern ${i + 1}/${DATA_PATTERNS.length}: ${pattern.name}`);

      try {
        const runner = new PatternTestRunner(pattern);
        const result = await runner.run();
        this.results.push(result);
        this.printPatternResult(result);

        if (i < DATA_PATTERNS.length - 1) {
          console.log(`\n[INFO] Waiting 5s before next pattern...`);
          await this.sleep(5000);
        }
      } catch (error) {
        console.error(`[ERROR] Pattern ${pattern.name} failed:`, error);
        this.results.push({
          patternName: pattern.name,
          approximation: { passed: false, resultCount: 0 },
          chunked: { passed: false, resultCount: 0 },
          fetching: { passed: false, resultCount: 0 },
          duration: 0,
        });
      }
    }

    const totalTime = Date.now() - startTime;
    this.printFinalSummary(totalTime);
    this.saveResultsToFile();
  }

  private printPatternResult(result: PatternTestResult): void {
    const approxIcon = result.approximation.passed ? "[OK]" : "[X]";
    const chunkedIcon = result.chunked.passed ? "[OK]" : "[X]";
    const fetchingIcon = result.fetching.passed ? "[OK]" : "[X]";

    console.log(`\n  ${result.patternName} Results (${(result.duration / 1000).toFixed(1)}s):`);
    console.log(
      `    ${approxIcon} Approximation: ${result.approximation.resultCount} results` +
        (result.approximation.avgValue ? ` (avg: ${result.approximation.avgValue.toFixed(2)})` : "")
    );
    console.log(
      `    ${chunkedIcon} Chunked:       ${result.chunked.resultCount} results` +
        (result.chunked.avgValue ? ` (avg: ${result.chunked.avgValue.toFixed(2)})` : "")
    );
    console.log(
      `    ${fetchingIcon} Fetching:      ${result.fetching.resultCount} results` +
        (result.fetching.avgValue ? ` (avg: ${result.fetching.avgValue.toFixed(2)})` : "")
    );
  }

  private printFinalSummary(totalTime: number): void {
    console.log("\n" + "=".repeat(70));
    console.log("FINAL PATTERN TEST SUMMARY");
    console.log("=".repeat(70));

    console.log(`\nTotal patterns tested: ${this.results.length}`);
    console.log(`Total time: ${(totalTime / 1000 / 60).toFixed(1)} minutes`);

    const approxSuccess = this.results.filter((r) => r.approximation.passed).length;
    const chunkedSuccess = this.results.filter((r) => r.chunked.passed).length;
    const fetchingSuccess = this.results.filter((r) => r.fetching.passed).length;

    console.log(`\nSuccess Rates:`);
    console.log(`  Approximation: ${approxSuccess}/${this.results.length} (${((approxSuccess / this.results.length) * 100).toFixed(0)}%)`);
    console.log(`  Chunked:       ${chunkedSuccess}/${this.results.length} (${((chunkedSuccess / this.results.length) * 100).toFixed(0)}%)`);
    console.log(`  Fetching:      ${fetchingSuccess}/${this.results.length} (${((fetchingSuccess / this.results.length) * 100).toFixed(0)}%)`);

    console.log(`\nPattern-by-Pattern Results:`);
    console.log(`  ${"Pattern".padEnd(20)} | ${"Approx".padEnd(6)} | ${"Chunked".padEnd(7)} | ${"Fetching".padEnd(8)}`);
    console.log(`  ${"-".repeat(20)} | ${"-".repeat(6)} | ${"-".repeat(7)} | ${"-".repeat(8)}`);

    this.results.forEach((result) => {
      const approxStatus = result.approximation.passed ? "PASS" : "FAIL";
      const chunkedStatus = result.chunked.passed ? "PASS" : "FAIL";
      const fetchingStatus = result.fetching.passed ? "PASS" : "FAIL";
      console.log(
        `  ${result.patternName.padEnd(20)} | ${approxStatus.padEnd(6)} | ${chunkedStatus.padEnd(7)} | ${fetchingStatus.padEnd(8)}`
      );
    });

    console.log("\n" + "-".repeat(70));

    const allPassed =
      approxSuccess === this.results.length &&
      chunkedSuccess === this.results.length &&
      fetchingSuccess === this.results.length;

    if (allPassed) {
      console.log("OVERALL: ALL APPROACHES PASSED ALL PATTERNS - READY FOR DEPLOYMENT");
    } else {
      console.log("OVERALL: SOME PATTERNS FAILED - REVIEW NEEDED");
    }

    console.log("=".repeat(70));
  }

  private saveResultsToFile(): void {
    const filename = `pattern-test-results-${new Date().toISOString().replace(/:/g, "-").split(".")[0]}.json`;
    const data = {
      timestamp: new Date().toISOString(),
      patternsCount: this.results.length,
      results: this.results,
      summary: {
        approximationSuccess: this.results.filter((r) => r.approximation.passed).length,
        chunkedSuccess: this.results.filter((r) => r.chunked.passed).length,
        fetchingSuccess: this.results.filter((r) => r.fetching.passed).length,
      },
    };

    fs.writeFileSync(filename, JSON.stringify(data, null, 2));
    console.log(`\nDetailed results saved to: ${filename}`);

    // Also save CSV summary
    const csvFilename = `pattern-test-summary-${new Date().toISOString().replace(/:/g, "-").split(".")[0]}.csv`;
    const csvLines = [
      "pattern,approximation_passed,approximation_count,approximation_avg,chunked_passed,chunked_count,chunked_avg,fetching_passed,fetching_count,fetching_avg",
    ];

    this.results.forEach((r) => {
      csvLines.push(
        [
          r.patternName,
          r.approximation.passed ? "1" : "0",
          r.approximation.resultCount,
          r.approximation.avgValue?.toFixed(2) || "",
          r.chunked.passed ? "1" : "0",
          r.chunked.resultCount,
          r.chunked.avgValue?.toFixed(2) || "",
          r.fetching.passed ? "1" : "0",
          r.fetching.resultCount,
          r.fetching.avgValue?.toFixed(2) || "",
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

// Run the comprehensive pattern test
const tester = new MultiPatternTester();
tester.runAllPatterns().then(() => {
  console.log("\n[INFO] Comprehensive pattern test complete");
  process.exit(0);
}).catch((err) => {
  console.error("\n[ERROR] Pattern test failed:", err);
  process.exit(1);
});
