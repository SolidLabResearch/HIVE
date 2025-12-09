import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";
import * as path from "path";

// --- Experiment Configuration ---

const MQTT_BROKER = "mqtt://localhost:1883";
const DATA_TOPIC_WEARABLE = "wearableX";
const DATA_TOPIC_SMARTPHONE = "smartphoneX";
const APPROX_RESULT_TOPIC = "approximation/output";
const GROUND_TRUTH_RESULT_TOPIC = "client_operation_output";

const EXPERIMENT_DURATION_S = 120; // 2 minutes to match data files
const DATA_FREQUENCY_HZ = 4; // Match existing 4Hz data
const ORCHESTRATOR_INIT_TIME_MS = 10000;
const POST_PROCESSING_TIME_MS = 20000; // Time to collect final results

const PROJECT_ROOT = path.resolve(__dirname, "..");
const DATA_BASE_PATH = "src/streamer/data/pattern_experiments";

interface StreamPattern {
  name: string;
  wearablePath: string;
  smartphonePath: string;
}

const streamPatterns: StreamPattern[] = [
  {
    name: "Low Variability",
    wearablePath: `${DATA_BASE_PATH}/low_variability/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/low_variability/smartphone/data.nt`,
  },
  {
    name: "Step Pattern",
    wearablePath: `${DATA_BASE_PATH}/step_pattern/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/step_pattern/smartphone/data.nt`,
  },
  {
    name: "Spike Pattern",
    wearablePath: `${DATA_BASE_PATH}/spike_pattern/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/spike_pattern/smartphone/data.nt`,
  },
  {
    name: "Low Frequency Oscillation",
    wearablePath: `${DATA_BASE_PATH}/low_frequency_oscillation/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/low_frequency_oscillation/smartphone/data.nt`,
  },
  {
    name: "High Frequency Oscillation",
    wearablePath: `${DATA_BASE_PATH}/high_frequency_oscillation/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/high_frequency_oscillation/smartphone/data.nt`,
  },
  {
    name: "Gradual Drift",
    wearablePath: `${DATA_BASE_PATH}/gradual_drift/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/gradual_drift/smartphone/data.nt`,
  },
  {
    name: "Random Walk",
    wearablePath: `${DATA_BASE_PATH}/random_walk/wearable/data.nt`,
    smartphonePath: `${DATA_BASE_PATH}/random_walk/smartphone/data.nt`,
  },
];

interface ExperimentResult {
  pattern: string;
  mape: number | null;
  approxResults: number[];
  groundTruthResults: number[];
  approxCount: number;
  groundTruthCount: number;
}

// --- Experiment Runner ---

class PatternAccuracyExperiment {
  private mqttClient: mqtt.MqttClient;

  constructor() {
    this.mqttClient = mqtt.connect(MQTT_BROKER);
    this.mqttClient.on("connect", () =>
      console.log("MQTT Client: Connected to broker."),
    );
    this.mqttClient.on("error", (err) =>
      console.error("MQTT Client: Error", err),
    );
  }

  public async runAllPatterns(): Promise<ExperimentResult[]> {
    console.log("--- Starting Stream Pattern Accuracy Experiment ---");
    const allResults: ExperimentResult[] = [];

    for (const pattern of streamPatterns) {
      console.log(`\n--- Running Pattern: ${pattern.name} ---`);
      const result = await this.runSinglePattern(pattern);
      allResults.push(result);
      console.log(`--- Finished Pattern: ${pattern.name} ---`);

      // Wait between patterns to clean up
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    this.mqttClient.end();
    console.log("\n--- All Patterns Executed ---");
    this.printSummary(allResults);
    return allResults;
  }

  private async runSinglePattern(
    pattern: StreamPattern,
  ): Promise<ExperimentResult> {
    const approxResults: number[] = [];
    const groundTruthResults: number[] = [];

    // Subscribe to result topics
    this.mqttClient.subscribe(APPROX_RESULT_TOPIC);
    this.mqttClient.subscribe(GROUND_TRUTH_RESULT_TOPIC);

    const messageHandler = (topic: string, message: Buffer) => {
      try {
        const messageStr = message.toString();

        if (topic === APPROX_RESULT_TOPIC) {
          // Approximation output is a JSON object with a 'unifiedResult' field
          try {
            const parsed = JSON.parse(messageStr);
            if (parsed && typeof parsed.unifiedResult === "number") {
              const value = parsed.unifiedResult;
              console.log(`Result (Approx): ${value}`);
              approxResults.push(value);
            }
          } catch (e) {
            console.error("Error parsing approx result JSON:", e);
          }
        } else if (topic === GROUND_TRUTH_RESULT_TOPIC) {
          // Ground Truth output is a JSON-stringified RDF string
          // e.g. " ... hasValue \"10.5\"^^ ... "
          let rdfString = messageStr;
          try {
            // Attempt to unwrap JSON string if it is one
            rdfString = JSON.parse(messageStr);
          } catch (e) {
            // It might be raw string already
          }

          if (typeof rdfString === "string") {
            const valueMatch = rdfString.match(/"([0-9.+-eE]+)"\^\^/);
            if (valueMatch && valueMatch[1]) {
              const value = parseFloat(valueMatch[1]);
              if (!isNaN(value)) {
                console.log(`Result (Ground Truth): ${value}`);
                groundTruthResults.push(value);
              }
            }
          }
        }
      } catch (e) {
        console.error("Error parsing result message:", e);
      }
    };
    this.mqttClient.on("message", messageHandler);

    // Launch orchestrators with unique ports
    console.log("Launching orchestrators...");
    const approxOrchestrator = this.spawnOrchestrator(
      "src/approaches/ApproximationApproachOrchestrator.ts",
      "ApproxOrch",
      { HTTP_PORT: "8081" }, // Use port 8081 to avoid conflicts
    );
    const fetchingOrchestrator = this.spawnOrchestrator(
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
      "FetchingOrch",
      {},
    );

    console.log(
      `Waiting ${ORCHESTRATOR_INIT_TIME_MS / 1000}s for orchestrators to initialize...`,
    );
    await new Promise((resolve) =>
      setTimeout(resolve, ORCHESTRATOR_INIT_TIME_MS),
    );

    // Start data publishers using existing infrastructure
    console.log("Starting data publishers...");
    const publisherProcesses = await this.startDataPublishers(
      pattern.wearablePath,
      pattern.smartphonePath,
    );

    console.log(`Data will stream for ${EXPERIMENT_DURATION_S}s...`);
    await new Promise((resolve) =>
      setTimeout(resolve, EXPERIMENT_DURATION_S * 1000),
    );

    console.log("Stopping data publishers...");
    publisherProcesses.forEach((proc) => proc.kill());

    console.log(
      `Waiting ${POST_PROCESSING_TIME_MS / 1000}s for processing to complete...`,
    );
    await new Promise((resolve) =>
      setTimeout(resolve, POST_PROCESSING_TIME_MS),
    );

    // Cleanup
    console.log("Cleaning up orchestrators...");
    approxOrchestrator.kill("SIGTERM");
    fetchingOrchestrator.kill("SIGTERM");

    // Give them time to cleanup
    await new Promise((resolve) => setTimeout(resolve, 2000));

    this.mqttClient.removeListener("message", messageHandler);

    // Calculate accuracy
    const mape = this.calculateMape(approxResults, groundTruthResults);

    console.log(`\nResults Summary for ${pattern.name}:`);
    console.log(`  Approx Results Count: ${approxResults.length}`);
    console.log(`  Ground Truth Results Count: ${groundTruthResults.length}`);
    console.log(`  MAPE: ${mape !== null ? mape.toFixed(2) + "%" : "N/A"}`);

    return {
      pattern: pattern.name,
      mape,
      approxResults,
      groundTruthResults,
      approxCount: approxResults.length,
      groundTruthCount: groundTruthResults.length,
    };
  }

  private spawnOrchestrator(
    scriptPath: string,
    name: string,
    env: { [key: string]: string } = {},
  ): ChildProcess {
    const child = spawn("npx", ["ts-node", scriptPath], {
      env: { ...process.env, ...env },
      cwd: PROJECT_ROOT,
    });

    child.stdout?.on("data", (data) => {
      const output = data.toString();
      // Filter out excessive debug logs for cleaner output
      if (
        !output.includes("Watermark is not increasing") &&
        !output.includes("DEBUG: Adding quad")
      ) {
        process.stdout.write(`[${name}]: ${output}`);
      }
    });

    child.stderr?.on("data", (data) => {
      const output = data.toString();
      // Filter out known non-critical errors
      if (
        !output.includes("Watermark is not increasing") &&
        !output.includes("EADDRINUSE")
      ) {
        process.stderr.write(`[${name} ERROR]: ${output}`);
      }
    });

    child.on("error", (err) => {
      console.error(`[${name}] Process error:`, err);
    });

    return child;
  }

  private async startDataPublishers(
    wearablePath: string,
    smartphonePath: string,
  ): Promise<ChildProcess[]> {
    const processes: ChildProcess[] = [];

    // Ensure we're using the correct paths
    const wearableFullPath = path.resolve(PROJECT_ROOT, wearablePath);
    const smartphoneFullPath = path.resolve(PROJECT_ROOT, smartphonePath);

    console.log(`  Wearable data: ${wearableFullPath}`);
    console.log(`  Smartphone data: ${smartphoneFullPath}`);

    // Start wearable publisher
    const wearablePublisher = spawn(
      "node",
      [
        "dist/src/streamer/src/experiment-publisher.js",
        wearableFullPath,
        DATA_TOPIC_WEARABLE,
        DATA_FREQUENCY_HZ.toString(),
      ],
      {
        cwd: PROJECT_ROOT,
        stdio: "pipe",
      },
    );

    wearablePublisher.stdout?.on("data", (data) => {
      const msg = data.toString().trim();
      if (!msg.includes("Published observation")) {
        console.log(`[Wearable Publisher]: ${msg}`);
      }
    });

    wearablePublisher.stderr?.on("data", (data) => {
      console.error(`[Wearable Publisher ERROR]: ${data.toString().trim()}`);
    });

    processes.push(wearablePublisher);

    // Start smartphone publisher
    const smartphonePublisher = spawn(
      "node",
      [
        "dist/src/streamer/src/experiment-publisher.js",
        smartphoneFullPath,
        DATA_TOPIC_SMARTPHONE,
        DATA_FREQUENCY_HZ.toString(),
      ],
      {
        cwd: PROJECT_ROOT,
        stdio: "pipe",
      },
    );

    smartphonePublisher.stdout?.on("data", (data) => {
      const msg = data.toString().trim();
      if (!msg.includes("Published observation")) {
        console.log(`[Smartphone Publisher]: ${msg}`);
      }
    });

    smartphonePublisher.stderr?.on("data", (data) => {
      console.error(`[Smartphone Publisher ERROR]: ${data.toString().trim()}`);
    });

    processes.push(smartphonePublisher);

    // Give publishers time to initialize
    await new Promise((resolve) => setTimeout(resolve, 2000));

    return processes;
  }

  private calculateMape(approx: number[], truth: number[]): number | null {
    if (approx.length === 0 || truth.length === 0) {
      console.log(
        "Cannot calculate MAPE: insufficient results from one or both approaches",
      );
      return null;
    }

    const minLength = Math.min(approx.length, truth.length);
    if (minLength === 0) return null;

    let errorSum = 0;
    let validPoints = 0;

    for (let i = 0; i < minLength; i++) {
      if (truth[i] !== 0) {
        errorSum += Math.abs((truth[i] - approx[i]) / truth[i]);
        validPoints++;
      }
    }

    if (validPoints === 0) return null;

    return (errorSum / validPoints) * 100;
  }

  private printSummary(results: ExperimentResult[]) {
    console.log("\n--- Experiment Summary ---");
    console.log("Pattern\t\t\t\t| Approx | Ground | MAPE (%)");
    console.log(
      "--------------------------------|--------|--------|----------",
    );
    results.forEach((r) => {
      const mapeStr = r.mape !== null ? r.mape.toFixed(2) : "N/A";
      console.log(
        `${r.pattern.padEnd(30)}\t| ${r.approxCount.toString().padEnd(6)} | ${r.groundTruthCount.toString().padEnd(6)} | ${mapeStr}`,
      );
    });
  }
}

// --- Main Execution ---

if (require.main === module) {
  const experiment = new PatternAccuracyExperiment();
  experiment.runAllPatterns().catch(console.error);
}
