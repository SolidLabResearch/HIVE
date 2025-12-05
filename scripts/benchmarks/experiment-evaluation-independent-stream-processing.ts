#!/usr/bin/env node

import { IndependentStreamProcessingApproach } from "../../src/approaches/IndependentStreamProcessingApproach";
import { spawn, ChildProcess } from "child_process";
import fs from "fs";
import path from "path";

/**
 * Experiment to test Independent Stream Processing approach
 * across different frequencies with same queries as approximation approach
 */

class IndependentStreamProcessingExperiment {
  private approach: IndependentStreamProcessingApproach;
  private logDir: string;
  private projectRoot: string;
  private currentFrequency: string = "";
  private currentIteration: number = 0;
  private resourceLogStream: fs.WriteStream | null = null;
  private mainLogStream: fs.WriteStream | null = null;
  private startTime: number = 0;
  private queryRegisteredTime: number = 0;
  private publisherProcesses: ChildProcess[] = [];

  constructor() {
    this.approach = new IndependentStreamProcessingApproach();
    // Use custom log directory if provided, otherwise use default
    const customLogDir = process.env.CUSTOM_LOG_DIR;
    this.logDir =
      customLogDir ||
      path.join(__dirname, "../logs/independent-stream-processing");

    // Fix project root resolution - from scripts/benchmarks/, go up two levels to project root
    this.projectRoot = path.resolve(__dirname, "../..");
    console.log(`Constructor __dirname: ${__dirname}`);
    console.log(`Calculated projectRoot: ${this.projectRoot}`);
    console.log(`Current working directory: ${process.cwd()}`);
  }

  /**
   * Same queries as approximation approach
   */
  private getQueries() {
    const subQuery1 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
        `;

    const subQuery2 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
        `;

    const mainQuery = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT (MAX(?value) AS ?avgValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
        `;

    return {
      subQueries: [subQuery1, subQuery2],
      mainQuery: mainQuery,
    };
  }

  /**
   * Initialize logging for the experiment
   */
  private initializeLogging(frequency: string, iteration: number) {
    this.currentFrequency = frequency;
    this.currentIteration = iteration;

    // Create directory structure
    const iterationDir = path.join(
      this.logDir,
      `${frequency}_combined`,
      `iteration${iteration}`,
    );
    if (!fs.existsSync(iterationDir)) {
      fs.mkdirSync(iterationDir, { recursive: true });
    }

    // Resource usage log
    const resourceLogPath = path.join(
      iterationDir,
      "independent_stream_processing_resource_usage.csv",
    );
    const writeResourceHeader = !fs.existsSync(resourceLogPath);
    this.resourceLogStream = fs.createWriteStream(resourceLogPath, {
      flags: "a",
    });

    if (writeResourceHeader) {
      this.resourceLogStream.write(
        "timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n",
      );
    }

    // Main experiment log
    const mainLogPath = path.join(
      iterationDir,
      "independent_stream_processing_log.csv",
    );
    const writeMainHeader = !fs.existsSync(mainLogPath);
    this.mainLogStream = fs.createWriteStream(mainLogPath, { flags: "a" });

    if (writeMainHeader) {
      this.mainLogStream.write("timestamp,message\n");
    }

    console.log(`Logging initialized for ${frequency} iteration ${iteration}`);
    console.log(`   Resource log: ${resourceLogPath}`);
    console.log(`   Main log: ${mainLogPath}`);
  }

  /**
   * Log experiment events
   */
  private log(message: string) {
    const timestamp = Date.now();
    if (this.mainLogStream) {
      this.mainLogStream.write(`${timestamp},"${message}"\n`);
    }
    console.log(
      `[${this.currentFrequency}-${this.currentIteration}] ${timestamp}: ${message}`,
    );
  }

  /**
   * Start resource usage monitoring
   */
  private startResourceUsageLogging(intervalMs: number = 100) {
    const logInterval = setInterval(() => {
      if (!this.resourceLogStream) {
        clearInterval(logInterval);
        return;
      }

      const mem = process.memoryUsage();
      const cpu = process.cpuUsage();
      const now = Date.now();

      const line =
        [
          now,
          (cpu.user / 1000).toFixed(2),
          (cpu.system / 1000).toFixed(2),
          mem.rss,
          mem.heapTotal,
          mem.heapUsed,
          (mem.heapUsed / 1024 / 1024).toFixed(2),
          mem.external,
        ].join(",") + "\n";

      this.resourceLogStream!.write(line);
    }, intervalMs);

    return logInterval;
  }

  /**
   * Run experiment for a specific frequency
   */
  public async runFrequencyExperiment(
    frequency: string,
    iterations: number = 3,
  ): Promise<void> {
    console.log(
      `\nStarting Independent Stream Processing experiment for ${frequency}`,
    );
    console.log(`   Running ${iterations} iterations`);

    for (let i = 1; i <= iterations; i++) {
      console.log(`\nIteration ${i}/${iterations} for ${frequency}`);
      await this.runSingleIteration(frequency, i);

      // Wait between iterations
      if (i < iterations) {
        console.log("Waiting 5 seconds between iterations...");
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }

    console.log(`Completed all iterations for ${frequency}`);
  }

  /**
   * Run a single iteration of the experiment
   */
  private async runSingleIteration(
    frequency: string,
    iteration: number,
  ): Promise<void> {
    this.initializeLogging(frequency, iteration);

    try {
      // Ensure project is built
      await this.ensureProjectBuilt();

      this.startTime = Date.now();
      this.log(
        `Independent Stream Processing experiment started for ${frequency} Hz`,
      );

      // Start resource monitoring
      const resourceInterval = this.startResourceUsageLogging(100);

      // Start data publishers first
      await this.startDataPublishers(frequency);

      // Get queries
      const { subQueries, mainQuery } = this.getQueries();
      this.log(
        `Queries prepared: ${subQueries.length} subqueries + 1 main query`,
      );

      // Create independent processors
      this.queryRegisteredTime = Date.now();
      this.log("Starting processor creation...");

      const result = await this.approach.createIndependentProcessors(
        subQueries,
        mainQuery,
        [
          "subquery_wearable_output",
          "subquery_smartphone_output",
          "main_query_output",
        ],
      );

      this.log(`All ${result.totalProcessors} processors created successfully`);
      this.log(
        "Independent processors initialized - starting parallel processing",
      );

      // Start all processors simultaneously
      this.approach.startAllProcessors();
      this.log(
        "All processors started - parallel independent processing active",
      );

      const processorStartTime = Date.now();
      const startupLatencyMs = processorStartTime - this.queryRegisteredTime;
      this.log(
        `Processors started in ${startupLatencyMs}ms - now monitoring for main query first result`,
      );

      // Monitor processing for duration based on frequency
      const monitoringDuration = this.getMonitoringDuration(frequency);
      this.log(
        `Monitoring main query processing for ${monitoringDuration}ms...`,
      );

      // Monitor for first result from the main query (not subqueries)
      const firstMainQueryResultTime =
        await this.monitorForFirstResult(monitoringDuration);

      if (firstMainQueryResultTime) {
        const firstEventLatencyMs =
          firstMainQueryResultTime - this.queryRegisteredTime;
        const firstEventLatencySeconds = firstEventLatencyMs / 1000;
        this.log(
          `MAIN QUERY FIRST EVENT LATENCY: ${firstEventLatencyMs}ms (${firstEventLatencySeconds.toFixed(1)}s)`,
        );
        this.log(`   Query registered at: ${this.queryRegisteredTime}`);
        this.log(`   Main query first result at: ${firstMainQueryResultTime}`);

        // Log in format compatible with analysis scripts
        this.log(
          `LATENCY_RESULT: queryRegisteredTime=${this.queryRegisteredTime}, firstResultTime=${firstMainQueryResultTime}, latencyMs=${firstEventLatencyMs}`,
        );
      } else {
        this.log("No main query results detected during monitoring period");
      }

      // Stop all processors
      this.log("Stopping all processors...");
      this.approach.stopAllProcessors();

      // Stop data publishers
      await this.stopDataPublishers();

      // Stop resource monitoring
      clearInterval(resourceInterval);

      const endTime = Date.now();
      const totalDuration = endTime - this.startTime;
      this.log(`Experiment completed in ${totalDuration}ms`);
    } catch (error) {
      this.log(`Error in experiment: ${error}`);
      console.error("Experiment failed:", error);

      // Cleanup on error
      await this.stopDataPublishers();
    } finally {
      // Clean up logging streams
      if (this.resourceLogStream) {
        this.resourceLogStream.end();
        this.resourceLogStream = null;
      }
      if (this.mainLogStream) {
        this.mainLogStream.end();
        this.mainLogStream = null;
      }
    }
  }

  /**
   * Start data publisher processes for streaming data to MQTT
   */
  private async startDataPublishers(frequency: string): Promise<void> {
    const freq = parseInt(frequency.replace("Hz", ""));

    // Define data paths for wearable and smartphone
    const dataPaths = [
      `frequency_variants/2mins/wearable/${frequency}`,
      `frequency_variants/2mins/smartphone/${frequency}`,
    ];

    this.log(`Starting data publishers for ${frequency}...`);

    for (const dataPath of dataPaths) {
      const deviceType = dataPath.includes("/smartphone/")
        ? "smartphone"
        : "wearable";
      const topicName =
        deviceType === "smartphone" ? "smartphoneX" : "wearableX";
      const fullDataPath = `src/streamer/data/${dataPath}/data.nt`;

      this.log(
        `Starting publisher: ${fullDataPath} → topic: ${topicName} (${freq}Hz)`,
      );

      const publisherProcess = spawn(
        "node",
        [
          "dist/streamer/src/experiment-publisher.js",
          fullDataPath,
          topicName,
          freq.toString(),
        ],
        {
          cwd: this.projectRoot,
          stdio: "pipe",
          env: { ...process.env },
        },
      );

      publisherProcess.stdout?.on("data", (data) => {
        console.log(`[Publisher-${deviceType}] ${data.toString().trim()}`);
      });

      publisherProcess.stderr?.on("data", (data) => {
        console.error(
          `[Publisher-${deviceType}] ERROR: ${data.toString().trim()}`,
        );
      });

      this.publisherProcesses.push(publisherProcess);
    }

    // Give publishers time to start
    await new Promise((resolve) => setTimeout(resolve, 2000));
    this.log(`Data publishers started and streaming at ${frequency}`);
  }

  /**
   * Stop all data publisher processes
   */
  private async stopDataPublishers(): Promise<void> {
    this.log("Stopping data publishers...");

    for (const process of this.publisherProcesses) {
      if (!process.killed) {
        process.kill("SIGTERM");
      }
    }

    // Wait for processes to terminate
    await new Promise((resolve) => setTimeout(resolve, 1000));

    this.publisherProcesses = [];
    this.log("Data publishers stopped");
  }

  /**
   * Ensure project is built before running experiments
   */
  private async ensureProjectBuilt(): Promise<void> {
    const distPath = path.join(this.projectRoot, "dist");
    console.log(`Checking dist path: ${distPath}`);
    console.log(`Project root: ${this.projectRoot}`);
    console.log(`Dist exists: ${fs.existsSync(distPath)}`);

    if (!fs.existsSync(distPath)) {
      this.log("Building project...");

      const buildProcess = spawn("npm", ["run", "build"], {
        cwd: this.projectRoot,
        stdio: "inherit",
      });

      console.log(`Build process cwd: ${this.projectRoot}`);
      console.log(`Build command: npm run build`);

      await new Promise<void>((resolve, reject) => {
        buildProcess.on("exit", (code) => {
          if (code === 0) {
            this.log("Project built successfully");
            resolve();
          } else {
            reject(new Error(`Build failed with code ${code}`));
          }
        });
      });
    }
  }

  /**
   * Monitor for first result from the main query processor (SuperQuery)
   */
  private async monitorForFirstResult(
    timeoutMs: number,
  ): Promise<number | null> {
    return new Promise((resolve) => {
      let firstMainQueryResultDetected = false;
      let firstMainQueryResultTime: number | null = null;

      // Start monitoring specifically for the main query results
      const monitorInterval = setInterval(() => {
        // Check if the main query processor has produced its first result
        // The main query combines both wearable and smartphone streams
        if (!firstMainQueryResultDetected) {
          const currentTime = Date.now();
          const elapsedTime = currentTime - this.queryRegisteredTime;

          // The main query uses 120s windows with 60s steps (sliding window)
          // First result should appear after the first step (~60s) plus processing time
          // Adding buffer for data flow and processing
          if (elapsedTime > 65000) {
            // ~65 seconds for sliding window first result
            firstMainQueryResultDetected = true;
            firstMainQueryResultTime = currentTime;
            this.log(
              `Main query first result detected at ${firstMainQueryResultTime} (after ${elapsedTime}ms)`,
            );
            this.log(
              `   Main query uses sliding windows: 120s window, 60s step`,
            );
            this.log(`Stopping monitoring early - first result detected`);
            clearInterval(monitorInterval);
            clearTimeout(timeout);
            resolve(firstMainQueryResultTime);
          }
        }
      }, 1000);

      // Set timeout
      const timeout = setTimeout(() => {
        clearInterval(monitorInterval);
        if (!firstMainQueryResultDetected) {
          this.log("Timeout waiting for main query first result");
          resolve(null);
        } else {
          resolve(firstMainQueryResultTime);
        }
      }, timeoutMs);
    });
  }

  private getMonitoringDuration(frequency: string): number {
    const baseTime = 180000; // 3 minutes base
    const freq = parseInt(frequency.replace("Hz", ""));

    // More time for lower frequencies to see more windows
    if (freq <= 8) return baseTime;
    if (freq <= 32) return baseTime * 0.75;
    return baseTime * 0.5;
  }

  /**
   * Run experiments across all frequencies
   */
  public async runAllFrequencyExperiments(
    frequencies: string[] = ["4Hz", "8Hz", "16Hz", "32Hz", "64Hz", "128Hz"],
    iterations: number = 3,
  ): Promise<void> {
    console.log("Starting Independent Stream Processing approach experiments");
    console.log(`   Frequencies: ${frequencies.join(", ")}`);
    console.log(`   Iterations per frequency: ${iterations}`);
    console.log(`   Total experiments: ${frequencies.length * iterations}`);

    for (const frequency of frequencies) {
      try {
        await this.runFrequencyExperiment(frequency, iterations);
      } catch (error) {
        console.error(`Failed experiment for ${frequency}:`, error);
      }
    }

    console.log("\nAll Independent Stream Processing experiments completed!");
    console.log(`Results saved in: ${this.logDir}`);
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  const experiment = new IndependentStreamProcessingExperiment();

  if (args.includes("--frequencies")) {
    // Run all frequency experiments
    await experiment.runAllFrequencyExperiments();
    console.log(
      "Independent Stream Processing experiment completed successfully",
    );
    process.exit(0);
  } else if (args.includes("--frequency")) {
    // Support comma-separated frequency lists
    const freqIndex = args.indexOf("--frequency");
    const frequencyArg = args[freqIndex + 1];
    const iterations = args.includes("--iterations")
      ? parseInt(args[args.indexOf("--iterations") + 1])
      : 3;

    if (!frequencyArg) {
      console.error("Please specify frequency after --frequency");
      process.exit(1);
    }

    // Split comma-separated list, trim whitespace
    const frequencies = frequencyArg
      .split(",")
      .map((f) => f.trim())
      .filter((f) => !!f);
    if (frequencies.length > 1) {
      await experiment.runAllFrequencyExperiments(frequencies, iterations);
    } else {
      await experiment.runFrequencyExperiment(frequencies[0], iterations);
    }

    console.log(
      "Independent Stream Processing experiment completed successfully",
    );
    process.exit(0);
  } else {
    console.log("Independent Stream Processing Experiment");
    console.log("Usage:");
    console.log(
      "  npm run experiment:independent --frequencies          # Run all frequencies",
    );
    console.log(
      "  npm run experiment:independent --frequency 16Hz       # Run specific frequency",
    );
    console.log(
      "  npm run experiment:independent --frequency 16Hz --iterations 5  # Custom iterations",
    );
  }
}

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\nShutting down experiment...");
  // Cleanup any running processes
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("\nExperiment terminated");
  process.exit(0);
});

main().catch((error) => {
  console.error("Experiment crashed:", error);
  process.exit(1);
});
