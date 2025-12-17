import { Orchestrator } from "../orchestrator/Orchestrator";
import fs from "fs";
import { CSVLogger } from "../util/logger/CSVLogger";
import {
  createOrchestratorLogger,
  UnifiedLogger,
} from "../util/logger/UnifiedLogger";
import { HealthStatus } from "../util/health/HealthStatus";
import { getTopicTracker } from "../util/topic/TopicTracker";
import * as http from "http";

/**
 * Approximation Approach Orchestrator
 * This orchestrator uses approximation techniques to process streaming queries.
 */
export class ApproximationApproachOrchestrator {
  private logger: CSVLogger;
  private unifiedLogger: UnifiedLogger;
  private healthStatus: HealthStatus;
  private topicTracker: ReturnType<typeof getTopicTracker>;
  private orchestrator: Orchestrator;
  private resourceLogStream?: fs.WriteStream;
  private resourceLogInterval?: ReturnType<typeof setInterval>;
  private healthServer?: http.Server;
  private healthPort: number;

  /**
   * Creates a new ApproximationApproachOrchestrator instance.
   */
  constructor() {
    this.healthPort = process.env.HEALTH_PORT
      ? parseInt(process.env.HEALTH_PORT)
      : 9091;
    this.logger = new CSVLogger("approximation_approach_log.csv");
    this.unifiedLogger = createOrchestratorLogger("approximation");
    this.healthStatus = new HealthStatus("approximation", "orchestrator");
    this.topicTracker = getTopicTracker("approximation");
    this.orchestrator = new Orchestrator("ApproximationApproachOperator");

    // Setup health check endpoint
    this.setupHealthCheckEndpoint();

    this.unifiedLogger.info("Approximation Approach Orchestrator initialized");
  }

  /**
   * Get the name of this approach.
   * @returns {string} The name of the approach.
   */
  public getName(): string {
    return "approximation-approach";
  }

  /**
   * Initialize and run the experiment.
   * @returns {Promise<any>} A promise that resolves with the experiment result.
   */
  public async runExperiment(): Promise<any> {
    console.log(`[ApproximationApproach] Starting experiment`);
    this.unifiedLogger.info("Starting experiment");

    try {
      // Register input topics
      this.topicTracker.registerInputTopic(
        "wearableX",
        "Wearable sensor data stream",
      );
      this.topicTracker.registerInputTopic(
        "smartphoneX",
        "Smartphone sensor data stream",
      );

      // Setup subqueries
      await this.setupSubQueries();

      // Register the main query
      await this.registerMainQuery();

      // Register final result topic
      this.topicTracker.registerResultTopic(
        "approximation/output",
        "Final approximation results",
      );

      // Start resource usage logging
      this.startResourceUsageLogging();

      // Run the registered query (spawns BeeWorker child process)
      this.orchestrator.runRegisteredQuery();

      console.log(
        `[ApproximationApproach] Query execution started, keeping process alive...`,
      );
      this.unifiedLogger.info("Query execution started");

      // Keep the process alive indefinitely - the experiment runner will terminate us
      // This prevents the orchestrator from exiting before BeeWorker can produce results
      return new Promise(() => {
        // This promise never resolves, keeping the process alive
        // The experiment runner script will kill this process when appropriate
      });
    } catch (error) {
      console.error(`[ApproximationApproach] Error during experiment:`, error);
      this.unifiedLogger.error("Experiment failed", { error: String(error) });
      this.healthStatus.recordError(String(error));
      throw error;
    }
  }

  /**
   * Setup subqueries for the approximation approach.
   * @returns {Promise<void>}
   */
  private async setupSubQueries(): Promise<void> {
    const query1 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
    `;

    const query2 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <output> AS
SELECT (MAX(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 60000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
    `;

    await this.orchestrator.addSubQuery(query1);
    await this.orchestrator.addSubQuery(query2);

    // Track subquery topics
    const topic1 = this.topicTracker.registerSubqueryTopic(
      query1,
      "Wearable MAX subquery",
    );
    const topic2 = this.topicTracker.registerSubqueryTopic(
      query2,
      "Smartphone MAX subquery",
    );

    this.logger.log(
      `Sub-queries added: ${JSON.stringify(this.orchestrator.getSubQueries())}`,
    );
    this.unifiedLogger.info("Subqueries registered", {
      count: 2,
      topics: [topic1, topic2],
    });

    // Log topic mappings for debugging
    console.log(`[ApproximationApproach] Subquery Topics:`);
    console.log(`  Wearable: ${topic1}`);
    console.log(`  Smartphone: ${topic2}`);
  }

  /**
   * Register the main query for the approximation approach.
   * @returns {Promise<void>}
   */
  private async registerMainQuery(): Promise<void> {
    const registeredQuery = `
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

    this.orchestrator.registerQuery(registeredQuery);
    this.logger.log(`Registered query: ${registeredQuery}`);

    this.unifiedLogger.logQueryRegistration(
      registeredQuery,
      "approximation/output",
    );
    this.healthStatus.markQueryRegistered();
  }

  /**
   * Logs CPU and memory usage to a CSV file at regular intervals.
   * @param {string} filePath - Path to the log file.
   * @param {number} intervalMs - Interval in milliseconds.
   * @returns {void}
   */
  private startResourceUsageLogging(
    filePath: string = "approximation_approach_resource_usage.csv",
    intervalMs: number = 100,
  ): void {
    const writeHeader = !fs.existsSync(filePath);
    this.resourceLogStream = fs.createWriteStream(filePath, { flags: "a" });

    if (writeHeader) {
      this.resourceLogStream.write(
        "timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n",
      );
    }

    this.resourceLogInterval = setInterval(() => {
      if (!this.resourceLogStream) return;

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
      this.resourceLogStream.write(line);
    }, intervalMs);
  }

  /**
   * Setup health check HTTP endpoint
   * @returns {void}
   */
  private setupHealthCheckEndpoint(): void {
    this.healthServer = http.createServer((req, res) => {
      if (req.url === "/health") {
        const health = this.healthStatus.getHealthCheck();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(health, null, 2));
      } else if (req.url === "/topics") {
        const topicReport = this.topicTracker.generateReport();
        const topicStats = this.topicTracker.getStats();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify(
            {
              report: topicReport,
              stats: topicStats,
              topics: this.topicTracker.getAllTopics(),
            },
            null,
            2,
          ),
        );
      } else if (req.url === "/status") {
        const status = this.healthStatus.getStatusString();
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end(status);
      } else {
        res.writeHead(404);
        res.end("Not Found");
      }
    });

    this.healthServer.listen(this.healthPort, () => {
      console.log(
        `[ApproximationApproach] Health check endpoint: http://localhost:${this.healthPort}/health`,
      );
      console.log(
        `[ApproximationApproach] Topic tracker endpoint: http://localhost:${this.healthPort}/topics`,
      );
      console.log(
        `[ApproximationApproach] Status endpoint: http://localhost:${this.healthPort}/status`,
      );
    });
  }

  /**
   * Gets current health status
   * @returns {object} Health check response
   */
  public getHealth(): any {
    return this.healthStatus.getHealthCheck();
  }

  /**
   * Gets topic tracker report
   * @returns {string} Topic report
   */
  public getTopicReport(): string {
    return this.topicTracker.generateReport();
  }

  /**
   * Clean up resources.
   * @returns {void}
   */
  public cleanup(): void {
    if (this.resourceLogInterval) {
      clearInterval(this.resourceLogInterval);
    }
    if (this.resourceLogStream) {
      this.resourceLogStream.end();
    }
    if (this.healthServer) {
      this.healthServer.close();
    }
    this.unifiedLogger.close();
    console.log("[ApproximationApproach] Cleanup completed");

    // Print final topic report
    console.log(this.topicTracker.generateReport());
  }
}

/**
 * Standalone execution function (for backward compatibility with direct script execution).
 * @returns {Promise<void>}
 */
async function runStandaloneApproximationApproach() {
  const orchestrator = new ApproximationApproachOrchestrator();

  try {
    // Start the experiment (this will keep running)
    orchestrator.runExperiment().catch((error) => {
      console.error("Error in orchestrator:", error);
      orchestrator.cleanup();
      process.exit(1);
    });

    // Set a timeout to exit after processing window completes
    // Query windows: RANGE 120000ms, STEP 60000ms means results at ~60s and ~120s
    // Add buffer for processing
    const experimentDuration = 150000; // 2.5 minutes
    setTimeout(() => {
      console.log("Approximation approach processing completed, exiting...");
      orchestrator.cleanup();
      process.exit(0);
    }, experimentDuration);
  } catch (error) {
    console.error("Error in orchestrator:", error);
    orchestrator.cleanup();
    process.exit(1);
  }
}

// Only run standalone if this file is executed directly
if (require.main === module) {
  runStandaloneApproximationApproach();
}

// Default export for experiment runner
export default ApproximationApproachOrchestrator;
