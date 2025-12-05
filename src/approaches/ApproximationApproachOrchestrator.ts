import { Orchestrator } from "../orchestrator/Orchestrator";
import fs from "fs";
import { CSVLogger } from "../util/logger/CSVLogger";

/**
 * Approximation Approach Orchestrator
 * This orchestrator uses approximation techniques to process streaming queries
 */
export class ApproximationApproachOrchestrator {
  private logger: CSVLogger;
  private orchestrator: Orchestrator;
  private resourceLogStream?: fs.WriteStream;
  private resourceLogInterval?: ReturnType<typeof setInterval>;

  constructor() {
    this.logger = new CSVLogger("approximation_approach_log.csv");
    this.orchestrator = new Orchestrator("ApproximationApproachOperator");
  }

  /**
   * Get the name of this approach
   */
  public getName(): string {
    return "approximation-approach";
  }

  /**
   * Initialize and run the experiment
   */
  public async runExperiment(_dataPath: string, _config: any): Promise<any> {
    console.log(`[ApproximationApproach] Starting experiment`);

    try {
      // Setup subqueries
      await this.setupSubQueries();

      // Register the main query
      await this.registerMainQuery();

      // Start resource usage logging
      this.startResourceUsageLogging();

      // Run the registered query
      const result = await this.orchestrator.runRegisteredQuery();

      console.log(`[ApproximationApproach] Experiment completed`);

      return result;
    } catch (error) {
      console.error(`[ApproximationApproach] Error during experiment:`, error);
      throw error;
    }
  }

  /**
   * Setup subqueries for the approximation approach
   */
  private async setupSubQueries(): Promise<void> {
    const query1 = `
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

    const query2 = `
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

    await this.orchestrator.addSubQuery(query1);
    await this.orchestrator.addSubQuery(query2);
    this.logger.log(
      `Sub-queries added: ${JSON.stringify(this.orchestrator.getSubQueries())}`,
    );
  }

  /**
   * Register the main query for the approximation approach
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
  }

  /**
   * Logs CPU and memory usage to a CSV file at regular intervals.
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
   * Clean up resources
   */
  public cleanup(): void {
    if (this.resourceLogInterval) {
      clearInterval(this.resourceLogInterval);
    }
    if (this.resourceLogStream) {
      this.resourceLogStream.end();
    }
    console.log("[ApproximationApproach] Cleanup completed");
  }
}

/**
 * Standalone execution function (for backward compatibility with direct script execution)
 */
async function runStandaloneApproximationApproach() {
  const orchestrator = new ApproximationApproachOrchestrator();

  try {
    await orchestrator.runExperiment("", {});

    // Add exit logic to ensure the process terminates after processing
    setTimeout(() => {
      console.log("Approximation approach processing completed, exiting...");
      orchestrator.cleanup();
      process.exit(0);
    }, 120000); // 2 minutes timeout
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
