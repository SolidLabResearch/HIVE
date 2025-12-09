import { fork, ChildProcess } from "child_process";
import * as path from "path";
import * as fs from "fs";

/**
 * Represents a worker bee dedicated to executing a specific hive query.
 * Manages the lifecycle of a child process running the query.
 */
export class HiveQueryBee {
  private process: ChildProcess;
  private query: string;

  /**
   * Creates a new HiveQueryBee instance.
   * @param {string} query - The SPARQL/RSPQL query string to execute.
   * @param {string} topic - The MQTT topic for publishing results.
   * @param {string} operator - The operator logic to use.
   * @param {string[]} [subQueries] - Optional array of sub-queries.
   */
  constructor(
    query: string,
    topic: string,
    operator: string,
    subQueries?: string[],
  ) {
    // Try to find BeeWorker - check for .js first (compiled), then .ts (ts-node)
    let beeWorkerPath = path.resolve(__dirname, "BeeWorker.js");

    if (!fs.existsSync(beeWorkerPath)) {
      // Running with ts-node, use .ts file
      beeWorkerPath = path.resolve(__dirname, "BeeWorker.ts");

      if (!fs.existsSync(beeWorkerPath)) {
        throw new Error(`BeeWorker not found at ${beeWorkerPath}`);
      }
    }

    this.query = query;
    this.process = fork(beeWorkerPath, [], {
      env: {
        QUERY: query,
        TOPIC: topic,
        OPERATOR_TYPE: operator,
        SUB_QUERIES: subQueries ? JSON.stringify(subQueries) : undefined,
      },
    });

    this.process.on("message", (msg) => {
      console.log(`Query: ${this.query}`, msg);
    });

    this.process.on("exit", () => {
      console.log(`Query: ${this.query} exited`);
    });
  }

  /**
   * Stops the child process associated with this query bee.
   * @returns {void}
   */
  stop() {
    this.process.kill();
  }
}
