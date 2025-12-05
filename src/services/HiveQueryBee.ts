import { fork, ChildProcess } from "child_process";
import * as path from "path";
import * as fs from "fs";

/**
 *
 */
export class HiveQueryBee {
  private process: ChildProcess;
  private query: string;

  /**
   *
   * @param query
   * @param topic
   * @param queryHash
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
   *
   */
  stop() {
    this.process.kill();
  }
}
