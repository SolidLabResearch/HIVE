import { fork, ChildProcess } from "child_process";
import * as path from "path";

/**
 * 
 */
export class HiveQueryBee {

    private process: ChildProcess;
    private query: string

    /**
     *
     * @param query
     * @param topic
     * @param queryHash
     */
    constructor(query: string, topic: string, operator: string, subQueries?: string[], additionalEnv?: Record<string, string>) {
        const beeWorkerPath = path.resolve(__dirname, "BeeWorker.js");

        this.query = query;
        this.process = fork(beeWorkerPath, [], {
            env: {
                ...process.env,
                ...additionalEnv,
                QUERY: query,
                TOPIC: topic,
                OPERATOR_TYPE: operator,
                HIVE_PROCESS_ROLE:
                    operator === "StreamingQueryChunkAggregatorOperator"
                        ? "chunked_bee_worker"
                        : operator === "ApproximationApproachOperator" ||
                          operator === "RateBasedApproximationApproachOperator"
                        ? "approximation_bee_worker"
                        : "bee_worker",
                SUB_QUERIES: subQueries ? JSON.stringify(subQueries) : undefined
            }
        });

        this.process.on("message", (msg) => {
            console.log(`Query: ${this.query}`, msg);
        });

        this.process.on("exit", () => {
            console.log(`Query: ${this.query} exited`);
        })
    }


    /**
     *
     */
    kill(signal?: NodeJS.Signals) {
        this.process.kill(signal || "SIGTERM");
    }

    /**
     *
     */
    stop() {
        this.process.kill("SIGTERM");
    }
}
