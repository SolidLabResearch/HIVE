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
    constructor(
        query: string,
        topic: string,
        operator: string,
        subQueries?: string[],
        additionalEnv?: Record<string, string>,
        callbacks?: {
            onMessage?: (message: unknown) => void;
            onExit?: (info: { code: number | null; signal: NodeJS.Signals | null }) => void;
            onError?: (error: Error) => void;
        },
    ) {
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
            callbacks?.onMessage?.(msg);
        });

        this.process.on("error", (error) => {
            callbacks?.onError?.(error);
        });

        this.process.on("exit", (code, signal) => {
            console.log(`Query: ${this.query} exited`);
            callbacks?.onExit?.({ code, signal });
        });
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

    getPid(): number | undefined {
        return this.process.pid;
    }
}
