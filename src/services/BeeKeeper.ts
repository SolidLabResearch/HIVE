import { hash_string_md5 } from "../util/Util";
import { HiveQueryBee } from "./HiveQueryBee";

/**
 * BeeKeeper class to manage the lifecycle of Bee workers
 * for executing queries in a distributed manner.
 * It is responsible for starting and stopping the workers
 * based on the queries received.
 * Each worker is responsible for executing a specific query
 * and publishing the results to a specified topic.
 * The workers are identified by a unique hash generated
 * from the query string. Each Worker Bee is a seperate NodeJS process 
 * that runs in parallel to the main process.
 */
export class BeeKeeper {
    private bees: Map<string, any>
    /**
     * Creates a new BeeKeeper instance.
     */
    constructor() {
        // Initialize the BeeKeeper with an empty map of bees
        this.bees = new Map();
    }

    /**
     * Execute a query using the HiveQueryBee worker.
     * A HiveQueryBee worker is spawned for the query and the results
     * are published to the specified topic.
     * @param {string} query - The query string to be executed.
     * @param {string} r2s_topic - The topic to which the results will be published.
     * @param {string} operator - The operator logic to apply to the query.
     * @param {string[]} [subQueries] - Optional array of sub-query strings.
     * @returns {void} - No return value.
     */
    public executeQuery(query: string, r2s_topic: string, operator: string, subQueries?: string[]) {
        const query_hash = hash_string_md5(query);
        const worker = new HiveQueryBee(query, r2s_topic, operator, subQueries);
        this.bees.set(query_hash, worker);
    }    /**
          * Stop a query by terminating the corresponding HiveQueryBee worker.
          * The worker is identified by the unique hash generated
          * from the query string. If the worker is found, it is stopped
          * and removed from the bees map.
          * @param {string} query - The query string whose worker needs to be stopped.
          * @returns {void} - No return value.
          * @throws {Error} - Throws an error if the worker is not found
          *                  for the given query string.
          */
    stopQuery(query: string) {
        const query_hash = hash_string_md5(query);
        const worker = this.bees.get(query_hash);
        if (worker) {
            worker.stop();
            this.bees.delete(query_hash);
        } else {
            throw new Error("Worker not found for the given query");

        }
    }
}