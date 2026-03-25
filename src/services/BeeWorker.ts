import { QueryCombiner } from "hive-thought-rewriter";
import { RSPQLParser, SPeCSWrapper } from "rspql-containment-checker";
import { ExtractedQuery, QueryMap } from "../util/Types";
import { StreamingQueryChunkAggregatorOperator } from "./operators/StreamingQueryChunkAggregatorOperator";
import { ApproximationApproachOperator } from "./operators/RateBasedApproximationApproachOperator";
import { IStreamQueryOperator } from "../util/Interfaces";

/**
 *
 */
export class BeeWorker {

    private query: string;
    private r2s_topic: string;
    private specsWrapper: SPeCSWrapper;
    private rspqlParser: RSPQLParser;
    private operator: IStreamQueryOperator;
    private queryCombiner: QueryCombiner;
    private queryFetchLocation: string;
    /**
     *
     */
    constructor() {
        const operatorType = process.env.OPERATOR_TYPE;
        if (operatorType === "StreamingQueryChunkAggregatorOperator") {
            this.operator = new StreamingQueryChunkAggregatorOperator();
        } else if (operatorType === "ApproximationApproachOperator" || operatorType === "RateBasedApproximationApproachOperator") {
            this.operator = new ApproximationApproachOperator();
        } else {
            throw new Error(`Unsupported operator type: ${operatorType}`);
        }
        this.specsWrapper = new SPeCSWrapper();
        this.rspqlParser = new RSPQLParser();
        this.queryCombiner = new QueryCombiner();
        this.queryFetchLocation = "http://localhost:8080/fetchQueries";
        const query = process.env.QUERY;
        const r2s_topic = process.env.TOPIC;
        const subQueriesStr = process.env.SUB_QUERIES;
        if (!query || !r2s_topic) {
            throw new Error("Missing required environment variables");
        }
        this.query = query;
        this.r2s_topic = r2s_topic;
        
        // Parse subqueries from environment if provided
        console.log(`Raw SUB_QUERIES env var: ${subQueriesStr}`);
        const providedSubQueries = subQueriesStr ? JSON.parse(subQueriesStr) : [];
        
        console.log(`BeeWorker initialized with query: ${this.query} and topic: ${this.r2s_topic}`);
        console.log(`Provided subqueries: ${providedSubQueries.length} queries`);
        console.log(`Provided subqueries content:`, providedSubQueries);
        this.process(providedSubQueries);
    }

    /**
     *
     */
    async process(providedSubQueries: string[] = []) {
        console.log(`process() method is called`);

        // If subqueries are provided from Orchestrator, use them directly
        if (providedSubQueries && providedSubQueries.length > 0) {
            console.log(`Using provided subqueries: ${providedSubQueries.length} queries.`);
            console.log(`Provided subqueries:`, providedSubQueries);
            
            this.operator.addOutputQuery(this.query);
            for (const subQuery of providedSubQueries) {
                console.log(`Adding subquery to operator: ${subQuery}`);
                this.operator.addSubQuery(subQuery);
            }
            console.log(`Total subqueries in operator: ${this.operator.getSubQueries().length}`);
            await this.operator.init();
            this.operator.handleAggregation();
            return;
        }

        // Fallback to existing logic for finding contained queries
        const existingQueries = await this.fetchExistingQueries(this.queryFetchLocation);
        const extractedQueries = await this.extractQueriesWithTopics(JSON.parse(existingQueries) as QueryMap);
        const containedQueries = await this.findContainedQueries(extractedQueries);
        console.log(`Found ${containedQueries.length} contained queries.`);

        if (containedQueries.length > 0) {
            for (const containedQuery of containedQueries) {
                this.queryCombiner.addQuery(containedQuery);
            }
            const combinedQuery = this.queryCombiner.ParsedToString(this.queryCombiner.combine());
            console.log(`Combined query: ${combinedQuery}`);

            const isValid = await this.validateQueryContainment(this.query, combinedQuery);
            console.log(`Is the combined query valid? ${isValid}`);
            if (isValid) {
                this.operator.addOutputQuery(this.query);
                for (const containedQuery of containedQueries) {
                    this.operator.addSubQuery(containedQuery);
                }
                await this.operator.init();
                this.operator.handleAggregation();
            }
            else {
                console.log(`The subqueries are contained but however the combined query cannot be utilized to make the original registered query.`);
            }


        } else {
            console.log("There is no containment relationship between the query registered in the Bee and the existing queries run by the RSP Agents.");

        }

    }

    /**
     *
     * @param extractedQueries
     */
    async findContainedQueries(extractedQueries: ExtractedQuery[]): Promise<string[]> {
        const containedQueries: string[] = [];
        for (const extractedQuery of extractedQueries) {
            let removedAggregationFunctionQuery = this.removeAggregationFunctions(this.query);
            let extractedQueryRspql = this.removeAggregationFunctions(extractedQuery.rspql_query);

            const isContained = await this.checkContainmentWithFlags(extractedQueryRspql, removedAggregationFunctionQuery, true, true);

            if (isContained) {
                console.log(`Query "${extractedQueryRspql}" is contained in the main query.`);
                containedQueries.push(extractedQuery.rspql_query);
            }
            else {
                console.log(`Query "${extractedQuery.rspql_query}" is not contained in the main query.`);
            }
        }
        return containedQueries;
    }

    /**
     * Calls the SPeCS containment checker directly with the -qc and -rename flags.
     * -qc  : explicitly enables query containment mode in SPeCS.
     * -rename : normalises variable names before the check so that structurally
     *           equivalent queries with different projection aliases (e.g. ?avgWearableX
     *           vs ?value) are correctly identified as equivalent.
     * @param subQuery   - The candidate sub-query (RSP-QL string, aggregations already stripped).
     * @param superQuery - The candidate super-query (RSP-QL string, aggregations already stripped).
     * @param rename     - Pass -rename to SPeCS (default true).
     * @param qc         - Pass -qc to SPeCS (default true).
     */
    async checkContainmentWithFlags(
        subQuery: string,
        superQuery: string,
        rename: boolean = true,
        qc: boolean = true,
    ): Promise<boolean> {
        const parsedSub   = this.rspqlParser.parse(subQuery);
        const parsedSuper = this.rspqlParser.parse(superQuery);

        // R2S operator check: IStream/DStream can be contained in RStream, same operator always ok.
        const r2sSub   = parsedSub.r2s.operator;
        const r2sSuper = parsedSuper.r2s.operator;
        const r2sOk = r2sSub === r2sSuper ||
                      ((r2sSub === 'IStream' || r2sSub === 'DStream') && r2sSuper === 'RStream');
        if (!r2sOk) return false;

        if (!parsedSub.sparql || !parsedSuper.sparql) return false;

        const specsOptions: { subquery: string; superquery: string; rename?: string; qc?: string } = {
            subquery:   parsedSub.sparql,
            superquery: parsedSuper.sparql,
        };
        if (rename) specsOptions.rename = 'true';
        if (qc)     specsOptions.qc     = 'true';

        const result = await this.specsWrapper.runSPeCS(specsOptions);
        if (result.exitCode !== 0 || result.containment === null) return false;
        return result.containment;
    }

    /**
     *
     * @param location
     */
    async fetchExistingQueries(location: string): Promise<string> {
        if (!location) {
            throw new Error("Location for fetching queries is not specified");
        }
        const response = await fetch(location, {
            'method': 'GET'
        });

        const queries = await response.text();

        if (!queries) {
            throw new Error("No queries found at the specified location");
        }

        return queries;
    }

    /**
     *
     * @param data
     */
    async extractQueriesWithTopics(data: QueryMap): Promise<ExtractedQuery[]> {
        const extractedQueries: ExtractedQuery[] = [];

        for (const key in data) {
            if (data.hasOwnProperty(key)) {
                const entry = data[key];
                if (entry.rspql_query && entry.r2s_topic) {
                    extractedQueries.push({
                        rspql_query: entry.rspql_query,
                        r2s_topic: entry.r2s_topic
                    });
                }
            }
        }

        return extractedQueries;
    }

    /**
     *
     * @param queryOne
     * @param queryTwo
     */
    async validateQueryContainment(queryOne: string, queryTwo: string): Promise<boolean> {
        // Completeness Check for query containment
        queryOne = this.removeAggregationFunctions(queryOne);
        queryTwo = this.removeAggregationFunctions(queryTwo);
        const isComplete = await this.checkContainmentWithFlags(queryOne, queryTwo);
        // Soundness Check for query containment
        const isSound = await this.checkContainmentWithFlags(queryTwo, queryOne);

        if (isComplete && isSound) {
            console.log(`Query "${queryOne}" is contained in "${queryTwo}" and vice versa.`);
            return true;
        }
        else if (isComplete) {
            console.log(`There is a completeness relationship between "${queryOne}" and "${queryTwo}"`);
            return false;
        }
        else if (isSound) {
            console.log(`There is a soundness relationship between "${queryOne}" and "${queryTwo}"`);
            return false;
        }
        else {
            console.log(`Query "${queryOne}" is not contained in "${queryTwo}" and vice versa.`);
            return false;
        }
    }

    /**
     *
     * @param query
     */
    removeAggregationFunctions(query: string): string {
        return query.replace(/\(\s*\w+\s*\(\s*\?(\w+)\s*\)\s+AS\s+\?\w+\s*\)/g, '?$1');
    }

    /**
     *
     */
    stop() {

    }
}

if (require.main === module) {
    const beeWorker = new BeeWorker();

    process.on("SIGINT", () => {
        beeWorker.stop();
        process.exit(0);
    });

    process.on("SIGTERM", () => {
        beeWorker.stop();
        process.exit(0);
    });
}

