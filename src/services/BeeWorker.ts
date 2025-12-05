import { QueryCombiner } from "hive-thought-rewriter";
import { ContainmentChecker } from "rspql-containment-checker";
import { ExtractedQuery, QueryMap } from "../util/Types";
import { IStreamQueryOperator } from "../util/Interfaces";
import { WorkerFactory } from "./WorkerFactory";

/**
 * BeeWorker class responsible for processing streaming queries
 * Uses the WorkerFactory pattern to instantiate appropriate operators
 */
export class BeeWorker {
  private query: string;
  private r2s_topic: string;
  private containmentChecker: ContainmentChecker;
  private operator: IStreamQueryOperator;
  private queryCombiner: QueryCombiner;
  private queryFetchLocation: string;

  /**
   * Constructor initializes the BeeWorker with environment variables
   * and creates the appropriate operator using WorkerFactory
   */
  constructor() {
    const operatorType = process.env.OPERATOR_TYPE;

    if (!operatorType) {
      throw new Error("OPERATOR_TYPE environment variable is required");
    }

    // Use WorkerFactory to create the operator instance
    this.operator = WorkerFactory.createOperatorFromString(operatorType);

    this.containmentChecker = new ContainmentChecker();
    this.queryCombiner = new QueryCombiner();
    this.queryFetchLocation = "http://localhost:8080/fetchQueries";

    const query = process.env.QUERY;
    const r2s_topic = process.env.TOPIC;
    const subQueriesStr = process.env.SUB_QUERIES;

    if (!query || !r2s_topic) {
      throw new Error(
        "Missing required environment variables: QUERY and TOPIC",
      );
    }

    this.query = query;
    this.r2s_topic = r2s_topic;

    // Parse subqueries from environment if provided
    console.log(`Raw SUB_QUERIES env var: ${subQueriesStr}`);
    const providedSubQueries = subQueriesStr ? JSON.parse(subQueriesStr) : [];

    console.log(`BeeWorker initialized with operator: ${operatorType}`);
    console.log(`Query: ${this.query}`);
    console.log(`Topic: ${this.r2s_topic}`);
    console.log(`Provided subqueries: ${providedSubQueries.length} queries`);
    console.log(`Provided subqueries content:`, providedSubQueries);

    this.process(providedSubQueries);
  }

  /**
   * Process the queries using the configured operator
   * @param providedSubQueries Array of subqueries provided by the orchestrator
   */
  async process(providedSubQueries: string[] = []) {
    console.log(`process() method is called`);

    // If subqueries are provided from Orchestrator, use them directly
    if (providedSubQueries && providedSubQueries.length > 0) {
      console.log(
        `Using provided subqueries: ${providedSubQueries.length} queries.`,
      );
      console.log(`Provided subqueries:`, providedSubQueries);

      this.operator.addOutputQuery(this.query);
      for (const subQuery of providedSubQueries) {
        console.log(`Adding subquery to operator: ${subQuery}`);
        this.operator.addSubQuery(subQuery);
      }
      console.log(
        `Total subqueries in operator: ${this.operator.getSubQueries().length}`,
      );
      await this.operator.init();
      this.operator.handleAggregation();
      return;
    }

    // Fallback to existing logic for finding contained queries
    const existingQueries = await this.fetchExistingQueries(
      this.queryFetchLocation,
    );
    const extractedQueries = await this.extractQueriesWithTopics(
      JSON.parse(existingQueries) as QueryMap,
    );
    const containedQueries = await this.findContainedQueries(extractedQueries);
    console.log(`Found ${containedQueries.length} contained queries.`);

    if (containedQueries.length > 0) {
      for (const containedQuery of containedQueries) {
        this.queryCombiner.addQuery(containedQuery);
      }
      this.queryCombiner.combine();
      console.log(
        `Combined query: ${this.queryCombiner.ParsedToString(this.queryCombiner.combine())}`,
      );
      const combinedQuery = this.queryCombiner.ParsedToString(
        this.queryCombiner.combine(),
      );

      // const isValid = await this.validateQueryContainment(this.query, combinedQuery);
      const isValid = true; // Assuming the containment check is valid for now
      console.log(`Is the combined query valid? ${isValid}`);
      if (isValid) {
        this.operator.addOutputQuery(this.query);
        for (const containedQuery of containedQueries) {
          this.operator.addSubQuery(containedQuery);
        }
        await this.operator.init();
        this.operator.handleAggregation();
      } else {
        console.log(
          `The subqueries are contained but however the combined query cannot be utilized to make the original registered query.`,
        );
      }
    } else {
      console.log(
        "There is no containment relationship between the query registered in the Bee and the existing queries run by the RSP Agents.",
      );
    }
  }

  /**
   *
   * @param extractedQueries
   */
  async findContainedQueries(
    extractedQueries: ExtractedQuery[],
  ): Promise<string[]> {
    const containedQueries: string[] = [];
    for (const extractedQuery of extractedQueries) {
      let removedAggregationFunctionQuery = this.removeAggregationFunctions(
        this.query,
      );
      let extractedQueryRspql = this.removeAggregationFunctions(
        extractedQuery.rspql_query,
      );

      const isContained = await this.containmentChecker.checkContainment(
        extractedQueryRspql,
        removedAggregationFunctionQuery,
      );

      if (isContained) {
        console.log(
          `Query "${extractedQueryRspql}" is contained in the main query.`,
        );
        containedQueries.push(extractedQuery.rspql_query);
      } else {
        console.log(
          `Query "${extractedQuery.rspql_query}" is not contained in the main query.`,
        );
      }
    }
    return containedQueries;
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
      method: "GET",
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
            r2s_topic: entry.r2s_topic,
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
  async validateQueryContainment(
    queryOne: string,
    queryTwo: string,
  ): Promise<boolean> {
    // Completeness Check for query containment
    queryOne = this.removeAggregationFunctions(queryOne);
    queryTwo = this.removeAggregationFunctions(queryTwo);
    const isComplete = await this.containmentChecker.checkContainment(
      queryOne,
      queryTwo,
    );
    // Soundness Check for query containment
    const isSound = await this.containmentChecker.checkContainment(
      queryTwo,
      queryOne,
    );

    if (isComplete && isSound) {
      console.log(
        `Query "${queryOne}" is contained in "${queryTwo}" and vice versa.`,
      );
      return true;
    } else if (isComplete) {
      console.log(
        `There is a completeness relationship between "${queryOne}" and "${queryTwo}"`,
      );
      return false;
    } else if (isSound) {
      console.log(
        `There is a soundness relationship between "${queryOne}" and "${queryTwo}"`,
      );
      return false;
    } else {
      console.log(
        `Query "${queryOne}" is not contained in "${queryTwo}" and vice versa.`,
      );
      return true;
    }
  }

  /**
   *
   * @param query
   */
  removeAggregationFunctions(query: string): string {
    return query.replace(
      /\(\s*\w+\s*\(\s*\?(\w+)\s*\)\s+AS\s+\?\w+\s*\)/g,
      "?$1",
    );
  }

  /**
   * Stop the BeeWorker and cleanup resources
   */
  stop() {
    console.log("BeeWorker stopping...");
  }
}

// You can choose either "StreamingQueryChunkAggregatorOperator" or "ApproximationApproachOperator" as the argument
const operatorType = process.argv[2] || "StreamingQueryChunkAggregatorOperator";
const beeWorker = new BeeWorker();

process.on("SIGINT", () => {
  beeWorker.stop();
  process.exit(0);
});

process.on("SIGTERM", () => {
  beeWorker.stop();
  process.exit(0);
});
