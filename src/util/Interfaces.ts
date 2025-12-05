/**
 * Enum defining the types of approaches available for orchestration
 */
export enum ApproachType {
  STREAMING_QUERY_CHUNKED = "StreamingQueryChunked",
  APPROXIMATION = "Approximation",
  FETCHING_CLIENT_SIDE = "FetchingClientSide",
  INDEPENDENT_STREAM_PROCESSING = "IndependentStreamProcessing",
}

/**
 * Enum defining the types of operators available for worker processing
 */
export enum OperatorType {
  STREAMING_QUERY_CHUNK_AGGREGATOR = "StreamingQueryChunkAggregatorOperator",
  RATE_BASED_APPROXIMATION = "RateBasedApproximationApproachOperator",
  NAIVE_APPROXIMATION = "NaiveApproximationApproachOperator",
}

/**
 * Interface for mapping approaches to their corresponding operators
 */
export interface ApproachOperatorMapping {
  approach: ApproachType;
  operator: OperatorType;
  description: string;
}

/**
 * Interface for stream query operators
 */
export interface IStreamQueryOperator {
  addSubQuery(query: string): void;
  addOutputQuery(query: string): void;
  getSubQueries(): string[];
  init(): Promise<void>;
  handleAggregation(): Promise<void>;
}

/**
 * Interface for orchestrator approaches
 */
export interface IOrchestrationApproach {
  getApproachType(): ApproachType;
  getOperatorType(): OperatorType;
  execute(): Promise<void>;
}
