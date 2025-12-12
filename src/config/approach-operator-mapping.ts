import { ApproachType, OperatorType, ApproachOperatorMapping } from "../util/Interfaces";

/**
 * Configuration mapping between Orchestrator Approaches and Worker Operators
 * This ensures that each approach in the orchestrator has a corresponding operator in the worker.
 */
export const APPROACH_OPERATOR_MAPPINGS: ApproachOperatorMapping[] = [
  {
    approach: ApproachType.STREAMING_QUERY_CHUNKED,
    operator: OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR,
    description: "Chunked query processing with aggregation on worker side",
  },
  {
    approach: ApproachType.APPROXIMATION,
    operator: OperatorType.RATE_BASED_APPROXIMATION,
    description: "Rate-based approximation approach for streaming queries",
  },
  {
    approach: ApproachType.FETCHING_CLIENT_SIDE,
    operator: OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR,
    description: "Client-side fetching with chunk aggregation",
  },
  {
    approach: ApproachType.INDEPENDENT_STREAM_PROCESSING,
    operator: OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR,
    description: "Independent stream processing with aggregation",
  },
];

/**
 * Get the operator type for a given approach type.
 * @param {ApproachType} approach - The approach type.
 * @returns {OperatorType} The corresponding operator type.
 * @throws Error if no mapping exists for the approach.
 */
export function getOperatorForApproach(approach: ApproachType): OperatorType {
  const mapping = APPROACH_OPERATOR_MAPPINGS.find((m) => m.approach === approach);
  if (!mapping) {
    throw new Error(
      `No operator mapping found for approach: ${approach}. ` +
      `Available approaches: ${APPROACH_OPERATOR_MAPPINGS.map((m) => m.approach).join(", ")}`
    );
  }
  return mapping.operator;
}

/**
 * Get the approach type for a given operator type.
 * @param {OperatorType} operator - The operator type.
 * @returns {ApproachType[]} An array of approach types that use this operator.
 */
export function getApproachesForOperator(operator: OperatorType): ApproachType[] {
  return APPROACH_OPERATOR_MAPPINGS
    .filter((m) => m.operator === operator)
    .map((m) => m.approach);
}

/**
 * Validate that an approach and operator combination is valid.
 * @param {ApproachType} approach - The approach type.
 * @param {OperatorType} operator - The operator type.
 * @returns {boolean} True if the combination is valid, false otherwise.
 */
export function isValidApproachOperatorCombination(
  approach: ApproachType,
  operator: OperatorType
): boolean {
  return APPROACH_OPERATOR_MAPPINGS.some(
    (m) => m.approach === approach && m.operator === operator
  );
}

/**
 * Get all valid approach-operator mappings.
 * @returns {ApproachOperatorMapping[]} Array of all configured mappings.
 */
export function getAllMappings(): ApproachOperatorMapping[] {
  return [...APPROACH_OPERATOR_MAPPINGS];
}
