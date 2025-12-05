import { IStreamQueryOperator } from "../util/Interfaces";
import { OperatorType } from "../util/Interfaces";
import { StreamingQueryChunkAggregatorOperator } from "./operators/StreamingQueryChunkAggregatorOperator";
import { ApproximationApproachOperator } from "./operators/RateBasedApproximationApproachOperator";
import { NaiveApproximationApproachOperator } from "./operators/NaiveApproximationApproachOperator";

/**
 * Factory class for creating worker operators based on type
 * This centralizes operator instantiation and ensures type safety
 */
export class WorkerFactory {
  /**
   * Create an operator instance based on the specified type
   * @param operatorType The type of operator to create
   * @returns An instance of the requested operator
   * @throws Error if the operator type is not supported
   */
  static createOperator(operatorType: OperatorType): IStreamQueryOperator {
    switch (operatorType) {
      case OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR:
        return new StreamingQueryChunkAggregatorOperator();

      case OperatorType.RATE_BASED_APPROXIMATION:
        return new ApproximationApproachOperator();

      case OperatorType.NAIVE_APPROXIMATION:
        return new NaiveApproximationApproachOperator();

      default:
        throw new Error(
          `Unsupported operator type: ${operatorType}. ` +
          `Supported types: ${Object.values(OperatorType).join(", ")}`
        );
    }
  }

  /**
   * Create an operator instance from a string type
   * This is useful when the operator type comes from environment variables or config
   * @param operatorTypeString The string representation of the operator type
   * @returns An instance of the requested operator
   * @throws Error if the operator type string is invalid or not supported
   */
  static createOperatorFromString(operatorTypeString: string): IStreamQueryOperator {
    // Validate that the string matches a known OperatorType
    const operatorType = this.parseOperatorType(operatorTypeString);
    return this.createOperator(operatorType);
  }

  /**
   * Parse a string to an OperatorType enum value
   * @param operatorTypeString The string to parse
   * @returns The corresponding OperatorType enum value
   * @throws Error if the string doesn't match any OperatorType
   */
  private static parseOperatorType(operatorTypeString: string): OperatorType {
    // Check if it's a valid enum value
    const enumValues = Object.values(OperatorType) as string[];

    if (enumValues.includes(operatorTypeString)) {
      return operatorTypeString as OperatorType;
    }

    // Legacy compatibility: map old string names to new enum values
    const legacyMappings: Record<string, OperatorType> = {
      "StreamingQueryChunkAggregatorOperator": OperatorType.STREAMING_QUERY_CHUNK_AGGREGATOR,
      "ApproximationApproachOperator": OperatorType.RATE_BASED_APPROXIMATION,
      "RateBasedApproximationApproachOperator": OperatorType.RATE_BASED_APPROXIMATION,
      "NaiveApproximationApproachOperator": OperatorType.NAIVE_APPROXIMATION,
    };

    if (operatorTypeString in legacyMappings) {
      return legacyMappings[operatorTypeString];
    }

    throw new Error(
      `Invalid operator type string: "${operatorTypeString}". ` +
      `Valid types: ${enumValues.join(", ")}`
    );
  }

  /**
   * Check if an operator type string is valid
   * @param operatorTypeString The string to check
   * @returns True if the string represents a valid operator type
   */
  static isValidOperatorType(operatorTypeString: string): boolean {
    try {
      this.parseOperatorType(operatorTypeString);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get all available operator types
   * @returns Array of all supported operator types
   */
  static getAvailableOperatorTypes(): OperatorType[] {
    return Object.values(OperatorType);
  }
}
