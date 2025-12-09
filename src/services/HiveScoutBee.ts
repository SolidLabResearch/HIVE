import {
  HiveScoutBee as HiveScoutBeeCore,
  SignatureExtractor,
  ApproachConfig,
  ApproachRecommendation as CoreApproachRecommendation,
  StreamSignature as CoreStreamSignature,
} from "hive-scout-bee";
import { Quad, DataFactory } from "n3";
import { ApproachRecommendation } from "../util/Types";

const { quad, namedNode, literal } = DataFactory;

/**
 * Enhanced wrapper around the hive-scout-bee package
 * Provides integration with the streaming query hive architecture
 * and adapts the core functionality for our specific use cases.
 */
export class HiveScoutBeeWrapper {
  private coreScoutBee: HiveScoutBeeCore;
  private signatureExtractor: SignatureExtractor;
  private dataBuffer: Array<{
    timestamp: number;
    value: number;
    topic: string;
  }> = [];
  private readonly maxBufferSize: number;

  /**
   * Creates a new HiveScoutBeeWrapper instance.
   * @param {number} maxBufferSize - Maximum number of data points to keep in the buffer.
   */
  constructor(maxBufferSize: number = 1000) {
    this.maxBufferSize = maxBufferSize;
    this.signatureExtractor = new SignatureExtractor();

    /**
     * Default approach configurations
     * Can be customized or extended as needed
     * In the future, could be loaded from external configs or learned dynamically with
     * Reinforcement Learning approach where the system adapts based on query performance feedback.
     * For now, we define some sensible defaults. In a real system, the values will be tuned.
     * We have planned to implement this in our future work for the other extension along with adding approaches to the
     * Streaming Query Hive service.
     */
    const approachConfigs: ApproachConfig[] = [
      {
        name: "approximation-approach",
        description: "Suitable for stable, low-variance streams",
        maxThresholds: {
          variance: 50,
          entropy: 5.0,
          fftEntropy: 3.0,
        },
        priority: 1,
      },
      {
        name: "fetching-client-side",
        description: "Optimal for high-variance, complex streams",
        minThresholds: {
          variance: 20,
          entropy: 2.0,
        },
        priority: 2,
      },
      {
        name: "chunked-approach",
        description: "Best for high-volume, regular streams",
        minThresholds: {
          tripleCount: 50,
        },
        maxThresholds: {
          variance: 100,
        },
        priority: 3,
      },
      {
        name: "streaming-query-hive",
        description:
          "Advanced streaming query processing with intelligent orchestration",
        priority: 4,
      },
    ];

    this.coreScoutBee = new HiveScoutBeeCore(approachConfigs);
  }

  /**
   * Add a new data point to the internal buffer.
   * @param {number} timestamp - The timestamp of the data point in milliseconds since epoch.
   * @param {number} value - The numeric value of the data point.
   * @param {string} topic - The topic of the data point where it originated (i.e the MQTT topic).
   * @memberof HiveScoutBeeWrapper
   */
  addDataPoint(timestamp: number, value: number, topic: string): void {
    this.dataBuffer.push({ timestamp, value, topic });

    if (this.dataBuffer.length > this.maxBufferSize) {
      this.dataBuffer.shift();
    }
  }

  /**
   * Convert internal buffer to RDF quads for analysis.
   * @private
   * @returns {Set<Quad>} Set of RDF quads representing the buffered data.
   * @memberof HiveScoutBeeWrapper
   */
  private convertToQuads(): Set<Quad> {
    const quads = new Set<Quad>();

    this.dataBuffer.forEach((dataPoint, _index) => {
      const sensorIRI = namedNode(
        `http://example.org/sensor/${dataPoint.topic}`,
      );
      const valueIRI = namedNode("http://example.org/hasValue");
      const timestampIRI = namedNode("http://example.org/hasTimestamp");

      quads.add(
        quad(
          sensorIRI,
          valueIRI,
          literal(
            dataPoint.value.toString(),
            namedNode("http://www.w3.org/2001/XMLSchema#double"),
          ),
        ),
      );

      quads.add(
        quad(
          sensorIRI,
          timestampIRI,
          literal(
            dataPoint.timestamp.toString(),
            namedNode("http://www.w3.org/2001/XMLSchema#long"),
          ),
        ),
      );
    });

    return quads;
  }

  /**
   * Analyze the buffered data and get an approach recommendation from the hive-scout-bee core package .
   * @returns {ApproachRecommendation | null} The recommended approach or null if insufficient data.
   * @memberof HiveScoutBeeWrapper
   */
  getApproachRecommendation(): ApproachRecommendation | null {
    // The core package needs a minimum amount of data to perform analysis, we require at least 10 data points
    // Normally with such a small amount of data it is better to do a fetching approach on client side.
    if (this.dataBuffer.length < 10) {
      console.log(
        `Insufficient data for analysis (${this.dataBuffer.length} points)`,
      );
      return null;
    }

    try {
      const windowData = this.convertToQuads();

      const coreRecommendation: CoreApproachRecommendation =
        this.coreScoutBee.chooseApproach(windowData);

      const recommendation: ApproachRecommendation = {
        recommendedApproach: coreRecommendation.recommendedApproach as any,
        confidence: coreRecommendation.confidence,
        scores: {
          approximation:
            coreRecommendation.recommendedApproach === "approximation-approach"
              ? 100
              : 0,
          fetching:
            coreRecommendation.recommendedApproach === "fetching-client-side"
              ? 100
              : 0,
          chunked:
            coreRecommendation.recommendedApproach === "chunked-approach"
              ? 100
              : 0,
          hive:
            coreRecommendation.recommendedApproach === "streaming-query-hive"
              ? 100
              : 0,
        },
        reasoning: this.generateReasoning(coreRecommendation),
        signature: this.convertSignature(coreRecommendation.signature),
      };

      return recommendation;
    } catch (error) {
      console.error(`Error during approach recommendation:`, error);
      return null;
    }
  }

  /**
   * Extract the stream signature from the buffered data using the hive-scout-bee package's extractor.
   * @public
   * @returns {CoreStreamSignature | null} The extracted stream signature or null if insufficient data.
   * @memberof HiveScoutBeeWrapper
   */
  getStreamSignature(): CoreStreamSignature | null {
    if (this.dataBuffer.length < 10) {
      return null;
    }

    const windowData = this.convertToQuads();
    return this.signatureExtractor.extractSignature(windowData);
  }

  /**
   * Add a new approach configuration to the scout bee for a new approach
   * Currently not used in the system but provided for extensibility as we plan to add more approaches in future work.
   * We only have 3 approaches for now.
   * @param {ApproachConfig} config - The configuration for the new approach to add so that it can be recommended and chosen.
   * @returns {void} - No return value.
   * @memberof HiveScoutBeeWrapper
   */
  addApproachConfig(config: ApproachConfig): void {
    this.coreScoutBee.addApproach(config);
  }

  /**
   * Remove an existing approach configuration by name.
   * @param {string} approachName - The name of the approach to remove.
   * @returns {boolean} - True if the approach was removed, false otherwise.
   * @memberof HiveScoutBeeWrapper
   */
  removeApproachConfig(approachName: string): boolean {
    return this.coreScoutBee.removeApproach(approachName);
  }

  /**
   * Get a list of available approaches in the scout bee.
   * @returns {string[]} - Array of approach names available in the scout bee.
   * @memberof HiveScoutBeeWrapper
   */
  getAvailableApproaches(): string[] {
    return this.coreScoutBee.getAvailableApproaches();
  }

  /**
   * Get configuration details for a specific approach by name.
   * @param {string} approachName - The name of the approach to get configuration for.
   * @returns {(ApproachConfig | undefined)} - The approach configuration or undefined if not found.
   * @memberof HiveScoutBeeWrapper
   */
  getApproachConfig(approachName: string): ApproachConfig | undefined {
    return this.coreScoutBee.getApproachConfig(approachName);
  }

  /**
   * Get statistics about the current data buffer
   * Useful for monitoring and debugging purposes.
   * @returns {any} - An object containing buffer statistics.
   * @memberof HiveScoutBeeWrapper
   */
  getBufferStats(): any {
    if (this.dataBuffer.length === 0) {
      return { message: "No data in buffer" };
    }

    const values = this.dataBuffer.map((d) => d.value);
    const timestamps = this.dataBuffer.map((d) => d.timestamp);
    const topics = [...new Set(this.dataBuffer.map((d) => d.topic))];

    const mean = values.reduce((sum, v) => sum + v, 0) / values.length;
    const variance =
      values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length;
    const min = Math.min(...values);
    const max = Math.max(...values);

    const startTime = Math.min(...timestamps);
    const endTime = Math.max(...timestamps);

    return {
      count: this.dataBuffer.length,
      mean: mean,
      min: min,
      max: max,
      variance: variance,
      topics: topics,
      timeRange: {
        start: startTime,
        end: endTime,
        duration: endTime - startTime,
      },
      latest: this.dataBuffer[this.dataBuffer.length - 1],
      oldest: this.dataBuffer[0],
    };
  }

  /**
   * Generate human-readable reasoning for the approach recommendation.
   * @private
   * @param {CoreApproachRecommendation} coreRecommendation - The core recommendation from hive-scout-bee.
   * @returns {string[]} - Array of reasoning strings.
   * @memberof HiveScoutBeeWrapper
   */
  private generateReasoning(
    coreRecommendation: CoreApproachRecommendation,
  ): string[] {
    const reasons: string[] = [];
    const signature = coreRecommendation.signature;

    switch (coreRecommendation.recommendedApproach) {
      case "approximation-approach":
        reasons.push(
          `Low variance (${signature.variance.toFixed(2)}) indicates stable stream suitable for approximation`,
        );
        if (signature.entropy < 2.0) {
          reasons.push(
            `Low entropy (${signature.entropy.toFixed(2)}) suggests predictable patterns`,
          );
        }
        break;

      case "fetching-client-side":
        reasons.push(
          `High variance (${signature.variance.toFixed(2)}) requires precise fetching approach`,
        );
        if (signature.entropy > 3.0) {
          reasons.push(
            `High entropy (${signature.entropy.toFixed(2)}) indicates complex, unpredictable data`,
          );
        }
        break;

      case "chunked-approach":
        reasons.push(
          `High triple count (${signature.tripleCount}) benefits from chunked processing`,
        );
        if (signature.variance < 30) {
          reasons.push(
            `Moderate variance (${signature.variance.toFixed(2)}) suitable for chunking`,
          );
        }
        break;

      case "streaming-query-hive":
        reasons.push(
          `Moderate entropy (${signature.entropy.toFixed(2)}) and variance (${signature.variance.toFixed(2)}) suitable for advanced streaming query processing`,
        );
        reasons.push(
          `Complex patterns detected that benefit from intelligent orchestration`,
        );
        break;

      default:
        reasons.push(
          `Stream characteristics (variance: ${signature.variance.toFixed(2)}, entropy: ${signature.entropy.toFixed(2)}) analyzed`,
        );
        reasons.push(
          `Using ${coreRecommendation.recommendedApproach} approach based on current stream signature`,
        );
        break;
    }

    if (coreRecommendation.matchingApproaches.length > 1) {
      reasons.push(
        `Multiple approaches matched: ${coreRecommendation.matchingApproaches.join(", ")}`,
      );
    }

    return reasons;
  }

  /**
   * Convert the core stream signature to our extended format.
   * @private
   * @param {CoreStreamSignature} coreSignature - The core stream signature from hive-scout-bee.
   * @returns {any} - The converted stream signature.
   * @memberof HiveScoutBeeWrapper
   */
  private convertSignature(coreSignature: CoreStreamSignature): any {
    return {
      timestamp: Date.now(),
      windowSize: this.dataBuffer.length,
      dataPoints: coreSignature.tripleCount,
      statistics: {
        mean: 0,
        variance: coreSignature.variance,
        stdDev: Math.sqrt(coreSignature.variance),
        min: 0,
        max: 0,
      },
      temporal: {
        avgInterval: 0,
        intervalVariance: 0,
        dataRate: 0,
        regularity: 1,
      },
      patterns: {
        frequency: 0,
        trend: "stable" as const,
        volatility: coreSignature.variance / 100,
        seasonality: 0,
        oscillation: 0,
      },
      classification: {
        streamType: this.classifyStreamType(coreSignature),
        complexity: Math.min(Math.ceil(coreSignature.entropy), 10),
        predictability: Math.max(0, 1 - coreSignature.entropy / 5),
        aliasing_risk: "medium" as const,
      },
      core: coreSignature,
    };
  }

  /**
   * Classify the stream type based on signature characteristics.
   * @private
   * @param {CoreStreamSignature} signature - The core stream signature.
   * @returns {*}  {('stable' | 'volatile' | 'periodic' | 'irregular' | 'mixed')} - The classified stream type.
   * @memberof HiveScoutBeeWrapper
   */
  private classifyStreamType(
    signature: CoreStreamSignature,
  ): "stable" | "volatile" | "periodic" | "irregular" | "mixed" {
    if (signature.variance < 10 && signature.entropy < 2.0) return "stable";
    if (signature.variance > 50) return "volatile";
    if (signature.skewness > 1.0 || signature.skewness < -1.0)
      return "irregular";
    if (signature.fftEntropy > 2.0) return "periodic";
    return "mixed";
  }

  /**
   * Simulate different stream patterns for testing approach selection
   * This method helps test how different stream characteristics affect approach recommendations.
   * @param {('stable'|'volatile'|'periodic'|'mixed')} pattern - The pattern type to simulate.
   * @param {number} dataPoints - Number of data points to generate.
   * @param {string} topic - Topic name for the simulated data.
   * @memberof HiveScoutBeeWrapper
   */
  simulateStreamPattern(
    pattern: "stable" | "volatile" | "periodic" | "mixed",
    dataPoints: number = 50,
    topic: string = "simulated_sensor",
  ): void {
    const startTime = Date.now();

    for (let i = 0; i < dataPoints; i++) {
      const timestamp = startTime + i * 1000; // 1 second intervals
      let value: number;

      switch (pattern) {
        case "stable":
          // Low variance, predictable pattern
          value = 50 + Math.sin(i * 0.1) * 2 + (Math.random() - 0.5) * 0.5;
          break;

        case "volatile":
          // High variance, unpredictable pattern
          value = 50 + (Math.random() - 0.5) * 100 + Math.sin(i * 0.5) * 30;
          break;

        case "periodic":
          // Regular oscillations
          value = 50 + Math.sin(i * 0.3) * 20 + Math.cos(i * 0.1) * 10;
          break;

        case "mixed":
          // Combination of patterns
          value =
            50 +
            Math.sin(i * 0.2) * 15 +
            (Math.random() - 0.5) * 10 +
            Math.cos(i * 0.05) * 5;
          break;

        default:
          value = 50;
      }

      this.addDataPoint(timestamp, value, topic);
    }

    console.log(`Simulated ${dataPoints} data points with ${pattern} pattern`);
  }

  /**
   * Clear the internal data buffer
   * Useful for testing different stream patterns or resetting state.
   * @memberof HiveScoutBeeWrapper
   */
  clearBuffer(): void {
    this.dataBuffer = [];
    console.log("Buffer cleared");
  }
}
// Re-exporting for easier imports elsewhere in the project
export { HiveScoutBeeWrapper as HiveScoutBee };
