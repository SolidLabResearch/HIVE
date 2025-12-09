import { RSPAgent } from "../agent/RSPAgent";
import { BeeKeeper } from "../services/BeeKeeper";
import { HTTPServer } from "../services/server/HTTPServer";
import { HiveScoutBeeWrapper } from "../services/HiveScoutBee";
import { ApproachRecommendation, AnalysisSummary } from "../util/Types";
import config from "../config/httpServerConfig.json";
import { hash_string_md5 } from "../util/Util";

/**
 * Enhanced Orchestrator with intelligent approach selection
 * Uses Hive Scout Bee to analyze stream patterns and recommend optimal approaches
 * Supports both automatic stream analysis and manual approach specification
 */
export class IntelligentOrchestrator {
  private subQueriesToRun: string[];
  private registeredQuery: string;
  private beeKeeper: BeeKeeper;
  private operatorType: string;
  private http_server: HTTPServer;
  private scoutBee: HiveScoutBeeWrapper;
  private streamAnalysisEnabled: boolean;
  private lastRecommendation: ApproachRecommendation | null = null;
  private analysisHistory: ApproachRecommendation[] = [];
  private manualApproach: string | null = null;
  private analysisMode: "automatic" | "manual" | "hybrid" = "automatic";
  private cachedRecommendation: ApproachRecommendation | null = null;

  /**
   * Creates an instance of IntelligentOrchestrator.
   * @param {string} operatorType - The default operator type to use.
   * @param {boolean} [enableStreamAnalysis=true] - Whether to enable stream analysis for approach selection or not.
   * @memberof IntelligentOrchestrator
   */
  constructor(operatorType: string, enableStreamAnalysis: boolean = true) {
    this.subQueriesToRun = [];
    this.registeredQuery = "";
    this.beeKeeper = new BeeKeeper();
    const port = process.env.HTTP_PORT
      ? parseInt(process.env.HTTP_PORT)
      : config.port;
    this.http_server = new HTTPServer(port, console);
    this.operatorType = operatorType;
    this.streamAnalysisEnabled = enableStreamAnalysis;
    this.scoutBee = new HiveScoutBeeWrapper();

    console.log(`Intelligent Orchestrator started on port ${port}`);
    console.log(
      `Stream analysis: ${enableStreamAnalysis ? "ENABLED" : "DISABLED"}`,
    );
    console.log(`Analysis mode: ${this.analysisMode}`);
  }

  /**
   * Set analysis mode for approach selection
   * @param { 'automatic' | 'manual' | 'hybrid' } mode - 'automatic' for stream analysis, 'manual' for specified approach, 'hybrid' for both
   * @returns {void} - No return value.
   */
  setAnalysisMode(mode: "automatic" | "manual" | "hybrid"): void {
    this.analysisMode = mode;
    console.log(`Analysis mode changed to: ${mode}`);
  }

  /**
   * Set manual approach to use (only effective in 'manual' or 'hybrid' mode)
   * @param { 'approximation-approach' | 'fetching-client-side' | 'chunked-approach' | 'streaming-query-hive' | 'default' } approach - The approach to use manually for query execution
   * @returns {void} - No return value.
   */
  setManualApproach(
    approach:
      | "approximation-approach"
      | "fetching-client-side"
      | "chunked-approach"
      | "streaming-query-hive"
      | "default",
  ): void {
    this.manualApproach = approach;
    console.log(`Manual approach set to: ${approach}`);
  }

  /**
   * Get current analysis mode
   * @return { 'automatic' | 'manual' | 'hybrid' } - The current analysis mode being used for approach selection for query execution
   */
  getAnalysisMode(): "automatic" | "manual" | "hybrid" {
    return this.analysisMode;
  }

  /**
   * Get current manual approach (if set)
   * @return {string | null} - The current manual approach set, or null if none is set
   */
  getManualApproach(): string | null {
    return this.manualApproach;
  }

  /**
   * Enable or disable stream analysis for approach selection
   * @param {boolean} enabled - True to enable stream analysis, false to disable
   * @returns {void} - No return value.
   */
  toggleStreamAnalysis(enabled: boolean): void {
    this.streamAnalysisEnabled = enabled;
    console.log(`Stream analysis: ${enabled ? "ENABLED" : "DISABLED"}`);
  }

  /**
   * Adds a sub-query to the orchestrator.
   * @param {string} query - The sub-query to add.
   * @returns {void} - No return value.
   */
  addSubQuery(query: string): void {
    this.subQueriesToRun.push(query);
    const query_hash = hash_string_md5(query);

    const queryAgent = new RSPAgent(query, `chunked/${query_hash}`);
    queryAgent
      .process_streams()
      .then(() => {
        console.log(`Added sub-query: ${query}`);
      })
      .catch((error: Error) => {
        console.error(`Error processing sub-query "${query}":`, error);
      });
  }

  /**
   * Register output query with intelligent approach selection
   * @param {string} query - The output query to register with the orchestrator.
   * @returns {void} - No return value.
   */
  registerOutputQuery(query: string): void {
    this.registeredQuery = query;
    console.log(`Registered output query: ${query}`);

    if (this.streamAnalysisEnabled) {
      console.log(
        `Stream analysis will be performed to optimize approach selection`,
      );
    }
  }

  /**
   * Analyze stream data and add to scout bee analysis window
   * @param {number} timestamp - The timestamp of the data point.
   * @param {number} value - The value of the data point.
   * @param {string} topic - The topic/source of the data point.
   * @returns {void} - No return value.
   */
  analyzeStreamData(timestamp: number, value: number, topic: string): void {
    if (!this.streamAnalysisEnabled) return;

    this.scoutBee.addDataPoint(timestamp, value, topic);
  }

  /**
   * Pre-analyze streams and cache recommendation for later use
   * Useful for getting approach recommendations before query execution
   * @param {number} queryComplexity - Estimated complexity of the query to be executed (1-10 scale)
   * @returns {Promise<ApproachRecommendation | null>} - The cached recommendation, or null if analysis failed
   */
  async preAnalyzeStreams(
    queryComplexity: number = 5,
  ): Promise<ApproachRecommendation | null> {
    if (!this.streamAnalysisEnabled) {
      console.log("Stream analysis is disabled, cannot pre-analyze");
      return null;
    }

    try {
      console.log("Pre-analyzing streams for approach recommendation...");
      const recommendation =
        await this.getApproachRecommendation(queryComplexity);

      if (recommendation) {
        this.cachedRecommendation = recommendation;
        console.log(
          `Pre-analysis complete: ${recommendation.recommendedApproach} (confidence: ${(recommendation.confidence * 100).toFixed(1)}%)`,
        );
        return recommendation;
      } else {
        console.log("Pre-analysis failed: no recommendation generated");
        return null;
      }
    } catch (error) {
      console.error("Error during pre-analysis:", error);
      return null;
    }
  }

  /**
   * Clear the cached recommendation from pre-analysis
   * @returns {void} - No return value.
   */
  clearCachedRecommendation(): void {
    this.cachedRecommendation = null;
    console.log("Cached recommendation cleared");
  }

  /**
   * Get approach recommendation based on current stream analysis data
   * @param {number} queryComplexity - Estimated complexity of the registered query (1-10 scale)
   * @returns {Promise<ApproachRecommendation | null>} - The recommended approach and details, or null if no recommendation could be made
   */
  async getApproachRecommendation(
    queryComplexity: number = 5,
  ): Promise<ApproachRecommendation | null> {
    if (!this.streamAnalysisEnabled) return null;

    try {
      const recommendation = this.scoutBee.getApproachRecommendation();

      if (!recommendation) {
        console.log(`Could not generate recommendation`);
        return null;
      }

      this.lastRecommendation = recommendation;
      this.analysisHistory.push(recommendation);

      if (this.analysisHistory.length > 10) {
        this.analysisHistory.shift();
      }

      this.logRecommendation(recommendation);
      return recommendation;
    } catch (error) {
      console.error(`Error during approach recommendation:`, error);
      return null;
    }
  }

  /**
   * Run the registered query with intelligent approach selection based on analysis mode
   * @param {string} [forceApproach] - Optional approach to force (overrides analysis mode)
   * @returns {Promise<void>} - No return value and runs the registered query
   */
  async runRegisteredQueryIntelligent(forceApproach?: string): Promise<void> {
    if (this.registeredQuery === "") {
      console.log("No registered query to run.");
      return;
    }

    let selectedApproach = forceApproach || this.operatorType;

    if (!forceApproach) {
      selectedApproach = await this.selectApproachBasedOnMode();
    }

    // Execute the registered query with the selected approach and any sub-queries to the "output" MQTT topic.
    this.beeKeeper.executeQuery(
      this.registeredQuery,
      "output",
      selectedApproach,
      this.subQueriesToRun,
    );

    console.log(`Running registered query with approach: ${selectedApproach}`);
  }

  /**
   * Select approach based on current analysis mode
   * @returns {Promise<string>} - The selected approach to use for query execution
   * @private
   */
  private async selectApproachBasedOnMode(): Promise<string> {
    switch (this.analysisMode) {
      case "manual":
        if (this.manualApproach) {
          console.log(`Using manual approach: ${this.manualApproach}`);
          return this.mapApproachToOperatorType(this.manualApproach);
        } else {
          console.log(
            `No manual approach set, using default: ${this.operatorType}`,
          );
          return this.operatorType;
        }

      case "automatic":
        if (this.streamAnalysisEnabled) {
          // Use cached recommendation if available, otherwise analyze
          let recommendation = this.cachedRecommendation;
          if (!recommendation) {
            const queryComplexity = this.estimateQueryComplexity(
              this.registeredQuery,
            );
            recommendation =
              await this.getApproachRecommendation(queryComplexity);
          } else {
            console.log(
              `Using pre-analyzed recommendation: ${recommendation.recommendedApproach} (confidence: ${(recommendation.confidence * 100).toFixed(1)}%)`,
            );
          }

          if (recommendation && recommendation.confidence > 0.6) {
            const selectedApproach = this.mapApproachToOperatorType(
              recommendation.recommendedApproach,
            );
            console.log(
              `Using automatic recommendation: ${selectedApproach} (confidence: ${(recommendation.confidence * 100).toFixed(1)}%)`,
            );
            return selectedApproach;
          } else {
            console.log(
              `Low confidence recommendation, using default: ${this.operatorType}`,
            );
            return this.operatorType;
          }
        } else {
          console.log(
            `Stream analysis disabled, using default: ${this.operatorType}`,
          );
          return this.operatorType;
        }

      case "hybrid":
        // Try automatic first, fall back to manual if available
        if (this.streamAnalysisEnabled) {
          // Use cached recommendation if available, otherwise analyze
          let recommendation = this.cachedRecommendation;
          if (!recommendation) {
            const queryComplexity = this.estimateQueryComplexity(
              this.registeredQuery,
            );
            recommendation =
              await this.getApproachRecommendation(queryComplexity);
          } else {
            console.log(
              `Using pre-analyzed recommendation (hybrid mode): ${recommendation.recommendedApproach} (confidence: ${(recommendation.confidence * 100).toFixed(1)}%)`,
            );
          }

          if (recommendation && recommendation.confidence > 0.7) {
            const selectedApproach = this.mapApproachToOperatorType(
              recommendation.recommendedApproach,
            );
            console.log(
              `Using automatic recommendation (hybrid mode): ${selectedApproach} (confidence: ${(recommendation.confidence * 100).toFixed(1)}%)`,
            );
            return selectedApproach;
          }
        }

        // Fall back to manual approach
        if (this.manualApproach) {
          console.log(
            `Falling back to manual approach (hybrid mode): ${this.manualApproach}`,
          );
          return this.mapApproachToOperatorType(this.manualApproach);
        } else {
          console.log(
            `No high-confidence recommendation or manual approach, using default: ${this.operatorType}`,
          );
          return this.operatorType;
        }

      default:
        return this.operatorType;
    }
  }

  /**
   * Get summary of current analysis state and history and recommendations
   * @returns {AnalysisSummary} - Summary object containing current analysis settings and recent recommendation history
   */
  getAnalysisSummary(): AnalysisSummary {
    const summary: AnalysisSummary = {
      analysisMode: this.analysisMode,
      streamAnalysisEnabled: this.streamAnalysisEnabled,
      manualApproach: this.manualApproach,
      lastRecommendation: this.lastRecommendation,
      historyLength: this.analysisHistory.length,
      cachedRecommendation: this.cachedRecommendation,
    };

    if (!this.streamAnalysisEnabled && this.analysisMode !== "manual") {
      summary["warning"] =
        "Stream analysis is disabled but analysis mode is not manual";
    }

    if (this.analysisMode === "manual" && !this.manualApproach) {
      summary["warning"] =
        "Manual mode is set but no manual approach specified";
    }

    if (this.analysisHistory.length > 0) {
      const recentRecommendations = this.analysisHistory.slice(-5);
      const approachCounts = recentRecommendations.reduce(
        (acc, rec) => {
          acc[rec.recommendedApproach] =
            (acc[rec.recommendedApproach] || 0) + 1;
          return acc;
        },
        {} as Record<string, number>,
      );

      const avgConfidence =
        recentRecommendations.reduce((sum, rec) => sum + rec.confidence, 0) /
        recentRecommendations.length;

      summary["recentApproaches"] = approachCounts;
      summary["averageConfidence"] = avgConfidence;
    }

    return summary;
  }

  /**
   * Clear sub-queries from the orchestrator
   * @returns {void} - No return value.
   */
  clearSubQueries(): void {
    this.subQueriesToRun = [];
    console.log("Cleared all sub-queries.");
  }

  /**
   * Run sub-queries managed by the orchestrator.
   * It initiates processing for each sub-query using RSPAgent.
   * @returns {void} - No return value.
   */
  runSubQueries(): void {
    if (this.subQueriesToRun.length === 0) {
      console.log("No queries to run.");
      return;
    }

    this.subQueriesToRun.forEach((query) => {
      const hash = hash_string_md5(query);
      const queryAgent = new RSPAgent(query, `chunked/${hash}`);
      queryAgent
        .process_streams()
        .then(() => {
          console.log(`Starting query: ${query}`);
        })
        .catch((error: Error) => {
          console.error(`Error processing query "${query}":`, error);
        });
    });
  }

  /**
   * Legacy method for backward compatibility
   */
  runRegisteredQuery(): void {
    this.runRegisteredQueryIntelligent();
  }

  /**
   * Helper to get recent stream data for analysis
   * @private
   * @return {Array<{timestamp: number, value: number}>} - Recent stream data points
   * @memberof IntelligentOrchestrator
   */
  private getRecentStreamData(): Array<{ timestamp: number; value: number }> {
    // This would be implemented to get recent data from the scout bee's analysis window
    // For now, returning empty array - in real implementation, this would access
    // the scout bee's internal data
    return [];
  }

  /**
   * Estimate the complexity of a query
   * @private
   * @param {string} query - The query to analyze
   * @return {number} - The estimated complexity (1-10)
   * @memberof IntelligentOrchestrator
   */
  private estimateQueryComplexity(query: string): number {
    // Simple complexity estimation based on query characteristics
    let complexity = 1;

    // Count aggregation functions
    const aggFunctions = (query.match(/\b(SUM|AVG|MAX|MIN|COUNT)\b/gi) || [])
      .length;
    complexity += aggFunctions * 2;

    // Count JOIN operations
    const joins = (query.match(/\bJOIN\b/gi) || []).length;
    complexity += joins * 3;

    // Count FILTER operations
    const filters = (query.match(/\bFILTER\b/gi) || []).length;
    complexity += filters;

    // Count window operations
    const windows = (query.match(/\bWINDOW\b/gi) || []).length;
    complexity += windows * 2;

    return Math.min(complexity, 10);
  }

  /**
   * Map approach string to operator type
   * @private
   * @param {string} approach - The approach string to map
   * @return {string} - The corresponding operator type
   * @memberof IntelligentOrchestrator
   */
  private mapApproachToOperatorType(approach: string): string {
    const mapping: Record<string, string> = {
      approximation: "approximation-approach",
      fetching: "fetching-client-side",
      chunked: "chunked-approach",
      hive: "streaming-query-hive",
    };

    return mapping[approach] || this.operatorType;
  }

  /**
   * Log the approach recommendation details to console
   * @private
   * @param {ApproachRecommendation} recommendation - The recommendation to log
   * @returns {void} - No return value.
   * @memberof IntelligentOrchestrator
   */
  private logRecommendation(recommendation: ApproachRecommendation): void {
    console.log(`\n=== APPROACH RECOMMENDATION ===`);
    console.log(
      `Recommended: ${recommendation.recommendedApproach.toUpperCase()}`,
    );
    console.log(`Confidence: ${(recommendation.confidence * 100).toFixed(1)}%`);
    console.log(`Scores:`);
    Object.entries(recommendation.scores).forEach(([approach, score]) => {
      console.log(`   ${approach}: ${score}`);
    });
    console.log(`Reasoning:`);
    recommendation.reasoning.forEach((reason) => {
      console.log(`   • ${reason}`);
    });
    console.log(
      `Stream Type: ${recommendation.signature.classification.streamType}`,
    );
    console.log(
      `Data Rate: ${recommendation.signature.temporal.dataRate.toFixed(2)} Hz`,
    );
    console.log(
      `Volatility: ${(recommendation.signature.patterns.volatility * 100).toFixed(1)}%`,
    );
    console.log(
      `Aliasing Risk: ${recommendation.signature.classification.aliasing_risk}`,
    );
    console.log(`================================\n`);
  }
}
