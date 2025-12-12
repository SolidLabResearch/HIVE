/**
 * Health Status Tracker for Streaming Query Approaches
 * Tracks query registration, data processing, results, and errors
 */

export interface HealthMetrics {
  queryRegistered: boolean;
  queryRegistrationTime?: number;
  dataProcessingActive: boolean;
  lastDataProcessedTime?: number;
  dataPointsProcessed: number;
  resultsGenerated: number;
  lastResultTime?: number;
  lastResultValue?: any;
  errorsEncountered: number;
  lastErrorTime?: number;
  lastErrorMessage?: string;
  uptimeMs: number;
  status: "healthy" | "degraded" | "error" | "initializing";
}

export interface HealthCheckResponse {
  approach: string;
  component: string;
  timestamp: number;
  metrics: HealthMetrics;
  details: {
    queryInfo?: {
      registered: boolean;
      registrationAge: number;
    };
    processingInfo?: {
      active: boolean;
      timeSinceLastData: number;
      dataRate: number;
    };
    resultInfo?: {
      count: number;
      timeSinceLastResult: number;
      lastValue: any;
    };
    errorInfo?: {
      count: number;
      timeSinceLastError: number;
      lastMessage: string;
    };
  };
}

/**
 * Health Status Tracker
 */
export class HealthStatus {
  private approach: string;
  private component: string;
  private startTime: number;
  private metrics: HealthMetrics;

  /**
   * Creates a new HealthStatus tracker
   * @param {string} approach - Approach name (e.g., "approximation", "fetching")
   * @param {string} component - Component name (e.g., "orchestrator", "operator")
   */
  constructor(approach: string, component: string) {
    this.approach = approach;
    this.component = component;
    this.startTime = Date.now();

    this.metrics = {
      queryRegistered: false,
      dataProcessingActive: false,
      dataPointsProcessed: 0,
      resultsGenerated: 0,
      errorsEncountered: 0,
      uptimeMs: 0,
      status: "initializing",
    };
  }

  /**
   * Marks query as registered
   */
  public markQueryRegistered(): void {
    this.metrics.queryRegistered = true;
    this.metrics.queryRegistrationTime = Date.now();
    this.updateStatus();
  }

  /**
   * Records data processing
   * @param {number} count - Number of data points processed
   */
  public recordDataProcessing(count: number = 1): void {
    this.metrics.dataProcessingActive = true;
    this.metrics.lastDataProcessedTime = Date.now();
    this.metrics.dataPointsProcessed += count;
    this.updateStatus();
  }

  /**
   * Records result generation
   * @param {any} value - The result value
   */
  public recordResult(value: any): void {
    this.metrics.resultsGenerated++;
    this.metrics.lastResultTime = Date.now();
    this.metrics.lastResultValue = value;
    this.updateStatus();
  }

  /**
   * Records an error
   * @param {string} message - Error message
   */
  public recordError(message: string): void {
    this.metrics.errorsEncountered++;
    this.metrics.lastErrorTime = Date.now();
    this.metrics.lastErrorMessage = message;
    this.updateStatus();
  }

  /**
   * Updates overall health status
   */
  private updateStatus(): void {
    const now = Date.now();
    this.metrics.uptimeMs = now - this.startTime;

    // Determine status based on metrics
    if (this.metrics.errorsEncountered > 10) {
      this.metrics.status = "error";
    } else if (!this.metrics.queryRegistered) {
      this.metrics.status = "initializing";
    } else if (
      this.metrics.dataProcessingActive &&
      this.metrics.lastDataProcessedTime &&
      now - this.metrics.lastDataProcessedTime < 60000
    ) {
      // Active if data processed in last 60 seconds
      this.metrics.status = "healthy";
    } else if (this.metrics.errorsEncountered > 0) {
      this.metrics.status = "degraded";
    } else if (this.metrics.queryRegistered) {
      this.metrics.status = "healthy";
    } else {
      this.metrics.status = "initializing";
    }
  }

  /**
   * Gets current health status
   * @returns {HealthMetrics} Current metrics
   */
  public getMetrics(): HealthMetrics {
    this.metrics.uptimeMs = Date.now() - this.startTime;
    return { ...this.metrics };
  }

  /**
   * Gets comprehensive health check response
   * @returns {HealthCheckResponse} Complete health status
   */
  public getHealthCheck(): HealthCheckResponse {
    const now = Date.now();
    this.updateStatus();

    const response: HealthCheckResponse = {
      approach: this.approach,
      component: this.component,
      timestamp: now,
      metrics: this.getMetrics(),
      details: {},
    };

    // Query info
    if (this.metrics.queryRegistrationTime) {
      response.details.queryInfo = {
        registered: this.metrics.queryRegistered,
        registrationAge: now - this.metrics.queryRegistrationTime,
      };
    }

    // Processing info
    if (this.metrics.lastDataProcessedTime) {
      const timeSinceLastData = now - this.metrics.lastDataProcessedTime;
      const dataRate =
        this.metrics.dataPointsProcessed /
        (this.metrics.uptimeMs / 1000 / 60); // per minute

      response.details.processingInfo = {
        active: timeSinceLastData < 60000,
        timeSinceLastData,
        dataRate: parseFloat(dataRate.toFixed(2)),
      };
    }

    // Result info
    if (this.metrics.lastResultTime) {
      response.details.resultInfo = {
        count: this.metrics.resultsGenerated,
        timeSinceLastResult: now - this.metrics.lastResultTime,
        lastValue: this.metrics.lastResultValue,
      };
    }

    // Error info
    if (this.metrics.lastErrorTime) {
      response.details.errorInfo = {
        count: this.metrics.errorsEncountered,
        timeSinceLastError: now - this.metrics.lastErrorTime,
        lastMessage: this.metrics.lastErrorMessage || "Unknown error",
      };
    }

    return response;
  }

  /**
   * Gets a simple status string
   * @returns {string} Status string
   */
  public getStatusString(): string {
    const status = this.metrics.status.toUpperCase();
    const uptime = (this.metrics.uptimeMs / 1000).toFixed(0);
    return `[${status}] Uptime: ${uptime}s | Data: ${this.metrics.dataPointsProcessed} | Results: ${this.metrics.resultsGenerated} | Errors: ${this.metrics.errorsEncountered}`;
  }

  /**
   * Checks if system is healthy
   * @returns {boolean} True if healthy
   */
  public isHealthy(): boolean {
    return this.metrics.status === "healthy";
  }

  /**
   * Resets all metrics
   */
  public reset(): void {
    this.startTime = Date.now();
    this.metrics = {
      queryRegistered: false,
      dataProcessingActive: false,
      dataPointsProcessed: 0,
      resultsGenerated: 0,
      errorsEncountered: 0,
      uptimeMs: 0,
      status: "initializing",
    };
  }
}
