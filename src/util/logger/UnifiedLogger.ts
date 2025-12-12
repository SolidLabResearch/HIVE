import * as fs from "fs";
import * as path from "path";

/**
 * Log levels for the unified logger
 */
export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

/**
 * Log entry structure
 */
export interface LogEntry {
  timestamp: number;
  level: LogLevel;
  component: string;
  message: string;
  metadata?: Record<string, any>;
}

/**
 * Unified Logger Configuration
 */
export interface LoggerConfig {
  component: string;
  logDirectory: string;
  logLevel?: LogLevel;
  enableConsole?: boolean;
  enableFile?: boolean;
}

/**
 * Unified Logger for all streaming query components
 * Provides consistent logging across orchestrators, operators, and agents
 */
export class UnifiedLogger {
  private component: string;
  private logDirectory: string;
  private logLevel: LogLevel;
  private enableConsole: boolean;
  private enableFile: boolean;
  private logStream?: fs.WriteStream;
  private logFilePath: string;

  /**
   * Creates a new UnifiedLogger instance
   * @param {LoggerConfig} config - Logger configuration
   */
  constructor(config: LoggerConfig) {
    this.component = config.component;
    this.logDirectory = config.logDirectory;
    this.logLevel = config.logLevel ?? LogLevel.INFO;
    this.enableConsole = config.enableConsole ?? true;
    this.enableFile = config.enableFile ?? true;

    // Ensure log directory exists
    this.ensureLogDirectory();

    // Setup log file
    this.logFilePath = path.join(this.logDirectory, `${this.component}.csv`);
    if (this.enableFile) {
      this.initializeLogFile();
    }
  }

  /**
   * Ensures the log directory exists
   */
  private ensureLogDirectory(): void {
    if (!fs.existsSync(this.logDirectory)) {
      fs.mkdirSync(this.logDirectory, { recursive: true });
    }
  }

  /**
   * Initializes the log file with headers
   */
  private initializeLogFile(): void {
    const fileExists = fs.existsSync(this.logFilePath);

    this.logStream = fs.createWriteStream(this.logFilePath, {
      flags: "a",
      encoding: "utf8",
    });

    // Write header if file is new
    if (!fileExists || fs.statSync(this.logFilePath).size === 0) {
      this.logStream.write("timestamp,level,component,message,metadata\n");
    }
  }

  /**
   * Formats a log entry for CSV output
   * @param {LogEntry} entry - Log entry to format
   * @returns {string} Formatted CSV line
   */
  private formatCsvEntry(entry: LogEntry): string {
    const levelName = LogLevel[entry.level];
    const message = entry.message.replace(/"/g, '""'); // Escape quotes
    const metadata = entry.metadata
      ? JSON.stringify(entry.metadata).replace(/"/g, '""')
      : "";

    return `${entry.timestamp},"${levelName}","${entry.component}","${message}","${metadata}"\n`;
  }

  /**
   * Formats a log entry for console output
   * @param {LogEntry} entry - Log entry to format
   * @returns {string} Formatted console line
   */
  private formatConsoleEntry(entry: LogEntry): string {
    const levelName = LogLevel[entry.level].padEnd(5);
    const timestamp = new Date(entry.timestamp).toISOString();
    const metadataStr = entry.metadata
      ? ` | ${JSON.stringify(entry.metadata)}`
      : "";

    return `[${timestamp}] [${levelName}] [${this.component}] ${entry.message}${metadataStr}`;
  }

  /**
   * Writes a log entry
   * @param {LogLevel} level - Log level
   * @param {string} message - Log message
   * @param {Record<string, any>} [metadata] - Optional metadata
   */
  private log(
    level: LogLevel,
    message: string,
    metadata?: Record<string, any>,
  ): void {
    // Check log level threshold
    if (level < this.logLevel) {
      return;
    }

    const entry: LogEntry = {
      timestamp: Date.now(),
      level,
      component: this.component,
      message,
      metadata,
    };

    // Write to console
    if (this.enableConsole) {
      const consoleMsg = this.formatConsoleEntry(entry);
      if (level === LogLevel.ERROR) {
        console.error(consoleMsg);
      } else if (level === LogLevel.WARN) {
        console.warn(consoleMsg);
      } else {
        console.log(consoleMsg);
      }
    }

    // Write to file
    if (this.enableFile && this.logStream) {
      const csvEntry = this.formatCsvEntry(entry);
      this.logStream.write(csvEntry);
    }
  }

  /**
   * Logs a debug message
   * @param {string} message - Debug message
   * @param {Record<string, any>} [metadata] - Optional metadata
   */
  public debug(message: string, metadata?: Record<string, any>): void {
    this.log(LogLevel.DEBUG, message, metadata);
  }

  /**
   * Logs an info message
   * @param {string} message - Info message
   * @param {Record<string, any>} [metadata] - Optional metadata
   */
  public info(message: string, metadata?: Record<string, any>): void {
    this.log(LogLevel.INFO, message, metadata);
  }

  /**
   * Logs a warning message
   * @param {string} message - Warning message
   * @param {Record<string, any>} [metadata] - Optional metadata
   */
  public warn(message: string, metadata?: Record<string, any>): void {
    this.log(LogLevel.WARN, message, metadata);
  }

  /**
   * Logs an error message
   * @param {string} message - Error message
   * @param {Record<string, any>} [metadata] - Optional metadata
   */
  public error(message: string, metadata?: Record<string, any>): void {
    this.log(LogLevel.ERROR, message, metadata);
  }

  /**
   * Logs query registration
   * @param {string} query - The query being registered
   * @param {string} topic - The result topic
   */
  public logQueryRegistration(query: string, topic: string): void {
    this.info("Query registered", {
      query: query.substring(0, 100) + "...",
      topic,
    });
  }

  /**
   * Logs data processing
   * @param {number} count - Number of data points processed
   * @param {string} source - Data source
   */
  public logDataProcessing(count: number, source: string): void {
    this.info(`Processing data`, { count, source });
  }

  /**
   * Logs result generation
   * @param {any} result - The result value
   * @param {string} topic - The topic where result is published
   */
  public logResultGeneration(result: any, topic: string): void {
    this.info("Result generated", { result, topic });
  }

  /**
   * Logs result publication
   * @param {string} topic - The topic where result was published
   * @param {boolean} success - Whether publication was successful
   */
  public logResultPublication(topic: string, success: boolean): void {
    if (success) {
      this.info("Result published", { topic });
    } else {
      this.error("Result publication failed", { topic });
    }
  }

  /**
   * Logs MQTT connection status
   * @param {string} broker - MQTT broker URL
   * @param {boolean} connected - Connection status
   */
  public logMQTTConnection(broker: string, connected: boolean): void {
    if (connected) {
      this.info("MQTT connected", { broker });
    } else {
      this.warn("MQTT disconnected", { broker });
    }
  }

  /**
   * Logs MQTT subscription
   * @param {string} topic - Subscribed topic
   * @param {boolean} success - Whether subscription was successful
   */
  public logMQTTSubscription(topic: string, success: boolean): void {
    if (success) {
      this.info("MQTT subscribed", { topic });
    } else {
      this.error("MQTT subscription failed", { topic });
    }
  }

  /**
   * Logs window trigger
   * @param {number} windowStart - Window start timestamp
   * @param {number} windowEnd - Window end timestamp
   */
  public logWindowTrigger(windowStart: number, windowEnd: number): void {
    this.debug("Window triggered", {
      start: windowStart,
      end: windowEnd,
      duration: windowEnd - windowStart,
    });
  }

  /**
   * Logs aggregation computation
   * @param {string} aggregationType - Type of aggregation (MAX, AVG, etc.)
   * @param {any} result - Aggregation result
   * @param {number} inputCount - Number of inputs aggregated
   */
  public logAggregation(
    aggregationType: string,
    result: any,
    inputCount: number,
  ): void {
    this.info("Aggregation computed", {
      type: aggregationType,
      result,
      inputs: inputCount,
    });
  }

  /**
   * Closes the log stream
   */
  public close(): void {
    if (this.logStream) {
      this.logStream.end();
    }
  }

  /**
   * Gets the log file path
   * @returns {string} Path to the log file
   */
  public getLogFilePath(): string {
    return this.logFilePath;
  }

  /**
   * Reads the last N lines from the log file
   * @param {number} lines - Number of lines to read
   * @returns {string[]} Array of log lines
   */
  public readLastLines(lines: number = 10): string[] {
    if (!fs.existsSync(this.logFilePath)) {
      return [];
    }

    const content = fs.readFileSync(this.logFilePath, "utf8");
    const allLines = content.split("\n").filter((line) => line.trim());

    return allLines.slice(-lines);
  }

  /**
   * Gets log statistics
   * @returns {object} Log statistics
   */
  public getStats(): {
    totalLines: number;
    fileSize: number;
    filePath: string;
  } {
    if (!fs.existsSync(this.logFilePath)) {
      return { totalLines: 0, fileSize: 0, filePath: this.logFilePath };
    }

    const stats = fs.statSync(this.logFilePath);
    const content = fs.readFileSync(this.logFilePath, "utf8");
    const lines = content.split("\n").filter((line) => line.trim());

    return {
      totalLines: lines.length,
      fileSize: stats.size,
      filePath: this.logFilePath,
    };
  }
}

/**
 * Creates a logger for an orchestrator
 * @param {string} approachName - Name of the approach (e.g., "approximation", "fetching")
 * @param {LogLevel} [logLevel] - Log level threshold
 * @returns {UnifiedLogger} Logger instance
 */
export function createOrchestratorLogger(
  approachName: string,
  logLevel?: LogLevel,
): UnifiedLogger {
  return new UnifiedLogger({
    component: "orchestrator",
    logDirectory: path.join("logs", approachName),
    logLevel,
    enableConsole: true,
    enableFile: true,
  });
}

/**
 * Creates a logger for an operator
 * @param {string} approachName - Name of the approach
 * @param {string} operatorName - Name of the operator
 * @param {LogLevel} [logLevel] - Log level threshold
 * @returns {UnifiedLogger} Logger instance
 */
export function createOperatorLogger(
  approachName: string,
  operatorName: string,
  logLevel?: LogLevel,
): UnifiedLogger {
  return new UnifiedLogger({
    component: operatorName,
    logDirectory: path.join("logs", approachName),
    logLevel,
    enableConsole: true,
    enableFile: true,
  });
}

/**
 * Creates a logger for a subquery processor
 * @param {string} approachName - Name of the approach
 * @param {string} queryHash - Hash of the subquery
 * @param {LogLevel} [logLevel] - Log level threshold
 * @returns {UnifiedLogger} Logger instance
 */
export function createSubqueryLogger(
  approachName: string,
  queryHash: string,
  logLevel?: LogLevel,
): UnifiedLogger {
  return new UnifiedLogger({
    component: `subquery_${queryHash.substring(0, 8)}`,
    logDirectory: path.join("logs", approachName),
    logLevel,
    enableConsole: false, // Reduce console noise for subqueries
    enableFile: true,
  });
}
