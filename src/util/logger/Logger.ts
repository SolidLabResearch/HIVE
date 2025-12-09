import * as fs from "fs";
import { LogLevel, LogDestination } from "./LoggerEnum";

/**
 *
 */
export class Logger {
  private log_level: LogLevel;
  private loggable_classes: string[];
  private log_destination: any;

  /**
   * Creates a new Logger instance.
   * @param {LogLevel} logLevel - The minimum log level to output.
   * @param {string[]} loggableClasses - Array of class names that are allowed to log.
   * @param {LogDestination} logDestination - The destination for log output (CONSOLE or FILE).
   */
  constructor(
    logLevel: LogLevel,
    loggableClasses: string[],
    logDestination: LogDestination,
  ) {
    this.log_level = logLevel;
    this.loggable_classes = loggableClasses;
    this.log_destination = logDestination;
    console.log(
      `Logger initialized with log level ${this.log_level}, loggable classes ${this.loggable_classes}, and log destination ${this.log_destination}`,
    );
  }

  /**
   * Sets the minimum log level for this logger.
   * @param {LogLevel} logLevel - The minimum log level to output.
   */
  setLogLevel(logLevel: LogLevel) {
    this.log_level = logLevel;
  }

  /**
   * Sets the array of class names that are allowed to log.
   * @param {string[]} loggableClasses - Array of class names that are allowed to log.
   */
  setLoggableClasses(loggableClasses: string[]) {
    this.loggable_classes = loggableClasses;
  }

  /**
   * Sets the destination for log output.
   * @param {LogDestination} logDestination - The destination for log output (CONSOLE or FILE).
   */
  setLogDestination(logDestination: LogDestination) {
    this.log_destination = logDestination;
  }

  /**
   * Logs a message at the specified level for a given class.
   * @param {LogLevel} level - The log level for this message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  log(level: LogLevel, message: string, className: string) {
    if (level >= this.log_level && this.loggable_classes.includes(className)) {
      const _logPrefix = `[${LogLevel[level]}] [${className}]`;
      const logMessage = `${Date.now()},${message}`;
      switch (this.log_destination) {
        case "CONSOLE":
          console.log(logMessage);
          break;
        case "FILE":
          try {
            fs.appendFileSync(`./logs/${className}.log`, `${logMessage}\n`);
          } catch (error) {
            console.error(`Error writing to file: ${error}`);
          }
          break;
        default:
          console.log(`Invalid log destination: ${this.log_destination}`);
      }
    }
  }

  /**
   * Logs a TRACE level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  trace(message: string, className: string) {
    this.log(LogLevel.TRACE, message, className);
  }

  /**
   * Logs a DEBUG level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  debug(message: string, className: string) {
    this.log(LogLevel.DEBUG, message, className);
  }

  /**
   * Logs an INFO level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  info(message: string, className: string) {
    this.log(LogLevel.INFO, message, className);
  }

  /**
   * Logs a CONFIG level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  config(message: string, className: string) {
    this.log(LogLevel.CONFIG, message, className);
  }

  /**
   * Logs a WARN level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  warn(message: string, className: string) {
    this.log(LogLevel.WARN, message, className);
  }

  /**
   * Logs an ERROR level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  error(message: string, className: string) {
    this.log(LogLevel.ERROR, message, className);
  }

  /**
   * Logs a FATAL level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  fatal(message: string, className: string) {
    this.log(LogLevel.FATAL, message, className);
  }

  /**
   * Logs a SEVERE level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  severe(message: string, className: string) {
    this.log(LogLevel.SEVERE, message, className);
  }

  /**
   * Logs an AUDIT level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  audit(message: string, className: string) {
    this.log(LogLevel.AUDIT, message, className);
  }

  /**
   * Logs a STATS level message.
   * @param {string} message - The message to log.
   * @param {string} className - The name of the class logging this message.
   */
  stats(message: string, className: string) {
    this.log(LogLevel.STATS, message, className);
  }

  /**
   * Creates and returns a new Logger instance with the specified settings.
   * @param {LogLevel} logLevel - The minimum log level to output.
   * @param {string[]} loggableClasses - Array of class names that are allowed to log.
   * @param {LogDestination} logDestination - The destination for log output (CONSOLE or FILE).
   * @returns {Logger} A new Logger instance with the specified settings.
   */
  static getLogger(
    logLevel: LogLevel,
    loggableClasses: string[],
    logDestination: LogDestination,
  ) {
    return new Logger(logLevel, loggableClasses, logDestination);
  }

  /**
   * Creates and returns a Logger instance with default settings.
   * @returns {Logger} A Logger instance with INFO level and CONSOLE destination.
   */
  static getLoggerWithDefaults() {
    return new Logger(LogLevel.INFO, [], LogDestination.CONSOLE);
  }
}
