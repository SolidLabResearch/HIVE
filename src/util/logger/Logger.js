"use strict";
exports.__esModule = true;
exports.Logger = void 0;
var fs = require("fs");
var LoggerEnum_1 = require("./LoggerEnum");
/**
 *
 */
var Logger = /** @class */ (function () {
    /**
     *
     * @param logLevel
     * @param loggableClasses
     * @param logDestination
     */
    function Logger(logLevel, loggableClasses, logDestination) {
        this.log_level = logLevel;
        this.loggable_classes = loggableClasses;
        this.log_destination = logDestination;
        this.max_log_file_size_bytes = this.getMaxFileSizeBytes();
        console.log("Logger initialized with log level ".concat(this.log_level, ", loggable classes ").concat(this.loggable_classes, ", and log destination ").concat(this.log_destination));
    }
    Logger.prototype.getMaxFileSizeBytes = function () {
        var envValue = process.env.LOG_MAX_FILE_SIZE_MB;
        var parsed = envValue ? Number(envValue) : NaN;
        var maxSizeMb = Number.isFinite(parsed) && parsed > 0 ? parsed : 256;
        return Math.floor(maxSizeMb * 1024 * 1024);
    };
    Logger.prototype.appendToClassFile = function (className, logMessage) {
        var logsDir = './logs';
        var logPath = "".concat(logsDir, "/").concat(className, ".log");
        var rotatedPath = "".concat(logPath, ".1");
        var line = "".concat(logMessage, "\n");
        var incomingBytes = Buffer.byteLength(line, 'utf8');
        fs.mkdirSync(logsDir, { recursive: true });
        if (fs.existsSync(logPath)) {
            var currentSize = fs.statSync(logPath).size;
            if (currentSize + incomingBytes > this.max_log_file_size_bytes) {
                if (fs.existsSync(rotatedPath)) {
                    fs.unlinkSync(rotatedPath);
                }
                fs.renameSync(logPath, rotatedPath);
                fs.writeFileSync(logPath, "".concat(Date.now(), ",log_file_rotated,max_bytes=").concat(this.max_log_file_size_bytes, "\n"));
            }
        }
        fs.appendFileSync(logPath, line);
    };
    /**
     *
     * @param logLevel
     */
    Logger.prototype.setLogLevel = function (logLevel) {
        this.log_level = logLevel;
    };
    /**
     *
     * @param loggableClasses
     */
    Logger.prototype.setLoggableClasses = function (loggableClasses) {
        this.loggable_classes = loggableClasses;
    };
    /**
     *
     * @param logDestination
     */
    Logger.prototype.setLogDestination = function (logDestination) {
        this.log_destination = logDestination;
    };
    /**
     *
     * @param level
     * @param message
     * @param className
     */
    Logger.prototype.log = function (level, message, className) {
        if (level >= this.log_level && this.loggable_classes.includes(className)) {
            var logMessage = "".concat(Date.now(), ",").concat(message);
            switch (this.log_destination) {
                case 'CONSOLE':
                    console.log(logMessage);
                    break;
                case 'FILE':
                    try {
                        this.appendToClassFile(className, logMessage);
                    }
                    catch (error) {
                        console.error("Error writing to file: ".concat(error));
                    }
                    break;
                default:
                    console.log("Invalid log destination: ".concat(this.log_destination));
            }
        }
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.trace = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.TRACE, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.debug = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.DEBUG, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.info = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.INFO, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.config = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.CONFIG, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.warn = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.WARN, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.error = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.ERROR, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.fatal = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.FATAL, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.severe = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.SEVERE, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.audit = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.AUDIT, message, className);
    };
    /**
     *
     * @param message
     * @param className
     */
    Logger.prototype.stats = function (message, className) {
        this.log(LoggerEnum_1.LogLevel.STATS, message, className);
    };
    /**
     *
     * @param logLevel
     * @param loggableClasses
     * @param logDestination
     */
    Logger.getLogger = function (logLevel, loggableClasses, logDestination) {
        return new Logger(logLevel, loggableClasses, logDestination);
    };
    /**
     *
     */
    Logger.getLoggerWithDefaults = function () {
        return new Logger(LoggerEnum_1.LogLevel.INFO, [], LoggerEnum_1.LogDestination.CONSOLE);
    };
    return Logger;
}());
exports.Logger = Logger;
