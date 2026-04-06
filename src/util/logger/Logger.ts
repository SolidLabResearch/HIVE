import * as fs from 'fs';
import { LogLevel, LogDestination } from './LoggerEnum';

/**
 *
 */
export class Logger {
    private log_level: LogLevel;
    private loggable_classes: string[];
    private log_destination: any;
    private max_log_file_size_bytes: number;

    /**
     *
     * @param logLevel
     * @param loggableClasses
     * @param logDestination
     */
    constructor(logLevel: LogLevel, loggableClasses: string[], logDestination: any) {
        this.log_level = logLevel;
        this.loggable_classes = loggableClasses;
        this.log_destination = logDestination;
        this.max_log_file_size_bytes = this.getMaxFileSizeBytes();
        console.log(`Logger initialized with log level ${this.log_level}, loggable classes ${this.loggable_classes}, and log destination ${this.log_destination}`);

    }

    private getMaxFileSizeBytes(): number {
        const envValue = process.env.LOG_MAX_FILE_SIZE_MB;
        const parsed = envValue ? Number(envValue) : NaN;
        const maxSizeMb = Number.isFinite(parsed) && parsed > 0 ? parsed : 256;
        return Math.floor(maxSizeMb * 1024 * 1024);
    }

    private appendToClassFile(className: string, logMessage: string): void {
        const logsDir = './logs';
        const logPath = `${logsDir}/${className}.log`;
        const rotatedPath = `${logPath}.1`;
        const line = `${logMessage}\n`;
        const incomingBytes = Buffer.byteLength(line, 'utf8');

        fs.mkdirSync(logsDir, { recursive: true });

        if (fs.existsSync(logPath)) {
            const currentSize = fs.statSync(logPath).size;
            if (currentSize + incomingBytes > this.max_log_file_size_bytes) {
                if (fs.existsSync(rotatedPath)) {
                    fs.unlinkSync(rotatedPath);
                }
                fs.renameSync(logPath, rotatedPath);
                fs.writeFileSync(logPath, `${Date.now()},log_file_rotated,max_bytes=${this.max_log_file_size_bytes}\n`);
            }
        }

        fs.appendFileSync(logPath, line);
    }

    /**
     *
     * @param logLevel
     */
    setLogLevel(logLevel: LogLevel) {
        this.log_level = logLevel;
    }

    /**
     *
     * @param loggableClasses
     */
    setLoggableClasses(loggableClasses: string[]) {
        this.loggable_classes = loggableClasses;
    }

    /**
     *
     * @param logDestination
     */
    setLogDestination(logDestination: LogDestination) {
        this.log_destination = logDestination;
    }

    /**
     *
     * @param level
     * @param message
     * @param className
     */
    log(level: LogLevel, message: string, className: string) {
        if (level >= this.log_level && this.loggable_classes.includes(className)) {
            const logMessage = `${Date.now()},${message}`;
            switch (this.log_destination) {
                case 'CONSOLE':
                    console.log(logMessage);
                    break;
                case 'FILE':
                    try {
                        this.appendToClassFile(className, logMessage);
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
     *
     * @param message
     * @param className
     */
    trace(message: string, className: string) {
        this.log(LogLevel.TRACE, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    debug(message: string, className: string) {
        this.log(LogLevel.DEBUG, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    info(message: string, className: string) {
        this.log(LogLevel.INFO, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    config(message: string, className: string) {
        this.log(LogLevel.CONFIG, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    warn(message: string, className: string) {
        this.log(LogLevel.WARN, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    error(message: string, className: string) {
        this.log(LogLevel.ERROR, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    fatal(message: string, className: string) {
        this.log(LogLevel.FATAL, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    severe(message: string, className: string) {
        this.log(LogLevel.SEVERE, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    audit(message: string, className: string) {
        this.log(LogLevel.AUDIT, message, className);
    }

    /**
     *
     * @param message
     * @param className
     */
    stats(message: string, className: string) {
        this.log(LogLevel.STATS, message, className);
    }

    /**
     *
     * @param logLevel
     * @param loggableClasses
     * @param logDestination
     */
    static getLogger(logLevel: LogLevel, loggableClasses: string[], logDestination: LogDestination) {
        return new Logger(logLevel, loggableClasses, logDestination);
    }

    /**
     *
     */
    static getLoggerWithDefaults() {
        return new Logger(LogLevel.INFO, [], LogDestination.CONSOLE);
    }
}
