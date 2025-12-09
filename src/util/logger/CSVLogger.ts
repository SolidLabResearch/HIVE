import fs from "fs";
import path from "path";

/**
 * A logger that writes data to a CSV file.
 */
export class CSVLogger {
  private stream: fs.WriteStream;

  /**
   * Creates a new CSVLogger instance.
   * @param {string} filePath - The path to the CSV file to write to.
   */
  constructor(filePath: string) {
    // Check for custom log directory from environment variable
    let fullPath = filePath;
    if (process.env.CUSTOM_LOG_DIR) {
      // Ensure the custom log directory exists
      if (!fs.existsSync(process.env.CUSTOM_LOG_DIR)) {
        fs.mkdirSync(process.env.CUSTOM_LOG_DIR, { recursive: true });
      }
      fullPath = path.join(process.env.CUSTOM_LOG_DIR, filePath);
    }

    this.stream = fs.createWriteStream(fullPath, { flags: "a" });
    this.stream.write("timestamp,message\n"); // Write header
  }

  /**
   * Logs data to the CSV file.
   * @param {any} data - The data to log, will be JSON stringified.
   */
  log(data: any) {
    const timestamp = Date.now();
    this.stream.write(`${timestamp},${JSON.stringify(data)}\n`);
  }

  /**
   * Closes the CSV file stream.
   */
  close() {
    this.stream.end();
  }
}
