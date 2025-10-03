import fs from 'fs';
import path from 'path';

/**
 *
 */
export class CSVLogger {

    private stream: fs.WriteStream;

    /**
     *
     * @param filePath
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
        
        this.stream = fs.createWriteStream(fullPath, { flags: 'a' });
        this.stream.write('timestamp,message\n'); // Write header
    }

    /**
     *
     * @param data
     */
    log(data: any) {
        const timestamp = Date.now();
        this.stream.write(`${timestamp},${JSON.stringify(data)}\n`);
    }

    /**
     *
     */
    close() {
        this.stream.end();
    }
}