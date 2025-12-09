/**
 * Logger class to calculate the resource used by the process in CPU and Memory.
 */
export class ResourceLogger { 
  private logFilePath : string;
  private intervalMs : number
  private logStream : import("fs").WriteStream;
  private intervalId : any;
  private writeHeader : boolean;
  
  /**
   * Constructor to initialize the ResourceLogger.
   * @param {string} filePath - Path to the CSV file.
   * @param {number} intervalMs - Interval in milliseconds for logging.
   */
  constructor(filePath: string, intervalMs: number){
    this.logFilePath = filePath;
    this.intervalMs = intervalMs;
    this.writeHeader = !require('fs').existsSync(this.logFilePath);
    this.logStream = require('fs').createWriteStream(this.logFilePath, { flags: 'a' });
  }
  
  /**
   * Writing the resource usage, CPU and Memory usage to a file.
   * @returns {void} - Nothing.
   */
  startResourceUsageLogging() : void { 
    if (this.writeHeader){
      this.logStream.write('timestamp,cpu_user,cpu_system,rss,heapTotal,heapUsed,heapUsedMB,external\n');
      this.writeHeader = false;
    }
    else {
      console.log("The write header doesn't exist, something went wrong.")
    }
    
    setInterval(() => {
      
      const memoryUsage = process.memoryUsage();
      const cpuUsage  = process.cpuUsage();
      const time_now = Date.now();
      const line_to_write = [
        time_now,
        (cpuUsage.user / 1000).toFixed(2),
        (cpuUsage.system / 1000).toFixed(2),
        memoryUsage.rss,
        memoryUsage.heapTotal,
        memoryUsage.heapUsed,
        (memoryUsage.heapUsed / 1024 / 1024).toFixed(2),
        memoryUsage.external
        ].join(',') + '\n';
      this.logStream.write(line_to_write);
  
    }, this.intervalMs);
  }
}