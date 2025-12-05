#!/usr/bin/env ts-node

/**
 * Setup and validation script for frequency-based streaming query experiments
 *
 * This TypeScript script validates the environment and prepares for running
 * experiments across different approaches and frequencies.
 */

import * as fs from "fs";
import * as path from "path";
import { execSync } from "child_process";

interface FrequencyDataInfo {
  frequency: string;
  deviceType: "smartphone" | "wearable";
  dataPath: string;
  expectedObservations: number;
  exists: boolean;
}

interface ValidationResult {
  success: boolean;
  message: string;
  details?: any;
}

class FrequencyExperimentSetup {
  private readonly projectRoot: string;
  private readonly dataBasePath: string;
  private readonly frequencies = [
    "4Hz",
    "8Hz",
    "16Hz",
    "32Hz",
    "64Hz",
    "128Hz",
  ];
  private readonly deviceTypes: ("smartphone" | "wearable")[] = [
    "smartphone",
    "wearable",
  ];
  private readonly approaches = [
    "fetching-client-side",
    "streaming-query-hive",
    "chunked-approach",
    "approximation-approach",
  ];

  constructor() {
    this.projectRoot = path.resolve(__dirname, "../..");
    this.dataBasePath = path.join(
      this.projectRoot,
      "src/streamer/data/frequency_variants/2mins",
    );
  }

  /**
   * Main setup and validation routine
   */
  public async run(): Promise<void> {
    console.log(" Frequency-Based Streaming Query Experiment Setup");
    console.log("=".repeat(60));
    console.log(`Project Root: ${this.projectRoot}`);
    console.log(`Data Path: ${this.dataBasePath}`);
    console.log("");

    try {
      // Step 1: Validate project structure
      console.log(" Validating project structure...");
      const structureResult = this.validateProjectStructure();
      this.printResult("Project Structure", structureResult);

      // Step 2: Validate frequency data
      console.log("\n Validating frequency datasets...");
      const dataResult = this.validateFrequencyData();
      this.printResult("Frequency Data", dataResult);

      // Step 3: Check TypeScript compilation
      console.log("\n Checking TypeScript compilation...");
      const compileResult = this.validateTypeScriptCompilation();
      this.printResult("TypeScript Compilation", compileResult);

      // Step 4: Validate approach files
      console.log("\n Validating approach implementations...");
      const approachResult = this.validateApproaches();
      this.printResult("Approach Files", approachResult);

      // Step 5: Create output directories
      console.log("\n Creating experiment output directories...");
      const outputResult = this.createOutputDirectories();
      this.printResult("Output Directories", outputResult);

      // Step 6: Generate experiment configuration
      console.log("\n  Generating experiment configuration...");
      const configResult = this.generateExperimentConfig();
      this.printResult("Experiment Configuration", configResult);

      console.log("\n" + "=".repeat(60));
      console.log("[OK] Setup completed successfully!");
      console.log("\nNext steps:");
      console.log("1. Run: npm run build (if not already compiled)");
      console.log(
        "2. Run: ts-node scripts/benchmarks/run-frequency-experiment.ts",
      );
      console.log(
        "3. Run: ts-node scripts/analysis/analyze-frequency-results.ts",
      );
    } catch (error) {
      console.error("\n[FAIL] Setup failed:", error);
      process.exit(1);
    }
  }

  /**
   * Validate basic project structure
   */
  private validateProjectStructure(): ValidationResult {
    const requiredPaths = [
      "src",
      "src/approaches",
      "src/orchestrator",
      "scripts/benchmarks",
      "src/streamer/data/frequency_variants/2mins",
    ];

    const missing: string[] = [];

    for (const requiredPath of requiredPaths) {
      const fullPath = path.join(this.projectRoot, requiredPath);
      if (!fs.existsSync(fullPath)) {
        missing.push(requiredPath);
      }
    }

    if (missing.length > 0) {
      return {
        success: false,
        message: "Missing required directories",
        details: missing,
      };
    }

    return {
      success: true,
      message: "All required directories exist",
    };
  }

  /**
   * Validate frequency datasets exist and have expected structure
   */
  private validateFrequencyData(): ValidationResult {
    const dataInfo: FrequencyDataInfo[] = [];
    const expectedCounts = {
      "4Hz": 480,
      "8Hz": 960,
      "16Hz": 1920,
      "32Hz": 3840,
      "64Hz": 7679,
      "128Hz": { smartphone: 15358, wearable: 15359 },
    };

    let allValid = true;
    const issues: string[] = [];

    for (const frequency of this.frequencies) {
      for (const deviceType of this.deviceTypes) {
        const dataPath = path.join(
          this.dataBasePath,
          deviceType,
          frequency,
          "data.nt",
        );
        const exists = fs.existsSync(dataPath);

        let expectedObs =
          expectedCounts[frequency as keyof typeof expectedCounts];
        if (typeof expectedObs === "object") {
          expectedObs = expectedObs[deviceType];
        }

        const info: FrequencyDataInfo = {
          frequency,
          deviceType,
          dataPath,
          expectedObservations: expectedObs as number,
          exists,
        };

        dataInfo.push(info);

        if (!exists) {
          allValid = false;
          issues.push(`Missing: ${deviceType}/${frequency}/data.nt`);
        } else {
          // Validate observation count
          try {
            const lineCount = this.countLines(dataPath);
            if (lineCount !== info.expectedObservations) {
              issues.push(
                `${deviceType}/${frequency}: Expected ${info.expectedObservations} obs, found ${lineCount}`,
              );
            }
          } catch (error) {
            issues.push(`Error reading ${deviceType}/${frequency}: ${error}`);
            allValid = false;
          }
        }
      }
    }

    return {
      success: allValid && issues.length === 0,
      message: allValid
        ? "All frequency datasets validated"
        : "Dataset validation issues found",
      details: { dataInfo, issues },
    };
  }

  /**
   * Check if TypeScript can be compiled
   */
  private validateTypeScriptCompilation(): ValidationResult {
    try {
      const tsConfigPath = path.join(this.projectRoot, "tsconfig.json");
      if (!fs.existsSync(tsConfigPath)) {
        return {
          success: false,
          message: "tsconfig.json not found",
        };
      }

      // Try to compile (dry run)
      execSync("npx tsc --noEmit", {
        cwd: this.projectRoot,
        stdio: "pipe",
      });

      return {
        success: true,
        message: "TypeScript compilation successful",
      };
    } catch (error) {
      return {
        success: false,
        message: "TypeScript compilation failed",
        details: error,
      };
    }
  }

  /**
   * Validate that all approach files exist
   */
  private validateApproaches(): ValidationResult {
    const approachFiles = [
      "src/approaches/FetchingClientSideApproachOrchestrator.ts",
      "src/approaches/ChunkedQueryApproachOrchestrator.ts",
      "src/approaches/ApproximationApproachOrchestrator.ts",
    ];

    const missing: string[] = [];

    for (const filePath of approachFiles) {
      const fullPath = path.join(this.projectRoot, filePath);
      if (!fs.existsSync(fullPath)) {
        missing.push(filePath);
      }
    }

    if (missing.length > 0) {
      return {
        success: false,
        message: "Missing approach files",
        details: missing,
      };
    }

    return {
      success: true,
      message: "All approach files exist",
    };
  }

  /**
   * Create necessary output directories for experiments
   */
  private createOutputDirectories(): ValidationResult {
    try {
      const outputDirs = [
        "results/frequency-experiments",
        "results/frequency-experiments/fetching-client-side",
        "results/frequency-experiments/streaming-query-hive",
        "results/frequency-experiments/chunked-approach",
        "results/frequency-experiments/approximation-approach",
        "results/frequency-experiments/analysis",
      ];

      for (const dir of outputDirs) {
        const fullPath = path.join(this.projectRoot, dir);
        fs.mkdirSync(fullPath, { recursive: true });
      }

      return {
        success: true,
        message: "Output directories created",
      };
    } catch (error) {
      return {
        success: false,
        message: "Failed to create output directories",
        details: error,
      };
    }
  }

  /**
   * Generate experiment configuration file
   */
  private generateExperimentConfig(): ValidationResult {
    try {
      const config = {
        experiment: {
          name: "Frequency-Based Streaming Query Comparison",
          description:
            "Compare different streaming query approaches across multiple frequencies",
          timestamp: new Date().toISOString(),
          iterations: 5,
        },
        frequencies: this.frequencies,
        deviceTypes: this.deviceTypes,
        approaches: this.approaches,
        dataBasePath: this.dataBasePath,
        outputPath: path.join(
          this.projectRoot,
          "results/frequency-experiments",
        ),
        queries: {
          windowSize: 1000,
          avgAcceleration: {
            name: "Average Acceleration",
            sparql: `
                            PREFIX saref: <https://saref.etsi.org/core/>
                            PREFIX dahcc: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
                            SELECT (AVG(?value) as ?avgAcceleration)
                            WHERE {
                                ?obs saref:hasValue ?value .
                                ?obs saref:relatesToProperty dahcc:smartphoneX .
                            }
                        `,
          },
        },
        metrics: {
          latency: "Response time per query (ms)",
          memory: "Peak memory usage (MB)",
          accuracy: "Deviation from ground truth (%)",
          throughput: "Observations processed per second",
        },
      };

      const configPath = path.join(
        this.projectRoot,
        "scripts/benchmarks/frequency-experiment-config.json",
      );
      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

      return {
        success: true,
        message: "Experiment configuration generated",
        details: { configPath },
      };
    } catch (error) {
      return {
        success: false,
        message: "Failed to generate configuration",
        details: error,
      };
    }
  }

  /**
   * Count lines in a file
   */
  private countLines(filePath: string): number {
    const content = fs.readFileSync(filePath, "utf8");
    return content.split("\n").filter((line) => line.trim().length > 0).length;
  }

  /**
   * Print validation result with formatting
   */
  private printResult(step: string, result: ValidationResult): void {
    const status = result.success ? "[OK]" : "[FAIL]";
    console.log(`${status} ${step}: ${result.message}`);

    if (result.details && !result.success) {
      if (Array.isArray(result.details)) {
        result.details.forEach((detail) => console.log(`   - ${detail}`));
      } else if (typeof result.details === "object") {
        console.log(`   Details: ${JSON.stringify(result.details, null, 2)}`);
      } else {
        console.log(`   Details: ${result.details}`);
      }
    }
  }
}

// Run the setup if this file is executed directly
if (require.main === module) {
  const setup = new FrequencyExperimentSetup();
  setup.run().catch((error) => {
    console.error("Setup failed:", error);
    process.exit(1);
  });
}

export { FrequencyExperimentSetup };
