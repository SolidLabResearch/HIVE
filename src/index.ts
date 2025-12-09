#!/usr/bin/env node
/**
 * Minimal CLI bootstrap for the Streaming Query Hive application.
 *
 * - Build: `npm run build` will compile this to `dist/index.js`
 * - Start (prod): `npm start` runs `node dist/index.js`
 * - Dev: `npx ts-node src/index.ts ...`.
 *
 * Responsibilities:
 *  - Parse CLI args (commander)
 *  - Create an IntelligentOrchestrator with the requested operator and analysis flag
 *  - Optionally pre-analyze streams before running a registered query
 *  - Run the registered query intelligently (or force an approach)
 *  - Provide basic graceful shutdown handling.
 */

import { program } from "commander";
import { IntelligentOrchestrator } from "./orchestrator/IntelligentOrchestrator";

program
  .name("streaming-query-hive")
  .description("CLI bootstrap for the Streaming Query Hive orchestrator")
  .option("-o, --operator <type>", "operator type", "streaming-query-hive")
  .option(
    "-m, --mode <mode>",
    "analysis mode (automatic|manual|hybrid)",
    "automatic",
  )
  .option("--no-analysis", "disable stream analysis")
  .option("-r, --register <query>", "register an output query to run")
  .option(
    "-f, --force-approach <approach>",
    "force a specific approach (overrides recommendation)",
  )
  .option(
    "--preanalyze [complexity]",
    "perform pre-analysis and cache a recommendation (optional complexity 1-10)",
    (v) => parseInt(v as string, 10),
    5,
  )
  .option("-v, --verbose", "enable verbose logging", false)
  .parse(process.argv);

const opts = program.opts() as {
  operator: string;
  mode: "automatic" | "manual" | "hybrid" | string;
  analysis?: boolean;
  register?: string;
  forceApproach?: string;
  preanalyze?: number | boolean;
  verbose?: boolean;
};

const enableAnalysis = opts.analysis !== false;
const operatorType = opts.operator || "streaming-query-hive";
const analysisMode =
  (opts.mode as "automatic" | "manual" | "hybrid") || "automatic";
const verbose = !!opts.verbose;

/**
 * Boot and run the orchestrator according to provided options.
 */
async function main(): Promise<void> {
  if (verbose) {
    console.log("Options:", {
      operatorType,
      analysisMode,
      enableAnalysis,
      register: opts.register,
      forceApproach: opts.forceApproach,
      preanalyze: opts.preanalyze,
    });
  }

  const orchestrator = new IntelligentOrchestrator(
    operatorType,
    enableAnalysis,
  );

  // Set analysis mode if provided
  if (analysisMode) {
    orchestrator.setAnalysisMode(
      analysisMode as "automatic" | "manual" | "hybrid",
    );
  }

  // If preanalyze was requested: perform pre-analysis and cache a recommendation
  if (opts.preanalyze !== undefined && opts.preanalyze !== false) {
    // opts.preanalyze might be a boolean true when flag provided without value; default complexity 5
    const complexity =
      typeof opts.preanalyze === "number"
        ? Math.max(1, Math.min(10, opts.preanalyze))
        : 5;
    if (enableAnalysis) {
      if (verbose)
        console.log(`Running pre-analysis (complexity=${complexity})...`);
      try {
        const rec = await orchestrator.preAnalyzeStreams(complexity);
        if (verbose) {
          if (rec) {
            console.log(
              "Pre-analysis recommendation:",
              rec.recommendedApproach,
              `(confidence=${(rec.confidence * 100).toFixed(1)}%)`,
            );
          } else {
            console.log(
              "Pre-analysis completed but no recommendation produced.",
            );
          }
        }
      } catch (err) {
        console.error("Pre-analysis failed:", err);
      }
    } else {
      console.log("Pre-analysis skipped: stream analysis is disabled.");
    }
  }

  // Register an output query if provided
  if (opts.register) {
    orchestrator.registerOutputQuery(opts.register);
  }

  // If register was provided, run it intelligently (or forced approach)
  if (opts.register) {
    try {
      if (opts.forceApproach) {
        if (verbose) console.log(`Forcing approach: ${opts.forceApproach}`);
        await orchestrator.runRegisteredQueryIntelligent(opts.forceApproach);
      } else {
        await orchestrator.runRegisteredQueryIntelligent();
      }
    } catch (err) {
      console.error("Error running registered query:", err);
    }
  } else {
    if (verbose) {
      console.log(
        "No registered query supplied. Orchestrator started and ready.",
      );
    }
  }

  // If we didn't run anything that keeps the process alive, keep running to allow HTTP server or agents to operate.
  // IntelligentOrchestrator starts an HTTP server internally (per its constructor), so we just wait here.
  console.log("Orchestrator bootstrapped. Press Ctrl-C to exit.");
}

// Graceful shutdown
/**
 *
 */
function setupShutdown(): void {
  const shutdown = (signal: string) => {
    console.log(`Received ${signal}. Shutting down...`);
    // If there were async cleanup tasks (closing servers, flushing logs, etc.), do them here.
    // For now, exit immediately.
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("uncaughtException", (err) => {
    console.error("Uncaught exception:", err);
    process.exit(1);
  });
  process.on("unhandledRejection", (reason) => {
    console.error("Unhandled rejection:", reason);
  });
}

setupShutdown();

main().catch((err) => {
  console.error("Fatal error during bootstrap:", err);
  process.exit(1);
});
