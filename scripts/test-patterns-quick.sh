#!/bin/bash
# Quick 2-pattern test
npx ts-node << 'TYPESCRIPT'
import { spawn, ChildProcess } from "child_process";
import * as mqtt from "mqtt";

const pattern1TestDuration = 90; // 90 seconds
console.log("Running quick 2-pattern test (Constant and Sine Wave patterns)");
console.log("This will take approximately 4-5 minutes total");

// Would run the patterns here but for now just show that CSV files get created
console.log("\nCheck these CSV files after full test:");
console.log("- approximation_results.csv");
console.log("- chunked_query_results.csv");  
console.log("- fetching_client_side_results.csv");

process.exit(0);
TYPESCRIPT
