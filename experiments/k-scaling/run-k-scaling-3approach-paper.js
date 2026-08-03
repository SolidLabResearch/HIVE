#!/usr/bin/env node

const { execFileSync } = require("child_process");
const path = require("path");

const runnerPath = path.resolve(
  __dirname,
  "run-k-scaling-3approach-local-smoke.js",
);

const forwardedArgs = process.argv.slice(2);
const args = [runnerPath, "--iterations", "35", ...forwardedArgs];

execFileSync("node", args, { stdio: "inherit" });
