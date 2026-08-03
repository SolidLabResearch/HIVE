#!/usr/bin/env node

const { execFileSync } = require("child_process");
const path = require("path");

const runnerPath = path.resolve(__dirname, "run-window-parameter-sensitivity.js");
const forwardedArgs = process.argv.slice(2);
const args = [
  runnerPath,
  "--iterations",
  "35",
  "--approaches",
  "fetching,approximation,chunked",
  "--patterns",
  "low_variability",
  ...forwardedArgs,
];

execFileSync("node", args, { stdio: "inherit" });
