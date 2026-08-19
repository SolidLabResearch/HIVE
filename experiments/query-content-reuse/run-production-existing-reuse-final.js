#!/usr/bin/env node
/* Sequential, orchestration-only wrapper for the frozen Experiment 2 workload. */
const { execFileSync, execSync } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");

const ROOT = path.resolve(__dirname, "..", "..");
const RUNNER = path.join(__dirname, "run-production-different-things-scaling.js");
const EXPECTED_RSPJS_SHA = "0039e59fcad7a7b6472f7bbd6b0b915c39e335f5";
const DEFAULT_TARGETS = [2, 4, 8, 16];
const DEFAULT_APPROACHES = ["fetching", "approximation", "chunked"];
const RSP_JS_PATH = process.env.RSP_JS_PATH || "/Users/kushbisen/Code/RSP-JS";

function command(command) { return execSync(command, { cwd: ROOT, encoding: "utf8" }).trim(); }
function sha(value) { return crypto.createHash("sha256").update(value).digest("hex"); }
function mkdir(dir) { fs.mkdirSync(dir, { recursive: true }); }
function write(file, value) { mkdir(path.dirname(file)); fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`); }
function timestamp() { return new Date().toISOString().replace(/[:.]/g, "-"); }
function parse(argv) {
  const args = { targets: DEFAULT_TARGETS, approaches: DEFAULT_APPROACHES, iterations: 35, timeoutMs: 180000, resultRoot: null, smoke: false };
  for (let i = 0; i < argv.length; i += 1) {
    const value = argv[i + 1];
    if (argv[i] === "--targets") { args.targets = value.split(",").map(Number); i += 1; }
    else if (argv[i] === "--approaches") { args.approaches = value.split(",").map((entry) => entry.trim().toLowerCase()).filter(Boolean); i += 1; }
    else if (argv[i] === "--iterations") { args.iterations = Number(value); i += 1; }
    else if (argv[i] === "--timeout-ms") { args.timeoutMs = Number(value); i += 1; }
    else if (argv[i] === "--result-root") { args.resultRoot = path.resolve(value); i += 1; }
    else if (argv[i] === "--smoke") args.smoke = true;
    else throw new Error(`Unknown argument: ${argv[i]}`);
  }
  if (!args.targets.every((value) => DEFAULT_TARGETS.includes(value))) throw new Error("Targets must be a subset of 2,4,8,16");
  if (!args.approaches.every((value) => DEFAULT_APPROACHES.includes(value))) throw new Error("Approaches must be fetching,approximation,chunked");
  if (!Number.isInteger(args.iterations) || args.iterations < 1) throw new Error("--iterations must be a positive integer");
  return args;
}
function assertPreflight() {
  const rsp = command(`git -C ${JSON.stringify(RSP_JS_PATH)} rev-parse HEAD`);
  if (rsp !== EXPECTED_RSPJS_SHA) throw new Error(`RSP-JS SHA mismatch: ${rsp}`);
  const port = command("lsof -nP -iTCP:8080 -sTCP:LISTEN -t || true");
  if (port) throw new Error(`Port 8080 is occupied by ${port}`);
}
function latestRunnerRoot(before) {
  const parent = path.join(ROOT, "results", "query-content-reuse");
  const roots = fs.readdirSync(parent).filter((name) => name.startsWith("production-different-things-scaling-")).map((name) => path.join(parent, name)).filter((entry) => !before.has(entry));
  if (roots.length !== 1) throw new Error(`Expected one canonical runner root, found ${roots.length}`);
  return roots[0];
}
function main() {
  const args = parse(process.argv.slice(2));
  assertPreflight();
  const resultRoot = args.resultRoot || path.join(ROOT, "results", "paper-benchmarks", `experiment2-existing-reuse-final-${timestamp()}`);
  if (fs.existsSync(resultRoot) && fs.readdirSync(resultRoot).length) throw new Error(`Refusing to overwrite ${resultRoot}`);
  mkdir(resultRoot);
  const manifestSource = fs.readFileSync(path.join(__dirname, "different-things-scaling-common.js"), "utf8");
  write(path.join(resultRoot, "campaign_metadata.json"), {
    experiment: "experiment2-existing-reuse-final", branch: command("git branch --show-current"), hiveSha: command("git rev-parse HEAD"), rspJsPath: RSP_JS_PATH, rspJsSha: command(`git -C ${JSON.stringify(RSP_JS_PATH)} rev-parse HEAD`), node: process.version, platform: `${process.platform}/${process.arch}`, cpu: os.cpus()[0]?.model || null, targets: args.targets, approaches: args.approaches, iterations: args.iterations, discardPolicy: args.iterations === 35 ? { warmup: [1, 2, 3], tail: [34, 35], analyzed: [4, 33] } : { warmup: [], tail: [], analyzed: [1, args.iterations] }, timeoutMs: args.timeoutMs, measurementScope: "full authoritative server process tree", samplingInterval: "canonical runner process-tree sampler", workloadManifestSha256: sha(manifestSource), startedAt: new Date().toISOString(), smoke: args.smoke,
  });
  const parent = path.join(ROOT, "results", "query-content-reuse");
  for (const target of args.targets) for (const approach of args.approaches) for (let iteration = 1; iteration <= args.iterations; iteration += 1) {
    assertPreflight();
    const before = new Set(fs.readdirSync(parent).filter((name) => name.startsWith("production-different-things-scaling-")).map((name) => path.join(parent, name)));
    const destination = path.join(resultRoot, `M${target}`, approach, `iteration-${String(iteration).padStart(2, "0")}`);
    mkdir(path.dirname(destination));
    execFileSync("node", [RUNNER, "--mode", "existing-reuse-density", "--targets", String(target), "--approaches", approach, "--timeout-ms", String(args.timeoutMs)], { cwd: ROOT, stdio: "inherit" });
    const canonicalRoot = latestRunnerRoot(before);
    const source = path.join(canonicalRoot, `existing-reuse-density-m${target}`, approach);
    if (!fs.existsSync(path.join(source, "approach_summary.json"))) throw new Error(`Canonical runner did not produce ${source}/approach_summary.json`);
    fs.cpSync(source, destination, { recursive: true, errorOnExist: true });
    write(path.join(destination, "iteration_metadata.json"), { target, approach, iteration, canonicalRoot, completedAt: new Date().toISOString() });
  }
  const metadata = JSON.parse(fs.readFileSync(path.join(resultRoot, "campaign_metadata.json"), "utf8"));
  metadata.finishedAt = new Date().toISOString(); write(path.join(resultRoot, "campaign_metadata.json"), metadata);
  console.log(resultRoot);
}
if (require.main === module) main();

module.exports = { DEFAULT_APPROACHES, DEFAULT_TARGETS, parse };
