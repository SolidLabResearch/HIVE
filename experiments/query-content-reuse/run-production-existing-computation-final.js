#!/usr/bin/env node
/* Strictly sequential final wrapper for Experiment 2: existing computations. */
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
const command = (cmd) => execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
const write = (file, value) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`); };
const stamp = () => new Date().toISOString().replace(/[:.]/g, "-");
const sha = (value) => crypto.createHash("sha256").update(value).digest("hex");

function parse(argv) {
  const args = { targets: [...DEFAULT_TARGETS], approaches: [...DEFAULT_APPROACHES], iterations: 35, timeoutMs: 180000, resultRoot: null, smoke: false };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index + 1];
    if (argv[index] === "--targets") { args.targets = value.split(",").map(Number); index += 1; }
    else if (argv[index] === "--approaches") { args.approaches = value.split(",").map((entry) => entry.trim().toLowerCase()).filter(Boolean); index += 1; }
    else if (argv[index] === "--iterations") { args.iterations = Number(value); index += 1; }
    else if (argv[index] === "--timeout-ms") { args.timeoutMs = Number(value); index += 1; }
    else if (argv[index] === "--result-root") { args.resultRoot = path.resolve(value); index += 1; }
    else if (argv[index] === "--smoke") args.smoke = true;
    else throw new Error(`Unknown argument: ${argv[index]}`);
  }
  if (!args.targets.every((target) => DEFAULT_TARGETS.includes(target))) throw new Error("Targets must be a subset of 2,4,8,16");
  if (!args.approaches.every((approach) => DEFAULT_APPROACHES.includes(approach))) throw new Error("Approaches must be fetching,approximation,chunked");
  if (!Number.isInteger(args.iterations) || args.iterations < 1) throw new Error("--iterations must be a positive integer");
  return args;
}
function assertPreflight() {
  const actual = command(`git -C ${JSON.stringify(RSP_JS_PATH)} rev-parse HEAD`);
  if (actual !== EXPECTED_RSPJS_SHA) throw new Error(`RSP-JS SHA mismatch: ${actual}`);
  const pids = command("lsof -nP -iTCP:8080 -sTCP:LISTEN -t || true");
  if (pids) throw new Error(`Port 8080 is occupied by ${pids}`);
}
function latestRunnerRoot(before) {
  const parent = path.join(ROOT, "results", "query-content-reuse");
  const roots = fs.readdirSync(parent).filter((name) => name.startsWith("production-different-things-scaling-")).map((name) => path.join(parent, name)).filter((entry) => !before.has(entry));
  if (roots.length !== 1) throw new Error(`Expected one canonical runner root, found ${roots.length}`);
  return roots[0];
}
function buildCampaignOrder(args) {
  return args.targets.flatMap((target) => args.approaches.flatMap((approach) =>
    Array.from({ length: args.iterations }, (_unused, index) => ({ target, approach, iteration: index + 1 }))));
}
function main() {
  const args = parse(process.argv.slice(2));
  assertPreflight();
  const resultRoot = args.resultRoot || path.join(ROOT, "results", "paper-benchmarks", `experiment2-existing-computation-final-${stamp()}`);
  if (fs.existsSync(resultRoot) && fs.readdirSync(resultRoot).length) throw new Error(`Refusing to overwrite ${resultRoot}`);
  fs.mkdirSync(resultRoot, { recursive: true });
  const workload = fs.readFileSync(path.join(__dirname, "different-things-scaling-common.js"), "utf8");
  write(path.join(resultRoot, "campaign_metadata.json"), { experiment: "experiment2-existing-computation-scaling", title: "Experiment 2 — Scaling with Existing Reusable Computations", hiveSha: command("git rev-parse HEAD"), rspJsSha: command(`git -C ${JSON.stringify(RSP_JS_PATH)} rev-parse HEAD`), branch: command("git branch --show-current"), node: process.version, platform: `${process.platform}/${process.arch}`, cpu: os.cpus()[0]?.model || null, targets: args.targets, approaches: args.approaches, iterations: args.iterations, workloadManifestSha256: sha(workload), startedAt: new Date().toISOString(), smoke: args.smoke, discardPolicy: args.iterations === 35 ? { warmup: [1, 2, 3], analyzed: [4, 33], tail: [34, 35] } : { warmup: [], analyzed: [1, args.iterations], tail: [] } });
  const parent = path.join(ROOT, "results", "query-content-reuse");
  for (const { target, approach, iteration } of buildCampaignOrder(args)) {
    assertPreflight();
    const before = new Set(fs.readdirSync(parent).filter((name) => name.startsWith("production-different-things-scaling-")).map((name) => path.join(parent, name)));
    const destination = path.join(resultRoot, `M${target}`, approach, `iteration-${String(iteration).padStart(2, "0")}`);
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    execFileSync("node", [RUNNER, "--mode", "existing-computation-scaling", "--targets", String(target), "--approaches", approach, "--timeout-ms", String(args.timeoutMs)], { cwd: ROOT, stdio: "inherit" });
    const source = path.join(latestRunnerRoot(before), `existing-computation-scaling-m${target}`, approach);
    if (!fs.existsSync(path.join(source, "approach_summary.json"))) throw new Error(`Canonical runner did not produce ${source}/approach_summary.json`);
    fs.cpSync(source, destination, { recursive: true, errorOnExist: true });
    write(path.join(destination, "iteration_metadata.json"), { target, approach, iteration, completedAt: new Date().toISOString() });
  }
  const metadataPath = path.join(resultRoot, "campaign_metadata.json"); const metadata = JSON.parse(fs.readFileSync(metadataPath, "utf8")); metadata.finishedAt = new Date().toISOString(); write(metadataPath, metadata);
  console.log(resultRoot);
}
if (require.main === module) main();
module.exports = { DEFAULT_APPROACHES, DEFAULT_TARGETS, buildCampaignOrder, parse };
