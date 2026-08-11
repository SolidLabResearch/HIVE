const fs = require("fs");
const path = require("path");

const FETCHING_ORACLE_FILE = /^benchmark_window_cap_summary(?:_consumer_[^.]+)?\.json$/;
const CHUNKED_COMPUTE_ROLES = new Set([
  "chunked_bee_worker",
  "shared_chunked_reconstruction_runtime",
]);

function numeric(value) {
  return Number.isFinite(Number(value)) ? Number(value) : null;
}

/**
 * Finds the durable Fetching oracle by its completed-window content, never by
 * an incidental diagnostic consumer filename.  All valid artifacts belong to
 * the scenario run directory; duplicate, identical observations are accepted
 * as redundant evidence, while conflicting observations fail closed.
 */
function discoverFetchingOracle(runRoot, { expectedAggregation = "AVG", targetWindowCount = 1 } = {}) {
  const candidates = fs.readdirSync(runRoot, { withFileTypes: true })
    .filter((entry) => entry.isFile() && FETCHING_ORACLE_FILE.test(entry.name))
    .map((entry) => {
      const artifactPath = path.join(runRoot, entry.name);
      try {
        return { artifactPath, summary: JSON.parse(fs.readFileSync(artifactPath, "utf8")) };
      } catch (error) {
        throw new Error(`Failed to parse Fetching oracle artifact ${artifactPath}: ${error.message}`);
      }
    })
    .filter(({ summary }) => isValidFetchingOracle(summary, { expectedAggregation, targetWindowCount }));
  if (candidates.length === 0) {
    throw new Error(`No valid durable Fetching oracle found in ${runRoot}`);
  }
  const values = new Map();
  for (const candidate of candidates) {
    const summary = candidate.summary;
    const key = JSON.stringify({
      resultValue: Number(summary.resultValue), windowStart: Number(summary.windowStart),
      windowEnd: Number(summary.windowEnd), windowNumber: Number(summary.windowNumber),
    });
    values.set(key, [...(values.get(key) ?? []), candidate]);
  }
  if (values.size !== 1) {
    throw new Error(`Conflicting durable Fetching oracles in ${runRoot}: ${[...values.values()].flat().map((entry) => entry.artifactPath).join(", ")}`);
  }
  const identical = [...values.values()][0];
  return { ...identical[0].summary, artifactPaths: identical.map((entry) => entry.artifactPath), expectedAggregation };
}

function isValidFetchingOracle(summary, { expectedAggregation, targetWindowCount }) {
  if (!summary || typeof summary !== "object") return false;
  const aggregation = summary.aggregationType ?? summary.aggregation ?? expectedAggregation;
  return aggregation === expectedAggregation &&
    summary.coverageComplete === true && summary.isPartialWindow === false &&
    summary.isComparableWindow === true && summary.stoppedAfterTargetWindows === true &&
    summary.stopReason === "target_window_count_reached" &&
    Number(summary.emittedFinalWindowCount) === Number(targetWindowCount) &&
    numeric(summary.windowNumber) !== null && numeric(summary.windowStart) !== null &&
    numeric(summary.windowEnd) !== null && numeric(summary.resultValue) !== null &&
    numeric(summary.resultEmittedAt) !== null && Number(summary.windowEnd) > Number(summary.windowStart);
}

/** Physical final computations, deliberately distinct from consumer deliveries. */
function summarizePhysicalFinalComputations(profiles, approach) {
  const roles = approach === "chunked" ? CHUNKED_COMPUTE_ROLES :
    approach === "approximation" ? new Set(["approximation_bee_worker"]) : new Set();
  const unique = new Map();
  for (const profile of profiles) {
    if (!roles.has(profile.processRole)) continue;
    const identity = profile.artifactPath || `${profile.processId ?? "unknown"}:${profile.processRole}`;
    const prior = unique.get(identity);
    if (!prior) unique.set(identity, profile);
    else if (Number(profile.finishedAt || 0) > Number(prior.finishedAt || 0)) unique.set(identity, profile);
  }
  const selected = [...unique.values()];
  const total = (field) => selected.reduce((sum, profile) => sum + Number(profile.counters?.[field] ?? profile[field] ?? 0), 0);
  return {
    mqttMessagesReceived: total("mqtt_messages_received"),
    emittedResults: total("emitted_results"),
    reconstructedSuperqueryResults: total("reconstructed_superquery_results"),
    comparableWindowsEmitted: total("comparable_windows_emitted"),
    chunkGroupsCompleted: total("chunk_groups_completed"),
  };
}

function compareAgainstFetchingOracle(values, oracleValue, exactTolerance = 1e-9) {
  if (!Number.isFinite(oracleValue)) throw new Error("Fetching oracle value must be finite");
  const errors = values.map((value) => Math.abs(value - oracleValue));
  return {
    errors,
    mae: errors.length ? errors.reduce((sum, value) => sum + value, 0) / errors.length : null,
    maxAbsoluteError: errors.length ? Math.max(...errors) : null,
    exactCount: errors.filter((value) => value <= exactTolerance).length,
  };
}

module.exports = { discoverFetchingOracle, isValidFetchingOracle, summarizePhysicalFinalComputations, compareAgainstFetchingOracle };
