import {
  AggregationFunction,
  buildBenchmarkStreamIri,
  buildBenchmarkTopicName,
  buildOutputSelectClause,
  buildSubQuerySelectClause,
} from "./runtimeConfig";

export type BenchmarkTargetSource = "real" | "synthetic";

export type QueryTargetDefinition = {
  name: string;
  topicName: string;
  propertyName: string;
};

const REAL_QUERY_TARGETS: QueryTargetDefinition[] = [
  {
    name: "wearableX",
    topicName: "wearableX",
    propertyName: "wearableX",
  },
  {
    name: "smartphoneX",
    topicName: "smartphoneX",
    propertyName: "smartphoneX",
  },
];

const SYNTHETIC_TARGET_PREFIX = "syntheticTarget";

function toSelectSuffix(targetName: string): string {
  const sanitized = String(targetName || "").replace(/[^a-zA-Z0-9]/g, "");
  return sanitized.length > 0
    ? sanitized[0].toUpperCase() + sanitized.slice(1)
    : "Target";
}

export function getRealBenchmarkTargets(): QueryTargetDefinition[] {
  return REAL_QUERY_TARGETS.map((target) => ({ ...target }));
}

export function buildSyntheticBenchmarkTargets(count: number): QueryTargetDefinition[] {
  const results: QueryTargetDefinition[] = [];
  for (let index = 1; index <= count; index += 1) {
    const name = `${SYNTHETIC_TARGET_PREFIX}${index}`;
    results.push({
      name,
      topicName: name,
      propertyName: name,
    });
  }
  return results;
}

export function getConfiguredBenchmarkTargetSource(): BenchmarkTargetSource {
  const raw = String(process.env.BENCHMARK_TARGET_SOURCE || "real")
    .trim()
    .toLowerCase();
  return raw === "synthetic" ? "synthetic" : "real";
}

export function getConfiguredBenchmarkTargetNames(): string[] {
  return String(process.env.BENCHMARK_TARGET_NAMES || process.env.BENCHMARK_QUERY_TARGET_NAMES || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export function getConfiguredBenchmarkTargets(): QueryTargetDefinition[] {
  const names = getConfiguredBenchmarkTargetNames();
  if (names.length === 0) {
    return getConfiguredBenchmarkTargetSource() === "synthetic"
      ? buildSyntheticBenchmarkTargets(
          Number.parseInt(process.env.BENCHMARK_TARGET_COUNT || "2", 10) || 2,
        )
      : getRealBenchmarkTargets();
  }

  return names.map((name) => ({
    name,
    topicName: name,
    propertyName: name,
  }));
}

export function buildQueryTargetScalingSubQuery(
  target: QueryTargetDefinition,
  aggregation: AggregationFunction,
  subWindowRange: number,
  subWindowStep: number,
): string {
  const streamIri = buildBenchmarkStreamIri(target.topicName);
  const topicName = buildBenchmarkTopicName(target.topicName);
  const suffix = toSelectSuffix(target.name);

  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggregation, suffix)}
FROM NAMED WINDOW <${streamIri}> ON STREAM mqtt_broker:${topicName} [RANGE ${subWindowRange} STEP ${subWindowStep}]
WHERE {
    WINDOW <${streamIri}> {
        ?s saref:hasValue ?value .
        ?s saref:hasTimestamp ?ts .
        ?s saref:relatesToProperty dahccsensors:${target.propertyName} .
    }
}
  `;
}

export function buildQueryTargetScalingSuperQuery(
  targets: QueryTargetDefinition[],
  aggregation: AggregationFunction,
  outputWindowRange: number,
  outputWindowStep: number,
): string {
  const fromClauses = targets
    .map((target) => {
      const streamIri = buildBenchmarkStreamIri(target.topicName);
      const topicName = buildBenchmarkTopicName(target.topicName);
      return `FROM NAMED WINDOW <${streamIri}> ON STREAM mqtt_broker:${topicName} [RANGE ${outputWindowRange} STEP ${outputWindowStep}]`;
    })
    .join("\n");

  const unionClauses = targets
    .map((target) => {
      const streamIri = buildBenchmarkStreamIri(target.topicName);
      return `{
        WINDOW <${streamIri}> {
            ?s saref:hasValue ?value .
            ?s saref:hasTimestamp ?ts .
            ?s saref:relatesToProperty dahccsensors:${target.propertyName} .
        }
    }`;
    })
    .join(" UNION ");

  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT ${buildOutputSelectClause(aggregation)}
${fromClauses}
WHERE {
    ${unionClauses}
}
  `;
}
