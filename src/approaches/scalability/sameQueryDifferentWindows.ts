import {
  AggregationFunction,
  buildBenchmarkStreamIri,
  buildBenchmarkTopicName,
  buildOutputSelectClause,
  buildSubQuerySelectClause,
  getConfiguredAggregation,
  getOutputWindowRange,
  getOutputWindowStep,
} from "../../util/runtimeConfig";

export type ReusableQueryWindow = {
  id: string;
  rangeMs: number;
  stepMs: number;
};

export const SAME_QUERY_DIFFERENT_WINDOWS = {
  scenario: "same_query_different_windows",
  property: "smartphone.accel.x",
  outputWindowRangeMs: 120000,
  outputWindowStepMs: 60000,
  supportedScales: [2, 4, 6, 8, 10],
  reusableQueryWindows: [
    { id: "Q1", rangeMs: 30000, stepMs: 15000 },
    { id: "Q2", rangeMs: 45000, stepMs: 15000 },
    { id: "Q3", rangeMs: 60000, stepMs: 30000 },
    { id: "Q4", rangeMs: 75000, stepMs: 15000 },
    { id: "Q5", rangeMs: 90000, stepMs: 30000 },
    { id: "Q6", rangeMs: 105000, stepMs: 15000 },
    { id: "Q7", rangeMs: 120000, stepMs: 30000 },
    { id: "Q8", rangeMs: 135000, stepMs: 15000 },
    { id: "Q9", rangeMs: 150000, stepMs: 30000 },
    { id: "Q10", rangeMs: 180000, stepMs: 60000 },
  ] as ReusableQueryWindow[],
} as const;

function parseScale(rawScale: string | undefined): number {
  const normalized = String(rawScale || "").trim();
  const cleaned = normalized.replace(/^scale_?/i, "");
  const parsed = Number.parseInt(cleaned, 10);
  if ((SAME_QUERY_DIFFERENT_WINDOWS.supportedScales as readonly number[]).includes(parsed)) {
    return parsed;
  }
  return 2;
}

export function getConfiguredScale(): number {
  return parseScale(process.env.BENCHMARK_SCALE);
}

export function getReusableWindowsForScale(
  scale = getConfiguredScale(),
): ReusableQueryWindow[] {
  return SAME_QUERY_DIFFERENT_WINDOWS.reusableQueryWindows.slice(0, scale);
}

export function getSmartphoneStreamConfig() {
  return {
    topicName: buildBenchmarkTopicName("smartphoneX"),
    streamIri: buildBenchmarkStreamIri("smartphoneX"),
    propertyIriSuffix: "smartphoneX",
  };
}

export function buildScalabilitySubQuery(
  index: number,
  window: ReusableQueryWindow,
  aggregation: AggregationFunction = getConfiguredAggregation(),
): string {
  const stream = getSmartphoneStreamConfig();
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <output> AS
SELECT ${buildSubQuerySelectClause(aggregation, `SmartphoneScale${index}`)}
FROM NAMED WINDOW <${stream.streamIri}> ON STREAM mqtt_broker:${stream.topicName} [RANGE ${window.rangeMs} STEP ${window.stepMs}]
WHERE {
    WINDOW <${stream.streamIri}> {
        ?s saref:hasValue ?value .
        ?s saref:hasTimestamp ?ts .
        ?s saref:relatesToProperty dahccsensors:${stream.propertyIriSuffix} .
    }
}
  `;
}

export function buildScalabilitySuperQuery(
  aggregation: AggregationFunction = getConfiguredAggregation(),
): string {
  const stream = getSmartphoneStreamConfig();
  const outputWindowRange = getOutputWindowRange();
  const outputWindowStep = getOutputWindowStep();
  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>

REGISTER RStream <sensor_averages> AS
SELECT ${buildOutputSelectClause(aggregation)}
FROM NAMED WINDOW <${stream.streamIri}> ON STREAM mqtt_broker:${stream.topicName} [RANGE ${outputWindowRange} STEP ${outputWindowStep}]
WHERE {
    WINDOW <${stream.streamIri}> {
        ?s saref:hasValue ?value .
        ?s saref:hasTimestamp ?ts .
        ?s saref:relatesToProperty dahccsensors:${stream.propertyIriSuffix} .
    }
}
  `;
}

export function getRawInputSubscriberCount(
  approach: string,
  scale = getConfiguredScale(),
): number {
  switch (approach) {
    case "approximation":
      return scale;
    case "chunked":
      return scale + 1;
    case "naive_distributed":
      return scale + 1;
    case "fetching":
      return 1;
    default:
      return 1;
  }
}
