const PROPERTY_NAME = "sharedNumericProperty";
const OUTPUT_RANGE_MS = 120000;
const OUTPUT_STEP_MS = 60000;
const CHUNK_RANGE_MS = 60000;
const CHUNK_STEP_MS = 60000;
const ALIGNMENT_ORIGIN_MS = 1785924000000;
const WATERMARK_SENTINEL_OFFSET_MS = 130000;
const PRELIMINARY_THING_COUNTS = [2, 5, 10];
const REUSE_DENSITY_PRODUCER_COUNT = 7;
const REUSE_DENSITY_TARGET_COUNTS = [2, 4, 8, 16];
const ALL_APPROACHES = ["fetching", "chunked", "approximation"];
const FLOAT_TOLERANCE = 1e-9;

// Fixed, balanced 4-of-7 manifest. The first two queries cover the entire
// pool and share P1; every later scaling point uses a cumulative prefix.
const REUSE_DENSITY_MANIFEST = [
  ["thing1", "thing2", "thing3", "thing4"],
  ["thing1", "thing5", "thing6", "thing7"],
  ["thing2", "thing3", "thing4", "thing5"],
  ["thing1", "thing2", "thing6", "thing7"],
  ["thing3", "thing4", "thing5", "thing6"],
  ["thing1", "thing2", "thing3", "thing7"],
  ["thing4", "thing5", "thing6", "thing7"],
  ["thing1", "thing2", "thing3", "thing5"],
  ["thing1", "thing4", "thing6", "thing7"],
  ["thing2", "thing3", "thing4", "thing6"],
  ["thing1", "thing2", "thing5", "thing7"],
  ["thing3", "thing4", "thing5", "thing7"],
  ["thing1", "thing2", "thing3", "thing6"],
  ["thing1", "thing4", "thing5", "thing6"],
  ["thing2", "thing3", "thing4", "thing7"],
  ["thing2", "thing5", "thing6", "thing7"],
].map((dependencies) => Object.freeze([...dependencies]));

const THING_EVENT_OFFSETS_MS = {
  thing1: [5000, 35000, 65000, 95000],
  thing2: [5000, 15000, 35000, 65000, 95000],
  thing3: [5000, 15000, 35000, 45000, 65000, 95000],
  thing4: [5000, 35000, 65000, 95000, 105000],
  thing5: [5000, 25000, 35000, 55000, 65000, 95000],
  thing6: [5000, 20000, 35000, 65000, 80000, 95000],
  thing7: [5000, 35000, 50000, 65000, 95000, 110000],
  thing8: [5000, 18000, 35000, 65000, 78000, 95000],
  thing9: [5000, 22000, 35000, 48000, 65000, 95000],
  thing10: [5000, 35000, 65000, 88000, 95000],
};

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function formatTimestamp(timestampMs) {
  return new Date(timestampMs).toISOString();
}

function buildThingName(index) {
  return `thing${index}`;
}

function buildThingDefinitions(maxThings = 10) {
  return Array.from({ length: maxThings }, (_unused, index) => {
    const thingName = buildThingName(index + 1);
    return {
      index: index + 1,
      thingName,
      topicName: thingName,
      streamIri: `mqtt://localhost:1883/${thingName}`,
      propertyName: PROPERTY_NAME,
      measurementIri: `https://example.org/sensors/${thingName}`,
      datasetIri: `https://example.org/datasets/${thingName}`,
    };
  });
}

function buildThingValue(thingIndex, eventIndex) {
  const streamOffset = thingIndex * 10;
  const eventOffset = eventIndex * 1.5;
  const fractionalOffset = (thingIndex % 3) * 0.25;
  return Number((streamOffset + eventOffset + fractionalOffset).toFixed(6));
}

function buildObservationPayload({ thing, observationId, timestampMs, value }) {
  const subjectIri = `https://example.org/observations/${observationId}`;
  const timestamp = formatTimestamp(timestampMs);
  return [
    `<${subjectIri}> <http://rdfs.org/ns/void#inDataset> <${thing.datasetIri}> .`,
    `<${subjectIri}> <https://saref.etsi.org/core/measurementMadeBy> <${thing.measurementIri}> .`,
    `<${subjectIri}> <http://purl.org/dc/terms/isVersionOf> <https://saref.etsi.org/core/Measurement> .`,
    `<${subjectIri}> <https://saref.etsi.org/core/relatesToProperty> <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/${thing.propertyName}> .`,
    `<${subjectIri}> <https://saref.etsi.org/core/hasTimestamp> "${timestamp}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .`,
    `<${subjectIri}> <https://saref.etsi.org/core/hasValue> "${value.toFixed(6)}"^^<http://www.w3.org/2001/XMLSchema#float> .`,
  ].join(" ");
}

function buildFixture(maxThings = 10, anchorMs = ALIGNMENT_ORIGIN_MS) {
  const things = buildThingDefinitions(maxThings).map((thing) => {
    const offsets = THING_EVENT_OFFSETS_MS[thing.thingName];
    assert(
      Array.isArray(offsets) && offsets.length > 0,
      `Missing offsets for ${thing.thingName}`,
    );
    const events = offsets.map((offsetMs, eventIndex) => {
      const timestampMs = anchorMs + offsetMs;
      const value = buildThingValue(thing.index, eventIndex);
      const observationId = `${thing.thingName}-obs-${eventIndex + 1}`;
      return {
        observationId,
        offsetMs,
        timestampMs,
        timestamp: formatTimestamp(timestampMs),
        value,
        payload: buildObservationPayload({
          thing,
          observationId,
          timestampMs,
          value,
        }),
      };
    });
    const count = events.length;
    const sum = events.reduce((total, event) => total + event.value, 0);
    const sentinelTimestamp = anchorMs + WATERMARK_SENTINEL_OFFSET_MS;
    const sentinelObservationId = `${thing.thingName}-watermark-sentinel`;
    const watermarkSentinel = {
      isWatermarkSentinel: true,
      sentinelTimestamp,
      sentinelStream: thing.thingName,
      sentinelExcludedFromOracle: true,
      observationId: sentinelObservationId,
      offsetMs: WATERMARK_SENTINEL_OFFSET_MS,
      timestampMs: sentinelTimestamp,
      timestamp: formatTimestamp(sentinelTimestamp),
      value: 0,
      payload: buildObservationPayload({
        thing,
        observationId: sentinelObservationId,
        timestampMs: sentinelTimestamp,
        value: 0,
      }),
    };
    return {
      ...thing,
      events,
      watermarkSentinel,
      oracle: {
        count,
        sum,
        average: count > 0 ? sum / count : null,
        firstTimestamp: events[0]?.timestampMs ?? null,
        lastTimestamp: events[count - 1]?.timestampMs ?? null,
      },
    };
  });

  return {
    anchorMs,
    windowStart: anchorMs,
    windowEnd: anchorMs + OUTPUT_RANGE_MS,
    propertyName: PROPERTY_NAME,
    watermarkSentinels: things.map((thing) => thing.watermarkSentinel),
    things,
  };
}

function buildFinalQuery({
  includedThings,
  topicPrefix = "",
  outputIri,
  rangeMs = OUTPUT_RANGE_MS,
  stepMs = OUTPUT_STEP_MS,
}) {
  const normalizedPrefix = String(topicPrefix || "")
    .trim()
    .replace(/^\/+|\/+$/g, "");
  const topicFor = (thingName) =>
    normalizedPrefix ? `${normalizedPrefix}/${thingName}` : thingName;
  const streamIriFor = (thingName) => `mqtt://localhost:1883/${topicFor(thingName)}`;
  const fromClauses = includedThings
    .map(
      (thingName) =>
        `FROM NAMED WINDOW <${streamIriFor(thingName)}> ON STREAM mqtt_broker:${topicFor(thingName)} [RANGE ${rangeMs} STEP ${stepMs}]`,
    )
    .join("\n");
  const unionClauses = includedThings
    .map(
      (thingName) => `{
    WINDOW <${streamIriFor(thingName)}> {
      ?obs_${thingName} saref:hasValue ?value .
      ?obs_${thingName} saref:hasTimestamp ?ts .
      ?obs_${thingName} saref:relatesToProperty dahccsensors:${PROPERTY_NAME} .
    }
  }`,
    )
    .join("\n  UNION\n");

  return `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX rspjs: <https://rsp.js>

REGISTER RStream <${outputIri}> AS
SELECT (AVG(?value) AS ?resultValue) (COUNT(?value) AS ?eventCount) (SUM(?value) AS ?sumValue) (MIN(?ts) AS ?firstEventTimestamp) (MAX(?ts) AS ?lastEventTimestamp)
${fromClauses}
WHERE {
  ${unionClauses}
}
`;
}

function buildScenarioQueryDefinitions(thingCount, options = {}) {
  assert(Number.isInteger(thingCount) && thingCount > 0, "thingCount must be a positive integer");
  const things = buildThingDefinitions(thingCount);
  return things.map((thing, index) => ({
    queryLabel: `Q${index + 1}`,
    queryIndex: index + 1,
    includedThings: things.slice(0, index + 1).map((entry) => entry.thingName),
    finalThing: thing.thingName,
    expectedWindowStart: ALIGNMENT_ORIGIN_MS,
    expectedWindowEnd: ALIGNMENT_ORIGIN_MS + OUTPUT_RANGE_MS,
    rangeMs: OUTPUT_RANGE_MS,
    stepMs: OUTPUT_STEP_MS,
    query: buildFinalQuery({
      includedThings: things.slice(0, index + 1).map((entry) => entry.thingName),
      topicPrefix: options.topicPrefix,
      outputIri:
        options.outputIriBuilder?.(`Q${index + 1}`, index + 1) ??
        `mqtt://localhost:1883/results/q${index + 1}`,
      rangeMs: OUTPUT_RANGE_MS,
      stepMs: OUTPUT_STEP_MS,
    }),
  }));
}

function buildScenarioOracle(thingCount, fixture = buildFixture()) {
  const selectedThings = fixture.things.slice(0, thingCount);
  let cumulativeCount = 0;
  let cumulativeSum = 0;
  return selectedThings.map((thing, index) => {
    cumulativeCount += thing.oracle.count;
    cumulativeSum += thing.oracle.sum;
    return {
      queryLabel: `Q${index + 1}`,
      includedThings: selectedThings
        .slice(0, index + 1)
        .map((entry) => entry.thingName),
      windowStart: fixture.windowStart,
      windowEnd: fixture.windowEnd,
      count: cumulativeCount,
      sum: cumulativeSum,
      average: cumulativeSum / cumulativeCount,
    };
  });
}

function buildProducerExpectations(thingCount) {
  return Array.from({ length: thingCount }, (_unused, index) => ({
    thingName: buildThingName(index + 1),
    expectedReferenceCount: thingCount - index,
    dependentQueryLabels: Array.from(
      { length: thingCount - index },
      (_inner, offset) => `Q${index + offset + 1}`,
    ),
  }));
}

function buildScenarioMetrics(thingCount) {
  const totalProducerDependencies = (thingCount * (thingCount + 1)) / 2;
  const reusedProducerAcquisitions = totalProducerDependencies - thingCount;
  return {
    thingCount,
    finalQueries: thingCount,
    uniqueProducers: thingCount,
    totalProducerDependencies,
    reusedProducerAcquisitions,
    producerReusePercentage:
      totalProducerDependencies > 0
        ? (reusedProducerAcquisitions / totalProducerDependencies) * 100
        : 0,
  };
}

function buildReuseDensityQueryDefinitions(targetCount, options = {}) {
  assert(
    Number.isInteger(targetCount) && targetCount > 0 && targetCount <= REUSE_DENSITY_MANIFEST.length,
    `targetCount must be between 1 and ${REUSE_DENSITY_MANIFEST.length}`,
  );
  return REUSE_DENSITY_MANIFEST.slice(0, targetCount).map((includedThings, index) => ({
    queryLabel: `Q${index + 1}`,
    queryIndex: index + 1,
    includedThings: [...includedThings],
    expectedWindowStart: ALIGNMENT_ORIGIN_MS,
    expectedWindowEnd: ALIGNMENT_ORIGIN_MS + OUTPUT_RANGE_MS,
    rangeMs: OUTPUT_RANGE_MS,
    stepMs: OUTPUT_STEP_MS,
    query: buildFinalQuery({
      includedThings,
      topicPrefix: options.topicPrefix,
      outputIri:
        options.outputIriBuilder?.(`Q${index + 1}`, index + 1) ??
        `mqtt://localhost:1883/results/reuse-density-q${index + 1}`,
      rangeMs: OUTPUT_RANGE_MS,
      stepMs: OUTPUT_STEP_MS,
    }),
  }));
}

function buildReuseDensityOracle(targetCount, fixture = buildFixture(REUSE_DENSITY_PRODUCER_COUNT)) {
  const thingByName = new Map(fixture.things.map((thing) => [thing.thingName, thing]));
  return buildReuseDensityQueryDefinitions(targetCount).map((query) => {
    const contributions = query.includedThings.map((thingName) => {
      const thing = thingByName.get(thingName);
      assert(thing, `missing fixture thing ${thingName}`);
      return thing.oracle;
    });
    const count = contributions.reduce((sum, contribution) => sum + contribution.count, 0);
    const sum = contributions.reduce((total, contribution) => total + contribution.sum, 0);
    return {
      queryLabel: query.queryLabel,
      includedThings: [...query.includedThings],
      windowStart: fixture.windowStart,
      windowEnd: fixture.windowEnd,
      count,
      sum,
      average: sum / count,
    };
  });
}

function buildReuseDensityProducerExpectations(targetCount) {
  const queries = buildReuseDensityQueryDefinitions(targetCount);
  return buildThingDefinitions(REUSE_DENSITY_PRODUCER_COUNT).map((thing) => {
    const dependentQueryLabels = queries
      .filter((query) => query.includedThings.includes(thing.thingName))
      .map((query) => query.queryLabel);
    return {
      thingName: thing.thingName,
      expectedReferenceCount: dependentQueryLabels.length,
      dependentQueryLabels,
    };
  });
}

function buildReuseDensityMetrics(targetCount) {
  assert(
    Number.isInteger(targetCount) && targetCount > 0 && targetCount <= REUSE_DENSITY_MANIFEST.length,
    `targetCount must be between 1 and ${REUSE_DENSITY_MANIFEST.length}`,
  );
  const totalProducerDependencies = targetCount * 4;
  const uniqueProducers = REUSE_DENSITY_PRODUCER_COUNT;
  const reusedProducerAcquisitions = totalProducerDependencies - uniqueProducers;
  assert(
    reusedProducerAcquisitions >= 0,
    "reuse-density mode requires enough target queries to cover the fixed producer pool",
  );
  return {
    targetCount,
    finalQueries: targetCount,
    uniqueProducers,
    totalProducerDependencies,
    reusedProducerAcquisitions,
    producerReusePercentage:
      (reusedProducerAcquisitions / totalProducerDependencies) * 100,
  };
}

module.exports = {
  ALIGNMENT_ORIGIN_MS,
  ALL_APPROACHES,
  CHUNK_RANGE_MS,
  CHUNK_STEP_MS,
  FLOAT_TOLERANCE,
  OUTPUT_RANGE_MS,
  OUTPUT_STEP_MS,
  PRELIMINARY_THING_COUNTS,
  REUSE_DENSITY_MANIFEST,
  REUSE_DENSITY_PRODUCER_COUNT,
  REUSE_DENSITY_TARGET_COUNTS,
  PROPERTY_NAME,
  WATERMARK_SENTINEL_OFFSET_MS,
  buildFixture,
  buildFinalQuery,
  buildObservationPayload,
  buildProducerExpectations,
  buildScenarioMetrics,
  buildScenarioOracle,
  buildScenarioQueryDefinitions,
  buildThingDefinitions,
  buildReuseDensityMetrics,
  buildReuseDensityOracle,
  buildReuseDensityProducerExpectations,
  buildReuseDensityQueryDefinitions,
  formatTimestamp,
};
