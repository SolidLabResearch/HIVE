const {
  ALIGNMENT_ORIGIN_MS,
  OUTPUT_RANGE_MS,
  EXISTING_COMPUTATION_TARGET_COUNTS,
  buildExistingComputationCompositeQueryDefinition,
  buildExistingComputationOracle,
  buildExistingComputationPrimitiveQueryDefinitions,
  buildFixture,
  buildProducerExpectations,
  buildReuseDensityMetrics,
  buildReuseDensityProducerExpectations,
  buildReuseDensityQueryDefinitions,
  REUSE_DENSITY_MANIFEST,
  REUSE_DENSITY_PRODUCER_COUNT,
  REUSE_DENSITY_TARGET_COUNTS,
  buildScenarioMetrics,
  buildScenarioOracle,
  buildScenarioQueryDefinitions,
} = require("./different-things-scaling-common");
const {
  buildExistingPrimitiveRegistrationBodies,
  buildExistingComputationPrimitiveRegistrationBodies,
  parseArgs,
} = require("./run-production-different-things-scaling");

describe("different-things-scaling common helpers", () => {
  test("builds ten nested queries with the fixed final window", () => {
    const queries = buildScenarioQueryDefinitions(10, {
      topicPrefix: "experiment2/scenario-n10",
      outputIriBuilder: (label) => `mqtt://localhost:1883/results/${label}`,
    });

    expect(queries).toHaveLength(10);
    expect(queries[0].includedThings).toEqual(["thing1"]);
    expect(queries[4].includedThings).toEqual([
      "thing1",
      "thing2",
      "thing3",
      "thing4",
      "thing5",
    ]);
    expect(queries[9].includedThings).toEqual([
      "thing1",
      "thing2",
      "thing3",
      "thing4",
      "thing5",
      "thing6",
      "thing7",
      "thing8",
      "thing9",
      "thing10",
    ]);
    for (const query of queries) {
      expect(query.expectedWindowStart).toBe(ALIGNMENT_ORIGIN_MS);
      expect(query.expectedWindowEnd).toBe(ALIGNMENT_ORIGIN_MS + OUTPUT_RANGE_MS);
      expect(query.query).toContain("[RANGE 120000 STEP 60000]");
      expect(query.query).toContain("dahccsensors:sharedNumericProperty");
    }
  });

  test("marks one out-of-window watermark sentinel per stream and excludes it from the oracle", () => {
    const fixture = buildFixture(2);

    for (const thing of fixture.things) {
      expect(thing.watermarkSentinel).toMatchObject({
        isWatermarkSentinel: true,
        sentinelStream: thing.thingName,
        sentinelExcludedFromOracle: true,
        value: 0,
      });
      expect(thing.watermarkSentinel.offsetMs).toBe(
        thing.events[0].offsetMs + OUTPUT_RANGE_MS + 1,
      );
      expect(thing.watermarkSentinel.sentinelTimestamp).toBe(
        thing.events[0].timestampMs + OUTPUT_RANGE_MS + 1,
      );
      expect(thing.watermarkSentinel.sentinelTimestamp).toBeGreaterThanOrEqual(fixture.windowEnd);
      expect(thing.events).not.toContainEqual(
        expect.objectContaining({ isWatermarkSentinel: true }),
      );
      expect(thing.oracle.count).toBe(thing.events.length);
      expect(thing.oracle.sum).toBe(
        thing.events.reduce((sum, event) => sum + event.value, 0),
      );
    }
  });

  test("builds a deterministic oracle using weighted count and sum", () => {
    const fixture = buildFixture();
    const oracle = buildScenarioOracle(3, fixture);

    const thing1 = fixture.things[0].oracle;
    const thing2 = fixture.things[1].oracle;
    const thing3 = fixture.things[2].oracle;
    const combinedCount = thing1.count + thing2.count + thing3.count;
    const combinedSum = thing1.sum + thing2.sum + thing3.sum;

    expect(oracle[2]).toEqual({
      queryLabel: "Q3",
      includedThings: ["thing1", "thing2", "thing3"],
      windowStart: ALIGNMENT_ORIGIN_MS,
      windowEnd: ALIGNMENT_ORIGIN_MS + OUTPUT_RANGE_MS,
      count: combinedCount,
      sum: combinedSum,
      average: combinedSum / combinedCount,
    });
  });

  test("computes producer expectations and reuse percentage for N=10", () => {
    const expectations = buildProducerExpectations(10);
    const metrics = buildScenarioMetrics(10);

    expect(expectations[0]).toEqual({
      thingName: "thing1",
      expectedReferenceCount: 10,
      dependentQueryLabels: ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
    });
    expect(expectations[9]).toEqual({
      thingName: "thing10",
      expectedReferenceCount: 1,
      dependentQueryLabels: ["Q10"],
    });
    expect(metrics.totalProducerDependencies).toBe(55);
    expect(metrics.reusedProducerAcquisitions).toBe(45);
    expect(metrics.producerReusePercentage).toBeCloseTo(81.8181818, 6);
  });

  test("defines a fixed, cumulative, balanced reuse-density manifest", () => {
    expect(REUSE_DENSITY_MANIFEST).toHaveLength(16);
    expect(REUSE_DENSITY_TARGET_COUNTS).toEqual([2, 4, 8, 16]);
    expect(REUSE_DENSITY_PRODUCER_COUNT).toBe(7);

    const queries = buildReuseDensityQueryDefinitions(16);
    expect(new Set(queries.map((query) => query.includedThings.join("|"))).size).toBe(16);
    expect(queries.every((query) => query.includedThings.length === 4)).toBe(true);
    expect(
      new Set(queries.flatMap((query) => query.includedThings)),
    ).toEqual(new Set(["thing1", "thing2", "thing3", "thing4", "thing5", "thing6", "thing7"]));
    expect(new Set(queries.slice(0, 2).flatMap((query) => query.includedThings))).toEqual(
      new Set(["thing1", "thing2", "thing3", "thing4", "thing5", "thing6", "thing7"]),
    );
    expect(queries[0].includedThings).toEqual(["thing1", "thing2", "thing3", "thing4"]);
    expect(queries[1].includedThings).toEqual(["thing1", "thing5", "thing6", "thing7"]);

    for (const targetCount of REUSE_DENSITY_TARGET_COUNTS) {
      expect(buildReuseDensityQueryDefinitions(targetCount)).toEqual(queries.slice(0, targetCount));
    }
  });

  test("keeps the producer pool fixed while reuse density rises", () => {
    const expected = new Map([
      [2, { dependencies: 8, reused: 1, reusePct: 12.5 }],
      [4, { dependencies: 16, reused: 9, reusePct: 56.25 }],
      [8, { dependencies: 32, reused: 25, reusePct: 78.125 }],
      [16, { dependencies: 64, reused: 57, reusePct: 89.0625 }],
    ]);
    for (const [targetCount, expectation] of expected) {
      const metrics = buildReuseDensityMetrics(targetCount);
      const producerExpectations = buildReuseDensityProducerExpectations(targetCount);
      expect(metrics.uniqueProducers).toBe(7);
      expect(metrics.totalProducerDependencies).toBe(expectation.dependencies);
      expect(metrics.reusedProducerAcquisitions).toBe(expectation.reused);
      expect(metrics.producerReusePercentage).toBe(expectation.reusePct);
      expect(producerExpectations).toHaveLength(7);
      expect(producerExpectations.reduce((sum, entry) => sum + entry.expectedReferenceCount, 0)).toBe(expectation.dependencies);
      expect(producerExpectations.every((entry) => entry.expectedReferenceCount > 0)).toBe(true);
    }
  });

  test("builds the separate existing-reuse-density Phase-1 workload from exactly seven primitives", () => {
    const primitives = buildExistingPrimitiveRegistrationBodies({
      approach: "chunked",
      topicPrefix: "experiment2/existing-reuse-density-m2",
    });
    expect(primitives).toHaveLength(7);
    expect(primitives.map((entry) => entry.query_label)).toEqual(["P1", "P2", "P3", "P4", "P5", "P6", "P7"]);
    expect(primitives.map((entry) => entry.included_things)).toEqual([
      ["thing1"], ["thing2"], ["thing3"], ["thing4"], ["thing5"], ["thing6"], ["thing7"],
    ]);
    expect(parseArgs(["--mode", "existing-reuse-density", "--approaches", "fetching,approximation,chunked"])).toMatchObject({
      mode: "existing-reuse-density",
      things: [2, 4, 8, 16],
      approaches: ["fetching", "approximation", "chunked"],
    });
    expect(buildExistingPrimitiveRegistrationBodies({
      approach: "approximation",
      topicPrefix: "experiment2/existing-reuse-density-m2",
    }).every((entry) => entry.approximation_config?.policy === "rate-based-completed-window")).toBe(true);
  });

  test.each([2, 4, 8, 16])("existing-computation scaling registers exactly P1..P%s and Q%s uses every stream", (target) => {
    const primitives = buildExistingComputationPrimitiveQueryDefinitions(target);
    const composite = buildExistingComputationCompositeQueryDefinition(target);
    expect(EXISTING_COMPUTATION_TARGET_COUNTS).toEqual([2, 4, 8, 16]);
    expect(primitives.map((entry) => entry.queryLabel)).toEqual(Array.from({ length: target }, (_unused, index) => `P${index + 1}`));
    expect(primitives.flatMap((entry) => entry.includedThings)).toEqual(Array.from({ length: target }, (_unused, index) => `thing${index + 1}`));
    expect(composite.queryLabel).toBe(`Q${target}`);
    expect(composite.includedThings).toEqual(Array.from({ length: target }, (_unused, index) => `thing${index + 1}`));
    expect(composite.query).toContain("(AVG(?value) AS ?resultValue)");
    expect(composite.query.match(/FROM NAMED WINDOW/g)).toHaveLength(target);
    expect(buildExistingComputationPrimitiveRegistrationBodies({ approach: "approximation", thingCount: target, topicPrefix: "test" }).every((entry) => entry.approximation_config?.policy === "rate-based-completed-window")).toBe(true);
    expect(buildExistingComputationPrimitiveRegistrationBodies({ approach: "chunked", thingCount: target, topicPrefix: "test" }).every((entry) => entry.approach === "chunked" && entry.approximation_config === undefined)).toBe(true);
  });

  test("existing-computation oracle is deterministic and weighted across all dependencies", () => {
    const fixture = buildFixture(16);
    expect(buildExistingComputationOracle(16, fixture)).toEqual(buildExistingComputationOracle(16, fixture));
    const row = buildExistingComputationOracle(4, fixture)[0];
    expect(row.count).toBe(fixture.things.slice(0, 4).reduce((total, thing) => total + thing.oracle.count, 0));
    expect(row.sum).toBe(fixture.things.slice(0, 4).reduce((total, thing) => total + thing.oracle.sum, 0));
  });

  test("existing-computation scaling CLI accepts only the paper targets", () => {
    expect(parseArgs(["--mode", "existing-computation-scaling", "--targets", "2,4,8,16", "--approaches", "fetching,approximation,chunked"])).toMatchObject({ mode: "existing-computation-scaling", things: [2, 4, 8, 16] });
    expect(() => parseArgs(["--mode", "existing-computation-scaling", "--targets", "3"])).toThrow("Unsupported target count");
  });
});


const { RSPQLContainmentService } = require("../../dist/services/reuse/RSPQLContainmentService");
const { StreamingQueryChunkAggregatorOperator } = require("../../dist/services/operators/StreamingQueryChunkAggregatorOperator");
const {
  buildApproachScenarioMetrics,
  buildNestedDependencyTopology,
  buildRspEngineEvidence,
  classifyProcessTopology,
  compareTopologyPhases,
  subtractNumericRecords,
  validateDeliveryProducerIdentities,
  validateProducerIdentityMappings,
} = require("./run-production-different-things-scaling");

describe("approach-specific scaling metrics", () => {
  test("reports fetching incidences without producer reuse", () => {
    expect(buildApproachScenarioMetrics("fetching", 5)).toMatchObject({
      finalQueries: 5,
      uniqueProducers: 0,
      totalProducerDependencies: 15,
      reusedProducerAcquisitions: 0,
      producerReusePercentage: 0,
    });
    expect(buildApproachScenarioMetrics("chunked", 5)).toMatchObject({
      uniqueProducers: 5,
      totalProducerDependencies: 15,
      reusedProducerAcquisitions: 10,
    });
  });
});

describe("existing-reuse-density phase evidence helpers", () => {
  test("persists comparable topology identities and phase additions", () => {
    const phase1 = { processes: [
      { pid: 1, alive: true, classification: "server" },
      { pid: 2, alive: true, classification: "managed_producer" },
    ] };
    const post = { processes: [
      ...phase1.processes,
      { pid: 3, alive: true, classification: "reconstruction_worker" },
    ] };
    expect(compareTopologyPhases(phase1, post)).toMatchObject({
      processCountDelta: 1,
      addedProcesses: [expect.objectContaining({ pid: 3, classification: "reconstruction_worker" })],
      phase1RoleCounts: { server: 1, managed_producer: 1 },
      postAdditionRoleCounts: { server: 1, managed_producer: 1, reconstruction_worker: 1 },
    });
  });

  test("calculates profile/MQTT counter deltas without CPU-percent arithmetic", () => {
    expect(subtractNumericRecords(
      { rsp_engines_created: 7, mqtt_messages_published: 11 },
      { rsp_engines_created: 7, mqtt_messages_published: 4 },
    )).toEqual({ mqtt_messages_published: 7, rsp_engines_created: 0 });
  });

  test("proves Chunked primitive engine identity is fixed across phase boundaries", () => {
    const phase1 = buildRspEngineEvidence({ approach: "chunked", registrations: [], producerMappings: [
      { runtimeProducerId: "p1" }, { runtimeProducerId: "p2" },
    ] });
    const post = buildRspEngineEvidence({ approach: "chunked", registrations: [], producerMappings: [
      { runtimeProducerId: "p1" }, { runtimeProducerId: "p2" }, { runtimeProducerId: "p1" },
    ] });
    expect(phase1.primitiveRspEngineCount).toBe(2);
    expect(post.primitiveRspEngineCount - phase1.primitiveRspEngineCount).toBe(0);
  });
});

describe("explicit producer identity runner helpers", () => {
  const mapping = (canonicalProducerId, runtimeProducerId, topic) => ({
    canonicalProducerId,
    runtimeProducerId,
    topic,
  });

  const registration = {
    queryLabel: "Q1",
    includedThings: ["thing1"],
    producerIdentityMappings: [mapping("canonical-1", "runtime-1", "chunks/1")],
  };

  const validDelivery = {
    queryLabel: "Q1",
    producerIdentityMappings: [mapping("canonical-1", "runtime-1", "chunks/1")],
    requiredCanonicalProducerIds: ["canonical-1"],
    receivedCanonicalProducerIds: ["canonical-1"],
    missingCanonicalProducerIds: [],
    requiredRuntimeProducerIds: ["runtime-1"],
    receivedRuntimeProducerIds: ["runtime-1"],
    missingRuntimeProducerIds: [],
    localProducerSpawnCount: 0,
    managedProducerMode: true,
  };

  test("rejects a mapped canonical/runtime mismatch", () => {
    expect(() => validateDeliveryProducerIdentities({
      ...validDelivery,
      producerIdentityMappings: [mapping("canonical-1", "runtime-other", "chunks/1")],
    }, registration)).toThrow(/canonical\/runtime\/topic mapping mismatch/);
  });

  test("rejects unknown canonical and runtime IDs", () => {
    expect(() => validateDeliveryProducerIdentities({
      ...validDelivery,
      producerIdentityMappings: [mapping("canonical-unknown", "runtime-unknown", "chunks/unknown")],
      requiredCanonicalProducerIds: ["canonical-unknown"],
      receivedCanonicalProducerIds: ["canonical-unknown"],
      requiredRuntimeProducerIds: ["runtime-unknown"],
      receivedRuntimeProducerIds: ["runtime-unknown"],
    }, registration)).toThrow(/mapped canonical IDs/);
  });

  test("rejects duplicate canonical and runtime mappings", () => {
    expect(() => validateProducerIdentityMappings([
      mapping("canonical-1", "runtime-1", "chunks/1"),
      mapping("canonical-1", "runtime-2", "chunks/2"),
    ])).toThrow(/duplicate canonicalProducerId/);
    expect(() => validateProducerIdentityMappings([
      mapping("canonical-1", "runtime-1", "chunks/1"),
      mapping("canonical-2", "runtime-1", "chunks/2"),
    ])).toThrow(/duplicate runtimeProducerId/);
  });

  test("builds nested topology from registration-order set differences", () => {
    const topology = buildNestedDependencyTopology({
      thingCount: 3,
      registrations: [
        {
          queryLabel: "Q1",
          includedThings: ["thing1"],
          producerIdentityMappings: [mapping("canonical-1", "runtime-1", "chunks/1")],
        },
        {
          queryLabel: "Q2",
          includedThings: ["thing1", "thing2"],
          producerIdentityMappings: [
            mapping("canonical-2", "runtime-2", "chunks/2"),
            mapping("canonical-1", "runtime-1", "chunks/1"),
          ],
        },
        {
          queryLabel: "Q3",
          includedThings: ["thing3", "thing1", "thing2"],
          producerIdentityMappings: [
            mapping("canonical-3", "runtime-3", "chunks/3"),
            mapping("canonical-1", "runtime-1", "chunks/1"),
            mapping("canonical-2", "runtime-2", "chunks/2"),
          ],
        },
      ],
    });

    expect(topology.nodes).toEqual([
      expect.objectContaining({ thingName: "thing1", canonicalProducerId: "canonical-1", runtimeProducerId: "runtime-1" }),
      expect.objectContaining({ thingName: "thing2", canonicalProducerId: "canonical-2", runtimeProducerId: "runtime-2" }),
      expect.objectContaining({ thingName: "thing3", canonicalProducerId: "canonical-3", runtimeProducerId: "runtime-3" }),
    ]);
    expect(topology.finalProducerIdentityMappings).toHaveLength(3);
  });

  test("classifies authoritative server, managed producers, reconstruction workers, and descendants", () => {
    const topology = classifyProcessTopology({
      serverPid: 100,
      producerSnapshots: [{
        pid: 110,
        canonicalProducerId: "canonical-1",
        runtimeProducerId: "runtime-1",
      }],
      reconstructionWorkerIds: ["120", "logical-worker"],
      processRows: [
        { pid: 100, ppid: 1, command: "node server.js" },
        { pid: 110, ppid: 100, command: "node producer.js" },
        { pid: 120, ppid: 100, command: "node reconstruction.js" },
        { pid: 121, ppid: 120, command: "helper" },
        { pid: 999, ppid: 1, command: "unrelated" },
      ],
    });

    expect(topology.processes).toEqual(expect.arrayContaining([
      expect.objectContaining({ pid: 100, ppid: 1, command: "node server.js", classification: "server", alive: true }),
      expect.objectContaining({ pid: 110, classification: "managed_producer", canonicalProducerId: "canonical-1" }),
      expect.objectContaining({ pid: 120, classification: "reconstruction_worker" }),
      expect.objectContaining({ pid: 121, classification: "descendant" }),
    ]));
    expect(topology.processes).not.toEqual(expect.arrayContaining([
      expect.objectContaining({ pid: 999 }),
    ]));
    expect(topology.unresolvedReconstructionWorkerIds).toEqual(["logical-worker"]);
  });
});

describe("Query Generation and Parser Integration", () => {
  let queries;
  let service;

  beforeAll(() => {
    service = new RSPQLContainmentService();
    queries = buildScenarioQueryDefinitions(2, {
      topicPrefix: "experiment2/n2/n2",
      outputIriBuilder: (label) => `mqtt://localhost:1883/results/${label.toLowerCase()}`,
    });
  });

  test("generated Q1 query has unique projected variables", () => {
    const q1 = queries[0].query;
    const selectMatch = q1.match(/SELECT\s+([\s\S]+?)(?=\bFROM\b|\bWHERE\b)/i);
    expect(selectMatch).not.toBeNull();
    
    const selectBody = selectMatch[1];
    const aliases = Array.from(selectBody.matchAll(/AS\s+(\?[A-Za-z0-9_]+)/gi)).map(m => m[1]);
    const uniqueAliases = new Set(aliases);
    expect(aliases.length).toBe(uniqueAliases.size);
  });

  test("generated Q2 query has unique projected variables", () => {
    const q2 = queries[1].query;
    const selectMatch = q2.match(/SELECT\s+([\s\S]+?)(?=\bFROM\b|\bWHERE\b)/i);
    expect(selectMatch).not.toBeNull();
    
    const selectBody = selectMatch[1];
    const aliases = Array.from(selectBody.matchAll(/AS\s+(\?[A-Za-z0-9_]+)/gi)).map(m => m[1]);
    const uniqueAliases = new Set(aliases);
    expect(aliases.length).toBe(uniqueAliases.size);
  });

  test("Q1 and Q2 parse successfully with the DeduplicatingRSPQLParser", () => {
    const parser = service.createParser();
    
    const q1Parsed = parser.parse(queries[0].query);
    expect(q1Parsed).toBeDefined();
    expect(q1Parsed.sparql).toBeDefined();

    const q2Parsed = parser.parse(queries[1].query);
    expect(q2Parsed).toBeDefined();
    expect(q2Parsed.sparql).toBeDefined();
  });

  test("containment adapter accepts supported generated queries and detects equivalent aggregation", () => {
    const q1 = queries[0].query;
    const q1Parsed = service.parseForChecker(q1);
    expect(q1Parsed.aggregationFunction).toBe("AVG");

    const q2 = queries[1].query;
    const q2Parsed = service.parseForChecker(q2);
    expect(q2Parsed.aggregationFunction).toBe("AVG");
  });
});

describe("Explicit Runtime-Producer Watermark and Contribution Tracking", () => {
  let latestWatermarkByRuntimeProducer;
  let acceptedContributions;
  let requiredRuntimeProducerIds;

  beforeEach(() => {
    latestWatermarkByRuntimeProducer = new Map();
    acceptedContributions = new Set();
    requiredRuntimeProducerIds = ["runtime-1", "runtime-2"];
  });

  function processChunk({ runtimeProducerId, chunkTimestamp, start, end }) {
    const prevWatermark = latestWatermarkByRuntimeProducer.get(runtimeProducerId);
    let accepted = false;

    if (prevWatermark === undefined || chunkTimestamp >= prevWatermark) {
      latestWatermarkByRuntimeProducer.set(runtimeProducerId, chunkTimestamp);
      accepted = true;
    }

    const contributionKey = `${runtimeProducerId}:${start}:${end}`;
    const isDuplicateContribution = acceptedContributions.has(contributionKey);
    if (accepted && !isDuplicateContribution) {
      acceptedContributions.add(contributionKey);
    }

    const allRuntimeProducersHaveWatermark = requiredRuntimeProducerIds.every(
      (requiredRuntimeProducerId) =>
        latestWatermarkByRuntimeProducer.has(requiredRuntimeProducerId),
    );
    const reconstructionWatermark = allRuntimeProducersHaveWatermark
      ? Math.min(...requiredRuntimeProducerIds.map(
          (requiredRuntimeProducerId) =>
            latestWatermarkByRuntimeProducer.get(requiredRuntimeProducerId),
        ))
      : null;

    return { accepted, isDuplicateContribution, reconstructionWatermark };
  }

  test("distinct runtime producers maintain independent watermark state", () => {
    const res1 = processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 100, start: 0, end: 100 });
    const res2 = processChunk({ runtimeProducerId: "runtime-2", chunkTimestamp: 100, start: 0, end: 100 });

    expect(res1.accepted).toBe(true);
    expect(res2.accepted).toBe(true);
    expect(latestWatermarkByRuntimeProducer.get("runtime-1")).toBe(100);
    expect(latestWatermarkByRuntimeProducer.get("runtime-2")).toBe(100);
    expect(res2.reconstructionWatermark).toBe(100);
  });

  test("uses the minimum required runtime-producer watermark", () => {
    processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 120, start: 0, end: 120 });
    const res2 = processChunk({ runtimeProducerId: "runtime-2", chunkTimestamp: 100, start: 0, end: 100 });
    expect(res2.reconstructionWatermark).toBe(100);
  });

  test("deduplicates contributions by explicit runtime producer and interval", () => {
    const res1 = processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 120, start: 0, end: 120 });
    const res2 = processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 120, start: 0, end: 120 });
    expect(res1.isDuplicateContribution).toBe(false);
    expect(res2.isDuplicateContribution).toBe(true);
  });

  test("rejects runtime-producer watermark regression", () => {
    processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 120, start: 0, end: 120 });
    const result = processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 110, start: 0, end: 110 });
    expect(result.accepted).toBe(false);
    expect(latestWatermarkByRuntimeProducer.get("runtime-1")).toBe(120);
  });

  test("counts the same interval once per runtime producer", () => {
    processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 100, start: 0, end: 100 });
    processChunk({ runtimeProducerId: "runtime-2", chunkTimestamp: 100, start: 0, end: 100 });
    const duplicate = processChunk({ runtimeProducerId: "runtime-1", chunkTimestamp: 100, start: 0, end: 100 });
    expect(duplicate.isDuplicateContribution).toBe(true);
    expect(acceptedContributions.size).toBe(2);
  });
});

describe("Chunk Recomposition and Coverage", () => {
  let operator;
  let state;

  beforeEach(() => {
    operator = new StreamingQueryChunkAggregatorOperator();
    operator.windowRange = 120000;
    operator.windowSlide = 60000;
    operator.outputQuery = "REGISTER RStream <mqtt://localhost/out> AS SELECT (AVG(?value) AS ?resultValue) (COUNT(?value) AS ?eventCount) (SUM(?value) AS ?sumValue) FROM NAMED WINDOW <w1> ON STREAM s1 [RANGE 120000 STEP 60000] WHERE { }";
    
    state = {
      chunksByWindow: new Map(),
      chunkCoverageByWindow: new Map(),
      completedChunkGroups: new Map(),
      orderedCompletedChunkGroups: [],
      readyChunkGroupSet: new Set(),
      readyChunkGroupIds: [],
      nextComparableWindowStartIndex: 0,
      nextComparableWindowStartMs: 1785924000000,
      expectedSubqueryIds: ["thing1", "thing2"],
      outputAggregationFunction: "AVG",
      chunksPerComparableWindow: 2,
      chunkGroupsPerOutputStep: 1,
      chunkWindowWidthMs: 60000,
      alignmentOriginMs: 1785924000000,
      comparableOutputCadenceOnly: true,
    };
  });

  test("same interval from two producers counts as two contributions, duplicate counts once", () => {
    const c1 = {
      queryId: "q",
      subqueryId: "thing1",
      chunkId: "c1",
      aggregateFunction: "AVG",
      count: 2,
      sum: 20,
      value: 10,
      window: { start: 1785924000000, end: 1785924060000 },
    };
    
    const outcome1 = operator.collectChunkByWindow(
      state.chunksByWindow,
      c1,
      ["thing1", "thing2"]
    );
    expect(outcome1.isComplete).toBe(false);
    expect(outcome1.missingSubqueryIds).toEqual(["thing2"]);

    const outcome2 = operator.collectChunkByWindow(
      state.chunksByWindow,
      c1,
      ["thing1", "thing2"]
    );
    expect(outcome2.isComplete).toBe(false);

    const c2 = {
      queryId: "q",
      subqueryId: "thing2",
      chunkId: "c2",
      aggregateFunction: "AVG",
      count: 1,
      sum: 30,
      value: 30,
      window: { start: 1785924000000, end: 1785924060000 },
    };
    const outcome3 = operator.collectChunkByWindow(
      state.chunksByWindow,
      c2,
      ["thing1", "thing2"]
    );
    expect(outcome3.isComplete).toBe(true);
    expect(outcome3.missingSubqueryIds).toEqual([]);
  });

  test("Q1 completes without thing2 but Q2 does not complete without thing2", () => {
    const c1 = {
      queryId: "q",
      subqueryId: "thing1",
      chunkId: "c1",
      aggregateFunction: "AVG",
      count: 2,
      sum: 20,
      value: 10,
      window: { start: 1785924000000, end: 1785924060000 },
    };

    const outcomeQ1 = operator.collectChunkByWindow(
      state.chunksByWindow,
      c1,
      ["thing1"]
    );
    expect(outcomeQ1.isComplete).toBe(true);

    state.chunksByWindow.clear();

    const outcomeQ2 = operator.collectChunkByWindow(
      state.chunksByWindow,
      c1,
      ["thing1", "thing2"]
    );
    expect(outcomeQ2.isComplete).toBe(false);
  });
});
