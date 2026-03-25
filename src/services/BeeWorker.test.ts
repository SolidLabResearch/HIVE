import { BeeWorker } from './BeeWorker';
import { ContainmentChecker } from 'rspql-containment-checker';

jest.mock('./operators/StreamingQueryChunkAggregatorOperator', () => ({
    StreamingQueryChunkAggregatorOperator: jest.fn().mockImplementation(() => ({
        addOutputQuery: jest.fn(),
        addSubQuery: jest.fn(),
        getSubQueries: jest.fn().mockReturnValue([]),
        init: jest.fn().mockResolvedValue(undefined),
        handleAggregation: jest.fn(),
    })),
}));

jest.mock('./operators/RateBasedApproximationApproachOperator', () => ({
    ApproximationApproachOperator: jest.fn().mockImplementation(() => ({
        addOutputQuery: jest.fn(),
        addSubQuery: jest.fn(),
        getSubQueries: jest.fn().mockReturnValue([]),
        init: jest.fn().mockResolvedValue(undefined),
        handleAggregation: jest.fn(),
    })),
}));

// The two queries from the user, wrapped in RSP-QL format required by ContainmentChecker.
// The subquery has an extra triple pattern (hasValue) so every answer it produces
// is also an answer of the super query — making it contained.
const SUPER_QUERY = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output> AS
SELECT ?s1
FROM NAMED WINDOW ex:w1 ON STREAM ex:stream1 [RANGE 60000 STEP 60000]
WHERE {
    WINDOW ex:w1 {
        ?s1 <http://saref.org/relatesToProperty> <http://example.org/wearableX> .
    }
}`;

const SUB_QUERY = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output> AS
SELECT ?s1
FROM NAMED WINDOW ex:w1 ON STREAM ex:stream1 [RANGE 60000 STEP 60000]
WHERE {
    WINDOW ex:w1 {
        ?s1 <http://saref.org/hasValue> ?value .
        ?s1 <http://saref.org/relatesToProperty> <http://example.org/wearableX> .
    }
}`;

function makeBeeWorker(registeredQuery: string): BeeWorker {
    process.env.OPERATOR_TYPE = 'StreamingQueryChunkAggregatorOperator';
    process.env.QUERY = registeredQuery;
    process.env.TOPIC = 'test-topic';
    delete process.env.SUB_QUERIES;

    global.fetch = jest.fn().mockResolvedValue({
        text: jest.fn().mockResolvedValue('{}'),
    }) as unknown as typeof fetch;

    return new BeeWorker();
}

// ---------------------------------------------------------------------------
// 1. ContainmentChecker directly — verifies the fundamental relationship
// ---------------------------------------------------------------------------
describe('ContainmentChecker — subquery vs super query', () => {
    let checker: ContainmentChecker;

    beforeEach(() => {
        checker = new ContainmentChecker();
    });

    test('subquery IS contained in the super query', async () => {
        // SUB_QUERY has more constraints, so its answers are a strict subset
        const result = await checker.checkContainment(SUB_QUERY, SUPER_QUERY);
        expect(result).toBe(true);
    });

    test('super query is NOT contained in the subquery (reverse is false)', async () => {
        // SUPER_QUERY is broader; its answers are NOT a subset of SUB_QUERY's answers
        const result = await checker.checkContainment(SUPER_QUERY, SUB_QUERY);
        expect(result).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// 2. BeeWorker.findContainedQueries — checks the orchestrator-level logic
// ---------------------------------------------------------------------------
describe('BeeWorker.findContainedQueries', () => {
    test('returns the subquery when registered query is the super query', async () => {
        const worker = makeBeeWorker(SUPER_QUERY);
        const extractedQueries = [{ rspql_query: SUB_QUERY, r2s_topic: 'test' }];

        const contained = await worker.findContainedQueries(extractedQueries);

        expect(contained).toHaveLength(1);
        expect(contained[0]).toBe(SUB_QUERY);
    });

    test('returns nothing when registered query is the subquery (super query not contained)', async () => {
        const worker = makeBeeWorker(SUB_QUERY);
        const extractedQueries = [{ rspql_query: SUPER_QUERY, r2s_topic: 'test' }];

        const contained = await worker.findContainedQueries(extractedQueries);

        expect(contained).toHaveLength(0);
    });

    test('returns nothing for an empty extracted queries list', async () => {
        const worker = makeBeeWorker(SUPER_QUERY);

        const contained = await worker.findContainedQueries([]);

        expect(contained).toHaveLength(0);
    });
});

// ---------------------------------------------------------------------------
// 3. BeeWorker.validateQueryContainment — tests the fixed bug
// ---------------------------------------------------------------------------
describe('BeeWorker.validateQueryContainment', () => {
    test('returns true for two equivalent queries', async () => {
        const worker = makeBeeWorker(SUPER_QUERY);
        const result = await worker.validateQueryContainment(SUPER_QUERY, SUPER_QUERY);
        expect(result).toBe(true);
    });

    test('returns false when no containment exists in either direction (bug fix)', async () => {
        // Completely unrelated query — neither contains the other
        const unrelatedQuery = `
PREFIX ex: <http://example.org/>
REGISTER RStream <output> AS
SELECT ?x
FROM NAMED WINDOW ex:w2 ON STREAM ex:stream2 [RANGE 60000 STEP 60000]
WHERE {
    WINDOW ex:w2 {
        ?x <http://example.org/differentPredicate> <http://example.org/differentObject> .
    }
}`;
        const worker = makeBeeWorker(SUPER_QUERY);
        const result = await worker.validateQueryContainment(SUPER_QUERY, unrelatedQuery);
        expect(result).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// 4. Orchestration queries — SUB_QUERY_1, SUB_QUERY_2, and ORCHESTRATION_SUPER_QUERY
//    These are the real queries used in the chunked/approximation experiments.
//    Both subqueries cover a single sensor stream with a 60s/30s window;
//    the superquery spans both streams with a 120s/60s window and uses UNION.
// ---------------------------------------------------------------------------

const ORCH_SUB_QUERY_1 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <subquery1_output> AS
SELECT (AVG(?value) AS ?avgWearableX)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/wearableX> {
        ?s1 saref:hasValue ?value .
        ?s1 saref:relatesToProperty dahccsensors:wearableX .
    }
}
`;

const ORCH_SUB_QUERY_2 = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <subquery2_output> AS
SELECT (AVG(?value) AS ?avgSmartphoneX)
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 60000 STEP 30000]
WHERE {
    WINDOW <mqtt://localhost:1883/smartphoneX> {
        ?s2 saref:hasValue ?value .
        ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
    }
}
`;

const ORCH_SUPER_QUERY = `
PREFIX mqtt_broker: <mqtt://localhost:1883/>
PREFIX saref: <https://saref.etsi.org/core/>
PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
PREFIX : <https://rsp.js>
REGISTER RStream <sensor_averages> AS
SELECT (AVG(?value) AS ?avgValue) (COUNT(?value) AS ?countValue)
FROM NAMED WINDOW <mqtt://localhost:1883/wearableX> ON STREAM mqtt_broker:wearableX [RANGE 120000 STEP 60000]
FROM NAMED WINDOW <mqtt://localhost:1883/smartphoneX> ON STREAM mqtt_broker:smartphoneX [RANGE 120000 STEP 60000]
WHERE {
    {
        WINDOW <mqtt://localhost:1883/wearableX> {
            ?s1 saref:hasValue ?value .
            ?s1 saref:relatesToProperty dahccsensors:wearableX .
        }
    } UNION {
        WINDOW <mqtt://localhost:1883/smartphoneX> {
            ?s2 saref:hasValue ?value .
            ?s2 saref:relatesToProperty dahccsensors:smartphoneX .
        }
    }
}
`;

describe('Orchestration queries — ContainmentChecker direct', () => {
    let checker: ContainmentChecker;

    beforeEach(() => {
        checker = new ContainmentChecker();
    });

    test('SUB_QUERY_1 IS contained in SUPER_QUERY (after aggregation removal)', async () => {
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const strippedSub = worker.removeAggregationFunctions(ORCH_SUB_QUERY_1);
        const strippedSuper = worker.removeAggregationFunctions(ORCH_SUPER_QUERY);
        const result = await checker.checkContainment(strippedSub, strippedSuper);
        expect(result).toBe(true);
    });

    test('SUB_QUERY_2 IS contained in SUPER_QUERY (after aggregation removal)', async () => {
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const strippedSub = worker.removeAggregationFunctions(ORCH_SUB_QUERY_2);
        const strippedSuper = worker.removeAggregationFunctions(ORCH_SUPER_QUERY);
        const result = await checker.checkContainment(strippedSub, strippedSuper);
        expect(result).toBe(true);
    });

    test('SUPER_QUERY vs SUB_QUERY_1 reverse: checker returns true (UNION false-positive)', async () => {
        // Known limitation: the checker returns true for checkContainment(SUPER, SUB1) even though
        // SUPER covers wearableX UNION smartphoneX while SUB1 covers wearableX only.
        // The checker incorrectly considers SUPER ⊆ SUB1, likely because the first UNION arm of
        // SUPER shares the same stream and variable names (?s1) as SUB1, and the checker matches
        // only that arm rather than evaluating the full UNION.
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const strippedSub = worker.removeAggregationFunctions(ORCH_SUB_QUERY_1);
        const strippedSuper = worker.removeAggregationFunctions(ORCH_SUPER_QUERY);
        const result = await checker.checkContainment(strippedSuper, strippedSub);
        expect(result).toBe(true); // false positive from the library
    });

    test('SUPER_QUERY is NOT contained in SUB_QUERY_2 (checker correctly returns false)', async () => {
        // The checker correctly returns false here: SUPER uses ?s1/?s2 across two streams,
        // SUB2 only covers smartphoneX with ?s2. The first UNION arm of SUPER (?s1/wearableX)
        // does not match SUB2 (?s2/smartphoneX), so no false positive occurs.
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const strippedSub = worker.removeAggregationFunctions(ORCH_SUB_QUERY_2);
        const strippedSuper = worker.removeAggregationFunctions(ORCH_SUPER_QUERY);
        const result = await checker.checkContainment(strippedSuper, strippedSub);
        expect(result).toBe(false);
    });
});

describe('Orchestration queries — BeeWorker.findContainedQueries', () => {
    test('finds both subqueries as contained in the super query', async () => {
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const extractedQueries = [
            { rspql_query: ORCH_SUB_QUERY_1, r2s_topic: 'wearableX' },
            { rspql_query: ORCH_SUB_QUERY_2, r2s_topic: 'smartphoneX' },
        ];

        const contained = await worker.findContainedQueries(extractedQueries);

        expect(contained).toHaveLength(2);
        expect(contained).toContain(ORCH_SUB_QUERY_1);
        expect(contained).toContain(ORCH_SUB_QUERY_2);
    });

    test('SUPER_QUERY is falsely found as contained when registered = SUB_QUERY_1 (UNION false-positive)', async () => {
        // This test documents a false positive: when registered = SUB_QUERY_1 and the server
        // has SUPER_QUERY running, findContainedQueries incorrectly returns SUPER_QUERY as
        // contained. This is caused by the same UNION false-positive in the checker described above.
        // In practice this means the orchestrator could try to aggregate from a broader query
        // as if it were a sub-component, which would produce wrong results.
        const worker = makeBeeWorker(ORCH_SUB_QUERY_1);
        const extractedQueries = [
            { rspql_query: ORCH_SUPER_QUERY, r2s_topic: 'sensor_averages' },
        ];

        const contained = await worker.findContainedQueries(extractedQueries);

        expect(contained).toHaveLength(1); // false positive — ideally should be 0
    });
});

// ---------------------------------------------------------------------------
// 5. Query combination + validateQueryContainment
//    Mirrors the actual process() flow: combine SUB_QUERY_1 + SUB_QUERY_2
//    via QueryCombiner, then validate the combined result against SUPER_QUERY.
// ---------------------------------------------------------------------------
describe('Orchestration queries — combined query vs super query', () => {
    // For the subqueries to fully answer the super query, both directions must hold:
    //
    //   Soundness:    combined ⊆ super  (combined produces no answers outside super)
    //   Completeness: super ⊆ combined  (combined misses no answers that super would produce)
    //
    // Both together mean the queries are equivalent and the subqueries can entirely
    // replace the super query. validateQueryContainment checks exactly this.
    //
    // NOTE: the ContainmentChecker strips the RSP-QL header (REGISTER, FROM NAMED WINDOW)
    // entirely before comparing — it only sees the SPARQL WHERE + SELECT.
    // Window sizes (RANGE/STEP) are therefore NOT the issue here.
    //
    // The actual failure cause is a SELECT variable name mismatch:
    //   - QueryCombiner produces:  SELECT ?avgWearableX ?avgSmartphoneX
    //   - removeAggregationFunctions on super produces: SELECT ?value ?value
    // These projections are structurally different, so the checker cannot prove equivalence.

    let combinedQuery: string;

    beforeEach(async () => {
        const { QueryCombiner } = await import('hive-thought-rewriter');
        const combiner = new QueryCombiner();
        combiner.addQuery(ORCH_SUB_QUERY_1);
        combiner.addQuery(ORCH_SUB_QUERY_2);
        combinedQuery = combiner.ParsedToString(combiner.combine());
    });

    test('soundness: combined ⊆ super — fails due to SELECT variable name mismatch after combination', async () => {
        // QueryCombiner preserves the aggregation aliases (?avgWearableX, ?avgSmartphoneX)
        // as plain SELECT variables. removeAggregationFunctions does not strip these
        // (they are not in (FUNC(?x) AS ?alias) form). The super stripped SELECT is ?value ?value.
        // The checker sees different projection variables and cannot confirm soundness.
        const checker = new ContainmentChecker();
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const strippedCombined = worker.removeAggregationFunctions(combinedQuery);
        const strippedSuper = worker.removeAggregationFunctions(ORCH_SUPER_QUERY);

        const isSound = await checker.checkContainment(strippedCombined, strippedSuper);
        expect(isSound).toBe(false); // SELECT ?avgWearableX ?avgSmartphoneX ≠ SELECT ?value ?value
    });

    test('completeness: super ⊆ combined — fails due to SELECT variable name mismatch after combination', async () => {
        // Same root cause as soundness: the combined SELECT projects ?avgWearableX and
        // ?avgSmartphoneX, while the stripped super projects ?value twice. The WHERE
        // clauses are structurally identical (both are the same wearableX UNION smartphoneX
        // pattern), so fixing the SELECT projection mismatch would make this pass.
        const checker = new ContainmentChecker();
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const strippedCombined = worker.removeAggregationFunctions(combinedQuery);
        const strippedSuper = worker.removeAggregationFunctions(ORCH_SUPER_QUERY);

        const isComplete = await checker.checkContainment(strippedSuper, strippedCombined);
        expect(isComplete).toBe(false); // SELECT ?avgWearableX ?avgSmartphoneX ≠ SELECT ?value ?value
    });

    test('validateQueryContainment: both soundness AND completeness fail — root cause is SELECT variable mismatch not window size', async () => {
        // The checker ignores window sizes (strips RSP-QL header before comparing).
        // The real blocker is that QueryCombiner outputs ?avgWearableX/?avgSmartphoneX
        // in SELECT while the stripped super uses ?value/?value.
        // Fixing removeAggregationFunctions to also normalise plain alias variables in
        // the combined query, or aligning the SELECT projections, would unblock this.
        const worker = makeBeeWorker(ORCH_SUPER_QUERY);
        const isValid = await worker.validateQueryContainment(ORCH_SUPER_QUERY, combinedQuery);
        expect(isValid).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// 6. BeeWorker.removeAggregationFunctions
// ---------------------------------------------------------------------------
describe('BeeWorker.removeAggregationFunctions', () => {
    let worker: BeeWorker;

    beforeEach(() => {
        worker = makeBeeWorker(SUPER_QUERY);
    });

    test('strips AVG aggregation', () => {
        const query = 'SELECT (AVG(?value) AS ?avgValue) WHERE { ?s ?p ?value }';
        expect(worker.removeAggregationFunctions(query)).toBe('SELECT ?value WHERE { ?s ?p ?value }');
    });

    test('strips COUNT aggregation', () => {
        const query = 'SELECT (COUNT(?x) AS ?count) WHERE { ?s ?p ?x }';
        expect(worker.removeAggregationFunctions(query)).toBe('SELECT ?x WHERE { ?s ?p ?x }');
    });

    test('leaves queries without aggregation unchanged', () => {
        const query = 'SELECT ?s1 WHERE { ?s1 ?p ?o }';
        expect(worker.removeAggregationFunctions(query)).toBe(query);
    });
});
