jest.mock('mqtt', () => ({
    connect: jest.fn().mockReturnValue({
        on: jest.fn().mockReturnThis(),
        subscribe: jest.fn(),
        publish: jest.fn()
    })
}));

jest.mock('../../util/logger/CSVLogger', () => ({
    CSVLogger: jest.fn().mockImplementation(() => ({ log: jest.fn() }))
}));

// Prevent latency log file creation during tests
jest.mock('fs', () => ({
    ...jest.requireActual('fs'),
    existsSync: jest.fn().mockReturnValue(true),
    createWriteStream: jest.fn().mockReturnValue({ write: jest.fn(), end: jest.fn() })
}));

import { StreamingQueryChunkAggregatorOperator } from './StreamingQueryChunkAggregatorOperator';

const QUERY_SINGLE_WINDOW = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
WHERE {
    WINDOW :w1 { ?sensor :value ?v }
}`;

const QUERY_RANGE_20_STEP_5 = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 20 STEP 5]
WHERE {
    WINDOW :w1 { ?sensor :value ?v }
}`;

describe('StreamingQueryChunkAggregatorOperator', () => {
    let operator: StreamingQueryChunkAggregatorOperator;

    beforeEach(() => {
        jest.clearAllMocks();
        operator = new StreamingQueryChunkAggregatorOperator();
    });

    describe('subQuery management', () => {
        test('should start with no subqueries', () => {
            expect(operator.getSubQueries()).toHaveLength(0);
        });

        test('should add a subquery', () => {
            operator.addSubQuery(QUERY_SINGLE_WINDOW);
            expect(operator.getSubQueries()).toHaveLength(1);
            expect(operator.getSubQueries()[0]).toBe(QUERY_SINGLE_WINDOW);
        });

        test('should add multiple subqueries', () => {
            operator.addSubQuery(QUERY_SINGLE_WINDOW);
            operator.addSubQuery(QUERY_RANGE_20_STEP_5);
            expect(operator.getSubQueries()).toHaveLength(2);
        });

        test('should clear all subqueries', () => {
            operator.addSubQuery(QUERY_SINGLE_WINDOW);
            operator.addSubQuery(QUERY_RANGE_20_STEP_5);
            operator.clearSubQueries();
            expect(operator.getSubQueries()).toHaveLength(0);
        });
    });

    describe('output query management', () => {
        test('should start with an empty output query', () => {
            expect(operator.getOutputQuery()).toBe('');
        });

        test('should set the output query', () => {
            operator.setOutputQuery(QUERY_SINGLE_WINDOW);
            expect(operator.getOutputQuery()).toBe(QUERY_SINGLE_WINDOW);
        });

        test('should overwrite a previously set output query', () => {
            operator.setOutputQuery(QUERY_SINGLE_WINDOW);
            operator.setOutputQuery(QUERY_RANGE_20_STEP_5);
            expect(operator.getOutputQuery()).toBe(QUERY_RANGE_20_STEP_5);
        });
    });

    describe('findGCD', () => {
        test('should return GCD of two numbers', () => {
            expect(operator.findGCD([10, 2])).toBe(2);
        });

        test('should return GCD of multiple numbers', () => {
            expect(operator.findGCD([12, 8, 4])).toBe(4);
        });

        test('should return 1 for coprime numbers', () => {
            expect(operator.findGCD([7, 13])).toBe(1);
        });

        test('should return 1 for an empty array', () => {
            expect(operator.findGCD([])).toBe(1);
        });

        test('should return the number itself for a single-element array', () => {
            expect(operator.findGCD([6])).toBe(6);
        });
    });

    describe('findLCM', () => {
        test('should return LCM of two numbers', () => {
            expect(operator.findLCM([4, 6])).toBe(12);
        });

        test('should return LCM of multiple numbers', () => {
            expect(operator.findLCM([2, 3, 4])).toBe(12);
        });

        test('should return the number itself for a single-element array', () => {
            expect(operator.findLCM([7])).toBe(7);
        });
    });

    describe('findGCDChunk', () => {
        test('should extract GCD from matching subquery and output query window parameters', () => {
            operator.addSubQuery(QUERY_SINGLE_WINDOW);
            operator.setOutputQuery(QUERY_SINGLE_WINDOW);
            // RANGE 10 STEP 2 → window_params = [10, 2, 10, 2] → GCD = 2
            const gcd = operator.findGCDChunk(operator.getSubQueries(), operator.getOutputQuery());
            expect(gcd).toBe(2);
        });

        test('should return a positive number for valid queries', () => {
            operator.addSubQuery(QUERY_SINGLE_WINDOW);
            operator.setOutputQuery(QUERY_RANGE_20_STEP_5);
            const gcd = operator.findGCDChunk(operator.getSubQueries(), operator.getOutputQuery());
            expect(gcd).toBeGreaterThan(0);
        });

        test('should return 1 for an empty subQueries array and a valid output query', () => {
            operator.setOutputQuery(QUERY_SINGLE_WINDOW);
            const gcd = operator.findGCDChunk([], operator.getOutputQuery());
            // Only output query params → GCD([10, 2]) = 2
            expect(gcd).toBe(2);
        });
    });

    describe('detectAggregationFunction', () => {
        test('should detect AVG', () => {
            expect(operator.detectAggregationFunction('SELECT (AVG(?v) AS ?avg)')).toBe('AVG');
        });

        test('should detect SUM', () => {
            expect(operator.detectAggregationFunction('SELECT (SUM(?v) AS ?total)')).toBe('SUM');
        });

        test('should detect COUNT', () => {
            expect(operator.detectAggregationFunction('SELECT (COUNT(?v) AS ?n)')).toBe('COUNT');
        });

        test('should detect MIN', () => {
            expect(operator.detectAggregationFunction('SELECT (MIN(?v) AS ?min)')).toBe('MIN');
        });

        test('should detect MAX', () => {
            expect(operator.detectAggregationFunction('SELECT (MAX(?v) AS ?max)')).toBe('MAX');
        });

        test('should return null when no aggregation function is present', () => {
            expect(operator.detectAggregationFunction('SELECT ?v WHERE { ?s ?p ?v }')).toBeNull();
        });
    });

    describe('getAggregationSPARQLQuery', () => {
        test('should generate a weighted-average SPARQL query for AVG', () => {
            const query = operator.getAggregationSPARQLQuery('AVG', 'o');
            expect(query).toContain('SUM(?val * ?cnt)');
            expect(query).toContain('saref:hasValue');
            expect(query).toContain('saref:hasCount');
        });

        test('should generate a SUM SPARQL query', () => {
            const query = operator.getAggregationSPARQLQuery('SUM', 'val');
            expect(query).toContain('SUM(?val)');
            expect(query).toContain('AS ?result');
        });

        test('should generate a COUNT SPARQL query', () => {
            const query = operator.getAggregationSPARQLQuery('COUNT', 'val');
            expect(query).toContain('SUM(?val)');
            expect(query).toContain('saref:hasValue');
        });

        test('should use saref:hasValue as the source predicate', () => {
            const query = operator.getAggregationSPARQLQuery('SUM', 'myVar');
            expect(query).toContain('saref:hasValue');
        });

        test('should return empty string for an invalid aggregation function', () => {
            const query = operator.getAggregationSPARQLQuery('INVALID', 'val');
            expect(query).toBe('');
        });

        test('should return empty string when aggregation function is missing', () => {
            const query = operator.getAggregationSPARQLQuery('', 'val');
            expect(query).toBe('');
        });
    });

    describe('generateOutputQueryEvent', () => {
        test('should produce a valid RDF triple string', () => {
            const event = operator.generateOutputQueryEvent('42.0');
            expect(event).toContain('<https://rsp.js/outputQueryEvent/');
            expect(event).toContain('<https://saref.etsi.org/core/hasValue>');
            expect(event).toContain('"42.0"^^<http://www.w3.org/2001/XMLSchema#float>');
        });

        test('should include a unique UUID for each call', () => {
            const event1 = operator.generateOutputQueryEvent('1.0');
            const event2 = operator.generateOutputQueryEvent('1.0');
            // Extract UUIDs from the IRIs
            const uuid1 = event1.match(/outputQueryEvent\/([^>]+)/)?.[1];
            const uuid2 = event2.match(/outputQueryEvent\/([^>]+)/)?.[1];
            expect(uuid1).not.toBe(uuid2);
        });
    });
});
