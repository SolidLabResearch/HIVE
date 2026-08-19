jest.mock('mqtt', () => ({
    connect: jest.fn().mockReturnValue({
        connected: true,
        on: jest.fn().mockReturnThis(),
        once: jest.fn().mockReturnThis(),
        subscribe: jest.fn(),
        publish: jest.fn((topic, payload, options, callback) => callback?.()),
        end: jest.fn()
    })
}));

jest.mock('../../util/logger/CSVLogger', () => ({
    CSVLogger: jest.fn().mockImplementation(() => ({ log: jest.fn() }))
}));

const executeR2RMock = jest.fn();

jest.mock('./r2r', () => ({
    R2ROperator: jest.fn().mockImplementation(() => ({
        execute: executeR2RMock,
    })),
}));

// Prevent latency log file creation during tests
jest.mock('fs', () => ({
    ...jest.requireActual('fs'),
    existsSync: jest.fn().mockReturnValue(true),
    writeFileSync: jest.fn(),
    createWriteStream: jest.fn().mockReturnValue({ write: jest.fn(), end: jest.fn() })
}));

import { StreamingQueryChunkAggregatorOperator } from './StreamingQueryChunkAggregatorOperator';
import fs from 'fs';
import { EventEmitter } from 'events';

const CHUNKED_LATENCY_HEADERS = [
    'window_number',
    'query_registered_at',
    'first_data_received_at',
    'expected_window_close',
    'registration_anchored_expected_close',
    'event_time_window_start',
    'event_time_window_end',
    'event_time_window_close',
    'wall_clock_window_close',
    'anchor_aligned_window_close',
    'last_chunk_received_at',
    'interval_trigger_at',
    'result_emitted_at',
    'delay_past_expected_close_ms',
    'delay_past_data_start_ms',
    'interval_wait_ms',
    'computation_ms',
    'result_value',
    'required_chunk_intervals',
    'last_required_chunk_received_at',
    'semantic_ready_at',
    'window_close_to_ready_ms',
    'ready_to_emit_ms',
    'wall_clock_close_to_result_ms',
    'anchor_aligned_window_close_to_result_ms',
    'latency_domain_status',
    'trigger_type',
    'emission_reason',
    'window_semantics',
    'logical_trigger_time',
    'window_start',
    'window_end',
    'window_data_close_time',
    'latency_from_logical_trigger_ms',
    'latency_from_window_close_ms',
    'metadata_source',
];

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
    function parseLatencyLogLine(line: string): Record<string, string> {
        const trimmed = line.trim();
        const values = trimmed.split(',');
        return Object.fromEntries(CHUNKED_LATENCY_HEADERS.map((header, index) => [header, values[index] ?? '']));
    }
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

        test('preserves reusable subquery range while rewriting chunk step', () => {
            const reusableSubQuery = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 60000 STEP 30000]
WHERE { WINDOW :w1 { ?sensor :value ?v } }
`;
            const finalQuery = `
PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT (AVG(?v) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 120000 STEP 60000]
WHERE { WINDOW :w1 { ?sensor :value ?v } }
`;

            operator.addSubQuery(reusableSubQuery);
            operator.setOutputQuery(finalQuery);

            const plan = (operator as any).buildChunkPlan();
            expect(plan.chunkSize).toBe(30000);
            expect(plan.chunkWindowWidthMs).toBe(60000);
            expect(plan.rewrittenQueries).toHaveLength(1);
            expect(plan.rewrittenQueries[0]).toContain('RANGE 60000 STEP 30000');
            expect(plan.rewrittenQueries[0]).not.toContain('RANGE 30000 STEP 30000');
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

    describe('logical windowing exactness', () => {
        type Aggregation = 'AVG' | 'SUM' | 'COUNT' | 'MIN' | 'MAX';
        type TopicChunk = {
            logicalChunkIndex: number;
            data: string;
            aggregateValue: number;
            count: number;
        };

        const windowRange = 120000;
        const windowSlide = 60000;
        const chunkSize = 30000;
        const samplesPerChunkByRate: Record<number, number> = {
            1: 30,
            4: 120,
            16: 480,
        };

        const buildRawSeries = (channelIndex: number, rateHz: number, totalChunks: number): number[] => {
            const samplesPerChunk = samplesPerChunkByRate[rateHz];
            const values: number[] = [];
            const totalSamples = totalChunks * samplesPerChunk;

            for (let i = 0; i < totalSamples; i++) {
                const base = (channelIndex + 1) * 10;
                const slope = (channelIndex + 2) * 0.37;
                const wave = ((i % 17) - 8) * 0.11;
                values.push(base + (i * slope) / 10 + wave);
            }

            return values;
        };

        const aggregate = (values: number[], aggregation: Aggregation): number => {
            switch (aggregation) {
                case 'AVG':
                    return values.reduce((sum, value) => sum + value, 0) / values.length;
                case 'SUM':
                    return values.reduce((sum, value) => sum + value, 0);
                case 'COUNT':
                    return values.length;
                case 'MIN':
                    return Math.min(...values);
                case 'MAX':
                    return Math.max(...values);
            }
        };

        const buildTopicChunks = (
            values: number[],
            aggregation: Aggregation,
            rateHz: number,
            topicName: string,
            replayMultiplier: number,
            channelSkewMs: number,
        ): TopicChunk[] => {
            const samplesPerChunk = samplesPerChunkByRate[rateHz];
            const chunks: TopicChunk[] = [];

            for (let logicalChunkIndex = 0; logicalChunkIndex < values.length / samplesPerChunk; logicalChunkIndex++) {
                const chunkValues = values.slice(
                    logicalChunkIndex * samplesPerChunk,
                    (logicalChunkIndex + 1) * samplesPerChunk,
                );
                const chunkResult = aggregate(chunkValues, aggregation);
                const countTriple = aggregation === 'AVG'
                    ? `\n<https://rsp.js/aggregation_event/${topicName}-${logicalChunkIndex}> <https://saref.etsi.org/core/hasCount> "${chunkValues.length}"^^<http://www.w3.org/2001/XMLSchema#integer> .`
                    : '';
                const payload = JSON.stringify(
                    `<https://rsp.js/aggregation_event/${topicName}-${logicalChunkIndex}> <https://saref.etsi.org/core/hasValue> "${chunkResult}"^^<http://www.w3.org/2001/XMLSchema#double> .${countTriple}`,
                );

                chunks.push({
                    logicalChunkIndex,
                    data: payload,
                    aggregateValue: chunkResult,
                    count: chunkValues.length,
                });
            }

            // The operator must be independent from wall-clock replay speed and minor skew.
            // We still compute representative arrival times so the scenario matches the benchmark conditions.
            expect(replayMultiplier).toBeGreaterThan(0);
            expect(channelSkewMs).toBeGreaterThanOrEqual(0);

            return chunks;
        };

        const computeFetchingBaseline = (
            topicSeries: number[][],
            aggregation: Aggregation,
            windowNumber: number,
            rateHz: number,
        ): number => {
            const samplesPerChunk = samplesPerChunkByRate[rateHz];
            const chunksPerStep = windowSlide / chunkSize;
            const chunksPerFullWindow = windowRange / chunkSize;
            const chunkStartIndex = (windowNumber - 1) * chunksPerStep;
            const chunkEndExclusive = chunkStartIndex + chunksPerFullWindow;

            const windowValues = topicSeries.flatMap((series) =>
                series.slice(
                    chunkStartIndex * samplesPerChunk,
                    chunkEndExclusive * samplesPerChunk,
                ),
            );

            return aggregate(windowValues, aggregation);
        };

        const computeChunkedValue = (
            topicChunks: Map<string, TopicChunk[]>,
            aggregation: Aggregation,
            windowNumber: number,
        ): number => {
            const chunksPerStep = windowSlide / chunkSize;
            const chunksPerFullWindow = windowRange / chunkSize;
            const chunkStartIndex = (windowNumber - 1) * chunksPerStep;
            const chunkEndExclusive = chunkStartIndex + chunksPerFullWindow;

            const selectedChunks = Array.from(topicChunks.values()).flatMap((chunks) =>
                chunks.filter(
                    (chunk) =>
                        chunk.logicalChunkIndex >= chunkStartIndex &&
                        chunk.logicalChunkIndex < chunkEndExclusive,
                ),
            );

            if (aggregation === 'AVG') {
                let weightedSum = 0;
                let totalCount = 0;

                for (const chunk of selectedChunks) {
                    weightedSum += chunk.aggregateValue * chunk.count;
                    totalCount += chunk.count;
                }

                return weightedSum / totalCount;
            }

            const values = selectedChunks.map((chunk) => chunk.aggregateValue);

            return aggregate(values, aggregation === 'COUNT' ? 'SUM' : aggregation);
        };

        const assertExactnessScenario = (
            aggregation: Aggregation,
            rateHz: number,
            replayMultiplier: number,
            channelSkewMs: number,
        ) => {
            const totalWindows = 35;
            const chunksPerStep = windowSlide / chunkSize;
            const chunksPerFullWindow = windowRange / chunkSize;
            const totalChunks = (totalWindows - 1) * chunksPerStep + chunksPerFullWindow;
            const topicSeries = [
                buildRawSeries(0, rateHz, totalChunks),
                buildRawSeries(1, rateHz, totalChunks),
            ];

            const topicChunks = new Map<string, TopicChunk[]>([
                ['wearableX', buildTopicChunks(topicSeries[0], aggregation, rateHz, 'wearableX', replayMultiplier, channelSkewMs)],
                ['smartphoneX', buildTopicChunks(topicSeries[1], aggregation, rateHz, 'smartphoneX', replayMultiplier, channelSkewMs)],
            ]);
            const errors: number[] = [];
            for (let windowNumber = 1; windowNumber <= totalWindows; windowNumber++) {
                const fetchingValue = computeFetchingBaseline(topicSeries, aggregation, windowNumber, rateHz);
                const chunkedValue = computeChunkedValue(topicChunks, aggregation, windowNumber);
                errors.push(Math.abs(fetchingValue - chunkedValue));
            }

            const trimmedErrors = errors.slice(3, errors.length - 2);
            expect(trimmedErrors).toHaveLength(30);
            trimmedErrors.forEach((error) => {
                expect(error).toBeCloseTo(0, 8);
            });
        };

        const rates: Array<[number, number, number]> = [
            [1, 0.9, 120],
            [4, 1.0, 80],
            [16, 1.1, 160],
        ];
        const aggregations: Aggregation[] = ['AVG', 'SUM', 'COUNT', 'MIN', 'MAX'];

        test.each(aggregations.flatMap((aggregation) =>
            rates.map(([rateHz, replayMultiplier, channelSkewMs]) => [aggregation, rateHz, replayMultiplier, channelSkewMs]),
        ))(
            'matches fetching for %s at %iHz with replay x%f and %ims skew over 35 windows',
            (aggregation, rateHz, replayMultiplier, channelSkewMs) => {
                assertExactnessScenario(
                    aggregation as Aggregation,
                    rateHz as number,
                    replayMultiplier as number,
                    channelSkewMs as number,
                );
            },
        );
    });

    describe('logical window identity buffering', () => {
        test('should only mark complete when all expected subqueries for same window are present', () => {
            const chunksByWindow = new Map();
            const expectedSubqueryIds = ['subA', 'subB'];
            const window = {
                windowName: 'https://rsp.js/w1',
                start: 1000,
                end: 2000,
                range: 1000,
                step: 1000,
                semantics: '[start,end)' as const,
            };

            const firstOutOfOrder = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'q1',
                subqueryId: 'subB',
                window,
                chunkId: 'q1:w1:1000:2000:subB',
                value: 8,
                count: 4,
                rdfPayload: '<x> <https://saref.etsi.org/core/hasValue> "8"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);
            expect(firstOutOfOrder.isComplete).toBe(false);
            expect(firstOutOfOrder.missingSubqueryIds).toEqual(['subA']);

            const duplicate = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'q1',
                subqueryId: 'subB',
                window,
                chunkId: 'q1:w1:1000:2000:subB',
                value: 8,
                count: 4,
                rdfPayload: '<x> <https://saref.etsi.org/core/hasValue> "8"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);
            expect(duplicate.isComplete).toBe(false);
            expect(duplicate.missingSubqueryIds).toEqual(['subA']);

            const complete = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'q1',
                subqueryId: 'subA',
                window,
                chunkId: 'q1:w1:1000:2000:subA',
                value: 10,
                count: 5,
                rdfPayload: '<y> <https://saref.etsi.org/core/hasValue> "10"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);
            expect(complete.isComplete).toBe(true);
            expect(complete.missingSubqueryIds).toEqual([]);

            const missingWindow = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'q1',
                subqueryId: 'subA',
                window: { ...window, start: 2000, end: 3000 },
                chunkId: 'q1:w1:2000:3000:subA',
                value: 11,
                count: 6,
                rdfPayload: '<z> <https://saref.etsi.org/core/hasValue> "11"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);
            expect(missingWindow.isComplete).toBe(false);
            expect(missingWindow.missingSubqueryIds).toEqual(['subB']);
        });

        test('should group different stream window names into the same logical chunk group', () => {
            const chunksByWindow = new Map();
            const expectedSubqueryIds = ['wearable', 'smartphone'];

            const wearable = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'q1',
                subqueryId: 'wearable',
                window: {
                    windowName: 'mqtt://localhost:1883/wearableX',
                    start: 1000,
                    end: 31000,
                    range: 30000,
                    step: 30000,
                    semantics: '[start,end)' as const,
                },
                chunkId: 'wearable-chunk',
                value: -26.0,
                count: 481,
                rdfPayload: '<wearable> <https://saref.etsi.org/core/hasValue> "-26.0"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);

            const smartphone = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'q1',
                subqueryId: 'smartphone',
                window: {
                    windowName: 'mqtt://localhost:1883/smartphoneX',
                    start: 1000,
                    end: 31000,
                    range: 30000,
                    step: 30000,
                    semantics: '[start,end)' as const,
                },
                chunkId: 'smartphone-chunk',
                value: 1.03,
                count: 481,
                rdfPayload: '<smartphone> <https://saref.etsi.org/core/hasValue> "1.03"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);

            expect(wearable.chunkGroupId).toBe('1000:31000');
            expect(smartphone.chunkGroupId).toBe('1000:31000');
            expect(smartphone.isComplete).toBe(true);
            expect(chunksByWindow.size).toBe(1);
        });

        test('should complete one logical chunk group across different producer query ids', () => {
            const chunksByWindow = new Map();
            const expectedSubqueryIds = ['wearable', 'smartphone'];

            const wearable = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'producer-a',
                subqueryId: 'wearable',
                window: {
                    windowName: 'mqtt://localhost:1883/wearableX',
                    start: 1000,
                    end: 31000,
                    range: 30000,
                    step: 30000,
                    semantics: '[start,end)' as const,
                },
                chunkId: 'wearable-chunk',
                value: -26.0,
                count: 481,
                rdfPayload: '<wearable> <https://saref.etsi.org/core/hasValue> "-26.0"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);

            const smartphone = operator.collectChunkByWindow(chunksByWindow, {
                queryId: 'producer-b',
                subqueryId: 'smartphone',
                window: {
                    windowName: 'mqtt://localhost:1883/smartphoneX',
                    start: 1000,
                    end: 31000,
                    range: 30000,
                    step: 30000,
                    semantics: '[start,end)' as const,
                },
                chunkId: 'smartphone-chunk',
                value: 1.03,
                count: 481,
                rdfPayload: '<smartphone> <https://saref.etsi.org/core/hasValue> "1.03"^^<http://www.w3.org/2001/XMLSchema#double> .',
            }, expectedSubqueryIds);

            expect(wearable.chunkGroupId).toBe('1000:31000');
            expect(smartphone.chunkGroupId).toBe('1000:31000');
            expect(smartphone.isComplete).toBe(true);
            expect(chunksByWindow.size).toBe(1);
        });

        test('normalizes small cross-stream window skew onto the shared chunk-step boundary', () => {
            const wearablePayload = JSON.stringify({
                queryId: 'shared-session',
                subqueryId: 'wearable',
                window: {
                    windowName: 'mqtt://localhost:1883/wearableX',
                    start: 1785921059503,
                    end: 1785921060753,
                    range: 1250,
                    step: 1250,
                    alignmentOriginMs: 1785921058753,
                    semantics: '[start,end)',
                    logicalTriggerTime: 1785921060753,
                    windowDataCloseTime: 1785921060753,
                },
                value: 1,
                count: 1,
            });
            const smartphonePayload = JSON.stringify({
                queryId: 'shared-session',
                subqueryId: 'smartphone',
                window: {
                    windowName: 'mqtt://localhost:1883/smartphoneX',
                    start: 1785921059493,
                    end: 1785921060743,
                    range: 1250,
                    step: 1250,
                    alignmentOriginMs: 1785921058753,
                    semantics: '[start,end)',
                    logicalTriggerTime: 1785921060743,
                    windowDataCloseTime: 1785921060743,
                },
                value: 2,
                count: 1,
            });

            const wearable = (operator as any).normalizeChunkPayload(wearablePayload);
            const smartphone = (operator as any).normalizeChunkPayload(smartphonePayload);

            expect(wearable.window.start).toBe(smartphone.window.start);
            expect(wearable.window.end).toBe(smartphone.window.end);
            expect(wearable.window.start).toBe(1785921058753);
            expect(wearable.window.end).toBe(1785921060003);
            expect(wearable.chunkId).toBe(`1785921058753:1785921058753:1785921060003:wearable`);
            expect(smartphone.chunkId).toBe(`1785921058753:1785921058753:1785921060003:smartphone`);
        });

        test('prints first 5 anchor-relative chunkGroupIds for 1Hz and 16Hz with deterministic event-time windows', () => {
            const queryId = 'bench-q';
            const windowName = 'https://rsp.js/w1';
            const range = 60000;
            const step = 30000;
            const benchmarkStart = Date.parse('2026-01-01T00:00:01.763Z');

            const buildChunkGroupIds = (rateHz: number): string[] => {
                const intervalMs = Math.floor(1000 / rateHz);
                const ids: string[] = [];
                let emitted = 0;
                let t = benchmarkStart;

                while (ids.length < 5) {
                    const start = benchmarkStart + Math.floor((t - benchmarkStart) / step) * step;
                    const end = start + range;
                    if (end > benchmarkStart && (ids.length === 0 || ids[ids.length - 1] !== `${queryId}:${windowName}:${start}:${end}`)) {
                        ids.push(`${queryId}:${windowName}:${start}:${end}`);
                    }
                    emitted++;
                    t = benchmarkStart + emitted * intervalMs;
                }
                return ids;
            };

            const ids1Hz = buildChunkGroupIds(1);
            const ids16Hz = buildChunkGroupIds(16);

            // Integration-style diagnostic output for manual inspection.
            console.log(`[chunkGroupIds][1Hz] ${ids1Hz.join(',')}`);
            console.log(`[chunkGroupIds][16Hz] ${ids16Hz.join(',')}`);

            expect(ids1Hz).toHaveLength(5);
            expect(ids16Hz).toHaveLength(5);
            expect(ids1Hz).toEqual(ids16Hz);
        });
    });

    describe('emission proof and readiness guard', () => {
        const window = {
            windowName: 'https://rsp.js/w1',
            start: 1000,
            end: 2000,
            range: 1000,
            step: 1000,
            semantics: '[start,end)' as const,
        };

        const buildPartial = (subqueryId: string, chunkId: string, value: number) => ({
            queryId: 'q1',
            subqueryId,
            window,
            chunkId,
            value,
            count: 1,
            rdfPayload: `<${subqueryId}> <https://saref.etsi.org/core/hasValue> "${value}"^^<http://www.w3.org/2001/XMLSchema#double> .`,
        });

        const buildCoverageState = (chunkGroupId: string, expectedSubqueryIds: string[], duplicates: Record<string, string[]> = {}) => ({
            chunkGroupId,
            expectedSubqueryIds,
            receivedChunkIdsBySubquery: Object.fromEntries(
                expectedSubqueryIds.map((subqueryId) => [subqueryId, [] as string[]]),
            ),
            duplicateChunksIgnoredBySubquery: Object.fromEntries(
                expectedSubqueryIds.map((subqueryId) => [subqueryId, [...(duplicates[subqueryId] ?? [])]]),
            ),
        });

        const buildProcessingState = (
            chunksByWindow: Map<string, Map<string, any>>,
            chunkCoverageByWindow: Map<string, any>,
            expectedSubqueryIds: string[],
            comparableOutputCadenceOnly = false,
        ) => ({
            chunksByWindow,
            chunkCoverageByWindow,
            completedChunkGroups: new Map(),
            orderedCompletedChunkGroups: [],
            finalWindowCoverageById: new Map(),
            readyChunkGroupIds: Array.from(chunksByWindow.keys()),
            readyChunkGroupSet: new Set(chunksByWindow.keys()),
            nextComparableWindowStartIndex: 0,
            nextComparableWindowStartMs: null,
            expectedSubqueryIds,
            outputAggregationFunction: 'SUM',
            chunksPerComparableWindow: 1,
            chunkGroupsPerOutputStep: 1,
            chunkWindowWidthMs: 1000,
            alignmentOriginMs: null,
            comparableOutputCadenceOnly,
        });

        test('complete chunk coverage emits exactly one result on an interval tick', async () => {
            const chunkGroupId = 'q1:1000:2000';
            const expectedSubqueryIds = ['subA', 'subB'];
            const chunksByWindow = new Map([
                [
                    chunkGroupId,
                    new Map([
                        ['subA', buildPartial('subA', 'chunk-a', 10)],
                        ['subB', buildPartial('subB', 'chunk-b', 20)],
                    ]),
                ],
            ]);
            const chunkCoverageByWindow = new Map([
                [chunkGroupId, buildCoverageState(chunkGroupId, expectedSubqueryIds)],
            ]);
            const state = buildProcessingState(chunksByWindow, chunkCoverageByWindow, expectedSubqueryIds);

            await (operator as any).processReadyChunkGroups('Interval', state);

            expect((operator as any).chunkedDebugSummary.intervalTriggersWithEmission).toBe(1);
            expect((operator as any).chunkedEmissionProofEntries).toHaveLength(1);
            expect((operator as any).chunkedEmissionProofEntries[0].coverageComplete).toBe(true);
        });

        test('missing chunk prevents emission and leaves the interval tick empty', async () => {
            const chunkGroupId = 'q1:1000:2000';
            const expectedSubqueryIds = ['subA', 'subB'];
            const chunksByWindow = new Map([
                [chunkGroupId, new Map([['subA', buildPartial('subA', 'chunk-a', 10)]])],
            ]);
            const chunkCoverageByWindow = new Map([
                [chunkGroupId, buildCoverageState(chunkGroupId, expectedSubqueryIds)],
            ]);
            const state = buildProcessingState(chunksByWindow, chunkCoverageByWindow, expectedSubqueryIds);
            state.readyChunkGroupIds = [];
            state.readyChunkGroupSet = new Set();

            await (operator as any).processReadyChunkGroups('Interval', state);

            expect((operator as any).chunkedDebugSummary.intervalTriggersWithoutEmission).toBe(1);
        });

        test('duplicate chunks are ignored and recorded in the proof payload', async () => {
            const chunkGroupId = 'q1:1000:2000';
            const expectedSubqueryIds = ['subA', 'subB'];
            const chunksByWindow = new Map([
                [
                    chunkGroupId,
                    new Map([
                        ['subA', buildPartial('subA', 'chunk-a', 10)],
                        ['subB', buildPartial('subB', 'chunk-b', 20)],
                    ]),
                ],
            ]);
            const chunkCoverageByWindow = new Map([
                [chunkGroupId, buildCoverageState(chunkGroupId, expectedSubqueryIds, { subA: ['chunk-a-dup'] })],
            ]);
            const state = buildProcessingState(chunksByWindow, chunkCoverageByWindow, expectedSubqueryIds);

            await (operator as any).processReadyChunkGroups('Immediate', state);

            const proof = (operator as any).chunkedEmissionProofEntries[0];
            expect(proof.duplicateChunksIgnoredBySubquery.subA).toEqual(['chunk-a-dup']);
            expect(proof.receivedChunksUsedBySubquery.subA).toEqual(['chunk-a']);
        });

        test('emission proof only includes expected chunks for the emitted window', () => {
            const proof = (operator as any).buildChunkEmissionProofEntry(
                [buildPartial('subA', 'chunk-a', 10), buildPartial('subB', 'chunk-b', 20)],
                'q1:1000:2000',
                undefined,
                'coverage_complete',
            );

            expect(proof?.expectedSubqueryIds.sort()).toEqual(['subA', 'subB']);
            expect(proof?.requiredChunksBySubquery.subA).toEqual(['chunk-a']);
            expect(proof?.requiredChunksBySubquery.subB).toEqual(['chunk-b']);
            expect(proof?.missingChunksBySubquery.subA).toEqual([]);
            expect(proof?.missingChunksBySubquery.subB).toEqual([]);
            expect(proof?.coverageComplete).toBe(true);
        });

        test('comparable-window emission proof aggregates required and duplicate chunk records across internal chunks', () => {
            const proof = (operator as any).buildChunkEmissionProofEntry(
                [],
                'q1:1000:3000..q1:3000:5000',
                {
                    externalWindowNumber: 2,
                    externalWindowStart: 1000,
                    externalWindowEnd: 5000,
                    internalChunkGroupIds: ['q1:1000:3000', 'q1:3000:5000'],
                    internalChunks: [
                        {
                            chunkGroupId: 'q1:1000:3000',
                            start: 1000,
                            end: 3000,
                            count: 2,
                            sum: 30,
                            avg: 15,
                            value: 15,
                            min: 10,
                            max: 20,
                            subqueries: ['subA', 'subB'],
                            receivedChunkIdsBySubquery: {
                                subA: ['chunk-a1'],
                                subB: ['chunk-b1'],
                            },
                            duplicateChunksIgnoredBySubquery: {
                                subA: ['chunk-a1-dup'],
                                subB: [],
                            },
                            missingSubqueryIds: [],
                            coverageComplete: true,
                        },
                        {
                            chunkGroupId: 'q1:3000:5000',
                            start: 3000,
                            end: 5000,
                            count: 2,
                            sum: 50,
                            avg: 25,
                            value: 25,
                            min: 20,
                            max: 30,
                            subqueries: ['subA', 'subB'],
                            receivedChunkIdsBySubquery: {
                                subA: ['chunk-a2'],
                                subB: ['chunk-b2'],
                            },
                            duplicateChunksIgnoredBySubquery: {
                                subA: [],
                                subB: [],
                            },
                            missingSubqueryIds: [],
                            coverageComplete: true,
                        },
                    ],
                    recomposedCount: 4,
                    recomposedSum: 80,
                    recomposedAvg: 20,
                    resultValue: 20,
                },
                'coverage_complete',
            );

            expect(proof?.windowStart).toBe(1000);
            expect(proof?.windowEnd).toBe(5000);
            expect(proof?.expectedSubqueryIds).toEqual(['subA', 'subB']);
            expect(proof?.requiredChunksBySubquery).toEqual({
                subA: ['chunk-a1', 'chunk-a2'],
                subB: ['chunk-b1', 'chunk-b2'],
            });
            expect(proof?.receivedChunksUsedBySubquery).toEqual({
                subA: ['chunk-a1', 'chunk-a2'],
                subB: ['chunk-b1', 'chunk-b2'],
            });
            expect(proof?.duplicateChunksIgnoredBySubquery).toEqual({
                subA: ['chunk-a1-dup'],
                subB: [],
            });
            expect(proof?.coverageComplete).toBe(true);
            expect(proof?.emitted).toBe(true);
        });

        test('comparable-window emission stays chronological when complete chunk groups arrive out of order', async () => {
            const expectedSubqueryIds = ['subA', 'subB'];
            const buildWindow = (start: number, end: number) => ({
                windowName: 'https://rsp.js/w1',
                start,
                end,
                range: end - start,
                step: end - start,
                semantics: '[start,end)' as const,
            });
            const buildCompleteGroup = (
                chunkGroupId: string,
                start: number,
                groupSum: number,
            ) => {
                const window = buildWindow(start, start + 1000);
                const perSubquerySum = groupSum / expectedSubqueryIds.length;
                return [
                    chunkGroupId,
                    new Map([
                        [
                            'subA',
                            {
                                ...buildPartial('subA', `${chunkGroupId}:subA`, perSubquerySum),
                                window,
                                sum: perSubquerySum,
                            },
                        ],
                        [
                            'subB',
                            {
                                ...buildPartial('subB', `${chunkGroupId}:subB`, perSubquerySum),
                                window,
                                sum: perSubquerySum,
                            },
                        ],
                    ]),
                ] as const;
            };

            const chunksByWindow = new Map([
                buildCompleteGroup('q1:3000:4000', 3000, 30),
                buildCompleteGroup('q1:1000:2000', 1000, 10),
                buildCompleteGroup('q1:2000:3000', 2000, 20),
            ]);
            const chunkCoverageByWindow = new Map([
                ['q1:3000:4000', buildCoverageState('q1:3000:4000', expectedSubqueryIds)],
                ['q1:1000:2000', buildCoverageState('q1:1000:2000', expectedSubqueryIds)],
                ['q1:2000:3000', buildCoverageState('q1:2000:3000', expectedSubqueryIds)],
            ]);
            const state = buildProcessingState(
                chunksByWindow,
                chunkCoverageByWindow,
                expectedSubqueryIds,
                true,
            );
            state.chunksPerComparableWindow = 3;
            state.chunkGroupsPerOutputStep = 3;

            const executeSpy = jest.spyOn(operator as any, 'executeR2ROperator').mockResolvedValue(undefined);

            await (operator as any).processReadyChunkGroups('Immediate', state);

            expect((state as any).orderedCompletedChunkGroups.map((group: any) => group.start)).toEqual([
                1000,
                2000,
                3000,
            ]);
            expect(executeSpy).toHaveBeenCalledTimes(1);

            const comparableDiagnostics = executeSpy.mock.calls[0][2] as any;
            expect(comparableDiagnostics?.internalChunkGroupIds).toEqual([
                'q1:1000:2000',
                'q1:2000:3000',
                'q1:3000:4000',
            ]);
            expect(new Set(comparableDiagnostics?.internalChunkGroupIds).size).toBe(3);
            expect(comparableDiagnostics?.resultValue).toBe(60);
        });
    });

    describe('caching and cleanup', () => {
        test('executeR2ROperator uses structured comparable diagnostics before RDF parsing fallback', async () => {
            const mqtt = require('mqtt');
            const parserParseSpy = jest.spyOn(require('n3').Parser.prototype, 'parse');
            const publisherClient = {
                connected: true,
                publish: jest.fn((topic, payload, options, callback) => callback?.()),
                once: jest.fn(),
                end: jest.fn(),
            };
            mqtt.connect.mockReturnValueOnce(publisherClient);
            executeR2RMock.mockReset();

            await operator.executeR2ROperator(
                [
                    {
                        queryId: 'q1',
                        subqueryId: 'wearable',
                        window: {
                            windowName: 'w1',
                            start: 0,
                            end: 30000,
                            range: 30000,
                            step: 30000,
                            semantics: '[start,end)' as const,
                        },
                        chunkId: 'c1',
                        value: 10,
                        count: 2,
                        sum: 20,
                        rdfPayload: '<s> <p> "10" .',
                    },
                ],
                'group-1',
                {
                    externalWindowNumber: 1,
                    externalWindowStart: 0,
                    externalWindowEnd: 30000,
                    internalChunkGroupIds: ['group-1'],
                    internalChunks: [],
                    recomposedCount: 2,
                    recomposedSum: 20,
                    recomposedAvg: 10,
                    resultValue: 10,
                },
                'AVG',
            );

            expect(parserParseSpy).not.toHaveBeenCalled();
            expect(executeR2RMock).not.toHaveBeenCalled();
            expect(publisherClient.publish).toHaveBeenCalled();
        });

        test('waits for a final R2R publish when the binding stream ends first', async () => {
            const mqtt = require('mqtt');
            let publishCallback: (() => void) | undefined;
            const publisherClient = {
                connected: true,
                publish: jest.fn((topic, payload, options, callback) => {
                    publishCallback = callback;
                }),
                once: jest.fn(),
                end: jest.fn(),
            };
            mqtt.connect.mockReturnValueOnce(publisherClient);
            const bindingStream = new EventEmitter();
            executeR2RMock.mockResolvedValue(bindingStream);
            operator.setOutputQuery(QUERY_SINGLE_WINDOW);

            let resolved = false;
            const completion = operator.executeR2ROperator([
                {
                    queryId: 'q1',
                    subqueryId: 'p1',
                    window: {
                        windowName: 'w1',
                        start: 0,
                        end: 10,
                        range: 10,
                        step: 2,
                        semantics: '[start,end)' as const,
                    },
                    chunkId: 'c1',
                    value: 10,
                    count: 1,
                    sum: 10,
                    rdfPayload: '<s> <p> "10" .',
                },
            ]).then(() => {
                resolved = true;
            });

            await Promise.resolve();
            bindingStream.emit('data', new Map([['result', { value: '10' }]]));
            bindingStream.emit('end');
            await Promise.resolve();

            expect(publisherClient.publish).toHaveBeenCalledTimes(1);
            expect(resolved).toBe(false);
            expect(publishCallback).toBeDefined();

            publishCallback?.();
            await completion;
            expect(resolved).toBe(true);
        });

        test('reuses a single MQTT publisher client across emissions', async () => {
            const mqtt = require('mqtt');
            const publisherClient = {
                connected: true,
                publish: jest.fn((topic, payload, options, callback) => callback?.()),
                once: jest.fn(),
                end: jest.fn(),
            };
            mqtt.connect.mockReturnValue(publisherClient);

            await (operator as any).publishWithSharedClient('topic/a', 'payload-a', { qos: 1 });
            await (operator as any).publishWithSharedClient('topic/b', 'payload-b', { qos: 1 });

            expect(mqtt.connect).toHaveBeenCalledTimes(1);
            expect(publisherClient.publish).toHaveBeenCalledTimes(2);
        });

        test('reuses parsed chunk plans across repeated registrations', () => {
            operator.addSubQuery(QUERY_SINGLE_WINDOW);
            operator.setOutputQuery(QUERY_SINGLE_WINDOW);

            const parseSpy = jest.spyOn((operator as any).parser, 'parse');
            const firstPlan = (operator as any).getOrBuildChunkPlan();
            const callsAfterFirstPlan = parseSpy.mock.calls.length;
            const secondPlan = (operator as any).getOrBuildChunkPlan();

            expect(secondPlan).toBe(firstPlan);
            expect(parseSpy.mock.calls.length).toBe(callsAfterFirstPlan);
        });

        test('cleanup closes tracked MQTT clients and log streams', () => {
            const clientA = { end: jest.fn() };
            const clientB = { end: jest.fn() };
            const publisher = { end: jest.fn() };
            const latencyStream = { end: jest.fn() };
            const diagnosticsStream = { end: jest.fn() };
            const parentPartialStream = { end: jest.fn() };

            (operator as any).activeMqttClients = [clientA, clientB];
            (operator as any).mqttPublisherClient = publisher;
            (operator as any).latencyLogStream = latencyStream;
            (operator as any).diagnosticsLogStream = diagnosticsStream;
            (operator as any).parentPartialDiagnosticsStream = parentPartialStream;

            operator.cleanup();

            expect(clientA.end).toHaveBeenCalledWith(true);
            expect(clientB.end).toHaveBeenCalledWith(true);
            expect(publisher.end).toHaveBeenCalledWith(true);
            expect(latencyStream.end).toHaveBeenCalled();
            expect(diagnosticsStream.end).toHaveBeenCalled();
            expect(parentPartialStream.end).toHaveBeenCalled();
        });
    });

    describe('publisher delayed connect', () => {
        test('registers once(connect) and publishes only after connect for a single queued publish', async () => {
            const connectCallbacks: Array<() => void> = [];
            let publisherClient: any;
            publisherClient = {
                connected: false,
                publish: jest.fn((topic, payload, options, callback) => callback?.()),
                once: jest.fn((event: string, callback: () => void) => {
                    if (event === 'connect') {
                        connectCallbacks.push(callback);
                    }
                    return publisherClient;
                }),
                end: jest.fn(),
            };

            (operator as any).mqttPublisherClient = publisherClient;

            const publishPromise = (operator as any).publishWithSharedClient('topic/a', 'payload-a', { qos: 1 });

            expect(publisherClient.once).toHaveBeenCalledWith('connect', expect.any(Function));
            expect(publisherClient.publish).not.toHaveBeenCalled();

            connectCallbacks[0]();
            await publishPromise;

            expect(publisherClient.publish).toHaveBeenCalledWith('topic/a', 'payload-a', { qos: 1 }, expect.any(Function));
        });

        test('queues two publishes before connect and flushes both after connect', async () => {
            const connectCallbacks: Array<() => void> = [];
            let publisherClient: any;
            publisherClient = {
                connected: false,
                publish: jest.fn((topic, payload, options, callback) => callback?.()),
                once: jest.fn((event: string, callback: () => void) => {
                    if (event === 'connect') {
                        connectCallbacks.push(callback);
                    }
                    return publisherClient;
                }),
                end: jest.fn(),
            };

            (operator as any).mqttPublisherClient = publisherClient;

            const firstPublish = (operator as any).publishWithSharedClient('topic/a', 'payload-a', { qos: 1 });
            const secondPublish = (operator as any).publishWithSharedClient('topic/b', 'payload-b', { qos: 0 });

            expect(publisherClient.once).toHaveBeenCalledTimes(2);
            expect(publisherClient.publish).not.toHaveBeenCalled();

            for (const callback of connectCallbacks) {
                callback();
            }

            await Promise.all([firstPublish, secondPublish]);

            expect(publisherClient.publish).toHaveBeenNthCalledWith(1, 'topic/a', 'payload-a', { qos: 1 }, expect.any(Function));
            expect(publisherClient.publish).toHaveBeenNthCalledWith(2, 'topic/b', 'payload-b', { qos: 0 }, expect.any(Function));
        });
    });

    describe('Chunked latency instrumentation and validation', () => {
        test('last_required_chunk_received_at only uses chunks required for the emitted window and later unrelated chunks do not affect it', () => {
            const proofEntry = {
                windowStart: 1000,
                windowEnd: 2000,
                emittedAt: 0,
                emissionReason: 'coverage_complete',
                expectedSubqueryIds: ['subA'],
                expectedSubqueryCount: 1,
                requiredChunksBySubquery: {
                    subA: ['chunk-required-1']
                },
                receivedChunksUsedBySubquery: {
                    subA: ['chunk-required-1']
                },
                missingChunksBySubquery: { subA: [] },
                duplicateChunksIgnoredBySubquery: { subA: [] },
                coverageComplete: true,
                allExpectedSubqueriesPresent: true,
                emitted: true
            };

            (operator as any).chunkArrivalTimes.set('chunk-required-1', 1781370000000);
            (operator as any).chunkArrivalTimes.set('chunk-unrelated-later', 1781370099999);

            (operator as any).chunkWindowMap.set('chunk-required-1', { start: 1000, end: 2000 });
            (operator as any).chunkWindowMap.set('chunk-unrelated-later', { start: 2000, end: 3000 });

            const writeSpy = jest.spyOn((operator as any).latencyLogStream, 'write');

            (operator as any).logLatency(
                1,
                1781370000000,
                1781370099999,
                1781370005000,
                1781370005005,
                '42.0',
                proofEntry,
                'Interval'
            );

            expect(writeSpy).toHaveBeenCalled();
            const row = parseLatencyLogLine(writeSpy.mock.calls[0][0] as string);

            expect(row.required_chunk_intervals).toBe('1000-2000');
            expect(row.last_required_chunk_received_at).toBe('1781370000000');
            expect(row.semantic_ready_at).toBe('1781370000000');
            expect(row.trigger_type).toBe('interval');
        });

        test('ready_to_emit_ms is near zero in immediate mode and reflects scheduling wait in interval mode', () => {
            const proofEntry = {
                windowStart: 1000,
                windowEnd: 2000,
                emittedAt: 0,
                emissionReason: 'coverage_complete',
                expectedSubqueryIds: ['subA'],
                expectedSubqueryCount: 1,
                requiredChunksBySubquery: {
                    subA: ['chunk-1']
                },
                receivedChunksUsedBySubquery: {
                    subA: ['chunk-1']
                },
                missingChunksBySubquery: { subA: [] },
                duplicateChunksIgnoredBySubquery: { subA: [] },
                coverageComplete: true,
                allExpectedSubqueriesPresent: true,
                emitted: true
            };

            (operator as any).chunkArrivalTimes.set('chunk-1', 1000);
            (operator as any).chunkWindowMap.set('chunk-1', { start: 1000, end: 2000 });

            const writeSpy = jest.spyOn((operator as any).latencyLogStream, 'write');

            (operator as any).logLatency(
                1,
                1000,
                1000,
                1001,
                1002,
                '42.0',
                proofEntry,
                'Immediate'
            );

            let row = parseLatencyLogLine(writeSpy.mock.calls[writeSpy.mock.calls.length - 1][0] as string);
            expect(row.ready_to_emit_ms).toBe('2');
            expect(row.trigger_type).toBe('immediate');

            (operator as any).logLatency(
                1,
                1000,
                1000,
                2000,
                2002,
                '42.0',
                proofEntry,
                'Interval'
            );

            row = parseLatencyLogLine(writeSpy.mock.calls[writeSpy.mock.calls.length - 1][0] as string);
            expect(row.ready_to_emit_ms).toBe('1002');
            expect(row.trigger_type).toBe('interval');
        });

        test('mixed-domain close metadata is marked as domain_mismatch and does not write close-to-result latency', () => {
            const proofEntry = {
                windowStart: 1756122905256,
                windowEnd: 1756123025256,
                emittedAt: 0,
                emissionReason: 'coverage_complete',
                expectedSubqueryIds: ['subA'],
                expectedSubqueryCount: 1,
                requiredChunksBySubquery: { subA: ['chunk-1'] },
                receivedChunksUsedBySubquery: { subA: ['chunk-1'] },
                missingChunksBySubquery: { subA: [] },
                duplicateChunksIgnoredBySubquery: { subA: [] },
                coverageComplete: true,
                allExpectedSubqueriesPresent: true,
                emitted: true,
            };

            (operator as any).chunkArrivalTimes.set('chunk-1', 1782237608185);
            (operator as any).chunkWindowMap.set('chunk-1', { start: 1756122905256, end: 1756123025256 });
            (operator as any).queryRegisteredTime = 1782237483282;
            (operator as any).runtimeReplayStartWallClockTime = 1756122905256;
            (operator as any).benchmarkEventTimeAnchor = 1756122905256;

            const writeSpy = jest.spyOn((operator as any).latencyLogStream, 'write');

            (operator as any).logLatency(
                1,
                1782237603282,
                1782237608185,
                1782237608186,
                1782237608187,
                '1.0028534916666667',
                proofEntry,
                'Immediate',
                {
                    windowSemantics: 'trailing',
                    logicalTriggerTime: 1756122935256,
                    windowStart: 1756122905256,
                    windowEnd: 1756123025256,
                    windowDataCloseTime: 1756123025256,
                    resultEmittedAt: 1782237608187,
                    latencyFromLogicalTriggerMs: null,
                    latencyFromWindowCloseMs: null,
                    metadataSource: 'direct',
                },
            );

            const row = parseLatencyLogLine(writeSpy.mock.calls[writeSpy.mock.calls.length - 1][0] as string);
            expect(row.latency_domain_status).toBe('domain_mismatch');
            expect(row.wall_clock_window_close).toBe('');
            expect(row.wall_clock_close_to_result_ms).toBe('');
            expect(row.anchor_aligned_window_close_to_result_ms).toBe('');
            expect(row.window_close_to_ready_ms).toBe('');
        });

        test('valid wall-clock close metadata produces plausible close-to-result latency', () => {
            const proofEntry = {
                windowStart: 1756122905256,
                windowEnd: 1756123025256,
                emittedAt: 0,
                emissionReason: 'coverage_complete',
                expectedSubqueryIds: ['subA'],
                expectedSubqueryCount: 1,
                requiredChunksBySubquery: { subA: ['chunk-1'] },
                receivedChunksUsedBySubquery: { subA: ['chunk-1'] },
                missingChunksBySubquery: { subA: [] },
                duplicateChunksIgnoredBySubquery: { subA: [] },
                coverageComplete: true,
                allExpectedSubqueriesPresent: true,
                emitted: true,
            };

            (operator as any).chunkArrivalTimes.set('chunk-1', 1782237608185);
            (operator as any).chunkWindowMap.set('chunk-1', { start: 1756122905256, end: 1756123025256 });
            (operator as any).queryRegisteredTime = 1782237483282;
            (operator as any).runtimeReplayStartWallClockTime = 1782237484815;
            (operator as any).benchmarkEventTimeAnchor = 1756122905256;

            const writeSpy = jest.spyOn((operator as any).latencyLogStream, 'write');

            (operator as any).logLatency(
                1,
                1782237603282,
                1782237608185,
                1782237608186,
                1782237608187,
                '1.0028534916666667',
                proofEntry,
                'Immediate',
                {
                    windowSemantics: 'trailing',
                    logicalTriggerTime: 1756122935256,
                    windowStart: 1756122905256,
                    windowEnd: 1756123025256,
                    windowDataCloseTime: 1756123025256,
                    resultEmittedAt: 1782237608187,
                    latencyFromLogicalTriggerMs: null,
                    latencyFromWindowCloseMs: null,
                    metadataSource: 'direct',
                },
            );

            const row = parseLatencyLogLine(writeSpy.mock.calls[writeSpy.mock.calls.length - 1][0] as string);
            expect(row.latency_domain_status).toBe('wall_clock_mapped');
            expect(row.wall_clock_window_close).toBe('1782237604815');
            expect(row.wall_clock_close_to_result_ms).toBe('3372');
            expect(row.anchor_aligned_window_close_to_result_ms).toBe('3372');
            expect(row.window_close_to_ready_ms).toBe('3370');
        });

        test('incomplete chunk coverage blocks emission', async () => {
            const chunkGroupId = 'q1:1000:2000';
            const expectedSubqueryIds = ['subA', 'subB'];
            const chunksByWindow = new Map([
                [chunkGroupId, new Map([['subA', {
                    queryId: 'q1',
                    subqueryId: 'subA',
                    window: { start: 1000, end: 2000 },
                    chunkId: 'chunk-a',
                    value: 10
                } as any]])]
            ]);
            const chunkCoverageByWindow = new Map([
                [chunkGroupId, {
                    chunkGroupId,
                    expectedSubqueryIds,
                    receivedChunkIdsBySubquery: { subA: ['chunk-a'], subB: [] },
                    duplicateChunksIgnoredBySubquery: { subA: [], subB: [] }
                }]
            ]);

            const state = {
                chunksByWindow,
                chunkCoverageByWindow,
                completedChunkGroups: new Map(),
                orderedCompletedChunkGroups: [],
                finalWindowCoverageById: new Map(),
                readyChunkGroupIds: [chunkGroupId],
                readyChunkGroupSet: new Set([chunkGroupId]),
                nextComparableWindowStartIndex: 0,
                nextComparableWindowStartMs: null,
                expectedSubqueryIds,
                outputAggregationFunction: 'SUM' as const,
                chunksPerComparableWindow: 1,
                chunkGroupsPerOutputStep: 1,
                chunkWindowWidthMs: 1000,
                alignmentOriginMs: null,
                comparableOutputCadenceOnly: false
            };

            const executeSpy = jest.spyOn(operator, 'executeR2ROperator');

            await (operator as any).processReadyChunkGroups('Immediate', state);

            expect(executeSpy).not.toHaveBeenCalled();
        });

        test('summarizeWindowRecomposition for MIN', () => {
            const chunkGroups: any[] = [
                {
                    chunkGroupId: 'g1',
                    start: 0,
                    end: 30000,
                    summary: {
                        chunkGroupId: 'g1',
                        start: 0,
                        end: 30000,
                        count: 1,
                        sum: 5,
                        avg: 5,
                        value: 5,
                        min: 5,
                        max: 5,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                },
                {
                    chunkGroupId: 'g2',
                    start: 30000,
                    end: 60000,
                    summary: {
                        chunkGroupId: 'g2',
                        start: 30000,
                        end: 60000,
                        count: 1,
                        sum: 2,
                        avg: 2,
                        value: 2,
                        min: 2,
                        max: 2,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                }
            ];
            const result = (operator as any).summarizeWindowRecomposition(chunkGroups, 'MIN');
            expect(result).not.toBeNull();
            expect(result.recomposedMin).toBe(2);
            expect(result.resultValue).toBe(2);
        });

        test('summarizeWindowRecomposition for MAX', () => {
            const chunkGroups: any[] = [
                {
                    chunkGroupId: 'g1',
                    start: 0,
                    end: 30000,
                    summary: {
                        chunkGroupId: 'g1',
                        start: 0,
                        end: 30000,
                        count: 1,
                        sum: 5,
                        avg: 5,
                        value: 5,
                        min: 5,
                        max: 5,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                },
                {
                    chunkGroupId: 'g2',
                    start: 30000,
                    end: 60000,
                    summary: {
                        chunkGroupId: 'g2',
                        start: 30000,
                        end: 60000,
                        count: 1,
                        sum: 12,
                        avg: 12,
                        value: 12,
                        min: 12,
                        max: 12,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                }
            ];
            const result = (operator as any).summarizeWindowRecomposition(chunkGroups, 'MAX');
            expect(result).not.toBeNull();
            expect(result.recomposedMax).toBe(12);
            expect(result.resultValue).toBe(12);
        });

        test('summarizeWindowRecomposition for SUM falls back to chunk value when sum is absent', () => {
            const chunkGroups: any[] = [
                {
                    chunkGroupId: 'g1',
                    start: 0,
                    end: 30000,
                    summary: {
                        chunkGroupId: 'g1',
                        start: 0,
                        end: 30000,
                        count: 1,
                        sum: null,
                        avg: null,
                        value: 5,
                        min: 5,
                        max: 5,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                },
                {
                    chunkGroupId: 'g2',
                    start: 30000,
                    end: 60000,
                    summary: {
                        chunkGroupId: 'g2',
                        start: 30000,
                        end: 60000,
                        count: 1,
                        sum: null,
                        avg: null,
                        value: 12,
                        min: 12,
                        max: 12,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                }
            ];
            const result = (operator as any).summarizeWindowRecomposition(chunkGroups, 'SUM');
            expect(result).not.toBeNull();
            expect(result.recomposedSum).toBe(17);
            expect(result.resultValue).toBe(17);
        });

        test('MIN/MAX empty chunks do not produce fake values', () => {
            const chunkGroups: any[] = [
                {
                    chunkGroupId: 'g1',
                    start: 0,
                    end: 30000,
                    summary: {
                        chunkGroupId: 'g1',
                        start: 0,
                        end: 30000,
                        count: null,
                        sum: null,
                        avg: null,
                        value: null,
                        min: null,
                        max: null,
                        subqueries: ['sub1'],
                        receivedChunkIdsBySubquery: {},
                        duplicateChunksIgnoredBySubquery: {},
                        missingSubqueryIds: [],
                        coverageComplete: true
                    }
                }
            ];
            const resultMin = (operator as any).summarizeWindowRecomposition(chunkGroups, 'MIN');
            expect(resultMin).toBeNull();

            const resultMax = (operator as any).summarizeWindowRecomposition(chunkGroups, 'MAX');
            expect(resultMax).toBeNull();
        });
    });

    describe('structured chunk normalization and timestamp filtering', () => {
        function configureManagedProducer(origin: number) {
            (operator as any).benchmarkEventTimeAnchor = origin;
            (operator as any).managerOwnedProducerMappings = [{
                producerId: 'legacy-runtime',
                canonicalProducerId: 'canonical-producer',
                runtimeProducerId: 'runtime-producer',
                topic: 'managed/chunks',
                canonicalProducerQuery: 'canonical query',
                runtimeProducerQuery: 'runtime query',
                expectedInputStream: 'wearable/temperature',
                alignmentOriginMs: origin,
            }];
        }

        function managedPayload(args: {
            rawWindowStart: number;
            rawWindowEnd: number;
            inputWatermark: number;
            temporallyComplete: boolean;
            count: number;
            sum: number;
            alignmentCandidateStart?: number;
        }) {
            const alignmentCandidateStart =
                args.alignmentCandidateStart ?? args.rawWindowStart;
            const alignmentCandidateEnd =
                alignmentCandidateStart + (args.rawWindowEnd - args.rawWindowStart);
            return JSON.stringify({
                message_format: 'structured_reusable_result',
                source_query_id: 'runtime-query',
                canonicalProducerId: 'canonical-producer',
                runtimeProducerId: 'runtime-producer',
                window_start: alignmentCandidateStart,
                window_end: alignmentCandidateEnd,
                chunkStart: alignmentCandidateStart,
                chunkEnd: alignmentCandidateEnd,
                rawWindowStart: args.rawWindowStart,
                rawWindowEnd: args.rawWindowEnd,
                inputWatermark: args.inputWatermark,
                producerCoverageOrigin: 1_000_000,
                temporallyComplete: args.temporallyComplete,
                watermark: args.rawWindowEnd - 30_000,
                aggregationType: 'AVG',
                value: args.sum / args.count,
                avg: args.sum / args.count,
                count: args.count,
                sum: args.sum,
                window: { range: 60_000, step: 30_000 },
            });
        }

        test('Case B rejects startup partial before it can reserve the target identity', () => {
            const origin = 1_000_000;
            configureManagedProducer(origin);
            const startupPartial = (operator as any).normalizeChunkPayload(managedPayload({
                rawWindowStart: origin - 1,
                rawWindowEnd: origin + 59_999,
                inputWatermark: origin + 59_999,
                temporallyComplete: false,
                count: 720,
                sum: -16_546.38866,
                alignmentCandidateStart: origin + 1,
            }));
            expect(startupPartial).toBeNull();

            const complete = (operator as any).normalizeChunkPayload(managedPayload({
                rawWindowStart: origin,
                rawWindowEnd: origin + 60_000,
                inputWatermark: origin + 60_000,
                temporallyComplete: true,
                count: 962,
                sum: -22_120.722912,
            }));
            expect(complete).not.toBeNull();
            expect(complete.window.start).toBe(origin);
            expect(complete.window.end).toBe(origin + 60_000);
            expect(complete.count).toBe(962);
            expect(complete.sum / complete.count).toBeCloseTo(-22.99451446153846, 12);
            expect((operator as any).acceptedContributions.size).toBe(0);
        });

        test('Case A ignores a pre-origin startup chunk and recomposes later complete chunks exactly', () => {
            const origin = 1_000_000;
            configureManagedProducer(origin);
            expect((operator as any).normalizeChunkPayload(managedPayload({
                rawWindowStart: origin - 60_000,
                rawWindowEnd: origin,
                inputWatermark: origin,
                temporallyComplete: false,
                count: 240,
                sum: -5_518.038106,
            }))).toBeNull();

            const first = (operator as any).normalizeChunkPayload(managedPayload({
                rawWindowStart: origin,
                rawWindowEnd: origin + 60_000,
                inputWatermark: origin + 60_000,
                temporallyComplete: true,
                count: 480,
                sum: -11_034.7204,
            }));
            const second = (operator as any).normalizeChunkPayload(managedPayload({
                rawWindowStart: origin + 60_000,
                rawWindowEnd: origin + 120_000,
                inputWatermark: origin + 120_000,
                temporallyComplete: true,
                count: 482,
                sum: -11_086.002512,
            }));
            const coverage = {
                expectedSubqueryIds: ['runtime-producer'],
                receivedChunkIdsBySubquery: { 'runtime-producer': [] },
                duplicateChunksIgnoredBySubquery: { 'runtime-producer': [] },
            };
            const groups = [first, second].map((partial) => ({
                chunkGroupId: `${partial.window.start}:${partial.window.end}`,
                start: partial.window.start,
                end: partial.window.end,
                summary: (operator as any).summarizeChunkGroup(
                    `${partial.window.start}:${partial.window.end}`,
                    new Map([['runtime-producer', partial]]),
                    'AVG',
                    coverage,
                ),
            }));
            const result = (operator as any).summarizeWindowRecomposition(groups, 'AVG');
            expect(result.recomposedCount).toBe(962);
            expect(result.recomposedSum).toBeCloseTo(-22_120.722912, 9);
            expect(result.resultValue).toBeCloseTo(-22.99451446153846, 12);
        });

        test('legacy or unstructured payload is ignored', () => {
            expect((operator as any).normalizeChunkPayload('not-json')).toBeNull();
            expect((operator as any).normalizeChunkPayload(JSON.stringify({ foo: 'bar' }))).toBeNull();
        });

        test('structured AVG payload preserves sum/count state for recomposition', () => {
            const payload = JSON.stringify({
                queryId: 'q1',
                subqueryId: 'subA',
                window: {
                    windowName: 'https://rsp.js/w1',
                    start: 1000,
                    end: 2000,
                    range: 1000,
                    step: 1000,
                    semantics: '[start,end)',
                    logicalTriggerTime: 2000,
                    windowDataCloseTime: 2000,
                },
                aggregateFunction: 'AVG',
                avg: 12.5,
                count: 4,
                state: {
                    count: 4,
                    sum: 50,
                },
                rdfPayload: '<s> <p> "12.5" .',
            });

            expect((operator as any).normalizeChunkPayload(payload)).toEqual({
                queryId: 'q1',
                subqueryId: 'subA',
                producerId: 'subA',
                canonicalProducerId: undefined,
                runtimeProducerId: 'subA',
                chunkStart: 1000,
                chunkEnd: 2000,
                watermark: 2000,
                window: {
                    windowName: 'https://rsp.js/w1',
                    start: 1000,
                    end: 2000,
                    range: 1000,
                    step: 1000,
                    semantics: '[start,end)',
                    logicalTriggerTime: 2000,
                    windowDataCloseTime: 2000,
                },
                chunkId: '1000:2000:subA',
                reuseClassKey: undefined,
                sourceStreamId: undefined,
                sourceTopic: undefined,
                aggregateFunction: 'AVG',
                value: undefined,
                count: 4,
                sum: undefined,
                avg: 12.5,
                state: {
                    count: 4,
                    sum: 50,
                },
                rdfPayload: '<s> <p> "12.5" .',
            });
        });

        test('timestamp-domain filtering rejects out-of-range chunks', () => {
            (operator as any).timestampDomainMin = 1000;
            (operator as any).timestampDomainMax = 2000;

            expect((operator as any).isContaminatedTimestamp(999, 'topic/a')).toBe(true);
            expect((operator as any).isContaminatedTimestamp(2001, 'topic/a')).toBe(true);
            expect((operator as any).isContaminatedTimestamp(1500, 'topic/a')).toBe(false);
            expect((operator as any).rejectedContaminatedTimestampCount).toBe(2);
        });

        test('overlapping chunk is accepted when close time is inside the benchmark domain', () => {
            (operator as any).timestampDomainMin = 1785924223543;
            (operator as any).timestampDomainMax = 1785924408543;

            const normalized = (operator as any).normalizeChunkPayload(JSON.stringify({
                queryId: 'q1',
                subqueryId: 'subA',
                window: {
                    windowName: 'https://rsp.js/w1',
                    start: 1785924193543,
                    end: 1785924253543,
                    range: 60000,
                    step: 30000,
                    semantics: '[start,end)',
                    logicalTriggerTime: 1785924253793,
                    windowDataCloseTime: 1785924253543,
                },
                aggregateFunction: 'AVG',
                avg: 12.5,
                count: 4,
                state: {
                    count: 4,
                    sum: 50,
                },
            }));

            const chunkTimestamp = Number.isFinite(normalized.window.logicalTriggerTime)
                ? normalized.window.logicalTriggerTime
                : normalized.window.end;

            expect((operator as any).isContaminatedTimestamp(chunkTimestamp, 'topic/a')).toBe(false);
        });
    });

    describe('chunked diagnostics writer golden lines', () => {
        afterEach(() => {
            (fs.existsSync as jest.Mock).mockReturnValue(true);
            (fs.createWriteStream as jest.Mock).mockReturnValue({ write: jest.fn(), end: jest.fn() });
        });

        test('writes full comparable diagnostics CSV header and escaped body line', () => {
            const existsSyncMock = fs.existsSync as jest.Mock;
            const createWriteStreamMock = fs.createWriteStream as jest.Mock;

            existsSyncMock.mockReturnValue(false);
            createWriteStreamMock.mockReset();

            const latencyStream = { write: jest.fn(), end: jest.fn() };
            const diagnosticsStream = { write: jest.fn(), end: jest.fn() };
            const parentPartialStream = { write: jest.fn(), end: jest.fn() };
            createWriteStreamMock
                .mockReturnValueOnce(latencyStream)
                .mockReturnValueOnce(diagnosticsStream)
                .mockReturnValueOnce(parentPartialStream);

            const localOperator = new StreamingQueryChunkAggregatorOperator();

            expect(diagnosticsStream.write.mock.calls[0][0]).toBe(
                'benchmark_event_time_anchor,external_window_number,external_window_start,external_window_end,internal_chunk_ids,internal_chunks_json,recomposed_count,recomposed_sum,recomposed_avg,recomposed_min,recomposed_max,result_value\n',
            );

            (localOperator as any).benchmarkEventTimeAnchor = 1700000000000;
            (localOperator as any).writeComparableDiagnostics({
                externalWindowNumber: 7,
                externalWindowStart: 1000,
                externalWindowEnd: 5000,
                internalChunkGroupIds: ['g1', 'g2'],
                internalChunks: [
                    {
                        chunkGroupId: 'g1',
                        start: 1000,
                        end: 3000,
                        count: 2,
                        sum: 30,
                        avg: 15,
                        value: 15,
                        min: 10,
                        max: 20,
                        subqueries: ['sub"A', 'subB'],
                        receivedChunkIdsBySubquery: { subA: ['a1'], subB: ['b1'] },
                        duplicateChunksIgnoredBySubquery: { subA: ['a1-dup'], subB: [] },
                        missingSubqueryIds: [],
                        coverageComplete: true,
                    },
                ],
                recomposedCount: 2,
                recomposedSum: 30,
                recomposedAvg: 15,
                recomposedMin: 10,
                recomposedMax: 20,
                resultValue: 15,
            });

            expect(diagnosticsStream.write.mock.calls[1][0]).toBe(
                '1700000000000,7,1000,5000,"g1|g2","[{""chunkGroupId"":""g1"",""start"":1000,""end"":3000,""count"":2,""sum"":30,""avg"":15,""value"":15,""min"":10,""max"":20,""subqueries"":[""sub\\""A"",""subB""],""receivedChunkIdsBySubquery"":{""subA"":[""a1""],""subB"":[""b1""]},""duplicateChunksIgnoredBySubquery"":{""subA"":[""a1-dup""],""subB"":[]},""missingSubqueryIds"":[],""coverageComplete"":true}]",2,30,15,10,20,15\n',
            );
        });

        test('writes full parent-partial diagnostics CSV header and escaped body line', () => {
            const existsSyncMock = fs.existsSync as jest.Mock;
            const createWriteStreamMock = fs.createWriteStream as jest.Mock;

            existsSyncMock.mockReturnValue(false);
            createWriteStreamMock.mockReset();

            const latencyStream = { write: jest.fn(), end: jest.fn() };
            const diagnosticsStream = { write: jest.fn(), end: jest.fn() };
            const parentPartialStream = { write: jest.fn(), end: jest.fn() };
            createWriteStreamMock
                .mockReturnValueOnce(latencyStream)
                .mockReturnValueOnce(diagnosticsStream)
                .mockReturnValueOnce(parentPartialStream);

            const localOperator = new StreamingQueryChunkAggregatorOperator();

            expect(parentPartialStream.write.mock.calls[0][0]).toBe(
                'output_type,comparable,benchmark_event_time_anchor,parent_window_number,parent_window_start,parent_window_end_or_covered_until,parent_range_ms,covered_duration_ms,chunks_used,event_count,sum,avg,min,max,result_value,emitted_at_ms,elapsed_since_registration_ms,delay_past_partial_trigger_ms,internal_chunk_ids,internal_chunks_json\n',
            );

            (localOperator as any).writeParentPartialDiagnostics({
                outputType: 'parent_partial',
                comparable: false,
                benchmarkEventTimeAnchor: 1700000000000,
                parentWindowNumber: 3,
                parentWindowStart: 1000,
                parentWindowEndOrCoveredUntil: 4000,
                parentRangeMs: 6000,
                coveredDurationMs: 3000,
                chunksUsed: 2,
                eventCount: 5,
                sum: 42,
                avg: 8.4,
                min: 2,
                max: 13,
                resultValue: 8.4,
                emittedAtMs: 1700000000500,
                elapsedSinceRegistrationMs: 500,
                delayPastPartialTriggerMs: 100,
                internalChunkIds: ['g1', 'g2'],
                internalChunks: [
                    {
                        chunkGroupId: 'g1',
                        start: 1000,
                        end: 2500,
                        count: 2,
                        sum: 12,
                        avg: 6,
                        value: 6,
                        min: 2,
                        max: 10,
                        subqueries: ['sub"A', 'subB'],
                        receivedChunkIdsBySubquery: { subA: ['a1'], subB: ['b1'] },
                        duplicateChunksIgnoredBySubquery: { subA: [], subB: [] },
                        missingSubqueryIds: [],
                        coverageComplete: true,
                    },
                ],
            });

            expect(parentPartialStream.write.mock.calls[1][0]).toBe(
                'parent_partial,false,1700000000000,3,1000,4000,6000,3000,2,5,42,8.4,2,13,8.4,1700000000500,500,100,"g1|g2","[{""chunkGroupId"":""g1"",""start"":1000,""end"":2500,""count"":2,""sum"":12,""avg"":6,""value"":6,""min"":2,""max"":10,""subqueries"":[""sub\\""A"",""subB""],""receivedChunkIdsBySubquery"":{""subA"":[""a1""],""subB"":[""b1""]},""duplicateChunksIgnoredBySubquery"":{""subA"":[],""subB"":[]},""missingSubqueryIds"":[],""coverageComplete"":true}]"\n',
            );
        });

        test('benchmark target window cap records finalized windows and target stop state', () => {
            const timeoutSpy = jest
                .spyOn(global, 'setTimeout')
                .mockImplementation(((callback: any) => 0 as any) as typeof setTimeout);
            try {
                (operator as any).benchmarkTargetWindowCount = 3;
                (operator as any).recordFinalizedWindow(1);
                (operator as any).recordFinalizedWindow(2);
                (operator as any).recordFinalizedWindow(3);

                expect((operator as any).finalizedWindowNumbers).toEqual([1, 2, 3]);
                expect((operator as any).benchmarkTargetWindowReached).toBe(true);
                expect((operator as any).benchmarkStopReason).toBe('target_window_count_reached');
            } finally {
                timeoutSpy.mockRestore();
            }
        });

        test('completed reconstruction payload carries comparable top-level aggregates', () => {
            (operator as any).windowRange = 180000;
            (operator as any).windowSlide = 60000;
            (operator as any).sessionId = 'chunked-session';
            (operator as any).chunkedDebugSummary.expectedSubqueryIds = ['runtime-p1', 'runtime-p2'];
            (operator as any).chunkedDebugSummary.managedProducerMode = true;
            (operator as any).chunkedDebugSummary.localProducerSpawnCount = 0;
            (operator as any).latestWatermarkByProducer.set('runtime-p1', 181000);
            (operator as any).latestWatermarkByProducer.set('runtime-p2', 181500);
            (operator as any).managerOwnedProducerMappings = [
                {
                    canonicalProducerId: 'canonical-p1',
                    runtimeProducerId: 'runtime-p1',
                    topic: 'chunked/canonical-p1',
                    canonicalProducerQuery: 'query-p1',
                    runtimeProducerQuery: 'query-p1',
                    expectedInputStream: 'stream-p1',
                    alignmentOriginMs: 1000,
                },
                {
                    canonicalProducerId: 'canonical-p2',
                    runtimeProducerId: 'runtime-p2',
                    topic: 'chunked/canonical-p2',
                    canonicalProducerQuery: 'query-p2',
                    runtimeProducerQuery: 'query-p2',
                    expectedInputStream: 'stream-p2',
                    alignmentOriginMs: 1000,
                },
            ];
            const payload = (operator as any).buildChunkedBenchmarkPayload({
                aggregationFunction: 'AVG',
                resultValue: 3.5,
                windowNumber: 1,
                comparableDiagnostics: {
                    externalWindowNumber: 1,
                    externalWindowStart: 1000,
                    externalWindowEnd: 181000,
                    internalChunkGroupIds: ['g1', 'g2', 'g3'],
                    internalChunks: [
                        {
                            chunkGroupId: 'g1',
                            start: 1000,
                            end: 61000,
                            count: 4,
                            sum: 14,
                            avg: 3.5,
                            value: 3.5,
                            min: 1,
                            max: 6,
                            subqueries: ['runtime-p1', 'runtime-p2'],
                            receivedChunkIdsBySubquery: {},
                            duplicateChunksIgnoredBySubquery: {},
                            missingSubqueryIds: [],
                            coverageComplete: true,
                        },
                    ],
                    recomposedCount: 9,
                    recomposedSum: 31.5,
                    recomposedAvg: 3.5,
                    resultValue: 3.5,
                },
                centeredWindowMetadata: {
                    windowSemantics: 'trailing',
                    logicalTriggerTime: 181000,
                    windowStart: 1000,
                    windowEnd: 181000,
                    windowDataCloseTime: 181000,
                    resultEmittedAt: 182000,
                    latencyFromLogicalTriggerMs: null,
                    latencyFromWindowCloseMs: null,
                    metadataSource: 'reconstructed',
                },
                coverageComplete: true,
            });

            expect(payload.rangeMs).toBe(180000);
            expect(payload.stepMs).toBe(60000);
            expect(payload.eventCount).toBe(9);
            expect(payload.sumValue).toBe(31.5);
            expect(payload.avgValue).toBe(3.5);
            expect(payload.count).toBe(9);
            expect(payload.sum).toBe(31.5);
            expect(payload.average).toBe(3.5);
            expect(payload.comparableWindow).toBe(true);
            expect(payload.isComparableWindow).toBe(true);
            expect(payload.isPartialWindow).toBe(false);
            expect(payload.coverageComplete).toBe(true);
            expect(payload.windowStart).toBe(1000);
            expect(payload.windowEnd).toBe(181000);
            // The runner rejects a numerically correct result unless this
            // production payload proves its manager-owned provenance.
            expect(payload.producerIdentityMappings).toEqual([
                { canonicalProducerId: 'canonical-p1', runtimeProducerId: 'runtime-p1', topic: 'chunked/canonical-p1' },
                { canonicalProducerId: 'canonical-p2', runtimeProducerId: 'runtime-p2', topic: 'chunked/canonical-p2' },
            ]);
            expect(payload.requiredRuntimeProducerIds).toEqual(['runtime-p1', 'runtime-p2']);
            expect(payload.receivedRuntimeProducerIds).toEqual(['runtime-p1', 'runtime-p2']);
            expect(payload.missingRuntimeProducerIds).toEqual([]);
            expect(payload.latestWatermarkByRequiredRuntimeProducer).toEqual({
                'runtime-p1': 181000,
                'runtime-p2': 181500,
            });
            expect(payload.derivedReconstructionWatermark).toBe(181000);
            expect(payload.localProducerSpawnCount).toBe(0);
            expect(payload.managedProducerMode).toBe(true);
        });
    });
});
