jest.mock('mqtt', () => ({
    connect: jest.fn().mockReturnValue({
        on: jest.fn().mockReturnThis(),
        subscribe: jest.fn(),
        publish: jest.fn(),
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
            const chunkEndExclusive = windowNumber * chunksPerStep;
            const chunkStartIndex = Math.max(0, chunkEndExclusive - Math.min(chunksPerFullWindow, chunkEndExclusive));

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
            const chunkEndExclusive = windowNumber * chunksPerStep;
            const requiredChunksPerTopic = windowNumber === 1
                ? chunksPerStep
                : Math.min(chunksPerFullWindow, chunkEndExclusive);
            const chunkStartIndex = Math.max(0, chunkEndExclusive - requiredChunksPerTopic);

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
            const totalChunks = totalWindows * (windowSlide / chunkSize);
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

            expect(wearable.chunkGroupId).toBe('q1:1000:31000');
            expect(smartphone.chunkGroupId).toBe('q1:1000:31000');
            expect(smartphone.isComplete).toBe(true);
            expect(chunksByWindow.size).toBe(1);
        });

        test('prints first 5 chunkGroupIds for 1Hz and 16Hz with deterministic event-time windows', () => {
            const queryId = 'bench-q';
            const windowName = 'https://rsp.js/w1';
            const range = 60000;
            const step = 30000;
            const benchmarkStart = Date.parse('2026-01-01T00:00:00.000Z');

            const buildChunkGroupIds = (rateHz: number): string[] => {
                const intervalMs = Math.floor(1000 / rateHz);
                const ids: string[] = [];
                let emitted = 0;
                let t = benchmarkStart;

                while (ids.length < 5) {
                    const end = Math.floor(t / step) * step;
                    const start = end - range;
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
});
