const mockMqttClient = {
    on: jest.fn().mockReturnThis(),
    subscribe: jest.fn(),
    publish: jest.fn(),
    end: jest.fn()
};

jest.mock('mqtt', () => ({
    connect: jest.fn().mockReturnValue(mockMqttClient)
}));

const mockRStreamEmitter = { on: jest.fn() };

jest.mock('rsp-js', () => ({
    RSPEngine: jest.fn().mockImplementation(() => ({
        register: jest.fn().mockReturnValue(mockRStreamEmitter),
        getStream: jest.fn()
    })),
    RSPQLParser: jest.fn().mockImplementation(() => ({
        parse: jest.fn()
    })),
    RDFStream: jest.fn()
}));

import { RSPQueryProcess } from './RSPQueryProcess';

const TEST_QUERY = `
PREFIX saref: <https://saref.etsi.org/core/>
REGISTER RStream <output> AS
SELECT (AVG(?o) AS ?avgTemp)
FROM NAMED WINDOW :w1 ON STREAM mqtt://localhost:1883/sensor1 [RANGE 60000 STEP 60000]
WHERE { WINDOW :w1 { ?s saref:hasValue ?o . } }
`;

describe('RSPQueryProcess', () => {
    let rspProcess: RSPQueryProcess;
    const originalBenchmarkAnchor = globalThis.process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;

    beforeEach(() => {
        jest.clearAllMocks();
        mockMqttClient.on.mockReturnThis();
        if (originalBenchmarkAnchor === undefined) {
            delete globalThis.process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
        } else {
            globalThis.process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR = originalBenchmarkAnchor;
        }
        rspProcess = new RSPQueryProcess(TEST_QUERY, 'output/topic');
    });

    afterAll(() => {
        if (originalBenchmarkAnchor === undefined) {
            delete globalThis.process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
        } else {
            globalThis.process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR = originalBenchmarkAnchor;
        }
    });

    describe('constructor', () => {
        test('should store query and rstream_topic', () => {
            expect(rspProcess.query).toBe(TEST_QUERY);
            expect(rspProcess.rstream_topic).toBe('output/topic');
        });

        test('should connect to the MQTT broker for result publishing', () => {
            const mqtt = require('mqtt');
            expect(mqtt.connect).toHaveBeenCalledWith('mqtt://localhost:1883');
        });
    });

    describe('generate_aggregation_event', () => {
        test('should generate a hasValue triple for avg-prefixed bindings', () => {
            const result = rspProcess.generate_aggregation_event({ avgTemp: '22.5' }, Date.now());
            expect(result).toContain('<https://saref.etsi.org/core/hasValue>');
            expect(result).toContain('"22.5"^^<http://www.w3.org/2001/XMLSchema#double>');
        });

        test('should generate a hasCount triple for count-prefixed bindings', () => {
            const result = rspProcess.generate_aggregation_event({ countReadings: '10' }, Date.now());
            expect(result).toContain('<https://saref.etsi.org/core/hasCount>');
            expect(result).toContain('"10"^^<http://www.w3.org/2001/XMLSchema#integer>');
        });

        test('should generate triples for multiple bindings', () => {
            const result = rspProcess.generate_aggregation_event(
                { avgTemp: '22.5', countReadings: '5' },
                Date.now()
            );
            expect(result).toContain('<https://saref.etsi.org/core/hasValue>');
            expect(result).toContain('<https://saref.etsi.org/core/hasCount>');
        });

        test('should skip bindings with unrecognised key prefixes', () => {
            const result = rspProcess.generate_aggregation_event({ unknownKey: '99' }, Date.now());
            expect(result).toBe('');
        });

        test('should return empty string for empty bindings', () => {
            const result = rspProcess.generate_aggregation_event({}, Date.now());
            expect(result).toBe('');
        });

        test('should include the aggregation event IRI in the triple', () => {
            const result = rspProcess.generate_aggregation_event({ avgX: '1.0' }, Date.now());
            expect(result).toMatch(/^<https:\/\/rsp\.js\/aggregation_event\//);
        });

        test('should keep AVG payload values typed as double when the subquery also emits a count', () => {
            const result = rspProcess.generate_aggregation_event(
                { countTemp: '481', aggTemp: '1.0329299812889814' },
                Date.now(),
            );
            expect(result).toContain('<https://saref.etsi.org/core/hasCount> "481"^^<http://www.w3.org/2001/XMLSchema#integer>');
            expect(result).toContain('<https://saref.etsi.org/core/hasValue> "1.0329299812889814"^^<http://www.w3.org/2001/XMLSchema#double>');
        });
    });

    describe('generate_partial_chunk_result', () => {
        test('should extract window bounds from rsp-js timestamp_from/timestamp_to fields', () => {
            const parsedQuery = {
                s2r: [
                    {
                        window_name: 'mqtt://localhost:1883/sensor1',
                        stream_name: 'mqtt://localhost:1883/sensor1',
                        width: 30000,
                        slide: 30000,
                    },
                ],
            };
            rspProcess.rspql_parser.parse = jest.fn().mockReturnValue(parsedQuery);

            const partial = rspProcess['generate_partial_chunk_result'](
                { avgTemp: '22.5', countTemp: '4' },
                {
                    bindings: { entries: {} },
                    timestamp_from: 1779449067922,
                    timestamp_to: 1779449097922,
                },
            );

            expect(partial).not.toBeNull();
            expect(partial?.window.start).toBe(1779449067922);
            expect(partial?.window.end).toBe(1779449097922);
            expect(partial?.value).toBe(22.5);
            expect(partial?.count).toBe(4);
            expect(partial?.chunkId).toContain('1779449067922:1779449097922');
        });

        test('should preserve explicit rsp-js window bounds when a benchmark anchor is configured', () => {
            globalThis.process.env.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR = '1782765001763';
            const anchoredProcess = new RSPQueryProcess(TEST_QUERY, 'output/topic');
            const parsedQuery = {
                s2r: [
                    {
                        window_name: 'mqtt://localhost:1883/sensor1',
                        stream_name: 'mqtt://localhost:1883/sensor1',
                        width: 30000,
                        slide: 15000,
                    },
                ],
            };
            anchoredProcess.rspql_parser.parse = jest.fn().mockReturnValue(parsedQuery);

            const partial = anchoredProcess['generate_partial_chunk_result'](
                { avgTemp: '22.5', countTemp: '4' },
                {
                    bindings: { entries: {} },
                    timestamp_from: 1782765091763,
                    timestamp_to: 1782765121763,
                },
            );

            expect(partial).not.toBeNull();
            expect(partial?.window.start).toBe(1782765091763);
            expect(partial?.window.end).toBe(1782765121763);
            expect(partial?.window.windowDataCloseTime).toBe(1782765121763);
        });
    });

    describe('cleanup', () => {
        test('should close tracked MQTT clients', () => {
            (rspProcess as any).mqttClients = [];
            const cleanupClient = { end: jest.fn() };
            (rspProcess as any).mqttClients.push(cleanupClient);

            rspProcess.cleanup();

            expect(cleanupClient.end).toHaveBeenCalledWith(true);
        });
    });
});
