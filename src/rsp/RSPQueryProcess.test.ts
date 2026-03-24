const mockMqttClient = {
    on: jest.fn().mockReturnThis(),
    subscribe: jest.fn(),
    publish: jest.fn()
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
    let process: RSPQueryProcess;

    beforeEach(() => {
        jest.clearAllMocks();
        mockMqttClient.on.mockReturnThis();
        process = new RSPQueryProcess(TEST_QUERY, 'output/topic');
    });

    describe('constructor', () => {
        test('should store query and rstream_topic', () => {
            expect(process.query).toBe(TEST_QUERY);
            expect(process.rstream_topic).toBe('output/topic');
        });

        test('should connect to the MQTT broker for result publishing', () => {
            const mqtt = require('mqtt');
            expect(mqtt.connect).toHaveBeenCalledWith('mqtt://localhost:1883');
        });
    });

    describe('generate_aggregation_event', () => {
        test('should generate a hasValue triple for avg-prefixed bindings', () => {
            const result = process.generate_aggregation_event({ avgTemp: '22.5' }, Date.now());
            expect(result).toContain('<https://saref.etsi.org/core/hasValue>');
            expect(result).toContain('"22.5"^^<http://www.w3.org/2001/XMLSchema#float>');
        });

        test('should generate a hasCount triple for count-prefixed bindings', () => {
            const result = process.generate_aggregation_event({ countReadings: '10' }, Date.now());
            expect(result).toContain('<https://saref.etsi.org/core/hasCount>');
            expect(result).toContain('"10"^^<http://www.w3.org/2001/XMLSchema#float>');
        });

        test('should generate triples for multiple bindings', () => {
            const result = process.generate_aggregation_event(
                { avgTemp: '22.5', countReadings: '5' },
                Date.now()
            );
            expect(result).toContain('<https://saref.etsi.org/core/hasValue>');
            expect(result).toContain('<https://saref.etsi.org/core/hasCount>');
        });

        test('should skip bindings with unrecognised key prefixes', () => {
            const result = process.generate_aggregation_event({ unknownKey: '99' }, Date.now());
            expect(result).toBe('');
        });

        test('should return empty string for empty bindings', () => {
            const result = process.generate_aggregation_event({}, Date.now());
            expect(result).toBe('');
        });

        test('should include the aggregation event IRI in the triple', () => {
            const result = process.generate_aggregation_event({ avgX: '1.0' }, Date.now());
            expect(result).toMatch(/^<https:\/\/rsp\.js\/aggregation_event\//);
        });
    });
});
