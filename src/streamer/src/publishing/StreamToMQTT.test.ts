jest.mock('mqtt', () => ({
    connect: jest.fn(() => ({
        publish: jest.fn(),
    })),
}));

const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, literal, quad } = DataFactory;

import { StreamToMQTT } from './StreamToMQTT';

describe('StreamToMQTT deterministic replay timestamps', () => {
    const timestampPredicate = namedNode('https://saref.etsi.org/core/hasTimestamp');
    const measurementPredicate = namedNode('https://saref.etsi.org/core/measurementMadeBy');

    const extractTimestamp = (payload: string): string => {
        const match = payload.match(/hasTimestamp> "([^"]+)"/);
        expect(match).not.toBeNull();
        return match![1];
    };

    const createPublisher = () => {
        const publisher = Object.create(StreamToMQTT.prototype) as any;
        publisher.store = new N3.Store([
            quad(
                namedNode('https://example.com/obs/1'),
                measurementPredicate,
                namedNode('https://example.com/sensor'),
            ),
            quad(
                namedNode('https://example.com/obs/1'),
                timestampPredicate,
                literal('2020-01-01T00:00:00.000Z'),
            ),
            quad(
                namedNode('https://example.com/obs/2'),
                measurementPredicate,
                namedNode('https://example.com/sensor'),
            ),
            quad(
                namedNode('https://example.com/obs/2'),
                timestampPredicate,
                literal('2020-01-01T00:01:00.000Z'),
            ),
        ]);
        publisher.sorted_observation_subjects = [
            'https://example.com/obs/1',
            'https://example.com/obs/2',
        ];
        publisher.observation_pointer = 0;
        publisher.number_of_publish = 0;
        publisher.sort_subject_length = 2;
        publisher.topic_to_publish = 'wearableX';
        publisher.file_location = 'fixture.nt';
        publisher.successfulPublishes = 0;
        publisher.failedPublishes = 0;
        publisher.deterministicEventTime = true;
        publisher.benchmarkStartTime = Date.parse('2026-01-01T00:00:00.000Z');
        publisher.datasetStartTime = null;
        publisher.datasetDuration = 0;
        publisher.replayLoopIndex = 0;
        publisher.originalObservationTimestamps = new Map();
        publisher.originalObservationOffsets = new Map();
        publisher.debugChunksEnabled = false;

        const publishedPayloads: string[] = [];
        publisher.mqtt_client = {
            publish: jest.fn((_topic: string, data: string, _options: unknown, callback: (err?: Error) => void) => {
                publishedPayloads.push(data);
                callback();
            }),
        };

        return { publisher, publishedPayloads };
    };

    const originalEnv = { ...process.env };

    beforeEach(() => {
        jest.resetModules();
        process.env.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME = '1';
        process.env.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME = String(Date.parse('2026-01-01T00:00:00.000Z'));
    });

    afterEach(() => {
        process.env = { ...originalEnv };
    });

    test('advances repeated loop timestamps by datasetDuration without mutating original timestamps', async () => {
        const { publisher, publishedPayloads } = createPublisher();

        publisher.captureOriginalObservationTiming();

        expect(publisher.datasetDuration).toBe(60_000);
        expect(publisher.originalObservationTimestamps.get('https://example.com/obs/1')).toBe('2020-01-01T00:00:00.000Z');
        expect(publisher.originalObservationTimestamps.get('https://example.com/obs/2')).toBe('2020-01-01T00:01:00.000Z');
        expect(publisher.originalObservationOffsets.get('https://example.com/obs/1')).toBe(0);
        expect(publisher.originalObservationOffsets.get('https://example.com/obs/2')).toBe(60_000);

        await publisher.publish_one_observation();
        await publisher.publish_one_observation();

        publisher.observation_pointer = 0;
        publisher.number_of_publish = 0;
        publisher.replayLoopIndex = 1;

        await publisher.publish_one_observation();
        await publisher.publish_one_observation();

        const emittedTimestamps = publishedPayloads.map(extractTimestamp);

        expect(emittedTimestamps).toEqual([
            '2026-01-01T00:00:00.000Z',
            '2026-01-01T00:01:00.000Z',
            '2026-01-01T00:01:00.000Z',
            '2026-01-01T00:02:00.000Z',
        ]);

        expect(
            Date.parse(emittedTimestamps[2]) - Date.parse(emittedTimestamps[0]),
        ).toBe(60_000);
        expect(
            Date.parse(emittedTimestamps[3]) - Date.parse(emittedTimestamps[1]),
        ).toBe(60_000);
        expect(emittedTimestamps.every((timestamp) => timestamp.startsWith('2026-01-01'))).toBe(true);
        expect(publisher.originalObservationTimestamps.get('https://example.com/obs/1')).toBe('2020-01-01T00:00:00.000Z');
        expect(publisher.originalObservationTimestamps.get('https://example.com/obs/2')).toBe('2020-01-01T00:01:00.000Z');
    });

    test('tracks cumulative replayed event time across loop resets for diagnostics', () => {
        const { publisher } = createPublisher();

        publisher.loopDurationMs = 60_000;
        publisher.replayLoopIndex = 0;
        expect(publisher.getCumulativeEventTimeSpanMs(0)).toBe(0);
        expect(publisher.getCumulativeEventTimeSpanMs(60_000)).toBe(60_000);

        publisher.replayLoopIndex = 1;
        expect(publisher.getCumulativeEventTimeSpanMs(0)).toBe(60_000);
        expect(publisher.getCumulativeEventTimeSpanMs(60_000)).toBe(120_000);

        publisher.replayLoopIndex = 3;
        expect(publisher.getCumulativeEventTimeSpanMs(15_000)).toBe(195_000);
        expect(publisher.getCumulativeEventTimeSpanMs(undefined)).toBeUndefined();
    });
});
