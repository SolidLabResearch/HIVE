import * as childProcess from 'child_process';
import { HiveQueryBee } from './HiveQueryBee';

const mockChildProcess = {
    on: jest.fn().mockReturnThis(),
    kill: jest.fn()
};

jest.mock('child_process', () => ({
    fork: jest.fn()
}));

describe('HiveQueryBee', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        (childProcess.fork as jest.Mock).mockReturnValue(mockChildProcess);
    });

    describe('constructor', () => {
        test('should fork BeeWorker.js on construction', () => {
            new HiveQueryBee('SELECT * WHERE {}', 'output-topic', 'approximation-approach');
            expect(childProcess.fork).toHaveBeenCalledTimes(1);
            const [forkedPath] = (childProcess.fork as jest.Mock).mock.calls[0];
            expect(forkedPath).toMatch(/BeeWorker\.js$/);
        });

        test('should pass QUERY env var to the forked process', () => {
            const query = 'SELECT * WHERE { ?s ?p ?o }';
            new HiveQueryBee(query, 'topic', 'approximation-approach');
            const [, , options] = (childProcess.fork as jest.Mock).mock.calls[0];
            expect(options.env.QUERY).toBe(query);
        });

        test('should pass TOPIC env var to the forked process', () => {
            new HiveQueryBee('SELECT * WHERE {}', 'my-output-topic', 'approximation-approach');
            const [, , options] = (childProcess.fork as jest.Mock).mock.calls[0];
            expect(options.env.TOPIC).toBe('my-output-topic');
        });

        test('should pass OPERATOR_TYPE env var to the forked process', () => {
            new HiveQueryBee('SELECT * WHERE {}', 'topic', 'chunked-approach');
            const [, , options] = (childProcess.fork as jest.Mock).mock.calls[0];
            expect(options.env.OPERATOR_TYPE).toBe('chunked-approach');
        });

        test('should pass SUB_QUERIES as JSON string when provided', () => {
            const subQueries = ['SELECT ?x WHERE {}', 'SELECT ?y WHERE {}'];
            new HiveQueryBee('SELECT * WHERE {}', 'topic', 'chunked-approach', subQueries);
            const [, , options] = (childProcess.fork as jest.Mock).mock.calls[0];
            expect(options.env.SUB_QUERIES).toBe(JSON.stringify(subQueries));
        });

        test('should leave SUB_QUERIES undefined when not provided', () => {
            new HiveQueryBee('SELECT * WHERE {}', 'topic', 'approximation-approach');
            const [, , options] = (childProcess.fork as jest.Mock).mock.calls[0];
            expect(options.env.SUB_QUERIES).toBeUndefined();
        });

        test('should register message and exit listeners on the child process', () => {
            new HiveQueryBee('SELECT * WHERE {}', 'topic', 'approximation-approach');
            expect(mockChildProcess.on).toHaveBeenCalledWith('message', expect.any(Function));
            expect(mockChildProcess.on).toHaveBeenCalledWith('exit', expect.any(Function));
        });
    });

    describe('stop', () => {
        test('should kill the child process', () => {
            const bee = new HiveQueryBee('SELECT * WHERE {}', 'topic', 'approximation-approach');
            bee.stop();
            expect(mockChildProcess.kill).toHaveBeenCalledTimes(1);
        });
    });
});
