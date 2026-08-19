import { BeeKeeper } from './BeeKeeper';

const mockStop = jest.fn();

jest.mock('./HiveQueryBee', () => ({
    HiveQueryBee: jest.fn().mockImplementation(() => ({ stop: mockStop }))
}));

import { HiveQueryBee } from './HiveQueryBee';

describe('BeeKeeper', () => {
    let beeKeeper: BeeKeeper;

    beforeEach(() => {
        jest.clearAllMocks();
        beeKeeper = new BeeKeeper();
    });

    describe('constructor', () => {
        test('should initialize without errors', () => {
            expect(beeKeeper).toBeDefined();
        });
    });

    describe('executeQuery', () => {
        test('should create a HiveQueryBee worker with correct arguments', () => {
            beeKeeper.executeQuery('SELECT * WHERE {}', 'output-topic', 'approximation-approach');
            expect(HiveQueryBee).toHaveBeenCalledTimes(1);
            expect(HiveQueryBee).toHaveBeenCalledWith(
                'SELECT * WHERE {}',
                'output-topic',
                'approximation-approach',
                undefined,
                undefined,
                undefined,
            );
        });

        test('should pass subQueries to HiveQueryBee when provided', () => {
            const subQueries = ['SELECT ?x WHERE { ?x ?y ?z }', 'SELECT ?a WHERE { ?a ?b ?c }'];
            beeKeeper.executeQuery('SELECT * WHERE {}', 'output-topic', 'chunked-approach', subQueries);
            expect(HiveQueryBee).toHaveBeenCalledWith(
                'SELECT * WHERE {}',
                'output-topic',
                'chunked-approach',
                subQueries,
                undefined,
                undefined,
            );
        });

        test('should register multiple workers for different queries', () => {
            beeKeeper.executeQuery('SELECT ?x WHERE {}', 'topic-1', 'approximation-approach');
            beeKeeper.executeQuery('SELECT ?y WHERE {}', 'topic-2', 'fetching-client-side');
            expect(HiveQueryBee).toHaveBeenCalledTimes(2);
        });
    });

    describe('stopQuery', () => {
        test('should call stop() on the worker for a running query', () => {
            const query = 'SELECT * WHERE {}';
            beeKeeper.executeQuery(query, 'output-topic', 'approximation-approach');
            beeKeeper.stopQuery(query);
            expect(mockStop).toHaveBeenCalledTimes(1);
        });

        test('should remove the worker from registry after stopping', () => {
            const query = 'SELECT * WHERE {}';
            beeKeeper.executeQuery(query, 'output-topic', 'approximation-approach');
            beeKeeper.stopQuery(query);
            expect(() => beeKeeper.stopQuery(query)).toThrow('Worker not found for the given query');
        });

        test('should throw when stopping a query that was never started', () => {
            expect(() => beeKeeper.stopQuery('non-existent query')).toThrow(
                'Worker not found for the given query'
            );
        });
    });
});
