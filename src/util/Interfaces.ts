export interface IStreamQueryOperator {
    addSubQuery(query: string) : void;
    addOutputQuery(query: string): void;
    getSubQueries(): string[];
    init(): Promise<void>;
    handleAggregation(): Promise<void>;
    cleanup?(): void | Promise<void>;
}