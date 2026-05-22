export type WindowDescriptor = {
  windowName: string;
  start: number;
  end: number;
  range?: number;
  step?: number;
  semantics: "[start,end)";
};

export type PartialChunkResult = {
  queryId: string;
  subqueryId: string;
  window: WindowDescriptor;
  chunkId: string;
  aggregateFunction?: string;
  value?: number;
  count?: number;
  rdfPayload?: string;
};
