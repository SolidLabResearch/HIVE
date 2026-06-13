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
  reuseClassKey?: string;
  sourceStreamId?: string;
  sourceTopic?: string;
  aggregateFunction?: string;
  value?: number;
  count?: number;
  sum?: number;
  avg?: number;
  state?: {
    count?: number;
    sum?: number;
  };
  rdfPayload?: string;
};
