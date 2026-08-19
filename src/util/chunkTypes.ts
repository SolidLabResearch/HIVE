export type WindowDescriptor = {
  windowName: string;
  start: number;
  end: number;
  range?: number;
  step?: number;
  alignmentOriginMs?: number | null;
  semantics: "[start,end)";
  windowSemantics?: string;
  logicalTriggerTime?: number | null;
  windowDataCloseTime?: number | null;
  resultEmittedAt?: number | null;
  latencyFromLogicalTriggerMs?: number | null;
  latencyFromWindowCloseMs?: number | null;
  metadataSource?: "direct" | "reconstructed";
};

export type PartialChunkResult = {
  queryId: string;
  subqueryId: string;
  /** Legacy runtime message identity. */
  producerId?: string;
  canonicalProducerId?: string;
  runtimeProducerId?: string;
  chunkStart?: number;
  chunkEnd?: number;
  watermark?: number;
  rawWindowStart?: number;
  rawWindowEnd?: number;
  inputWatermark?: number;
  producerCoverageOrigin?: number;
  temporallyComplete?: boolean;
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
  min?: number;
  max?: number;
  state?: {
    count?: number;
    sum?: number;
    min?: number;
    max?: number;
  };
  rdfPayload?: string;
};
