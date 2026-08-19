import crypto from "crypto";
import { RSPQLParser } from "hive-thought-rewriter";
import { hash_string_md5 } from "../../util/Util";
import { stripConsumerOutputTarget } from "./RSPQLContainmentService";

export interface ProducerIdentity {
  canonicalProducerId: string;
  runtimeProducerId: string;
}

export type InputStreamIdentity = {
  streamName: string;
  brokerUrl: string;
  topic: string;
};

export type SubqueryProducerStartupContract = ProducerIdentity & {
  canonicalSubqueryId: string;
  outputTopic: string;
  alignmentOriginMs: number;
  inputStreams: InputStreamIdentity[];
};

export type SubqueryRuntimeIdentity = {
  canonicalQuery: string;
  canonicalId: string;
  outputTopic: string;
  inputStreams: InputStreamIdentity[];
};

function normalizeSubquery(query: string): string {
  return stripConsumerOutputTarget(query).replace(/\s+/g, " ").trim();
}

export function createRuntimeProducerId(): string {
  return crypto.randomBytes(16).toString("hex");
}

export function buildInputStreamIdentity(streamName: string): InputStreamIdentity {
  let url: URL;
  try {
    url = new URL(streamName);
  } catch (_error) {
    throw new Error(`Invalid producer input stream identity: ${streamName}`);
  }

  if (url.protocol !== "mqtt:" && url.protocol !== "mqtts:") {
    throw new Error(
      `Unsupported producer input stream protocol ${url.protocol} for ${streamName}`,
    );
  }

  const topic = url.pathname.replace(/^\/+/, "");
  if (!url.hostname || !topic) {
    throw new Error(`Incomplete producer input stream identity: ${streamName}`);
  }

  return {
    streamName: url.toString(),
    brokerUrl: `${url.protocol}//${url.host}/`,
    topic,
  };
}

export function extractInputStreamIdentities(query: string): InputStreamIdentity[] {
  const parsedQuery = new RSPQLParser().parse(query);
  const streamNames = Array.from(parsedQuery?.s2r ?? []).map((stream: any) =>
    String(stream?.stream_name ?? ""),
  );
  if (streamNames.length === 0 || streamNames.some((streamName) => !streamName)) {
    throw new Error("Manager-owned producer query must declare an input stream");
  }

  const identities = streamNames.map(buildInputStreamIdentity);
  const uniqueByName = new Map(
    identities.map((identity) => [identity.streamName, identity]),
  );
  return Array.from(uniqueByName.values()).sort((left, right) =>
    left.streamName.localeCompare(right.streamName),
  );
}

export function validateAlignmentOriginMs(alignmentOriginMs: number): number {
  if (!Number.isFinite(alignmentOriginMs)) {
    throw new Error(
      `Producer alignmentOriginMs must be finite; received ${String(alignmentOriginMs)}`,
    );
  }
  return alignmentOriginMs;
}

export function assertInputStreamIdentitiesMatch(
  actual: InputStreamIdentity[],
  expected: InputStreamIdentity[],
): void {
  const normalize = (identities: InputStreamIdentity[]) =>
    identities
      .map((identity) =>
        `${identity.streamName}|${identity.brokerUrl}|${identity.topic}`,
      )
      .sort();
  const actualValues = normalize(actual);
  const expectedValues = normalize(expected);
  if (
    actualValues.length !== expectedValues.length ||
    actualValues.some((value, index) => value !== expectedValues[index])
  ) {
    throw new Error(
      `Producer input stream identity mismatch: expected ${JSON.stringify(
        expectedValues,
      )}, received ${JSON.stringify(actualValues)}`,
    );
  }
}

export function buildSubqueryRuntimeIdentity(
  query: string,
): SubqueryRuntimeIdentity {
  const canonicalQuery = normalizeSubquery(query);
  const canonicalId = hash_string_md5(canonicalQuery);
  return {
    canonicalQuery,
    canonicalId,
    outputTopic: `chunked/${canonicalId}`,
    inputStreams: extractInputStreamIdentities(query),
  };
}
