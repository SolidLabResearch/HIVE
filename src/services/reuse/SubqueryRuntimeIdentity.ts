import { hash_string_md5 } from "../../util/Util";
import { stripConsumerOutputTarget } from "./RSPQLContainmentService";

export type SubqueryRuntimeIdentity = {
  canonicalQuery: string;
  canonicalId: string;
  outputTopic: string;
};

function normalizeSubquery(query: string): string {
  return stripConsumerOutputTarget(query).replace(/\s+/g, " ").trim();
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
  };
}
