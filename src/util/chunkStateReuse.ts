import { RSPQLParser } from "rsp-js";
import { hash_string_md5 } from "./Util";

export type SupportedAggregationTerm = "AVG" | "COUNT" | "SUM";

export type CompatibleAvgChunkReuseSpec = {
  aggregationStateSignature: "sum,count";
  aggregationVariable: "value";
  chunkRange: number;
  chunkSlide: number;
  originalOutputTopic: string;
  originalQueryHash: string;
  originalWindowRange: number;
  originalWindowStep: number;
  projectionTerms: SupportedAggregationTerm[];
  reuseClassKey: string;
  sourceStreamId: string;
  sourceTopic: string;
};

const parser = new RSPQLParser();

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function containsUnsupportedShape(query: string): boolean {
  return /\b(FILTER|HAVING|UNION|JOIN|GROUP BY|ORDER BY|DISTINCT|OPTIONAL)\b/i.test(
    query,
  );
}

function extractProjectionTerms(query: string): SupportedAggregationTerm[] | null {
  const selectMatch = query.match(/SELECT\s+([\s\S]*?)\s+FROM\b/i);
  if (!selectMatch?.[1]) {
    return null;
  }

  const terms: SupportedAggregationTerm[] = [];
  const regex = /\((AVG|COUNT|SUM)\s*\(\s*\?value\s*\)\s+AS\s+\?[A-Za-z0-9_]+\s*\)/gi;
  let match: RegExpExecArray | null = null;
  while ((match = regex.exec(selectMatch[1])) !== null) {
    terms.push(match[1].toUpperCase() as SupportedAggregationTerm);
  }

  if (terms.length === 0) {
    return null;
  }

  return terms;
}

export function detectCompatibleAvgChunkReuse(
  query: string,
): CompatibleAvgChunkReuseSpec | null {
  if (containsUnsupportedShape(query)) {
    return null;
  }

  const projectionTerms = extractProjectionTerms(query);
  if (
    !projectionTerms ||
    projectionTerms.length !== 4 ||
    projectionTerms.join(",") !== "AVG,COUNT,SUM,AVG"
  ) {
    return null;
  }

  const parsed = parser.parse(query);
  if (!parsed?.s2r || parsed.s2r.length !== 1) {
    return null;
  }

  const window = parsed.s2r[0];
  if (
    !window ||
    !Number.isFinite(window.width) ||
    !Number.isFinite(window.slide) ||
    window.width <= 0 ||
    window.slide <= 0
  ) {
    return null;
  }

  const normalizedQuery = normalizeWhitespace(query);
  if (!/AVG\s*\(\s*\?value\s*\)/i.test(normalizedQuery)) {
    return null;
  }
  if (
    !/saref:hasValue\s+\?value\s*\./i.test(normalizedQuery) ||
    !/saref:hasTimestamp\s+\?ts\s*\./i.test(normalizedQuery)
  ) {
    return null;
  }

  const sourceStreamId = String(window.stream_name || "");
  if (!sourceStreamId.startsWith("mqtt://")) {
    return null;
  }

  const sourceTopic = new URL(sourceStreamId).pathname.replace(/^\/+/, "");
  if (!sourceTopic) {
    return null;
  }

  const graphPatternHash = hash_string_md5(
    normalizedQuery
      .replace(/\s+/g, " ")
      .replace(/REGISTER\s+RStream\s+<[^>]+>\s+AS\s*/i, "")
      .replace(/SELECT\s+[\s\S]*?\s+FROM\b/i, "FROM "),
  );

  const reuseClassKey = [
    sourceStreamId,
    graphPatternHash,
    "value",
    "sum,count",
    `${window.width}`,
    `${window.slide}`,
  ].join("::");

  return {
    aggregationStateSignature: "sum,count",
    aggregationVariable: "value",
    chunkRange: window.slide,
    chunkSlide: window.slide,
    originalOutputTopic: `chunked/${hash_string_md5(query)}`,
    originalQueryHash: hash_string_md5(query),
    originalWindowRange: window.width,
    originalWindowStep: window.slide,
    projectionTerms,
    reuseClassKey,
    sourceStreamId,
    sourceTopic,
  };
}

export function deriveAvgProjectionValues(
  projectionTerms: SupportedAggregationTerm[],
  sum: number,
  count: number,
): string[] {
  const avg = count > 0 ? sum / count : 0;
  return projectionTerms.map((term) => {
    if (term === "COUNT") {
      return `${count}`;
    }
    if (term === "SUM") {
      return `${sum}`;
    }
    return `${avg}`;
  });
}
