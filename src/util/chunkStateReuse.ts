import { RSPQLParser } from "rsp-js";
import { hash_string_md5 } from "./Util";

export type SupportedAggregationTerm = "AVG" | "COUNT" | "SUM" | "MIN" | "MAX";

export type CompatibleChunkReuseSpec = {
  aggregationFunction: "AVG" | "SUM" | "COUNT" | "MIN" | "MAX";
  aggregationStateSignature: "sum,count" | "sum" | "count" | "min" | "max";
  aggregationVariable?: string;
  outputVariable: string;
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

type ProjectionSpec = {
  aggregationFunction: SupportedAggregationTerm;
  variable: string;
  alias: string;
};

function extractProjectionSpecs(selectClause: string): ProjectionSpec[] | null {
  const selectWithoutSpaces = selectClause.replace(/\s+/g, "");
  const specs: ProjectionSpec[] = [];
  const regex = /\((AVG|SUM|COUNT|MIN|MAX)\((\?[A-Za-z0-9_]+|\*)\)AS(\?[A-Za-z0-9_]+)\)/gi;
  let match: RegExpExecArray | null = null;
  while ((match = regex.exec(selectWithoutSpaces)) !== null) {
    specs.push({
      aggregationFunction: match[1].toUpperCase() as SupportedAggregationTerm,
      variable: match[2],
      alias: match[3],
    });
  }
  return specs.length > 0 ? specs : null;
}

export function detectCompatibleChunkReuse(
  query: string,
): CompatibleChunkReuseSpec | null {
  if (containsUnsupportedShape(query)) {
    return null;
  }

  const selectMatch = query.match(/SELECT\s+([\s\S]*?)\s+FROM\b/i);
  if (!selectMatch?.[1]) {
    return null;
  }

  const selectClause = selectMatch[1];
  const selectWithoutSpaces = selectClause.replace(/\s+/g, "");
  const specs = extractProjectionSpecs(selectClause);
  if (!specs) {
    return null;
  }

  const selectCleaned = selectWithoutSpaces.replace(/\((AVG|SUM|COUNT|MIN|MAX)\((\?[A-Za-z0-9_]+|\*)\)AS(\?[A-Za-z0-9_]+)\)/gi, "");
  if (selectCleaned.length > 0) {
    return null;
  }

  let aggregationVariable: string | undefined = undefined;
  for (const spec of specs) {
    if (spec.variable !== "*") {
      const varName = spec.variable.replace(/^\?/, "");
      if (aggregationVariable === undefined) {
        aggregationVariable = varName;
      } else if (aggregationVariable !== varName) {
        return null;
      }
    }
  }

  const hasAvg = specs.some((s) => s.aggregationFunction === "AVG");
  const hasSum = specs.some((s) => s.aggregationFunction === "SUM");
  const hasCount = specs.some((s) => s.aggregationFunction === "COUNT");
  const hasMin = specs.some((s) => s.aggregationFunction === "MIN");
  const hasMax = specs.some((s) => s.aggregationFunction === "MAX");

  let aggregationFunction: "AVG" | "SUM" | "COUNT" | "MIN" | "MAX";
  let aggregationStateSignature: "sum,count" | "sum" | "count" | "min" | "max";

  if (hasAvg) {
    aggregationFunction = "AVG";
    aggregationStateSignature = "sum,count";
  } else if (hasSum) {
    aggregationFunction = "SUM";
    aggregationStateSignature = "sum";
  } else if (hasCount) {
    aggregationFunction = "COUNT";
    aggregationStateSignature = "count";
  } else if (hasMin) {
    aggregationFunction = "MIN";
    aggregationStateSignature = "min";
  } else if (hasMax) {
    aggregationFunction = "MAX";
    aggregationStateSignature = "max";
  } else {
    return null;
  }

  const primarySpec = specs.find((s) => s.aggregationFunction === aggregationFunction);
  if (!primarySpec || !primarySpec.alias) {
    return null;
  }
  const outputVariable = primarySpec.alias;

  if ((aggregationFunction === "AVG" || aggregationFunction === "SUM" || aggregationFunction === "MIN" || aggregationFunction === "MAX") && !aggregationVariable) {
    return null;
  }

  const normalizedQuery = normalizeWhitespace(query);
  if (aggregationVariable) {
    const hasValuePattern = new RegExp(`saref:hasValue\\s+\\?${aggregationVariable}\\s*\\.`, "i");
    if (!hasValuePattern.test(normalizedQuery)) {
      return null;
    }
  }

  if (!/saref:hasTimestamp\s+\?[A-Za-z0-9_]+\s*\./i.test(normalizedQuery)) {
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
    aggregationVariable || "*",
    aggregationStateSignature,
    `${window.width}`,
    `${window.slide}`,
  ].join("::");

  const projectionTerms = specs.map((s) => s.aggregationFunction);

  return {
    aggregationFunction,
    aggregationStateSignature,
    aggregationVariable,
    outputVariable,
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

export function deriveProjectionValues(
  projectionTerms: SupportedAggregationTerm[],
  sum?: number,
  count?: number,
  min?: number,
  max?: number,
): string[] {
  const safeSum = sum ?? 0;
  const safeCount = count ?? 0;
  const avg = safeCount > 0 ? safeSum / safeCount : 0;
  return projectionTerms.map((term) => {
    if (term === "COUNT") {
      return `${safeCount}`;
    }
    if (term === "SUM") {
      return `${safeSum}`;
    }
    if (term === "MIN") {
      return min !== undefined ? `${min}` : "";
    }
    if (term === "MAX") {
      return max !== undefined ? `${max}` : "";
    }
    return `${avg}`;
  });
}
