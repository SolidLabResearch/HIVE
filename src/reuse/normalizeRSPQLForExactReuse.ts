import { createHash } from "crypto";

/**
 * Phase 1 exact-reuse canonicalization.
 *
 * This intentionally does not attempt full SPARQL/RSP-QL algebra isomorphism.
 * It is a conservative string-normalization pass aimed at the current
 * benchmark family so exact duplicate queries with consumer-specific wrappers
 * normalize to the same canonical form.
 */
export function normalizeRSPQLForExactReuse(query: string): string {
  const withoutComments = query
    .replace(/(^|[\t ])#.*$/gm, "$1")
    .trim();

  const prefixEntries: Array<{ label: string; iri: string }> = [];
  const bodyWithoutPrefixes = withoutComments.replace(
    /PREFIX\s+([A-Za-z][\w-]*:)\s*<([^>]+)>/gi,
    (_match, label: string, iri: string) => {
      prefixEntries.push({
        label: label.toLowerCase(),
        iri,
      });
      return " ";
    },
  );

  const bodyLines = bodyWithoutPrefixes
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  const normalizedPrefixes = prefixEntries
    .sort((a, b) =>
      a.label === b.label ? a.iri.localeCompare(b.iri) : a.label.localeCompare(b.label),
    )
    .map((entry) => `PREFIX ${entry.label} <${entry.iri}>`);

  const body = bodyLines
    .join(" ")
    .replace(/REGISTER\s+RSTREAM\s+<[^>]+>\s+AS\s+/i, "")
    .replace(/\s+/g, " ")
    .replace(/\s*([\[\]{}(),;])\s*/g, "$1")
    .replace(/\s*<\s*/g, "<")
    .replace(/\s*>\s*/g, ">")
    .trim();

  const keywordNormalized = body
    .replace(/\bselect\b/gi, "SELECT")
    .replace(/\bfrom\b/gi, "FROM")
    .replace(/\bnamed window\b/gi, "NAMED WINDOW")
    .replace(/\bon stream\b/gi, "ON STREAM")
    .replace(/\brange\b/gi, "RANGE")
    .replace(/\bstep\b/gi, "STEP")
    .replace(/\bwhere\b/gi, "WHERE")
    .replace(/\bwindow\b/gi, "WINDOW")
    .replace(/\bunion\b/gi, "UNION")
    .replace(/\bas\b/gi, "AS")
    .replace(/\bavg\b/gi, "AVG")
    .replace(/\bsum\b/gi, "SUM")
    .replace(/\bcount\b/gi, "COUNT")
    .replace(/\bmin\b/gi, "MIN")
    .replace(/\bmax\b/gi, "MAX");

  return [...normalizedPrefixes, keywordNormalized].join(" ").trim();
}

export function hashNormalizedRSPQLForExactReuse(normalizedQuery: string): string {
  return createHash("sha256").update(normalizedQuery).digest("hex");
}

export function getCanonicalRSPQLQueryHash(query: string): {
  normalizedQuery: string;
  canonicalQueryHash: string;
} {
  const normalizedQuery = normalizeRSPQLForExactReuse(query);
  return {
    normalizedQuery,
    canonicalQueryHash: hashNormalizedRSPQLForExactReuse(normalizedQuery),
  };
}
