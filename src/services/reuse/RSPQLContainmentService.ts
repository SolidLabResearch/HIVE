import crypto from "crypto";
import {
  ContainmentChecker,
  RSPQLParser,
  SPeCSWrapper,
} from "rspql-containment-checker";
import { profileCount, profileTime } from "../../util/profiling";

const DEFAULT_CACHE_MAX_ENTRIES = 512;
const DEFAULT_SUCCESS_TTL_MS = 60 * 60 * 1000;
const DEFAULT_UNSUPPORTED_TTL_MS = 15 * 60 * 1000;
const DEFAULT_TIMEOUT_TTL_MS = 60 * 1000;
const DEFAULT_REGISTER_TARGET = "semantic-equivalence-target";
const CHECKER_VERSION = "rspql-containment-checker@2.7.0";
const UNSUPPORTED_SYNTAX_PATTERNS: Array<[RegExp, string]> = [
  [/\bFILTER\b/i, "FILTER not supported by production equivalence adapter"],
  [/\bBIND\b/i, "BIND not supported by production equivalence adapter"],
  [/\bVALUES\b/i, "VALUES not supported by production equivalence adapter"],
  [/\bOPTIONAL\b/i, "OPTIONAL not supported by production equivalence adapter"],
  [/\bMINUS\b/i, "MINUS not supported by production equivalence adapter"],
  [/\bSERVICE\b/i, "SERVICE not supported by production equivalence adapter"],
  [/\bTUMBLING\b/i, "Window alignment syntax not supported by checker adapter"],
];

export type ContainmentFailureReason =
  | "UNSUPPORTED_QUERY"
  | "PARSER_FAILURE"
  | "SOLVER_FAILURE"
  | "TIMEOUT"
  | "AMBIGUOUS_RESULT"
  | "UNKNOWN_EXTENSION_FUNCTION"
  | "WINDOW_SEMANTICS_UNSUPPORTED"
  | "UNEXPECTED_EXCEPTION";

export interface ContainmentResult {
  contained: boolean;
  supported: boolean;
  reason?: string;
  durationMs: number;
  cacheHit: boolean;
  direction: "subquery_in_superquery";
  failureKind?: ContainmentFailureReason;
  checkerVersion: string;
}

export interface EquivalenceResult {
  equivalent: boolean;
  supported: boolean;
  forward: ContainmentResult;
  reverse: ContainmentResult;
  durationMs: number;
}

type CachedContainmentResult = Omit<ContainmentResult, "durationMs" | "cacheHit"> & {
  expiresAt: number;
};

type ParsedCheckerQuery = {
  sanitizedQuery: string;
  streamNames: string[];
  windowWidths: number[];
  windowSlides: number[];
  aggregationFunction: string;
  r2sOperator: string;
};

function nowMs(): number {
  return Date.now();
}

function hashValue(value: string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function canonicalizeRegisterTarget(query: string): string {
  return query.replace(
    /REGISTER\s+(RSTREAM|ISTREAM|DSTREAM)\s+<[^>]+>\s+AS/i,
    `REGISTER $1 <${DEFAULT_REGISTER_TARGET}> AS`,
  );
}

function expandPrefixes(query: string): string {
  const prefixEntries = Array.from(
    query.matchAll(/^\s*PREFIX\s+([A-Za-z][\w-]*)\:\s*<([^>]+)>\s*$/gim),
  ).map((match) => ({
    label: match[1],
    iri: match[2],
  }));
  let body = query.replace(/^\s*PREFIX\s+[A-Za-z][\w-]*\:\s*<[^>]+>\s*$/gim, "").trim();
  for (const entry of prefixEntries) {
    const escapedLabel = entry.label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    body = body.replace(
      new RegExp(`\\b${escapedLabel}:([A-Za-z_][\\w-]*)`, "g"),
      `<${entry.iri}$1>`,
    );
  }
  return body;
}

function canonicalizeVariables(query: string): string {
  const variables = Array.from(new Set(query.match(/\?[A-Za-z_][\w-]*/g) ?? []));
  const replacements = new Map<string, string>();
  variables.forEach((variable, index) => {
    replacements.set(variable, `?v${index + 1}`);
  });
  let normalized = query;
  for (const [from, to] of replacements.entries()) {
    normalized = normalized.replace(new RegExp(`\\${from}(?![\\w-])`, "g"), to);
  }
  return normalized;
}

function normalizeQueryForChecker(query: string): string {
  return canonicalizeVariables(expandPrefixes(canonicalizeRegisterTarget(query)));
}

function inferFailureKind(error: unknown): ContainmentFailureReason {
  const message = error instanceof Error ? error.message : String(error);
  if (/timeout/i.test(message)) {
    return "TIMEOUT";
  }
  if (/extension/i.test(message)) {
    return "UNKNOWN_EXTENSION_FUNCTION";
  }
  if (/window/i.test(message)) {
    return "WINDOW_SEMANTICS_UNSUPPORTED";
  }
  if (/parse/i.test(message) || /sparql/i.test(message)) {
    return "PARSER_FAILURE";
  }
  if (/SPeCS/i.test(message) || /solver/i.test(message) || /z3/i.test(message)) {
    return "SOLVER_FAILURE";
  }
  return "UNEXPECTED_EXCEPTION";
}

export function stripConsumerOutputTarget(query: string): string {
  return canonicalizeRegisterTarget(query);
}

export class RSPQLContainmentService {
  private readonly parser = new RSPQLParser();
  private readonly specsWrapper = new SPeCSWrapper();
  private readonly checker = new (ContainmentChecker as unknown as {
    new (qc?: boolean, rename?: boolean): ContainmentChecker;
  })(true, true);
  private readonly cache = new Map<string, CachedContainmentResult>();
  private readonly cacheMaxEntries: number;

  constructor(cacheMaxEntries = DEFAULT_CACHE_MAX_ENTRIES) {
    this.cacheMaxEntries = cacheMaxEntries;
  }

  getCheckerVersion(): string {
    return CHECKER_VERSION;
  }

  getNormalizedInputHash(query: string): string {
    return hashValue(normalizeQueryForChecker(query).replace(/\s+/g, " ").trim());
  }

  async checkContainment(
    containedQuery: string,
    containingQuery: string,
  ): Promise<ContainmentResult> {
    const startedAt = nowMs();
    profileCount("semantic_containment_checks");
    const cacheKey = this.buildCacheKey(containedQuery, containingQuery);
    const cached = this.readCache(cacheKey);
    if (cached) {
      profileCount("semantic_containment_cache_hits");
      return {
        ...cached,
        durationMs: 0,
        cacheHit: true,
      };
    }

    try {
      const subQuery = this.parseForChecker(containedQuery);
      const superQuery = this.parseForChecker(containingQuery);

      const unsupportedReason =
        this.detectUnsupportedSyntax(subQuery.sanitizedQuery) ||
        this.detectUnsupportedSyntax(superQuery.sanitizedQuery);
      if (unsupportedReason) {
        return this.finishAndCache(
          cacheKey,
          {
            contained: false,
            supported: false,
            reason: unsupportedReason,
            direction: "subquery_in_superquery",
            failureKind: "UNSUPPORTED_QUERY",
            checkerVersion: CHECKER_VERSION,
          },
          startedAt,
          DEFAULT_UNSUPPORTED_TTL_MS,
        );
      }

      const compatibilityFailure = this.checkWindowAndStreamCompatibility(
        subQuery,
        superQuery,
      );
      if (compatibilityFailure) {
        return this.finishAndCache(
          cacheKey,
          {
            contained: false,
            supported: false,
            reason: compatibilityFailure,
            direction: "subquery_in_superquery",
            failureKind: "WINDOW_SEMANTICS_UNSUPPORTED",
            checkerVersion: CHECKER_VERSION,
          },
          startedAt,
          DEFAULT_UNSUPPORTED_TTL_MS,
        );
      }

      const result = await this.runChecker(
        subQuery.sanitizedQuery,
        superQuery.sanitizedQuery,
      );
      return this.finishAndCache(
        cacheKey,
        {
          contained: result,
          supported: true,
          direction: "subquery_in_superquery",
          checkerVersion: CHECKER_VERSION,
        },
        startedAt,
        DEFAULT_SUCCESS_TTL_MS,
      );
    } catch (error) {
      const failureKind = inferFailureKind(error);
      const ttlMs =
        failureKind === "TIMEOUT" ? DEFAULT_TIMEOUT_TTL_MS : DEFAULT_UNSUPPORTED_TTL_MS;
      return this.finishAndCache(
        cacheKey,
        {
          contained: false,
          supported: false,
          reason: error instanceof Error ? error.message : String(error),
          direction: "subquery_in_superquery",
          failureKind,
          checkerVersion: CHECKER_VERSION,
        },
        startedAt,
        ttlMs,
      );
    }
  }

  async checkEquivalence(
    queryA: string,
    queryB: string,
  ): Promise<EquivalenceResult> {
    const startedAt = nowMs();
    let parsedA: ParsedCheckerQuery;
    let parsedB: ParsedCheckerQuery;
    try {
      parsedA = this.parseForChecker(queryA);
      parsedB = this.parseForChecker(queryB);
    } catch (error) {
      const durationMs = nowMs() - startedAt;
      const failed = this.buildEquivalenceFailureResult(error, durationMs);
      return {
        equivalent: false,
        supported: false,
        forward: failed,
        reverse: failed,
        durationMs,
      };
    }
    const [forward, reverse] = await Promise.all([
      this.checkContainment(queryA, queryB),
      this.checkContainment(queryB, queryA),
    ]);
    const durationMs = nowMs() - startedAt;
    profileTime("semantic_equivalence_lookup_duration_ms", durationMs);
    const sameFinalSemantics = this.haveSameFinalSemantics(parsedA, parsedB);

    return {
      equivalent:
        sameFinalSemantics &&
        forward.supported &&
        reverse.supported &&
        forward.contained &&
        reverse.contained,
      supported: sameFinalSemantics && forward.supported && reverse.supported,
      forward,
      reverse,
      durationMs,
    };
  }

  private buildCacheKey(containedQuery: string, containingQuery: string): string {
    return [
      this.getNormalizedInputHash(containedQuery),
      this.getNormalizedInputHash(containingQuery),
      CHECKER_VERSION,
      "subquery_in_superquery",
    ].join(":");
  }

  private readCache(cacheKey: string): CachedContainmentResult | null {
    const cached = this.cache.get(cacheKey);
    if (!cached) {
      return null;
    }
    if (cached.expiresAt <= nowMs()) {
      this.cache.delete(cacheKey);
      return null;
    }
    this.cache.delete(cacheKey);
    this.cache.set(cacheKey, cached);
    return cached;
  }

  private writeCache(cacheKey: string, value: CachedContainmentResult): void {
    this.cache.set(cacheKey, value);
    while (this.cache.size > this.cacheMaxEntries) {
      const firstKey = this.cache.keys().next().value;
      if (!firstKey) {
        break;
      }
      this.cache.delete(firstKey);
    }
  }

  private finishAndCache(
    cacheKey: string,
    result: Omit<ContainmentResult, "durationMs" | "cacheHit">,
    startedAt: number,
    ttlMs: number,
  ): ContainmentResult {
    const durationMs = nowMs() - startedAt;
    if (result.supported) {
      profileTime("semantic_containment_duration_ms", durationMs);
    } else {
      profileCount("semantic_containment_fail_closed");
      if (result.failureKind === "TIMEOUT") {
        profileCount("semantic_containment_timeouts");
      } else {
        profileCount("semantic_containment_unsupported");
      }
    }
    this.writeCache(cacheKey, {
      ...result,
      expiresAt: nowMs() + ttlMs,
    });
    return {
      ...result,
      durationMs,
      cacheHit: false,
    };
  }

  private parseForChecker(query: string): ParsedCheckerQuery {
    const sanitizedQuery = normalizeQueryForChecker(query);
    const parsed = this.parser.parse(sanitizedQuery);
    if (!parsed?.sparql) {
      throw new Error("Parsed queries do not contain valid SPARQL");
    }
    return {
      sanitizedQuery,
      streamNames: Array.isArray(parsed.s2r)
        ? parsed.s2r.map((entry: { stream_name: string }) => entry.stream_name)
        : [],
      windowWidths: Array.isArray(parsed.s2r)
        ? parsed.s2r.map((entry: { width: number }) => Number(entry.width))
        : [],
      windowSlides: Array.isArray(parsed.s2r)
        ? parsed.s2r.map((entry: { slide: number }) => Number(entry.slide))
        : [],
      aggregationFunction: String(parsed.aggregation_function || "").toUpperCase(),
      r2sOperator: String(parsed.r2s?.operator || ""),
    };
  }

  private detectUnsupportedSyntax(query: string): string | null {
    for (const [pattern, reason] of UNSUPPORTED_SYNTAX_PATTERNS) {
      if (pattern.test(query)) {
        return reason;
      }
    }
    if (/[A-Za-z_][\w-]*\s*\(/.test(query) && !/\b(AVG|SUM|COUNT|MIN|MAX)\s*\(/i.test(query)) {
      return "Unknown extension function or call syntax not supported";
    }
    return null;
  }

  private checkWindowAndStreamCompatibility(
    subQuery: ParsedCheckerQuery,
    superQuery: ParsedCheckerQuery,
  ): string | null {
    if (subQuery.streamNames.length === 0 || superQuery.streamNames.length === 0) {
      return "Missing stream association";
    }
    if (!subQuery.r2sOperator || !superQuery.r2sOperator) {
      return "Missing R2S operator";
    }
    return null;
  }

  private async runChecker(subQuery: string, superQuery: string): Promise<boolean> {
    // The package-level checker is the public API. We still invoke the wrapper directly
    // to preserve timeout and ambiguous-result classification.
    await this.checker.checkContainment(subQuery, superQuery);
    const parsedSub = this.parser.parse(subQuery);
    const parsedSuper = this.parser.parse(superQuery);
    const specsResult = await this.specsWrapper.runSPeCS({
      subquery: parsedSub.sparql,
      superquery: parsedSuper.sparql,
      qc: "true",
      rename: "true",
    });
    if (specsResult.containment === null) {
      throw new Error("SPeCS returned an ambiguous containment result");
    }
    return specsResult.containment;
  }

  private haveSameFinalSemantics(
    left: ParsedCheckerQuery,
    right: ParsedCheckerQuery,
  ): boolean {
    return (
      left.r2sOperator === right.r2sOperator &&
      left.aggregationFunction === right.aggregationFunction &&
      JSON.stringify(left.streamNames) === JSON.stringify(right.streamNames) &&
      JSON.stringify(left.windowWidths) === JSON.stringify(right.windowWidths) &&
      JSON.stringify(left.windowSlides) === JSON.stringify(right.windowSlides)
    );
  }

  private buildEquivalenceFailureResult(
    error: unknown,
    durationMs: number,
  ): ContainmentResult {
    return {
      contained: false,
      supported: false,
      reason: error instanceof Error ? error.message : String(error),
      durationMs,
      cacheHit: false,
      direction: "subquery_in_superquery",
      failureKind: inferFailureKind(error),
      checkerVersion: CHECKER_VERSION,
    };
  }
}
