import crypto from "crypto";
import path from "path";
import { spawn } from "child_process";
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
  /** Observational metadata; never read by containment or reuse decisions. */
  diagnostic?: ContainmentDiagnostic;
}

export type ContainmentDiagnostic = {
  containedQueryInputHash: string;
  containingQueryInputHash: string;
  checkerInvocationMode: "package-checker-plus-direct-specs" | "not-invoked";
  checkerVersion: string;
  executablePath?: string;
  workingDirectory?: string;
  timeoutMs?: number;
  timeout: boolean;
  diagnosticReplayTimeout?: boolean;
  subprocessExitCode?: number | null;
  subprocessSignal?: NodeJS.Signals | null;
  checkerResult?: boolean;
  stdoutExcerpt?: string;
  stdoutBytes?: number;
  stderrExcerpt?: string;
  stderrBytes?: number;
  errorType?: string;
  errorMessage?: string;
  errorCode?: string | number;
  diagnosticError?: string;
};

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

type CheckerRunResult = {
  contained: boolean;
  diagnostic: Omit<ContainmentDiagnostic, "containedQueryInputHash" | "containingQueryInputHash">;
};

const SPECS_TIMEOUT_MS = 30_000;
const DIAGNOSTIC_EXCERPT_MAX_BYTES = 4_096;

function nowMs(): number {
  return Date.now();
}

function hashValue(value: string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function excerpt(value: string): string {
  return Buffer.from(value).subarray(0, DIAGNOSTIC_EXCERPT_MAX_BYTES).toString();
}

function errorDiagnostic(error: unknown): Pick<
  ContainmentDiagnostic,
  "errorType" | "errorMessage" | "errorCode"
> {
  if (error instanceof Error) {
    const code = (error as NodeJS.ErrnoException).code;
    return {
      errorType: error.name,
      errorMessage: error.message,
      ...(typeof code === "string" || typeof code === "number" ? { errorCode: code } : {}),
    };
  }
  return { errorType: typeof error, errorMessage: String(error) };
}

function stripLineComments(query: string): string {
  return query
    .split("\n")
    .filter((line) => !line.trimStart().startsWith("#"))
    .join("\n");
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
  return canonicalizeVariables(
    expandPrefixes(canonicalizeRegisterTarget(stripLineComments(query))),
  );
}

function inferFailureKind(error: unknown): ContainmentFailureReason {
  const message = error instanceof Error ? error.message : String(error);
  if (/unsupported_query/i.test(message) || /fail-closed/i.test(message)) {
    return "UNSUPPORTED_QUERY";
  }
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
    const diagnosticBase = this.buildDiagnosticBase(containedQuery, containingQuery);
    profileCount("semantic_containment_checks");
    const cacheKey = this.buildCacheKey(containedQuery, containingQuery);
    const cached = this.readCache(cacheKey);
    if (cached) {
      profileCount("semantic_containment_cache_hits");
      return {
        ...cached,
        diagnostic: cached.diagnostic
          ? { ...cached.diagnostic, ...diagnosticBase }
          : diagnosticBase,
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
            diagnostic: { ...diagnosticBase, checkerInvocationMode: "not-invoked" },
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
            diagnostic: { ...diagnosticBase, checkerInvocationMode: "not-invoked" },
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
          contained: result.contained,
          supported: true,
          direction: "subquery_in_superquery",
          checkerVersion: CHECKER_VERSION,
          diagnostic: { ...diagnosticBase, ...result.diagnostic },
        },
        startedAt,
        DEFAULT_SUCCESS_TTL_MS,
      );
    } catch (error) {
      const failureKind = inferFailureKind(error);
      const ttlMs =
        failureKind === "TIMEOUT" ? DEFAULT_TIMEOUT_TTL_MS : DEFAULT_UNSUPPORTED_TTL_MS;
      const diagnostic = await this.captureFailureDiagnostic(
        containedQuery,
        containingQuery,
        error,
      );
      return this.finishAndCache(
        cacheKey,
        {
          contained: false,
          supported: false,
          reason: error instanceof Error ? error.message : String(error),
          direction: "subquery_in_superquery",
          failureKind,
          checkerVersion: CHECKER_VERSION,
          diagnostic: { ...diagnosticBase, ...diagnostic },
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
    const forward = await this.checkContainment(queryA, queryB);
    const reverse = await this.checkContainment(queryB, queryA);
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
    const unsupported = this.detectUnsupportedSyntax(sanitizedQuery);
    if (unsupported) {
      throw new Error(`UNSUPPORTED_QUERY: ${unsupported}`);
    }
    const parsed = this.createParser().parse(sanitizedQuery);
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
      aggregationFunction: detectAggregationFromSelect(query),
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

  private async runChecker(subQuery: string, superQuery: string): Promise<CheckerRunResult> {
    const deduppedSub = deduplicateSelectExpressions(subQuery);
    const deduppedSuper = deduplicateSelectExpressions(superQuery);
    const checker = this.createChecker();
    const parser = this.createParser();
    const specsWrapper = this.createSPeCSWrapper();
    // The package-level checker is the public API. We still invoke the wrapper directly
    // to preserve timeout and ambiguous-result classification.
    const checkerResult = await checker.checkContainment(deduppedSub, deduppedSuper);
    const parsedSub = parser.parse(deduppedSub);
    const parsedSuper = parser.parse(deduppedSuper);
    const specsResult = await specsWrapper.runSPeCS({
      subquery: parsedSub.sparql,
      superquery: parsedSuper.sparql,
      qc: "true",
      rename: "true",
    });
    if (specsResult.containment === null) {
      throw new Error("SPeCS returned an ambiguous containment result");
    }
    return {
      contained: specsResult.containment,
      diagnostic: {
        checkerInvocationMode: "package-checker-plus-direct-specs",
        checkerVersion: CHECKER_VERSION,
        executablePath: this.getSPeCSExecutablePath(),
        workingDirectory: process.cwd(),
        timeoutMs: SPECS_TIMEOUT_MS,
        timeout: false,
        checkerResult,
        subprocessExitCode: specsResult.exitCode,
        subprocessSignal: null,
        stdoutBytes: Buffer.byteLength(specsResult.stdout),
        stdoutExcerpt: excerpt(specsResult.stdout),
        stderrBytes: Buffer.byteLength(specsResult.stderr),
        stderrExcerpt: excerpt(specsResult.stderr),
      },
    };
  }

  private buildDiagnosticBase(
    containedQuery: string,
    containingQuery: string,
  ): Pick<ContainmentDiagnostic, "containedQueryInputHash" | "containingQueryInputHash" | "checkerVersion" | "checkerInvocationMode" | "timeout"> {
    return {
      containedQueryInputHash: this.getNormalizedInputHash(containedQuery),
      containingQueryInputHash: this.getNormalizedInputHash(containingQuery),
      checkerVersion: CHECKER_VERSION,
      checkerInvocationMode: "not-invoked",
      timeout: false,
    };
  }

  private getSPeCSExecutablePath(): string {
    return path.resolve(
      path.dirname(require.resolve("rspql-containment-checker")),
      "../specs/src/specs",
    );
  }

  /**
   * The package wrapper discards process metadata on rejection. A failed
   * directional check therefore gets one diagnostic-only replay with the
   * wrapper's same executable, arguments, cwd, and timeout. Its result is
   * never used by containment or reuse.
   */
  private async captureFailureDiagnostic(
    containedQuery: string,
    containingQuery: string,
    originalError: unknown,
  ): Promise<Omit<ContainmentDiagnostic, "containedQueryInputHash" | "containingQueryInputHash">> {
    const base = {
      checkerInvocationMode: "package-checker-plus-direct-specs" as const,
      checkerVersion: CHECKER_VERSION,
      executablePath: this.getSPeCSExecutablePath(),
      workingDirectory: process.cwd(),
      timeoutMs: SPECS_TIMEOUT_MS,
      timeout: false,
      ...errorDiagnostic(originalError),
    };
    try {
      const subQuery = this.parseForChecker(containedQuery).sanitizedQuery;
      const superQuery = this.parseForChecker(containingQuery).sanitizedQuery;
      const parser = this.createParser();
      const result = await this.runSPeCSDiagnostic(
        parser.parse(deduplicateSelectExpressions(subQuery)).sparql,
        parser.parse(deduplicateSelectExpressions(superQuery)).sparql,
      );
      return {
        ...base,
        ...result,
        timeout: /timeout/i.test(base.errorMessage ?? "") || result.timeout,
        diagnosticReplayTimeout: result.timeout,
      };
    } catch (diagnosticError) {
      return {
        ...base,
        diagnosticError: errorDiagnostic(diagnosticError).errorMessage,
      };
    }
  }

  private async runSPeCSDiagnostic(
    subquery: string,
    superquery: string,
  ): Promise<Pick<ContainmentDiagnostic, "timeout" | "subprocessExitCode" | "subprocessSignal" | "stdoutBytes" | "stdoutExcerpt" | "stderrBytes" | "stderrExcerpt" | "errorType" | "errorMessage" | "errorCode">> {
    return new Promise((resolve) => {
      const child = spawn(this.getSPeCSExecutablePath(), [
        "-superquery", superquery,
        "-subquery", subquery,
        "-rename",
        "-qc",
      ], { stdio: ["pipe", "pipe", "pipe"] });
      let stdout = "";
      let stderr = "";
      let timedOut = false;
      let spawnError: unknown;
      const timer = setTimeout(() => {
        timedOut = true;
        child.kill("SIGKILL");
      }, SPECS_TIMEOUT_MS);
      child.stdout?.on("data", (data: Buffer) => { stdout += data.toString(); });
      child.stderr?.on("data", (data: Buffer) => { stderr += data.toString(); });
      child.on("error", (error) => { spawnError = error; });
      child.on("close", (code, signal) => {
        clearTimeout(timer);
        resolve({
          timeout: timedOut,
          subprocessExitCode: code,
          subprocessSignal: signal,
          stdoutBytes: Buffer.byteLength(stdout),
          stdoutExcerpt: excerpt(stdout),
          stderrBytes: Buffer.byteLength(stderr),
          stderrExcerpt: excerpt(stderr),
          ...(spawnError ? errorDiagnostic(spawnError) : {}),
        });
      });
    });
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

  private createParser(): RSPQLParser {
    return new DeduplicatingRSPQLParser();
  }

  private createChecker(): ContainmentChecker {
    return new (ContainmentChecker as unknown as {
      new (qc?: boolean, rename?: boolean): ContainmentChecker;
    })(true, true);
  }

  private createSPeCSWrapper(): SPeCSWrapper {
    return new SPeCSWrapper();
  }
}

class DeduplicatingRSPQLParser extends RSPQLParser {
  parse(query: string) {
    const deduppedQuery = deduplicateSelectExpressions(query);
    return super.parse(deduppedQuery);
  }
}

type ProjectionExpression = {
  raw: string;
  variable: string | null;
  alias: string | null;
  aggregation: string | null;
};

function parseSelectClause(selectClauseStr: string): ProjectionExpression[] {
  const expressions: ProjectionExpression[] = [];
  let remaining = selectClauseStr.trim();
  while (remaining.length > 0) {
    if (remaining.startsWith("(")) {
      let depth = 0;
      let endIdx = -1;
      for (let i = 0; i < remaining.length; i++) {
        if (remaining[i] === "(") depth++;
        else if (remaining[i] === ")") {
          depth--;
          if (depth === 0) {
            endIdx = i;
            break;
          }
        }
      }
      if (endIdx === -1) {
        throw new Error("Malformed SELECT clause: unbalanced parentheses");
      }
      const rawExpr = remaining.substring(0, endIdx + 1);
      remaining = remaining.substring(endIdx + 1).trim();
      
      const aggMatch = rawExpr.match(/^\(\s*(AVG|COUNT|SUM|MIN|MAX)\s*\(\s*(\?[A-Za-z_][\w-]*)\s*\)\s+AS\s+(\?[A-Za-z_][\w-]*)\s*\)$/i);
      if (aggMatch) {
        expressions.push({
          raw: rawExpr,
          aggregation: aggMatch[1].toUpperCase(),
          variable: aggMatch[2],
          alias: aggMatch[3],
        });
        continue;
      }
      
      const aliasMatch = rawExpr.match(/^\(\s*(\?[A-Za-z_][\w-]*)\s+AS\s+(\?[A-Za-z_][\w-]*)\s*\)$/i);
      if (aliasMatch) {
        expressions.push({
          raw: rawExpr,
          aggregation: null,
          variable: aliasMatch[1],
          alias: aliasMatch[2],
        });
        continue;
      }
      
      throw new Error(`Unsupported SELECT projection expression syntax: ${rawExpr}`);
    } else if (remaining.startsWith("?")) {
      const varMatch = remaining.match(/^(\?[A-Za-z_][\w-]*)/);
      if (!varMatch) {
        throw new Error("Malformed variable in SELECT clause");
      }
      const rawVar = varMatch[1];
      remaining = remaining.substring(rawVar.length).trim();
      expressions.push({
        raw: rawVar,
        aggregation: null,
        variable: rawVar,
        alias: null,
      });
    } else {
      throw new Error(`Unexpected token in SELECT clause: ${remaining}`);
    }
  }
  return expressions;
}

function deduplicateSelectExpressions(query: string): string {
  const selectMatch = query.match(/SELECT\s+([\s\S]+?)(?=\bFROM\b|\bWHERE\b)/i);
  if (!selectMatch) {
    return query;
  }
  const selectBody = selectMatch[1];
  const expressions = parseSelectClause(selectBody);
  
  const seenBareVars = new Set<string>();
  const seenAliases = new Map<string, string>(); // alias -> raw expression (normalized)
  const uniqueExpressions: ProjectionExpression[] = [];
  
  for (const expr of expressions) {
    if (expr.alias) {
      const alias = expr.alias.toLowerCase();
      const normExpr = expr.raw.replace(/\s+/g, "").toLowerCase();
      if (seenAliases.has(alias)) {
        const existing = seenAliases.get(alias)!;
        if (existing !== normExpr) {
          throw new Error(
            `Fail-closed: Duplicate alias '${expr.alias}' used for different projection expressions: '${existing}' vs '${normExpr}'`
          );
        }
        // Identical aliased expressions are safe to deduplicate.
      } else {
        seenAliases.set(alias, normExpr);
        uniqueExpressions.push(expr);
      }
    } else if (expr.variable) {
      // Bare variable
      const variable = expr.variable.toLowerCase();
      if (!seenBareVars.has(variable)) {
        seenBareVars.add(variable);
        uniqueExpressions.push(expr);
      }
    } else {
      uniqueExpressions.push(expr);
    }
  }
  
  const newSelect = `SELECT ${uniqueExpressions.map((e) => e.raw).join(" ")}`;
  return query.replace(/SELECT\s+[\s\S]+?(?=\bFROM\b|\bWHERE\b)/i, (matched) => {
    const suffix = matched.match(/\s*$/)?.[0] ?? "";
    return newSelect + suffix;
  });
}

export const rspqlContainmentTestHooks = {
  deduplicateSelectExpressions,
};

function detectAggregationFromSelect(query: string): string {
  const selectMatch = query.match(/SELECT\s+([\s\S]+?)(?=\bFROM\b|\bWHERE\b)/i);
  if (!selectMatch) {
    return "AVG";
  }
  const selectBody = selectMatch[1];
  const expressions = parseSelectClause(selectBody);
  
  const primaryExpr = expressions.find(
    (e) =>
      e.alias &&
      (e.alias.toLowerCase().includes("result") ||
        e.alias.toLowerCase().startsWith("?agg"))
  );
  if (primaryExpr && primaryExpr.aggregation) {
    return primaryExpr.aggregation;
  }
  
  const aggregates = expressions
    .map((e) => e.aggregation)
    .filter((agg): agg is string => agg !== null && agg !== "MIN" && agg !== "MAX");
    
  if (aggregates.length === 0) {
    return "AVG";
  }
  
  const uniqueMain = Array.from(new Set(aggregates));
  if (uniqueMain.length > 1) {
    throw new Error(`Ambiguous aggregation functions: multiple different aggregates found: ${uniqueMain.join(", ")}`);
  }
  return uniqueMain[0];
}
