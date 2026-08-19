import crypto from "crypto";
import {
  ContainmentResult,
  EquivalenceResult,
  RSPQLContainmentService,
  stripConsumerOutputTarget,
} from "../services/reuse/RSPQLContainmentService";

export type ReuseMode =
  | "fresh_execution"
  | "final_result_reuse"
  | "chunk_state_reuse";

export type ExecutionState =
  | "reserved"
  | "starting"
  | "active"
  | "failed"
  | "stopping"
  | "stopped";

export type ReusableApproach = "approximation" | "chunked";

export interface ActiveExecutionHandle {
  executionId: string;
  approach: "fetching" | ReusableApproach;
  canonicalQueryId: string;
  sharedOutputTopic: string;
  workerIds: string[];
  /** Legacy producer IDs; currently canonical producer IDs. */
  producerIds?: string[];
  canonicalProducerIds?: string[];
  runtimeProducerIds?: string[];
  producerTopics?: string[];
  producerSnapshots?: import("../services/reuse/SubqueryProducerManager").SubqueryProducerRuntimeSnapshot[];
  state: ExecutionState;
  stop(): Promise<void>;
}

export type QueryReuseDecisionEvent = {
  consumerId: string;
  incomingQueryId: string;
  matchedActiveQueryId?: string;
  candidateQueryIdsInspected: string[];
  forwardContained: boolean;
  reverseContained: boolean;
  mutuallyContained: boolean;
  supported: boolean;
  reuseHit: boolean;
  executionId: string;
  resultTopic: string;
  cacheHit: boolean;
  lookupDurationMs: number;
  containmentDurationMs: number;
  /** Preserves both checker directions without affecting the reuse decision. */
  directionalContainment?: {
    forward: ContainmentResult;
    reverse: ContainmentResult;
    equivalenceDurationMs: number;
    incomingCanonicalQueryId: string;
    candidateCanonicalQueryId: string;
  };
  timestamp: number;
};

export type FinalResultEntry = {
  queryId: string;
  executionId: string;
  candidateSignature: string;
  checkerInputHash: string;
  strippedQuery: string;
  originalQuery: string;
  resultTopic: string;
  ownerQueryId: string;
  registeredConsumers: Set<string>;
  createdAt: number;
  approximationConfigHash?: string;
  approach?: ReusableApproach;
  state?: ExecutionState;
  runtimeHandle?: ActiveExecutionHandle;
  lastActivityAt?: number;
};

export type FinalResultReuseHit = {
  mode: "final_result_reuse";
  queryId: string;
  executionId: string;
  resultTopic: string;
  ownerQueryId: string;
  candidateQueryIdsInspected: string[];
  equivalence: EquivalenceResult;
  cacheHit: boolean;
  lookupDurationMs: number;
};

type ResolveRegistrationParams = {
  query: string;
  resultTopic: string;
  ownerQueryId: string;
  consumerId: string;
  executionId?: string;
  approximationConfigHash?: string;
};

type ResolveRegistrationResult = {
  entry: FinalResultEntry;
  decision: QueryReuseDecisionEvent;
};

type ResolveReusableRuntimeParams = ResolveRegistrationParams & {
  approach: ReusableApproach;
  createExecution: (canonicalQueryId: string) => Promise<ActiveExecutionHandle>;
};

type ResolveReusableRuntimeResult = ResolveRegistrationResult & {
  executionCreated: boolean;
};

type CandidateSummary = {
  aggregationFamily: string;
  queryForm: string;
  outputMode: string;
};

class KeyedLock {
  private readonly active = new Set<string>();
  private readonly waiters = new Map<string, Array<() => void>>();

  async runExclusive<T>(key: string, task: () => Promise<T>): Promise<T> {
    await this.acquire(key);
    try {
      return await task();
    } finally {
      this.release(key);
    }
  }

  private async acquire(key: string): Promise<void> {
    if (!this.active.has(key)) {
      this.active.add(key);
      return;
    }

    await new Promise<void>((resolve) => {
      const queue = this.waiters.get(key) ?? [];
      queue.push(resolve);
      this.waiters.set(key, queue);
    });
    this.active.add(key);
  }

  private release(key: string): void {
    const queue = this.waiters.get(key);
    const next = queue?.shift();
    if (queue && queue.length > 0) {
      this.waiters.set(key, queue);
    } else {
      this.waiters.delete(key);
    }
    this.active.delete(key);
    next?.();
  }
}

function hashValue(value: string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function detectQueryForm(query: string): string {
  if (/\bCONSTRUCT\b/i.test(query)) {
    return "CONSTRUCT";
  }
  if (/\bASK\b/i.test(query)) {
    return "ASK";
  }
  if (/\bDESCRIBE\b/i.test(query)) {
    return "DESCRIBE";
  }
  return /\bSELECT\b/i.test(query) ? "SELECT" : "UNKNOWN";
}

function detectAggregationFamily(query: string): string {
  const matches = Array.from(query.matchAll(/\b(AVG|SUM|COUNT|MIN|MAX)\s*\(/gi)).map(
    (match) => match[1].toUpperCase(),
  );
  return matches.sort().join(",") || "NONE";
}

function detectOutputMode(query: string): string {
  const match = query.match(/REGISTER\s+([A-Z]+STREAM)\s+</i);
  return match?.[1].toUpperCase() ?? "UNKNOWN";
}

function buildCandidateSummary(query: string): CandidateSummary {
  const stripped = stripConsumerOutputTarget(query);
  return {
    aggregationFamily: detectAggregationFamily(stripped),
    queryForm: detectQueryForm(stripped),
    outputMode: detectOutputMode(stripped),
  };
}

function buildCandidateSignature(
  query: string,
  approximationConfigHash?: string,
): string {
  const summary = buildCandidateSummary(query);
  return JSON.stringify({
    aggregationFamily: summary.aggregationFamily,
    queryForm: summary.queryForm,
    outputMode: summary.outputMode,
    approximationConfigHash: approximationConfigHash ?? null,
  });
}

type CandidateEvaluation = {
  candidateQueryIdsInspected: string[];
  bestObserved?: EquivalenceResult;
  bestObservedCandidate?: FinalResultEntry;
  matchedEntry?: FinalResultEntry;
};

function choosePreferredEquivalence(
  current: EquivalenceResult | undefined,
  candidate: EquivalenceResult,
): EquivalenceResult {
  if (!current) {
    return candidate;
  }
  const currentScore =
    Number(current.equivalent) * 4 +
    Number(current.forward.contained || current.reverse.contained) * 2 +
    Number(current.supported);
  const candidateScore =
    Number(candidate.equivalent) * 4 +
    Number(candidate.forward.contained || candidate.reverse.contained) * 2 +
    Number(candidate.supported);
  if (candidateScore !== currentScore) {
    return candidateScore > currentScore ? candidate : current;
  }
  return candidate.durationMs <= current.durationMs ? candidate : current;
}

function buildQueryId(query: string): string {
  return hashValue(normalizeWhitespace(stripConsumerOutputTarget(query)));
}

export function buildCanonicalQueryId(query: string): string {
  return buildQueryId(query);
}

function expandPrefixedTerm(term: string, prefixes: Map<string, string>): string {
  const trimmed = term.trim();
  if (trimmed.startsWith("<") && trimmed.endsWith(">")) {
    return trimmed.slice(1, -1);
  }
  const match = trimmed.match(/^([A-Za-z][A-Za-z0-9_-]*):(.+)$/);
  if (!match) {
    return trimmed;
  }
  return `${prefixes.get(match[1]) ?? `${match[1]}:`}${match[2]}`;
}

/**
 * Exact final-result reuse cannot cross distinct source declarations. This
 * precondition is deliberately independent of the semantic checker: it makes
 * a checker false-positive fail closed before it can attach a consumer to the
 * wrong final execution.
 */
export function hasEquivalentInputWindowDeclarations(
  queryA: string,
  queryB: string,
): boolean {
  const declarationsFor = (query: string): string[] => {
    const prefixes = new Map<string, string>();
    for (const match of query.matchAll(/PREFIX\s+([A-Za-z][A-Za-z0-9_-]*):\s*<([^>]+)>/gi)) {
      prefixes.set(match[1], match[2]);
    }
    const declarations: string[] = [];
    for (const match of query.matchAll(
      /FROM\s+NAMED\s+WINDOW\s+(<[^>]+>|[^\s]+)\s+ON\s+STREAM\s+([^\s\[]+)\s*\[\s*RANGE\s+(\d+)\s+STEP\s+(\d+)\s*\]/gi,
    )) {
      declarations.push([
        expandPrefixedTerm(match[1], prefixes),
        expandPrefixedTerm(match[2], prefixes),
        match[3],
        match[4],
      ].join("|"));
    }
    return declarations.sort();
  };
  const left = declarationsFor(queryA);
  const right = declarationsFor(queryB);
  return left.length > 0 && JSON.stringify(left) === JSON.stringify(right);
}

function cacheHitFromEquivalence(equivalence: EquivalenceResult): boolean {
  return equivalence.forward.cacheHit && equivalence.reverse.cacheHit;
}

function buildMissDecisionEvidence(equivalence?: EquivalenceResult) {
  return {
    forwardContained: equivalence?.forward.contained ?? false,
    reverseContained: equivalence?.reverse.contained ?? false,
    mutuallyContained: equivalence?.equivalent ?? false,
    supported: equivalence?.supported ?? false,
    cacheHit: equivalence ? cacheHitFromEquivalence(equivalence) : false,
    containmentDurationMs: equivalence
      ? equivalence.forward.durationMs + equivalence.reverse.durationMs
      : 0,
  };
}

function buildDirectionalContainmentEvidence(
  incomingQuery: string,
  candidateQuery: string,
  equivalence?: EquivalenceResult,
): QueryReuseDecisionEvent["directionalContainment"] {
  if (!equivalence) {
    return undefined;
  }
  return {
    forward: equivalence.forward,
    reverse: equivalence.reverse,
    equivalenceDurationMs: equivalence.durationMs,
    incomingCanonicalQueryId: buildQueryId(incomingQuery),
    candidateCanonicalQueryId: buildQueryId(candidateQuery),
  };
}

export class QueryReuseRegistry {
  private readonly finalResults = new Map<string, FinalResultEntry[]>();
  private readonly lock = new KeyedLock();
  private readonly containmentService: RSPQLContainmentService;

  constructor(containmentService = new RSPQLContainmentService()) {
    this.containmentService = containmentService;
  }

  async findExactFinalResult(
    query: string,
    approximationConfigHash?: string,
  ): Promise<FinalResultReuseHit | undefined> {
    const startedAt = Date.now();
    const signature = buildCandidateSignature(query, approximationConfigHash);
    const candidates = this.finalResults.get(signature) ?? [];
    const evaluation = await this.evaluateCandidates(
      query,
      candidates,
      (candidate) => candidate.approximationConfigHash === approximationConfigHash,
    );
    if (evaluation.matchedEntry && evaluation.bestObserved) {
      return {
        mode: "final_result_reuse",
        queryId: evaluation.matchedEntry.queryId,
        executionId: evaluation.matchedEntry.executionId,
        resultTopic: evaluation.matchedEntry.resultTopic,
        ownerQueryId: evaluation.matchedEntry.ownerQueryId,
        candidateQueryIdsInspected: evaluation.candidateQueryIdsInspected,
        equivalence: evaluation.bestObserved,
        cacheHit: cacheHitFromEquivalence(evaluation.bestObserved),
        lookupDurationMs: Date.now() - startedAt,
      };
    }

    return undefined;
  }

  async resolveFinalResultRegistration(
    params: ResolveRegistrationParams,
  ): Promise<ResolveRegistrationResult> {
    const signature = buildCandidateSignature(
      params.query,
      params.approximationConfigHash,
    );
    return this.lock.runExclusive(signature, async () => {
      const lookupStartedAt = Date.now();
      const candidates = this.finalResults.get(signature) ?? [];
      const evaluation = await this.evaluateCandidates(
        params.query,
        candidates,
        (candidate) => candidate.approximationConfigHash === params.approximationConfigHash,
      );
      if (evaluation.matchedEntry && evaluation.bestObserved) {
        const entry = this.getEntryById(evaluation.matchedEntry.queryId);
        if (!entry) {
          throw new Error(
            `Active final-result entry disappeared for queryId=${evaluation.matchedEntry.queryId}`,
          );
        }
        entry.registeredConsumers.add(params.consumerId);
        return {
          entry,
          decision: {
            consumerId: params.consumerId,
            incomingQueryId: buildQueryId(params.query),
            matchedActiveQueryId: entry.queryId,
            candidateQueryIdsInspected: evaluation.candidateQueryIdsInspected,
            forwardContained: evaluation.bestObserved.forward.contained,
            reverseContained: evaluation.bestObserved.reverse.contained,
            mutuallyContained: evaluation.bestObserved.equivalent,
            supported: evaluation.bestObserved.supported,
            reuseHit: true,
            executionId: entry.executionId,
            resultTopic: entry.resultTopic,
            cacheHit: cacheHitFromEquivalence(evaluation.bestObserved),
            lookupDurationMs: Date.now() - lookupStartedAt,
            containmentDurationMs:
              evaluation.bestObserved.forward.durationMs +
              evaluation.bestObserved.reverse.durationMs,
            directionalContainment: buildDirectionalContainmentEvidence(
              params.query,
              evaluation.matchedEntry.originalQuery,
              evaluation.bestObserved,
            ),
            timestamp: Date.now(),
          },
        };
      }

      const strippedQuery = stripConsumerOutputTarget(params.query);
      const entry: FinalResultEntry = {
        queryId: buildQueryId(params.query),
        executionId: params.executionId ?? buildQueryId(`${params.ownerQueryId}:${params.query}`),
        candidateSignature: signature,
        checkerInputHash: this.containmentService.getNormalizedInputHash(params.query),
        strippedQuery,
        originalQuery: params.query,
        resultTopic: params.resultTopic,
        ownerQueryId: params.ownerQueryId,
        registeredConsumers: new Set([params.consumerId]),
        createdAt: Date.now(),
        approximationConfigHash: params.approximationConfigHash,
      };

      const entries = this.finalResults.get(signature) ?? [];
      entries.push(entry);
      this.finalResults.set(signature, entries);
      return {
        entry,
        decision: {
          consumerId: params.consumerId,
          incomingQueryId: entry.queryId,
          candidateQueryIdsInspected: evaluation.candidateQueryIdsInspected,
          ...buildMissDecisionEvidence(evaluation.bestObserved),
          directionalContainment: buildDirectionalContainmentEvidence(
            params.query,
            evaluation.bestObservedCandidate?.originalQuery ?? params.query,
            evaluation.bestObserved,
          ),
          reuseHit: false,
          executionId: entry.executionId,
          resultTopic: entry.resultTopic,
          lookupDurationMs: Date.now() - lookupStartedAt,
          timestamp: Date.now(),
        },
      };
    });
  }

  async resolveReusableRuntimeRegistration(
    params: ResolveReusableRuntimeParams,
  ): Promise<ResolveReusableRuntimeResult> {
    const signature = buildCandidateSignature(
      params.query,
      params.approximationConfigHash,
    );
    return this.lock.runExclusive(signature, async () => {
      const lookupStartedAt = Date.now();
      const candidates = this.finalResults.get(signature) ?? [];
      const evaluation = await this.evaluateCandidates(
        params.query,
        candidates,
        (candidate) =>
          Boolean(candidate.runtimeHandle) &&
          (candidate.state === "starting" || candidate.state === "active") &&
          candidate.approximationConfigHash === params.approximationConfigHash,
      );
      if (evaluation.matchedEntry && evaluation.bestObserved) {
        evaluation.matchedEntry.registeredConsumers.add(params.consumerId);
        evaluation.matchedEntry.lastActivityAt = Date.now();
        return {
          entry: evaluation.matchedEntry,
          executionCreated: false,
          decision: {
            consumerId: params.consumerId,
            incomingQueryId: buildQueryId(params.query),
            matchedActiveQueryId: evaluation.matchedEntry.queryId,
            candidateQueryIdsInspected: evaluation.candidateQueryIdsInspected,
            forwardContained: evaluation.bestObserved.forward.contained,
            reverseContained: evaluation.bestObserved.reverse.contained,
            mutuallyContained: evaluation.bestObserved.equivalent,
            supported: evaluation.bestObserved.supported,
            reuseHit: true,
            executionId: evaluation.matchedEntry.executionId,
            resultTopic: evaluation.matchedEntry.resultTopic,
            cacheHit: cacheHitFromEquivalence(evaluation.bestObserved),
            lookupDurationMs: Date.now() - lookupStartedAt,
            containmentDurationMs:
              evaluation.bestObserved.forward.durationMs +
              evaluation.bestObserved.reverse.durationMs,
            directionalContainment: buildDirectionalContainmentEvidence(
              params.query,
              evaluation.matchedEntry.originalQuery,
              evaluation.bestObserved,
            ),
            timestamp: Date.now(),
          },
        };
      }

      const canonicalQueryId = buildQueryId(params.query);
      const runtimeHandle = await params.createExecution(canonicalQueryId);
      const entry: FinalResultEntry = {
        queryId: canonicalQueryId,
        executionId: runtimeHandle.executionId,
        candidateSignature: signature,
        checkerInputHash: this.containmentService.getNormalizedInputHash(params.query),
        strippedQuery: stripConsumerOutputTarget(params.query),
        originalQuery: params.query,
        resultTopic: runtimeHandle.sharedOutputTopic,
        ownerQueryId: params.ownerQueryId,
        registeredConsumers: new Set([params.consumerId]),
        createdAt: Date.now(),
        approximationConfigHash: params.approximationConfigHash,
        approach: params.approach,
        state: runtimeHandle.state,
        runtimeHandle,
        lastActivityAt: Date.now(),
      };
      const entries = this.finalResults.get(signature) ?? [];
      entries.push(entry);
      this.finalResults.set(signature, entries);
      return {
        entry,
        executionCreated: true,
        decision: {
          consumerId: params.consumerId,
          incomingQueryId: entry.queryId,
          candidateQueryIdsInspected: evaluation.candidateQueryIdsInspected,
          ...buildMissDecisionEvidence(evaluation.bestObserved),
          directionalContainment: buildDirectionalContainmentEvidence(
            params.query,
            evaluation.bestObservedCandidate?.originalQuery ?? params.query,
            evaluation.bestObserved,
          ),
          reuseHit: false,
          executionId: entry.executionId,
          resultTopic: entry.resultTopic,
          lookupDurationMs: Date.now() - lookupStartedAt,
          timestamp: Date.now(),
        },
      };
    });
  }

  registerFinalResult(params: {
    query: string;
    resultTopic: string;
    ownerQueryId: string;
    consumerId?: string;
    executionId?: string;
    approximationConfigHash?: string;
  }): FinalResultEntry {
    const signature = buildCandidateSignature(
      params.query,
      params.approximationConfigHash,
    );
    const entry: FinalResultEntry = {
      queryId: buildQueryId(params.query),
      executionId: params.executionId ?? buildQueryId(`${params.ownerQueryId}:${params.query}`),
      candidateSignature: signature,
      checkerInputHash: this.containmentService.getNormalizedInputHash(params.query),
      strippedQuery: stripConsumerOutputTarget(params.query),
      originalQuery: params.query,
      resultTopic: params.resultTopic,
      ownerQueryId: params.ownerQueryId,
      registeredConsumers: new Set(params.consumerId ? [params.consumerId] : []),
      createdAt: Date.now(),
      approximationConfigHash: params.approximationConfigHash,
    };
    const entries = this.finalResults.get(signature) ?? [];
    entries.push(entry);
    this.finalResults.set(signature, entries);
    return entry;
  }

  registerConsumer(params: {
    queryId: string;
    consumerId: string;
  }): void {
    const entry = this.getEntryById(params.queryId);
    if (!entry) {
      throw new Error(`No final-result entry registered for queryId=${params.queryId}`);
    }
    entry.registeredConsumers.add(params.consumerId);
  }

  getAllEntries(): FinalResultEntry[] {
    return Array.from(this.finalResults.values()).flat();
  }

  private getEntryById(queryId: string): FinalResultEntry | undefined {
    return this.getAllEntries().find((entry) => entry.queryId === queryId);
  }

  invalidateExecution(queryId: string): void {
    for (const [signature, entries] of this.finalResults.entries()) {
      const remaining = entries.filter((entry) => entry.queryId !== queryId);
      if (remaining.length === 0) {
        this.finalResults.delete(signature);
      } else if (remaining.length !== entries.length) {
        this.finalResults.set(signature, remaining);
      }
    }
  }

  private async evaluateCandidates(
    query: string,
    candidates: FinalResultEntry[],
    candidateFilter: (candidate: FinalResultEntry) => boolean,
  ): Promise<CandidateEvaluation> {
    const candidateQueryIdsInspected: string[] = [];
    let bestObserved: EquivalenceResult | undefined;
    let bestObservedCandidate: FinalResultEntry | undefined;
    let matchedEntry: FinalResultEntry | undefined;

    for (const candidate of candidates) {
      if (!candidateFilter(candidate)) {
        continue;
      }
      candidateQueryIdsInspected.push(candidate.queryId);
      const equivalence = await this.containmentService.checkEquivalence(
        query,
        candidate.originalQuery,
      );
      const preferred = choosePreferredEquivalence(bestObserved, equivalence);
      if (preferred === equivalence) {
        bestObservedCandidate = candidate;
      }
      bestObserved = preferred;
      if (
        equivalence.equivalent &&
        hasEquivalentInputWindowDeclarations(query, candidate.originalQuery)
      ) {
        matchedEntry = candidate;
        bestObserved = equivalence;
        break;
      }
    }

    return {
      candidateQueryIdsInspected,
      bestObserved,
      bestObservedCandidate,
      matchedEntry,
    };
  }

  static buildApproximationConfigHash(config: unknown): string {
    return hashValue(JSON.stringify(config));
  }
}

export function summarizeContainment(
  result: ContainmentResult,
): Pick<ContainmentResult, "contained" | "supported" | "reason" | "failureKind"> {
  return {
    contained: result.contained,
    supported: result.supported,
    reason: result.reason,
    failureKind: result.failureKind,
  };
}
