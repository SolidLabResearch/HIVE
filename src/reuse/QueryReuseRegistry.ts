import crypto from "crypto";
import { RSPQLParser } from "rspql-containment-checker";
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

type CandidateSummary = {
  aggregationFamily: string;
  queryForm: string;
  inputStreams: string[];
  outputMode: string;
  projectionArity: number;
  windowCount: number;
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

const signatureParser = new RSPQLParser();

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

function detectProjectionArity(query: string): number {
  const selectMatch = query.match(/\bSELECT\b([\s\S]*?)\bFROM\b/i);
  if (!selectMatch) {
    return 0;
  }
  return selectMatch[1]
    .split(/\s+/)
    .filter((token) => token.startsWith("?") || token.includes("AS"))
    .length;
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
  const parsed = signatureParser.parse(stripped);
  return {
    aggregationFamily: detectAggregationFamily(stripped),
    queryForm: detectQueryForm(stripped),
    inputStreams: parsed.s2r.map((entry) => entry.stream_name).sort(),
    outputMode: detectOutputMode(stripped),
    projectionArity: detectProjectionArity(stripped),
    windowCount: parsed.s2r.length,
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
    inputStreams: summary.inputStreams,
    outputMode: summary.outputMode,
    projectionArity: summary.projectionArity,
    windowCount: summary.windowCount,
    approximationConfigHash: approximationConfigHash ?? null,
  });
}

function buildQueryId(query: string): string {
  return hashValue(normalizeWhitespace(stripConsumerOutputTarget(query)));
}

function cacheHitFromEquivalence(equivalence: EquivalenceResult): boolean {
  return equivalence.forward.cacheHit && equivalence.reverse.cacheHit;
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
    const candidateQueryIdsInspected: string[] = [];

    for (const candidate of candidates) {
      candidateQueryIdsInspected.push(candidate.queryId);
      const equivalence = await this.containmentService.checkEquivalence(
        query,
        candidate.originalQuery,
      );
      if (
        equivalence.equivalent &&
        candidate.approximationConfigHash === approximationConfigHash
      ) {
        return {
          mode: "final_result_reuse",
          queryId: candidate.queryId,
          executionId: candidate.executionId,
          resultTopic: candidate.resultTopic,
          ownerQueryId: candidate.ownerQueryId,
          candidateQueryIdsInspected,
          equivalence,
          cacheHit: cacheHitFromEquivalence(equivalence),
          lookupDurationMs: Date.now() - startedAt,
        };
      }
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
      const hit = await this.findExactFinalResult(
        params.query,
        params.approximationConfigHash,
      );
      if (hit) {
        const entry = this.getEntryById(hit.queryId);
        if (!entry) {
          throw new Error(`Active final-result entry disappeared for queryId=${hit.queryId}`);
        }
        entry.registeredConsumers.add(params.consumerId);
        return {
          entry,
          decision: {
            consumerId: params.consumerId,
            incomingQueryId: buildQueryId(params.query),
            matchedActiveQueryId: entry.queryId,
            candidateQueryIdsInspected: hit.candidateQueryIdsInspected,
            forwardContained: hit.equivalence.forward.contained,
            reverseContained: hit.equivalence.reverse.contained,
            mutuallyContained: hit.equivalence.equivalent,
            supported: hit.equivalence.supported,
            reuseHit: true,
            executionId: entry.executionId,
            resultTopic: entry.resultTopic,
            cacheHit: hit.cacheHit,
            lookupDurationMs: hit.lookupDurationMs,
            containmentDurationMs:
              hit.equivalence.forward.durationMs + hit.equivalence.reverse.durationMs,
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
          candidateQueryIdsInspected: entries.slice(0, -1).map((candidate) => candidate.queryId),
          forwardContained: false,
          reverseContained: false,
          mutuallyContained: false,
          supported: false,
          reuseHit: false,
          executionId: entry.executionId,
          resultTopic: entry.resultTopic,
          cacheHit: false,
          lookupDurationMs: Date.now() - lookupStartedAt,
          containmentDurationMs: 0,
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
