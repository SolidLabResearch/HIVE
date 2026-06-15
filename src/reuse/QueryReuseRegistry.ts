import {
  getCanonicalRSPQLQueryHash,
} from "./normalizeRSPQLForExactReuse";

export type ReuseMode =
  | "fresh_execution"
  | "final_result_reuse"
  | "chunk_state_reuse";

export type FinalResultEntry = {
  canonicalQueryHash: string;
  normalizedQuery: string;
  resultTopic: string;
  ownerQueryId: string;
  registeredConsumers: Set<string>;
  createdAt: number;
};

export type FinalResultReuseHit = {
  mode: "final_result_reuse";
  canonicalQueryHash: string;
  resultTopic: string;
  ownerQueryId: string;
};

export class QueryReuseRegistry {
  private readonly finalResults = new Map<string, FinalResultEntry>();

  findExactFinalResult(query: string): FinalResultReuseHit | undefined {
    const { canonicalQueryHash } = getCanonicalRSPQLQueryHash(query);
    const entry = this.finalResults.get(canonicalQueryHash);
    if (entry) {
      return {
        mode: "final_result_reuse",
        canonicalQueryHash: entry.canonicalQueryHash,
        resultTopic: entry.resultTopic,
        ownerQueryId: entry.ownerQueryId,
      };
    }

    return undefined;
  }

  registerFinalResult(params: {
    query: string;
    resultTopic: string;
    ownerQueryId: string;
    consumerId?: string;
  }): FinalResultEntry {
    const { normalizedQuery, canonicalQueryHash } = getCanonicalRSPQLQueryHash(
      params.query,
    );
    const existing = this.finalResults.get(canonicalQueryHash);
    if (existing) {
      if (params.consumerId) {
        existing.registeredConsumers.add(params.consumerId);
      }
      return existing;
    }

    const entry: FinalResultEntry = {
      canonicalQueryHash,
      normalizedQuery,
      resultTopic: params.resultTopic,
      ownerQueryId: params.ownerQueryId,
      registeredConsumers: new Set<string>(
        params.consumerId ? [params.consumerId] : [],
      ),
      createdAt: Date.now(),
    };

    this.finalResults.set(canonicalQueryHash, entry);
    return entry;
  }

  registerConsumer(params: {
    canonicalQueryHash: string;
    consumerId: string;
  }): void {
    const entry = this.finalResults.get(params.canonicalQueryHash);
    if (!entry) {
      throw new Error(
        `No final-result entry registered for canonicalQueryHash=${params.canonicalQueryHash}`,
      );
    }
    entry.registeredConsumers.add(params.consumerId);
  }
}
