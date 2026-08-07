import {
  QueryReuseRegistry,
  QueryReuseDecisionEvent,
} from "../../reuse/QueryReuseRegistry";
import {
  QueryExecutionDispatcher,
  RegistrationApproach,
  deriveCanonicalQueryId,
} from "./QueryExecutionDispatcher";
import { SubqueryProducerRuntimeSnapshot } from "./SubqueryProducerManager";

export type RuntimeRegistrationRequest = {
  approach: RegistrationApproach;
  query: string;
  requestedOutputTopic: string;
  ownerQueryId: string;
  consumerId: string;
  approximationConfigHash?: string;
};

export type RuntimeRegistrationResponse = {
  consumerId: string;
  canonicalQueryId: string;
  executionId: string;
  executionCreated: boolean;
  reuseHit: boolean;
  sharedOutputTopic: string;
  executionState: string;
  containmentDecision: QueryReuseDecisionEvent;
  registrationTimestamp: number;
  producerSnapshots?: SubqueryProducerRuntimeSnapshot[];
};

export class ProductionQueryRegistrationService {
  private readonly registry: QueryReuseRegistry;
  private readonly dispatcher: QueryExecutionDispatcher;

  constructor(
    registry = new QueryReuseRegistry(),
    dispatcher?: QueryExecutionDispatcher,
  ) {
    this.registry = registry;
    this.dispatcher =
      dispatcher ??
      new QueryExecutionDispatcher(undefined, {
        onExecutionFailed: (canonicalQueryId) => {
          this.registry.invalidateExecution(canonicalQueryId);
        },
      });
  }

  async shutdown(): Promise<void> {
    await this.dispatcher.shutdown();
  }

  async register(
    request: RuntimeRegistrationRequest,
  ): Promise<RuntimeRegistrationResponse> {
    const registrationTimestamp = Date.now();

    if (request.approach === "fetching") {
      const canonicalQueryId = deriveCanonicalQueryId(request.query);
      const execution = await this.dispatcher.createExecution({
        approach: request.approach,
        query: request.query,
        canonicalQueryId,
        requestedOutputTopic: request.requestedOutputTopic,
      });
      return {
        consumerId: request.consumerId,
        canonicalQueryId: execution.canonicalQueryId,
        executionId: execution.executionId,
        executionCreated: true,
        reuseHit: false,
        sharedOutputTopic: execution.sharedOutputTopic,
        executionState: execution.state,
        containmentDecision: {
          consumerId: request.consumerId,
          incomingQueryId: canonicalQueryId,
          candidateQueryIdsInspected: [],
          forwardContained: false,
          reverseContained: false,
          mutuallyContained: false,
          supported: false,
          reuseHit: false,
          executionId: execution.executionId,
          resultTopic: execution.sharedOutputTopic,
          cacheHit: false,
          lookupDurationMs: 0,
          containmentDurationMs: 0,
          timestamp: registrationTimestamp,
        },
        registrationTimestamp,
        producerSnapshots: execution.producerSnapshots,
      };
    }

    const resolved = await this.registry.resolveReusableRuntimeRegistration({
      approach: request.approach,
      query: request.query,
      resultTopic: request.requestedOutputTopic,
      ownerQueryId: request.ownerQueryId,
      consumerId: request.consumerId,
      approximationConfigHash: request.approximationConfigHash,
      createExecution: async (canonicalQueryId) =>
        this.dispatcher.createExecution({
          approach: request.approach,
          query: request.query,
          approximationConfigHash: request.approximationConfigHash,
          canonicalQueryId,
          requestedOutputTopic: request.requestedOutputTopic,
        }),
    });

    return {
      consumerId: request.consumerId,
      canonicalQueryId: resolved.entry.queryId,
      executionId: resolved.entry.executionId,
      executionCreated: resolved.executionCreated,
      reuseHit: resolved.decision.reuseHit,
      sharedOutputTopic: resolved.entry.resultTopic,
      executionState: resolved.entry.state || "active",
      containmentDecision: resolved.decision,
      registrationTimestamp,
      producerSnapshots: resolved.entry.runtimeHandle?.producerSnapshots,
    };
  }
}
