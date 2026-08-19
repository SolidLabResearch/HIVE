import path from "path";
import { FetchingConsumerSummary, readJsonIfExists } from "./fetchingKScalingArtifacts";

type ParentCompletionTrace = (event: string, extra?: Record<string, unknown>) => void;

export type WaitForFetchingParentCompletionArgs = {
  expectedConsumerCount: number;
  logRoot: string;
  completionPromises: Array<Promise<void>>;
  durableEventsObserved: Set<number>;
  completionPromisesResolved: Set<number>;
  consumerSummaryValid: Set<number>;
  appendParentTrace: ParentCompletionTrace;
  dumpParentTimeoutState: (reason: string) => void;
  pollIntervalMs?: number;
  durableStateMismatchTimeoutMs?: number;
  delay?: (ms: number) => Promise<void>;
  now?: () => number;
};

export type FetchingParentCompletionResult = {
  reconciledFromDurableState: boolean;
  completionStateMismatch: boolean;
};

function isValidComparableSummary(summary: FetchingConsumerSummary | null): boolean {
  return Boolean(summary?.isComparableWindow && summary?.emittedFinalWindowCount >= 1);
}

export async function waitForFetchingParentCompletion({
  expectedConsumerCount,
  logRoot,
  completionPromises,
  durableEventsObserved,
  completionPromisesResolved,
  consumerSummaryValid,
  appendParentTrace,
  dumpParentTimeoutState,
  pollIntervalMs = 100,
  durableStateMismatchTimeoutMs = 30_000,
  delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  now = () => Date.now(),
}: WaitForFetchingParentCompletionArgs): Promise<FetchingParentCompletionResult> {
  let durableReconciliationStartedAt: number | null = null;

  while (completionPromisesResolved.size < expectedConsumerCount) {
    if (durableEventsObserved.size === expectedConsumerCount) {
      if (durableReconciliationStartedAt === null) {
        durableReconciliationStartedAt = now();
      }
      appendParentTrace("aggregate_read_started");
      consumerSummaryValid.clear();
      for (let consumerIndex = 1; consumerIndex <= expectedConsumerCount; consumerIndex += 1) {
        appendParentTrace("consumer_summary_read_started", { consumerIndex });
        const summary = await readJsonIfExists<FetchingConsumerSummary>(
          path.join(logRoot, `benchmark_window_cap_summary_consumer_${consumerIndex}.json`),
        );
        const valid = isValidComparableSummary(summary);
        appendParentTrace("consumer_summary_read_completed", {
          consumerIndex,
          found: Boolean(summary),
          valid,
        });
        if (valid) {
          consumerSummaryValid.add(consumerIndex);
        }
      }
      if (consumerSummaryValid.size === expectedConsumerCount) {
        const mismatch = completionPromisesResolved.size !== expectedConsumerCount;
        if (mismatch) {
          appendParentTrace("promise_rejected", {
            reason: "PARENT_COMPLETION_STATE_MISMATCH",
            resolvedCount: completionPromisesResolved.size,
          });
        }
        return {
          reconciledFromDurableState: true,
          completionStateMismatch: mismatch,
        };
      }

      if ((now() - durableReconciliationStartedAt) >= durableStateMismatchTimeoutMs) {
        dumpParentTimeoutState("completion_wait_deadline_exceeded");
        throw new Error(
          `PARENT_COMPLETION_STATE_MISMATCH durable=${durableEventsObserved.size}/${expectedConsumerCount} resolved=${completionPromisesResolved.size}/${expectedConsumerCount} validSummaries=${consumerSummaryValid.size}/${expectedConsumerCount}`,
        );
      }
    }

    await Promise.race([
      Promise.all(completionPromises).catch((error) => {
        throw error;
      }),
      delay(pollIntervalMs),
    ]);

    if (completionPromisesResolved.size === expectedConsumerCount) {
      appendParentTrace("all_completion_promises_resolved");
      return {
        reconciledFromDurableState: false,
        completionStateMismatch: false,
      };
    }
  }

  appendParentTrace("all_completion_promises_resolved");
  return {
    reconciledFromDurableState: false,
    completionStateMismatch: false,
  };
}
