import {
  ApproximationConsumerSummary,
  buildApproximationConsumerSummaryPath,
  readJsonIfExists,
} from "./approximationKScalingArtifacts";

type ApproximationParentTrace = (
  event: string,
  extra?: Record<string, unknown>,
) => void;

export type WaitForApproximationParentCompletionArgs = {
  expectedConsumerCount: number;
  logRoot: string;
  completionPromises: Array<Promise<void>>;
  completionPromisesResolved: Set<number>;
  completionEventsObserved: Set<number>;
  consumerSummaryValid: Set<number>;
  reconciliationEligible: () => boolean;
  appendParentTrace: ApproximationParentTrace;
  dumpParentTimeoutState: (reason: string) => void;
  pollIntervalMs?: number;
  durableStateMismatchTimeoutMs?: number;
  delay?: (ms: number) => Promise<void>;
  now?: () => number;
};

export type ApproximationParentCompletionResult = {
  reconciledFromDurableState: boolean;
  completionStateMismatch: boolean;
};

function isValidComparableSummary(
  summary: ApproximationConsumerSummary | null,
): boolean {
  return Boolean(summary?.isComparableWindow && summary?.emittedFinalWindowCount >= 1);
}

export async function waitForApproximationParentCompletion({
  expectedConsumerCount,
  logRoot,
  completionPromises,
  completionPromisesResolved,
  completionEventsObserved,
  consumerSummaryValid,
  reconciliationEligible,
  appendParentTrace,
  dumpParentTimeoutState,
  pollIntervalMs = 100,
  durableStateMismatchTimeoutMs = 30_000,
  delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  now = () => Date.now(),
}: WaitForApproximationParentCompletionArgs): Promise<ApproximationParentCompletionResult> {
  let reconciliationStartedAt: number | null = null;

  while (completionPromisesResolved.size < expectedConsumerCount) {
    if (reconciliationEligible()) {
      if (reconciliationStartedAt === null) {
        reconciliationStartedAt = now();
      }
      consumerSummaryValid.clear();
      appendParentTrace("aggregate_read_started", {
        completionEventsObserved: completionEventsObserved.size,
      });
      for (let consumerIndex = 1; consumerIndex <= expectedConsumerCount; consumerIndex += 1) {
        const summary = await readJsonIfExists<ApproximationConsumerSummary>(
          buildApproximationConsumerSummaryPath(logRoot, consumerIndex),
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
        return {
          reconciledFromDurableState: mismatch,
          completionStateMismatch: mismatch,
        };
      }

      if (
        reconciliationStartedAt !== null &&
        (now() - reconciliationStartedAt) >= durableStateMismatchTimeoutMs
      ) {
        dumpParentTimeoutState("completion_wait_deadline_exceeded");
        throw new Error(
          `APPROXIMATION_PARENT_COMPLETION_STATE_MISMATCH events=${completionEventsObserved.size}/${expectedConsumerCount} resolved=${completionPromisesResolved.size}/${expectedConsumerCount} validSummaries=${consumerSummaryValid.size}/${expectedConsumerCount}`,
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
