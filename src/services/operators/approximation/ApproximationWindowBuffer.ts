import {
  ApproximationAggregation,
  ApproximationTargetWindow,
  mergeMultipleSlidingWindowResults,
} from "./RateBasedApproximationMath";

export interface BufferedTopicWindow {
  start: number;
  end: number;
  value: number;
  agg: ApproximationAggregation;
}

export type TopicWindowBuffers = Map<string, BufferedTopicWindow[]>;

export function appendTopicResult(
  windowBuffers: TopicWindowBuffers,
  topic: string,
  result: BufferedTopicWindow,
): void {
  if (!windowBuffers.has(topic)) {
    windowBuffers.set(topic, []);
  }
  windowBuffers.get(topic)!.push(result);
}

export function cleanupOldWindows(
  buffer: BufferedTopicWindow[],
  windowStartGlobal: number,
): void {
  while (buffer.length && buffer[0].end < windowStartGlobal) {
    buffer.shift();
  }
}

export function getLatestTopicValue(
  buffer: BufferedTopicWindow[] | undefined,
): number | undefined {
  if (!buffer || buffer.length === 0) {
    return undefined;
  }

  return buffer[buffer.length - 1].value;
}

export function computeTopicLevelApproximationResult(
  buffer: BufferedTopicWindow[],
  target: ApproximationTargetWindow,
): number | string {
  return mergeMultipleSlidingWindowResults(
    buffer,
    target,
    buffer[buffer.length - 1].agg,
  );
}
