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

export interface TopicWindowBufferState {
  windows: BufferedTopicWindow[];
  headIndex: number;
}

export type TopicWindowBuffers = Map<string, TopicWindowBufferState>;

function getOrCreateBufferState(
  windowBuffers: TopicWindowBuffers,
  topic: string,
): TopicWindowBufferState {
  let state = windowBuffers.get(topic);
  if (!state) {
    state = { windows: [], headIndex: 0 };
    windowBuffers.set(topic, state);
  }

  return state;
}

export function appendTopicResult(
  windowBuffers: TopicWindowBuffers,
  topic: string,
  result: BufferedTopicWindow,
): void {
  getOrCreateBufferState(windowBuffers, topic).windows.push(result);
}

export function cleanupOldWindows(
  buffer: TopicWindowBufferState,
  windowStartGlobal: number,
): void {
  while (
    buffer.headIndex < buffer.windows.length &&
    buffer.windows[buffer.headIndex].end < windowStartGlobal
  ) {
    buffer.headIndex++;
  }

  if (buffer.headIndex > 0 && buffer.headIndex * 2 >= buffer.windows.length) {
    buffer.windows = buffer.windows.slice(buffer.headIndex);
    buffer.headIndex = 0;
  }
}

export function getLatestTopicValue(
  buffer: TopicWindowBufferState | undefined,
): number | undefined {
  if (!buffer || buffer.windows.length === 0) {
    return undefined;
  }

  return buffer.windows[buffer.windows.length - 1].value;
}

export function getActiveWindowCount(
  buffer: TopicWindowBufferState | undefined,
): number {
  if (!buffer) {
    return 0;
  }

  return Math.max(0, buffer.windows.length - buffer.headIndex);
}

export function computeTopicLevelApproximationResult(
  buffer: TopicWindowBufferState,
  target: ApproximationTargetWindow,
): number | string {
  return mergeMultipleSlidingWindowResults(
    buffer.windows,
    target,
    buffer.windows[buffer.windows.length - 1].agg,
    buffer.headIndex,
  );
}
