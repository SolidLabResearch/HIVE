function getReplayMetadata(baseEnv = process.env) {
  const deterministicEventTimeRaw =
    baseEnv.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME;
  const benchmarkStartTimeRaw =
    baseEnv.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME;

  return {
    streamingQueryHiveDeterministicEventTime:
      deterministicEventTimeRaw !== undefined
        ? String(deterministicEventTimeRaw)
        : "1",
    streamingQueryHiveBenchmarkStartTime:
      benchmarkStartTimeRaw !== undefined
        ? String(benchmarkStartTimeRaw)
        : String(Date.now()),
    deterministicEventTimeSource:
      deterministicEventTimeRaw !== undefined ? "manual-env" : "helper-default",
    benchmarkStartTimeSource:
      benchmarkStartTimeRaw !== undefined ? "manual-env" : "helper-default",
    windowSemantics: "[start,end)",
  };
}

function attachReplayMetadata(target, baseEnv = process.env) {
  return {
    ...target,
    replayMetadata: getReplayMetadata(baseEnv),
  };
}

module.exports = {
  getReplayMetadata,
  attachReplayMetadata,
};
