function getReplayMetadata(baseEnv = process.env) {
  const deterministicEventTimeRaw =
    baseEnv.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME;
  const benchmarkStartTimeRaw =
    baseEnv.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME;
  const benchmarkEventTimeAnchorRaw =
    baseEnv.STREAMING_QUERY_HIVE_BENCHMARK_EVENT_TIME_ANCHOR;
  const outputWindowRangeRaw = baseEnv.OUTPUT_WINDOW_RANGE;
  const outputWindowStepRaw = baseEnv.OUTPUT_WINDOW_STEP;
  const subWindowRangeRaw = baseEnv.SUB_WINDOW_RANGE;
  const subWindowStepRaw = baseEnv.SUB_WINDOW_STEP;
  const benchmarkTopicPrefixRaw =
    baseEnv.STREAMING_QUERY_HIVE_BENCHMARK_TOPIC_PREFIX;
  const timestampDomainMinRaw =
    baseEnv.STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MIN;
  const timestampDomainMaxRaw =
    baseEnv.STREAMING_QUERY_HIVE_TIMESTAMP_DOMAIN_MAX;

  return {
    streamingQueryHiveDeterministicEventTime:
      deterministicEventTimeRaw !== undefined
        ? String(deterministicEventTimeRaw)
        : "1",
    streamingQueryHiveBenchmarkStartTime:
      benchmarkStartTimeRaw !== undefined
        ? String(benchmarkStartTimeRaw)
        : null,
    benchmarkEventTimeAnchor:
      benchmarkEventTimeAnchorRaw !== undefined &&
      benchmarkEventTimeAnchorRaw !== ""
        ? String(benchmarkEventTimeAnchorRaw)
        : null,
    deterministicEventTimeSource:
      deterministicEventTimeRaw !== undefined ? "manual-env" : "helper-default",
    benchmarkStartTimeSource:
      benchmarkStartTimeRaw !== undefined ? "manual-env" : "unknown",
    benchmarkEventTimeAnchorSource:
      benchmarkEventTimeAnchorRaw !== undefined &&
      benchmarkEventTimeAnchorRaw !== ""
        ? "manual-env"
        : "unknown",
    benchmarkTopicPrefix:
      benchmarkTopicPrefixRaw !== undefined && benchmarkTopicPrefixRaw !== ""
        ? String(benchmarkTopicPrefixRaw)
        : null,
    timestampDomainMin:
      timestampDomainMinRaw !== undefined && timestampDomainMinRaw !== ""
        ? String(timestampDomainMinRaw)
        : null,
    timestampDomainMax:
      timestampDomainMaxRaw !== undefined && timestampDomainMaxRaw !== ""
        ? String(timestampDomainMaxRaw)
        : null,
    outputWindowRangeMs:
      outputWindowRangeRaw !== undefined && outputWindowRangeRaw !== ""
        ? Number.parseInt(outputWindowRangeRaw, 10)
        : null,
    outputWindowStepMs:
      outputWindowStepRaw !== undefined && outputWindowStepRaw !== ""
        ? Number.parseInt(outputWindowStepRaw, 10)
        : null,
    subWindowRangeMs:
      subWindowRangeRaw !== undefined && subWindowRangeRaw !== ""
        ? Number.parseInt(subWindowRangeRaw, 10)
        : null,
    subWindowStepMs:
      subWindowStepRaw !== undefined && subWindowStepRaw !== ""
        ? Number.parseInt(subWindowStepRaw, 10)
        : null,
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
