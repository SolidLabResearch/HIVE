function createBenchmarkReplayRunEnv(baseEnv = process.env) {
  const runStartTime =
    baseEnv.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME || `${Date.now()}`;

  function withBenchmarkReplayEnv(env = {}) {
    return {
      ...env,
      STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME:
        env.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME ||
        baseEnv.STREAMING_QUERY_HIVE_DETERMINISTIC_EVENT_TIME ||
        "1",
      STREAMING_QUERY_HIVE_BENCHMARK_START_TIME:
        env.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME ||
        baseEnv.STREAMING_QUERY_HIVE_BENCHMARK_START_TIME ||
        runStartTime,
    };
  }

  return {
    runStartTime,
    withBenchmarkReplayEnv,
  };
}

module.exports = {
  createBenchmarkReplayRunEnv,
};
