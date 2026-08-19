const fs = require('fs');
const path = require('path');
const {
  ProcessTreeTracker,
  collectTreeMetrics,
  summarizeResourceSamples,
} = require('../../experiments/utils/processTreeMetrics');

function startProcessTreeResourceLogging(filePath, rootPid, intervalMs = 100) {
  const rootPids = Array.isArray(rootPid) ? rootPid : [rootPid];
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const writeHeader = !fs.existsSync(filePath);
  const logStream = fs.createWriteStream(filePath, { flags: 'a' });
  if (writeHeader) {
    logStream.write('timestamp,root_pid,process_count,tree_rss_bytes,tree_cpu_seconds,tree_cpu_seconds_delta,tree_cpu_seconds_raw_snapshot,total_cpu_pct\n');
  }

  let stopped = false;
  const tracker = new ProcessTreeTracker();
  const startedAt = Date.now();

  const writeSample = () => {
    if (stopped) {
      return;
    }

    try {
      const timestamp = Date.now();
      const sample = collectTreeMetrics(rootPids, tracker, timestamp, timestamp - startedAt);
      if (!sample) {
        return;
      }
      logStream.write(
        [
          timestamp,
          rootPids.join(";"),
          sample.processCount,
          (sample.totalRssMb * 1024 * 1024).toFixed(0),
          sample.treeCpuSeconds.toFixed(3),
          sample.treeCpuSecondsDelta.toFixed(3),
          sample.treeCpuSecondsRawSnapshot.toFixed(3),
          sample.totalCpuPct.toFixed(3),
        ].join(',') + '\n',
      );
    } catch (error) {
      logStream.write(
        [
          Date.now(),
          rootPids.join(";"),
          0,
          0,
          0,
          0,
          0,
          0,
        ].join(',') + '\n',
      );
    }
  };

  const timer = setInterval(writeSample, intervalMs);
  timer.unref?.();
  writeSample();

  return {
    stop() {
      if (stopped) {
        return summarizeResourceSamples([]);
      }
      stopped = true;
      clearInterval(timer);
      logStream.end();
      const summary = {
        rootPids,
        logPath: filePath,
        trackerNegativeDeltaCount: tracker.negativeDeltaEvents.length,
        trackerResetLikeDeltaCount: tracker.resetLikeDeltaCount,
        negativeDeltaEvents: tracker.negativeDeltaEvents,
        perPid: tracker.getPerPidSummary(),
      };
      try {
        fs.writeFileSync(
          filePath.replace(/\.csv$/i, '_per_pid_summary.json'),
          `${JSON.stringify(summary, null, 2)}\n`,
        );
      } catch {
        // Ignore sidecar write failures; the main CSV should remain authoritative.
      }
      return {
        ...summarizeResourceSamples([]),
        ...summary,
      };
    },
  };
}

module.exports = {
  startProcessTreeResourceLogging,
};
