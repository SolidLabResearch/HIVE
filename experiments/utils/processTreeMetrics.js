const { execSync } = require("child_process");

function parseCpuTime(value) {
  if (!value) {
    return 0;
  }

  const trimmed = String(value).trim();
  const match = trimmed.match(/(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$/);
  if (!match) {
    return 0;
  }

  const days = Number(match[1] || 0);
  const hours = Number(match[2] || 0);
  const minutes = Number(match[3] || 0);
  const seconds = Number(match[4] || 0);
  return (((days * 24 + hours) * 60 + minutes) * 60) + seconds;
}

function parsePsSnapshot(psOutput) {
  return psOutput
    .trim()
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.split(/\s+/, 6);
      if (parts.length < 5) {
        return null;
      }

      const pid = Number.parseInt(parts[0], 10);
      const ppid = Number.parseInt(parts[1], 10);
      const cpuPct = Number.parseFloat(parts[2]);
      const rssKb = Number.parseFloat(parts[3]);
      const cpuTime = parts[4] || "";
      const command = parts[5] || "";

      if (!Number.isFinite(pid) || !Number.isFinite(ppid)) {
        return null;
      }

      return {
        pid,
        ppid,
        cpuPct: Number.isFinite(cpuPct) ? cpuPct : 0,
        rssKb: Number.isFinite(rssKb) ? rssKb : 0,
        cpuSeconds: parseCpuTime(cpuTime),
        command,
      };
    })
    .filter(Boolean);
}

function readPsSnapshot() {
  return parsePsSnapshot(
    execSync("ps -axo pid=,ppid=,%cpu=,rss=,time=,command=", {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      maxBuffer: 1024 * 1024 * 8,
    }),
  );
}

function collectProcessTree(snapshot, rootPids) {
  const liveRootPids = rootPids.filter((pid) => {
    if (!Number.isFinite(pid)) {
      return false;
    }

    try {
      process.kill(pid, 0);
      return true;
    } catch {
      return false;
    }
  });

  if (liveRootPids.length === 0) {
    return null;
  }

  const childrenByParent = new Map();
  for (const proc of snapshot) {
    const children = childrenByParent.get(proc.ppid) || [];
    children.push(proc.pid);
    childrenByParent.set(proc.ppid, children);
  }

  const pidMap = new Map(snapshot.map((proc) => [proc.pid, proc]));
  const seen = new Set();
  const queue = [...liveRootPids];
  const tree = [];

  while (queue.length > 0) {
    const pid = queue.shift();
    if (seen.has(pid)) {
      continue;
    }
    seen.add(pid);

    const proc = pidMap.get(pid);
    if (!proc) {
      continue;
    }

    tree.push(proc);
    const children = childrenByParent.get(pid) || [];
    for (const childPid of children) {
      if (!seen.has(childPid)) {
        queue.push(childPid);
      }
    }
  }

  if (tree.length === 0) {
    return null;
  }

  return {
    rootPids: liveRootPids,
    tree,
  };
}

class ProcessTreeTracker {
  constructor() {
    this.lastCpuSecondsByPid = new Map();
    this.totalCpuSeconds = 0;
    this.pidStats = new Map();
    this.negativeDeltaEvents = [];
    this.resetLikeDeltaCount = 0;
  }

  update(tree, timestamp, elapsedMs) {
    let sampleDeltaCpuSeconds = 0;
    let rawSnapshotCpuSeconds = 0;

    for (const proc of tree) {
      rawSnapshotCpuSeconds += proc.cpuSeconds;

      const prevCpuSeconds = this.lastCpuSecondsByPid.get(proc.pid);
      let deltaCpuSeconds = 0;
      if (Number.isFinite(prevCpuSeconds)) {
        if (proc.cpuSeconds >= prevCpuSeconds) {
          deltaCpuSeconds = proc.cpuSeconds - prevCpuSeconds;
        } else {
          this.resetLikeDeltaCount += 1;
          this.negativeDeltaEvents.push({
            timestamp,
            elapsedMs,
            pid: proc.pid,
            previousCpuSeconds: prevCpuSeconds,
            currentCpuSeconds: proc.cpuSeconds,
            command: proc.command || "",
          });
          // PID reuse or counter reset: preserve monotonicity without inventing
          // pre-observation CPU that belongs to an earlier process lifetime.
          deltaCpuSeconds = 0;
        }
      }

      this.lastCpuSecondsByPid.set(proc.pid, proc.cpuSeconds);
      sampleDeltaCpuSeconds += deltaCpuSeconds;

      const pidStats = this.pidStats.get(proc.pid) || {
        pid: proc.pid,
        ppid: proc.ppid,
        command: proc.command || "",
        sampleCount: 0,
        firstElapsedMs: elapsedMs,
        lastElapsedMs: elapsedMs,
        cpuSeconds: 0,
        cpuPctSum: 0,
        peakCpuPct: 0,
        rssMbSum: 0,
        peakRssMb: 0,
      };
      const rssMb = proc.rssKb / 1024;
      pidStats.ppid = proc.ppid;
      pidStats.command = proc.command || pidStats.command || "";
      pidStats.sampleCount += 1;
      pidStats.lastElapsedMs = elapsedMs;
      pidStats.cpuSeconds += deltaCpuSeconds;
      pidStats.cpuPctSum += proc.cpuPct;
      pidStats.peakCpuPct = Math.max(pidStats.peakCpuPct, proc.cpuPct);
      pidStats.rssMbSum += rssMb;
      pidStats.peakRssMb = Math.max(pidStats.peakRssMb, rssMb);
      this.pidStats.set(proc.pid, pidStats);
    }

    this.totalCpuSeconds += sampleDeltaCpuSeconds;

    return {
      sampleDeltaCpuSeconds,
      cumulativeCpuSeconds: this.totalCpuSeconds,
      rawSnapshotCpuSeconds,
    };
  }

  getPerPidSummary() {
    return Array.from(this.pidStats.values())
      .map((stats) => ({
        pid: stats.pid,
        ppid: stats.ppid,
        command: stats.command,
        sampleCount: stats.sampleCount,
        wallTimeSec:
          stats.sampleCount > 1
            ? (stats.lastElapsedMs - stats.firstElapsedMs) / 1000
            : 0,
        cpuSeconds: stats.cpuSeconds,
        meanCpuPct: stats.sampleCount > 0 ? stats.cpuPctSum / stats.sampleCount : 0,
        peakCpuPct: stats.peakCpuPct,
        meanRssMb: stats.sampleCount > 0 ? stats.rssMbSum / stats.sampleCount : 0,
        peakRssMb: stats.peakRssMb,
      }))
      .sort((left, right) => right.cpuSeconds - left.cpuSeconds);
  }
}

function collectTreeMetrics(rootPids, tracker, timestamp, elapsedMs) {
  const snapshot = readPsSnapshot();
  const treeState = collectProcessTree(snapshot, rootPids);
  if (!treeState) {
    return null;
  }

  const accounting = tracker.update(treeState.tree, timestamp, elapsedMs);
  const totalCpuPct = treeState.tree.reduce((sum, proc) => sum + proc.cpuPct, 0);
  const totalRssKb = treeState.tree.reduce((sum, proc) => sum + proc.rssKb, 0);

  return {
    rootPids: treeState.rootPids,
    tree: treeState.tree,
    processCount: treeState.tree.length,
    totalCpuPct,
    totalRssMb: totalRssKb / 1024,
    peakRssMb: treeState.tree.reduce((max, proc) => Math.max(max, proc.rssKb / 1024), 0),
    treeCpuSeconds: accounting.cumulativeCpuSeconds,
    treeCpuSecondsDelta: accounting.sampleDeltaCpuSeconds,
    treeCpuSecondsRawSnapshot: accounting.rawSnapshotCpuSeconds,
  };
}

function summarizeResourceSamples(samples) {
  const sampleCount = samples.length;
  return {
    sampleCount,
    meanCpuPct:
      sampleCount > 0
        ? samples.reduce((sum, sample) => sum + sample.totalCpuPct, 0) / sampleCount
        : 0,
    peakCpuPct:
      sampleCount > 0 ? Math.max(...samples.map((sample) => sample.totalCpuPct)) : 0,
    meanRssMb:
      sampleCount > 0
        ? samples.reduce((sum, sample) => sum + sample.totalRssMb, 0) / sampleCount
        : 0,
    peakRssMb:
      sampleCount > 0 ? Math.max(...samples.map((sample) => sample.totalRssMb)) : 0,
    peakProcessCount:
      sampleCount > 0 ? Math.max(...samples.map((sample) => sample.processCount)) : 0,
    cpuSeconds:
      sampleCount > 0 ? samples[samples.length - 1].treeCpuSeconds : 0,
  };
}

function computeCpuSecondsFromLegacyTreeRows(rows, options = {}) {
  const timestampKey = options.timestampKey || "timestamp";
  const cpuKey = options.cpuKey || "tree_cpu_seconds";
  const sortedRows = [...rows]
    .map((row) => ({
      timestamp: Number(row[timestampKey]),
      cpuSeconds: Number(row[cpuKey]),
      processCount: Number(row.process_count),
    }))
    .filter((row) => Number.isFinite(row.timestamp) && Number.isFinite(row.cpuSeconds))
    .sort((left, right) => left.timestamp - right.timestamp);

  let cumulativeCpuSeconds = 0;
  let negativeDeltaCount = 0;
  const negativeDeltaEvents = [];
  let previous = null;

  for (const row of sortedRows) {
    if (previous) {
      const delta = row.cpuSeconds - previous.cpuSeconds;
      if (delta >= 0) {
        cumulativeCpuSeconds += delta;
      } else {
        negativeDeltaCount += 1;
        negativeDeltaEvents.push({
          timestamp: row.timestamp,
          previousCpuSeconds: previous.cpuSeconds,
          currentCpuSeconds: row.cpuSeconds,
          processCount: row.processCount,
        });
      }
    }
    previous = row;
  }

  return {
    rowCount: sortedRows.length,
    cumulativeCpuSeconds,
    negativeDeltaCount,
    negativeDeltaEvents,
    rawStartCpuSeconds: sortedRows[0]?.cpuSeconds ?? 0,
    rawEndCpuSeconds: sortedRows[sortedRows.length - 1]?.cpuSeconds ?? 0,
    rawPeakCpuSeconds:
      sortedRows.length > 0 ? Math.max(...sortedRows.map((row) => row.cpuSeconds)) : 0,
  };
}

module.exports = {
  ProcessTreeTracker,
  collectTreeMetrics,
  computeCpuSecondsFromLegacyTreeRows,
  parseCpuTime,
  parsePsSnapshot,
  readPsSnapshot,
  summarizeResourceSamples,
};
