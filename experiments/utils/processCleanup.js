const fs = require("fs");
const { spawnSync } = require("child_process");

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) {
    return false;
  }

  try {
    process.kill(pid, 0);
    return true;
  } catch (_) {
    return false;
  }
}

function isProcessGroupAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0 || process.platform === "win32") {
    return false;
  }

  try {
    process.kill(-pid, 0);
    return true;
  } catch (_) {
    return false;
  }
}

function signalProcessTree(childOrPid, signal) {
  const pid = typeof childOrPid === "number" ? childOrPid : childOrPid?.pid;
  if (!Number.isFinite(pid) || pid <= 0) {
    return false;
  }

  if (process.platform !== "win32") {
    try {
      process.kill(-pid, signal);
      return true;
    } catch (_) {
      // Fall through to a direct signal if the process group is unavailable.
    }
  }

  try {
    process.kill(pid, signal);
    return true;
  } catch (_) {
    return false;
  }
}

function waitForChildClose(child, timeoutMs) {
  if (!child || child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve(true);
  }

  return new Promise((resolve) => {
    let settled = false;

    const settle = (closed) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      child.removeListener("close", onClose);
      resolve(closed);
    };

    const onClose = () => settle(true);
    const timer = setTimeout(() => settle(false), timeoutMs);
    child.once("close", onClose);
  });
}

async function terminateChildProcessTree(
  child,
  {
    name = "process",
    logger = console.log,
    termWaitMs = 2000,
    killWaitMs = 2000,
  } = {},
) {
  if (!child) {
    return {
      name,
      pid: null,
      sentSigterm: false,
      sentSigkill: false,
      exitedCleanly: true,
    };
  }

  const pid = child.pid ?? null;
  const alreadyExited = child.exitCode !== null || child.signalCode !== null;

  let sentSigterm = false;
  let sentSigkill = false;

  if (pid !== null) {
    sentSigterm = signalProcessTree(child, "SIGTERM");
  }

  if (logger) {
    logger(
      `[cleanup] ${name}: pid=${pid ?? "n/a"} SIGTERM=${sentSigterm ? "sent" : "skipped"}`,
    );
  }

  if (!alreadyExited) {
    const terminatedAfterSigterm = await waitForChildClose(child, termWaitMs);

    if (!terminatedAfterSigterm && pid !== null && isAlive(pid)) {
      sentSigkill = signalProcessTree(child, "SIGKILL");
      if (logger) {
        logger(
          `[cleanup] ${name}: pid=${pid} SIGKILL=${sentSigkill ? "sent" : "skipped"}`,
        );
      }
      await waitForChildClose(child, killWaitMs);
    }
  } else if (pid !== null && isProcessGroupAlive(pid)) {
    await delay(termWaitMs);
    if (isProcessGroupAlive(pid)) {
      sentSigkill = signalProcessTree(child, "SIGKILL");
      if (logger) {
        logger(
          `[cleanup] ${name}: pid=${pid} SIGKILL=${sentSigkill ? "sent" : "skipped"}`,
        );
      }
      await delay(killWaitMs);
    }
  }

  const exitedCleanly = child.exitCode === 0;

  if (logger) {
    logger(
      `[cleanup] ${name}: pid=${pid ?? "n/a"} exitedCleanly=${exitedCleanly} exitCode=${child.exitCode ?? "null"} signalCode=${child.signalCode ?? "null"}`,
    );
  }

  return {
    name,
    pid,
    sentSigterm,
    sentSigkill,
    exitedCleanly,
    exitCode: child.exitCode,
    signalCode: child.signalCode,
  };
}

function killProcessesMatching(patterns, logger = console.log) {
  for (const pattern of patterns) {
    const result = spawnSync("pkill", ["-f", pattern], { stdio: "ignore" });
    const matched = result.status === 0;
    if (logger) {
      logger(
        `[cleanup] stale-pattern pattern=${pattern} matched=${matched ? "yes" : "no"}`,
      );
    }
  }
}

function killPortOwners(port, logger = console.log) {
  const result = spawnSync("lsof", [`-ti:${port}`], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  const pids = (result.stdout || "")
    .split(/\s+/)
    .map((entry) => Number.parseInt(entry, 10))
    .filter((pid) => Number.isFinite(pid) && pid > 0);

  for (const pid of pids) {
    try {
      process.kill(pid, "SIGKILL");
      if (logger) {
        logger(`[cleanup] stale-port port=${port} pid=${pid} SIGKILL=sent`);
      }
    } catch (_) {
      if (logger) {
        logger(`[cleanup] stale-port port=${port} pid=${pid} SIGKILL=skipped`);
      }
    }
  }
}

async function cleanupStaleBenchmarkProcesses({
  logger = console.log,
  quiescenceMs = 750,
} = {}) {
  killProcessesMatching(
    [
      "dist/approaches/StreamingQueryFetchingClientSideApproachOrchestrator\\.js",
      "dist/approaches/StreamingQueryApproximationApproachOrchestrator\\.js",
      "dist/approaches/StreamingQueryChunkedApproachOrchestrator\\.js",
      "dist/approaches/StreamingQueryNaiveDistributedApproachOrchestrator\\.js",
      "dist/approaches/ScalabilitySameQueryDifferentWindowsApproximationOrchestrator\\.js",
      "dist/approaches/ScalabilitySameQueryDifferentWindowsChunkedOrchestrator\\.js",
      "dist/approaches/ScalabilitySameQueryDifferentWindowsNaiveDistributedOrchestrator\\.js",
      "dist/approaches/ScalabilitySameQueryDifferentWindowsFetchingOracleOrchestrator\\.js",
      "dist/approaches/FetchingAllDataClientSide\\.js",
      "dist/approaches/ApproximationApproachOrchestrator\\.js",
      "dist/approaches/StreamingQueryHiveApproachOrchestrator\\.js",
      "dist/approaches/monitoring/FetchingDataClientSideParentProcess\\.js",
      "dist/approaches/monitoring/ApproximationApproachParentProcess\\.js",
      "dist/approaches/monitoring/StreamingQueryHiveParentProcess\\.js",
      "dist/streamer/src/publish\\.js",
      "dist/streamer/src/publishSmartphoneOnly\\.js",
      "dist/streamer/src/publishing/StreamToMQTT\\.js",
    ],
    logger,
  );

  killPortOwners(8080, logger);
  await delay(quiescenceMs);
}

module.exports = {
  cleanupStaleBenchmarkProcesses,
  delay,
  isAlive,
  isProcessGroupAlive,
  signalProcessTree,
  terminateChildProcessTree,
  waitForChildClose,
};
