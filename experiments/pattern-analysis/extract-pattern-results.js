#!/usr/bin/env node

/**
 * Extract Pattern Results from Logs
 *
 * Extracts query results and metadata from pattern comparison experiment logs.
 * Supports fetching, approximation, and chunked approaches.
 *
 * Usage:
 *   node extract-pattern-results.js <approach> <pattern_name> <log_dir>
 *
 * Example:
 *   node extract-pattern-results.js fetching exponential_growth_rate_1 ./logs/pattern-comparison/fetching/exponential_growth_rate_1/iteration1
 */

const fs = require("fs");
const path = require("path");
const {
  getReplayMetadata,
} = require("../utils/benchmarkResultMetadata");

class PatternResultExtractor {
  constructor(approach, patternName, logDir) {
    this.approach = approach;
    this.patternName = patternName;
    this.logDir = logDir;

    // Determine log file names based on approach
    const logFileMap = {
      fetching: "fetching_client_side_log.csv",
      approximation: "approximation_approach_log.csv",
      chunked: "streaming_query_chunk_aggregator_log.csv",
    };

    const latencyLogMap = {
      fetching: "fetching_latency_log.csv",
      approximation: "approximation_latency_log.csv",
      chunked: "chunked_latency_log.csv",
    };

    this.logFile = path.join(logDir, logFileMap[approach]);
    this.latencyFile = path.join(logDir, latencyLogMap[approach]);
    this.outputFile = path.join(logDir, `${approach}_results.csv`);
    this.metadataFile = path.join(logDir, `${approach}_metadata.json`);
    this.diagnosticsFile = approach === "fetching"
      ? path.join(logDir, "fetching_window_diagnostics.csv")
      : approach === "chunked"
        ? path.join(logDir, "chunked_window_diagnostics.csv")
        : null;
  }

  readAttemptMetadata() {
    const filePath = path.join(this.logDir, "attempt_metadata.json");
    if (!fs.existsSync(filePath)) {
      return null;
    }

    try {
      return JSON.parse(fs.readFileSync(filePath, "utf8"));
    } catch {
      return null;
    }
  }

  countContaminationMarkers() {
    const contaminationLogFile = this.approach === "fetching"
      ? path.join(this.logDir, "fetching_orchestrator.log")
      : this.logFile;

    if (!fs.existsSync(contaminationLogFile)) {
      return 0;
    }
    const content = fs.readFileSync(contaminationLogFile, "utf8");
    return (content.match(/Rejected contaminated timestamp|\[CONTAMINATION\]/g) || [])
      .length;
  }

  parseCsvLine(line) {
    const values = [];
    let current = "";
    let inQuotes = false;

    for (let index = 0; index < line.length; index += 1) {
      const char = line[index];
      if (char === "\"") {
        if (inQuotes && line[index + 1] === "\"") {
          current += "\"";
          index += 1;
          continue;
        }
        inQuotes = !inQuotes;
        continue;
      }

      if (char === "," && !inQuotes) {
        values.push(current);
        current = "";
        continue;
      }

      current += char;
    }

    values.push(current);
    return values;
  }

  getHeaderIndex(headerIndex, candidates) {
    for (const candidate of candidates) {
      if (headerIndex.has(candidate)) {
        return headerIndex.get(candidate);
      }
    }
    return undefined;
  }

  buildApproximationResultsFromLatency(latencyContent, diagnosticsByWindow, queryRegisteredTimeRef) {
    const latencyLines = latencyContent.trim().split("\n");
    const headers = latencyLines[0]?.split(",").map((header) => header.trim()) || [];
    const headerIndex = new Map(headers.map((header, index) => [header, index]));
    const windowNumberIndex = this.getHeaderIndex(headerIndex, ["window_number"]);
    const registeredAtIndex = this.getHeaderIndex(headerIndex, ["query_registered_at"]);
    const timestampIndex = this.getHeaderIndex(headerIndex, ["result_emitted_at"]);
    const resultValueIndex = this.getHeaderIndex(headerIndex, ["result_value"]);
    const approximationStatusIndex = this.getHeaderIndex(headerIndex, ["approximation_status"]);

    if (
      windowNumberIndex === undefined ||
      timestampIndex === undefined ||
      resultValueIndex === undefined
    ) {
      return {
        parsed: false,
        queryRegisteredTime: queryRegisteredTimeRef ?? null,
        firstResultTime: null,
        results: [],
      };
    }

    let queryRegisteredTime = queryRegisteredTimeRef ?? null;
    let firstResultTime = null;
    const results = [];

    for (const line of latencyLines.slice(1)) {
      if (!line.trim()) continue;
      const parts = this.parseCsvLine(line);
      const requiredColumnCount = Math.max(
        windowNumberIndex,
        timestampIndex,
        resultValueIndex,
      );
      if (parts.length <= requiredColumnCount) {
        continue;
      }

      const approximationStatus = approximationStatusIndex !== undefined &&
        parts.length > approximationStatusIndex
        ? String(parts[approximationStatusIndex] || "").trim()
        : "";
      if (
        approximationStatus &&
        approximationStatus !== "completed_window_approximation"
      ) {
        continue;
      }

      const windowNumber = parseInt(parts[windowNumberIndex], 10);
      const timestamp = parseInt(parts[timestampIndex], 10);
      const rawResultValue = parts[resultValueIndex];
      const resultValue = parseFloat(rawResultValue);

      if (!Number.isFinite(windowNumber) || !Number.isFinite(resultValue)) {
        continue;
      }

      if (
        !Number.isFinite(queryRegisteredTime) &&
        registeredAtIndex !== undefined &&
        parts.length > registeredAtIndex
      ) {
        const registeredAt = parseInt(parts[registeredAtIndex], 10);
        if (Number.isFinite(registeredAt)) {
          queryRegisteredTime = registeredAt;
          console.log(`Found query registration time from latency log: ${queryRegisteredTime}`);
        }
      }

      if (!Number.isFinite(firstResultTime) && Number.isFinite(timestamp)) {
        firstResultTime = timestamp;
        console.log(`Found first result time from latency log: ${firstResultTime}`);
      }

      results.push({
        timestamp: Number.isFinite(timestamp) ? timestamp : Date.now(),
        resultValue,
        windowNumber,
        ...diagnosticsByWindow.get(windowNumber),
      });

      console.log(
        `  Window ${windowNumber}: ${resultValue.toFixed(6)} at ${Number.isFinite(timestamp) ? timestamp : "N/A"}`,
      );
    }

    return {
      parsed: results.length > 0,
      queryRegisteredTime,
      firstResultTime,
      results,
    };
  }

  buildFetchingLatencySummary(logContent, queryRegisteredTime, finalizedTimestamps) {
    const lines = logContent.split("\n");
    let parentFirstTriggerTime = null;
    let firstFinalizedComparableTime = null;
    const acceptedTimestamps = [];

    for (const line of lines) {
      const rawMatch = line.match(/RStream result generated:\s*[-\d.]+\s+at timestamp:\s*(\d+)/);
      if (rawMatch) {
        const timestamp = Number.parseInt(rawMatch[1], 10);
        if (Number.isFinite(timestamp) && !Number.isFinite(parentFirstTriggerTime)) {
          parentFirstTriggerTime = timestamp;
        }
      }

      const acceptedMatch = line.match(/LOG:\s*(\d+)\s*-\s*Accepted\/finalized:/);
      if (acceptedMatch) {
        const timestamp = Number.parseInt(acceptedMatch[1], 10);
        if (Number.isFinite(timestamp)) {
          acceptedTimestamps.push(timestamp);
          if (!Number.isFinite(firstFinalizedComparableTime)) {
            firstFinalizedComparableTime = timestamp;
          }
        }
      }
    }

    const finalizedComparableTimestamps = acceptedTimestamps.length > 0
      ? acceptedTimestamps
      : finalizedTimestamps;

    const steadyStateOutputIntervalMs = acceptedTimestamps.length > 1
      ? acceptedTimestamps
          .slice(1)
          .map((timestamp, index) => timestamp - acceptedTimestamps[index])
          .reduce((sum, value) => sum + value, 0) / (acceptedTimestamps.length - 1)
      : finalizedComparableTimestamps.length > 1
        ? finalizedComparableTimestamps
            .slice(1)
            .map((timestamp, index) => timestamp - finalizedComparableTimestamps[index])
            .reduce((sum, value) => sum + value, 0) / (finalizedComparableTimestamps.length - 1)
      : null;

    return {
      queryRegisteredTime,
      parentFirstTriggerTime,
      firstFinalizedComparableTime: firstFinalizedComparableTime ?? finalizedComparableTimestamps[0] ?? null,
      steadyStateOutputIntervalMs,
      finalizedTimestamps: finalizedComparableTimestamps,
    };
  }

  buildChunkedLatencySummary(logContent, latencyContent) {
    const lines = logContent.split("\n");
    const latencyLines = latencyContent.trim().split("\n");
    const headers = latencyLines[0]?.split(",").map((header) => header.trim()) || [];
    const headerIndex = new Map(headers.map((header, index) => [header, index]));
    const windowNumberIndex = this.getHeaderIndex(headerIndex, ["window_number"]);
    const registeredAtIndex = this.getHeaderIndex(headerIndex, ["query_registered_at"]);
    const firstDataReceivedIndex = this.getHeaderIndex(headerIndex, ["first_data_received_at"]);
    const resultEmittedIndex = this.getHeaderIndex(headerIndex, ["result_emitted_at"]);

    let queryRegisteredTime = null;
    let firstInternalChunkAvailableTime = null;
    let firstFinalizedComparableAvailableTime = null;
    let firstFinalizedComparablePublishedTime = null;
    const publishedTimestamps = [];

    if (latencyLines.length > 1) {
      const firstDataParts = this.parseCsvLine(latencyLines[1]);
      if (firstDataParts.length > registeredAtIndex) {
        const registeredAt = Number.parseInt(firstDataParts[registeredAtIndex], 10);
        if (Number.isFinite(registeredAt)) {
          queryRegisteredTime = registeredAt;
        }
      }
      if (firstDataParts.length > firstDataReceivedIndex) {
        const firstDataReceivedAt = Number.parseInt(firstDataParts[firstDataReceivedIndex], 10);
        if (Number.isFinite(firstDataReceivedAt)) {
          firstInternalChunkAvailableTime = firstDataReceivedAt;
        }
      }
      if (firstDataParts.length > resultEmittedIndex) {
        const firstPublishedAt = Number.parseInt(firstDataParts[resultEmittedIndex], 10);
        if (Number.isFinite(firstPublishedAt)) {
          firstFinalizedComparablePublishedTime = firstPublishedAt;
          publishedTimestamps.push(firstPublishedAt);
        }
      }
    }

    for (const line of lines) {
      const availableMatch = line.match(/^(\d+),"Chunked window diagnostics:/);
      if (availableMatch) {
        const timestamp = Number.parseInt(availableMatch[1], 10);
        if (Number.isFinite(timestamp) && !Number.isFinite(firstFinalizedComparableAvailableTime)) {
          firstFinalizedComparableAvailableTime = timestamp;
        }
      }

      const publishMatch = line.match(/^(\d+),"Output query event published to topic output"/);
      if (publishMatch) {
        const timestamp = Number.parseInt(publishMatch[1], 10);
        if (Number.isFinite(timestamp)) {
          publishedTimestamps.push(timestamp);
          if (!Number.isFinite(firstFinalizedComparablePublishedTime)) {
            firstFinalizedComparablePublishedTime = timestamp;
          }
        }
      }
    }

    const parentPartialLatencyPath = path.join(this.logDir, "chunked_parent_partial_latency_log.csv");
    let parentPartialAvailableTime = null;
    if (fs.existsSync(parentPartialLatencyPath)) {
      const partialLines = fs.readFileSync(parentPartialLatencyPath, "utf8").trim().split("\n");
      if (partialLines.length > 1) {
        const parts = this.parseCsvLine(partialLines[1]);
        const partialHeaders = partialLines[0]?.split(",").map((header) => header.trim()) || [];
        const partialHeaderIndex = new Map(partialHeaders.map((header, index) => [header, index]));
        const emittedAtIndex = this.getHeaderIndex(partialHeaderIndex, ["emitted_at_ms"]);
        if (parts.length > emittedAtIndex) {
          const emittedAt = Number.parseInt(parts[emittedAtIndex], 10);
          if (Number.isFinite(emittedAt)) {
            parentPartialAvailableTime = emittedAt;
          }
        }
      }
    }

    const steadyStateOutputIntervalMs = publishedTimestamps.length > 1
      ? publishedTimestamps
          .slice(1)
          .map((timestamp, index) => timestamp - publishedTimestamps[index])
          .reduce((sum, value) => sum + value, 0) / (publishedTimestamps.length - 1)
      : null;

    return {
      queryRegisteredTime,
      firstInternalChunkAvailableTime,
      parentPartialAvailableTime,
      firstFinalizedComparableAvailableTime,
      firstFinalizedComparablePublishedTime: firstFinalizedComparablePublishedTime,
      finalizedPublicationDelayMs: (
        Number.isFinite(firstFinalizedComparableAvailableTime) &&
        Number.isFinite(firstFinalizedComparablePublishedTime)
      )
        ? firstFinalizedComparablePublishedTime - firstFinalizedComparableAvailableTime
        : null,
      steadyStateOutputIntervalMs,
    };
  }

  readFetchingFinalizedRows() {
    if (
      !this.diagnosticsFile ||
      !fs.existsSync(this.diagnosticsFile) ||
      !fs.statSync(this.diagnosticsFile).isFile()
    ) {
      return [];
    }

    const content = fs.readFileSync(this.diagnosticsFile, "utf8").trim();
    if (!content) {
      return [];
    }

    const lines = content.split("\n");
    const headers = this.parseCsvLine(lines[0]).map((header) => header.trim());
    const headerIndex = new Map(headers.map((header, index) => [header, index]));

    const windowNumberIndex = this.getHeaderIndex(headerIndex, ["window_number"]);
    const windowStartIndex = this.getHeaderIndex(headerIndex, ["window_start"]);
    const windowEndIndex = this.getHeaderIndex(headerIndex, ["window_end"]);
    const eventCountIndex = this.getHeaderIndex(headerIndex, ["event_count"]);
    const expectedEventCountIndex = this.getHeaderIndex(headerIndex, ["expected_event_count"]);
    const sumIndex = this.getHeaderIndex(headerIndex, ["sum"]);
    const avgIndex = this.getHeaderIndex(headerIndex, ["avg"]);
    const firstEventTimestampIndex = this.getHeaderIndex(headerIndex, ["first_event_timestamp"]);
    const lastEventTimestampIndex = this.getHeaderIndex(headerIndex, ["last_event_timestamp"]);
    const completenessStatusIndex = this.getHeaderIndex(headerIndex, ["completeness_status"]);
    const acceptedOrSuppressedIndex = this.getHeaderIndex(headerIndex, ["accepted_or_suppressed"]);
    const reasonIndex = this.getHeaderIndex(headerIndex, ["reason"]);
    const resultValueIndex = this.getHeaderIndex(headerIndex, ["result_value", "avg"]);

    const finalizedRows = [];

    for (const line of lines.slice(1)) {
      if (!line.trim()) continue;
      const parts = this.parseCsvLine(line);
      if (parts.length <= windowNumberIndex) continue;

      const windowNumber = parseInt(parts[windowNumberIndex], 10);
      if (!Number.isFinite(windowNumber)) continue;

      const acceptedOrSuppressed = acceptedOrSuppressedIndex !== undefined
        ? (parts[acceptedOrSuppressedIndex] || "")
        : "";
      const completenessStatus = completenessStatusIndex !== undefined
        ? (parts[completenessStatusIndex] || "")
        : "";
      const reason = reasonIndex !== undefined
        ? (parts[reasonIndex] || "")
        : "";
      const isFinalized = acceptedOrSuppressed === "accepted" ||
        completenessStatus === "complete" ||
        reason === "finalized_settled_window";

      if (!isFinalized) {
        continue;
      }

      const windowStart = windowStartIndex !== undefined && parts.length > windowStartIndex
        ? parseInt(parts[windowStartIndex], 10)
        : null;
      const windowEnd = windowEndIndex !== undefined && parts.length > windowEndIndex
        ? parseInt(parts[windowEndIndex], 10)
        : null;
      const rawResultValue = resultValueIndex !== undefined && parts.length > resultValueIndex
        ? parts[resultValueIndex]
        : null;
      const resultValue = parseFloat(rawResultValue);

      if (!Number.isFinite(resultValue)) {
        continue;
      }

      finalizedRows.push({
        windowNumber,
        windowStart: Number.isFinite(windowStart) ? windowStart : null,
        windowEnd: Number.isFinite(windowEnd) ? windowEnd : null,
        eventCount: eventCountIndex !== undefined && parts.length > eventCountIndex
          ? parseFloat(parts[eventCountIndex])
          : null,
        expectedEventCount: expectedEventCountIndex !== undefined && parts.length > expectedEventCountIndex
          ? parseFloat(parts[expectedEventCountIndex])
          : null,
        sumValue: sumIndex !== undefined && parts.length > sumIndex
          ? parseFloat(parts[sumIndex])
          : null,
        avgValue: avgIndex !== undefined && parts.length > avgIndex
          ? parseFloat(parts[avgIndex])
          : null,
        firstEventTimestamp: firstEventTimestampIndex !== undefined && parts.length > firstEventTimestampIndex
          ? parts[firstEventTimestampIndex]
          : null,
        lastEventTimestamp: lastEventTimestampIndex !== undefined && parts.length > lastEventTimestampIndex
          ? parts[lastEventTimestampIndex]
          : null,
        completenessStatus,
        acceptedOrSuppressed,
        reason,
        resultValue,
      });
    }

    finalizedRows.sort((a, b) => a.windowNumber - b.windowNumber);
    return finalizedRows;
  }

  readFetchingLatencyRows() {
    if (
      !this.latencyFile ||
      !fs.existsSync(this.latencyFile) ||
      !fs.statSync(this.latencyFile).isFile()
    ) {
      return new Map();
    }

    const content = fs.readFileSync(this.latencyFile, "utf8").trim();
    if (!content) {
      return new Map();
    }

    const lines = content.split("\n");
    const headers = this.parseCsvLine(lines[0]).map((header) => header.trim());
    const headerIndex = new Map(headers.map((header, index) => [header, index]));
    const windowNumberIndex = this.getHeaderIndex(headerIndex, ["window_number"]);
    const queryRegisteredAtIndex = this.getHeaderIndex(headerIndex, ["query_registered_at"]);
    const firstDataReceivedIndex = this.getHeaderIndex(headerIndex, ["first_data_received_at"]);
    const expectedWindowCloseIndex = this.getHeaderIndex(headerIndex, ["expected_window_close"]);
    const lastObsReceivedIndex = this.getHeaderIndex(headerIndex, ["last_obs_received_at"]);
    const resultEmittedIndex = this.getHeaderIndex(headerIndex, ["result_emitted_at"]);
    const resultValueIndex = this.getHeaderIndex(headerIndex, ["result_value"]);

    const rows = new Map();

    for (const line of lines.slice(1)) {
      if (!line.trim()) continue;
      const parts = this.parseCsvLine(line);
      if (parts.length <= windowNumberIndex) continue;

      const windowNumber = parseInt(parts[windowNumberIndex], 10);
      if (!Number.isFinite(windowNumber)) continue;

      rows.set(windowNumber, {
        queryRegisteredAt: queryRegisteredAtIndex !== undefined && parts.length > queryRegisteredAtIndex
          ? parseInt(parts[queryRegisteredAtIndex], 10)
          : null,
        firstDataReceivedAt: firstDataReceivedIndex !== undefined && parts.length > firstDataReceivedIndex
          ? parseInt(parts[firstDataReceivedIndex], 10)
          : null,
        expectedWindowClose: expectedWindowCloseIndex !== undefined && parts.length > expectedWindowCloseIndex
          ? parseInt(parts[expectedWindowCloseIndex], 10)
          : null,
        lastObsReceivedAt: lastObsReceivedIndex !== undefined && parts.length > lastObsReceivedIndex
          ? parseInt(parts[lastObsReceivedIndex], 10)
          : null,
        resultEmittedAt: resultEmittedIndex !== undefined && parts.length > resultEmittedIndex
          ? parseInt(parts[resultEmittedIndex], 10)
          : null,
        resultValue: resultValueIndex !== undefined && parts.length > resultValueIndex
          ? parseFloat(parts[resultValueIndex])
          : null,
      });
    }

    return rows;
  }

  readFetchingEmissionTimestamps(logContent) {
    if (!logContent) {
      return [];
    }

    const timestamps = [];
    for (const line of logContent.split("\n")) {
      const match = line.match(/^(\d+),"RStream result generated:/);
      if (!match) continue;
      const timestamp = parseInt(match[1], 10);
      if (Number.isFinite(timestamp)) {
        timestamps.push(timestamp);
      }
    }

    return timestamps;
  }

  readDiagnosticsByWindow() {
    if (
      !this.diagnosticsFile ||
      !fs.existsSync(this.diagnosticsFile) ||
      !fs.statSync(this.diagnosticsFile).isFile()
    ) {
      return new Map();
    }

    const content = fs.readFileSync(this.diagnosticsFile, "utf8").trim();
    if (!content) {
      return new Map();
    }

    const lines = content.split("\n");
    const headers = lines[0].split(",");
    const rows = new Map();

    if (this.approach === "fetching") {
      const idx = new Map(headers.map((header, i) => [header.trim(), i]));
      for (const line of lines.slice(1)) {
        if (!line.trim()) continue;
        const parts = line.split(",");
        const windowNumber = parseInt(parts[idx.get("window_number")], 10);
        if (!Number.isFinite(windowNumber)) continue;
        rows.set(windowNumber, {
          windowStart: parseInt(parts[idx.get("window_start")], 10),
          windowEnd: parseInt(parts[idx.get("window_end")], 10),
          eventCount: parseFloat(parts[idx.get("event_count")]),
          expectedEventCount: parseFloat(parts[idx.get("expected_event_count")]),
          sumValue: parseFloat(parts[idx.get("sum")]),
          avgValue: parseFloat(parts[idx.get("avg")]),
          firstEventTimestamp: parts[idx.get("first_event_timestamp")],
          lastEventTimestamp: parts[idx.get("last_event_timestamp")],
          completenessStatus: parts[idx.get("completeness_status")],
          acceptedOrSuppressed: parts[idx.get("accepted_or_suppressed")],
          reason: parts[idx.get("reason")],
          resultValue: parseFloat(parts[idx.get("result_value")]),
        });
      }
      return rows;
    }

    const csvParseLine = (line) => {
      const matches = line.match(/("([^"]|"")*"|[^,]+)/g) || [];
      return matches.map((entry) =>
        entry.startsWith("\"") && entry.endsWith("\"")
          ? entry.slice(1, -1).replace(/""/g, "\"")
          : entry,
      );
    };

    const idx = new Map(headers.map((header, i) => [header.trim(), i]));
    for (const line of lines.slice(1)) {
      if (!line.trim()) continue;
      const parts = csvParseLine(line);
      const windowNumber = parseInt(parts[idx.get("external_window_number")], 10);
      if (!Number.isFinite(windowNumber)) continue;
      rows.set(windowNumber, {
        windowStart: parseInt(parts[idx.get("external_window_start")], 10),
        windowEnd: parseInt(parts[idx.get("external_window_end")], 10),
        recomposedCount: parseFloat(parts[idx.get("recomposed_count")]),
        recomposedSum: parseFloat(parts[idx.get("recomposed_sum")]),
        recomposedAvg: parseFloat(parts[idx.get("recomposed_avg")]),
        internalChunkIds: parts[idx.get("internal_chunk_ids")],
        internalChunksJson: parts[idx.get("internal_chunks_json")],
      });
    }
    return rows;
  }

  extractResults() {
    const fetchingFinalizedFile = this.approach === "fetching" && this.diagnosticsFile
      && fs.existsSync(this.diagnosticsFile)
      ? this.diagnosticsFile
      : this.logFile;
    console.log(`Extracting results from: ${fetchingFinalizedFile}`);

    if (!fs.existsSync(fetchingFinalizedFile)) {
      console.error(`Log file not found: ${fetchingFinalizedFile}`);
      return null;
    }

    const content = fs.readFileSync(fetchingFinalizedFile, "utf8");
    const results = [];
    const diagnosticsByWindow = this.readDiagnosticsByWindow();
    const attemptMetadata = this.readAttemptMetadata();
    const benchmarkEventAnchor = Number.parseInt(
      attemptMetadata?.benchmark_event_time_anchor ?? "",
      10,
    );
    const outputWindowRange = Number.parseInt(
      attemptMetadata?.output_window_range ?? attemptMetadata?.window_width ?? 120000,
      10,
    );
    const outputWindowStep = Number.parseInt(
      attemptMetadata?.output_window_step ?? attemptMetadata?.window_slide ?? 60000,
      10,
    );
    const deriveApproximationWindowBounds = (windowNumber) => {
      if (!Number.isFinite(benchmarkEventAnchor) || !Number.isFinite(windowNumber)) {
        return { windowStart: null, windowEnd: null };
      }

      const windowStart =
        benchmarkEventAnchor + ((windowNumber - 1) * outputWindowStep);
      return {
        windowStart,
        windowEnd: windowStart + outputWindowRange,
      };
    };
    let firstResultTime = null;
    let queryRegisteredTime = null;
    let latencySummary = null;
    const clientLogContent = this.approach === "fetching" && fs.existsSync(this.logFile)
      ? fs.readFileSync(this.logFile, "utf8")
      : content;

    // Try to extract metadata from latency log first
    if (fs.existsSync(this.latencyFile)) {
      try {
        console.log(`Reading latency log: ${this.latencyFile}`);
        const latencyContent = fs.readFileSync(this.latencyFile, "utf8");
        const latencyLines = latencyContent.trim().split("\n");
        const latencyHeaders = latencyLines[0]?.split(",").map((header) => header.trim()) || [];
        const latencyHeaderIndex = new Map(latencyHeaders.map((header, index) => [header, index]));
        const registeredAtIndex = this.getHeaderIndex(latencyHeaderIndex, ["query_registered_at"]);
        const resultTimeIndex = this.getHeaderIndex(latencyHeaderIndex, ["result_emitted_at"]);

        // Skip header, read first data line
        if (latencyLines.length > 1) {
          const parts = this.parseCsvLine(latencyLines[1]);
          if (registeredAtIndex !== undefined && parts.length > registeredAtIndex) {
            queryRegisteredTime = parseInt(parts[registeredAtIndex], 10);
            console.log(
              `Found query registration time from latency log: ${queryRegisteredTime}`,
            );
          }
          if (resultTimeIndex !== undefined && parts.length > resultTimeIndex) {
            firstResultTime = parseInt(parts[resultTimeIndex], 10);
            console.log(
              `Found first result time from latency log: ${firstResultTime}`,
            );
          }
        }
      } catch (e) {
        console.warn(`Failed to parse latency log: ${e.message}`);
      }
    }

    // Fetching approach
    if (this.approach === "fetching") {
      const latencyRowsByWindow = this.readFetchingLatencyRows();
      const emittedTimestamps = this.readFetchingEmissionTimestamps(clientLogContent);
      const finalizedRows = this.readFetchingFinalizedRows();
      const finalizedRowsFromDiagnosticsMap = finalizedRows.length === 0
        ? Array.from(diagnosticsByWindow.entries())
            .filter(([windowNumber, row]) =>
              Number.isFinite(windowNumber) && Number.isFinite(row?.avgValue),
            )
            .map(([windowNumber, row]) => ({
              windowNumber,
              windowStart: row.windowStart ?? null,
              windowEnd: row.windowEnd ?? null,
              eventCount: row.eventCount ?? null,
              expectedEventCount: row.expectedEventCount ?? null,
              sumValue: row.sumValue ?? null,
              avgValue: row.avgValue ?? null,
              firstEventTimestamp: row.firstEventTimestamp ?? null,
              lastEventTimestamp: row.lastEventTimestamp ?? null,
              completenessStatus: row.completenessStatus ?? null,
              acceptedOrSuppressed: row.acceptedOrSuppressed ?? null,
              reason: row.reason ?? null,
              resultValue: row.resultValue ?? row.avgValue,
            }))
            .sort((a, b) => a.windowNumber - b.windowNumber)
        : [];
      const effectiveFinalizedRows = finalizedRows.length > 0
        ? finalizedRows
        : finalizedRowsFromDiagnosticsMap;

      const firstLatencyRow = Array.from(latencyRowsByWindow.values())
        .find((row) => Number.isFinite(row?.queryRegisteredAt));
      if (Number.isFinite(firstLatencyRow?.queryRegisteredAt)) {
        queryRegisteredTime = firstLatencyRow.queryRegisteredAt;
        console.log(`Found query registration time from latency log: ${queryRegisteredTime}`);
      } else {
        const registrationLine = clientLogContent
          .split("\n")
          .find((line) => line.includes("fetching_query_registered"));
        const registrationMatch = registrationLine?.match(/^(\d+),"fetching_query_registered"$/);
        if (registrationMatch) {
          queryRegisteredTime = parseInt(registrationMatch[1], 10);
          console.log(`Found query registration time: ${queryRegisteredTime}`);
        }
      }

      if (effectiveFinalizedRows.length > 0) {
        effectiveFinalizedRows.forEach((row, index) => {
          const latencyRow = latencyRowsByWindow.get(row.windowNumber);
          const timestamp = Number.isFinite(latencyRow?.resultEmittedAt)
            ? latencyRow.resultEmittedAt
            : emittedTimestamps[index];
          const finalTimestamp = Number.isFinite(timestamp)
            ? timestamp
            : Date.now();

          if (!firstResultTime) {
            firstResultTime = finalTimestamp;
          }

          results.push({
            timestamp: finalTimestamp,
            resultValue: row.resultValue,
            windowNumber: row.windowNumber,
            ...deriveApproximationWindowBounds(row.windowNumber),
            ...row,
          });

          console.log(
            `  Window ${row.windowNumber}: ${row.resultValue.toFixed(6)} at ${finalTimestamp}`,
          );
        });
      } else {
        const lines = clientLogContent.split("\n");
        let registrationLineIndex = -1;
        for (let i = lines.length - 1; i >= 0; i -= 1) {
          if (lines[i].includes("fetching_query_registered")) {
            registrationLineIndex = i;
            break;
          }
        }

        if (registrationLineIndex !== -1) {
          const registrationMatch = lines[registrationLineIndex].match(
            /^(\d+),"fetching_query_registered"$/,
          );
          if (registrationMatch) {
            queryRegisteredTime = parseInt(registrationMatch[1], 10);
            console.log(`Found query registration time: ${queryRegisteredTime}`);
          }
        }

        const finalizedMatches = lines
          .slice(registrationLineIndex === -1 ? 0 : registrationLineIndex + 1)
          .map((line) => {
            const parts = this.parseCsvLine(line);
            if (parts.length < 2) {
              return null;
            }

            const timestamp = Number.parseInt(parts[0], 10);
            const message = parts[1] || "";
            const payloadPrefix = "Accepted/finalized: ";
            if (!message.startsWith(payloadPrefix)) {
              return null;
            }

            const payloadText = message.slice(payloadPrefix.length).trim();
            try {
              return {
                timestamp,
                payload: JSON.parse(payloadText),
              };
            } catch (_) {
              return null;
            }
          })
          .filter(Boolean);

        if (finalizedMatches.length > 0) {
          for (const finalized of finalizedMatches) {
            const timestamp = finalized.timestamp;
            const resultValue = parseFloat(
              finalized.payload.resultValue ??
              finalized.payload.result_value ??
              finalized.payload.avg ??
              finalized.payload.sum ??
              finalized.payload.count ??
              finalized.payload.min ??
              finalized.payload.max
            );
            const windowNumber = parseInt(finalized.payload.window_number, 10);

            if (
              !Number.isFinite(timestamp) ||
              !Number.isFinite(resultValue) ||
              !Number.isFinite(windowNumber)
            ) {
              continue;
            }

            if (!firstResultTime) {
              firstResultTime = timestamp;
            }

            results.push({
              timestamp,
              resultValue,
              windowNumber,
              ...deriveApproximationWindowBounds(windowNumber),
              ...diagnosticsByWindow.get(windowNumber),
            });

            console.log(
              `  Window ${windowNumber}: ${resultValue.toFixed(6)} at ${timestamp}`,
            );
          }
        } else {
          throw new Error(
            "Fetching extraction failed: no accepted/finalized windows found; refusing fallback to raw RStream rows.",
          );
        }
      }

      if (results.length === 0) {
        throw new Error(
          "Fetching extraction failed: no accepted/finalized windows found; refusing fallback to raw RStream rows.",
        );
      }
    }
    // Approximation approach
    else if (this.approach === "approximation") {
      let parsedFromLatency = false;
      if (fs.existsSync(this.latencyFile)) {
        try {
          const latencyContent = fs.readFileSync(this.latencyFile, "utf8");
          const parsedLatency = this.buildApproximationResultsFromLatency(
            latencyContent,
            diagnosticsByWindow,
            queryRegisteredTime,
          );
          parsedFromLatency = parsedLatency.parsed;
          queryRegisteredTime = parsedLatency.queryRegisteredTime ?? queryRegisteredTime;
          firstResultTime = parsedLatency.firstResultTime ?? firstResultTime;
          for (const result of parsedLatency.results) {
            results.push({
              ...result,
              ...deriveApproximationWindowBounds(result.windowNumber),
            });
          }
        } catch (error) {
          console.warn(`Failed to parse approximation latency log: ${error.message}`);
        }
      }

      if (!parsedFromLatency) {
        const lines = content.split("\n");

        // Legacy fallback: scan the orchestration log for published result text.
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i];

          const latencyMatch = line.match(
            /From query registration:\s*(-?\d+)ms\s*\(expected close:\s*(\d+),\s*result:\s*(\d+)\)/,
          );

          if (latencyMatch) {
            const expectedCloseTime = parseInt(latencyMatch[2], 10);
            const resultTimestamp = parseInt(latencyMatch[3], 10);
            const windowRangeMs = 120000;
            const calculatedRegistrationTime = expectedCloseTime - windowRangeMs;

            if (!queryRegisteredTime) {
              queryRegisteredTime = calculatedRegistrationTime;
              console.log(`Found query registration time: ${queryRegisteredTime}`);
            }

            if (!firstResultTime) {
              firstResultTime = resultTimestamp;
              console.log(`Found first result time: ${firstResultTime}`);
            }
          }

          if (line.includes("Successfully published unified cross-sensor")) {
            const match = line.match(
              /Successfully published unified cross-sensor \w+:\s*([\d.]+)/,
            );
            if (match) {
              const resultValue = parseFloat(match[1]);
              let timestamp = firstResultTime || Date.now();

              for (let j = Math.max(0, i - 20); j < i; j++) {
                const tsMatch = lines[j].match(/result:\s*(\d{13})/);
                if (tsMatch) {
                  timestamp = parseInt(tsMatch[1], 10);
                }
              }

              results.push({
                timestamp,
                resultValue,
                windowNumber: results.length + 1,
                ...diagnosticsByWindow.get(results.length + 1),
              });

              console.log(
                `  Window ${results.length}: ${resultValue.toFixed(6)} at ${timestamp}`,
              );
            }
          }
        }

        if (!queryRegisteredTime && results.length > 0 && firstResultTime) {
          queryRegisteredTime = firstResultTime - 60000;
          console.log(`Estimated query registration time: ${queryRegisteredTime}`);
        }
      }
    }
    // Chunked approach
    else if (this.approach === "chunked") {
      const latencyContent = fs.readFileSync(this.latencyFile, "utf8").trim();
      const latencyLines = latencyContent.split("\n");
      const headers = latencyLines[0]?.split(",").map((header) => header.trim()) || [];
      const headerIndex = new Map(
        headers.map((header, index) => [header, index]),
      );
      const windowNumberIndex = this.getHeaderIndex(headerIndex, ["window_number"]);
      const registeredAtIndex = this.getHeaderIndex(headerIndex, ["query_registered_at"]);
      const timestampIndex = this.getHeaderIndex(headerIndex, ["result_emitted_at"]);
      const resultValueIndex = this.getHeaderIndex(headerIndex, ["result_value"]);

      if (
        windowNumberIndex === undefined ||
        registeredAtIndex === undefined ||
        timestampIndex === undefined ||
        resultValueIndex === undefined
      ) {
        throw new Error(
          `Chunked latency log is missing required columns. Required: window_number, query_registered_at, result_emitted_at, result_value. Available headers: ${headers.join(", ") || "(none)"}`,
        );
      }

      const dataLines = latencyLines.slice(1);
      let firstRowIndex = 0;

      for (let i = dataLines.length - 1; i >= 0; i--) {
        const parts = dataLines[i].split(",");
        if (parts.length > windowNumberIndex && parts[windowNumberIndex] === "1") {
          firstRowIndex = i;
          break;
        }
      }

      const selectedLines = dataLines.slice(firstRowIndex);
      if (selectedLines.length > 0) {
        for (let i = 0; i < selectedLines.length; i++) {
          const parts = selectedLines[i].split(",");
          const requiredColumnCount = Math.max(
            windowNumberIndex,
            registeredAtIndex,
            timestampIndex,
            resultValueIndex,
          );
          if (parts.length <= requiredColumnCount) {
            continue;
          }

          const windowNumber = parseInt(parts[windowNumberIndex], 10);
          const registeredAt = parseInt(parts[registeredAtIndex], 10);
          const timestamp = parseInt(parts[timestampIndex], 10);
          const rawResultValue = parts[resultValueIndex];
          const resultValue = parseFloat(rawResultValue);

          if (!Number.isFinite(resultValue)) {
            throw new Error(
              `Chunked latency log has non-numeric result_value "${rawResultValue}" on row ${i + 2}. Available headers: ${headers.join(", ")}`,
            );
          }

          if (!Number.isFinite(windowNumber)) {
            continue;
          }

          if (i === 0 && Number.isFinite(registeredAt)) {
            queryRegisteredTime = registeredAt;
            console.log(
              `Found query registration time from latency log: ${queryRegisteredTime}`,
            );
          }

          if (i === 0 && Number.isFinite(timestamp)) {
            firstResultTime = timestamp;
            console.log(`Found first result time from latency log: ${firstResultTime}`);
          }

          results.push({
            timestamp: Number.isFinite(timestamp) ? timestamp : Date.now(),
            resultValue,
            windowNumber,
            ...deriveApproximationWindowBounds(windowNumber),
            ...diagnosticsByWindow.get(windowNumber),
          });

          console.log(
            `  Window ${windowNumber}: ${resultValue.toFixed(6)} at ${Number.isFinite(timestamp) ? timestamp : "N/A"}`,
          );
        }
      }
    }

    if (this.approach === "fetching") {
      latencySummary = this.buildFetchingLatencySummary(
        clientLogContent,
        queryRegisteredTime,
        results.map((result) => result.timestamp),
      );
      queryRegisteredTime = latencySummary.queryRegisteredTime ?? queryRegisteredTime;
      firstResultTime = latencySummary.firstFinalizedComparableTime ?? firstResultTime;
    } else if (this.approach === "chunked") {
      const chunkedLatencyContent = fs.readFileSync(this.latencyFile, "utf8");
      latencySummary = this.buildChunkedLatencySummary(content, chunkedLatencyContent);
      queryRegisteredTime = latencySummary.queryRegisteredTime ?? queryRegisteredTime;
      firstResultTime = latencySummary.firstFinalizedComparablePublishedTime ?? firstResultTime;
    }

    console.log(`\nExtracted ${results.length} results`);

    if (results.length === 0) {
      console.warn("No results found in log file");
      return null;
    }

    return {
      results,
      queryRegisteredTime,
      firstResultTime,
      latencySummary,
    };
  }

  saveResults(extractedData) {
    if (
      !extractedData ||
      !extractedData.results ||
      extractedData.results.length === 0
    ) {
      console.error("No data to save");
      return false;
    }

    const { results, queryRegisteredTime, firstResultTime, latencySummary } = extractedData;
    const contaminationMarkerCount = this.countContaminationMarkers();
    const attemptMetadata = this.readAttemptMetadata();
    const outputWindowRange = Number.parseInt(
      attemptMetadata?.output_window_range ?? attemptMetadata?.window_width ?? 120000,
      10,
    );
    const outputWindowStep = Number.parseInt(
      attemptMetadata?.output_window_step ?? attemptMetadata?.window_slide ?? 60000,
      10,
    );

    // Write results CSV
    let csvContent =
      "timestamp,window_number,window_start,window_end,result_value,elapsed_since_registration_ms,delay_past_expected_close_ms\n";

    results.forEach((result) => {
      const elapsedSinceRegistrationMs = queryRegisteredTime
        ? result.timestamp - queryRegisteredTime
        : "N/A";
      const expectedWindowClose = queryRegisteredTime && Number.isFinite(result.windowNumber)
        ? queryRegisteredTime + outputWindowRange + ((result.windowNumber - 1) * outputWindowStep)
        : null;
      const delayPastExpectedCloseMs = queryRegisteredTime && Number.isFinite(expectedWindowClose)
        ? result.timestamp - expectedWindowClose
        : "N/A";
      csvContent += `${result.timestamp},${result.windowNumber},${Number.isFinite(result.windowStart) ? result.windowStart : ""},${Number.isFinite(result.windowEnd) ? result.windowEnd : ""},${result.resultValue},${elapsedSinceRegistrationMs},${delayPastExpectedCloseMs}\n`;
    });

    fs.writeFileSync(this.outputFile, csvContent);
    console.log(`\nSaved results to: ${this.outputFile}`);

    // Calculate statistics
    const values = results.map((r) => r.resultValue);
    const avgValue = values.reduce((a, b) => a + b, 0) / values.length;
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);

    const finalizedTimestamps = results
      .map((result) => result.timestamp)
      .filter((timestamp) => Number.isFinite(timestamp));
    const steadyStateOutputIntervalMs = finalizedTimestamps.length > 1
      ? finalizedTimestamps
          .slice(1)
          .map((timestamp, index) => timestamp - finalizedTimestamps[index])
          .reduce((sum, value) => sum + value, 0) / (finalizedTimestamps.length - 1)
      : latencySummary?.steadyStateOutputIntervalMs ?? null;

    const parentFirstTriggerElapsedMs = latencySummary?.parentFirstTriggerTime && queryRegisteredTime
      ? latencySummary.parentFirstTriggerTime - queryRegisteredTime
      : null;
    const firstFinalizedComparableElapsedMs = queryRegisteredTime && firstResultTime
      ? firstResultTime - queryRegisteredTime
      : null;
    const finalizedDelayPastExpectedCloseMs = queryRegisteredTime && firstResultTime
      ? firstResultTime - (queryRegisteredTime + outputWindowRange)
      : null;
    const firstInternalChunkAvailableElapsedMs = latencySummary?.firstInternalChunkAvailableTime && queryRegisteredTime
      ? latencySummary.firstInternalChunkAvailableTime - queryRegisteredTime
      : null;
    const parentPartialAvailableElapsedMs = latencySummary?.parentPartialAvailableTime && queryRegisteredTime
      ? latencySummary.parentPartialAvailableTime - queryRegisteredTime
      : null;
    const firstFinalizedComparableAvailableElapsedMs = latencySummary?.firstFinalizedComparableAvailableTime && queryRegisteredTime
      ? latencySummary.firstFinalizedComparableAvailableTime - queryRegisteredTime
      : firstFinalizedComparableElapsedMs;
    const firstFinalizedComparablePublishedElapsedMs = latencySummary?.firstFinalizedComparablePublishedTime && queryRegisteredTime
      ? latencySummary.firstFinalizedComparablePublishedTime - queryRegisteredTime
      : firstFinalizedComparableElapsedMs;
    const finalizedPublicationDelayMs = (
      Number.isFinite(latencySummary?.firstFinalizedComparableAvailableTime) &&
      Number.isFinite(latencySummary?.firstFinalizedComparablePublishedTime)
    )
      ? latencySummary.firstFinalizedComparablePublishedTime - latencySummary.firstFinalizedComparableAvailableTime
      : null;
    const firstEventLatency = firstFinalizedComparableElapsedMs;

    // Write metadata
    const metadata = {
      approach: this.approach,
      pattern: this.patternName,
      queryRegisteredTime: queryRegisteredTime,
      firstResultTime: firstResultTime,
      totalResults: results.length,
      latencySummary: {
        parent_first_trigger_elapsed_ms: parentFirstTriggerElapsedMs,
        first_finalized_comparable_elapsed_ms: firstFinalizedComparableElapsedMs,
        finalized_delay_past_expected_close_ms: finalizedDelayPastExpectedCloseMs,
        steady_state_output_interval_ms: steadyStateOutputIntervalMs,
        first_internal_chunk_available_elapsed_ms: firstInternalChunkAvailableElapsedMs,
        parent_partial_available_elapsed_ms: parentPartialAvailableElapsedMs,
        first_finalized_comparable_available_elapsed_ms: firstFinalizedComparableAvailableElapsedMs,
        first_finalized_comparable_published_elapsed_ms: firstFinalizedComparablePublishedElapsedMs,
        finalized_publication_delay_ms: finalizedPublicationDelayMs,
        elapsed_since_registration_ms: firstFinalizedComparableElapsedMs,
        delay_past_expected_close_ms: finalizedDelayPastExpectedCloseMs,
      },
      parentFirstTriggerElapsedMs,
      firstInternalChunkAvailableElapsedMs,
      parentPartialAvailableElapsedMs,
      firstFinalizedComparableElapsedMs,
      finalizedDelayPastExpectedCloseMs,
      steadyStateOutputIntervalMs,
      firstFinalizedComparableAvailableElapsedMs,
      firstFinalizedComparablePublishedElapsedMs,
      finalizedPublicationDelayMs,
      elapsedSinceRegistrationMs: firstFinalizedComparableElapsedMs,
      delayPastExpectedCloseMs: finalizedDelayPastExpectedCloseMs,
      firstEventLatency: firstEventLatency,
      firstEventLatencySeconds: firstEventLatency
        ? (firstEventLatency / 1000).toFixed(2)
        : null,
      statistics: {
        avgValue: avgValue,
        minValue: minValue,
        maxValue: maxValue,
        range: maxValue - minValue,
      },
      extractionMethod: "log_parsing",
      extractionDate: new Date().toISOString(),
      benchmark_event_time_anchor:
        attemptMetadata?.benchmark_event_time_anchor ?? null,
      timestamp_domain_min:
        attemptMetadata?.timestamp_domain_min ?? null,
      timestamp_domain_max:
        attemptMetadata?.timestamp_domain_max ?? null,
      contamination_detected: contaminationMarkerCount > 0,
      contamination_marker_count: contaminationMarkerCount,
      window_width:
        attemptMetadata?.output_window_range ?? null,
      window_slide:
        attemptMetadata?.output_window_step ?? null,
      sub_window_range:
        attemptMetadata?.sub_window_range ?? null,
      sub_window_step:
        attemptMetadata?.sub_window_step ?? null,
      attemptMetadata,
      replayMetadata: getReplayMetadata(process.env),
    };

    fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
    console.log(`Saved metadata to: ${this.metadataFile}`);

    // Print summary
    console.log("\n=== Summary ===");
    console.log(`Approach: ${this.approach}`);
    console.log(`Pattern: ${this.patternName}`);
    console.log(
      `Query registered at: ${queryRegisteredTime ? new Date(queryRegisteredTime).toISOString() : "N/A"}`,
    );
    console.log(
      `First finalized comparable result at: ${firstResultTime ? new Date(firstResultTime).toISOString() : "N/A"}`,
    );
    console.log(
      `Parent first trigger elapsed: ${parentFirstTriggerElapsedMs !== null ? `${parentFirstTriggerElapsedMs} ms` : "N/A"}`,
    );
    console.log(
      `First finalized comparable elapsed: ${firstFinalizedComparableElapsedMs !== null ? `${firstFinalizedComparableElapsedMs} ms` : "N/A"}`,
    );
    console.log(
      `Finalized delay past expected close: ${finalizedDelayPastExpectedCloseMs !== null ? `${finalizedDelayPastExpectedCloseMs} ms` : "N/A"}`,
    );
    if (this.approach === "chunked") {
      console.log(
        `First internal chunk available elapsed: ${firstInternalChunkAvailableElapsedMs !== null ? `${firstInternalChunkAvailableElapsedMs} ms` : "N/A"}`,
      );
      console.log(
        `Parent partial available elapsed: ${parentPartialAvailableElapsedMs !== null ? `${parentPartialAvailableElapsedMs} ms` : "N/A"}`,
      );
      console.log(
        `First finalized comparable available elapsed: ${firstFinalizedComparableAvailableElapsedMs !== null ? `${firstFinalizedComparableAvailableElapsedMs} ms` : "N/A"}`,
      );
      console.log(
        `First finalized comparable published elapsed: ${firstFinalizedComparablePublishedElapsedMs !== null ? `${firstFinalizedComparablePublishedElapsedMs} ms` : "N/A"}`,
      );
      console.log(
        `Finalized publication delay: ${finalizedPublicationDelayMs !== null ? `${finalizedPublicationDelayMs} ms` : "N/A"}`,
      );
    }
    console.log(
      `Steady-state output interval: ${steadyStateOutputIntervalMs !== null ? `${steadyStateOutputIntervalMs.toFixed(2)} ms` : "N/A"}`,
    );
    console.log(`Total results: ${results.length}`);
    console.log(`Average value: ${avgValue.toFixed(6)}`);
    console.log(`Value range: ${minValue.toFixed(6)} - ${maxValue.toFixed(6)}`);

    return true;
  }

  run() {
    console.log("Pattern Result Extractor");
    console.log("=======================");
    console.log(`Approach: ${this.approach}`);
    console.log(`Pattern: ${this.patternName}`);
    console.log(`Log dir: ${this.logDir}`);
    console.log("");

    const extractedData = this.extractResults();

    if (extractedData) {
      this.saveResults(extractedData);
      console.log("\n✓ Extraction completed successfully");
      return true;
    } else {
      console.log("\n✗ Extraction failed");
      return false;
    }
  }
}

if (require.main === module) {
  const args = process.argv.slice(2);

  if (args.length < 3) {
    console.error(
      "Usage: node extract-pattern-results.js <approach> <pattern_name> <log_dir>",
    );
    console.error(
      "Example: node extract-pattern-results.js fetching exponential_growth_rate_1 ./logs/...",
    );
    process.exit(1);
  }

  const [approach, patternName, logDir] = args;

  if (
    approach !== "fetching" &&
    approach !== "approximation" &&
    approach !== "chunked"
  ) {
    console.error(
      'Error: Approach must be "fetching", "approximation", or "chunked"',
    );
    process.exit(1);
  }

  const extractor = new PatternResultExtractor(approach, patternName, logDir);
  const success = extractor.run();

  process.exit(success ? 0 : 1);
}

module.exports = {
  PatternResultExtractor,
};
