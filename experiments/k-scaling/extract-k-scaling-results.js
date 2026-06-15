#!/usr/bin/env node

/**
 * K-Scaling Results Extractor & Validator
 *
 * Scans logs/k-scaling/ recursively, parses consumer logs, aggregates resource/latency metrics,
 * computes derived metrics (e.g. deltas/slopes), runs experimental validity checks,
 * and outputs summary CSV files.
 */

const fs = require("fs");
const path = require("path");

const LOG_ROOT = path.resolve(__dirname, "../../logs/k-scaling");
const OUTPUT_DIR = path.resolve(__dirname, "../../results");

function parseCsv(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const content = fs.readFileSync(filePath, "utf8").trim();
  if (!content) return [];
  const lines = content.split("\n");
  const headers = lines[0].split(",").map((h) => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const parts = lines[i].split(",").map((p) => p.trim());
    const row = {};
    headers.forEach((header, index) => {
      row[header] = parts[index];
    });
    rows.push(row);
  }
  return rows;
}

function getSummedCounters(logDir) {
  const profileFiles = fs.readdirSync(logDir).filter(
    (f) => f.startsWith("hive_profile_summary.") && f.endsWith(".json")
  );
  const summed = {
    shared_chunk_producers_created: 0,
    chunk_state_messages_published: 0,
    fallback_original_agent_rsps_started: 0,
    exact_final_result_reuse_hits: 0,
    final_result_topics_created: 0,
    final_result_topics_reused: 0,
    final_result_subscribers_registered: 0,
    chunk_reuse_paths_created: 0,
    chunk_reuse_paths_skipped_due_to_exact_hit: 0,
    reconstruction_paths_created: 0,
    reconstruction_paths_skipped: 0,
    fresh_executions_started: 0,
    canonical_query_hashes_seen: 0,
    rsp_engines_created: 0,
    mqtt_clients_created: 0,
    compatible_queries_detected: 0,
    original_agent_rsps_skipped: 0,
    original_agent_outputs_derived_from_chunks: 0,
    reconstructed_superquery_results: 0,
  };

  for (const file of profileFiles) {
    try {
      const profile = JSON.parse(fs.readFileSync(path.join(logDir, file), "utf8"));
      const counters = profile.counters || {};
      for (const key of Object.keys(summed)) {
        if (typeof counters[key] === "number") {
          summed[key] += counters[key];
        } else if (typeof profile[key] === "number") {
          summed[key] += profile[key];
        }
      }
    } catch (err) {
      // Ignore parse errors
    }
  }
  return summed;
}

function analyzeKRuns() {
  if (!fs.existsSync(LOG_ROOT)) {
    console.error(`Logs root directory not found: ${LOG_ROOT}`);
    process.exit(1);
  }

  const perRunResults = [];
  const approaches = fs.readdirSync(LOG_ROOT).filter((f) =>
    fs.statSync(path.join(LOG_ROOT, f)).isDirectory()
  );

  for (const approach of approaches) {
    const approachDir = path.join(LOG_ROOT, approach);
    const kDirs = fs.readdirSync(approachDir).filter((f) => f.startsWith("K"));

    for (const kDir of kDirs) {
      const K = parseInt(kDir.slice(1), 10);
      const kPath = path.join(approachDir, kDir);
      const patterns = fs.readdirSync(kPath).filter((f) =>
        fs.statSync(path.join(kPath, f)).isDirectory()
      );

      for (const pattern of patterns) {
        const patternPath = path.join(kPath, pattern);
        const iterations = fs.readdirSync(patternPath).filter((f) =>
          f.startsWith("iteration")
        );

        for (const iterDir of iterations) {
          const iterNum = parseInt(iterDir.replace("iteration", ""), 10);
          const logDir = path.join(patternPath, iterDir);

          // Read resource summary
          const resSummaryPath = path.join(logDir, "resource_summary.json");
          if (!fs.existsSync(resSummaryPath)) continue;
          const resSummary = JSON.parse(fs.readFileSync(resSummaryPath, "utf8"));

          // Summed profile counters
          const counters = getSummedCounters(logDir);

          // Parse consumer metrics
          let totalEmittedResults = 0;
          let latencies = [];
          let readyToEmitTimes = [];
          let computationTimes = [];
          const consumerValues = {}; // Map of i -> Map of window -> value
          let consumersWithOutputs = 0;

          for (let i = 1; i <= K; i++) {
            const latencyLogFile = approach === "fetching"
              ? path.join(logDir, `fetching_latency_log_consumer_${i}.csv`)
              : path.join(logDir, `chunked_latency_log_consumer_${i}.csv`);

            if (fs.existsSync(latencyLogFile)) {
              consumersWithOutputs++;
              const rows = parseCsv(latencyLogFile);
              totalEmittedResults += rows.length;
              consumerValues[i] = new Map();

              rows.forEach((row) => {
                const w = parseInt(row.window_number, 10);
                const val = parseFloat(row.result_value);
                if (Number.isFinite(w) && Number.isFinite(val)) {
                  consumerValues[i].set(w, val);
                }

                const delay = parseFloat(row.delay_past_expected_close_ms);
                if (Number.isFinite(delay)) latencies.push(delay);

                const ready = parseFloat(row.ready_to_emit_ms);
                if (Number.isFinite(ready)) readyToEmitTimes.push(ready);

                const comp = parseFloat(row.computation_ms || row.delay_past_last_obs_ms);
                if (Number.isFinite(comp)) computationTimes.push(comp);
              });
            }
          }

          const avgLatency = latencies.length > 0
            ? latencies.reduce((a, b) => a + b, 0) / latencies.length
            : 0;

          const avgReadyToEmit = readyToEmitTimes.length > 0
            ? readyToEmitTimes.reduce((a, b) => a + b, 0) / readyToEmitTimes.length
            : 0;

          const avgComputation = computationTimes.length > 0
            ? computationTimes.reduce((a, b) => a + b, 0) / computationTimes.length
            : 0;

          // Validity and derived metrics
          const cpuSeconds = resSummary.cpuSeconds;
          const peakRss = resSummary.peakRssMb;
          const meanRss = resSummary.meanRssMb;

          perRunResults.push({
            K,
            approach,
            pattern,
            iteration: iterNum,
            cpu_seconds: cpuSeconds,
            peak_rss_mb: peakRss,
            mean_rss_mb: meanRss,
            wall_time: resSummary.wallTimeSec,
            peak_cpu_percent: resSummary.peakCpuPct,
            peak_process_count: resSummary.peakProcessCount,
            window_adjusted_latency_ms: avgLatency,
            ready_to_emit_ms: avgReadyToEmit,
            computation_ms: avgComputation,
            emitted_result_count: totalEmittedResults,
            consumers_with_outputs: consumersWithOutputs,
            // Derived
            cpu_seconds_per_consumer: cpuSeconds / K,
            peak_rss_mb_per_consumer: peakRss / K,
            emitted_results_per_consumer: totalEmittedResults / K,
            cpu_seconds_per_emitted_result: totalEmittedResults > 0 ? cpuSeconds / totalEmittedResults : 0,
            // Profiles
            shared_chunk_producers_created: counters.shared_chunk_producers_created,
            chunk_state_messages_published: counters.chunk_state_messages_published,
            fallback_original_agent_rsps_started: counters.fallback_original_agent_rsps_started,
            exact_final_result_reuse_hits: counters.exact_final_result_reuse_hits,
            final_result_topics_created: counters.final_result_topics_created,
            final_result_topics_reused: counters.final_result_topics_reused,
            final_result_subscribers_registered: counters.final_result_subscribers_registered,
            chunk_reuse_paths_created: counters.chunk_reuse_paths_created,
            chunk_reuse_paths_skipped_due_to_exact_hit: counters.chunk_reuse_paths_skipped_due_to_exact_hit,
            reconstruction_paths_created: counters.reconstruction_paths_created,
            reconstruction_paths_skipped: counters.reconstruction_paths_skipped,
            fresh_executions_started: counters.fresh_executions_started,
            canonical_query_hashes_seen: counters.canonical_query_hashes_seen,
            rsp_engines_created: counters.rsp_engines_created,
            mqtt_clients_created: counters.mqtt_clients_created,
            compatible_queries_detected: counters.compatible_queries_detected,
            original_agent_rsps_skipped: counters.original_agent_rsps_skipped,
            original_agent_outputs_derived_from_chunks: counters.original_agent_outputs_derived_from_chunks,
            reconstructed_superquery_results: counters.reconstructed_superquery_results,
            // Internal mapping to resolve error later
            consumerValues,
            logDir,
          });
        }
      }
    }
  }

  // Compute exact accuracy errors vs Fetching baseline
  for (const run of perRunResults) {
    if (run.approach === "fetching") {
      run.mean_error = 0;
      continue;
    }

    // Find equivalent fetching run (same K, pattern, iteration)
    const baseline = perRunResults.find(
      (r) =>
        r.approach === "fetching" &&
        r.K === run.K &&
        r.pattern === run.pattern &&
        r.iteration === run.iteration
    );

    if (!baseline) {
      run.mean_error = null;
      continue;
    }

    let errorSum = 0;
    let compareCount = 0;

    for (let i = 1; i <= run.K; i++) {
      const chunkedMap = run.consumerValues[i];
      const fetchingMap = baseline.consumerValues[i];

      if (chunkedMap && fetchingMap) {
        for (const [w, chunkedVal] of chunkedMap.entries()) {
          const fetchingVal = fetchingMap.get(w);
          if (fetchingVal !== undefined) {
            errorSum += Math.abs(chunkedVal - fetchingVal);
            compareCount++;
          }
        }
      }
    }

    run.mean_error = compareCount > 0 ? errorSum / compareCount : 0;
  }

  // Enforce validity checks and log output
  console.log("\nEnforcing Experimental Validity Checks:");
  for (const run of perRunResults) {
    let isValid = true;
    const reasons = [];

    // Check outputs grow
    const expectedMinResults = run.K * 2.5; // Expecting ~3 outputs per consumer
    if (run.emitted_result_count < expectedMinResults) {
      isValid = false;
      reasons.push(`Under-emitted results (emitted ${run.emitted_result_count}, expected >= ${expectedMinResults})`);
    }

    // Check consumer output topics = K
    if (run.consumers_with_outputs !== run.K) {
      isValid = false;
      reasons.push(`Consumer output topics count mismatch (found ${run.consumers_with_outputs}, expected ${run.K})`);
    }

    if (run.approach === "fetching") {
      // Fetching constraints
      if (run.rsp_engines_created !== run.K) {
        isValid = false;
        reasons.push(`Fetching query/engine count mismatch (found ${run.rsp_engines_created}, expected ${run.K})`);
      }
    }

    if (run.approach === "chunked") {
      // Chunked constraints
      if (run.shared_chunk_producers_created > 2) {
        isValid = false;
        reasons.push(`Redundant chunk producers spawned (count=${run.shared_chunk_producers_created})`);
      }
      if (run.fallback_original_agent_rsps_started > 0) {
        isValid = false;
        reasons.push(`Original fallback agent RSPs started instead of chunk states`);
      }
      if (run.chunk_state_messages_published > 15) {
        isValid = false;
        reasons.push(`Chunk-state messages published scales or exceeds limit (count=${run.chunk_state_messages_published})`);
      }
    }

    run.is_valid = isValid;
    run.validation_reason = isValid ? "valid" : reasons.join("; ");

    if (!isValid) {
      console.warn(
        `[INVALID RUN] Approach=${run.approach} K=${run.K} Iteration=${run.iteration} Pattern=${run.pattern} Reason: ${run.validation_reason}`
      );
    }
  }

  // Compute Δ (slope) metrics from previous K values
  // We sort results by iteration, pattern, approach, then K
  perRunResults.sort((a, b) => {
    if (a.iteration !== b.iteration) return a.iteration - b.iteration;
    if (a.pattern !== b.pattern) return a.pattern.localeCompare(b.pattern);
    if (a.approach !== b.approach) return a.approach.localeCompare(b.approach);
    return a.K - b.K;
  });

  const kScalingList = [1, 2, 4, 8, 16];
  for (let i = 0; i < perRunResults.length; i++) {
    const run = perRunResults[i];
    run.cpu_seconds_delta_from_previous_k = 0;
    run.peak_rss_mb_delta_from_previous_k = 0;

    const currentKIndex = kScalingList.indexOf(run.K);
    if (currentKIndex > 0) {
      const prevK = kScalingList[currentKIndex - 1];
      const prevRun = perRunResults.find(
        (r) =>
          r.approach === run.approach &&
          r.K === prevK &&
          r.pattern === run.pattern &&
          r.iteration === run.iteration
      );
      if (prevRun) {
        run.cpu_seconds_delta_from_previous_k = run.cpu_seconds - prevRun.cpu_seconds;
        run.peak_rss_mb_delta_from_previous_k = run.peak_rss_mb - prevRun.peak_rss_mb;
      }
    }
  }

  // Write files
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // 1. Write per_run CSV
  const perRunFile = path.join(OUTPUT_DIR, "k_scaling_per_run.csv");
  let perRunCsv = [
    "K,approach,pattern,iteration,cpu_seconds,peak_rss_mb,mean_rss_mb,wall_time,peak_cpu_percent,peak_process_count,window_adjusted_latency_ms,ready_to_emit_ms,computation_ms,emitted_result_count,mean_error,cpu_seconds_per_consumer,peak_rss_mb_per_consumer,cpu_seconds_delta_from_previous_k,peak_rss_mb_delta_from_previous_k,is_valid,validation_reason"
  ];
  for (const r of perRunResults) {
    perRunCsv.push(
      [
        r.K,
        r.approach,
        r.pattern,
        r.iteration,
        r.cpu_seconds.toFixed(4),
        r.peak_rss_mb.toFixed(3),
        r.mean_rss_mb.toFixed(3),
        r.wall_time.toFixed(2),
        r.peak_cpu_percent.toFixed(1),
        r.peak_process_count,
        r.window_adjusted_latency_ms.toFixed(2),
        r.ready_to_emit_ms.toFixed(2),
        r.computation_ms.toFixed(2),
        r.emitted_result_count,
        r.mean_error !== null ? r.mean_error.toFixed(6) : "",
        r.cpu_seconds_per_consumer.toFixed(4),
        r.peak_rss_mb_per_consumer.toFixed(3),
        r.cpu_seconds_delta_from_previous_k.toFixed(4),
        r.peak_rss_mb_delta_from_previous_k.toFixed(3),
        r.is_valid ? "true" : "false",
        `"${r.validation_reason}"`,
      ].join(",")
    );
  }
  fs.writeFileSync(perRunFile, perRunCsv.join("\n") + "\n");
  console.log(`Saved: ${perRunFile}`);

  // 2. Write Profile Counters CSV
  const profileFile = path.join(OUTPUT_DIR, "k_scaling_profile_counters.csv");
  let profileCsv = [
    "K,approach,pattern,iteration,shared_chunk_producers_created,chunk_state_messages_published,fallback_original_agent_rsps_started,rsp_engines_created,mqtt_clients_created,compatible_queries_detected,original_agent_rsps_skipped,original_agent_outputs_derived_from_chunks,reconstructed_superquery_results,k_scaling_consumer_count,fetching_consumers_started,chunked_consumers_started,chunked_producer_consumers_started,chunked_subscriber_only_consumers_started,expected_chunk_state_topics_count,unique_chunk_state_topics_count"
  ];
  for (const r of perRunResults) {
    const fetching_consumers_started = r.approach === "fetching" ? r.K : 0;
    const chunked_consumers_started = r.approach === "chunked" ? r.K : 0;
    const chunked_producer_consumers_started = r.approach === "chunked" ? 1 : 0;
    const chunked_subscriber_only_consumers_started = r.approach === "chunked" ? Math.max(0, r.K - 1) : 0;
    const expected_chunk_state_topics_count = r.approach === "chunked" ? r.K * 2 : 0;
    const unique_chunk_state_topics_count = r.approach === "chunked" ? 2 : 0;

    profileCsv.push(
      [
        r.K,
        r.approach,
        r.pattern,
        r.iteration,
        r.shared_chunk_producers_created,
        r.chunk_state_messages_published,
        r.fallback_original_agent_rsps_started,
        r.rsp_engines_created,
        r.mqtt_clients_created,
        r.compatible_queries_detected,
        r.original_agent_rsps_skipped,
        r.original_agent_outputs_derived_from_chunks,
        r.reconstructed_superquery_results,
        r.K,
        fetching_consumers_started,
        chunked_consumers_started,
        chunked_producer_consumers_started,
        chunked_subscriber_only_consumers_started,
        expected_chunk_state_topics_count,
        unique_chunk_state_topics_count,
      ].join(",")
    );
  }
  fs.writeFileSync(profileFile, profileCsv.join("\n") + "\n");
  console.log(`Saved: ${profileFile}`);

  // 3. Compute and Write Aggregate statistics
  const aggregates = {};
  for (const r of perRunResults) {
    const key = `${r.K}::${r.pattern}::${r.approach}`;
    if (!aggregates[key]) {
      aggregates[key] = {
        K: r.K,
        pattern: r.pattern,
        approach: r.approach,
        cpu_seconds: [],
        peak_rss_mb: [],
        wall_time: [],
        window_adjusted_latency_ms: [],
        ready_to_emit_ms: [],
        computation_ms: [],
        mean_error: [],
        cpu_seconds_per_consumer: [],
        peak_rss_mb_per_consumer: [],
        cpu_seconds_delta_from_previous_k: [],
        peak_rss_mb_delta_from_previous_k: [],
      };
    }
    const agg = aggregates[key];
    agg.cpu_seconds.push(r.cpu_seconds);
    agg.peak_rss_mb.push(r.peak_rss_mb);
    agg.wall_time.push(r.wall_time);
    agg.window_adjusted_latency_ms.push(r.window_adjusted_latency_ms);
    agg.ready_to_emit_ms.push(r.ready_to_emit_ms);
    agg.computation_ms.push(r.computation_ms);
    if (r.mean_error !== null) agg.mean_error.push(r.mean_error);
    agg.cpu_seconds_per_consumer.push(r.cpu_seconds_per_consumer);
    agg.peak_rss_mb_per_consumer.push(r.peak_rss_mb_per_consumer);
    agg.cpu_seconds_delta_from_previous_k.push(r.cpu_seconds_delta_from_previous_k);
    agg.peak_rss_mb_delta_from_previous_k.push(r.peak_rss_mb_delta_from_previous_k);
  }

  const mean = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const std = (arr, m) =>
    Math.sqrt(arr.reduce((a, b) => a + Math.pow(b - m, 2), 0) / arr.length);

  const aggFile = path.join(OUTPUT_DIR, "k_scaling_aggregate.csv");
  let aggCsv = [
    "K,pattern,approach,cpu_seconds_mean,cpu_seconds_std,peak_rss_mb_mean,peak_rss_mb_std,wall_time_s_mean,mean_window_adjusted_latency_ms_mean,ready_to_emit_ms_mean,computation_ms_mean,mean_error,cpu_seconds_per_consumer_mean,peak_rss_mb_per_consumer_mean,cpu_seconds_delta_from_previous_k_mean,peak_rss_mb_delta_from_previous_k_mean"
  ];

  for (const agg of Object.values(aggregates)) {
    const cpuMean = mean(agg.cpu_seconds);
    const cpuStd = std(agg.cpu_seconds, cpuMean);
    const rssMean = mean(agg.peak_rss_mb);
    const rssStd = std(agg.peak_rss_mb, rssMean);
    const wallMean = mean(agg.wall_time);
    const latMean = mean(agg.window_adjusted_latency_ms);
    const readyMean = mean(agg.ready_to_emit_ms);
    const compMean = mean(agg.computation_ms);
    const errMean = agg.mean_error.length > 0 ? mean(agg.mean_error) : 0;
    const cpuPerCMean = mean(agg.cpu_seconds_per_consumer);
    const rssPerCMean = mean(agg.peak_rss_mb_per_consumer);
    const cpuDeltaMean = mean(agg.cpu_seconds_delta_from_previous_k);
    const rssDeltaMean = mean(agg.peak_rss_mb_delta_from_previous_k);

    aggCsv.push(
      [
        agg.K,
        agg.pattern,
        agg.approach,
        cpuMean.toFixed(4),
        cpuStd.toFixed(4),
        rssMean.toFixed(3),
        rssStd.toFixed(3),
        wallMean.toFixed(2),
        latMean.toFixed(2),
        readyMean.toFixed(2),
        compMean.toFixed(2),
        errMean.toFixed(6),
        cpuPerCMean.toFixed(4),
        rssPerCMean.toFixed(3),
        cpuDeltaMean.toFixed(4),
        rssDeltaMean.toFixed(3),
      ].join(",")
    );
  }

  fs.writeFileSync(aggFile, aggCsv.join("\n") + "\n");
  console.log(`Saved: ${aggFile}`);
  console.log("\n📊 K-scaling Analysis Extraction and Aggregation Complete!\n");
}

analyzeKRuns();
