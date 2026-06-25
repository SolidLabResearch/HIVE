const fs = require("fs");
const os = require("os");
const path = require("path");

const { PatternResultExtractor } = require("./extract-pattern-results");

describe("pattern approximation extractor", () => {
test("accepts completed-window approximation latency rows", () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pattern-approx-extract-"));

    try {
      const logDir = path.join(tempRoot, "approximation", "low_variability", "iteration1");
      fs.mkdirSync(logDir, { recursive: true });

      fs.writeFileSync(
        path.join(logDir, "attempt_metadata.json"),
        `${JSON.stringify({
          benchmark_event_time_anchor: 1716454104620,
          output_window_range: "120000",
          output_window_step: "60000",
        }, null, 2)}\n`,
      );
      fs.writeFileSync(path.join(logDir, "approximation_approach_log.csv"), "timestamp,message\n");
      fs.writeFileSync(
        path.join(logDir, "approximation_latency_log.csv"),
        [
          "window_number,query_registered_at,result_emitted_at,approximation_status,result_value",
          "1,1782380768681,1782380890155,completed_window_approximation,-23.00835540094843",
          "2,1782380768681,1782380950164,completed_window_approximation,-23.012102293042382",
        ].join("\n"),
      );

      const extractor = new PatternResultExtractor("approximation", "low_variability", logDir);
      const extracted = extractor.extractResults();

      expect(extracted).not.toBeNull();
      expect(extracted.queryRegisteredTime).toBe(1782380768681);
      expect(extracted.firstResultTime).toBe(1782380890155);
      expect(extracted.results).toHaveLength(2);
      expect(extracted.results[0]).toMatchObject({
        windowNumber: 1,
        resultValue: -23.00835540094843,
        timestamp: 1782380890155,
        windowStart: 1716454104620,
        windowEnd: 1716454224620,
      });
      expect(extracted.results[1]).toMatchObject({
        windowNumber: 2,
        resultValue: -23.012102293042382,
        timestamp: 1782380950164,
        windowStart: 1716454164620,
        windowEnd: 1716454284620,
      });
    } finally {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });
  test("preserves empty latency columns when parsing completed-window approximation rows", () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pattern-approx-empty-cols-"));
    const logDir = path.join(tempDir, "approximation", "low_variability", "iteration1");
    fs.mkdirSync(logDir, { recursive: true });

    fs.writeFileSync(
      path.join(logDir, "attempt_metadata.json"),
      JSON.stringify({
        benchmark_event_time_anchor: 1756122905256,
        output_window_range: 120000,
        output_window_step: 60000,
      }),
    );

    fs.writeFileSync(
      path.join(logDir, "approximation_approach_log.csv"),
      "timestamp,message\n",
    );

    fs.writeFileSync(
      path.join(logDir, "approximation_latency_log.csv"),
      [
        "window_number,query_registered_at,first_data_received_at,expected_window_close,registration_anchored_expected_close,event_time_window_close,wall_clock_window_close,last_data_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_data_ms,wall_clock_close_to_result_ms,latency_domain_status,approximation_status,window_semantics,logical_trigger_time,window_start,window_end,window_data_close_time,latency_from_logical_trigger_ms,latency_from_window_close_ms,metadata_source,result_value",
        "1,1782309099900,1782309131343,1782309219900,1782309219900,1756123025256,,1782309221565,1782309221566,1666,-29777,1,,domain_mismatch,completed_window_approximation,trailing,1756122965256,1756122905256,1756123025256,1756123025256,,,reconstructed,1.0020238541666666",
      ].join("\n"),
    );

    const extractor = new PatternResultExtractor("approximation", "low_variability", logDir);
    const extracted = extractor.extractResults();

    expect(extracted.results).toHaveLength(1);
    expect(extracted.results[0]).toMatchObject({
      windowNumber: 1,
      timestamp: 1782309221566,
      resultValue: 1.0020238541666666,
      windowStart: 1756122905256,
      windowEnd: 1756123025256,
    });
    expect(extracted.queryRegisteredTime).toBe(1782309099900);
    expect(extracted.firstResultTime).toBe(1782309221566);
  });
});
