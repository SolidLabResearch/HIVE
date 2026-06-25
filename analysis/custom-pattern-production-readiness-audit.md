# Custom-Pattern Production Readiness Robustness Audit

## Scope

Robustness and readiness audit of the custom-pattern benchmark pipeline for long-running multi-iteration evaluations on the server.

Approaches analyzed: fetching, approximation, chunked.
Patterns analyzed: low_variability, step_pattern, spike_pattern, low_freq_oscillation, high_freq_oscillation.

---

## 1. Reproducibility

### Deterministic Inputs and Pattern Generation
- Step, spike, and oscillation patterns use pure deterministic mathematical formulas based on sampling intervals and time offsets.
- The low_variability pattern uses gaussianRandom() which relies directly on Math.random(). This is a reproducibility issue. If datasets are regenerated, low_variability will produce different data values, changing the exact baseline and approximation metrics.

### Event-Time Anchor Handling
- The runner extracts the min timestamp from the generated dataset to set the event-time anchor.
- Since the base date in the generator is fixed to 2024-05-23T08:48:24.620Z, the event-time anchor is always 1716454104620. This is fully deterministic.

### Window Numbering
- Window numbering is aligned across all approaches (windows 1..35).
- Accuracy comparisons match by windowNumber, removing any alignment drift due to minor timing variations.

---

## 2. Completeness

- Target windows cap stops the runs after target windows are reached (e.g. 5 or 35).
- Trimmed-window methodology is consistently applied. Raw summaries contain all windows (1..5 or 1..35) and trimmed summaries contain windows 4..33 (in 35-window runs) or 4..5 (in 5-window runs).
- Completeness metrics (matched windows, missing windows, and extra windows) are calculated accurately in compareResults().

---

## 3. Failure Recovery

### Process Crashes
- Publisher crash: Detected via publisherProc.on(close). Sets attempt state to failed.
- Orchestrator crash: approachProc.on(close) only logs the event. It does not terminate the run early or set benchmarkStatus to failed. The replayer continues to run and the case waits for the watchdog timeout, wasting significant runtime (up to 39 minutes for a 35-window run). This is a critical blocker.
- MQTT broker restart: Processes will lose connections. Although they attempt reconnection, the timing offsets in real-time streaming will cause extraction failures or timeouts.
- Analysis script crash: Caught in the main run harness. The raw test outputs inside custom_pattern_comparison_summary.json and the iteration folders are preserved.

### Partial Results
- Case results are saved to disk immediately upon completion of each test case.
- One failure does not abort the entire run; the runner moves to the next approach/pattern and completes the remaining tests.

---

## 4. Resource Collection

- Sourced from process-tree metrics, providing accurate CPU-seconds and peak/mean RSS in MiB.
- Sourced from mqtt_traffic_summary.json, providing detailed byte counts.
- Consistently written to iteration folders for all approaches.

---

## 5. Analysis Integrity

- Raw summaries are written to summary.raw.json and summary.raw.csv.
- Trimmed summaries are written to summary.trimmed-4-33.json and summary.trimmed-4-33.csv.
- Backwards compatible summary.json and summary.csv are overwritten with trimmed results as expected.
- MAE/MAPE/RMSE calculations protect against division-by-zero using Number.EPSILON checks on baseline values.

---

## 6. Long-Run Risks

### WATCHDOG TIMEOUTS
- Watchdog timeout scales dynamically: Math.max((derivedReplayDurationSeconds * 1000) + 120000, explicitTimeout).
- For a 35-window run, the timeout is 38 minutes, which accommodates the 36-minute replay time plus a 2-minute buffer.

### PROCESS HANGS
- The watchdog timer ensures that hung orchestrator or publisher processes are terminated via SIGTERM (falling back to SIGKILL if alive after 2 seconds).
- Stale process cleanups are executed at startup and shutdown, preventing process leaks on the server.

---

## 7. Server Readiness Blocker List

### Blocker 1: Orchestrator exit is ignored during execution
- Classification: Critical
- Impact: If the approach orchestrator crashes on startup or during execution, the runner does not abort early. It waits for the publisher to finish or the watchdog timeout to expire, wasting up to 39 minutes of server execution time per case.

### Blocker 2: Non-deterministic pattern generation for low_variability
- Classification: Important
- Impact: Math.random() is used to generate the low_variability dataset. Regenerating the dataset changes the actual input values, rendering error metrics non-reproducible.

### Blocker 3: Hardcoded trim window range (4..33) in analysis
- Classification: Important
- Impact: Running with target windows different from 35 (e.g. 5) results in an empty or misleading trimmed summary file because the bounds are hardcoded to 4..33.

---

## Production Readiness Verdict

- Ready for 35-iteration server execution: NO
- Critical issues count: 1
- Important issues count: 2
- Nice-to-have issues count: 0
