# Experiments

This directory contains experiments for evaluating the performance, accuracy, and resource usage of different streaming query approaches.

## Directory Structure

*   **`real-data-comparison/`**: **(Primary Suite)**
    *   Tests all 3 approaches (Fetching, Approximation, Chunked) using **real sensor data**.
    *   Measures Latency, Accuracy, and Resource Usage.
    *   **How to Run:**
        *   `node experiments/real-data-comparison/run-real-data-3-approaches.js` (Runs 3 iterations by default)
        *   `node experiments/real-data-comparison/run-real-data-3-approaches.js --iterations 1` (Quick test)
    *   **Logs Location:** `experiments/real-data-comparison/logs/`

*   **`pattern-analysis/`**:
    *   Tests approaches against specific synthetic data patterns (Exponential Growth/Decay, Noise).
    *   Measures how well algorithms handle specific data shapes.
    *   **How to Run:**
        *   `node experiments/pattern-analysis/run-all-patterns-comparison.js` (Runs full suite, 35 iterations default)
        *   `node experiments/pattern-analysis/run-all-patterns-comparison.js --iterations 1` (Quick test)
        *   `node experiments/pattern-analysis/run-all-patterns-comparison.js exponential_growth 0.1` (Specific pattern)
    *   **Logs Location:** `experiments/pattern-analysis/logs/`

*   **`frequency-comparison/`**:
    *   Tests the **Nyquist limit** and aliasing effects by varying signal frequency.
    *   Compares Fetching vs. Approximation accuracy as frequency increases.
    *   **How to Run:**
        *   `node experiments/frequency-comparison/run-frequency-comparison-with-capture.js` (Runs full suite)
    *   **Logs Location:** `experiments/frequency-comparison/logs/`

## Prerequisites

1.  **Build the Project:** `npm run build`
2.  **Start MQTT Broker:** Ensure `mosquitto` is running.
3.  **Data:** Ensure data files exist in `src/streamer/data/`.

## Note on Iterations

*   **Development/Quick Test:** Use `--iterations 1` (or `-i 1`) to verify functionality quickly (takes a few minutes).
*   **Production/Analysis:** Use defaults (usually 35 or 3 iterations) for statistical significance (can take hours).
