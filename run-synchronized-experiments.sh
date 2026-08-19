#!/bin/bash

# Run all three approaches with synchronized 3-minute duration for fair comparison

echo "=== Running Synchronized Experiments at 0.1 Hz ==="
echo "Each experiment will run for exactly 3 minutes (180 seconds)"
echo ""

# Clean old logs
echo "Cleaning previous latency logs..."
> fetching_latency_log.csv
echo "window_number,query_registered_at,first_data_received_at,expected_window_close,last_obs_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_obs_ms,result_value" > fetching_latency_log.csv

> approximation_latency_log.csv
echo "window_number,query_registered_at,first_data_received_at,expected_window_close,last_data_received_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,latency_from_last_data_ms,result_value" > approximation_latency_log.csv

> chunked_latency_log.csv
echo "window_number,query_registered_at,first_data_received_at,expected_window_close,last_chunk_received_at,interval_trigger_at,result_emitted_at,latency_from_query_reg_ms,latency_from_data_start_ms,interval_wait_ms,computation_ms,result_value" > chunked_latency_log.csv

echo ""

# Run Fetching
echo "node experiments/frequency-comparison/experiment-frequency-comparison-fetching.js test complex_oscillation 0.1"
node experiments/frequency-comparison/experiment-frequency-comparison-fetching.js test complex_oscillation 0.1 &
fetching_pid=$!
sleep 180
pkill -P $fetching_pid
kill $fetching_pid 2>/dev/null
echo "Fetching experiment completed"
echo ""
sleep 5

# Run Approximation  
echo "node experiments/frequency-comparison/experiment-frequency-comparison-approximation.js test complex_oscillation 0.1"
node experiments/frequency-comparison/experiment-frequency-comparison-approximation.js test complex_oscillation 0.1 &
approx_pid=$!
sleep 180
pkill -P $approx_pid
kill $approx_pid 2>/dev/null
echo "Approximation experiment completed"
echo ""
sleep 5

# Run Chunked (uses different argument format - just frequency)
echo "node experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js 0.1"
node experiments/frequency-comparison/experiment-frequency-chunked-with-capture.js 0.1 &
chunked_pid=$!
sleep 180
pkill -P $chunked_pid
kill $chunked_pid 2>/dev/null
echo "Chunked experiment completed"
echo ""

echo "=== All Experiments Completed ==="
echo ""
echo "Results:"
echo "Fetching windows:"
tail -n +2 fetching_latency_log.csv | wc -l
echo "Approximation windows:"
tail -n +2 approximation_latency_log.csv | wc -l  
echo "Chunked windows:"
tail -n +2 chunked_latency_log.csv | wc -l
